"""
worker.py

worker class for handling cron and delegated tasks.
"""

import importlib
import os
import signal
import sys
import time

from autocron.schedule import CronScheduler
from autocron import sql_interface


WORKER_IDLE_TIME = 4.0  # seconds


class Worker:
    """
    Runs in a separate process for task-handling.
    Gets supervised and monitored from the engine.
    """
    def __init__(self, database_filename):
        self.active = True
        self.result = None
        self.error_message = None
        signal.signal(signal.SIGINT, self.terminate)
        signal.signal(signal.SIGTERM, self.terminate)
        # Get a SQLiteInterface instance 'as is' without the
        # initialization step to clean up tasks.
        # Providing the databasename will set the dtatabase
        # to initialized-state. So no new tasks are registered
        # from a worker.
        # (keep in mind that every worker runs in a separate process.)
        self.interface = sql_interface.SQLiteInterface()
        self.interface.db_name=database_filename
        self.interface.accept_registrations = False

    def terminate(self, *args):  # pylint: disable=unused-argument
        """
        Signal handler to stop the process, terminates the loop in
        `run()`.
        """
        self.active = False

    def run(self):
        """
        Main event loop for the worker. Takes callables and processes
        them as long as callables are available. Otherwise keep idle.
        """
        pid = os.getpid()
        self.interface.increment_running_workers(pid)
        while self.active:
            if not self.handle_tasks():
                # nothing to do, check for results to delete:
                self.interface.delete_outdated_results()
                time.sleep(self.interface.get_worker_idle_time())
        self.interface.decrement_running_workers(pid)

    def handle_tasks(self):
        """
        Checks for tasks and process them. If there are no tasks to
        handle the method return `False` indicating that the main loop
        can switch to idle state. If  tasks have been handled, the
        method return `True` to indicate that meanwhile more tasks may be
        waiting.
        """
        tasks = self.interface.get_tasks_on_due(
            status=sql_interface.TASK_STATUS_WAITING,
            new_status=sql_interface.TASK_STATUS_PROCESSING
        )
        if tasks:
            for task in tasks:
                if self.active is False:
                    # terminate as soon as possible
                    return True
                self.error_message = None
                self.result = None
                self.process_task(task)
                self.postprocess_task(task)
            return True
        return False

    def process_task(self, task):
        """
        Handle the given task. The task is a dictionary as returned from
        SQLInterface._fetch_all_callable_entries(cursor):

            {
                "rowid": integer,
                "uuid": string,
                "schedule": datetime,
                "crontab": string,
                "function_module": string,
                "function_name": string,
                "args": tuple(of original datatypes),
                "kwargs": dict(of original datatypes),
            }

        """
        module = importlib.import_module(task.function_module)
        function = getattr(module, task.function_name)
        try:
            self.result = function(*task.args, **task.kwargs)
        except Exception as err:  # pylint: disable=broad-exception-caught
            self.error_message = repr(err)

    def postprocess_task(self, task):
        """
        Delete or update the task and do something with the result or
        error-message.
        """
        if task.uuid:
            # if the task has a uuid, store the result / error-message
            self.interface.update_result(
                uuid=task.uuid,
                result=self.result,
                error_message=self.error_message
            )
        if task.crontab:
            # if the task has a crontab calculate new schedule
            # and update the task-entry
            scheduler = CronScheduler(crontab=task.crontab)
            schedule = scheduler.get_next_schedule()
            self.interface.update_crontask_schedule(
                rowid=task.rowid,
                schedule=schedule
            )
        else:
            # not a cronjob: delete the task from the db
            self.interface.delete_callable(task)


def start_worker():
    """subprocess entry-point"""
    # insert cwd of hosting application to pythonpath
    sys.path.insert(0, os.getcwd())
    # engine provides the database name as first argument:
    database_filename = sys.argv[1]
    worker = Worker(database_filename)
    worker.run()


if __name__ == "__main__":
    start_worker()
