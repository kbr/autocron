"""
worker.py

worker class for handling cron and delegated tasks.
"""

import importlib
import math
import os
import signal
import sys
import time

from autocron.schedule import CronScheduler
from autocron import sql_interface


WORKER_IDLE_TIME = 1  # one second as base idle time for auto-calculation


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
        # Providing the databasename will set the database
        # to initialized-state. So no new tasks are registered
        # from a worker.
        # (keep in mind that every worker runs in a separate process.)
        self.interface = sql_interface.SQLiteInterface()
        self.interface.db_name=database_filename
        self.interface.accept_registrations = False
        self.worker_idle_time = self.interface.get_worker_idle_time()

    def _get_worker_idle_time(self):
        """
        If worker_idle_time is in auto-mode (value in database settings
        is 0), then calculate the idle time based on the number of
        active workers. A higher idle time is necessary on higher
        numbers of workers to keep the  sqlite database accessible and
        reactive.

        Up to three workers the idle time is 1 second.
        From 4 worker on the idle time is int(log2(workers)):
        From 4 to 7 workers the idle time are 2 seconds,
        from 8 to 15 are 3 seconds,
        from 16 to 31 are 4 seconds
        and so on.
        """
        idle_time = self.interface.get_worker_idle_time()
        if not idle_time:
            max_workers = self.interface.get_max_workers()
            if max_workers >= 8:
                idle_time = int(math.log2(workers)) - 1
            else:
                idle_time = WORKER_IDLE_TIME
        return idle_time

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
                time.sleep(self.worker_idle_time)
        self.interface.decrement_running_workers(pid)

    def handle_tasks(self):
        """
        Checks for a task on due and process the task. If there are no tasks to
        handle the method return `False` indicating that the main loop
        can switch to idle state. If a task have been handled, the
        method return `True` to indicate that meanwhile more tasks may be
        waiting.
        """
        task = self.interface.get_next_task()
        if task:
            if self.active is False:
                # don't process the task and terminate as soon as possible.
                # crontasks will register on next start again and unhandled
                # task will remain in the database and marked as waiting
                # on next start to get handled again.
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
            self.interface.update_task_schedule(task, schedule)
        else:
            # not a cronjob: delete the task from the db
            self.interface.delete_task(task)


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
