"""
worker.py

worker class for handling cron and delegated tasks.
"""

import importlib
import os
import signal
import sys
import time

from autocron.configuration import configuration
from autocron.schedule import CronScheduler
from autocron.sql_interface import interface


class Worker:
    """
    Runs in a separate process for task-handling.
    Gets supervised and monitored from the engine.
    """
    def __init__(self):
        self.active = True
        self.result = None
        self.error_message = None
        signal.signal(signal.SIGINT, self.terminate)
        signal.signal(signal.SIGTERM, self.terminate)
        # prevent decorated function to register itself again
        # when called as task.
        # (this is save because the worker runs in its own process)
        configuration.is_active = False

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
        while self.active:
            if not self.handle_tasks():
                # nothing to do, check for results to delete:
                interface.delete_outdated_results()
                time.sleep(configuration.worker_idle_time)

    def handle_tasks(self):
        """
        Checks for tasks and process them. If there are no tasks to
        handle the method return `False` indicating that the main loop
        can switch to idle state. If  tasks have been handled, the
        method return `True` to indicate that meanwhile more tasks may be
        waiting.
        """
        tasks = interface.get_tasks_on_due()
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
            interface.update_result(
                uuid=task.uuid,
                result=self.result,
                error_message=self.error_message
            )
        if task.crontab:
            # if the task has a crontab calculate new schedule
            # and update the task-entry
            scheduler = CronScheduler(crontab=task.crontab)
            schedule = scheduler.get_next_schedule()
            interface.update_schedule(rowid=task.rowid, schedule=schedule)
        else:
            # not a cronjob: delete the task from the db
            interface.delete_callable(task)


def start_worker():
    """subprocess entry-point"""
    # insert cwd of hosting application to pythonpath
    sys.path.insert(0, os.getcwd())
    worker = Worker()
    worker.run()


if __name__ == "__main__":
    start_worker()
