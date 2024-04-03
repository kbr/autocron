"""
worker.py

worker class for handling cron- and delayed-tasks.
"""

import importlib
import os
import signal
import sys
import time

from autocron.schedule import CronScheduler
from autocron import sqlite_interface


# base idle time (in seconds) for auto-calculation
DEFAULT_WORKER_IDLE_TIME = 1


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
        # to an initialized-state.
        self.interface = sqlite_interface.SQLiteInterface()
        self.interface.init_database(database_filename)
        # prevent the interface to register functions from the worker-process:
        self.interface.accept_registrations = False
        self.worker_idle_time = self._get_worker_idle_time()

    def _get_worker_idle_time(self):
        """
        If worker_idle_time is in auto-mode (value in database settings
        is 0), then calculate the idle time based on the number of
        active workers. A higher idle time is necessary on higher
        numbers of workers to keep the sqlite database accessible and
        reactive.
        auto_idle time keeps 1 second for up to 8 workers. Then it adds
        0.025 seconds per additional worker.
        """
        idle_time = self.interface.worker_idle_time
        if not idle_time:
            workers = self.interface.max_workers
            default = DEFAULT_WORKER_IDLE_TIME
            idle_time = max(default, default + 0.025 * (workers - 8))
        return idle_time

    def terminate(self, *args):  # pylint: disable=unused-argument
        """
        Signal handler to stop the process, terminates the loop in
        `run()`. As a signal handler terminate must accept optional
        positional arguments.
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
            if not self.handle_task():
                # nothing to do, check for results to delete:
                self.interface.delete_outdated_results()
                # don't sleep too long in case of longer idle-times
                # wake up at least every second to check for self.active
                # to terminate as soon as possible:
                idle_time = self.worker_idle_time
                while idle_time > 0:
                    time.sleep(min(1, idle_time))
                    if not self.active:
                        break
                    idle_time -= 1

    def handle_task(self):
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
