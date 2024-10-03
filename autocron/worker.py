"""
worker.py

worker class for handling cron- and delayed-tasks.
"""

# this module is part of autocron
# copyright (c) 2024 Klaus Bremer
# all rights reserved
#
# license: MIT

import argparse
import importlib
import os
import signal
import sys
import time

from autocron.schedule import CronScheduler
from autocron import sqlite_interface

# check for django, because this will need a modified setup and shutdown
try:
    import django
except ImportError:
    DJANGO_FRAMEWORK_IN_USE = False
else:
    DJANGO_FRAMEWORK_IN_USE = True

# base idle time (in seconds) for auto-calculation
DEFAULT_WORKER_IDLE_TIME = 1
NOOP_SIGNAL = 0


class Worker:
    """
    Runs in a separate process for task-handling.
    Gets supervised and monitored from the engine.
    """

    def __init__(self, args):
        self.active = True
        self.result = None
        self.error_message = None
        self.monitor_pid = args.monitorpid
        signal.signal(signal.SIGINT, self.terminate)
        signal.signal(signal.SIGTERM, self.terminate)

        # Get a SQLiteInterface instance 'as is' without the
        # initialization step to clean up tasks.
        # Providing the databasename will set the database
        # to an initialized-state.
        self.interface = sqlite_interface.SQLiteInterface()
        self.interface.init_database(args.dbfile)

        # prevent the interface to register functions from the worker-process:
        self.interface.accept_registrations = False

        self.worker_idle_time = self._get_worker_idle_time()
        if DJANGO_FRAMEWORK_IN_USE:
            django.setup()

    @property
    def monitor_missing(self):
        if self.monitor_pid is not None:
            try:
                # signal 0 does nothing but tries to access the process
                os.kill(self.monitor_pid, NOOP_SIGNAL)
            except OSError:
                # master process not found
                return True
        return False

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

                # check for missing monitor without receiving a SIGTERM
                # indication an unfriendly shutdown
                if self.monitor_missing and self.active:
                    # tear down database and exit worker-process
                    self.interface.tear_down_database()
                    self.active = False

    def handle_task(self):
        """
        Checks for a task on due and process the task. If there are no
        tasks to handle the method return `False` indicating that the
        main loop can switch to idle state. If a task have been handled,
        the method return `True` to indicate that meanwhile more tasks
        may be waiting.
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
                error_message=self.error_message,
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


def get_arguments():
    """takes `--dbfile` and `--monitorpid` as required arguments"""
    parser = argparse.ArgumentParser(prog="autocron.worker")
    parser.add_argument("--dbfile")
    parser.add_argument("--monitorpid", type=int)
    return parser.parse_args()


def start_worker():
    """subprocess entry-point"""
    # insert cwd of hosting application to pythonpath
    sys.path.insert(0, os.getcwd())
    worker = Worker(get_arguments())
    worker.run()


if __name__ == "__main__":
    start_worker()
    if DJANGO_FRAMEWORK_IN_USE:
        # escape the django reloader
        os._exit(0)
