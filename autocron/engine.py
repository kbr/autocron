"""
The Engine is the entry-point for autocron.

To use the Engine create an instance and call ``.start(filename)`` on
this instance with the database filename to use as argument. The
function ``start(filename)`` in the ``__init__.py`` module of autocron
handles this.
"""

import pathlib
import queue
import signal
import subprocess
import sys
import threading
import time
from types import SimpleNamespace

from .sqlite_interface import SQLiteInterface


WORKER_MODULE_NAME = "worker.py"
WORKER_START_DELAY = 0.02

REGISTER_BACKGROUND_TASK_TIMEOUT = 2.0


def start_subprocess(database_file):
    """
    Starts the worker process in a detached subprocess. The
    `database_file` is a string with an absolute or relative path to the
    database in use.
    """
    worker_file = pathlib.Path(__file__).parent / WORKER_MODULE_NAME
    cmd = [sys.executable, worker_file]
    if database_file:
        cmd.append(database_file)
    cwd = pathlib.Path.cwd()
    return subprocess.Popen(cmd, cwd=cwd)


# ---------------------------------------------------------------------
# Thread based background-task registration

class TaskRegistrator:
    """
    Handles the task registration in a separate thread so that
    registration is a non-blocking operation.
    """

    def __init__(self, interface):
        self.interface = interface
        self.task_queue = queue.Queue()
        self.exit_event = threading.Event()
        self.registration_thread = None

    def register(self, func, schedule=None, crontab="", uuid="",
                       args=(), kwargs=None, unique=False):
        """
        Register a task for later processing. Arguments are the same as
        for `SQLiteInterface.register_task()` which is called from a
        seperate thread.
        """
        if kwargs is None:
            kwargs = {}
        self.task_queue.put({
            "func": func,
            "schedule": schedule,
            "crontab": crontab,
            "uuid": uuid,
            "args": args,
            "kwargs": kwargs,
            "unique": unique
        })

    def _process_queue(self):
        """
        Register task in a separate thread taking the tasks from a
        task_queue.
        """
        while True:
            try:
                data = self.task_queue.get(
                    timeout=REGISTER_BACKGROUND_TASK_TIMEOUT
                )
            except queue.Empty:
                # check for exit_event on empty queue so the queue items
                # can get handled before terminating the thread
                if self.exit_event.is_set():
                    break
            else:
                # got a task for registration:
                # The data is a dict with the locals() from self.register()
                # excluding "self".
                self.interface.register_task(**data)

    def start(self):
        """
        Start processing the queue in a seperate thread.
        """
        # don't start multiple threads
        if self.registration_thread is None:
            self.registration_thread = threading.Thread(
                target=self._process_queue
            )
            self.registration_thread.start()

    def stop(self):
        """
        Terminates the running registration thread.
        """
        if self.registration_thread:
            self.exit_event.set()
            self.registration_thread = None


# ---------------------------------------------------------------------
# autocron entry point:
# Engine.start()
# see: __init__.py

class Engine:
    """
    The Engine is the entry-point for autocron. Starting the engine will
    start the monitor thread. The monitor-thread in turn starts and
    supervise the workers. (The interface argument is used for testing.)
    """
    def __init__(self, interface=None):
        # the ìnterface argument is for testing. In production this
        # argument is not provided for initialization.
        self.interface = interface if interface else SQLiteInterface()
        self.exit_event = None
        self.monitor_thread = None
        self.orig_signal_handlers = {}
        self.processes = []
        self.task_registrator = TaskRegistrator(self.interface)

    def worker_monitor(self):
        """
        Starts the worker processes and monitors that the workers are up and
        running. Restart workers if necessary. This function must run in a
        separate thread. The 'exit_event' is a threading.Event() instance
        and the `database_file` is a string with an absolute or relative
        path to the database in use. If the monitor receives an exit_event
        the running workers are terminated and the function will return,
        terminating its own thread as well.
        """
        database_file = self.interface.db_name
        for _ in range(self.interface.max_workers):
            process = start_subprocess(database_file)
            self.processes.append(
                SimpleNamespace(pid=process.pid, process=process)
            )
            time.sleep(WORKER_START_DELAY)
        while True:
            for entry in self.processes:
                if entry.process.poll() is not None:
                    # trouble: process is not running any more.
                    # deregister the terminated process from the setting
                    # and start a new process.
                    self.interface.decrement_running_workers(entry.pid)
                    new_process = start_subprocess(database_file)
                    entry.pid = new_process.pid
                    entry.process = new_process
                    # in case more than one process needs a restart:
                    time.sleep(WORKER_START_DELAY)
            if self.exit_event.wait(timeout=self.interface.monitor_idle_time):
                # terminate thread on exit-event:
                break

    def set_signal_handlers(self):
        """
        Set self._terminate() as handler for a couple of
        termination-signals and store the orinal handlers for this
        signals.
        """
        signalnums = [
            signal.SIGINT,
            signal.SIGTERM,
            signal.SIGABRT,
            signal.SIGHUP,  # availablity: Unix
        ]
        for signalnum in signalnums:
            self.orig_signal_handlers[signalnum] = signal.getsignal(signalnum)
            signal.signal(signalnum, self._terminate)

    def start(self, database_file):
        """
        Starts the autocron worker in case autocron is active and no
        other application process has already started the workers. The
        *database_file* argument is a string with the name of the
        database to use (like "the_application.db") or a Path instance.
        If the name represents a relative path, the database is stored
        in the ``~/.autocron`` directory. This directory will get
        created if not already existing. If the name represents an
        absolute path, then this path will be used as is. In this case
        all directories of the path must exist.

        The function returns a boolean: True if workers have been
        started and False otherwise. A return value of False does not
        mean, that no workers are running – another application process
        may have be first.
        """
        self.interface.init_database(database_file)
        if (
            self.interface.autocron_lock_is_set
            or not self.interface.is_worker_master
        ):
            # inactive or another process is already the worker master
            return False

        # this is a safety check for not starting more than
        # one monitor thread:
        if not self.monitor_thread:
            self.set_signal_handlers()
            self.exit_event = threading.Event()
            self.monitor_thread = threading.Thread(target=self.worker_monitor)
            self.monitor_thread.start()

        # and start the interface register_background_task_thread
        self.task_registrator.start()
        return True

    def stop(self):
        """
        Shut down the monitor-thread which in turn will stop all running
        workers. Also release the monitor_lock flag.
        """
        # no more registrations
        self.task_registrator.stop()
        if self.monitor_thread:
            # check for self.exit_event for a test-scenario.
            # in production if self.monitor_thread is not None
            # self.exit_event is also not None
            if self.exit_event:
                # terminate the monitor thread
                self.exit_event.set()
            self.monitor_thread = None
        # clean up the database
        self.interface.shut_down_process()
        # terminate the workers here in the main process:
        for entry in self.processes:
            entry.process.terminate()

    def _terminate(self, signalnum, stackframe=None):
        """
        Terminate autocron by calling the engine.stop() method.
        Afterward reraise the signal again for the original
        signal-handler.
        """
        # stackframe may be given to the signal-handler, but is unused
        # pylint: disable=unused-argument
        self.stop()
        signal.signal(signalnum, self.orig_signal_handlers[signalnum])
        signal.raise_signal(signalnum)  # requires Python >= 3.8
