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
        result = False
        if self.interface.autocron_lock:
            return result

        # init the database first to try to aquire the monitor lock
        # for becoming the worker master:
        self.interface.init_database(database_file)

        # start the registrator thread to populate the database:
        self.interface.registrator.start()

        # start the monitor thread if the process is the worker master
        # but dont't start the monitor twice:
        if self.interface.is_worker_master and not self.monitor_thread:
            self.set_signal_handlers()
            self.exit_event = threading.Event()
            self.monitor_thread = threading.Thread(target=self.worker_monitor)
            self.monitor_thread.start()
            result = True
        return result

    def stop(self):
        """
        Shut down the monitor-thread which in turn will stop all running
        workers. Also release the monitor_lock flag.
        """
        if self.monitor_thread:
            # check for self.exit_event for a test-scenario.
            # in production if self.monitor_thread is not None
            # self.exit_event is also not None
            if self.exit_event:
                # terminate the monitor thread
                self.exit_event.set()
            self.monitor_thread = None
        # stop registration and clean up the database
        self.interface.registrator.stop()
        self.interface.tear_down_database()
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
