"""
The Engine is the entry-point for autocron.

To use the Engine create an instance and call ``.start(filename)`` on
this instance with the database filename to use as argument. The
function ``start(filename)`` in the ``__init__.py`` module of autocron
handles this.
"""

import os
import pathlib
import signal
import subprocess
import sys
import threading
import time

from .sqlite_interface import SQLiteInterface


WORKER_MODULE_NAME = "worker.py"
WORKER_START_DELAY = 0.02


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
        Starts the worker processes and monitors that the workers are up
        and running. Restart workers if necessary. This function must
        run in a separate thread. The 'exit_event' is a
        threading.Event() instance and the `database_file` is a string
        with an absolute or relative path to the database in use. If the
        monitor receives an exit_event the function will return,
        terminating its own thread.
        """
        database_file = self.interface.db_name
        timeout = self.interface.monitor_idle_time

        for _ in range(self.interface.max_workers):
            self.processes.append(start_subprocess(database_file))
            time.sleep(WORKER_START_DELAY)

        while True:
            for process in self.processes:
                if process.poll() is not None:
                    self.interface.decrement_running_workers(process.pid)
                    self.processes.remove(process)
                    self.processes.append(start_subprocess(database_file))
                    # in case more workers need a restart:
                    time.sleep(WORKER_START_DELAY)
            if self.exit_event.wait(timeout=timeout):
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

    def start(self, database_file, workers=None):
        """
        Starts the autocron worker in case autocron is active and no
        other application process has already started the workers. The
        ``database_file`` argument is a string with the name of the
        database to use (like "the_application.db") or a Path instance.
        If the name represents a relative path, the database is stored
        in the ``~/.autocron`` directory. This directory will get
        created if not already existing. If the name represents an
        absolute path, then this path will be used as is. In this case
        all directories of the path must exist.

        The ``workers`` argument takes the number of workers (as
        integer) and stores this value in the database. If the value is
        ``None`` (default) the number of workers is taken from the
        database.

        The function returns a boolean: ``True`` if workers have been
        started and ``False`` otherwise. A return value of ``False``
        does not mean that no workers are running – another application
        process may have been first on aquiring the rights to start and
        monitor the worker processes.
        """
        result = False
        self.interface.init_database(database_file)

        # don't start the engine if autocron is not active:
        if self.interface.autocron_lock:
            return result

        # autocron is active:
        # check whether start() has been called from a worker process
        # (this can happen depending on the framework architecture)
        pid = os.getpid()
        if self.interface.is_worker_pid(pid):
            # in this case the engine should not start
            return result

        # check whether the process monitors the workers,
        # but dont't start the monitor twice:
        if self.interface.acquire_monitor_lock() and not self.monitor_thread:

            # adapt number of workers if given
            if workers is not None:
                # override the already loaded value
                self.interface.max_workers = workers
                # and update the settings
                settings = self.interface.get_settings()
                settings.max_workers = workers
                self.interface.update_settings(settings)

            self.set_signal_handlers()
            self.exit_event = threading.Event()
            self.monitor_thread = threading.Thread(target=self.worker_monitor)
            self.monitor_thread.start()
            result = True

        # start the registrator thread to populate the database:
        self.interface.registrator.start()
        return result

    def stop(self):
        """
        Terminate the workers and tear-down the database. This method is
        called when the application itself terminates. It is not
        necessary to call this method directly.
        """
        if self.monitor_thread:
            # check for self.exit_event for a test-scenario.
            # in production if self.monitor_thread is not None
            # self.exit_event is also not None
            if self.exit_event:
                # terminate the monitor thread
                self.exit_event.set()
            self.monitor_thread = None

        # terminate the workers (if any) here in the main process:
        # don't do this in the monitor thread, because the monitor thread
        # may be unable to send the terminate signal to all workers before
        # the main process terminates.
        for process in self.processes:
            process.terminate()

        # stop registration and clean up the database
        self.interface.registrator.stop()
        self.interface.tear_down_database()

    def _terminate(self, signalnum, stackframe=None):
        """
        Terminate autocron by calling the engine.stop() method.
        Afterward reraise the signal again for the original
        signal-handler.
        """
        # a stackframe may be given to the signal-handler
        # which is not used here.
        # pylint: disable=unused-argument
        self.stop()
        signal.signal(signalnum, self.orig_signal_handlers[signalnum])
        signal.raise_signal(signalnum)  # requires Python >= 3.8
