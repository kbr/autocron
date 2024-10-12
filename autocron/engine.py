"""
The Engine is the entry-point for autocron.

To use the Engine create an instance and call ``.start(filename)`` on
this instance with the database filename to use as argument. The
function ``start(filename)`` in the ``__init__.py`` module of autocron
handles this.
"""

# this module is part of autocron
# copyright (c) 2024 Klaus Bremer
# all rights reserved
#
# license: MIT

import os
import pathlib
import signal
import subprocess
import sys

from .sqlite_interface import SQLiteInterface


MONITOR_MODULE_NAME = "monitor.py"


class Engine:
    """
    The Engine is the entry-point for autocron. Starting the engine will
    start the monitor process. The monitor-process in turn starts and
    supervises the workers. The monitor-process also checks whether the web-application itself is running. In case of an unexpected termination (or even a ``kill 9``), all worker processes will shut down gracefully - no orphaned processes or zombies left.
    (The interface argument is used for testing.)
    """

    def __init__(self, interface=None):
        # the ìnterface argument is for testing. In production this
        # argument is not provided for initialization.
        self.interface = interface if interface else SQLiteInterface()
        self.exit_event = None
        self.monitor_process = None

        # handlers for SIGINT and SIGTERM
        self.orig_signal_handlers = {}
        self.set_signal_handlers()
        # handler for SIGCHLD
        signal.signal(signal.SIGCHLD, self._check_monitor_child)

    def set_signal_handlers(self):
        """
        Set self._terminate() as handler for a couple of
        termination-signals and store the orinal handlers for this
        signals.
        """
        signalnums = [
            signal.SIGINT,
            signal.SIGTERM,
        ]
        for signalnum in signalnums:
            self.orig_signal_handlers[signalnum] = signal.getsignal(signalnum)
            signal.signal(signalnum, self._terminate)

    def reset_signal_handlers(self):
        """
        Reset the original signal handlers.
        """
        for signalnum, signalhandler in self.orig_signal_handlers.items():
            signal.signal(signalnum, signalhandler)

    def start(self, database_file, workers=None):
        """
        Starts the autocron workers in case autocron is active and no
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
        database. If ``workers`` is given, it will override and update
        the corresponding database setting.

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
        if self.interface.acquire_monitor_lock() and not self.monitor_process:
            # adapt number of workers if given
            if workers is not None:
                # override the already loaded value
                self.interface.max_workers = workers
                # and update the settings
                settings = self.interface.get_settings()
                settings.max_workers = workers
                self.interface.update_settings(settings)

            # start the monitor process:
            monitor_file = pathlib.Path(__file__).parent / MONITOR_MODULE_NAME
            cmd = [
                sys.executable,
                monitor_file,
                f"--dbfile={database_file}",
                f"--mainpid={pid}",
            ]
            cwd = pathlib.Path.cwd()
            self.monitor_process = subprocess.Popen(cmd, cwd=cwd)
            result = True

        # start the registrator thread to populate the database:
        # this is a long running task and it is a known issue that django
        # in debug mode with a reloader activated will not work properly,
        # because the reloder makes a process replacement without triggering
        # signal so other threads have no chance to terminate. This can result
        # in erratic behaviour or stalling.
        # As a workaround using django deactivate the reloader in debug mode
        # or set the autocron-lock flag to false in the autocron-database
        # or set blocking-mode to true.
        if not self.interface.blocking_mode:
            self.interface.registrator.start()
        return result

    def stop(self):
        """
        Terminate the workers and tear-down the database. This method is
        called when the application itself terminates. It is not
        necessary to call this method directly.
        """
        if self.monitor_process:
            self.monitor_process.terminate()
            self.monitor_process = None

        # stop registration and clean up the database
        self.interface.registrator.stop()
        self.interface.tear_down_database()

    def _check_monitor_child(self, signalnum, stackframe=None):
        """
        Check the monitor process in case of SIGCHLD to clear the
        according kernel process slot and prevent the monitor to become
        a zombie.
        """
        if self.monitor_process:
            self.monitor_process.poll()

    def _terminate(self, signalnum, stackframe=None):
        """
        Terminate autocron by calling the engine.stop() method.
        """
        # a stackframe may be given to the signal-handler
        # which is not used here.
        # pylint: disable=unused-argument
        self.stop()
        self.reset_signal_handlers()
        # reraise to not hide the signal from the main application:
        # (requires Python >= 3.8)
        signal.raise_signal(signalnum)
