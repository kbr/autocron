"""
engine.py

Implementation of the autocron engine and the worker monitor.
"""

import pathlib
import signal
import subprocess
import sys
import threading

from .configuration import configuration
from .sql_interface import interface


WORKER_MODULE_NAME = "worker.py"


def start_subprocess(database_file=None):
    """
    Starts the worker process in a detached subprocess.
    An optional `database_file` will get forwarded to the worker to use
    this instead of the configured one. This argument is for testing.
    """
    worker_file = pathlib.Path(__file__).parent / WORKER_MODULE_NAME
    cmd = [sys.executable, worker_file]
    if database_file:
        cmd.append(database_file)
    cwd = configuration.cwd
    return subprocess.Popen(cmd, cwd=cwd)


def start_worker_monitor(exit_event, database_file=None):
    """
    Monitors the subprocess and start/restart if the process is not up.
    """
    process = None
    while True:
        if process is None or process.poll() is not None:
            process = start_subprocess(database_file)
        if exit_event.wait(timeout=configuration.monitor_idle_time):
            break
    # got exit event: terminate worker
    # running_worker decrement is triggered in the stop() method
    # (which is the termination handler registered in the main thread)
    process.terminate()


class Engine:
    """
    The Engine is the entry-point for autocron. On import an Entry
    instance gets created and the method start is called. Depending on
    the configuration will start the worker-monitor and the background
    process. If the (auto-)configuration is not active, the method start
    will just return doing nothing.
    """
    # pylint: disable=redefined-outer-name
    def __init__(self, interface=interface):
        self.interface = interface  # allow dependency injection for tests
        self.exit_event = threading.Event()
        self.monitor_thread = None
        self.original_handlers = {
            signalnum: signal.signal(signalnum, self._terminate)
            for signalnum in (signal.SIGINT, signal.SIGTERM)
        }

    def is_start_allowed(self):
        """
        Return True if autocron is active and more running worker are
        allowed. Return False if a worker is already running, no more
        worker allowed or autocron is not active.
        """
        if configuration.is_active and not self.monitor_thread:
            return self.interface.try_increment_running_workers()
        return False

    def django_autostart(self, database_file=None):
        """
        Special start-method for django.
        Will start autocron if django.settings.DEBUG is False
        """
        # this will block if django is not ready.
        # therefore django_autostart must get called from the
        # AppConfig.ready() method.
        debug_mode = configuration.get_django_debug_setting()
        if not debug_mode:
            self.start(database_file=database_file)

    def start(self, database_file=None):
        """
        Starts the monitor in case no other monitor is already running
        and the configuration indicates that autocron is active. To
        start the engine, a project-name is required. The project-name
        represents the name of the directory in '~/.autocron' where
        project-specific data are stored (like the database).
        """
        if self.is_start_allowed():
            # start monitor thread
            self.monitor_thread = threading.Thread(
                target=start_worker_monitor,
                args=(self.exit_event, database_file)
            )
            self.monitor_thread.start()

    def stop(self):
        """
        Shut down monitor thread and release semaphore file. `args`
        collect arguments provided because the method is a
        signal-handler. The arguments are the signal number and the
        current stack frame, that could be None or a frame object. To
        shut down, both arguments are ignored.
        """
        if self.monitor_thread:  # and self.monitor_thread.is_alive():
            self.exit_event.set()
            self.monitor_thread = None
            self.interface.decrement_running_workers()

    def _terminate(self, signalnum, stackframe=None):

        """
        Terminate autocron by calling the engine.stop method. Afterward
        reraise the signal again for the original signal-handler.
        """
        # stackframe may be given to the signal-handler, but is unused
        # pylint: disable=unused-argument
        self.stop()
        signal.signal(signalnum, self.original_handlers[signalnum])
        signal.raise_signal(signalnum)  # requires Python >= 3.8


engine = Engine()
