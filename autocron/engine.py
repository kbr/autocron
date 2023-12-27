"""
engine.py

Implementation of the autocron engine and the worker monitor.
"""

import pathlib
import signal
import subprocess
import sys
import threading

from .sql_interface import SQLiteInterface


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
    cwd = pathlib.Path.cwd()
    return subprocess.Popen(cmd, cwd=cwd)


def start_worker_monitor(exit_event, database_file=None):
    """
    Monitors the subprocess and start/restart if the process is not up.
    """
    process = None
    interface = SQLiteInterface()
    interface.init_database(database_file)
    while True:
        if process is None or process.poll() is not None:
            process = start_subprocess(database_file)
        idle_time = interface.get_monitor_idle_time()
        if exit_event.wait(timeout=idle_time):
            break
    # got exit event: terminate worker
    # running_worker decrement is triggered in the stop() method
    # (which is the termination handler registered in the main thread)
    process.terminate()


class Engine:
    """
    The Engine is the entry-point for autocron. On import an Entry
    instance gets created and the method start is called.
    TODO: this is no longer true. The engine must get started explicitly.

    Depending on
    the configuration will start the worker-monitor and the background
    process. If the (auto-)configuration is not active, the method start
    will just return doing nothing.
    """
    # pylint: disable=redefined-outer-name
    def __init__(self, interface=None):
        self.interface = interface if interface else SQLiteInterface()
        self.exit_event = threading.Event()
        self.monitor_thread = None
        self.original_handlers = {
            signalnum: signal.signal(signalnum, self._terminate)
            for signalnum in (signal.SIGINT, signal.SIGTERM)
        }

    def start(self, database_file):
        """
        Starts the monitor in case no other monitor is already running
        and the configuration indicates that autocron is active. To
        start the engine, a project-name is required. The project-name
        represents the name of the directory in '~/.autocron' where
        project-specific data are stored (like the database).
        Returns a boolean: True if a monitor-thread has been started and
        False otherwise. Returning False does not mean that no monitor
        is running. There could be a monitor-thread in another process
        that has started earlier.
        """
        self.interface.init_database(database_file)
        if (
            self.interface.autocron_lock_is_set
            or self.interface.monitor_lock_flag_is_set
        ):
            # in both cases start is not allowed
            return False
        self.interface.set_monitor_lock_flag(True)
        if not self.monitor_thread:
            # this is a safety check for not starting more than
            # one monitor thread (however, this condition should not happen)
            self.monitor_thread = threading.Thread(
                target=start_worker_monitor,
                args=(self.exit_event, database_file)
            )
            self.monitor_thread.start()
        return True

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
            # TODO: adapt this for multiple workers!
            # (should better be done in the monitor thread)
            # self.interface.decrement_running_workers()
            self.interface.set_monitor_lock_flag(False)

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
