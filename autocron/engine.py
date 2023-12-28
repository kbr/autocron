"""
engine.py

Implementation of the autocron engine and the worker monitor.
"""

import pathlib
import signal
import subprocess
import sys
import threading
import time
from types import SimpleNamespace

from .sql_interface import SQLiteInterface


WORKER_MODULE_NAME = "worker.py"
WORKER_START_DELAY = 0.2


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


def run_worker_monitor(exit_event, database_file):
    """
    Starts the worker processes and monitors that the workers are up and
    running. Restart workers if necessary. This function must run in a
    separate thread. The 'exit_event' threading.Event instance. If
    received the workers are terminated and the function will return,
    terminating its own thread as well.
    """
    processes = []
    interface = SQLiteInterface()
    interface.init_database(database_file)
    max_workers = interface.get_max_workers()
    for _ in range(max_workers):
        process = start_subprocess(database_file)
        processes.append(SimpleNamespace(pid=process.pid, process=process))
        # don't start multiple workers too fast one after the other
        # because they may run into a sqlite write-lock. This will not cause
        # a wrong behaviour on starting, monitoring and stopping the workers
        # but can lead to a wrong statistic in the autocron admin/setting.
        time.sleep(WORKER_START_DELAY)
    while True:
        for entry in processes:
            if entry.process.poll() is not None:
                # trouble: process is not running any more
                # deregister the terminated process from the setting
                # and start a new process
                interface.decrement_running_workers(entry.pid)
                new_process = start_subprocess(database_file)
                entry.pid = new_process.pid
                entry.process = new_process
        idle_time = interface.get_monitor_idle_time()
        if exit_event.wait(timeout=idle_time):
            break
    for entry in processes:
        entry.process.terminate()


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
        self.exit_event = None
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
        # this is a safety check for not starting more than
        # one monitor thread:
        if not self.monitor_thread:
            self.exit_event = threading.Event()
            self.monitor_thread = threading.Thread(
                target=run_worker_monitor,
                args=(self.exit_event, database_file)
            )
            self.monitor_thread.start()
        return True

    def stop(self):
        """
        Shut down the monitor thread which in turn will stop all running
        workers. Also release the monitor_lock flag.
        """
        if self.monitor_thread:
            # check for self.exit_event for a test-scenario.
            # in production if self.monitor_thread is not None
            # self.exit_event is also not None
            if self.exit_event:
                self.exit_event.set()
            self.monitor_thread = None
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
