"""
monitor.py

External monitor process to start and stop the workers.
"""
# this module is part of autocron
# copyright (c) 2024 Klaus Bremer
# all rights reserved
#
# license: MIT

import argparse
import os
import pathlib
import signal
import subprocess
import sys
import time

from autocron import sqlite_interface

NOOP_SIGNAL = 0
WORKER_MODULE_NAME = "worker.py"
WORKER_START_DELAY = 0.02


class Monitor:
    def __init__(self, args):
        self.pid = os.getpid()
        self.sub_processes = []
        self.terminate_monitor = False
        self.main_pid = args.mainpid
        self.database_file = args.dbfile
        self.interface = sqlite_interface.SQLiteInterface()
        self.interface.init_database(self.database_file)
        self.monitor_idle_time = self.interface.monitor_idle_time
        signal.signal(signal.SIGINT, self.terminate)
        signal.signal(signal.SIGTERM, self.terminate)

    def terminate(self, *args):
        self.terminate_monitor = True

    def start_subprocess(self):
        """
        Starts the worker process in a detached subprocess. The
        `database_file` is a string with an absolute or relative path to the
        database in use.
        """
        worker_file = pathlib.Path(__file__).parent / WORKER_MODULE_NAME
        cmd = [
            sys.executable,
            worker_file,
            f"--dbfile={self.database_file}",
            f"--monitorpid={self.pid}",
        ]
        cwd = pathlib.Path.cwd()
        self.sub_processes.append(subprocess.Popen(cmd, cwd=cwd))

    def start_workers(self):
        for _ in range(self.interface.max_workers):
            self.start_subprocess()
            time.sleep(WORKER_START_DELAY)

    def stop_workers(self):
        for process in self.sub_processes:
            process.terminate()

    def monitor_workers(self):
        for process in self.sub_processes:
            if process.poll() is not None:
                self.interface.decrement_running_workers(process.pid)
                self.sub_processes.remove(process)
                self.start_subprocess()
                # in case more workers need a restart:
                time.sleep(WORKER_START_DELAY)

    def run(self):
        self.start_workers()
        while not self.terminate_monitor:
            if self.master_missing:
                break
            self.monitor_workers()
            time.sleep(self.monitor_idle_time)
        # tear down in case the engine was killed:
        self.interface.tear_down_database()
        self.stop_workers()

    @property
    def master_missing(self):
        if self.main_pid is not None:
            try:
                # signal 0 does nothing but tries to access the process
                os.kill(self.main_pid, NOOP_SIGNAL)
            except OSError:
                # master process not found
                return True
        return False


def get_arguments():
    """takes `--dbfile` and `--mainpid` as required arguments"""
    parser = argparse.ArgumentParser(prog="autocron.monitor")
    parser.add_argument("--dbfile")
    parser.add_argument("--mainpid", type=int)
    return parser.parse_args()


def main():
    monitor = Monitor(get_arguments())
    monitor.run()


if __name__ == "__main__":
    main()
