"""
test_engine.py

tests for the engine and the worker.
"""
import pathlib
import subprocess
import sys
import time
import unittest

from autocron import engine
from autocron import worker


class TestWorkerStartStop(unittest.TestCase):

    def setUp(self):
        self.cmd = [sys.executable, worker.__file__]
        self.cwd = pathlib.Path.cwd()

    def test_start_and_stop_workerprocess(self):
        process = subprocess.Popen(self.cmd, cwd=self.cwd)
        assert process.poll() is None  # subprocess runs
        process.terminate()
        time.sleep(0.1)
        assert process.poll() is not None
