"""
test_engine.py

tests for the engine and the worker.

The configuration sets a flag `is_active`. If this flag is True the
engine should start a monitor-thread. The monitor-thread then starts the
worker process. On terminating the engine sets a threading event to
terminate the monitor-thread and the monitor thread should shut down the
worker process.
"""

import pathlib
import subprocess
import time
import unittest

from autocron import configuration
from autocron import engine
from autocron import sql_interface


TEST_DB_NAME = configuration.configuration.autocron_path / "test.db"


class TestEngine(unittest.TestCase):

    def setUp(self):
        self.interface = sql_interface.SQLiteInterface(db_name=TEST_DB_NAME)
        self._configuration_is_active = configuration.configuration.is_active
        self.engine = engine.Engine(interface=self.interface)
        self.cc = configuration.configuration

    def tearDown(self):
        # clean up if tests don't run through
        configuration.configuration.is_active = self._configuration_is_active
        pathlib.Path(self.interface.db_name).unlink()

    def test_start_subprocess(self):
        process = engine.start_subprocess()
        assert isinstance(process, subprocess.Popen) is True
        assert process.poll() is None
        process.terminate()
        time.sleep(0.02)  # give process some time to terminate
        assert process.poll() is not None

    def test_is_start_allowed(self):
        # autocron not active -> no start
        self.cc.is_active = False
        self.assertFalse(self.engine.is_start_allowed())
        # autocron active and already a monitor_thread -> no start
        self.engine.monitor_thread = "some reference"
        self.cc.is_active = True
        self.assertFalse(self.engine.is_start_allowed())
        # autocron active and not monitor_thread -> start allowed
        self.engine.monitor_thread = None
        self.assertTrue(self.engine.is_start_allowed())


