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

from autocron import engine
from autocron import sql_interface


TEST_DB_NAME = "test.db"


class TestEngine(unittest.TestCase):

    def setUp(self):
        sql_interface.SQLiteInterface._instance = None
        self.interface = sql_interface.SQLiteInterface()
        self.engine = engine.Engine(interface=self.interface)

    def tearDown(self):
        # clean up if tests don't run through
        if self.interface.db_name:
            pathlib.Path(self.interface.db_name).unlink()

    def test_start_subprocess(self):
        process = engine.start_subprocess()
        assert isinstance(process, subprocess.Popen) is True
        assert process.poll() is None
        process.terminate()
        time.sleep(0.02)  # give process some time to terminate
        assert process.poll() is not None

    def test_start_is_allowed(self):
        # don't start a thread here:
        self.engine.monitor_thread = "some reference"
        result = self.engine.start(TEST_DB_NAME)
        # on default the  autocron- and monitor-lock flags are False:
        assert result is True
        # starting a second time should not work because the
        # monitor-lock flag should be set now:
        result = self.engine.start(TEST_DB_NAME)
        assert result is False
        # check direct for the monitor_lock flag:
        self.assertTrue(self.engine.interface.monitor_lock_flag_is_set)
        # check for releasing the flag on stop
        self.engine.stop()
        self.assertFalse(self.engine.interface.monitor_lock_flag_is_set)


class TestAutocronFlagInjection(unittest.TestCase):

    def test_new_interface_instance(self):
        sql_interface.SQLiteInterface._instance = None
        interface1 = sql_interface.SQLiteInterface()
        interface2 = sql_interface.SQLiteInterface()
        assert interface1 is interface2
        sql_interface.SQLiteInterface._instance = None
        interface3 = sql_interface.SQLiteInterface()
        assert interface1 is not interface3

    def test_injection(self):
        sql_interface.SQLiteInterface._instance = None
        self.interface = sql_interface.SQLiteInterface()
        self.interface.init_database(db_name=TEST_DB_NAME)
        assert self.interface.autocron_lock_is_set is False
        settings = self.interface.get_settings()
        settings.autocron_lock = True
        self.interface.set_settings(settings)
        # get new instance with existing db
        sql_interface.SQLiteInterface._instance = None
        self.interface = sql_interface.SQLiteInterface()
        self.interface.init_database(db_name=TEST_DB_NAME)
        assert self.interface.autocron_lock_is_set is True
        pathlib.Path(self.interface.db_name).unlink()


class TestAutocronFlag(unittest.TestCase):

    def setUp(self):
        # create self.interface twice to inject the autocron-flag:
        sql_interface.SQLiteInterface._instance = None
        self.interface = sql_interface.SQLiteInterface()
        self.interface.init_database(db_name=TEST_DB_NAME)
        settings = self.interface.get_settings()
        settings.autocron_lock = True
        self.interface.set_settings(settings)
        sql_interface.SQLiteInterface._instance = None
        self.interface = sql_interface.SQLiteInterface()
        self.engine = engine.Engine(interface=self.interface)

    def tearDown(self):
        # clean up if tests don't run through
        pathlib.Path(self.interface.db_name).unlink()

    def test_autocron_flag(self):
        self.engine.monitor_thread = "some reference"
        result = self.engine.start(TEST_DB_NAME)
        assert result is False
