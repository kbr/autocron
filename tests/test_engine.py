"""
test_engine.py

tests for the engine and the worker.
"""

import pathlib
import subprocess
import sys
import threading
import time
import warnings
import unittest

from autocron import engine
from autocron import sql_interface
from autocron import worker


TEST_DB_NAME = "test.db"


# all labels starting with 'tst_' are test-functions:

def tst_cron():
    pass


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
        process = engine.start_subprocess(self.interface.db_name)
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
    """
    Test that the autocron_lock flag from the settings is readable by
    the SQLiteInterface. Also test whether SQLiteInterface indeed
    behaves like a singleton.
    """

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
    """
    Injects the autocron_lock flag set to True in the database: the
    engine should not start.
    """

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


class TestWorkerStartStop(unittest.TestCase):

    def setUp(self):
        # set worker_idle_time to a low value
        self._worker_idle_time = 0.01
        self._monitor_idle_time = 0.01
        sql_interface.SQLiteInterface._instance = None
        self.interface = sql_interface.SQLiteInterface()
        self.interface.init_database(db_name=TEST_DB_NAME)
        settings = self.interface.get_settings()
        settings.worker_idle_time = self._worker_idle_time
        settings.worker_idle_time = self._monitor_idle_time
        self.interface.set_settings(settings)
        self.cmd = [sys.executable, worker.__file__, self.interface.db_name]
        self.cwd = pathlib.Path.cwd()
        warnings.simplefilter("ignore", ResourceWarning)

    def tearDown(self):
        # clean up if tests don't run through
        pathlib.Path(self.interface.db_name).unlink(missing_ok=True)

    def test_start_and_stop_workerprocess(self):
        process = subprocess.Popen(self.cmd, cwd=self.cwd)
        assert process.poll() is None  # subprocess runs
        process.terminate()
        time.sleep(self._worker_idle_time * 2)
        assert process.poll() is not None


    def test_delete_crontasks_on_shutdown(self):
        """
        Scenario: put a cron-task in the database. Then close the
        database (to simulate a shutdown of the application). The
        cron-task should be deleted.
        """
        # put a crontask in the database:
        self.interface.register_callable(tst_cron, crontab="* * * * *")
        tasks = self.interface.get_tasks_by_signature(tst_cron)
        self.assertTrue(tasks)

        # simulate an engine in running mode and stop it:
        engine_ = engine.Engine(interface=self.interface)
        engine_.monitor_thread = True
        engine_.stop()

        # the crontask should now be deleted:
        tasks = self.interface.get_tasks_by_signature(tst_cron)
        self.assertFalse(tasks)
