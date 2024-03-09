"""
test_engine.py

tests for the engine and the worker.
"""

import pathlib
import subprocess
import time
import warnings

import pytest

from autocron import engine
from autocron import sql_interface
from autocron import worker


TEST_DB_NAME = "test.db"


# all labels starting with 'tst_' are test-functions:

def tst_add():
    pass

def tst_cron():
    pass

def tst_cron2():
    pass


@pytest.fixture
def interface():
    """
    Returns a new database instance.
    """
    # set class attribute to None to not return a singleton
    sql_interface.SQLiteInterface._instance = None
    interface = sql_interface.SQLiteInterface()
    yield interface
    if interface.db_name:
        pathlib.Path(interface.db_name).unlink(missing_ok=True)


def test_start_subprocess():
    """
    Start and stop a subprocess.
    """
    # Needs interface.db_name as argument but makes no use of it.
    process = engine.start_subprocess("unused_db_name")
    assert isinstance(process, subprocess.Popen) is True
    assert process.poll() is None
    process.terminate()
    time.sleep(0.02)  # give process some time to terminate
    assert process.poll() is not None


def test_start_is_allowed(interface):
    """
    Scenario: by default autocron- and monitor-lock flags are False,
    allowing to start a new monitor. In this case engine.start() returns
    True. This should set the monitor-lock flag preventing to start the
    engine a second time. In this case engine.start() return False.
    Stopping the engine should release the monitor-lock flag.
    """
    engine_ = engine.Engine(interface=interface)
    # prevent the test to start a thread:
    engine_.monitor_thread = "some reference"
    result = engine_.start(TEST_DB_NAME)
    assert result is True

    # try to start a second time:
    result = engine_.start(TEST_DB_NAME)
    assert result is False

    # check the monito-lock flag is set and released after engine.stop()
    assert engine_.interface.monitor_lock_flag_is_set is True
    engine_.stop()
    assert engine_.interface.monitor_lock_flag_is_set is False


def test_start_and_stop_workerprocess():
    """
    Test to start and terminate a subprocess.
    """
    warnings.simplefilter("ignore", ResourceWarning)
    process = engine.start_subprocess(None)
    assert process.poll() is None  # subprocess runs
    process.terminate()
    time.sleep(0.2)
    assert process.poll() is not None


def test_delete_crontasks_on_shutdown(interface):
    """
    Scenario: put a cron-task in the database. Then close the
    database (to simulate a shutdown of the application). The
    cron-task should be deleted.
    """
    interface.init_database(db_name=TEST_DB_NAME)

    # put two crontasks and a normal task in the database:
    interface.register_task(tst_add)
    interface.register_task(tst_cron, crontab="* * * * *")
    interface.register_task(tst_cron2, crontab="* * * * *")
    entries = interface.count_tasks()
    assert entries == 3

    # simulate an engine in running mode and stop it:
    engine_ = engine.Engine(interface=interface)
    engine_.monitor_thread = True
    engine_.stop()

    # the crontasks should now be deleted:
    entries = interface.count_tasks()
    assert entries == 1

    # and the remaining task should be the add-task:
    task = interface.get_tasks()[0]
    assert bool(task.crontab) is False
