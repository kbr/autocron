"""
test_engine.py

tests for the engine.
"""

import pathlib
import subprocess
import time
import warnings

import pytest

from autocron import engine
from autocron import sqlite_interface


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
    sqlite_interface.SQLiteInterface._instance = None
    interface = sqlite_interface.SQLiteInterface()
    tmp_db_name = interface.db_name
    yield interface
    for db_name in (interface.db_name, tmp_db_name):
        if db_name is not None:
            pathlib.Path(interface.db_name).unlink(missing_ok=True)


def xtest_start_subprocess():
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


def xtest_start_and_stop_workerprocess():
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

