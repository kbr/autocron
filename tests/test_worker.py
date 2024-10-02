"""
test_worker.py

test worker functionality. Start and stop of worker process is testet in
test_engine.py
"""

import pathlib
import types

import pytest

from autocron import sqlite_interface
from autocron import worker


MONITOR_PID = 0  # dummy pid
TEST_DB_NAME = "test.db"
TEST_ARGS = types.SimpleNamespace(dbfile=TEST_DB_NAME, monitorpid=MONITOR_PID)


def tst_add(a, b):
    return a + b


@pytest.fixture
def interface():
    """
    Returns a new initialised database instance.
    """
    # set class attribute to None to not return a singleton
    sqlite_interface.SQLiteInterface._instance = None
    interface = sqlite_interface.SQLiteInterface()
    tmp_db_name = interface.db_name
    interface.init_database(db_name=TEST_DB_NAME)
    yield interface
    for db_name in (interface.db_name, tmp_db_name):
        if db_name is not None:
            pathlib.Path(db_name).unlink(missing_ok=True)

def test_init_worker(interface):
    """
    Should run without error after refactoring and because the interface
    is a singleton and set during the __init__ method.
    """
    assert interface.accept_registrations is True
    worker_ = worker.Worker(TEST_ARGS)
    assert worker_.interface is interface


def test_handle_delayed_task(interface):
    """
    Handle a simple task and check for the correct processing.
    """
    # register a delayed task (the uuid must just be a unique string)
    uuid_ = "testid"
    interface.register_task(func=tst_add, args=(40, 2), uuid=uuid_)

    # check for table entries:
    assert interface.count_tasks() == 1
    assert interface.count_results() == 1

    # create a worker and handle the task:
    worker_ = worker.Worker(TEST_ARGS)
    has_processed_a_task = worker_.handle_task()
    assert has_processed_a_task is True

    # check for table entries (the task should have been deleted):
    assert interface.count_tasks() == 0
    assert interface.count_results() == 1

    # get the result by the known uuid:
    result = interface.get_result_by_uuid(uuid=uuid_)
    assert result.is_ready() is True
    assert result.function_result == 42
