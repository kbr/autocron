"""
test_sqlite_interface.py
"""

import datetime
import pathlib
# import uuid

import pytest

from autocron import sqlite_interface

from autocron.sqlite_interface import (
    SETTINGS_DEFAULT_WORKERS,
    Connection,
    Settings,
    Task
)


TEST_DB_NAME = "test.db"


def tst_function(*args, **kwargs):
    return args, kwargs


@pytest.fixture
def raw_interface():
    """
    Returns a new uninitialised database instance.
    """
    # set class attribute to None to not return a singleton
    sqlite_interface.SQLiteInterface._instance = None
    interface = sqlite_interface.SQLiteInterface()
    yield interface
    if interface.db_name is not None:
        pathlib.Path(interface.db_name).unlink(missing_ok=True)


@pytest.fixture
def interface():
    """
    Returns a new initialised database instance.
    """
    # set class attribute to None to not return a singleton
    sqlite_interface.SQLiteInterface._instance = None
    interface = sqlite_interface.SQLiteInterface()
    interface.init_database(db_name=TEST_DB_NAME)
    yield interface
    if interface.db_name is not None:
        pathlib.Path(interface.db_name).unlink(missing_ok=True)


@pytest.mark.parametrize(
    "db_name, parent_dir", [
        ("db_name.db", ".autocron"),
        ("path/db_name.db", ".autocron"),
        ("/path/db_name.db", "path")
    ]
)
def test_storage_location(db_name, parent_dir, raw_interface):
    """
    Test for storage location: a relative path gets stored to ~.autocron
    and an absolute path as is.
    """
    raw_interface.db_name = db_name
    parent = raw_interface.db_name.parent
    assert parent_dir == parent.stem


def test_init_database(raw_interface):
    """
    Should set the standard settings on a new database.
    Implicit testing .store()
    """
    raw_interface.init_database(TEST_DB_NAME)
    with Connection(raw_interface.db_name) as conn:
        settings = Settings(conn)
        rows = settings.count_rows()
        assert rows == 1

def test_update_settings(raw_interface):
    """
    Test the .update() method on Model. Implicit testing.read()
    """
    max_workers = 2
    raw_interface.init_database(TEST_DB_NAME)
    with Connection(raw_interface.db_name) as conn:
        settings = Settings(conn)
        settings.read()
        assert settings.max_workers == SETTINGS_DEFAULT_WORKERS
        settings.max_workers = max_workers
        settings.update(max_workers=max_workers)
        settings.read()
        assert settings.max_workers == max_workers
        # also the 'worker_master' should be True
        assert raw_interface.is_worker_master is True
        assert settings.monitor_lock is True

def test_store_and_read_task(interface):
    """
    Store a task and read the data with the correct datatypes.
    """
    schedule = datetime.datetime(2000, 1, 1)
    args = (3.14, "test", 42)
    kwargs = {"pi": 3.14159, "a": 42, 10: "b"}
    with Connection(interface.db_name) as conn:
        task = Task(conn)
        task.store(tst_function, schedule=schedule, args=args, kwargs=kwargs)

    # after storage the task should get returned by read_next_task()
    schedule = datetime.datetime.now()
    with Connection(interface.db_name) as conn:
        task = Task(conn)
        success = task.read_next_task(schedule)
        assert success is task
        assert task.args == args
        assert task.kwargs == kwargs



