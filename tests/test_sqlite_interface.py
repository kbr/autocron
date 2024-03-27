"""
test_sqlite_interface.py
"""

import datetime
import pathlib
import uuid

import pytest

from autocron import sqlite_interface

from autocron.sqlite_interface import (
    SETTINGS_DEFAULT_WORKERS,
    TASK_STATUS_WAITING,
    TASK_STATUS_PROCESSING,
    Connection,
    Settings,
    Result,
    Task
)


TEST_DB_NAME = "test.db"


def tst_function(*args, **kwargs):
    return args, kwargs

def tst_cron_function():
    return None

def tst_add_function(a, b):
    return a + b


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
        rows = Settings.count_rows(conn)
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
        task = Task(
            connection=conn, func=tst_function, schedule=schedule,
            args=args, kwargs=kwargs
        )
        task.store()

    # after storage the task should get returned by read_next_task()
    schedule = datetime.datetime.now()
    with Connection(interface.db_name) as conn:
        task = Task(conn)
        success = task.read_next_task(schedule)
        assert success is task
        assert task.args == args
        assert task.kwargs == kwargs


def test_delete_task(interface):
    """
    Delete a task.
    """
    with Connection(interface.db_name) as conn:
        task = Task(conn, func=tst_function)
        task.store()
        assert Task.count_rows(conn) == 1
        task.delete()
        assert Task.count_rows(conn) == 0


def test_delete_task_via_interface(interface):
    """
    Delete a task via an interface-method. The task given as argument
    may no longer have a valid connection attribute.
    """
    with Connection(interface.db_name) as conn:
        task = Task(conn, func=tst_function)
        task.store()
        assert Task.count_rows(conn) == 1

    # invalidate the connection (which already closed from the context):
    task.connection = None

    # delete the task and check whether it has worked:
    interface.delete_task(task)
    with Connection(interface.db_name) as conn:
        assert Task.count_rows(conn) == 0


def test_register_cron_task(interface):
    """
    Register a cron task should create a task entry but no result
    entry.
    """
    interface.register_task(tst_function, crontab="*")
    with Connection(interface.db_name) as conn:
        assert Task.count_rows(conn) == 1
        assert Result.count_rows(conn) == 0


def test_register_delayed_task(interface):
    """
    Register a delayed task should create a task entry and also a result
    entry.
    """
    interface.register_task(tst_cron_function, uuid="*")
    with Connection(interface.db_name) as conn:
        assert Task.count_rows(conn) == 1
        assert Result.count_rows(conn) == 1


def test_get_next_task(interface):
    """
    Returns the next task on due and crontasks first.
    """
    delta = datetime.timedelta(hours=1)
    now = datetime.datetime.now()
    interface.register_task(
        tst_cron_function, crontab="*", schedule=now - 2 * delta)
    interface.register_task(
        tst_function, crontab="*", schedule=now - delta)
    interface.register_task(
        tst_add_function, crontab="*", schedule=now + delta)

    # get_next_task should return at first the tst_cron_function
    # and then the tst_function.
    # The tst_add_function should not get returned:

    task = interface.get_next_task()
    assert task.function_name == tst_cron_function.__name__

    task = interface.get_next_task()
    assert task.function_name == tst_function.__name__

    task = interface.get_next_task()
    assert task is None


def test_update_task_schedule(interface):
    """
    Test to update the schedule of a given task.
    """
    now = datetime.datetime.now()
    then = now + datetime.timedelta(hours=1)
    with Connection(interface.db_name) as conn:
        task = Task(conn, func=tst_function, schedule=now)
        task.store()

    # there is a single entry in the database.
    # check for the old schedule
    task = interface.get_tasks()[0]
    assert task.schedule == now

    # update and check for the new schedule:
    interface.update_task_schedule(task, then)
    task = interface.get_tasks()[0]
    assert task.schedule == then


def test_delete_outdated_results(interface):
    """
    Test to store and delete results.
    """
    delta = datetime.timedelta(hours=1)
    now = datetime.datetime.now()
    with Connection(interface.db_name) as conn:
        outdated = Result(conn, func=tst_function, uuid=uuid.uuid4().hex)
        outdated.store()
        old = Result(conn, func=tst_function, uuid=uuid.uuid4().hex)
        old.store()
        survivor = Result(conn, func=tst_function, uuid=uuid.uuid4().hex)
        survivor.store()
    assert interface.count_results() == 3

    # nothing will happen because no result is outdated
    interface.delete_outdated_results()
    assert interface.count_results() == 3

    # now update the results as it will happen by the workers:
    interface.update_result(uuid=outdated.uuid, ttl=now-delta)
    interface.update_result(uuid=old.uuid, ttl=now-delta)
    interface.update_result(uuid=survivor.uuid)

    interface.delete_outdated_results()
    assert interface.count_results() == 1


def test_increment_running_workers(interface):
    """
    Check to increment the list of running workers.
    """
    # add the first worker:
    first_pid = 123
    running_workers = 1
    interface.increment_running_workers(pid=first_pid)

    # check for correct setup
    with Connection(interface.db_name) as conn:
        settings = Settings(connection=conn)
        settings.read()
    assert settings.worker_pids == str(first_pid)
    assert settings.running_workers == running_workers

    # provide an additional pid and call increment
    new_pid = 42
    interface.increment_running_workers(pid=new_pid)

    # check for updated settings:
    worker_pids = f"{first_pid},{new_pid}"
    running_workers += 1
    with Connection(interface.db_name) as conn:
        settings = Settings(connection=conn)
        settings.read()
    assert settings.worker_pids == worker_pids
    assert settings.running_workers == running_workers


def test_decrement_running_workers(interface):
    """
    Check to decrement the list of running workers.
    """
    # setting up three running workers:
    worker_pids = "123,456,789"
    running_workers = 3
    with Connection(interface.db_name) as conn:
        settings = Settings(connection=conn)
        settings.read()
        settings.update(
            running_workers=running_workers, worker_pids=worker_pids
        )

    # do the modification: delete pid 456
    pid = 456
    interface.decrement_running_workers(pid=pid)
    with Connection(interface.db_name) as conn:
        settings = Settings(connection=conn)
        settings.read()
    assert settings.worker_pids == "123,789"
    assert settings.running_workers == 2

    # delete a non-existing pid should have no effect:
    pid = 42
    interface.decrement_running_workers(pid=pid)
    with Connection(interface.db_name) as conn:
        settings = Settings(connection=conn)
        settings.read()
    assert settings.worker_pids == "123,789"
    assert settings.running_workers == 2

    # remove the remaining two pids:
    interface.decrement_running_workers(pid="123")
    interface.decrement_running_workers(pid="789")
    with Connection(interface.db_name) as conn:
        settings = Settings(connection=conn)
        settings.read()
    assert settings.worker_pids == ""
    assert settings.running_workers == 0

    # don't go below zero for the running_workers counter
    # (should be covered by ignoring unknown pids):
    interface.decrement_running_workers(pid="23")
    with Connection(interface.db_name) as conn:
        settings = Settings(connection=conn)
        settings.read()
    assert settings.worker_pids == ""
    assert settings.running_workers == 0
