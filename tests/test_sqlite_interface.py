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
    TEMPORARY_PREFIX,
    Connection,
    Settings,
    Result,
    Task
)


TEST_DB_NAME = "test.db"
TEMPORARY_TEST_DB_NAME = TEMPORARY_PREFIX + TEST_DB_NAME


def tst_function(*args, **kwargs):
    return args, kwargs


def tst_cron_function():
    return None


def tst_add_function(a, b):
    return a + b


@pytest.fixture
def raw_interface():
    """
    Returns a new database instance with a temporary db-file.
    """
    # set class attribute to None to not return a singleton
    sqlite_interface.SQLiteInterface._instance = None
    interface = sqlite_interface.SQLiteInterface()
    tmp_db_name = interface.db_name
    yield interface
    for db_name in (interface.db_name, tmp_db_name):
        if db_name is not None:
            pathlib.Path(db_name).unlink(missing_ok=True)


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


def test_init_database(interface):
    """
    Initialize with and without settings.
    """
    # init_database has created the db and set the default settings
    # check for the settings entry in the database
    with Connection(interface.db_name) as conn:
        rows = Settings.count_rows(conn)
        assert rows == 1

    settings = interface.get_settings()
    assert settings.autocron_lock is False
    assert settings.monitor_lock is False


def test_update_settings(interface):
    """
    Test the .update() method on Model.
    """
    max_workers = 2
    with Connection(interface.db_name) as conn:
        settings = Settings.select_all(conn)[0]
        assert settings.max_workers == SETTINGS_DEFAULT_WORKERS

        # check this because otherwise the test makes no sense:
        assert max_workers != SETTINGS_DEFAULT_WORKERS
        settings.max_workers = max_workers
        settings.update()

    with Connection(interface.db_name) as conn:
        settings = Settings.select_all(conn)[0]
        assert settings.max_workers == max_workers


def test_acquire_monitor_lock(interface):
    """Test to set the monitor_lock flag.
    """
    # after initialization the monito_lock flag is False,
    # therefor acquisition is possible:
    settings = interface.get_settings()
    assert settings.monitor_lock is False
    result = interface.acquire_monitor_lock()
    assert result is True

    # monitor_lock has been set to True:
    settings = interface.get_settings()
    assert settings.monitor_lock is True

    # no acquisition is no longer possible:
    result = interface.acquire_monitor_lock()
    assert result is False

    # and the flag has not changed:
    settings = interface.get_settings()
    assert settings.monitor_lock is True


def test_crud_task(interface):
    """
    Make the crud test on the model using a task.
    """
    # C: create with attributes
    schedule = datetime.datetime(2000, 1, 1)
    args = (3.14, "test", 42)
    kwargs = {"pi": 3.14159, "a": 42, 10: "b"}
    with Connection(interface.db_name) as conn:
        task = Task(
            connection=conn, func=tst_function, schedule=schedule,
            args=args, kwargs=kwargs
        )
        task.store()

    # after storage the task should have a rowid attribute
    rowid = task.rowid

    # R: read by rowid and check for correct attribute-reading
    with Connection(interface.db_name) as conn:
        task = Task.select(conn, rowid=rowid)
    assert task.schedule == schedule
    assert task.args == args
    assert task.kwargs == kwargs
    assert task.crontab == ""
    assert task.uuid == ""
    assert task.function_module == tst_function.__module__
    assert task.function_name == tst_function.__name__

    # U: update a selected attribute:
    now = datetime.datetime.now()
    task.schedule = now
    with Connection(interface.db_name) as conn:
        task.connection = conn
        task.update()
    with Connection(interface.db_name) as conn:
        task = Task.select(conn, rowid=rowid)
    assert task.schedule == now

    # D: delete the task:
    with Connection(interface.db_name) as conn:
        task.connection = conn
        task.delete()
    with Connection(interface.db_name) as conn:
        assert Task.count_rows(conn) == 0


def test_delete_task_via_interface(interface):
    """Delete a task via an interface-method.
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
    entry. Also crontasks should not get registered twice.
    """
    interface.register_task(tst_cron_function, crontab="*")
    with Connection(interface.db_name) as conn:
        assert Task.count_rows(conn) == 1
        assert Result.count_rows(conn) == 0

    # try to register again:
    interface.register_task(tst_cron_function, crontab="*")
    with Connection(interface.db_name) as conn:
        assert Task.count_rows(conn) == 1

    # registration of another crontask should work:
    interface.register_task(tst_function, crontab="*")
    with Connection(interface.db_name) as conn:
        assert Task.count_rows(conn) == 2

    # registration of a delayed function registered already as
    # a crontask should also work. (this is a theoretical case.)
    interface.register_task(tst_function)
    with Connection(interface.db_name) as conn:
        assert Task.count_rows(conn) == 3


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
    Test to just return a task on due.
    """
    now = datetime.datetime.now()
    delta = datetime.timedelta(hours=1)

    # add a task that is not on due:
    with Connection(interface.db_name) as conn:
        task = Task(connection=conn, func=tst_function, schedule=now+delta)
        task.store()
    next_task = interface.get_next_task()
    assert next_task is None

    # add a task that is on due:
    with Connection(interface.db_name) as conn:
        task = Task(connection=conn, func=tst_add_function, schedule=now-delta)
        task.store()
    next_task = interface.get_next_task()
    assert next_task is not None
    assert next_task.function_name == tst_add_function.__name__


def test_get_next_task_priority(interface):
    """
    Returns the next task on due and crontasks first.
    """
    delta = datetime.timedelta(hours=1)
    now = datetime.datetime.now()
    interface.register_task(
        tst_cron_function, crontab="*", schedule=now - 2 * delta)
    interface.register_task(
        tst_function, uuid="*", schedule=now - delta)
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


def test_get_task_by_function_name(interface):
    """Store two tasks and get one by name.
    """
    with Connection(interface.db_name) as conn:
        Task(connection=conn, func=tst_function).store()
        Task(connection=conn, func=tst_cron_function).store()
    with Connection(interface.db_name) as conn:
        assert Task.count_rows(conn) == 2
        task = Task.get_by_function_name(tst_cron_function, conn)
        assert task is not None
        assert task.function_module == tst_cron_function.__module__
        assert task.function_name == tst_cron_function.__name__


def test_task_is_ready(interface):
    """
    Test for correct refresh of a Result instance on calling 'is_ready()'.
    """
    with Connection(interface.db_name) as conn:
        uuid_ = uuid.uuid4().hex
        result = Result(conn, func=tst_function, uuid=uuid_)
        result.store()
    assert result.uuid == uuid_
    assert result.is_ready() is False

    # update and result and check for changes:
    answer = 42
    interface.update_result(uuid=uuid_, result=answer)
    assert result.is_ready() is True
    assert result.has_error is False
    assert result.result == answer



def test_delete_outdated_results(interface):
    """
    Test to store and delete results.
    """
    delta = datetime.timedelta(hours=1)
    now = datetime.datetime.now()
    with Connection(interface.db_name) as conn:
        outdated = Result(conn, func=tst_function, uuid=uuid.uuid4().hex)
        outdated.store()
        also_outdated = Result(conn, func=tst_function, uuid=uuid.uuid4().hex)
        also_outdated.store()
        not_outdated = Result(conn, func=tst_function, uuid=uuid.uuid4().hex)
        not_outdated.store()
    assert interface.count_results() == 3

    # nothing will happen because no result is outdated
    interface.delete_outdated_results()
    assert interface.count_results() == 3

    # now update the results as it will happen by the workers:
    interface.update_result(uuid=not_outdated.uuid)
    interface.update_result(uuid=outdated.uuid, ttl=now-delta)
    interface.update_result(uuid=also_outdated.uuid, ttl=now-delta)

    assert interface.count_results() == 3  # as before
    interface.delete_outdated_results()
    assert interface.count_results() == 1  # but now two have been deleted


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
        settings = Settings.read(connection=conn)
    assert settings.worker_pids == str(first_pid)
    assert settings.running_workers == running_workers

    # provide an additional pid and call increment
    new_pid = 42
    interface.increment_running_workers(pid=new_pid)

    # check for updated settings:
    worker_pids = f"{first_pid},{new_pid}"
    running_workers += 1
    with Connection(interface.db_name) as conn:
        settings = Settings.read(connection=conn)
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
        settings = Settings.read(connection=conn)
        settings.running_workers=running_workers
        settings.worker_pids=worker_pids
        settings.update()

    # do the modification: delete pid 456
    pid = 456
    interface.decrement_running_workers(pid=pid)
    with Connection(interface.db_name) as conn:
        settings = Settings.read(connection=conn)
    assert settings.worker_pids == "123,789"
    assert settings.running_workers == 2

    # delete a non-existing pid should have no effect:
    pid = 42
    interface.decrement_running_workers(pid=pid)
    with Connection(interface.db_name) as conn:
        settings = Settings.read(connection=conn)
    assert settings.worker_pids == "123,789"
    assert settings.running_workers == 2

    # remove the remaining two pids:
    interface.decrement_running_workers(pid="123")
    interface.decrement_running_workers(pid="789")
    with Connection(interface.db_name) as conn:
        settings = Settings.read(connection=conn)
    assert settings.worker_pids == ""
    assert settings.running_workers == 0

    # don't go below zero for the running_workers counter
    # (should be covered by ignoring unknown pids):
    interface.decrement_running_workers(pid="23")
    with Connection(interface.db_name) as conn:
        settings = Settings.read(connection=conn)
    assert settings.worker_pids == ""
    assert settings.running_workers == 0


@pytest.mark.parametrize(
    "pids, pid, expected_result", [
        ("123,456,789,1012,1024,300,4578", 456, True),
        ("123,456,789,1012,1024,300,4578", 301, False),
        ("", 987652, False),
    ]
)
def test_is_worker_pid(pids, pid, expected_result, interface):
    with Connection(interface.db_name) as conn:
        settings = Settings.read(connection=conn)
        settings.worker_pids = pids
        settings.update()
    result = interface.is_worker_pid(pid)
    assert result == expected_result


def test_delete_database(interface):
    """
    Check that the interface can delete its own database.
    """
    db_path = pathlib.Path(interface.db_name)
    assert db_path.exists() is True
    interface._delete_database()
    assert db_path.exists() is False


def test_check_temporary_database_property(raw_interface):
    assert raw_interface.has_temporary_database is True
    raw_interface.db_name = TEST_DB_NAME
    assert raw_interface.has_temporary_database is False
    raw_interface.db_name = TEMPORARY_TEST_DB_NAME
    assert raw_interface.has_temporary_database is True


def test_temporary_database(raw_interface):
    """
    Test story: create a temporary database. add a task and set a
    not-temporary name for the database. The temporary database should
    be gone and the task should be transferred to the new database.
    """
    db = raw_interface  # for less typing
    tmp_name = db.db_name
    assert tmp_name.name.startswith(TEMPORARY_PREFIX)
    assert tmp_name.exists() is True

    # register a cron task in the temporary database
    db.register_task(tst_cron_function, crontab="*")
    with Connection(db.db_name) as conn:
        assert Task.count_rows(conn) == 1

    # init database again with a non temporary name
    # the former database should have been deleted
    # and the previous registered cron task should now be
    # in the new database
    db.init_database(db_name=TEST_DB_NAME)
    assert tmp_name.exists() is False
    with Connection(db.db_name) as conn:
        assert Task.count_rows(conn) == 1
        task = Task.get_by_function_name(tst_cron_function, conn)
        assert task is not None
        assert task.function_module == tst_cron_function.__module__
        assert task.function_name == tst_cron_function.__name__
