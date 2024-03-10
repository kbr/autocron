"""
test_sql_interface.py

testcode for sql-actions and for decorators
"""

import datetime
import pathlib
import uuid

import pytest

from autocron import decorators
from autocron import sql_interface
from autocron import worker

from autocron.sql_interface import TaskResult


TEST_DB_NAME = "test.db"
ANOTHER_FILE_NAME = "another_file_name.db"


# all labels starting with 'tst_' are test-functions:

def tst_callable(*args, **kwargs):
    return args, kwargs

def tst_add(a, b):
    return a + b

def tst_multiply(a, b):
    return a * b

def tst_join_task(a, b, c="c", d="d"):
    return "".join([a, b, c, d])


@pytest.fixture
def raw_interface():
    """
    Returns a new uninitialised database instance.
    """
    # set class attribute to None to not return a singleton
    sql_interface.SQLiteInterface._instance = None
    interface = sql_interface.SQLiteInterface()
    yield interface
    if interface.db_name is not None:
        pathlib.Path(interface.db_name).unlink(missing_ok=True)


@pytest.fixture
def interface():
    """
    Returns a new initialised database instance.
    """
    # set class attribute to None to not return a singleton
    sql_interface.SQLiteInterface._instance = None
    interface = sql_interface.SQLiteInterface()
    interface.init_database(db_name=TEST_DB_NAME)
    yield interface
    if interface.db_name is not None:
        pathlib.Path(interface.db_name).unlink(missing_ok=True)


def test_hybrid_namespace_dict_access():
    """
    HybridNamespace should behave like a dict.
    """
    hns = sql_interface.HybridNamespace()
    hns["one"] = 1
    assert hns["one"] == 1


def test_hybrid_namespace_attribute_access():
    """
    HybridNamespace should behave like an object with attributes.
    """
    hns = sql_interface.HybridNamespace()
    hns.one = 1
    assert hns.one == 1


def test_hybrid_namespace_mixed_access():
    """
    HybridNamespace should behave like a dict and an object with attributes.
    """
    hns = sql_interface.HybridNamespace()
    hns.one = 1
    assert hns["one"] == 1
    hns["two"] = 2
    assert hns.two == 2


def test_hybrid_namespace_init():
    """
    HybridNamespace can get initialised with a dict.
    """
    data = {"one": 1, "two": 2}
    hns = sql_interface.HybridNamespace(data)
    assert hns["one"] == hns.one
    assert hns.two == hns["two"]


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


def test_initialize_database(raw_interface):
    """
    Test for running the initialization process.
    """
    interface = raw_interface
    assert interface.is_initialized is False
    interface.init_database(db_name=TEST_DB_NAME)

    # check whether get_settings() has worked on the new database
    assert interface.autocron_lock_is_set is bool(
        sql_interface.DEFAULT_AUTOCRON_LOCK)
    ttl = datetime.timedelta(seconds=sql_interface.DEFAULT_RESULT_TTL)
    assert interface._result_ttl == ttl


def test_register_task(interface):
    """
    Test to store a task and find this task later on.
    """
    # no task stored in fresh database
    entries = interface.get_tasks()
    assert bool(entries) is False

    # store a task and retrieve the task
    interface.register_task(tst_callable)
    entries = interface.get_tasks()
    assert bool(entries) is True


def test_store_multiple_tasks(interface):
    """
    Store more than one task and find them all later.
    """
    functions = [tst_callable, tst_multiply, tst_multiply]
    for func in functions:
        interface.register_task(func)
    entries = interface.get_tasks()
    assert len(entries) == len(functions)


def test_get_row_num(interface):
    """
    Test to get the number of rows/entries in a table.
    """
    # no entries at first
    num = interface.get_row_num(sql_interface.DB_TABLE_NAME_TASK)
    assert num == 0

    # store three entries and count three entries
    functions = [tst_callable, tst_multiply, tst_multiply]
    for func in functions:
        interface.register_task(func)
    num = interface.get_row_num(sql_interface.DB_TABLE_NAME_TASK)
    assert num == len(functions)


def test_get_next_task(interface):
    """
    Test to get one of the nexts task on due. Store two tasks, one on
    due. Test that get_next_task() return the one on due.
    """
    # no task on due:
    task = interface.get_next_task()
    assert task is None

    # add tasks
    now = datetime.datetime.now()
    delta = datetime.timedelta(seconds=30)
    interface.register_task(tst_add, schedule=now-delta)
    interface.register_task(tst_multiply, schedule=now+delta)

    # add() is on due:
    task = interface.get_next_task()
    assert task is not None
    assert task.function_name == tst_add.__name__

    # task should now be in processing state:
    assert task.status == sql_interface.TASK_STATUS_PROCESSING

    # the status change should also be stored in the database
    # so a next call to get_next_task() should not return the entry again
    task = interface.get_next_task()
    assert task is None


def test_get_next_crontask(interface):
    """
    Test to get the next cron-task on due.
    """
    # at first no task are on due:
    task = interface.get_next_task()
    assert task is None

    # add three tasks (non-empty crontab indicates a cron-task):
    now = datetime.datetime.now()
    delta = datetime.timedelta(seconds=60)
    interface.register_task(tst_add, schedule=now-delta)
    delta = datetime.timedelta(seconds=30)
    interface.register_task(tst_callable, schedule=now-delta, crontab="*")
    interface.register_task(tst_multiply, schedule=now+delta)

    # tst_callable() is on due:
    task = interface.get_next_task()
    assert task is not None
    assert task.function_name == tst_callable.__name__

    # next call should return tst_add() which is not a cron task
    task = interface.get_next_task()
    assert task is not None
    assert task.function_name == tst_add.__name__

    # now all tasks on due have been consumed
    task = interface.get_next_task()
    assert task is None

    # but there are still three entries in the database
    num = interface.get_row_num(sql_interface.DB_TABLE_NAME_TASK)
    assert num == 3


def test_crud_result(interface):
    """
    Result entries are created when tasks with a uui are stored.
    Test to create, read, update and delete a result.
    """
    # no entries at first
    num = interface.get_row_num(sql_interface.DB_TABLE_NAME_RESULT)
    assert num == 0

    # inject a timedelta of 0 for ttl so results will be outdated fast
    interface._result_ttl = datetime.timedelta(seconds=0)

    # add tasks
    uuid = "some_id"
    interface.register_task(tst_add, crontab="*")
    interface.register_task(tst_multiply, uuid=uuid)

    # there should be one entry now in results
    num = interface.get_row_num(sql_interface.DB_TABLE_NAME_RESULT)
    assert num == 1

    # try to get this result
    task_result = interface.get_result_by_uuid(uuid=uuid)
    assert task_result is not None
    assert task_result.function_name == tst_multiply.__name__
    assert task_result.function_result is None
    assert task_result.is_waiting is True

    # update result:
    interface.update_result(uuid, result=42)
    task_result = interface.get_result_by_uuid(uuid=uuid)
    assert task_result.is_ready is True
    assert task_result.result == 42

    # delete results if outdated
    interface.delete_outdated_results()
    num = interface.count_results()
    assert num == 0


def test_task_arguments(interface):
    """
    Test the argument storage (storage is a blob).
    """
    # add a task with arguments
    uuid = "some_id"
    args = [42, 3.141, ("one", "two")]
    kwargs = {"answer": 41, 10: "ten", "data": [1, 2, {"pi": 3.141, "g": 9.81}]}
    interface.register_task(tst_multiply, uuid=uuid, args=args, kwargs=kwargs)

    # retrive the task and check the arguments
    task = interface.get_tasks()[0]
    assert task.args == args
    assert task.kwargs == kwargs


def test_delete_task(interface):
    """
    Delete a given task from the database.
    """
    # add a task
    interface.register_task(tst_multiply)

    # retrive and delete the task
    task = interface.get_tasks()[0]
    assert interface.count_tasks() == 1
    interface.delete_task(task)
    assert interface.count_tasks() == 0


def test_update_task_schedule(interface):
    """
    Update the schedule on a task.
    """
    now = datetime.datetime.now()
    interface.register_task(tst_multiply, schedule=now)
    task = interface.get_tasks()[0]
    assert task.schedule == now

    then = now + datetime.timedelta(seconds=30)
    interface.update_task_schedule(task, then)
    task = interface.get_tasks()[0]
    assert task.schedule == then


def test_get_result_by_uuid(interface):
    """
    Check for a result entry in the database after registering a task.
    """
    uid = uuid.uuid4().hex
    func = tst_callable
    args = (42, 3.141)
    kwargs = {"a": 100, "b": "test"}
    data = {"args": args, "kwargs": kwargs, "uuid": uid}
    interface.register_task(func, **data)
    result = interface.get_result_by_uuid(uuid=uid)
    assert result.function_name == func.__name__
    assert result.function_arguments == (args, kwargs)
    assert result.is_waiting is True
    assert result.is_ready is False


def test_update_result(interface):
    """
    Register a task with a waiting result. Update the result and check
    for the changed result-entry in the database.
    """
    # prepare task
    uid = uuid.uuid4().hex
    func = tst_callable
    data = {"uuid": uid}
    interface.register_task(func, **data)

    # check pending result
    result = interface.get_result_by_uuid(uuid=uid)
    assert result.is_waiting is True

    # check for delayed result storage
    calculated_result = 42
    interface.update_result(uid, result=calculated_result)
    result = interface.get_result_by_uuid(uuid=uid)
    assert result.is_waiting is False
    assert result.is_ready is True
    assert result.has_error is False
    assert result.result == calculated_result


def test_delete_outdated_result(interface):
    """
    Store two results in TASK_STATUS_READY state, one of them outdated.
    After deletion of outdated results just one result should survive.
    """
    # register not outdated result and update result to change
    # status to TASK_STATUS_READY
    result = 42
    uuid_ = uuid.uuid4().hex
    interface.register_task(tst_callable, uuid=uuid_)
    interface.update_result(uuid_, result=result)

    # now register the outdated result
    # setting the interface.result_ttl to a timedelta of zero
    interface._result_ttl = datetime.timedelta()
    _uuid = uuid.uuid4().hex
    interface.register_task(tst_add, uuid=_uuid)
    interface.update_result(_uuid, result=32)

    # now there are two result entries in the database:
    entries = interface.count_results()
    assert entries == 2

    # after deletion of the outdated result just one result
    # should be stored:
    interface.delete_outdated_results()
    entries = interface.count_results()
    assert entries == 1

    # and this should be the one for the tst_callable() function:
    entry = interface.get_result_by_uuid(uuid_)
    assert entry.result == result


def test_delete_crontask(interface):
    """
    Delete crontasks from the database but keep other tasks.
    """
    # register three tasks, two of them are callables
    interface.register_task(tst_callable, uuid=uuid.uuid4().hex)
    interface.register_task(tst_add, crontab="* * * * *")
    interface.register_task(tst_multiply, crontab="* * * * *")
    entries = interface.count_tasks()
    assert entries == 3

    # now delete the cronjobs:
    interface.delete_crontasks()
    entries = interface.count_tasks()
    assert entries == 1

    # the remaining task should be the tst_callable() function:
    task = interface.get_tasks()[0]
    assert task.function_module == tst_callable.__module__
    assert task.function_name == tst_callable.__name__


def test_settings_table(interface):
    """
    Test story:
    Create a new db with default settings values.
    Change one value (i.e. the max_workers).
    """

    # check for default value
    settings = interface.get_settings()
    assert settings.max_workers == sql_interface.DEFAULT_MAX_WORKERS

    # change default value
    new_max_workers = sql_interface.DEFAULT_MAX_WORKERS + 1
    settings.max_workers = new_max_workers
    interface.set_settings(settings)
    settings = interface.get_settings()
    assert settings.max_workers == new_max_workers

    # another db-init has no effect
    interface.init_database(db_name=TEST_DB_NAME)
    assert settings.max_workers == new_max_workers

    # create new interface (data survive a restart)
    interface = sql_interface.SQLiteInterface()
    interface.init_database(db_name=TEST_DB_NAME)
    assert settings.max_workers == new_max_workers


def test_worker_pids(interface):
    """
    Test for increment and decrement of the worker pids in the settings.
    """

    def check_settings():
        """
        Helper function compares the pids in the settings
        with a known list of pids.
        """
        settings = interface.get_settings()
        assert settings.running_workers == len(test_pids)
        text = ",".join(map(str, test_pids))
        assert settings.worker_pids == text

    # register the pids with increment and check for success
    test_pids = [42, 377, 42980]
    for pid in test_pids:
        interface.increment_running_workers(pid)
    check_settings()

    # remove one pid and check again
    pid = test_pids.pop(1)
    interface.decrement_running_workers(pid)
    check_settings()

    # remove all remaining pids
    while test_pids:
        interface.decrement_running_workers(test_pids.pop())
    check_settings()


@pytest.mark.parametrize(
    "func, args, kwargs, expected_result", [
        (tst_join_task, ("a", "b"), {}, "abcd"),
        (tst_join_task, ("a", "b"), {"c":"c", "d":"d"}, "abcd"),
        (tst_join_task, ("a", "b"), {"c":"e", "d":"f"}, "abef"),
        (tst_add, (30, 12), {}, 42),
        (tst_multiply, (5, 7), {}, 35),
    ]
)
def test_task_result(func, args, kwargs, expected_result):
    """
    Calls the test function 'func' with the argument given by 'args' and
    'kwargs'.
    """
    tr = TaskResult.from_function_call(
        func, *args, **kwargs
    )
    assert tr.result == expected_result


def test_get_results(interface):
    """
    Check to get a list of all results in TASK_STATUS_READY as a list of
    TaskResult instances.
    """
    # register two tasks with results in TASK_STATUS_READY state
    result_functions = (tst_callable, tst_add)
    for func in result_functions:
        _uuid = uuid.uuid4().hex
        interface.register_task(func, uuid=_uuid)
        interface.update_result(_uuid)
    # and one task with result status TASK_STATUS_WAITING
    interface.register_task(tst_multiply, uuid=uuid.uuid4().hex)

    # get the 'ready' results which should be the ones for
    # tst_callable and tst_add
    results = interface.get_results()
    assert len(results) == len(result_functions)
    result_function_names = [result.function_name for result in results]
    for func in result_functions:
        assert func.__name__ in result_function_names
