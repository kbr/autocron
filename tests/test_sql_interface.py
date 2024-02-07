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
    if interface.db_name:
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
    if interface.db_name:
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
    raw_interface._set_storage_location(db_name)
    parent = raw_interface.db_name.parent
    assert parent_dir == parent.stem


def test_store_task(interface):
    """
    Test to store a task and find this task later on.
    """
    # no task stored in fresh database
    entries = interface.get_tasks()
    assert bool(entries) is False

    # store a task and retrieve the task
    interface.register_callable(tst_callable)
    entries = interface.get_tasks()
    assert bool(entries) is True


def test_store_multiple_tasks(interface):
    """
    Store more than one task and find them all later.
    """
    functions = [tst_callable, tst_multiply, tst_multiply]
    for func in functions:
        interface.register_callable(func)
    entries = interface.get_tasks()
    assert len(entries) == len(functions)


def test_task_signature(interface):
    """
    Tasks are HybridNamespaces storing callables by their name and
    corresponding module.
    """
    interface.register_callable(tst_callable)
    task = interface.get_tasks()[0]  # just a single entry

    # the Task is a HybridNamespace
    assert isinstance(task, sql_interface.HybridNamespace) is True

    # and known the name and the module of the callable
    assert task["function_module"] == tst_callable.__module__
    assert task["function_name"] == tst_callable.__name__


def test_task_arguments(interface):
    """
    Arguments are stored as blobs. Test to store and retrieve the
    arguments with the original types and values.
    """
    args = [42, 3.141, ("one", "two")]
    kwargs = {"answer": 41, 10: "ten", "data": [1, 2, {"pi": 3.141, "g": 9.81}]}
    interface.register_callable(tst_callable, args=args, kwargs=kwargs)
    task = interface.get_tasks()[0]  # just a single entry

    # check the unpickled blobs:
    assert task.args == args
    assert task.kwargs == kwargs


def test_get_task_on_due(interface):
    """
    Store two tasks, one is on due. Retrieve the task on due.
    """
    now = datetime.datetime.now()
    delta = datetime.timedelta(seconds=30)
    interface.register_callable(tst_add, schedule=now-delta)
    interface.register_callable(tst_multiply, schedule=now+delta)

    # add() is on due:
    entries = interface.get_tasks_on_due()
    assert len(entries) == 1
    task = entries[0]
    assert task.function_name == tst_add.__name__


def test_delete_task(interface):
    """
    The delete_callable() method takes a task as argument that must
    provide a row-id. Store two tasks, one on due. Select the one on due
    and delete this task. The other task should be still there. This is
    what happens after execution of a delayed task.
    """

    now = datetime.datetime.now()
    delta = datetime.timedelta(seconds=30)
    interface.register_callable(tst_add, schedule=now-delta)
    interface.register_callable(tst_multiply, schedule=now+delta)

    # add() is on due: select and delete the task
    task = interface.get_tasks_on_due()[0]  # single entry, tested elsewere
    interface.delete_callable(task)

    # multiply must have survived:
    tasks = interface.get_tasks()
    assert len(tasks) == 1
    task = tasks[0]
    assert task.function_name == tst_multiply.__name__


def test_get_tasks_by_signature(interface):
    """
    Callables can get stored multiple times for delayed execution and
    can get also selected by their names and modules (the signature).
    """
    functions = [tst_multiply, tst_add, tst_multiply]
    for function in functions:
        interface.register_callable(function)

    # select all tst_multiply tasks:
    tasks = interface.get_tasks_by_signature(tst_multiply)
    assert len(tasks) == functions.count(tst_multiply)
    for task in tasks:
        assert task.function_name == tst_multiply.__name__


def test_get_task_on_due_and_set_status(interface):
    """
    Calling interface.get_tasks_on_due() accepts aguments for filtering
    by status and setting a new status.
    """
    # register two callables on due, default state is WAITING
    now = datetime.datetime.now()
    delta = datetime.timedelta(seconds=30)
    functions = [tst_add, tst_multiply]
    for function in functions:
        interface.register_callable(function, schedule=now-delta)

    # access this WAITING task on due and change state to PROCESSING:
    tasks = interface.get_tasks_on_due(
                status=sql_interface.TASK_STATUS_WAITING,
                new_status=sql_interface.TASK_STATUS_PROCESSING
            )
    assert len(tasks) == len(functions)

    # try to get these functions again does not work
    # because of the status change
    tasks = interface.get_tasks_on_due(
                status=sql_interface.TASK_STATUS_WAITING
            )
    assert bool(tasks) is False

    # but the tasks are still stored:
    tasks = interface.get_tasks()
    assert len(tasks) == len(functions)


def test_update_task_schedule(interface):
    """
    A task can get a new schedule. This is usefull for crontasks. Doing
    a schedule update also set the task-state to WAITING.
    """
    now = datetime.datetime.now()
    delta = datetime.timedelta(seconds=30)
    interface.register_callable(tst_callable, schedule=now-delta)

    # fetch the task on due and update the schedule to be in the future
    tasks = interface.get_tasks_on_due(
                status=sql_interface.TASK_STATUS_WAITING,
                new_status=sql_interface.TASK_STATUS_PROCESSING
            )
    task = tasks[0]
    new_schedule = now + delta
    interface.update_task_schedule(task, new_schedule)

    # no tasks on due any more:
    tasks = interface.get_tasks_on_due()
    assert bool(tasks) is False

    # but the task is waiting:
    task = interface.get_tasks()[0]
    assert task.schedule == new_schedule
    assert task.status == sql_interface.TASK_STATUS_WAITING


def test_get_result_by_invalid_uuid(interface):
    """
    Accessing a result entry by an invalid uuid should return None.
    """
    # no result in the database, so this uuid is invalide
    uuid_ = uuid.uuid4().hex
    result = interface.get_result_by_uuid(uuid_)
    assert result is None


def test_get_waiting_result(interface):
    """
    Once registered a result entry is available but in waiting state
    because the result-value is not available. The result is of type
    TaskResult.
    """
    uuid_ = uuid.uuid4().hex
    interface.register_result(tst_add, uuid=uuid_)

    # fetch the result in waiting state:
    result = interface.get_result_by_uuid(uuid_)
    assert result is not None
    assert isinstance(result, TaskResult) is True
    assert result.is_waiting is True
    assert result.function_result is None


def test_ready_result(interface):
    """
    A registered result in waiting state can updated with a result. The
    state then changes to ready.
    """
    uuid_ = uuid.uuid4().hex
    interface.register_result(tst_add, uuid=uuid_)

    # now provide a result:
    answer = 42
    interface.update_result(uuid_, result=answer)

    # fetch the result that is in ready state now
    result = interface.get_result_by_uuid(uuid_)
    assert result is not None
    assert isinstance(result, TaskResult) is True
    assert result.is_waiting is False
    assert result.is_ready is True
    assert result.function_result == answer


def test_result_state(interface):
    """
    A TaskResult can update itself. (doing the query internal.)
    """

    uuid_ = uuid.uuid4().hex
    interface.register_result(tst_add, uuid=uuid_)

    # fetch result which is in waiting state
    result = interface.get_result_by_uuid(uuid_)
    result.interface = interface
    assert result.is_ready is False

    # now provide a result:
    answer = 42
    interface.update_result(uuid_, result=answer)

    # and check the TaskResult object again:
    assert result.is_ready is True
    assert result.function_result == answer


def test_update_result_with_error(interface):
    """
    A TaskResult gets updated with an error message.
    The result should report the error state.
    """
    message = "ValueError: the error text"
    uuid_ = uuid.uuid4().hex
    interface.register_result(tst_add, uuid=uuid_)
    interface.update_result(uuid_, error_message=message)

    # check result for error status
    result = interface.get_result_by_uuid(uuid_)
    assert result.has_error is True


def test_delete_outdated_result(interface):
    """
    Store two results in TASK_STATUS_READY state, one of them outdated.
    After deletion of outdated results just one result should survive.
    """
    status=sql_interface.TASK_STATUS_READY
    uuid_ = uuid.uuid4().hex
    interface.register_result(tst_callable, uuid_, status=status)

    # now register the outdated result
    # setting the interface.result_ttl to a timedelta of zero
    interface._result_ttl = datetime.timedelta()
    interface.register_result(tst_add, uuid.uuid4().hex, status=status)

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
    assert entry.function_module == tst_callable.__module__
    assert entry.function_name == tst_callable.__name__


def test_delete_cronjobs(interface):
    """
    Register three tasks, two of them cronjobs.
    Delete the cronjobs and just one task should survive.
    """
    interface.register_callable(tst_callable)
    interface.register_callable(tst_add, crontab="* * * * *")
    interface.register_callable(tst_multiply, crontab="* * * * *")
    entries = interface.count_tasks()
    assert entries == 3

    # now delete the cronjobs:
    interface.delete_cronjobs()
    entries = interface.count_tasks()
    assert entries == 1

    # the remaining task should be the tst_callable() function:
    entry = interface.get_tasks_on_due()[0]
    assert entry.function_module == tst_callable.__module__
    assert entry.function_name == tst_callable.__name__


def test_settings_table(raw_interface):
    """
    Test story:
    Create a new db and initialize settings with default values.
    Change one value (i.e. the max_workers).
    Initialize the settings again: should have no effect.
    """
    interface = raw_interface  # more convenient name
    interface._set_storage_location(TEST_DB_NAME)
    interface._create_tables()
    interface._initialize_settings_table()

    # check for default value
    settings = interface.get_settings()
    assert settings.max_workers == sql_interface.DEFAULT_MAX_WORKERS

    # change default value
    new_max_workers = sql_interface.DEFAULT_MAX_WORKERS + 1
    settings.max_workers = new_max_workers
    interface.set_settings(settings)
    settings = interface.get_settings()
    assert settings.max_workers == new_max_workers

    # another db-init has no effect (data survive a restart)
    interface._create_tables()
    interface._initialize_settings_table()
    settings = interface.get_settings()
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
