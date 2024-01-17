"""
test_sql_interface.py

testcode for sql-actions and for decorators
"""

import collections
import datetime
import pathlib
import sqlite3
import time
import unittest
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

def tst_cron():
    pass

def tst_delay():
    return 42


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



class TestSQLInterface(unittest.TestCase):

    def setUp(self):
        sql_interface.SQLiteInterface._instance = None
        self.interface = sql_interface.SQLiteInterface()
        self.interface.init_database(db_name=TEST_DB_NAME)
        self._result_ttl = self.interface._result_ttl

    def tearDown(self):
        pathlib.Path(self.interface.db_name).unlink(missing_ok=True)
        self.interface._result_ttl = self._result_ttl

    def _test_storage_location(self):
        path = pathlib.Path.home() / sql_interface.DEFAULT_STORAGE / TEST_DB_NAME
        assert self.interface.db_name == path
        # don't allow setting the db a second time:
        self.interface.init_database(db_name=ANOTHER_FILE_NAME)
        assert self.interface.db_name == path

    def _test_storage_location_absolute(self):
        pathlib.Path(self.interface.db_name).unlink()
        self.interface.db_name = None
        path = pathlib.Path.cwd() / ANOTHER_FILE_NAME
        self.interface.init_database(db_name=path)
        assert self.interface.db_name == path

    def _test_storage(self):
        entries = self.interface.get_tasks_on_due()
        self.assertFalse(list(entries))
        self.interface.register_callable(tst_callable)
        entries = self.interface.get_tasks_on_due()
        assert len(entries) == 1

    def _test_entry_signature(self):
        self.interface.register_callable(tst_callable)
        entries = self.interface.get_tasks_on_due()
        obj = entries[0]
        assert isinstance(obj, sql_interface.HybridNamespace) is True
        assert obj["function_module"] == tst_callable.__module__
        assert obj["function_name"] == tst_callable.__name__

    def _test_arguments(self):
        args = ["pi", 3.141]
        kwargs = {"answer": 41, 10: "ten"}
        crontab = "* 1 * * *"
        self.interface.register_callable(
            tst_callable, crontab=crontab, args=args, kwargs=kwargs
        )
        entries = list(self.interface.get_tasks_on_due())
        obj = entries[0]
        assert obj["crontab"] == crontab
        assert obj["args"] == args
        assert obj["kwargs"] == kwargs

    def _test_get_tasks(self):
        # test the generic function to select all tasks:
        schedule = datetime.datetime.now() + datetime.timedelta(seconds=10)
        self.interface.register_callable(tst_add, schedule=schedule)
        self.interface.register_callable(tst_callable)
        self.interface.register_callable(tst_multiply, crontab="* * * * *")
        # should return everything:
        entries = self.interface.get_tasks()
        assert len(entries) == 3

    def _test_schedules_get_one_of_two(self):
        # register two callables, one with a schedule in the future
        schedule = datetime.datetime.now() + datetime.timedelta(seconds=10)
        self.interface.register_callable(tst_add, schedule=schedule)
        self.interface.register_callable(tst_callable)
        # test to get one callable at due
        entries = self.interface.get_tasks_on_due()
        assert len(entries) == 1

    def _test_schedules_get_two_of_two(self):
        # register two callables, both scheduled in the present or past
        schedule = datetime.datetime.now() - datetime.timedelta(seconds=10)
        self.interface.register_callable(tst_add, schedule=schedule)
        self.interface.register_callable(tst_callable)
        # test to get one callable at due
        entries = self.interface.get_tasks_on_due()
        assert len(entries) == 2

    def _test_delete(self):
        # register two callables, one with a schedule in the future
        now = datetime.datetime.now()
        future_schedule = now + datetime.timedelta(milliseconds=2)
        past_schedule = now - datetime.timedelta(seconds=10)
        self.interface.register_callable(tst_add, schedule=future_schedule)
        self.interface.register_callable(tst_callable, schedule=past_schedule)
        # test to get the `tst_callable` function on due
        # and delete it from the db
        entry = self.interface.get_tasks_on_due()[0]
        assert entry["function_name"] == tst_callable.__name__
        self.interface.delete_callable(entry)
        # wait and test to get the remaining single entry
        # and check whether it is the `tst_add` function
        time.sleep(0.002)
        entries = self.interface.get_tasks_on_due()
        assert len(entries) == 1
        entry = entries[0]
        assert entry["function_name"] == tst_add.__name__

    def _test_get_task_by_signature(self):
        # register two callables, one with a schedule in the future
        schedule = datetime.datetime.now() + datetime.timedelta(seconds=10)
        self.interface.register_callable(tst_add, schedule=schedule)
        self.interface.register_callable(tst_callable)
        # find a nonexistent callable should return an empty generator
        entries = self.interface.get_tasks_by_signature(tst_multiply)
        assert len(entries) == 0
        # find a callable scheduled for the future:
        entries = self.interface.get_tasks_by_signature(tst_add)
        assert len(entries) == 1

    def _test_get_tasks_by_signature(self):
        # it is allowed to register the same callables multiple times.
        # regardless of the schedule `get_tasks_by_signature()` should return
        # all entries.
        schedule = datetime.datetime.now() + datetime.timedelta(seconds=10)
        self.interface.register_callable(tst_add, schedule=schedule)
        self.interface.register_callable(tst_add)
        entries = list(self.interface.get_tasks_by_signature(tst_add))
        assert len(entries) == 2

    def _test_get_task_on_due_and_set_status(self):
        # set two task which are on due with status WAITING (default).
        # Select them by setting the status to PROCESSED.
        # A second selection should not work.
        schedule = datetime.datetime.now() - datetime.timedelta(seconds=10)
        self.interface.register_callable(tst_add, schedule=schedule)
        self.interface.register_callable(tst_add, schedule=schedule)
        entries = self.interface.get_tasks_on_due(
            status=sql_interface.TASK_STATUS_WAITING,
            new_status=sql_interface.TASK_STATUS_PROCESSING
        )
        assert len(entries) == 2
        # do this a second time should not work
        entries = self.interface.get_tasks_on_due(
            status=sql_interface.TASK_STATUS_WAITING,
            new_status=sql_interface.TASK_STATUS_PROCESSING
        )
        assert len(entries) == 0
        # but the two entries are still there:
        entries = self.interface.get_tasks()
        assert len(entries) == 2

    def _test_update_schedule(self):
        # entries like cronjobs should not get deleted from the tasks
        # but updated with the next schedule
        schedule = datetime.datetime.now()
        next_schedule = schedule + datetime.timedelta(seconds=10)
        self.interface.register_callable(tst_add, schedule=schedule)
        entry = self.interface.get_tasks_by_signature(tst_add)[0]
        assert entry["schedule"] == schedule
        self.interface.update_crontask_schedule(entry["rowid"], next_schedule)
        entry = self.interface.get_tasks_by_signature(tst_add)[0]
        assert entry["schedule"] == next_schedule

    def _test_update_crontask(self):
        """
        When a crontask is selected for handling because it is 'on due',
        the status changes from WAITING to PROCESSING. After
        task-handling and schedule update the status must get reset to
        WAITING.
        """
        # after adding a crontask the task is in WAITING state:
        self.interface.register_callable(tst_add)
        task = self.interface.get_tasks_by_signature(tst_add)[0]
        assert task.status == sql_interface.TASK_STATUS_WAITING
        # after retrieving on due, the state changes to PROCESSING
        # for the returned task-object and also for the stored task-object:
        task = self.interface.get_tasks_on_due(
            new_status=sql_interface.TASK_STATUS_PROCESSING
        )[0]
        assert task.status == sql_interface.TASK_STATUS_PROCESSING
        task = self.interface.get_tasks_by_signature(tst_add)[0]
        assert task.status == sql_interface.TASK_STATUS_PROCESSING
        # after calling update_crontask_schedule() the status must
        # get reset to WAITING again:
        schedule = datetime.datetime.now()
        rowid = task.rowid
        self.interface.update_task_schedule(task, schedule)
        task = self.interface.get_tasks_by_signature(tst_add)[0]
        assert task.status == sql_interface.TASK_STATUS_WAITING

    def _test_result_by_uuid_no_result(self):
        # result should be None if no entry found
        uuid_ = uuid.uuid4().hex
        result = self.interface.get_result_by_uuid(uuid_)
        assert result is None

    def _test_result_by_uuid_result_registered(self):
        uuid_ = uuid.uuid4().hex
        self.interface.register_result(tst_add, uuid=uuid_)
        result = self.interface.get_result_by_uuid(uuid_)
        # return a TaskResult instance:
        assert result is not None
        assert result.is_waiting is True

    def _test_update_result_no_error(self):
        answer = 42
        uuid_ = uuid.uuid4().hex
        self.interface.register_result(tst_add, uuid=uuid_)
        self.interface.update_result(uuid_, result=answer)
        result = self.interface.get_result_by_uuid(uuid_)
        assert result.is_ready is True
        assert result.function_result == answer
        # test shortcut for function_result:
        assert result.result == answer

    def test_update_result_with_error(self):
        message = "ValueError: more text here ..."
        uuid_ = uuid.uuid4().hex
        self.interface.register_result(tst_add, uuid=uuid_)
        self.interface.update_result(uuid_, error_message=message)
        result = self.interface.get_result_by_uuid(uuid_)
        assert result.has_error is True

    def test_do_not_delete_waiting_results(self):
        self.interface._result_ttl = datetime.timedelta()
        self.interface.register_result(tst_callable, uuid.uuid4().hex)
        self.interface.register_result(tst_add, uuid.uuid4().hex)
        entries = self.interface.count_results()
        assert entries == 2

    def test_delete_outdated_results(self):
        # register two results, one of them outdated.
        uuid_ = uuid.uuid4().hex
        self.interface.register_result(
            tst_callable,
            uuid_,
            status=sql_interface.TASK_STATUS_READY
        )
        # set ttl to 0:
        self.interface._result_ttl = datetime.timedelta()
        # this result is outdated
        self.interface.register_result(
            tst_add,
            uuid.uuid4().hex,
            status=sql_interface.TASK_STATUS_READY
        )
        entries = self.interface.count_results()
        assert entries == 2
        self.interface.delete_outdated_results()
        entries = self.interface.count_results()
        assert entries == 1
        # the remaining entry should be the `tst_callable` result
        entry = self.interface.get_result_by_uuid(uuid_)
        assert entry.function_module == tst_callable.__module__
        assert entry.function_name == tst_callable.__name__

    def test_delete_mixed_results(self):
        # register a waiting result, a regular result, an outdated result
        # and an outdated result with error state.
        # After deleting the outdated results the entries should be
        # decreased by one.
        # the waiting result:
        self.interface.register_result(tst_callable, uuid.uuid4().hex)
        # the regular result (not outdated)
        self.interface.register_result(
            tst_callable,
            uuid.uuid4().hex,
            status=sql_interface.TASK_STATUS_READY
        )
        # set ttl to 0:
        self.interface._result_ttl = datetime.timedelta()
        # the outdated result:
        self.interface.register_result(
            tst_callable,
            uuid.uuid4().hex,
            status=sql_interface.TASK_STATUS_READY
        )
        # the outdated result in error state:
        self.interface.register_result(
            tst_callable,
            uuid.uuid4().hex,
            status=sql_interface.TASK_STATUS_ERROR
        )
        # test for decreasing entries:
        entries = self.interface.count_results()
        self.interface.delete_outdated_results()
        remaining_entries = self.interface.count_results()
        assert entries - remaining_entries == 1

    def test_count_rows(self):
        # call _count_table_rows on an empty table:
        rows = self.interface._count_table_rows(
            table_name=sql_interface.DB_TABLE_NAME_TASK)
        assert rows == 0
        # create a row and count again:
        self.interface.register_callable(tst_callable)
        rows = self.interface._count_table_rows(
            table_name=sql_interface.DB_TABLE_NAME_TASK)
        assert rows == 1
        # check for exception in case of an unknown table:
        self.assertRaises(
            sqlite3.OperationalError,
            self.interface._count_table_rows,
            table_name="unknwon_table_name"
        )

    def test_count_results(self):
        # register three results.
        # check whether there are three entries in the database
        self.interface.register_result(tst_callable, uuid.uuid4().hex)
        self.interface.register_result(tst_add, uuid.uuid4().hex)
        self.interface.register_result(tst_multiply, uuid.uuid4().hex)
        entries = self.interface.count_results()
        assert entries == 3

    def test_count_tasks(self):
        # register three callables, two as cronjobs.
        # check whether there are three entries in the database
        self.interface.register_callable(tst_callable)
        self.interface.register_callable(tst_add, crontab="* * * * *")
        self.interface.register_callable(tst_multiply, crontab="* * * * *")
        entries = self.interface.count_tasks()
        assert entries == 3

    def test_delete_cronjobs(self):
        # register three callables, two as cronjobs.
        # delete the cronjobs and check that a single item in left
        # in the database.
        self.interface.register_callable(tst_callable)
        self.interface.register_callable(tst_add, crontab="* * * * *")
        self.interface.register_callable(tst_multiply, crontab="* * * * *")
        self.interface.delete_cronjobs()
        entries = self.interface.count_tasks()
        assert entries == 1
        # remaining entry should be the tst_callable
        entry = self.interface.get_tasks_on_due()[0]
        assert entry.function_module == tst_callable.__module__
        assert entry.function_name == tst_callable.__name__

    def test_initialize_settings_table(self):
        """
        Combined test for
            _initialize_settings_table()
            get_settings()
            set_settings()
        """

        def get_rows():
            return self.interface._count_table_rows(
                table_name=sql_interface.DB_TABLE_NAME_SETTINGS
            )
        # call to _initialize_settings_table() should add an entry:
        self.interface._initialize_settings_table()
        assert get_rows() == 1
        settings = self.interface.get_settings()
        assert settings.max_workers == sql_interface.DEFAULT_MAX_WORKERS
        new_max_workers = sql_interface.DEFAULT_MAX_WORKERS + 1
        settings.max_workers = new_max_workers
        self.interface.set_settings(settings)
        # value of max_workers should have changed
        new_settings = self.interface.get_settings()
        assert new_settings.max_workers == new_max_workers

        # a second call should have no effect:
        self.interface._initialize_settings_table()
        assert get_rows() == 1

        # and the former changed values are also unchanged:
        the_settings = self.interface.get_settings()
        assert the_settings.max_workers == new_max_workers

    def test_worker_settings(self):
        # increment and decrement worker_pids in the settings

        def check_settings():
            settings = self.interface.get_settings()
            assert settings.running_workers == len(test_pids)
            text = ",".join(map(str, test_pids))
            assert settings.worker_pids == text

        # register pids
        test_pids = [42, 377, 42980]
        for pid in test_pids:
            self.interface.increment_running_workers(pid)
        check_settings()
        # remove a single one
        pid = test_pids.pop(1)
        self.interface.decrement_running_workers(pid)
        check_settings()
        # remove the remaining ones
        while test_pids:
            self.interface.decrement_running_workers(test_pids.pop())
        check_settings()


# decorator testing includes database access.
# for easier testing decorator tests are included here.

class TestCronDecorator(unittest.TestCase):

    def setUp(self):
        sql_interface.SQLiteInterface._instance = None
        self.interface = sql_interface.SQLiteInterface()
        self.interface.init_database(db_name=TEST_DB_NAME)
        self.orig_interface = decorators.interface
        decorators.interface = self.interface

    def tearDown(self):
        pathlib.Path(decorators.interface.db_name).unlink()
        decorators.interface = self.orig_interface

    def test_cron_no_arguments_active(self):
        # the database should have one entry with the default crontab
        wrapper = decorators.cron()
        func = wrapper(tst_cron)
        assert func == tst_cron
        entries = list(self.interface.get_tasks_by_signature(tst_cron))
        assert len(entries) == 1
        entry = entries[0]
        assert entry["crontab"] == decorators.DEFAULT_CRONTAB

    def test_suppress_identic_cronjobs(self):
        # register multiple cronjobs of a single callable.
        # then add the cronjob again by means of the decorator.
        # the db then should hold just a single entry deleting
        # the other ones.
        # should not happen:
        self.interface.register_callable(tst_cron, crontab=decorators.DEFAULT_CRONTAB)
        self.interface.register_callable(tst_cron, crontab=decorators.DEFAULT_CRONTAB)
        entries = list(self.interface.get_tasks_by_signature(tst_cron))
        assert len(entries) == 2
        # now add the same function with the cron decorator:
        crontab = "10 2 1 * *"
        wrapper = decorators.cron(crontab=crontab)
        func = wrapper(tst_cron)
        # just a single entry should now be in the database
        # (the one added by the decorator):
        entries = list(self.interface.get_tasks_by_signature(tst_cron))
        assert len(entries) == 1
        entry = entries[0]
        assert entry["crontab"] == crontab


class TestDelayDecorator(unittest.TestCase):

    def setUp(self):
        sql_interface.SQLiteInterface._instance = None
        self.interface = sql_interface.SQLiteInterface()
        self.interface.init_database(db_name=TEST_DB_NAME)
        self.orig_decorator_interface = decorators.interface
        decorators.interface = self.interface

    def tearDown(self):
        pathlib.Path(self.interface.db_name).unlink()
        decorators.interface = self.orig_decorator_interface

    def _activate(self):
        self.interface.accept_registrations = True

    def _deactivate(self):
        self.interface.accept_registrations = False

    def test_wrapper_seen_by_worker(self):
        # does not return the original function but calls
        # the original function indirect instead of registering
        # the task in the db.
        self._deactivate()
        wrapper = decorators.delay(tst_delay)
        task_result = wrapper()
        assert task_result == 42
        # activate so that get_tasks_by_signature() works
        self._activate()
        entries = self.interface.get_tasks_by_signature(tst_delay)
        assert len(entries) == 0

    def test_active(self):
        wrapper = decorators.delay(tst_delay)
        wrapper_return_value = wrapper()
        assert isinstance(wrapper_return_value, TaskResult) is True
        entries = self.interface.get_tasks_by_signature(tst_delay)
        assert len(entries) == 1

    def test_inactive(self):
        # autocron does not run, but the wrapper returns a
        # TaskResult instance for the application.
        self.interface.autocron_lock_is_set = True
        wrapper = decorators.delay(tst_delay)
        wrapper_return_value = wrapper()
        assert isinstance(wrapper_return_value, TaskResult) is True
        assert wrapper_return_value.is_ready is True
        assert wrapper_return_value.result == 42

    def test_active_and_get_result(self):
        """
        Test story:

        1. wrap a function with the delay decorator.
           This should return a TaskResult.
        2. Check for the task entry in db.
        3. Then call `Worker.handle_tasks` what should return True.
        4. Then call `interface.get_result_by_uuid` which should return
           a TaskResult instance with the correct result.

        """
        # 1: wrap function and call the wrapper with arguments
        wrapper = decorators.delay(tst_add)
        task_result = wrapper(40, 2)
        assert task_result.uuid is not None
        assert isinstance(task_result.uuid, str)

        # 2: a single entry is now in both tables:
        time.sleep(0.001)  # have some patience with the db.
        task_entries = self.interface.get_tasks_by_signature(tst_add)
        assert len(task_entries) == 1
        # result is also of type TaskResult()
        result = self.interface.get_result_by_uuid(task_result.uuid)
        assert result is not None
        assert result.is_waiting is True

        # 3: let the worker handle the task:
        # instanciate but don't start the worker
        worker_ = worker.Worker(self.interface.db_name)
        # return True if at least one task has handled:
        return_value = worker_.handle_tasks()
        assert return_value is True
        time.sleep(0.001)  # have some patience with the db.
        # after handling the task should be removed from the db:
        task_entries = self.interface.get_tasks_by_signature(tst_add)
        assert len(task_entries) == 0

        # 4: check whether the worker has updated the result entry in the db:
        result = self.interface.get_result_by_uuid(task_result.uuid)
        assert result.is_ready is True
        assert result.result == 42  # 40 + 2


class x_TestHybridNamespace(unittest.TestCase):

    def setUp(self):
        self.data = {"pi": 3.141, "answer": 42}
        self.attr_dict = sql_interface.HybridNamespace(self.data)

    def test_dict_access(self):
        self.attr_dict["one"] = 1
        assert self.attr_dict["one"] == 1

    def test_attribute_access(self):
        self.attr_dict.two = 2
        assert self.attr_dict.two == 2

    def test_mixed_access(self):
        self.attr_dict.three = 3
        assert self.attr_dict["three"] == 3
        self.attr_dict["four"] = 4
        assert self.attr_dict.four == 4

    def test_get_init_values(self):
        assert self.attr_dict["pi"] == self.data["pi"]
        assert self.attr_dict.pi == self.data["pi"]
        assert self.attr_dict["answer"] == self.data["answer"]
        assert self.attr_dict.answer == self.data["answer"]


def task_result_function(a, b, c="c", d="d"):
    return "".join([a, b, c, d])


class TestTaskResult(unittest.TestCase):

    def setUp(self):
        self.interface = sql_interface.SQLiteInterface()
        self.interface.init_database(TEST_DB_NAME)

    def tearDown(self):
        if self.interface.db_name:
            pathlib.Path(self.interface.db_name).unlink()

    def test_task_result_from_function_call(self):
        args = ("a", "b")
        tr = sql_interface.TaskResult.from_function_call(
            task_result_function,
            *args
        )
        assert tr.status == sql_interface.TASK_STATUS_READY
        assert tr.result == "abcd"
        args = ("e", "f")
        kwargs = {"c": "g", "d": "h"}
        tr = sql_interface.TaskResult.from_function_call(
            task_result_function,
            *args,
            **kwargs
        )
        assert tr.result == "efgh"

    def test_update_task_result(self):
        """
        Create an empty TaskResult in waiting state and create a result.
        Both with the same uuid. Check whether TaskResult can update
        itself with a delated result.
        """
        uid = uuid.uuid4().hex
        tr = sql_interface.TaskResult.from_registration(uid, self.interface)
        assert tr.status is sql_interface.TASK_STATUS_WAITING
        assert tr.is_waiting is True
        # next is a hack because self.assertRaises expects as second argument
        # a callable, like self.assertRaises(AttributeError, tr.result).
        # This does not work with properties.
        try:
            tr.result
        except AttributeError:
            exception_was_raised = True
        else:
            exception_was_raised = False
        assert exception_was_raised is True
        # now inject the task to execute:
        self.interface.register_result(task_result_function, uid, ("a", "b"))
        # the task is still in waiting state:
        assert tr.is_waiting is True
        # update result, we calculate the result here:
        result = task_result_function("a", "b")
        self.interface.update_result(uid, result=result)
        # now the state hase changed:
        assert tr.is_waiting is False
        assert tr.is_ready is True
        assert tr.result == "abcd"


class TestDelayedInitialization(unittest.TestCase):
    """
    Special case of the TestCronDecorator suite assuming that the name
    of the database will get provided by the start() function which will
    get called after the @con-decorators have been executed at import
    time. So these tests are testing the preregistration of callables.
    """

    def setUp(self):
        # set up a non-initialized instance of the sql_interface,
        self.orig_interface = decorators.interface
        decorators.interface = sql_interface.SQLiteInterface()

    def tearDown(self):
        if decorators.interface.db_name is not None:
            pathlib.Path(decorators.interface.db_name).unlink()
        decorators.interface = self.orig_interface

    def test_preregistration(self):
        # register a cronfunction (defined near TestCronDecorator()) before
        # the database has been set up.
        decorators.interface.register_callable(tst_cron)
        # should fail, because the database has not been set up yet:
        self.assertRaises(OSError, decorators.interface.get_tasks)
        # after initializing the task should be available:
        decorators.interface.init_database(TEST_DB_NAME)
        tasks = decorators.interface.get_tasks()
        assert len(tasks) == 1
        # check that the task is indeed the tst_cron()
        # (the tasks are of type 'HybridNamespace'. See worker.process_task)
        task = tasks[0]
        assert task.function_name == tst_cron.__name__

    def test_preregister_tst_cron(self):
        # use the cron decorator to register cron functions
        cron_decorator = decorators.cron()
        cron_decorator(tst_cron)
        cron_decorator(tst_delay)
        # set the database and check for two entries
        decorators.interface.init_database(TEST_DB_NAME)
        decorators.interface._register_preregistered_tasks()
        tasks = decorators.interface.get_tasks()
        assert len(tasks) == 2
        # register the same tst_cron() again
        # should not duplicate the cron-tasks:
        cron_decorator(tst_cron)
        tasks = decorators.interface.get_tasks()
        assert len(tasks) == 2
        # registration of other functions should work as expected:
        decorators.interface.register_callable(tst_multiply)
        tasks = decorators.interface.get_tasks()
        assert len(tasks) == 3

    def test_register_tst_cron_after_start(self):
        decorators.interface.init_database(TEST_DB_NAME)
        tasks = decorators.interface.get_tasks()
        assert len(tasks) == 0
        cron_decorator = decorators.cron()
        cron_decorator(tst_cron)
        tasks = decorators.interface.get_tasks()
        assert len(tasks) == 1
        # is the crontab attribute set?
        task = tasks[0]
        self.assertTrue(task.crontab)

