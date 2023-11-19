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

from autocron import configuration
from autocron import decorators
from autocron import sql_interface
from autocron import worker


TEST_DB_NAME = configuration.configuration.autocron_path / "test.db"


def test_callable(*args, **kwargs):
    return args, kwargs

def test_adder(a, b):
    return a + b

def test_multiply(a, b):
    return a * b


class TestSQLInterface(unittest.TestCase):

    def setUp(self):
        self.interface = sql_interface.SQLiteInterface(db_name=TEST_DB_NAME)
        self._result_ttl = configuration.configuration.result_ttl

    def tearDown(self):
        pathlib.Path(self.interface.db_name).unlink()
        configuration.configuration.result_ttl = self._result_ttl

    def test_storage(self):
        entries = self.interface.get_tasks_on_due()
        self.assertFalse(list(entries))
        self.interface.register_callable(test_callable)
        entries = self.interface.get_tasks_on_due()
        assert len(entries) == 1

    def test_entry_signature(self):
        self.interface.register_callable(test_callable)
        entries = self.interface.get_tasks_on_due()
        obj = entries[0]
        assert isinstance(obj, sql_interface.HybridNamespace) is True
        assert obj["function_module"] == test_callable.__module__
        assert obj["function_name"] == test_callable.__name__

    def test_arguments(self):
        args = ["pi", 3.141]
        kwargs = {"answer": 41, 10: "ten"}
        crontab = "* 1 * * *"
        self.interface.register_callable(
            test_callable, crontab=crontab, args=args, kwargs=kwargs
        )
        entries = list(self.interface.get_tasks_on_due())
        obj = entries[0]
        assert obj["crontab"] == crontab
        assert obj["args"] == args
        assert obj["kwargs"] == kwargs

    def test_get_tasks(self):
        # test the generic function to select all tasks:
        schedule = datetime.datetime.now() + datetime.timedelta(seconds=10)
        self.interface.register_callable(test_adder, schedule=schedule)
        self.interface.register_callable(test_callable)
        self.interface.register_callable(test_multiply, crontab="* * * * *")
        # should return everything:
        entries = self.interface.get_tasks()
        assert len(entries) == 3

    def test_schedules_get_one_of_two(self):
        # register two callables, one with a schedule in the future
        schedule = datetime.datetime.now() + datetime.timedelta(seconds=10)
        self.interface.register_callable(test_adder, schedule=schedule)
        self.interface.register_callable(test_callable)
        # test to get one callable at due
        entries = self.interface.get_tasks_on_due()
        assert len(entries) == 1

    def test_schedules_get_two_of_two(self):
        # register two callables, both scheduled in the present or past
        schedule = datetime.datetime.now() - datetime.timedelta(seconds=10)
        self.interface.register_callable(test_adder, schedule=schedule)
        self.interface.register_callable(test_callable)
        # test to get one callable at due
        entries = self.interface.get_tasks_on_due()
        assert len(entries) == 2

    def test_delete(self):
        # register two callables, one with a schedule in the future
        schedule = datetime.datetime.now() + datetime.timedelta(milliseconds=1)
        self.interface.register_callable(test_adder, schedule=schedule)
        self.interface.register_callable(test_callable)
        # test to get the `test_callable` function on due
        # and delete it from the db
        entry = self.interface.get_tasks_on_due()[0]
        assert entry["function_name"] == test_callable.__name__
        self.interface.delete_callable(entry)
        # wait and test to get the remaining single entry
        # and check whether it is the `test_adder` function
        time.sleep(0.001)
        entries = self.interface.get_tasks_on_due()
        assert len(entries) == 1
        entry = entries[0]
        assert entry["function_name"] == test_adder.__name__

    def test_get_task_by_signature(self):
        # register two callables, one with a schedule in the future
        schedule = datetime.datetime.now() + datetime.timedelta(seconds=10)
        self.interface.register_callable(test_adder, schedule=schedule)
        self.interface.register_callable(test_callable)
        # find a nonexistent callable should return an empty generator
        entries = self.interface.get_tasks_by_signature(test_multiply)
        assert len(entries) == 0
        # find a callable scheduled for the future:
        entries = self.interface.get_tasks_by_signature(test_adder)
        assert len(entries) == 1

    def test_get_tasks_by_signature(self):
        # it is allowed to register the same callables multiple times.
        # regardless of the schedule `get_tasks_by_signature()` should return
        # all entries.
        schedule = datetime.datetime.now() + datetime.timedelta(seconds=10)
        self.interface.register_callable(test_adder, schedule=schedule)
        self.interface.register_callable(test_adder)
        entries = list(self.interface.get_tasks_by_signature(test_adder))
        assert len(entries) == 2

    def test_update_schedule(self):
        # entries like cronjobs should not get deleted from the tasks
        # but updated with the next schedule
        schedule = datetime.datetime.now()
        next_schedule = schedule + datetime.timedelta(seconds=10)
        self.interface.register_callable(test_adder, schedule=schedule)
        entry = self.interface.get_tasks_by_signature(test_adder)[0]
        assert entry["schedule"] == schedule
        self.interface.update_schedule(entry["rowid"], next_schedule)
        entry = self.interface.get_tasks_by_signature(test_adder)[0]
        assert entry["schedule"] == next_schedule

    def test_result_by_uuid_no_result(self):
        # result should be None if no entry found
        uuid_ = uuid.uuid4().hex
        result = self.interface.get_result_by_uuid(uuid_)
        assert result is None

    def test_result_by_uuid_result_registered(self):
        uuid_ = uuid.uuid4().hex
        self.interface.register_result(test_adder, uuid=uuid_)
        result = self.interface.get_result_by_uuid(uuid_)
        # return a TaskResult instance:
        assert result is not None
        assert result.is_waiting is True

    def test_update_result_no_error(self):
        answer = 42
        uuid_ = uuid.uuid4().hex
        self.interface.register_result(test_adder, uuid=uuid_)
        self.interface.update_result(uuid_, result=answer)
        result = self.interface.get_result_by_uuid(uuid_)
        assert result.is_ready is True
        assert result.function_result == answer
        # test shortcut for function_result:
        assert result.result == answer

    def test_update_result_with_error(self):
        message = "ValueError: more text here ..."
        uuid_ = uuid.uuid4().hex
        self.interface.register_result(test_adder, uuid=uuid_)
        self.interface.update_result(uuid_, error_message=message)
        result = self.interface.get_result_by_uuid(uuid_)
        assert result.has_error is True

    def test_do_not_delete_waiting_results(self):
        configuration.configuration.result_ttl = datetime.timedelta()
        self.interface.register_result(test_callable, uuid.uuid4().hex)
        self.interface.register_result(test_adder, uuid.uuid4().hex)
        entries = self.interface.count_results()
        assert entries == 2

    def test_delete_outdated_results(self):
        # register two results, one of them outdated.
        uuid_ = uuid.uuid4().hex
        self.interface.register_result(
            test_callable,
            uuid_,
            status=sql_interface.TASK_STATUS_READY
        )
        configuration.configuration.result_ttl = datetime.timedelta()
        # this result is outdated
        self.interface.register_result(
            test_adder,
            uuid.uuid4().hex,
            status=sql_interface.TASK_STATUS_READY
        )
        entries = self.interface.count_results()
        assert entries == 2
        self.interface.delete_outdated_results()
        entries = self.interface.count_results()
        assert entries == 1
        # the remaining entry should be the `test_callable` result
        entry = self.interface.get_result_by_uuid(uuid_)
        assert entry.function_module == test_callable.__module__
        assert entry.function_name == test_callable.__name__

    def test_delete_mixed_results(self):
        # register a waiting result, a regular result, an outdated result
        # and an outdated result with error state.
        # After deleting the outdated results the entries should be
        # decreased by one.
        # the waiting result:
        self.interface.register_result(test_callable, uuid.uuid4().hex)
        # the regular result (not outdated)
        self.interface.register_result(
            test_callable,
            uuid.uuid4().hex,
            status=sql_interface.TASK_STATUS_READY
        )
        # set ttl to 0:
        configuration.configuration.result_ttl = datetime.timedelta()
        # the outdated result:
        self.interface.register_result(
            test_callable,
            uuid.uuid4().hex,
            status=sql_interface.TASK_STATUS_READY
        )
        # the outdated result in error state:
        self.interface.register_result(
            test_callable,
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
        self.interface.register_callable(test_callable)
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
        self.interface.register_result(test_callable, uuid.uuid4().hex)
        self.interface.register_result(test_adder, uuid.uuid4().hex)
        self.interface.register_result(test_multiply, uuid.uuid4().hex)
        entries = self.interface.count_results()
        assert entries == 3

    def test_count_tasks(self):
        # register three callables, two as cronjobs.
        # check whether there are three entries in the database
        self.interface.register_callable(test_callable)
        self.interface.register_callable(test_adder, crontab="* * * * *")
        self.interface.register_callable(test_multiply, crontab="* * * * *")
        entries = self.interface.count_tasks()
        assert entries == 3

    def test_delete_cronjobs(self):
        # register three callables, two as cronjobs.
        # delete the cronjobs and check that a single item in left
        # in the database.
        self.interface.register_callable(test_callable)
        self.interface.register_callable(test_adder, crontab="* * * * *")
        self.interface.register_callable(test_multiply, crontab="* * * * *")
        self.interface.delete_cronjobs()
        entries = self.interface.count_tasks()
        assert entries == 1
        # remaining entry should be the test_callable
        entry = self.interface.get_tasks_on_due()[0]
        assert entry.function_module == test_callable.__module__
        assert entry.function_name == test_callable.__name__

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
        assert settings.max_workers == sql_interface.MAX_WORKERS_DEFAULT
        new_max_workers = sql_interface.MAX_WORKERS_DEFAULT + 1
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

    def test_increment_running_workers(self):
        settings = self.interface.get_settings()
        running_workers = settings.running_workers
        self.interface.increment_running_workers()
        settings = self.interface.get_settings()
        assert running_workers == settings.running_workers - 1

    def test_decrement_running_workers(self):
        settings = self.interface.get_settings()
        settings.running_workers = 1
        self.interface.set_settings(settings)
        self.interface.decrement_running_workers()
        settings = self.interface.get_settings()
        assert settings.running_workers == 0
        # second decrement should not go below zero
        self.interface.decrement_running_workers()
        settings = self.interface.get_settings()
        assert settings.running_workers == 0

    def test_try_increment_running_workers(self):
        # no workers allowed: -> False
        settings = self.interface.get_settings()
        settings.max_workers = 0
        self.interface.set_settings(settings)
        self.assertFalse(self.interface.try_increment_running_workers())
        # default settings: -> True
        settings = self.interface.get_settings()
        settings.max_workers = 1
        settings.running_workers = 0
        self.interface.set_settings(settings)
        self.assertTrue(self.interface.try_increment_running_workers())
        # but an additional worker is not allowed:
        self.assertFalse(self.interface.try_increment_running_workers())



# decorator testing includes database access.
# for easier testing decorator tests are included here.

def cron_function():
    pass


class TestCronDecorator(unittest.TestCase):

    def setUp(self):
        self.orig_interface = decorators.interface
        decorators.interface = sql_interface.SQLiteInterface(db_name=TEST_DB_NAME)

    def tearDown(self):
        pathlib.Path(decorators.interface.db_name).unlink()
        decorators.interface = self.orig_interface

#     def test_cron_no_arguments_inactive(self):
#         # the database should have no entry with the default crontab
#         # if configuration is not active
#         wrapper = decorators.cron()
#         func = wrapper(cron_function)
#         assert func == cron_function
#         entries = list(decorators.interface.get_tasks_by_signature(cron_function))
#         assert len(entries) == 0

    def test_cron_no_arguments_active(self):
        # the database should have one entry with the default crontab
        # if configuration is active
#         configuration.configuration.is_active = True
        wrapper = decorators.cron()
        func = wrapper(cron_function)
        assert func == cron_function
        entries = list(decorators.interface.get_tasks_by_signature(cron_function))
        assert len(entries) == 1
        entry = entries[0]
        assert entry["crontab"] == decorators.DEFAULT_CRONTAB
#         configuration.configuration.is_active = False

    def test_suppress_identic_cronjobs(self):
        # register multiple cronjobs of a single callable.
        # then add the cronjob again by means of the decorator.
        # the db then should hold just a single entry deleting
        # the other ones.
        # should not happen:
        decorators.interface.register_callable(cron_function, crontab=decorators.DEFAULT_CRONTAB)
        decorators.interface.register_callable(cron_function, crontab=decorators.DEFAULT_CRONTAB)
        entries = list(decorators.interface.get_tasks_by_signature(cron_function))
        assert len(entries) == 2
        # now add the same function with the cron decorator:
        crontab = "10 2 1 * *"
        configuration.configuration.is_active = True
        wrapper = decorators.cron(crontab=crontab)
        func = wrapper(cron_function)
        # just a single entry should no be in the database
        # (the one added by the decorator):
        entries = list(decorators.interface.get_tasks_by_signature(cron_function))
        assert len(entries) == 1
        entry = entries[0]
        assert entry["crontab"] == crontab
        configuration.configuration.is_active = False


def delay_function():
    return 42


class TestDelayDecorator(unittest.TestCase):

    def setUp(self):
        self.orig_decorator_interface = decorators.interface
        self.orig_worker_interface = worker.interface
        worker.interface = decorators.interface =\
            sql_interface.SQLiteInterface(db_name=TEST_DB_NAME)
        self._deactivate()

    def tearDown(self):
        pathlib.Path(decorators.interface.db_name).unlink()
        decorators.interface = self.orig_decorator_interface
        worker.interface = self.orig_worker_interface
        self._deactivate()

    @staticmethod
    def _activate():
        configuration.configuration.is_active = True

    @staticmethod
    def _deactivate():
        configuration.configuration.is_active = False

    def test_inactive(self):
        # does not return the original function but calls
        # the original function indirect instead of registering
        # the task in the db.
        wrapper = decorators.delay(delay_function)
        assert wrapper() == 42
        entries = decorators.interface.get_tasks_by_signature(delay_function)
        assert len(entries) == 0

    def test_active(self):
        self._activate()
        wrapper = decorators.delay(delay_function)
        wrapper_return_value = wrapper()
        assert wrapper_return_value != 42
        assert isinstance(wrapper_return_value, str) is True  # return uuid as string
        assert len(wrapper_return_value) == 32  # length of a uuid.hex string
        entries = decorators.interface.get_tasks_by_signature(delay_function)
        assert len(entries) == 1

    def test_active_and_get_result(self):
        """
        Test story:

        1. wrap a function with the delegate decorator.
           This should return a uuid.
        2. Check for task entry in db.
        3. Then call `Worker.handle_tasks` what should return True.
        4. Then call `interface.get_result_by_uuid` which should return
           a TaskResult instance with the correct result.

        """
        # 1: wrap function and call the wrapper with arguments
        self._activate()
        wrapper = decorators.delay(test_adder)
        uuid_ = wrapper(40, 2)
        assert uuid_ is not None

        # 2: a single entry is now in both tables:
        time.sleep(0.001)  # have some patience with the db.
        task_entries = decorators.interface.get_tasks_by_signature(test_adder)
        assert len(task_entries) == 1
        result = decorators.interface.get_result_by_uuid(uuid_)
        assert result is not None
        assert result.is_waiting is True

        # 3: let the worker handle the task:
        # instanciate but don't start the worker
        # this will set configuration.is_active to False, because the
        # worker assumes to run in a separate process.
        # This is important as otherwise calling the task will not execute
        # the task but registering the task again by the wrapper.
        worker_ = worker.Worker()
        # return True if at least one task has handled:
        return_value = worker_.handle_tasks()
        assert return_value is True
        time.sleep(0.001)  # have some patience with the db.
        # after handling the task should be removed from the db:
        task_entries = decorators.interface.get_tasks_by_signature(test_adder)
        assert len(task_entries) == 0

        # 4: check whether the worker has updated the result entry in the db:
        result = decorators.interface.get_result_by_uuid(uuid_)
        assert result.is_ready is True
        assert result.result == 42  # 40 + 2


class TestHybridNamespace(unittest.TestCase):

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
