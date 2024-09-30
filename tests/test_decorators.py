
import pathlib
import time

import pytest

from autocron import decorators
from autocron import sqlite_interface
# from autocron import worker


TEST_DB_NAME = "test.db"


# test marker functions:
def tst_cron():
    """tst_cron_docstring"""
    pass

def tst_add(a, b):
    """tst_add_docstring"""
    return a + b

def tst_div(a, b):
    """tst_div_docstring"""
    return a / b


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
    # inject the interface to the decorator module
    # so the decorators access the same interface
    decorators_interface = decorators.interface
    decorators.interface = interface
    yield interface
    decorators.interface = decorators_interface
    for db_name in (interface.db_name, tmp_db_name):
        if db_name is not None:
            pathlib.Path(interface.db_name).unlink(missing_ok=True)


def test_cron_inactive(interface):
    """
    The cron decorator should return the original function but should
    not register an entry in the database. As the registrator thread is
    not running, the registration end in the queue and nothing else
    happens.
    (This has changed by allowing to register a thread even if no
    registration thread is running. This was a change for django in
    debug mode with the reloader enabled.)
    """
    wrapper = decorators.cron()
    result = wrapper(tst_cron)
    assert result is tst_cron
    assert interface.count_tasks() == 1


def test_cron_active(interface):
    """
    The cron decorator should return the original function and should
    it save in the database.
    """
    wrapper = decorators.cron()
    result = wrapper(tst_cron)
    assert result is tst_cron
    # start registrator after execution of the decorator
    # processing the already populated queue.
    interface.registrator.start()
    time.sleep(0.1)
    assert interface.count_tasks() == 1
    interface.registrator.stop()


def test_delay_inactive(interface):
    """
    In inactive mode the delay decorator returns a result-instance with
    the function result.
    """
    interface.autocron_lock = True  # autocron is inactive
    wrapper = decorators.delay(tst_add)
    result = wrapper(40, 2)
    assert result.is_ready() is True
    assert result.has_error is False
    assert result.function_result == 42
    assert interface.count_tasks() == 0

    # raise an error during function execution:
    wrapper = decorators.delay(tst_div)
    result = wrapper(1, 0)
    assert result.is_ready() is True
    assert result.has_error is True
    assert result.function_result is None


def test_delay_active(interface):
    """
    In active mode the delay decorator returns a result-instance in
    waiting state and creates a task- and a result-entry in the
    database.
    """
    interface.registrator.start()
    assert interface.count_tasks() == 0
    assert interface.count_results() == 0
    wrapper = decorators.delay(tst_add)
    result = wrapper(40, 2)
    assert result.is_ready() is False
    time.sleep(0.1)
    assert interface.count_tasks() == 1
    assert interface.count_results() == 1
    interface.registrator.stop()


def test_delay_arguments(interface):
    """
    The delay decorator optional can take arguments for delaying. In
    this case a catcher is returned taking the callable to wrap.
    Otherwise it should behave like `test_delay_inactive` or
    `test_delay_active`.
    """
    interface.autocron_lock = True  # autocron is inactive
    catcher = decorators.delay(minutes=5)
    wrapper = catcher(tst_add)
    result = wrapper(40, 2)
    assert result.is_ready() is True
    assert result.has_error is False
    assert result.function_result == 42
    assert interface.count_tasks() == 0
