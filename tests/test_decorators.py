
import pathlib
import time

import pytest

from autocron import decorators
from autocron import sql_interface
from autocron import worker


TEST_DB_NAME = "test.db"


# test marker functions:
def tst_cron():
    pass

def tst_callable():
    pass

def tst_func():
    pass

def tst_add(a, b):
    return a + b


class InterfaceFixture:
    """
    Provides the initialized database, keeps it in sync with the
    decorator module and provides a teardown. This is implemented as a
    helper class for the pytest interface-fixture, but can also used
    separate.
    """
    def __init__(self, db_name=TEST_DB_NAME):
        self.db_name = db_name
        # set class attribute to None to not return a singleton
        sql_interface.SQLiteInterface._instance = None
        self.interface = sql_interface.SQLiteInterface()
        # inject the new interface in the decorators-module
        # so the decorator-function access the same db-interface:
        self.decorators_interface = decorators.interface
        decorators.interface = self.interface

    def set_up(self):
        # allows to initialize the database in a later step.
        # this is necessary for the preregistration test.
        self.interface.init_database(db_name=self.db_name)
        self.interface.task_registrator.start()


    def tear_down(self):
        self.interface.task_registrator.stop()
        decorators.interface = self.decorators_interface
        if self.interface.db_name:
            pathlib.Path(self.interface.db_name).unlink(missing_ok=True)


@pytest.fixture
def interface():
    """
    Returns a new initialised database instance and tears it down
    afterwards.
    """
    interface_fixture = InterfaceFixture()
    interface_fixture.set_up()
    yield interface_fixture.interface
    interface_fixture.tear_down()


def test_cron_with_default_crontab(interface):
    """
    Applying cron() without an argument should set the DEFAULT_CRONTAB
    """
    # cron() returns the original callable
    wrapper = decorators.cron()
    func = wrapper(tst_cron)
    assert func == tst_cron

    # give the registrator a bit time to do the job
    time.sleep(0.1)

    # the cron.wrapper() has registered the task in the database
    entries = interface.get_tasks()
    assert len(entries) == 1

    # crontab has the default value
    entry = entries[0]
    assert entry["crontab"] == decorators.DEFAULT_CRONTAB

    # and entry is an HybridNamespace object
    assert entry.crontab == decorators.DEFAULT_CRONTAB


def test_suppress_identic_cronjobs(interface):
    """
    A cronjob should get registered only once. The decorator should take
    care about this.
    Test-story: register the same callable multiple times. Then register
    the same callable by means of the cron() decorator. Just the
    callable registered by the cron() decorater should be in the
    database.
    """
    interface.register_task(tst_cron, crontab=decorators.DEFAULT_CRONTAB)
    interface.register_task(tst_cron, crontab=decorators.DEFAULT_CRONTAB)
    entries = interface.count_tasks()
    assert entries == 2

    # add same callable again by means of cron() providing a different
    # crontab for identification
    crontab = "10 2 1 * *"
    decorators.cron(crontab=crontab)(tst_cron)

    # give the registrator a bit time to do the job
    time.sleep(0.1)

    # just a single entry should now be in the database
    # and should have the crontab "10 2 1 * *"
    entries = interface.count_tasks()
    assert entries == 1
    task = interface.get_tasks()[0]
    assert task.crontab == crontab


@pytest.mark.parametrize(
    "functions, expected_entries", [
        ([tst_cron], 1),
        ([tst_cron, tst_cron], 1),
        ([tst_callable, tst_cron], 2),
        ([tst_callable, tst_cron, tst_func], 3),
        ([tst_func, tst_cron, tst_func], 2),
        ([tst_func, tst_func, tst_func], 1),
    ]
)
def test_allow_different_cronjobs(functions, expected_entries, interface):
    """
    A single cronjob is allowed just once, but different callables
    should not get affected.
    """
    for function in functions:
        decorators.cron()(function)

    # give the registrator a bit time to do the jobs
    time.sleep(0.1)

    assert expected_entries == interface.count_tasks()


@pytest.mark.parametrize(
    "function, is_cron, allowed", [
        (tst_func, False, True),
        (tst_func, False, False),
        (tst_cron, True, True),
        (tst_cron, True, False),
    ]
)
def test_register_cronjob_if_allowed(function, is_cron, allowed, interface):
    """
    callables should only registered if the interface accepts
    registrations. This check is not implemented by
    interface.task_registrator.register() but has to be made by the
    decorators.
    """
    assert interface.autocron_lock_is_set is False
    interface.accept_registrations = allowed
    if is_cron:
        decorators.cron()(function)
    else:
        decorators.delay(function)()
    # give the registrator a bit time to do the job
    time.sleep(0.1)
    assert allowed is bool(interface.count_tasks())


def test_handle_registered_task(interface):
    """
    Test story:
    1. register a delayed task, what should return a TaskResult object.
    2. let the worker handle the task.
    3. check for result.
    """
    arguments = (30, 12)
    expected_result = sum(arguments)
    patience = 0.001  # be a bit patient with the database
    # the database is clean, no entries:
    assert interface.count_tasks() == 0
    assert interface.count_results() == 0

    # register delayed task
    task_result = decorators.delay(tst_add)(30, 12)
    assert isinstance(task_result.uuid, str)
    assert len(task_result.uuid) > 0
    assert task_result.is_waiting is True

    # give the registrator a bit time to do the job
    time.sleep(0.1)

    # a single entry is now in the task- and the result-table:
    assert interface.count_tasks() == 1
    assert interface.count_results() == 1

    # let the worker handle the task:
    # the return_value is true if the worker has handled at least one task.
    worker_ = worker.Worker(interface.db_name)
    return_value = worker_.handle_tasks()
    assert return_value is True

    time.sleep(patience)  # have some patience with the db.

    # after handling the task, the task should be removed from the database,
    # but the reusult entry should be there:
    assert interface.count_tasks() == 0
    assert interface.count_results() == 1

    # check whether the worker has done the job:
    result = interface.get_result_by_uuid(task_result.uuid)
    assert result.is_ready is True
    assert result.result == expected_result


def test_handle_delayed_task_when_autocron_is_inactive(interface):
    """
    When autocron is inactive calling delayed decorated functions are
    returning a TaskResult object that is in ready state and holds the
    result.
    """
    arguments = (30, 12)
    expected_result = sum(arguments)

    # deactivate autocron:
    interface.autocron_lock_is_set = True
    assert interface.accept_registrations is False

    # call a delay decorated function and get the result right back:
    task_result = decorators.delay(tst_add)(30, 12)
    assert task_result.is_ready is True
    assert task_result.result == expected_result

    # and there is no uuid-attribute on the TaskResult because
    # the task was never registered:
    assert task_result.uuid == ""


def test_preregistration():
    """
    Register a task (here a cronjob) before the database has been setup.
    This can happen on importing modules with cron() decorated functions
    before autocron.start() has been called.
    """
    # create an uninitialized interface
    fixture = InterfaceFixture()

    # register two callables
    decorators.cron()(tst_cron)
    decorators.cron()(tst_func)

    # now do the initialization:
    fixture.set_up()
    interface = fixture.interface

    # give the registrator a bit time to do the job
    time.sleep(0.1)

    # the database should now have both functions registered:
    assert interface.count_tasks() == 2

    # manually tear down the fixture:
#     interface.task_registrator.stop()
    fixture.tear_down()
