"""
Implementation of the decorators ``cron`` and ``delay`` for running
recurring task and to delegate long running tasks to a background
process.
"""

import functools
import uuid

from .schedule import CronScheduler
from .sqlite_interface import (
    TASK_STATUS_WAITING,
    TASK_STATUS_READY,
    TASK_STATUS_ERROR,
    Result,
    SQLiteInterface
)


# default: run every minute:
DEFAULT_CRONTAB = "* * * * *"

interface = SQLiteInterface()


# pylint: disable=too-many-arguments
def cron(crontab=None,
         minutes=None,
         hours=None,
         days=None,
         months=None,
         days_of_week=None):
    """
    Decorator for a cronjob. Functions running cronjobs should not get
    called from the main program and therefore don't get arguments.
    Example usage for a cronjob to run every hour, at the beginning of
    the hour:

    >>> @cron("0 * * * *")
    >>> def some_callable():
    >>>    # do periodic stuff here ...

    The decorator can take a couple of arguments but if just the first
    argument `crontab` is given then all other arguments are ignored. To
    use the other arguments instead, provide them all as
    keyword-arguments. If no arguments are given the default-crontab
    ``(* * * * *)`` is used to execute a task every minute.

    :crontab:
        a string representing a valid crontab. See:
        `https://en.wikipedia.org/wiki/Cron#CRON_expression
        <https://en.wikipedia.org/wiki/Cron#CRON_expression>`_ with the
        restriction that only integers and the special signs (* , -) are
        allowed. Some examples ::

            The order of arguments is:
            'minutes hours dow months dom'

            '* * * * *':       runs every minute
                               (same as @periodic_task(seconds=60))
            '15,30 7 * * *':   runs every day at 7:15 and 7:30
            '* 9 0 4,7 10-15': runs at 9:00 every monday and
                               from the 10th to the 15th of a month
                               but only in April and July.


    :minutes:
        list of minutes during an hour when the task should run. Valid
        entries are integers in the range 0-59. Defaults to None which
        is the same as ``*`` in a crontab, meaning that the task gets
        executed every minute.

    :hours:
        list of hours during a day when the task should run. Valid
        entries are integers in the range 0-23. Defaults to None which
        is the same as ``*`` in a crontab, meaning that the task gets
        executed every hour.

    :days:
        list of days in an month the task should run. Valid entries are
        integers in the range 1-31. Defaults to None which is the same
        as ``*`` in a crontab, meaning that the task gets executed every
        day.

    :months:
        list of month during a year when the task should run. Valid
        entries are integers in the range 1-12. Defaults to None which
        is the same as ``*`` in a crontab, meaning that the task gets
        executed every month.

    :days_of_week:
        days of week. A list of integers from 0 to 6 with Monday as 0.
        The task runs only on the given weekdays. Defaults to None which
        is the same as ``*`` in a crontab, meaning that the task gets
        executed every day of the week.

    If neither *days* nor *days_of_week* are given, then the task will
    run every day of a month. If one of both is set, then the given
    restrictions apply. If both are set, then the allowed days
    complement each other.
    """
    # set crontab to default if no other arguments are given:
    if not any(locals().values()):
        crontab = DEFAULT_CRONTAB

    def wrapper(func):
        # send the function to the registerer. The contas will get registered
        # when autocron starts. If autocron is not active nothing bad happens.
        scheduler = CronScheduler(
            minutes=minutes,
            hours=hours,
            days=days,
            months=months,
            days_of_week=days_of_week,
            crontab=crontab
        )
        schedule = scheduler.get_next_schedule()
        interface.registrator.register(
            func,
            schedule=schedule,
            crontab=crontab
        )
        return func
    return wrapper


def delay(func):
    """
    Decorator for a delayed task. Apply this as:

    >>> @delay
    >>> def sendmail(recipient, message):
    >>>     # code goes here ...

    The decorator does not take any arguments. Calling ``sendmail()``
    will return from the call immediately and this callable will get
    executed later in another process.

    The decorated function will return a Result-instance. If autocron is
    active the result will be in waiting mode and may not be in the
    database because the dataset gets created in a separate thread. If
    autocron is not active, the result-instance will be in ready- or
    error-mode, depending on the function call.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # the wrapper will not get called during import time.
        # at runtime the database is initialized and it is safe
        # to check the settings:
        if not interface.accept_registrations:
            # this is the case when the decorated function gets called
            # in a worker process. In this case the wrapper returns the
            # result from the function call and not a Result instance,
            # because the Result instance is already existing and just
            # updated by the worker.
            # (also the error handling is done by the worker.)
            return func(*args, **kwargs)

        # in the 'main' process autocron may be active or not:
        if interface.autocron_lock:
            # inactive: call the function and return a Result-instance
            # in ready- or error-state:
            try:
                function_result = func(*args, **kwargs)
            except Exception as err:  # pylint: disable=broad-exception-caught
                error_message = str(err)
                status = TASK_STATUS_ERROR
                function_result = None
            else:
                error_message = ""
                status = TASK_STATUS_READY
            result = Result.from_registration(
                func, args, kwargs, status=status,
                function_result=function_result,
                error_message=error_message
            )
        else:
            # active: register in task_queue and return a Result-instance
            # in waiting-state:
            uuid_ = uuid.uuid4().hex
            interface.registrator.register(
                func, args=args, kwargs=kwargs, uuid=uuid_
            )
            result = Result.from_registration(
                func, args, kwargs, uuid=uuid_, status=TASK_STATUS_WAITING
            )
        return result
    return wrapper
