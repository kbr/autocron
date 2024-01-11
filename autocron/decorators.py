"""
Implementatiom of the decorators ``cron`` and ``delay`` for running
recurring task and to delegate long running tasks to a background
process.
"""

import uuid

from .schedule import CronScheduler
from .sql_interface import SQLiteInterface, TaskResult


# default: run every minute:
DEFAULT_CRONTAB = "* * * * *"

interface = SQLiteInterface()


# pylint: disable=too-many-arguments
def cron(crontab=None,
         minutes=None,
         hours=None,
         dow=None,
         months=None,
         dom=None):
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

    :dow:
        days of week. A list of integers from 0 to 6 with Monday as 0.
        The task runs only on the given weekdays. Defaults to None which
        is the same as ``*`` in a crontab, meaning that the task gets
        executed every day of the week.

    :months:
        list of month during a year when the task should run. Valid
        entries are integers in the range 1-12. Defaults to None which
        is the same as ``*`` in a crontab, meaning that the task gets
        executed every month.

    :dom:
        list of days in an month the task should run. Valid entries are
        integers in the range 1-31. Defaults to None which is the same
        as ``*`` in a crontab, meaning that the task gets executed every
        day.

    If neither *dom* nor *dow* are given, then the task will run every
    day of a month. If one of both is set, then the given restrictions
    apply. If both are set, then the allowed days complement each other.
    """
    # set crontab to default if no other arguments are given:
    if not any(locals().values()):
        crontab = DEFAULT_CRONTAB

    def wrapper(func):
        scheduler = CronScheduler(
            minutes=minutes,
            hours=hours,
            dow=dow,
            months=months,
            dom=dom,
            crontab=crontab
        )
        schedule = scheduler.get_next_schedule()
        interface.register_callable(
            func,
            schedule=schedule,
            crontab=crontab,
            unique=True  # don't register cron-tasks twice
        )
        return func

    return wrapper


def delay(func):
    """
    Decorator for a delayed task. Apply this as:

    >>> @delay
    >>> def sendmail(recipient, message):
    >>>     # conde goes here ...

    The decorator does not take any arguments. Calling ``sendmail()``
    will return from the call immediately and this callable will get
    executed later in another process.
    """

    def wrapper(*args, **kwargs):
        if interface.accept_registrations:
            # active: return TaskResult in waiting state
            uid = uuid.uuid4().hex
            data = {"args": args, "kwargs": kwargs, "uuid": uid}
            interface.register_callable(func, **data)
            result = TaskResult.from_registration(uid, interface)
        elif interface.autocron_lock_is_set:
            # inactive: return TaskResult in ready state
            # by calling the wrapped function. This will keep the
            # type of the returned value consistant for the application.
            result = TaskResult.from_function_call(func, *args, **kwargs)
        else:
            # active but registration not allowed.
            # this is the wrapper as seen by a worker process:
            # return result from the original callable
            result = func(*args, **kwargs)
        return result

    return wrapper
