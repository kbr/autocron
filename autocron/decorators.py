"""
decorators.py

For cron jobs and delayed tasks.
"""

import uuid

from .configuration import configuration
from .schedule import CronScheduler
from .sql_interface import interface


# run every minute:
DEFAULT_CRONTAB = "* * * * *"


# pylint: disable=too-many-arguments
def cron(crontab=None,
         minutes=None,
         hours=None,
         dow=None,
         months=None,
         dom=None):
    """
    Decorator function for a cronjob.

    Functions running cronjobs should not get called from the main
    program and therefore don't get attributes. Usage for a cronjob to
    run every hour:

        @cron("* 1 * * *")
        def some_callable():
            # do periodic stuff here ...

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
    Decorator for a delayed task
    """
    def wrapper(*args, **kwargs):
        if configuration.is_active:
            uid = uuid.uuid4().hex
            data = {"args": args, "kwargs": kwargs, "uuid": uid}
            interface.register_callable(func, **data)
            interface.register_result(func, **data)
            return uid
        return func(*args, **kwargs)
    return wrapper
