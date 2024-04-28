"""
Scheduler for cron-tasks.

Takes a last schedule datetime-object and a crontab string and calcultes
the next time a command should get executed. The crontab format used has
five fields and a resolution of one minute
(https://en.wikipedia.org/wiki/Cron):

# ┌───────────── minute (0–59)
# │ ┌───────────── hour (0–23)
# │ │ ┌───────────── day of the month (1–31)
# │ │ │ ┌───────────── month (1–12)
# │ │ │ │ ┌───────────── day of the week (0–6) (Sunday to Saturday;
# │ │ │ │ │                                   7 is also Sunday on some systems)
# │ │ │ │ │
# │ │ │ │ │
# * * * * * <command to execute>

The fields are separated by at least a single whitespace. The fields
themselfes don't have whitespaces.

Every field can have the values: "*", "*/n" or "x,y,z".
Also a range "m-n" or a combination of list and
range like "a,b,m-n,c,p-q" is allowed.
"""

# this module is part of autocron
# copyright (c) 2024 Klaus Bremer
# all rights reserved
#
# license: MIT

import calendar
import datetime
import re
import types


CRONTAB_PARTS = ["minutes", "hours", "days", "months", "days_of_week"]
CRONTAB_MAX_VALUES = [59, 23, 31, 12, 6]
CRONTAB_MIN_VALUES = [0, 0, 1, 1, 0]
CRONTAB_SUBSTITUTE = re.compile(r"\[|\]|\s|_")

RE_REPEAT = re.compile(r"\*/(\d+)")
RE_SEQUENCE = re.compile(r"(\d+)-(\d+)")

DAYS_PER_WEEK = 7
MAX_SCHEDULE_ITERATIONS = 10_000
MAX_SCHEDULE_ITERATIONS_ERROR_MSG = """\
max schedule iteration ({}) exceeded. Date to far in the future.
Current date: {}"""


def get_next_value(value, values):
    """
    Returns the next value from values which is larger then value or
    None if there is no larger value. Assumes the values are in sorted
    order.
    """
    for item in values:
        if item > value:
            return item
    return None


def get_numeric_sequence(pattern, min_value, max_value):
    """
    Converts a pattern to a numeric sequence:
    * -> [0..max_value] stepwidth 1
    */n -> [0..max_value] stepwidth n
    m-n -> [m..n] stepwidth 1
    m,n -> [m,n]
    a-b,m,p-q -> [a..b,m,p..q] partial stepwidth 1
    """
    # handle the * case
    if pattern == "*":
        return list(range(min_value, max_value + 1))

    # handle the */n case
    if mo := RE_REPEAT.match(pattern):
        stepwidth = int(mo.group(1))
        return list(range(min_value, max_value + 1, stepwidth))

    # handle everything else
    values = []
    for element in pattern.split(","):
        if mo := RE_SEQUENCE.match(element):
            values.extend(list(range(int(mo.group(1)), int(mo.group(2)) + 1)))
        else:
            values.append(int(element))
    return sorted(set(values))


def get_cron_parts(crontab):
    """
    Returns a SimpleNamespace object with attribute-names given in
    CRONTAB_PARTS and values provided by get_numeric_sequence() for the
    given crontab parts. Example:

    >>> cp = get_cron_parts("* 5 2-4 11 *")
    >>> cp.hours
    [5]
    >>> cp.days
    [2, 3, 4]

    The other attributes are also lists with the according values.
    """
    data = {
        name: get_numeric_sequence(item, min_value, max_value)
        for name, item, min_value, max_value in zip(
            CRONTAB_PARTS,
            crontab.split(),
            CRONTAB_MIN_VALUES,
            CRONTAB_MAX_VALUES,
        )
    }
    return types.SimpleNamespace(**data)


def get_days_per_month(year=None, month=None, schedule=None):
    """
    Takes year and month and returns the number of days of the scheduled
    month. In case that schedule is given (as a datetime object), the
    year- and month-attributes from the schedule are used (shortcut
    function for better readability).
    """
    if schedule:
        year = schedule.year
        month = schedule.month
    _, days_per_month = calendar.monthrange(year, month)
    return days_per_month


def get_weekday(year=None, month=None, day=None, schedule=None):
    """
    Takes year, month and day and returns the weekday as an integer,
    where monday is 1 and sunday is 0. This is in sync with the used
    crontab but differs from the calender-module, where monday is 0 and
    sunday is 6. If schedule is given this is assumed to be a datetime
    object and in this case the other arguments are ignored.
    """
    if schedule is not None:
        year = schedule.year
        month = schedule.month
        day = schedule.day
    weekday = calendar.weekday(year, month, day) + 1
    return 0 if weekday > 6 else weekday


class CronScheduler:
    """
    Schedules a cron task.

    Usage:

    cs = CronScheduler()
    next_schedule = cs.get_next_schedule()
    """

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        crontab=None,
        minutes=None,
        hours=None,
        days=None,
        months=None,
        days_of_week=None,
        strict_mode=False,
    ):
        if not crontab:
            items = [
                str(item) if item else "*"
                for item in (minutes, hours, days, months, days_of_week)
            ]
            crontab = CRONTAB_SUBSTITUTE.sub(
                lambda mo: " " if mo.group() == "_" else "", "_".join(items)
            )
        self.cron_parts = get_cron_parts(crontab)
        self.strict_mode = strict_mode

    @property
    def all_weekdays_allowed(self):
        """
        Returns true if in the crontab all days of a week are allowed,
        i.e. the asterisk "*" is set.
        """
        return len(self.cron_parts.days_of_week) >= DAYS_PER_WEEK

    def get_next_schedule(self, previous_schedule=None):
        """
        Calculates the next schedule based on the current date or the
        given previous_schedule (a datetime-object). Returns a
        datetime-object.
        """
        dt = datetime.datetime
        if previous_schedule is None:
            previous_schedule = dt.now()

        year = previous_schedule.year
        month = previous_schedule.month
        day = previous_schedule.day
        hour = previous_schedule.hour
        minute = previous_schedule.minute

        minute = self.get_next_minute(minute)
        if minute is not None:
            return dt(year, month, day, hour, minute)

        minute = self.cron_parts.minutes[0]  # get first minute
        hour = self.get_next_hour(hour)
        if hour is not None:
            return dt(year, month, day, hour, minute)

        hour = self.cron_parts.hours[0]  # get first hour
        day = self.get_next_day(year, month, day)
        if day is not None:
            return dt(year, month, day, hour, minute)

        for counter in range(MAX_SCHEDULE_ITERATIONS):
            month = self.get_next_month(month)
            if month:
                day = self.get_first_day(year, month)
                if day is not None:
                    return dt(year, month, day, hour, minute)
                continue
            year += 1
            month = self.cron_parts.months[0]  # get first month
            day = self.get_first_day(year, month)
            if day is not None:
                return dt(year, month, day, hour, minute)

        # not returning from inside the loop is a potential
        # endless loop. So after MAX_SCHEDULE_ITERATIONS there
        # is a hard break here:
        schedule = dt(year, month, day, hour, minute)
        msg = MAX_SCHEDULE_ITERATIONS_ERROR_MSG.format(counter, schedule)
        raise ValueError(msg)

    def get_next_minute(self, minute):
        """
        Returns the next minute of configured minutes. Returns None if
        there is no next minute after the given one.
        """
        return get_next_value(minute, self.cron_parts.minutes)

    def get_next_hour(self, hour):
        """
        Returns the next hour of configured hours. Returns None if
        there is no next hour after the given one.
        """
        return get_next_value(hour, self.cron_parts.hours)

    def get_first_day(self, year, month):
        """
        Wrapper for get_next_day with day=0 to get the first allowed day
        of a month or None, if the month has no allwed days.
        """
        return self.get_next_day(year, month, day=0)

    def get_next_day(self, year, month, day):
        """
        Returns the next allowed day after the given day. Returns the
        day or None in case that there is no follow up day for the month
        and year.
        """
        # pylint:disable=too-many-return-statements
        next_day = get_next_value(day, self.cron_parts.days)

        if self.all_weekdays_allowed:
            # strict_mode doesn't matter
            if next_day is not None:
                if next_day <= get_days_per_month(year, month):
                    return next_day
            return None

        if self.strict_mode:
            if next_day is None:
                return None
            # check whether next_day is an allowed day of the
            # current month.
            if next_day > get_days_per_month(year, month):
                return None
            # the day must match one of the allowed weekdays
            weekday_of_next_day = get_weekday(year, month, next_day)
            if weekday_of_next_day in self.cron_parts.days_of_week:
                return next_day
            # recursion is save because there are just a few days per month
            return self.get_next_day(year, month, next_day)

        # no strict mode but days_of_week are defined:
        # the next day and the next allowed weekday are valid days,
        # return the one that comes first

        # `day` could be zero to get the first day of the month.
        # this is necessary for the first get_next_value() call
        # but will trigger a ValueError in the calendar module.
        # in this case set day to 1:

        if day == 0:
            day = 1
        weekday_of_day = get_weekday(year, month, day)
        next_weekday = get_next_value(
            weekday_of_day, self.cron_parts.days_of_week
        )
        if next_weekday is None:
            next_weekday = self.cron_parts.days_of_week[0] + DAYS_PER_WEEK
        delta = next_weekday - weekday_of_day
        next_weekday_day = day + delta

        if next_day is None:
            next_day = next_weekday_day
        else:
            next_day = min(next_day, next_weekday_day)
        if next_day <= get_days_per_month(year, month):
            return next_day
        return None

    def get_next_month(self, month):
        """
        Returns the next month of configured months. Returns None if
        there is no next month after the given one.
        """
        return get_next_value(month, self.cron_parts.months)
