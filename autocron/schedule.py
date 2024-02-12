"""
Scheduler for cron-tasks.

Takes a last schedule datetime-object and a crontab string and calcultes the next time a command should get executed. The crontab format used has five fields and a resolution of one minute (https://en.wikipedia.org/wiki/Cron):

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

import calendar
import datetime
import itertools
import re
import types


CRONTAB_PARTS = ["minutes", "hours", "days", "months", "days_of_week"]
CRONTAB_MAX_VALUES = [59, 23, 31, 12, 6]
CRONTAB_MIN_VALUES = [0, 0, 1, 1, 0]
CRONTAB_SUBSTITUTE = re.compile(r"\[|\]|\s|_")

RE_REPEAT = re.compile(r"\*/(\d+)")
RE_SEQUENCE = re.compile(r"(\d+)-(\d+)")

MINUTES_PER_HOUR = 60
HOURS_PER_DAY = 24
DAYS_PER_WEEK = 7
MAX_DAYS_PER_MONTH = 31
MAX_ADAPT_SCHEDULE_ITERATION = 10_000
MAX_ADAPT_SCHEDULE_ERROR_MSG = """\
max schedule iteration ({}) exceeded. Date to far in the future.
Current date: {}"""


def get_next_value(value, values):
    """
    Returns the next value from values which is larger then value or the
    first item from the sequence values.
    """
    for item in values:
        if item > value:
            return item
    return values[0]


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
            CRONTAB_MAX_VALUES
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


# ---------------------------------------------------
# refactor from here to methods:




# def get_next_minute(values, previous_schedule):
#     """
#     Calculates the next minute to execute a task based on the previous
#     schedule and the minute values of the parsed crontab. Return a tuple
#     with a delta_minute and the next_minute. delta minute is the
#     difference between the next_minute and the minute from the
#     previous_schedule.
#     """
#     previous_minute = previous_schedule.minute
#     next_minute = get_next_value(previous_minute, values)
#     delta_minutes = next_minute - previous_minute
#     if next_minute <= previous_minute:
#         delta_minutes += MINUTES_PER_HOUR
#     return delta_minutes, next_minute
#
#
# def get_next_hour(values, previous_schedule, next_minute):
#     """
#     Calculates the next hour to execute a task. If the next_minute is
#     larger than the minute from the previous_schedule then the hour
#     should not change. Otherwise increment the hour. Returns a tuple
#     with delta_hour and the next_hour.
#     """
#     previous_minute = previous_schedule.minute
#     previous_hour = previous_schedule.hour
#     if next_minute > previous_minute and previous_hour in values:
#         delta_hours = 0
#         next_hour = previous_hour
#     else:
#         next_hour = get_next_value(previous_hour, values)
#         delta_hours = next_hour - previous_hour
#         if next_hour <= previous_hour:
#             delta_hours += HOURS_PER_DAY
#     return delta_hours, next_hour
#
#
# def get_next_day(values, previous_schedule, next_hour):
#     """
#     Calculates the next day to execute a task. If the next_hour is
#     larger than the hour from the previous_schedule the the day should
#     not change. Otherwise increment the day. Furthermore check, whether
#     the incremented day is a valid day for the month of the previous
#     schedule. Returns a tuple with the delta_days and the next_day.
#     """
#     previous_hour = previous_schedule.hour
#     previous_day = previous_schedule.day
#     if next_hour > previous_hour and previous_day in values:
#         delta_days = 0
#         next_day = previous_day
#     else:
#         _, days_per_month = calendar.monthrange(
#             previous_schedule.year, previous_schedule.month
#         )
#         next_day = get_next_value(previous_day, values)
#         delta_days = next_day - previous_day
#         if next_day <= previous_day:
#             delta_days += days_per_month
#         if next_day > days_per_month:
#             next_day -= days_per_month
#     return delta_days, next_day
#
#
# def get_next_month_and_year(values, previous_schedule, next_day):
#     """
#     Calculates the next month to execute the task. If the next day is larger than the day from the previous_schedule the month should not change. Otherwise increment the month. Returns a tuple with the next year and the next month (as integer values).
#     """
#     next_year = previous_schedule.year
#     if next_day > previous_schedule.day and previous_schedule.month in values:
#         next_month = previous_schedule.month
#     else:
#         next_month = get_next_value(previous_schedule.month, values)
#         if next_month <= previous_schedule.month:
#             next_year += 1
#     return next_month, next_year
#
#
# def adapt_schedule_for_valid_day_of_week(values, schedule):
#     """
#     Checks whether the given schedule applys to the days of week given by values (0 to 6 for sunday to saturday). If this is not the case the
#     """
#
#
# def get_next_schedule(crontab, previous_schedule):
#     """
#     Takes a crontab string and the datetime-object of the previous
#     schedule. Calculates the next schedule and returns this also as a
#     datetime-object.
#     """
#     cron_parts = get_cron_parts(crontab)
#     delta_minutes, next_minute = get_next_minute(
#         cron_parts.minutes,
#         previous_schedule
#     )
#     delta_hours, next_hour = get_next_hour(
#         cron_parts.hours,
#         previous_schedule,
#         next_minute
#     )


# interface class for refactoring
class CronScheduler():

    def __init__(self,
                 crontab=None,
                 minutes=None,
                 hours=None,
                 days=None,
                 months=None,
                 days_of_week=None,
                 strict_mode=False):
        if not crontab:
            items = [str(item) if item else "*"
                     for item in (minutes, hours, days, months, days_of_week)]
            crontab = CRONTAB_SUBSTITUTE.sub(
                lambda mo: " " if mo.group() == "_" else "",
                "_".join(items)
            )
        self.cron_parts = get_cron_parts(crontab)
        self.strict_mode = strict_mode
        self.previous_schedule = None

    @property
    def all_days_allowed(self):
        """
        Returns a boolean if all days in a month are allowed. By default
        these are 31 days represented by "*". The correct number of days
        for a given month is calculated elsewhere. So it is not
        necessary to provide a list like "[1..28]" for February – indeed
        this would be an error.
        """
        return len(self.cron_parts.days) >= MAX_DAYS_PER_MONTH

    @property
    def all_weekdays_allowed(self):
        """
        Returns a boolean whether all weekdays are allowed (corresponds
        to the * or 0-6 in the crontab). In this case return True,
        otherwise False.
        """
        return len(self.cron_parts.days_of_week) >= DAYS_PER_WEEK

    def get_next_schedule(self, previous_schedule=None):
        """
        Calculates the next schedule based on the current date or the
        given previous_schedule (a datetime-object). Returns a
        datetime-object.
        """
        if previous_schedule is None:
            previous_schedule = datetime.datetime.now()
        self.previous_schedule = previous_schedule
        next_minute = self.get_next_minute()

    def get_next_minute(self):
        """
        Calculates the next minute to execute a task based on the
        previous schedule and the minute values of the parsed crontab.
        Return the next_minute.
        """
        return get_next_value(
            self.previous_schedule.minute, self.cron_parts.minutes)

    def get_next_hour(self, next_minute):
        """
        Calculates the next hour to execute a task. If the next_minute
        is larger than the minute from the previous_schedule then the
        hour should not change. Otherwise increment the hour. Returns
        the next hour.
        """
        prev = self.previous_schedule
        cron = self.cron_parts
        if next_minute > prev.minute and prev.hour in cron.hours:
            return prev.hour
        return get_next_value(prev.hour, cron.hours)


    def x_get_next_day(self, next_hour, schedule=None):
        """
        Calculates the next day to execute a task. If the next_hour is
        larger than the hour from the previous_schedule the day should
        not change. Otherwise increment the day. Furthermore check, whether
        the incremented day is a valid day for the month of the previous
        schedule. Returns the next_day.
        """
        prev =  schedule if schedule else self.previous_schedule
        cron = self.cron_parts
        if next_hour > prev.hour and prev.day in cron.days:
            return prev.day

        # get next day and check for valid day in month (i.e no yy/02/30)
        days_per_month = get_days_per_month(schedule=prev)
        next_day = get_next_value(prev.day, cron.days)
        if next_day > days_per_month:
            next_day -= days_per_month
        return next_day

    def get_next_day(self, next_hour, schedule=None):
        """
        Calculates the next day to execute a task. If the next_hour is
        larger than the hour from the previous_schedule the day should
        not change. Otherwise increment the day. If restriced_mode is not set (default) check if also weekdays are given. If one of the given weekdays
        Furthermore check, whether
        the incremented day is a valid day for the month of the previous
        schedule. Returns the next_day.
        """
        prev =  schedule if schedule else self.previous_schedule
        cron = self.cron_parts

        def get_next_day_from_days():
            # get next day from list of days:
            if next_hour > prev.hour and prev.day in cron.days:
                return prev.day
            return get_next_value(prev.day, cron.days)

        def get_next_day_from_weekdays():
            # get next day fromlist of weekdays:
            this_weekday = get_weekday(schedule=prev)
            next_weekday = get_next_value(this_weekday, cron.days_of_week)
            delta = (
                next_weekday - this_weekday + DAYS_PER_WEEK
            ) % DAYS_PER_WEEK
            return prev.day + delta

        if self.all_weekdays_allowed:
            next_day = get_next_day_from_days()
        else:
            if self.all_days_allowed:
                next_day = get_next_day_from_weekdays()
            else:
                if self.strict_mode:
                    # check the day in a later step
                    next_day = get_next_day_from_days()
                else:
                    next_day = min(
                        get_next_day_from_days(),
                        get_next_day_from_weekdays()
                    )

        # check for valid day in month (i.e no yy/02/30)
        days_per_month = get_days_per_month(schedule=prev)
        if next_day > days_per_month:
            next_day -= days_per_month
        return next_day

    def get_next_month_and_year(self, next_day, schedule=None):
        """
        Calculates the next month to execute the task. If the next day
        is larger than the day from the previous_schedule the month
        should not change. Otherwise increment the month. Returns a
        tuple with the next year and the next month (as integer values).
        """
        prev =  schedule if schedule else self.previous_schedule
        cron = self.cron_parts
        next_year = prev.year
        prev_month = prev.month
        if next_day > prev.day and prev_month in cron.months:
            # the next_day provided as argument is in the allowed
            # range of days of the current month (-> prev.month)
            next_month = prev_month
        else:
            for n in range(MAX_ADAPT_SCHEDULE_ITERATION):
                next_month = get_next_value(prev_month, cron.months)
                if next_month <= prev_month:
                    next_year += 1
                # check whether the new month has enough days:
                days_per_month = get_days_per_month(next_year, next_month)
                if next_day > days_per_month:
                    # invalid month: this can happen if the next day is in
                    # the range 29-31, but the next month has fewer days.
                    # in this case the next day is the first of the allowed
                    # days and the next allowed month has to be found.
                    next_day = cron.days[0]
                    prev_month = next_month
                else:
                    # valid month and year found
                    break
            else:
                # range exceeded: even if this is unlikely do a hard break
                # for not running into an endless loop:
                schedule = datetime.datetime(next_year, next_month, next_day)
                msg = MAX_ADAPT_SCHEDULE_ERROR_MSG.format(n, schedule)
                raise ValueError(msg)
        return next_month, next_year

    def get_adapted_schedule_for_valid_day_of_week(self, schedule):
        """
        Checks whether the given schedule applys to the days of week
        given by values (0 to 6 for sunday to saturday). If this is not
        the case the next day to execute the task is calculated and
        tested again until a match is found. If the number of tests
        exceed the MAX_ADAPT_SCHEDULE_ITERATION limit a ValueError is
        raised. On success returns the new schedule as a datetime
        object. (This method must get called in restricted mode, but not
        otherwise.)
        """
        for n in itertools.count():
            if n > MAX_ADAPT_SCHEDULE_ITERATION:
                msg = MAX_ADAPT_SCHEDULE_ERROR_MSG.format(n, schedule)
                raise ValueError(msg)
            weekday = calendar.weekday(
                schedule.year,
                schedule.month,
                schedule.day
            )
            if weekday not in self.cron_parts.days_of_week:
                next_day = self.get_next_day(
                    next_hour=schedule.hour,
                    schedule=schedule
                )
                next_month, next_year = self.get_next_month_and_year(
                    next_day,
                    schedule=schedule
                )
                schedule = datetime.datetime(
                    next_year,
                    next_month,
                    next_day,
                    schedule.hour,
                    schedule.minute
                )
            else:
                return schedule
