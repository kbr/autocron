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

Every field can have the values: "*", "*/n" or "x,y,z"


"""

import calendar
import re
import types


CRONTAB_PARTS = ["minutes", "hours", "days", "months", "days_of_week"]
CRONTAB_MAX_VALUES = [59, 23, 31, 12, 6]
CRONTAB_MIN_VALUES = [0, 0, 1, 1, 0]

MINUTES_PER_HOUR = 60
HOURS_PER_DAY = 24

RE_REPEAT = re.compile(r"\*/(\d+)")
RE_SEQUENCE = re.compile(r"(\d+)-(\d+)")


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
    pattern = pattern.strip()
    if pattern == "*":
        return list(range(min_value, max_value + 1))
    if mo := RE_REPEAT.match(pattern):
        stepwidth = int(mo.group(1))
        return list(range(min_value, max_value + 1, stepwidth))
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


def get_next_value(value, values):
    """
    Returns the next value from values which is larger then value or the
    first item from the sequence values.
    """
    for item in values:
        if item > value:
            return item
    return values[0]


def get_next_minute(values, previous_schedule):
    """
    Calculates the next minute to execute a task based on the previous
    schedule and the minute values of the parsed crontab. Return a tuple
    with a delta_minute and the next_minute. delta minute is the
    difference between the next_minute and the minute from the
    previous_schedule.
    """
    previous_minute = previous_schedule.minute
    next_minute = get_next_value(previous_minute, values)
    delta_minutes = next_minute - previous_minute
    if next_minute <= previous_minute:
        delta_minutes += MINUTES_PER_HOUR
    return delta_minutes, next_minute


def get_next_hour(values, previous_schedule, next_minute):
    """
    Calculates the next hour to execute a task. If the next_minute is
    larger than the minute from the previous_schedule then the hour
    should not change. Otherwise increment the hour. Returns a tuple
    with delta_hour and the next_hour.
    """
    previous_minute = previous_schedule.minute
    previous_hour = previous_schedule.hour
    if next_minute > previous_minute and previous_hour in values:
        delta_hours = 0
        next_hour = previous_hour
    else:
        next_hour = get_next_value(previous_hour, values)
        delta_hours = next_hour - previous_hour
        if next_hour <= previous_hour:
            delta_hours += HOURS_PER_DAY
    return delta_hours, next_hour


def get_next_day(values, previous_schedule, next_hour):
    """
    Calculates the next day to execute a task. If the next_hour is
    larger than the hour from the previous_schedule the the day should
    not change. Otherwise increment the day. Furthermore check, whether
    the incremented day is a valid day for the month of the previous
    schedule. Returns a tuple with the delta_days and the next_day.
    """
    previous_hour = previous_schedule.hour
    previous_day = previous_schedule.day
    if next_hour > previous_hour and previous_day in values:
        delta_days = 0
        next_day = previous_day
    else:
        _, days_per_month = calendar.monthrange(
            previous_schedule.year, previous_schedule.month
        )
        next_day = get_next_value(previous_day, values)
        delta_days = next_day - previous_day
        if next_day <= previous_day:
            delta_days += days_per_month
        if next_day > days_per_month:
            next_day -= days_per_month
    return delta_days, next_day


def get_next_month(values, previous_schedule, next_day):
    """
    Calculates the next month to execute the task. If the next day is larger than the day from the previous_schedule the month should not change. Otherwise increment the month. Returns a tuple with
    """


def get_next_schedule(crontab, previous_schedule):
    """
    Takes a crontab string and the datetime-object of the previous
    schedule. Calculates the next schedule and returns this also as a
    datetime-object.
    """
    cron_parts = get_cron_parts(crontab)
    delta_minutes, next_minute = get_next_minute(
        cron_parts.minutes,
        previous_schedule
    )
    delta_hours, next_hour = get_next_hour(
        cron_parts.hours,
        previous_schedule,
        next_minute
    )

class CronScheduler():
    pass
