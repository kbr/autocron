
from datetime import datetime as dt

import pytest

from autocron.schedule import (
    get_cron_parts,
    get_next_value,
    get_numeric_sequence,
    get_weekday,
    CronScheduler,
)


@pytest.mark.parametrize(
    'value, values, expected_result', [
        (2, [5, 10, 15], 5),
        (5, [5, 10, 15], 10),
        (10, [5, 10, 15], 15),
        (15, [5, 10, 15], None),
        (20, [5, 10, 15], None),
        (2, [7], 7),
        (7, [7], None),
        (9, [7], None),
    ])
def test_get_next_value(value, values, expected_result):
    """
    Check to get the next element from a sequence of values larger than
    a given value. If there is no larger value, None gets returned
    """
    result = get_next_value(value, values)
    assert result == expected_result


@pytest.mark.parametrize(
    "pattern, min_value, max_value, expected_result", [
        ("*", 0, 5, list(range(6))),
        ("*/2", 0, 6, list(range(0, 7, 2))),
        ("*/15", 0, 59, list(range(0, 60, 15))),
        ("12,5,30", 0, 59, [5, 12, 30]),
        ("10-20", 0, 59, list(range(10, 21))),
        ("2,4-8,22", 0, 59, [2]+list(range(4, 9))+[22]),
        ("20-30,8,6,4,10-19",
            0, 59, list(range(4, 9, 2)) + list(range(10, 31))),
        ("*", 1, 30, list(range(1, 31))),
        ("*/2", 0, 10, list(range(0, 11, 2))),
        ("*/2", 1, 10, list(range(1, 11, 2))),
    ]
)
def test_get_numeric_sequence(pattern, min_value, max_value, expected_result):
    """
    Test to convert a pattern like "*", "*/n" etc. to a sequence of
    numeric values.
    """
    result = get_numeric_sequence(pattern, min_value, max_value)
    assert result == expected_result


def test_get_cron_parts():
    """
    Test the crontab parsing into list of values.
    """
    crontab = "2,3-5 * 2-4 */4 */2"
    cp = get_cron_parts(crontab)
    assert cp.minutes == [2, 3, 4, 5]
    assert cp.hours == list(range(24))
    assert cp.days == [2, 3, 4]
    assert cp.months == [1, 5, 9]
    assert cp.days_of_week == [0, 2, 4, 6]


def test_cronscheduler_init():
    """
    Test if the CronScheduler converts the init-data according to
    test_get_cron_parts in case that the keyword-arguments minutes,
    hours etc. are given.
    """
    def check_cp():
        cp = cs.cron_parts
        assert cp.minutes == [2, 3, 4, 5]
        assert cp.hours == list(range(24))
        assert cp.days == [2, 3, 4]
        assert cp.months == [1, 5, 9]
        assert cp.days_of_week == [0, 2, 4, 6]

    # test with crontab
    crontab = "2,3-5 * 2-4 */4 */2"
    cs = CronScheduler(crontab)
    check_cp()

    # test with keyword arguments
    cs = CronScheduler(
        minutes=[2, 3, 4, 5],
        days=[2, 3, 4],
        months=[1, 5, 9],
        days_of_week=[0, 2, 4, 6]
    )
    check_cp()


@pytest.mark.parametrize(
    'schedule, expected_result', [
        (dt(2024, 2, 12), 1),  # this is a monday
        (dt(2024, 2, 11), 0),  # this is a sunday
    ]
)
def test_get_weekday(schedule, expected_result):
    result = get_weekday(schedule=schedule)
    assert result == expected_result


@pytest.mark.parametrize(
    'crontab, minute, expected_result', [
        ("10,20 * * * *", 5, 10),
        ("10,20 * * * *", 15, 20),
        ("10,20 * * * *", 20, None),
        ("10,20 * * * *", 25, None),
    ]
)
def test_get_next_minute(crontab, minute, expected_result):
    cs = CronScheduler(crontab)
    result = cs.get_next_minute(minute)
    assert result == expected_result


@pytest.mark.parametrize(
    'crontab, hour, expected_result', [
        ("* 10,20 * * *", 5, 10),
        ("* 10,20 * * *", 15, 20),
        ("* 10,20 * * *", 20, None),
        ("* 10,20 * * *", 23, None),
    ]
)
def test_get_next_hour(crontab, hour, expected_result):
    cs = CronScheduler(crontab)
    result = cs.get_next_hour(hour)
    assert result == expected_result


@pytest.mark.parametrize(
    'crontab, month, expected_result', [
        ("* * * * *", 5, 6),
        ("* * * 7 *", 5, 7),
        ("* * * 7,12 *", 7, 12),
        ("* * * 7 *", 7, None),
        ("* * * */2 *", 3, 5),
        ("* * * */2 *", 11, None),
    ]
)
def test_get_next_month(crontab, month, expected_result):
    cs = CronScheduler(crontab)
    result = cs.get_next_month(month)
    assert result == expected_result


@pytest.mark.parametrize(
    'crontab, year, month, strict_mode, expected_result', [
        ("* * * * *", 2024, 2, False, 1),
        ("* * 10 * *", 2024, 2, False, 10),
        ("* * 10 * 1", 2024, 2, False, 5),
        ("* * 10 * 6", 2024, 2, True, 10),
        ("* * 10 * 5", 2024, 2, True, None),
    ]
)
def test_get_first_day(crontab, year, month, strict_mode, expected_result):
    cs = CronScheduler(crontab, strict_mode=strict_mode)
    result = cs.get_first_day(year, month)
    assert result == expected_result


@pytest.mark.parametrize(
    'crontab, year, month, day, strict_mode, expected_result', [
        ("* * * * *", 2024, 2, 2, False, 3),
        ("* * * * *", 2024, 2, 28, False, 29),
        ("* * * * *", 2025, 2, 28, False, None),
        ("* * 10,20 * *", 2024, 2, 5, False, 10),
        ("* * 10,20 * *", 2024, 2, 10, False, 20),
        ("* * 10,20 * *", 2024, 2, 22, False, None),
        ("* * 10,20 * 1", 2024, 2, 28, False, None),
        ("* * 10,20 * 2,5", 2024, 2, 2, False, 6),
        ("* * 10,20 * 2,5", 2024, 2, 14, False, 16),
        ("* * 10,20 * 2,5", 2024, 2, 23, False, 27),
        ("* * 10,20 * 2,5", 2024, 2, 2, True, 20),
        ("* * 10,21 * 3", 2024, 2, 2, True, 21),
        ("* * 10,22 * 3", 2024, 2, 2, True, None),
    ]
)
def test_get_next_day(crontab, year, month, day, strict_mode, expected_result):
    cs = CronScheduler(crontab, strict_mode=strict_mode)
    result = cs.get_next_day(year, month, day)
    assert result == expected_result


@pytest.mark.parametrize(
    'crontab, previous_schedule, strict_mode, expected_result', [
        ("* * * * *", dt(2024, 2, 8, 10, 0), False, dt(2024, 2, 8, 10, 1)),
        ("*/5 * * * *", dt(2024, 2, 8, 10, 24), False, dt(2024, 2, 8, 10, 25)),
        ("*/5 * * * *", dt(2024, 2, 8, 10, 25), False, dt(2024, 2, 8, 10, 30)),
        ("0,30 * * * *", dt(2024, 2, 8, 10, 25), False, dt(2024, 2, 8, 10, 30)),
        ("0,30 * * * *", dt(2024, 2, 8, 10, 30), False, dt(2024, 2, 8, 11, 0)),
        ("0,30 5,17 * * *", dt(2024, 2, 8, 10, 30), False, dt(2024, 2, 8, 17, 0)),
        ("0,30 5,17 * * *", dt(2024, 2, 8, 17, 30), False, dt(2024, 2, 9, 5, 0)),
        ("30 13 * * 5", dt(2024, 2, 8, 17, 30), False, dt(2024, 2, 9, 13, 30)),
        ("30 13 * * 5", dt(2024, 2, 9, 13, 30), False, dt(2024, 2, 10, 13, 30)),
        ("30 13 * * 5", dt(2024, 2, 9, 13, 30), True, dt(2024, 2, 16, 13, 30)),
        ("30 13 * * 5", dt(2024, 2, 16, 13, 30), True, dt(2024, 2, 23, 13, 30)),
        ("30 13 * * 5", dt(2024, 2, 23, 13, 30), True, dt(2024, 3, 1, 13, 30)),
        ("30 13 29 2 *", dt(2024, 2, 27, 13, 30), False, dt(2024, 2, 29, 13, 30)),
        ("30 13 29 2 *", dt(2024, 2, 29, 13, 30), False, dt(2028, 2, 29, 13, 30)),
        ("30 13 29 2 0", dt(2024, 2, 29, 13, 30), False, dt(2025, 2, 2, 13, 30)),
        ("30 13 29 2 0", dt(2024, 2, 29, 13, 30), True, dt(2032, 2, 29, 13, 30)),
        ("30 13 7 * 3", dt(2024, 2, 7, 13, 30), True, dt(2024, 8, 7, 13, 30)),
        ("30 13 7 2 3", dt(2024, 2, 7, 13, 30), True, dt(2029, 2, 7, 13, 30)),
    ]
)
def test_get_next_schedule(crontab,
                           previous_schedule,
                           strict_mode,
                           expected_result):
    cs = CronScheduler(crontab, strict_mode=strict_mode)
    result = cs.get_next_schedule(previous_schedule)
    assert result == expected_result
