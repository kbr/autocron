
from datetime import datetime as dt

import pytest

from autocron.schedule import (
    get_cron_parts,
#     get_next_day,
#     get_next_hour,
#     get_next_minute,
#     get_next_month_and_year,
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
        (15, [5, 10, 15], 5),
        (20, [5, 10, 15], 5),
        (2, [7], 7),
        (7, [7], 7),
        (9, [7], 7),
    ])
def test_get_next_value(value, values, expected_result):
    """
    Check to get the next element from a sequence of values larger than
    a given value. If there is no larger value, the first element from
    the sequence should get returned.
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
    'crontab, previous_schedule, expected_result', [
        ("10,20,25 * * * *", dt(2024, 2, 8, 2, 0), 10),
        ("10,20,25 * * * *", dt(2024, 2, 8, 2, 10), 20),
        ("10,20,25 * * * *", dt(2024, 2, 8, 2, 20), 25),
        ("10,20,25 * * * *", dt(2024, 2, 8, 2, 25), 10),
        ("10,20,25 * * * *", dt(2024, 2, 8, 2, 55), 10),
    ]
)
def test_get_next_minute(crontab, previous_schedule, expected_result):
    """
    Test to get the next minute from the crontab list depending on the
    previous schedule.
    """
    cs = CronScheduler(crontab)
    cs.previous_schedule = previous_schedule
    result = cs.get_next_minute()
    assert result == expected_result


@pytest.mark.parametrize(
    'crontab, previous_schedule, next_minute, expected_result', [
        ("* 10,12 * * *", dt(2024, 2, 8, 10, 30), 40, 10),
        ("* 10,12 * * *", dt(2024, 2, 8, 10, 40), 20, 12),
        ("* 10,12 * * *", dt(2024, 2, 8, 12, 20), 10, 10),
        ("* 10,12 * * *", dt(2024, 2, 8, 2, 20), 10, 10),
        ("* 10,12 * * *", dt(2024, 2, 8, 2, 20), 30, 10),
        ("* 10,12 * * *", dt(2024, 2, 8, 11, 20), 30, 12),
    ]
)
def test_get_next_hour(crontab, previous_schedule, next_minute,
                       expected_result):
    """
    Test to get the next hour from the crontab list depending on the
    previous schedule and the calculated next minute.
    """
    cs = CronScheduler(crontab)
    cs.previous_schedule = previous_schedule
    result = cs.get_next_hour(next_minute)
    assert result == expected_result


@pytest.mark.parametrize(
    'crontab, previous_schedule, next_hour, expected_result', [
        ("* * * * *", dt(2024, 2, 8, 10, 0), 11, 8),
        ("* * 10,12 * *", dt(2024, 2, 8, 10, 0), 11, 10),
        ("* * 10,12 * *", dt(2024, 2, 10, 10, 0), 11, 10),
        ("* * 10,12 * *", dt(2024, 2, 10, 10, 0), 5, 12),
        ("* * 10,12 * *", dt(2024, 2, 10, 10, 0), 10, 12),
        ("* * 10,12 * *", dt(2024, 2, 11, 10, 0), 10, 12),
        ("* * 10,12 * *", dt(2024, 2, 11, 10, 0), 14, 12),
        ("* * 10,12 * *", dt(2024, 2, 12, 10, 0), 8, 10),
        ("* * * * 1,4", dt(2024, 2, 8, 10, 0), 8, 12),
        ("* * * * 1,4", dt(2024, 2, 12, 10, 0), 8, 15),
        ("* * 6,13 * 1,4", dt(2024, 2, 12, 10, 0), 8, 13),
        ("* * 6,16 * 1,4", dt(2024, 2, 12, 10, 0), 8, 15),
    ]
)
def test_get_next_day(crontab, previous_schedule, next_hour, expected_result):
    """
    Test to get the next day from the crontab list depending on the
    previous schedule and the calculated next hour.
    """
    cs = CronScheduler(crontab)
    cs.previous_schedule = previous_schedule
    result = cs.get_next_day(next_hour)
    assert result == expected_result


@pytest.mark.parametrize(
    'crontab, previous_schedule, next_day, expected_result', [
        ("* * * 2,6,10 *", dt(2024, 2, 8), 10, (2, 2024)),
        ("* * * 2,6,10 *", dt(2024, 2, 8), 4, (6, 2024)),
        ("* * * 2,6,10 *", dt(2024, 6, 8), 4, (10, 2024)),
        ("* * * 2,6,10 *", dt(2024, 10, 8), 4, (2, 2025)),
        ("* * * 2,6,10 *", dt(2024, 11, 8), 2, (2, 2025)),
        ("* * * 2,6,10 *", dt(2024, 11, 8), 12, (2, 2025)),
        ("* * * 1,2,3 *", dt(2024, 1, 31), 30, (3, 2024)),
        ("* * * 2 *", dt(2024, 2, 29), 29, (3, 2024)),
    ]
)
def x_test_get_next_month_and_year(crontab,
                                 previous_schedule,
                                 next_day,
                                 expected_result):
    """
    Test to get the next month and year for a given crontab and previous
    schedule.
    """
    cs = CronScheduler(crontab)
    cs.previous_schedule = previous_schedule
    result = cs.get_next_month_and_year(next_day)
    assert result == expected_result





@pytest.mark.parametrize(
    'crontab, schedule, expected_schedule', [
        ("* * * * *", dt(2024, 2, 8), dt(2024, 2, 8)),
        ("* * * * 3", dt(2024, 2, 8), dt(2024, 2, 8)),
        ("* * * * */3", dt(2024, 2, 8), dt(2024, 2, 8)),
        ("* * 8,10 * 2", dt(2024, 2, 8), dt(2024, 4, 10)),
        ("* * 13 * 4", dt(2024, 2, 13), dt(2024, 9, 13)),
        ("* * 13 11 4", dt(2024, 2, 13), dt(2026, 11, 13)),
        ("* * 8,15 2,7,8 2", dt(2024, 2, 8), dt(2026, 7, 8)),
        ("* * 29 2 *", dt(2024, 2, 29), dt(2024, 2, 29)),
#         ("* * 29 2 1", dt(2024, 2, 29), dt(2028, 2, 29)),
    ]
)
def x_test_get_adapted_schedule_for_valid_day_of_week(crontab,
                                                    schedule,
                                                    expected_schedule):
    """
    Test to get the next schedule depending on the day_of_week in the
    crontab.
    """
    cs = CronScheduler(crontab)
    new_schedule = cs.get_adapted_schedule_for_valid_day_of_week(schedule)
    assert new_schedule == expected_schedule




# @pytest.mark.parametrize(
#     'values, previous_schedule, next_hour, expected_result', [
#         ([10, 12], dt(2024, 2, 8, 10, 0), 11, (2, 10)),
#         ([10, 12], dt(2024, 2, 10, 10, 0), 11, (0, 10)),
#         ([10, 12], dt(2024, 2, 10, 10, 0), 5, (2, 12)),
#         ([10, 12], dt(2024, 2, 10, 10, 0), 10, (2, 12)),
#         ([10, 12], dt(2024, 2, 12, 10, 0), 5, (27, 10)),  # leap year
#         ([10, 12], dt(2023, 2, 12, 10, 0), 5, (26, 10)),  # not a leap year
#         ([10, 12], dt(2023, 6, 12, 10, 0), 5, (28, 10)),  # from jun to jul
#         ([10, 12], dt(2023, 7, 12, 10, 0), 5, (29, 10)),  # from jul to aug
#     ]
# )
# def _test_get_next_day(values, previous_schedule, next_hour, expected_result):
#     """
#     Test to get the next day from the crontab list depending on the
#     previous schedule and the calculated next hour.
#     """
#     result = get_next_day(values, previous_schedule, next_hour)
#     assert result == expected_result


# @pytest.mark.parametrize(
#     'values, previous_schedule, next_minute, expected_result', [
#         ([10, 12], dt(2024, 2, 8, 10, 30), 40, (0, 10)),
#         ([10, 12], dt(2024, 2, 8, 10, 40), 20, (2, 12)),
#         ([10, 12], dt(2024, 2, 8, 12, 20), 10, (22, 10)),
#         ([10, 12], dt(2024, 2, 8, 2, 20), 10, (8, 10)),
#         ([10, 12], dt(2024, 2, 8, 2, 20), 30, (8, 10)),
#         ([10, 12], dt(2024, 2, 8, 11, 20), 30, (1, 12)),
#     ]
# )
# def _test_get_next_hour(values, previous_schedule, next_minute, expected_result):
#     """
#     Test to get the next hour from the crontab list depending on the
#     previous schedule and the calculated next minute.
#     """
#     result = get_next_hour(values, previous_schedule, next_minute)
#     assert result == expected_result


# @pytest.mark.parametrize(
#     'values, previous_schedule, expected_result', [
#         ([10, 20, 25], dt(2024, 2, 8, 2, 0), (10, 10)),
#         ([10, 20, 25], dt(2024, 2, 8, 2, 10), (10, 20)),
#         ([10, 20, 25], dt(2024, 2, 8, 2, 20), (5, 25)),
#         ([10, 20, 25], dt(2024, 2, 8, 2, 25), (45, 10)),
#         ([10, 20, 25], dt(2024, 2, 8, 2, 55), (15, 10)),
#     ]
# )
# def _test_get_next_minute(values, previous_schedule, expected_result):
#     """
#     Test to get the next minute from the crontab list depending on the
#     previous schedule.
#     """
#     result = get_next_minute(values, previous_schedule)
#     assert result == expected_result


# @pytest.mark.parametrize(
#     'values, previous_schedule, next_day, expected_result', [
#         ([2, 6, 10], dt(2024, 2, 8), 10, (2, 2024)),
#         ([2, 6, 10], dt(2024, 2, 8), 4, (6, 2024)),
#         ([2, 6, 10], dt(2024, 6, 8), 4, (10, 2024)),
#         ([2, 6, 10], dt(2024, 10, 8), 4, (2, 2025)),
#         ([2, 6, 10], dt(2024, 11, 8), 2, (2, 2025)),
#         ([2, 6, 10], dt(2024, 11, 8), 12, (2, 2025)),
#     ]
# )
# def _test_get_next_month_and_year(values,
#                                  previous_schedule,
#                                  next_day,
#                                  expected_result):
#     """
#     Test to get the next month and year for a given crontab and previous
#     schedule.
#     """
#     result = get_next_month_and_year(values, previous_schedule, next_day)
#     assert result == expected_result

