import datetime as dt
import calendar
from typing import Tuple

DUBAI_OFFSET = dt.timedelta(hours=4)


def dubai_now() -> dt.datetime:
    return dt.datetime.utcnow() + DUBAI_OFFSET


def first_weekday_of_month(year: int, month: int, weekday: int) -> int:
    first_day = dt.date(year, month, 1)
    delta = (weekday - first_day.weekday()) % 7
    return 1 + delta


def nth_weekday_of_month(year: int, month: int, weekday: int, n: int) -> int:
    first = first_weekday_of_month(year, month, weekday)
    day = first + (n - 1) * 7
    if day <= calendar.monthrange(year, month)[1]:
        return day
    return last_weekday_of_month(year, month, weekday)


def last_weekday_of_month(year: int, month: int, weekday: int) -> int:
    last_day = calendar.monthrange(year, month)[1]
    last_date = dt.date(year, month, last_day)
    delta = (last_date.weekday() - weekday) % 7
    return last_day - delta


def monthly_windows(now: dt.datetime) -> Tuple[dt.date, dt.date, dt.date, dt.date]:
    current_month_start = dt.date(now.year, now.month, 1)
    current_range_end = now.date()

    if now.month == 1:
        prev_year = now.year - 1
        prev_month = 12
    else:
        prev_year = now.year
        prev_month = now.month - 1

    previous_month_start = dt.date(prev_year, prev_month, 1)
    previous_month_end = dt.date(prev_year, prev_month, calendar.monthrange(prev_year, prev_month)[1])

    return (
        previous_month_start,
        previous_month_end,
        current_month_start,
        current_range_end,
    )


def daily_windows(now: dt.datetime) -> Tuple[dt.date, dt.date]:
    today = now.date()
    weekday = today.weekday()
    first_occurrence = first_weekday_of_month(today.year, today.month, weekday)
    nth = 1 + max((today.day - first_occurrence) // 7, 0)

    if today.month == 1:
        prev_year = today.year - 1
        prev_month = 12
    else:
        prev_year = today.year
        prev_month = today.month - 1

    prev_day = nth_weekday_of_month(prev_year, prev_month, weekday, nth)
    previous_date = dt.date(prev_year, prev_month, prev_day)
    return today, previous_date


def required_start_date(now: dt.datetime) -> dt.date:
    _, prev_daily = daily_windows(now)
    prev_month_start, _, _, _ = monthly_windows(now)
    return min(prev_daily, prev_month_start)
