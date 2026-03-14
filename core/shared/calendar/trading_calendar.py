"""
Shared NYSE Trading Calendar — holidays, trading-day checks, business-day math.

Single source of truth for all collection scripts, market-hours guards,
calendar context, and UI trading-day checks.

Update annually — NYSE publishes the full schedule each November.
"""

from datetime import date, datetime, timedelta
from typing import Optional, Union


# ── NYSE Holiday Calendar ─────────────────────────────────────────────────────
# Hardcoded for 2024-2027. Add years as needed.
# Source: NYSE holiday schedule (https://www.nyse.com/markets/hours-calendars)
NYSE_HOLIDAYS: set[date] = {
    # 2024
    date(2024, 1, 1),   # New Year's Day
    date(2024, 1, 15),  # MLK Day
    date(2024, 2, 19),  # Presidents' Day
    date(2024, 3, 29),  # Good Friday
    date(2024, 5, 27),  # Memorial Day
    date(2024, 6, 19),  # Juneteenth
    date(2024, 7, 4),   # Independence Day
    date(2024, 9, 2),   # Labor Day
    date(2024, 11, 28), # Thanksgiving
    date(2024, 12, 25), # Christmas
    # 2025
    date(2025, 1, 1),   # New Year's Day
    date(2025, 1, 9),   # National Day of Mourning (Carter)
    date(2025, 1, 20),  # MLK Day
    date(2025, 2, 17),  # Presidents' Day
    date(2025, 4, 18),  # Good Friday
    date(2025, 5, 26),  # Memorial Day
    date(2025, 6, 19),  # Juneteenth
    date(2025, 7, 4),   # Independence Day
    date(2025, 9, 1),   # Labor Day
    date(2025, 11, 27), # Thanksgiving
    date(2025, 12, 25), # Christmas
    # 2026
    date(2026, 1, 1),   # New Year's Day
    date(2026, 1, 19),  # MLK Day
    date(2026, 2, 16),  # Presidents' Day
    date(2026, 4, 3),   # Good Friday
    date(2026, 5, 25),  # Memorial Day
    date(2026, 6, 19),  # Juneteenth
    date(2026, 7, 3),   # Independence Day (observed, Friday)
    date(2026, 9, 7),   # Labor Day
    date(2026, 11, 26), # Thanksgiving
    date(2026, 12, 25), # Christmas
    # 2027
    date(2027, 1, 1),   # New Year's Day
    date(2027, 1, 18),  # MLK Day
    date(2027, 2, 15),  # Presidents' Day
    date(2027, 3, 26),  # Good Friday
    date(2027, 5, 31),  # Memorial Day
    date(2027, 6, 18),  # Juneteenth (observed, Friday)
    date(2027, 7, 5),   # Independence Day (observed, Monday)
    date(2027, 9, 6),   # Labor Day
    date(2027, 11, 25), # Thanksgiving
    date(2027, 12, 24), # Christmas (observed, Friday)
}


def _to_date(d: Union[datetime, date, None]) -> date:
    """Coerce datetime/date/None to date."""
    if d is None:
        return date.today()
    if isinstance(d, datetime):
        return d.date()
    return d


def is_trading_day(d: Union[datetime, date, None] = None) -> bool:
    """Return True if *d* (default: today) is a NYSE trading day."""
    d = _to_date(d)
    return d.weekday() < 5 and d not in NYSE_HOLIDAYS


def next_trading_day(d: Union[datetime, date, None] = None) -> date:
    """Return the next NYSE trading day after *d*."""
    d = _to_date(d)
    candidate = d + timedelta(days=1)
    while not is_trading_day(candidate):
        candidate += timedelta(days=1)
    return candidate


def prev_trading_day(d: Union[datetime, date, None] = None) -> date:
    """Return the most recent NYSE trading day before *d*."""
    d = _to_date(d)
    candidate = d - timedelta(days=1)
    while not is_trading_day(candidate):
        candidate -= timedelta(days=1)
    return candidate


def business_days_between(d1: date, d2: date) -> int:
    """
    Count NYSE business days between *d1* and *d2* (exclusive of d1, inclusive of d2).

    Returns 0 if d1 >= d2 or both are the same day.
    Useful for staleness computation: business_days_between(data_date, today) == 0
    means data is from today.
    """
    if d1 >= d2:
        return 0
    count = 0
    current = d1 + timedelta(days=1)
    while current <= d2:
        if is_trading_day(current):
            count += 1
        current += timedelta(days=1)
    return count
