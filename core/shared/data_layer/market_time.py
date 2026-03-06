"""
Market-aware time utilities (US equities).

Provides deterministic freshness checks relative to last market close.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date, time, timedelta
from typing import Optional, Tuple

try:
    from zoneinfo import ZoneInfo
    US_EASTERN = ZoneInfo("America/New_York")
except Exception:  # pragma: no cover
    import pytz
    US_EASTERN = pytz.timezone("America/New_York")

MARKET_OPEN = time(9, 30)
MARKET_CLOSE = time(16, 0)


def to_eastern(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=US_EASTERN)
    return dt.astimezone(US_EASTERN)


def is_trading_day(d: date) -> bool:
    return d.weekday() < 5


def previous_trading_day(d: date) -> date:
    cur = d
    while not is_trading_day(cur):
        cur = cur - timedelta(days=1)
    return cur


def last_market_close(now: Optional[datetime] = None) -> datetime:
    now = to_eastern(now or datetime.now(tz=US_EASTERN))
    if is_trading_day(now.date()) and now.time() >= MARKET_CLOSE:
        return datetime.combine(now.date(), MARKET_CLOSE, tzinfo=US_EASTERN)

    # If before close or weekend, last close is previous trading day at 16:00
    prev_day = previous_trading_day(now.date() - timedelta(days=1))
    return datetime.combine(prev_day, MARKET_CLOSE, tzinfo=US_EASTERN)


def is_market_open(now: Optional[datetime] = None) -> bool:
    now = to_eastern(now or datetime.now(tz=US_EASTERN))
    return is_trading_day(now.date()) and MARKET_OPEN <= now.time() < MARKET_CLOSE


def trading_days_diff(start: date, end: date) -> int:
    """Count trading day boundaries from start (exclusive) to end (inclusive)."""
    if start > end:
        start, end = end, start
    count = 0
    cur = start
    while cur < end:
        cur = cur + timedelta(days=1)
        if is_trading_day(cur):
            count += 1
    return count


def trading_days_since(ts: datetime, now: Optional[datetime] = None) -> int:
    now = to_eastern(now or datetime.now(tz=US_EASTERN))
    last_close = last_market_close(now)
    ts_e = to_eastern(ts)
    return trading_days_diff(ts_e.date(), last_close.date())


def ivhv_freshness(ts: datetime, max_trading_days: int = 1, now: Optional[datetime] = None) -> Tuple[bool, int]:
    age_td = trading_days_since(ts, now=now)
    return age_td <= max_trading_days, age_td


def fidelity_freshness(ts: datetime, max_trading_days: int = 2, now: Optional[datetime] = None) -> Tuple[bool, int]:
    age_td = trading_days_since(ts, now=now)
    return age_td <= max_trading_days, age_td


def price_freshness(last_bar_ts: datetime, now: Optional[datetime] = None) -> bool:
    now = to_eastern(now or datetime.now(tz=US_EASTERN))
    last_close = last_market_close(now)
    bar_date = to_eastern(last_bar_ts).date()
    if is_market_open(now):
        # During market hours Schwab daily history returns the previous session's
        # close as the last complete bar — today's candle is still forming.
        # Accept T-1 (yesterday's close) as fresh during the live session.
        prev_trading_day = previous_trading_day(now.date() - timedelta(days=1))
        return bar_date >= prev_trading_day
    # After market close: allow last bar to be 1 trading day old — period-based
    # Schwab requests may not include today's session close until T+1 propagation.
    prev_close_date = previous_trading_day(last_close.date() - timedelta(days=1))
    return bar_date >= prev_close_date

