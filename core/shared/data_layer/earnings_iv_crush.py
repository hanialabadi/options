"""
Earnings IV Crush: IV Crush + Expected/Actual Move Analytics

Extracted from earnings_history.py for maintainability.

Contains:
    - earnings_iv_crush table definition
    - _nearest_iv_reading / _nearest_price helpers
    - _compute_iv_ramp_start
    - compute_iv_crush_for_event (orchestrator)
"""

import duckdb
import math
import pandas as pd
from datetime import date, datetime, timedelta
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Table definition (called by earnings_history.initialize_tables)
# ---------------------------------------------------------------------------

def create_iv_crush_table(con: duckdb.DuckDBPyConnection) -> None:
    """Create earnings_iv_crush table (idempotent)."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS earnings_iv_crush (
            ticker            VARCHAR NOT NULL,
            earnings_date     DATE NOT NULL,
            iv_30d_5d_before  DOUBLE,
            iv_30d_1d_before  DOUBLE,
            iv_30d_1d_after   DOUBLE,
            iv_30d_5d_after   DOUBLE,
            iv_crush_pct      DOUBLE,
            iv_crush_5d_pct   DOUBLE,
            iv_buildup_pct    DOUBLE,
            iv_ramp_start_days INTEGER,
            expected_move_pct DOUBLE,
            actual_move_pct   DOUBLE,
            move_ratio        DOUBLE,
            close_1d_before   DOUBLE,
            close_day_of      DOUBLE,
            close_1d_after    DOUBLE,
            close_5d_after    DOUBLE,
            gap_pct           DOUBLE,
            day_move_pct      DOUBLE,
            move_5d_pct       DOUBLE,
            iv_data_quality   VARCHAR,
            price_data_quality VARCHAR,
            computed_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ticker, earnings_date)
        )
    """)


# ---------------------------------------------------------------------------
# Nearest-reading helpers (sparse data resolution)
# ---------------------------------------------------------------------------

def _nearest_iv_reading(
    iv_con: duckdb.DuckDBPyConnection,
    ticker: str,
    target_date: date,
    window_before: int = 10,
    window_after: int = 3,
) -> Optional[float]:
    """Find nearest iv_30d reading within a date window around target_date."""
    start = target_date - timedelta(days=window_before)
    end = target_date + timedelta(days=window_after)
    rows = iv_con.execute("""
        SELECT iv_30d, date,
               ABS(DATEDIFF('day', date, ?::DATE)) AS gap
        FROM iv_term_history
        WHERE ticker = ?
          AND date BETWEEN ?::DATE AND ?::DATE
          AND iv_30d IS NOT NULL
        ORDER BY gap ASC
        LIMIT 1
    """, [target_date, ticker, start, end]).fetchall()
    if rows:
        return float(rows[0][0])
    return None


def _nearest_price(
    pipeline_con: duckdb.DuckDBPyConnection,
    ticker: str,
    target_date: date,
    window: int = 5,
) -> Optional[float]:
    """Find nearest closing price within window days of target_date."""
    start = target_date - timedelta(days=window)
    end = target_date + timedelta(days=window)
    rows = pipeline_con.execute("""
        SELECT close_price, date,
               ABS(DATEDIFF('day', date, ?::DATE)) AS gap
        FROM price_history
        WHERE ticker = ?
          AND date BETWEEN ?::DATE AND ?::DATE
          AND close_price IS NOT NULL
        ORDER BY gap ASC
        LIMIT 1
    """, [target_date, ticker, start, end]).fetchall()
    if rows:
        return float(rows[0][0])
    return None


# ---------------------------------------------------------------------------
# IV ramp detection
# ---------------------------------------------------------------------------

def _compute_iv_ramp_start(
    iv_con: duckdb.DuckDBPyConnection,
    ticker: str,
    earnings_date: date,
    lookback_days: int = 30,
) -> Optional[int]:
    """
    Walk backwards from D-2 to find when IV started climbing before earnings.
    Returns number of days before earnings that the ramp began, or None.
    """
    start = earnings_date - timedelta(days=lookback_days)
    end = earnings_date - timedelta(days=2)
    rows = iv_con.execute("""
        SELECT date, iv_30d
        FROM iv_term_history
        WHERE ticker = ?
          AND date BETWEEN ?::DATE AND ?::DATE
          AND iv_30d IS NOT NULL
        ORDER BY date DESC
    """, [ticker, start, end]).fetchall()

    if len(rows) < 3:
        return None

    # Find where IV starts declining backwards (first local trough)
    iv_values = [float(r[1]) for r in rows]  # newest first
    dates = [r[0] for r in rows]

    # The ramp started where IV was at its minimum before earnings
    peak_iv = iv_values[0]  # closest to earnings = highest (usually)
    min_idx = 0
    min_iv = peak_iv

    for i, iv in enumerate(iv_values):
        if iv < min_iv:
            min_iv = iv
            min_idx = i

    if min_idx == 0 or peak_iv <= 0:
        return None

    # Ramp is meaningful only if buildup > 5%
    buildup = (peak_iv - min_iv) / min_iv
    if buildup < 0.05:
        return None

    ramp_date = dates[min_idx]
    if isinstance(ramp_date, datetime):
        ramp_date = ramp_date.date()
    ed = earnings_date
    if isinstance(ed, datetime):
        ed = ed.date()

    return (ed - ramp_date).days


# ---------------------------------------------------------------------------
# IV crush computation (orchestrator)
# ---------------------------------------------------------------------------

def compute_iv_crush_for_event(
    pipeline_con: duckdb.DuckDBPyConnection,
    iv_con: duckdb.DuckDBPyConnection,
    ticker: str,
    earnings_date: date,
) -> Optional[Dict]:
    """
    Compute IV crush + price moves for a single earnings event.
    Writes to earnings_iv_crush table and returns the computed row dict.
    """
    ed = earnings_date
    if isinstance(ed, datetime):
        ed = ed.date()

    # IV readings (nearest within window)
    iv_5d_before = _nearest_iv_reading(iv_con, ticker, ed - timedelta(days=5), window_before=3, window_after=3)
    iv_1d_before = _nearest_iv_reading(iv_con, ticker, ed - timedelta(days=1), window_before=2, window_after=0)
    iv_1d_after = _nearest_iv_reading(iv_con, ticker, ed + timedelta(days=1), window_before=0, window_after=3)
    iv_5d_after = _nearest_iv_reading(iv_con, ticker, ed + timedelta(days=5), window_before=2, window_after=3)

    # Price readings
    close_1d_before = _nearest_price(pipeline_con, ticker, ed - timedelta(days=1))
    close_day_of = _nearest_price(pipeline_con, ticker, ed, window=2)
    close_1d_after = _nearest_price(pipeline_con, ticker, ed + timedelta(days=1), window=3)
    close_5d_after = _nearest_price(pipeline_con, ticker, ed + timedelta(days=5), window=3)

    # Derived IV metrics
    iv_crush_pct = None
    iv_crush_5d_pct = None
    iv_buildup_pct = None

    if iv_1d_before and iv_1d_after and iv_1d_before > 0:
        iv_crush_pct = (iv_1d_before - iv_1d_after) / iv_1d_before

    if iv_5d_before and iv_5d_after and iv_5d_before > 0:
        iv_crush_5d_pct = (iv_5d_before - iv_5d_after) / iv_5d_before

    if iv_5d_before and iv_1d_before and iv_5d_before > 0:
        iv_buildup_pct = (iv_1d_before - iv_5d_before) / iv_5d_before

    # IV ramp detection
    iv_ramp_start_days = _compute_iv_ramp_start(iv_con, ticker, ed)

    # Expected vs actual move
    expected_move_pct = None
    actual_move_pct = None
    move_ratio = None

    if iv_1d_before and close_1d_before and close_1d_before > 0:
        # ATM straddle-implied 1-day move ≈ iv × √(1/252)
        # iv_term_history stores IV as percentage (26.5 = 26.5%), convert to decimal
        iv_decimal = iv_1d_before / 100.0 if iv_1d_before > 1.0 else iv_1d_before
        expected_move_pct = iv_decimal * math.sqrt(1.0 / 252.0)

    if close_1d_before and close_day_of and close_1d_before > 0:
        actual_move_pct = abs(close_day_of - close_1d_before) / close_1d_before

    if expected_move_pct and actual_move_pct and expected_move_pct > 0.001:
        move_ratio = actual_move_pct / expected_move_pct

    # Price moves
    gap_pct = None
    day_move_pct = None
    move_5d_pct = None

    if close_1d_before and close_day_of and close_1d_before > 0:
        day_move_pct = (close_day_of - close_1d_before) / close_1d_before

    if close_1d_before and close_1d_after and close_1d_before > 0:
        gap_pct = (close_1d_after - close_1d_before) / close_1d_before

    if close_1d_before and close_5d_after and close_1d_before > 0:
        move_5d_pct = (close_5d_after - close_1d_before) / close_1d_before

    # Data quality
    iv_count = sum(1 for v in [iv_5d_before, iv_1d_before, iv_1d_after, iv_5d_after] if v is not None)
    price_count = sum(1 for v in [close_1d_before, close_day_of, close_1d_after, close_5d_after] if v is not None)

    iv_quality = "COMPLETE" if iv_count == 4 else ("PARTIAL" if iv_count > 0 else "MISSING")
    price_quality = "COMPLETE" if price_count == 4 else ("PARTIAL" if price_count > 0 else "MISSING")

    row = {
        "ticker": ticker,
        "earnings_date": ed,
        "iv_30d_5d_before": iv_5d_before,
        "iv_30d_1d_before": iv_1d_before,
        "iv_30d_1d_after": iv_1d_after,
        "iv_30d_5d_after": iv_5d_after,
        "iv_crush_pct": iv_crush_pct,
        "iv_crush_5d_pct": iv_crush_5d_pct,
        "iv_buildup_pct": iv_buildup_pct,
        "iv_ramp_start_days": iv_ramp_start_days,
        "expected_move_pct": expected_move_pct,
        "actual_move_pct": actual_move_pct,
        "move_ratio": move_ratio,
        "close_1d_before": close_1d_before,
        "close_day_of": close_day_of,
        "close_1d_after": close_1d_after,
        "close_5d_after": close_5d_after,
        "gap_pct": gap_pct,
        "day_move_pct": day_move_pct,
        "move_5d_pct": move_5d_pct,
        "iv_data_quality": iv_quality,
        "price_data_quality": price_quality,
    }

    # Upsert into earnings_iv_crush
    df_crush = pd.DataFrame([row])
    pipeline_con.execute("""
        INSERT INTO earnings_iv_crush
            (ticker, earnings_date, iv_30d_5d_before, iv_30d_1d_before,
             iv_30d_1d_after, iv_30d_5d_after, iv_crush_pct, iv_crush_5d_pct,
             iv_buildup_pct, iv_ramp_start_days, expected_move_pct,
             actual_move_pct, move_ratio, close_1d_before, close_day_of,
             close_1d_after, close_5d_after, gap_pct, day_move_pct,
             move_5d_pct, iv_data_quality, price_data_quality)
        SELECT ticker, earnings_date, iv_30d_5d_before, iv_30d_1d_before,
               iv_30d_1d_after, iv_30d_5d_after, iv_crush_pct, iv_crush_5d_pct,
               iv_buildup_pct, iv_ramp_start_days, expected_move_pct,
               actual_move_pct, move_ratio, close_1d_before, close_day_of,
               close_1d_after, close_5d_after, gap_pct, day_move_pct,
               move_5d_pct, iv_data_quality, price_data_quality
        FROM df_crush
        ON CONFLICT (ticker, earnings_date) DO UPDATE SET
            iv_30d_5d_before = EXCLUDED.iv_30d_5d_before,
            iv_30d_1d_before = EXCLUDED.iv_30d_1d_before,
            iv_30d_1d_after = EXCLUDED.iv_30d_1d_after,
            iv_30d_5d_after = EXCLUDED.iv_30d_5d_after,
            iv_crush_pct = EXCLUDED.iv_crush_pct,
            iv_crush_5d_pct = EXCLUDED.iv_crush_5d_pct,
            iv_buildup_pct = EXCLUDED.iv_buildup_pct,
            iv_ramp_start_days = EXCLUDED.iv_ramp_start_days,
            expected_move_pct = EXCLUDED.expected_move_pct,
            actual_move_pct = EXCLUDED.actual_move_pct,
            move_ratio = EXCLUDED.move_ratio,
            close_1d_before = EXCLUDED.close_1d_before,
            close_day_of = EXCLUDED.close_day_of,
            close_1d_after = EXCLUDED.close_1d_after,
            close_5d_after = EXCLUDED.close_5d_after,
            gap_pct = EXCLUDED.gap_pct,
            day_move_pct = EXCLUDED.day_move_pct,
            move_5d_pct = EXCLUDED.move_5d_pct,
            iv_data_quality = EXCLUDED.iv_data_quality,
            price_data_quality = EXCLUDED.price_data_quality
    """)

    return row
