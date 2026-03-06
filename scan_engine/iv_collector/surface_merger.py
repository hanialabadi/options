"""
surface_merger.py — 15:45 ET daily IV surface merger job

PURPOSE
-------
Merges the REST baseline (iv_term_history, written at market open by rest_collector)
with the intraday streamer drift (iv_intraday_stream, written throughout the day
by streamer_collector) to produce the canonical end-of-day IV surface row.

Run at 15:45 ET to capture:
    ✅ Latest intraday IV at good liquidity (45 min before close)
    ✅ Stable Greeks / OI / volume near close
    ✅ Final ATM mapping before close

WHY 15:45 (not 16:00)
---------------------
Options market officially closes at 16:00 ET, but:
- Schwab stops returning live IV before 16:00 on many symbols
- Last 15 minutes often has low volume / erratic prices
- 15:45 balances freshness vs stability

MERGE LOGIC
-----------
For each ticker × bucket:

1. If fresh intraday push exists (within last 90 min) → use it
2. Else fall back to REST baseline value
3. Null gate: if iv_30d still null after merge → skip ticker (don't write)
4. Write merged row to iv_term_history (upsert — replaces any earlier REST value)
5. Tag source as 'schwab_merged' so downstream audit can distinguish

MERGE DECISION TABLE
---------------------
| REST baseline | Intraday push | Result                  |
|---------------|---------------|-------------------------|
| present       | present       | intraday wins (fresher) |
| present       | absent/stale  | REST baseline kept      |
| absent        | present       | intraday only (partial) |
| absent        | absent        | skip ticker             |

PUBLIC API
----------
    merge_daily_surface(trade_date, con) -> MergeResult
    run_daily_merger(trade_date)         — convenience one-liner with own connection

    MergeResult:
        merged_count  int
        rest_only     int    — tickers where no intraday push arrived
        stream_wins   int    — tickers where intraday value was fresher
        skipped       int    — tickers dropped (null iv_30d)
        trade_date    date
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional

import duckdb
import pandas as pd

from core.shared.data_layer.iv_term_history import (
    get_iv_history_db_path,
    initialize_iv_term_history_table,
    initialize_iv_intraday_stream_table,
    append_daily_iv_data,
    get_latest_intraday_iv,
    get_intraday_stream_summary,
)
from scan_engine.iv_collector.chain_surface import MATURITY_BUCKETS

logger = logging.getLogger(__name__)

# How old can a streamer push be and still override the REST baseline?
INTRADAY_FRESHNESS_MINUTES: int = 90


# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------

@dataclass
class MergeResult:
    merged_count: int    # Tickers successfully written to iv_term_history
    rest_only:    int    # Tickers where intraday had no fresh push (REST used)
    stream_wins:  int    # Tickers where at least one bucket used intraday value
    skipped:      int    # Tickers dropped (iv_30d null after merge)
    trade_date:   date


# ---------------------------------------------------------------------------
# Main function
# ---------------------------------------------------------------------------

def merge_daily_surface(
    trade_date: date,
    con: duckdb.DuckDBPyConnection,
) -> MergeResult:
    """
    Merge REST baseline + intraday stream → canonical daily surface.

    Parameters
    ----------
    trade_date : date
        The trading date to merge.
    con : duckdb.DuckDBPyConnection
        Open read-write connection to iv_history.duckdb.

    Returns
    -------
    MergeResult
    """
    initialize_iv_term_history_table(con)
    initialize_iv_intraday_stream_table(con)

    logger.info("=" * 65)
    logger.info("📊 IV SURFACE MERGER — %s", trade_date)
    logger.info("=" * 65)

    # ------------------------------------------------------------------ #
    # 1. Load REST baseline for trade_date
    # ------------------------------------------------------------------ #
    rest_df = con.execute("""
        SELECT ticker, iv_7d, iv_14d, iv_30d, iv_60d, iv_90d, iv_120d, iv_180d, iv_360d
        FROM iv_term_history
        WHERE date = ?
    """, [trade_date]).df()

    logger.info("REST baseline: %d tickers", len(rest_df))

    if rest_df.empty:
        logger.warning("No REST baseline for %s — merger has nothing to merge", trade_date)
        return MergeResult(0, 0, 0, 0, trade_date)

    # ------------------------------------------------------------------ #
    # 2. Load latest intraday push per ticker × bucket
    # ------------------------------------------------------------------ #
    freshness_cutoff = datetime.utcnow() - timedelta(minutes=INTRADAY_FRESHNESS_MINUTES)

    intraday_rows = con.execute("""
        WITH ranked AS (
            SELECT
                ticker, bucket, iv, ts, atm_symbol,
                ROW_NUMBER() OVER (PARTITION BY ticker, bucket ORDER BY ts DESC) AS rn
            FROM iv_intraday_stream
            WHERE trade_date = ?
              AND iv > 0
        )
        SELECT ticker, bucket, iv, ts, atm_symbol
        FROM ranked
        WHERE rn = 1
    """, [trade_date]).df()

    # Mark fresh vs stale
    if not intraday_rows.empty:
        intraday_rows['ts'] = pd.to_datetime(intraday_rows['ts'])
        intraday_rows['is_fresh'] = intraday_rows['ts'] >= freshness_cutoff

    logger.info(
        "Intraday stream: %d ticker×bucket readings (%d fresh)",
        len(intraday_rows),
        intraday_rows['is_fresh'].sum() if not intraday_rows.empty else 0,
    )

    # Pivot intraday to: ticker → bucket → {iv, is_fresh}
    intraday_pivot: dict[str, dict[int, dict]] = {}
    for _, row in intraday_rows.iterrows():
        t = row['ticker']
        b = int(row['bucket'])
        if t not in intraday_pivot:
            intraday_pivot[t] = {}
        intraday_pivot[t][b] = {
            "iv":       float(row['iv']),
            "is_fresh": bool(row['is_fresh']),
            "ts":       row['ts'],
        }

    # ------------------------------------------------------------------ #
    # 3. Merge per ticker
    # ------------------------------------------------------------------ #
    merged_rows: list[dict] = []
    rest_only_count  = 0
    stream_wins_count = 0
    skipped_count    = 0

    for _, rest_row in rest_df.iterrows():
        ticker  = rest_row['ticker']
        ticker_intraday = intraday_pivot.get(ticker, {})

        merged = {"ticker": ticker, "source": "schwab_merged"}
        any_stream_win = False

        for bucket in MATURITY_BUCKETS:
            col = f"iv_{bucket}d"
            rest_val = rest_row.get(col)
            stream_entry = ticker_intraday.get(bucket)

            if stream_entry and stream_entry["is_fresh"]:
                # Intraday wins — fresher signal
                merged[col] = stream_entry["iv"]
                any_stream_win = True
            elif rest_val is not None and not _is_null(rest_val):
                # REST baseline stands
                merged[col] = float(rest_val)
            else:
                merged[col] = None

        # Null gate
        if _is_null(merged.get("iv_30d")):
            logger.debug("[%s] Skipped — iv_30d null after merge", ticker)
            skipped_count += 1
            continue

        merged_rows.append(merged)
        if any_stream_win:
            stream_wins_count += 1
        else:
            rest_only_count += 1

    # ------------------------------------------------------------------ #
    # 4. Write merged surface to iv_term_history (upsert)
    # ------------------------------------------------------------------ #
    if merged_rows:
        df_merged = pd.DataFrame(merged_rows)
        append_daily_iv_data(con, df_merged, trade_date=trade_date)

    # ------------------------------------------------------------------ #
    # 5. Log summary
    # ------------------------------------------------------------------ #
    result = MergeResult(
        merged_count  = len(merged_rows),
        rest_only     = rest_only_count,
        stream_wins   = stream_wins_count,
        skipped       = skipped_count,
        trade_date    = trade_date,
    )

    stream_summary = get_intraday_stream_summary(con, trade_date)

    logger.info("=" * 65)
    logger.info(
        "✅ MERGER COMPLETE | merged=%d  rest_only=%d  stream_wins=%d  skipped=%d",
        result.merged_count, result.rest_only, result.stream_wins, result.skipped,
    )
    logger.info(
        "   Intraday stream: %d total pushes across %d tickers",
        stream_summary.get("total_pushes", 0),
        stream_summary.get("tickers", 0),
    )
    logger.info("=" * 65)

    return result


def _is_null(val) -> bool:
    """Return True if val is None, NaN, or non-positive."""
    if val is None:
        return True
    try:
        import math
        if math.isnan(float(val)):
            return True
        return float(val) <= 0
    except (TypeError, ValueError):
        return True


# ---------------------------------------------------------------------------
# Convenience one-liner
# ---------------------------------------------------------------------------

def run_daily_merger(trade_date: Optional[date] = None) -> MergeResult:
    """
    Run the daily IV surface merger with its own DuckDB connection.

    Parameters
    ----------
    trade_date : date, optional
        Defaults to today.

    Returns
    -------
    MergeResult
    """
    trade_date = trade_date or date.today()
    db_path = get_iv_history_db_path()
    con = duckdb.connect(str(db_path), read_only=False)
    try:
        return merge_daily_surface(trade_date, con)
    finally:
        con.close()
