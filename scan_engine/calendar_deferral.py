"""
Calendar Deferral Tracking
===========================
Persists READY candidates that were calendar-deferred (Friday/pre-holiday theta bleed)
so the system can resurface them on the next trading day with priority.

Table: calendar_deferred (in pipeline.duckdb)

Lifecycle:
    1. Friday scan → READY + ELEVATED_BLEED/HIGH_BLEED → INSERT status=PENDING
    2. Monday scan → read PENDING deferrals → tag matching candidates as DEFERRED_RETURN
    3. After Monday scan completes → mark matched as FILLED, expire stale (>3 trading days)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

# Only long-premium calendar flags trigger deferral
_DEFERRAL_FLAGS = {'ELEVATED_BLEED', 'HIGH_BLEED'}

# Candidates older than this many calendar days auto-expire
_MAX_AGE_DAYS = 5


def initialize_calendar_deferred_table(con: duckdb.DuckDBPyConnection) -> None:
    """Create the calendar_deferred table if it doesn't exist."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS calendar_deferred (
            ticker              VARCHAR NOT NULL,
            strategy_name       VARCHAR NOT NULL,
            deferred_date       DATE NOT NULL,
            calendar_flag       VARCHAR NOT NULL,
            dqs_score           DOUBLE,
            entry_price         DOUBLE,
            strike              DOUBLE,
            expiration          VARCHAR,
            contract_symbol     VARCHAR,
            dte                 INTEGER,
            trade_bias          VARCHAR,
            gate_reason         VARCHAR,
            status              VARCHAR DEFAULT 'PENDING',
            resume_date         DATE,
            created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ticker, strategy_name, deferred_date)
        )
    """)


def persist_deferred_candidates(
    df_ready: pd.DataFrame,
    con: duckdb.DuckDBPyConnection,
    deferred_date: Optional[datetime] = None,
) -> int:
    """
    Persist READY candidates with calendar deferral flags.

    Parameters
    ----------
    df_ready : pd.DataFrame
        The READY-only DataFrame from Step 12.
    con : duckdb.DuckDBPyConnection
        Pipeline DuckDB connection.
    deferred_date : datetime, optional
        Override date (for testing). Defaults to today.

    Returns
    -------
    int
        Number of deferred candidates persisted.
    """
    if df_ready.empty:
        return 0

    initialize_calendar_deferred_table(con)

    if deferred_date is None:
        deferred_date = datetime.now(timezone.utc)

    d_date = deferred_date.date() if hasattr(deferred_date, 'date') else deferred_date

    # Filter to rows with deferral-triggering calendar flags
    cal_col = 'Calendar_Risk_Flag'
    if cal_col not in df_ready.columns:
        return 0

    deferred = df_ready[
        df_ready[cal_col].astype(str).str.upper().isin(_DEFERRAL_FLAGS)
    ]
    if deferred.empty:
        return 0

    count = 0
    for _, row in deferred.iterrows():
        try:
            con.execute("""
                INSERT OR REPLACE INTO calendar_deferred
                (ticker, strategy_name, deferred_date, calendar_flag, dqs_score,
                 entry_price, strike, expiration, contract_symbol, dte,
                 trade_bias, gate_reason, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING')
            """, [
                str(row.get('Ticker') or ''),
                str(row.get('Strategy_Name') or row.get('Strategy') or ''),
                d_date,
                str(row.get(cal_col) or ''),
                float(row['DQS_Score']) if pd.notna(row.get('DQS_Score')) else None,
                float(row.get('Mid_Price') or row.get('entry_price') or 0),
                float(row.get('Strike') or 0),
                str(row.get('Expiration') or ''),
                str(row.get('Contract_Symbol') or row.get('Symbol') or ''),
                int(row.get('Actual_DTE') or row.get('DTE') or 0) if pd.notna(row.get('Actual_DTE') or row.get('DTE')) else None,
                str(row.get('Trade_Bias') or ''),
                str(row.get('Gate_Reason') or ''),
            ])
            count += 1
        except Exception as e:
            logger.debug(f"[CalendarDeferral] Insert failed for {row.get('Ticker')}: {e}")

    logger.info(f"[CalendarDeferral] Persisted {count} deferred candidates for {d_date}")
    return count


def read_pending_deferrals(
    con: duckdb.DuckDBPyConnection,
) -> pd.DataFrame:
    """
    Read PENDING calendar deferrals that should be resurfaced today.

    Returns only deferrals from prior trading days (not today's).
    """
    initialize_calendar_deferred_table(con)

    try:
        df = con.execute("""
            SELECT *
            FROM calendar_deferred
            WHERE status = 'PENDING'
              AND deferred_date < CURRENT_DATE
              AND deferred_date >= CURRENT_DATE - INTERVAL ? DAY
            ORDER BY dqs_score DESC
        """, [_MAX_AGE_DAYS]).fetchdf()
        if not df.empty:
            logger.info(f"[CalendarDeferral] Found {len(df)} pending deferrals from prior sessions")
        return df
    except Exception as e:
        logger.warning(f"[CalendarDeferral] Read failed (non-critical): {e}")
        return pd.DataFrame()


def mark_deferrals_filled(
    tickers_strategies: list[tuple[str, str]],
    con: duckdb.DuckDBPyConnection,
) -> int:
    """Mark deferred candidates as FILLED when they reappear in today's scan."""
    if not tickers_strategies:
        return 0

    count = 0
    today = datetime.now(timezone.utc).date()
    for ticker, strategy in tickers_strategies:
        try:
            con.execute("""
                UPDATE calendar_deferred
                SET status = 'FILLED', resume_date = ?
                WHERE ticker = ? AND strategy_name = ? AND status = 'PENDING'
            """, [today, ticker, strategy])
            count += 1
        except Exception:
            pass
    if count:
        logger.info(f"[CalendarDeferral] Marked {count} deferrals as FILLED")
    return count


def expire_stale_deferrals(con: duckdb.DuckDBPyConnection) -> int:
    """Expire deferrals older than _MAX_AGE_DAYS."""
    initialize_calendar_deferred_table(con)

    try:
        result = con.execute("""
            UPDATE calendar_deferred
            SET status = 'EXPIRED'
            WHERE status = 'PENDING'
              AND deferred_date < CURRENT_DATE - INTERVAL ? DAY
        """, [_MAX_AGE_DAYS])
        expired = result.fetchone()
        # DuckDB UPDATE doesn't return row count directly; use a count query
        count = con.execute("""
            SELECT COUNT(*) FROM calendar_deferred
            WHERE status = 'EXPIRED'
              AND deferred_date < CURRENT_DATE - INTERVAL ? DAY
        """, [_MAX_AGE_DAYS]).fetchone()[0]
        if count:
            logger.info(f"[CalendarDeferral] Expired {count} stale deferrals")
        return count
    except Exception as e:
        logger.debug(f"[CalendarDeferral] Expire failed: {e}")
        return 0
