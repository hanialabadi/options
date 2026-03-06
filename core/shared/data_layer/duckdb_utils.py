import duckdb
import pandas as pd
import logging # Added missing import
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

import os
from core.shared.data_contracts.config import PIPELINE_DB_PATH, DEBUG_PIPELINE_DB_PATH

logger = logging.getLogger(__name__)

def _debug_mode_enabled() -> bool:
    return os.getenv("PIPELINE_DEBUG") == "1" or os.getenv("DEBUG_TICKER_MODE") == "1"

_DEBUG_SHARED_CONNECTION = None

class _DebugConnectionWrapper:
    def __init__(self, con):
        self._con = con

    def __getattr__(self, name):
        return getattr(self._con, name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def close(self):
        # Debug uses a shared connection; do not close here.
        return None

def _get_debug_shared_connection() -> _DebugConnectionWrapper:
    global _DEBUG_SHARED_CONNECTION
    if _DEBUG_SHARED_CONNECTION is None:
        db_file_path = DEBUG_PIPELINE_DB_PATH
        db_file_path.parent.mkdir(parents=True, exist_ok=True)
        _DEBUG_SHARED_CONNECTION = duckdb.connect(database=str(db_file_path), read_only=False)
    return _DebugConnectionWrapper(_DEBUG_SHARED_CONNECTION)

def _resolve_db_path(db_path: Optional[str]) -> Path:
    if db_path is None:
        return DEBUG_PIPELINE_DB_PATH if _debug_mode_enabled() else PIPELINE_DB_PATH

    try:
        resolved = Path(db_path)
        if _debug_mode_enabled() and resolved == PIPELINE_DB_PATH:
            return DEBUG_PIPELINE_DB_PATH
        return resolved
    except Exception:
        return DEBUG_PIPELINE_DB_PATH if _debug_mode_enabled() else PIPELINE_DB_PATH

def get_duckdb_connection(
    db_path: Optional[str] = None,
    read_only: bool = True
) -> duckdb.DuckDBPyConnection:
    """
    Establishes and returns a DuckDB connection.

    When read_only=True and a stale write-lock is detected from a crashed writer
    (PID dead, OS lock released but DuckDB header still marked dirty), automatically
    performs a WAL recovery via a brief write-mode open + CHECKPOINT, then retries
    the read-only connection. This prevents the dashboard from failing after an
    unclean pipeline shutdown.
    """
    if _debug_mode_enabled():
        return _get_debug_shared_connection()

    db_file_path = _resolve_db_path(db_path)
    db_file_path.parent.mkdir(parents=True, exist_ok=True)
    mode = "read-only" if read_only else "read-write"
    logger.debug(f"DEBUG: Attempting to connect to DuckDB ({mode}): {db_file_path}")

    try:
        return duckdb.connect(database=str(db_file_path), read_only=read_only)
    except Exception as e:
        if read_only and ("Conflicting lock" in str(e) or "lock" in str(e).lower()):
            logger.warning(
                f"[DuckDB] Stale write-lock on {db_file_path.name} — "
                f"attempting WAL recovery (original error: {e})"
            )
            try:
                _rc = duckdb.connect(database=str(db_file_path), read_only=False)
                _rc.execute("CHECKPOINT")
                _rc.close()
                logger.info(f"[DuckDB] WAL recovery complete — retrying read-only open.")
                return duckdb.connect(database=str(db_file_path), read_only=True)
            except Exception as recover_err:
                logger.error(f"[DuckDB] WAL recovery failed: {recover_err}")
                raise
        raise

def connect_read_only(db_path: str) -> duckdb.DuckDBPyConnection:
    """
    Convenience wrapper: opens a DuckDB file by path string in read-only mode
    with automatic WAL recovery on stale-lock errors.

    Intended for dashboard views that hold a direct path (not using PIPELINE_DB_PATH).
    Use as a drop-in replacement for duckdb.connect(path, read_only=True).
    """
    return get_duckdb_connection(db_path=db_path, read_only=True)


def get_duckdb_write_connection(db_path: Optional[str] = None) -> duckdb.DuckDBPyConnection:
    """Establishes and returns a read-write DuckDB connection."""
    if _debug_mode_enabled():
        return _get_debug_shared_connection()
    return get_duckdb_connection(db_path=db_path, read_only=False)

def _table_exists(con, table_name: str) -> bool:
    """Checks if a table exists in the connected DuckDB database."""
    return con.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables 
        WHERE table_name = '{table_name}' AND table_schema = 'main'
    """).fetchone()[0] > 0

def _column_exists(con, table_name: str, column_name: str) -> bool:
    """Checks if a column exists in a given table."""
    return con.execute(f"""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_name = '{table_name}' AND column_name = '{column_name}' AND table_schema = 'main'
    """).fetchone()[0] > 0

def fetch_historical_legs_data(
    trade_ids: List[str],
    days_ago: int,
    db_path: Optional[str] = None
) -> Optional[pd.DataFrame]:
    """
    Fetches historical leg data for given trade IDs from management_recommendations,
    closest to a specified number of days ago.

    Source: management_recommendations (data/pipeline.duckdb) — authoritative daily
    Greek snapshots written by run_all.py on every engine run. One deduplicated row
    per LegID per calendar day (latest Snapshot_TS within the day wins).

    Falls back gracefully: returns None if table missing or no data in window.
    """
    try:
        with get_duckdb_connection(db_path, read_only=True) as con:
            if not _table_exists(con, 'management_recommendations'):
                logger.warning("management_recommendations not found — windowed ROC unavailable.")
                return None

            placeholders = ', '.join(['?' for _ in trade_ids])
            query = f"""
                WITH daily AS (
                    -- One row per LegID per calendar day (latest Snapshot_TS wins)
                    SELECT
                        LegID,
                        Delta, Gamma, Vega, Theta, "UL Last", IV_Now,
                        Snapshot_TS,
                        Snapshot_TS::DATE AS snap_date,
                        ABS(DATE_DIFF('day', Snapshot_TS::DATE, CURRENT_DATE) - {days_ago}) AS day_diff
                    FROM management_recommendations
                    WHERE TradeID IN ({placeholders})
                      AND LegID IS NOT NULL
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY LegID, Snapshot_TS::DATE
                        ORDER BY Snapshot_TS DESC
                    ) = 1
                ),
                in_window AS (
                    -- Closest day to target, within ±2 calendar days
                    SELECT *
                    FROM daily
                    WHERE snap_date <= (CURRENT_DATE - INTERVAL '{days_ago} day')
                      AND day_diff <= 2
                )
                SELECT *
                FROM in_window
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY LegID
                    ORDER BY day_diff ASC, Snapshot_TS DESC
                ) = 1
            """
            result = con.execute(query, trade_ids).df()
            if not result.empty:
                logger.debug(f"fetch_historical_legs_data({days_ago}d): {len(result)} legs found")
            return result if not result.empty else None
    except Exception as e:
        logger.error(f"Error fetching historical legs data: {e}")
        return None


def fetch_drift_history_for_smoothing(
    trade_ids: List[str],
    db_path: Optional[str] = None
) -> Optional[pd.DataFrame]:
    """
    Fetches per-TradeID daily delta drift history for smoothing (SMA, acceleration,
    stability) from management_recommendations.

    Returns one row per TradeID with:
      snapshot_count       — number of distinct days available
      delta_drift_sma_3    — 3-day SMA of delta drift vs entry
      delta_drift_accel    — change in delta drift day-over-day (latest - prior)
      delta_drift_stability — stddev of delta drift over last 5 days
    """
    try:
        with get_duckdb_connection(db_path, read_only=True) as con:
            if not _table_exists(con, 'management_recommendations'):
                logger.warning("management_recommendations not found — drift smoothing unavailable.")
                return None

            placeholders = ', '.join(['?' for _ in trade_ids])
            query = f"""
                WITH daily AS (
                    -- One deduplicated row per TradeID per calendar day
                    SELECT
                        TradeID,
                        Delta,
                        Delta_Entry,
                        Snapshot_TS::DATE AS snap_date,
                        ROW_NUMBER() OVER (
                            PARTITION BY TradeID, Snapshot_TS::DATE
                            ORDER BY Snapshot_TS DESC
                        ) AS rn
                    FROM management_recommendations
                    WHERE TradeID IN ({placeholders})
                      AND Delta IS NOT NULL
                      AND Delta_Entry IS NOT NULL
                ),
                deduped AS (
                    SELECT TradeID, snap_date,
                           (Delta - Delta_Entry) AS delta_drift
                    FROM daily WHERE rn = 1
                ),
                ranked AS (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY TradeID ORDER BY snap_date DESC
                           ) AS recency
                    FROM deduped
                )
                SELECT
                    TradeID,
                    COUNT(*)                                                        AS snapshot_count,
                    AVG(delta_drift)    FILTER (WHERE recency <= 3)                AS delta_drift_sma_3,
                    MAX(delta_drift)    FILTER (WHERE recency = 1)
                        - MAX(delta_drift) FILTER (WHERE recency = 2)              AS delta_drift_accel,
                    STDDEV(delta_drift) FILTER (WHERE recency <= 5)                AS delta_drift_stability
                FROM ranked
                GROUP BY TradeID
            """
            result = con.execute(query, trade_ids).df()
            return result if not result.empty else None
    except Exception as e:
        logger.error(f"Error fetching drift smoothing history: {e}")
        return None

PRICE_HISTORY_METADATA_TABLE = "price_history_metadata"

# Module-level flag to ensure initialization runs only once
_metadata_table_initialized = False

def initialize_price_history_metadata_table():
    """Initializes or updates the price_history_metadata table in DuckDB."""
    global _metadata_table_initialized
    if _metadata_table_initialized:
        logger.debug(f"Table {PRICE_HISTORY_METADATA_TABLE} already initialized. Skipping.")
        return

    try:
        # Ensure a non-read-only connection is used and properly closed.
        with get_duckdb_write_connection() as con:
            if not _table_exists(con, PRICE_HISTORY_METADATA_TABLE):
                con.execute(f"""
                    CREATE TABLE {PRICE_HISTORY_METADATA_TABLE} (
                        Ticker VARCHAR PRIMARY KEY,
                        Last_Fetch_TS TIMESTAMP,
                        Source VARCHAR,
                        Days_History INTEGER,
                        Backoff_Until TIMESTAMP NULL
                    )
                """)
                logger.info(f"✅ {PRICE_HISTORY_METADATA_TABLE} table initialized in DuckDB.")
            else:
                # Check if Backoff_Until column exists, if not, add it
                if not _column_exists(con, PRICE_HISTORY_METADATA_TABLE, 'Backoff_Until'):
                    con.execute(f"ALTER TABLE {PRICE_HISTORY_METADATA_TABLE} ADD COLUMN Backoff_Until TIMESTAMP NULL")
                    logger.info(f"✅ Added 'Backoff_Until' column to {PRICE_HISTORY_METADATA_TABLE} table.")
                logger.debug(f"Table {PRICE_HISTORY_METADATA_TABLE} already exists. Skipping full initialization.")
        _metadata_table_initialized = True
    except Exception as e:
        logger.error(f"❌ Failed to initialize or update {PRICE_HISTORY_METADATA_TABLE} table: {e}")

# The table initialization should be called explicitly at application startup or before first use,
# not automatically at module import, to prevent potential connection conflicts.
# initialize_price_history_metadata_table()

PRICE_HISTORY_TABLE = "price_history"

def initialize_price_history_table():
    """
    Initializes the price_history table in DuckDB for OHLC data caching.

    This table stores OHLC data fetched from Yahoo Finance (via yf_fetch.py)
    and is queried by load_price_history() as a cache layer.
    """
    try:
        with get_duckdb_write_connection() as con:
            if not _table_exists(con, PRICE_HISTORY_TABLE):
                con.execute(f"""
                    CREATE TABLE {PRICE_HISTORY_TABLE} (
                        ticker VARCHAR,
                        date DATE,
                        open_price DOUBLE,
                        high_price DOUBLE,
                        low_price DOUBLE,
                        close_price DOUBLE,
                        volume BIGINT,
                        source VARCHAR DEFAULT 'YAHOO_FINANCE',
                        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (ticker, date)
                    )
                """)
                logger.info(f"✅ {PRICE_HISTORY_TABLE} table initialized in DuckDB.")
            else:
                logger.debug(f"Table {PRICE_HISTORY_TABLE} already exists. Skipping initialization.")
    except Exception as e:
        logger.error(f"❌ Failed to initialize {PRICE_HISTORY_TABLE} table: {e}")
