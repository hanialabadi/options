"""
IV Term History: Constant-Maturity IV History Table

PURPOSE:
    Authoritative daily IV history for all tickers, sourced from Schwab API.
    Stores constant-maturity IV points (7D, 14D, 30D, 60D, 90D, 120D, 180D, 360D)
    for historical analysis and IV Rank calculation.

DESIGN PRINCIPLES:
    - DuckDB is the single source of truth for IV history
    - Scan engine reads from this table, never recomputes
    - Daily collection job appends new data
    - Schwab API is the sole IV source

SCHEMA:
    - ticker (VARCHAR): Stock symbol
    - date (DATE): Trading date
    - iv_7d (DOUBLE): 7-day constant maturity IV
    - iv_14d (DOUBLE): 14-day constant maturity IV
    - iv_30d (DOUBLE): 30-day constant maturity IV
    - iv_60d (DOUBLE): 60-day constant maturity IV
    - iv_90d (DOUBLE): 90-day constant maturity IV
    - iv_120d (DOUBLE): 120-day constant maturity IV
    - iv_180d (DOUBLE): 180-day constant maturity IV
    - iv_360d (DOUBLE): 360-day constant maturity IV
    - source (VARCHAR): 'schwab'
    - created_at (TIMESTAMP): Record creation timestamp

PRIMARY KEY: (ticker, date)
"""

import duckdb
import pandas as pd
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# iv_intraday_stream table
# ---------------------------------------------------------------------------
# This is the WRITE target for the streamer collector.
# It is NOT the canonical surface — it is a raw time-series of IV pushes.
#
# The canonical daily surface (iv_term_history) is only updated by:
#   1. rest_collector — once at market open
#   2. merge_intraday_surface() — the 15:45 ET snapshot merger job
#
# Schema:
#   ticker    VARCHAR   — equity symbol
#   ts        TIMESTAMP — push timestamp (UTC)
#   trade_date DATE     — trading date (for partitioning)
#   bucket    INTEGER   — maturity bucket (7, 14, 30, 60, 90, 120, 180, 360)
#   iv        DOUBLE    — implied volatility (Schwab field 10, raw %)
#   source    VARCHAR   — 'schwab_streamer'
#   atm_symbol VARCHAR  — Schwab streamer symbol used (e.g. "AAPL  260225C00265000")
#
# PRIMARY KEY: none — append-only time-series


def initialize_iv_surface_meta_table(con: duckdb.DuckDBPyConnection) -> None:
    """
    Create iv_surface_meta table if it doesn't exist.

    Stores per-bucket contract metadata alongside each daily IV surface row.
    Provides auditability for ATM selection, expiry matching, and data quality.

    PRIMARY KEY: (ticker, date, bucket)

    Schema:
        ticker      VARCHAR   — equity symbol
        date        DATE      — trading date
        bucket      INTEGER   — maturity bucket (7, 14, 30, 60, 90, 120, 180, 360)
        atm_strike  DOUBLE    — ATM strike used for IV extraction
        actual_dte  INTEGER   — actual DTE of expiry selected for this bucket
        dte_gap     INTEGER   — |actual_dte - bucket| (0 = perfect match)
        tolerance   INTEGER   — max(bucket*0.25, 7) acceptance window used
        spot_used   DOUBLE    — stock price at collection time
        chain_size  INTEGER   — number of expirations in chain response
        source      VARCHAR   — 'schwab_rest' or 'schwab_merged'
        created_at  TIMESTAMP — record creation time
    """
    con.execute("""
        CREATE TABLE IF NOT EXISTS iv_surface_meta (
            ticker      VARCHAR   NOT NULL,
            date        DATE      NOT NULL,
            bucket      INTEGER   NOT NULL,
            atm_strike  DOUBLE,
            actual_dte  INTEGER,
            dte_gap     INTEGER,
            tolerance   INTEGER,
            spot_used   DOUBLE,
            chain_size  INTEGER,
            source      VARCHAR   DEFAULT 'schwab_rest',
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ticker, date, bucket)
        )
    """)
    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_surface_meta_ticker_date
        ON iv_surface_meta(ticker, date DESC)
    """)
    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_surface_meta_date
        ON iv_surface_meta(date DESC)
    """)
    logger.debug("iv_surface_meta table initialized")


def append_surface_meta(
    con: duckdb.DuckDBPyConnection,
    rows: list[dict],
    trade_date,
) -> None:
    """
    Upsert per-bucket contract metadata into iv_surface_meta.

    Each row must have:
        ticker   str
        bucket   int
        atm_strike  float | None
        actual_dte  int | None
        dte_gap     int | None    (|actual_dte - bucket|)
        tolerance   int | None
        spot_used   float | None
        chain_size  int | None
    Optional: source (defaults to 'schwab_rest')
    """
    if not rows:
        return

    clean = []
    for r in rows:
        clean.append({
            "ticker":     r["ticker"],
            "date":       trade_date,
            "bucket":     int(r["bucket"]),
            "atm_strike": r.get("atm_strike"),
            "actual_dte": r.get("actual_dte"),
            "dte_gap":    r.get("dte_gap"),
            "tolerance":  r.get("tolerance"),
            "spot_used":  r.get("spot_used"),
            "chain_size": r.get("chain_size"),
            "source":     r.get("source", "schwab_rest"),
        })

    if not clean:
        return

    df_insert = pd.DataFrame(clean)
    con.execute("""
        INSERT INTO iv_surface_meta
            (ticker, date, bucket, atm_strike, actual_dte, dte_gap, tolerance,
             spot_used, chain_size, source)
        SELECT * FROM df_insert
        ON CONFLICT (ticker, date, bucket) DO UPDATE SET
            atm_strike = EXCLUDED.atm_strike,
            actual_dte = EXCLUDED.actual_dte,
            dte_gap    = EXCLUDED.dte_gap,
            tolerance  = EXCLUDED.tolerance,
            spot_used  = EXCLUDED.spot_used,
            chain_size = EXCLUDED.chain_size,
            source     = EXCLUDED.source
    """)
    logger.debug("Upserted %d rows into iv_surface_meta", len(clean))


def initialize_iv_intraday_stream_table(con: duckdb.DuckDBPyConnection) -> None:
    """Create iv_intraday_stream table if it doesn't exist."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS iv_intraday_stream (
            ticker      VARCHAR   NOT NULL,
            ts          TIMESTAMP NOT NULL,
            trade_date  DATE      NOT NULL,
            bucket      INTEGER   NOT NULL,
            iv          DOUBLE    NOT NULL,
            source      VARCHAR   DEFAULT 'schwab_streamer',
            atm_symbol  VARCHAR
        )
    """)
    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_intraday_ticker_date
        ON iv_intraday_stream(ticker, trade_date DESC)
    """)
    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_intraday_date_bucket
        ON iv_intraday_stream(trade_date DESC, bucket)
    """)
    logger.debug("iv_intraday_stream table initialized")


def append_intraday_stream_rows(
    con: duckdb.DuckDBPyConnection,
    rows: list[dict],
) -> None:
    """
    Append raw streamer IV pushes to iv_intraday_stream.

    Each row must have:
        ticker, ts (datetime), trade_date (date), bucket (int), iv (float)
    Optional: source, atm_symbol

    Rows with iv <= 0 are silently dropped.
    """
    if not rows:
        return

    clean = []
    for r in rows:
        iv = r.get("iv", 0)
        if iv is None or iv <= 0:
            continue
        clean.append({
            "ticker":     r["ticker"],
            "ts":         r.get("ts", datetime.utcnow()),
            "trade_date": r.get("trade_date", date.today()),
            "bucket":     int(r["bucket"]),
            "iv":         float(iv),
            "source":     r.get("source", "schwab_streamer"),
            "atm_symbol": r.get("atm_symbol"),
        })

    if not clean:
        return

    df_insert = pd.DataFrame(clean)
    con.execute("INSERT INTO iv_intraday_stream SELECT * FROM df_insert")
    logger.debug("Appended %d rows to iv_intraday_stream", len(clean))


def get_latest_intraday_iv(
    con: duckdb.DuckDBPyConnection,
    trade_date: date,
    tickers: Optional[List[str]] = None,
    bucket: int = 30,
) -> pd.DataFrame:
    """
    Get the most recent streamer push per ticker for a given bucket and date.

    Used by the merger job to get the latest intraday IV reading.

    Returns DataFrame with columns: ticker, iv, ts, atm_symbol
    """
    where_parts = ["trade_date = ?", "bucket = ?"]
    params: list = [trade_date, bucket]

    if tickers:
        placeholders = ",".join(["?"] * len(tickers))
        where_parts.append(f"ticker IN ({placeholders})")
        params.extend(tickers)

    where = " AND ".join(where_parts)

    return con.execute(f"""
        WITH ranked AS (
            SELECT ticker, iv, ts, atm_symbol,
                   ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY ts DESC) AS rn
            FROM iv_intraday_stream
            WHERE {where}
        )
        SELECT ticker, iv, ts, atm_symbol
        FROM ranked
        WHERE rn = 1
        ORDER BY ticker
    """, params).df()


def get_intraday_stream_summary(
    con: duckdb.DuckDBPyConnection,
    trade_date: date,
) -> dict:
    """Return summary stats for a trading day's stream."""
    row = con.execute("""
        SELECT
            COUNT(*)           AS total_pushes,
            COUNT(DISTINCT ticker) AS tickers,
            MIN(ts)            AS first_push,
            MAX(ts)            AS last_push,
            AVG(iv)            AS avg_iv,
            MIN(iv)            AS min_iv,
            MAX(iv)            AS max_iv
        FROM iv_intraday_stream
        WHERE trade_date = ?
    """, [trade_date]).fetchone()

    return {
        "total_pushes": row[0],
        "tickers":      row[1],
        "first_push":   row[2],
        "last_push":    row[3],
        "avg_iv":       round(row[4], 2) if row[4] else None,
        "min_iv":       row[5],
        "max_iv":       row[6],
    }


def get_iv_history_db_path() -> Path:
    """Get path to IV history DuckDB database."""
    from core.shared.data_contracts.config import IV_HISTORY_DB_PATH
    db_path = IV_HISTORY_DB_PATH
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return db_path


def initialize_iv_term_history_table(con: duckdb.DuckDBPyConnection):
    """
    Initialize the iv_term_history table with proper schema.

    Args:
        con: DuckDB connection
    """
    con.execute("""
        CREATE TABLE IF NOT EXISTS iv_term_history (
            ticker VARCHAR NOT NULL,
            date DATE NOT NULL,
            iv_7d DOUBLE,
            iv_14d DOUBLE,
            iv_21d DOUBLE,
            iv_30d DOUBLE,
            iv_60d DOUBLE,
            iv_90d DOUBLE,
            iv_120d DOUBLE,
            iv_180d DOUBLE,
            iv_360d DOUBLE,
            source VARCHAR DEFAULT 'schwab',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ticker, date)
        )
    """)

    # Create indexes for fast queries
    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_iv_term_ticker_date
        ON iv_term_history(ticker, date DESC)
    """)

    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_iv_term_date
        ON iv_term_history(date DESC)
    """)

    # Migrate existing table: add iv_21d column if absent (idempotent)
    existing_cols = [r[1] for r in con.execute("PRAGMA table_info('iv_term_history')").fetchall()]
    if 'iv_21d' not in existing_cols:
        con.execute("ALTER TABLE iv_term_history ADD COLUMN iv_21d DOUBLE")
        logger.info("✅ Migrated iv_term_history: added iv_21d column")

    logger.info("✅ iv_term_history table initialized")


def append_daily_iv_data(
    con: duckdb.DuckDBPyConnection,
    df_iv_data: pd.DataFrame,
    trade_date: Optional[date] = None
):
    """
    Append daily IV data to iv_term_history table.

    Args:
        con: DuckDB connection
        df_iv_data: DataFrame with columns [ticker, iv_7d, iv_14d, iv_21d, iv_30d, ...]
        trade_date: Trading date (defaults to today)
    """
    if df_iv_data.empty:
        logger.warning("No IV data to append")
        return

    if trade_date is None:
        trade_date = date.today()

    # Add date column
    df_iv_data = df_iv_data.copy()
    df_iv_data['date'] = trade_date

    # Ensure required columns exist
    required_cols = ['ticker', 'date']
    iv_cols = ['iv_7d', 'iv_14d', 'iv_21d', 'iv_30d', 'iv_60d', 'iv_90d', 'iv_120d', 'iv_180d', 'iv_360d']

    for col in iv_cols:
        if col not in df_iv_data.columns:
            df_iv_data[col] = None

    # Add source column
    if 'source' not in df_iv_data.columns:
        df_iv_data['source'] = 'schwab'

    # Select only relevant columns
    insert_cols = required_cols + iv_cols + ['source']
    df_insert = df_iv_data[insert_cols]

    # Insert with conflict handling (ON CONFLICT UPDATE)
    con.execute("""
        INSERT INTO iv_term_history (ticker, date, iv_7d, iv_14d, iv_21d, iv_30d, iv_60d, iv_90d, iv_120d, iv_180d, iv_360d, source)
        SELECT * FROM df_insert
        ON CONFLICT (ticker, date) DO UPDATE SET
            iv_7d = EXCLUDED.iv_7d,
            iv_14d = EXCLUDED.iv_14d,
            iv_21d = EXCLUDED.iv_21d,
            iv_30d = EXCLUDED.iv_30d,
            iv_60d = EXCLUDED.iv_60d,
            iv_90d = EXCLUDED.iv_90d,
            iv_120d = EXCLUDED.iv_120d,
            iv_180d = EXCLUDED.iv_180d,
            iv_360d = EXCLUDED.iv_360d,
            source = EXCLUDED.source
    """)

    logger.info(f"✅ Appended {len(df_insert)} ticker IV records for {trade_date}")


def get_iv_history_depth(
    con: duckdb.DuckDBPyConnection,
    ticker: str
) -> int:
    """
    Get number of TRADING days (Mon-Fri only) of IV history available for a ticker.

    CORRECTED (2026-02-03): Excludes weekends to prevent calendar contamination.
    Post-forensic audit: Database contains calendar days (including weekends).
    This function now counts only trading days for accurate maturity assessment.

    Args:
        con: DuckDB connection
        ticker: Stock symbol

    Returns:
        Number of trading days (Mon-Fri) of history available
    """
    result = con.execute("""
        SELECT COUNT(DISTINCT date) as trading_days
        FROM iv_term_history
        WHERE ticker = ?
        AND CAST(strftime('%w', date) AS INTEGER) NOT IN (0, 6)  -- Exclude Sundays (0) and Saturdays (6)
    """, [ticker]).fetchone()

    return result[0] if result else 0


def get_latest_iv_data(
    con: duckdb.DuckDBPyConnection,
    tickers: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Get latest IV data for specified tickers.

    Args:
        con: DuckDB connection
        tickers: List of tickers (None = all tickers)

    Returns:
        DataFrame with latest IV data per ticker
    """
    if tickers:
        ticker_list = "'" + "','".join(tickers) + "'"
        where_clause = f"WHERE ticker IN ({ticker_list})"
    else:
        where_clause = ""

    df = con.execute(f"""
        WITH latest_dates AS (
            SELECT ticker, MAX(date) as max_date
            FROM iv_term_history
            {where_clause}
            GROUP BY ticker
        )
        SELECT
            h.*,
            (SELECT COUNT(DISTINCT date)
             FROM iv_term_history h2
             WHERE h2.ticker = h.ticker) as history_depth
        FROM iv_term_history h
        INNER JOIN latest_dates ld
            ON h.ticker = ld.ticker AND h.date = ld.max_date
        ORDER BY h.ticker
    """).df()

    return df


def calculate_iv_rank(
    con: duckdb.DuckDBPyConnection,
    ticker: str,
    current_iv: float,
    lookback_days: int = 252
) -> Tuple[float, int]:
    """
    Calculate IV Rank for a ticker based on historical data.

    IV Rank = (Current IV - Min IV) / (Max IV - Min IV) * 100

    Args:
        con: DuckDB connection
        ticker: Stock symbol
        current_iv: Current 30-day IV
        lookback_days: Historical lookback period (default: 252 trading days)

    Returns:
        (iv_rank, history_depth): IV Rank percentage and actual days of history
    """
    # Query historical IV_30d data (TRADING DAYS ONLY - excludes weekends)
    # CORRECTED (2026-02-03): Filter weekends to prevent calendar contamination
    df_history = con.execute("""
        SELECT iv_30d, date
        FROM iv_term_history
        WHERE ticker = ?
        AND iv_30d IS NOT NULL
        AND CAST(strftime('%w', date) AS INTEGER) NOT IN (0, 6)  -- Exclude weekends
        ORDER BY date DESC
        LIMIT ?
    """, [ticker, lookback_days]).df()

    if df_history.empty:
        return 0.0, 0

    # INVARIANT CHECK: Ensure unique trading days (no duplicate intraday inserts)
    unique_dates = df_history['date'].nunique()
    total_rows = len(df_history)

    if unique_dates != total_rows:
        logger.warning(
            f"⚠️ INVARIANT VIOLATION: {ticker} has {total_rows} rows but only {unique_dates} unique dates. "
            f"Duplicate intraday inserts detected!"
        )
        # Use unique dates count as the authoritative history depth
        history_depth = unique_dates
    else:
        history_depth = total_rows

    if history_depth < 120:  # Minimum 120 days for reliable percentiles (Hull Ch.15)
        return 0.0, history_depth

    min_iv = df_history['iv_30d'].min()
    max_iv = df_history['iv_30d'].max()

    if max_iv <= min_iv:  # Avoid division by zero
        return 50.0, history_depth

    iv_rank = ((current_iv - min_iv) / (max_iv - min_iv)) * 100

    return iv_rank, history_depth


def get_history_summary(con: duckdb.DuckDBPyConnection) -> Dict[str, any]:
    """
    Get summary statistics for IV history database.

    Returns:
        Dictionary with summary metrics
    """
    summary = {}

    # Total tickers
    result = con.execute("SELECT COUNT(DISTINCT ticker) FROM iv_term_history").fetchone()
    summary['total_tickers'] = result[0] if result else 0

    # Date range
    result = con.execute("SELECT MIN(date), MAX(date) FROM iv_term_history").fetchone()
    if result and result[0]:
        summary['earliest_date'] = result[0]
        summary['latest_date'] = result[1]
        summary['date_range_days'] = (result[1] - result[0]).days
    else:
        summary['earliest_date'] = None
        summary['latest_date'] = None
        summary['date_range_days'] = 0

    # Depth distribution
    df_depth = con.execute("""
        SELECT
            ticker,
            COUNT(DISTINCT date) as depth
        FROM iv_term_history
        GROUP BY ticker
    """).df()

    if not df_depth.empty:
        summary['avg_depth'] = df_depth['depth'].mean()
        summary['median_depth'] = df_depth['depth'].median()
        summary['min_depth'] = df_depth['depth'].min()
        summary['max_depth'] = df_depth['depth'].max()
        summary['tickers_120plus'] = (df_depth['depth'] >= 120).sum()
        summary['tickers_252plus'] = (df_depth['depth'] >= 252).sum()
    else:
        summary['avg_depth'] = 0
        summary['median_depth'] = 0
        summary['min_depth'] = 0
        summary['max_depth'] = 0
        summary['tickers_120plus'] = 0
        summary['tickers_252plus'] = 0

    return summary
