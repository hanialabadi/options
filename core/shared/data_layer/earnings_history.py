"""
Earnings History: Historical EPS Data + Summary Stats

Core module for earnings analytics. Owns:
    - earnings_history table (raw EPS data from yfinance)
    - earnings_stats table (per-ticker summary)
    - initialize_tables() — creates ALL earnings tables (delegates to submodules)

Submodules:
    - earnings_iv_crush.py — IV crush + expected/actual move analytics
    - earnings_formation.py — Phase 1→2→3 formation detection

DESIGN PRINCIPLES:
    - DuckDB is the single source of truth
    - Management engine reads from earnings_stats (one row per ticker)
    - Non-blocking: missing data never blocks trades
    - ETFs excluded (no earnings)
"""

import duckdb
import math
import pandas as pd
from datetime import date, datetime
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

# Re-export IV crush functions for backward compatibility
from core.shared.data_layer.earnings_iv_crush import (  # noqa: F401
    _nearest_iv_reading,
    _nearest_price,
    _compute_iv_ramp_start,
    compute_iv_crush_for_event,
)


# ---------------------------------------------------------------------------
# Table initialization (all earnings tables)
# ---------------------------------------------------------------------------

def initialize_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Create all earnings tables + indexes (idempotent)."""

    # 1. Raw earnings history
    con.execute("""
        CREATE TABLE IF NOT EXISTS earnings_history (
            ticker            VARCHAR NOT NULL,
            earnings_date     DATE NOT NULL,
            fiscal_quarter    VARCHAR,
            eps_estimate      DOUBLE,
            eps_actual        DOUBLE,
            eps_surprise_pct  DOUBLE,
            beat_miss         VARCHAR,
            source            VARCHAR DEFAULT 'yfinance',
            created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ticker, earnings_date)
        )
    """)
    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_earnings_ticker_date
        ON earnings_history(ticker, earnings_date DESC)
    """)
    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_earnings_date
        ON earnings_history(earnings_date DESC)
    """)

    # 2. IV crush table (delegated to submodule)
    from core.shared.data_layer.earnings_iv_crush import create_iv_crush_table
    create_iv_crush_table(con)

    # 3. Per-ticker summary (single read target)
    con.execute("""
        CREATE TABLE IF NOT EXISTS earnings_stats (
            ticker                  VARCHAR NOT NULL PRIMARY KEY,
            quarters_available      INTEGER,
            beat_rate               DOUBLE,
            miss_rate               DOUBLE,
            avg_surprise_pct        DOUBLE,
            avg_iv_crush_pct        DOUBLE,
            avg_iv_buildup_pct      DOUBLE,
            avg_iv_ramp_start_days  DOUBLE,
            avg_expected_move_pct   DOUBLE,
            avg_actual_move_pct     DOUBLE,
            avg_move_ratio          DOUBLE,
            avg_gap_pct             DOUBLE,
            avg_5d_drift_pct        DOUBLE,
            last_earnings_date      DATE,
            last_surprise_pct       DOUBLE,
            last_beat_miss          VARCHAR,
            consecutive_beats       INTEGER,
            consecutive_misses      INTEGER,
            updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 4. Formation tables (delegated to submodule, if available)
    try:
        from core.shared.data_layer.earnings_formation import create_formation_tables
        create_formation_tables(con)
    except ImportError:
        pass  # Formation module not yet created

    logger.info("earnings tables initialized")


# ---------------------------------------------------------------------------
# Beat/Miss classification
# ---------------------------------------------------------------------------

def classify_beat_miss(surprise_pct: Optional[float]) -> str:
    """Classify EPS surprise: BEAT (>1%), MISS (<-1%), INLINE."""
    if surprise_pct is None or (isinstance(surprise_pct, float) and math.isnan(surprise_pct)):
        return "UNKNOWN"
    if surprise_pct > 1.0:
        return "BEAT"
    elif surprise_pct < -1.0:
        return "MISS"
    return "INLINE"


# ---------------------------------------------------------------------------
# Upsert
# ---------------------------------------------------------------------------

def upsert_earnings_batch(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> int:
    """
    Upsert earnings history rows. ON CONFLICT UPDATE.

    Expected columns: ticker, earnings_date, fiscal_quarter, eps_estimate,
                      eps_actual, eps_surprise_pct, beat_miss, source
    Returns: number of rows upserted.
    """
    if df.empty:
        return 0

    df = df.copy()

    # Ensure required columns
    for col in ["fiscal_quarter", "source"]:
        if col not in df.columns:
            df[col] = "yfinance" if col == "source" else None

    if "beat_miss" not in df.columns:
        df["beat_miss"] = df["eps_surprise_pct"].apply(classify_beat_miss)

    # Select columns for insert
    insert_cols = [
        "ticker", "earnings_date", "fiscal_quarter",
        "eps_estimate", "eps_actual", "eps_surprise_pct",
        "beat_miss", "source",
    ]
    df_insert = df[[c for c in insert_cols if c in df.columns]]

    con.execute("""
        INSERT INTO earnings_history
            (ticker, earnings_date, fiscal_quarter, eps_estimate, eps_actual,
             eps_surprise_pct, beat_miss, source)
        SELECT ticker, earnings_date, fiscal_quarter, eps_estimate, eps_actual,
               eps_surprise_pct, beat_miss, source
        FROM df_insert
        ON CONFLICT (ticker, earnings_date) DO UPDATE SET
            fiscal_quarter = EXCLUDED.fiscal_quarter,
            eps_estimate = EXCLUDED.eps_estimate,
            eps_actual = EXCLUDED.eps_actual,
            eps_surprise_pct = EXCLUDED.eps_surprise_pct,
            beat_miss = EXCLUDED.beat_miss,
            source = EXCLUDED.source
    """)

    logger.info(f"Upserted {len(df_insert)} earnings history rows")
    return len(df_insert)


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

def get_earnings_history(
    con: duckdb.DuckDBPyConnection,
    ticker: str,
    limit: int = 8,
) -> pd.DataFrame:
    """Return last N quarters of earnings for a ticker, ordered desc."""
    return con.execute("""
        SELECT ticker, earnings_date, fiscal_quarter, eps_estimate, eps_actual,
               eps_surprise_pct, beat_miss, source
        FROM earnings_history
        WHERE ticker = ?
        ORDER BY earnings_date DESC
        LIMIT ?
    """, [ticker, limit]).df()


def get_ticker_earnings_stats(
    con: duckdb.DuckDBPyConnection,
    ticker: str,
) -> Optional[Dict]:
    """Read pre-computed summary from earnings_stats table. Returns None if absent."""
    rows = con.execute(
        "SELECT * FROM earnings_stats WHERE ticker = ?", [ticker]
    ).df()
    if rows.empty:
        return None
    return rows.iloc[0].to_dict()


def get_all_earnings_stats(
    con: duckdb.DuckDBPyConnection,
    tickers: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Batch read earnings_stats for multiple tickers."""
    if tickers:
        placeholders = ",".join(["?"] * len(tickers))
        return con.execute(
            f"SELECT * FROM earnings_stats WHERE ticker IN ({placeholders})",
            tickers,
        ).df()
    return con.execute("SELECT * FROM earnings_stats").df()


# ---------------------------------------------------------------------------
# Summary stats refresh
# ---------------------------------------------------------------------------

def refresh_earnings_stats(
    con: duckdb.DuckDBPyConnection,
    ticker: str,
) -> Optional[Dict]:
    """Recompute earnings_stats row for a single ticker from history + crush tables."""

    # Get earnings history
    hist = con.execute("""
        SELECT earnings_date, eps_surprise_pct, beat_miss
        FROM earnings_history
        WHERE ticker = ?
        ORDER BY earnings_date DESC
    """, [ticker]).df()

    if hist.empty:
        return None

    quarters = len(hist)
    beats = (hist["beat_miss"] == "BEAT").sum()
    misses = (hist["beat_miss"] == "MISS").sum()
    beat_rate = beats / quarters if quarters > 0 else 0.0
    miss_rate = misses / quarters if quarters > 0 else 0.0

    # Avg surprise (excluding NaN)
    valid_surprise = hist["eps_surprise_pct"].dropna()
    avg_surprise = float(valid_surprise.mean()) if len(valid_surprise) > 0 else None

    # Consecutive streak (from most recent)
    consecutive_beats = 0
    consecutive_misses = 0
    for bm in hist["beat_miss"].values:
        if bm == "BEAT":
            if consecutive_misses == 0:
                consecutive_beats += 1
            else:
                break
        elif bm == "MISS":
            if consecutive_beats == 0:
                consecutive_misses += 1
            else:
                break
        else:
            break  # INLINE breaks streak

    # Last earnings
    last_row = hist.iloc[0]
    last_date = last_row["earnings_date"]
    last_surprise = last_row["eps_surprise_pct"]
    last_bm = last_row["beat_miss"]

    # Get crush analytics (may not exist for all events)
    crush = con.execute("""
        SELECT iv_crush_pct, iv_buildup_pct, iv_ramp_start_days,
               expected_move_pct, actual_move_pct, move_ratio,
               gap_pct, move_5d_pct
        FROM earnings_iv_crush
        WHERE ticker = ?
          AND iv_data_quality != 'MISSING'
    """, [ticker]).df()

    def _safe_mean(series):
        valid = series.dropna()
        return float(valid.mean()) if len(valid) > 0 else None

    avg_crush = _safe_mean(crush["iv_crush_pct"]) if not crush.empty else None
    avg_buildup = _safe_mean(crush["iv_buildup_pct"]) if not crush.empty else None
    avg_ramp = _safe_mean(crush["iv_ramp_start_days"].astype(float)) if not crush.empty else None
    avg_expected = _safe_mean(crush["expected_move_pct"]) if not crush.empty else None
    avg_actual = _safe_mean(crush["actual_move_pct"]) if not crush.empty else None
    avg_ratio = _safe_mean(crush["move_ratio"]) if not crush.empty else None
    avg_gap = float(crush["gap_pct"].abs().dropna().mean()) if not crush.empty and len(crush["gap_pct"].dropna()) > 0 else None
    avg_5d = _safe_mean(crush["move_5d_pct"]) if not crush.empty else None

    stats = {
        "ticker": ticker,
        "quarters_available": quarters,
        "beat_rate": beat_rate,
        "miss_rate": miss_rate,
        "avg_surprise_pct": avg_surprise,
        "avg_iv_crush_pct": avg_crush,
        "avg_iv_buildup_pct": avg_buildup,
        "avg_iv_ramp_start_days": avg_ramp,
        "avg_expected_move_pct": avg_expected,
        "avg_actual_move_pct": avg_actual,
        "avg_move_ratio": avg_ratio,
        "avg_gap_pct": avg_gap,
        "avg_5d_drift_pct": avg_5d,
        "last_earnings_date": last_date,
        "last_surprise_pct": float(last_surprise) if pd.notna(last_surprise) else None,
        "last_beat_miss": last_bm,
        "consecutive_beats": consecutive_beats,
        "consecutive_misses": consecutive_misses,
    }

    df_stats = pd.DataFrame([stats])
    con.execute("""
        INSERT INTO earnings_stats
            (ticker, quarters_available, beat_rate, miss_rate, avg_surprise_pct,
             avg_iv_crush_pct, avg_iv_buildup_pct, avg_iv_ramp_start_days,
             avg_expected_move_pct, avg_actual_move_pct, avg_move_ratio,
             avg_gap_pct, avg_5d_drift_pct, last_earnings_date,
             last_surprise_pct, last_beat_miss, consecutive_beats,
             consecutive_misses)
        SELECT ticker, quarters_available, beat_rate, miss_rate, avg_surprise_pct,
               avg_iv_crush_pct, avg_iv_buildup_pct, avg_iv_ramp_start_days,
               avg_expected_move_pct, avg_actual_move_pct, avg_move_ratio,
               avg_gap_pct, avg_5d_drift_pct, last_earnings_date,
               last_surprise_pct, last_beat_miss, consecutive_beats,
               consecutive_misses
        FROM df_stats
        ON CONFLICT (ticker) DO UPDATE SET
            quarters_available = EXCLUDED.quarters_available,
            beat_rate = EXCLUDED.beat_rate,
            miss_rate = EXCLUDED.miss_rate,
            avg_surprise_pct = EXCLUDED.avg_surprise_pct,
            avg_iv_crush_pct = EXCLUDED.avg_iv_crush_pct,
            avg_iv_buildup_pct = EXCLUDED.avg_iv_buildup_pct,
            avg_iv_ramp_start_days = EXCLUDED.avg_iv_ramp_start_days,
            avg_expected_move_pct = EXCLUDED.avg_expected_move_pct,
            avg_actual_move_pct = EXCLUDED.avg_actual_move_pct,
            avg_move_ratio = EXCLUDED.avg_move_ratio,
            avg_gap_pct = EXCLUDED.avg_gap_pct,
            avg_5d_drift_pct = EXCLUDED.avg_5d_drift_pct,
            last_earnings_date = EXCLUDED.last_earnings_date,
            last_surprise_pct = EXCLUDED.last_surprise_pct,
            last_beat_miss = EXCLUDED.last_beat_miss,
            consecutive_beats = EXCLUDED.consecutive_beats,
            consecutive_misses = EXCLUDED.consecutive_misses
    """)

    logger.info(f"Refreshed earnings_stats for {ticker}: {quarters}Q, beat_rate={beat_rate:.0%}")
    return stats


def refresh_all_earnings_stats(con: duckdb.DuckDBPyConnection) -> int:
    """Recompute earnings_stats for all tickers with history. Returns count."""
    tickers = con.execute(
        "SELECT DISTINCT ticker FROM earnings_history ORDER BY ticker"
    ).fetchall()
    count = 0
    for (t,) in tickers:
        result = refresh_earnings_stats(con, t)
        if result:
            count += 1
    logger.info(f"Refreshed earnings_stats for {count} tickers")
    return count
