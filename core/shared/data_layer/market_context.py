"""
Market Context — CRUD module for market-wide indicator storage.

Daily snapshot design: date as PK, one canonical end-of-day row per trading day.
INSERT OR REPLACE on same-day re-collection.

Table: market_context_daily in data/market.duckdb (MARKET domain).
"""

import json
import logging
import pandas as pd
from datetime import date
from typing import Optional

from core.shared.data_layer.duckdb_utils import (
    DbDomain, get_domain_connection, get_domain_write_connection,
)
from core.shared.calendar.trading_calendar import business_days_between

logger = logging.getLogger(__name__)


# ── Schema ────────────────────────────────────────────────────────────────────

_TABLE = "market_context_daily"

_CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {_TABLE} (
    date DATE PRIMARY KEY,
    -- VIX complex (source-agnostic naming)
    vix DOUBLE,
    vix_3m DOUBLE,
    vvix DOUBLE,
    skew DOUBLE,
    vix_term_spread DOUBLE,
    vix_term_ratio DOUBLE,
    -- Credit proxy
    credit_spread_proxy DOUBLE,
    hyg_price DOUBLE,
    lqd_price DOUBLE,
    -- Universe breadth (our ticker universe, NOT NYSE/SPX breadth)
    universe_breadth_pct_sma50 DOUBLE,
    universe_breadth_advancing_5d DOUBLE,
    -- Correlation
    avg_correlation DOUBLE,
    -- Derived
    vix_percentile_252d DOUBLE,
    vix_sma_20 DOUBLE,
    -- Classifier output
    market_regime VARCHAR,
    regime_score DOUBLE,
    regime_confidence DOUBLE,
    regime_basis VARCHAR,
    -- Audit
    regime_detail_json VARCHAR,
    collection_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR DEFAULT 'yfinance'
)
"""

_COLUMNS = [
    "date", "vix", "vix_3m", "vvix", "skew",
    "vix_term_spread", "vix_term_ratio",
    "credit_spread_proxy", "hyg_price", "lqd_price",
    "universe_breadth_pct_sma50", "universe_breadth_advancing_5d",
    "avg_correlation",
    "vix_percentile_252d", "vix_sma_20",
    "market_regime", "regime_score", "regime_confidence", "regime_basis",
    "regime_detail_json", "collection_ts", "source",
]


# ── Table Initialization ─────────────────────────────────────────────────────

def initialize_market_context_table(con=None) -> None:
    """Create table if not exists. Safe to call repeatedly (idempotent)."""
    own_con = con is None
    if own_con:
        con = get_domain_write_connection(DbDomain.MARKET)
    try:
        con.execute(_CREATE_SQL)
        con.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_mkt_ctx_date
            ON {_TABLE} (date DESC)
        """)
    finally:
        if own_con:
            con.close()


# ── Duplicate Guard ───────────────────────────────────────────────────────────

def market_context_collected_today(d: date | None = None) -> bool:
    """Return True if market context was already collected for *d* (default: today)."""
    d = d or date.today()
    try:
        con = get_domain_connection(DbDomain.MARKET, read_only=True)
        try:
            result = con.execute(
                f"SELECT COUNT(*) FROM {_TABLE} WHERE date = ?", [d]
            ).fetchone()
            return result is not None and result[0] > 0
        finally:
            con.close()
    except Exception:
        return False


# ── Write ─────────────────────────────────────────────────────────────────────

def write_market_context(data: dict, d: date | None = None) -> None:
    """
    Write (INSERT OR REPLACE) a single day's market context row.

    *data* should contain column names as keys. Missing keys → NULL.
    *d* overrides the date key in data (default: today).
    """
    d = d or data.get("date") or date.today()
    data["date"] = d

    con = get_domain_write_connection(DbDomain.MARKET)
    try:
        initialize_market_context_table(con)

        cols = [c for c in _COLUMNS if c in data]
        placeholders = ", ".join(["?"] * len(cols))
        col_str = ", ".join(cols)
        values = []
        for c in cols:
            v = data[c]
            # Serialize dicts/lists to JSON string
            if isinstance(v, (dict, list)):
                v = json.dumps(v)
            values.append(v)

        con.execute(
            f"INSERT OR REPLACE INTO {_TABLE} ({col_str}) VALUES ({placeholders})",
            values,
        )
        logger.info(f"[MarketCtx] Wrote market context for {d}")
    finally:
        con.close()


# ── Read ──────────────────────────────────────────────────────────────────────

def get_latest_market_context() -> Optional[dict]:
    """
    Return the most recent market context row as a dict, or None if empty.

    Adds computed field 'staleness_bdays' — business days between data date and today.
    """
    try:
        con = get_domain_connection(DbDomain.MARKET, read_only=True)
        try:
            df = con.execute(
                f"SELECT * FROM {_TABLE} ORDER BY date DESC LIMIT 1"
            ).fetchdf()
            if df.empty:
                return None
            row = {k: (None if isinstance(v, float) and pd.isna(v) else v)
                   for k, v in df.iloc[0].to_dict().items()}
            # Compute business-day staleness
            data_date = row.get("date")
            if data_date is not None:
                if isinstance(data_date, pd.Timestamp):
                    data_date = data_date.date()
                row["staleness_bdays"] = business_days_between(data_date, date.today())
            else:
                row["staleness_bdays"] = 999
            # Parse regime_detail_json if present
            rdj = row.get("regime_detail_json")
            if rdj and isinstance(rdj, str):
                try:
                    row["regime_detail"] = json.loads(rdj)
                except (json.JSONDecodeError, TypeError):
                    row["regime_detail"] = None
            return row
        finally:
            con.close()
    except Exception as e:
        logger.debug(f"[MarketCtx] Failed to read latest context: {e}")
        return None


def query_vix_history(days: int = 252) -> pd.Series:
    """
    Return a Series of VIX values for the last *days* trading days.

    Index is date, values are VIX. Used for percentile computation.
    Returns empty Series on failure.
    """
    try:
        con = get_domain_connection(DbDomain.MARKET, read_only=True)
        try:
            df = con.execute(
                f"""SELECT date, vix FROM {_TABLE}
                    WHERE vix IS NOT NULL
                    ORDER BY date DESC LIMIT ?""",
                [days],
            ).fetchdf()
            if df.empty:
                return pd.Series(dtype=float)
            return df.set_index("date")["vix"].sort_index()
        finally:
            con.close()
    except Exception:
        return pd.Series(dtype=float)


def query_market_context(d: date | None = None, lookback_days: int = 1) -> pd.DataFrame:
    """Return the last *lookback_days* rows up to and including *d* (default: today)."""
    d = d or date.today()
    try:
        con = get_domain_connection(DbDomain.MARKET, read_only=True)
        try:
            df = con.execute(
                f"""SELECT * FROM {_TABLE}
                    WHERE date <= ?
                    ORDER BY date DESC LIMIT ?""",
                [d, lookback_days],
            ).fetchdf()
            return df
        finally:
            con.close()
    except Exception:
        return pd.DataFrame()
