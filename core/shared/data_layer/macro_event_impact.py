"""
Macro Event Impact — Track actual market reactions to macro events.

Records what happened on each FOMC/CPI/NFP/GDP/PCE day so MC simulations
can use empirical event impact distributions instead of generic assumptions.

Each event day captures:
  - VIX change (absolute and %)
  - SPY/universe price moves
  - IV term structure shift
  - Regime before → after
  - Universe breadth change

Data sources: market_context_daily + price_history (both already collected daily).

Table: macro_event_impact in data/market.duckdb (MARKET domain).
"""

import json
import logging
import math
import pandas as pd
from datetime import date
from typing import Optional

from core.shared.data_layer.duckdb_utils import (
    DbDomain, get_domain_connection, get_domain_write_connection,
)

logger = logging.getLogger(__name__)


# ── Schema ────────────────────────────────────────────────────────────────────

_TABLE = "macro_event_impact"

_CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {_TABLE} (
    event_date DATE NOT NULL,
    event_type VARCHAR NOT NULL,           -- FOMC | CPI | NFP | GDP | PCE
    event_label VARCHAR,                   -- "FOMC Rate Decision", "CPI Report", etc.
    event_impact VARCHAR,                  -- HIGH | MEDIUM (from macro calendar)

    -- VIX reaction
    vix_prior DOUBLE,                      -- VIX close on prior trading day
    vix_close DOUBLE,                      -- VIX close on event day
    vix_change DOUBLE,                     -- absolute change
    vix_change_pct DOUBLE,                 -- percent change

    -- SPY reaction (proxy for broad market)
    spy_prior_close DOUBLE,
    spy_close DOUBLE,
    spy_change_pct DOUBLE,                 -- SPY % move on event day

    -- Universe reaction
    universe_avg_move_pct DOUBLE,          -- mean |return| across our ticker universe
    universe_median_move_pct DOUBLE,       -- median |return|
    universe_pct_advancing DOUBLE,         -- % of tickers with positive return
    universe_pct_declining DOUBLE,         -- % with negative return

    -- IV reaction
    vix_term_spread_prior DOUBLE,          -- term spread day before
    vix_term_spread_after DOUBLE,          -- term spread on event day
    vix_term_ratio_prior DOUBLE,
    vix_term_ratio_after DOUBLE,

    -- Regime reaction
    regime_prior VARCHAR,                  -- regime on prior day
    regime_after VARCHAR,                  -- regime on event day
    regime_score_prior DOUBLE,
    regime_score_after DOUBLE,
    regime_changed BOOLEAN,                -- did regime bucket change?

    -- Breadth reaction
    breadth_sma50_prior DOUBLE,
    breadth_sma50_after DOUBLE,

    -- Metadata
    collection_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (event_date, event_type)
)
"""


# ── Table Initialization ─────────────────────────────────────────────────────

def initialize_event_impact_table(con=None) -> None:
    """Create table if not exists. Safe to call repeatedly."""
    own_con = con is None
    if own_con:
        con = get_domain_write_connection(DbDomain.MARKET)
    try:
        con.execute(_CREATE_SQL)
    finally:
        if own_con:
            con.close()


# ── Write ─────────────────────────────────────────────────────────────────────

def write_event_impact(data: dict) -> None:
    """Write a single macro event impact record (INSERT OR REPLACE)."""
    con = get_domain_write_connection(DbDomain.MARKET)
    try:
        initialize_event_impact_table(con)

        cols = [k for k in data.keys() if k != "collection_ts"]
        placeholders = ", ".join(["?"] * len(cols))
        col_str = ", ".join(cols)
        values = [data[c] for c in cols]

        con.execute(
            f"INSERT OR REPLACE INTO {_TABLE} ({col_str}) VALUES ({placeholders})",
            values,
        )
        logger.info(
            f"[MacroImpact] Wrote {data.get('event_type')} impact for {data.get('event_date')}"
        )
    finally:
        con.close()


# ── Read ──────────────────────────────────────────────────────────────────────

def event_impact_exists(event_date: date, event_type: str) -> bool:
    """Check if impact record already exists for this event."""
    try:
        con = get_domain_connection(DbDomain.MARKET, read_only=True)
        try:
            result = con.execute(
                f"SELECT COUNT(*) FROM {_TABLE} WHERE event_date = ? AND event_type = ?",
                [event_date, event_type],
            ).fetchone()
            return result is not None and result[0] > 0
        finally:
            con.close()
    except Exception:
        return False


def query_event_impact_by_type(
    event_type: str,
    limit: int = 50,
) -> pd.DataFrame:
    """Return historical impact records for a specific event type.

    Ordered most recent first. Used by MC to build empirical distributions.
    """
    try:
        con = get_domain_connection(DbDomain.MARKET, read_only=True)
        try:
            return con.execute(
                f"""SELECT * FROM {_TABLE}
                    WHERE event_type = ?
                    ORDER BY event_date DESC
                    LIMIT ?""",
                [event_type, limit],
            ).fetchdf()
        finally:
            con.close()
    except Exception:
        return pd.DataFrame()


def query_event_stats(event_type: str) -> Optional[dict]:
    """Compute aggregate statistics for a macro event type.

    Returns dict with empirical distribution parameters for MC consumption:
      - avg_spy_move_pct, median_spy_move_pct, std_spy_move_pct
      - avg_vix_change_pct, avg_vix_change_abs
      - avg_universe_move_pct
      - regime_change_rate (% of events causing regime change)
      - n_events (sample size)

    Returns None if no data available.
    """
    try:
        con = get_domain_connection(DbDomain.MARKET, read_only=True)
        try:
            df = con.execute(
                f"""SELECT
                        COUNT(*) AS n_events,
                        AVG(spy_change_pct) AS avg_spy_move_pct,
                        MEDIAN(spy_change_pct) AS median_spy_move_pct,
                        STDDEV(spy_change_pct) AS std_spy_move_pct,
                        AVG(ABS(spy_change_pct)) AS avg_spy_abs_move_pct,
                        AVG(vix_change_pct) AS avg_vix_change_pct,
                        AVG(vix_change) AS avg_vix_change_abs,
                        AVG(universe_avg_move_pct) AS avg_universe_move_pct,
                        AVG(CASE WHEN regime_changed THEN 1.0 ELSE 0.0 END) AS regime_change_rate
                    FROM {_TABLE}
                    WHERE event_type = ?""",
                [event_type],
            ).fetchdf()
            if df.empty or df.iloc[0]["n_events"] == 0:
                return None
            row = df.iloc[0].to_dict()
            # Convert numpy types to Python native
            return {k: (float(v) if v is not None and not (isinstance(v, float) and math.isnan(v)) else None)
                    for k, v in row.items()}
        finally:
            con.close()
    except Exception as e:
        logger.debug(f"[MacroImpact] query_event_stats failed: {e}")
        return None


def query_all_event_stats() -> dict:
    """Return stats for all event types. Keys are event types, values are stats dicts."""
    result = {}
    for evt_type in ("FOMC", "CPI", "NFP", "GDP", "PCE"):
        stats = query_event_stats(evt_type)
        if stats and stats.get("n_events", 0) > 0:
            result[evt_type] = stats
    return result


# ── MC Calibration Interface ──────────────────────────────────────────────────

# Minimum sample size before using empirical data.
# Below this, MC falls back to default jump parameters.
_MIN_EVENTS_FOR_CALIBRATION = 3

# Default macro event jump parameters (used when no empirical data).
# More conservative than generic jump params (macro events = known fat tails).
_DEFAULT_MACRO_JUMP = {
    "jump_intensity_mult": 1.5,    # 1.5× base jump probability
    "jump_std_mult": 1.3,          # 1.3× base jump std dev
    "jump_mean_adj": 0.0,          # no directional bias (events can go either way)
}


def get_mc_macro_calibration(event_type: str) -> Optional[dict]:
    """Return MC-ready calibration parameters for a macro event type.

    If sufficient empirical data exists (≥3 events), calibrates from actual
    observed market reactions. Otherwise returns conservative defaults.

    Returns dict with:
      - jump_intensity_mult: multiplier for JUMP_INTENSITY (>1 = more frequent jumps)
      - jump_std_mult: multiplier for JUMP_STD (>1 = larger jump magnitudes)
      - jump_mean_adj: additive adjustment to JUMP_MEAN (0 = no directional bias)
      - avg_spy_abs_move_pct: empirical average |SPY move| on this event type
      - avg_vix_change_pct: empirical average VIX % change
      - n_events: sample size (0 if using defaults)
      - calibration_source: 'empirical' or 'default'
    """
    stats = query_event_stats(event_type)

    if stats is None or stats.get("n_events", 0) < _MIN_EVENTS_FOR_CALIBRATION:
        return {
            **_DEFAULT_MACRO_JUMP,
            "avg_spy_abs_move_pct": None,
            "avg_vix_change_pct": None,
            "n_events": 0,
            "calibration_source": "default",
        }

    n = stats["n_events"]
    avg_abs_spy = stats.get("avg_spy_abs_move_pct")
    std_spy = stats.get("std_spy_move_pct")
    avg_vix_chg = stats.get("avg_vix_change_pct")

    # Calibrate jump intensity from how often events cause large moves.
    # If avg |SPY move| > 1%, events are high-impact → amplify jumps more.
    if avg_abs_spy is not None and avg_abs_spy > 0:
        # Scale: 0.5% avg move → 1.2×, 1% → 1.5×, 2% → 2.0×
        intensity_mult = min(2.5, max(1.0, 1.0 + avg_abs_spy * 50))
    else:
        intensity_mult = _DEFAULT_MACRO_JUMP["jump_intensity_mult"]

    # Calibrate jump std from observed SPY return std on event days.
    if std_spy is not None and std_spy > 0:
        # Compare to baseline daily SPY std (~1%). Scale jump_std proportionally.
        std_mult = min(2.5, max(1.0, std_spy / 0.01))
    else:
        std_mult = _DEFAULT_MACRO_JUMP["jump_std_mult"]

    return {
        "jump_intensity_mult": round(intensity_mult, 3),
        "jump_std_mult": round(std_mult, 3),
        "jump_mean_adj": 0.0,  # no directional assumption — events are two-sided
        "avg_spy_abs_move_pct": avg_abs_spy,
        "avg_vix_change_pct": avg_vix_chg,
        "n_events": n,
        "calibration_source": "empirical",
    }
