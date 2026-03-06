"""
Entry Anchor Retroactive Enrichment
====================================
For positions first seen after their actual entry date (e.g., broker CSV exported
late), the entry anchor has:
  - Entry_Chart_State_* = NULL  (chart state unknown at entry)
  - IV_Entry = today's IV      (not the entry-day IV)

This module patches those anchors ONCE using data we already have:
  1. Price history (pipeline.duckdb price_history) → compute chart signals
     at the entry date by slicing history up to that day
  2. IV history (iv_history.duckdb iv_term_history) → look up iv_30d on
     the entry date

Greeks (Delta/Gamma/Vega/Theta) are NOT backfilled — the IV surface changes
daily and we cannot reconstruct pricing model inputs reliably.
Those remain NaN for historical positions and are refreshed live during
market hours by the live_greeks_provider.

Called once per management run, after entry_anchors are loaded from DuckDB.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Optional

import duckdb
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

_PIPELINE_DB   = "data/pipeline.duckdb"
_IV_HISTORY_DB = "data/iv_history.duckdb"

# Columns that indicate chart state was already frozen (no backfill needed)
_CHART_STATE_COLS = [
    "Entry_Chart_State_PriceStructure",
    "Entry_Chart_State_TrendIntegrity",
    "Entry_Chart_State_VolatilityState",
    "Entry_Chart_State_CompressionMaturity",
]


def backfill_entry_anchors(
    db_path: str = _PIPELINE_DB,
    iv_db_path: str = _IV_HISTORY_DB,
) -> int:
    """
    Scan entry_anchors for rows missing Entry_Chart_State_* or IV_Entry,
    and patch them using historical databases.

    Returns the number of anchors patched.
    """
    try:
        with duckdb.connect(db_path) as con:
            anchors = con.execute("""
                SELECT TradeID, LegID, Symbol, Underlying_Ticker,
                       Entry_Snapshot_TS, Underlying_Price_Entry,
                       IV_Entry, IV_Entry_Source,
                       Entry_Chart_State_PriceStructure,
                       Entry_Chart_State_TrendIntegrity,
                       Is_Active
                FROM entry_anchors
                WHERE Is_Active = TRUE
                  AND (
                    Entry_Chart_State_PriceStructure IS NULL
                    OR IV_Entry IS NULL
                    OR IV_Entry = 0.0
                  )
            """).fetchdf()

        if anchors.empty:
            logger.debug("[EntryBackfill] No anchors need backfill.")
            return 0

        logger.info(f"[EntryBackfill] {len(anchors)} anchor rows need backfill.")

        # Load price history for all affected underlyings
        tickers = anchors["Underlying_Ticker"].dropna().unique().tolist()
        price_history = _load_price_history(tickers, db_path)
        iv_history    = _load_iv_history(tickers, iv_db_path)

        patched = 0
        with duckdb.connect(db_path) as con:
            for _, row in anchors.iterrows():
                ticker   = row["Underlying_Ticker"]
                leg_id   = row["LegID"]
                entry_ts = pd.to_datetime(row["Entry_Snapshot_TS"])
                if pd.isna(entry_ts):
                    continue
                entry_date = entry_ts.date()

                updates: dict = {}

                # ── Chart state backfill ────────────────────────────────────
                if pd.isna(row["Entry_Chart_State_PriceStructure"]):
                    chart = _compute_chart_state_at(ticker, entry_date, price_history)
                    if chart:
                        updates.update(chart)

                # ── IV backfill ─────────────────────────────────────────────
                iv_val = row.get("IV_Entry")
                needs_iv = iv_val is None or pd.isna(iv_val) or float(iv_val or 0) == 0.0
                if needs_iv:
                    iv30 = _lookup_iv_at(ticker, entry_date, iv_history)
                    if iv30 is not None:
                        updates["IV_Entry"]        = float(iv30) / 100.0  # stored as decimal
                        updates["IV_Entry_Source"] = "IV_HISTORY_BACKFILL"

                if not updates:
                    continue

                # Build SET clause
                set_parts = []
                vals = []
                for col, val in updates.items():
                    set_parts.append(f'"{col}" = ?')
                    vals.append(val)
                vals.append(leg_id)

                sql = f"""
                    UPDATE entry_anchors
                    SET {', '.join(set_parts)}
                    WHERE LegID = ?
                """
                con.execute(sql, vals)
                patched += 1

                logger.info(
                    f"[EntryBackfill] {ticker} / {leg_id} patched: "
                    f"{list(updates.keys())}"
                )

        if patched:
            logger.info(f"[EntryBackfill] ✅ {patched} anchor rows backfilled.")
        return patched

    except Exception as e:
        logger.warning(f"[EntryBackfill] Failed (non-fatal): {e}")
        return 0


# ── Price history helpers ─────────────────────────────────────────────────────

def _load_price_history(tickers: list, db_path: str) -> dict[str, pd.DataFrame]:
    """Load price history for all tickers. Returns {ticker: df with OHLCV}."""
    result: dict[str, pd.DataFrame] = {}
    try:
        with duckdb.connect(db_path, read_only=True) as con:
            for ticker in tickers:
                df = con.execute("""
                    SELECT date, open_price AS Open, high_price AS High,
                           low_price AS Low, close_price AS Close, volume AS Volume
                    FROM price_history
                    WHERE ticker = ?
                    ORDER BY date ASC
                """, [ticker]).fetchdf()
                if not df.empty:
                    df["date"] = pd.to_datetime(df["date"])
                    df = df.set_index("date")
                    result[ticker] = df
    except Exception as e:
        logger.debug(f"[EntryBackfill] Price history load error: {e}")
    return result


def _compute_chart_state_at(
    ticker: str,
    entry_date: date,
    price_history: dict[str, pd.DataFrame],
) -> Optional[dict]:
    """
    Slice price history up to entry_date and compute chart state signals.
    Returns dict of Entry_Chart_State_* values, or None if insufficient data.
    """
    hist = price_history.get(ticker)
    if hist is None or hist.empty:
        return None

    # Slice to entry date (inclusive) — need at least 50 bars for reliable signals
    cutoff = pd.Timestamp(entry_date)
    sliced = hist[hist.index <= cutoff]

    if len(sliced) < 50:
        logger.debug(
            f"[EntryBackfill] {ticker}: only {len(sliced)} bars at {entry_date} — skipping chart backfill"
        )
        return None

    try:
        signals = _compute_signals(sliced)
        return {
            "Entry_Chart_State_PriceStructure":    _classify_price_structure(signals),
            "Entry_Chart_State_TrendIntegrity":    _classify_trend_integrity(signals),
            "Entry_Chart_State_VolatilityState":   _classify_volatility_state(signals),
            "Entry_Chart_State_CompressionMaturity": _classify_compression_maturity(signals),
        }
    except Exception as e:
        logger.debug(f"[EntryBackfill] {ticker} chart compute error at {entry_date}: {e}")
        return None


def _compute_signals(hist: pd.DataFrame) -> dict:
    """Compute raw signals from OHLCV. Mirrors _calculate_primitives_for_ticker logic."""
    c = hist["Close"]
    h = hist["High"]
    l = hist["Low"]

    ema20 = c.ewm(span=20, adjust=False).mean()
    ema50 = c.ewm(span=50, adjust=False).mean()
    sma20 = c.rolling(window=20).mean()

    prev_c = c.shift(1)
    tr = pd.concat([h - l, (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)
    atr14 = tr.rolling(window=14).mean()

    std20 = c.rolling(window=20).std()
    bb_upper = sma20 + (std20 * 2)
    bb_lower = sma20 - (std20 * 2)
    bb_width_pct = (bb_upper - bb_lower) / sma20

    ema20_slope = (ema20.iloc[-1] - ema20.iloc[-5]) / 5 if len(ema20) >= 5 else 0.0
    roc20 = ((c.iloc[-1] - c.iloc[-21]) / c.iloc[-21]) * 100 if len(c) >= 21 and c.iloc[-21] != 0 else 0.0

    # HV percentile
    log_ret   = np.log(c / c.shift(1)).dropna()
    hv20_series = log_ret.rolling(20).std() * np.sqrt(252)
    hv20_current = hv20_series.iloc[-1] if not hv20_series.empty else np.nan
    hv20_history = hv20_series.dropna().tail(252)
    hv20_pct = float((hv20_history < hv20_current).mean()) if len(hv20_history) > 5 else 0.5

    # Choppiness index (14)
    n = 14
    atr_sum = tr.tail(n).sum()
    h14_max  = h.tail(n).max()
    l14_min  = l.tail(n).min()
    price_range = h14_max - l14_min
    if price_range > 0 and atr_sum > 0:
        chop = 100.0 * np.log10(atr_sum / price_range) / np.log10(n)
        chop = float(np.clip(chop, 0, 100))
    else:
        chop = 50.0

    # BB width z-score (compression maturity)
    bb_w_tail = bb_width_pct.tail(20)
    bb_w_z = float(
        (bb_width_pct.iloc[-1] - bb_w_tail.mean()) / bb_w_tail.std()
    ) if bb_w_tail.std() > 0 else 0.0

    # Swing structure
    hh_mask = h > h.shift(1).rolling(5).max()
    ll_mask  = l < l.shift(1).rolling(5).min()

    return {
        "ema20_slope":    float(ema20_slope),
        "roc20":          float(roc20),
        "ema_above_50":   bool(ema20.iloc[-1] > ema50.iloc[-1]),
        "atr14":          float(atr14.iloc[-1]) if not pd.isna(atr14.iloc[-1]) else 1.0,
        "hv20_pct":       float(hv20_pct),
        "chop":           float(chop),
        "bb_width_z":     float(bb_w_z),
        "hh_count":       int(hh_mask.tail(20).sum()),
        "ll_count":       int(ll_mask.tail(20).sum()),
        "close":          float(c.iloc[-1]),
        "ema20":          float(ema20.iloc[-1]),
        "bb_width_pct":   float(bb_width_pct.iloc[-1]) if not pd.isna(bb_width_pct.iloc[-1]) else 0.1,
    }


def _classify_price_structure(s: dict) -> str:
    hh, ll = s["hh_count"], s["ll_count"]
    roc20  = s["roc20"]
    if hh >= 3 and ll == 0:
        return "STRUCTURAL_UP"
    if ll >= 3 and hh == 0:
        return "STRUCTURAL_DOWN"
    if abs(roc20) < 5 and hh < 3 and ll < 3:
        return "RANGE_BOUND"
    return "STRUCTURE_BROKEN"


def _classify_trend_integrity(s: dict) -> str:
    slope  = s["ema20_slope"]
    roc20  = s["roc20"]
    above  = s["ema_above_50"]
    if abs(slope) > 0.3 and abs(roc20) > 8 and above == (slope > 0):
        return "STRONG_TREND"
    if abs(slope) > 0.1 or abs(roc20) > 4:
        return "WEAK_TREND"
    if s["chop"] > 60:
        return "TREND_EXHAUSTED"
    return "NO_TREND"


def _classify_volatility_state(s: dict) -> str:
    hv_pct = s["hv20_pct"]
    chop   = s["chop"]
    if hv_pct > 0.80:
        return "EXPANDING" if chop < 50 else "EXTREME"
    if hv_pct < 0.30:
        return "COMPRESSED"
    return "NORMAL"


def _classify_compression_maturity(s: dict) -> str:
    bb_z   = s["bb_width_z"]
    bb_pct = s["bb_width_pct"]
    if bb_z < -1.5 and bb_pct < 0.08:
        return "MATURE_COMPRESSION"
    if bb_z < -0.5:
        return "DEVELOPING_COMPRESSION"
    return "NOT_COMPRESSED"


# ── IV history helpers ────────────────────────────────────────────────────────

def _load_iv_history(tickers: list, iv_db_path: str) -> dict[str, pd.DataFrame]:
    """Load iv_term_history for all tickers. Returns {ticker: df indexed by date}."""
    result: dict[str, pd.DataFrame] = {}
    try:
        with duckdb.connect(iv_db_path, read_only=True) as con:
            for ticker in tickers:
                df = con.execute("""
                    SELECT date, iv_30d, iv_60d, iv_90d
                    FROM iv_term_history
                    WHERE ticker = ?
                      AND iv_30d IS NOT NULL
                    ORDER BY date ASC
                """, [ticker]).fetchdf()
                if not df.empty:
                    df["date"] = pd.to_datetime(df["date"])
                    df = df.set_index("date")
                    result[ticker] = df
    except Exception as e:
        logger.debug(f"[EntryBackfill] IV history load error: {e}")
    return result


def _lookup_iv_at(
    ticker: str,
    entry_date: date,
    iv_history: dict[str, pd.DataFrame],
) -> Optional[float]:
    """
    Return iv_30d for ticker on or just before entry_date.
    Falls back up to 5 trading days back if exact date missing.
    """
    df = iv_history.get(ticker)
    if df is None or df.empty:
        return None

    target = pd.Timestamp(entry_date)
    candidates = df[df.index <= target]
    if candidates.empty:
        return None

    # Use closest available date ≤ entry_date
    row = candidates.iloc[-1]
    iv30 = row.get("iv_30d")
    if iv30 is not None and not pd.isna(iv30) and float(iv30) > 0:
        return float(iv30)
    return None
