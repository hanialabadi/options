"""
Extension Monitor: Daily re-check of WAIT_PULLBACK / WAIT_PRICE entries.

PURPOSE:
    Runs each pipeline scan against all ACTIVE wait_list entries that were
    deferred due to:
      - Timing_Gate = WAIT_PULLBACK / DEFER  (TQS-based, R3.2.TIMING)
      - Price_Gate  = WAIT_PRICE             (BS fair-value, R3.2.PRICE)

    For each active entry, evaluates all TECHNICAL and PRICE_LEVEL conditions
    using the current scan's technical_indicators snapshot. When ALL conditions
    clear, promotes the entry to PROMOTED and surfaces it as a fresh READY
    candidate in the next scan cycle.

DOCTRINE:
    Murphy Ch.4: "Wait for the pullback — the market gives second chances."
    Bulkowski: Statistical recovery after extended entries — most revert within
               3-5 sessions.
    Natenberg Ch.8: "Price discipline at entry compounds over hundreds of trades."

INTEGRATION:
    Called from scan_engine/pipeline.py inside _step_minus_1_reevaluate_wait_list()
    alongside the existing wait loop evaluator, using the same DuckDB connection.

    from core.wait_loop.extension_monitor import run_extension_monitor
    result = run_extension_monitor(ctx, db_con)
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

# Gate codes that this monitor handles
_TIMING_GATES = {"R3.2.TIMING", "R3.2.TIMING_AND_PRICE"}
_PRICE_GATES  = {"R3.2.PRICE",  "R3.2.TIMING_AND_PRICE"}
_ALL_GATES    = _TIMING_GATES | _PRICE_GATES


def run_extension_monitor(ctx, db_con: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    """
    Re-evaluate ACTIVE wait_list entries gated by Timing_Gate or Price_Gate.

    Args:
        ctx: PipelineContext (provides ctx.results for current snapshot data)
        db_con: DuckDB connection (pipeline.duckdb)

    Returns:
        dict with keys: checked, promoted, still_waiting, invalidated
    """
    result = {"checked": 0, "promoted": 0, "still_waiting": 0, "invalidated": 0}

    # Pull current snapshot: technical indicators + contract mid prices
    _tech_df = _get_current_technicals(ctx)
    _mid_map  = _get_current_mids(ctx)       # Ticker → {contract_symbol: mid}

    if _tech_df.empty:
        logger.info("[ExtensionMonitor] No technical indicators available — skipping")
        return result

    # Fetch all ACTIVE entries whose gate_reason matches timing/price codes
    try:
        rows = db_con.execute("""
            SELECT wait_id, ticker, strategy_name, strategy_type,
                   proposed_strike, contract_symbol,
                   wait_conditions, conditions_met, wait_progress,
                   entry_price, wait_started_at, wait_expires_at,
                   evaluation_count, status
            FROM wait_list
            WHERE status = 'ACTIVE'
        """).fetchall()
        cols = [
            "wait_id", "ticker", "strategy_name", "strategy_type",
            "proposed_strike", "contract_symbol",
            "wait_conditions", "conditions_met", "wait_progress",
            "entry_price", "wait_started_at", "wait_expires_at",
            "evaluation_count", "status"
        ]
        entries = [dict(zip(cols, r)) for r in rows]
    except Exception as e:
        logger.warning(f"[ExtensionMonitor] Could not read wait_list: {e}")
        return result

    for entry in entries:
        wait_id = entry["wait_id"]
        ticker  = entry["ticker"]

        # Filter: only handle timing/price gate entries
        try:
            conds_raw = entry.get("wait_conditions") or "[]"
            if isinstance(conds_raw, str):
                conds = json.loads(conds_raw)
            else:
                conds = list(conds_raw)
        except Exception:
            conds = []

        # Check if any condition is a TECHNICAL or PRICE_LEVEL type
        has_technical = any(c.get("type") in ("technical", "price_level") for c in conds)
        if not has_technical:
            continue  # Not an extension-monitor entry

        result["checked"] += 1

        # Get current data for this ticker
        _mkt = _get_market_data_for_ticker(ticker, _tech_df, _mid_map, entry)

        # Evaluate each condition
        from .conditions import ConditionFactory, ConditionType
        met_ids = set(json.loads(entry.get("conditions_met") or "[]"))
        newly_met = []

        for cond_dict in conds:
            cid  = cond_dict["condition_id"]
            ctype = cond_dict.get("type", "")

            if cid in met_ids:
                continue  # Already satisfied

            try:
                cond_obj = ConditionFactory.create_from_dict(cond_dict)
                is_met = cond_obj.check(_mkt, entry)
                if is_met:
                    newly_met.append(cid)
                    logger.info(
                        f"[ExtensionMonitor] ✅ {ticker} ({wait_id[:8]}): "
                        f"condition met — {cond_dict.get('description', cid)}"
                    )
            except Exception as e:
                logger.debug(f"[ExtensionMonitor] Condition check error {cid}: {e}")

        met_ids.update(newly_met)
        total_conds = len(conds)
        n_met = len(met_ids)
        progress = n_met / total_conds if total_conds > 0 else 0.0

        # Check expiry
        now = datetime.now()
        expires_at = entry.get("wait_expires_at")
        if expires_at:
            if isinstance(expires_at, str):
                expires_at = datetime.fromisoformat(expires_at)
            if now > expires_at:
                _update_wait_entry(db_con, wait_id, "EXPIRED", progress, met_ids, now)
                result["invalidated"] += 1
                logger.info(f"[ExtensionMonitor] ⏱️ {ticker} ({wait_id[:8]}): EXPIRED — TTL exceeded")
                continue

        if n_met >= total_conds:
            # All conditions cleared — PROMOTE
            _update_wait_entry(db_con, wait_id, "PROMOTED", 1.0, met_ids, now)
            result["promoted"] += 1
            logger.info(
                f"[ExtensionMonitor] 🟢 PROMOTED: {ticker} {entry.get('strategy_name', '')} "
                f"— all {total_conds} conditions met. Re-scan will surface as READY."
            )
        else:
            # Still waiting — update progress
            new_count = int(entry.get("evaluation_count") or 1) + 1
            _update_wait_entry_progress(db_con, wait_id, progress, met_ids, now, new_count)
            result["still_waiting"] += 1
            logger.debug(
                f"[ExtensionMonitor] 🟡 {ticker}: {n_met}/{total_conds} conditions met "
                f"(progress {progress:.0%})"
            )

    if result["checked"] > 0:
        logger.info(
            f"[ExtensionMonitor] Summary — "
            f"checked={result['checked']}, promoted={result['promoted']}, "
            f"still_waiting={result['still_waiting']}, expired={result['invalidated']}"
        )

    return result


# ── Helpers ──────────────────────────────────────────────────────────────────

def _get_current_technicals(ctx) -> pd.DataFrame:
    """Extract latest technical indicators from pipeline context."""
    tech = ctx.results.get("charted")
    if tech is None:
        tech = ctx.results.get("snapshot")
    if tech is None or not isinstance(tech, pd.DataFrame):
        return pd.DataFrame()
    return tech


def _get_current_mids(ctx) -> Dict[str, float]:
    """Build ticker → option mid price map from selected_contracts."""
    mids: Dict[str, float] = {}
    contracts = ctx.results.get("selected_contracts")
    if contracts is None or not isinstance(contracts, pd.DataFrame):
        return mids
    if "Mid_Price" in contracts.columns and "Ticker" in contracts.columns:
        for _, row in contracts.iterrows():
            t = str(row.get("Ticker") or "")
            m = row.get("Mid_Price")
            if t and pd.notna(m):
                mids[t] = float(m)
    return mids


def _get_market_data_for_ticker(
    ticker: str,
    tech_df: pd.DataFrame,
    mid_map: Dict[str, float],
    entry: Dict[str, Any],
) -> Dict[str, Any]:
    """Build market_data dict for a single ticker from current snapshot."""
    mkt: Dict[str, Any] = {}

    # Technical indicators
    t_rows = tech_df[tech_df.get("Ticker", tech_df.get("ticker", pd.Series(dtype=str))) == ticker]
    if not t_rows.empty:
        row = t_rows.iloc[0]
        for col in ["RSI", "SMA20", "SMA50", "ADX", "MACD", "ATR_14",
                    "Last", "last_price", "HV_30", "IV_30D_Call"]:
            v = row.get(col) if col in row.index else None
            if v is not None and pd.notna(v):
                mkt[col] = float(v)
        # Normalise Last → last_price
        if "Last" in mkt and "last_price" not in mkt:
            mkt["last_price"] = mkt["Last"]

    # Option mid price (for Price_Gate conditions)
    opt_mid = mid_map.get(ticker)
    if opt_mid is not None:
        mkt["option_mid"] = opt_mid

    # Pass through wait_entry fields for progress calculation
    mkt["wait_started_at"] = entry.get("wait_started_at", datetime.now())

    return mkt


def _update_wait_entry(
    db_con: duckdb.DuckDBPyConnection,
    wait_id: str,
    new_status: str,
    progress: float,
    met_ids: set,
    now: datetime,
) -> None:
    try:
        db_con.execute("""
            UPDATE wait_list
            SET status           = ?,
                wait_progress    = ?,
                conditions_met   = ?,
                last_evaluated_at = ?,
                updated_at        = ?
            WHERE wait_id = ?
        """, [
            new_status,
            progress,
            json.dumps(list(met_ids)),
            now,
            now,
            wait_id,
        ])
    except Exception as e:
        logger.error(f"[ExtensionMonitor] Failed to update {wait_id}: {e}")


def _update_wait_entry_progress(
    db_con: duckdb.DuckDBPyConnection,
    wait_id: str,
    progress: float,
    met_ids: set,
    now: datetime,
    eval_count: int,
) -> None:
    try:
        db_con.execute("""
            UPDATE wait_list
            SET wait_progress     = ?,
                conditions_met    = ?,
                last_evaluated_at = ?,
                evaluation_count  = ?,
                updated_at        = ?
            WHERE wait_id = ?
        """, [
            progress,
            json.dumps(list(met_ids)),
            now,
            eval_count,
            now,
            wait_id,
        ])
    except Exception as e:
        logger.error(f"[ExtensionMonitor] Failed to update progress {wait_id}: {e}")
