"""
Trade Decision Ledger
=====================
Persistent memory for the management engine.

Provides:
  - ``trade_decision_timeline`` VIEW on ``management_recommendations`` (no new table)
  - ``executed_actions`` TABLE for tracking when recommendations are acted upon
  - Query functions for timeline, flip detection, roll chain, and execution suppression

All functions accept an open DuckDB ``con`` and return empty results on failure
(graceful degradation — engine continues normally if ledger is unavailable).
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

# ── Table / View names ────────────────────────────────────────────────────────
_VIEW_TIMELINE = "trade_decision_timeline"
_TABLE_EXECUTED = "executed_actions"
_TABLE_RECS = "management_recommendations"
_TABLE_LEDGER = "premium_ledger"

# Regex to extract ticker from TradeID (e.g. "PLTR260306_150_BW_5376" → "PLTR")
_TICKER_RE = re.compile(r"^([A-Z]+)\d")


# ── DDL ───────────────────────────────────────────────────────────────────────

_TIMELINE_VIEW_DDL = f"""
CREATE OR REPLACE VIEW {_VIEW_TIMELINE} AS
WITH deduped AS (
    SELECT
        TradeID,
        Underlying_Ticker,
        Strategy,
        Action,
        Urgency,
        Strike,
        "UL Last" AS spot,
        DTE,
        LEFT(Rationale, 200) AS rationale_digest,
        Doctrine_Source,
        Snapshot_TS,
        CAST(Snapshot_TS AS DATE) AS run_date
    FROM {_TABLE_RECS}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY TradeID, CAST(Snapshot_TS AS DATE)
        ORDER BY Snapshot_TS DESC,
                 CASE WHEN Strike IS NOT NULL THEN 0 ELSE 1 END ASC
    ) = 1
),
daily AS (
    SELECT *,
        LAG(Action) OVER (PARTITION BY TradeID ORDER BY run_date) AS prev_action,
        LAG(Urgency) OVER (PARTITION BY TradeID ORDER BY run_date) AS prev_urgency,
        LAG(Strike) OVER (PARTITION BY TradeID ORDER BY run_date) AS prev_strike,
        LAG(spot) OVER (PARTITION BY TradeID ORDER BY run_date) AS prev_spot
    FROM deduped
)
SELECT *,
    CASE WHEN Action != prev_action AND prev_action IS NOT NULL
         THEN TRUE ELSE FALSE END AS action_changed,
    CASE WHEN Strike IS NOT NULL AND prev_strike IS NOT NULL
              AND ABS(Strike - prev_strike) > 0.01
         THEN TRUE ELSE FALSE END AS strike_changed
FROM daily
"""

_EXECUTED_ACTIONS_DDL = f"""
CREATE TABLE IF NOT EXISTS {_TABLE_EXECUTED} (
    trade_id     VARCHAR NOT NULL,
    action       VARCHAR NOT NULL,
    executed_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    confirmed_by VARCHAR DEFAULT 'manual',
    strike_old   DOUBLE,
    strike_new   DOUBLE,
    notes        VARCHAR,
    PRIMARY KEY (trade_id, executed_at)
)
"""


# ── Ensure DDL ────────────────────────────────────────────────────────────────

def _table_exists(con, table_name: str) -> bool:
    try:
        result = con.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
            [table_name],
        ).fetchone()
        return result[0] > 0
    except Exception:
        return False


def ensure_decision_ledger_view(con) -> None:
    """Create or replace the trade_decision_timeline view. Idempotent."""
    if not _table_exists(con, _TABLE_RECS):
        logger.debug("[DecisionLedger] management_recommendations table not found — skipping view.")
        return
    con.execute(_TIMELINE_VIEW_DDL)
    logger.debug("[DecisionLedger] trade_decision_timeline view created/updated.")


def ensure_executed_actions_table(con) -> None:
    """Create the executed_actions table if it does not exist. Idempotent."""
    con.execute(_EXECUTED_ACTIONS_DDL)


# ── Timeline Queries ──────────────────────────────────────────────────────────

def get_trade_timeline(con, trade_id: str) -> pd.DataFrame:
    """Return the full decision timeline for a single TradeID.

    Columns: run_date, Action, Urgency, prev_action, action_changed,
             Strike, prev_strike, strike_changed, spot, DTE,
             rationale_digest, Doctrine_Source
    """
    try:
        return con.execute(
            f"SELECT * FROM {_VIEW_TIMELINE} WHERE TradeID = ? ORDER BY run_date",
            [trade_id],
        ).fetchdf()
    except Exception as e:
        logger.debug(f"[DecisionLedger] get_trade_timeline failed: {e}")
        return pd.DataFrame()


def get_ticker_timeline(con, ticker: str) -> pd.DataFrame:
    """Return the decision timeline across ALL TradeIDs for an underlying ticker.

    Useful for BUY_WRITE positions that generate new TradeIDs on each roll.
    """
    try:
        return con.execute(
            f"SELECT * FROM {_VIEW_TIMELINE} WHERE Underlying_Ticker = ? ORDER BY run_date, TradeID",
            [ticker],
        ).fetchdf()
    except Exception as e:
        logger.debug(f"[DecisionLedger] get_ticker_timeline failed: {e}")
        return pd.DataFrame()


# ── Flip Detection ────────────────────────────────────────────────────────────

def detect_action_flips(con, trade_id: str, window_days: int = 5) -> List[Dict]:
    """Detect decision instability: 3+ action changes within `window_days`.

    Returns a list of dicts:
      [{"first_date": date, "last_date": date, "flip_count": int, "action_sequence": [str]}]
    """
    try:
        df = con.execute(
            f"""
            SELECT run_date, Action, action_changed
            FROM {_VIEW_TIMELINE}
            WHERE TradeID = ?
              AND run_date >= CURRENT_DATE - INTERVAL '{window_days}' DAY
            ORDER BY run_date
            """,
            [trade_id],
        ).fetchdf()
    except Exception as e:
        logger.debug(f"[DecisionLedger] detect_action_flips failed: {e}")
        return []

    if df.empty or len(df) < 2:
        return []

    changes = df[df["action_changed"] == True]  # noqa: E712
    flip_count = len(changes)
    if flip_count < 2:
        return []

    return [{
        "first_date": df["run_date"].iloc[0],
        "last_date": df["run_date"].iloc[-1],
        "flip_count": flip_count,
        "action_sequence": df["Action"].tolist(),
    }]


def count_action_changes(con, trade_id: str, window_days: int = 5) -> int:
    """Count the number of action transitions in the last `window_days`.

    Returns 0 on failure or insufficient data.
    """
    try:
        result = con.execute(
            f"""
            SELECT COALESCE(SUM(CASE WHEN action_changed THEN 1 ELSE 0 END), 0)
            FROM {_VIEW_TIMELINE}
            WHERE TradeID = ?
              AND run_date >= CURRENT_DATE - INTERVAL '{window_days}' DAY
            """,
            [trade_id],
        ).fetchone()
        return int(result[0]) if result else 0
    except Exception:
        return 0


# ── Roll Chain ────────────────────────────────────────────────────────────────

def get_roll_chain_summary(con, ticker: str) -> Dict:
    """Build the full roll chain for a ticker from premium_ledger.

    Returns:
        {
            "ticker": "PLTR",
            "strike_chain": [250.0, 225.0, ...],
            "cycle_credits": [2.10, 1.85, ...],
            "cycle_close_costs": [0.0, 0.85, ...],
            "cycle_dates": ["2026-01-10", ...],
            "total_gross": 8.71,
            "total_net": 6.96,
            "total_close_cost": 1.75,
            "cycle_count": 6,
        }
    """
    if not _table_exists(con, _TABLE_LEDGER):
        return {}

    try:
        # premium_ledger uses trade_id containing the ticker.
        # Query all rows for the ticker pattern and order by cycle.
        df = con.execute(
            f"""
            SELECT trade_id, cycle_number, strike, credit_received,
                   close_cost, expiry, opened_at, status
            FROM {_TABLE_LEDGER}
            WHERE trade_id LIKE ? || '%'
            ORDER BY opened_at, cycle_number
            """,
            [ticker],
        ).fetchdf()
    except Exception as e:
        logger.debug(f"[DecisionLedger] get_roll_chain_summary failed: {e}")
        return {}

    if df.empty:
        return {}

    strikes = []
    credits = []
    close_costs = []
    dates = []

    for _, row in df.iterrows():
        s = row.get("strike")
        if pd.notna(s) and (not strikes or s != strikes[-1]):
            strikes.append(float(s))
        elif not strikes and pd.notna(s):
            strikes.append(float(s))

        cr = float(row.get("credit_received", 0) or 0)
        cc = float(row.get("close_cost", 0) or 0)
        credits.append(cr)
        close_costs.append(cc)

        opened = row.get("opened_at")
        if pd.notna(opened):
            dates.append(str(pd.Timestamp(opened).date()))
        else:
            dates.append("")

    total_gross = sum(credits)
    total_close = sum(close_costs)

    return {
        "ticker": ticker,
        "strike_chain": strikes,
        "cycle_credits": credits,
        "cycle_close_costs": close_costs,
        "cycle_dates": dates,
        "total_gross": round(total_gross, 2),
        "total_net": round(total_gross - total_close, 2),
        "total_close_cost": round(total_close, 2),
        "cycle_count": len(credits),
    }


# ── Execution Tracking ────────────────────────────────────────────────────────

def mark_action_executed(
    con,
    trade_id: str,
    action: str,
    notes: str = "",
    confirmed_by: str = "manual",
    strike_old: Optional[float] = None,
    strike_new: Optional[float] = None,
) -> bool:
    """Record that a recommendation was acted upon. Called from dashboard button."""
    try:
        con.execute(
            f"""
            INSERT INTO {_TABLE_EXECUTED}
                (trade_id, action, executed_at, confirmed_by, strike_old, strike_new, notes)
            VALUES (?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?)
            """,
            [trade_id, action, confirmed_by, strike_old, strike_new, notes],
        )
        logger.info(f"[DecisionLedger] Marked {action} executed for {trade_id}")
        return True
    except Exception as e:
        logger.warning(f"[DecisionLedger] mark_action_executed failed: {e}")
        return False


def get_recent_executions(
    con, trade_ids: List[str], within_days: int = 3
) -> pd.DataFrame:
    """Return recent execution marks for a list of trade_ids.

    Used by the suppression logic in run_all.py to avoid stale re-recommendations.
    """
    if not trade_ids:
        return pd.DataFrame()
    try:
        # Build placeholder list for IN clause
        placeholders = ", ".join(["?" for _ in trade_ids])
        return con.execute(
            f"""
            SELECT trade_id, action, executed_at, confirmed_by, strike_old, strike_new, notes
            FROM {_TABLE_EXECUTED}
            WHERE trade_id IN ({placeholders})
              AND executed_at >= CURRENT_TIMESTAMP - INTERVAL '{within_days}' DAY
            ORDER BY executed_at DESC
            """,
            trade_ids,
        ).fetchdf()
    except Exception as e:
        logger.debug(f"[DecisionLedger] get_recent_executions failed: {e}")
        return pd.DataFrame()


def auto_detect_executions(con, df_current: pd.DataFrame) -> int:
    """Detect executed actions by comparing current positions to prior recommendations.

    If the prior run recommended ROLL for a TradeID and the current data shows a
    different strike for the same underlying, auto-mark as executed.

    Returns count of auto-detected executions.
    """
    if df_current.empty:
        return 0

    try:
        # Get prior recommendations where Action was ROLL/EXIT
        prior = con.execute(
            f"""
            SELECT TradeID, Action, Strike, Underlying_Ticker
            FROM v_latest_recommendations
            WHERE Action IN ('ROLL', 'EXIT', 'TRIM')
              AND Strike IS NOT NULL
            """,
        ).fetchdf()
    except Exception as e:
        logger.debug(f"[DecisionLedger] auto_detect_executions query failed: {e}")
        return 0

    if prior.empty:
        return 0

    count = 0
    for _, prev_row in prior.iterrows():
        tid = prev_row["TradeID"]
        prev_action = prev_row["Action"]
        prev_strike = prev_row["Strike"]
        prev_ticker = prev_row["Underlying_Ticker"]

        if pd.isna(prev_strike) or not prev_ticker:
            continue

        # Check if the same ticker now has a different strike in current data
        curr_rows = df_current[
            (df_current.get("Underlying_Ticker", pd.Series()) == prev_ticker)
            & (df_current.get("Strike", pd.Series()).notna())
        ]
        if curr_rows.empty:
            # Ticker not in current data — could be closed OR data feed gap.
            # Do NOT auto-mark EXIT: a single missing snapshot is unreliable
            # (pre-market runs, API timeouts, partial feeds all cause false
            # positives). EXIT must be confirmed manually by the user.
            continue

        # For ROLL: check if any row for this ticker has a different strike
        if prev_action == "ROLL":
            curr_strikes = set(curr_rows["Strike"].dropna().astype(float).unique())
            if curr_strikes and float(prev_strike) not in curr_strikes:
                new_strike = max(curr_strikes)  # most likely the new roll target
                # Check if already marked
                existing = get_recent_executions(con, [tid], within_days=5)
                if existing.empty:
                    mark_action_executed(
                        con, tid, "ROLL",
                        confirmed_by="auto_detected",
                        strike_old=float(prev_strike),
                        strike_new=new_strike,
                        notes=f"Strike changed from ${prev_strike:.0f} to ${new_strike:.0f}",
                    )
                    count += 1

    return count
