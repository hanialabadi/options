"""
Management State Store
======================
Persists daily market-state for the management engine to `management_state` table
in pipeline.duckdb.

Solves the core oscillation problem: without memory, a condition (e.g. dead_cat_bounce)
that resolved this morning can be re-detected tomorrow if HV hasn't fully normalized,
causing HOLD → ROLL → HOLD flips within the same day.

State tracked per (trade_id, condition_type):
  - onset_ts        : when the condition was first detected
  - last_seen_ts    : last run where condition was still active (for age calculation)
  - resolved_ts     : when condition first resolved (None if still active)
  - resolve_count   : how many times this condition has resolved (oscillation counter)
  - last_action     : last doctrine Action for this trade
  - thesis_state    : last Thesis_State written for this trade
  - thesis_ts       : when thesis_state was last updated

Oscillation guard:
  If a condition resolves and then re-fires within MIN_RESOLVE_HOLD_HOURS, it is
  treated as the *same* condition (not a new onset).  This prevents the engine from
  treating every intraday dip as a fresh dead_cat_bounce.

Usage (in run_all.py):
    store = ManagementStateStore()
    prior = store.load()                              # load at run start
    ...
    monitor.persist_conditions(df, prior_state=prior) # pass age context
    ...
    store.save(df_final)                              # save at run end
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

import pandas as pd

from core.shared.data_contracts.config import PIPELINE_DB_PATH
from core.shared.data_layer.duckdb_utils import get_duckdb_connection

logger = logging.getLogger(__name__)

# A resolved condition must stay resolved for this many hours before the engine
# treats a re-fire as a genuinely new onset.
MIN_RESOLVE_HOLD_HOURS = 24


_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS management_state (
    trade_id        VARCHAR  NOT NULL,
    condition_type  VARCHAR  NOT NULL,
    onset_ts        TIMESTAMP NOT NULL,
    last_seen_ts    TIMESTAMP,
    resolved_ts     TIMESTAMP,
    resolve_count   INTEGER  DEFAULT 0,
    last_action     VARCHAR,
    thesis_state    VARCHAR,
    thesis_ts       TIMESTAMP,
    PRIMARY KEY (trade_id, condition_type)
)
"""


class ManagementStateStore:
    """
    Lightweight persistent state layer for the management engine.
    One row per (trade_id, condition_type) — upserted on every run.
    """

    def __init__(self, db_path: str | None = None):
        self.db_path = str(db_path or PIPELINE_DB_PATH)
        self._ensure_table()

    # ── Setup ─────────────────────────────────────────────────────────────────

    def _ensure_table(self) -> None:
        try:
            with get_duckdb_connection(read_only=False) as con:
                con.execute(_CREATE_SQL)
        except Exception as e:
            logger.warning(f"[StateStore] Could not init management_state table: {e}")

    # ── Public API ────────────────────────────────────────────────────────────

    def load(self) -> Dict[str, Dict]:
        """
        Load all management_state rows.

        Returns
        -------
        dict keyed by "{trade_id}::{condition_type}" → row dict with fields:
          onset_ts, last_seen_ts, resolved_ts, resolve_count,
          last_action, thesis_state, thesis_ts, days_active
        """
        try:
            with get_duckdb_connection(read_only=True) as con:
                rows = con.execute("""
                    SELECT trade_id, condition_type,
                           onset_ts, last_seen_ts, resolved_ts,
                           resolve_count, last_action,
                           thesis_state, thesis_ts
                    FROM management_state
                """).fetchall()
        except Exception as e:
            logger.warning(f"[StateStore] Load failed: {e}")
            return {}

        now = datetime.now(tz=timezone.utc)
        result: Dict[str, Dict] = {}
        for row in rows:
            (trade_id, ctype, onset_ts, last_seen_ts, resolved_ts,
             resolve_count, last_action, thesis_state, thesis_ts) = row

            onset_dt = _to_aware(onset_ts)
            days_active = (now - onset_dt).days if onset_dt else 0

            key = f"{trade_id}::{ctype}"
            result[key] = {
                "trade_id":      trade_id,
                "condition_type": ctype,
                "onset_ts":      onset_dt,
                "last_seen_ts":  _to_aware(last_seen_ts),
                "resolved_ts":   _to_aware(resolved_ts),
                "resolve_count": resolve_count or 0,
                "last_action":   last_action,
                "thesis_state":  thesis_state,
                "thesis_ts":     _to_aware(thesis_ts),
                "days_active":   days_active,
            }
        logger.debug(f"[StateStore] Loaded {len(result)} state rows.")
        return result

    def save(self, df: pd.DataFrame) -> None:
        """
        Upsert state from the final enriched/doctrine df.

        Reads per-trade:
          - Action          → last_action
          - Thesis_State    → thesis_state
          - _Active_Conditions  → active condition types (to update last_seen_ts)
          - _Condition_Resolved → resolved condition types (to set resolved_ts)
        """
        now = datetime.now(tz=timezone.utc)

        # Build rows to upsert
        rows_to_upsert: List[Dict] = []

        for _, row in df.iterrows():
            trade_id = str(row.get("TradeID", "") or "")
            if not trade_id:
                continue

            last_action  = str(row.get("Action", "") or "")
            thesis_state = str(row.get("Thesis_State", "") or "") or None
            thesis_ts    = now if thesis_state else None

            # Parse active conditions string → set of ctype names
            active_raw = str(row.get("_Active_Conditions", "") or "")
            active_ctypes = _parse_condition_names(active_raw)

            # Parse resolved conditions string → set of ctype names
            resolved_raw = str(row.get("_Condition_Resolved", "") or "")
            resolved_ctypes = _parse_condition_names(resolved_raw)

            # Emit one upsert per active or resolved condition type
            all_ctypes = active_ctypes | resolved_ctypes
            for ctype in all_ctypes:
                is_resolved = ctype in resolved_ctypes
                rows_to_upsert.append({
                    "trade_id":     trade_id,
                    "ctype":        ctype,
                    "last_action":  last_action,
                    "thesis_state": thesis_state,
                    "thesis_ts":    thesis_ts,
                    "is_resolved":  is_resolved,
                    "now":          now,
                })

            # Always upsert a thesis-state row even if no conditions active
            if thesis_state and not all_ctypes:
                rows_to_upsert.append({
                    "trade_id":     trade_id,
                    "ctype":        "__thesis__",
                    "last_action":  last_action,
                    "thesis_state": thesis_state,
                    "thesis_ts":    thesis_ts,
                    "is_resolved":  False,
                    "now":          now,
                })

        if not rows_to_upsert:
            return

        try:
            # Load prior state to preserve onset_ts and resolve_count
            prior = self.load()

            with get_duckdb_connection(read_only=False) as con:
                for r in rows_to_upsert:
                    key = f"{r['trade_id']}::{r['ctype']}"
                    prior_row = prior.get(key)

                    if prior_row is None:
                        # New condition — set onset
                        onset_ts    = r["now"]
                        resolve_cnt = 0
                        resolved_ts = r["now"] if r["is_resolved"] else None
                    else:
                        onset_ts    = prior_row["onset_ts"] or r["now"]
                        resolve_cnt = prior_row["resolve_count"] or 0
                        if r["is_resolved"]:
                            # Only increment resolve_count if previously unresolved
                            if prior_row["resolved_ts"] is None:
                                resolve_cnt += 1
                            resolved_ts = prior_row["resolved_ts"] or r["now"]
                        else:
                            # Re-firing: check oscillation guard
                            prev_resolved = prior_row["resolved_ts"]
                            if prev_resolved is not None:
                                hours_since = (r["now"] - prev_resolved).total_seconds() / 3600
                                if hours_since < MIN_RESOLVE_HOLD_HOURS:
                                    # Re-fire within guard window — treat as same episode.
                                    # Keep original onset_ts and resolve_count so
                                    # oscillation labeling accumulates correctly.
                                    logger.debug(
                                        f"[StateStore] {key} re-fired within {hours_since:.1f}h "
                                        f"— treating as continuation (oscillation guard)."
                                    )
                                else:
                                    # Resolution held for ≥ 24h → genuinely new episode.
                                    # Reset onset AND resolve_count so analytics start clean.
                                    onset_ts    = r["now"]
                                    resolve_cnt = 0
                                    logger.debug(
                                        f"[StateStore] {key} re-fired after {hours_since:.1f}h "
                                        f"— new condition cycle (onset reset, resolve_count reset)."
                                    )
                            resolved_ts = None

                    last_seen_ts = r["now"] if not r["is_resolved"] else prior_row["last_seen_ts"] if prior_row else None

                    con.execute("""
                        INSERT INTO management_state
                            (trade_id, condition_type, onset_ts, last_seen_ts,
                             resolved_ts, resolve_count, last_action,
                             thesis_state, thesis_ts)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (trade_id, condition_type) DO UPDATE SET
                            last_seen_ts   = excluded.last_seen_ts,
                            resolved_ts    = excluded.resolved_ts,
                            resolve_count  = excluded.resolve_count,
                            last_action    = excluded.last_action,
                            thesis_state   = excluded.thesis_state,
                            thesis_ts      = excluded.thesis_ts
                    """, [
                        r["trade_id"], r["ctype"], onset_ts, last_seen_ts,
                        resolved_ts, resolve_cnt, r["last_action"],
                        r["thesis_state"], r["thesis_ts"],
                    ])

            logger.info(f"[StateStore] Saved {len(rows_to_upsert)} state rows.")
        except Exception as e:
            logger.warning(f"[StateStore] Save failed: {e}")

    def get_days_active(self, trade_id: str, condition_type: str,
                        prior: Dict[str, Dict] | None = None) -> int:
        """
        Return how many calendar days a condition has been continuously active.
        Uses pre-loaded `prior` dict if provided (avoids a second DB hit).
        Returns 0 if no prior state found.
        """
        key = f"{trade_id}::{condition_type}"
        if prior is not None:
            row = prior.get(key)
        else:
            row = self.load().get(key)

        if row is None:
            return 0
        return row.get("days_active", 0)

    def is_oscillating(self, trade_id: str, condition_type: str,
                       prior: Dict[str, Dict] | None = None) -> bool:
        """
        Return True if this condition has resolved and re-fired more than once
        (resolve_count ≥ 2), indicating genuine oscillation rather than a clean signal.
        """
        key = f"{trade_id}::{condition_type}"
        row = (prior or {}).get(key) or self.load().get(key)
        if row is None:
            return False
        return (row.get("resolve_count") or 0) >= 2

    def get_thesis_state(self, trade_id: str,
                         prior: Dict[str, Dict] | None = None) -> Optional[str]:
        """Return last persisted Thesis_State for a trade (fallback when yfinance fails)."""
        key = f"{trade_id}::__thesis__"
        row = (prior or {}).get(key) or self.load().get(key)
        return row.get("thesis_state") if row else None


# ── Helpers ───────────────────────────────────────────────────────────────────

def _to_aware(ts) -> Optional[datetime]:
    """Convert DuckDB timestamp (naive or aware) to UTC-aware datetime."""
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    try:
        dt = pd.to_datetime(ts)
        return dt.to_pydatetime().replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.to_pydatetime()
    except Exception:
        return None


def _parse_condition_names(raw: str) -> set:
    """
    Parse _Active_Conditions / _Condition_Resolved string into a set of condition type names.

    Format examples:
      "dead_cat_bounce [day 3, val=1.00]"
      "iv_depressed resolved: IV rank recovered | dead_cat_bounce resolved: ..."
      "__thesis__"
    """
    if not raw or raw.strip() == "":
        return set()

    names = set()
    known = {
        "iv_backwardation", "theta_dominance", "itm_defense",
        "dead_cat_bounce", "iv_depressed", "__thesis__",
    }
    parts = raw.split("|")
    for part in parts:
        part = part.strip()
        for name in known:
            if part.startswith(name):
                names.add(name)
                break
    return names
