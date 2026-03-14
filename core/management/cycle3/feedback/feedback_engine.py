"""
Doctrine Feedback Engine
========================

Owns three responsibilities:

1. MFE/MAE Tracking (per-run)
   Update the option_price_extremes table with each run's Last price.
   Peak = MFE (max favorable excursion), Trough = MAE (max adverse excursion).

2. Closure Detection (per-run)
   Compare current TradeIDs to last-run TradeIDs. Disappeared IDs = closed.
   Write a full closure record to closed_trades with entry snapshot, exit
   snapshot, MFE/MAE, and outcome classification.

3. Doctrine Feedback Aggregation (per-closure)
   Update doctrine_feedback with rolling statistics keyed on condition buckets.
   Enforce minimum N guard before emitting TIGHTEN/RELAX suggestions.

All operations are non-blocking: DB failure logs a warning, never halts pipeline.

Design contract:
  - Deterministic: same data in → same record out
  - Book-anchored: outcome classification is delegated to outcome_classifier.py
  - Statistically guarded: TIGHTEN/RELAX only when N >= MIN_SAMPLE
  - Append-only: closed_trades is never updated; only new rows inserted
"""

from __future__ import annotations

import logging
import pandas as pd
from datetime import datetime, timezone
from typing import Optional

from core.management.cycle3.feedback.outcome_classifier import (
    classify_outcome, outcome_emoji, gate_tag_from_doctrine_source,
)

logger = logging.getLogger(__name__)

# Staged sample thresholds for feedback calibration.
# Partial signal at 5 trades (dampened by feedback_calibration.py consumer),
# full signal at 15. Prevents months of zero calibration while accumulating.
MIN_SAMPLE_PARTIAL = 5    # enough to classify direction
MIN_SAMPLE_FULL = 15      # full confidence — original threshold

# ── DDL ───────────────────────────────────────────────────────────────────────

_DDL_OPTION_EXTREMES = """
CREATE TABLE IF NOT EXISTS option_price_extremes (
    TradeID         VARCHAR NOT NULL,
    Strategy        VARCHAR,
    first_seen_ts   TIMESTAMP,
    last_seen_ts    TIMESTAMP,
    entry_price     DOUBLE,    -- Premium_Entry (absolute value, per share)
    peak_price      DOUBLE,    -- highest Last seen during hold = MFE proxy
    trough_price    DOUBLE,    -- lowest Last seen during hold = MAE proxy
    last_price      DOUBLE,    -- most recent Last
    entry_ul_price  DOUBLE,    -- UL Last at first_seen_ts
    PRIMARY KEY (TradeID)
)
"""

_DDL_CLOSED_TRADES = """
CREATE TABLE IF NOT EXISTS closed_trades (
    TradeID                 VARCHAR NOT NULL,
    Underlying_Ticker       VARCHAR,
    Strategy                VARCHAR,
    -- Entry snapshot
    Entry_TS                TIMESTAMP,
    Entry_UL_Price          DOUBLE,
    Entry_Premium           DOUBLE,
    Entry_DTE               DOUBLE,
    Entry_MomentumState     VARCHAR,
    Entry_IV_HV_Ratio       DOUBLE,
    Entry_RSI               DOUBLE,
    Entry_ROC20             DOUBLE,
    Entry_PCS               DOUBLE,
    Entry_Action            VARCHAR,
    Entry_Urgency           VARCHAR,
    -- Exit snapshot
    Exit_TS                 TIMESTAMP,
    Exit_UL_Price           DOUBLE,
    Exit_Premium            DOUBLE,
    Exit_DTE                DOUBLE,
    Exit_Reason             VARCHAR,
    Exit_Action             VARCHAR,
    -- Outcome metrics
    PnL_Pct                 DOUBLE,
    PnL_Dollar              DOUBLE,
    Days_Held               DOUBLE,
    MFE_Pct                 DOUBLE,
    MAE_Pct                 DOUBLE,
    -- Classification
    Outcome_Type            VARCHAR,
    Outcome_Emoji           VARCHAR,
    Gate_Failed             VARCHAR,    -- gate that SHOULD have caught the problem (retrospective)
    Gate_Fired              VARCHAR,    -- gate that ACTUALLY fired at exit (from Doctrine_Source)
    Outcome_Note            VARCHAR,
    -- Signal discipline
    Exit_Signal_Followed    BOOLEAN,
    -- Metadata
    closed_at               TIMESTAMP,
    PRIMARY KEY (TradeID)
)
"""

_DDL_DOCTRINE_FEEDBACK = """
CREATE TABLE IF NOT EXISTS doctrine_feedback (
    condition_key       VARCHAR NOT NULL,   -- e.g. "LONG_CALL::LATE_CYCLE"
    strategy            VARCHAR,
    condition_label     VARCHAR,            -- human readable
    sample_n            INTEGER,
    win_n               INTEGER,
    loss_n              INTEGER,
    warn_n              INTEGER,
    win_rate            DOUBLE,
    avg_pnl_pct         DOUBLE,
    avg_days_held       DOUBLE,
    avg_mfe_pct         DOUBLE,
    avg_mae_pct         DOUBLE,
    last_updated        TIMESTAMP,
    suggested_action    VARCHAR,            -- TIGHTEN / RELAX / HOLD / INSUFFICIENT_SAMPLE
    confidence          VARCHAR,            -- LOW / MEDIUM / HIGH / INSUFFICIENT_SAMPLE
    PRIMARY KEY (condition_key)
)
"""


# ── Public API ────────────────────────────────────────────────────────────────

def run_feedback_cycle(df_current: pd.DataFrame, con) -> None:
    """
    Main entry point. Called once per pipeline run after recommendations are persisted.

    df_current: the final df_final from this run (option legs only matters for Last prices).
    con: an open read-write DuckDB connection (pipeline.duckdb).
    """
    _ensure_tables(con)

    # 1. Update MFE/MAE extremes with this run's prices
    _update_extremes(df_current, con)

    # 2. Detect closures (TradeIDs in extremes but not in df_current)
    _detect_and_record_closures(df_current, con)

    # 3. Refresh aggregation table
    _refresh_feedback_aggregates(con)


# ── Internal helpers ──────────────────────────────────────────────────────────

def _ensure_tables(con) -> None:
    con.execute(_DDL_OPTION_EXTREMES)
    con.execute(_DDL_CLOSED_TRADES)
    con.execute(_DDL_DOCTRINE_FEEDBACK)
    # Migration: add Gate_Fired column to existing closed_trades tables (idempotent)
    try:
        con.execute("ALTER TABLE closed_trades ADD COLUMN Gate_Fired VARCHAR DEFAULT ''")
    except Exception:
        pass  # Column already exists — safe to ignore


def _update_extremes(df: pd.DataFrame, con) -> None:
    """
    For each live TradeID with a Last price, upsert into option_price_extremes.
    Tracks peak (MFE) and trough (MAE) over the full hold period.
    Only processes OPTION legs (AssetType == OPTION or has Call/Put).
    """
    opt_mask = (
        df.get("AssetType", pd.Series(dtype=str)).str.upper().isin(["OPTION", "OPTIONS"])
        | df.get("Call/Put", pd.Series(dtype=str)).notna()
    )
    opt = df[opt_mask].copy() if opt_mask.any() else df.copy()

    if opt.empty:
        return

    now_ts = datetime.now(timezone.utc)

    for _, row in opt.iterrows():
        trade_id = str(row.get("TradeID") or "")
        if not trade_id:
            continue

        last_raw  = row.get("Last")
        entry_raw = row.get("Premium_Entry")
        ul_raw    = row.get("UL Last") or row.get("Underlying_Price_Entry")
        strategy  = str(row.get("Strategy") or "")
        snap_ts   = row.get("Snapshot_TS")

        try:
            last_price  = abs(float(last_raw))  if last_raw  is not None and not pd.isna(last_raw)  else None
            entry_price = abs(float(entry_raw)) if entry_raw is not None and not pd.isna(entry_raw) else None
            ul_price    = float(ul_raw)          if ul_raw    is not None and not pd.isna(ul_raw)    else None
        except (TypeError, ValueError):
            continue

        if last_price is None:
            continue

        existing = con.execute(
            "SELECT peak_price, trough_price FROM option_price_extremes WHERE TradeID = ?",
            [trade_id]
        ).fetchone()

        if existing is None:
            # First time seeing this trade
            con.execute("""
                INSERT INTO option_price_extremes
                    (TradeID, Strategy, first_seen_ts, last_seen_ts,
                     entry_price, peak_price, trough_price, last_price, entry_ul_price)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                trade_id, strategy,
                snap_ts or now_ts, snap_ts or now_ts,
                entry_price, last_price, last_price, last_price, ul_price
            ])
        else:
            peak, trough = existing
            new_peak   = max(peak   or last_price, last_price)
            new_trough = min(trough or last_price, last_price)
            con.execute("""
                UPDATE option_price_extremes
                SET last_seen_ts = ?,
                    peak_price   = ?,
                    trough_price = ?,
                    last_price   = ?
                WHERE TradeID = ?
            """, [snap_ts or now_ts, new_peak, new_trough, last_price, trade_id])


def _detect_and_record_closures(df_current: pd.DataFrame, con) -> None:
    """
    A trade is closed when its TradeID was previously in option_price_extremes
    but is absent from the current run's df.

    Two-pass detection:
    1. Primary: TradeID in extremes but not in df_current (normal per-run detection).
    2. Retroactive: last_seen_ts is more than 2 hours older than the latest snapshot
       in management_recommendations. Catches trades that closed during pipeline gaps
       (e.g. runs skipped over several days while positions expired).
    """
    live_ids = set(
        str(t) for t in df_current["TradeID"].dropna().unique()
        if str(t).strip()
    )

    # All TradeIDs we have tracked extremes for (with timestamps for retroactive pass)
    extremes_df = con.execute(
        "SELECT TradeID, last_seen_ts FROM option_price_extremes"
    ).fetchdf()
    tracked = extremes_df["TradeID"].tolist()

    # Already recorded as closed
    already_closed = set(
        con.execute("SELECT TradeID FROM closed_trades").fetchdf()["TradeID"].tolist()
    )

    # Pass 1: primary detection — not in current run's live positions
    pass1 = [t for t in tracked if t not in live_ids and t not in already_closed]

    # Pass 2: retroactive — last_seen_ts significantly stale vs latest snapshot.
    # Threshold: 2 hours (covers market session gaps). Management runs typically
    # happen every few hours during market hours. If a trade's last_seen_ts is
    # more than 2h behind the most recent management_recommendations snapshot,
    # it was not present in that run — treat as closed.
    pass2: list[str] = []
    try:
        latest_snap_row = con.execute(
            "SELECT MAX(Snapshot_TS) AS latest FROM management_recommendations"
        ).fetchone()
        if latest_snap_row and latest_snap_row[0] is not None:
            latest_snap = pd.Timestamp(latest_snap_row[0])
            _staleness_threshold_hours = 2.0
            for _, row in extremes_df.iterrows():
                tid = str(row["TradeID"])
                if tid in live_ids or tid in already_closed or tid in pass1:
                    continue
                last_seen = pd.Timestamp(row["last_seen_ts"]) if row["last_seen_ts"] else None
                if last_seen is not None:
                    staleness_h = (latest_snap - last_seen).total_seconds() / 3600
                    if staleness_h > _staleness_threshold_hours:
                        pass2.append(tid)
    except Exception as _retro_err:
        logger.debug(f"[FeedbackEngine] Retroactive pass failed (non-fatal): {_retro_err}")

    newly_closed = pass1 + pass2

    if not newly_closed:
        return

    if pass2:
        logger.info(
            f"[FeedbackEngine] Detected {len(pass1)} primary + {len(pass2)} retroactive "
            f"closed trade(s): {newly_closed}"
        )
    else:
        logger.info(f"[FeedbackEngine] Detected {len(newly_closed)} newly closed trade(s): {newly_closed}")

    for trade_id in newly_closed:
        _record_closure(trade_id, con)


def _safe_float(val, default=None):
    """Coerce val to float, returning default on NaN/None/NA/non-numeric."""
    if val is None:
        return default
    try:
        import math
        f = float(val)
        return default if math.isnan(f) else f
    except (TypeError, ValueError):
        return default


def _record_closure(trade_id: str, con) -> None:
    """
    Pull entry snapshot (first run) and exit snapshot (last run) from
    management_recommendations, combine with MFE/MAE extremes, classify outcome,
    write to closed_trades.
    """
    try:
        # Entry snapshot — earliest run for this TradeID
        entry_df = con.execute("""
            SELECT
                Underlying_Ticker, Strategy,
                Snapshot_TS         AS entry_ts,
                "UL Last"           AS ul_last,
                Premium_Entry,
                DTE,
                MomentumVelocity_State,
                IV_30D, HV_20D,
                rsi_14, roc_20,
                NULL                AS PCS,
                Action, Urgency,
                Total_GL_Decimal
            FROM management_recommendations
            WHERE TradeID = ?
            ORDER BY Snapshot_TS ASC
            LIMIT 1
        """, [trade_id]).fetchdf()

        # Exit snapshot — latest run
        exit_df = con.execute("""
            SELECT
                Snapshot_TS         AS exit_ts,
                "UL Last"           AS ul_last,
                Last,
                DTE,
                Action,
                Doctrine_Source,
                Total_GL_Decimal,
                Premium_Entry
            FROM management_recommendations
            WHERE TradeID = ?
            ORDER BY Snapshot_TS DESC
            LIMIT 1
        """, [trade_id]).fetchdf()

        # MFE/MAE
        ext_df = con.execute("""
            SELECT entry_price, peak_price, trough_price
            FROM option_price_extremes
            WHERE TradeID = ?
        """, [trade_id]).fetchdf()

        if entry_df.empty or exit_df.empty:
            logger.warning(f"[FeedbackEngine] Missing entry or exit snapshot for {trade_id}, skipping closure.")
            return

        e = entry_df.iloc[0]
        x = exit_df.iloc[0]
        ext = ext_df.iloc[0] if not ext_df.empty else {}

        # ── Derive metrics ────────────────────────────────────────────────────
        entry_premium = abs(float(e.get("Premium_Entry") or 0))
        exit_premium  = abs(float(x.get("Last") or x.get("Premium_Entry") or 0))

        pnl_pct = (
            (exit_premium - entry_premium) / entry_premium
            if entry_premium > 0 else 0.0
        )
        pnl_dollar = (exit_premium - entry_premium) * 100  # per contract

        entry_ts = pd.Timestamp(e["entry_ts"])
        exit_ts  = pd.Timestamp(x["exit_ts"])
        days_held = max(0.0, (exit_ts - entry_ts).total_seconds() / 86400)

        peak   = float(ext.get("peak_price")   or exit_premium or entry_premium)
        trough = float(ext.get("trough_price") or exit_premium or entry_premium)
        mfe_pct = (peak   - entry_premium) / entry_premium if entry_premium > 0 else None
        mae_pct = (trough - entry_premium) / entry_premium if entry_premium > 0 else None

        entry_iv  = float(e.get("IV_30D") or 0)
        entry_hv  = float(e.get("HV_20D") or 0)
        iv_hv_ratio = (entry_iv / entry_hv) if entry_hv > 0 else None

        # Was the exit signal followed?
        # Proxy: if final Action was EXIT and trade closed, assume yes.
        # If final Action was HOLD and trade closed (broker action), signal was NOT followed.
        exit_action_str  = str(x.get("Action") or "").upper()
        exit_signal_followed = exit_action_str in ("EXIT", "EXPIRED", "ASSIGNED", "ROLL")

        # ── Outcome classification ────────────────────────────────────────────
        outcome_type, gate_failed, outcome_note = classify_outcome(
            strategy              = str(e.get("Strategy") or ""),
            pnl_pct               = pnl_pct,
            days_held             = days_held,
            entry_momentum_state  = str(e.get("MomentumVelocity_State") or ""),
            entry_iv_hv_ratio     = iv_hv_ratio,
            entry_rsi             = _safe_float(e.get("rsi_14"), default=50.0),
            entry_roc20           = _safe_float(e.get("roc_20"), default=0.0),
            entry_pcs             = None,  # PCS column removed from schema
            entry_dte             = _safe_float(e.get("DTE"), default=0.0),
            exit_action           = exit_action_str,
            exit_doctrine_source  = str(x.get("Doctrine_Source") or ""),
            exit_signal_followed  = exit_signal_followed,
            mfe_pct               = mfe_pct,
            mae_pct               = mae_pct,
        )

        emoji = outcome_emoji(outcome_type)

        # Derive the structured gate tag from the Doctrine_Source that actually fired.
        # gate_failed = what should have caught the problem (retrospective classifier).
        # gate_fired  = what gate actually fired at exit (structured, from Doctrine_Source).
        exit_doctrine_source_str = str(x.get("Doctrine_Source") or "")
        gate_fired = gate_tag_from_doctrine_source(exit_doctrine_source_str)

        # ── Write closure record ──────────────────────────────────────────────
        # Use named columns to avoid positional mismatch with ALTER TABLE'd schema
        con.execute("""
            INSERT OR REPLACE INTO closed_trades (
                TradeID, Underlying_Ticker, Strategy,
                Entry_TS, Entry_UL_Price, Entry_Premium, Entry_DTE,
                Entry_MomentumState, Entry_IV_HV_Ratio, Entry_RSI, Entry_ROC20,
                Entry_PCS, Entry_Action, Entry_Urgency,
                Exit_TS, Exit_UL_Price, Exit_Premium, Exit_DTE,
                Exit_Reason, Exit_Action,
                PnL_Pct, PnL_Dollar, Days_Held, MFE_Pct, MAE_Pct,
                Outcome_Type, Outcome_Emoji, Gate_Failed, Gate_Fired, Outcome_Note,
                Exit_Signal_Followed, closed_at
            ) VALUES (
                ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?
            )
        """, [
            trade_id,
            str(e.get("Underlying_Ticker") or ""),
            str(e.get("Strategy") or ""),
            # Entry
            entry_ts, _safe_float(e.get("ul_last")),
            entry_premium,
            _safe_float(e.get("DTE")),
            str(e.get("MomentumVelocity_State") or ""),
            iv_hv_ratio,
            _safe_float(e.get("rsi_14"), default=0.0),
            _safe_float(e.get("roc_20"), default=0.0),
            None,  # PCS removed from schema
            str(e.get("Action") or ""),
            str(e.get("Urgency") or ""),
            # Exit
            exit_ts, _safe_float(x.get("ul_last")),
            exit_premium,
            _safe_float(x.get("DTE")),
            exit_doctrine_source_str,
            exit_action_str,
            # Outcome metrics
            pnl_pct, pnl_dollar, days_held, mfe_pct, mae_pct,
            # Classification
            outcome_type, emoji, gate_failed or "", gate_fired, outcome_note,
            # Signal discipline
            exit_signal_followed,
            # Meta
            datetime.now(timezone.utc),
        ])

        logger.info(
            f"[FeedbackEngine] Closed trade recorded: {trade_id} "
            f"| {outcome_type} {emoji} | P&L={pnl_pct:+.0%} | {days_held:.0f}d"
        )

    except Exception as err:
        logger.warning(f"[FeedbackEngine] Failed to record closure for {trade_id}: {err}")


def _refresh_feedback_aggregates(con) -> None:
    """
    Rebuild doctrine_feedback from closed_trades.
    Keyed on (strategy, entry_momentum_state) condition buckets.
    Adds HIGH RSI and IV>HV sub-buckets where data is available.
    Only emits TIGHTEN/RELAX when N >= MIN_SAMPLE.
    """
    try:
        closed = con.execute("""
            SELECT
                Strategy,
                Entry_MomentumState,
                Entry_IV_HV_Ratio,
                Entry_RSI,
                Outcome_Type,
                PnL_Pct,
                Days_Held,
                MFE_Pct,
                MAE_Pct
            FROM closed_trades
        """).fetchdf()

        if closed.empty:
            return

        from core.management.cycle3.feedback.outcome_classifier import WIN_TYPES, WARN_TYPES, LOSS_TYPES

        def _bucket_key(row):
            strat = str(row.get("Strategy") or "UNKNOWN").upper()
            mom   = str(row.get("Entry_MomentumState") or "UNKNOWN").upper()
            return f"{strat}::{mom}"

        def _bucket_label(row):
            strat = str(row.get("Strategy") or "UNKNOWN")
            mom   = str(row.get("Entry_MomentumState") or "UNKNOWN")
            return f"{strat} entered during {mom}"

        closed["_key"]   = closed.apply(_bucket_key,   axis=1)
        closed["_label"] = closed.apply(_bucket_label, axis=1)

        rows = []
        for key, grp in closed.groupby("_key"):
            n        = len(grp)
            win_n    = int(grp["Outcome_Type"].isin(WIN_TYPES).sum())
            loss_n   = int(grp["Outcome_Type"].isin(LOSS_TYPES).sum())
            warn_n   = int(grp["Outcome_Type"].isin(WARN_TYPES).sum())
            win_rate = win_n / n if n > 0 else 0.0
            avg_pnl  = float(grp["PnL_Pct"].mean())
            avg_days = float(grp["Days_Held"].mean())
            avg_mfe  = float(grp["MFE_Pct"].dropna().mean()) if grp["MFE_Pct"].notna().any() else None
            avg_mae  = float(grp["MAE_Pct"].dropna().mean()) if grp["MAE_Pct"].notna().any() else None
            label    = grp["_label"].iloc[0]
            strat    = str(grp["Strategy"].iloc[0] or "")

            if n < MIN_SAMPLE_PARTIAL:
                suggested = "INSUFFICIENT_SAMPLE"
                confidence = "INSUFFICIENT_SAMPLE"
            elif win_rate < 0.35 and avg_pnl < -0.10:
                suggested  = "TIGHTEN"
                confidence = "HIGH" if n >= 30 else ("MEDIUM" if n >= MIN_SAMPLE_FULL else "LOW")
            elif win_rate > 0.70 and avg_pnl > 0.20:
                suggested  = "REINFORCE"
                confidence = "HIGH" if n >= 30 else ("MEDIUM" if n >= MIN_SAMPLE_FULL else "LOW")
            else:
                suggested  = "HOLD"
                confidence = "MEDIUM" if n >= MIN_SAMPLE_FULL else "LOW"

            rows.append({
                "condition_key":    key,
                "strategy":         strat,
                "condition_label":  label,
                "sample_n":         n,
                "win_n":            win_n,
                "loss_n":           loss_n,
                "warn_n":           warn_n,
                "win_rate":         win_rate,
                "avg_pnl_pct":      avg_pnl,
                "avg_days_held":    avg_days,
                "avg_mfe_pct":      avg_mfe,
                "avg_mae_pct":      avg_mae,
                "last_updated":     datetime.now(timezone.utc),
                "suggested_action": suggested,
                "confidence":       confidence,
            })

        if not rows:
            return

        df_agg = pd.DataFrame(rows)
        # Replace entire aggregation table (derived from closed_trades, fully recomputable)
        con.execute("DELETE FROM doctrine_feedback")
        con.execute("INSERT INTO doctrine_feedback SELECT * FROM df_agg")
        logger.info(f"[FeedbackEngine] Refreshed doctrine_feedback: {len(rows)} condition buckets.")

    except Exception as err:
        logger.warning(f"[FeedbackEngine] Aggregate refresh failed: {err}")
