"""
Management Condition Monitor
=============================
Persists "monitor" conditions to DuckDB and auto-resolves them on subsequent runs.

When doctrine HOLDs a position with a monitoring note (e.g., "monitor for vol normalization"),
this module:
1. Writes the condition to `management_conditions` table.
2. On every subsequent run, re-evaluates ACTIVE conditions against current data.
3. When a condition resolves → writes updated Action/Urgency/Rationale into the df
   BEFORE doctrine runs (so doctrine sees a pre-resolved signal).

Five condition types:
  iv_backwardation  — LEAP + IV slope < -1.5pt.  Resolves when slope > -0.5.
  theta_dominance   — THETA_DOMINANT + FLAT + DTE≤60 + pnl<-20%.  Resolves when DTE≤10 (window expired).
  itm_defense       — SHORT_PUT + ITM + DTE≤14.  Resolves when price moves OTM (price > strike).
  dead_cat_bounce   — RecoveryQuality_State = DEAD_CAT_BOUNCE.  Blocks discretionary rolls.
                      Resolves when state flips to STRUCTURAL_RECOVERY.
  iv_depressed      — Dual-threshold: IV_Rank_30D < 30 (bottom 30th percentile of own history)
                      AND IV_30D / HV_20D < 0.85 (premium structurally below realized vol).
                      Resolves when IV_Rank_30D ≥ 40 OR IV/HV ratio ≥ 0.90.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import List, Dict, Optional

import pandas as pd

logger = logging.getLogger(__name__)

_DB_PATH = "data/pipeline.duckdb"

# ── Table DDL ─────────────────────────────────────────────────────────────────

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS management_conditions (
    condition_id   VARCHAR PRIMARY KEY,
    trade_id       VARCHAR NOT NULL,
    leg_id         VARCHAR,
    condition_type VARCHAR NOT NULL,
    description    VARCHAR,
    metric_col     VARCHAR,
    operator       VARCHAR,
    threshold      DOUBLE,
    current_value  DOUBLE,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_checked   TIMESTAMP,
    resolved_at    TIMESTAMP,
    status         VARCHAR DEFAULT 'ACTIVE',
    check_count    INTEGER DEFAULT 0
)
"""


class ConditionMonitor:
    """
    Detect, persist, and resolve monitoring conditions for open positions.
    Mirrors the scan engine's wait_loop pattern — but for position management.
    """

    def __init__(self, db_path: str = _DB_PATH):
        self.db_path = db_path
        self._ensure_table()

    # ── Setup ─────────────────────────────────────────────────────────────────

    def _ensure_table(self) -> None:
        try:
            import duckdb
            with duckdb.connect(self.db_path) as con:
                con.execute(_CREATE_TABLE_SQL)
        except Exception as e:
            logger.warning(f"[ConditionMonitor] Could not init table: {e}")

    # ── Public API ────────────────────────────────────────────────────────────

    def persist_conditions(self, df: pd.DataFrame, con=None,
                           prior_state: dict | None = None) -> None:
        """
        Scan df for monitoring triggers.  Upsert ACTIVE conditions to DuckDB.
        Existing ACTIVE conditions are updated (current_value, check_count).

        prior_state : dict returned by ManagementStateStore.load() — used to
                      annotate days_active on conditions so doctrine can see
                      how long a condition has been running.
        """
        conditions = self._detect_conditions(df)
        if not conditions:
            return
        # Annotate days_active from prior state (non-blocking — defaults to 0)
        if prior_state:
            for c in conditions:
                key = f"{c['trade_id']}::{c['condition_type']}"
                prior_row = prior_state.get(key)
                c['days_active'] = prior_row.get('days_active', 0) if prior_row else 0
        self._upsert_conditions(conditions, con)
        logger.info(f"[ConditionMonitor] Persisted {len(conditions)} conditions to DuckDB.")

    def check_conditions(self, df: pd.DataFrame, con=None) -> List[Dict]:
        """
        Load ACTIVE conditions from DuckDB, evaluate against current df.
        Returns list of resolved condition dicts (with resolution metadata).
        """
        active = self._load_active(con)
        if not active:
            return []

        resolved = []
        still_active = []
        now = datetime.now(tz=timezone.utc)

        for cond in active:
            trade_id = cond['trade_id']
            ctype = cond['condition_type']

            # Find matching rows in current df
            mask = self._get_trade_mask(df, trade_id)
            if not mask.any():
                # Position no longer in portfolio — abandon
                cond['_resolution'] = 'ABANDONED'
                cond['_rationale'] = "Position no longer in portfolio."
                resolved.append(cond)
                continue

            row = df[mask].iloc[0]
            cond['_current_value'] = self._get_current_value(row, ctype)
            cond['_check_ts'] = now

            resolution = self._evaluate_resolution(row, cond)
            if resolution:
                cond['_resolution'] = 'RESOLVED'
                cond['_rationale'] = resolution['rationale']
                cond['_action'] = resolution.get('action', 'HOLD')
                cond['_urgency'] = resolution.get('urgency', 'LOW')
                resolved.append(cond)
            else:
                still_active.append(cond)

        # Persist resolution and update check counts
        self._mark_resolved(resolved, con)
        self._update_checked(still_active, con)

        logger.info(
            f"[ConditionMonitor] Checked {len(active)} conditions: "
            f"{len(resolved)} resolved, {len(still_active)} still active."
        )
        return resolved

    def apply_resolutions(self, df: pd.DataFrame, resolved: List[Dict],
                          prior_state: dict | None = None) -> pd.DataFrame:
        """
        Write resolution metadata into df BEFORE doctrine runs.
        Sets:
          _Condition_Resolved  — e.g. "iv_backwardation resolved: IV normalized"
          _Active_Conditions   — e.g. "theta_dominance [day 3, val=1.00] ⚠️ oscillating"
        Doctrine reads these to prepend context to rationale.

        prior_state : dict from ManagementStateStore.load() — supplies days_active
                      and oscillation flag so rationale is richer and doctrine can
                      weight condition severity by persistence.
        """
        if '_Condition_Resolved' not in df.columns:
            df['_Condition_Resolved'] = ''
        if '_Active_Conditions' not in df.columns:
            df['_Active_Conditions'] = ''

        for cond in resolved:
            trade_id = cond['trade_id']
            mask = self._get_trade_mask(df, trade_id)
            if not mask.any():
                continue
            existing = df.loc[mask, '_Condition_Resolved'].iloc[0] or ''
            note = f"{cond['condition_type']} resolved: {cond.get('_rationale','')}"
            df.loc[mask, '_Condition_Resolved'] = (existing + ' | ' + note).lstrip(' | ')

            # Also override Action/Urgency if resolution specifies
            if cond.get('_action'):
                df.loc[mask, '_Resolved_Action'] = cond['_action']
                df.loc[mask, '_Resolved_Urgency'] = cond['_urgency']

        # Annotate still-active conditions with persistence + oscillation context
        active = self._load_active()
        for cond in active:
            trade_id = cond['trade_id']
            mask = self._get_trade_mask(df, trade_id)
            if not mask.any():
                continue

            # Prefer days_active from state store (calendar days) over check_count (run count)
            prior_row = (prior_state or {}).get(f"{trade_id}::{cond['condition_type']}")
            days = prior_row.get('days_active', 0) if prior_row else cond.get('check_count', 0)
            oscillating = (prior_row.get('resolve_count', 0) >= 2) if prior_row else False

            val = cond.get('current_value')
            val_str = f"{val:.2f}" if val is not None else "?"
            osc_flag = " ⚠️ oscillating" if oscillating else ""
            note = f"{cond['condition_type']} [day {days}, val={val_str}]{osc_flag}"
            existing = df.loc[mask, '_Active_Conditions'].iloc[0] or ''
            df.loc[mask, '_Active_Conditions'] = (existing + ' | ' + note).lstrip(' | ')

        return df

    # ── Detection ─────────────────────────────────────────────────────────────

    def _detect_conditions(self, df: pd.DataFrame) -> List[Dict]:
        """Scan df for rows that trigger a monitor condition."""
        conditions = []
        now = datetime.now(tz=timezone.utc)

        for _, row in df.iterrows():
            trade_id = str(row.get('TradeID', '') or '')
            leg_id = str(row.get('LegID', '') or '')
            strategy = str(row.get('Strategy', '') or '').upper()
            if not trade_id:
                continue

            # ── 1. IV Backwardation (LEAP positions only) ──────────────────
            is_leap = float(row.get('DTE', 0) or 0) >= 180
            iv_shape = str(row.get('iv_surface_shape', '') or '').upper()
            slope = row.get('iv_ts_slope_30_90')
            if (is_leap
                    and iv_shape == 'BACKWARDATION'
                    and slope is not None
                    and not pd.isna(slope)
                    and float(slope) < -1.5):
                conditions.append({
                    'condition_id':   f"{trade_id}_iv_backwardation",
                    'trade_id':       trade_id,
                    'leg_id':         leg_id,
                    'condition_type': 'iv_backwardation',
                    'description':    'IV term structure inverted on LEAP — monitor for normalization',
                    'metric_col':     'iv_ts_slope_30_90',
                    'operator':       'gt',          # resolves when slope > threshold
                    'threshold':      -0.5,
                    'current_value':  float(slope),
                    'created_at':     now,
                    'last_checked':   now,
                    'status':         'ACTIVE',
                    'check_count':    0,
                })

            # ── 2. Theta Dominance (long options with time running out) ────
            is_option = str(row.get('AssetType', '') or '').upper() == 'OPTION'
            long_strategy = strategy in ('LONG_CALL', 'LONG_PUT', 'LEAPS_CALL',
                                         'LEAPS_PUT', 'BUY_CALL', 'BUY_PUT')
            dte = float(row.get('DTE', 999) or 999)
            pnl_pct = float(row.get('Total_GL_Decimal', 0) or 0)
            drift_dir = str(row.get('Drift_Direction', '') or '').upper()
            greek_state = str(row.get('GreekDominance_State', '') or '').upper()
            mom_state = str(row.get('MomentumVelocity_State', '') or '').upper()

            if (is_option
                    and long_strategy
                    and dte <= 60
                    and pnl_pct < -0.20
                    and 'THETA' in greek_state
                    and drift_dir == 'FLAT'
                    and mom_state in ('REVERSING', 'STALLING', 'UNKNOWN')):
                conditions.append({
                    'condition_id':   f"{trade_id}_theta_dominance",
                    'trade_id':       trade_id,
                    'leg_id':         leg_id,
                    'condition_type': 'theta_dominance',
                    'description':    'Theta dominant with flat price — monitor for catalyst within DTE window',
                    'metric_col':     'DTE',
                    'operator':       'lt',          # resolves (window expired) when DTE < threshold
                    'threshold':      10.0,
                    'current_value':  dte,
                    'created_at':     now,
                    'last_checked':   now,
                    'status':         'ACTIVE',
                    'check_count':    0,
                })

            # ── 3. ITM Defense (short put approaching expiry ITM) ──────────
            is_short_put = strategy == 'CSP'
            moneyness = str(row.get('Moneyness_Label', '') or '').upper()
            if (is_short_put
                    and moneyness == 'ITM'
                    and dte <= 14):
                conditions.append({
                    'condition_id':   f"{trade_id}_itm_defense",
                    'trade_id':       trade_id,
                    'leg_id':         leg_id,
                    'condition_type': 'itm_defense',
                    'description':    'Short put ITM near expiry — monitor for price recovery above strike',
                    'metric_col':     'Moneyness_Label',
                    'operator':       'eq',          # resolves when OTM
                    'threshold':      0.0,           # unused for string comparison
                    'current_value':  dte,
                    'created_at':     now,
                    'last_checked':   now,
                    'status':         'ACTIVE',
                    'check_count':    0,
                })

            # ── 4. Dead-Cat Bounce (blocks discretionary rolls) ─────────────
            # RecoveryQuality_State = DEAD_CAT_BOUNCE means a short-term uptick is
            # occurring inside a still-broken structure.  Rolling now would lock in
            # a strike at an artificially elevated price.  Passarelli Ch.6.
            rq_state = str(row.get('RecoveryQuality_State', '') or '').upper()
            if rq_state == 'DEAD_CAT_BOUNCE':
                conditions.append({
                    'condition_id':   f"{trade_id}_dead_cat_bounce",
                    'trade_id':       trade_id,
                    'leg_id':         leg_id,
                    'condition_type': 'dead_cat_bounce',
                    'description':    (
                        'Dead-cat bounce detected — structure has not changed. '
                        'Wait for higher low + break above prior swing high + ROC10 > 0 + EMA20 up.'
                    ),
                    'metric_col':     'RecoveryQuality_State',
                    'operator':       'eq',
                    'threshold':      0.0,           # unused for string comparison
                    'current_value':  0.0,           # presence of DEAD_CAT is the signal
                    'created_at':     now,
                    'last_checked':   now,
                    'status':         'ACTIVE',
                    'check_count':    0,
                })

            # ── 5. IV Depressed (dual-threshold — don't sell premium into a vol trough) ─
            # Condition is active only when BOTH:
            #   (a) IV_Rank_30D < 30  — IV is in the bottom 30th pctile of its own history
            #   (b) IV_30D / HV_20D < 0.85  — premium is structurally below realized vol
            # Neither alone is sufficient: HV can collapse too, making the ratio misleading.
            # Natenberg Ch.5: premium sellers need IV > HV edge; Passarelli Ch.4: rank matters.
            iv_rank = row.get('IV_Rank_30D')
            iv_30d_raw = row.get('IV_30D')
            hv_20d_raw = row.get('HV_20D')

            iv_rank_ok = iv_rank is not None and not pd.isna(iv_rank)
            iv_hv_ok   = (iv_30d_raw is not None and not pd.isna(iv_30d_raw)
                          and hv_20d_raw is not None and not pd.isna(hv_20d_raw)
                          and float(hv_20d_raw) > 0)

            if iv_rank_ok and iv_hv_ok:
                iv_rank_val = float(iv_rank)
                iv_hv_ratio = float(iv_30d_raw) / float(hv_20d_raw)
                if iv_rank_val < 30 and iv_hv_ratio < 0.85:
                    conditions.append({
                        'condition_id':   f"{trade_id}_iv_depressed",
                        'trade_id':       trade_id,
                        'leg_id':         leg_id,
                        'condition_type': 'iv_depressed',
                        'description':    (
                            f'IV depressed: rank={iv_rank_val:.0f}/100, '
                            f'IV/HV ratio={iv_hv_ratio:.2f}. '
                            'Wait for IV rank ≥ 40 OR IV/HV ≥ 0.90 before selling premium.'
                        ),
                        'metric_col':     'IV_Rank_30D',
                        'operator':       'gte',       # resolves when rank ≥ threshold
                        'threshold':      40.0,
                        'current_value':  iv_rank_val,
                        'created_at':     now,
                        'last_checked':   now,
                        'status':         'ACTIVE',
                        'check_count':    0,
                    })

        return conditions

    # ── Resolution Evaluation ─────────────────────────────────────────────────

    def _evaluate_resolution(self, row: pd.Series, cond: Dict) -> Optional[Dict]:
        """
        Check if a condition has resolved.  Returns resolution dict or None.
        """
        ctype = cond['condition_type']

        if ctype == 'iv_backwardation':
            slope = row.get('iv_ts_slope_30_90')
            if slope is not None and not pd.isna(slope) and float(slope) > -0.5:
                return {
                    'rationale': (
                        f"IV term structure normalized: slope={float(slope):+.1f}pt "
                        f"(was {cond.get('current_value', 0):+.1f}pt). "
                        f"LEAP carry structure improving — thesis confirmed (Natenberg Ch.5)."
                    ),
                    'action': 'HOLD',
                    'urgency': 'LOW',
                }
            # Also resolve if shape is no longer BACKWARDATION
            iv_shape = str(row.get('iv_surface_shape', '') or '').upper()
            if iv_shape in ('CONTANGO', 'FLAT'):
                return {
                    'rationale': (
                        f"IV term structure flipped to {iv_shape}. "
                        f"Backwardation resolved — LEAP thesis validated (Natenberg Ch.5)."
                    ),
                    'action': 'HOLD',
                    'urgency': 'LOW',
                }

        elif ctype == 'theta_dominance':
            dte = float(row.get('DTE', 999) or 999)
            if dte <= 10:
                # Window expired without catalyst
                return {
                    'rationale': (
                        f"Monitor window expired at DTE={dte:.0f} — no directional catalyst "
                        f"materialized during theta-dominant phase. Exit to recover residual value "
                        f"(Passarelli Ch.2: Theta Awareness)."
                    ),
                    'action': 'EXIT',
                    'urgency': 'LOW',
                }
            # Also resolve if momentum improved (catalyst appeared)
            greek_state = str(row.get('GreekDominance_State', '') or '').upper()
            mom_state = str(row.get('MomentumVelocity_State', '') or '').upper()
            if 'DELTA' in greek_state or mom_state in ('ACCELERATING', 'TRENDING'):
                return {
                    'rationale': (
                        f"Catalyst appeared: {greek_state} with {mom_state} momentum. "
                        f"Theta dominance phase ended — reassess directional thesis (Passarelli Ch.2)."
                    ),
                    'action': 'HOLD',
                    'urgency': 'LOW',
                }

        elif ctype == 'itm_defense':
            moneyness = str(row.get('Moneyness_Label', '') or '').upper()
            if moneyness == 'OTM':
                return {
                    'rationale': (
                        f"Short put moved OTM — assignment risk resolved. "
                        f"Position returned to safe zone (McMillan: Assignment Management)."
                    ),
                    'action': 'HOLD',
                    'urgency': 'LOW',
                }
            dte = float(row.get('DTE', 999) or 999)
            if dte <= 2:
                return {
                    'rationale': (
                        f"ITM at DTE={dte:.0f} — expiration imminent. "
                        f"Roll or accept assignment now (McMillan: Expiration Management)."
                    ),
                    'action': 'ROLL',
                    'urgency': 'HIGH',
                }

        elif ctype == 'dead_cat_bounce':
            # Resolves when the regime has genuinely shifted to structural recovery
            rq_state = str(row.get('RecoveryQuality_State', '') or '').upper()
            if rq_state == 'STRUCTURAL_RECOVERY':
                rq_reason = str(row.get('RecoveryQuality_Resolution_Reason', '') or '')
                return {
                    'rationale': (
                        f"Structural recovery confirmed — regime has genuinely shifted. "
                        f"{rq_reason}. "
                        f"Roll candidates pre-staged; assess now (Passarelli Ch.6)."
                    ),
                    'action': 'ROLL',
                    'urgency': 'MEDIUM',
                }
            # Also resolve (abandon watch) if position is no longer under pressure
            if rq_state == 'NOT_IN_RECOVERY':
                return {
                    'rationale': (
                        "Position drift recovered above -5% threshold — "
                        "dead-cat bounce condition no longer applicable."
                    ),
                    'action': 'HOLD',
                    'urgency': 'LOW',
                }

        elif ctype == 'iv_depressed':
            # Dual resolution: IV rank recovered OR IV/HV ratio normalized
            iv_rank = row.get('IV_Rank_30D')
            iv_30d_raw = row.get('IV_30D')
            hv_20d_raw = row.get('HV_20D')

            rank_resolved = (
                iv_rank is not None
                and not pd.isna(iv_rank)
                and float(iv_rank) >= 40
            )
            ratio_resolved = (
                iv_30d_raw is not None and not pd.isna(iv_30d_raw)
                and hv_20d_raw is not None and not pd.isna(hv_20d_raw)
                and float(hv_20d_raw) > 0
                and float(iv_30d_raw) / float(hv_20d_raw) >= 0.90
            )

            if rank_resolved or ratio_resolved:
                rank_str = f"{float(iv_rank):.0f}/100" if iv_rank is not None else "?"
                ratio_str = (
                    f"{float(iv_30d_raw)/float(hv_20d_raw):.2f}"
                    if (iv_30d_raw is not None and hv_20d_raw is not None
                        and float(hv_20d_raw) > 0)
                    else "?"
                )
                trigger = "IV rank" if rank_resolved else "IV/HV ratio"
                return {
                    'rationale': (
                        f"IV depression resolved via {trigger}: "
                        f"rank={rank_str}, IV/HV={ratio_str}. "
                        f"Premium edge restored — roll now viable (Natenberg Ch.5, Passarelli Ch.4)."
                    ),
                    'action': 'ROLL',
                    'urgency': 'MEDIUM',
                }

        return None

    # ── DuckDB Helpers ────────────────────────────────────────────────────────

    def _upsert_conditions(self, conditions: List[Dict], con=None) -> None:
        try:
            import duckdb
            db = con or duckdb.connect(self.db_path)
            try:
                for c in conditions:
                    db.execute("""
                        INSERT INTO management_conditions
                            (condition_id, trade_id, leg_id, condition_type, description,
                             metric_col, operator, threshold, current_value,
                             created_at, last_checked, status, check_count)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                        ON CONFLICT (condition_id) DO UPDATE SET
                            current_value = excluded.current_value,
                            last_checked  = excluded.last_checked,
                            status        = CASE
                                WHEN management_conditions.status = 'ACTIVE'
                                THEN 'ACTIVE'
                                ELSE management_conditions.status
                            END,
                            check_count   = management_conditions.check_count + 1
                    """, [
                        c['condition_id'], c['trade_id'], c['leg_id'],
                        c['condition_type'], c['description'],
                        c['metric_col'], c['operator'], c['threshold'], c['current_value'],
                        c['created_at'], c['last_checked'], c['status'],
                    ])
            finally:
                if con is None:
                    db.close()
        except Exception as e:
            logger.warning(f"[ConditionMonitor] Upsert failed: {e}")

    def _load_active(self, con=None) -> List[Dict]:
        try:
            import duckdb
            db = con or duckdb.connect(self.db_path, read_only=(con is None))
            try:
                rows = db.execute("""
                    SELECT condition_id, trade_id, leg_id, condition_type,
                           description, metric_col, operator, threshold,
                           current_value, check_count
                    FROM management_conditions
                    WHERE status = 'ACTIVE'
                """).fetchall()
                cols = ['condition_id', 'trade_id', 'leg_id', 'condition_type',
                        'description', 'metric_col', 'operator', 'threshold',
                        'current_value', 'check_count']
                return [dict(zip(cols, r)) for r in rows]
            finally:
                if con is None:
                    db.close()
        except Exception as e:
            logger.warning(f"[ConditionMonitor] Load active failed: {e}")
            return []

    def _mark_resolved(self, resolved: List[Dict], con=None) -> None:
        if not resolved:
            return
        try:
            import duckdb
            now = datetime.now(tz=timezone.utc)
            db = con or duckdb.connect(self.db_path)
            try:
                for c in resolved:
                    db.execute("""
                        UPDATE management_conditions
                        SET status       = ?,
                            resolved_at  = ?,
                            last_checked = ?,
                            check_count  = check_count + 1
                        WHERE condition_id = ?
                    """, [c.get('_resolution', 'RESOLVED'), now, now, c['condition_id']])
            finally:
                if con is None:
                    db.close()
        except Exception as e:
            logger.warning(f"[ConditionMonitor] Mark resolved failed: {e}")

    def _update_checked(self, active: List[Dict], con=None) -> None:
        if not active:
            return
        try:
            import duckdb
            now = datetime.now(tz=timezone.utc)
            db = con or duckdb.connect(self.db_path)
            try:
                for c in active:
                    db.execute("""
                        UPDATE management_conditions
                        SET last_checked  = ?,
                            current_value = ?,
                            check_count   = check_count + 1
                        WHERE condition_id = ?
                    """, [now, c.get('_current_value', c.get('current_value')), c['condition_id']])
            finally:
                if con is None:
                    db.close()
        except Exception as e:
            logger.warning(f"[ConditionMonitor] Update checked failed: {e}")

    # ── Utilities ─────────────────────────────────────────────────────────────

    @staticmethod
    def _get_trade_mask(df: pd.DataFrame, trade_id: str) -> pd.Series:
        if 'TradeID' in df.columns:
            return df['TradeID'].astype(str) == trade_id
        return pd.Series([False] * len(df), index=df.index)

    @staticmethod
    def _get_current_value(row: pd.Series, ctype: str) -> Optional[float]:
        col_map = {
            'iv_backwardation': 'iv_ts_slope_30_90',
            'theta_dominance':  'DTE',
            'itm_defense':      'DTE',
            'dead_cat_bounce':  None,   # state-based: no single numeric metric
            'iv_depressed':     'IV_Rank_30D',
        }
        col = col_map.get(ctype)
        if col is None:
            # For dead_cat_bounce: encode state as numeric (1=still bouncing, 0=resolved)
            if ctype == 'dead_cat_bounce':
                rq = str(row.get('RecoveryQuality_State', '') or '').upper()
                return 1.0 if rq == 'DEAD_CAT_BOUNCE' else 0.0
            return None
        if col in row.index:
            v = row[col]
            try:
                return float(v)
            except Exception:
                return None
        return None
