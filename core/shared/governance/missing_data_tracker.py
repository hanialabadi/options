"""
Missing-Data Diagnosis Layer — root-cause tracking for NaN fields.

Complements audit_harness.py (which records *that* data is missing) with
*why* it is missing.  Every tracked field gets a companion column
``{Field}_Missing_Reason`` whose value is one of the ``MissingReason``
enum members.  At pipeline end, ``generate_report()`` produces a compact
health summary that distinguishes expected missingness (immature ticker,
non-applicable strategy) from suspicious missingness (broken join, API
failure, computation error).

Usage in pipeline.py::

    tracker = MissingDataTracker(run_id)
    # after each step:
    tracker.diagnose(df, step_num=2)
    tracker.audit_stage("step2", df_before=None, df_after=df)
    # at finalize:
    tracker.check_impossible(df, step_num=12)
    report = tracker.generate_report()
    tracker.persist(db_con)
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════════════
# Enums
# ════════════════════════════════════════════════════════════════════

class MissingReason(str, Enum):
    """Why a field is NaN."""
    IMMATURE_HISTORY = "IMMATURE_HISTORY"   # <30d IV or <28d price history
    SOURCE_MISSING   = "SOURCE_MISSING"     # upstream never populated
    API_FAIL         = "API_FAIL"           # Schwab/external API error
    MERGE_FAIL       = "MERGE_FAIL"         # data existed upstream but lost after join
    COMPUTE_FAIL     = "COMPUTE_FAIL"       # inputs present, derivation returned NaN
    NOT_APPLICABLE   = "NOT_APPLICABLE"     # field irrelevant for this strategy/row
    SCHEMA_MISMATCH  = "SCHEMA_MISMATCH"    # column renamed or missing entirely
    UNKNOWN          = "UNKNOWN"            # catch-all
    PRESENT          = "PRESENT"            # field is non-null (for companion column)


class MissingnessClass(str, Enum):
    """Severity of a missing field after its required step."""
    EXPECTED   = "EXPECTED"      # known-safe (immature ticker, wrong strategy)
    SUSPICIOUS = "SUSPICIOUS"    # investigate (data should exist)
    IMPOSSIBLE = "IMPOSSIBLE"    # fail-loud (must be non-null after owning step)


# ════════════════════════════════════════════════════════════════════
# Registry
# ════════════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class FieldSpec:
    """One tracked field in the registry."""
    field_name: str
    owning_step: int               # step that first produces this field
    required_after_step: int       # null after this step = class-level severity
    missingness_class: MissingnessClass
    reason_if_expected: MissingReason
    strategy_scope: Optional[str] = None  # "INCOME" | "DIRECTIONAL" | None=all


# Strategy name sets used for scope matching (lowercase, matches evaluator _types.py)
_DIRECTIONAL_NAMES = {
    'long call', 'long put', 'long call leap', 'long put leap',
    'bull call spread', 'bear put spread', 'call debit spread', 'put debit spread',
}
_INCOME_NAMES = {
    'cash-secured put', 'covered call', 'buy-write', 'pmcc',
    'short iron condor', 'credit spread',
}

# ── The registry ──────────────────────────────────────────────────
# One line per tracked field.  To add a field, add one FieldSpec.
TRACKED_FIELDS: List[FieldSpec] = [
    # --- IV family (Step 2) ---
    FieldSpec("IV_Rank_30D",       2, 2,  MissingnessClass.EXPECTED,   MissingReason.IMMATURE_HISTORY),
    FieldSpec("IV_Maturity_State", 2, 2,  MissingnessClass.IMPOSSIBLE, MissingReason.SOURCE_MISSING),
    FieldSpec("Signal_Type",       2, 2,  MissingnessClass.IMPOSSIBLE, MissingReason.SOURCE_MISSING),
    FieldSpec("Regime",            2, 2,  MissingnessClass.SUSPICIOUS, MissingReason.SOURCE_MISSING),

    # --- TA family (Step 2, required by Step 5) ---
    FieldSpec("ADX",               2, 5,  MissingnessClass.EXPECTED,   MissingReason.IMMATURE_HISTORY),
    FieldSpec("RSI_14",            2, 5,  MissingnessClass.EXPECTED,   MissingReason.IMMATURE_HISTORY),
    FieldSpec("SMA20",             2, 5,  MissingnessClass.EXPECTED,   MissingReason.IMMATURE_HISTORY),
    FieldSpec("MACD",              2, 5,  MissingnessClass.EXPECTED,   MissingReason.IMMATURE_HISTORY),
    FieldSpec("Price_vs_SMA20",    2, 5,  MissingnessClass.EXPECTED,   MissingReason.IMMATURE_HISTORY),

    # --- Greeks family (Step 10) ---
    FieldSpec("Delta",             10, 10, MissingnessClass.SUSPICIOUS, MissingReason.API_FAIL),
    FieldSpec("Gamma",             10, 10, MissingnessClass.SUSPICIOUS, MissingReason.API_FAIL),
    FieldSpec("Theta",             10, 10, MissingnessClass.SUSPICIOUS, MissingReason.API_FAIL),
    FieldSpec("Vega",              10, 10, MissingnessClass.SUSPICIOUS, MissingReason.API_FAIL),
    FieldSpec("Strike",            10, 10, MissingnessClass.SUSPICIOUS, MissingReason.API_FAIL),
    FieldSpec("Bid",               10, 10, MissingnessClass.SUSPICIOUS, MissingReason.API_FAIL),
    FieldSpec("Ask",               10, 10, MissingnessClass.SUSPICIOUS, MissingReason.API_FAIL),

    # --- Scoring (Step 10, strategy-scoped) ---
    FieldSpec("DQS_Score",         10, 10, MissingnessClass.EXPECTED,   MissingReason.NOT_APPLICABLE, strategy_scope="DIRECTIONAL"),
    FieldSpec("TQS_Score",         10, 10, MissingnessClass.EXPECTED,   MissingReason.NOT_APPLICABLE, strategy_scope="DIRECTIONAL"),
    FieldSpec("PCS_Final",         10, 10, MissingnessClass.EXPECTED,   MissingReason.NOT_APPLICABLE, strategy_scope="INCOME"),

    # --- Derived (Step 10-12) ---
    FieldSpec("Premium_vs_FairValue_Pct", 10, 12, MissingnessClass.SUSPICIOUS, MissingReason.COMPUTE_FAIL),
    FieldSpec("Liquidity_Grade",          10, 12, MissingnessClass.SUSPICIOUS, MissingReason.API_FAIL),
    FieldSpec("Contract_Symbol",          10, 10, MissingnessClass.SUSPICIOUS, MissingReason.API_FAIL),
]

# ── Management pipeline registry ─────────────────────────────────
# Steps use a different numbering: enrichment=1, measurement=2, doctrine=3.
# "owning_step" and "required_after_step" use management cycle numbers.
MANAGEMENT_TRACKED_FIELDS: List[FieldSpec] = [
    # --- Identity & pricing (Cycle 1: load/clean) ---
    FieldSpec("UL Last",            1, 1,  MissingnessClass.IMPOSSIBLE, MissingReason.SOURCE_MISSING),
    FieldSpec("Strategy",           1, 1,  MissingnessClass.IMPOSSIBLE, MissingReason.SOURCE_MISSING),
    FieldSpec("Symbol",             1, 1,  MissingnessClass.IMPOSSIBLE, MissingReason.SOURCE_MISSING),

    # --- Option fields (Cycle 1, option rows only) ---
    FieldSpec("DTE",                1, 1,  MissingnessClass.SUSPICIOUS, MissingReason.SOURCE_MISSING),
    FieldSpec("Strike",             1, 1,  MissingnessClass.SUSPICIOUS, MissingReason.SOURCE_MISSING),
    FieldSpec("Expiration",         1, 1,  MissingnessClass.SUSPICIOUS, MissingReason.SOURCE_MISSING),

    # --- Greeks (Cycle 2: Schwab/live provider) ---
    FieldSpec("Delta",              2, 2,  MissingnessClass.SUSPICIOUS, MissingReason.API_FAIL),
    FieldSpec("Gamma",              2, 2,  MissingnessClass.SUSPICIOUS, MissingReason.API_FAIL),
    FieldSpec("Theta",              2, 2,  MissingnessClass.SUSPICIOUS, MissingReason.API_FAIL),
    FieldSpec("Vega",               2, 2,  MissingnessClass.SUSPICIOUS, MissingReason.API_FAIL),

    # --- IV & volatility (Cycle 2: iv_history + Schwab) ---
    FieldSpec("IV_Now",             2, 2,  MissingnessClass.SUSPICIOUS, MissingReason.API_FAIL),
    FieldSpec("IV_30D",             2, 2,  MissingnessClass.EXPECTED,   MissingReason.IMMATURE_HISTORY),
    FieldSpec("IV_Rank",            2, 2,  MissingnessClass.EXPECTED,   MissingReason.IMMATURE_HISTORY),
    FieldSpec("HV_20D",             2, 2,  MissingnessClass.EXPECTED,   MissingReason.IMMATURE_HISTORY),

    # --- Trajectory & PnL (Cycle 2: drift computation) ---
    FieldSpec("Trajectory_PnL_Pct", 2, 2,  MissingnessClass.SUSPICIOUS, MissingReason.COMPUTE_FAIL),
    FieldSpec("Trajectory_MFE",     2, 2,  MissingnessClass.SUSPICIOUS, MissingReason.COMPUTE_FAIL),
    FieldSpec("Basis",              1, 2,  MissingnessClass.SUSPICIOUS, MissingReason.SOURCE_MISSING),
    FieldSpec("Premium_Entry",      1, 2,  MissingnessClass.EXPECTED,   MissingReason.SOURCE_MISSING),

    # --- Chart state (Cycle 2: chart primitives) ---
    FieldSpec("PriceStructure_State",   2, 2, MissingnessClass.EXPECTED,   MissingReason.IMMATURE_HISTORY),
    FieldSpec("TrendIntegrity_State",   2, 2, MissingnessClass.EXPECTED,   MissingReason.IMMATURE_HISTORY),
    FieldSpec("MomentumVelocity_State", 2, 2, MissingnessClass.EXPECTED,   MissingReason.IMMATURE_HISTORY),
    FieldSpec("Thesis_State",           2, 3, MissingnessClass.SUSPICIOUS, MissingReason.COMPUTE_FAIL),

    # --- Technical indicators (Cycle 2: scan signal hub merge) ---
    FieldSpec("rsi_14",             2, 2,  MissingnessClass.EXPECTED,   MissingReason.MERGE_FAIL),
    FieldSpec("adx_14",             2, 2,  MissingnessClass.EXPECTED,   MissingReason.MERGE_FAIL),

    # --- Income-specific (Cycle 2) ---
    FieldSpec("Short_Call_Delta",   2, 2,  MissingnessClass.EXPECTED,   MissingReason.NOT_APPLICABLE),
    FieldSpec("Short_Call_Strike",  2, 2,  MissingnessClass.EXPECTED,   MissingReason.NOT_APPLICABLE),

    # --- Doctrine output (Cycle 3) ---
    FieldSpec("Action",             3, 3,  MissingnessClass.IMPOSSIBLE, MissingReason.COMPUTE_FAIL),
    FieldSpec("Urgency",            3, 3,  MissingnessClass.IMPOSSIBLE, MissingReason.COMPUTE_FAIL),
    FieldSpec("Doctrine_Source",    3, 3,  MissingnessClass.IMPOSSIBLE, MissingReason.COMPUTE_FAIL),
]

# Index for fast lookup
_FIELD_INDEX: Dict[str, FieldSpec] = {fs.field_name: fs for fs in TRACKED_FIELDS}

# TA lookback requirements (days of price history needed)
_TA_LOOKBACK = {"ADX": 28, "RSI_14": 14, "SMA20": 20, "MACD": 35, "Price_vs_SMA20": 20}


# ════════════════════════════════════════════════════════════════════
# Dataclasses for audit & report
# ════════════════════════════════════════════════════════════════════

@dataclass
class StageAudit:
    step_name: str
    rows_entering: int
    rows_exiting: int
    missing_counts: Dict[str, int] = field(default_factory=dict)
    missing_reasons: Dict[str, Dict[str, int]] = field(default_factory=dict)
    rows_dropped: int = 0
    rows_with_gaps: int = 0


@dataclass
class HealthReport:
    run_id: str
    timestamp: str
    total_rows: int
    overall_health: str                      # GREEN | YELLOW | RED
    completeness: Dict[str, float] = field(default_factory=dict)
    reason_distribution: Dict[str, int] = field(default_factory=dict)
    top_missing: List[Dict[str, Any]] = field(default_factory=list)
    impossible_violations: List[Dict[str, Any]] = field(default_factory=list)
    stage_audits: List[Dict[str, Any]] = field(default_factory=list)


# ════════════════════════════════════════════════════════════════════
# Tracker
# ════════════════════════════════════════════════════════════════════

class MissingDataTracker:
    """Central tracker for missing-data diagnosis across pipeline steps.

    Args:
        run_id: Unique identifier for this pipeline run.
        registry: Which field registry to use.  Defaults to ``TRACKED_FIELDS``
            (scan pipeline).  Pass ``MANAGEMENT_TRACKED_FIELDS`` for the
            management pipeline.
    """

    def __init__(self, run_id: str, registry: Optional[List[FieldSpec]] = None):
        self.run_id = run_id
        self._registry = registry if registry is not None else TRACKED_FIELDS
        self._stage_audits: List[StageAudit] = []
        self._impossible_violations: List[Dict[str, Any]] = []
        self._last_reason_snapshot: Dict[str, Dict[str, int]] = {}  # field → {reason: count}

    # ── Public API ────────────────────────────────────────────────

    def diagnose(self, df: pd.DataFrame, step_num: int) -> pd.DataFrame:
        """Tag NaN fields with ``{Field}_Missing_Reason`` companion columns.

        Mutates *df* in-place and returns it.  Non-blocking: exceptions
        are logged but never propagated.
        """
        try:
            self._diagnose_impl(df, step_num)
        except Exception as e:
            logger.warning(f"[MissingData] diagnose failed at step {step_num}: {e}")
        return df

    def audit_stage(
        self,
        step_name: str,
        df_before: Optional[pd.DataFrame],
        df_after: pd.DataFrame,
    ) -> Optional[StageAudit]:
        """Compute and log stage audit.  Accumulates internally."""
        try:
            entry = self._audit_stage_impl(step_name, df_before, df_after)
            self._stage_audits.append(entry)
            self._log_stage(entry)
            return entry
        except Exception as e:
            logger.warning(f"[MissingData] audit_stage failed at {step_name}: {e}")
            return None

    def check_impossible(self, df: pd.DataFrame, step_num: int) -> List[Dict[str, Any]]:
        """Check IMPOSSIBLE-class fields.  Logs errors.  Non-blocking."""
        violations: List[Dict[str, Any]] = []
        try:
            for spec in self._registry:
                if spec.missingness_class != MissingnessClass.IMPOSSIBLE:
                    continue
                if step_num < spec.required_after_step:
                    continue
                if spec.field_name not in df.columns:
                    v = {
                        "field": spec.field_name,
                        "step": step_num,
                        "count": len(df),
                        "tickers": _safe_ticker_list(df, slice(None)),
                        "reason": MissingReason.SCHEMA_MISMATCH.value,
                    }
                    violations.append(v)
                    logger.error(
                        f"[MissingData] IMPOSSIBLE: column '{spec.field_name}' "
                        f"missing entirely after step {step_num}"
                    )
                    continue

                null_mask = df[spec.field_name].isna()
                null_count = int(null_mask.sum())
                if null_count > 0:
                    v = {
                        "field": spec.field_name,
                        "step": step_num,
                        "count": null_count,
                        "tickers": _safe_ticker_list(df, null_mask),
                        "reason": "NULL_AFTER_REQUIRED_STEP",
                    }
                    violations.append(v)
                    logger.error(
                        f"[MissingData] IMPOSSIBLE: {spec.field_name} has "
                        f"{null_count} NaN rows after step {step_num} — "
                        f"tickers: {v['tickers'][:5]}"
                    )
        except Exception as e:
            logger.warning(f"[MissingData] check_impossible failed: {e}")

        self._impossible_violations.extend(violations)
        return violations

    def generate_report(self) -> HealthReport:
        """Build end-of-run health report from accumulated audits."""
        try:
            return self._generate_report_impl()
        except Exception as e:
            logger.warning(f"[MissingData] generate_report failed: {e}")
            return HealthReport(
                run_id=self.run_id,
                timestamp=datetime.now().isoformat(),
                total_rows=0,
                overall_health="UNKNOWN",
            )

    def persist(self, con) -> None:
        """Write health report to DuckDB ``missing_data_health`` table."""
        try:
            report = self.generate_report()
            self._persist_impl(con, report)
        except Exception as e:
            logger.warning(f"[MissingData] persist failed: {e}")

    # ── Private implementation ────────────────────────────────────

    def _diagnose_impl(self, df: pd.DataFrame, step_num: int) -> None:
        """Core diagnosis logic — three field families."""
        n = len(df)
        if n == 0:
            return

        reason_snapshot: Dict[str, Dict[str, int]] = {}

        for spec in self._registry:
            # Skip fields not yet born at this step
            if step_num < spec.owning_step:
                continue

            col = spec.field_name
            reason_col = f"{col}_Missing_Reason"

            # Column doesn't exist at all
            if col not in df.columns:
                df[reason_col] = MissingReason.SCHEMA_MISMATCH.value
                reason_snapshot[col] = {MissingReason.SCHEMA_MISMATCH.value: n}
                continue

            null_mask = df[col].isna()
            present_mask = ~null_mask
            null_count = int(null_mask.sum())

            if null_count == 0:
                df[reason_col] = MissingReason.PRESENT.value
                reason_snapshot[col] = {MissingReason.PRESENT.value: n}
                continue

            # Initialize reason column
            reasons = pd.Series(MissingReason.PRESENT.value, index=df.index)

            # Only diagnose null rows
            null_idx = df.index[null_mask]

            if spec.strategy_scope is not None:
                # Strategy-scoped field: check if row's strategy is out of scope
                strat_col = _get_strategy_column(df)
                if strat_col:
                    row_strats = df.loc[null_idx, strat_col].fillna('').str.lower().str.strip()
                    scope_set = _DIRECTIONAL_NAMES if spec.strategy_scope == "DIRECTIONAL" else _INCOME_NAMES
                    out_of_scope = ~row_strats.isin(scope_set)
                    reasons.loc[null_idx[out_of_scope]] = MissingReason.NOT_APPLICABLE.value
                    # Remaining nulls that ARE in scope
                    in_scope_null = null_idx[~out_of_scope]
                    if len(in_scope_null) > 0:
                        reasons.loc[in_scope_null] = _infer_reason_generic(df, in_scope_null, spec)
                else:
                    reasons.loc[null_idx] = MissingReason.NOT_APPLICABLE.value
            elif col in _TA_LOOKBACK:
                reasons.loc[null_idx] = _infer_reason_ta(df, null_idx, col)
            elif col in ("IV_Rank_30D",):
                reasons.loc[null_idx] = _infer_reason_iv(df, null_idx)
            elif col in ("Delta", "Gamma", "Theta", "Vega", "Strike", "Bid", "Ask", "Contract_Symbol"):
                reasons.loc[null_idx] = _infer_reason_greeks(df, null_idx)
            elif col == "Regime":
                reasons.loc[null_idx] = _infer_reason_iv(df, null_idx)
            else:
                reasons.loc[null_idx] = _infer_reason_generic(df, null_idx, spec)

            df[reason_col] = reasons

            # Build reason distribution for this field
            dist: Dict[str, int] = {}
            for r in reasons.loc[null_idx].unique():
                dist[r] = int((reasons.loc[null_idx] == r).sum())
            reason_snapshot[col] = dist

        self._last_reason_snapshot = reason_snapshot

    def _audit_stage_impl(
        self,
        step_name: str,
        df_before: Optional[pd.DataFrame],
        df_after: pd.DataFrame,
    ) -> StageAudit:
        rows_entering = len(df_before) if df_before is not None else len(df_after)
        rows_exiting = len(df_after)
        rows_dropped = max(0, rows_entering - rows_exiting)

        # Count missing tracked fields in df_after
        missing_counts: Dict[str, int] = {}
        missing_reasons: Dict[str, Dict[str, int]] = {}
        rows_with_any_gap = set()

        for spec in self._registry:
            col = spec.field_name
            if col not in df_after.columns:
                continue
            null_mask = df_after[col].isna()
            null_count = int(null_mask.sum())
            if null_count > 0:
                missing_counts[col] = null_count
                rows_with_any_gap.update(df_after.index[null_mask].tolist())

                # Get reason distribution from companion column if available
                reason_col = f"{col}_Missing_Reason"
                if reason_col in df_after.columns:
                    reason_vals = df_after.loc[null_mask, reason_col]
                    dist = reason_vals.value_counts().to_dict()
                    missing_reasons[col] = {str(k): int(v) for k, v in dist.items()}
                elif col in self._last_reason_snapshot:
                    missing_reasons[col] = self._last_reason_snapshot[col]

        return StageAudit(
            step_name=step_name,
            rows_entering=rows_entering,
            rows_exiting=rows_exiting,
            missing_counts=missing_counts,
            missing_reasons=missing_reasons,
            rows_dropped=rows_dropped,
            rows_with_gaps=len(rows_with_any_gap),
        )

    def _log_stage(self, entry: StageAudit) -> None:
        """Log compact one-line summary."""
        parts = []
        for col, count in sorted(entry.missing_counts.items(), key=lambda x: -x[1])[:5]:
            reasons = entry.missing_reasons.get(col, {})
            top_reason = max(reasons, key=reasons.get) if reasons else "?"
            parts.append(f"{col}={count}({top_reason})")
        gaps_str = ", ".join(parts) if parts else "none"
        logger.info(
            f"[MissingData] {entry.step_name}: "
            f"{entry.rows_entering}→{entry.rows_exiting} | "
            f"Gaps: {gaps_str} | "
            f"{entry.rows_dropped} dropped, {entry.rows_with_gaps} w/gaps"
        )

    def _generate_report_impl(self) -> HealthReport:
        # Use the last stage audit for totals
        last = self._stage_audits[-1] if self._stage_audits else None
        total_rows = last.rows_exiting if last else 0

        # Aggregate reason distribution across all fields from last audit
        reason_dist: Dict[str, int] = {}
        if last:
            for col, reasons in last.missing_reasons.items():
                for reason, count in reasons.items():
                    if reason != MissingReason.PRESENT.value:
                        reason_dist[reason] = reason_dist.get(reason, 0) + count

        # Top missing fields from last audit
        top_missing = []
        if last and total_rows > 0:
            for col, count in sorted(last.missing_counts.items(), key=lambda x: -x[1])[:10]:
                reasons = last.missing_reasons.get(col, {})
                top_reason = max(reasons, key=reasons.get) if reasons else "UNKNOWN"
                top_missing.append({
                    "field": col,
                    "count": count,
                    "pct": round(count / total_rows * 100, 1),
                    "top_reason": top_reason,
                })

        # Completeness metrics from last audit
        completeness = self._compute_completeness(last, total_rows)

        # Health classification
        suspicious_total = sum(
            count for reason, count in reason_dist.items()
            if reason not in (
                MissingReason.IMMATURE_HISTORY.value,
                MissingReason.NOT_APPLICABLE.value,
                MissingReason.PRESENT.value,
            )
        )
        total_field_checks = total_rows * len(self._registry)
        suspicious_pct = (suspicious_total / total_field_checks * 100) if total_field_checks > 0 else 0

        if self._impossible_violations:
            health = "RED"
        elif suspicious_pct > 15:
            health = "RED"
        elif suspicious_pct > 5:
            health = "YELLOW"
        else:
            health = "GREEN"

        return HealthReport(
            run_id=self.run_id,
            timestamp=datetime.now().isoformat(),
            total_rows=total_rows,
            overall_health=health,
            completeness=completeness,
            reason_distribution=reason_dist,
            top_missing=top_missing,
            impossible_violations=[asdict_safe(v) for v in self._impossible_violations],
            stage_audits=[asdict_safe(asdict(sa)) for sa in self._stage_audits],
        )

    def _compute_completeness(self, last: Optional[StageAudit], total: int) -> Dict[str, float]:
        if not last or total == 0:
            return {}

        def _pct_present(cols: List[str]) -> float:
            missing = sum(last.missing_counts.get(c, 0) for c in cols)
            total_checks = total * len(cols)
            return round((1 - missing / total_checks) * 100, 1) if total_checks > 0 else 100.0

        return {
            "iv_rank_pct": _pct_present(["IV_Rank_30D"]),
            "ta_indicators_pct": _pct_present(["ADX", "RSI_14", "SMA20"]),
            "greeks_pct": _pct_present(["Delta", "Gamma", "Theta", "Vega"]),
            "dqs_inputs_pct": _pct_present(["IV_Rank_30D", "RSI_14", "MACD", "ADX"]),
        }

    def _persist_impl(self, con, report: HealthReport) -> None:
        """Write report to DuckDB. Connection is passed in by the caller."""
        con.execute("""
            CREATE TABLE IF NOT EXISTS missing_data_health (
                run_id              VARCHAR,
                scan_ts             TIMESTAMP,
                overall_health      VARCHAR,
                total_rows          INTEGER,
                completeness_json   VARCHAR,
                reason_dist_json    VARCHAR,
                top_missing_json    VARCHAR,
                impossible_count    INTEGER,
                stage_audits_json   VARCHAR
            )
        """)

        con.execute(
            """
            INSERT INTO missing_data_health VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                report.run_id,
                report.timestamp,
                report.overall_health,
                report.total_rows,
                json.dumps(report.completeness),
                json.dumps(report.reason_distribution),
                json.dumps(report.top_missing),
                len(report.impossible_violations),
                json.dumps(report.stage_audits),
            ],
        )
        logger.info(
            f"[MissingData] Persisted health report: "
            f"{report.overall_health} ({report.total_rows} rows, "
            f"{len(report.impossible_violations)} impossible violations)"
        )


# ════════════════════════════════════════════════════════════════════
# Reason inference helpers (pure, no I/O)
# ════════════════════════════════════════════════════════════════════

def _infer_reason_iv(df: pd.DataFrame, null_idx: pd.Index) -> pd.Series:
    """IV family: IV_Rank_30D, Regime."""
    reasons = pd.Series(MissingReason.UNKNOWN.value, index=null_idx)

    # Check IV maturity state
    mat_col = "IV_Maturity_State"
    if mat_col in df.columns:
        mat = df.loc[null_idx, mat_col].fillna("").str.upper().str.strip()
        immature_mask = mat.isin(("IMMATURE", "MISSING", ""))
        reasons.loc[immature_mask] = MissingReason.IMMATURE_HISTORY.value
        reasons.loc[~immature_mask] = MissingReason.COMPUTE_FAIL.value
    else:
        # IV_Maturity_State itself missing → likely schema issue upstream
        hist_col = "IV_History_Count"
        if hist_col in df.columns:
            hist = pd.to_numeric(df.loc[null_idx, hist_col], errors="coerce").fillna(0)
            reasons.loc[hist < 30] = MissingReason.IMMATURE_HISTORY.value
            reasons.loc[hist >= 30] = MissingReason.COMPUTE_FAIL.value
        else:
            reasons[:] = MissingReason.SOURCE_MISSING.value

    return reasons


def _infer_reason_ta(df: pd.DataFrame, null_idx: pd.Index, col: str) -> pd.Series:
    """TA family: ADX, RSI_14, SMA20, MACD, Price_vs_SMA20."""
    reasons = pd.Series(MissingReason.UNKNOWN.value, index=null_idx)

    # Check price history day count (set by step2 enrichment)
    hist_col = "Price_History_Days"
    required = _TA_LOOKBACK.get(col, 20)

    if hist_col in df.columns:
        days = pd.to_numeric(df.loc[null_idx, hist_col], errors="coerce").fillna(0)
        reasons.loc[days < required] = MissingReason.IMMATURE_HISTORY.value
        reasons.loc[days >= required] = MissingReason.COMPUTE_FAIL.value
    else:
        # No price history count → assume immature if IV is also immature
        mat_col = "IV_Maturity_State"
        if mat_col in df.columns:
            mat = df.loc[null_idx, mat_col].fillna("").str.upper().str.strip()
            immature = mat.isin(("IMMATURE", "MISSING", ""))
            reasons.loc[immature] = MissingReason.IMMATURE_HISTORY.value
            reasons.loc[~immature] = MissingReason.COMPUTE_FAIL.value
        else:
            reasons[:] = MissingReason.IMMATURE_HISTORY.value

    return reasons


def _infer_reason_greeks(df: pd.DataFrame, null_idx: pd.Index) -> pd.Series:
    """Greeks family: Delta, Gamma, Theta, Vega, Strike, Bid, Ask."""
    reasons = pd.Series(MissingReason.UNKNOWN.value, index=null_idx)

    # Check if contract was fetched
    sym_col = "Contract_Symbol"
    if sym_col in df.columns:
        no_contract = df.loc[null_idx, sym_col].isna()
        reasons.loc[no_contract] = MissingReason.SOURCE_MISSING.value

        # Contract exists but Greek missing → API_FAIL or MERGE_FAIL
        has_contract = ~no_contract
        if has_contract.any():
            # Check for API failure markers
            for marker_col in ("Contract_Status", "Scraper_Status"):
                if marker_col in df.columns:
                    status = df.loc[null_idx[has_contract], marker_col].fillna("").str.upper()
                    failed = status.str.contains("FAIL|ERROR|TIMEOUT", na=False)
                    reasons.loc[null_idx[has_contract][failed]] = MissingReason.API_FAIL.value
                    reasons.loc[null_idx[has_contract][~failed]] = MissingReason.MERGE_FAIL.value
                    break
            else:
                reasons.loc[null_idx[has_contract]] = MissingReason.MERGE_FAIL.value
    else:
        reasons[:] = MissingReason.SOURCE_MISSING.value

    return reasons


def _infer_reason_generic(
    df: pd.DataFrame, null_idx: pd.Index, spec: FieldSpec
) -> pd.Series:
    """Fallback inference.  For strategy-scoped in-scope nulls, use COMPUTE_FAIL
    (the data should have been computed but wasn't).  Otherwise use the spec default."""
    if spec.strategy_scope is not None:
        # In-scope null → computation should have produced a value
        return pd.Series(MissingReason.COMPUTE_FAIL.value, index=null_idx)
    return pd.Series(spec.reason_if_expected.value, index=null_idx)


# ════════════════════════════════════════════════════════════════════
# Utilities
# ════════════════════════════════════════════════════════════════════

def _get_strategy_column(df: pd.DataFrame) -> Optional[str]:
    """Return the strategy column name present in df."""
    for col in ("Strategy_Name", "Strategy"):
        if col in df.columns:
            return col
    return None


def _safe_ticker_list(df: pd.DataFrame, mask) -> List[str]:
    """Extract up to 10 ticker names for violation reporting."""
    if "Ticker" in df.columns:
        try:
            return df.loc[mask, "Ticker"].dropna().unique().tolist()[:10]
        except Exception:
            return []
    return []


def asdict_safe(obj: Any) -> Any:
    """Recursively convert to JSON-safe dict."""
    if isinstance(obj, dict):
        return {str(k): asdict_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [asdict_safe(x) for x in obj]
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    return obj
