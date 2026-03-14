"""
Data Integrity Monitor — structured health checks for management pipeline.

Runs after schema enforcement in run_all.py, before DuckDB persistence.
Detects silent failures: NaN contamination, stale data, missing Greeks,
anomalous value ranges, and resolution reason distribution shifts.

Outputs:
  - Structured log lines (logger.warning / logger.error)
  - DuckDB `data_integrity_audit` table (one row per run)
  - CLI summary block at end of run
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── Critical columns: NaN here means something upstream broke silently ──
# NOTE: DTE/Strike/Expiration are option-only — checked separately in
# _check_option_required_columns() to avoid false positives on stock rows.
CRITICAL_COLUMNS = [
    "Action", "Urgency", "Doctrine_Source", "Strategy", "TradeID",
    "Underlying_Ticker", "Symbol", "UL Last",
]

# ── Option-only critical columns: NaN on OPTION rows = ERROR ──
OPTION_CRITICAL_COLUMNS = ["DTE", "Strike", "Expiration"]

# ── Important columns: NaN is tolerable but worth tracking ──
IMPORTANT_COLUMNS = [
    "Delta", "Gamma", "Theta", "Vega",
    "IV_Contract", "IV_Underlying_30D", "HV_20D", "IV_Rank",
    "rsi_14", "adx_14", "roc_5", "roc_20", "momentum_slope",
    "PriceStructure_State", "TrendIntegrity_State", "MomentumVelocity_State",
    "Equity_Integrity_State", "Thesis_State",
    "Basis",
]

# ── Income strategy stock legs: IV columns needed for recovery detection ──
INCOME_STRATEGIES = {"BUY_WRITE", "COVERED_CALL", "PMCC"}

# ── Gate-input columns: NaN here causes silent comparison bugs ──
# (NaN > threshold is False in Python, bypassing guard clauses)
# These columns are used in `if value > X` or `if value < X` checks
# in doctrine gates.  Any NaN will produce wrong branch decisions.
GATE_INPUT_COLUMNS = [
    "DTE", "Delta", "Gamma", "Theta", "Vega", "Strike",
    "IV_Now", "IV_30D", "IV_Rank",
    "Trajectory_MFE", "Trajectory_PnL_Pct",
    "UL Last", "Basis",
    "Premium_Entry", "Short_Call_Delta", "Short_Call_Strike", "Short_Call_Last",
]

# ── Value range sanity checks: (column, min, max, description) ──
RANGE_CHECKS = [
    ("DTE", -1, 1500, "Days to expiration"),
    ("Delta", -1.05, 1.05, "Option delta"),
    ("Gamma", -5.0, 5.0, "Option gamma"),
    ("IV_Rank", -1, 101, "IV percentile rank"),
    ("rsi_14", 0, 100, "RSI(14)"),
    ("adx_14", 0, 100, "ADX(14)"),
    ("HV_20D", 0, 500, "Historical volatility 20D"),
]

# ── Valid enum values ──
VALID_ACTIONS = {"HOLD", "ROLL", "EXIT", "REVIEW", "SCALE_UP", "BUYBACK", "TRIM", "LET_EXPIRE", "ACCEPT_CALL_AWAY", "ACCEPT_SHARE_ASSIGNMENT"}
VALID_URGENCIES = {"LOW", "MEDIUM", "HIGH", "CRITICAL"}
VALID_RESOLUTION_REASONS = {
    "", "N/A", "MISSING_PRIMITIVES", "MISSING_INDICATORS", "MISSING_GREEKS",
    "STALE_OHLC", "OK", "PARTIAL",
}


@dataclass
class IntegrityAlert:
    """Single integrity finding."""
    severity: str          # ERROR | WARNING | INFO
    category: str          # NULL_RATE | RANGE | ENUM | STALE | DISTRIBUTION
    column: str
    message: str
    affected_tickers: List[str] = field(default_factory=list)
    value: float = 0.0     # metric (e.g. null rate %)


@dataclass
class IntegrityReport:
    """Aggregate report for one pipeline run."""
    run_id: str
    timestamp: str
    total_positions: int
    alerts: List[IntegrityAlert] = field(default_factory=list)
    null_rates: Dict[str, float] = field(default_factory=dict)
    resolution_distribution: Dict[str, int] = field(default_factory=dict)
    action_distribution: Dict[str, int] = field(default_factory=dict)
    overall_health: str = "HEALTHY"  # HEALTHY | DEGRADED | CRITICAL

    @property
    def error_count(self) -> int:
        return sum(1 for a in self.alerts if a.severity == "ERROR")

    @property
    def warning_count(self) -> int:
        return sum(1 for a in self.alerts if a.severity == "WARNING")

    def to_dict(self) -> dict:
        d = {
            "run_id": self.run_id,
            "timestamp": self.timestamp,
            "total_positions": self.total_positions,
            "overall_health": self.overall_health,
            "error_count": self.error_count,
            "warning_count": self.warning_count,
            "null_rates_json": json.dumps(self.null_rates),
            "resolution_distribution_json": json.dumps(self.resolution_distribution),
            "action_distribution_json": json.dumps(self.action_distribution),
            "alerts_json": json.dumps([asdict(a) for a in self.alerts]),
        }
        return d


def _check_null_rates(df: pd.DataFrame, report: IntegrityReport) -> None:
    """Check NaN/None rates in critical and important columns."""
    n = len(df)
    if n == 0:
        return

    # Critical columns — any NaN is an ERROR
    for col in CRITICAL_COLUMNS:
        if col not in df.columns:
            continue
        null_count = df[col].isna().sum()
        null_pct = (null_count / n) * 100
        report.null_rates[col] = round(null_pct, 1)

        if null_count > 0:
            affected = df.loc[df[col].isna(), "Underlying_Ticker"].dropna().unique().tolist()
            report.alerts.append(IntegrityAlert(
                severity="ERROR",
                category="NULL_RATE",
                column=col,
                message=f"{col}: {null_count}/{n} ({null_pct:.1f}%) NULL — critical column",
                affected_tickers=affected[:10],
                value=null_pct,
            ))

    # Important columns — track rate, warn if >50%
    for col in IMPORTANT_COLUMNS:
        if col not in df.columns:
            continue
        null_count = df[col].isna().sum()
        null_pct = (null_count / n) * 100
        report.null_rates[col] = round(null_pct, 1)

        if null_pct > 50:
            affected = df.loc[df[col].isna(), "Underlying_Ticker"].dropna().unique().tolist()
            report.alerts.append(IntegrityAlert(
                severity="WARNING",
                category="NULL_RATE",
                column=col,
                message=f"{col}: {null_count}/{n} ({null_pct:.1f}%) NULL — majority missing",
                affected_tickers=affected[:10],
                value=null_pct,
            ))


def _check_option_required_columns(df: pd.DataFrame, report: IntegrityReport) -> None:
    """Check DTE/Strike/Expiration NaN on OPTION rows only (stocks legitimately lack these)."""
    opt_mask = df.get("AssetType", pd.Series("", index=df.index)).isin(["OPTION", "Option"])
    if opt_mask.sum() == 0:
        return

    opts = df[opt_mask]
    n_opts = len(opts)

    for col in OPTION_CRITICAL_COLUMNS:
        if col not in opts.columns:
            continue
        null_count = opts[col].isna().sum()
        if null_count > 0:
            null_pct = (null_count / n_opts) * 100
            affected = opts.loc[opts[col].isna(), "Underlying_Ticker"].dropna().unique().tolist()
            report.null_rates[f"{col}_options"] = round(null_pct, 1)
            report.alerts.append(IntegrityAlert(
                severity="ERROR",
                category="NULL_RATE",
                column=col,
                message=f"{col}: {null_count}/{n_opts} ({null_pct:.1f}%) NULL on OPTION rows — critical for doctrine",
                affected_tickers=affected[:10],
                value=null_pct,
            ))


def _check_income_stock_iv(df: pd.DataFrame, report: IntegrityReport) -> None:
    """Check that income strategy STOCK legs have IV data for recovery detection.

    This catches the exact bug that broke EOSE: stock leg has NULL IV on all columns,
    detect_recovery_state() defaults to 0%, declares "IV permanently depressed",
    and hard_stop_exit fires instead of recovery_ladder.
    """
    stock_mask = df.get("AssetType", pd.Series("", index=df.index)).isin(["STOCK", "Stock"])
    if stock_mask.sum() == 0:
        return
    if "Strategy" not in df.columns:
        return

    strat = df["Strategy"].fillna("").str.upper()
    income_stock_mask = stock_mask & strat.isin(INCOME_STRATEGIES)
    income_stocks = df[income_stock_mask]
    n = len(income_stocks)
    if n == 0:
        return

    # Check if ANY of the IV columns have data on these stock rows
    iv_cols = ["IV_Now", "IV_30D", "IV_Contract", "IV_Underlying_30D"]
    available_iv_cols = [c for c in iv_cols if c in income_stocks.columns]
    if not available_iv_cols:
        return

    # A stock row with ALL IV columns NULL is at risk for recovery detection bugs
    all_iv_null_mask = pd.DataFrame(
        {c: income_stocks[c].isna() for c in available_iv_cols}
    ).all(axis=1)
    blind_count = all_iv_null_mask.sum()

    if blind_count > 0:
        affected = income_stocks.loc[
            all_iv_null_mask, "Underlying_Ticker"
        ].dropna().unique().tolist()
        report.alerts.append(IntegrityAlert(
            severity="ERROR",
            category="NULL_RATE",
            column="IV_recovery_blind",
            message=(
                f"{blind_count}/{n} income strategy stock leg(s) have NO IV data "
                f"(IV_Now, IV_30D, IV_Contract, IV_Underlying_30D all NULL) — "
                f"recovery detection will misfire (false EXIT instead of recovery ladder)"
            ),
            affected_tickers=affected[:10],
            value=(blind_count / n) * 100,
        ))


def _check_gate_input_nan(df: pd.DataFrame, report: IntegrityReport) -> None:
    """Flag NaN in columns that feed into doctrine gate comparisons.

    Python NaN comparison semantics: ``NaN > threshold`` is ``False``,
    ``NaN < threshold`` is ``False``.  Any gate using ``if value > X``
    will silently take the wrong branch when value is NaN.

    This check surfaces the exact positions at risk so the data gap
    can be fixed upstream — not papered over with fallback logic.
    """
    n = len(df)
    if n == 0:
        return

    nan_summary = []
    for col in GATE_INPUT_COLUMNS:
        if col not in df.columns:
            continue
        null_count = df[col].isna().sum()
        if null_count > 0:
            null_pct = (null_count / n) * 100
            affected = df.loc[df[col].isna(), "Underlying_Ticker"].dropna().unique().tolist()
            nan_summary.append(f"{col}: {null_count}/{n} ({null_pct:.0f}%)")
            report.null_rates[f"gate_{col}"] = round(null_pct, 1)

    if nan_summary:
        report.alerts.append(IntegrityAlert(
            severity="WARNING",
            category="GATE_NAN",
            column="gate_inputs",
            message=(
                f"NaN in gate-input columns (may cause silent comparison bugs): "
                f"{'; '.join(nan_summary)}"
            ),
            value=float(len(nan_summary)),
        ))


def _check_value_ranges(df: pd.DataFrame, report: IntegrityReport) -> None:
    """Check numeric columns for out-of-range values."""
    for col, vmin, vmax, desc in RANGE_CHECKS:
        if col not in df.columns:
            continue
        series = pd.to_numeric(df[col], errors="coerce")
        oob_mask = (series < vmin) | (series > vmax)
        oob_count = oob_mask.sum()
        if oob_count > 0:
            affected = df.loc[oob_mask, "Underlying_Ticker"].dropna().unique().tolist()
            bad_vals = series[oob_mask].dropna().tolist()[:5]
            report.alerts.append(IntegrityAlert(
                severity="WARNING",
                category="RANGE",
                column=col,
                message=f"{desc} ({col}): {oob_count} values out of [{vmin}, {vmax}] — sample: {bad_vals}",
                affected_tickers=affected[:10],
                value=float(oob_count),
            ))


def _check_enum_values(df: pd.DataFrame, report: IntegrityReport) -> None:
    """Check categorical columns for unexpected values."""
    if "Action" in df.columns:
        actions = df["Action"].dropna().unique()
        bad = [a for a in actions if a not in VALID_ACTIONS]
        if bad:
            report.alerts.append(IntegrityAlert(
                severity="ERROR",
                category="ENUM",
                column="Action",
                message=f"Unexpected Action values: {bad}",
                value=float(len(bad)),
            ))

    if "Urgency" in df.columns:
        urgencies = df["Urgency"].dropna().unique()
        bad = [u for u in urgencies if u not in VALID_URGENCIES]
        if bad:
            report.alerts.append(IntegrityAlert(
                severity="ERROR",
                category="ENUM",
                column="Urgency",
                message=f"Unexpected Urgency values: {bad}",
                value=float(len(bad)),
            ))


def _check_resolution_reasons(df: pd.DataFrame, report: IntegrityReport) -> None:
    """Track Resolution_Reason distribution — MISSING_* signals upstream failures."""
    if "Resolution_Reason" not in df.columns:
        return

    dist = df["Resolution_Reason"].fillna("").value_counts().to_dict()
    report.resolution_distribution = {str(k): int(v) for k, v in dist.items()}

    n = len(df)
    for reason in ("MISSING_PRIMITIVES", "MISSING_INDICATORS", "MISSING_GREEKS"):
        count = dist.get(reason, 0)
        if count > 0:
            pct = (count / n) * 100
            affected = df.loc[
                df["Resolution_Reason"].fillna("") == reason,
                "Underlying_Ticker"
            ].dropna().unique().tolist()
            severity = "ERROR" if pct > 20 else "WARNING"
            report.alerts.append(IntegrityAlert(
                severity=severity,
                category="DISTRIBUTION",
                column="Resolution_Reason",
                message=f"{reason}: {count}/{n} ({pct:.1f}%) positions affected",
                affected_tickers=affected[:10],
                value=pct,
            ))


def _check_action_distribution(df: pd.DataFrame, report: IntegrityReport) -> None:
    """Track action distribution — flag anomalies (e.g., 100% EXIT)."""
    if "Action" not in df.columns:
        return

    dist = df["Action"].fillna("UNKNOWN").value_counts().to_dict()
    report.action_distribution = {str(k): int(v) for k, v in dist.items()}

    n = len(df)
    exit_count = dist.get("EXIT", 0)
    if n > 5 and exit_count == n:
        report.alerts.append(IntegrityAlert(
            severity="WARNING",
            category="DISTRIBUTION",
            column="Action",
            message=f"ALL {n} positions marked EXIT — possible circuit breaker or data issue",
            value=100.0,
        ))


def _check_greek_completeness(df: pd.DataFrame, report: IntegrityReport) -> None:
    """Check that option positions have Greek values populated."""
    opt_mask = df.get("AssetType", pd.Series("", index=df.index)).isin(["OPTION", "Option"])
    if opt_mask.sum() == 0:
        return

    opts = df[opt_mask]
    n_opts = len(opts)
    for greek in ["Delta", "Gamma", "Theta", "Vega"]:
        if greek not in opts.columns:
            continue
        null_count = opts[greek].isna().sum()
        if null_count > 0:
            pct = (null_count / n_opts) * 100
            affected = opts.loc[opts[greek].isna(), "Underlying_Ticker"].dropna().unique().tolist()
            report.alerts.append(IntegrityAlert(
                severity="WARNING" if pct < 50 else "ERROR",
                category="NULL_RATE",
                column=greek,
                message=f"{greek} missing on {null_count}/{n_opts} ({pct:.1f}%) option legs",
                affected_tickers=affected[:10],
                value=pct,
            ))


def _check_chart_primitives(df: pd.DataFrame, report: IntegrityReport) -> None:
    """Check chart primitive coverage — the bug we just fixed."""
    primitives = ["roc_5", "roc_20", "momentum_slope", "adx_14", "atr_14"]
    n = len(df)
    if n == 0:
        return

    all_null = []
    for col in primitives:
        if col not in df.columns:
            continue
        null_count = df[col].isna().sum()
        if null_count == n:
            all_null.append(col)
        elif null_count > 0:
            pct = (null_count / n) * 100
            affected = df.loc[df[col].isna(), "Underlying_Ticker"].dropna().unique().tolist()
            report.alerts.append(IntegrityAlert(
                severity="WARNING",
                category="NULL_RATE",
                column=col,
                message=f"Chart primitive {col}: {null_count}/{n} ({pct:.1f}%) NULL",
                affected_tickers=affected[:10],
                value=pct,
            ))

    if all_null:
        report.alerts.append(IntegrityAlert(
            severity="ERROR",
            category="NULL_RATE",
            column="chart_primitives",
            message=f"ALL values NULL for: {', '.join(all_null)} — OHLC loading likely failed",
            value=100.0,
        ))


def _check_expired_positions(df: pd.DataFrame, report: IntegrityReport) -> None:
    """Flag option positions with DTE <= 0 still being processed."""
    if "DTE" not in df.columns:
        return

    opt_mask = df.get("AssetType", pd.Series("", index=df.index)).isin(["OPTION", "Option"])
    if opt_mask.sum() == 0:
        return

    opts = df[opt_mask]
    expired_mask = pd.to_numeric(opts["DTE"], errors="coerce").fillna(99) <= 0
    expired_count = expired_mask.sum()
    if expired_count > 0:
        affected = opts.loc[expired_mask, "Underlying_Ticker"].dropna().unique().tolist()
        # Check if they are AWAITING_SETTLEMENT (expected) or not (stale)
        ds = opts.loc[expired_mask, "Decision_State"].fillna("")
        non_settlement = (ds != "AWAITING_SETTLEMENT").sum()
        if non_settlement > 0:
            report.alerts.append(IntegrityAlert(
                severity="WARNING",
                category="STALE",
                column="DTE",
                message=(
                    f"{non_settlement} expired option(s) (DTE≤0) still active "
                    f"(not AWAITING_SETTLEMENT) — may be zombie positions"
                ),
                affected_tickers=affected[:10],
                value=float(non_settlement),
            ))


def _check_snapshot_age(df: pd.DataFrame, report: IntegrityReport) -> None:
    """Flag if Snapshot_TS is significantly older than current time."""
    if "Snapshot_TS" not in df.columns:
        return

    try:
        ts = pd.to_datetime(df["Snapshot_TS"], errors="coerce")
        valid_ts = ts.dropna()
        if valid_ts.empty:
            return

        latest = valid_ts.max()
        now = pd.Timestamp.now()
        # Make both tz-naive for comparison
        if latest.tzinfo is not None:
            latest = latest.tz_localize(None)

        age_hours = (now - latest).total_seconds() / 3600
        if age_hours > 24:
            report.alerts.append(IntegrityAlert(
                severity="WARNING",
                category="STALE",
                column="Snapshot_TS",
                message=f"Latest snapshot is {age_hours:.0f}h old — data may be stale",
                value=age_hours,
            ))
    except Exception:
        pass


def _determine_health(report: IntegrityReport) -> str:
    """Set overall_health based on alert severity counts."""
    if report.error_count > 0:
        return "CRITICAL"
    if report.warning_count > 3:
        return "DEGRADED"
    if report.warning_count > 0:
        return "DEGRADED"
    return "HEALTHY"


# ── Public API ──────────────────────────────────────────────────────────────

def run_integrity_checks(df: pd.DataFrame, run_id: str) -> IntegrityReport:
    """
    Run all integrity checks on the final dataframe.

    Called from run_all.py after enforce_management_schema().
    Returns an IntegrityReport with alerts and metrics.
    """
    report = IntegrityReport(
        run_id=run_id,
        timestamp=datetime.now().isoformat(),
        total_positions=len(df),
    )

    if df.empty:
        report.alerts.append(IntegrityAlert(
            severity="ERROR",
            category="DISTRIBUTION",
            column="df_final",
            message="Empty dataframe — no positions to evaluate",
        ))
        report.overall_health = "CRITICAL"
        return report

    _check_null_rates(df, report)
    _check_option_required_columns(df, report)
    _check_income_stock_iv(df, report)
    _check_gate_input_nan(df, report)
    _check_value_ranges(df, report)
    _check_enum_values(df, report)
    _check_resolution_reasons(df, report)
    _check_action_distribution(df, report)
    _check_greek_completeness(df, report)
    _check_chart_primitives(df, report)
    _check_expired_positions(df, report)
    _check_snapshot_age(df, report)

    report.overall_health = _determine_health(report)
    return report


def persist_audit(report: IntegrityReport, con) -> None:
    """Persist audit row to DuckDB `data_integrity_audit` table."""
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS data_integrity_audit (
                run_id VARCHAR,
                timestamp VARCHAR,
                total_positions INTEGER,
                overall_health VARCHAR,
                error_count INTEGER,
                warning_count INTEGER,
                null_rates_json VARCHAR,
                resolution_distribution_json VARCHAR,
                action_distribution_json VARCHAR,
                alerts_json VARCHAR
            )
        """)
        d = report.to_dict()
        con.execute(
            "INSERT INTO data_integrity_audit VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                d["run_id"], d["timestamp"], d["total_positions"],
                d["overall_health"], d["error_count"], d["warning_count"],
                d["null_rates_json"], d["resolution_distribution_json"],
                d["action_distribution_json"], d["alerts_json"],
            ],
        )
    except Exception as e:
        logger.warning(f"⚠️ Failed to persist integrity audit: {e}")


# ── Decision Input Audit — typed snapshot of doctrine gate inputs ──────────
# Columns extracted flat for direct SQL querying. Remaining columns captured
# as JSON blob for deep debugging.  Two-phase persist: Phase 1 captures
# inputs before doctrine; Phase 2 updates with decision outputs after.

_VALID_SNAPSHOT_STAGES = {'INPUT_CAPTURED', 'OUTPUT_UPDATED'}

# Flat column mapping: df column name → audit table column name.
# These are the exact fields doctrine gates branch on.
FLAT_COLUMN_MAP = {
    # Price
    'UL Last': 'ul_last',
    'Price_Source': 'ul_last_source',
    'Price_TS': 'price_ts',
    # Greeks
    'Delta': 'delta',
    'Gamma': 'gamma',
    'Theta': 'theta',
    'Vega': 'vega',
    'Greeks_Source': 'greeks_source',
    'Greeks_TS': 'greeks_ts',
    # Gate-driving
    'DTE': 'dte',
    'IV_Now': 'iv_now',
    'IV_Rank': 'iv_rank',
    'IV_30D': 'iv_30d',
    'HV_20D': 'hv_20d',
    'roc_5': 'roc_5',
    'roc_10': 'roc_10',
    'roc_20': 'roc_20',
    'Price_Drift_Pct': 'price_drift_pct',
    'Drift_Direction': 'drift_direction',
    'Drift_Magnitude': 'drift_magnitude',
    'Total_GL_Decimal': 'pnl_pct',
    'PnL_Total': 'pnl_total',
    'Basis': 'basis',
    'Current_Value': 'current_value',
    'Days_In_Trade': 'days_in_trade',
    'Lifecycle_Phase': 'lifecycle_phase',
    # Thesis/signals
    'Thesis_State': 'thesis_state',
    'Conviction_Status': 'conviction_status',
    'Thesis_Drawdown_Type': 'thesis_drawdown_type',
    'Sector_Relative_Strength': 'sector_relative_strength',
    'Sector_RS_ZScore': 'sector_rs_zscore',
    'Market_Structure': 'market_structure',
    'Weekly_Trend_Bias': 'weekly_trend_bias',
    'RSI_Divergence': 'rsi_divergence',
    'MACD_Divergence': 'macd_divergence',
    'Keltner_Squeeze_On': 'keltner_squeeze_on',
    'OBV_Slope': 'obv_slope',
    'RS_vs_SPY_20d': 'rs_vs_spy_20d',
    'adx_14': 'adx_14',
    'rsi_14': 'rsi_14',
    'momentum_slope': 'momentum_slope',
    'Recovery_Feasibility': 'recovery_feasibility',
    # Entry anchors
    'Underlying_Price_Entry': 'underlying_price_entry',
    'Delta_Entry': 'delta_entry',
    'IV_Entry': 'iv_entry',
    'DTE_Entry': 'dte_entry',
    'Entry_Chart_State_PriceStructure': 'entry_price_structure',
    'Entry_Chart_State_TrendIntegrity': 'entry_trend_integrity',
    'Entry_Structure': 'entry_structure',
    # Prior decision
    'Prior_Action': 'prior_action',
    'Prior_Doctrine_Source': 'prior_doctrine_source',
    'Prior_Snapshot_TS': 'prior_snapshot_ts',
    'Prior_Action_Streak': 'prior_action_streak',
    # Macro
    'Days_To_Macro': 'days_to_macro',
    'Macro_Impact': 'macro_impact',
    'Macro_Next_Type': 'macro_next_type',
    # Pre-doctrine validation
    'Pre_Doctrine_Flag': 'pre_doctrine_flag',
    'Pre_Doctrine_Detail': 'pre_doctrine_detail',
}

# Columns that are the flat audit columns (used to exclude from JSON blob)
_FLAT_DF_COLS = set(FLAT_COLUMN_MAP.keys())

_CREATE_DECISION_INPUT_AUDIT = """
CREATE TABLE IF NOT EXISTS decision_input_audit (
    snapshot_id VARCHAR NOT NULL,
    run_id VARCHAR NOT NULL,
    snapshot_ts TIMESTAMP,
    trade_id VARCHAR NOT NULL,
    leg_id VARCHAR,
    ticker VARCHAR NOT NULL,
    strategy VARCHAR,
    asset_type VARCHAR,
    symbol VARCHAR,
    snapshot_stage VARCHAR NOT NULL,
    input_snapshot_ts TIMESTAMP,
    output_update_ts TIMESTAMP,
    ul_last DOUBLE,
    ul_last_source VARCHAR,
    price_ts TIMESTAMP,
    delta DOUBLE,
    gamma DOUBLE,
    theta DOUBLE,
    vega DOUBLE,
    greeks_source VARCHAR,
    greeks_ts TIMESTAMP,
    dte DOUBLE,
    iv_now DOUBLE,
    iv_rank DOUBLE,
    iv_30d DOUBLE,
    hv_20d DOUBLE,
    roc_5 DOUBLE,
    roc_10 DOUBLE,
    roc_20 DOUBLE,
    price_drift_pct DOUBLE,
    drift_direction VARCHAR,
    drift_magnitude VARCHAR,
    pnl_pct DOUBLE,
    pnl_total DOUBLE,
    basis DOUBLE,
    current_value DOUBLE,
    days_in_trade DOUBLE,
    lifecycle_phase VARCHAR,
    thesis_state VARCHAR,
    conviction_status VARCHAR,
    thesis_drawdown_type VARCHAR,
    sector_relative_strength VARCHAR,
    sector_rs_zscore DOUBLE,
    market_structure VARCHAR,
    weekly_trend_bias VARCHAR,
    rsi_divergence VARCHAR,
    macd_divergence VARCHAR,
    keltner_squeeze_on VARCHAR,
    obv_slope DOUBLE,
    rs_vs_spy_20d DOUBLE,
    adx_14 DOUBLE,
    rsi_14 DOUBLE,
    momentum_slope DOUBLE,
    recovery_feasibility VARCHAR,
    underlying_price_entry DOUBLE,
    delta_entry DOUBLE,
    iv_entry DOUBLE,
    dte_entry DOUBLE,
    entry_price_structure VARCHAR,
    entry_trend_integrity VARCHAR,
    entry_structure VARCHAR,
    prior_action VARCHAR,
    prior_doctrine_source VARCHAR,
    prior_snapshot_ts TIMESTAMP,
    prior_action_streak INTEGER,
    days_to_macro DOUBLE,
    macro_impact VARCHAR,
    macro_next_type VARCHAR,
    pre_doctrine_flag VARCHAR,
    pre_doctrine_detail VARCHAR,
    action VARCHAR,
    urgency VARCHAR,
    doctrine_source VARCHAR,
    decision_state VARCHAR,
    rationale_excerpt VARCHAR,
    input_context_json VARCHAR,
    PRIMARY KEY (snapshot_id)
)
"""


def _ensure_audit_schema(con) -> None:
    """Create table if missing; ALTER TABLE for new columns if schema drifted."""
    con.execute(_CREATE_DECISION_INPUT_AUDIT)
    # Schema evolution: add any columns missing from an older table version
    db_cols_info = con.execute(
        "PRAGMA table_info('decision_input_audit')"
    ).fetchall()
    existing_cols = {row[1] for row in db_cols_info}
    # All expected flat audit columns (extract from CREATE statement)
    _expected = {
        'snapshot_id', 'run_id', 'snapshot_ts', 'trade_id', 'leg_id',
        'ticker', 'strategy', 'asset_type', 'symbol',
        'snapshot_stage', 'input_snapshot_ts', 'output_update_ts',
        'action', 'urgency', 'doctrine_source', 'decision_state',
        'rationale_excerpt', 'input_context_json',
    }
    _expected.update(FLAT_COLUMN_MAP.values())
    for col in _expected:
        if col not in existing_cols:
            col_type = 'DOUBLE' if col in {
                'ul_last', 'delta', 'gamma', 'theta', 'vega', 'dte',
                'iv_now', 'iv_rank', 'iv_30d', 'hv_20d',
                'roc_5', 'roc_10', 'roc_20',
                'price_drift_pct', 'pnl_pct', 'pnl_total', 'basis',
                'current_value', 'days_in_trade', 'sector_rs_zscore',
                'obv_slope', 'rs_vs_spy_20d', 'adx_14', 'rsi_14',
                'momentum_slope', 'underlying_price_entry', 'delta_entry',
                'iv_entry', 'dte_entry', 'days_to_macro',
            } else 'TIMESTAMP' if col in {
                'snapshot_ts', 'input_snapshot_ts', 'output_update_ts',
                'price_ts', 'greeks_ts', 'prior_snapshot_ts',
            } else 'INTEGER' if col in {
                'prior_action_streak',
            } else 'VARCHAR'
            try:
                con.execute(
                    f'ALTER TABLE decision_input_audit ADD COLUMN "{col}" {col_type}'
                )
                logger.info(f"[DecisionInputAudit] Added column '{col}' ({col_type})")
            except Exception:
                pass  # column already exists or concurrent add


def _make_snapshot_id(run_id: str, trade_id: str, leg_id) -> str:
    """Deterministic snapshot ID: run_id:trade_id:leg_id|STOCK."""
    leg = str(leg_id) if leg_id and pd.notna(leg_id) and str(leg_id).strip() else 'STOCK'
    return f"{run_id}:{trade_id}:{leg}"


def _safe_val(row, col):
    """Extract value from row, converting NaN/NaT to None for DuckDB."""
    val = row.get(col)
    if val is None:
        return None
    if isinstance(val, float) and (np.isnan(val) or np.isinf(val)):
        return None
    if isinstance(val, pd.Timestamp) and pd.isna(val):
        return None
    return val


def capture_decision_inputs(df: pd.DataFrame, run_id: str) -> int:
    """Phase 1: Capture pre-doctrine gate inputs to decision_input_audit.

    Returns number of rows inserted.
    """
    from core.shared.data_contracts.config import PIPELINE_DB_PATH
    from core.shared.data_layer.duckdb_utils import get_duckdb_connection

    now_ts = datetime.utcnow().isoformat()
    rows_inserted = 0

    try:
        with get_duckdb_connection(read_only=False) as con:
            _ensure_audit_schema(con)

            for idx, row in df.iterrows():
                trade_id = str(row.get('TradeID', '') or '')
                leg_id = row.get('LegID')
                ticker = str(row.get('Underlying_Ticker', '') or '')
                if not trade_id or not ticker:
                    continue

                snapshot_id = _make_snapshot_id(run_id, trade_id, leg_id)
                snapshot_ts = _safe_val(row, 'Snapshot_TS')
                strategy = str(_safe_val(row, 'Strategy') or '')
                asset_type = str(_safe_val(row, 'AssetType') or '')
                # Symbol: OCC for options, ticker for stocks
                if asset_type.upper() in ('OPTION', 'OPTIONS'):
                    symbol = str(_safe_val(row, 'Symbol') or '')
                else:
                    symbol = ticker

                # Build flat columns
                flat_vals = {}
                for df_col, audit_col in FLAT_COLUMN_MAP.items():
                    flat_vals[audit_col] = _safe_val(row, df_col)

                # Build JSON context blob with remaining columns
                _excluded = _FLAT_DF_COLS | {
                    'TradeID', 'LegID', 'Underlying_Ticker', 'Strategy',
                    'AssetType', 'Symbol', 'Snapshot_TS', 'Rationale',
                }
                context = {}
                for col in df.columns:
                    if col in _excluded:
                        continue
                    v = _safe_val(row, col)
                    if v is not None:
                        # Convert non-serializable types
                        if isinstance(v, (pd.Timestamp, datetime)):
                            v = v.isoformat()
                        elif isinstance(v, (np.integer,)):
                            v = int(v)
                        elif isinstance(v, (np.floating,)):
                            v = float(v) if not np.isnan(v) else None
                        elif isinstance(v, (np.bool_,)):
                            v = bool(v)
                        if v is not None:
                            context[col] = v
                context_json = json.dumps(context, default=str)

                # INSERT with snapshot_stage = INPUT_CAPTURED
                con.execute("""
                    INSERT OR REPLACE INTO decision_input_audit (
                        snapshot_id, run_id, snapshot_ts, trade_id, leg_id,
                        ticker, strategy, asset_type, symbol,
                        snapshot_stage, input_snapshot_ts, output_update_ts,
                        ul_last, ul_last_source, price_ts,
                        delta, gamma, theta, vega, greeks_source, greeks_ts,
                        dte, iv_now, iv_rank, iv_30d, hv_20d,
                        roc_5, roc_10, roc_20,
                        price_drift_pct, drift_direction, drift_magnitude,
                        pnl_pct, pnl_total, basis, current_value,
                        days_in_trade, lifecycle_phase,
                        thesis_state, conviction_status, thesis_drawdown_type,
                        sector_relative_strength, sector_rs_zscore,
                        market_structure, weekly_trend_bias,
                        rsi_divergence, macd_divergence, keltner_squeeze_on,
                        obv_slope, rs_vs_spy_20d, adx_14, rsi_14, momentum_slope,
                        recovery_feasibility,
                        underlying_price_entry, delta_entry, iv_entry, dte_entry,
                        entry_price_structure, entry_trend_integrity, entry_structure,
                        prior_action, prior_doctrine_source, prior_snapshot_ts,
                        prior_action_streak,
                        days_to_macro, macro_impact, macro_next_type,
                        pre_doctrine_flag, pre_doctrine_detail,
                        action, urgency, doctrine_source, decision_state,
                        rationale_excerpt, input_context_json
                    ) VALUES (
                        ?, ?, ?, ?, ?,
                        ?, ?, ?, ?,
                        'INPUT_CAPTURED', ?, NULL,
                        ?, ?, ?,
                        ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?,
                        ?, ?, ?,
                        ?, ?, ?,
                        ?, ?, ?, ?,
                        ?, ?,
                        ?, ?, ?,
                        ?, ?,
                        ?, ?,
                        ?, ?, ?,
                        ?, ?, ?, ?, ?,
                        ?,
                        ?, ?, ?, ?,
                        ?, ?, ?,
                        ?, ?, ?,
                        ?,
                        ?, ?, ?,
                        ?, ?,
                        NULL, NULL, NULL, NULL,
                        NULL, ?
                    )
                """, [
                    snapshot_id, run_id, snapshot_ts, trade_id,
                    str(leg_id) if leg_id and pd.notna(leg_id) else None,
                    ticker, strategy, asset_type, symbol,
                    now_ts,
                    flat_vals.get('ul_last'), flat_vals.get('ul_last_source'),
                    flat_vals.get('price_ts'),
                    flat_vals.get('delta'), flat_vals.get('gamma'),
                    flat_vals.get('theta'), flat_vals.get('vega'),
                    flat_vals.get('greeks_source'), flat_vals.get('greeks_ts'),
                    flat_vals.get('dte'), flat_vals.get('iv_now'),
                    flat_vals.get('iv_rank'), flat_vals.get('iv_30d'),
                    flat_vals.get('hv_20d'),
                    flat_vals.get('roc_5'), flat_vals.get('roc_10'),
                    flat_vals.get('roc_20'),
                    flat_vals.get('price_drift_pct'),
                    flat_vals.get('drift_direction'),
                    flat_vals.get('drift_magnitude'),
                    flat_vals.get('pnl_pct'), flat_vals.get('pnl_total'),
                    flat_vals.get('basis'), flat_vals.get('current_value'),
                    flat_vals.get('days_in_trade'),
                    flat_vals.get('lifecycle_phase'),
                    flat_vals.get('thesis_state'),
                    flat_vals.get('conviction_status'),
                    flat_vals.get('thesis_drawdown_type'),
                    flat_vals.get('sector_relative_strength'),
                    flat_vals.get('sector_rs_zscore'),
                    flat_vals.get('market_structure'),
                    flat_vals.get('weekly_trend_bias'),
                    flat_vals.get('rsi_divergence'),
                    flat_vals.get('macd_divergence'),
                    str(flat_vals.get('keltner_squeeze_on'))
                    if flat_vals.get('keltner_squeeze_on') is not None else None,
                    flat_vals.get('obv_slope'),
                    flat_vals.get('rs_vs_spy_20d'),
                    flat_vals.get('adx_14'), flat_vals.get('rsi_14'),
                    flat_vals.get('momentum_slope'),
                    flat_vals.get('recovery_feasibility'),
                    flat_vals.get('underlying_price_entry'),
                    flat_vals.get('delta_entry'),
                    flat_vals.get('iv_entry'), flat_vals.get('dte_entry'),
                    flat_vals.get('entry_price_structure'),
                    flat_vals.get('entry_trend_integrity'),
                    flat_vals.get('entry_structure'),
                    flat_vals.get('prior_action'),
                    flat_vals.get('prior_doctrine_source'),
                    flat_vals.get('prior_snapshot_ts'),
                    int(flat_vals['prior_action_streak'])
                    if flat_vals.get('prior_action_streak') is not None else None,
                    flat_vals.get('days_to_macro'),
                    flat_vals.get('macro_impact'),
                    flat_vals.get('macro_next_type'),
                    flat_vals.get('pre_doctrine_flag'),
                    flat_vals.get('pre_doctrine_detail'),
                    context_json,
                ])
                rows_inserted += 1

        logger.info(
            f"[DecisionInputAudit] Phase 1: captured {rows_inserted} input snapshots"
        )
    except Exception as e:
        logger.warning(f"[DecisionInputAudit] Phase 1 failed (non-fatal): {e}")

    return rows_inserted


def update_decision_outputs(df: pd.DataFrame, run_id: str) -> int:
    """Phase 2: Update decision_input_audit with doctrine outputs.

    Returns number of rows updated.
    """
    from core.shared.data_layer.duckdb_utils import get_duckdb_connection

    now_ts = datetime.utcnow().isoformat()
    rows_updated = 0

    try:
        with get_duckdb_connection(read_only=False) as con:
            _ensure_audit_schema(con)

            for _, row in df.iterrows():
                trade_id = str(row.get('TradeID', '') or '')
                leg_id = row.get('LegID')
                if not trade_id:
                    continue

                snapshot_id = _make_snapshot_id(run_id, trade_id, leg_id)
                action = _safe_val(row, 'Action')
                urgency = _safe_val(row, 'Urgency')
                doctrine_source = _safe_val(row, 'Doctrine_Source')
                decision_state = _safe_val(row, 'Decision_State')
                rationale = str(_safe_val(row, 'Rationale') or '')
                rationale_excerpt = rationale[:500] if rationale else None

                con.execute("""
                    UPDATE decision_input_audit
                    SET snapshot_stage = 'OUTPUT_UPDATED',
                        output_update_ts = ?,
                        action = ?,
                        urgency = ?,
                        doctrine_source = ?,
                        decision_state = ?,
                        rationale_excerpt = ?
                    WHERE snapshot_id = ?
                """, [
                    now_ts, action, urgency, doctrine_source,
                    decision_state, rationale_excerpt, snapshot_id,
                ])
                rows_updated += 1

        logger.info(
            f"[DecisionInputAudit] Phase 2: updated {rows_updated} rows with outputs"
        )
    except Exception as e:
        logger.warning(f"[DecisionInputAudit] Phase 2 failed (non-fatal): {e}")

    return rows_updated


# ── Pre-Doctrine Validation Gate ──────────────────────────────────────────

# Strategy → required columns (exact df column names at validation point).
# If any listed column is NaN for a position with that strategy → BLOCKING.
STRATEGY_REQUIRED_INPUTS = {
    'LONG_CALL': ['UL Last', 'DTE', 'Delta', 'roc_5', 'Price_Drift_Pct'],
    'LONG_PUT':  ['UL Last', 'DTE', 'Delta', 'roc_5', 'Price_Drift_Pct'],
    'BUY_WRITE': ['UL Last', 'DTE', 'Delta', 'Theta'],
    'COVERED_CALL': ['UL Last', 'DTE', 'Delta', 'Theta'],
    'PMCC':      ['UL Last', 'DTE', 'Delta', 'Theta'],
    'SHORT_PUT': ['UL Last', 'DTE', 'Delta', 'Theta'],
    '_DEFAULT':  ['UL Last'],
}

# Conditionally-required: only checked if column exists in df
STRATEGY_CONDITIONAL_INPUTS = {
    'BUY_WRITE': ['Short_Call_Delta'],
    'COVERED_CALL': ['Short_Call_Delta'],
    'PMCC': ['Short_Call_Delta'],
}

# Staleness threshold (conservative elapsed-time heuristic, not full
# market-calendar model). 28h covers overnight + buffer. May flag
# long-weekend data as stale — acceptable for v1.
_STALE_HOURS = 28


@dataclass
class PreDoctrinePosition:
    """One position's pre-doctrine validation result."""
    idx: int                # DataFrame index
    flag: str               # DATA_BLOCKED | PRICE_STALE | GREEKS_MISSING | WARNING
    detail: str             # human-readable reason
    blocking: bool = True   # True = BLOCKING, False = WARNING only


@dataclass
class PreDoctrineReport:
    """Aggregate pre-doctrine validation report."""
    run_id: str
    total_positions: int
    positions: List[PreDoctrinePosition] = field(default_factory=list)

    @property
    def blocked_positions(self) -> List[tuple]:
        """Return (idx, flag, detail) for BLOCKING positions only."""
        return [
            (p.idx, p.flag, p.detail)
            for p in self.positions
            if p.blocking
        ]

    @property
    def blocked_count(self) -> int:
        return sum(1 for p in self.positions if p.blocking)

    @property
    def warning_count(self) -> int:
        return sum(1 for p in self.positions if not p.blocking)


def _is_price_stale(price_ts, snapshot_ts) -> bool:
    """Conservative elapsed-time heuristic for price staleness.

    v1 design: simple age threshold. NOT a full market-calendar model.
    28h covers overnight + buffer. May flag long-weekend data as stale
    (acceptable: conservative is safer than permissive for v1).
    Future: integrate market_calendar for precise session-aware staleness.
    """
    if price_ts is None or pd.isna(price_ts):
        return True  # no timestamp = assume stale
    try:
        _snap = pd.to_datetime(snapshot_ts)
        _price = pd.to_datetime(price_ts)
        # Normalize tz: if one is naive and the other aware, strip tz from
        # the aware one. Snapshot_TS (from broker CSV) is typically naive;
        # Price_TS (from live_price_provider) is UTC-aware. Mixing them
        # causes TypeError that the old except clause silently ate.
        if _snap.tzinfo is None and _price.tzinfo is not None:
            _price = _price.tz_localize(None)
        elif _snap.tzinfo is not None and _price.tzinfo is None:
            _snap = _snap.tz_localize(None)
        age = _snap - _price
        return age > pd.Timedelta(hours=_STALE_HOURS)
    except Exception as e:
        logger.warning(f"[DataIntegrity] _is_price_stale error: {e} "
                       f"(price_ts={price_ts}, snapshot_ts={snapshot_ts})")
        return True


def validate_pre_doctrine(
    df: pd.DataFrame, run_id: str
) -> PreDoctrineReport:
    """Validate gate inputs before doctrine runs.

    Returns PreDoctrineReport with per-position flags.
    Caller should set Pre_Doctrine_Flag/Detail on blocked positions.
    """
    report = PreDoctrineReport(run_id=run_id, total_positions=len(df))

    for idx, row in df.iterrows():
        asset_type = str(row.get('AssetType', '') or '').upper()
        is_option = asset_type in ('OPTION', 'OPTIONS')
        strategy = str(row.get('Strategy', '') or '').upper()
        ticker = str(row.get('Underlying_Ticker', '') or '')
        issues = []

        # 1. UL Last invalid (ALL instruments)
        ul_last = row.get('UL Last')
        if pd.isna(ul_last) or (isinstance(ul_last, (int, float)) and ul_last <= 0):
            issues.append(('DATA_BLOCKED', f'UL Last invalid ({ul_last})'))

        # 2. DTE missing (OPTION only)
        if is_option:
            dte_val = row.get('DTE')
            if pd.isna(dte_val):
                issues.append(('DATA_BLOCKED', 'DTE is NaN on OPTION row'))

        # 3. Price staleness (ALL, based on Price_TS with Snapshot_TS fallback)
        # Price_TS is set by live_price_provider when it refreshes prices.
        # If Price_TS isn't populated yet (transition period), fall back to
        # Snapshot_TS — the broker CSV timestamp from the current run.
        # Only flag PRICE_STALE when we have a real timestamp that's old.
        snapshot_ts = row.get('Snapshot_TS')
        price_ts = row.get('Price_TS')
        _effective_price_ts = price_ts if (price_ts is not None and not pd.isna(price_ts)) else snapshot_ts
        if snapshot_ts and _is_price_stale(_effective_price_ts, snapshot_ts):
            issues.append(('PRICE_STALE', f'Price_TS stale or missing ({price_ts})'))

        # 4. Greeks missing (OPTION only)
        if is_option:
            delta_val = row.get('Delta')
            theta_val = row.get('Theta')
            if pd.isna(delta_val) and pd.isna(theta_val):
                issues.append(('GREEKS_MISSING', 'Delta and Theta both NaN'))

        # 5. Strategy-specific required inputs
        # Skip option-only columns (DTE, Delta, Gamma, Theta, Vega) on STOCK rows
        _option_only_cols = {'DTE', 'Delta', 'Gamma', 'Theta', 'Vega'}
        required = STRATEGY_REQUIRED_INPUTS.get(
            strategy, STRATEGY_REQUIRED_INPUTS['_DEFAULT']
        )
        missing_cols = []
        for col in required:
            if col not in df.columns:
                continue
            if not is_option and col in _option_only_cols:
                continue  # stock rows don't have option-specific fields
            if pd.isna(row.get(col)):
                missing_cols.append(col)
        if missing_cols:
            issues.append((
                'DATA_BLOCKED',
                f'Strategy {strategy} missing required: {", ".join(missing_cols)}'
            ))

        # 6. Conditionally-required inputs
        conditional = STRATEGY_CONDITIONAL_INPUTS.get(strategy, [])
        for col in conditional:
            if col in df.columns and pd.isna(row.get(col)):
                # WARNING only — column may not be populated yet
                report.positions.append(PreDoctrinePosition(
                    idx=idx,
                    flag='WARNING',
                    detail=f'{col} is NaN (conditional for {strategy})',
                    blocking=False,
                ))

        # 7. Chart primitives all null (WARNING)
        chart_cols = ['roc_5', 'roc_10', 'adx_14', 'rsi_14']
        existing_chart = [c for c in chart_cols if c in df.columns]
        if existing_chart and all(pd.isna(row.get(c)) for c in existing_chart):
            report.positions.append(PreDoctrinePosition(
                idx=idx,
                flag='WARNING',
                detail='All chart primitives (roc_5/roc_10/adx_14/rsi_14) are NaN',
                blocking=False,
            ))

        # Emit blocking issues (use worst flag)
        if issues:
            # Pick the most severe flag
            flags = [i[0] for i in issues]
            if 'DATA_BLOCKED' in flags:
                flag = 'DATA_BLOCKED'
            elif 'GREEKS_MISSING' in flags:
                flag = 'GREEKS_MISSING'
            else:
                flag = flags[0]
            detail = '; '.join(f'[{f}] {d}' for f, d in issues)
            report.positions.append(PreDoctrinePosition(
                idx=idx,
                flag=flag,
                detail=f'{ticker}: {detail}',
                blocking=True,
            ))

    if report.blocked_count > 0:
        logger.warning(
            f"[PreDoctrine] {report.blocked_count}/{report.total_positions} "
            f"positions BLOCKED, {report.warning_count} warnings"
        )
    else:
        logger.info(
            f"[PreDoctrine] All {report.total_positions} positions passed "
            f"({report.warning_count} warnings)"
        )

    return report


def log_report(report: IntegrityReport) -> str:
    """
    Log the report and return a formatted CLI summary string.

    Severity mapping:
      HEALTHY  → single INFO line
      DEGRADED → WARNING block with alert details
      CRITICAL → ERROR block with alert details
    """
    lines = []
    lines.append("")
    lines.append("=" * 60)
    lines.append(f"  DATA INTEGRITY AUDIT — {report.overall_health}")
    lines.append(f"  Run: {report.run_id}  |  Positions: {report.total_positions}")
    lines.append("=" * 60)

    if report.overall_health == "HEALTHY":
        lines.append("  All checks passed. No silent failures detected.")
        lines.append("=" * 60)
        summary = "\n".join(lines)
        logger.info(summary)
        return summary

    # Group alerts by severity
    errors = [a for a in report.alerts if a.severity == "ERROR"]
    warnings = [a for a in report.alerts if a.severity == "WARNING"]

    if errors:
        lines.append(f"\n  ERRORS ({len(errors)}):")
        for a in errors:
            lines.append(f"    [{a.category}] {a.message}")
            if a.affected_tickers:
                lines.append(f"      Tickers: {', '.join(a.affected_tickers)}")

    if warnings:
        lines.append(f"\n  WARNINGS ({len(warnings)}):")
        for a in warnings:
            lines.append(f"    [{a.category}] {a.message}")
            if a.affected_tickers:
                lines.append(f"      Tickers: {', '.join(a.affected_tickers)}")

    # Null rate summary for critical columns with any nulls
    critical_nulls = {k: v for k, v in report.null_rates.items()
                      if v > 0 and k in CRITICAL_COLUMNS}
    if critical_nulls:
        lines.append(f"\n  CRITICAL NULL RATES:")
        for col, rate in sorted(critical_nulls.items(), key=lambda x: -x[1]):
            lines.append(f"    {col}: {rate}%")

    # Resolution reason breakdown
    if report.resolution_distribution:
        lines.append(f"\n  RESOLUTION REASONS:")
        for reason, count in sorted(report.resolution_distribution.items(), key=lambda x: -x[1]):
            label = reason if reason else "(empty/OK)"
            lines.append(f"    {label}: {count}")

    lines.append("=" * 60)
    summary = "\n".join(lines)

    if report.overall_health == "CRITICAL":
        logger.error(summary)
    else:
        logger.warning(summary)

    return summary
