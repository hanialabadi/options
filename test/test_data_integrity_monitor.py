"""Tests for core.management.data_integrity_monitor."""

import json
import math

import numpy as np
import pandas as pd
import pytest

from core.management.data_integrity_monitor import (
    IntegrityAlert,
    IntegrityReport,
    run_integrity_checks,
    log_report,
    CRITICAL_COLUMNS,
    IMPORTANT_COLUMNS,
    INCOME_STRATEGIES,
    GATE_INPUT_COLUMNS,
)


def _make_df(n=10, **overrides):
    """Build a minimal valid df_final for testing."""
    data = {
        "TradeID": [f"T{i}" for i in range(n)],
        "Underlying_Ticker": [f"TICK{i}" for i in range(n)],
        "Symbol": [f"SYM{i}" for i in range(n)],
        "Strategy": ["COVERED_CALL"] * n,
        "AssetType": ["OPTION"] * n,
        "Action": ["HOLD"] * n,
        "Urgency": ["LOW"] * n,
        "Doctrine_Source": ["test_gate"] * n,
        "DTE": [30.0] * n,
        "UL Last": [100.0] * n,
        "Delta": [0.30] * n,
        "Gamma": [0.02] * n,
        "Theta": [-0.05] * n,
        "Vega": [0.10] * n,
        "IV_Contract": [0.25] * n,
        "IV_Underlying_30D": [0.28] * n,
        "HV_20D": [0.22] * n,
        "IV_Rank": [45.0] * n,
        "rsi_14": [55.0] * n,
        "adx_14": [25.0] * n,
        "roc_5": [0.02] * n,
        "roc_20": [0.05] * n,
        "momentum_slope": [0.01] * n,
        "atr_14": [2.5] * n,
        "Strike": [105.0] * n,
        "Expiration": ["2026-04-17"] * n,
        "Basis": [3.50] * n,
        "PriceStructure_State": ["HEALTHY"] * n,
        "TrendIntegrity_State": ["INTACT"] * n,
        "MomentumVelocity_State": ["NEUTRAL"] * n,
        "Equity_Integrity_State": ["HEALTHY"] * n,
        "Thesis_State": ["INTACT"] * n,
        "Resolution_Reason": [""] * n,
    }
    data.update(overrides)
    return pd.DataFrame(data)


class TestHealthyReport:
    def test_clean_df_returns_healthy(self):
        df = _make_df()
        report = run_integrity_checks(df, "test_run_1")
        assert report.overall_health == "HEALTHY"
        assert report.error_count == 0
        assert report.warning_count == 0

    def test_report_metadata(self):
        df = _make_df(n=5)
        report = run_integrity_checks(df, "run_abc")
        assert report.run_id == "run_abc"
        assert report.total_positions == 5

    def test_to_dict_valid_json(self):
        df = _make_df()
        report = run_integrity_checks(df, "test_json")
        d = report.to_dict()
        # All JSON fields should be parseable
        json.loads(d["null_rates_json"])
        json.loads(d["alerts_json"])


class TestNullRateChecks:
    def test_critical_null_triggers_error(self):
        df = _make_df()
        df.loc[0, "Action"] = None
        report = run_integrity_checks(df, "null_test")
        assert report.overall_health == "CRITICAL"
        errors = [a for a in report.alerts if a.severity == "ERROR" and a.column == "Action"]
        assert len(errors) == 1

    def test_important_col_majority_null_triggers_warning(self):
        df = _make_df(n=10)
        df.loc[:5, "rsi_14"] = np.nan  # 60% null
        report = run_integrity_checks(df, "imp_null")
        warnings = [a for a in report.alerts if a.column == "rsi_14"]
        assert len(warnings) >= 1

    def test_important_col_minority_null_no_alert(self):
        df = _make_df(n=10)
        df.loc[0, "rsi_14"] = np.nan  # 10% null — under 50% threshold
        report = run_integrity_checks(df, "minor_null")
        warnings = [a for a in report.alerts if a.column == "rsi_14"]
        assert len(warnings) == 0


class TestValueRangeChecks:
    def test_delta_out_of_range(self):
        df = _make_df()
        df.loc[0, "Delta"] = 5.0  # way out of [-1.05, 1.05]
        report = run_integrity_checks(df, "range_test")
        range_alerts = [a for a in report.alerts if a.category == "RANGE" and a.column == "Delta"]
        assert len(range_alerts) == 1

    def test_rsi_out_of_range(self):
        df = _make_df()
        df.loc[0, "rsi_14"] = 150.0
        report = run_integrity_checks(df, "rsi_range")
        range_alerts = [a for a in report.alerts if a.column == "rsi_14"]
        assert len(range_alerts) >= 1

    def test_valid_ranges_no_alert(self):
        df = _make_df()
        report = run_integrity_checks(df, "valid_range")
        range_alerts = [a for a in report.alerts if a.category == "RANGE"]
        assert len(range_alerts) == 0


class TestEnumChecks:
    def test_bad_action_triggers_error(self):
        df = _make_df()
        df.loc[0, "Action"] = "YOLO"
        report = run_integrity_checks(df, "enum_test")
        enum_alerts = [a for a in report.alerts if a.category == "ENUM" and a.column == "Action"]
        assert len(enum_alerts) == 1
        assert "YOLO" in enum_alerts[0].message

    def test_bad_urgency_triggers_error(self):
        df = _make_df()
        df.loc[0, "Urgency"] = "EXTREME"
        report = run_integrity_checks(df, "urg_test")
        enum_alerts = [a for a in report.alerts if a.column == "Urgency"]
        assert len(enum_alerts) == 1


class TestResolutionReasonChecks:
    def test_missing_primitives_triggers_alert(self):
        df = _make_df(n=10)
        df.loc[:4, "Resolution_Reason"] = "MISSING_PRIMITIVES"  # 50%
        report = run_integrity_checks(df, "res_test")
        res_alerts = [a for a in report.alerts if "MISSING_PRIMITIVES" in a.message]
        assert len(res_alerts) == 1
        assert res_alerts[0].severity == "ERROR"  # >20%

    def test_low_missing_primitives_is_warning(self):
        df = _make_df(n=10)
        df.loc[0, "Resolution_Reason"] = "MISSING_PRIMITIVES"  # 10%
        report = run_integrity_checks(df, "res_low")
        res_alerts = [a for a in report.alerts if "MISSING_PRIMITIVES" in a.message]
        assert len(res_alerts) == 1
        assert res_alerts[0].severity == "WARNING"


class TestActionDistribution:
    def test_all_exit_triggers_warning(self):
        df = _make_df(n=10, Action=["EXIT"] * 10)
        report = run_integrity_checks(df, "all_exit")
        dist_alerts = [a for a in report.alerts if a.category == "DISTRIBUTION" and a.column == "Action"]
        assert len(dist_alerts) == 1


class TestGreekCompleteness:
    def test_option_missing_delta_triggers_alert(self):
        df = _make_df(n=5)
        df["AssetType"] = "OPTION"
        df.loc[:2, "Delta"] = np.nan  # 3/5 = 60%
        report = run_integrity_checks(df, "greek_test")
        greek_alerts = [a for a in report.alerts
                        if a.category == "NULL_RATE" and a.column == "Delta"
                        and "option legs" in a.message]
        assert len(greek_alerts) == 1
        assert greek_alerts[0].severity == "ERROR"  # >50%

    def test_stock_missing_delta_no_option_alert(self):
        df = _make_df(n=5)
        df["AssetType"] = "STOCK"
        df["Delta"] = np.nan
        report = run_integrity_checks(df, "stock_no_greek")
        # Should not trigger greek completeness alert (stocks don't need option greeks)
        greek_alerts = [a for a in report.alerts if "option legs" in a.message]
        assert len(greek_alerts) == 0


class TestChartPrimitives:
    def test_all_primitives_null_triggers_error(self):
        df = _make_df(n=5)
        for col in ["roc_5", "roc_20", "momentum_slope", "adx_14", "atr_14"]:
            df[col] = np.nan
        report = run_integrity_checks(df, "prim_test")
        prim_alerts = [a for a in report.alerts if a.column == "chart_primitives"]
        assert len(prim_alerts) == 1
        assert prim_alerts[0].severity == "ERROR"

    def test_partial_primitive_null_triggers_warning(self):
        df = _make_df(n=10)
        df.loc[:7, "roc_5"] = np.nan  # 80% null
        report = run_integrity_checks(df, "partial_prim")
        prim_alerts = [a for a in report.alerts if a.column == "roc_5"]
        assert len(prim_alerts) >= 1


class TestEmptyDataframe:
    def test_empty_df_returns_critical(self):
        df = pd.DataFrame()
        report = run_integrity_checks(df, "empty_test")
        assert report.overall_health == "CRITICAL"
        assert report.error_count == 1


class TestExpiredPositions:
    def test_expired_non_settlement_triggers_warning(self):
        df = _make_df(n=3)
        df["DTE"] = [0.0, -1.0, 30.0]
        df["Decision_State"] = ["ACTIONABLE", "ACTIONABLE", "ACTIONABLE"]
        report = run_integrity_checks(df, "expired_test")
        stale_alerts = [a for a in report.alerts if a.category == "STALE" and a.column == "DTE"]
        assert len(stale_alerts) == 1
        assert "zombie" in stale_alerts[0].message

    def test_expired_awaiting_settlement_no_alert(self):
        df = _make_df(n=2)
        df["DTE"] = [0.0, 0.0]
        df["Decision_State"] = ["AWAITING_SETTLEMENT", "AWAITING_SETTLEMENT"]
        report = run_integrity_checks(df, "settlement_test")
        stale_alerts = [a for a in report.alerts if a.category == "STALE" and a.column == "DTE"]
        assert len(stale_alerts) == 0


class TestSnapshotAge:
    def test_fresh_snapshot_no_alert(self):
        df = _make_df(n=2)
        df["Snapshot_TS"] = pd.Timestamp.now()
        report = run_integrity_checks(df, "fresh_snap")
        stale_alerts = [a for a in report.alerts if a.column == "Snapshot_TS"]
        assert len(stale_alerts) == 0

    def test_stale_snapshot_triggers_warning(self):
        df = _make_df(n=2)
        df["Snapshot_TS"] = pd.Timestamp.now() - pd.Timedelta(hours=48)
        report = run_integrity_checks(df, "stale_snap")
        stale_alerts = [a for a in report.alerts if a.column == "Snapshot_TS"]
        assert len(stale_alerts) == 1
        assert "stale" in stale_alerts[0].message


class TestLogReport:
    def test_healthy_log_returns_string(self):
        df = _make_df()
        report = run_integrity_checks(df, "log_test")
        summary = log_report(report)
        assert "HEALTHY" in summary
        assert "All checks passed" in summary

    def test_degraded_log_shows_warnings(self):
        df = _make_df(n=10)
        df.loc[:5, "rsi_14"] = np.nan
        report = run_integrity_checks(df, "deg_test")
        summary = log_report(report)
        assert "DEGRADED" in summary
        assert "rsi_14" in summary

    def test_critical_log_shows_errors(self):
        df = _make_df()
        df.loc[0, "Action"] = None
        report = run_integrity_checks(df, "crit_test")
        summary = log_report(report)
        assert "CRITICAL" in summary
        assert "NULL" in summary


class TestOptionRequiredColumns:
    def test_dte_null_on_option_triggers_error(self):
        df = _make_df(n=5)
        df["AssetType"] = "OPTION"
        df.loc[0, "DTE"] = np.nan
        report = run_integrity_checks(df, "opt_dte")
        alerts = [a for a in report.alerts if a.column == "DTE" and "OPTION" in a.message]
        assert len(alerts) == 1
        assert alerts[0].severity == "ERROR"

    def test_dte_null_on_stock_no_error(self):
        """Stock rows legitimately have no DTE — should NOT fire an error."""
        df = _make_df(n=5)
        df["AssetType"] = "STOCK"
        df["DTE"] = np.nan
        report = run_integrity_checks(df, "stock_dte")
        # Should not have option-specific DTE errors
        dte_errors = [a for a in report.alerts if a.column == "DTE" and "OPTION" in a.message]
        assert len(dte_errors) == 0

    def test_strike_null_on_option_triggers_error(self):
        df = _make_df(n=3)
        df["AssetType"] = "OPTION"
        df.loc[0, "Strike"] = np.nan
        report = run_integrity_checks(df, "opt_strike")
        alerts = [a for a in report.alerts if a.column == "Strike" and "OPTION" in a.message]
        assert len(alerts) == 1


class TestIncomeStockIV:
    def test_bw_stock_no_iv_triggers_error(self):
        """BUY_WRITE stock leg with ALL IV columns NULL — the EOSE bug."""
        df = _make_df(n=3)
        df["AssetType"] = "STOCK"
        df["Strategy"] = "BUY_WRITE"
        df["IV_Contract"] = np.nan
        df["IV_Underlying_30D"] = np.nan
        df["IV_Now"] = np.nan
        df["IV_30D"] = np.nan
        report = run_integrity_checks(df, "bw_iv")
        iv_alerts = [a for a in report.alerts if a.column == "IV_recovery_blind"]
        assert len(iv_alerts) == 1
        assert iv_alerts[0].severity == "ERROR"
        assert "recovery" in iv_alerts[0].message.lower()

    def test_bw_stock_with_iv_now_no_alert(self):
        df = _make_df(n=3)
        df["AssetType"] = "STOCK"
        df["Strategy"] = "BUY_WRITE"
        df["IV_Contract"] = np.nan
        df["IV_Underlying_30D"] = np.nan
        df["IV_Now"] = 0.50          # at least one IV source present
        df["IV_30D"] = np.nan
        report = run_integrity_checks(df, "bw_iv_ok")
        iv_alerts = [a for a in report.alerts if a.column == "IV_recovery_blind"]
        assert len(iv_alerts) == 0

    def test_stock_only_no_iv_no_alert(self):
        """STOCK_ONLY strategy doesn't need IV for recovery — no alert."""
        df = _make_df(n=3)
        df["AssetType"] = "STOCK"
        df["Strategy"] = "STOCK_ONLY"
        df["IV_Contract"] = np.nan
        df["IV_Underlying_30D"] = np.nan
        df["IV_Now"] = np.nan
        df["IV_30D"] = np.nan
        report = run_integrity_checks(df, "so_iv")
        iv_alerts = [a for a in report.alerts if a.column == "IV_recovery_blind"]
        assert len(iv_alerts) == 0

    def test_cc_stock_no_iv_triggers_error(self):
        df = _make_df(n=2)
        df["AssetType"] = "STOCK"
        df["Strategy"] = "COVERED_CALL"
        df["IV_Contract"] = np.nan
        df["IV_Underlying_30D"] = np.nan
        df["IV_Now"] = np.nan
        df["IV_30D"] = np.nan
        report = run_integrity_checks(df, "cc_iv")
        iv_alerts = [a for a in report.alerts if a.column == "IV_recovery_blind"]
        assert len(iv_alerts) == 1


class TestGateInputNan:
    """_check_gate_input_nan: flag NaN in columns used by doctrine gate comparisons."""

    def test_clean_df_no_gate_nan_alert(self):
        df = _make_df()
        report = run_integrity_checks(df, "gate_clean")
        gate_alerts = [a for a in report.alerts if a.category == "GATE_NAN"]
        assert len(gate_alerts) == 0

    def test_nan_delta_triggers_gate_nan_warning(self):
        df = _make_df(n=5)
        df.loc[0, "Delta"] = np.nan
        report = run_integrity_checks(df, "gate_delta")
        gate_alerts = [a for a in report.alerts if a.category == "GATE_NAN"]
        assert len(gate_alerts) == 1
        assert gate_alerts[0].severity == "WARNING"
        assert "Delta" in gate_alerts[0].message

    def test_multiple_nan_gate_cols_single_alert(self):
        """Multiple gate-input columns with NaN → single WARNING listing all."""
        df = _make_df(n=5)
        df.loc[0, "Delta"] = np.nan
        df.loc[1, "DTE"] = np.nan
        df.loc[2, "IV_Rank"] = np.nan
        report = run_integrity_checks(df, "gate_multi")
        gate_alerts = [a for a in report.alerts if a.category == "GATE_NAN"]
        assert len(gate_alerts) == 1
        assert "Delta" in gate_alerts[0].message
        assert "DTE" in gate_alerts[0].message
        assert "IV_Rank" in gate_alerts[0].message

    def test_gate_nan_populates_null_rates(self):
        df = _make_df(n=10)
        df.loc[:1, "Basis"] = np.nan  # 2/10 = 20%
        report = run_integrity_checks(df, "gate_rates")
        assert "gate_Basis" in report.null_rates
        assert report.null_rates["gate_Basis"] == 20.0


class TestPriceStaleness:
    """_is_price_stale: timezone mismatch handling between Snapshot_TS and Price_TS."""

    def test_naive_vs_utc_aware_not_stale(self):
        """Naive Snapshot_TS vs UTC-aware Price_TS from same moment → NOT stale."""
        from core.management.data_integrity_monitor import _is_price_stale
        from datetime import datetime, timezone
        naive_now = pd.Timestamp.now()  # tz-naive
        utc_now = datetime.now(tz=timezone.utc).isoformat()  # tz-aware string
        assert _is_price_stale(utc_now, naive_now) is False

    def test_naive_vs_naive_not_stale(self):
        """Both naive timestamps from same moment → NOT stale."""
        from core.management.data_integrity_monitor import _is_price_stale
        now = pd.Timestamp.now()
        assert _is_price_stale(now, now) is False

    def test_actually_stale_detected(self):
        """Price 48h old vs current snapshot → stale."""
        from core.management.data_integrity_monitor import _is_price_stale
        now = pd.Timestamp.now()
        old = now - pd.Timedelta(hours=48)
        assert _is_price_stale(old, now) is True

    def test_none_price_ts_is_stale(self):
        """None Price_TS → stale."""
        from core.management.data_integrity_monitor import _is_price_stale
        assert _is_price_stale(None, pd.Timestamp.now()) is True

    def test_utc_aware_vs_naive_recent(self):
        """UTC-aware Price_TS 5 min before naive Snapshot_TS → NOT stale."""
        from core.management.data_integrity_monitor import _is_price_stale
        from datetime import datetime, timezone, timedelta
        utc_price = (datetime.now(tz=timezone.utc) - timedelta(minutes=5)).isoformat()
        naive_snap = pd.Timestamp.now()
        assert _is_price_stale(utc_price, naive_snap) is False
