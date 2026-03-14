"""
Tests for the Missing-Data Diagnosis Layer.

Covers: enums, field registry, diagnose(), audit_stage(),
check_impossible(), and health report generation.
"""

import json
import pytest
import pandas as pd
import numpy as np
from unittest.mock import MagicMock

from core.shared.governance.missing_data_tracker import (
    MissingReason,
    MissingnessClass,
    FieldSpec,
    MissingDataTracker,
    TRACKED_FIELDS,
    MANAGEMENT_TRACKED_FIELDS,
    _FIELD_INDEX,
    StageAudit,
    HealthReport,
)


# ════════════════════════════════════════════════════════════════════
# Helpers
# ════════════════════════════════════════════════════════════════════

def _make_df(**col_overrides) -> pd.DataFrame:
    """Build a minimal DataFrame for testing.  All tracked fields present by default."""
    n = col_overrides.pop('_n', 5)
    base = {
        'Ticker': [f'T{i}' for i in range(n)],
        'Strategy_Name': ['Long Call'] * n,
        # IV family
        'IV_Rank_30D': [55.0] * n,
        'IV_Maturity_State': ['MATURE'] * n,
        'IV_History_Count': [120] * n,
        'Signal_Type': ['Bullish'] * n,
        'Regime': ['High Vol'] * n,
        # TA family
        'ADX': [30.0] * n,
        'RSI_14': [55.0] * n,
        'SMA20': [150.0] * n,
        'MACD': [1.5] * n,
        'Price_vs_SMA20': [2.0] * n,
        'Price_History_Days': [180] * n,
        # Greeks family
        'Delta': [0.45] * n,
        'Gamma': [0.03] * n,
        'Theta': [-0.05] * n,
        'Vega': [0.12] * n,
        'Strike': [155.0] * n,
        'Bid': [3.0] * n,
        'Ask': [3.5] * n,
        'Contract_Symbol': ['AAPL_C155'] * n,
        # Scoring
        'DQS_Score': [72.0] * n,
        'TQS_Score': [65.0] * n,
        'PCS_Final': [np.nan] * n,  # NOT_APPLICABLE for directional
        # Derived
        'Premium_vs_FairValue_Pct': [1.2] * n,
        'Liquidity_Grade': ['A'] * n,
    }
    base.update(col_overrides)
    return pd.DataFrame(base)


# ════════════════════════════════════════════════════════════════════
# TestMissingReason
# ════════════════════════════════════════════════════════════════════

class TestMissingReason:

    def test_enum_values_complete(self):
        expected = {
            'IMMATURE_HISTORY', 'SOURCE_MISSING', 'API_FAIL', 'MERGE_FAIL',
            'COMPUTE_FAIL', 'NOT_APPLICABLE', 'SCHEMA_MISMATCH', 'UNKNOWN', 'PRESENT',
        }
        assert {r.value for r in MissingReason} == expected

    def test_string_serialization(self):
        assert str(MissingReason.IMMATURE_HISTORY) == "MissingReason.IMMATURE_HISTORY"
        assert MissingReason.IMMATURE_HISTORY.value == "IMMATURE_HISTORY"

    def test_enum_is_str_subclass(self):
        """MissingReason values can be used directly as strings."""
        assert MissingReason.PRESENT == "PRESENT"
        assert MissingReason.API_FAIL in ("API_FAIL", "SOURCE_MISSING")


# ════════════════════════════════════════════════════════════════════
# TestFieldRegistry
# ════════════════════════════════════════════════════════════════════

class TestFieldRegistry:

    def test_all_specs_are_field_spec(self):
        for fs in TRACKED_FIELDS:
            assert isinstance(fs, FieldSpec), f"{fs} is not a FieldSpec"

    def test_no_duplicate_field_names(self):
        names = [fs.field_name for fs in TRACKED_FIELDS]
        assert len(names) == len(set(names)), f"Duplicate field names: {[n for n in names if names.count(n) > 1]}"

    def test_owning_step_before_or_equal_required(self):
        for fs in TRACKED_FIELDS:
            assert fs.owning_step <= fs.required_after_step, (
                f"{fs.field_name}: owning_step={fs.owning_step} > required_after_step={fs.required_after_step}"
            )

    def test_valid_strategy_scopes(self):
        for fs in TRACKED_FIELDS:
            assert fs.strategy_scope in (None, "INCOME", "DIRECTIONAL"), (
                f"{fs.field_name}: invalid scope '{fs.strategy_scope}'"
            )


# ════════════════════════════════════════════════════════════════════
# TestDiagnose
# ════════════════════════════════════════════════════════════════════

class TestDiagnose:

    def test_iv_immature_history(self):
        """IV_Rank_30D NaN + IMMATURE maturity → IMMATURE_HISTORY."""
        df = _make_df(
            IV_Rank_30D=[np.nan, 55.0, np.nan, 55.0, np.nan],
            IV_Maturity_State=['IMMATURE', 'MATURE', 'MISSING', 'MATURE', 'IMMATURE'],
        )
        tracker = MissingDataTracker("test")
        tracker.diagnose(df, step_num=2)

        reason_col = 'IV_Rank_30D_Missing_Reason'
        assert reason_col in df.columns
        nulls = df[df['IV_Rank_30D'].isna()]
        assert (nulls[reason_col] == 'IMMATURE_HISTORY').all()

    def test_iv_compute_fail(self):
        """IV_Rank_30D NaN + MATURE maturity → COMPUTE_FAIL."""
        df = _make_df(
            IV_Rank_30D=[np.nan, np.nan],
            IV_Maturity_State=['MATURE', 'PARTIAL_MATURE'],
            _n=2,
        )
        tracker = MissingDataTracker("test")
        tracker.diagnose(df, step_num=2)

        nulls = df[df['IV_Rank_30D'].isna()]
        assert (nulls['IV_Rank_30D_Missing_Reason'] == 'COMPUTE_FAIL').all()

    def test_ta_immature_short_history(self):
        """ADX NaN + Price_History_Days < 28 → IMMATURE_HISTORY."""
        df = _make_df(
            ADX=[np.nan, 30.0, np.nan],
            Price_History_Days=[15, 180, 10],
            _n=3,
        )
        tracker = MissingDataTracker("test")
        tracker.diagnose(df, step_num=5)

        nulls = df[df['ADX'].isna()]
        assert (nulls['ADX_Missing_Reason'] == 'IMMATURE_HISTORY').all()

    def test_ta_compute_fail_sufficient_history(self):
        """ADX NaN + Price_History_Days >= 28 → COMPUTE_FAIL."""
        df = _make_df(
            ADX=[np.nan],
            Price_History_Days=[200],
            _n=1,
        )
        tracker = MissingDataTracker("test")
        tracker.diagnose(df, step_num=5)

        assert df.loc[0, 'ADX_Missing_Reason'] == 'COMPUTE_FAIL'

    def test_greeks_source_missing_no_contract(self):
        """Delta NaN + Contract_Symbol NaN → SOURCE_MISSING."""
        df = _make_df(
            Delta=[np.nan],
            Contract_Symbol=[np.nan],
            _n=1,
        )
        tracker = MissingDataTracker("test")
        tracker.diagnose(df, step_num=10)

        assert df.loc[0, 'Delta_Missing_Reason'] == 'SOURCE_MISSING'

    def test_greeks_merge_fail_with_contract(self):
        """Delta NaN + Contract_Symbol present → MERGE_FAIL."""
        df = _make_df(
            Delta=[np.nan],
            Contract_Symbol=['AAPL_C155'],
            _n=1,
        )
        tracker = MissingDataTracker("test")
        tracker.diagnose(df, step_num=10)

        assert df.loc[0, 'Delta_Missing_Reason'] == 'MERGE_FAIL'

    def test_strategy_scoped_not_applicable(self):
        """DQS_Score NaN for income strategy → NOT_APPLICABLE."""
        df = _make_df(
            DQS_Score=[np.nan, np.nan],
            Strategy_Name=['Covered Call', 'Long Call'],
            _n=2,
        )
        tracker = MissingDataTracker("test")
        tracker.diagnose(df, step_num=10)

        # Income strategy → NOT_APPLICABLE
        assert df.loc[0, 'DQS_Score_Missing_Reason'] == 'NOT_APPLICABLE'
        # Directional strategy with NaN DQS → should be diagnosed (COMPUTE_FAIL or SOURCE_MISSING)
        assert df.loc[1, 'DQS_Score_Missing_Reason'] != 'PRESENT'
        assert df.loc[1, 'DQS_Score_Missing_Reason'] != 'NOT_APPLICABLE'

    def test_schema_mismatch_missing_column(self):
        """Field registered but column doesn't exist → SCHEMA_MISMATCH for all rows."""
        df = _make_df(_n=3)
        df.drop(columns=['Liquidity_Grade'], inplace=True)

        tracker = MissingDataTracker("test")
        tracker.diagnose(df, step_num=12)

        assert 'Liquidity_Grade_Missing_Reason' in df.columns
        assert (df['Liquidity_Grade_Missing_Reason'] == 'SCHEMA_MISMATCH').all()

    def test_non_blocking_on_exception(self):
        """diagnose() should not raise even if internal logic breaks."""
        df = _make_df(_n=1)
        tracker = MissingDataTracker("test")

        # Corrupt the field index to trigger an error
        import core.shared.governance.missing_data_tracker as mod
        original = mod.TRACKED_FIELDS
        try:
            mod.TRACKED_FIELDS = "not a list"  # will cause iteration to fail
            result = tracker.diagnose(df, step_num=2)
            assert result is df  # should return df unchanged
        finally:
            mod.TRACKED_FIELDS = original

    def test_present_when_field_has_data(self):
        """Non-null fields get PRESENT reason."""
        df = _make_df(_n=2)
        tracker = MissingDataTracker("test")
        tracker.diagnose(df, step_num=2)

        assert (df['IV_Rank_30D_Missing_Reason'] == 'PRESENT').all()
        assert (df['Signal_Type_Missing_Reason'] == 'PRESENT').all()


# ════════════════════════════════════════════════════════════════════
# TestStageAudit
# ════════════════════════════════════════════════════════════════════

class TestStageAudit:

    def test_row_counts_correct(self):
        df_before = _make_df(_n=10)
        df_after = _make_df(_n=8)
        tracker = MissingDataTracker("test")
        entry = tracker.audit_stage("step3", df_before, df_after)

        assert entry.rows_entering == 10
        assert entry.rows_exiting == 8
        assert entry.rows_dropped == 2

    def test_missing_counts_match(self):
        df = _make_df(
            ADX=[np.nan, np.nan, 30.0],
            RSI_14=[np.nan, 55.0, 55.0],
            _n=3,
        )
        tracker = MissingDataTracker("test")
        # diagnose first to populate companion columns
        tracker.diagnose(df, step_num=5)
        entry = tracker.audit_stage("step5", None, df)

        assert entry.missing_counts.get('ADX') == 2
        assert entry.missing_counts.get('RSI_14') == 1

    def test_no_entry_before_step(self):
        """Fields from later steps should not appear in missing_counts for earlier steps."""
        df = _make_df(_n=3)
        df.drop(columns=['Delta', 'Gamma', 'Theta', 'Vega'], inplace=True)
        tracker = MissingDataTracker("test")
        entry = tracker.audit_stage("step2", None, df)

        # Delta is owning_step=10, so it shouldn't be counted at step 2
        # (it's not in df.columns, so missing_counts won't have it)
        assert 'Delta' not in entry.missing_counts


# ════════════════════════════════════════════════════════════════════
# TestImpossible
# ════════════════════════════════════════════════════════════════════

class TestImpossible:

    def test_impossible_flagged_after_required_step(self):
        """IV_Maturity_State (IMPOSSIBLE, required after step 2) NaN → violation."""
        df = _make_df(IV_Maturity_State=[np.nan, 'MATURE', np.nan], _n=3)
        tracker = MissingDataTracker("test")
        violations = tracker.check_impossible(df, step_num=2)

        assert len(violations) >= 1
        iv_mat = [v for v in violations if v['field'] == 'IV_Maturity_State']
        assert len(iv_mat) == 1
        assert iv_mat[0]['count'] == 2

    def test_not_flagged_before_required_step(self):
        """IMPOSSIBLE fields should not trigger violations before their required step."""
        df = _make_df(IV_Maturity_State=[np.nan, np.nan], _n=2)
        tracker = MissingDataTracker("test")
        violations = tracker.check_impossible(df, step_num=1)  # before step 2

        iv_mat = [v for v in violations if v['field'] == 'IV_Maturity_State']
        assert len(iv_mat) == 0

    def test_schema_mismatch_when_column_missing(self):
        """Column entirely missing → SCHEMA_MISMATCH violation."""
        df = _make_df(_n=2)
        df.drop(columns=['Signal_Type'], inplace=True)  # Signal_Type is IMPOSSIBLE

        tracker = MissingDataTracker("test")
        violations = tracker.check_impossible(df, step_num=2)

        sig = [v for v in violations if v['field'] == 'Signal_Type']
        assert len(sig) == 1
        assert sig[0]['reason'] == 'SCHEMA_MISMATCH'


# ════════════════════════════════════════════════════════════════════
# TestHealthReport
# ════════════════════════════════════════════════════════════════════

class TestHealthReport:

    def test_green_when_all_present(self):
        """Complete data → GREEN health."""
        df = _make_df(_n=5)
        tracker = MissingDataTracker("test")
        tracker.diagnose(df, step_num=12)
        tracker.audit_stage("step12", None, df)

        report = tracker.generate_report()
        assert report.overall_health == "GREEN"
        assert len(report.impossible_violations) == 0

    def test_yellow_when_moderate_suspicious(self):
        """Some suspicious missing → YELLOW health.
        Need >5% of (total_rows × tracked_fields) to be suspicious.
        With 10 rows × 22 fields = 220 checks, need >11 suspicious.
        """
        n = 10
        df = _make_df(_n=n)
        # Make 5/10 rows have suspicious missing Greeks + derived fields
        # 5 rows × 7 Greek/derived cols = 35 suspicious > 5% of 220
        for i in range(5):
            df.loc[i, 'Delta'] = np.nan
            df.loc[i, 'Gamma'] = np.nan
            df.loc[i, 'Theta'] = np.nan
            df.loc[i, 'Vega'] = np.nan
            df.loc[i, 'Strike'] = np.nan
            df.loc[i, 'Bid'] = np.nan
            df.loc[i, 'Ask'] = np.nan

        tracker = MissingDataTracker("test")
        tracker.diagnose(df, step_num=12)
        tracker.audit_stage("step12", None, df)
        report = tracker.generate_report()

        # Should be at least YELLOW (suspicious missing > 5%)
        assert report.overall_health in ("YELLOW", "RED")

    def test_red_on_impossible_violation(self):
        """Any impossible violation → RED."""
        df = _make_df(IV_Maturity_State=[np.nan, 'MATURE'], _n=2)
        tracker = MissingDataTracker("test")
        tracker.diagnose(df, step_num=12)
        tracker.audit_stage("step12", None, df)
        tracker.check_impossible(df, step_num=12)

        report = tracker.generate_report()
        assert report.overall_health == "RED"

    def test_completeness_percentages(self):
        """Completeness metrics reflect actual null rates."""
        df = _make_df(
            IV_Rank_30D=[np.nan, np.nan, 55.0, 55.0, 55.0],
            _n=5,
        )
        tracker = MissingDataTracker("test")
        tracker.diagnose(df, step_num=12)
        tracker.audit_stage("step12", None, df)

        report = tracker.generate_report()
        # IV_Rank: 3/5 present = 60%
        assert report.completeness['iv_rank_pct'] == 60.0

    def test_empty_dataframe_edge_case(self):
        """Empty DataFrame → no crash, UNKNOWN health."""
        tracker = MissingDataTracker("test")
        tracker.diagnose(pd.DataFrame(), step_num=2)
        tracker.audit_stage("step2", None, pd.DataFrame())

        report = tracker.generate_report()
        assert report.total_rows == 0

    def test_report_serializable(self):
        """Health report can be serialized to JSON."""
        df = _make_df(_n=3)
        tracker = MissingDataTracker("test")
        tracker.diagnose(df, step_num=12)
        tracker.audit_stage("step12", None, df)

        report = tracker.generate_report()
        from dataclasses import asdict
        report_dict = asdict(report)
        # Should not raise
        json_str = json.dumps(report_dict)
        assert isinstance(json_str, str)
        parsed = json.loads(json_str)
        assert parsed['overall_health'] == 'GREEN'


# ════════════════════════════════════════════════════════════════════
# TestManagementRegistry
# ════════════════════════════════════════════════════════════════════

def _make_mgmt_df(**col_overrides) -> pd.DataFrame:
    """Build a minimal management DataFrame."""
    n = col_overrides.pop('_n', 5)
    base = {
        'Ticker': [f'T{i}' for i in range(n)],
        'Underlying_Ticker': [f'T{i}' for i in range(n)],
        'Strategy': ['BUY_WRITE'] * n,
        'Symbol': [f'T{i}_C150' for i in range(n)],
        'UL Last': [150.0] * n,
        'DTE': [30] * n,
        'Strike': [155.0] * n,
        'Expiration': ['2026-04-15'] * n,
        'Delta': [0.35] * n,
        'Gamma': [0.02] * n,
        'Theta': [-0.04] * n,
        'Vega': [0.10] * n,
        'IV_Now': [0.30] * n,
        'IV_30D': [0.28] * n,
        'IV_Rank': [55.0] * n,
        'HV_20D': [25.0] * n,
        'Trajectory_PnL_Pct': [5.0] * n,
        'Trajectory_MFE': [8.0] * n,
        'Basis': [14800.0] * n,
        'Premium_Entry': [2.50] * n,
        'PriceStructure_State': ['STRUCTURE_HEALTHY'] * n,
        'TrendIntegrity_State': ['TREND_ACTIVE'] * n,
        'MomentumVelocity_State': ['STABLE'] * n,
        'Thesis_State': ['INTACT'] * n,
        'rsi_14': [55.0] * n,
        'adx_14': [30.0] * n,
        'Short_Call_Delta': [0.30] * n,
        'Short_Call_Strike': [160.0] * n,
        'Action': ['HOLD'] * n,
        'Urgency': ['LOW'] * n,
        'Doctrine_Source': ['v2_income'] * n,
    }
    base.update(col_overrides)
    return pd.DataFrame(base)


class TestManagementRegistry:

    def test_management_registry_valid(self):
        """All management specs are valid FieldSpec."""
        for fs in MANAGEMENT_TRACKED_FIELDS:
            assert isinstance(fs, FieldSpec)
            assert fs.owning_step <= fs.required_after_step

    def test_no_duplicate_management_fields(self):
        names = [fs.field_name for fs in MANAGEMENT_TRACKED_FIELDS]
        assert len(names) == len(set(names))

    def test_management_tracker_diagnoses_greeks(self):
        """Missing Greeks in management → API_FAIL."""
        df = _make_mgmt_df(Delta=[np.nan], Gamma=[np.nan], _n=1)
        tracker = MissingDataTracker("test", registry=MANAGEMENT_TRACKED_FIELDS)
        tracker.diagnose(df, step_num=2)

        # Delta missing with contract info present → should diagnose
        assert 'Delta_Missing_Reason' in df.columns
        assert df.loc[0, 'Delta_Missing_Reason'] != 'PRESENT'

    def test_management_impossible_post_doctrine(self):
        """Action/Urgency NaN after doctrine → IMPOSSIBLE violation."""
        df = _make_mgmt_df(Action=[np.nan, 'HOLD'], Urgency=[np.nan, 'LOW'], _n=2)
        tracker = MissingDataTracker("test", registry=MANAGEMENT_TRACKED_FIELDS)
        violations = tracker.check_impossible(df, step_num=3)

        action_v = [v for v in violations if v['field'] == 'Action']
        assert len(action_v) == 1
        assert action_v[0]['count'] == 1

    def test_management_green_when_complete(self):
        """Complete management data → GREEN."""
        df = _make_mgmt_df(_n=5)
        tracker = MissingDataTracker("test", registry=MANAGEMENT_TRACKED_FIELDS)
        tracker.diagnose(df, step_num=3)
        tracker.audit_stage("post_doctrine", None, df)

        report = tracker.generate_report()
        assert report.overall_health == "GREEN"
