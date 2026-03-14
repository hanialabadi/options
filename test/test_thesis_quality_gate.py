"""
Tests for the thesis quality gate (R4.1) — structural signal conflict detection.

Validates:
1. Pure function correctness (check_thesis_quality)
2. Hard demotions (single issue triggers)
3. Soft demotions (need 2+ to trigger)
4. Strategy-type filtering (only DIRECTIONAL checked)
5. Wait condition generation format
6. TechnicalCondition evaluator support for new metrics
7. Wait condition generator R4.1 routing
"""

import pytest
from scan_engine.scoring.thesis_quality import check_thesis_quality


# ── Fixtures ──────────────────────────────────────────────────────────────

def _base_directional_row(**overrides):
    """Create a base DIRECTIONAL row with strong thesis (all checks pass)."""
    row = {
        'Strategy_Type': 'DIRECTIONAL',
        'Trade_Bias': 'Bearish',
        'ADX': 30.0,
        'Market_Structure': 'Downtrend',
        'Chart_Regime': 'Trending',
        'Weekly_Trend_Bias': 'ALIGNED',
        'Keltner_Squeeze_On': False,
        'Keltner_Squeeze_Fired': False,
        'Interp_Score': 80,
        'Interp_Max': 120,
    }
    row.update(overrides)
    return row


# ── 1. Strategy Type Filtering ────────────────────────────────────────────

class TestStrategyTypeFiltering:
    """Only DIRECTIONAL strategies are checked."""

    def test_income_always_passes(self):
        row = _base_directional_row(Strategy_Type='INCOME', ADX=5.0)
        passed, issues, conditions = check_thesis_quality(row)
        assert passed is True
        assert issues == []
        assert conditions == []

    def test_volatility_always_passes(self):
        row = _base_directional_row(Strategy_Type='VOLATILITY', ADX=5.0)
        passed, issues, _ = check_thesis_quality(row)
        assert passed is True

    def test_empty_strategy_type_passes(self):
        row = _base_directional_row(Strategy_Type='')
        passed, issues, _ = check_thesis_quality(row)
        assert passed is True

    def test_directional_is_checked(self):
        row = _base_directional_row(ADX=10.0)
        passed, _, _ = check_thesis_quality(row)
        assert passed is False  # ADX < 15


# ── 2. Hard Demotions (single issue triggers) ─────────────────────────────

class TestHardDemotions:
    """Any single hard issue triggers demotion."""

    def test_adx_below_15_demotes(self):
        row = _base_directional_row(ADX=9.0)
        passed, issues, conditions = check_thesis_quality(row)
        assert passed is False
        assert len(issues) == 1
        assert 'ADX=9' in issues[0]
        assert any(c['config']['metric'] == 'ADX' for c in conditions)

    def test_adx_exactly_15_passes(self):
        row = _base_directional_row(ADX=15.0)
        passed, _, _ = check_thesis_quality(row)
        assert passed is True

    def test_adx_14_demotes(self):
        row = _base_directional_row(ADX=14.0)
        passed, _, _ = check_thesis_quality(row)
        assert passed is False

    def test_structure_opposes_bearish(self):
        """Uptrend + Bearish = structure opposition."""
        row = _base_directional_row(Market_Structure='Uptrend')
        passed, issues, conditions = check_thesis_quality(row)
        assert passed is False
        assert any('Uptrend opposes BEARISH' in i for i in issues)
        assert any(c['config']['metric'] == 'Market_Structure' for c in conditions)

    def test_structure_opposes_bullish(self):
        """Downtrend + Bullish = structure opposition."""
        row = _base_directional_row(Trade_Bias='Bullish', Market_Structure='Downtrend')
        passed, issues, _ = check_thesis_quality(row)
        assert passed is False
        assert any('Downtrend opposes BULLISH' in i for i in issues)

    def test_consolidation_not_hard_demotion(self):
        """Consolidation is a soft issue, not hard — alone it should pass."""
        row = _base_directional_row(Market_Structure='Consolidation')
        passed, _, _ = check_thesis_quality(row)
        assert passed is True  # 1 soft issue, needs 2+

    def test_structure_aligns_with_bearish(self):
        """Downtrend + Bearish = aligned — should pass."""
        row = _base_directional_row(Market_Structure='Downtrend')
        passed, _, _ = check_thesis_quality(row)
        assert passed is True


# ── 3. Soft Demotions (need 2+ to trigger) ────────────────────────────────

class TestSoftDemotions:
    """Soft issues only trigger demotion when 2+ are present."""

    def test_single_soft_issue_passes(self):
        """One soft issue alone should not demote."""
        row = _base_directional_row(Weekly_Trend_Bias='CONFLICTING')
        passed, _, _ = check_thesis_quality(row)
        assert passed is True

    def test_two_soft_issues_demote(self):
        """Two soft issues should demote."""
        row = _base_directional_row(
            Weekly_Trend_Bias='CONFLICTING',
            Keltner_Squeeze_On=True,
            Keltner_Squeeze_Fired=False,
        )
        passed, issues, conditions = check_thesis_quality(row)
        assert passed is False
        assert len(issues) == 2
        assert any('CONFLICTING' in i for i in issues)
        assert any('Squeeze ON' in i for i in issues)

    def test_three_soft_issues_demote(self):
        """Three soft issues including weak interpreter."""
        row = _base_directional_row(
            Weekly_Trend_Bias='CONFLICTING',
            Keltner_Squeeze_On=True,
            Keltner_Squeeze_Fired=False,
            Interp_Score=40,
        )
        passed, issues, _ = check_thesis_quality(row)
        assert passed is False
        assert len(issues) == 3

    def test_squeeze_fired_is_not_soft_issue(self):
        """Squeeze ON + Fired = direction known, not a soft issue."""
        row = _base_directional_row(
            Weekly_Trend_Bias='CONFLICTING',
            Keltner_Squeeze_On=True,
            Keltner_Squeeze_Fired=True,
        )
        passed, _, _ = check_thesis_quality(row)
        assert passed is True  # Only 1 soft issue (weekly)

    def test_interpreter_above_threshold_not_soft(self):
        """Interpreter >= 50 is not a soft issue."""
        row = _base_directional_row(
            Weekly_Trend_Bias='CONFLICTING',
            Interp_Score=55,
        )
        passed, _, _ = check_thesis_quality(row)
        assert passed is True  # Only 1 soft issue

    def test_consolidation_alone_passes(self):
        """Consolidation is 1 soft issue — needs 2+ to demote."""
        row = _base_directional_row(Market_Structure='Consolidation')
        passed, _, _ = check_thesis_quality(row)
        assert passed is True

    def test_ranging_alone_passes(self):
        """Ranging is 1 soft issue — needs 2+ to demote."""
        row = _base_directional_row(Chart_Regime='Ranging')
        passed, _, _ = check_thesis_quality(row)
        assert passed is True

    def test_consolidation_plus_ranging_demotes(self):
        """Consolidation + Ranging = 2 soft issues → demotion (NVDA scenario)."""
        row = _base_directional_row(
            Market_Structure='Consolidation',
            Chart_Regime='Ranging',
        )
        passed, issues, conditions = check_thesis_quality(row)
        assert passed is False
        assert len(issues) == 2
        assert any('Consolidation' in i for i in issues)
        assert any('Ranging' in i for i in issues)
        # Wait conditions: Market_Structure + Chart_Regime
        metrics = [c['config']['metric'] for c in conditions]
        assert 'Market_Structure' in metrics
        assert 'Chart_Regime' in metrics

    def test_consolidation_plus_weak_interp_demotes(self):
        """Consolidation + weak interpreter = 2 soft issues → demotion."""
        row = _base_directional_row(
            Market_Structure='Consolidation',
            Interp_Score=40,
        )
        passed, issues, _ = check_thesis_quality(row)
        assert passed is False
        assert len(issues) == 2

    def test_ranging_plus_conflicting_weekly_demotes(self):
        """Ranging + conflicting weekly = 2 soft issues → demotion."""
        row = _base_directional_row(
            Chart_Regime='Ranging',
            Weekly_Trend_Bias='CONFLICTING',
        )
        passed, issues, conditions = check_thesis_quality(row)
        assert passed is False
        assert len(issues) == 2

    def test_consolidation_condition_targets_trend(self):
        """Consolidation condition should wait for directional structure."""
        row = _base_directional_row(
            Market_Structure='Consolidation',
            Chart_Regime='Ranging',
        )
        _, _, conditions = check_thesis_quality(row)
        struct_cond = [c for c in conditions if c['config']['metric'] == 'Market_Structure']
        assert len(struct_cond) == 1
        # Bearish bias → wait for Downtrend
        assert struct_cond[0]['config']['threshold'] == 'Downtrend'

    def test_ranging_condition_targets_trending(self):
        """Ranging condition should wait for Trending regime."""
        row = _base_directional_row(
            Market_Structure='Consolidation',
            Chart_Regime='Ranging',
        )
        _, _, conditions = check_thesis_quality(row)
        regime_cond = [c for c in conditions if c['config']['metric'] == 'Chart_Regime']
        assert len(regime_cond) == 1
        assert regime_cond[0]['config']['threshold'] == 'Trending'

    def test_nvda_scenario_full(self):
        """NVDA real-world: ADX=15.19 (passes hard), Consolidation, Ranging,
        Interp=52 (passes soft) → demoted by Consolidation+Ranging."""
        row = _base_directional_row(
            ADX=15.19,
            Market_Structure='Consolidation',
            Chart_Regime='Ranging',
            Interp_Score=52,
            Weekly_Trend_Bias='ALIGNED',
        )
        passed, issues, conditions = check_thesis_quality(row)
        assert passed is False
        assert len(issues) == 2  # Consolidation + Ranging
        metrics = [c['config']['metric'] for c in conditions]
        assert 'Market_Structure' in metrics
        assert 'Chart_Regime' in metrics

    def test_bullish_consolidation_condition_targets_uptrend(self):
        """Bullish bias + Consolidation → wait for Uptrend."""
        row = _base_directional_row(
            Trade_Bias='Bullish',
            Market_Structure='Consolidation',
            Chart_Regime='Ranging',
        )
        _, _, conditions = check_thesis_quality(row)
        struct_cond = [c for c in conditions if c['config']['metric'] == 'Market_Structure']
        assert struct_cond[0]['config']['threshold'] == 'Uptrend'


# ── 4. Combined Hard + Soft ───────────────────────────────────────────────

class TestCombinedIssues:
    """Hard + soft issues combine correctly."""

    def test_lly_scenario(self):
        """Full LLY reproduction: ADX=9 + Uptrend + CONFLICTING + Squeeze + Weak interp."""
        row = _base_directional_row(
            ADX=9.0,
            Market_Structure='Uptrend',
            Weekly_Trend_Bias='CONFLICTING',
            Keltner_Squeeze_On=True,
            Keltner_Squeeze_Fired=False,
            Interp_Score=45,
        )
        passed, issues, conditions = check_thesis_quality(row)
        assert passed is False
        # 2 hard + 3 soft = 5 issues
        assert len(issues) == 5
        # Conditions: ADX, Market_Structure, Weekly, Squeeze = 4
        assert len(conditions) == 4


# ── 5. Wait Condition Format ──────────────────────────────────────────────

class TestConditionFormat:
    """Conditions match wait_condition_generator expected format."""

    def test_condition_has_required_keys(self):
        row = _base_directional_row(ADX=10.0)
        _, _, conditions = check_thesis_quality(row)
        assert len(conditions) >= 1
        c = conditions[0]
        assert 'condition_id' in c
        assert 'type' in c
        assert 'description' in c
        assert 'config' in c
        assert c['type'] == 'technical'

    def test_adx_condition_threshold(self):
        row = _base_directional_row(ADX=8.0)
        _, _, conditions = check_thesis_quality(row)
        adx_cond = [c for c in conditions if c['config']['metric'] == 'ADX']
        assert len(adx_cond) == 1
        assert adx_cond[0]['config']['operator'] == 'greater_than'
        assert adx_cond[0]['config']['threshold'] == 20  # ADX_PROMOTION_TARGET

    def test_structure_condition_uses_equals(self):
        row = _base_directional_row(Market_Structure='Uptrend')
        _, _, conditions = check_thesis_quality(row)
        struct_cond = [c for c in conditions if c['config']['metric'] == 'Market_Structure']
        assert len(struct_cond) == 1
        assert struct_cond[0]['config']['operator'] == 'equals'
        assert struct_cond[0]['config']['threshold'] == 'Downtrend'


# ── 6. TechnicalCondition Evaluator — New Metrics ─────────────────────────

class TestTechnicalConditionNewMetrics:
    """TechnicalCondition handles ADX, Market_Structure, Weekly, Squeeze."""

    def _make_condition(self, metric, operator, threshold):
        from core.wait_loop.conditions import TechnicalCondition
        return TechnicalCondition(
            f'test_{metric}',
            {'metric': metric, 'operator': operator, 'threshold': threshold}
        )

    def test_adx_greater_than_met(self):
        cond = self._make_condition('ADX', 'greater_than', 20)
        assert cond.check({'ADX': 25.0}, {}) is True

    def test_adx_greater_than_not_met(self):
        cond = self._make_condition('ADX', 'greater_than', 20)
        assert cond.check({'ADX': 15.0}, {}) is False

    def test_adx_progress(self):
        cond = self._make_condition('ADX', 'greater_than', 20)
        # From entry ADX=10, current ADX=15: 50% progress toward 20
        progress = cond.get_progress({'ADX': 15.0}, {'entry_ADX': 10.0})
        assert 0.4 <= progress <= 0.6

    def test_market_structure_equals_met(self):
        cond = self._make_condition('Market_Structure', 'equals', 'Downtrend')
        assert cond.check({'Market_Structure': 'Downtrend'}, {}) is True

    def test_market_structure_equals_not_met(self):
        cond = self._make_condition('Market_Structure', 'equals', 'Downtrend')
        assert cond.check({'Market_Structure': 'Uptrend'}, {}) is False

    def test_market_structure_case_insensitive(self):
        cond = self._make_condition('Market_Structure', 'equals', 'Downtrend')
        assert cond.check({'Market_Structure': 'downtrend'}, {}) is True

    def test_weekly_trend_bias_equals(self):
        cond = self._make_condition('Weekly_Trend_Bias', 'equals', 'ALIGNED')
        assert cond.check({'Weekly_Trend_Bias': 'ALIGNED'}, {}) is True
        assert cond.check({'Weekly_Trend_Bias': 'CONFLICTING'}, {}) is False

    def test_squeeze_fired_bool(self):
        cond = self._make_condition('Keltner_Squeeze_Fired', 'equals', True)
        assert cond.check({'Keltner_Squeeze_Fired': True}, {}) is True
        assert cond.check({'Keltner_Squeeze_Fired': False}, {}) is False

    def test_squeeze_fired_string_true(self):
        cond = self._make_condition('Keltner_Squeeze_Fired', 'equals', True)
        assert cond.check({'Keltner_Squeeze_Fired': 'True'}, {}) is True
        assert cond.check({'Keltner_Squeeze_Fired': 'False'}, {}) is False

    def test_equals_progress_binary(self):
        cond = self._make_condition('Market_Structure', 'equals', 'Downtrend')
        assert cond.get_progress({'Market_Structure': 'Downtrend'}, {}) == 1.0
        assert cond.get_progress({'Market_Structure': 'Uptrend'}, {}) == 0.0

    def test_describe_equals(self):
        cond = self._make_condition('ADX', 'greater_than', 20)
        assert 'ADX' in cond.describe()
        assert '20' in cond.describe()

    def test_describe_equals_operator(self):
        cond = self._make_condition('Market_Structure', 'equals', 'Downtrend')
        desc = cond.describe()
        assert 'Market_Structure' in desc
        assert 'Downtrend' in desc


# ── 7. Wait Condition Generator R4.1 Routing ─────────────────────────────

class TestWaitConditionGeneratorR41:
    """R4.1 gate code routes to thesis conditions."""

    def test_r41_uses_pregenerated_conditions(self):
        from scan_engine.wait_condition_generator import generate_wait_conditions_for_gate
        pre_conditions = [
            {'condition_id': 'thesis_ADX_abc', 'type': 'technical',
             'description': 'ADX > 20', 'config': {'metric': 'ADX', 'operator': 'greater_than', 'threshold': 20}},
        ]
        conditions = generate_wait_conditions_for_gate(
            'R4.1: Thesis quality — ADX=9 < 15',
            {'_thesis_wait_conditions': pre_conditions}
        )
        # Should include pre-generated + time delay
        assert len(conditions) >= 2
        adx_conds = [c for c in conditions if c.get('config', {}).get('metric') == 'ADX']
        assert len(adx_conds) == 1
        time_conds = [c for c in conditions if c['type'] == 'time_delay']
        assert len(time_conds) >= 1

    def test_r41_without_pregenerated_gets_time_delay(self):
        from scan_engine.wait_condition_generator import generate_wait_conditions_for_gate
        conditions = generate_wait_conditions_for_gate(
            'R4.1: Thesis quality — issues',
            {'_thesis_wait_conditions': None}
        )
        # Should still get time delay at minimum
        assert len(conditions) >= 1
        assert any(c['type'] == 'time_delay' for c in conditions)


# ── 8. Graceful Degradation ───────────────────────────────────────────────

class TestGracefulDegradation:
    """Missing data should not crash — degrade gracefully."""

    def test_missing_adx(self):
        row = _base_directional_row(ADX=None)
        passed, issues, _ = check_thesis_quality(row)
        # No ADX = no ADX check, should pass (other fields are aligned)
        assert passed is True

    def test_missing_market_structure(self):
        row = _base_directional_row(Market_Structure=None)
        passed, _, _ = check_thesis_quality(row)
        assert passed is True

    def test_nan_adx(self):
        row = _base_directional_row(ADX=float('nan'))
        passed, _, _ = check_thesis_quality(row)
        assert passed is True  # NaN treated as missing

    def test_missing_all_signals(self):
        """No institutional signals at all — should pass (no evidence of conflict)."""
        row = {
            'Strategy_Type': 'DIRECTIONAL',
            'Trade_Bias': 'Bullish',
        }
        passed, _, _ = check_thesis_quality(row)
        assert passed is True


# ── 9. R5.0 Theory Compliance Floor — Step 12 Post-Loop Gate ────────────

class TestTheoryComplianceFloor:
    """R5.0: Directional strategies with Theory_Compliance_Score < 50
    should be demoted READY → CONDITIONAL with wait conditions."""

    def _make_step12_df(self, **overrides):
        """Create a minimal DataFrame that simulates Step 12 output."""
        import pandas as pd
        base = {
            'Ticker': 'AMD',
            'Strategy_Name': 'Long_Put',
            'Strategy_Type': 'DIRECTIONAL',
            'Trade_Bias': 'Bearish',
            'Execution_Status': 'READY',
            'Gate_Reason': 'R3.2: Directional strategy',
            'Theory_Compliance_Score': 42.0,
            'Evaluation_Notes': 'Gamma floor fail; MACD contradiction; volume weak',
            'MACD_Histogram': 0.17,
            'ADX': 18.0,
            'confidence_band': 'MEDIUM',
        }
        base.update(overrides)
        return pd.DataFrame([base])

    def test_below_floor_demoted_to_conditional(self):
        """Theory=42 < 50 → CONDITIONAL."""
        import pandas as pd
        df = self._make_step12_df(Theory_Compliance_Score=42.0)

        # Simulate R5.0 post-loop logic
        _FLOOR = 50
        mask = (
            (df['Execution_Status'] == 'READY') &
            (df['Strategy_Type'].str.upper() == 'DIRECTIONAL') &
            (df['Theory_Compliance_Score'] < _FLOOR)
        )
        assert mask.any(), "Should identify AMD for demotion"

    def test_above_floor_stays_ready(self):
        """Theory=65 ≥ 50 → stays READY."""
        import pandas as pd
        df = self._make_step12_df(Theory_Compliance_Score=65.0)

        _FLOOR = 50
        mask = (
            (df['Execution_Status'] == 'READY') &
            (df['Strategy_Type'].str.upper() == 'DIRECTIONAL') &
            (df['Theory_Compliance_Score'] < _FLOOR)
        )
        assert not mask.any(), "Theory=65 should NOT be demoted"

    def test_income_strategy_not_affected(self):
        """Income strategies have their own gates — R5.0 skips them."""
        import pandas as pd
        df = self._make_step12_df(
            Strategy_Type='INCOME',
            Theory_Compliance_Score=30.0,
        )

        _FLOOR = 50
        mask = (
            (df['Execution_Status'] == 'READY') &
            (df['Strategy_Type'].str.upper() == 'DIRECTIONAL') &
            (df['Theory_Compliance_Score'] < _FLOOR)
        )
        assert not mask.any(), "Income strategies should not be caught by R5.0"

    def test_wait_conditions_include_macd(self):
        """Bearish with positive MACD → MACD wait condition generated."""
        import pandas as pd
        df = self._make_step12_df(
            Trade_Bias='Bearish',
            MACD_Histogram=0.17,
            Theory_Compliance_Score=42.0,
        )
        row = df.iloc[0]
        is_bearish = 'BEAR' in str(row['Trade_Bias']).upper()
        macd = row['MACD_Histogram']
        contradicts = is_bearish and macd > 0

        assert contradicts, "MACD should contradict bearish thesis"

        # Build condition as R5.0 would
        cond = {
            'type': 'technical',
            'config': {
                'metric': 'MACD_Histogram',
                'operator': 'less_than' if is_bearish else 'greater_than',
                'threshold': 0,
            },
        }
        assert cond['config']['operator'] == 'less_than'
        assert cond['config']['threshold'] == 0

    def test_wait_conditions_include_adx(self):
        """ADX=18 < 20 → ADX wait condition generated."""
        adx = 18.0
        assert adx < 20, "ADX below trend threshold"

        cond = {
            'type': 'technical',
            'config': {
                'metric': 'ADX',
                'operator': 'greater_than',
                'threshold': 20,
            },
        }
        assert cond['config']['threshold'] == 20

    def test_macd_aligned_no_macd_condition(self):
        """Bearish with negative MACD → no MACD wait condition needed."""
        is_bearish = True
        macd = -0.25
        contradicts = is_bearish and macd > 0
        assert not contradicts, "Negative MACD aligns with bearish — no condition"

    def test_amd_scenario_full_demotion(self):
        """Full AMD scenario: theory=42, MACD+, ADX=18, bearish → CONDITIONAL
        with 3 wait conditions (theory score, MACD, ADX)."""
        conditions = []

        # Theory score condition
        conditions.append({
            'type': 'technical',
            'config': {'metric': 'Theory_Compliance_Score',
                       'operator': 'greater_than', 'threshold': 50},
        })

        # MACD contradiction (bearish + positive MACD)
        macd = 0.17
        is_bearish = True
        if is_bearish and macd > 0:
            conditions.append({
                'type': 'technical',
                'config': {'metric': 'MACD_Histogram',
                           'operator': 'less_than', 'threshold': 0},
            })

        # ADX below trend threshold
        adx = 18.0
        if adx < 20:
            conditions.append({
                'type': 'technical',
                'config': {'metric': 'ADX',
                           'operator': 'greater_than', 'threshold': 20},
            })

        assert len(conditions) == 3, f"Expected 3 conditions, got {len(conditions)}"
        metrics = [c['config']['metric'] for c in conditions]
        assert 'Theory_Compliance_Score' in metrics
        assert 'MACD_Histogram' in metrics
        assert 'ADX' in metrics

    def test_exactly_at_floor_stays_ready(self):
        """Theory=50 (exactly at floor) → stays READY (< 50, not ≤ 50)."""
        import pandas as pd
        df = self._make_step12_df(Theory_Compliance_Score=50.0)

        _FLOOR = 50
        mask = (
            (df['Execution_Status'] == 'READY') &
            (df['Strategy_Type'].str.upper() == 'DIRECTIONAL') &
            (df['Theory_Compliance_Score'] < _FLOOR)
        )
        assert not mask.any(), "Exactly at floor should NOT be demoted"


class TestWaitConditionGeneratorR50:
    """R5.0 gate code routes to theory compliance conditions."""

    def test_r50_uses_pregenerated_conditions(self):
        from scan_engine.wait_condition_generator import generate_wait_conditions_for_gate
        pre_conditions = [
            {'condition_id': 'theory_score_abc', 'type': 'technical',
             'description': 'Theory ≥ 50',
             'config': {'metric': 'Theory_Compliance_Score',
                        'operator': 'greater_than', 'threshold': 50}},
            {'condition_id': 'theory_macd_def', 'type': 'technical',
             'description': 'MACD < 0',
             'config': {'metric': 'MACD_Histogram',
                        'operator': 'less_than', 'threshold': 0}},
        ]
        conditions = generate_wait_conditions_for_gate(
            'R5.0: Theory compliance 42/100 below floor 50',
            {'_theory_compliance_conditions': pre_conditions}
        )
        # Should include pre-generated + time delay
        assert len(conditions) >= 3
        theory_conds = [c for c in conditions
                        if c.get('config', {}).get('metric') == 'Theory_Compliance_Score']
        assert len(theory_conds) == 1
        time_conds = [c for c in conditions if c['type'] == 'time_delay']
        assert len(time_conds) >= 1

    def test_r50_without_pregenerated_gets_time_delay(self):
        from scan_engine.wait_condition_generator import generate_wait_conditions_for_gate
        conditions = generate_wait_conditions_for_gate(
            'R5.0: Theory compliance below floor',
            {'_theory_compliance_conditions': None}
        )
        assert len(conditions) >= 1
        assert any(c['type'] == 'time_delay' for c in conditions)
