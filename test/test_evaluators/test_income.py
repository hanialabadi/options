"""
Tests for the income strategy evaluator.
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pytest
import pandas as pd
import numpy as np

from scan_engine.evaluators.income import evaluate_income


def _row(**overrides):
    """Build a minimal valid income row."""
    base = {
        'Strategy_Name': 'Cash-Secured Put',
        'Contract_Status': 'OK',
        'Theta': 0.05,
        'Vega': 0.20,
        'Gamma': -0.03,
        'IVHV_gap_30D': 5.0,
        'RV_IV_Ratio': 0.85,
        'Probability_Of_Profit': 72.0,
        'IV_Percentile': 55.0,
        'Actual_DTE': 35,
        'Trend': 'Bullish',
        'Signal_Type': 'Bullish',
        'Regime': 'Compression',
        'Price_vs_SMA20': 1.5,
    }
    base.update(overrides)
    return pd.Series(base)


class TestIncomeEvaluator:
    def test_strong_setup_valid(self):
        r = evaluate_income(_row())
        assert r.validation_status == 'Valid'
        assert r.theory_compliance_score >= 70

    def test_rv_iv_below_1_favorable(self):
        """CRITICAL: RV/IV < 1.0 = IV > HV = selling rich = FAVORABLE."""
        r = evaluate_income(_row(RV_IV_Ratio=0.75))
        assert 'Strong premium edge' in r.evaluation_notes

    def test_rv_iv_above_1_unfavorable(self):
        """RV/IV > 1.0 = HV > IV = selling cheap."""
        r = evaluate_income(_row(RV_IV_Ratio=1.30))
        assert r.theory_compliance_score < evaluate_income(_row()).theory_compliance_score

    def test_rv_iv_direction_opposite_from_vol(self):
        """Income should favor low RV/IV; vol should favor high."""
        r_low = evaluate_income(_row(RV_IV_Ratio=0.80))
        r_high = evaluate_income(_row(RV_IV_Ratio=1.20))
        assert r_low.theory_compliance_score > r_high.theory_compliance_score

    def test_missing_theta_incomplete(self):
        r = evaluate_income(_row(Theta=np.nan))
        assert r.validation_status == 'Incomplete_Data'

    def test_missing_vega_incomplete(self):
        r = evaluate_income(_row(Vega=np.nan))
        assert r.validation_status == 'Incomplete_Data'

    # ── POP graduated gate ────────────────────────────────────

    def test_pop_above_65_passes(self):
        r = evaluate_income(_row(Probability_Of_Profit=72.0))
        assert r.validation_status == 'Valid'

    def test_pop_58_penalty_not_reject(self):
        """POP 50-65 should get penalty, NOT reject."""
        r = evaluate_income(_row(Probability_Of_Profit=58.0))
        assert r.validation_status != 'Reject'
        assert '50-65%' in r.evaluation_notes

    def test_pop_48_hard_reject(self):
        """POP < 50 is hard reject."""
        r = evaluate_income(_row(Probability_Of_Profit=48.0))
        assert r.validation_status == 'Reject'

    def test_pop_missing_covered_call_mild(self):
        """Missing POP for covered call = mild penalty (stock is hedge)."""
        r = evaluate_income(_row(
            Strategy_Name='Covered Call',
            Probability_Of_Profit=np.nan,
        ))
        assert r.validation_status != 'Reject'

    def test_pop_missing_csp_severe(self):
        """Missing POP for CSP = severe penalty."""
        r_with = evaluate_income(_row())
        r_without = evaluate_income(_row(Probability_Of_Profit=np.nan))
        assert r_without.theory_compliance_score < r_with.theory_compliance_score - 20

    # ── Gamma sign ────────────────────────────────────────────

    def test_positive_gamma_warning(self):
        r = evaluate_income(_row(Gamma=0.08))
        assert 'Positive Gamma' in r.evaluation_notes or 'verify contract' in r.evaluation_notes.lower()

    def test_negative_gamma_good(self):
        r = evaluate_income(_row(Gamma=-0.04))
        assert 'Negative Gamma' in r.evaluation_notes

    # ── Market structure (Murphy) ─────────────────────────────

    def test_csp_bearish_trend_penalty(self):
        r = evaluate_income(_row(Trend='Bearish'))
        assert r.theory_compliance_score < evaluate_income(_row()).theory_compliance_score

    def test_cc_bearish_trend_penalty(self):
        r = evaluate_income(_row(Strategy_Name='Covered Call', Trend='Bearish'))
        assert r.theory_compliance_score < evaluate_income(_row(Strategy_Name='Covered Call')).theory_compliance_score

    # ── IV Momentum ───────────────────────────────────────────

    def test_iv_momentum_rising_mild_penalty(self):
        """Income: rising IV = selling richer but headwind."""
        r_stable = evaluate_income(_row(IV_30D_5D_ROC=0.01))
        r_rising = evaluate_income(_row(IV_30D_5D_ROC=0.08))
        assert r_rising.theory_compliance_score < r_stable.theory_compliance_score

    def test_iv_momentum_falling_no_penalty(self):
        """Income: falling IV = favorable."""
        r = evaluate_income(_row(IV_30D_5D_ROC=-0.05))
        assert 'IV stable or falling' in r.evaluation_notes
