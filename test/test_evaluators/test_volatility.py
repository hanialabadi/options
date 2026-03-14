"""
Tests for the volatility strategy evaluator.
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pytest
import pandas as pd
import numpy as np

from scan_engine.evaluators.volatility import evaluate_volatility


def _row(**overrides):
    """Build a minimal valid volatility row."""
    base = {
        'Strategy_Name': 'Long Straddle',
        'Contract_Status': 'OK',
        'Delta': 0.05,
        'Gamma': 0.08,
        'Vega': 0.60,
        'Theta': -0.05,
        'Put_Call_Skew': 1.05,
        'RV_IV_Ratio': 1.10,
        'IV_Percentile': 35.0,
        'IV_Rank': 35.0,
        'Stock_Price': 150.0,
        'Actual_DTE': 30,
        'Regime': 'Compression',
        'Volatility_Regime': 'Compression',
        'Signal_Type': 'Bullish',
    }
    base.update(overrides)
    return pd.Series(base)


class TestVolatilityEvaluator:
    def test_strong_setup_valid(self):
        r = evaluate_volatility(_row())
        assert r.validation_status == 'Valid'
        assert r.theory_compliance_score >= 70

    def test_rv_iv_above_1_favorable(self):
        """CRITICAL: RV/IV > 1.0 = HV > IV = options cheap = FAVORABLE."""
        r = evaluate_volatility(_row(RV_IV_Ratio=1.20))
        assert 'Strong vol edge' in r.evaluation_notes
        assert r.theory_compliance_score >= 70

    def test_rv_iv_below_1_unfavorable(self):
        """CRITICAL: RV/IV < 1.0 = IV > HV = buying expensive = UNFAVORABLE."""
        r = evaluate_volatility(_row(RV_IV_Ratio=0.60))
        assert r.theory_compliance_score < evaluate_volatility(_row()).theory_compliance_score

    def test_rv_iv_direction_correct(self):
        """RV/IV 1.20 should score BETTER than 0.80 for long vol."""
        r_good = evaluate_volatility(_row(RV_IV_Ratio=1.20))
        r_bad = evaluate_volatility(_row(RV_IV_Ratio=0.80))
        assert r_good.theory_compliance_score > r_bad.theory_compliance_score

    def test_skew_hard_gate_rejects(self):
        r = evaluate_volatility(_row(Put_Call_Skew=1.30))
        assert r.validation_status == 'Reject'
        assert 'SKEW VIOLATION' in r.evaluation_notes

    def test_vvix_hard_gate_rejects(self):
        r = evaluate_volatility(_row(VVIX=140.0))
        assert r.validation_status == 'Reject'
        assert 'HIGH VVIX' in r.evaluation_notes

    def test_vol_spike_recent_rejects(self):
        r = evaluate_volatility(_row(Recent_Vol_Spike=True, Days_Since_Vol_Spike=2))
        assert r.validation_status == 'Reject'

    def test_missing_vega_incomplete(self):
        r = evaluate_volatility(_row(Vega=np.nan))
        assert r.validation_status == 'Incomplete_Data'

    def test_negative_gamma_severe_penalty(self):
        r = evaluate_volatility(_row(Gamma=-0.05))
        assert r.theory_compliance_score < 50

    def test_high_iv_percentile_penalty(self):
        r = evaluate_volatility(_row(IV_Percentile=85.0))
        assert r.theory_compliance_score < evaluate_volatility(_row()).theory_compliance_score

    def test_expansion_regime_penalty(self):
        r = evaluate_volatility(_row(Volatility_Regime='Expansion'))
        assert r.theory_compliance_score < evaluate_volatility(_row()).theory_compliance_score

    def test_delta_neutral_required(self):
        r = evaluate_volatility(_row(Delta=0.30))
        assert 'Directional bias' in r.evaluation_notes

    # ── Term structure ────────────────────────────────────────

    def test_term_structure_contango_no_penalty(self):
        r = evaluate_volatility(_row(IV_30D=25.0, iv_30d=25.0, IV_60D=30.0, iv_60d=30.0))
        assert 'Contango' in r.evaluation_notes

    def test_term_structure_inverted_penalty(self):
        r_contango = evaluate_volatility(_row(IV_30D=25.0, iv_30d=25.0, IV_60D=30.0, iv_60d=30.0))
        r_inverted = evaluate_volatility(_row(IV_30D=35.0, iv_30d=35.0, IV_60D=30.0, iv_60d=30.0))
        assert r_inverted.theory_compliance_score < r_contango.theory_compliance_score

    def test_term_structure_steep_inversion_severe(self):
        """Steep inversion (>15%) should get extra penalty beyond mild inversion."""
        r_mild = evaluate_volatility(_row(IV_30D=32.0, iv_30d=32.0, IV_60D=30.0, iv_60d=30.0))
        r_steep = evaluate_volatility(_row(IV_30D=40.0, iv_30d=40.0, IV_60D=30.0, iv_60d=30.0))
        assert r_steep.theory_compliance_score < r_mild.theory_compliance_score

    # ── IV Momentum ───────────────────────────────────────────

    def test_iv_momentum_falling_penalty(self):
        r_stable = evaluate_volatility(_row(IV_30D_5D_ROC=0.01))
        r_falling = evaluate_volatility(_row(IV_30D_5D_ROC=-0.08))
        assert r_falling.theory_compliance_score < r_stable.theory_compliance_score

    def test_iv_momentum_collapsing_severe_penalty(self):
        r_falling = evaluate_volatility(_row(IV_30D_5D_ROC=-0.08))
        r_collapse = evaluate_volatility(_row(IV_30D_5D_ROC=-0.08, IV_30D_10D_ROC=-0.20))
        assert r_collapse.theory_compliance_score < r_falling.theory_compliance_score

    def test_iv_momentum_rising_no_penalty(self):
        r = evaluate_volatility(_row(IV_30D_5D_ROC=0.05))
        # Should not lose points for IV momentum when rising
        assert 'IV stable or rising' in r.evaluation_notes

    # ── Gamma/Theta ratio ─────────────────────────────────────

    def test_gamma_theta_ratio_excellent_no_penalty(self):
        """Gamma/theta >= 1.5 → no penalty."""
        r = evaluate_volatility(_row(Gamma=0.10, Theta=-0.04, Stock_Price=150.0))
        # dollar_gamma = 15, abs_theta = 0.04, ratio = 375 — excellent
        assert 'Excellent convexity' in r.evaluation_notes

    def test_gamma_theta_ratio_poor_heavy_penalty(self):
        """Gamma/theta < 0.5 → heavy penalty."""
        # Gamma=0.001 on $150 stock → dollar_gamma=$0.15, theta=-0.50 → ratio=0.30
        r = evaluate_volatility(_row(Gamma=0.001, Theta=-0.50, Stock_Price=150.0))
        assert 'Poor convexity' in r.evaluation_notes

    # ── Expected move coverage ────────────────────────────────

    def test_expected_move_coverage_high_no_penalty(self):
        """Large expected move vs premium → no penalty."""
        # Stock=150, IV_30D=40, DTE=30, Total_Debit=5
        # EM = 150 * 0.40 * sqrt(30/365) = 150 * 0.40 * 0.287 ≈ 17.2
        # ratio = 17.2 / 5 = 3.44 → no penalty
        r = evaluate_volatility(_row(
            IV_30D=40.0, iv_30d=40.0, IV_60D=45.0, iv_60d=45.0,
            Total_Debit=5.0, Stock_Price=150.0, Actual_DTE=30,
        ))
        assert 'Expected move well exceeds' in r.evaluation_notes

    def test_expected_move_overpaying_penalty(self):
        """Premium exceeds expected move → penalty."""
        # Stock=150, IV_30D=15, DTE=30, Total_Debit=20
        # EM = 150 * 0.15 * sqrt(30/365) ≈ 6.45
        # ratio = 6.45 / 20 = 0.32 → severe penalty
        r = evaluate_volatility(_row(
            IV_30D=15.0, iv_30d=15.0, IV_60D=20.0, iv_60d=20.0,
            Total_Debit=20.0, Stock_Price=150.0, Actual_DTE=30,
        ))
        assert 'Premium exceeds expected move' in r.evaluation_notes
