"""
Tests for the directional strategy evaluator.
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pytest
import pandas as pd
import numpy as np

from scan_engine.evaluators.directional import evaluate_directional


def _row(**overrides):
    """Build a minimal valid directional row."""
    base = {
        'Strategy_Name': 'Long Call',
        'Contract_Status': 'OK',
        'Delta': 0.55,
        'Gamma': 0.04,
        'Vega': 0.25,
        'Actual_DTE': 45,
        'Trend': 'Bullish',
        'Signal_Type': 'Bullish',
        'Regime': 'Compression',
        'Price_vs_SMA20': 2.0,
        'Volume_Trend': 'Rising',
    }
    base.update(overrides)
    return pd.Series(base)


class TestDirectionalEvaluator:
    def test_strong_setup_valid(self):
        r = evaluate_directional(_row())
        assert r.validation_status == 'Valid'
        assert r.theory_compliance_score >= 70

    def test_weak_delta_penalty(self):
        r = evaluate_directional(_row(Delta=0.30))
        assert r.theory_compliance_score < evaluate_directional(_row()).theory_compliance_score

    def test_low_gamma_penalty(self):
        r = evaluate_directional(_row(Gamma=0.01))
        assert r.theory_compliance_score < evaluate_directional(_row()).theory_compliance_score

    def test_missing_greeks_incomplete(self):
        r = evaluate_directional(_row(Delta=np.nan, Gamma=np.nan))
        assert r.validation_status == 'Incomplete_Data'

    def test_trend_misalignment_penalty(self):
        r = evaluate_directional(_row(Trend='Bearish'))
        assert r.theory_compliance_score < evaluate_directional(_row()).theory_compliance_score

    def test_bearish_strategy_bearish_trend(self):
        r = evaluate_directional(_row(Strategy_Name='Long Put', Trend='Bearish'))
        assert r.validation_status == 'Valid'

    def test_leap_lower_gamma_floor(self):
        """LEAPs should use relaxed gamma floor (0.008 instead of 0.03)."""
        r = evaluate_directional(_row(Strategy_Name='Long Call LEAP', Gamma=0.01, Actual_DTE=365))
        assert 'convexity adequate for LEAP' in r.evaluation_notes

    def test_leap_below_gamma_floor(self):
        r = evaluate_directional(_row(Strategy_Name='Long Call LEAP', Gamma=0.005, Actual_DTE=365))
        assert 'Low Gamma' in r.evaluation_notes

    def test_volume_not_supporting_penalty(self):
        r = evaluate_directional(_row(Volume_Trend='Falling'))
        assert r.theory_compliance_score < evaluate_directional(_row()).theory_compliance_score

    def test_pattern_bonus(self):
        r_no = evaluate_directional(_row())
        r_pat = evaluate_directional(_row(Chart_Pattern='Cup and Handle', Pattern_Confidence=75.0))
        assert r_pat.theory_compliance_score > r_no.theory_compliance_score

    def test_leap_fallback_penalty(self):
        r = evaluate_directional(_row(
            Strategy_Name='Long Call LEAP',
            Contract_Status='LEAP_FALLBACK',
            Actual_DTE=200,
            Gamma=0.01,
        ))
        assert 'LEAP_FALLBACK' in r.evaluation_notes
