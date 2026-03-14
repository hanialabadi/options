"""
Tests for the doctrine layer — DoctrineRule, GraduatedRule, and per-family
doctrine correctness (especially RV/IV direction cross-checks).
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pytest
from scan_engine.evaluators.doctrine._rule import DoctrineRule, GraduatedRule
from scan_engine.evaluators.doctrine import income_doctrine, volatility_doctrine, directional_doctrine


# ── DoctrineRule.check() basic operators ──────────────────────

class TestDoctrineRuleCheck:
    def test_gt(self):
        r = DoctrineRule(name="t", threshold=1.0, comparison="gt")
        assert r.check(1.5) is True
        assert r.check(1.0) is False
        assert r.check(0.5) is False

    def test_lt(self):
        r = DoctrineRule(name="t", threshold=1.0, comparison="lt")
        assert r.check(0.5) is True
        assert r.check(1.0) is False

    def test_gte(self):
        r = DoctrineRule(name="t", threshold=1.0, comparison="gte")
        assert r.check(1.0) is True
        assert r.check(0.99) is False

    def test_lte(self):
        r = DoctrineRule(name="t", threshold=1.0, comparison="lte")
        assert r.check(1.0) is True
        assert r.check(1.01) is False

    def test_between_both_bounds(self):
        r = DoctrineRule(name="t", threshold=40.0, comparison="between", high=80.0)
        assert r.check(50.0) is True
        assert r.check(40.0) is True
        assert r.check(80.0) is True
        assert r.check(39.9) is False
        assert r.check(80.1) is False

    def test_between_no_lower(self):
        r = DoctrineRule(name="t", threshold=None, comparison="between", high=80.0)
        assert r.check(-999.0) is True
        assert r.check(80.0) is True
        assert r.check(80.1) is False

    def test_between_no_upper(self):
        r = DoctrineRule(name="t", threshold=40.0, comparison="between", high=None)
        assert r.check(40.0) is True
        assert r.check(999.0) is True
        assert r.check(39.9) is False

    def test_between_no_bounds(self):
        r = DoctrineRule(name="t", threshold=None, comparison="between", high=None)
        assert r.check(0.0) is True
        assert r.check(-999.0) is True

    def test_none_value_returns_false(self):
        r = DoctrineRule(name="t", threshold=1.0, comparison="gt")
        assert r.check(None) is False

    def test_custom_rule(self):
        r = DoctrineRule(name="t", comparison="custom", custom_fn=lambda x, y: x > y)
        assert r.check(x=5, y=3) is True
        assert r.check(x=1, y=3) is False

    def test_custom_no_fn(self):
        r = DoctrineRule(name="t", comparison="custom")
        assert r.check() is False


# ── GraduatedRule.evaluate() ──────────────────────────────────

class TestGraduatedRule:
    def test_pop_above_65_passes(self):
        ded, note, reject = income_doctrine.POP_GATE.evaluate(70.0)
        assert ded == 0
        assert reject is False

    def test_pop_50_to_65_penalty(self):
        ded, note, reject = income_doctrine.POP_GATE.evaluate(58.0)
        assert ded == 20
        assert reject is False

    def test_pop_below_50_rejects(self):
        ded, note, reject = income_doctrine.POP_GATE.evaluate(48.0)
        assert reject is True

    def test_pop_exactly_50_no_reject(self):
        ded, note, reject = income_doctrine.POP_GATE.evaluate(50.0)
        assert reject is False
        assert ded == 20  # marginal band

    def test_pop_exactly_65_passes(self):
        ded, note, reject = income_doctrine.POP_GATE.evaluate(65.0)
        assert ded == 0

    def test_none_value_uses_bottom_tier(self):
        ded, note, reject = income_doctrine.POP_GATE.evaluate(None)
        assert reject is False  # None → bottom tier, not hard reject

    def test_gamma_theta_excellent(self):
        ded, note, _ = volatility_doctrine.GAMMA_THETA_RATIO.evaluate(2.0)
        assert ded == 0
        assert "Excellent" in note

    def test_gamma_theta_marginal(self):
        ded, note, _ = volatility_doctrine.GAMMA_THETA_RATIO.evaluate(0.6)
        assert ded == 15

    def test_gamma_theta_poor(self):
        ded, note, _ = volatility_doctrine.GAMMA_THETA_RATIO.evaluate(0.3)
        assert ded == 30


# ── RV/IV DIRECTION CROSS-CHECK (the critical test) ──────────

class TestRvIvDirection:
    """Income and volatility MUST have opposite RV/IV directions."""

    def test_income_rv_iv_below_1_passes(self):
        """Income sells premium: RV/IV < 1.0 = IV > HV = favorable."""
        assert income_doctrine.RV_IV_EDGE.check(0.85) is True

    def test_income_rv_iv_above_1_fails(self):
        """Income: RV/IV > 1.0 = HV > IV = unfavorable for selling."""
        assert income_doctrine.RV_IV_EDGE.check(1.10) is False

    def test_vol_rv_iv_above_1_passes(self):
        """Long vol: RV/IV > 1.0 = HV > IV = options cheap = favorable."""
        assert volatility_doctrine.RV_IV_EDGE.check(1.10) is True

    def test_vol_rv_iv_below_1_fails(self):
        """Long vol: RV/IV < 1.0 = IV > HV = buying expensive."""
        assert volatility_doctrine.RV_IV_EDGE.check(0.90) is False

    def test_directions_are_opposite(self):
        """At the SAME RV/IV value, income and vol must disagree."""
        # At 0.90: income says GOOD, vol says BAD
        assert income_doctrine.RV_IV_EDGE.check(0.90) is True
        assert volatility_doctrine.RV_IV_EDGE.check(0.90) is False
        # At 1.10: income says BAD, vol says GOOD
        assert income_doctrine.RV_IV_EDGE.check(1.10) is False
        assert volatility_doctrine.RV_IV_EDGE.check(1.10) is True


# ── Custom rules (term structure, IV momentum) ────────────────

class TestCustomRules:
    def test_term_structure_contango_passes(self):
        """Contango: IV30 < IV60 → favorable for long vol."""
        assert volatility_doctrine.TERM_STRUCTURE.check(iv30=25.0, iv60=30.0) is True

    def test_term_structure_inverted_fails(self):
        """Inverted: IV30 > IV60 → headwind for long vol."""
        # check() returns True when rule PASSES. Inverted = rule fails.
        result = volatility_doctrine.TERM_STRUCTURE.check(iv30=35.0, iv60=30.0)
        assert result is False

    def test_term_structure_steep_inversion_severe(self):
        """Steep inversion (>15%) → severe penalty."""
        # iv30=40, iv60=30 → (40-30)/30 = 0.333 > 0.15 → rule FAILS (returns False)
        assert volatility_doctrine.TERM_STRUCTURE_SEVERE.check(iv30=40.0, iv60=30.0) is False

    def test_term_structure_mild_inversion_no_severe(self):
        """Mild inversion (< 15%) → no severe penalty."""
        # iv30=32, iv60=30 → (32-30)/30 = 0.067 < 0.15 → rule passes
        assert volatility_doctrine.TERM_STRUCTURE_SEVERE.check(iv30=32.0, iv60=30.0) is True

    def test_iv_momentum_falling_penalty_vol(self):
        """Vol: IV falling > 5% → headwind."""
        assert volatility_doctrine.IV_MOMENTUM_FALLING.check(iv_30d_5d_roc=-0.08) is False

    def test_iv_momentum_stable_no_penalty_vol(self):
        assert volatility_doctrine.IV_MOMENTUM_FALLING.check(iv_30d_5d_roc=-0.02) is True

    def test_iv_momentum_collapsing_severe(self):
        assert volatility_doctrine.IV_MOMENTUM_COLLAPSING.check(iv_30d_10d_roc=-0.20) is False

    def test_iv_momentum_rising_income_penalty(self):
        """Income: IV rising > 5% → mild headwind (but selling richer)."""
        # check() returns True when unfavorable condition IS met
        assert income_doctrine.IV_MOMENTUM_RISING.check(iv_30d_5d_roc=0.08) is True

    def test_iv_momentum_stable_income_no_penalty(self):
        assert income_doctrine.IV_MOMENTUM_RISING.check(iv_30d_5d_roc=0.02) is False


# ── Expected move coverage ────────────────────────────────────

class TestExpectedMoveCoverage:
    def test_high_ratio_no_penalty(self):
        ded, note, _ = volatility_doctrine.EXPECTED_MOVE_COVERAGE.evaluate(2.0)
        assert ded == 0

    def test_tight_ratio_mild_penalty(self):
        ded, _, _ = volatility_doctrine.EXPECTED_MOVE_COVERAGE.evaluate(1.2)
        assert ded == 10

    def test_low_ratio_penalty(self):
        ded, _, _ = volatility_doctrine.EXPECTED_MOVE_COVERAGE.evaluate(0.8)
        assert ded == 25

    def test_overpaying_severe(self):
        ded, _, _ = volatility_doctrine.EXPECTED_MOVE_COVERAGE.evaluate(0.5)
        assert ded == 35
