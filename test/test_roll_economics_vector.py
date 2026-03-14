"""
Tests for roll economics vector — decomposed roll analysis per candidate.

Validates:
1. _score_candidate returns full vector with all sub-scores
2. _compute_economics_vector assembles leg/strike/time/post-roll components
3. _classify_candidate_edge produces correct edge labels
4. Case A vs B vs C: same net credit, different economic meaning
5. Slippage/liquidity warnings fire correctly
6. Assignment risk classification
7. Gamma reduction estimate
8. Score decomposition embedded in economics dict
"""

import numpy as np
import pandas as pd
import pytest

from core.management.cycle3.roll.roll_candidate_engine import (
    _score_candidate,
    _compute_economics_vector,
    _classify_candidate_edge,
)


# ── Shared fixtures ─────────────────────────────────────────────────────────

_SCORE_BASE = dict(
    delta=0.30, target_delta=0.30, actual_dte=45, target_dte=45,
    iv=0.30, current_iv=0.30, liq_grade="GOOD", hv_20d=0.25,
    ul_price=100.0, theta=-0.02, strategy_key="BUY_WRITE",
    mid=2.50, strike=105.0, net_cost_basis=98.0, roll_mode="NORMAL",
)


def _make_candidate(**overrides) -> dict:
    """Build a minimal candidate dict with score_vector and trader metrics."""
    sv = _score_candidate(**_SCORE_BASE)
    base = {
        "strike": 107.0, "expiry": "2026-05-01", "dte": 45,
        "delta": 0.28, "gamma": 0.02, "vega": 0.10, "theta": -0.03,
        "iv": 0.30, "bid": 2.40, "ask": 2.60, "mid": 2.50,
        "bid_size": 50, "ask_size": 40, "oi": 500, "volume": 200,
        "spread_pct": 8.0, "liq_grade": "GOOD", "score": sv["composite"],
        "score_vector": sv,
        "roll_from_strike": 105.0, "roll_from_dte": 30,
        "roll_from_iv": 0.28,
        "breakeven_after_roll": 97.50, "annualized_yield_pct": 18.2,
        "otm_pct": 7.0, "prob_otm_at_expiry": 72.0,
        "theta_per_day_dollars": 3.00,
    }
    base.update(overrides)
    return base


def _make_cost(**overrides) -> dict:
    base = {"net_per_contract": 0.10, "net_total": 10.0,
            "type": "credit", "contracts": 1}
    base.update(overrides)
    return base


def _make_row(**overrides) -> pd.Series:
    base = {"Ticker": "TEST", "Strategy": "BUY_WRITE", "UL Last": 100.0,
            "Strike": 105.0, "DTE": 30, "Last": 1.80}
    base.update(overrides)
    return pd.Series(base)


# ── Score vector tests ──────────────────────────────────────────────────────

class TestScoreVector:
    """_score_candidate returns a dict with all sub-scores."""

    def test_returns_dict(self):
        result = _score_candidate(**_SCORE_BASE)
        assert isinstance(result, dict)

    def test_has_composite(self):
        result = _score_candidate(**_SCORE_BASE)
        assert "composite" in result
        assert 0 <= result["composite"] <= 1.0

    def test_has_all_sub_scores(self):
        result = _score_candidate(**_SCORE_BASE)
        for key in ("delta_score", "yield_score", "dte_score", "iv_score", "liq_score"):
            assert key in result, f"Missing sub-score: {key}"
            assert 0 <= result[key] <= 1.0, f"{key} out of range: {result[key]}"

    def test_has_adjustment_fields(self):
        result = _score_candidate(**_SCORE_BASE)
        for key in ("net_roll_adj", "recovery_score", "mode_bonus"):
            assert key in result

    def test_has_multiplier_fields(self):
        result = _score_candidate(**_SCORE_BASE)
        for key in ("earnings_mult", "dividend_mult", "churn_mult"):
            assert key in result
            assert result[key] <= 1.0  # multipliers cap at 1.0 (penalties only)

    def test_has_applied_weights(self):
        result = _score_candidate(**_SCORE_BASE)
        w_sum = result["w_delta"] + result["w_yield"] + result["w_dte"] + result["w_iv"] + result["w_liq"]
        assert abs(w_sum - 1.0) < 0.001, f"Weights should sum to 1.0, got {w_sum}"


# ── Economics vector tests ──────────────────────────────────────────────────

class TestEconomicsVector:
    """_compute_economics_vector assembles all components."""

    def test_has_leg_economics(self):
        cand = _make_candidate()
        cost = _make_cost()
        row = _make_row()
        econ = _compute_economics_vector(cand, cost, row, 98.0, 100.0, "BUY_WRITE")
        assert "net_credit_debit" in econ
        assert "cost_type" in econ
        assert econ["cost_type"] == "credit"
        assert econ["net_credit_debit"] == 0.10

    def test_has_strike_change(self):
        cand = _make_candidate(strike=110.0, roll_from_strike=105.0)
        cost = _make_cost()
        row = _make_row()
        econ = _compute_economics_vector(cand, cost, row, 98.0, 100.0, "BUY_WRITE")
        assert econ["strike_change"] == 5.0
        assert econ["strike_change_pct"] == pytest.approx(4.8, abs=0.1)

    def test_basis_improvement_positive(self):
        """Strike lift from 105 → 110 when basis=98 should show positive improvement."""
        cand = _make_candidate(strike=110.0, roll_from_strike=105.0)
        cost = _make_cost()
        row = _make_row()
        econ = _compute_economics_vector(cand, cost, row, 98.0, 100.0, "BUY_WRITE")
        assert econ["basis_improvement_pct"] > 0

    def test_basis_improvement_negative(self):
        """Strike drop from 105 → 100 should show negative improvement."""
        cand = _make_candidate(strike=100.0, roll_from_strike=105.0)
        cost = _make_cost()
        row = _make_row()
        econ = _compute_economics_vector(cand, cost, row, 98.0, 100.0, "BUY_WRITE")
        assert econ["basis_improvement_pct"] < 0

    def test_dte_extension(self):
        cand = _make_candidate(dte=60, roll_from_dte=30)
        cost = _make_cost()
        row = _make_row()
        econ = _compute_economics_vector(cand, cost, row, 98.0, 100.0, "BUY_WRITE")
        assert econ["dte_extension"] == 30

    def test_gamma_reduction_positive(self):
        """Extending DTE should produce positive gamma reduction."""
        cand = _make_candidate(dte=60, roll_from_dte=30)
        cost = _make_cost()
        row = _make_row()
        econ = _compute_economics_vector(cand, cost, row, 98.0, 100.0, "BUY_WRITE")
        assert econ["gamma_reduction_pct"] is not None
        assert econ["gamma_reduction_pct"] > 0  # gamma ∝ 1/√T

    def test_assignment_risk_improved(self):
        """Rolling strike from 102 → 110 at spot 100 should improve assignment risk."""
        cand = _make_candidate(strike=110.0, roll_from_strike=102.0)
        cost = _make_cost()
        row = _make_row()
        econ = _compute_economics_vector(cand, cost, row, 98.0, 100.0, "BUY_WRITE")
        assert econ["assignment_risk_change"] == "IMPROVED"

    def test_assignment_risk_worsened(self):
        """Rolling strike from 110 → 101 at spot 100 should worsen assignment risk."""
        cand = _make_candidate(strike=101.0, roll_from_strike=110.0)
        cost = _make_cost()
        row = _make_row()
        econ = _compute_economics_vector(cand, cost, row, 98.0, 100.0, "BUY_WRITE")
        assert econ["assignment_risk_change"] == "WORSENED"

    def test_slippage_warning_on_wide_spread(self):
        cand = _make_candidate(spread_pct=15.0)
        cost = _make_cost()
        row = _make_row()
        econ = _compute_economics_vector(cand, cost, row, 98.0, 100.0, "BUY_WRITE")
        assert econ["slippage_warning"] is True

    def test_slippage_warning_on_thin_liq(self):
        cand = _make_candidate(liq_grade="THIN", spread_pct=5.0)
        cost = _make_cost()
        row = _make_row()
        econ = _compute_economics_vector(cand, cost, row, 98.0, 100.0, "BUY_WRITE")
        assert econ["slippage_warning"] is True

    def test_no_slippage_warning_good_liq(self):
        cand = _make_candidate(liq_grade="EXCELLENT", spread_pct=2.0)
        cost = _make_cost()
        row = _make_row()
        econ = _compute_economics_vector(cand, cost, row, 98.0, 100.0, "BUY_WRITE")
        assert econ["slippage_warning"] is False

    def test_score_decomposition_embedded(self):
        """Economics vector should include score sub-components."""
        cand = _make_candidate()
        cost = _make_cost()
        row = _make_row()
        econ = _compute_economics_vector(cand, cost, row, 98.0, 100.0, "BUY_WRITE")
        assert econ["delta_score"] is not None
        assert econ["yield_score"] is not None
        assert econ["composite_score"] is not None

    def test_post_roll_state_fields(self):
        cand = _make_candidate(
            breakeven_after_roll=97.50, annualized_yield_pct=18.2,
            otm_pct=7.0, prob_otm_at_expiry=72.0, theta_per_day_dollars=3.00,
        )
        cost = _make_cost()
        row = _make_row()
        econ = _compute_economics_vector(cand, cost, row, 98.0, 100.0, "BUY_WRITE")
        assert econ["new_breakeven"] == 97.50
        assert econ["new_annualized_yield_pct"] == 18.2
        assert econ["prob_expire_otm"] == 72.0


# ── Edge classification tests ───────────────────────────────────────────────

class TestEdgeClassification:
    """_classify_candidate_edge produces correct labels."""

    def test_income_extension(self):
        """Same strike, longer DTE, small credit → INCOME_EXTENSION."""
        econ = {
            "net_credit_debit": 0.10, "cost_type": "credit",
            "strike_change": 0.0, "strike_change_pct": 0.0,
            "basis_improvement_pct": 0.0, "assignment_risk_change": "UNCHANGED",
            "dte_extension": 30, "gamma_reduction_pct": 20,
            "slippage_warning": False, "liq_score": 0.75,
            "composite_score": 0.65,
        }
        label, summary = _classify_candidate_edge(econ)
        assert label == "INCOME_EXTENSION"
        assert "+30d DTE" in summary

    def test_strike_improvement(self):
        """Strike lifted $2.50, credit, basis improved > 2% → STRIKE_IMPROVEMENT."""
        econ = {
            "net_credit_debit": 0.10, "cost_type": "credit",
            "strike_change": 2.50, "strike_change_pct": 2.4,
            "basis_improvement_pct": 2.5, "assignment_risk_change": "IMPROVED",
            "dte_extension": 15, "gamma_reduction_pct": 10,
            "slippage_warning": False, "liq_score": 0.75,
            "composite_score": 0.70,
        }
        label, summary = _classify_candidate_edge(econ)
        assert label == "STRIKE_IMPROVEMENT"
        assert "recovery" in summary.lower()

    def test_recovery_roll(self):
        """Debit roll with large basis improvement → RECOVERY_ROLL."""
        econ = {
            "net_credit_debit": -0.15, "cost_type": "debit",
            "strike_change": 5.0, "strike_change_pct": 5.0,
            "basis_improvement_pct": 5.0, "assignment_risk_change": "IMPROVED",
            "dte_extension": 15, "gamma_reduction_pct": 10,
            "slippage_warning": False, "liq_score": 0.70,
            "composite_score": 0.55,
        }
        label, summary = _classify_candidate_edge(econ)
        assert label == "RECOVERY_ROLL"
        assert "debit" in summary.lower()

    def test_income_credit(self):
        """Meaningful credit, small strike change → INCOME_CREDIT."""
        econ = {
            "net_credit_debit": 0.30, "cost_type": "credit",
            "strike_change": 0.50, "strike_change_pct": 0.5,
            "basis_improvement_pct": 0.5, "assignment_risk_change": "UNCHANGED",
            "dte_extension": 10, "gamma_reduction_pct": 5,
            "slippage_warning": False, "liq_score": 0.80,
            "composite_score": 0.72,
        }
        label, summary = _classify_candidate_edge(econ)
        assert label == "INCOME_CREDIT"

    def test_defensive_roll(self):
        """Credit roll with assignment risk improvement → DEFENSIVE_ROLL."""
        econ = {
            "net_credit_debit": 0.05, "cost_type": "credit",
            "strike_change": 3.0, "strike_change_pct": 3.0,
            "basis_improvement_pct": 1.5, "assignment_risk_change": "IMPROVED",
            "dte_extension": 20, "gamma_reduction_pct": 15,
            "slippage_warning": False, "liq_score": 0.75,
            "composite_score": 0.68,
        }
        label, summary = _classify_candidate_edge(econ)
        assert label == "DEFENSIVE_ROLL"
        assert "assignment" in summary.lower()

    def test_assignment_preferable(self):
        """Very low composite score → ASSIGNMENT_PREFERABLE."""
        econ = {
            "net_credit_debit": -0.50, "cost_type": "debit",
            "strike_change": 1.0, "strike_change_pct": 1.0,
            "basis_improvement_pct": 0.5, "assignment_risk_change": "UNCHANGED",
            "dte_extension": 5, "gamma_reduction_pct": 3,
            "slippage_warning": False, "liq_score": 0.40,
            "composite_score": 0.20,
        }
        label, summary = _classify_candidate_edge(econ)
        assert label == "ASSIGNMENT_PREFERABLE"

    def test_weak_liquidity(self):
        """Slippage warning + low liquidity score → WEAK_LIQUIDITY."""
        econ = {
            "net_credit_debit": 0.10, "cost_type": "credit",
            "strike_change": 2.0, "strike_change_pct": 2.0,
            "basis_improvement_pct": 1.5, "assignment_risk_change": "IMPROVED",
            "dte_extension": 15, "gamma_reduction_pct": 10,
            "slippage_warning": True, "liq_score": 0.20,
            "composite_score": 0.55,
        }
        label, summary = _classify_candidate_edge(econ)
        assert label == "WEAK_LIQUIDITY"
        assert "fill" in summary.lower() or "spread" in summary.lower()


# ── Case A vs B vs C (user's examples) ──────────────────────────────────────

class TestCaseComparison:
    """Same net credit, different economic meaning — edge labels must differ."""

    def test_case_a_income_extension(self):
        """Case A: +0.10 credit, strike unchanged, +35d DTE → income extension."""
        econ = {
            "net_credit_debit": 0.10, "cost_type": "credit",
            "strike_change": 0.0, "basis_improvement_pct": 0.0,
            "assignment_risk_change": "UNCHANGED",
            "dte_extension": 35, "gamma_reduction_pct": 25,
            "slippage_warning": False, "liq_score": 0.75,
            "composite_score": 0.65,
        }
        label, _ = _classify_candidate_edge(econ)
        assert label == "INCOME_EXTENSION"

    def test_case_b_strike_improvement(self):
        """Case B: +0.10 credit, strike up $2.50 → recovery improvement."""
        econ = {
            "net_credit_debit": 0.10, "cost_type": "credit",
            "strike_change": 2.50, "strike_change_pct": 2.5,
            "basis_improvement_pct": 2.5, "assignment_risk_change": "IMPROVED",
            "dte_extension": 15, "gamma_reduction_pct": 10,
            "slippage_warning": False, "liq_score": 0.75,
            "composite_score": 0.70,
        }
        label, _ = _classify_candidate_edge(econ)
        assert label == "STRIKE_IMPROVEMENT"

    def test_case_c_weak_liquidity(self):
        """Case C: +0.10 credit on paper, awful spread → WEAK_LIQUIDITY."""
        econ = {
            "net_credit_debit": 0.10, "cost_type": "credit",
            "strike_change": 0.0, "basis_improvement_pct": 0.0,
            "assignment_risk_change": "UNCHANGED",
            "dte_extension": 15, "gamma_reduction_pct": 10,
            "slippage_warning": True, "liq_score": 0.15,
            "composite_score": 0.40,
        }
        label, _ = _classify_candidate_edge(econ)
        assert label == "WEAK_LIQUIDITY"

    def test_all_three_cases_differ(self):
        """Cases A, B, C with same net credit produce different edge labels."""
        econ_a = {
            "net_credit_debit": 0.10, "cost_type": "credit",
            "strike_change": 0.0, "basis_improvement_pct": 0.0,
            "assignment_risk_change": "UNCHANGED", "dte_extension": 35,
            "gamma_reduction_pct": 25, "slippage_warning": False,
            "liq_score": 0.75, "composite_score": 0.65,
        }
        econ_b = {
            "net_credit_debit": 0.10, "cost_type": "credit",
            "strike_change": 2.50, "strike_change_pct": 2.5,
            "basis_improvement_pct": 2.5, "assignment_risk_change": "IMPROVED",
            "dte_extension": 15, "gamma_reduction_pct": 10,
            "slippage_warning": False, "liq_score": 0.75,
            "composite_score": 0.70,
        }
        econ_c = {
            "net_credit_debit": 0.10, "cost_type": "credit",
            "strike_change": 0.0, "basis_improvement_pct": 0.0,
            "assignment_risk_change": "UNCHANGED", "dte_extension": 15,
            "gamma_reduction_pct": 10, "slippage_warning": True,
            "liq_score": 0.15, "composite_score": 0.40,
        }
        label_a, _ = _classify_candidate_edge(econ_a)
        label_b, _ = _classify_candidate_edge(econ_b)
        label_c, _ = _classify_candidate_edge(econ_c)
        assert len({label_a, label_b, label_c}) == 3, (
            f"Three different economic profiles should get different labels: "
            f"{label_a}, {label_b}, {label_c}"
        )


# ── Gamma reduction math ───────────────────────────────────────────────────

class TestGammaReduction:
    """Verify gamma reduction estimate (gamma ∝ 1/√T)."""

    def test_doubling_dte_reduces_gamma_about_29pct(self):
        """DTE 30→60 → gamma reduced by ~(1 - √(30/60)) ≈ 29%."""
        cand = _make_candidate(dte=60, roll_from_dte=30)
        cost = _make_cost()
        row = _make_row()
        econ = _compute_economics_vector(cand, cost, row, 98.0, 100.0, "BUY_WRITE")
        assert econ["gamma_reduction_pct"] is not None
        assert 25 < econ["gamma_reduction_pct"] < 35

    def test_no_reduction_if_same_dte(self):
        """Same DTE → no extension → no gamma reduction."""
        cand = _make_candidate(dte=30, roll_from_dte=30)
        cost = _make_cost()
        row = _make_row()
        econ = _compute_economics_vector(cand, cost, row, 98.0, 100.0, "BUY_WRITE")
        # dte_extension is 0, so gamma_reduction should be None (no extension)
        assert econ["gamma_reduction_pct"] is None
