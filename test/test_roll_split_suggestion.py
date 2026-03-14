"""
Tests for roll split execution suggestion — multi-contract roll optimization.

Validates:
1. No suggestion for < 4 contracts
2. ROLL_PARTIAL_HOLD: income credit candidate → roll half, hold half
3. SPLIT_DEBIT_EXPOSURE: recovery debit → split to reduce debit commitment
4. STAGGER_EXPIRY: two candidates with different edges + different expiry
5. SPLIT_STRIKE: two candidates with different edges + same expiry
6. PARTIAL_CLOSE_PARTIAL_ROLL: top candidate is ASSIGNMENT_PREFERABLE
7. No suggestion when all candidates have same edge type
8. 50/50 split math is correct for odd contract counts
"""

import pytest

from core.management.cycle3.roll.roll_candidate_engine import (
    _compute_split_suggestion,
)


# ── Shared helpers ──────────────────────────────────────────────────────────

def _cand(edge="INCOME_CREDIT", net=0.10, strike=107.0, expiry="2026-05-01",
          dte=45, basis_imp=0.5, **kw):
    """Build a minimal candidate with economics and edge classification."""
    econ = {
        "net_credit_debit": net,
        "cost_type": "credit" if net >= 0 else "debit",
        "strike_change": strike - 105.0,
        "basis_improvement_pct": basis_imp,
        "assignment_risk_change": "UNCHANGED",
        "dte_extension": dte - 30,
        "theta_per_day": 3.00,
        "composite_score": 0.65,
        "liq_score": 0.75,
        "slippage_warning": False,
    }
    econ.update(kw.pop("econ_overrides", {}))
    base = {
        "strike": strike, "expiry": expiry, "dte": dte,
        "primary_edge": edge, "edge_summary": "test summary",
        "economics": econ, "score": 0.65,
    }
    base.update(kw)
    return base


# ── No suggestion tests ─────────────────────────────────────────────────────

class TestNoSuggestion:
    """Cases where no split should be recommended."""

    def test_single_contract(self):
        result = _compute_split_suggestion(
            [_cand()], total_contracts=1, strategy_key="BUY_WRITE"
        )
        assert result is None

    def test_two_contracts(self):
        result = _compute_split_suggestion(
            [_cand()], total_contracts=2, strategy_key="BUY_WRITE"
        )
        assert result is None

    def test_three_contracts(self):
        result = _compute_split_suggestion(
            [_cand()], total_contracts=3, strategy_key="BUY_WRITE"
        )
        assert result is None

    def test_no_candidates(self):
        result = _compute_split_suggestion(
            [], total_contracts=10, strategy_key="BUY_WRITE"
        )
        assert result is None

    def test_no_economics(self):
        """Candidate without economics dict → no suggestion."""
        cand = {"strike": 107, "primary_edge": "INCOME_CREDIT"}
        result = _compute_split_suggestion(
            [cand], total_contracts=10, strategy_key="BUY_WRITE"
        )
        assert result is None


# ── Roll partial + hold ─────────────────────────────────────────────────────

class TestRollPartialHold:
    """Income credit/extension → roll half, hold half."""

    def test_income_credit_triggers_split(self):
        result = _compute_split_suggestion(
            [_cand(edge="INCOME_CREDIT", net=0.20)],
            total_contracts=10, strategy_key="BUY_WRITE",
        )
        assert result is not None
        assert result["type"] == "ROLL_PARTIAL_HOLD"
        assert result["roll_contracts"] == 5
        assert result["hold_contracts"] == 5

    def test_income_extension_triggers_split(self):
        result = _compute_split_suggestion(
            [_cand(edge="INCOME_EXTENSION", net=0.05)],
            total_contracts=8, strategy_key="COVERED_CALL",
        )
        assert result is not None
        assert result["type"] == "ROLL_PARTIAL_HOLD"

    def test_odd_contracts_round_down(self):
        """7 contracts → 3 roll, 4 hold."""
        result = _compute_split_suggestion(
            [_cand(edge="INCOME_CREDIT", net=0.15)],
            total_contracts=7, strategy_key="BUY_WRITE",
        )
        assert result is not None
        assert result["roll_contracts"] == 3
        assert result["hold_contracts"] == 4

    def test_not_triggered_for_long_options(self):
        """Income split only applies to short-vol strategies."""
        result = _compute_split_suggestion(
            [_cand(edge="INCOME_CREDIT", net=0.20)],
            total_contracts=10, strategy_key="LONG_CALL",
        )
        assert result is None or result["type"] != "ROLL_PARTIAL_HOLD"

    def test_rationale_includes_credit_total(self):
        result = _compute_split_suggestion(
            [_cand(edge="INCOME_CREDIT", net=0.20)],
            total_contracts=10, strategy_key="BUY_WRITE",
        )
        assert "$" in result["rationale"]
        assert "credit" in result["rationale"].lower() or "Credit" in result["rationale"]


# ── Split debit exposure ────────────────────────────────────────────────────

class TestSplitDebitExposure:
    """Recovery roll (debit) → split to reduce commitment."""

    def test_recovery_debit_triggers_split(self):
        result = _compute_split_suggestion(
            [_cand(edge="RECOVERY_ROLL", net=-0.50, basis_imp=5.0)],
            total_contracts=6, strategy_key="BUY_WRITE",
        )
        assert result is not None
        assert result["type"] == "SPLIT_DEBIT_EXPOSURE"
        assert result["roll_contracts"] == 3
        assert result["hold_contracts"] == 3

    def test_small_basis_improvement_no_split(self):
        """Debit roll without meaningful basis improvement → no split."""
        result = _compute_split_suggestion(
            [_cand(edge="RECOVERY_ROLL", net=-0.50, basis_imp=1.0)],
            total_contracts=6, strategy_key="BUY_WRITE",
        )
        # basis_imp <= 2.0 → should not trigger SPLIT_DEBIT_EXPOSURE
        assert result is None or result["type"] != "SPLIT_DEBIT_EXPOSURE"

    def test_rationale_shows_debit_reduction(self):
        result = _compute_split_suggestion(
            [_cand(edge="RECOVERY_ROLL", net=-0.50, basis_imp=5.0)],
            total_contracts=10, strategy_key="BUY_WRITE",
        )
        assert result is not None
        assert "debit" in result["rationale"].lower()
        assert "50%" in result["rationale"] or "reduces" in result["rationale"].lower()


# ── Stagger expiry ──────────────────────────────────────────────────────────

class TestStaggerExpiry:
    """Two candidates with different edges and different expirations."""

    def test_different_edges_different_expiry(self):
        c1 = _cand(edge="INCOME_CREDIT", net=0.20, expiry="2026-05-01")
        c2 = _cand(edge="STRIKE_IMPROVEMENT", net=0.05, strike=110.0,
                    expiry="2026-06-01", basis_imp=3.0)
        result = _compute_split_suggestion(
            [c1, c2], total_contracts=8, strategy_key="BUY_WRITE",
        )
        assert result is not None
        assert result["type"] == "STAGGER_EXPIRY"
        assert result["tranche_a_contracts"] == 4
        assert result["tranche_b_contracts"] == 4

    def test_same_edge_no_stagger(self):
        """Two candidates with same edge type → no stagger suggestion."""
        c1 = _cand(edge="INCOME_CREDIT", net=0.20, expiry="2026-05-01")
        c2 = _cand(edge="INCOME_CREDIT", net=0.15, expiry="2026-06-01")
        result = _compute_split_suggestion(
            [c1, c2], total_contracts=8, strategy_key="BUY_WRITE",
        )
        # Should fall through to ROLL_PARTIAL_HOLD instead
        assert result is None or result["type"] != "STAGGER_EXPIRY"


# ── Split strike ────────────────────────────────────────────────────────────

class TestSplitStrike:
    """Two candidates with different edges, same expiration."""

    def test_different_edges_same_expiry(self):
        c1 = _cand(edge="INCOME_CREDIT", net=0.20, strike=107.0, expiry="2026-05-01")
        c2 = _cand(edge="DEFENSIVE_ROLL", net=0.05, strike=112.0,
                    expiry="2026-05-01", basis_imp=2.5,
                    econ_overrides={"assignment_risk_change": "IMPROVED"})
        result = _compute_split_suggestion(
            [c1, c2], total_contracts=6, strategy_key="BUY_WRITE",
        )
        assert result is not None
        assert result["type"] == "SPLIT_STRIKE"


# ── Partial close + partial roll ────────────────────────────────────────────

class TestPartialClosePartialRoll:
    """Top candidate is ASSIGNMENT_PREFERABLE, second has value."""

    def test_close_half_roll_half(self):
        c1 = _cand(edge="ASSIGNMENT_PREFERABLE", net=-0.80,
                    econ_overrides={"composite_score": 0.20})
        c2 = _cand(edge="INCOME_CREDIT", net=0.10, strike=108.0, expiry="2026-06-01")
        result = _compute_split_suggestion(
            [c1, c2], total_contracts=8, strategy_key="BUY_WRITE",
        )
        assert result is not None
        assert result["type"] == "PARTIAL_CLOSE_PARTIAL_ROLL"
        assert result["close_contracts"] == 4
        assert result["roll_contracts"] == 4

    def test_both_preferable_no_split(self):
        """Both candidates are ASSIGNMENT_PREFERABLE → no split."""
        c1 = _cand(edge="ASSIGNMENT_PREFERABLE", net=-0.80)
        c2 = _cand(edge="ASSIGNMENT_PREFERABLE", net=-0.60)
        result = _compute_split_suggestion(
            [c1, c2], total_contracts=8, strategy_key="BUY_WRITE",
        )
        assert result is None or result["type"] != "PARTIAL_CLOSE_PARTIAL_ROLL"


# ── Return structure ────────────────────────────────────────────────────────

class TestReturnStructure:
    """Verify all split suggestions have required fields."""

    def test_roll_partial_hold_fields(self):
        result = _compute_split_suggestion(
            [_cand(edge="INCOME_CREDIT", net=0.20)],
            total_contracts=10, strategy_key="BUY_WRITE",
        )
        assert result is not None
        assert "type" in result
        assert "rationale" in result
        assert "edge_type" in result
        assert "roll_contracts" in result
        assert "hold_contracts" in result

    def test_stagger_fields(self):
        c1 = _cand(edge="INCOME_CREDIT", net=0.20, expiry="2026-05-01")
        c2 = _cand(edge="STRIKE_IMPROVEMENT", net=0.05, strike=110.0,
                    expiry="2026-06-01", basis_imp=3.0)
        result = _compute_split_suggestion(
            [c1, c2], total_contracts=8, strategy_key="BUY_WRITE",
        )
        assert result is not None
        assert "tranche_a" in result
        assert "tranche_b" in result
        assert "tranche_a_contracts" in result
        assert "tranche_b_contracts" in result
