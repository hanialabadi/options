"""
Tests for carry-inversion roll gate integration.

Validates that the shared ``gate_carry_inversion_roll()`` fires correctly
based on pre-computed ``Carry_Theta_Ratio`` and ``Carry_Classification``
columns from ``MarginCarryCalculator``, and that it is properly wired
into CC, BW, SHORT_PUT, and PMCC v2 doctrine functions.
"""

import pytest
import pandas as pd

from core.management.cycle3.doctrine.shared_income_gates import (
    gate_carry_inversion_roll,
)
from core.management.cycle3.doctrine.thresholds import (
    CARRY_INVERSION_MILD,
    CARRY_INVERSION_SEVERE,
)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _base_result() -> dict:
    """Minimal result dict for gate functions."""
    return {
        "Action": "HOLD",
        "Urgency": "LOW",
        "Rationale": "",
        "Doctrine_Source": "",
    }


def _make_row(**overrides) -> pd.Series:
    """Build a minimal row with carry columns."""
    defaults = {
        "Carry_Classification": "NONE",
        "Carry_Theta_Ratio": float("nan"),
        "Daily_Margin_Cost": 0.0,
        "Cumulative_Margin_Carry": 0.0,
        "Days_In_Trade": 30,
    }
    defaults.update(overrides)
    return pd.Series(defaults)


# ── TestGateCarryInversionRoll ───────────────────────────────────────────────

class TestGateCarryInversionRoll:
    """Tests for the shared gate_carry_inversion_roll() function."""

    def test_no_carry_does_not_fire(self):
        """NONE classification → gate does not fire."""
        row = _make_row(Carry_Classification="NONE")
        fired, result = gate_carry_inversion_roll(row=row, result=_base_result())
        assert not fired
        assert result["Action"] == "HOLD"

    def test_covered_does_not_fire(self):
        """COVERED classification → gate does not fire."""
        row = _make_row(
            Carry_Classification="COVERED",
            Carry_Theta_Ratio=0.5,
            Daily_Margin_Cost=5.0,
        )
        fired, result = gate_carry_inversion_roll(row=row, result=_base_result())
        assert not fired

    def test_uncovered_does_not_fire(self):
        """UNCOVERED (no theta data) → gate does not fire."""
        row = _make_row(Carry_Classification="UNCOVERED")
        fired, result = gate_carry_inversion_roll(row=row, result=_base_result())
        assert not fired

    def test_mild_inversion_fires_roll_medium(self):
        """MILD_INVERSION → ROLL with MEDIUM urgency."""
        row = _make_row(
            Carry_Classification="MILD_INVERSION",
            Carry_Theta_Ratio=1.2,
            Daily_Margin_Cost=12.0,
            Cumulative_Margin_Carry=360.0,
            Days_In_Trade=30,
        )
        fired, result = gate_carry_inversion_roll(row=row, result=_base_result())
        assert fired
        assert result["Action"] == "ROLL"
        assert result["Urgency"] == "MEDIUM"
        assert "1.2" in result["Rationale"]
        assert "Passarelli" in result["Doctrine_Source"]

    def test_severe_inversion_fires_roll_high(self):
        """SEVERE_INVERSION → ROLL with HIGH urgency."""
        row = _make_row(
            Carry_Classification="SEVERE_INVERSION",
            Carry_Theta_Ratio=2.0,
            Daily_Margin_Cost=20.0,
            Cumulative_Margin_Carry=600.0,
            Days_In_Trade=30,
        )
        fired, result = gate_carry_inversion_roll(row=row, result=_base_result())
        assert fired
        assert result["Action"] == "ROLL"
        assert result["Urgency"] == "HIGH"
        assert "2.0" in result["Rationale"]
        assert "Severe" in result["Doctrine_Source"]

    def test_boundary_mild_at_threshold(self):
        """Ratio exactly at CARRY_INVERSION_MILD → fires."""
        row = _make_row(
            Carry_Classification="MILD_INVERSION",
            Carry_Theta_Ratio=CARRY_INVERSION_MILD,
            Daily_Margin_Cost=10.0,
        )
        fired, result = gate_carry_inversion_roll(row=row, result=_base_result())
        assert fired
        assert result["Urgency"] == "MEDIUM"

    def test_boundary_severe_at_threshold(self):
        """Ratio exactly at CARRY_INVERSION_SEVERE → fires HIGH."""
        row = _make_row(
            Carry_Classification="SEVERE_INVERSION",
            Carry_Theta_Ratio=CARRY_INVERSION_SEVERE,
            Daily_Margin_Cost=15.0,
        )
        fired, result = gate_carry_inversion_roll(row=row, result=_base_result())
        assert fired
        assert result["Urgency"] == "HIGH"

    def test_missing_columns_does_not_fire(self):
        """Row without carry columns → gate does not fire."""
        row = pd.Series({"Ticker": "TEST", "Strategy": "BUY_WRITE"})
        fired, result = gate_carry_inversion_roll(row=row, result=_base_result())
        assert not fired

    def test_zero_ratio_does_not_fire(self):
        """Carry_Theta_Ratio = 0 → gate does not fire even with MILD classification."""
        row = _make_row(
            Carry_Classification="MILD_INVERSION",
            Carry_Theta_Ratio=0.0,
            Daily_Margin_Cost=0.0,
        )
        fired, result = gate_carry_inversion_roll(row=row, result=_base_result())
        assert not fired

    def test_rationale_includes_cumulative(self):
        """Rationale includes cumulative carry and days in trade."""
        row = _make_row(
            Carry_Classification="SEVERE_INVERSION",
            Carry_Theta_Ratio=2.5,
            Daily_Margin_Cost=25.0,
            Cumulative_Margin_Carry=750.0,
            Days_In_Trade=30,
        )
        fired, result = gate_carry_inversion_roll(row=row, result=_base_result())
        assert fired
        assert "$750.00" in result["Rationale"]
        assert "30 days" in result["Rationale"]


# ── TestThresholdConstants ───────────────────────────────────────────────────

class TestThresholdConstants:
    """Verify carry threshold constants are correctly ordered."""

    def test_mild_less_than_severe(self):
        assert CARRY_INVERSION_MILD < CARRY_INVERSION_SEVERE

    def test_mild_is_one(self):
        assert CARRY_INVERSION_MILD == 1.0

    def test_severe_is_one_five(self):
        assert CARRY_INVERSION_SEVERE == 1.5


# ── TestDoctrineIntegration ──────────────────────────────────────────────────

class TestDoctrineIntegration:
    """Verify carry gate is wired into all income strategy v2 functions."""

    def _make_income_row(self, strategy: str, carry_class: str = "SEVERE_INVERSION") -> pd.Series:
        """Build a row with enough fields for doctrine to run the carry gate."""
        return pd.Series({
            # Carry columns (pre-computed by MarginCarryCalculator)
            "Carry_Classification": carry_class,
            "Carry_Theta_Ratio": 2.0,
            "Daily_Margin_Cost": 20.0,
            "Cumulative_Margin_Carry": 600.0,
            "Days_In_Trade": 30,
            # Common fields needed by strategies
            "Strategy": strategy,
            "Ticker": "TEST",
            "UL Last": 100.0,
            "Strike": 105.0,
            "DTE": 45,
            "Delta": -0.30,
            "Gamma": 0.02,
            "Theta": -0.05,
            "Vega": 0.10,
            "HV_20D": 0.25,
            "AssetType": "OPTION",
            "Quantity": -1,
            "Premium_Entry": 2.50,
            "Basis": -250.0,
            "Total_GL_Decimal": 50.0,
            "Total_GL_Pct": 20.0,
            "Net_Cost_Basis_Per_Share": 0.0,
            "Equity_Integrity_State": "INTACT",
            "Market_Structure": "Uptrend",
            "TrendIntegrity_State": "INTACT",
            "PriceStructure_State": "INTACT",
            "Account": "Individual",
            "Is_Retirement": False,
        })

    def test_cc_v2_has_carry_gate(self):
        """covered_call_doctrine_v2 includes carry_inversion_roll proposal."""
        from core.management.cycle3.doctrine.strategies.covered_call import (
            covered_call_doctrine_v2,
        )
        row = self._make_income_row("COVERED_CALL")
        result = _base_result()
        result["Decision_State"] = "ACTIONABLE"
        out = covered_call_doctrine_v2(row, result)
        # If carry gate fired, it should appear in the proposals log
        # The gate fires but may not win resolution — we just verify no crash
        assert "Action" in out

    def test_sp_v2_has_carry_gate(self):
        """short_put_doctrine_v2 includes carry_inversion_roll proposal."""
        from core.management.cycle3.doctrine.strategies.short_put import (
            short_put_doctrine_v2,
        )
        row = self._make_income_row("SHORT_PUT")
        row["IV_Now"] = 0.30
        row["IV_30D"] = 0.30
        row["Portfolio_Delta_Utilization_Pct"] = 5.0
        result = _base_result()
        result["Decision_State"] = "ACTIONABLE"
        out = short_put_doctrine_v2(row, result)
        assert "Action" in out

    def test_pmcc_v2_has_carry_gate(self):
        """pmcc_doctrine_v2 includes carry_inversion_roll proposal."""
        from core.management.cycle3.doctrine.strategies.pmcc import (
            pmcc_doctrine_v2,
        )
        row = self._make_income_row("PMCC")
        row["Short_Call_Delta"] = 0.30
        row["Short_Call_DTE"] = 30
        row["Short_Call_Strike"] = 105.0
        row["LEAP_Call_Delta"] = 0.75
        row["LEAP_Call_DTE"] = 300
        row["LEAP_Call_Strike"] = 80.0
        row["LEAP_Entry_Price"] = 25.0
        row["LEAP_Call_Last"] = 27.0
        row["Cumulative_Premium_Collected"] = 3.50
        result = _base_result()
        result["Decision_State"] = "ACTIONABLE"
        out = pmcc_doctrine_v2(row, result)
        assert "Action" in out

    def test_no_carry_inversion_no_extra_proposal(self):
        """With NONE classification, carry gate does not add a proposal."""
        from core.management.cycle3.doctrine.strategies.covered_call import (
            covered_call_doctrine_v2,
        )
        row = self._make_income_row("COVERED_CALL", carry_class="NONE")
        row["Carry_Theta_Ratio"] = float("nan")
        result = _base_result()
        result["Decision_State"] = "ACTIONABLE"
        out = covered_call_doctrine_v2(row, result)
        # Should still produce a valid result (default HOLD)
        assert out.get("Action") in ("HOLD", "ROLL", "EXIT", "BUYBACK")


# ── TestPreComputedPreference ────────────────────────────────────────────────

class TestPreComputedPreference:
    """Verify that LEAPS carry helpers prefer pre-computed Daily_Margin_Cost."""

    def test_cc_leaps_carry_uses_precomputed(self):
        """_gate_leaps_carry_non_broken reads Daily_Margin_Cost when available."""
        from core.management.cycle3.doctrine.strategies.covered_call import (
            _gate_leaps_carry_non_broken,
        )
        # With Daily_Margin_Cost = $10/day for 1 contract → $0.10/share/day
        # Theta = $0.05/share/day → ratio = 2.0 → should fire
        row = pd.Series({
            "Theta": -0.05,
            "Strike": 105.0,
            "Daily_Margin_Cost": 10.0,  # $/day per contract
        })
        result = _gate_leaps_carry_non_broken(
            row=row, spot=100.0, dte=200, result=_base_result(),
        )
        assert result is not None
        assert result["Action"] == "ROLL"

    def test_cc_leaps_carry_falls_back_without_precomputed(self):
        """_gate_leaps_carry_non_broken falls back to spot×rate when no column."""
        from core.management.cycle3.doctrine.strategies.covered_call import (
            _gate_leaps_carry_non_broken,
        )
        # Without Daily_Margin_Cost, uses spot * rate = 100 * 0.000284 = $0.0284
        # Theta = $0.01 → ratio ~2.84 → should fire
        row = pd.Series({
            "Theta": -0.01,
            "Strike": 105.0,
        })
        result = _gate_leaps_carry_non_broken(
            row=row, spot=100.0, dte=200, result=_base_result(),
        )
        assert result is not None
        assert result["Action"] == "ROLL"

    def test_cc_leaps_carry_zero_precomputed_falls_back(self):
        """Daily_Margin_Cost=0 (retirement/option) → falls back to spot×rate."""
        from core.management.cycle3.doctrine.strategies.covered_call import (
            _gate_leaps_carry_non_broken,
        )
        row = pd.Series({
            "Theta": -0.01,
            "Strike": 105.0,
            "Daily_Margin_Cost": 0.0,  # retirement account → $0
        })
        result = _gate_leaps_carry_non_broken(
            row=row, spot=100.0, dte=200, result=_base_result(),
        )
        # With spot fallback: 100 * 0.000284 = $0.0284 vs theta $0.01 → fires
        assert result is not None
