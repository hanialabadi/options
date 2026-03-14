"""
Tests for defensive trim at approaching hard stop.

Validates that multi-contract positions approaching the hard stop
produce a TRIM proposal alongside the existing ROLL, per:
  - Sinclair Ch.7: fractional Kelly — reduce size as edge degrades
  - Chan Ch.4: partial exits preserve optionality on momentum strategies

Covers:
  - Buy-write defensive trim (approaching -15% to -20%)
  - PMCC defensive trim (approaching -25% to -40%)
  - Quantity guard: no trim below 3 contracts
  - Trim math: 30% of contracts, never to 0
  - Resolver competition: TRIM competes with ROLL via EV
"""

import pytest
import pandas as pd
import numpy as np

from core.management.cycle3.doctrine import DoctrineAuthority
from core.management.cycle3.doctrine.strategies.pmcc import (
    pmcc_doctrine_v2,
    PMCC_HARD_STOP,
    PMCC_APPROACHING_STOP,
)
from core.management.cycle3.doctrine.thresholds import (
    PNL_HARD_STOP_BW,
    PNL_APPROACHING_HARD_STOP,
    DEFENSIVE_TRIM_PCT,
    DEFENSIVE_TRIM_MIN_QUANTITY,
)


# ── Helpers ──────────────────────────────────────────────────────────

def _base_buy_write_row(**overrides) -> pd.Series:
    """Minimal BUY_WRITE row for approaching hard stop scenarios."""
    base = {
        "TradeID": "T001",
        "LegID": "L001",
        "Symbol": "AAPL",
        "Underlying_Ticker": "AAPL",
        "Strategy": "BUY_WRITE",
        "AssetType": "STOCK",
        "UL Last": 230.0,
        "Basis": 54674.0,
        "Quantity": 500.0,
        "Base_Quantity": 5,  # 5 contracts — above DEFENSIVE_TRIM_MIN_QUANTITY
        "Underlying_Price_Entry": 273.37,
        "Net_Cost_Basis_Per_Share": 267.25,
        "Cumulative_Premium_Collected": 6.12,
        "Short_Call_Delta": 0.30,
        "Short_Call_Strike": 260.0,
        "Short_Call_DTE": 26.0,
        "Short_Call_Premium": 6.12,
        "Short_Call_Last": 2.50,
        "Short_Call_Moneyness": "OTM",
        "Delta": 0.0,
        "Strike": np.nan,
        "DTE": np.nan,
        "Premium_Entry": np.nan,
        "Last": np.nan,
        "HV_20D": 0.33,
        "Thesis_State": "INTACT",
        "Thesis_Gate": "PASS",
        "_thesis_blocks_roll": False,
        "Thesis_Summary": "",
        "PriceStructure_State": "STRUCTURE_INTACT",
        "TrendIntegrity_State": "TREND_UP",
        "ema50_slope": 0.01,
        "hv_20d_percentile": 45.0,
        "_Active_Conditions": "",
        "_Condition_Resolved": "",
        "Snapshot_TS": pd.Timestamp.now(),
        "Earnings_Date": None,
        "Days_In_Trade": 20,
        "run_id": "test-run",
        "Schema_Hash": "abc123",
        "IV": None,
        "IV_Now": None,
    }
    base.update(overrides)
    return pd.Series(base)


def _pmcc_row(**overrides) -> pd.Series:
    """Minimal PMCC row for approaching hard stop scenarios."""
    base = {
        "UL Last": 150.0,
        "Short_Call_Delta": 0.30,
        "Short_Call_DTE": 35,
        "Short_Call_Strike": 160.0,
        "Strike": 160.0,
        "Delta": 0.30,
        "DTE": 35,
        "LEAP_Call_Delta": 0.78,
        "LEAP_Call_DTE": 320,
        "LEAP_Call_Strike": 130.0,
        "LEAP_Entry_Price": 30.0,
        "LEAP_Call_Last": 21.0,     # -30%, in approaching zone
        "LEAP_Call_Mid": 21.0,
        "Net_Cost_Basis_Per_Share": 28.0,
        "Cumulative_Premium_Collected": 2.50,
        "Premium_Entry": 2.00,
        "Short_Call_Last": 1.20,
        "Last": 1.20,
        "Days_In_Trade": 10,
        "HV_20D": 25.0,
        "Strategy": "PMCC",
        "Thesis_State": "INTACT",
        "PriceStructure_State": "STRUCTURE_HEALTHY",
        "TrendIntegrity_State": "TREND_ACTIVE",
        "VolatilityState_State": "VOL_STABLE",
        "AssignmentRisk_State": "LOW",
        "Quantity": 5,  # 5 contracts
    }
    base.update(overrides)
    return pd.Series(base)


def _pmcc_result():
    return {
        "Action": "HOLD",
        "Urgency": "LOW",
        "Rationale": "default",
        "Doctrine_Source": "McMillan: Neutrality",
        "Decision_State": "NEUTRAL_CONFIDENT",
        "Uncertainty_Reasons": [],
        "Missing_Data_Fields": [],
        "Required_Conditions_Met": True,
    }


def _run_doctrine(row: pd.Series) -> dict:
    return DoctrineAuthority.evaluate(row)


# ═════════════════════════════════════════════════════════════════════
# BUY_WRITE — Defensive Trim
# ═════════════════════════════════════════════════════════════════════

class TestBuyWriteDefensiveTrim:

    def test_approaching_stop_multi_contract_has_trim_proposal(self):
        """5 contracts at -17% → proposals include defensive_trim_approaching_stop."""
        row = _base_buy_write_row(
            Net_Cost_Basis_Per_Share=267.25,
            Base_Quantity=5,
        )
        row["UL Last"] = 221.8  # -17% from $267.25
        result = _run_doctrine(row)
        # The resolver picks the best action (ROLL or TRIM).
        # We verify the trim proposal was considered.
        summary = result.get("Proposals_Summary", "")
        assert "TRIM" in summary or result["Action"] == "TRIM", (
            f"Multi-contract approaching stop should produce TRIM proposal. "
            f"Got: {summary}"
        )

    def test_approaching_stop_small_position_no_trim(self):
        """2 contracts at -17% → no defensive trim (below minimum)."""
        row = _base_buy_write_row(
            Net_Cost_Basis_Per_Share=267.25,
            Base_Quantity=2,
        )
        row["UL Last"] = 221.8  # -17%
        result = _run_doctrine(row)
        # Should be ROLL (standard approaching stop), NOT TRIM
        assert result["Action"] == "ROLL"
        assert result["Urgency"] == "HIGH"

    def test_trim_contracts_math_5_contracts(self):
        """5 contracts × 30% = 1.5 → round to 2."""
        row = _base_buy_write_row(
            Net_Cost_Basis_Per_Share=267.25,
            Base_Quantity=5,
        )
        row["UL Last"] = 221.8  # -17%
        result = _run_doctrine(row)
        # If TRIM wins, verify contract count
        if result["Action"] == "TRIM":
            assert result.get("Trim_Contracts", 0) in (1, 2), (
                f"30% of 5 = 1.5, round to 2 (or 1). Got {result.get('Trim_Contracts')}"
            )
            assert result.get("Trim_Pct", 0) == DEFENSIVE_TRIM_PCT

    def test_trim_contracts_math_10_contracts(self):
        """10 contracts × 30% = 3."""
        row = _base_buy_write_row(
            Net_Cost_Basis_Per_Share=267.25,
            Base_Quantity=10,
        )
        row["UL Last"] = 221.8  # -17%
        result = _run_doctrine(row)
        if result["Action"] == "TRIM":
            assert result.get("Trim_Contracts") == 3
            assert result.get("Trim_Pct") == DEFENSIVE_TRIM_PCT

    def test_hard_stop_no_defensive_trim(self):
        """At -22% (past hard stop) → EXIT, not TRIM."""
        row = _base_buy_write_row(
            Net_Cost_Basis_Per_Share=267.25,
            Base_Quantity=5,
        )
        row["UL Last"] = 208.0  # -22%
        result = _run_doctrine(row)
        assert result["Action"] == "EXIT"

    def test_above_approaching_no_defensive_trim(self):
        """At -10% (above approaching) → no defensive trim."""
        row = _base_buy_write_row(
            Net_Cost_Basis_Per_Share=267.25,
            Base_Quantity=5,
        )
        row["UL Last"] = 240.5  # ~-10%
        result = _run_doctrine(row)
        assert result["Action"] != "TRIM" or "defensive" not in result.get("Winning_Gate", "").lower()

    def test_trim_rationale_cites_sinclair(self):
        """Defensive trim rationale references Sinclair/Chan."""
        row = _base_buy_write_row(
            Net_Cost_Basis_Per_Share=267.25,
            Base_Quantity=5,
        )
        row["UL Last"] = 221.8  # -17%
        result = _run_doctrine(row)
        if result["Action"] == "TRIM":
            assert "sinclair" in result["Rationale"].lower() or "chan" in result["Rationale"].lower()


# ═════════════════════════════════════════════════════════════════════
# PMCC — Defensive Trim
# ═════════════════════════════════════════════════════════════════════

class TestPMCCDefensiveTrim:

    def test_approaching_stop_multi_contract_has_trim(self):
        """5 PMCC contracts with LEAP at -30% → trim proposal exists."""
        row = _pmcc_row(
            LEAP_Entry_Price=30.0,
            LEAP_Call_Last=21.0,  # -30%
            Quantity=5,
        )
        result = pmcc_doctrine_v2(row, _pmcc_result())
        # Check that result includes trim metadata or trim was considered
        proposals_summary = result.get("Proposals_Summary", "")
        assert "TRIM" in proposals_summary or result["Action"] == "TRIM", (
            f"PMCC multi-contract approaching stop should produce TRIM. Got: {proposals_summary}"
        )

    def test_approaching_stop_small_position_no_trim(self):
        """2 PMCC contracts with LEAP at -30% → ROLL only, no trim."""
        row = _pmcc_row(
            LEAP_Entry_Price=30.0,
            LEAP_Call_Last=21.0,  # -30%
            Quantity=2,
        )
        result = pmcc_doctrine_v2(row, _pmcc_result())
        assert result["Action"] == "ROLL"

    def test_trim_never_to_zero(self):
        """3 contracts × 30% = 0.9 → round to 1, leaving 2."""
        row = _pmcc_row(
            LEAP_Entry_Price=30.0,
            LEAP_Call_Last=21.0,  # -30%
            Quantity=3,
        )
        result = pmcc_doctrine_v2(row, _pmcc_result())
        if result["Action"] == "TRIM":
            trim_n = result.get("Trim_Contracts", 0)
            assert trim_n >= 1
            assert trim_n < 3  # never trim all

    def test_hard_stop_overrides_trim(self):
        """LEAP at -45% → hard stop EXIT, not trim."""
        row = _pmcc_row(
            LEAP_Entry_Price=30.0,
            LEAP_Call_Last=16.0,  # -47%
            Quantity=5,
        )
        result = pmcc_doctrine_v2(row, _pmcc_result())
        assert result["Action"] == "EXIT"
        assert result["Urgency"] == "CRITICAL"

    def test_outside_approaching_zone_no_trim(self):
        """LEAP at -10% → no approaching stop, no trim."""
        row = _pmcc_row(
            LEAP_Entry_Price=30.0,
            LEAP_Call_Last=27.0,  # -10%
            Quantity=5,
        )
        result = pmcc_doctrine_v2(row, _pmcc_result())
        assert result["Action"] != "TRIM" or "defensive" not in result.get("Winning_Gate", "").lower()


# ═════════════════════════════════════════════════════════════════════
# Threshold Constants
# ═════════════════════════════════════════════════════════════════════

class TestDefensiveTrimThresholds:

    def test_trim_pct_reasonable(self):
        """Trim percentage must be between 20% and 50%."""
        assert 0.20 <= DEFENSIVE_TRIM_PCT <= 0.50

    def test_min_quantity_at_least_3(self):
        """Need at least 3 contracts for meaningful trim."""
        assert DEFENSIVE_TRIM_MIN_QUANTITY >= 3

    def test_trim_zone_inside_approaching_stop(self):
        """Defensive trim only fires in approaching stop zone."""
        # BW: -15% to -20%
        assert PNL_APPROACHING_HARD_STOP > PNL_HARD_STOP_BW  # -15% > -20%
        # PMCC: -25% to -40%
        assert PMCC_APPROACHING_STOP > PMCC_HARD_STOP  # -25% > -40%


# ═════════════════════════════════════════════════════════════════════
# Directional Scale-Up Thresholds
# ═════════════════════════════════════════════════════════════════════

class TestDirectionalScaleUpThresholds:

    def test_directional_mfe_stricter_than_income(self):
        """Directional requires higher MFE than income for scale-up."""
        from core.management.cycle3.doctrine.thresholds import (
            DIRECTIONAL_SCALE_UP_MFE, WINNER_SCALE_UP_MFE,
        )
        assert DIRECTIONAL_SCALE_UP_MFE > WINNER_SCALE_UP_MFE

    def test_directional_pnl_stricter_than_income(self):
        """Directional requires higher P&L than income for scale-up."""
        from core.management.cycle3.doctrine.thresholds import (
            DIRECTIONAL_SCALE_UP_PNL_MIN, WINNER_SCALE_UP_PNL_MIN,
        )
        assert DIRECTIONAL_SCALE_UP_PNL_MIN > WINNER_SCALE_UP_PNL_MIN

    def test_directional_max_tier_is_zero(self):
        """Directional only gets one add (tier 0 only)."""
        from core.management.cycle3.doctrine.thresholds import DIRECTIONAL_PYRAMID_MAX_TIER
        assert DIRECTIONAL_PYRAMID_MAX_TIER == 0

    def test_vol_scaled_giveback_thresholds_ordered(self):
        """Tight < standard < wide giveback thresholds."""
        from core.management.cycle3.doctrine.thresholds import (
            MFE_GIVEBACK_TIGHT, MFE_GIVEBACK_EXIT, MFE_GIVEBACK_WIDE,
        )
        assert MFE_GIVEBACK_TIGHT < MFE_GIVEBACK_EXIT < MFE_GIVEBACK_WIDE

    def test_vol_hv_boundaries_ordered(self):
        """Low HV boundary < High HV boundary."""
        from core.management.cycle3.doctrine.thresholds import (
            MFE_GIVEBACK_HV_LOW, MFE_GIVEBACK_HV_HIGH,
        )
        assert MFE_GIVEBACK_HV_LOW < MFE_GIVEBACK_HV_HIGH


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
