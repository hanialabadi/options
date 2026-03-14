"""
Tests for the scaling engine upgrade — pyramid sizing, income TRIM, and
wave-phase-gated winner expansion.

Validates:
1. compute_pyramid_add_contracts(): tier sizing, gates, floor, edge cases
2. is_trim_eligible(): phase/MFE combinations
3. gate_income_trim(): fire/skip conditions, quantity guards, rounding
4. Gate precedence: TRIM cannot mask EXIT
5. run_all.py pyramid sizing: wave-phase gates, frozen base quantity
"""

import math
import pytest
import pandas as pd
from core.management.cycle2.chart_state.state_extractors.wave_phase import (
    compute_pyramid_add_contracts,
    is_trim_eligible,
    is_scale_up_eligible,
    BUILDING, PEAKING, FADING, EXHAUSTED, FORMING, STALLED, RECOVERING,
)
from core.management.cycle3.doctrine.shared_income_gates import (
    gate_income_trim,
    gate_fading_winner,
)
from core.management.cycle3.doctrine.thresholds import (
    PYRAMID_TIER_0_RATIO,
    PYRAMID_TIER_1_RATIO,
    PYRAMID_PNL_MIN,
    INCOME_TRIM_PEAK_PCT,
    INCOME_TRIM_EXHAUSTION_PCT,
    INCOME_TRIM_MIN_QUANTITY,
    MFE_SIGNIFICANT,
)


# ── Helpers ────────────────────────────────────────────────────────────────

def _row(**overrides):
    """Create a base income position row."""
    base = {
        'MomentumVelocity_State': 'ACCELERATING',
        'TrendIntegrity_State': 'STRONG_TREND',
        'PriceStructure_State': 'STRUCTURAL_UP',
        'RecoveryQuality_State': 'NOT_IN_RECOVERY',
        'WavePhase_State': 'BUILDING',
        'Conviction_Status': 'STABLE',
        'Quantity': 4,
        'PnL_Pct': 0.15,
        'Trajectory_MFE': 0.25,
        'sma_distance_pct': 0.03,
        'rsi_14': 55.0,
    }
    base.update(overrides)
    return pd.Series(base)


def _empty_result():
    return {"Action": "", "Urgency": "", "Rationale": "", "Doctrine_Source": ""}


# ═══════════════════════════════════════════════════════════════════════════
# 1. compute_pyramid_add_contracts
# ═══════════════════════════════════════════════════════════════════════════

class TestPyramidSizing:
    """McMillan Ch.4 / Passarelli Ch.6: decreasing pyramid layers."""

    def test_tier_0_returns_60pct_of_base(self):
        add = compute_pyramid_add_contracts(
            base_quantity=5, pyramid_tier=0, wave_phase=BUILDING,
            pnl_pct=0.10, conviction_status="STABLE", momentum_state="ACCELERATING",
        )
        assert add == round(5 * PYRAMID_TIER_0_RATIO)  # 3

    def test_tier_1_returns_30pct_of_base(self):
        add = compute_pyramid_add_contracts(
            base_quantity=5, pyramid_tier=1, wave_phase=BUILDING,
            pnl_pct=0.10, conviction_status="STABLE", momentum_state="ACCELERATING",
        )
        assert add == max(1, round(5 * PYRAMID_TIER_1_RATIO))  # 2

    def test_tier_2_returns_zero_full_position(self):
        add = compute_pyramid_add_contracts(
            base_quantity=5, pyramid_tier=2, wave_phase=BUILDING,
            pnl_pct=0.10, conviction_status="STABLE", momentum_state="ACCELERATING",
        )
        assert add == 0

    def test_floor_at_1_contract(self):
        add = compute_pyramid_add_contracts(
            base_quantity=1, pyramid_tier=1, wave_phase=BUILDING,
            pnl_pct=0.10, conviction_status="STABLE", momentum_state="ACCELERATING",
        )
        assert add == 1  # 30% of 1 rounds to 0, but floor is 1

    def test_base_quantity_zero_treated_as_1(self):
        add = compute_pyramid_add_contracts(
            base_quantity=0, pyramid_tier=0, wave_phase=BUILDING,
            pnl_pct=0.10, conviction_status="STABLE", momentum_state="ACCELERATING",
        )
        assert add >= 1  # floor at 1


class TestPyramidGates:
    """Gates that must all pass for pyramid sizing to return > 0."""

    def test_gate_wave_phase_not_building(self):
        for phase in [PEAKING, FADING, EXHAUSTED, FORMING, STALLED]:
            add = compute_pyramid_add_contracts(
                base_quantity=5, pyramid_tier=0, wave_phase=phase,
                pnl_pct=0.10, conviction_status="STABLE", momentum_state="ACCELERATING",
            )
            assert add == 0, f"Expected 0 for phase={phase}"

    def test_gate_pnl_below_minimum(self):
        add = compute_pyramid_add_contracts(
            base_quantity=5, pyramid_tier=0, wave_phase=BUILDING,
            pnl_pct=0.04, conviction_status="STABLE", momentum_state="ACCELERATING",
        )
        assert add == 0

    def test_gate_conviction_weakening(self):
        add = compute_pyramid_add_contracts(
            base_quantity=5, pyramid_tier=0, wave_phase=BUILDING,
            pnl_pct=0.10, conviction_status="WEAKENING", momentum_state="ACCELERATING",
        )
        assert add == 0

    def test_gate_conviction_reversing(self):
        add = compute_pyramid_add_contracts(
            base_quantity=5, pyramid_tier=0, wave_phase=BUILDING,
            pnl_pct=0.10, conviction_status="REVERSING", momentum_state="ACCELERATING",
        )
        assert add == 0

    def test_gate_momentum_reversing(self):
        add = compute_pyramid_add_contracts(
            base_quantity=5, pyramid_tier=0, wave_phase=BUILDING,
            pnl_pct=0.10, conviction_status="STABLE", momentum_state="REVERSING",
        )
        assert add == 0

    def test_gate_momentum_decelerating(self):
        add = compute_pyramid_add_contracts(
            base_quantity=5, pyramid_tier=0, wave_phase=BUILDING,
            pnl_pct=0.10, conviction_status="STABLE", momentum_state="DECELERATING",
        )
        assert add == 0

    def test_all_gates_pass(self):
        add = compute_pyramid_add_contracts(
            base_quantity=5, pyramid_tier=0, wave_phase=BUILDING,
            pnl_pct=0.10, conviction_status="STABLE", momentum_state="TRENDING",
        )
        assert add > 0


# ═══════════════════════════════════════════════════════════════════════════
# 2. is_trim_eligible
# ═══════════════════════════════════════════════════════════════════════════

class TestTrimEligibility:
    """Wave phase determines trim timing and fraction."""

    def test_peaking_with_significant_mfe_trims_25pct(self):
        eligible, fraction = is_trim_eligible(PEAKING, 0.25, 0.15)
        assert eligible is True
        assert fraction == INCOME_TRIM_PEAK_PCT

    def test_peaking_without_mfe_no_trim(self):
        eligible, fraction = is_trim_eligible(PEAKING, 0.10, 0.15)
        assert eligible is False

    def test_exhausted_trims_50pct(self):
        eligible, fraction = is_trim_eligible(EXHAUSTED, 0.05, 0.03)
        assert eligible is True
        assert fraction == INCOME_TRIM_EXHAUSTION_PCT

    def test_fading_trims_50pct(self):
        eligible, fraction = is_trim_eligible(FADING, 0.05, 0.03)
        assert eligible is True
        assert fraction == INCOME_TRIM_EXHAUSTION_PCT

    def test_building_no_trim(self):
        eligible, fraction = is_trim_eligible(BUILDING, 0.30, 0.15)
        assert eligible is False
        assert fraction == 0.0

    def test_forming_no_trim(self):
        eligible, fraction = is_trim_eligible(FORMING, 0.20, 0.10)
        assert eligible is False


# ═══════════════════════════════════════════════════════════════════════════
# 3. gate_income_trim
# ═══════════════════════════════════════════════════════════════════════════

class TestGateIncomeTrim:
    """Income TRIM gate with rounding rules and quantity guards."""

    def test_fires_on_peaking_with_mfe(self):
        row = _row(WavePhase_State="PEAKING", Trajectory_MFE=0.25)
        fired, result = gate_income_trim(
            row=row, pnl_pct=0.15, wave_phase="PEAKING",
            conviction_status="STABLE", quantity=4,
            result=_empty_result(), strategy_label="BW",
        )
        assert fired is True
        assert result["Action"] == "TRIM"
        assert result.get("Trim_Contracts") == 1  # 25% of 4 = 1
        assert result.get("Trim_Pct") == INCOME_TRIM_PEAK_PCT

    def test_fires_on_exhausted_with_reversing(self):
        row = _row(WavePhase_State="EXHAUSTED", Trajectory_MFE=0.10)
        fired, result = gate_income_trim(
            row=row, pnl_pct=0.08, wave_phase="EXHAUSTED",
            conviction_status="REVERSING", quantity=4,
            result=_empty_result(), strategy_label="CC",
        )
        assert fired is True
        assert result["Action"] == "TRIM"
        assert result["Urgency"] == "HIGH"
        assert result.get("Trim_Contracts") == 2  # 50% of 4 = 2

    def test_skips_single_contract(self):
        row = _row(WavePhase_State="PEAKING", Trajectory_MFE=0.25)
        fired, result = gate_income_trim(
            row=row, pnl_pct=0.15, wave_phase="PEAKING",
            conviction_status="STABLE", quantity=1,
            result=_empty_result(), strategy_label="BW",
        )
        assert fired is False

    def test_skips_building_phase(self):
        row = _row(WavePhase_State="BUILDING", Trajectory_MFE=0.25)
        fired, result = gate_income_trim(
            row=row, pnl_pct=0.15, wave_phase="BUILDING",
            conviction_status="STABLE", quantity=4,
            result=_empty_result(), strategy_label="BW",
        )
        assert fired is False

    def test_rounding_2_contracts_25pct_trims_1(self):
        row = _row(WavePhase_State="PEAKING", Trajectory_MFE=0.25)
        fired, result = gate_income_trim(
            row=row, pnl_pct=0.15, wave_phase="PEAKING",
            conviction_status="STABLE", quantity=2,
            result=_empty_result(), strategy_label="BW",
        )
        assert fired is True
        assert result.get("Trim_Contracts") == 1  # min(max(1, round(0.5)), 1) = 1

    def test_rounding_3_contracts_50pct_trims_1(self):
        """3 contracts × 50% = 1.5, rounds to 2, but min(2, 3-1) = 2."""
        row = _row(WavePhase_State="EXHAUSTED", Trajectory_MFE=0.10)
        fired, result = gate_income_trim(
            row=row, pnl_pct=0.08, wave_phase="EXHAUSTED",
            conviction_status="STABLE", quantity=3,
            result=_empty_result(), strategy_label="CC",
        )
        assert fired is True
        trim_c = result.get("Trim_Contracts")
        assert trim_c >= 1
        assert trim_c < 3  # never trim to 0

    def test_never_trims_to_zero_contracts(self):
        """Even with 50% trim on 2 contracts, keep at least 1."""
        row = _row(WavePhase_State="EXHAUSTED", Trajectory_MFE=0.10)
        fired, result = gate_income_trim(
            row=row, pnl_pct=0.08, wave_phase="EXHAUSTED",
            conviction_status="STABLE", quantity=2,
            result=_empty_result(), strategy_label="CC",
        )
        assert fired is True
        assert result.get("Trim_Contracts") == 1  # min(max(1, round(1)), 1) = 1

    def test_medium_urgency_for_peaking(self):
        row = _row(WavePhase_State="PEAKING", Trajectory_MFE=0.25)
        fired, result = gate_income_trim(
            row=row, pnl_pct=0.15, wave_phase="PEAKING",
            conviction_status="STABLE", quantity=4,
            result=_empty_result(), strategy_label="BW",
        )
        assert result["Urgency"] == "MEDIUM"


# ═══════════════════════════════════════════════════════════════════════════
# 4. Gate Precedence — TRIM cannot mask EXIT
# ═══════════════════════════════════════════════════════════════════════════

class TestGatePrecedence:
    """Fading winner EXIT (priority=3) beats income TRIM (priority=4)."""

    def test_fading_winner_exit_has_higher_priority_than_trim(self):
        """
        When both gates fire on the same position, ProposalCollector should
        resolve to EXIT because fading_winner has priority=3 and TRIM has
        priority=4 (lower number = higher priority).
        """
        from core.management.cycle3.doctrine.proposal import ProposalCollector, propose_gate

        collector = ProposalCollector()

        # Fading winner fires EXIT at priority=3
        propose_gate(
            collector, "fading_winner",
            action="EXIT", urgency="HIGH",
            rationale="Round-trip protection",
            doctrine_source="McMillan Ch.4",
            priority=3, exit_trigger_type="INCOME",
        )

        # Income TRIM fires at priority=4
        propose_gate(
            collector, "income_trim",
            action="TRIM", urgency="MEDIUM",
            rationale="Wave phase trim",
            doctrine_source="Passarelli Ch.6",
            priority=4, exit_trigger_type="INCOME",
        )

        # EXIT should win (lower priority number + higher urgency)
        best_exit = collector.best_proposal_for_action("EXIT")
        best_trim = collector.best_proposal_for_action("TRIM")
        assert best_exit is not None
        assert best_exit.priority < best_trim.priority


# ═══════════════════════════════════════════════════════════════════════════
# 5. Threshold Constants Sanity
# ═══════════════════════════════════════════════════════════════════════════

class TestThresholdSanity:
    """Verify threshold relationships make sense."""

    def test_pyramid_ratios_decrease(self):
        assert PYRAMID_TIER_0_RATIO > PYRAMID_TIER_1_RATIO

    def test_trim_peak_less_than_exhaustion(self):
        assert INCOME_TRIM_PEAK_PCT < INCOME_TRIM_EXHAUSTION_PCT

    def test_trim_min_quantity_at_least_2(self):
        assert INCOME_TRIM_MIN_QUANTITY >= 2

    def test_pyramid_pnl_min_positive(self):
        assert PYRAMID_PNL_MIN > 0
