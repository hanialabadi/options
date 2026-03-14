"""
Tests for the wave phase classifier — determines where a position is in its
move lifecycle and gates scale-up eligibility.

Validates:
1. Phase classification from Cycle 2 state inputs
2. Scale-up eligibility gating
3. Edge cases (missing data, boundary values)
4. Surfer metaphor: BUILDING = scale window, FADING/EXHAUSTED = don't chase
"""

import pytest
import pandas as pd
from core.management.cycle2.chart_state.state_extractors.wave_phase import (
    compute_wave_phase,
    is_scale_up_eligible,
    FORMING, BUILDING, PEAKING, FADING, EXHAUSTED, RECOVERING, STALLED, UNKNOWN,
)


# ── Fixtures ──────────────────────────────────────────────────────────────

def _row(**overrides):
    """Create a base row with strong-trend BUILDING setup."""
    base = {
        'MomentumVelocity_State': 'ACCELERATING',
        'TrendIntegrity_State': 'STRONG_TREND',
        'PriceStructure_State': 'STRUCTURAL_UP',
        'RecoveryQuality_State': 'NOT_IN_RECOVERY',
        'sma_distance_pct': 0.03,   # 3% from SMA20 — not extended
        'rsi_14': 55.0,
        'rsi_slope': 0.5,
        'momentum_slope': 0.15,
        'PnL_Pct': 0.10,
        'Price_Drift_Pct': 5.0,
    }
    base.update(overrides)
    return pd.Series(base)


# ── 1. Phase Classification ──────────────────────────────────────────────

class TestBuildingPhase:
    """BUILDING = confirmed trend + momentum expanding + not overextended."""

    def test_accelerating_strong_trend(self):
        row = _row()
        result = compute_wave_phase(row)
        assert result.state == BUILDING
        assert 'scale-up' in result.resolution_reason.lower()

    def test_trending_strong_trend_near_sma(self):
        row = _row(MomentumVelocity_State='TRENDING', sma_distance_pct=0.02)
        result = compute_wave_phase(row)
        assert result.state == BUILDING

    def test_accelerating_weak_trend_building(self):
        row = _row(TrendIntegrity_State='WEAK_TREND')
        result = compute_wave_phase(row)
        assert result.state == BUILDING

    def test_not_building_when_overextended(self):
        """Even with ACCELERATING, >12% from SMA = PEAKING, not BUILDING."""
        row = _row(sma_distance_pct=0.15)
        result = compute_wave_phase(row)
        assert result.state == PEAKING


class TestFormingPhase:
    """FORMING = trend emerging but not confirmed yet."""

    def test_accelerating_no_trend(self):
        row = _row(TrendIntegrity_State='NO_TREND')
        result = compute_wave_phase(row)
        assert result.state == FORMING

    def test_trending_weak_trend(self):
        row = _row(MomentumVelocity_State='TRENDING',
                   TrendIntegrity_State='WEAK_TREND',
                   sma_distance_pct=0.06)  # >5% so not BUILDING
        result = compute_wave_phase(row)
        assert result.state == FORMING


class TestPeakingPhase:
    """PEAKING = move extended, don't add."""

    def test_trending_overextended(self):
        row = _row(MomentumVelocity_State='TRENDING', sma_distance_pct=0.15)
        result = compute_wave_phase(row)
        assert result.state == PEAKING

    def test_trending_rsi_overbought_flattening(self):
        row = _row(MomentumVelocity_State='TRENDING',
                   TrendIntegrity_State='STRONG_TREND',
                   rsi_14=75, rsi_slope=-0.5,
                   sma_distance_pct=0.08)  # moderate extension
        result = compute_wave_phase(row)
        assert result.state == PEAKING

    def test_trending_rsi_oversold_recovering_put_side(self):
        """Put-side peaking: RSI oversold and recovering."""
        row = _row(MomentumVelocity_State='TRENDING',
                   TrendIntegrity_State='STRONG_TREND',
                   rsi_14=25, rsi_slope=0.3,
                   sma_distance_pct=0.04)
        result = compute_wave_phase(row)
        assert result.state == PEAKING

    def test_accelerating_extreme_extension(self):
        """Accelerating but way overextended = chasing."""
        row = _row(sma_distance_pct=0.20)
        result = compute_wave_phase(row)
        assert result.state == PEAKING


class TestFadingPhase:
    """FADING = momentum declining, divergences present."""

    def test_late_cycle(self):
        row = _row(MomentumVelocity_State='LATE_CYCLE')
        result = compute_wave_phase(row)
        assert result.state == FADING

    def test_decelerating_weak_trend(self):
        row = _row(MomentumVelocity_State='DECELERATING',
                   TrendIntegrity_State='WEAK_TREND')
        result = compute_wave_phase(row)
        assert result.state == FADING

    def test_dead_cat_bounce(self):
        row = _row(RecoveryQuality_State='DEAD_CAT_BOUNCE',
                   MomentumVelocity_State='TRENDING')
        result = compute_wave_phase(row)
        assert result.state == FADING
        assert 'dead cat' in result.resolution_reason.lower()

    def test_reversing_without_exhaustion(self):
        """REVERSING without broken trend = FADING, not EXHAUSTED."""
        row = _row(MomentumVelocity_State='REVERSING',
                   TrendIntegrity_State='WEAK_TREND')
        result = compute_wave_phase(row)
        assert result.state == FADING


class TestExhaustedPhase:
    """EXHAUSTED = move complete, reversal signals."""

    def test_reversing_trend_exhausted(self):
        row = _row(MomentumVelocity_State='REVERSING',
                   TrendIntegrity_State='TREND_EXHAUSTED')
        result = compute_wave_phase(row)
        assert result.state == EXHAUSTED

    def test_reversing_no_trend(self):
        row = _row(MomentumVelocity_State='REVERSING',
                   TrendIntegrity_State='NO_TREND')
        result = compute_wave_phase(row)
        assert result.state == EXHAUSTED

    def test_reversing_structure_broken(self):
        row = _row(MomentumVelocity_State='REVERSING',
                   PriceStructure_State='STRUCTURE_BROKEN')
        result = compute_wave_phase(row)
        assert result.state == EXHAUSTED


class TestRecoveringPhase:
    """RECOVERING = bounce after decline."""

    def test_structural_recovery(self):
        row = _row(RecoveryQuality_State='STRUCTURAL_RECOVERY')
        result = compute_wave_phase(row)
        assert result.state == RECOVERING


class TestStalledPhase:
    """STALLED = no directional conviction."""

    def test_stalling_momentum(self):
        row = _row(MomentumVelocity_State='STALLING')
        result = compute_wave_phase(row)
        assert result.state == STALLED

    def test_decelerating_strong_trend(self):
        """Decelerating + strong trend = STALLED (not FADING — trend still strong)."""
        row = _row(MomentumVelocity_State='DECELERATING',
                   TrendIntegrity_State='STRONG_TREND')
        result = compute_wave_phase(row)
        assert result.state == STALLED


# ── 2. Scale-Up Eligibility ──────────────────────────────────────────────

class TestScaleUpEligibility:
    """Only BUILDING + profitable positions are eligible for scale-up."""

    def test_building_profitable(self):
        assert is_scale_up_eligible(BUILDING, 0.10) is True

    def test_building_breakeven(self):
        """At breakeven, not profitable enough (need 5%)."""
        assert is_scale_up_eligible(BUILDING, 0.02) is False

    def test_building_no_pnl(self):
        assert is_scale_up_eligible(BUILDING, None) is False

    def test_forming_profitable(self):
        """FORMING = too early for scale-up."""
        assert is_scale_up_eligible(FORMING, 0.20) is False

    def test_peaking_profitable(self):
        """PEAKING = don't chase."""
        assert is_scale_up_eligible(PEAKING, 0.30) is False

    def test_fading_profitable(self):
        """FADING = wave dying, don't add."""
        assert is_scale_up_eligible(FADING, 0.25) is False

    def test_exhausted(self):
        assert is_scale_up_eligible(EXHAUSTED, 0.50) is False

    def test_stalled(self):
        assert is_scale_up_eligible(STALLED, 0.15) is False

    def test_building_threshold_exact(self):
        """Exactly 5% P&L = eligible (>= threshold)."""
        assert is_scale_up_eligible(BUILDING, 0.05) is True


# ── 3. Edge Cases ─────────────────────────────────────────────────────────

class TestEdgeCases:
    """Missing data, boundary values."""

    def test_missing_momentum_velocity(self):
        row = _row(MomentumVelocity_State=None)
        result = compute_wave_phase(row)
        assert result.state == UNKNOWN
        assert result.data_complete is False

    def test_unknown_momentum_velocity(self):
        row = _row(MomentumVelocity_State='UNKNOWN')
        result = compute_wave_phase(row)
        assert result.state == UNKNOWN

    def test_not_applicable_momentum(self):
        """Stock positions get NOT_APPLICABLE."""
        row = _row(MomentumVelocity_State='NOT_APPLICABLE')
        result = compute_wave_phase(row)
        assert result.state == UNKNOWN

    def test_missing_trend_integrity(self):
        """Missing trend integrity should still classify based on momentum."""
        row = _row(TrendIntegrity_State=None)
        result = compute_wave_phase(row)
        assert result.state == FORMING  # ACCELERATING + no trend = FORMING

    def test_nan_sma_distance(self):
        """NaN SMA distance defaults to 0.0 — not overextended."""
        row = _row(sma_distance_pct=float('nan'))
        result = compute_wave_phase(row)
        assert result.state == BUILDING  # default 0.0 = near SMA

    def test_sma_extension_boundary_12pct(self):
        """Exactly at 12% boundary — should be PEAKING for TRENDING."""
        row = _row(MomentumVelocity_State='TRENDING',
                   sma_distance_pct=0.12)
        result = compute_wave_phase(row)
        assert result.state == PEAKING  # > threshold (strict >)

    def test_sma_extension_just_below_12pct(self):
        """Just below 12% — should be BUILDING for TRENDING + strong trend
        but >5% so not near SMA for BUILDING second condition."""
        row = _row(MomentumVelocity_State='TRENDING',
                   TrendIntegrity_State='STRONG_TREND',
                   sma_distance_pct=0.11)
        result = compute_wave_phase(row)
        # TRENDING + STRONG_TREND but sma_dist > 5% → not BUILDING
        # Not overextended (< 12%), not RSI extreme → FORMING fallback
        assert result.state in (FORMING, BUILDING, PEAKING)


# ── 4. Real-World Scenarios ──────────────────────────────────────────────

class TestRealScenarios:
    """Scenarios based on observed position behavior."""

    def test_nvda_ranging_stalled(self):
        """NVDA: ranging market, no trend, stalling momentum — STALLED."""
        row = _row(
            MomentumVelocity_State='STALLING',
            TrendIntegrity_State='NO_TREND',
            PriceStructure_State='RANGE_BOUND',
            sma_distance_pct=-0.005,
        )
        result = compute_wave_phase(row)
        assert result.state == STALLED

    def test_winning_bw_building(self):
        """Winning buy-write: trending + strong + near SMA — BUILDING."""
        row = _row(
            MomentumVelocity_State='ACCELERATING',
            TrendIntegrity_State='STRONG_TREND',
            PriceStructure_State='STRUCTURAL_UP',
            sma_distance_pct=0.04,
            PnL_Pct=0.15,
        )
        result = compute_wave_phase(row)
        assert result.state == BUILDING

    def test_late_cycle_winner_fading(self):
        """Winner with LATE_CYCLE momentum — FADING, don't add."""
        row = _row(
            MomentumVelocity_State='LATE_CYCLE',
            TrendIntegrity_State='STRONG_TREND',
            sma_distance_pct=0.10,
            PnL_Pct=0.30,
        )
        result = compute_wave_phase(row)
        assert result.state == FADING
        # Scale-up should be blocked
        assert is_scale_up_eligible(result.state, 0.30) is False

    def test_bouncing_from_low_recovering(self):
        """Position bouncing from drawdown — RECOVERING."""
        row = _row(
            MomentumVelocity_State='ACCELERATING',
            RecoveryQuality_State='STRUCTURAL_RECOVERY',
            PnL_Pct=-0.05,
        )
        result = compute_wave_phase(row)
        assert result.state == RECOVERING

    def test_put_side_negative_sma_distance(self):
        """Put position: stock below SMA20 (negative SMA distance) — BUILDING if confirmed."""
        row = _row(
            MomentumVelocity_State='ACCELERATING',
            TrendIntegrity_State='STRONG_TREND',
            PriceStructure_State='STRUCTURAL_DOWN',
            sma_distance_pct=-0.04,  # stock below SMA20 (bearish = good for puts)
        )
        result = compute_wave_phase(row)
        assert result.state == BUILDING  # abs(sma_dist) < 12%
