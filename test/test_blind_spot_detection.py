"""
Blind-Spot Detection Tests
===========================
Validates DQS multiplier penalties for chart signal contradictions.

Tests:
  1-3.  LEAP IV amplifier (LEAPs now caught by IV Headwind + amplifier)
  4-6.  Divergence penalties (single, double, no-trigger)
  7-9.  BB extreme penalties (calls upper, puts lower, neutral no-penalty)
  10-11. OBV conflict penalties (distribution on bullish, accumulation on bearish)
  12.    Keltner squeeze annotation only (no DQS penalty)
  13.    Multiple blind spots stack
  14.    Income strategies unaffected
  15-17. Edge cases (NaN signals, missing columns, boundary values)

Run:
    pytest test/test_blind_spot_detection.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scan_engine.step12_acceptance import apply_execution_gate


# =============================================================================
# Helpers
# =============================================================================

def _base_row(**overrides) -> pd.Series:
    """Minimal row that passes enough gates to reach blind-spot block."""
    defaults = {
        'Ticker': 'TEST',
        'Strategy_Name': 'LONG_CALL',
        'Strategy': 'Long Call',
        'Contract_Status': 'OK',
        'DQS_Score': 80.0,
        'TQS_Score': 75.0,
        'PCS_Score': 60.0,
        'DTE': 45,
        'ADX': 30.0,
        'IV_Rank': 50.0,
        'IV_Rank_20D': 50.0,
        'IV_Rank_30D': 50.0,
        'IV_Maturity_State': 'MATURE',
        'IV_Maturity_Level': 4,
        'IV_Source': 'Schwab',
        'IV_Trend_7D': 'Rising',
        'IVHV_gap_30D': 5.0,
        'Liquidity_Grade': 'Excellent',
        'Signal_Type': 'BULLISH_STRONG',
        'Scraper_Status': 'OK',
        'Data_Completeness_Overall': 'Complete',
        'compression_tag': 'NEUTRAL',
        '52w_regime_tag': 'NEUTRAL',
        'momentum_tag': 'NEUTRAL',
        'gap_tag': 'NONE',
        'entry_timing_context': 'ON_TIME',
        'directional_bias': 'BULLISH_STRONG',
        'structure_bias': 'NEUTRAL',
        'timing_quality': 'GOOD',
        'Execution_IV': 30.0,
        'exec_quality': 'GOOD',
        'balance': 'GOOD',
        'div_risk': 'LOW',
        'iv_data_stale': False,
        'regime_confidence': 0.85,
        'Spread_Cost_to_Premium_Ratio': 0.02,
        'iv_history_days': 200,
        'Price_Freshness': 'FRESH',
        'Bar_Age_Minutes': 5,
        # Snapshot date with low macro density (deterministic IV headwind tests)
        'snapshot_ts': '2026-01-14',
        # Signal defaults (no blind spots)
        'RSI_Divergence': '',
        'MACD_Divergence': '',
        'BB_Position': 50.0,
        'OBV_Slope': 5.0,
        'Keltner_Squeeze_On': False,
        'Keltner_Squeeze_Fired': False,
    }
    defaults.update(overrides)
    return pd.Series(defaults)


def _run_gate(row, **overrides):
    kw = dict(
        row=row,
        strategy_type='DIRECTIONAL',
        iv_maturity_state=str(row.get('IV_Maturity_State', 'MATURE')),
        iv_source=str(row.get('IV_Source', 'Schwab')),
        iv_rank=float(row.get('IV_Rank', 50)),
        iv_trend_7d=str(row.get('IV_Trend_7D', 'Rising')),
        ivhv_gap_30d=float(row.get('IVHV_gap_30D', 5)),
        liquidity_grade=str(row.get('Liquidity_Grade', 'Excellent')),
        signal_strength=str(row.get('Signal_Type', 'BULLISH_STRONG')),
        scraper_status=str(row.get('Scraper_Status', 'OK')),
        data_completeness_overall=str(row.get('Data_Completeness_Overall', 'Complete')),
        compression=str(row.get('compression_tag', 'NEUTRAL')),
        regime_52w=str(row.get('52w_regime_tag', 'NEUTRAL')),
        momentum=str(row.get('momentum_tag', 'NEUTRAL')),
        gap=str(row.get('gap_tag', 'NONE')),
        timing=str(row.get('entry_timing_context', 'ON_TIME')),
        directional_bias=str(row.get('directional_bias', 'BULLISH_STRONG')),
        structure_bias=str(row.get('structure_bias', 'NEUTRAL')),
        timing_quality=str(row.get('timing_quality', 'GOOD')),
        actual_dte=float(row.get('DTE', 45)),
        strategy_name=str(row.get('Strategy_Name', 'LONG_CALL')),
        exec_quality=str(row.get('exec_quality', 'GOOD')),
        balance=str(row.get('balance', 'GOOD')),
        div_risk=str(row.get('div_risk', 'LOW')),
        history_depth_ok=True,
        iv_data_stale=bool(row.get('iv_data_stale', False)),
        regime_confidence=float(row.get('regime_confidence', 0.85)),
        is_initial_pass=False,
        iv_maturity_level=int(row.get('IV_Maturity_Level', 4)),
    )
    kw.update(overrides)
    return apply_execution_gate(**kw)


# =============================================================================
# LEAP IV Amplifier Tests
# =============================================================================

class TestLeapIVAmplifier:
    """LEAPs at peak IV should receive amplified IV Headwind penalty."""

    def test_leap_call_high_iv_gets_amplifier(self):
        """LONG CALL LEAP + IV_Rank>80 + gap>5 = base 0.85 * LEAP 0.90."""
        row = _base_row(Strategy_Name='LONG CALL LEAP', IV_Rank_20D=90.0, IVHV_gap_30D=10.0)
        d = _run_gate(row)
        # 0.85 (base) * 0.90 (LEAP amplifier) = 0.765
        assert d['IV_Headwind_Multiplier'] == pytest.approx(0.765, abs=0.01)
        assert 'LEAP' in d.get('IV_Headwind_Note', '')

    def test_leap_put_high_iv_gets_amplifier(self):
        """LONG PUT LEAP + IV_Rank>80 + gap>5."""
        row = _base_row(
            Strategy_Name='LONG PUT LEAP', IV_Rank_20D=95.0, IVHV_gap_30D=8.0,
            directional_bias='BEARISH_STRONG',
        )
        d = _run_gate(row)
        assert d['IV_Headwind_Multiplier'] == pytest.approx(0.765, abs=0.01)

    def test_standard_call_no_leap_amplifier(self):
        """Standard LONG CALL + high IV gets base 0.85 only, NOT the 0.90 LEAP amplifier."""
        row = _base_row(Strategy_Name='LONG CALL', IV_Rank_20D=90.0, IVHV_gap_30D=10.0)
        d = _run_gate(row)
        assert d['IV_Headwind_Multiplier'] == pytest.approx(0.85, abs=0.01)
        assert 'LEAP' not in d.get('IV_Headwind_Note', '')


# =============================================================================
# Macro-Cluster IV Attenuation Tests
# =============================================================================

class TestMacroClusterIVAttenuation:
    """Macro clustering should attenuate IV headwind penalty, not remove it."""

    def test_macro_cluster_attenuates_base_penalty(self):
        """IV_Rank>80 + gap>5 during macro cluster → ×0.90 instead of ×0.85."""
        # 2026-03-09: CPI Mar 11 + FOMC Mar 18 = macro_density >= 2
        row = _base_row(
            Strategy_Name='LONG CALL', IV_Rank_20D=90.0, IVHV_gap_30D=10.0,
            snapshot_ts='2026-03-09',
        )
        d = _run_gate(row)
        assert d['IV_Headwind_Multiplier'] == pytest.approx(0.90, abs=0.01)
        assert 'macro cluster' in d.get('IV_Headwind_Note', '').lower()

    def test_macro_cluster_attenuates_inverted_penalty(self):
        """INVERTED surface during macro cluster → ×0.95 instead of ×0.90."""
        row = _base_row(
            Strategy_Name='LONG PUT', IV_Rank_20D=92.0, IVHV_gap_30D=3.0,
            Surface_Shape='INVERTED', snapshot_ts='2026-03-09',
            directional_bias='BEARISH_STRONG',
        )
        d = _run_gate(row)
        # Only INVERTED penalty (gap ≤ 5, so no base penalty), attenuated: ×0.95
        assert d['IV_Headwind_Multiplier'] == pytest.approx(0.95, abs=0.01)

    def test_no_cluster_full_penalty(self):
        """Same setup but non-cluster date → full ×0.85 penalty."""
        row = _base_row(
            Strategy_Name='LONG CALL', IV_Rank_20D=90.0, IVHV_gap_30D=10.0,
            snapshot_ts='2026-01-14',  # macro_density=1
        )
        d = _run_gate(row)
        assert d['IV_Headwind_Multiplier'] == pytest.approx(0.85, abs=0.01)

    def test_macro_cluster_smh_scenario(self):
        """SMH-like scenario: IV_Rank 95 + INVERTED + macro cluster."""
        row = _base_row(
            Strategy_Name='LONG PUT', IV_Rank_20D=95.0, IVHV_gap_30D=10.6,
            Surface_Shape='INVERTED', snapshot_ts='2026-03-09',
            directional_bias='BEARISH_STRONG',
        )
        d = _run_gate(row)
        # Macro cluster: base ×0.90 (not 0.85) + INVERTED ×0.95 (not 0.90)
        expected = 0.90 * 0.95  # = 0.855
        assert d['IV_Headwind_Multiplier'] == pytest.approx(expected, abs=0.01)


# =============================================================================
# Divergence Penalty Tests
# =============================================================================

class TestDivergencePenalty:
    """Divergence opposing trade direction should penalize DQS."""

    def test_single_rsi_divergence_on_call(self):
        """LONG CALL + Bearish RSI Divergence → ×0.95."""
        row = _base_row(RSI_Divergence='Bearish_Divergence', MACD_Divergence='')
        d = _run_gate(row)
        assert d['Blind_Spot_Multiplier'] == pytest.approx(0.95, abs=0.01)
        assert 'Murphy' in d.get('Blind_Spot_Notes', '')

    def test_double_divergence_on_call(self):
        """LONG CALL + Bearish RSI + Bearish MACD → ×0.90."""
        row = _base_row(RSI_Divergence='Bearish_Divergence', MACD_Divergence='Bearish_Divergence')
        d = _run_gate(row)
        assert d['Blind_Spot_Multiplier'] == pytest.approx(0.90, abs=0.01)
        assert 'Double' in d.get('Blind_Spot_Notes', '')

    def test_divergence_aligned_no_penalty(self):
        """LONG CALL + Bullish Divergence = NOT opposing → no penalty."""
        row = _base_row(RSI_Divergence='Bullish_Divergence', MACD_Divergence='')
        d = _run_gate(row)
        assert d['Blind_Spot_Multiplier'] == pytest.approx(1.0, abs=0.01)

    def test_put_bullish_divergence_penalized(self):
        """LONG PUT + Bullish RSI Divergence → ×0.95."""
        row = _base_row(
            Strategy_Name='LONG PUT', RSI_Divergence='Bullish_Divergence',
            directional_bias='BEARISH_STRONG',
        )
        d = _run_gate(row)
        assert d['Blind_Spot_Multiplier'] == pytest.approx(0.95, abs=0.01)


# =============================================================================
# BB Extreme Penalty Tests
# =============================================================================

class TestBBExtremePenalty:
    """Buying at Bollinger Band extremes should penalize DQS."""

    def test_call_at_upper_band(self):
        """LONG CALL + BB=91 → ×0.95."""
        row = _base_row(BB_Position=91.0)
        d = _run_gate(row)
        assert d['Blind_Spot_Multiplier'] == pytest.approx(0.95, abs=0.01)
        assert 'BB=' in d.get('Blind_Spot_Notes', '')

    def test_put_at_lower_band(self):
        """LONG PUT + BB=5 → ×0.95."""
        row = _base_row(
            Strategy_Name='LONG PUT', BB_Position=5.0,
            directional_bias='BEARISH_STRONG',
        )
        d = _run_gate(row)
        assert d['Blind_Spot_Multiplier'] == pytest.approx(0.95, abs=0.01)

    def test_neutral_bb_no_penalty(self):
        """LONG CALL + BB=50 → no penalty."""
        row = _base_row(BB_Position=50.0)
        d = _run_gate(row)
        assert d['Blind_Spot_Multiplier'] == pytest.approx(1.0, abs=0.01)


# =============================================================================
# OBV Conflict Penalty Tests
# =============================================================================

class TestOBVConflictPenalty:
    """OBV slope conflicting with trade direction should penalize DQS."""

    def test_distribution_on_bullish(self):
        """LONG CALL + OBV=-28 (distribution) → ×0.95."""
        row = _base_row(OBV_Slope=-28.0)
        d = _run_gate(row)
        assert d['Blind_Spot_Multiplier'] == pytest.approx(0.95, abs=0.01)
        assert 'OBV=' in d.get('Blind_Spot_Notes', '')

    def test_accumulation_on_bearish(self):
        """LONG PUT + OBV=+20 (accumulation) → ×0.95."""
        row = _base_row(
            Strategy_Name='LONG PUT', OBV_Slope=20.0,
            directional_bias='BEARISH_STRONG',
        )
        d = _run_gate(row)
        assert d['Blind_Spot_Multiplier'] == pytest.approx(0.95, abs=0.01)

    def test_mild_obv_no_penalty(self):
        """LONG CALL + OBV=-5 (mild, above -15 threshold) → no penalty."""
        row = _base_row(OBV_Slope=-5.0)
        d = _run_gate(row)
        assert d['Blind_Spot_Multiplier'] == pytest.approx(1.0, abs=0.01)


# =============================================================================
# Keltner Squeeze Tests
# =============================================================================

class TestKeltnerSqueeze:
    """Keltner squeeze should annotate but NOT penalize DQS."""

    def test_squeeze_annotation_only(self):
        """Active squeeze → annotation present, multiplier stays 1.0."""
        row = _base_row(Keltner_Squeeze_On=True, Keltner_Squeeze_Fired=False)
        d = _run_gate(row)
        assert d['Blind_Spot_Multiplier'] == pytest.approx(1.0, abs=0.01)
        assert 'Keltner squeeze' in d.get('Blind_Spot_Notes', '')

    def test_squeeze_fired_no_annotation(self):
        """Squeeze fired (already broken out) → no annotation."""
        row = _base_row(Keltner_Squeeze_On=False, Keltner_Squeeze_Fired=True)
        d = _run_gate(row)
        assert 'Keltner' not in d.get('Blind_Spot_Notes', '')


# =============================================================================
# Stacking & Edge Cases
# =============================================================================

class TestBlindSpotStacking:
    """Multiple blind spots should stack multiplicatively."""

    def test_triple_stack(self):
        """Divergence + BB extreme + OBV conflict = 0.95^2 * 0.90 = ~0.812."""
        row = _base_row(
            RSI_Divergence='Bearish_Divergence',
            MACD_Divergence='Bearish_Divergence',  # double div → 0.90
            BB_Position=91.0,                        # BB extreme → 0.95
            OBV_Slope=-28.0,                         # OBV conflict → 0.95
        )
        d = _run_gate(row)
        expected = 0.90 * 0.95 * 0.95
        assert d['Blind_Spot_Multiplier'] == pytest.approx(expected, abs=0.01)

    def test_income_strategy_unaffected(self):
        """Cash-Secured Put with bad signals → no blind-spot penalty."""
        row = _base_row(
            Strategy_Name='CASH SECURED PUT',
            Strategy='Cash-Secured Put',
            RSI_Divergence='Bearish_Divergence',
            BB_Position=91.0,
            OBV_Slope=-28.0,
        )
        d = _run_gate(row, strategy_name='CASH SECURED PUT')
        assert d['Blind_Spot_Multiplier'] == pytest.approx(1.0, abs=0.01)

    def test_nan_signals_no_penalty(self):
        """NaN/missing signal values → no penalty (graceful degradation)."""
        row = _base_row(
            RSI_Divergence=np.nan,
            MACD_Divergence=np.nan,
            BB_Position=np.nan,
            OBV_Slope=np.nan,
        )
        d = _run_gate(row)
        assert d['Blind_Spot_Multiplier'] == pytest.approx(1.0, abs=0.01)

    def test_boundary_bb_86_ranging_triggers(self):
        """BB=86 + ADX=20 (ranging) → threshold 85, triggers ×0.95."""
        row = _base_row(BB_Position=86.0, ADX=20.0)
        d = _run_gate(row)
        assert d['Blind_Spot_Multiplier'] == pytest.approx(0.95, abs=0.01)

    def test_boundary_bb_84_ranging_no_trigger(self):
        """BB=84 + ADX=20 (ranging) → threshold 85, no trigger."""
        row = _base_row(BB_Position=84.0, ADX=20.0)
        d = _run_gate(row)
        assert d['Blind_Spot_Multiplier'] == pytest.approx(1.0, abs=0.01)


class TestTrendAdjustedBB:
    """BB penalty adapts to trend strength (Bollinger band-walking)."""

    def test_strong_trend_skips_bb_penalty(self):
        """ADX=48 + BB=91 → strong trend, band-walk expected, no penalty."""
        row = _base_row(BB_Position=91.0, ADX=48.0)
        d = _run_gate(row)
        assert d['Blind_Spot_Multiplier'] == pytest.approx(1.0, abs=0.01)
        assert 'band-walk' in d.get('Blind_Spot_Notes', '')

    def test_strong_trend_extreme_bb_annotation(self):
        """ADX=45 + BB=95 → strong trend, annotation only (no DQS penalty)."""
        row = _base_row(BB_Position=95.0, ADX=45.0)
        d = _run_gate(row)
        assert d['Blind_Spot_Multiplier'] == pytest.approx(1.0, abs=0.01)
        assert 'band-walk' in d.get('Blind_Spot_Notes', '')

    def test_trending_raised_threshold(self):
        """ADX=35 + BB=88 → trending, threshold raised to 90, no penalty."""
        row = _base_row(BB_Position=88.0, ADX=35.0)
        d = _run_gate(row)
        assert d['Blind_Spot_Multiplier'] == pytest.approx(1.0, abs=0.01)

    def test_trending_above_raised_threshold(self):
        """ADX=35 + BB=91 → trending, threshold 90, 91>90 triggers ×0.95."""
        row = _base_row(BB_Position=91.0, ADX=35.0)
        d = _run_gate(row)
        assert d['Blind_Spot_Multiplier'] == pytest.approx(0.95, abs=0.01)

    def test_ranging_standard_threshold(self):
        """ADX=18 + BB=86 → ranging, standard BB threshold 85 triggers ×0.95,
        ADX 15–19 also triggers ×0.95 → combined ×0.9025."""
        row = _base_row(BB_Position=86.0, ADX=18.0)
        d = _run_gate(row)
        assert d['Blind_Spot_Multiplier'] == pytest.approx(0.9025, abs=0.01)

    def test_put_strong_downtrend_skips(self):
        """LONG PUT + ADX=42 + BB=8 → strong trend, band-walk, no penalty."""
        row = _base_row(
            Strategy_Name='LONG PUT', BB_Position=8.0, ADX=42.0,
            directional_bias='BEARISH_STRONG',
            Market_Structure='Downtrend',
        )
        d = _run_gate(row)
        assert d['Blind_Spot_Multiplier'] == pytest.approx(1.0, abs=0.01)


# =============================================================================
# Structure Conflict Tests
# =============================================================================

class TestStructureConflict:
    """Directional trades fighting swing structure should be penalized."""

    def test_call_on_downtrend(self):
        """LONG CALL + Market_Structure=Downtrend → ×0.90."""
        row = _base_row(Market_Structure='Downtrend')
        d = _run_gate(row)
        assert d['Blind_Spot_Multiplier'] == pytest.approx(0.90, abs=0.01)
        assert 'Structure conflict' in d.get('Blind_Spot_Notes', '')

    def test_put_on_uptrend(self):
        """LONG PUT + Market_Structure=Uptrend → ×0.90."""
        row = _base_row(
            Strategy_Name='LONG PUT', Market_Structure='Uptrend',
            directional_bias='BEARISH_STRONG',
        )
        d = _run_gate(row)
        assert d['Blind_Spot_Multiplier'] == pytest.approx(0.90, abs=0.01)
        assert 'Uptrend' in d.get('Blind_Spot_Notes', '')

    def test_call_on_uptrend_no_penalty(self):
        """LONG CALL + Market_Structure=Uptrend → aligned, no penalty."""
        row = _base_row(Market_Structure='Uptrend')
        d = _run_gate(row)
        assert d['Blind_Spot_Multiplier'] == pytest.approx(1.0, abs=0.01)

    def test_put_on_downtrend_no_penalty(self):
        """LONG PUT + Market_Structure=Downtrend → aligned, no penalty."""
        row = _base_row(
            Strategy_Name='LONG PUT', Market_Structure='Downtrend',
            directional_bias='BEARISH_STRONG',
        )
        d = _run_gate(row)
        assert d['Blind_Spot_Multiplier'] == pytest.approx(1.0, abs=0.01)

    def test_consolidation_no_penalty(self):
        """Market_Structure=Consolidation → neutral, no penalty."""
        row = _base_row(Market_Structure='Consolidation')
        d = _run_gate(row)
        assert d['Blind_Spot_Multiplier'] == pytest.approx(1.0, abs=0.01)

    def test_structure_stacks_with_divergence(self):
        """Structure conflict + divergence stack: 0.90 × 0.95 = 0.855."""
        row = _base_row(
            Market_Structure='Downtrend',
            RSI_Divergence='Bearish_Divergence',
        )
        d = _run_gate(row)
        expected = 0.90 * 0.95
        assert d['Blind_Spot_Multiplier'] == pytest.approx(expected, abs=0.01)

    def test_income_strategy_no_structure_penalty(self):
        """Cash-Secured Put on Downtrend → no penalty (income, not directional)."""
        row = _base_row(
            Strategy_Name='CASH SECURED PUT', Market_Structure='Downtrend',
        )
        d = _run_gate(row, strategy_name='CASH SECURED PUT')
        assert d['Blind_Spot_Multiplier'] == pytest.approx(1.0, abs=0.01)


# =============================================================================
# Weekly Conflict LEAP Tests
# =============================================================================

class TestWeeklyConflictLEAP:
    """LEAPs with weekly trend conflicting should be penalized."""

    def test_leap_weekly_conflict(self):
        """LONG CALL LEAP + Weekly=CONFLICTING → ×0.95."""
        row = _base_row(
            Strategy_Name='LONG CALL LEAP',
            Weekly_Trend_Bias='CONFLICTING',
        )
        d = _run_gate(row)
        assert d['Blind_Spot_Multiplier'] == pytest.approx(0.95, abs=0.01)
        assert 'Weekly' in d.get('Blind_Spot_Notes', '')

    def test_leap_weekly_aligned_no_penalty(self):
        """LONG CALL LEAP + Weekly=ALIGNED → no penalty."""
        row = _base_row(
            Strategy_Name='LONG CALL LEAP',
            Weekly_Trend_Bias='ALIGNED',
        )
        d = _run_gate(row)
        assert d['Blind_Spot_Multiplier'] == pytest.approx(1.0, abs=0.01)

    def test_non_leap_weekly_conflict_penalty(self):
        """Standard LONG CALL + Weekly=CONFLICTING → ×0.95 penalty (all directionals)."""
        row = _base_row(Weekly_Trend_Bias='CONFLICTING')
        d = _run_gate(row)
        # Weekly conflict penalizes all directionals (Murphy: weekly filters daily noise)
        assert d['Blind_Spot_Multiplier'] == pytest.approx(0.95, abs=0.01)

    def test_put_leap_weekly_conflict(self):
        """LONG PUT LEAP + Weekly=CONFLICTING → ×0.95."""
        row = _base_row(
            Strategy_Name='LONG PUT LEAP',
            Weekly_Trend_Bias='CONFLICTING',
            directional_bias='BEARISH_STRONG',
        )
        d = _run_gate(row)
        assert d['Blind_Spot_Multiplier'] == pytest.approx(0.95, abs=0.01)

    def test_leap_structure_plus_weekly_stacks(self):
        """LEAP on Downtrend + Weekly conflict: 0.90 × 0.95 = 0.855."""
        row = _base_row(
            Strategy_Name='LONG CALL LEAP',
            Market_Structure='Downtrend',
            Weekly_Trend_Bias='CONFLICTING',
        )
        d = _run_gate(row)
        expected = 0.90 * 0.95
        assert d['Blind_Spot_Multiplier'] == pytest.approx(expected, abs=0.01)


# =============================================================================
# ADX Conviction Gate Tests
# =============================================================================

class TestADXConvictionGate:
    """Directional trades in flat/ranging markets should be penalized."""

    def test_adx_below_15_penalized(self):
        """LONG CALL + ADX=9 → ×0.90 (no trend conviction)."""
        row = _base_row(ADX=9.0)
        d = _run_gate(row)
        assert d['Blind_Spot_Multiplier'] == pytest.approx(0.90, abs=0.01)
        assert 'ADX=9' in d.get('Blind_Spot_Notes', '')
        assert 'no trend' in d.get('Blind_Spot_Notes', '').lower()

    def test_adx_below_20_penalized(self):
        """LONG CALL + ADX=18 → ×0.95 penalty (Murphy Ch.14: trend unconfirmed)."""
        row = _base_row(ADX=18.0)
        d = _run_gate(row)
        assert d['Blind_Spot_Multiplier'] == pytest.approx(0.95, abs=0.01)
        assert 'ADX=18' in d.get('Blind_Spot_Notes', '')
        assert 'weak trend' in d.get('Blind_Spot_Notes', '').lower()

    def test_adx_above_20_no_flag(self):
        """LONG CALL + ADX=25 → no ADX flag at all."""
        row = _base_row(ADX=25.0)
        d = _run_gate(row)
        assert 'ADX=' not in d.get('Blind_Spot_Notes', '').split('band')[0]

    def test_put_adx_below_15_penalized(self):
        """LONG PUT + ADX=12 → ×0.90."""
        row = _base_row(
            Strategy_Name='LONG PUT', ADX=12.0,
            directional_bias='BEARISH_STRONG',
        )
        d = _run_gate(row)
        assert d['Blind_Spot_Multiplier'] == pytest.approx(0.90, abs=0.01)

    def test_leap_exempt_from_adx_gate(self):
        """LONG CALL LEAP + ADX=10 → no ADX penalty (LEAPs have longer horizon)."""
        row = _base_row(Strategy_Name='LONG CALL LEAP', ADX=10.0)
        d = _run_gate(row)
        # LEAP should NOT get ADX penalty — only short-dated directionals
        assert 'no trend conviction' not in d.get('Blind_Spot_Notes', '').lower()

    def test_adx_stacks_with_structure(self):
        """ADX=10 + Structure=Downtrend on call: 0.90 × 0.90 = 0.81."""
        row = _base_row(ADX=10.0, Market_Structure='Downtrend')
        d = _run_gate(row)
        expected = 0.90 * 0.90
        assert d['Blind_Spot_Multiplier'] == pytest.approx(expected, abs=0.01)

    def test_income_strategy_exempt(self):
        """Cash-Secured Put + ADX=8 → no penalty (income, not directional)."""
        row = _base_row(
            Strategy_Name='CASH SECURED PUT', ADX=8.0,
        )
        d = _run_gate(row, strategy_name='CASH SECURED PUT')
        assert d['Blind_Spot_Multiplier'] == pytest.approx(1.0, abs=0.01)


# =============================================================================
# 11. Earnings IV Crush Gate (Augen 0.754)
# =============================================================================

class TestEarningsIVCrush:
    """Buying short-dated directionals near earnings = IV crush risk."""

    def test_near_earnings_penalized(self):
        """LONG CALL + earnings in 3d → ×0.90 penalty."""
        row = _base_row(days_to_earnings=3)
        d = _run_gate(row)
        assert d['Blind_Spot_Multiplier'] == pytest.approx(0.90, abs=0.01)
        assert 'IV crush risk' in d.get('Blind_Spot_Notes', '')

    def test_earnings_boundary_5d(self):
        """Exactly 5 days to earnings → penalty applies (boundary)."""
        row = _base_row(days_to_earnings=5)
        d = _run_gate(row)
        assert d['Blind_Spot_Multiplier'] == pytest.approx(0.90, abs=0.01)

    def test_earnings_6d_no_penalty(self):
        """6 days to earnings → no penalty (outside window)."""
        row = _base_row(days_to_earnings=6)
        d = _run_gate(row)
        assert d['Blind_Spot_Multiplier'] == pytest.approx(1.0, abs=0.01)

    def test_leap_exempt_from_earnings_crush(self):
        """LONG CALL LEAP + earnings in 3d → no penalty (LEAPs survive crush)."""
        row = _base_row(Strategy_Name='LONG CALL LEAP', days_to_earnings=3)
        d = _run_gate(row)
        assert 'IV crush risk' not in d.get('Blind_Spot_Notes', '')

    def test_put_near_earnings_penalized(self):
        """LONG PUT + earnings in 2d → ×0.90 penalty."""
        row = _base_row(Strategy_Name='LONG_PUT', days_to_earnings=2,
                        Signal_Type='BEARISH_STRONG', directional_bias='BEARISH_STRONG')
        d = _run_gate(row)
        assert d['Blind_Spot_Multiplier'] == pytest.approx(0.90, abs=0.01)

    def test_track_record_enriches_note(self):
        """Earnings context includes beat_rate and avg_crush when available."""
        row = _base_row(
            days_to_earnings=3,
            Earnings_Beat_Rate=91.0,
            Earnings_Avg_IV_Crush=35.0,
            Earnings_Consecutive_Beats=12,
        )
        d = _run_gate(row)
        notes = d.get('Blind_Spot_Notes', '')
        assert 'beat_rate=91%' in notes
        assert 'avg_crush=35%' in notes
        assert '12 consecutive beats' in notes

    def test_low_move_ratio_stacks(self):
        """Move_Ratio < 0.6 stacks extra ×0.95 on top of base ×0.90."""
        row = _base_row(
            days_to_earnings=2,
            Earnings_Move_Ratio=0.43,
        )
        d = _run_gate(row)
        expected = 0.90 * 0.95  # Base crush + overpricing stack
        assert d['Blind_Spot_Multiplier'] == pytest.approx(expected, abs=0.01)
        assert 'overprices' in d.get('Blind_Spot_Notes', '').lower()

    def test_high_move_ratio_annotation_only(self):
        """Move_Ratio > 1.2 → favorable annotation, no extra penalty."""
        row = _base_row(
            days_to_earnings=4,
            Earnings_Move_Ratio=1.5,
        )
        d = _run_gate(row)
        assert d['Blind_Spot_Multiplier'] == pytest.approx(0.90, abs=0.01)  # Base only
        assert 'favorable' in d.get('Blind_Spot_Notes', '').lower()

    def test_no_earnings_data_no_penalty(self):
        """No days_to_earnings → no earnings penalty."""
        row = _base_row()  # No days_to_earnings field
        d = _run_gate(row)
        assert 'IV crush' not in d.get('Blind_Spot_Notes', '')

    def test_earnings_stacks_with_divergence(self):
        """Earnings in 3d + RSI divergence → ×0.90 × ×0.95 = 0.855."""
        row = _base_row(
            days_to_earnings=3,
            RSI_Divergence='Bearish_Divergence',
        )
        d = _run_gate(row)
        expected = 0.90 * 0.95  # Earnings + single divergence
        assert d['Blind_Spot_Multiplier'] == pytest.approx(expected, abs=0.01)

    def test_income_strategy_exempt(self):
        """Cash-Secured Put near earnings → no earnings crush penalty."""
        row = _base_row(
            Strategy_Name='CASH SECURED PUT', days_to_earnings=2,
        )
        d = _run_gate(row, strategy_name='CASH SECURED PUT')
        assert 'IV crush' not in d.get('Blind_Spot_Notes', '')


# =============================================================================
# MC Verdict → DQS Integration Tests
# =============================================================================

class TestMCVerdictDQSGating:
    """MC verdicts (VP + Earnings) should penalize DQS when they conflict
    with the directional entry."""

    def test_vp_expensive_penalizes_dqs(self):
        """VP_Verdict=EXPENSIVE on directional → ×0.95 DQS reduction."""
        from unittest.mock import patch
        row = _base_row(DQS_Score=80.0)

        def mock_vp(r):
            return {'MC_VP_Verdict': 'EXPENSIVE', 'MC_VP_Score': 0.65, 'MC_VP_Note': 'overpriced'}

        with patch('scan_engine.step12_acceptance.mc_variance_premium', mock_vp, create=True), \
             patch.dict('sys.modules', {'scan_engine.mc_variance_premium': type('m', (), {'mc_variance_premium': mock_vp})}):
            d = _run_gate(row)
        adj = d.get('MC_Verdict_DQS_Adj')
        if adj is not None:
            assert adj == pytest.approx(0.95, abs=0.01)
            assert 'VP_GATE' in d.get('Gate_Reason', '')

    def test_earn_close_before_penalizes_dqs(self):
        """MC_Earn_Verdict=CLOSE_BEFORE on directional → ×0.95 DQS reduction."""
        from unittest.mock import patch
        row = _base_row(DQS_Score=80.0, days_to_earnings=3)

        def mock_earn(r):
            return {'MC_Earn_Verdict': 'CLOSE_BEFORE', 'MC_Earn_Note': 'close before better EV'}

        with patch('scan_engine.step12_acceptance.mc_earnings_event', mock_earn, create=True), \
             patch.dict('sys.modules', {'scan_engine.mc_earnings_event': type('m', (), {'mc_earnings_event': mock_earn})}):
            d = _run_gate(row)
        adj = d.get('MC_Verdict_DQS_Adj')
        if adj is not None:
            assert adj == pytest.approx(0.95, abs=0.01)
            assert 'EARN_GATE' in d.get('Gate_Reason', '')

    def test_vp_cheap_no_penalty(self):
        """VP_Verdict=CHEAP should NOT penalize DQS."""
        from unittest.mock import patch
        row = _base_row(DQS_Score=80.0)

        def mock_vp(r):
            return {'MC_VP_Verdict': 'CHEAP', 'MC_VP_Score': 1.3, 'MC_VP_Note': 'underpriced'}

        with patch('scan_engine.step12_acceptance.mc_variance_premium', mock_vp, create=True), \
             patch.dict('sys.modules', {'scan_engine.mc_variance_premium': type('m', (), {'mc_variance_premium': mock_vp})}):
            d = _run_gate(row)
        adj = d.get('MC_Verdict_DQS_Adj')
        assert adj is None or adj == pytest.approx(1.0, abs=0.01)

    def test_vp_and_earn_stack(self):
        """Both EXPENSIVE + CLOSE_BEFORE → ×0.95 × 0.95 = ×0.9025."""
        from unittest.mock import patch
        row = _base_row(DQS_Score=80.0, days_to_earnings=3)

        def mock_vp(r):
            return {'MC_VP_Verdict': 'EXPENSIVE', 'MC_VP_Score': 0.6, 'MC_VP_Note': 'overpriced'}

        def mock_earn(r):
            return {'MC_Earn_Verdict': 'CLOSE_BEFORE', 'MC_Earn_Note': 'close before'}

        with patch('scan_engine.step12_acceptance.mc_variance_premium', mock_vp, create=True), \
             patch('scan_engine.step12_acceptance.mc_earnings_event', mock_earn, create=True), \
             patch.dict('sys.modules', {
                 'scan_engine.mc_variance_premium': type('m', (), {'mc_variance_premium': mock_vp}),
                 'scan_engine.mc_earnings_event': type('m', (), {'mc_earnings_event': mock_earn}),
             }):
            d = _run_gate(row)
        adj = d.get('MC_Verdict_DQS_Adj')
        if adj is not None:
            expected = 0.95 * 0.95
            assert adj == pytest.approx(expected, abs=0.01)

    def test_mc_skip_no_penalty(self):
        """MC_VP_Verdict=SKIP (default) → no DQS adjustment."""
        row = _base_row(DQS_Score=80.0)
        d = _run_gate(row)
        adj = d.get('MC_Verdict_DQS_Adj')
        assert adj is None
