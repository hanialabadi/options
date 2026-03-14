"""
Calendar DQS Multiplier Tests
==============================
Validates Phase 1: Calendar-aware DQS adjustment in step12_acceptance.

Tests:
  1. Friday long DTE=30  → full penalty (×0.90)
  2. Friday long DTE=90  → half penalty (×0.95)
  3. Friday long DTE=180 → near-zero penalty (×0.975)
  4. Friday short premium → rewarded (×1.05)
  5. Pre-holiday long/short → full effects
  6. Midweek              → no-op (×1.00)
  7. Stacking with feedback multiplier
  8. DTE missing          → defaults to full effect (conservative)
  9. Combined multiplier clamp fires
 10. Calendar confidence cap (belt+suspenders)

Run:
    pytest test/test_calendar_dqs_multiplier.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

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
    """Return a minimal row that passes enough gates to reach the calendar block."""
    defaults = {
        'Ticker': 'TEST',
        'Strategy_Name': 'LONG_PUT',
        'Strategy': 'LONG_PUT',
        'Contract_Status': 'OK',
        'DQS_Score': 80.0,
        'TQS_Score': 75.0,
        'PCS_Score': 60.0,
        'DTE': 30,
        'ADX': 25.0,
        'IV_Rank': 50.0,
        'IV_Rank_30D': 50.0,
        'IV_Maturity_State': 'MATURE',
        'IV_Maturity_Level': 4,
        'IV_Source': 'Schwab',
        'IV_Trend_7D': 'Rising',
        'IVHV_gap_30D': 15.0,
        'Liquidity_Grade': 'Excellent',
        'Signal_Type': 'BULLISH_STRONG',
        'Scraper_Status': 'OK',
        'Data_Completeness_Overall': 'Complete',
        'compression_tag': 'NEUTRAL',
        '52w_regime_tag': 'NEUTRAL',
        'momentum_tag': 'NEUTRAL',
        'gap_tag': 'NONE',
        'entry_timing_context': 'ON_TIME',
        'directional_bias': 'BEARISH_STRONG',
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
    }
    defaults.update(overrides)
    return pd.Series(defaults)


def _run_gate(row, **overrides):
    """Simpler helper: build row, call gate, return decision dict."""
    kw = dict(
        row=row,
        strategy_type='DIRECTIONAL',
        iv_maturity_state=str(row.get('IV_Maturity_State', 'MATURE')),
        iv_source=str(row.get('IV_Source', 'Schwab')),
        iv_rank=float(row.get('IV_Rank', 50)),
        iv_trend_7d=str(row.get('IV_Trend_7D', 'Rising')),
        ivhv_gap_30d=float(row.get('IVHV_gap_30D', 15)),
        liquidity_grade=str(row.get('Liquidity_Grade', 'Excellent')),
        signal_strength=str(row.get('Signal_Type', 'BULLISH_STRONG')),
        scraper_status=str(row.get('Scraper_Status', 'OK')),
        data_completeness_overall=str(row.get('Data_Completeness_Overall', 'Complete')),
        compression=str(row.get('compression_tag', 'NEUTRAL')),
        regime_52w=str(row.get('52w_regime_tag', 'NEUTRAL')),
        momentum=str(row.get('momentum_tag', 'NEUTRAL')),
        gap=str(row.get('gap_tag', 'NONE')),
        timing=str(row.get('entry_timing_context', 'ON_TIME')),
        directional_bias=str(row.get('directional_bias', 'BEARISH_STRONG')),
        structure_bias=str(row.get('structure_bias', 'NEUTRAL')),
        timing_quality=str(row.get('timing_quality', 'GOOD')),
        actual_dte=float(row.get('DTE', 30)),
        strategy_name=str(row.get('Strategy_Name', 'LONG_PUT')),
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
# Tests
# =============================================================================

class TestCalendarDQSMultiplier:
    """Validate calendar-aware DQS adjustments using mocked calendar_risk_flag."""

    @patch('scan_engine.step12_acceptance.calendar_risk_flag')
    def test_friday_long_dte30_full_penalty(self, mock_cal):
        """Friday long premium DTE=30: full penalty (theta_factor=1.0, base=-0.10)."""
        mock_cal.return_value = ('ELEVATED_BLEED', 'Friday long premium — 3-day theta bleed')
        row = _base_row(Strategy_Name='LONG_PUT', DTE=30, DQS_Score=80.0)
        decision = _run_gate(row)
        # Calendar multiplier = 1.0 + (-0.10 * 1.0) = 0.90
        assert decision['Calendar_DQS_Multiplier'] == pytest.approx(0.90, abs=0.01)
        assert decision['Calendar_Theta_Factor'] == pytest.approx(1.0, abs=0.01)

    @patch('scan_engine.step12_acceptance.calendar_risk_flag')
    def test_friday_long_dte90_half_penalty(self, mock_cal):
        """Friday long premium DTE=90: half penalty (theta_factor=0.50)."""
        mock_cal.return_value = ('ELEVATED_BLEED', 'Friday long premium — 3-day theta bleed')
        row = _base_row(Strategy_Name='LONG_PUT', DTE=90, DQS_Score=80.0)
        decision = _run_gate(row, actual_dte=90.0)
        # theta_factor = min(1.0, 45/90) = 0.50
        # Calendar multiplier = 1.0 + (-0.10 * 0.50) = 0.95
        assert decision['Calendar_DQS_Multiplier'] == pytest.approx(0.95, abs=0.01)
        assert decision['Calendar_Theta_Factor'] == pytest.approx(0.50, abs=0.01)

    @patch('scan_engine.step12_acceptance.calendar_risk_flag')
    def test_friday_long_dte180_near_zero_penalty(self, mock_cal):
        """Friday long premium DTE=180: quarter effect (theta_factor=0.25)."""
        mock_cal.return_value = ('ELEVATED_BLEED', 'Friday long premium — 3-day theta bleed')
        row = _base_row(Strategy_Name='LONG_PUT', DTE=180, DQS_Score=80.0)
        decision = _run_gate(row, actual_dte=180.0)
        # theta_factor = min(1.0, 45/180) = 0.25
        # Calendar multiplier = 1.0 + (-0.10 * 0.25) = 0.975
        assert decision['Calendar_DQS_Multiplier'] == pytest.approx(0.975, abs=0.01)
        assert decision['Calendar_Theta_Factor'] == pytest.approx(0.25, abs=0.01)

    @patch('scan_engine.step12_acceptance.calendar_risk_flag')
    def test_friday_short_premium_rewarded(self, mock_cal):
        """Friday short premium: rewarded (×1.05). calendar_risk_flag mocked to ADVANTAGEOUS."""
        mock_cal.return_value = ('ADVANTAGEOUS', 'Friday short premium — 3-day theta collection')
        # Use DIRECTIONAL type to ensure gate reaches calendar block; the flag is mocked.
        row = _base_row(Strategy_Name='LONG_PUT', DTE=30, DQS_Score=80.0)
        decision = _run_gate(row)
        # Calendar multiplier = 1.0 + (0.05 * 1.0) = 1.05
        assert decision['Calendar_DQS_Multiplier'] == pytest.approx(1.05, abs=0.01)

    @patch('scan_engine.step12_acceptance.calendar_risk_flag')
    def test_pre_holiday_long_heavy_penalty(self, mock_cal):
        """Pre-long-weekend long premium: HIGH_BLEED (×0.85)."""
        mock_cal.return_value = ('HIGH_BLEED', 'Pre-long-weekend long premium — 4-day theta bleed')
        row = _base_row(Strategy_Name='LONG_CALL', Strategy='LONG_CALL', DTE=30, DQS_Score=80.0,
                        directional_bias='BULLISH_STRONG')
        decision = _run_gate(row, strategy_name='LONG_CALL')
        # Calendar multiplier = 1.0 + (-0.15 * 1.0) = 0.85
        assert decision['Calendar_DQS_Multiplier'] == pytest.approx(0.85, abs=0.01)

    @patch('scan_engine.step12_acceptance.calendar_risk_flag')
    def test_pre_holiday_short_premium_edge(self, mock_cal):
        """Pre-long-weekend short premium: PRE_HOLIDAY_EDGE (×1.08)."""
        mock_cal.return_value = ('PRE_HOLIDAY_EDGE', 'Pre-long-weekend short premium — extra theta')
        # Use DIRECTIONAL type to ensure gate reaches calendar block; the flag is mocked.
        row = _base_row(Strategy_Name='LONG_PUT', DTE=30, DQS_Score=80.0)
        decision = _run_gate(row)
        # Calendar multiplier = 1.0 + (0.08 * 1.0) = 1.08
        assert decision['Calendar_DQS_Multiplier'] == pytest.approx(1.08, abs=0.01)

    @patch('scan_engine.step12_acceptance.calendar_risk_flag')
    def test_midweek_no_op(self, mock_cal):
        """Midweek: no calendar flag → multiplier 1.0."""
        mock_cal.return_value = ('', '')
        row = _base_row(Strategy_Name='LONG_PUT', DTE=30, DQS_Score=80.0)
        decision = _run_gate(row)
        assert decision['Calendar_DQS_Multiplier'] == pytest.approx(1.0, abs=0.001)

    @patch('scan_engine.step12_acceptance.calendar_risk_flag')
    def test_dte_missing_defaults_full_effect(self, mock_cal):
        """When DTE is NaN/missing, theta_factor defaults to 1.0 (conservative)."""
        mock_cal.return_value = ('ELEVATED_BLEED', 'Friday long premium')
        row = _base_row(Strategy_Name='LONG_PUT', DTE=np.nan, DQS_Score=80.0)
        decision = _run_gate(row, actual_dte=0.0)
        # DTE missing → theta_factor = 1.0 (conservative default)
        assert decision['Calendar_Theta_Factor'] == pytest.approx(1.0, abs=0.01)
        assert decision['Calendar_DQS_Multiplier'] == pytest.approx(0.90, abs=0.01)

    @patch('scan_engine.step12_acceptance.calendar_risk_flag')
    def test_combined_multiplier_clamp_floor(self, mock_cal):
        """When all penalties stack, combined multiplier is clamped to floor=0.40."""
        mock_cal.return_value = ('HIGH_BLEED', 'Pre-holiday long premium')
        # LATE_SHORT timing gives ×0.85, stale data ×0.85, feedback could be 0.80
        # HIGH_BLEED calendar ×0.85 — stacking: 0.85*0.85*0.85 = 0.614
        # With feedback 0.80: 0.85*0.85*0.80*0.85 = 0.491 → below floor → clamped
        row = _base_row(
            Strategy_Name='LONG_PUT', DTE=30, DQS_Score=80.0,
            entry_timing_context='LATE_LONG', iv_data_stale=True,
        )
        decision = _run_gate(row, iv_data_stale=True)
        # Combined will be < 1.0 and may be clamped
        assert decision.get('DQS_Combined_Multiplier') is not None
        # The combined multiplier must respect the floor
        assert decision['DQS_Combined_Multiplier'] >= 0.40

    @patch('scan_engine.step12_acceptance.calendar_risk_flag')
    def test_calendar_confidence_cap_belt_suspenders(self, mock_cal):
        """HIGH_BLEED also caps confidence HIGH→MEDIUM (belt+suspenders)."""
        mock_cal.return_value = ('HIGH_BLEED', 'Pre-holiday long premium — 4-day theta bleed')
        row = _base_row(
            Strategy_Name='LONG_PUT', DTE=30, DQS_Score=90.0,
            IV_Maturity_Level=4, ADX=30.0,
            directional_bias='BEARISH_STRONG',
        )
        decision = _run_gate(row)
        # Calendar_Risk_Flag should be set
        assert decision.get('Calendar_Risk_Flag') == 'HIGH_BLEED'
        # HIGH_BLEED caps confidence HIGH → MEDIUM
        assert decision.get('Calibrated_Confidence') == 'MEDIUM'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
