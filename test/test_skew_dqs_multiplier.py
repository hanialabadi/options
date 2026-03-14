"""
CBOE SKEW DQS Multiplier Tests
================================
Validates SKEW-aware DQS adjustment in step12_acceptance.

SKEW measures market-wide tail risk pricing via OTM put premiums.
Elevated SKEW penalises long-vega entries (buying into expensive tails)
and rewards income sellers (collecting richer premiums).

Tests:
  1. Long call + SKEW 145 → mild penalty (×0.97)
  2. Long call + SKEW 155 → severe penalty (×0.93)
  3. Long put + SKEW 145 → mild penalty (×0.97)
  4. Income (BW) + SKEW 140 → boost (×1.03)
  5. Income (CSP) + SKEW 140 → boost (×1.03)
  6. Long call + SKEW 125 → no effect (below threshold)
  7. Income + SKEW 120 → no effect (below threshold)
  8. SKEW NaN → no effect (missing data)
  9. SKEW stacks with market regime multiplier
 10. SKEW output columns populated in decision

Run:
    pytest test/test_skew_dqs_multiplier.py -v
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
    """Minimal row that reaches the SKEW multiplier block."""
    defaults = {
        'Ticker': 'TEST',
        'Strategy_Name': 'LONG_CALL',
        'Strategy': 'LONG_CALL',
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
        'CBOE_SKEW': float('nan'),  # default: no SKEW data
        'Market_Regime': 'NORMAL',
    }
    defaults.update(overrides)
    return pd.Series(defaults)


def _run_gate(row, **overrides):
    """Build row, call gate, return decision dict."""
    kw = dict(
        row=row,
        strategy_type='DIRECTIONAL' if row.get('Strategy_Name', '') in (
            'LONG_CALL', 'LONG_PUT', 'LONG_CALL_LEAP', 'LONG_PUT_LEAP'
        ) else 'INCOME',
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
        directional_bias=str(row.get('directional_bias', 'BULLISH_STRONG')),
        structure_bias=str(row.get('structure_bias', 'NEUTRAL')),
        timing_quality=str(row.get('timing_quality', 'GOOD')),
        actual_dte=float(row.get('DTE', 30)),
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
# Tests
# =============================================================================

class TestSKEWDQSMultiplier:
    """Validate SKEW-aware DQS adjustments."""

    @patch('scan_engine.step12_acceptance.calendar_risk_flag')
    def test_long_call_skew_145_mild_penalty(self, mock_cal):
        """Long call + SKEW 145 → mild penalty ×0.97."""
        mock_cal.return_value = ('NONE', '')
        row = _base_row(Strategy_Name='LONG_CALL', CBOE_SKEW=145.0)
        decision = _run_gate(row)
        assert decision['SKEW_Multiplier'] == 0.97
        assert '145' in decision['SKEW_Note']

    @patch('scan_engine.step12_acceptance.calendar_risk_flag')
    def test_long_call_skew_155_severe_penalty(self, mock_cal):
        """Long call + SKEW 155 → severe penalty ×0.93."""
        mock_cal.return_value = ('NONE', '')
        row = _base_row(Strategy_Name='LONG_CALL', CBOE_SKEW=155.0)
        decision = _run_gate(row)
        assert decision['SKEW_Multiplier'] == 0.93
        assert '150' in decision['SKEW_Note']

    @patch('scan_engine.step12_acceptance.calendar_risk_flag')
    def test_long_put_skew_145_mild_penalty(self, mock_cal):
        """Long put also penalised — both long-vega strategies."""
        mock_cal.return_value = ('NONE', '')
        row = _base_row(Strategy_Name='LONG_PUT', CBOE_SKEW=145.0,
                        directional_bias='BEARISH_STRONG')
        decision = _run_gate(row)
        assert decision['SKEW_Multiplier'] == 0.97

    @patch('scan_engine.step12_acceptance.calendar_risk_flag')
    def test_income_bw_skew_140_boost(self, mock_cal):
        """Buy-write + SKEW 140 → income boost ×1.03."""
        mock_cal.return_value = ('NONE', '')
        row = _base_row(Strategy_Name='BUY-WRITE', CBOE_SKEW=140.0,
                        Strategy='BUY-WRITE')
        decision = _run_gate(row, strategy_name='BUY-WRITE')
        assert decision['SKEW_Multiplier'] == 1.03
        assert 'income' in decision['SKEW_Note'].lower()

    @patch('scan_engine.step12_acceptance.calendar_risk_flag')
    def test_income_csp_skew_140_boost(self, mock_cal):
        """Cash secured put + SKEW 140 → income boost ×1.03."""
        mock_cal.return_value = ('NONE', '')
        row = _base_row(Strategy_Name='CASH SECURED PUT', CBOE_SKEW=140.0,
                        Strategy='CASH SECURED PUT')
        decision = _run_gate(row, strategy_name='CASH SECURED PUT')
        assert decision['SKEW_Multiplier'] == 1.03

    @patch('scan_engine.step12_acceptance.calendar_risk_flag')
    def test_long_call_skew_125_no_effect(self, mock_cal):
        """SKEW 125 is normal range — no multiplier."""
        mock_cal.return_value = ('NONE', '')
        row = _base_row(Strategy_Name='LONG_CALL', CBOE_SKEW=125.0)
        decision = _run_gate(row)
        assert decision['SKEW_Multiplier'] == 1.0
        assert decision['SKEW_Note'] == ''

    @patch('scan_engine.step12_acceptance.calendar_risk_flag')
    def test_income_skew_120_no_effect(self, mock_cal):
        """SKEW 120 below income threshold — no boost."""
        mock_cal.return_value = ('NONE', '')
        row = _base_row(Strategy_Name='COVERED CALL', CBOE_SKEW=120.0,
                        Strategy='COVERED CALL')
        decision = _run_gate(row, strategy_name='COVERED CALL')
        assert decision['SKEW_Multiplier'] == 1.0

    @patch('scan_engine.step12_acceptance.calendar_risk_flag')
    def test_skew_nan_no_effect(self, mock_cal):
        """Missing SKEW data → no multiplier."""
        mock_cal.return_value = ('NONE', '')
        row = _base_row(Strategy_Name='LONG_CALL', CBOE_SKEW=float('nan'))
        decision = _run_gate(row)
        assert decision['SKEW_Multiplier'] == 1.0

    @patch('scan_engine.step12_acceptance.calendar_risk_flag')
    def test_skew_stacks_with_regime(self, mock_cal):
        """SKEW multiplier stacks with market regime multiplier."""
        mock_cal.return_value = ('NONE', '')
        row = _base_row(Strategy_Name='LONG_CALL', CBOE_SKEW=145.0,
                        Market_Regime='CAUTIOUS')
        decision = _run_gate(row)
        # SKEW ×0.97, regime ×0.95 → combined includes both
        combined = decision['DQS_Combined_Multiplier']
        assert combined < 0.97 * 0.96, \
            f"Combined {combined} should reflect both SKEW (0.97) and regime (0.95)"

    @patch('scan_engine.step12_acceptance.calendar_risk_flag')
    def test_skew_output_columns_populated(self, mock_cal):
        """SKEW decision columns always present."""
        mock_cal.return_value = ('NONE', '')
        row = _base_row(Strategy_Name='LONG_CALL', CBOE_SKEW=145.0)
        decision = _run_gate(row)
        assert 'SKEW_Multiplier' in decision
        assert 'SKEW_Note' in decision
        assert isinstance(decision['SKEW_Multiplier'], float)
        assert isinstance(decision['SKEW_Note'], str)
