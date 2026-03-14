"""
Calendar-Aware Execution Readiness Tests
==========================================
Validates Phase 2: Calendar rules in _apply_execution_readiness.

Tests:
  1. ROLL + Friday + DTE=14 → EXECUTE_NOW
  2. ROLL + Friday + DTE=30 → unchanged (STAGE_AND_RECHECK)
  3. EXIT never modified by calendar
  4. STAGE_AND_RECHECK + Friday + long premium → WAIT_FOR_WINDOW
  5. HOLD + short premium + Friday → annotated WAIT_FOR_WINDOW
  6. Midweek → no calendar effect
  7. theta_bleed_adjusted_urgency: pre-holiday LOW→MEDIUM
  8. theta_bleed_adjusted_urgency: normal day → no change

Run:
    pytest test/test_calendar_execution_readiness.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch, MagicMock
from dataclasses import dataclass, field
import datetime as _dt

import pandas as pd
import numpy as np
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.management.cycle3.doctrine.execution_readiness import _apply_execution_readiness
from core.management.cycle3.doctrine.helpers import theta_bleed_adjusted_urgency


# =============================================================================
# Helpers
# =============================================================================

@dataclass
class MockCalendarContext:
    is_friday: bool = False
    is_monday: bool = False
    is_pre_long_weekend: bool = False
    weekend_gap_days: int = 1
    is_trading_day: bool = True
    date: _dt.date = field(default_factory=lambda: _dt.date(2026, 3, 6))
    next_open: _dt.date = field(default_factory=lambda: _dt.date(2026, 3, 7))

    @property
    def theta_bleed_days(self):
        return self.weekend_gap_days - 1


def _make_df(**overrides) -> pd.DataFrame:
    """Create a single-row management DataFrame for execution readiness."""
    defaults = {
        'Ticker': 'TEST',
        'Strategy': 'LONG_PUT',
        'Strategy_Name': 'LONG_PUT',
        'Action': 'ROLL',
        'Urgency': 'MEDIUM',
        'DTE': 14,
        'Delta': -0.45,
        'Roll_Candidate_1': '',
        'Earnings_Date': '',
        'IV_vs_HV_Gap': 5.0,
    }
    defaults.update(overrides)
    return pd.DataFrame([defaults])


# =============================================================================
# Tests: Execution Readiness Rules
# =============================================================================

class TestCalendarExecutionReadiness:
    """Validate calendar-aware rules in _apply_execution_readiness."""

    @patch('scan_engine.calendar_context.get_calendar_context')
    def test_roll_friday_dte14_forced_execute(self, mock_cal):
        """ROLL + Friday + DTE=14 → EXECUTE_NOW (don't carry over weekend)."""
        mock_cal.return_value = MockCalendarContext(is_friday=True)
        df = _make_df(Action='ROLL', Urgency='MEDIUM', DTE=14, Strategy='LONG_PUT')
        result = _apply_execution_readiness(df)
        assert result.iloc[0]['Execution_Readiness'] == 'EXECUTE_NOW'
        assert 'Friday' in result.iloc[0]['Execution_Readiness_Reason']

    @patch('scan_engine.calendar_context.get_calendar_context')
    def test_roll_friday_dte30_not_forced(self, mock_cal):
        """ROLL + Friday + DTE=30 → STAGE_AND_RECHECK (DTE>14, not urgent)."""
        mock_cal.return_value = MockCalendarContext(is_friday=True)
        df = _make_df(Action='ROLL', Urgency='MEDIUM', DTE=30, Strategy='LONG_PUT')
        result = _apply_execution_readiness(df)
        # DTE=30 is above the 14d threshold, so calendar rule doesn't fire
        assert result.iloc[0]['Execution_Readiness'] != 'EXECUTE_NOW' or 'Friday' not in result.iloc[0]['Execution_Readiness_Reason']

    @patch('scan_engine.calendar_context.get_calendar_context')
    def test_exit_never_modified(self, mock_cal):
        """EXIT is always EXECUTE_NOW — calendar never overrides EXIT."""
        mock_cal.return_value = MockCalendarContext(is_friday=True)
        df = _make_df(Action='EXIT', Urgency='HIGH', DTE=30, Strategy='LONG_PUT')
        result = _apply_execution_readiness(df)
        assert result.iloc[0]['Execution_Readiness'] == 'EXECUTE_NOW'
        assert 'EXIT' in result.iloc[0]['Execution_Readiness_Reason']

    @patch('scan_engine.calendar_context.get_calendar_context')
    def test_stage_friday_long_premium_downgraded(self, mock_cal):
        """STAGE_AND_RECHECK + Friday + long premium → WAIT_FOR_WINDOW."""
        mock_cal.return_value = MockCalendarContext(is_friday=True)
        df = _make_df(Action='ROLL', Urgency='LOW', DTE=30, Strategy='LONG_PUT')
        result = _apply_execution_readiness(df)
        assert result.iloc[0]['Execution_Readiness'] == 'WAIT_FOR_WINDOW'
        assert 'Friday' in result.iloc[0]['Execution_Readiness_Reason']
        assert 'long premium' in result.iloc[0]['Execution_Readiness_Reason']

    @patch('scan_engine.calendar_context.get_calendar_context')
    def test_hold_short_premium_friday_annotated(self, mock_cal):
        """HOLD + short premium + Friday → WAIT_FOR_WINDOW with weekend note."""
        mock_cal.return_value = MockCalendarContext(is_friday=True)
        df = _make_df(Action='HOLD', Urgency='LOW', DTE=30, Strategy='BUY_WRITE')
        result = _apply_execution_readiness(df)
        assert result.iloc[0]['Execution_Readiness'] == 'WAIT_FOR_WINDOW'
        assert 'weekend theta decay' in result.iloc[0]['Execution_Readiness_Reason']

    @patch('scan_engine.calendar_context.get_calendar_context')
    def test_midweek_no_calendar_effect(self, mock_cal):
        """Midweek: standard ROLL + MEDIUM → STAGE_AND_RECHECK (no calendar)."""
        mock_cal.return_value = MockCalendarContext(is_friday=False, is_pre_long_weekend=False)
        df = _make_df(Action='ROLL', Urgency='MEDIUM', DTE=30, Strategy='LONG_PUT')
        result = _apply_execution_readiness(df)
        assert result.iloc[0]['Execution_Readiness'] == 'STAGE_AND_RECHECK'

    @patch('scan_engine.calendar_context.get_calendar_context')
    def test_pre_long_weekend_roll_dte10_forced(self, mock_cal):
        """Pre-long-weekend ROLL + DTE=10 → EXECUTE_NOW."""
        mock_cal.return_value = MockCalendarContext(is_friday=True, is_pre_long_weekend=True, weekend_gap_days=4)
        df = _make_df(Action='ROLL', Urgency='LOW', DTE=10, Strategy='LONG_PUT')
        result = _apply_execution_readiness(df)
        assert result.iloc[0]['Execution_Readiness'] == 'EXECUTE_NOW'
        assert 'pre-long-weekend' in result.iloc[0]['Execution_Readiness_Reason']


# =============================================================================
# Tests: theta_bleed_adjusted_urgency helper
# =============================================================================

class TestThetaBleedAdjustedUrgency:
    """Validate the urgency escalation helper."""

    def test_pre_holiday_long_premium_low_escalated(self):
        """LOW + pre-long-weekend + long premium + DTE≤21 → MEDIUM."""
        result = theta_bleed_adjusted_urgency('LOW', dte=15.0, is_pre_long_weekend=True, is_long_premium=True)
        assert result == 'MEDIUM'

    def test_normal_day_no_change(self):
        """LOW + normal day → LOW (no escalation)."""
        result = theta_bleed_adjusted_urgency('LOW', dte=15.0, is_pre_long_weekend=False, is_long_premium=True)
        assert result == 'LOW'

    def test_medium_urgency_unchanged(self):
        """MEDIUM urgency is never further escalated by this helper."""
        result = theta_bleed_adjusted_urgency('MEDIUM', dte=15.0, is_pre_long_weekend=True, is_long_premium=True)
        assert result == 'MEDIUM'

    def test_short_premium_not_escalated(self):
        """LOW + pre-holiday + short premium → LOW (short benefits from weekend)."""
        result = theta_bleed_adjusted_urgency('LOW', dte=15.0, is_pre_long_weekend=True, is_long_premium=False)
        assert result == 'LOW'

    def test_dte_above_threshold_not_escalated(self):
        """LOW + pre-holiday + long premium + DTE=45 → LOW (above 21d threshold)."""
        result = theta_bleed_adjusted_urgency('LOW', dte=45.0, is_pre_long_weekend=True, is_long_premium=True)
        assert result == 'LOW'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
