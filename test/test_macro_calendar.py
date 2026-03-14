"""
Tests for the Macro-Awareness Layer — config/macro_calendar.py + Section 3.0f decision modifier.

12 tests across 4 classes:
  TestMacroProximity (4)     — proximity computation from static calendar
  TestFormatMacroStrip (2)   — human-readable strip formatting
  TestDecisionModification (5) — strategy-aware urgency/action changes
  TestGracefulDegradation (1)  — safe defaults on edge cases
"""

import unittest
from datetime import date

import numpy as np
import pandas as pd

from config.macro_calendar import (
    MacroEvent,
    MacroProximity,
    get_macro_proximity,
    format_macro_strip,
)


class TestMacroProximity(unittest.TestCase):
    """Core proximity computation from the static 2026 calendar."""

    def test_nfp_day_returns_zero_days(self):
        """Mar 6 2026 is NFP day — days_to_next should be 0."""
        p = get_macro_proximity(date(2026, 3, 6))
        self.assertIsNotNone(p.next_event)
        self.assertEqual(p.next_event.event_type, "NFP")
        self.assertEqual(p.days_to_next, 0)

    def test_events_within_5d_includes_cpi(self):
        """Mar 6: NFP today + CPI Mar 11 (5d) should both be in events_within_5d."""
        p = get_macro_proximity(date(2026, 3, 6))
        types_5d = [e.event_type for e in p.events_within_5d]
        self.assertIn("NFP", types_5d)
        self.assertIn("CPI", types_5d)

    def test_is_macro_week_when_high_impact_within_5d(self):
        """Mar 6 has NFP (HIGH) today — is_macro_week should be True."""
        p = get_macro_proximity(date(2026, 3, 6))
        self.assertTrue(p.is_macro_week)

    def test_macro_density_counts_14d_events(self):
        """Mar 6: NFP (0d) + CPI (5d) + FOMC (12d) = density >= 3."""
        p = get_macro_proximity(date(2026, 3, 6))
        self.assertGreaterEqual(p.macro_density, 3)

    def test_mid_month_gap_no_macro_week(self):
        """Mar 19 (day after FOMC): next event is Mar 27 GDP/PCE (8d) — not macro week."""
        p = get_macro_proximity(date(2026, 3, 19))
        # Next HIGH-impact event should be > 5d away
        self.assertFalse(p.is_macro_week)


class TestFormatMacroStrip(unittest.TestCase):
    """Human-readable strip for dashboard display."""

    def test_strip_contains_event_types_and_days(self):
        """Strip should contain event type abbreviations and day counts."""
        p = get_macro_proximity(date(2026, 3, 6))
        strip = format_macro_strip(p)
        self.assertIn("NFP", strip)
        self.assertIn("0d", strip)
        self.assertIn("CPI", strip)

    def test_empty_strip_for_past_dates(self):
        """Date after all 2026 events should return empty strip."""
        p = get_macro_proximity(date(2027, 1, 1))
        strip = format_macro_strip(p)
        self.assertEqual(strip, "")


class TestDecisionModification(unittest.TestCase):
    """
    Test the decision modification rules from Section 3.0f.

    These tests simulate what run_all.py does: given a DataFrame with
    doctrine output + macro columns, apply the macro modifier rules.
    """

    def _apply_macro_modifier(self, df: pd.DataFrame) -> pd.DataFrame:
        """Replicate Section 3.0f logic on a test DataFrame."""
        _DIRECTIONAL = {'LONG_CALL', 'LONG_PUT', 'BUY_CALL', 'BUY_PUT', 'LEAPS_CALL', 'LEAPS_PUT'}
        _INCOME = {'BUY_WRITE', 'COVERED_CALL', 'CSP', 'CASH_SECURED_PUT'}
        _HIGH_IMPACT = {'FOMC', 'CPI', 'NFP'}

        df = df.copy()
        _dtm = pd.to_numeric(df['Days_To_Macro'], errors='coerce')
        _macro_type = df['Macro_Next_Type'].fillna('')
        _is_etf = df.get('Is_ETF', pd.Series([False] * len(df))).fillna(False)
        _strategy = df['Strategy'].fillna('').str.upper()
        _action = df['Action'].fillna('')
        _urgency = df['Urgency'].fillna('LOW')

        for idx in df.index:
            dtm_val = _dtm.get(idx)
            if pd.isna(dtm_val) or dtm_val > 5:
                continue

            dtm_int = int(dtm_val)
            evt_type = str(_macro_type.get(idx, ''))
            strat = str(_strategy.get(idx, ''))
            act = str(_action.get(idx, ''))
            urg = str(_urgency.get(idx, 'LOW'))
            etf_flag = bool(_is_etf.get(idx, False))
            is_high = evt_type in _HIGH_IMPACT

            is_directional = strat in _DIRECTIONAL or (etf_flag and strat not in _INCOME)
            is_income = strat in _INCOME

            if is_directional and is_high and dtm_int <= 3:
                if act == 'REVIEW':
                    df.at[idx, 'Action'] = 'EXIT'
                    df.at[idx, 'Urgency'] = 'MEDIUM'
                elif act == 'HOLD' and (urg == 'LOW' or (etf_flag and urg == 'MEDIUM')):
                    df.at[idx, 'Urgency'] = 'HIGH'

            elif etf_flag and dtm_int <= 3 and is_high and strat not in _INCOME:
                if act == 'HOLD' and urg in ('LOW', 'MEDIUM'):
                    df.at[idx, 'Urgency'] = 'HIGH'

        return df

    def test_d1_directional_review_exits_on_fomc_3d(self):
        """LONG_CALL + REVIEW + FOMC in 3d → EXIT MEDIUM."""
        df = pd.DataFrame([{
            'Strategy': 'LONG_CALL', 'Action': 'REVIEW', 'Urgency': 'LOW',
            'Days_To_Macro': 3, 'Macro_Next_Type': 'FOMC', 'Is_ETF': False,
            'Rationale': 'test',
        }])
        result = self._apply_macro_modifier(df)
        self.assertEqual(result.at[0, 'Action'], 'EXIT')
        self.assertEqual(result.at[0, 'Urgency'], 'MEDIUM')

    def test_d2_directional_hold_low_escalates_on_cpi_2d(self):
        """LONG_PUT + HOLD LOW + CPI in 2d → HOLD HIGH."""
        df = pd.DataFrame([{
            'Strategy': 'LONG_PUT', 'Action': 'HOLD', 'Urgency': 'LOW',
            'Days_To_Macro': 2, 'Macro_Next_Type': 'CPI', 'Is_ETF': False,
            'Rationale': 'test',
        }])
        result = self._apply_macro_modifier(df)
        self.assertEqual(result.at[0, 'Action'], 'HOLD')
        self.assertEqual(result.at[0, 'Urgency'], 'HIGH')

    def test_i1_income_hold_stays_hold_on_fomc_1d(self):
        """BUY_WRITE + HOLD LOW + FOMC in 1d → STAYS HOLD LOW (income benefits)."""
        df = pd.DataFrame([{
            'Strategy': 'BUY_WRITE', 'Action': 'HOLD', 'Urgency': 'LOW',
            'Days_To_Macro': 1, 'Macro_Next_Type': 'FOMC', 'Is_ETF': False,
            'Rationale': 'test',
        }])
        result = self._apply_macro_modifier(df)
        self.assertEqual(result.at[0, 'Action'], 'HOLD')
        self.assertEqual(result.at[0, 'Urgency'], 'LOW')

    def test_e1_etf_hold_escalates_on_nfp_1d(self):
        """ETF LONG_CALL + HOLD MEDIUM + NFP in 1d → HOLD HIGH."""
        df = pd.DataFrame([{
            'Strategy': 'LONG_CALL', 'Action': 'HOLD', 'Urgency': 'MEDIUM',
            'Days_To_Macro': 1, 'Macro_Next_Type': 'NFP', 'Is_ETF': True,
            'Rationale': 'test',
        }])
        result = self._apply_macro_modifier(df)
        self.assertEqual(result.at[0, 'Action'], 'HOLD')
        self.assertEqual(result.at[0, 'Urgency'], 'HIGH')

    def test_gdp_medium_impact_no_urgency_change(self):
        """LONG_CALL + HOLD LOW + GDP in 2d → STAYS HOLD LOW (MEDIUM impact)."""
        df = pd.DataFrame([{
            'Strategy': 'LONG_CALL', 'Action': 'HOLD', 'Urgency': 'LOW',
            'Days_To_Macro': 2, 'Macro_Next_Type': 'GDP', 'Is_ETF': False,
            'Rationale': 'test',
        }])
        result = self._apply_macro_modifier(df)
        self.assertEqual(result.at[0, 'Action'], 'HOLD')
        self.assertEqual(result.at[0, 'Urgency'], 'LOW')


class TestGracefulDegradation(unittest.TestCase):
    """Safe defaults when calendar has no upcoming events."""

    def test_no_upcoming_events_returns_safe_defaults(self):
        """Date beyond all 2026 events → all None/empty/False."""
        p = get_macro_proximity(date(2027, 6, 1))
        self.assertIsNone(p.next_event)
        self.assertIsNone(p.days_to_next)
        self.assertEqual(p.events_within_5d, [])
        self.assertEqual(p.events_within_14d, [])
        self.assertFalse(p.is_macro_week)
        self.assertEqual(p.macro_density, 0)


if __name__ == "__main__":
    unittest.main()
