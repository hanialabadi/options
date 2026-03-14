"""
Tests for strategy routing: correct family dispatch, no cross-calls.
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch

from scan_engine.evaluators import (
    DIRECTIONAL_STRATEGIES, VOLATILITY_STRATEGIES, INCOME_STRATEGIES,
)
from scan_engine.evaluators.income import evaluate_income as evaluate_income_fn
from scan_engine.evaluators.volatility import evaluate_volatility as evaluate_volatility_fn
from scan_engine.evaluators.directional import evaluate_directional as evaluate_directional_fn
from scan_engine.step8_independent_evaluation import _evaluate_row


def _base_row(**overrides):
    base = {
        'Contract_Status': 'OK',
        'Delta': 0.50, 'Gamma': 0.04, 'Vega': 0.30,
        'Theta': 0.03, 'Actual_DTE': 30,
        'Signal_Type': 'Bullish', 'Regime': 'Compression',
        'Trend': 'Bullish', 'Volume_Trend': 'Rising',
        'IVHV_gap_30D': 5.0, 'RV_IV_Ratio': 0.85,
        'Put_Call_Skew': 1.05, 'IV_Percentile': 40.0,
        'Stock_Price': 150.0,
        'Probability_Of_Profit': 72.0,
    }
    base.update(overrides)
    return pd.Series(base)


class TestRouting:
    def test_csp_routes_to_income(self):
        with patch('scan_engine.step8_independent_evaluation.evaluate_income', wraps=evaluate_income_fn) as mock:
            _evaluate_row(_base_row(Strategy_Name='Cash-Secured Put'))
            mock.assert_called_once()

    def test_straddle_routes_to_volatility(self):
        with patch('scan_engine.step8_independent_evaluation.evaluate_volatility', wraps=evaluate_volatility_fn) as mock:
            _evaluate_row(_base_row(Strategy_Name='Long Straddle'))
            mock.assert_called_once()

    def test_long_call_routes_to_directional(self):
        with patch('scan_engine.step8_independent_evaluation.evaluate_directional', wraps=evaluate_directional_fn) as mock:
            _evaluate_row(_base_row(Strategy_Name='Long Call'))
            mock.assert_called_once()

    def test_unknown_strategy_returns_watch(self):
        result = _evaluate_row(_base_row(Strategy_Name='Butterfly'))
        assert result[0] == 'Watch'  # validation_status
        assert 'not in known families' in result[4]

    def test_no_evaluator_cross_calls(self):
        """Income evaluator should NOT call vol evaluator, and vice versa."""
        with patch('scan_engine.step8_independent_evaluation.evaluate_volatility') as vol_mock:
            _evaluate_row(_base_row(Strategy_Name='Cash-Secured Put'))
            vol_mock.assert_not_called()

        with patch('scan_engine.step8_independent_evaluation.evaluate_income') as inc_mock:
            _evaluate_row(_base_row(Strategy_Name='Long Straddle'))
            inc_mock.assert_not_called()

    def test_neutral_strategy_handled(self):
        """Strategies not in any family get Watch, not crash."""
        result = _evaluate_row(_base_row(Strategy_Name='Iron Butterfly'))
        assert result[0] == 'Watch'


class TestFamilyClassification:
    def test_all_directional_classified(self):
        for s in DIRECTIONAL_STRATEGIES:
            r = _evaluate_row(_base_row(Strategy_Name=s))
            # Should not return "not in known families"
            assert 'not in known families' not in str(r[4])

    def test_all_volatility_classified(self):
        for s in VOLATILITY_STRATEGIES:
            r = _evaluate_row(_base_row(Strategy_Name=s))
            assert 'not in known families' not in str(r[4])

    def test_all_income_classified(self):
        for s in INCOME_STRATEGIES:
            r = _evaluate_row(_base_row(Strategy_Name=s))
            assert 'not in known families' not in str(r[4])
