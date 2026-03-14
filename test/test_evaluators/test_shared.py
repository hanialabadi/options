"""
Tests for shared utilities: safe_get, resolve_strategy_name,
check_required_data, contract_status_precheck.
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pytest
import pandas as pd
import numpy as np

from scan_engine.evaluators._shared import (
    safe_get, safe_float, resolve_strategy_name,
    check_required_data, contract_status_precheck,
)
from scan_engine.evaluators._types import EvaluationResult


class TestSafeGet:
    def test_returns_first_valid(self):
        row = pd.Series({'a': np.nan, 'b': 42, 'c': 99})
        assert safe_get(row, 'a', 'b', 'c') == 42

    def test_returns_default_when_all_nan(self):
        row = pd.Series({'a': np.nan, 'b': np.nan})
        assert safe_get(row, 'a', 'b', default='missing') == 'missing'

    def test_returns_default_when_key_absent(self):
        row = pd.Series({'x': 1})
        assert safe_get(row, 'z', default=None) is None

    def test_nan_is_not_truthy(self):
        """NaN must NOT pass through as a valid value."""
        row = pd.Series({'a': np.nan})
        assert safe_get(row, 'a') is None

    def test_zero_is_valid(self):
        row = pd.Series({'a': 0})
        assert safe_get(row, 'a') == 0

    def test_empty_string_is_valid(self):
        row = pd.Series({'a': ''})
        assert safe_get(row, 'a') == ''


class TestSafeFloat:
    def test_returns_float(self):
        row = pd.Series({'x': '3.14'})
        assert safe_float(row, 'x') == pytest.approx(3.14)

    def test_nan_returns_default(self):
        row = pd.Series({'x': np.nan})
        assert safe_float(row, 'x', default=-1) == -1


class TestResolveStrategyName:
    def test_strategy_name_first(self):
        row = pd.Series({'Strategy_Name': 'Long Call', 'Strategy': 'Bull Spread'})
        assert resolve_strategy_name(row) == 'Long Call'

    def test_falls_through_nan(self):
        row = pd.Series({'Strategy_Name': np.nan, 'Strategy': 'Bull Spread'})
        assert resolve_strategy_name(row) == 'Bull Spread'

    def test_returns_empty_default(self):
        row = pd.Series({'foo': 'bar'})
        assert resolve_strategy_name(row) == ''


class TestCheckRequiredData:
    def test_all_present(self):
        row = pd.Series({'Delta': 0.5, 'Gamma': 0.03})
        missing, pct = check_required_data(row, {'Delta': 'Delta', 'Gamma': 'Gamma'})
        assert missing == []
        assert pct == 100.0

    def test_partial_missing(self):
        row = pd.Series({'Delta': 0.5, 'Gamma': np.nan})
        missing, pct = check_required_data(row, {'Delta': 'Delta', 'Gamma': 'Gamma'})
        assert 'Gamma' in missing
        assert pct == 50.0


class TestContractStatusPrecheck:
    def test_ok_returns_none(self):
        row = pd.Series({'Contract_Status': 'OK'})
        assert contract_status_precheck(row) is None

    def test_leap_fallback_returns_none(self):
        row = pd.Series({'Contract_Status': 'LEAP_FALLBACK'})
        assert contract_status_precheck(row) is None

    def test_no_expirations_deferred(self):
        row = pd.Series({'Contract_Status': 'NO_EXPIRATIONS_IN_WINDOW', 'Failure_Reason': 'test'})
        r = contract_status_precheck(row)
        assert r.validation_status == 'Deferred_DTE'

    def test_failed_liquidity_off_hours_deferred(self):
        row = pd.Series({'Contract_Status': 'FAILED_LIQUIDITY_FILTER', 'Failure_Reason': 'test', 'is_market_open': False})
        r = contract_status_precheck(row)
        assert r.validation_status == 'Deferred_Liquidity'

    def test_failed_liquidity_market_open_rejects(self):
        row = pd.Series({'Contract_Status': 'FAILED_LIQUIDITY_FILTER', 'Failure_Reason': 'test', 'is_market_open': True})
        r = contract_status_precheck(row)
        assert r.validation_status == 'Reject'

    def test_no_chain_rejects(self):
        row = pd.Series({'Contract_Status': 'NO_CHAIN_RETURNED', 'Failure_Reason': 'test'})
        r = contract_status_precheck(row)
        assert r.validation_status == 'Reject'

    def test_unknown_status_rejects(self):
        row = pd.Series({'Contract_Status': 'SOMETHING_WEIRD'})
        r = contract_status_precheck(row)
        assert r.validation_status == 'Reject'
