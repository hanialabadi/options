"""Tests for detect_recovery_state in helpers.py — IV fallback chain."""

import math

import numpy as np
import pandas as pd
import pytest

from core.management.cycle3.doctrine.helpers import detect_recovery_state


def _recovery_row(**overrides):
    """Build a minimal row that qualifies for recovery mode."""
    data = {
        "Cumulative_Premium_Collected": 2.12,
        "Gross_Premium_Collected": 2.12,
        "_cycle_count": 3,
        "IV_Now": 1.065,       # 106.5% as decimal
        "IV_30D": 1.10,
        "IV_Contract": None,
        "IV_Underlying_30D": None,
        "IV_Rank": 50.0,
        "IV_Percentile": None,
        "CC_IV_Rank": None,
        "Thesis_State": "INTACT",
        "Equity_Integrity_State": "BROKEN",
        "Basis": 1490.0,
        "Quantity": 100,
    }
    data.update(overrides)
    return pd.Series(data)


class TestIVFallbackChain:
    """IV column cascade: IV_30D → IV_Underlying_30D → IV_Now → IV_Contract.

    Stock legs have IV_Now/IV_Contract = NaN by design (no contract IV).
    IV_30D (underlying ATM from iv_term_history) is checked first.
    """

    def test_iv_30d_used_first(self):
        row = _recovery_row(IV_30D=0.50, IV_Now=0.80)
        result = detect_recovery_state(row, spot=6.45, effective_cost=14.90)
        assert result["is_recovery"] is True

    def test_iv_underlying_30d_fallback(self):
        row = _recovery_row(IV_30D=None, IV_Underlying_30D=0.50, IV_Now=None)
        result = detect_recovery_state(row, spot=6.45, effective_cost=14.90)
        assert result["is_recovery"] is True

    def test_iv_now_fallback(self):
        """IV_Now as third fallback (populated on option legs)."""
        row = _recovery_row(IV_30D=None, IV_Underlying_30D=None, IV_Now=0.50)
        result = detect_recovery_state(row, spot=6.45, effective_cost=14.90)
        assert result["is_recovery"] is True

    def test_iv_contract_fallback(self):
        row = _recovery_row(IV_30D=None, IV_Underlying_30D=None,
                            IV_Now=None, IV_Contract=0.50)
        result = detect_recovery_state(row, spot=6.45, effective_cost=14.90)
        assert result["is_recovery"] is True

    def test_all_iv_nan_blocks_recovery(self):
        """Missing IV = no data to assess viability. Monitor flags this upstream."""
        row = _recovery_row(IV_Now=np.nan, IV_30D=np.nan,
                            IV_Contract=np.nan, IV_Underlying_30D=np.nan)
        result = detect_recovery_state(row, spot=6.45, effective_cost=14.90)
        assert result["is_recovery"] is False
        assert "permanently depressed" in result["exit_recovery"]

    def test_all_iv_none_blocks_recovery(self):
        """None IV = no data. Fix upstream (add ticker to iv_term_history)."""
        row = _recovery_row(IV_Now=None, IV_30D=None,
                            IV_Contract=None, IV_Underlying_30D=None)
        result = detect_recovery_state(row, spot=6.45, effective_cost=14.90)
        assert result["is_recovery"] is False
        assert "permanently depressed" in result["exit_recovery"]

    def test_truly_depressed_iv_blocks_recovery(self):
        """When IV IS present and below floor (0.15), recovery should be blocked."""
        row = _recovery_row(IV_30D=0.05, IV_Underlying_30D=0.05,
                            IV_Now=None, IV_Contract=None)  # 5% IV — genuinely depressed
        result = detect_recovery_state(row, spot=6.45, effective_cost=14.90)
        assert result["is_recovery"] is False
        assert "permanently depressed" in result["exit_recovery"]


class TestIVRankFallbackChain:
    """IV rank cascade: IV_Rank → IV_Percentile → CC_IV_Rank."""

    def test_iv_rank_used_first(self):
        row = _recovery_row(IV_Rank=60.0, IV_Percentile=20.0, CC_IV_Rank=10.0)
        result = detect_recovery_state(row, spot=6.45, effective_cost=14.90)
        assert result["is_recovery"] is True

    def test_iv_percentile_fallback(self):
        row = _recovery_row(IV_Rank=None, IV_Percentile=60.0)
        result = detect_recovery_state(row, spot=6.45, effective_cost=14.90)
        assert result["is_recovery"] is True

    def test_cc_iv_rank_fallback(self):
        row = _recovery_row(IV_Rank=None, IV_Percentile=None, CC_IV_Rank=60.0)
        result = detect_recovery_state(row, spot=6.45, effective_cost=14.90)
        assert result["is_recovery"] is True

    def test_all_rank_missing_defaults_to_50(self):
        row = _recovery_row(IV_Rank=None, IV_Percentile=None, CC_IV_Rank=None)
        result = detect_recovery_state(row, spot=6.45, effective_cost=14.90)
        # Default rank 50 > floor 5 → IV viable
        assert result["is_recovery"] is True


class TestIVNormalization:
    """Values > 5 are percentages (e.g. 106.5 → 1.065)."""

    def test_percentage_form_normalized(self):
        row = _recovery_row(IV_Now=106.5)  # 106.5%
        result = detect_recovery_state(row, spot=6.45, effective_cost=14.90)
        assert result["is_recovery"] is True

    def test_decimal_form_not_double_divided(self):
        row = _recovery_row(IV_Now=1.065)  # already decimal
        result = detect_recovery_state(row, spot=6.45, effective_cost=14.90)
        assert result["is_recovery"] is True


class TestRecoveryGuardrails:
    def test_thesis_broken_blocks_recovery(self):
        row = _recovery_row(Thesis_State="BROKEN")
        result = detect_recovery_state(row, spot=6.45, effective_cost=14.90)
        assert result["is_recovery"] is False
        assert "Thesis BROKEN" in result["exit_recovery"]

    def test_equity_broken_does_not_block(self):
        """Equity BROKEN is a symptom, not a guardrail."""
        row = _recovery_row(Equity_Integrity_State="BROKEN")
        result = detect_recovery_state(row, spot=6.45, effective_cost=14.90)
        assert result["is_recovery"] is True

    def test_shallow_loss_no_recovery(self):
        row = _recovery_row()
        result = detect_recovery_state(row, spot=12.0, effective_cost=14.90)
        # -19.5% < 25% threshold
        assert result["is_recovery"] is False

    def test_no_premium_no_recovery(self):
        row = _recovery_row(Cumulative_Premium_Collected=0, Gross_Premium_Collected=0)
        result = detect_recovery_state(row, spot=6.45, effective_cost=14.90)
        assert result["is_recovery"] is False

    def test_single_cycle_no_recovery(self):
        row = _recovery_row(_cycle_count=1)
        result = detect_recovery_state(row, spot=6.45, effective_cost=14.90)
        assert result["is_recovery"] is False
