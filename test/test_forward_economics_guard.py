"""Tests for forward-economics guards on hard stops.

Principle: sunk loss should not alone drive exit — forward EV is the primary lever.
Guards check forward income viability before firing EXIT on loss-based triggers.
"""

import math
import pytest
import pandas as pd
import numpy as np

from core.management.cycle3.doctrine.helpers import (
    compute_forward_income_economics,
    safe_row_float,
)
from core.management.cycle3.doctrine.thresholds import (
    FORWARD_ECON_MONTHS_BW_HARD_STOP,
    FORWARD_ECON_MONTHS_BW_APPROACHING,
    FORWARD_ECON_MONTHS_STOCK_DEEP_LOSS,
    FORWARD_ECON_IV_MIN_VIABLE,
    FORWARD_ECON_DRAWDOWN_THETA_OFFSET,
    FORWARD_ECON_THETA_ANNUAL_FLOOR,
)


# ── Helper ───────────────────────────────────────────────────────────────

def _make_row(**overrides) -> pd.Series:
    """Build a minimal doctrine row with sensible defaults."""
    base = {
        "Underlying_Ticker": "TEST",
        "IV_Now": 0.30,          # 30% IV (decimal)
        "IV_Rank": 50,
        "IV_30D": 0.30,
        "IV_Rank_30D": 50,
        "IV_Percentile": 50,
        "Short_Call_DTE": 30,
        "DTE": 30,
        "Premium_Entry": 2.50,
        "Last": 2.50,
        "Daily_Margin_Cost": 0.0,
        "Margin_Cost_Daily": 0.0,
        "Thesis_State": "INTACT",
        "Equity_Integrity_State": "INTACT",
        "Quantity": 100,
        "UL Last": 50.0,
        "Spot": 50.0,
        "_cycle_count": 1,
        "Cumulative_Premium_Collected": 0.0,
    }
    base.update(overrides)
    return pd.Series(base)


# ═════════════════════════════════════════════════════════════════════════
# 1. compute_forward_income_economics — Unit Tests
# ═════════════════════════════════════════════════════════════════════════

class TestForwardIncomeEconomics:
    """Test the shared helper that computes forward income viability."""

    def test_viable_with_good_iv(self):
        row = _make_row(IV_Now=0.30, Daily_Margin_Cost=0.05)
        result = compute_forward_income_economics(row, spot=50.0, effective_cost=60.0)
        assert result["viable"] is True
        assert result["monthly_income"] > 0
        assert result["net_monthly"] > 0
        assert result["gap_to_breakeven"] == 10.0
        assert result["months_to_breakeven"] < 100

    def test_not_viable_with_low_iv(self):
        row = _make_row(IV_Now=0.05, IV_30D=0.05)
        result = compute_forward_income_economics(row, spot=50.0, effective_cost=60.0)
        assert result["viable"] is False
        assert result["months_to_breakeven"] == float("inf")

    def test_not_viable_zero_spot(self):
        row = _make_row()
        result = compute_forward_income_economics(row, spot=0.0, effective_cost=60.0)
        assert result["viable"] is False

    def test_not_viable_zero_cost(self):
        row = _make_row()
        result = compute_forward_income_economics(row, spot=50.0, effective_cost=0.0)
        assert result["viable"] is False

    def test_margin_reduces_net_monthly(self):
        row_no_margin = _make_row(IV_Now=0.30, Daily_Margin_Cost=0.0)
        row_with_margin = _make_row(IV_Now=0.30, Daily_Margin_Cost=2.0)
        r1 = compute_forward_income_economics(row_no_margin, spot=50, effective_cost=60)
        r2 = compute_forward_income_economics(row_with_margin, spot=50, effective_cost=60)
        assert r2["net_monthly"] < r1["net_monthly"]
        assert r2["months_to_breakeven"] > r1["months_to_breakeven"]

    def test_margin_kills_viability(self):
        """If margin > income, net_monthly <= 0 and viable is False."""
        row = _make_row(IV_Now=0.16, Daily_Margin_Cost=50.0)  # huge margin
        result = compute_forward_income_economics(row, spot=50, effective_cost=60)
        assert result["viable"] is False

    def test_iv_percentage_normalization(self):
        """IV stored as percentage (e.g. 109.2) should be normalized."""
        row = _make_row(IV_Now=109.2, IV_30D=109.2)
        result = compute_forward_income_economics(row, spot=20.0, effective_cost=25.0)
        assert result["iv_now"] == pytest.approx(1.092)
        assert result["viable"] is True

    def test_no_gap_when_above_cost(self):
        """Spot > effective_cost → gap_to_breakeven <= 0 → months_to_breakeven = inf."""
        row = _make_row(IV_Now=0.30)
        result = compute_forward_income_economics(row, spot=70.0, effective_cost=60.0)
        assert result["gap_to_breakeven"] < 0
        assert result["months_to_breakeven"] == float("inf")


# ═════════════════════════════════════════════════════════════════════════
# 2. Buy-Write Hard Stop (-20%) — Forward-Economics Override
# ═════════════════════════════════════════════════════════════════════════

class TestBuyWriteHardStopGuard:
    """Test that 1st-cycle BW positions get forward-economics check at -20%."""

    def _run_bw(self, row):
        from core.management.cycle3.doctrine.strategies.buy_write import buy_write_doctrine
        result = {
            "Action": "HOLD", "Urgency": "LOW", "Rationale": "",
            "Doctrine_Source": "", "Decision_State": "", "Required_Conditions_Met": False,
        }
        return buy_write_doctrine(row, result)

    def test_1st_cycle_viable_income_holds(self):
        """1st-cycle position at -22%, viable income → HOLD (not EXIT)."""
        row = _make_row(
            IV_Now=0.40, _cycle_count=1,
            Cumulative_Premium_Collected=0.0,
            **{"UL Last": 39.0, "Spot": 39.0},
            **{"Net_Cost_Basis_Per_Share": 50.0, "Average Cost": 50.0},
            DTE=30, Short_Call_DTE=30,
            Thesis_State="INTACT",
            Daily_Margin_Cost=0.05,
            Quantity=100,
            Days_In_Trade=15,
        )
        result = self._run_bw(row)
        assert result["Action"] == "HOLD"
        assert "Forward-economics override" in result.get("Rationale", "")

    def test_1st_cycle_no_iv_exits(self):
        """1st-cycle at -22%, IV too low → EXIT (fallback to hard stop)."""
        row = _make_row(
            IV_Now=0.05, IV_30D=0.05, _cycle_count=1,
            Cumulative_Premium_Collected=0.0,
            **{"UL Last": 39.0, "Spot": 39.0},
            **{"Net_Cost_Basis_Per_Share": 50.0, "Average Cost": 50.0},
            DTE=30, Short_Call_DTE=30,
            Thesis_State="INTACT",
            Daily_Margin_Cost=0.50,
            Quantity=100,
            Days_In_Trade=15,
        )
        result = self._run_bw(row)
        assert result["Action"] == "EXIT"

    def test_1st_cycle_broken_thesis_exits(self):
        """1st-cycle at -22%, BROKEN thesis → EXIT regardless of income."""
        row = _make_row(
            IV_Now=0.40, _cycle_count=1,
            Cumulative_Premium_Collected=0.0,
            **{"UL Last": 39.0, "Spot": 39.0},
            **{"Net_Cost_Basis_Per_Share": 50.0, "Average Cost": 50.0},
            DTE=30, Short_Call_DTE=30,
            Thesis_State="BROKEN",
            Daily_Margin_Cost=0.50,
            Quantity=100,
            Days_In_Trade=15,
        )
        result = self._run_bw(row)
        assert result["Action"] == "EXIT"

    def test_multi_cycle_still_uses_recovery_ladder(self):
        """Multi-cycle (2+) position at -22% uses existing recovery ladder."""
        row = _make_row(
            IV_Now=0.40, _cycle_count=3,
            Cumulative_Premium_Collected=5.0,
            **{"UL Last": 39.0, "Spot": 39.0},
            **{"Net_Cost_Basis_Per_Share": 50.0, "Average Cost": 50.0},
            Premium_Entry=1.50,
            DTE=30, Short_Call_DTE=30,
            Thesis_State="INTACT",
            Quantity=100,
            Days_In_Trade=60,
        )
        result = self._run_bw(row)
        assert result["Action"] == "HOLD"
        assert "Recovery ladder" in result.get("Rationale", "")


# ═════════════════════════════════════════════════════════════════════════
# 3. Stock-Only Deep Loss (-50%) — CC Repair Override
# ═════════════════════════════════════════════════════════════════════════

class TestStockOnlyDeepLossGuard:
    """Test that stock-only at -50% considers CC overlay repair."""

    def _run_so(self, row):
        from core.management.cycle3.doctrine.strategies.stock_only import stock_only_doctrine
        result = {
            "Action": "HOLD", "Urgency": "LOW", "Rationale": "",
            "Doctrine_Source": "", "Decision_State": "", "Required_Conditions_Met": False,
        }
        return stock_only_doctrine(row, result)

    def test_viable_cc_overlay_holds(self):
        """100 shares at -55%, IV=40% → HOLD (CC repair viable)."""
        row = _make_row(
            IV_Now=0.40, IV_30D=0.40,
            Quantity=100,
            **{"UL Last": 22.5, "Spot": 22.5},
            PnL_Total=-2750,
            Total_GL_Decimal=-0.55,
            Equity_Integrity_State="WEAKENING",
        )
        result = self._run_so(row)
        assert result["Action"] == "HOLD"
        assert "CC overlay" in result.get("Rationale", "") or "CC repair" in result.get("Rationale", "")

    def test_low_iv_exits(self):
        """100 shares at -55%, IV=5% → EXIT (no premium)."""
        row = _make_row(
            IV_Now=0.05, IV_30D=0.05,
            Quantity=100,
            **{"UL Last": 22.5, "Spot": 22.5},
            PnL_Total=-2750,
            Total_GL_Decimal=-0.55,
            Equity_Integrity_State="WEAKENING",
        )
        result = self._run_so(row)
        assert result["Action"] == "EXIT"

    def test_insufficient_shares_exits(self):
        """50 shares at -55% → EXIT (can't sell CC)."""
        row = _make_row(
            IV_Now=0.40,
            Quantity=50,
            **{"UL Last": 22.5, "Spot": 22.5},
            PnL_Total=-1375,
            Total_GL_Decimal=-0.55,
            Equity_Integrity_State="WEAKENING",
        )
        result = self._run_so(row)
        assert result["Action"] == "EXIT"

    def test_broken_equity_exits(self):
        """100 shares at -55%, BROKEN equity → EXIT (gate 1 fires first)."""
        row = _make_row(
            IV_Now=0.40,
            Quantity=100,
            **{"UL Last": 22.5, "Spot": 22.5},
            PnL_Total=-2750,
            Total_GL_Decimal=-0.55,
            Equity_Integrity_State="BROKEN",
            Equity_Integrity_Reason="Structure collapsed",
        )
        result = self._run_so(row)
        assert result["Action"] == "EXIT"


# ═════════════════════════════════════════════════════════════════════════
# 4. Portfolio Circuit Breaker — Theta Carry Offset
# ═════════════════════════════════════════════════════════════════════════

class TestCircuitBreakerThetaOffset:
    """Test that net theta carry shifts the drawdown trigger threshold."""

    def _check(self, df, balance, peak, **kwargs):
        from core.management.portfolio_circuit_breaker import check_circuit_breaker
        return check_circuit_breaker(
            df_positions=df,
            account_balance=balance,
            peak_equity=peak,
            **kwargs,
        )

    def test_high_theta_shifts_threshold(self):
        """8.5% drawdown + $50/day theta → WARNING not TRIPPED (threshold shifted to 10%)."""
        df = pd.DataFrame({
            "Theta": [50.0],  # $50/day net theta
            "Action": ["HOLD"],
            "Urgency": ["LOW"],
        })
        # 8.5% drawdown: peak=100k, current=91.5k
        state, reason = self._check(df, balance=91_500, peak=100_000)
        # With $50/day theta at 100k account → 12.6% annualized → above floor
        # Effective trip shifts from 8% to 10%, so 8.5% is WARNING not TRIPPED
        assert state != "TRIPPED", f"Expected WARNING/OPEN but got TRIPPED: {reason}"

    def test_no_theta_original_threshold(self):
        """8.5% drawdown + $0 theta → TRIPPED (original 8% threshold)."""
        df = pd.DataFrame({
            "Theta": [0.0],
            "Action": ["HOLD"],
            "Urgency": ["LOW"],
        })
        state, reason = self._check(df, balance=91_500, peak=100_000)
        assert state == "TRIPPED"

    def test_extreme_drawdown_still_trips(self):
        """11% drawdown + $50/day theta → TRIPPED (exceeds even adjusted threshold)."""
        df = pd.DataFrame({
            "Theta": [50.0],
            "Action": ["HOLD"],
            "Urgency": ["LOW"],
        })
        state, reason = self._check(df, balance=89_000, peak=100_000)
        assert state == "TRIPPED"

    def test_small_drawdown_no_trip(self):
        """5% drawdown + any theta → no trip."""
        df = pd.DataFrame({
            "Theta": [0.0],
            "Action": ["HOLD"],
            "Urgency": ["LOW"],
        })
        state, reason = self._check(df, balance=95_000, peak=100_000)
        assert state != "TRIPPED"

    def test_missing_theta_column_fallback(self):
        """No Theta column → uses original threshold (graceful fallback)."""
        df = pd.DataFrame({
            "Action": ["HOLD"],
            "Urgency": ["LOW"],
        })
        state, reason = self._check(df, balance=91_500, peak=100_000)
        assert state == "TRIPPED"  # 8.5% > 8% original threshold


# ═════════════════════════════════════════════════════════════════════════
# 5. Threshold Constants — Sanity Checks
# ═════════════════════════════════════════════════════════════════════════

class TestThresholdConstants:
    """Verify threshold constants are sensible."""

    def test_bw_hard_stop_months(self):
        assert 6 <= FORWARD_ECON_MONTHS_BW_HARD_STOP <= 36

    def test_bw_approaching_wider_than_hard(self):
        assert FORWARD_ECON_MONTHS_BW_APPROACHING >= FORWARD_ECON_MONTHS_BW_HARD_STOP

    def test_stock_deep_loss_wider_than_bw(self):
        assert FORWARD_ECON_MONTHS_STOCK_DEEP_LOSS >= FORWARD_ECON_MONTHS_BW_HARD_STOP

    def test_iv_min_viable(self):
        assert 0.05 < FORWARD_ECON_IV_MIN_VIABLE < 0.50

    def test_drawdown_offset_small(self):
        assert 0.005 <= FORWARD_ECON_DRAWDOWN_THETA_OFFSET <= 0.05

    def test_theta_annual_floor(self):
        assert 0.01 <= FORWARD_ECON_THETA_ANNUAL_FLOOR <= 0.10
