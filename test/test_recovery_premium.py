"""Tests for Recovery Premium Mode — multi-cycle basis reduction doctrine.

Tests cover:
  - Mode detection (should_enter_recovery_premium_mode)
  - Action vocabulary (WRITE_NOW, HOLD_STOCK_WAIT, ROLL_UP_OUT, PAUSE_WRITING, EXIT_STOCK)
  - Strike discipline (cost-basis floor, Roth preservation)
  - Sell timing (IV regime, rally detection)
  - Recovery viability (months to breakeven, annualized yield)
  - Roth-specific capital preservation
  - Dispatch integration (BUY_WRITE routes to recovery_premium when criteria met)
"""

import pytest
import pandas as pd
import numpy as np

from core.management.cycle3.doctrine.helpers import should_enter_recovery_premium_mode
from core.management.cycle3.doctrine.strategies.recovery_premium import (
    recovery_premium_doctrine,
    ACTION_WRITE_NOW,
    ACTION_HOLD_STOCK_WAIT,
    ACTION_ROLL_UP_OUT,
    ACTION_PAUSE_WRITING,
    ACTION_EXIT_STOCK,
)


def _make_row(**overrides) -> pd.Series:
    """Build a recovery-eligible BW position row with sensible defaults."""
    defaults = {
        # Stock state
        'UL Last': 150.0,
        'Underlying_Price_Entry': 200.0,
        'Basis': 20000.0,       # 100 shares * $200
        'Quantity': 100,
        'Strategy': 'BUY_WRITE',
        'AssetType': 'OPTION',
        'Account': 'Individual',
        # Net cost after premium
        'Net_Cost_Basis_Per_Share': 195.0,
        'Cumulative_Premium_Collected': 500.0,  # $5/sh * 100
        'Gross_Premium_Collected': 500.0,
        '_cycle_count': 3,
        # IV
        'IV_30D': 35.0,         # 35% IV
        'IV_Rank': 50.0,
        'IV_Now': 0.35,
        # Call state
        'Short_Call_Strike': 200.0,
        'Short_Call_DTE': 25,
        'Short_Call_Delta': 0.25,
        'Strike': 200.0,
        'DTE': 25,
        'Delta': 0.25,
        'Premium_Entry': 3.50,
        'Last': 1.50,
        'Mid': 1.50,
        # Thesis
        'Thesis_State': 'INTACT',
        'Equity_Integrity_State': 'BROKEN',
        'TrendIntegrity_State': 'NO_TREND',
        # Chart
        'adx_14': 18,
        'rsi_14': 42,
        'ROC5': 0.5,
        'ROC10': -1.0,
        'HV_20D': 0.30,
        # Events
        'days_to_earnings': 45,
        'Earnings_Beat_Rate': 0.65,
        'Days_To_Macro': 15,
        'Macro_Next_Event': '',
        'Days_In_Trade': 60,
        'Days_Since_Last_Roll': 20,
        'Margin_Cost_Daily': 0.0,
    }
    defaults.update(overrides)
    return pd.Series(defaults)


def _base_result() -> dict:
    """Default result dict matching doctrine output shape."""
    return {
        "Action": "HOLD", "Urgency": "LOW",
        "Rationale": "default", "Doctrine_Source": "default",
        "Decision_State": "NEUTRAL_CONFIDENT",
        "Required_Conditions_Met": True,
    }


# ===========================================================================
# Mode Detection Tests
# ===========================================================================

class TestModeDetection:
    """Test should_enter_recovery_premium_mode()."""

    def test_activates_on_damaged_bw(self):
        row = _make_row()  # -23% loss, 3 cycles, IV viable, thesis INTACT
        result = should_enter_recovery_premium_mode(row, spot=150.0, effective_cost=195.0)
        assert result["should_activate"] is True
        assert result["context"]["loss_pct"] < -0.10
        assert result["context"]["cycles_completed"] == 3

    def test_rejects_small_loss(self):
        """Loss < 10% should NOT activate recovery premium."""
        row = _make_row()
        result = should_enter_recovery_premium_mode(row, spot=190.0, effective_cost=195.0)
        assert result["should_activate"] is False
        assert "loss_insufficient" in result["exit_reason"]

    def test_rejects_no_premium(self):
        """No premium collected = no income path."""
        row = _make_row(Cumulative_Premium_Collected=0, Gross_Premium_Collected=0)
        result = should_enter_recovery_premium_mode(row, spot=150.0, effective_cost=195.0)
        assert result["should_activate"] is False
        assert "no_income_path" in result["exit_reason"]

    def test_rejects_broken_thesis(self):
        row = _make_row(Thesis_State='BROKEN')
        result = should_enter_recovery_premium_mode(row, spot=150.0, effective_cost=195.0)
        assert result["should_activate"] is False
        assert "thesis_broken" in result["exit_reason"]

    def test_rejects_low_iv(self):
        row = _make_row(IV_30D=8.0, IV_Now=0.08)  # 8% IV
        result = should_enter_recovery_premium_mode(row, spot=150.0, effective_cost=195.0)
        assert result["should_activate"] is False
        assert "iv_too_low" in result["exit_reason"]

    def test_detects_roth_account(self):
        row = _make_row(Account='Roth IRA')
        result = should_enter_recovery_premium_mode(row, spot=150.0, effective_cost=195.0)
        assert result["should_activate"] is True
        assert result["context"]["is_retirement"] is True

    def test_detects_non_retirement(self):
        row = _make_row(Account='Individual Brokerage')
        result = should_enter_recovery_premium_mode(row, spot=150.0, effective_cost=195.0)
        assert result["should_activate"] is True
        assert result["context"]["is_retirement"] is False

    def test_context_has_all_fields(self):
        row = _make_row()
        result = should_enter_recovery_premium_mode(row, spot=150.0, effective_cost=195.0)
        ctx = result["context"]
        required_keys = [
            "loss_pct", "gap_to_breakeven", "premium_collected_per_share",
            "cycles_completed", "basis_reduction_pct", "monthly_income",
            "net_monthly", "months_to_breakeven", "annualized_yield",
            "has_active_call", "strike", "dte", "delta", "iv_now", "iv_rank",
            "is_retirement", "roc5", "adx", "rsi", "stock_basing", "thesis",
            "days_to_earnings", "spot", "effective_cost",
        ]
        for key in required_keys:
            assert key in ctx, f"Missing context key: {key}"

    def test_zero_margin_for_roth(self):
        """Roth accounts should have zero margin cost in context."""
        row = _make_row(Account='Roth IRA', Margin_Cost_Daily=0.05)
        result = should_enter_recovery_premium_mode(row, spot=150.0, effective_cost=195.0)
        assert result["context"]["margin_monthly"] == 0.0

    def test_degraded_thesis_still_activates(self):
        row = _make_row(Thesis_State='DEGRADED')
        result = should_enter_recovery_premium_mode(row, spot=150.0, effective_cost=195.0)
        assert result["should_activate"] is True


# ===========================================================================
# Strategy Doctrine Tests
# ===========================================================================

class TestThesisGuardrail:
    """Gate 0: thesis BROKEN = EXIT_STOCK."""

    def test_broken_thesis_exits(self):
        row = _make_row(Thesis_State='BROKEN')
        # Build context manually (bypass mode detection since it rejects BROKEN)
        ctx = {
            "spot": 150, "effective_cost": 195, "broker_cost_per_share": 200,
            "loss_pct": -0.23, "gap_to_breakeven": 45, "premium_collected_per_share": 5,
            "cycles_completed": 3, "basis_reduction_pct": 0.025,
            "monthly_income": 4.2, "net_monthly": 4.2, "margin_monthly": 0,
            "months_to_breakeven": 10.7, "annualized_yield": 0.034,
            "has_active_call": True, "strike": 200, "strike_vs_cost": 0.026,
            "dte": 25, "delta": 0.25, "last_premium": 3.5,
            "iv_now": 0.35, "iv_rank": 50, "is_retirement": False,
            "roc5": 0.5, "roc10": -1, "adx": 18, "rsi": 42,
            "stock_basing": True, "thesis": "BROKEN",
            "days_to_earnings": 45, "earnings_beat_rate": 0.65,
            "days_to_macro": 15, "macro_event": "", "days_since_roll": 20,
            "qty": 100,
        }
        result = recovery_premium_doctrine(row, _base_result(), ctx)
        assert result["Action"] == ACTION_EXIT_STOCK
        assert result["Urgency"] == "CRITICAL"
        assert "BROKEN" in result["Rationale"]


class TestEarningsRisk:
    """Gate 1: earnings risk pauses writing."""

    def test_earnings_imminent_pauses(self):
        row = _make_row(days_to_earnings=1)
        ctx = _build_ctx(row, days_to_earnings=1)
        result = recovery_premium_doctrine(row, _base_result(), ctx)
        assert result["Action"] == ACTION_PAUSE_WRITING

    def test_earnings_near_with_high_delta_rolls(self):
        row = _make_row(days_to_earnings=5, Short_Call_Delta=0.60)
        ctx = _build_ctx(row, days_to_earnings=5, delta=0.60)
        result = recovery_premium_doctrine(row, _base_result(), ctx)
        assert result["Action"] == ACTION_ROLL_UP_OUT


class TestStrikeDiscipline:
    """Gate 3: strike below cost basis triggers ROLL_UP_OUT."""

    def test_strike_far_below_basis_rolls(self):
        row = _make_row(Short_Call_Strike=180.0, Strike=180.0)
        ctx = _build_ctx(row, strike=180.0, strike_vs_cost=-0.077)
        result = recovery_premium_doctrine(row, _base_result(), ctx)
        assert result["Action"] == ACTION_ROLL_UP_OUT
        assert "below" in result["Rationale"].lower() or "basis" in result["Rationale"].lower()

    def test_strike_above_basis_holds(self):
        row = _make_row(Short_Call_Strike=200.0, Strike=200.0)
        ctx = _build_ctx(row, strike=200.0, strike_vs_cost=0.026)
        result = recovery_premium_doctrine(row, _base_result(), ctx)
        assert result["Action"] != ACTION_EXIT_STOCK  # should HOLD or similar

    def test_roth_strike_below_basis_warns_assignment(self):
        row = _make_row(Account='Roth IRA', Short_Call_Strike=175.0, Strike=175.0)
        ctx = _build_ctx(row, strike=175.0, strike_vs_cost=-0.103, is_retirement=True)
        result = recovery_premium_doctrine(row, _base_result(), ctx)
        assert result["Action"] == ACTION_ROLL_UP_OUT
        assert "Roth" in result["Rationale"]


class TestAssignmentEconomics:
    """Gate 4: deep ITM assignment evaluation."""

    def test_assignment_at_loss_rolls(self):
        row = _make_row(Short_Call_Delta=0.85, Delta=0.85, Short_Call_Strike=185.0, Strike=185.0)
        ctx = _build_ctx(row, delta=0.85, strike=185.0, strike_vs_cost=-0.051)
        result = recovery_premium_doctrine(row, _base_result(), ctx)
        assert result["Action"] == ACTION_ROLL_UP_OUT
        assert result["Urgency"] == "HIGH"


class TestExpirationProximity:
    """Gate 5: cycle transition at expiration."""

    def test_otm_expiring_writes_new(self):
        row = _make_row(Short_Call_DTE=3, DTE=3, Short_Call_Delta=0.10, Delta=0.10)
        ctx = _build_ctx(row, dte=3, delta=0.10, has_active_call=True)
        result = recovery_premium_doctrine(row, _base_result(), ctx)
        assert result["Action"] == ACTION_WRITE_NOW

    def test_itm_expiring_rolls(self):
        row = _make_row(Short_Call_DTE=3, DTE=3, Short_Call_Delta=0.65, Delta=0.65)
        ctx = _build_ctx(row, dte=3, delta=0.65, has_active_call=True)
        result = recovery_premium_doctrine(row, _base_result(), ctx)
        assert result["Action"] == ACTION_ROLL_UP_OUT


class TestIVEnvironment:
    """Gate 6: IV regime determines WRITE_NOW vs HOLD_STOCK_WAIT."""

    def test_high_iv_rank_writes(self):
        row = _make_row(Short_Call_DTE=0, DTE=0, IV_Rank=55)
        ctx = _build_ctx(row, has_active_call=False, iv_rank=55, dte=0)
        result = recovery_premium_doctrine(row, _base_result(), ctx)
        assert result["Action"] == ACTION_WRITE_NOW

    def test_low_iv_rank_waits(self):
        row = _make_row(Short_Call_DTE=0, DTE=0, IV_Rank=12)
        ctx = _build_ctx(row, has_active_call=False, iv_rank=12, dte=0)
        result = recovery_premium_doctrine(row, _base_result(), ctx)
        assert result["Action"] == ACTION_HOLD_STOCK_WAIT


class TestRallyTiming:
    """Gate 7: sell into rallies."""

    def test_rally_triggers_write(self):
        row = _make_row(Short_Call_DTE=0, DTE=0, ROC5=4.5, IV_Rank=35)
        ctx = _build_ctx(row, has_active_call=False, roc5=4.5, iv_rank=35)
        result = recovery_premium_doctrine(row, _base_result(), ctx)
        assert result["Action"] == ACTION_WRITE_NOW
        assert "rallied" in result["Rationale"].lower() or "rally" in result["Rationale"].lower()


class TestPremiumCapture:
    """Gate 8: 50% premium capture triggers new cycle."""

    def test_50pct_captured_resets_cycle(self):
        row = _make_row(Premium_Entry=4.00, Last=1.50, Mid=1.50, Short_Call_DTE=20, DTE=20)
        ctx = _build_ctx(row, last_premium=4.00, dte=20)
        result = recovery_premium_doctrine(row, _base_result(), ctx)
        assert result["Action"] == ACTION_WRITE_NOW


class TestRecoveryViability:
    """Gate 9: uneconomical recovery triggers EXIT_STOCK."""

    def test_very_long_recovery_exits(self):
        # Set premium close to mid so 50% capture doesn't fire
        row = _make_row(Premium_Entry=2.00, Last=1.50, Mid=1.50)
        ctx = _build_ctx(row, last_premium=2.00)
        ctx["months_to_breakeven"] = 72  # 6 years
        ctx["net_monthly"] = 0.50
        result = recovery_premium_doctrine(row, _base_result(), ctx)
        assert result["Action"] == ACTION_EXIT_STOCK
        assert "uneconomical" in result["Rationale"].lower()


class TestRothPreservation:
    """Gate 10: Roth-specific capital preservation."""

    def test_roth_near_spot_rolls_up(self):
        """Roth position with strike too close to spot on depressed stock."""
        row = _make_row(
            Account='Roth IRA',
            Short_Call_Strike=151.0, Strike=151.0,  # within 3% of spot=150
        )
        ctx = _build_ctx(row, is_retirement=True, strike=151.0, strike_vs_cost=-0.226)
        result = recovery_premium_doctrine(row, _base_result(), ctx)
        # Should prioritize ROLL_UP_OUT (strike below basis is higher priority)
        assert result["Action"] == ACTION_ROLL_UP_OUT

    def test_roth_deep_drawdown_waits(self):
        """Roth at >30% loss with no call should wait."""
        row = _make_row(Account='Roth IRA', Short_Call_DTE=0, DTE=0, IV_Rank=15)
        ctx = _build_ctx(
            row, is_retirement=True, has_active_call=False,
            loss_pct=-0.35, iv_rank=15,
        )
        result = recovery_premium_doctrine(row, _base_result(), ctx)
        assert result["Action"] == ACTION_HOLD_STOCK_WAIT


class TestDefaultHold:
    """Gate 11: default behavior with active call."""

    def test_active_call_defaults_to_hold(self):
        """Position with active call and no triggers should HOLD."""
        # Set Premium_Entry close to current value so 50% capture doesn't fire
        row = _make_row(Premium_Entry=2.00, Last=1.50, Mid=1.50)
        ctx = _build_ctx(row, last_premium=2.00)
        result = recovery_premium_doctrine(row, _base_result(), ctx)
        # With strike at 200 (above 195 cost), no premium capture trigger → HOLD
        assert result["Action"] == "HOLD"
        assert result["Doctrine_State"] == "RECOVERY_PREMIUM"
        assert result["Recovery_Mode"] is True


class TestDispatchIntegration:
    """Test that DoctrineAuthority routes damaged BW to recovery_premium."""

    def test_doctrine_state_set(self):
        """Recovery Premium Mode sets Doctrine_State = RECOVERY_PREMIUM."""
        row = _make_row()
        ctx = _build_ctx(row)
        result = recovery_premium_doctrine(row, _base_result(), ctx)
        assert result["Doctrine_State"] == "RECOVERY_PREMIUM"

    def test_mode_detection_feeds_strategy(self):
        """End-to-end: mode detection → strategy execution."""
        row = _make_row()
        rpm = should_enter_recovery_premium_mode(row, spot=150.0, effective_cost=195.0)
        assert rpm["should_activate"] is True
        result = recovery_premium_doctrine(row, _base_result(), rpm["context"])
        assert "RECOVERY PREMIUM MODE" in result["Rationale"]


# ===========================================================================
# Helpers
# ===========================================================================

def _build_ctx(row: pd.Series, **overrides) -> dict:
    """Build recovery context with defaults + overrides."""
    spot = float(overrides.get('spot', row.get('UL Last', 150)))
    effective_cost = float(overrides.get('effective_cost', row.get('Net_Cost_Basis_Per_Share', 195)))
    broker_cost = float(overrides.get('broker_cost_per_share', 200))
    strike = float(overrides.get('strike', row.get('Short_Call_Strike', 200)))
    dte = float(overrides.get('dte', row.get('Short_Call_DTE', 25)))
    delta = float(overrides.get('delta', abs(float(row.get('Short_Call_Delta', 0.25)))))
    has_call = overrides.get('has_active_call', dte > 0 and dte < 900 and strike > 0)

    loss_pct = overrides.get('loss_pct', (spot - effective_cost) / effective_cost if effective_cost > 0 else 0)
    gap = effective_cost - spot
    cum_ps = float(row.get('Cumulative_Premium_Collected', 500)) / max(abs(float(row.get('Quantity', 100))), 1)
    last_prem = float(overrides.get('last_premium', row.get('Premium_Entry', 3.5)))
    monthly = (last_prem / max(dte, 1)) * 30 if last_prem > 0 and dte > 0 else 0
    iv_rank = float(overrides.get('iv_rank', row.get('IV_Rank', 50)))

    ctx = {
        "spot": spot,
        "effective_cost": effective_cost,
        "broker_cost_per_share": broker_cost,
        "loss_pct": loss_pct,
        "gap_to_breakeven": gap,
        "premium_collected_per_share": cum_ps,
        "cycles_completed": int(row.get('_cycle_count', 3)),
        "basis_reduction_pct": cum_ps / broker_cost if broker_cost > 0 else 0,
        "monthly_income": monthly,
        "net_monthly": monthly,
        "margin_monthly": 0.0,
        "months_to_breakeven": gap / monthly if monthly > 0 and gap > 0 else float('inf'),
        "annualized_yield": (monthly * 12) / spot if spot > 0 and monthly > 0 else 0,
        "has_active_call": has_call,
        "strike": strike,
        "strike_vs_cost": (strike - effective_cost) / effective_cost if effective_cost > 0 else 0,
        "dte": dte,
        "delta": delta,
        "last_premium": last_prem,
        "iv_now": float(row.get('IV_Now', 0.35)),
        "iv_rank": iv_rank,
        "is_retirement": overrides.get('is_retirement', False),
        "roc5": float(overrides.get('roc5', row.get('ROC5', 0.5))),
        "roc10": float(row.get('ROC10', -1.0)),
        "adx": float(row.get('adx_14', 18)),
        "rsi": float(row.get('rsi_14', 42)),
        "stock_basing": True,
        "thesis": str(overrides.get('thesis', row.get('Thesis_State', 'INTACT'))).upper(),
        "days_to_earnings": float(overrides.get('days_to_earnings', row.get('days_to_earnings', 45))),
        "earnings_beat_rate": float(row.get('Earnings_Beat_Rate', 0.65)),
        "days_to_macro": float(row.get('Days_To_Macro', 15)),
        "macro_event": str(row.get('Macro_Next_Event', '')),
        "days_since_roll": float(row.get('Days_Since_Last_Roll', 20)),
        "qty": abs(float(row.get('Quantity', 100))),
    }
    ctx.update({k: v for k, v in overrides.items() if k in ctx})
    return ctx
