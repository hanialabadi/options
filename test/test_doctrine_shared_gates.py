"""
Tests for shared income gates — pure function tests for each gate
extracted from BUY_WRITE/COVERED_CALL duplication.
"""

import pytest
import pandas as pd

from core.management.cycle3.doctrine.shared_income_gates import (
    gate_post_buyback_sticky,
    gate_gamma_danger_zone,
    gate_equity_broken_gamma_conviction,
    gate_equity_broken_gamma_no_conviction,
    gate_dividend_assignment,
    gate_expiration_dte_7,
    gate_itm_defense,
    gate_consecutive_debit_roll_stop,
    gate_fading_winner,
)
from core.management.cycle3.doctrine.gate_result import STATE_ACTIONABLE


def _base_result():
    return {"Action": "HOLD", "Urgency": "LOW", "Rationale": "default"}


# ── gate_post_buyback_sticky ─────────────────────────────────────────

class TestPostBuybackSticky:
    def test_fires_when_prior_buyback_and_not_intact(self):
        row = pd.Series({"Prior_Action": "BUYBACK", "Equity_Integrity_State": "BROKEN"})
        fired, result = gate_post_buyback_sticky(row=row, spot=250.0, result=_base_result())
        assert fired is True
        assert result["Action"] == "HOLD"
        assert result["Urgency"] == "HIGH"
        assert "Post-BUYBACK" in result["Rationale"]

    def test_skips_when_intact(self):
        row = pd.Series({"Prior_Action": "BUYBACK", "Equity_Integrity_State": "INTACT"})
        fired, result = gate_post_buyback_sticky(row=row, spot=250.0, result=_base_result())
        assert fired is False

    def test_skips_when_prior_not_buyback(self):
        row = pd.Series({"Prior_Action": "HOLD", "Equity_Integrity_State": "BROKEN"})
        fired, result = gate_post_buyback_sticky(row=row, spot=250.0, result=_base_result())
        assert fired is False

    def test_skips_when_no_prior(self):
        row = pd.Series({"Prior_Action": None, "Equity_Integrity_State": "WEAKENING"})
        fired, result = gate_post_buyback_sticky(row=row, spot=250.0, result=_base_result())
        assert fired is False


# ── gate_gamma_danger_zone ───────────────────────────────────────────

class TestGammaDangerZone:
    def _row(self):
        return pd.Series({"Gamma_ROC_3D": 0.001})

    def test_fires_near_atm_low_dte_high_ratio(self):
        fired, result = gate_gamma_danger_zone(
            spot=100.0, strike=101.0, dte=15, theta=0.05, gamma=0.08,
            hv_20d=0.30, gamma_roc_3d=0.001, ei_state="WEAKENING",
            strategy_label="BW", result=_base_result(),
        )
        # Check if ratio conditions are met (gamma drag vs theta)
        # With spot=100, hv=0.30, sigma~1.89, drag~0.5*0.08*3.57~0.143
        # ratio = 0.143/0.05 = 2.86 > 1.5, ATM pct = 1% < 5%, 7 < 15 <= 21
        assert fired is True
        assert result["Action"] == "ROLL"
        assert "Gamma Danger Zone" in result["Rationale"]

    def test_fires_with_high_urgency_when_gamma_accelerating(self):
        fired, result = gate_gamma_danger_zone(
            spot=100.0, strike=101.0, dte=15, theta=0.05, gamma=0.08,
            hv_20d=0.30, gamma_roc_3d=0.005, ei_state="WEAKENING",
            strategy_label="BW", result=_base_result(),
        )
        assert fired is True
        assert result["Urgency"] == "HIGH"

    def test_medium_urgency_when_gamma_declining(self):
        fired, result = gate_gamma_danger_zone(
            spot=100.0, strike=101.0, dte=15, theta=0.05, gamma=0.08,
            hv_20d=0.30, gamma_roc_3d=-0.002, ei_state="WEAKENING",
            strategy_label="CC", result=_base_result(),
        )
        assert fired is True
        assert result["Urgency"] == "MEDIUM"

    def test_skips_when_far_otm(self):
        fired, result = gate_gamma_danger_zone(
            spot=100.0, strike=120.0, dte=15, theta=0.05, gamma=0.08,
            hv_20d=0.30, gamma_roc_3d=0.001, ei_state="WEAKENING",
            strategy_label="BW", result=_base_result(),
        )
        assert fired is False  # 20% from strike > 5%

    def test_skips_when_broken(self):
        fired, result = gate_gamma_danger_zone(
            spot=100.0, strike=101.0, dte=15, theta=0.05, gamma=0.08,
            hv_20d=0.30, gamma_roc_3d=0.001, ei_state="BROKEN",
            strategy_label="BW", result=_base_result(),
        )
        assert fired is False

    def test_skips_when_dte_too_low(self):
        fired, result = gate_gamma_danger_zone(
            spot=100.0, strike=101.0, dte=5, theta=0.05, gamma=0.08,
            hv_20d=0.30, gamma_roc_3d=0.001, ei_state="WEAKENING",
            strategy_label="BW", result=_base_result(),
        )
        assert fired is False  # DTE=5 <= 7, not in 7-21 zone

    def test_skips_when_dte_too_high(self):
        fired, result = gate_gamma_danger_zone(
            spot=100.0, strike=101.0, dte=30, theta=0.05, gamma=0.08,
            hv_20d=0.30, gamma_roc_3d=0.001, ei_state="WEAKENING",
            strategy_label="BW", result=_base_result(),
        )
        assert fired is False  # DTE=30 > 21


# ── gate_equity_broken_gamma_conviction ──────────────────────────────

class TestEquityBrokenGammaConviction:
    def test_fires_dte_conviction(self):
        """Expiration week + BROKEN + gamma dominant → ROLL HIGH."""
        row = pd.Series({
            "Short_Call_Last": 5.0, "Short_Call_Premium": 3.0,
            "adx_14": 30, "roc_20": 2.0,
            "Equity_Integrity_State": "BROKEN", "Equity_Integrity_Reason": "3 signals",
        })
        fired, result = gate_equity_broken_gamma_conviction(
            row=row, spot=100.0, strike=98.0, dte=5, theta=0.03,
            gamma=0.10, hv_20d=0.35, ei_state="BROKEN", ei_reason="3 signals",
            result=_base_result(),
        )
        assert fired is True
        assert result["Action"] == "ROLL"
        assert result["Urgency"] == "HIGH"
        assert "expiration week" in result["Rationale"]

    def test_fires_breakout_conviction(self):
        """Breakout through strike + BROKEN + gamma dominant → ROLL HIGH."""
        row = pd.Series({
            "Short_Call_Last": 5.0, "Short_Call_Premium": 3.0,
            "adx_14": 30, "roc_20": 2.0,
        })
        fired, result = gate_equity_broken_gamma_conviction(
            row=row, spot=105.0, strike=100.0, dte=15, theta=0.03,
            gamma=0.10, hv_20d=0.35, ei_state="BROKEN", ei_reason="3 signals",
            result=_base_result(),
        )
        assert fired is True
        assert "momentum" in result["Rationale"]

    def test_skips_when_not_broken(self):
        row = pd.Series({
            "Short_Call_Last": 5.0, "Short_Call_Premium": 3.0,
            "adx_14": 30, "roc_20": 2.0,
        })
        fired, result = gate_equity_broken_gamma_conviction(
            row=row, spot=100.0, strike=98.0, dte=5, theta=0.03,
            gamma=0.10, hv_20d=0.35, ei_state="WEAKENING", ei_reason="",
            result=_base_result(),
        )
        assert fired is False

    def test_skips_when_gamma_not_dominant(self):
        row = pd.Series({
            "Short_Call_Last": 2.0, "Short_Call_Premium": 3.0,
            "adx_14": 30, "roc_20": 2.0,
        })
        fired, result = gate_equity_broken_gamma_conviction(
            row=row, spot=100.0, strike=98.0, dte=5, theta=0.50,
            gamma=0.001, hv_20d=0.15, ei_state="BROKEN", ei_reason="3 signals",
            result=_base_result(),
        )
        assert fired is False  # gamma drag << theta


# ── gate_equity_broken_gamma_no_conviction ───────────────────────────

class TestEquityBrokenGammaNoConviction:
    def test_exit_when_negative_carry_below_cost(self):
        """BROKEN + gamma dominant + no conviction + below cost → EXIT MEDIUM."""
        row = pd.Series({
            "Short_Call_Last": 5.0, "adx_14": 10, "roc_20": -1.0,
        })
        fired, result = gate_equity_broken_gamma_no_conviction(
            row=row, spot=90.0, strike=95.0, dte=15, theta=0.02,
            gamma=0.10, hv_20d=0.40, effective_cost=100.0, cum_premium=5.0,
            ei_state="BROKEN", ei_reason="3 signals", result=_base_result(),
        )
        assert fired is True
        assert result["Action"] == "EXIT"
        assert result["Urgency"] == "MEDIUM"

    def test_hold_when_negative_carry_above_cost(self):
        """BROKEN + gamma dominant + no conviction + ABOVE cost → HOLD MEDIUM."""
        # Set call_last high enough that extrinsic > $0.20 (avoid conviction)
        # spot=110, strike=115 (OTM so intrinsic=0, extrinsic=call_last=3.0)
        # adx=10 (no breakout), roc=-1 (no momentum), dte=20 (not exp week)
        row = pd.Series({
            "Short_Call_Last": 3.0, "adx_14": 10, "roc_20": -1.0,
        })
        fired, result = gate_equity_broken_gamma_no_conviction(
            row=row, spot=110.0, strike=115.0, dte=20, theta=0.02,
            gamma=0.10, hv_20d=0.40, effective_cost=100.0, cum_premium=10.0,
            ei_state="BROKEN", ei_reason="3 signals", result=_base_result(),
        )
        assert fired is True
        assert result["Action"] == "HOLD"
        assert result["Urgency"] == "MEDIUM"
        assert "cushion" in result["Rationale"].lower()

    def test_skips_when_conviction_present(self):
        """If conviction present, this gate should not fire."""
        row = pd.Series({
            "Short_Call_Last": 0.10, "adx_14": 30, "roc_20": 5.0,
        })
        # Extrinsic < 0.20 → conviction present → skip
        fired, result = gate_equity_broken_gamma_no_conviction(
            row=row, spot=100.0, strike=98.0, dte=5, theta=0.02,
            gamma=0.10, hv_20d=0.35, effective_cost=95.0, cum_premium=5.0,
            ei_state="BROKEN", ei_reason="3 signals", result=_base_result(),
        )
        assert fired is False


# ── gate_dividend_assignment ─────────────────────────────────────────

class TestDividendAssignment:
    def test_fires_critical_when_2_days(self):
        row = pd.Series({"Days_To_Dividend": 1, "Dividend_Amount": 0.50})
        fired, result = gate_dividend_assignment(row=row, delta=0.65, result=_base_result())
        assert fired is True
        assert result["Action"] == "ROLL"
        assert result["Urgency"] == "CRITICAL"

    def test_fires_high_when_4_days(self):
        row = pd.Series({"Days_To_Dividend": 4, "Dividend_Amount": 0.50})
        fired, result = gate_dividend_assignment(row=row, delta=0.65, result=_base_result())
        assert fired is True
        assert result["Urgency"] == "HIGH"

    def test_skips_low_delta(self):
        row = pd.Series({"Days_To_Dividend": 3, "Dividend_Amount": 0.50})
        fired, result = gate_dividend_assignment(row=row, delta=0.30, result=_base_result())
        assert fired is False

    def test_skips_no_dividend(self):
        row = pd.Series({"Days_To_Dividend": 3, "Dividend_Amount": 0})
        fired, result = gate_dividend_assignment(row=row, delta=0.65, result=_base_result())
        assert fired is False

    def test_skips_far_div_date(self):
        row = pd.Series({"Days_To_Dividend": 30, "Dividend_Amount": 0.50})
        fired, result = gate_dividend_assignment(row=row, delta=0.65, result=_base_result())
        assert fired is False


# ── gate_expiration_dte_7 ────────────────────────────────────────────

class TestExpirationDTE7:
    def test_fires_when_dte_below_7(self):
        fired, result = gate_expiration_dte_7(
            dte=5, strike=100.0, spot=98.0, result=_base_result(),
        )
        assert fired is True
        assert result["Action"] == "ROLL"
        assert result["Urgency"] == "HIGH"
        assert "pin risk" in result["Rationale"].lower()

    def test_skips_when_dte_above_7(self):
        fired, result = gate_expiration_dte_7(
            dte=10, strike=100.0, spot=98.0, result=_base_result(),
        )
        assert fired is False

    def test_fires_at_dte_1(self):
        fired, result = gate_expiration_dte_7(
            dte=1, strike=100.0, spot=101.0, result=_base_result(),
        )
        assert fired is True


# ── gate_itm_defense ─────────────────────────────────────────────────

class TestITMDefense:
    def test_fires_when_deep_itm(self):
        fired, result = gate_itm_defense(
            delta=0.80, spot=110.0, strike=100.0, effective_cost=95.0,
            result=_base_result(),
        )
        assert fired is True
        assert result["Action"] == "ROLL"
        assert result["Urgency"] == "HIGH"
        assert "assignment" in result["Rationale"].lower()

    def test_skips_when_otm(self):
        fired, result = gate_itm_defense(
            delta=0.40, spot=95.0, strike=100.0, effective_cost=90.0,
            result=_base_result(),
        )
        assert fired is False

    def test_fires_at_boundary(self):
        fired, result = gate_itm_defense(
            delta=0.71, spot=102.0, strike=100.0, effective_cost=98.0,
            result=_base_result(),
        )
        assert fired is True

    def test_skips_at_exactly_070(self):
        fired, result = gate_itm_defense(
            delta=0.70, spot=100.0, strike=100.0, effective_cost=95.0,
            result=_base_result(),
        )
        assert fired is False  # needs to be > 0.70, not >=


# ── gate_consecutive_debit_roll_stop ───────────────────────────────

class TestConsecutiveDebitRollStop:
    def test_fires_at_threshold(self):
        row = pd.Series({
            "Trajectory_Consecutive_Debit_Rolls": 3,
            "Trajectory_Total_Roll_Cost": 450.0,
            "Cumulative_Premium_Collected": 300.0,
        })
        fired, result = gate_consecutive_debit_roll_stop(
            row=row, result=_base_result(), strategy_label="CC",
        )
        assert fired is True
        assert result["Action"] == "EXIT"
        assert result["Urgency"] == "HIGH"
        assert "consecutive net-debit" in result["Rationale"]
        assert "McMillan Ch.3" in result["Rationale"]

    def test_fires_above_threshold(self):
        row = pd.Series({
            "Trajectory_Consecutive_Debit_Rolls": 5,
            "Trajectory_Total_Roll_Cost": 800.0,
            "Cumulative_Premium_Collected": 200.0,
        })
        fired, result = gate_consecutive_debit_roll_stop(
            row=row, result=_base_result(), strategy_label="BW",
        )
        assert fired is True
        assert "5 consecutive" in result["Rationale"]

    def test_skips_below_threshold(self):
        row = pd.Series({
            "Trajectory_Consecutive_Debit_Rolls": 2,
            "Trajectory_Total_Roll_Cost": 200.0,
            "Cumulative_Premium_Collected": 500.0,
        })
        fired, result = gate_consecutive_debit_roll_stop(
            row=row, result=_base_result(), strategy_label="CSP",
        )
        assert fired is False

    def test_skips_zero_debits(self):
        row = pd.Series({
            "Trajectory_Consecutive_Debit_Rolls": 0,
        })
        fired, result = gate_consecutive_debit_roll_stop(
            row=row, result=_base_result(),
        )
        assert fired is False

    def test_handles_missing_field(self):
        row = pd.Series({"some_other_field": 42})
        fired, result = gate_consecutive_debit_roll_stop(
            row=row, result=_base_result(),
        )
        assert fired is False


# ── gate_fading_winner ──────────────────────────────────────────────────

class TestFadingWinner:
    def test_nan_mfe_skips_gate(self):
        """NaN MFE must skip — not fire false EXIT on new positions."""
        import numpy as np
        row = pd.Series({"Trajectory_MFE": np.nan})
        fired, result = gate_fading_winner(
            row=row, pnl_pct=0.02, result=_base_result(),
        )
        assert fired is False

    def test_none_mfe_skips_gate(self):
        row = pd.Series({"Trajectory_MFE": None})
        fired, result = gate_fading_winner(
            row=row, pnl_pct=0.02, result=_base_result(),
        )
        assert fired is False

    def test_missing_mfe_skips_gate(self):
        row = pd.Series({"some_col": 1})
        fired, result = gate_fading_winner(
            row=row, pnl_pct=0.02, result=_base_result(),
        )
        assert fired is False

    def test_low_mfe_skips_gate(self):
        row = pd.Series({"Trajectory_MFE": 0.10})  # below 20% threshold
        fired, result = gate_fading_winner(
            row=row, pnl_pct=0.08, result=_base_result(),
        )
        assert fired is False

    def test_roundtrip_fires_exit_high(self):
        row = pd.Series({"Trajectory_MFE": 0.25})  # was 25% up
        fired, result = gate_fading_winner(
            row=row, pnl_pct=0.03, result=_base_result(),  # now only 3%
        )
        assert fired is True
        assert result["Action"] == "EXIT"
        assert result["Urgency"] == "HIGH"

    def test_giveback_fires_exit_medium(self):
        row = pd.Series({"Trajectory_MFE": 0.30})  # was 30% up
        fired, result = gate_fading_winner(
            row=row, pnl_pct=0.12, result=_base_result(),  # now 12% (gave back 60%)
        )
        assert fired is True
        assert result["Action"] == "EXIT"
        assert result["Urgency"] == "MEDIUM"

    def test_negative_pnl_skips(self):
        row = pd.Series({"Trajectory_MFE": 0.30})
        fired, result = gate_fading_winner(
            row=row, pnl_pct=-0.05, result=_base_result(),
        )
        assert fired is False

    def test_rationale_includes_net_income(self):
        row = pd.Series({
            "Trajectory_Consecutive_Debit_Rolls": 3,
            "Trajectory_Total_Roll_Cost": 300.0,
            "Cumulative_Premium_Collected": 500.0,
        })
        fired, result = gate_consecutive_debit_roll_stop(
            row=row, result=_base_result(),
        )
        assert fired is True
        assert "net profit" in result["Rationale"]

    def test_rationale_shows_net_loss(self):
        row = pd.Series({
            "Trajectory_Consecutive_Debit_Rolls": 4,
            "Trajectory_Total_Roll_Cost": 800.0,
            "Cumulative_Premium_Collected": 200.0,
        })
        fired, result = gate_consecutive_debit_roll_stop(
            row=row, result=_base_result(),
        )
        assert fired is True
        assert "net loss" in result["Rationale"]


# ── Vol-Scaled MFE Giveback (Carver) ──────────────────────────────────

class TestVolScaledGiveback:
    """Validate that MFE giveback threshold scales with HV_20D."""

    def test_low_vol_tighter_threshold(self):
        """HV=10% (low vol) → 35% giveback triggers EXIT."""
        row = pd.Series({"Trajectory_MFE": 0.30, "HV_20D": 0.10})
        # 30% MFE, currently at 20% P&L → gave back 33%
        # Standard 50% wouldn't fire, but tight 35% should fire
        fired, result = gate_fading_winner(
            row=row, pnl_pct=0.20, result=_base_result(),
        )
        # 33% < 35%, shouldn't fire yet
        assert fired is False

    def test_low_vol_triggers_at_36pct_giveback(self):
        """HV=10% → 35% threshold. 36% giveback fires EXIT."""
        row = pd.Series({"Trajectory_MFE": 0.25, "HV_20D": 0.10})
        # 25% MFE, currently at 16% → gave back 36%
        fired, result = gate_fading_winner(
            row=row, pnl_pct=0.16, result=_base_result(),
        )
        assert fired is True
        assert result["Action"] == "EXIT"
        assert "tighter trail" in result["Rationale"]

    def test_high_vol_wider_threshold(self):
        """HV=50% (high vol) → 60% giveback needed. 55% shouldn't fire."""
        row = pd.Series({"Trajectory_MFE": 0.40, "HV_20D": 0.50})
        # 40% MFE, currently at 18% → gave back 55%
        fired, result = gate_fading_winner(
            row=row, pnl_pct=0.18, result=_base_result(),
        )
        assert fired is False  # 55% < 60% threshold

    def test_high_vol_triggers_at_61pct_giveback(self):
        """HV=50% → 60% threshold. 62% giveback fires EXIT."""
        row = pd.Series({"Trajectory_MFE": 0.40, "HV_20D": 0.50})
        # 40% MFE, currently at 15.2% → gave back ~62%
        fired, result = gate_fading_winner(
            row=row, pnl_pct=0.152, result=_base_result(),
        )
        assert fired is True
        assert "wider trail" in result["Rationale"]

    def test_mid_vol_uses_standard_threshold(self):
        """HV=25% (mid range) → standard 50% threshold."""
        row = pd.Series({"Trajectory_MFE": 0.30, "HV_20D": 0.25})
        # 30% MFE, currently at 16% → gave back 47%
        fired, result = gate_fading_winner(
            row=row, pnl_pct=0.16, result=_base_result(),
        )
        assert fired is False  # 47% < 50%

    def test_unknown_hv_uses_standard(self):
        """No HV_20D → defaults to standard 50%."""
        row = pd.Series({"Trajectory_MFE": 0.30})
        # 30% MFE, currently at 12% → gave back 60%
        fired, result = gate_fading_winner(
            row=row, pnl_pct=0.12, result=_base_result(),
        )
        assert fired is True
        assert "standard trail" in result["Rationale"]

    def test_hv_as_percentage_normalized(self):
        """HV_20D=10 (percentage, not decimal) → normalizes to 0.10."""
        row = pd.Series({"Trajectory_MFE": 0.25, "HV_20D": 10.0})
        # Should treat as 10% HV (low vol), not 1000%
        fired, result = gate_fading_winner(
            row=row, pnl_pct=0.16, result=_base_result(),
        )
        assert fired is True  # 36% giveback > 35% tight threshold
        assert "tighter trail" in result["Rationale"]

    def test_carver_citation_in_rationale(self):
        """Rationale cites Carver for vol-scaled trailing."""
        row = pd.Series({"Trajectory_MFE": 0.30, "HV_20D": 0.25})
        fired, result = gate_fading_winner(
            row=row, pnl_pct=0.12, result=_base_result(),
        )
        assert fired is True
        assert "Carver" in result["Rationale"]
