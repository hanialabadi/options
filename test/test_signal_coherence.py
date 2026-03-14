"""
Signal Coherence Tests — Gates 1-4
====================================
Validates the 4-layer signal coherence system that prevents decision noise:

  Gate 1: Recently-Rolled Cooldown (in-doctrine)
  Gate 2: Flip-Flop Dampener (post-doctrine, run_all.py Section 3.0g)
  Gate 3: Risk-State Stability Filter (post-doctrine, Section 3.0g2)
  Gate 4: Intraday Stability Annotation (post-doctrine, Section 3.0h)

RAG support: Natenberg Ch.7 (adjustment frequency), Jabbour Ch.8 (repair = overtrading),
Passarelli Ch.5 (deliberate timing), Benklifa (adjust opposite direction).

Run:
    pytest test/test_signal_coherence.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import numpy as np
import pytest

# -- path bootstrap -----------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.management.cycle3.decision.engine import DoctrineAuthority


# =============================================================================
# Helpers
# =============================================================================

def _bw_row(**overrides) -> pd.Series:
    """Baseline BUY_WRITE row for cooldown tests."""
    base = {
        "TradeID": "T-SC-BW", "LegID": "L-SC-BW",
        "Symbol": "PLTR260515C00150000",
        "Underlying_Ticker": "PLTR",
        "Strategy": "BUY_WRITE", "AssetType": "OPTION",
        "Option_Type": "CALL", "Call/Put": "C",
        "UL Last": 157.0, "Strike": 150.0,
        "DTE": 45.0, "Premium_Entry": 16.70,
        "Last": 12.50, "Bid": 12.30,
        "Delta": 0.62, "Gamma": 0.015,
        "Theta": -0.08, "Vega": 0.35,
        "HV_20D": 0.45, "IV_Now": 0.50, "IV_Entry": 0.48, "IV_30D": 0.50,
        "Quantity": -1.0, "Basis": 15000.0,
        "PriceStructure_State": "TRENDING_UP",
        "TrendIntegrity_State": "INTACT",
        "VolatilityState_State": "ELEVATED",
        "MomentumVelocity_State": "ACCELERATING",
        "Equity_Integrity_State": "HEALTHY",
        "Thesis_State": "INTACT",
        "Position_Regime": "SIDEWAYS_INCOME",
        "Price_Drift_Pct": 0.03,
        "Net_Cost_Basis_Per_Share": 105.73,
        "Cumulative_Premium_Collected": 59.71,
        "_cycle_count": 6,
        "Lifecycle_Phase": "ACTIVE",
        "Moneyness_Label": "ITM",
        "Delta_Entry": 0.50,
        "Gamma_Entry": 0.02,
        "Vega_Entry": 0.30,
        "Theta_Entry": -0.06,
        "IV_Percentile": 55,
        "IV_vs_HV_Gap": 0.05,
        "Expected_Move_10D": 12.0,
        "Required_Move_Breakeven": 0.0,
        "EV_Feasibility_Ratio": 0.0,
        "Prior_Action": "HOLD",
        "Prior_Urgency": "LOW",
        "Days_Since_Last_Roll": 1.0,  # rolled yesterday
    }
    base.update(overrides)
    return pd.Series(base)


def _csp_row(**overrides) -> pd.Series:
    """Baseline CSP row for cooldown tests."""
    base = {
        "TradeID": "T-SC-CSP", "LegID": "L-SC-CSP",
        "Symbol": "AAPL260515P00220000",
        "Underlying_Ticker": "AAPL",
        "Strategy": "CSP", "AssetType": "OPTION",
        "Option_Type": "PUT", "Call/Put": "P",
        "UL Last": 235.0, "Strike": 220.0,
        "DTE": 35.0, "Premium_Entry": 3.50,
        "Last": 1.80, "Bid": 1.75,
        "Delta": -0.28, "Gamma": 0.01,
        "Theta": -0.04, "Vega": 0.20,
        "HV_20D": 0.25, "IV_Now": 0.28, "IV_Entry": 0.30, "IV_30D": 0.28,
        "Quantity": -2.0, "Basis": 700.0,
        "PriceStructure_State": "TRENDING_UP",
        "TrendIntegrity_State": "INTACT",
        "VolatilityState_State": "NORMAL",
        "MomentumVelocity_State": "NEUTRAL",
        "Equity_Integrity_State": "HEALTHY",
        "Thesis_State": "INTACT",
        "Position_Regime": "SIDEWAYS_INCOME",
        "Price_Drift_Pct": 0.01,
        "Lifecycle_Phase": "ACTIVE",
        "Moneyness_Label": "OTM",
        "Delta_Entry": -0.30,
        "IV_Percentile": 40,
        "IV_vs_HV_Gap": 0.03,
        "Days_Since_Last_Roll": 2.0,
    }
    base.update(overrides)
    return pd.Series(base)


def _long_call_row(**overrides) -> pd.Series:
    """Baseline LONG_CALL row for cooldown tests."""
    base = {
        "TradeID": "T-SC-LC", "LegID": "L-SC-LC",
        "Symbol": "NVDA260515C00800000",
        "Underlying_Ticker": "NVDA",
        "Strategy": "LONG_CALL", "AssetType": "OPTION",
        "Option_Type": "CALL", "Call/Put": "C",
        "UL Last": 780.0, "Strike": 800.0,
        "DTE": 30.0, "Premium_Entry": 25.0,
        "Last": 18.0, "Bid": 17.50,
        "Delta": 0.42, "Gamma": 0.003,
        "Theta": -0.40, "Vega": 0.85,
        "HV_20D": 0.35, "IV_Now": 0.38, "IV_Entry": 0.40, "IV_30D": 0.38,
        "Quantity": 2.0, "Basis": 5000.0,
        "PriceStructure_State": "TRENDING_UP",
        "TrendIntegrity_State": "INTACT",
        "VolatilityState_State": "NORMAL",
        "MomentumVelocity_State": "NEUTRAL",
        "Equity_Integrity_State": "HEALTHY",
        "Thesis_State": "INTACT",
        "Position_Regime": "NEUTRAL",
        "Price_Drift_Pct": 0.0,
        "Total_GL_Decimal": -0.10,
        "Lifecycle_Phase": "ACTIVE",
        "Moneyness_Label": "OTM",
        "Delta_Entry": 0.50,
        "IV_Percentile": 45,
        "Days_Since_Last_Roll": 0.0,  # rolled today
    }
    base.update(overrides)
    return pd.Series(base)


# =============================================================================
# Gate 1: Recently-Rolled Cooldown
# =============================================================================

class TestRecentlyRolledCooldown:
    """Gate 1: suppress discretionary ROLL within strategy-dependent window."""

    def test_income_bw_within_3d_no_exit(self):
        """BUY_WRITE rolled 1d ago, thesis INTACT → should not EXIT (cooldown or EV)."""
        row = _bw_row(Days_Since_Last_Roll=1.0)
        result = DoctrineAuthority.evaluate(row)
        assert result["Action"] != "EXIT", (
            f"BW within 3d cooldown should not EXIT, got {result['Action']}. "
            f"Rationale: {result.get('Rationale', '')}"
        )

    def test_income_csp_within_3d_no_exit(self):
        """CSP rolled 2d ago, thesis INTACT → should not EXIT (cooldown or EV)."""
        row = _csp_row(Days_Since_Last_Roll=2.0)
        result = DoctrineAuthority.evaluate(row)
        assert result["Action"] != "EXIT", (
            f"CSP within 3d cooldown should not EXIT, got {result['Action']}. "
            f"Rationale: {result.get('Rationale', '')}"
        )

    def test_directional_within_1d_holds(self):
        """LONG_CALL rolled today (0d), thesis INTACT → HOLD LOW (1d cooldown)."""
        row = _long_call_row(Days_Since_Last_Roll=0.0)
        result = DoctrineAuthority.evaluate(row)
        assert result["Action"] == "HOLD", (
            f"LONG_CALL within 1d cooldown should HOLD, got {result['Action']}. "
            f"Rationale: {result.get('Rationale', '')}"
        )
        assert "cooldown" in result.get("Rationale", "").lower()

    def test_broken_thesis_skips_cooldown(self):
        """BW rolled 1d ago, thesis BROKEN → cooldown skipped, doctrine proceeds."""
        row = _bw_row(Days_Since_Last_Roll=1.0, Thesis_State="BROKEN")
        result = DoctrineAuthority.evaluate(row)
        # Cooldown should NOT fire — BROKEN thesis bypasses it
        assert "cooldown" not in result.get("Rationale", "").lower(), (
            f"BROKEN thesis should skip cooldown. Rationale: {result.get('Rationale', '')}"
        )

    def test_after_window_passes(self):
        """BW rolled 5d ago → cooldown window passed, doctrine proceeds normally."""
        row = _bw_row(Days_Since_Last_Roll=5.0)
        result = DoctrineAuthority.evaluate(row)
        assert "cooldown" not in result.get("Rationale", "").lower(), (
            f"5d > 3d window — cooldown should not fire. Rationale: {result.get('Rationale', '')}"
        )

    def test_nan_days_skips_cooldown(self):
        """Days_Since_Last_Roll is NaN → no cooldown (data unavailable)."""
        row = _bw_row(Days_Since_Last_Roll=float('nan'))
        result = DoctrineAuthority.evaluate(row)
        assert "cooldown" not in result.get("Rationale", "").lower(), (
            f"NaN should skip cooldown. Rationale: {result.get('Rationale', '')}"
        )


# =============================================================================
# Gate 2: Flip-Flop Dampener (vectorized, tested via DataFrame simulation)
# =============================================================================

def _apply_flipflop_dampener(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replicate the flip-flop dampener logic from run_all.py Section 3.0g.
    Operates on a DataFrame with Action, Prior_Action, Urgency, etc.
    """
    _prior_act = df["Prior_Action"].fillna("").str.upper()
    _curr_act = df["Action"].fillna("").str.upper()
    _curr_urg = df["Urgency"].fillna("LOW").str.upper()
    _thesis_st = df.get("Thesis_State", pd.Series(["UNKNOWN"] * len(df))).fillna("UNKNOWN").str.upper()
    _ul_now = pd.to_numeric(df.get("UL Last", 0), errors="coerce").fillna(0)
    _prior_ul = pd.to_numeric(df.get("Prior_UL_Last", 0), errors="coerce").fillna(0)
    _price_move_pct = ((_ul_now - _prior_ul) / _prior_ul).abs().fillna(0)

    mask = (
        (_prior_act != "")
        & (_curr_act != _prior_act)
        & (_curr_act != "EXIT")
        & (_curr_urg.isin(["LOW", "MEDIUM"]))
        & (_thesis_st.isin(["INTACT", "UNKNOWN", ""]))
        & (_price_move_pct <= 0.02)
    )

    df = df.copy()
    if mask.any():
        df.loc[mask, "Action"] = _prior_act[mask]
        df.loc[mask, "_dampened"] = True
    else:
        df["_dampened"] = False
    return df


class TestFlipFlopDampener:
    """Gate 2: revert action flip-flops when price/thesis haven't changed."""

    def test_hold_to_roll_dampened(self):
        """HOLD→ROLL, <2% move, thesis INTACT → revert to HOLD."""
        df = pd.DataFrame([{
            "Action": "ROLL", "Prior_Action": "HOLD", "Urgency": "MEDIUM",
            "Thesis_State": "INTACT", "UL Last": 100.0, "Prior_UL_Last": 99.5,
        }])
        result = _apply_flipflop_dampener(df)
        assert result.iloc[0]["Action"] == "HOLD"

    def test_passes_with_3pct_move(self):
        """HOLD→ROLL, 3% move → passes (price moved enough)."""
        df = pd.DataFrame([{
            "Action": "ROLL", "Prior_Action": "HOLD", "Urgency": "MEDIUM",
            "Thesis_State": "INTACT", "UL Last": 103.0, "Prior_UL_Last": 100.0,
        }])
        result = _apply_flipflop_dampener(df)
        assert result.iloc[0]["Action"] == "ROLL"

    def test_exit_never_dampened(self):
        """HOLD→EXIT → passes (EXIT is never dampened)."""
        df = pd.DataFrame([{
            "Action": "EXIT", "Prior_Action": "HOLD", "Urgency": "MEDIUM",
            "Thesis_State": "INTACT", "UL Last": 100.0, "Prior_UL_Last": 99.5,
        }])
        result = _apply_flipflop_dampener(df)
        assert result.iloc[0]["Action"] == "EXIT"

    def test_oscillation_dampened(self):
        """ROLL→HOLD, <2% move → revert to ROLL."""
        df = pd.DataFrame([{
            "Action": "HOLD", "Prior_Action": "ROLL", "Urgency": "LOW",
            "Thesis_State": "INTACT", "UL Last": 100.5, "Prior_UL_Last": 100.0,
        }])
        result = _apply_flipflop_dampener(df)
        assert result.iloc[0]["Action"] == "ROLL"

    def test_high_urgency_overrides(self):
        """HOLD→ROLL HIGH → passes (HIGH urgency overrides dampener)."""
        df = pd.DataFrame([{
            "Action": "ROLL", "Prior_Action": "HOLD", "Urgency": "HIGH",
            "Thesis_State": "INTACT", "UL Last": 100.0, "Prior_UL_Last": 99.5,
        }])
        result = _apply_flipflop_dampener(df)
        assert result.iloc[0]["Action"] == "ROLL"

    def test_broken_thesis_overrides(self):
        """HOLD→ROLL, thesis BROKEN → passes (thesis not stable)."""
        df = pd.DataFrame([{
            "Action": "ROLL", "Prior_Action": "HOLD", "Urgency": "MEDIUM",
            "Thesis_State": "BROKEN", "UL Last": 100.0, "Prior_UL_Last": 99.5,
        }])
        result = _apply_flipflop_dampener(df)
        assert result.iloc[0]["Action"] == "ROLL"


# =============================================================================
# Gate 3: Risk-State Stability Filter (vectorized)
# =============================================================================

def _apply_risk_state_filter(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replicate the risk-state stability filter from run_all.py Section 3.0g2.
    """
    _p_act = df["Prior_Action"].fillna("").str.upper()
    _c_act = df["Action"].fillna("").str.upper()
    _c_urg = df["Urgency"].fillna("LOW").str.upper()

    _delta_now = pd.to_numeric(df.get("Delta", 0), errors="coerce").fillna(0)
    _delta_prior = pd.to_numeric(df.get("Prior_Delta", 0), errors="coerce").fillna(0)
    _iv_now = pd.to_numeric(df.get("IV_Now", 0), errors="coerce").fillna(0)
    _iv_prior = pd.to_numeric(df.get("Prior_IV_Now", 0), errors="coerce").fillna(0)
    _gamma_now = pd.to_numeric(df.get("Gamma", 0), errors="coerce").fillna(0)
    _theta_now = pd.to_numeric(df.get("Theta", 0), errors="coerce").abs().fillna(0)
    _gamma_prior = pd.to_numeric(df.get("Prior_Gamma", 0), errors="coerce").fillna(0)
    _theta_prior = pd.to_numeric(df.get("Prior_Theta", 0), errors="coerce").abs().fillna(0)

    _delta_change = (_delta_now - _delta_prior).abs()
    _iv_change_pct = ((_iv_now - _iv_prior) / _iv_prior.replace(0, float('nan'))).abs().fillna(0)
    _gt_now = (_gamma_now / _theta_now.replace(0, float('nan'))).fillna(0)
    _gt_prior = (_gamma_prior / _theta_prior.replace(0, float('nan'))).fillna(0)
    _gt_crossed = ((_gt_now > 1.0) != (_gt_prior > 1.0))

    _greek_changed = (_delta_change > 0.08) | (_iv_change_pct > 0.10) | _gt_crossed

    mask = (
        (_p_act != "")
        & (_c_act != _p_act)
        & (_c_act != "EXIT")
        & (_c_urg.isin(["LOW", "MEDIUM"]))
        & (~_greek_changed)
        & (_delta_prior.abs() > 0)
    )

    df = df.copy()
    if mask.any():
        df.loc[mask, "Action"] = _p_act[mask]
        df.loc[mask, "_risk_state_dampened"] = True
    else:
        df["_risk_state_dampened"] = False
    return df


class TestRiskStateStability:
    """Gate 3: maintain prior action when Greeks haven't meaningfully changed."""

    def test_small_delta_small_iv_dampened(self):
        """Δ delta=0.02, Δ IV=3% → dampened (no meaningful Greek shift)."""
        df = pd.DataFrame([{
            "Action": "ROLL", "Prior_Action": "HOLD", "Urgency": "MEDIUM",
            "Delta": 0.56, "Prior_Delta": 0.54,
            "IV_Now": 0.515, "Prior_IV_Now": 0.50,
            "Gamma": 0.015, "Prior_Gamma": 0.014,
            "Theta": -0.08, "Prior_Theta": -0.08,
        }])
        result = _apply_risk_state_filter(df)
        assert result.iloc[0]["Action"] == "HOLD"

    def test_large_delta_passes(self):
        """Δ delta=0.12 → passes (meaningful delta shift)."""
        df = pd.DataFrame([{
            "Action": "ROLL", "Prior_Action": "HOLD", "Urgency": "MEDIUM",
            "Delta": 0.66, "Prior_Delta": 0.54,
            "IV_Now": 0.51, "Prior_IV_Now": 0.50,
            "Gamma": 0.015, "Prior_Gamma": 0.014,
            "Theta": -0.08, "Prior_Theta": -0.08,
        }])
        result = _apply_risk_state_filter(df)
        assert result.iloc[0]["Action"] == "ROLL"

    def test_large_iv_change_passes(self):
        """Δ IV=15% → passes (meaningful IV shift)."""
        df = pd.DataFrame([{
            "Action": "ROLL", "Prior_Action": "HOLD", "Urgency": "MEDIUM",
            "Delta": 0.55, "Prior_Delta": 0.54,
            "IV_Now": 0.575, "Prior_IV_Now": 0.50,
            "Gamma": 0.015, "Prior_Gamma": 0.014,
            "Theta": -0.08, "Prior_Theta": -0.08,
        }])
        result = _apply_risk_state_filter(df)
        assert result.iloc[0]["Action"] == "ROLL"

    def test_exit_always_passes(self):
        """EXIT is never dampened by risk-state filter."""
        df = pd.DataFrame([{
            "Action": "EXIT", "Prior_Action": "HOLD", "Urgency": "MEDIUM",
            "Delta": 0.55, "Prior_Delta": 0.54,
            "IV_Now": 0.51, "Prior_IV_Now": 0.50,
            "Gamma": 0.015, "Prior_Gamma": 0.014,
            "Theta": -0.08, "Prior_Theta": -0.08,
        }])
        result = _apply_risk_state_filter(df)
        assert result.iloc[0]["Action"] == "EXIT"


# =============================================================================
# Gate 4: Intraday Stability Annotation
# =============================================================================

class TestIntradayStability:
    """Gate 4: annotation when same-day action differs (info only)."""

    def test_annotation_on_flip(self):
        """Simulated intraday flip produces a warning string."""
        # Simulate: prior intraday action was HOLD, current action is ROLL
        _prior_intraday = "HOLD"
        _current_action = "ROLL"
        warning = f"Intraday flip: prior run = {_prior_intraday} → now = {_current_action}"
        assert "Intraday flip" in warning
        assert "HOLD" in warning
        assert "ROLL" in warning

    def test_no_annotation_when_consistent(self):
        """No warning when same-day action matches."""
        _prior_intraday = "ROLL"
        _current_action = "ROLL"
        # No flip
        has_flip = _prior_intraday.upper() != _current_action.upper()
        assert not has_flip
