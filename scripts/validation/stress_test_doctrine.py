#!/usr/bin/env python3
"""
Strategy Doctrine Stress Test — Synthetic Edge-Case Matrix.

Generates synthetic position rows across key axes (delta, DTE, PnL,
equity integrity, IV regime, carry sign) and runs them through both
v1 (shadow) and v2 (production) doctrine for every strategy.

Captures:
  - Which gate fires (doctrine_source)
  - Action + Urgency
  - v1 vs v2 agreement
  - Anomaly flags (contradictions, dead gates, cliff effects)

Usage:
    python scripts/validation/stress_test_doctrine.py [--strategy BUY_WRITE] [--csv]
"""

from __future__ import annotations

import argparse
import csv
import itertools
import math
import sys
import traceback
from dataclasses import dataclass, field
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

# ── Project root on path ──────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from core.management.cycle3.doctrine import (
    DoctrineAuthority,
    STATE_ACTIONABLE,
    STATE_NEUTRAL_CONFIDENT,
)
from core.management.cycle3.doctrine.strategies.buy_write import (
    buy_write_doctrine,
    buy_write_doctrine_v2,
)
from core.management.cycle3.doctrine.strategies.covered_call import (
    covered_call_doctrine,
    covered_call_doctrine_v2,
)
from core.management.cycle3.doctrine.strategies.long_option import (
    long_option_doctrine,
    long_option_doctrine_v2,
)
from core.management.cycle3.doctrine.strategies.short_put import (
    short_put_doctrine,
    short_put_doctrine_v2,
)
from core.management.cycle3.doctrine.strategies.multi_leg import (
    multi_leg_doctrine,
    multi_leg_doctrine_v2,
)
from core.management.cycle3.doctrine.strategies.stock_only import (
    stock_only_doctrine,
)

# ═══════════════════════════════════════════════════════════════════════════
# Axis Definitions
# ═══════════════════════════════════════════════════════════════════════════

# Core axes tested across all strategies
AXIS_DELTA = [0.05, 0.15, 0.30, 0.50, 0.70, 0.95]
AXIS_DTE = [1, 7, 14, 21, 45, 90, 180]
AXIS_PNL_PCT = [-0.30, -0.10, 0.0, 0.10, 0.30]
AXIS_EQUITY_INTEGRITY = ["STRONG", "WEAKENING", "BROKEN"]
AXIS_IV_REGIME = ["Low Vol", "Compression", "High Vol"]
AXIS_CARRY = ["positive", "negative"]  # theta vs margin+gamma drag

# Strategy-specific fields
AXIS_MONEYNESS = ["Deep_OTM", "OTM", "ATM", "ITM", "Deep_ITM"]
AXIS_LIFECYCLE = ["EARLY", "MID", "LATE"]
AXIS_DRIFT_DIR = ["Up", "Down", "Flat"]
AXIS_DRIFT_MAG = ["Low", "Moderate", "High"]
AXIS_THESIS = ["INTACT", "DEGRADED", "BROKEN"]

# ═══════════════════════════════════════════════════════════════════════════
# Strategy Dispatch
# ═══════════════════════════════════════════════════════════════════════════

STRATEGY_V1_V2 = {
    "BUY_WRITE":    (buy_write_doctrine,    buy_write_doctrine_v2),
    "COVERED_CALL": (covered_call_doctrine,  covered_call_doctrine_v2),
    "LONG_CALL":    (long_option_doctrine,   long_option_doctrine_v2),
    "LONG_PUT":     (long_option_doctrine,   long_option_doctrine_v2),
    "CSP":          (short_put_doctrine,     short_put_doctrine_v2),
    "STRADDLE":     (multi_leg_doctrine,     multi_leg_doctrine_v2),
}

# Which strategies are income (short premium) vs directional (long premium)
INCOME_STRATEGIES = {"BUY_WRITE", "COVERED_CALL", "CSP"}
DIRECTIONAL_STRATEGIES = {"LONG_CALL", "LONG_PUT"}
MULTI_LEG_STRATEGIES = {"STRADDLE"}


# ═══════════════════════════════════════════════════════════════════════════
# Synthetic Row Builder
# ═══════════════════════════════════════════════════════════════════════════

def _build_default_result(thesis: str = "INTACT") -> Dict[str, Any]:
    """Mirrors DoctrineAuthority.evaluate() default result dict.

    Sets ``_thesis_blocks_roll`` based on thesis state, matching the
    logic in ``DoctrineAuthority.evaluate()`` (``__init__.py`` line 321).
    """
    _thesis_blocks = thesis == "BROKEN"
    return {
        "Action": "HOLD",
        "Urgency": "HIGH" if _thesis_blocks else "LOW",
        "Rationale": "Stress test default.",
        "Doctrine_Source": "McMillan: Neutrality",
        "Decision_State": STATE_NEUTRAL_CONFIDENT,
        "Uncertainty_Reasons": [],
        "Missing_Data_Fields": [],
        "Required_Conditions_Met": True,
        "Doctrine_Trace": "",
        "Pyramid_Tier": 0,
        "Winner_Lifecycle": "THESIS_UNPROVEN",
        "_condition_blocks_roll": False,
        "_resolved_by_condition": False,
        "_thesis_blocks_roll": _thesis_blocks,
        "Thesis_Gate": "BLOCKED" if _thesis_blocks else "PASS",
    }


def _compute_derived(
    strategy: str, spot: float, delta: float, dte: int, pnl_pct: float,
    equity_integrity: str, iv_regime: str, carry: str,
    drift_dir: str, drift_mag: str, thesis: str,
) -> Dict[str, Any]:
    """
    Build a synthetic row dict with all fields each strategy reads.
    The row is internally consistent — strike, greeks, cost basis all
    derive from the core axis values so the doctrine sees plausible data.
    """
    # ── Price structure ──
    entry_price = 100.0  # normalized stock entry
    spot_price = entry_price * (1 + pnl_pct)
    strike = spot_price / delta if delta > 0 else spot_price * 2  # rough moneyness
    # For income strategies, strike is ABOVE spot (short call)
    if strategy in INCOME_STRATEGIES:
        # Short call strike: derive from delta
        # delta ~0.30 → ~5% OTM, delta ~0.50 → ATM, delta ~0.70 → ~5% ITM
        if delta < 0.40:
            strike = spot_price * (1 + 0.15 * (0.50 - delta))
        elif delta < 0.60:
            strike = spot_price * 1.01
        else:
            strike = spot_price * (1 - 0.10 * (delta - 0.60))
    elif strategy == "LONG_CALL":
        strike = spot_price * (1 - 0.05 * (delta - 0.50))
    elif strategy == "LONG_PUT":
        strike = spot_price * (1 + 0.05 * (delta - 0.50))
    elif strategy == "CSP":
        # Short put strike below spot
        if delta < 0.40:
            strike = spot_price * (1 - 0.15 * (0.50 - delta))
        elif delta < 0.60:
            strike = spot_price * 0.99
        else:
            strike = spot_price * (1 + 0.10 * (delta - 0.60))

    # ── Greeks (synthetic but consistent) ──
    theta = 0.03 if carry == "positive" else 0.005
    gamma = 0.02 if delta > 0.40 else 0.005
    hv_20d = {"Low Vol": 15.0, "Compression": 25.0, "High Vol": 45.0}[iv_regime]
    iv_30d = hv_20d * (1.1 if iv_regime == "High Vol" else 0.9 if iv_regime == "Low Vol" else 1.0)
    iv_now = iv_30d
    iv_entry = iv_30d * 0.95
    iv_pctl = {"Low Vol": 15.0, "Compression": 50.0, "High Vol": 85.0}[iv_regime]

    # ── Cost basis ──
    cum_premium = 2.50  # cumulative premium collected per share
    premium_entry = 1.50
    net_cost = entry_price - cum_premium
    n_shares = 100
    basis = net_cost * n_shares  # total cost for 100 shares

    # ── Moneyness / lifecycle ──
    pct_otm = max(0, (strike - spot_price) / spot_price) if strategy in INCOME_STRATEGIES else 0
    if delta >= 0.70:
        moneyness = "Deep_ITM"
    elif delta >= 0.50:
        moneyness = "ITM" if strategy in INCOME_STRATEGIES else "ATM"
    elif delta >= 0.30:
        moneyness = "ATM" if strategy in INCOME_STRATEGIES else "OTM"
    else:
        moneyness = "OTM" if strategy in INCOME_STRATEGIES else "Deep_OTM"

    if dte <= 14:
        lifecycle = "LATE"
    elif dte <= 45:
        lifecycle = "MID"
    else:
        lifecycle = "EARLY"

    # ── Equity integrity reason ──
    ei_reasons = {
        "STRONG": "",
        "WEAKENING": "Minor trend fade: EMA cross approaching",
        "BROKEN": "SMA20 < SMA50 + price below both + declining volume",
    }

    # ── Option last price ──
    intrinsic = max(0, spot_price - strike) if strategy != "LONG_PUT" else max(0, strike - spot_price)
    extrinsic = max(0.05, theta * dte * 0.7)
    last_price = intrinsic + extrinsic

    # ── MC columns (simulate available data) ──
    mc_assign_p = delta if strategy in INCOME_STRATEGIES else 0.0
    mc_hold_p_recovery = 0.50
    mc_hold_p_maxloss = 0.15
    mc_tb_p_profit = 0.45
    mc_tb_p_stop = 0.20

    # ── PnL ──
    pnl_total = pnl_pct * basis

    # ── Carry inversion fields ──
    margin_daily = net_cost * 0.065 / 365  # ~6.5% margin rate
    gamma_drag = gamma * (hv_20d / 100) * spot_price * 0.01

    # ── Build full row ──
    row = {
        # Identity
        "Strategy": strategy,
        "Symbol": f"TEST_{strategy}",
        "Underlying_Ticker": f"TEST",
        "AssetType": "OPTION" if strategy != "STOCK_ONLY" else "EQUITY",
        "TradeID": f"STRESS_{strategy}_{delta}_{dte}_{pnl_pct}",

        # Price
        "UL Last": spot_price,
        "Spot": spot_price,
        "Strike": strike,
        "Last": last_price,
        "Bid": last_price * 0.95,
        "Short_Call_Strike": strike if strategy in INCOME_STRATEGIES else 0,
        "Short_Call_Last": last_price if strategy in INCOME_STRATEGIES else 0,
        "Short_Call_Delta": delta if strategy in INCOME_STRATEGIES else 0,
        "Short_Call_DTE": dte if strategy in INCOME_STRATEGIES else 0,
        "Short_Call_Moneyness": moneyness if strategy in INCOME_STRATEGIES else "",
        "Short_Call_Premium": premium_entry if strategy in INCOME_STRATEGIES else 0,

        # Greeks
        "Delta": delta,
        "Theta": theta,
        "Gamma": gamma,
        "HV_20D": hv_20d,
        "hv_20d_percentile": iv_pctl,
        "Gamma_ROC_3D": 0.001,

        # IV
        "IV_30D": iv_30d,
        "IV_Now": iv_now,
        "IV_Entry": iv_entry,
        "IV_Percentile": iv_pctl,
        "IV_vs_HV_Gap": iv_30d - hv_20d,
        "iv_surface_shape": "normal",
        "iv_ts_slope_30_90": 0.02,
        "IV_Maturity_State": "MATURE",

        # Cost basis
        "Underlying_Price_Entry": entry_price,
        "Net_Cost_Basis_Per_Share": net_cost,
        "Cumulative_Premium_Collected": cum_premium,
        "Premium_Entry": premium_entry,
        "Gross_Premium_Collected": cum_premium,
        "Total_Close_Cost": 0,
        "Basis": basis,

        # Position
        "Quantity": n_shares,  # shares, not contracts — matches Basis
        "Qty": n_shares,
        "DTE": dte,
        "DTE_Entry": 45,
        "Days_In_Trade": 45 - dte,
        "Expiration": "2026-04-01",
        "Expiration_Entry": "2026-04-01",

        # State
        "Equity_Integrity_State": equity_integrity,
        "Equity_Integrity_Reason": ei_reasons[equity_integrity],
        "Thesis_State": thesis,
        "Thesis_Summary": f"Thesis {thesis}",
        "PriceStructure_State": "HEALTHY" if equity_integrity == "STRONG" else "DEGRADED",
        "TrendIntegrity_State": "HEALTHY" if equity_integrity != "BROKEN" else "DETERIORATING",
        "MomentumVelocity_State": "ACCELERATING" if drift_dir == "Up" else "DECELERATING",
        "VolatilityState_State": iv_regime.replace(" ", "_").upper(),
        "Position_Regime": iv_regime.replace(" ", "_").upper(),
        "GreekDominance_State": "THETA" if dte < 45 else "VEGA",
        "Market_Structure": "Uptrend" if drift_dir == "Up" else "Downtrend" if drift_dir == "Down" else "Range",
        "Moneyness_Label": moneyness,
        "Lifecycle_Phase": lifecycle,
        "Winner_Lifecycle": "THESIS_UNPROVEN",
        "Pyramid_Tier": 0,
        "Conviction_Status": "MEDIUM",

        # Drift
        "Drift_Direction": drift_dir,
        "Drift_Magnitude": drift_mag,
        "Drift_Persistence": 3,
        "Price_Drift_Pct": {"Up": 0.03, "Down": -0.03, "Flat": 0.0}[drift_dir],
        "Weekly_Trend_Bias": "ALIGNED" if drift_dir == "Up" else "CONFLICTING",
        "Keltner_Squeeze_On": False,

        # PnL
        "PnL_Total": pnl_total,
        "Total_GL_Dollar": pnl_total,
        "Total_GL_Decimal": pnl_pct,

        # Roll / history
        "Days_Since_Last_Roll": 30,
        "Has_Debit_Rolls": False,
        "Prior_Action": "HOLD",
        "_cycle_count": 3,
        "Trajectory_Consecutive_Debit_Rolls": 0,
        "Trajectory_Stock_Return": pnl_pct,

        # Earnings
        "Earnings_Date": "",
        "Earnings_Date": "",
        "Earnings_Track_Quarters": 8,
        "Earnings_Avg_IV_Crush_Pct": 5.0,
        "Earnings_Avg_Gap_Pct": 3.0,
        "Earnings_Avg_Move_Ratio": 0.6,
        "Earnings_Beat_Rate": 0.75,
        "Earnings_Last_Surprise_Pct": 2.0,

        # Dividend
        "Days_To_Dividend": 999,
        "Dividend_Amount": 0,

        # MC
        "MC_Assign_P_Expiry": mc_assign_p,
        "MC_Hold_P_Recovery": mc_hold_p_recovery,
        "MC_Hold_P_MaxLoss": mc_hold_p_maxloss,
        "MC_TB_P_Profit": mc_tb_p_profit,
        "MC_TB_P_Stop": mc_tb_p_stop,
        "MC_Hold_Verdict": "HOLD",
        "MC_Wait_Verdict": "WAIT",

        # Technicals
        "adx_14": 25.0,
        "rsi_14": 50.0,
        "roc_5": 0.5, "roc_10": 1.0, "roc_20": 2.0,
        "atr_14": spot_price * 0.02,
        "bb_width_z": 0.0,
        "ema50_slope": 0.1 if drift_dir == "Up" else -0.1,
        "ema9": spot_price,
        "EMA9": spot_price,
        "SMA20": spot_price * 0.99,
        "SMA50": spot_price * 0.98,
        "momentum_slope": 0.5 if drift_dir == "Up" else -0.5,
        "choppiness_index": 50.0,
        "LowerBand_20": spot_price * 0.95,
        "UpperBand_20": spot_price * 1.05,
        "MACD_Divergence": "NONE",
        "OBV_Slope": 0.01,

        # EV / feasibility
        "EV_Feasibility_Ratio": 1.2 if pnl_pct >= 0 else 0.8,
        "EV_50pct_Feasibility_Ratio": 1.0,
        "Expected_Move_10D": spot_price * 0.03,
        "Required_Move_Breakeven": spot_price * 0.05,
        "Required_Move": spot_price * 0.05,
        "Required_Move_50pct": spot_price * 0.03,
        "Recovery_Feasibility": "FEASIBLE" if pnl_pct > -0.20 else "IMPROBABLE",
        "Recovery_Move_Per_Day": 0.5,
        "Measured_Move": spot_price * 0.10,
        "HV_Daily_Move_1Sigma": spot_price * hv_20d / 100 / math.sqrt(252),
        "Intrinsic_Val": intrinsic,

        # Theta bleed
        "Theta_Bleed_Daily_Pct": (theta / last_price * 100) if last_price > 0 else 0,
        "Theta_Opportunity_Cost_Flag": False,

        # Carry inversion
        "Snapshot_TS": "2026-03-09 10:00:00",
        "Roll_Candidate_1": "",

        # Assignment
        "Assignment_Acceptable": True,

        # Portfolio context
        "Portfolio_Delta_Utilization_Pct": 40.0,
        "_Ticker_Has_Stock": True if strategy in INCOME_STRATEGIES else False,
        "_Ticker_Net_Delta": delta * 100,
        "_Ticker_Net_Theta": theta * 100,
        "_Ticker_Net_Vega": 5.0,
        "_Ticker_Trade_Count": 1,
        "_Ticker_Strategy_Mix": strategy,
        "_Ticker_Structure_Class": "INCOME_WITH_LEGS" if strategy in INCOME_STRATEGIES else "BULL_VOL_LEVERED",

        # Prior technicals (for long_option trend persistence)
        "Prior_rsi": 50.0,
        "Prior_adx": 25.0,
        "Prior_bb_width_z": 0.0,
        "Prior_momentum_slope": 0.5,
        "Prior2_momentum_slope": 0.5,

        # Long option specific
        "Delta_Entry": delta,
        "Delta_Deterioration_Streak": 0,
        "Price_Target_Entry": spot_price * 1.10,
        "Resistance_Level_1": spot_price * 1.05,
        "Entry_Chart_State_PriceStructure": "HEALTHY",
        "Entry_Chart_State_TrendIntegrity": "HEALTHY",
        "RS_vs_SPY_20d": 1.02,
        "Sector_Benchmark": "XLK",
        "Sector_Relative_Strength": 1.0,
        "Sector_RS_ZScore": 0.5,
        "Scale_Trigger_Price": 0,
        "Scale_Add_Contracts": 0,

        # Multi-leg specific
        "Call_Delta": delta,
        "Put_Delta": -delta,

        # Condition monitor (not active for stress test)
        "_Condition_Resolved": "",
        "_Resolved_Action": "",
        "_Resolved_Urgency": "",
        "_Active_Conditions": "",

        # Structural decay
        "_Structural_Decay_Regime": "NONE",
    }
    return row


# ═══════════════════════════════════════════════════════════════════════════
# Anomaly Detectors
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class AnomalyFlag:
    category: str      # CONTRADICTION, DEAD_GATE, CLIFF, DISAGREE
    severity: str      # HIGH, MEDIUM, LOW
    description: str
    scenario: str


def detect_anomalies(
    strategy: str,
    axes: Dict[str, Any],
    v1_result: Dict[str, Any],
    v2_result: Dict[str, Any],
) -> List[AnomalyFlag]:
    """Run anomaly detectors on a single scenario's results."""
    flags: List[AnomalyFlag] = []
    scenario = (
        f"δ={axes['delta']}, DTE={axes['dte']}, PnL={axes['pnl_pct']:.0%}, "
        f"EI={axes['equity_integrity']}, IV={axes['iv_regime']}, carry={axes['carry']}"
    )

    v2_action = v2_result.get("Action", "HOLD")
    v2_urgency = v2_result.get("Urgency", "LOW")
    v1_action = v1_result.get("Action", "HOLD")
    v1_urgency = v1_result.get("Urgency", "LOW")
    v2_source = v2_result.get("Doctrine_Source", "")

    # ── 1. v1 vs v2 Disagreement ──
    if v1_action != v2_action:
        flags.append(AnomalyFlag(
            "DISAGREE", "MEDIUM",
            f"v1={v1_action}/{v1_urgency} vs v2={v2_action}/{v2_urgency} | source: {v2_source}",
            scenario,
        ))

    # ── 2. EXIT on profitable far-OTM position ──
    if (v2_action == "EXIT" and axes["pnl_pct"] >= 0
            and axes["delta"] < 0.30 and strategy in INCOME_STRATEGIES):
        flags.append(AnomalyFlag(
            "CONTRADICTION", "HIGH",
            f"EXIT on profitable far-OTM income position (δ={axes['delta']}, PnL={axes['pnl_pct']:.0%}) | {v2_source}",
            scenario,
        ))

    # ── 3. EXIT CAPITAL on far-OTM with positive carry ──
    if (v2_action == "EXIT" and v2_urgency == "CRITICAL"
            and axes["delta"] < 0.30 and axes["carry"] == "positive"
            and strategy in INCOME_STRATEGIES):
        flags.append(AnomalyFlag(
            "CONTRADICTION", "HIGH",
            f"EXIT CRITICAL on far-OTM + positive carry | {v2_source}",
            scenario,
        ))

    # ── 4. ROLL on nearly worthless option ──
    if (v2_action == "ROLL" and axes["delta"] < 0.10 and axes["dte"] <= 7
            and strategy in INCOME_STRATEGIES):
        flags.append(AnomalyFlag(
            "CONTRADICTION", "HIGH",
            f"ROLL on nearly worthless option (δ={axes['delta']}, DTE={axes['dte']}) — let it expire | {v2_source}",
            scenario,
        ))

    # ── 5. HOLD on deep loss with broken equity ──
    if (v2_action == "HOLD" and axes["pnl_pct"] <= -0.25
            and axes["equity_integrity"] == "BROKEN"
            and strategy in INCOME_STRATEGIES):
        flags.append(AnomalyFlag(
            "CONTRADICTION", "MEDIUM",
            f"HOLD at -25%+ loss with BROKEN equity — should consider EXIT | {v2_source}",
            scenario,
        ))

    # ── 6. ROLL when thesis is BROKEN ──
    if v2_action == "ROLL" and axes["thesis"] == "BROKEN":
        flags.append(AnomalyFlag(
            "CONTRADICTION", "HIGH",
            f"ROLL with BROKEN thesis — McMillan Ch.3: don't roll broken thesis | {v2_source}",
            scenario,
        ))

    # ── 7. EXIT on profitable position with strong equity ──
    if (v2_action == "EXIT" and axes["pnl_pct"] > 0
            and axes["equity_integrity"] == "STRONG"
            and axes["thesis"] != "BROKEN"):
        flags.append(AnomalyFlag(
            "CONTRADICTION", "MEDIUM",
            f"EXIT on profitable position with STRONG equity and intact thesis | {v2_source}",
            scenario,
        ))

    # ── 8. Urgency cliff: adjacent delta values produce CRITICAL vs LOW ──
    # (handled in post-processing by comparing adjacent rows)

    # ── 9. HOLD on DTE=1 with significant position ──
    if v2_action == "HOLD" and axes["dte"] == 1 and strategy in INCOME_STRATEGIES:
        # DTE=1 income should be either ROLL or let expire, not HOLD
        if v2_urgency == "LOW":
            flags.append(AnomalyFlag(
                "CONTRADICTION", "MEDIUM",
                f"HOLD LOW at DTE=1 for income strategy — should have urgency | {v2_source}",
                scenario,
            ))

    # ── 10. Directional: EXIT on unrealized gain with momentum ──
    if (strategy in DIRECTIONAL_STRATEGIES and v2_action == "EXIT"
            and axes["pnl_pct"] > 0.10 and axes["drift_dir"] == "Up"
            and axes["equity_integrity"] == "STRONG"):
        flags.append(AnomalyFlag(
            "CONTRADICTION", "MEDIUM",
            f"EXIT directional with +10% gain + upward drift + strong equity | {v2_source}",
            scenario,
        ))

    return flags


def detect_cliff_effects(
    strategy: str, results: List[Dict[str, Any]]
) -> List[AnomalyFlag]:
    """Find adjacent axis values that produce radically different actions."""
    flags = []
    urgency_rank = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}

    for i in range(len(results)):
        for j in range(i + 1, len(results)):
            a = results[i]
            b = results[j]

            # Only compare rows that differ in exactly 1 axis
            diff_axes = []
            for key in ["delta", "dte", "pnl_pct", "equity_integrity", "iv_regime", "carry"]:
                if a["axes"][key] != b["axes"][key]:
                    diff_axes.append(key)
            if len(diff_axes) != 1:
                continue

            axis_name = diff_axes[0]
            a_action = a["v2_action"]
            b_action = b["v2_action"]
            a_urg = urgency_rank.get(a["v2_urgency"], 0)
            b_urg = urgency_rank.get(b["v2_urgency"], 0)

            # Cliff: action changes AND urgency jumps by 2+ levels
            if a_action != b_action and abs(a_urg - b_urg) >= 2:
                flags.append(AnomalyFlag(
                    "CLIFF", "MEDIUM",
                    f"Cliff on {axis_name}: "
                    f"{a['axes'][axis_name]}→{b['axes'][axis_name]} "
                    f"produces {a_action}/{a['v2_urgency']}→{b_action}/{b['v2_urgency']}",
                    f"{strategy} | {axis_name} cliff",
                ))

    return flags


# ═══════════════════════════════════════════════════════════════════════════
# Main Runner
# ═══════════════════════════════════════════════════════════════════════════

def run_strategy_stress(
    strategy: str,
    v1_fn,
    v2_fn,
    verbose: bool = False,
) -> Tuple[List[Dict], List[AnomalyFlag]]:
    """Run all axis combinations for one strategy. Returns (results, anomalies)."""
    results = []
    anomalies = []

    # For income strategies, use full axis set
    # For directional, skip carry axis (always negative — they pay theta)
    if strategy in INCOME_STRATEGIES:
        carry_axis = AXIS_CARRY
    else:
        carry_axis = ["negative"]  # long premium always bleeds theta

    combos = list(itertools.product(
        AXIS_DELTA, AXIS_DTE, AXIS_PNL_PCT,
        AXIS_EQUITY_INTEGRITY, AXIS_IV_REGIME,
        carry_axis,
    ))

    # Also vary drift and thesis for a representative subset
    drift_thesis_combos = [
        ("Up", "Low", "INTACT"),
        ("Down", "High", "INTACT"),
        ("Down", "High", "BROKEN"),
        ("Flat", "Moderate", "DEGRADED"),
    ]

    total = len(combos) * len(drift_thesis_combos)
    errors = 0

    for combo_idx, (delta, dte, pnl_pct, ei, iv, carry) in enumerate(combos):
        for drift_dir, drift_mag, thesis in drift_thesis_combos:
            axes = {
                "delta": delta, "dte": dte, "pnl_pct": pnl_pct,
                "equity_integrity": ei, "iv_regime": iv, "carry": carry,
                "drift_dir": drift_dir, "drift_mag": drift_mag, "thesis": thesis,
            }

            row_data = _compute_derived(strategy, 100.0, **axes)
            row = pd.Series(row_data)

            # Run v2 (production)
            try:
                result_v2 = _build_default_result(thesis=thesis)
                result_v2 = v2_fn(row, result_v2)
            except Exception as e:
                result_v2 = {"Action": "ERROR", "Urgency": "ERROR",
                             "Doctrine_Source": f"CRASH: {e}", "Rationale": traceback.format_exc()}
                errors += 1

            # Run v1 (shadow)
            try:
                result_v1 = _build_default_result(thesis=thesis)
                result_v1 = v1_fn(row, result_v1)
            except Exception as e:
                result_v1 = {"Action": "ERROR", "Urgency": "ERROR",
                             "Doctrine_Source": f"CRASH: {e}", "Rationale": traceback.format_exc()}
                errors += 1

            record = {
                "strategy": strategy,
                "axes": axes,
                "v1_action": result_v1.get("Action", "?"),
                "v1_urgency": result_v1.get("Urgency", "?"),
                "v1_source": result_v1.get("Doctrine_Source", ""),
                "v2_action": result_v2.get("Action", "?"),
                "v2_urgency": result_v2.get("Urgency", "?"),
                "v2_source": result_v2.get("Doctrine_Source", ""),
                "v2_rationale": result_v2.get("Rationale", "")[:200],
                "proposals_considered": result_v2.get("Proposals_Considered", ""),
                "proposals_summary": result_v2.get("Proposals_Summary", ""),
                "resolution_method": result_v2.get("Resolution_Method", ""),
                "agree": result_v1.get("Action") == result_v2.get("Action"),
            }
            results.append(record)

            # Detect anomalies
            row_anomalies = detect_anomalies(strategy, axes, result_v1, result_v2)
            anomalies.extend(row_anomalies)

    # Cliff detection (compare adjacent scenarios)
    cliff_flags = detect_cliff_effects(strategy, results)
    anomalies.extend(cliff_flags)

    return results, anomalies, errors, total


def print_report(
    all_results: Dict[str, List[Dict]],
    all_anomalies: Dict[str, List[AnomalyFlag]],
    all_errors: Dict[str, int],
    all_totals: Dict[str, int],
):
    """Print summary report to terminal."""
    print("=" * 80)
    print("  DOCTRINE STRESS TEST — SYNTHETIC EDGE-CASE MATRIX")
    print("=" * 80)

    grand_total = sum(all_totals.values())
    grand_agree = sum(sum(1 for r in res if r["agree"]) for res in all_results.values())
    grand_anomalies = sum(len(a) for a in all_anomalies.values())
    grand_errors = sum(all_errors.values())

    print(f"\n  Total scenarios: {grand_total:,}")
    print(f"  v1/v2 agreement: {grand_agree:,}/{grand_total:,} ({grand_agree/grand_total:.1%})")
    print(f"  Total anomalies: {grand_anomalies}")
    print(f"  Runtime errors:  {grand_errors}")

    for strategy in all_results:
        results = all_results[strategy]
        anomalies = all_anomalies[strategy]
        total = all_totals[strategy]
        errors = all_errors[strategy]
        agree = sum(1 for r in results if r["agree"])

        print(f"\n{'─' * 80}")
        print(f"  {strategy}")
        print(f"{'─' * 80}")
        print(f"  Scenarios: {total:,} | Agreement: {agree}/{total} ({agree/total:.1%}) | Errors: {errors}")

        # Action distribution (v2)
        action_dist = {}
        for r in results:
            a = r["v2_action"]
            action_dist[a] = action_dist.get(a, 0) + 1
        print(f"  v2 Action distribution: {dict(sorted(action_dist.items(), key=lambda x: -x[1]))}")

        # Anomaly summary
        if anomalies:
            by_cat = {}
            for a in anomalies:
                by_cat.setdefault(a.category, []).append(a)
            for cat in sorted(by_cat):
                items = by_cat[cat]
                high = sum(1 for a in items if a.severity == "HIGH")
                med = sum(1 for a in items if a.severity == "MEDIUM")
                print(f"  {cat}: {len(items)} ({high} HIGH, {med} MEDIUM)")

            # Print HIGH severity anomalies
            high_anomalies = [a for a in anomalies if a.severity == "HIGH"]
            if high_anomalies:
                print(f"\n  HIGH severity anomalies ({len(high_anomalies)}):")
                # Dedupe similar anomalies
                seen = set()
                for a in high_anomalies[:20]:
                    key = (a.category, a.description[:80])
                    if key in seen:
                        continue
                    seen.add(key)
                    print(f"    [{a.category}] {a.description}")
                    print(f"             @ {a.scenario}")
                if len(high_anomalies) > 20:
                    print(f"    ... and {len(high_anomalies) - 20} more")
        else:
            print("  No anomalies detected.")

    # ── Disagreement heatmap ──
    print(f"\n{'=' * 80}")
    print("  v1 → v2 DISAGREEMENT PATTERNS")
    print(f"{'=' * 80}")
    for strategy in all_results:
        disagreements = [r for r in all_results[strategy] if not r["agree"]]
        if not disagreements:
            continue
        print(f"\n  {strategy} — {len(disagreements)} disagreements:")
        # Group by (v1_action → v2_action)
        transitions = {}
        for d in disagreements:
            key = f"{d['v1_action']} → {d['v2_action']}"
            transitions[key] = transitions.get(key, 0) + 1
        for t, count in sorted(transitions.items(), key=lambda x: -x[1]):
            print(f"    {t}: {count}")


def write_csv(all_results: Dict[str, List[Dict]], path: str):
    """Write all results to CSV for offline analysis."""
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "strategy", "delta", "dte", "pnl_pct", "equity_integrity",
            "iv_regime", "carry", "drift_dir", "drift_mag", "thesis",
            "v1_action", "v1_urgency", "v1_source",
            "v2_action", "v2_urgency", "v2_source",
            "proposals_considered", "proposals_summary", "resolution_method",
            "agree",
        ])
        for strategy, results in all_results.items():
            for r in results:
                ax = r["axes"]
                writer.writerow([
                    strategy, ax["delta"], ax["dte"], ax["pnl_pct"],
                    ax["equity_integrity"], ax["iv_regime"], ax["carry"],
                    ax["drift_dir"], ax["drift_mag"], ax["thesis"],
                    r["v1_action"], r["v1_urgency"], r["v1_source"],
                    r["v2_action"], r["v2_urgency"], r["v2_source"],
                    r["proposals_considered"], r["proposals_summary"],
                    r["resolution_method"], r["agree"],
                ])
    print(f"\n  CSV written to: {path}")


def main():
    parser = argparse.ArgumentParser(description="Doctrine Stress Test")
    parser.add_argument("--strategy", type=str, default=None,
                        help="Run single strategy (e.g. BUY_WRITE)")
    parser.add_argument("--csv", action="store_true",
                        help="Write results to CSV")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    strategies = (
        {args.strategy.upper(): STRATEGY_V1_V2[args.strategy.upper()]}
        if args.strategy else STRATEGY_V1_V2
    )

    all_results = {}
    all_anomalies = {}
    all_errors = {}
    all_totals = {}

    for strategy, (v1_fn, v2_fn) in strategies.items():
        print(f"  Running {strategy}...", end=" ", flush=True)
        results, anomalies, errors, total = run_strategy_stress(
            strategy, v1_fn, v2_fn, verbose=args.verbose,
        )
        all_results[strategy] = results
        all_anomalies[strategy] = anomalies
        all_errors[strategy] = errors
        all_totals[strategy] = total
        print(f"{total:,} scenarios, {len(anomalies)} anomalies, {errors} errors")

    print_report(all_results, all_anomalies, all_errors, all_totals)

    if args.csv:
        csv_path = str(ROOT / "output" / "doctrine_stress_test.csv")
        write_csv(all_results, csv_path)


if __name__ == "__main__":
    main()
