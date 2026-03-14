"""
Roll Candidate Engine
=====================
When doctrine decides ROLL is the right action, this engine answers:
    "Roll to WHAT — and at what cost?"

It fetches the live option chain for the underlying, filters for contracts
that match the thesis (strategy type + directional bias + DTE window),
scores each candidate on delta alignment, liquidity, and cost-effectiveness,
and returns the top 3 ranked candidates with execution detail.

Design principles:
  - Only called when Action=ROLL (never speculatively)
  - Reuses existing SchwabClient (no new auth)
  - Respects the roll doctrine per strategy:
      LONG_CALL / LONG_PUT : roll to lower IV or further DTE, same directional delta
      BUY_WRITE / COVERED_CALL : roll to next cycle OTM call, preserve income
      SHORT_PUT : roll down-and-out to reduce assignment risk
  - Output: Roll_Candidate_1/2/3 columns with strike, expiry, mid price, delta,
    IV, OI, and cost-to-roll (net debit/credit)
  - Market-hours aware: if market closed, returns best estimate from last chain
    (logged as stale)

McMillan Ch.3 (Rolling Covered Calls) / Ch.4 (Managing Long Options) /
Passarelli Ch.6 (Roll Management) / Natenberg Ch.5 (Vol Edge)
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from core.management.cycle1.identity.constants import FIDELITY_MARGIN_RATE

logger = logging.getLogger(__name__)

# The yield scoring benchmark: a roll candidate must earn at least the Fidelity margin rate
# (10.375%/yr) to cover carrying cost. Earning 2× the rate = excellent (score=1.0).
# McMillan Ch.3: "Any roll that fails to cover the financing cost is negative carry — don't do it."
_YIELD_BENCHMARK_EXCELLENT = FIDELITY_MARGIN_RATE * 2   # ~20.75%/yr = score 1.0
_YIELD_BENCHMARK_NEUTRAL   = FIDELITY_MARGIN_RATE       # ~10.375%/yr = score 0.5 (just covering carry)

# ── Roll mode constants ────────────────────────────────────────────────────────
# Roll mode is determined by the current position's delta at the time doctrine fires.
# PRE_ITM   : delta 0.55–0.70 — roll window is open, credit still available.
#             Standard DTE cycle; prioritize credit rolls above cost basis.
# EMERGENCY : delta > 0.70 — deep ITM, extrinsic nearly gone.
#             Must extend DTE to 45–150 to find any strike above basis for a
#             small debit or break-even roll. Near-dated rolls are pure debit.
# NORMAL    : delta < 0.55 — discretionary roll (50% premium capture, etc.)
# WEEKLY          : Structural fragility detected. Two triggers (either → WEEKLY):
#                   1. Equity_Integrity = BROKEN  → unconditional (structure already failed)
#                   2. Equity_Integrity = WEAKENING + earnings ≤45d or unknown
#                   Fragile position — roll shorter (7–21 DTE) to minimize commitment
#                   and preserve flexibility. Cannot afford to be locked in 45+ days
#                   with structural deterioration already underway.
#                   McMillan Ch.3: "When a position is structurally impaired, shorter
#                   cycles reduce compounding risk — don't over-commit with a long roll."
#
# BROKEN_RECOVERY  : Structural failure + gamma dominance + IV < realized vol.
#                   Three conditions (all required → BROKEN_RECOVERY, overrides WEEKLY):
#                   1. Equity_Integrity = BROKEN
#                   2. Gamma drag > threshold × theta:
#                        1.8× when Gamma_ROC_3D ≤ 0.25 (structurally elevated)
#                        2.4× when Gamma_ROC_3D > 0.25 (spike guard — intraday artifact)
#                   3. IV < HV  (selling vol below realized — negative edge)
#                   Weekly is wrong here: gamma ∝ 1/√T, so 30–45 DTE cuts gamma drag
#                   significantly more than theta. Sacrifices speed for survivability.
#                   McMillan Ch.3: "Reduce basis, but do not fight the tape with short
#                   gamma when structure is broken."
#                   Natenberg Ch.7: "Short gamma when IV < realized vol is structurally
#                   negative expectancy — extend duration to reduce daily drag."
_ROLL_MODE_NORMAL          = "NORMAL"
_ROLL_MODE_PRE_ITM         = "PRE_ITM"          # 0.55 < delta ≤ 0.70
_ROLL_MODE_EMERGENCY       = "EMERGENCY"         # delta > 0.70
_ROLL_MODE_WEEKLY          = "WEEKLY"            # fragile: BROKEN or WEAKENING + earnings ≤45d
_ROLL_MODE_BROKEN_RECOVERY = "BROKEN_RECOVERY"   # BROKEN + gamma dominant + IV < HV
_ROLL_MODE_INCOME_SAME     = "INCOME_SAME"       # SIDEWAYS_INCOME: prefer same-strike credit rolls
_ROLL_MODE_BASIS_REDUCTION = "BASIS_REDUCTION"   # Approaching hard stop: roll DOWN to tighter strike
_ROLL_MODE_RECOVERY_PREMIUM = "RECOVERY_PREMIUM"  # Doctrine_State=RECOVERY_PREMIUM: max premium cycling for basis reduction

# ── Roll trigger classification ──────────────────────────────────────────────
# WHY the engine is rolling — passed from doctrine via Winning_Gate column.
# This gives the scorer explicit context for what matters most in candidate
# selection. Without this, the scorer infers mode from delta/drift/equity
# integrity, which misses doctrine-level intent.
#
# Each trigger adjusts the 5-component scoring weights:
#   delta(25%), yield(25%), dte(20%), liquidity(20%), iv(10%)  ← defaults
# and may apply trigger-specific bonuses/penalties.

ROLL_TRIGGER_INCOME_GATE    = "INCOME_GATE"       # 21-DTE income capture
ROLL_TRIGGER_PREMIUM_CAPTURE = "PREMIUM_CAPTURE"  # 50%+ premium earned → cycle
ROLL_TRIGGER_ASSIGNMENT_DEFENSE = "ASSIGNMENT_DEFENSE"  # ITM/ATM delta → rescue
ROLL_TRIGGER_HARD_STOP      = "HARD_STOP"         # approaching/breached hard stop
ROLL_TRIGGER_EARNINGS        = "EARNINGS"          # earnings approaching → reduce risk
ROLL_TRIGGER_GAMMA_DANGER    = "GAMMA_DANGER"      # gamma dominance
ROLL_TRIGGER_DIVIDEND        = "DIVIDEND"          # dividend assignment risk
ROLL_TRIGGER_RECOVERY        = "RECOVERY"          # moderate recovery roll-down
ROLL_TRIGGER_STRUCTURAL      = "STRUCTURAL"        # structural fragility
ROLL_TRIGGER_DISCRETIONARY   = "DISCRETIONARY"     # no special context — generic

# Map doctrine Winning_Gate names → roll trigger categories
_GATE_TO_TRIGGER = {
    # Income cycle gates
    "income_gate_21dte": ROLL_TRIGGER_INCOME_GATE,
    "income_gate_dte": ROLL_TRIGGER_INCOME_GATE,
    "cc_income_21_dte": ROLL_TRIGGER_INCOME_GATE,
    "short_expiration": ROLL_TRIGGER_INCOME_GATE,
    # Premium capture
    "premium_capture_50": ROLL_TRIGGER_PREMIUM_CAPTURE,
    "premium_capture": ROLL_TRIGGER_PREMIUM_CAPTURE,
    "natural_cycle_roll": ROLL_TRIGGER_PREMIUM_CAPTURE,
    # Assignment defense
    "itm_late_lifecycle_roll": ROLL_TRIGGER_ASSIGNMENT_DEFENSE,
    "atm_late_lifecycle_roll": ROLL_TRIGGER_ASSIGNMENT_DEFENSE,
    "itm_assignment_defense": ROLL_TRIGGER_ASSIGNMENT_DEFENSE,
    "deep_itm_rescue": ROLL_TRIGGER_ASSIGNMENT_DEFENSE,
    "assignment_defense": ROLL_TRIGGER_ASSIGNMENT_DEFENSE,
    # Hard stop
    "approaching_hard_stop_roll": ROLL_TRIGGER_HARD_STOP,
    "hard_stop_approaching": ROLL_TRIGGER_HARD_STOP,
    # Earnings
    "earnings_approaching": ROLL_TRIGGER_EARNINGS,
    "earnings_approaching_roll": ROLL_TRIGGER_EARNINGS,
    "pre_earnings_roll": ROLL_TRIGGER_EARNINGS,
    # Gamma
    "gamma_danger_zone": ROLL_TRIGGER_GAMMA_DANGER,
    "gamma_dominant_roll": ROLL_TRIGGER_GAMMA_DANGER,
    # Dividend
    "dividend_assignment_risk": ROLL_TRIGGER_DIVIDEND,
    "dividend_risk_roll": ROLL_TRIGGER_DIVIDEND,
    # Recovery
    "moderate_recovery_roll_down": ROLL_TRIGGER_RECOVERY,
    "basis_reduction_roll": ROLL_TRIGGER_RECOVERY,
    "strike_below_basis": ROLL_TRIGGER_RECOVERY,
    "strike_below_basis_mild": ROLL_TRIGGER_RECOVERY,
    "assignment_at_loss": ROLL_TRIGGER_RECOVERY,
    "expiration_itm": ROLL_TRIGGER_RECOVERY,
    "expiration_otm": ROLL_TRIGGER_RECOVERY,
    # Structural
    "structural_fragility_roll": ROLL_TRIGGER_STRUCTURAL,
    "equity_integrity_roll": ROLL_TRIGGER_STRUCTURAL,
}

# Per-trigger scoring weight adjustments (delta, yield, dte, liquidity, iv)
# Values are multipliers on the default weights (1.0 = no change).
_TRIGGER_WEIGHT_ADJUSTMENTS = {
    ROLL_TRIGGER_INCOME_GATE: {
        # Income cycle: yield matters most, delta secondary
        "yield_w": 1.4, "delta_w": 0.8, "dte_w": 1.0, "liq_w": 1.0, "iv_w": 0.8,
    },
    ROLL_TRIGGER_PREMIUM_CAPTURE: {
        # Premium captured: yield is the goal, accept wider delta
        "yield_w": 1.5, "delta_w": 0.7, "dte_w": 1.0, "liq_w": 1.0, "iv_w": 0.8,
    },
    ROLL_TRIGGER_ASSIGNMENT_DEFENSE: {
        # Delta reduction is critical; yield secondary
        "yield_w": 0.6, "delta_w": 1.6, "dte_w": 1.0, "liq_w": 1.0, "iv_w": 0.8,
    },
    ROLL_TRIGGER_HARD_STOP: {
        # Survival: delta + DTE flexibility, yield less important
        "yield_w": 0.5, "delta_w": 1.4, "dte_w": 1.3, "liq_w": 1.0, "iv_w": 0.8,
    },
    ROLL_TRIGGER_EARNINGS: {
        # Get short exposure off the table: DTE matters, avoid earnings window
        "yield_w": 0.8, "delta_w": 1.0, "dte_w": 1.5, "liq_w": 1.0, "iv_w": 0.7,
    },
    ROLL_TRIGGER_GAMMA_DANGER: {
        # Extend DTE to reduce gamma drag; delta secondary
        "yield_w": 0.7, "delta_w": 0.8, "dte_w": 1.6, "liq_w": 1.0, "iv_w": 0.9,
    },
    ROLL_TRIGGER_DIVIDEND: {
        # Avoid assignment: DTE beyond ex-div, delta secondary
        "yield_w": 0.8, "delta_w": 1.2, "dte_w": 1.2, "liq_w": 1.0, "iv_w": 0.8,
    },
    ROLL_TRIGGER_RECOVERY: {
        # Basis reduction: yield paramount, accept tighter strikes
        "yield_w": 1.5, "delta_w": 0.7, "dte_w": 0.8, "liq_w": 1.0, "iv_w": 1.0,
    },
    ROLL_TRIGGER_STRUCTURAL: {
        # Structural: DTE flexibility, cautious on everything else
        "yield_w": 0.7, "delta_w": 1.0, "dte_w": 1.5, "liq_w": 1.0, "iv_w": 0.8,
    },
    ROLL_TRIGGER_DISCRETIONARY: {
        # Default: balanced
        "yield_w": 1.0, "delta_w": 1.0, "dte_w": 1.0, "liq_w": 1.0, "iv_w": 1.0,
    },
}


def _classify_roll_trigger(winning_gate: str) -> str:
    """Map doctrine Winning_Gate to roll trigger category."""
    if not winning_gate:
        return ROLL_TRIGGER_DISCRETIONARY
    gate_lower = winning_gate.lower().strip()
    # Exact match first
    if gate_lower in _GATE_TO_TRIGGER:
        return _GATE_TO_TRIGGER[gate_lower]
    # Substring match fallback — gate names may have suffixes/prefixes
    for pattern, trigger in _GATE_TO_TRIGGER.items():
        if pattern in gate_lower or gate_lower in pattern:
            return trigger
    return ROLL_TRIGGER_DISCRETIONARY


def _get_trigger_weights(roll_trigger: str) -> dict:
    """Return scoring weight adjustments for the given roll trigger."""
    return _TRIGGER_WEIGHT_ADJUSTMENTS.get(
        roll_trigger, _TRIGGER_WEIGHT_ADJUSTMENTS[ROLL_TRIGGER_DISCRETIONARY]
    )

# ── Strategy roll configuration ───────────────────────────────────────────────

# (min_dte, max_dte, target_dte) for roll expiry selection
# NORMAL mode — standard income cycle
_ROLL_DTE_WINDOWS: Dict[str, Tuple[int, int, int]] = {
    "LONG_CALL":     (45, 180, 90),   # Roll out enough to buy time; not so far it's cheap
    "LONG_PUT":      (45, 180, 90),
    "BUY_WRITE":     (21, 60,  45),   # Next CC cycle — one to two months out
    "COVERED_CALL":  (21, 60,  45),
    "SHORT_PUT":     (21, 60,  45),   # Roll down-and-out: lower strike, further DTE
    "CSP":           (21, 60,  45),
    "LEAP":          (270, 730, 365), # Deeper LEAP roll
}

# PRE_ITM mode — same DTE cycle but bias toward above-basis credit strikes
# DTE window unchanged; filtering and scoring differ (see _select_roll_candidates)
_ROLL_DTE_WINDOWS_PRE_ITM: Dict[str, Tuple[int, int, int]] = {
    "BUY_WRITE":     (21, 75,  45),   # Slightly wider — find a strike above basis
    "COVERED_CALL":  (21, 75,  45),
    "SHORT_PUT":     (21, 60,  45),
    "CSP":           (21, 60,  45),
}

# EMERGENCY mode — extended DTE to find any viable above-basis strike
# Near-dated is pure debit; must go out 45–150 DTE to find credit or break-even
_ROLL_DTE_WINDOWS_EMERGENCY: Dict[str, Tuple[int, int, int]] = {
    "BUY_WRITE":     (45, 150, 90),   # Extended: skip near-dated pure-debit territory
    "COVERED_CALL":  (45, 150, 90),
    "SHORT_PUT":     (45, 120, 75),
    "CSP":           (45, 120, 75),
}

# WEEKLY mode — short-cycle rolls for fragile positions with structural deterioration
# Triggers: Equity_Integrity=BROKEN (unconditional) OR WEAKENING + earnings ≤45d.
# Shorter commitment = maximum flexibility to reassess / exit / pivot.
# McMillan Ch.3: do not over-commit when underlying is structurally impaired.
_ROLL_DTE_WINDOWS_WEEKLY: Dict[str, Tuple[int, int, int]] = {
    "BUY_WRITE":     (7, 21,  14),   # One-to-two week cycles only
    "COVERED_CALL":  (7, 21,  14),
    "SHORT_PUT":     (7, 21,  14),
    "CSP":           (7, 21,  14),
}

# Target delta ranges for roll candidates (call delta is positive)
_ROLL_DELTA_TARGETS: Dict[str, Tuple[float, float]] = {
    "LONG_CALL":     (0.35, 0.65),   # Stay near ATM — roll to regain delta efficiency
    "LONG_PUT":      (-0.65, -0.35),
    "BUY_WRITE":     (0.20, 0.40),   # OTM call for income
    "COVERED_CALL":  (0.20, 0.40),
    "SHORT_PUT":     (-0.35, -0.15), # Lower strike = less assignment risk
    "CSP":           (-0.35, -0.15),
    "LEAP":          (0.60, 0.85),   # Roll to ITM/deep LEAP to preserve intrinsic
}

# PRE_ITM delta targets — tighter OTM range, ensure new strike has room
_ROLL_DELTA_TARGETS_PRE_ITM: Dict[str, Tuple[float, float]] = {
    "BUY_WRITE":     (0.20, 0.40),   # Same — still OTM but must be above basis
    "COVERED_CALL":  (0.20, 0.40),
}

# EMERGENCY delta targets — accept slightly higher delta (less OTM) to find viable strikes
# With stock already deep ITM on current strike, new strikes above basis will be moderately ITM
_ROLL_DELTA_TARGETS_EMERGENCY: Dict[str, Tuple[float, float]] = {
    "BUY_WRITE":     (0.20, 0.55),   # Wider — accept up to delta 0.55 to find above-basis strikes
    "COVERED_CALL":  (0.20, 0.55),
    "SHORT_PUT":     (-0.45, -0.15),
    "CSP":           (-0.45, -0.15),
}

# WEEKLY mode delta targets — standard OTM range (don't tighten, just shorten DTE)
# Keep OTM positioning for income; the weekly is about cycle length, not strike placement.
_ROLL_DELTA_TARGETS_WEEKLY: Dict[str, Tuple[float, float]] = {
    "BUY_WRITE":     (0.20, 0.40),
    "COVERED_CALL":  (0.20, 0.40),
    "SHORT_PUT":     (-0.35, -0.15),
    "CSP":           (-0.35, -0.15),
}

# BROKEN_RECOVERY mode — 30–45 DTE, near-ATM delta.
# Gamma ∝ 1/√T: moving from 14 DTE to 35 DTE cuts gamma drag by ~√(35/14) ≈ 1.6×
# while theta drops by only ~30%. Net: better carry per unit of gamma risk.
# Near-ATM (delta 0.25–0.40) collects more premium to offset negative IV/HV edge.
# Natenberg Ch.7: "When IV < realized, maximize theta/gamma ratio by extending DTE."
_ROLL_DTE_WINDOWS_BROKEN_RECOVERY: Dict[str, Tuple[int, int, int]] = {
    "BUY_WRITE":     (28, 60, 35),   # 4–8 weeks: enough to outrun gamma, not so far it's cheap
    "COVERED_CALL":  (28, 60, 35),
    "SHORT_PUT":     (28, 60, 35),
    "CSP":           (28, 60, 35),
}

_ROLL_DELTA_TARGETS_BROKEN_RECOVERY: Dict[str, Tuple[float, float]] = {
    "BUY_WRITE":     (0.25, 0.40),   # Near-ATM: higher premium to offset IV/HV disadvantage
    "COVERED_CALL":  (0.25, 0.40),
    "SHORT_PUT":     (-0.40, -0.20),
    "CSP":           (-0.40, -0.20),
}

# INCOME_SAME mode — SIDEWAYS_INCOME regime: prefer same-strike credit rolls.
# McMillan Ch.3: in range-bound behaviour, roll OUT in time, not UP in strike.
# Accept delta up to 0.55 so same-strike ATM/near-ATM candidates are valid.
# Standard 30-45 DTE: next monthly cycle, same income tempo.
_ROLL_DTE_WINDOWS_INCOME_SAME: Dict[str, Tuple[int, int, int]] = {
    "BUY_WRITE":     (21, 60,  45),
    "COVERED_CALL":  (21, 60,  45),
    "SHORT_PUT":     (21, 60,  45),
    "CSP":           (21, 60,  45),
}

_ROLL_DELTA_TARGETS_INCOME_SAME: Dict[str, Tuple[float, float]] = {
    "BUY_WRITE":     (0.20, 0.55),   # Accept same-strike near-ATM → credit roll
    "COVERED_CALL":  (0.20, 0.55),
    "SHORT_PUT":     (-0.55, -0.15), # Accept higher-delta (nearer) put strikes
    "CSP":           (-0.55, -0.15),
}

# BASIS_REDUCTION mode — approaching hard stop: roll DOWN to tighter strike.
# Allows same-expiry candidates (no DTE > current_dte constraint).
# Stock has drifted significantly — far-OTM call is dead weight, need to roll
# to a strike that generates meaningful premium for basis recovery.
# McMillan Ch.3: "Roll down to recycle premium when OTM call is earning nothing."
_ROLL_DTE_WINDOWS_BASIS_REDUCTION: Dict[str, Tuple[int, int, int]] = {
    "BUY_WRITE":     (21, 90,  45),   # Accept same-expiry or one cycle out
    "COVERED_CALL":  (21, 90,  45),
    "SHORT_PUT":     (21, 60,  45),
    "CSP":           (21, 60,  45),
}

_ROLL_DELTA_TARGETS_BASIS_REDUCTION: Dict[str, Tuple[float, float]] = {
    "BUY_WRITE":     (0.25, 0.45),   # Tighter strike: more premium, more extrinsic
    "COVERED_CALL":  (0.25, 0.45),
    "SHORT_PUT":     (-0.45, -0.20),
    "CSP":           (-0.45, -0.20),
}

# RECOVERY_PREMIUM mode — Doctrine_State=RECOVERY_PREMIUM: damaged buy-write
# optimizing multi-cycle basis reduction. Short cycles (14-45 DTE) maximize
# premium cycling frequency. No above-basis filter — stock is far below basis,
# all reasonable OTM strikes are valid.
# Jabbour Ch.4: "In repair mode, premium collection frequency is the recovery
# driver — shorter cycles compound faster and allow strike re-optimization."
_ROLL_DTE_WINDOWS_RECOVERY_PREMIUM: Dict[str, Tuple[int, int, int]] = {
    "BUY_WRITE":     (14, 45,  30),   # 2-6 week cycles for maximum premium frequency
    "COVERED_CALL":  (14, 45,  30),
    "SHORT_PUT":     (14, 45,  30),
    "CSP":           (14, 45,  30),
}

_ROLL_DELTA_TARGETS_RECOVERY_PREMIUM: Dict[str, Tuple[float, float]] = {
    "BUY_WRITE":     (0.25, 0.45),   # OTM: maximize premium without inviting assignment
    "COVERED_CALL":  (0.25, 0.45),
    "SHORT_PUT":     (-0.45, -0.25),
    "CSP":           (-0.45, -0.25),
}

# How many candidates to return (heuristic shortlist).
# MC rerank (in mc_management.py) reduces these to the best 3 by EV.
_TOP_N = 5

# Schwab call throttle
_CHAIN_DELAY_SEC = 0.5


def find_roll_candidates(
    df: pd.DataFrame,
    schwab_client,
    session_chain_cache: Optional[Dict[str, dict]] = None,
    action_mask: Optional[pd.Series] = None,
) -> pd.DataFrame:
    """
    For every row where Action=ROLL (or covered by action_mask), fetch the chain
    and attach up to 5 heuristic-shortlisted roll candidates as
    Roll_Candidate_1 through Roll_Candidate_5 JSON columns.
    MC rerank (run_management_mc) later reduces these to the best 3 by EV.

    action_mask: optional boolean Series override — use when pre-staging candidates
        for HOLD rows with active blocking conditions (dead_cat_bounce, iv_depressed)
        so they are ready the moment the condition resolves.

    session_chain_cache: shared dict {ticker: chain_response} passed from
        LiveGreeksProvider to avoid duplicate chain calls in the same run.

    Returns df with added Roll_Candidate_* columns.
    """
    df = df.copy()

    roll_mask = action_mask if action_mask is not None else (df["Action"] == "ROLL")
    if not roll_mask.any():
        return df

    # Ensure output columns exist
    for i in range(1, _TOP_N + 1):
        col = f"Roll_Candidate_{i}"
        if col not in df.columns:
            df[col] = None
    if "Roll_Split_Suggestion" not in df.columns:
        df["Roll_Split_Suggestion"] = None

    session_cache = session_chain_cache or {}
    option_roll_rows = df[roll_mask & (df["AssetType"] == "OPTION")]

    # BUY_WRITE stock legs flagged for candidates (HOLD+HIGH, EXIT) may not have
    # a matching option leg in the mask. Resolve by finding the paired option leg
    # from the same TradeID and adding it to the processing set.
    _stock_flagged = df[roll_mask & (df["AssetType"] == "STOCK")]
    _extra_option_idxs = []
    if not _stock_flagged.empty and "TradeID" in df.columns:
        for _sidx in _stock_flagged.index:
            _tid = df.loc[_sidx, "TradeID"]
            _paired_opts = df[
                (df["TradeID"] == _tid) & (df["AssetType"] == "OPTION")
            ]
            for _oidx in _paired_opts.index:
                if _oidx not in option_roll_rows.index:
                    _extra_option_idxs.append(_oidx)
    if _extra_option_idxs:
        # Deduplicate by index (drop_duplicates() fails on DataFrames with list columns)
        _all_idxs = list(option_roll_rows.index) + _extra_option_idxs
        _unique_idxs = list(dict.fromkeys(_all_idxs))  # preserve order, remove dupes
        option_roll_rows = df.loc[_unique_idxs]

    processed_tickers = set()

    for idx in option_roll_rows.index:
        row      = df.loc[idx]
        ticker   = str(row.get("Underlying_Ticker", "") or "")
        strategy = str(row.get("Strategy", "") or "")
        cp       = str(row.get("Call/Put", "") or "").upper()
        strike        = float(row.get("Strike", 0) or 0)
        dte           = float(row.get("DTE", 0) or 0)
        ul_price      = float(row.get("UL Last", 0) or 0)
        current_iv    = float(row.get("IV_Now") if pd.notna(row.get("IV_Now")) else
                              row.get("IV_30D", 0) or 0)
        net_cost_basis = float(row.get("Net_Cost_Basis_Per_Share", 0) or 0)
        cum_premium   = float(row.get("Cumulative_Premium_Collected", 0) or 0)
        hv_20d        = float(row.get("HV_20D", 0) or 0)

        # Net_Cost_Basis_Per_Share is set by BuyWriteLedger on the STOCK leg only.
        # The OPTION leg row has 0/NaN → breakeven_after_roll computes as None → shows "?".
        # Two-pass fallback mirrors BuyWriteLedger.enrich():
        #   1. Same-TradeID stock leg (normal BUY_WRITE pairing)
        #   2. Any STOCK leg for same ticker (handles COVERED_CALL option-only positions)
        if net_cost_basis == 0 and "TradeID" in df.columns:
            _trade_id = row.get("TradeID")
            _stock_legs = df[(df["TradeID"] == _trade_id) & (df["AssetType"] == "STOCK")]
            if _stock_legs.empty and ticker and "Underlying_Ticker" in df.columns:
                # Pass 2: any STOCK row for same ticker with a valid basis
                _ticker_stocks = df[
                    (df["Underlying_Ticker"] == ticker) &
                    (df["AssetType"] == "STOCK") &
                    df["Net_Cost_Basis_Per_Share"].notna()
                ]
                if not _ticker_stocks.empty:
                    _stock_legs = _ticker_stocks
            if not _stock_legs.empty:
                _ncb = float(_stock_legs.iloc[0].get("Net_Cost_Basis_Per_Share", 0) or 0)
                if _ncb > 0:
                    net_cost_basis = _ncb
            if net_cost_basis == 0 and not _stock_legs.empty:
                # Also lift cum_premium from stock leg if missing on option row
                _cp = float(_stock_legs.iloc[0].get("Cumulative_Premium_Collected", 0) or 0)
                if _cp > 0:
                    cum_premium = _cp

        if not ticker or ul_price == 0:
            continue

        # Determine call/put from strategy if not explicit
        if not cp or cp not in ("C", "P", "CALL", "PUT"):
            cp = "C" if "CALL" in strategy or strategy in ("BUY_WRITE", "COVERED_CALL") else "P"
        cp_normalized = cp[0]  # "C" or "P"

        # Get roll config for this strategy
        strategy_key = _strategy_key(strategy, dte)

        # ── Roll mode detection ────────────────────────────────────────────────
        # Recovery Premium Mode override: when doctrine has determined the position
        # is in recovery premium mode, skip delta-based mode selection entirely.
        # The normal delta-based modes (EMERGENCY, PRE_ITM) apply filters that
        # are wrong for recovery: EMERGENCY hard-filters above-basis strikes,
        # which is impossible when stock is far below basis ($6 stock, $17 basis).
        # Recovery premium optimizes for premium cycling frequency, not strike
        # placement relative to an unreachable basis.
        # Jabbour Ch.4: "Recovery is about premium collection frequency, not
        # strike placement relative to a cost basis the market may never revisit."
        current_delta = abs(float(row.get("Delta", row.get("Short_Call_Delta", 0)) or 0))
        _doctrine_state_rc = str(row.get("Doctrine_State", "") or "").strip().upper()
        if _doctrine_state_rc == "RECOVERY_PREMIUM":
            roll_mode = _ROLL_MODE_RECOVERY_PREMIUM
            logger.info(
                f"[RollEngine] {ticker} → RECOVERY_PREMIUM mode: "
                f"Doctrine_State=RECOVERY_PREMIUM, stock=${ul_price:.2f}, "
                f"basis=${net_cost_basis:.2f}, delta={current_delta:.2f}"
            )
        elif current_delta > 0.70:
            roll_mode = _ROLL_MODE_EMERGENCY
        elif current_delta > 0.55:
            roll_mode = _ROLL_MODE_PRE_ITM
        else:
            roll_mode = _ROLL_MODE_NORMAL

        # ── WEEKLY mode override (fragility gate) ─────────────────────────────
        # Fires when the underlying stock is structurally impaired AND there is
        # earnings risk — either within the standard 45-day window OR unknown.
        # In this condition, committing to a 21–60 DTE roll is imprudent:
        # the position may need to be unwound or restructured before the new
        # expiry if deterioration continues.
        # Overrides NORMAL and PRE_ITM — do NOT override EMERGENCY (which is
        # in rescue mode with extended-DTE above-basis filtering that must be
        # preserved). At high delta (PRE_ITM), structural fragility still means
        # shorter commitment — WEEKLY is the correct override even when delta
        # is elevated (0.55–0.70), because DTE is not the crisis, structure is.
        # Guard: WEEKLY mode only makes sense for positions with DTE within
        # weekly-cycle range (≤90 DTE). LEAPS positions (DTE 200+) should not
        # compress to 7–21 DTE cycles regardless of structural fragility —
        # the investment thesis is measured in years, not weeks.
        _weekly_eligible = dte <= 90
        if roll_mode in (_ROLL_MODE_NORMAL, _ROLL_MODE_PRE_ITM) and _weekly_eligible:
            _ei_state = str(row.get("Equity_Integrity_State", "") or "").strip().upper()
            if not _ei_state or _ei_state == "HEALTHY":
                # Equity_Integrity_State is only populated on STOCK legs; OPTION legs
                # carry HEALTHY by default. Two-pass lookup:
                # 1. Same-TradeID stock leg (paired BUY_WRITE / COVERED_CALL)
                # 2. Any STOCK row for same ticker (isolated short call over same underlying)
                _ei_from_stock = ""
                if "TradeID" in df.columns:
                    _trade_id_ei = row.get("TradeID")
                    _stock_ei = df[(df["TradeID"] == _trade_id_ei) & (df["AssetType"] == "STOCK")]
                    if not _stock_ei.empty:
                        _ei_from_stock = str(_stock_ei.iloc[0].get("Equity_Integrity_State", "") or "").strip().upper()
                if not _ei_from_stock and ticker and "Underlying_Ticker" in df.columns:
                    # No same-trade stock leg — check any stock row for this ticker
                    # (handles isolated short calls where the stock leg has a different TradeID)
                    _ticker_stock = df[
                        (df["Underlying_Ticker"] == ticker) & (df["AssetType"] == "STOCK")
                    ]
                    if not _ticker_stock.empty:
                        _ei_from_stock = str(_ticker_stock.iloc[0].get("Equity_Integrity_State", "") or "").strip().upper()
                if not _ei_from_stock:
                    # Last resort: query DuckDB for the most recent Equity_Integrity_State
                    # on any stock leg for this ticker. This handles the case where the
                    # stock leg was present in a prior run but the broker export now shows
                    # only the option leg (position restructured / partial export).
                    try:
                        import duckdb as _ddb
                        from pathlib import Path as _Path
                        _db_path = _Path(__file__).parents[4] / "data" / "pipeline.duckdb"
                        _con = _ddb.connect(str(_db_path), read_only=True)
                        _hist_ei = _con.execute("""
                            SELECT Equity_Integrity_State
                            FROM management_recommendations
                            WHERE Underlying_Ticker = ?
                              AND AssetType = 'STOCK'
                              AND Equity_Integrity_State IS NOT NULL
                              AND Equity_Integrity_State NOT IN ('', 'HEALTHY', 'N/A')
                            ORDER BY Snapshot_TS DESC
                            LIMIT 1
                        """, [ticker]).fetchdf()
                        _con.close()
                        if not _hist_ei.empty:
                            _ei_from_stock = str(_hist_ei.iloc[0]["Equity_Integrity_State"] or "").strip().upper()
                    except Exception:
                        pass
                if _ei_from_stock in ("BROKEN", "WEAKENING"):
                    _ei_state = _ei_from_stock
                    logger.debug(
                        f"[RollEngine] {ticker} EI inherited from stock leg: {_ei_state}"
                    )
            if _ei_state == "BROKEN":
                # Structural failure confirmed. Default: WEEKLY (shortest commitment).
                # Exception: BROKEN_RECOVERY overrides WEEKLY when ALL three hold:
                #   1. Gamma drag > threshold × theta (1.8× stable, 2.4× spiking)
                #   2. IV < HV (IV/HV < 0.90)  — selling vol below realized: negative edge
                #   3. Strike within 30% of spot — gamma is physically meaningful
                # In this regime, weekly DTE makes the gamma problem WORSE (gamma ∝ 1/√T).
                # Moving to 30–45 DTE cuts daily gamma drag without proportionally
                # cutting theta. Sacrifices recovery speed for structural survivability.
                # Natenberg Ch.7: "Short gamma when IV < realized is negative expectancy."
                # ChatGPT Mode B: broken structure + HV > IV → move to 30–45 DTE.
                # Spike guard: if Gamma_ROC_3D > 0.25 (gamma accelerating = intraday spike),
                # raise threshold to 2.4× so BROKEN_RECOVERY doesn't flip on a single
                # day's gamma jump. Gamma spikes intraday; structural drag is persistent.
                import math as _math_rc
                _rc_theta   = abs(float(row.get("Theta", 0) or 0))
                _rc_gamma   = abs(float(row.get("Gamma", 0) or 0))
                from core.shared.finance_utils import normalize_iv as _niv_rc
                _rc_hv      = _niv_rc(float(row.get("HV_20D", 0.20) or 0.20)) or 0.20
                _rc_iv      = _niv_rc(float(row.get("IV", 0) or row.get("IV_30D", 0) or 0)) or 0.0
                _rc_spot    = float(row.get("Last", 0) or row.get("UL Last", 0) or 0)
                _rc_strike  = float(row.get("Strike", 0) or 0)
                _rc_sigma   = _rc_spot * (_rc_hv / _math_rc.sqrt(252)) if _rc_spot > 0 else 0
                _rc_gdrag   = 0.5 * _rc_gamma * (_rc_sigma ** 2)
                _rc_otm_pct = (abs(_rc_strike - _rc_spot) / _rc_spot
                               if _rc_strike > 0 and _rc_spot > 0 else 1.0)

                # Spike guard (ChatGPT: smooth gamma/theta to prevent daily regime flipping).
                # If Gamma_ROC_3D > 0.25, gamma is accelerating sharply — likely an
                # intraday or single-session spike, not a persistent structural regime.
                # Raise the threshold to 2.4× (vs normal 1.8×) so BROKEN_RECOVERY only
                # fires when the drag is confirmed structurally elevated, not just today's peak.
                # Gamma_ROC_3D ≤ 0.25 → stable/declining gamma → use normal 1.8× threshold.
                _rc_gamma_roc3 = float(row.get("Gamma_ROC_3D") or 0)
                _rc_gdrag_threshold = (
                    2.4 if _rc_gamma_roc3 > 0.25  # spike guard: require stronger confirmation
                    else 1.8                        # structurally elevated: normal threshold
                )

                _rc_gamma_dominant = (
                    _rc_theta > 0
                    and _rc_gdrag > _rc_theta * _rc_gdrag_threshold
                    and _rc_otm_pct <= 0.30
                )
                _rc_iv_depressed = (
                    _rc_iv > 0 and _rc_hv > 0
                    and (_rc_iv / _rc_hv) < 0.90      # IV at least 10% below HV
                )
                if _rc_gamma_dominant and _rc_iv_depressed:
                    roll_mode = _ROLL_MODE_BROKEN_RECOVERY
                    logger.info(
                        f"[RollEngine] {ticker} → BROKEN_RECOVERY mode: "
                        f"BROKEN + gamma_drag={_rc_gdrag:.4f} ({_rc_gdrag/_rc_theta:.1f}×theta, "
                        f"threshold={_rc_gdrag_threshold}×, Gamma_ROC_3D={_rc_gamma_roc3:.2f}) "
                        f"+ IV/HV={_rc_iv/_rc_hv:.2f} (depressed)"
                    )
                else:
                    roll_mode = _ROLL_MODE_WEEKLY
                    logger.info(
                        f"[RollEngine] {ticker} → WEEKLY mode: Equity_Integrity=BROKEN "
                        f"(gamma_dominant={_rc_gamma_dominant} [threshold={_rc_gdrag_threshold}×], "
                        f"iv_depressed={_rc_iv_depressed}, Gamma_ROC_3D={_rc_gamma_roc3:.2f})"
                    )
            elif _ei_state == "WEAKENING":
                # Deteriorating but not yet broken — only go weekly if earnings
                # compound the risk (≤45d) or earnings date is unknown.
                _earnings_date_raw = row.get("Earnings_Date")
                _earnings_risk = True  # default: assume risk if date unknown
                if _earnings_date_raw not in (None, "", "nan", "N/A") and not (
                    isinstance(_earnings_date_raw, float) and pd.isna(_earnings_date_raw)
                ):
                    try:
                        _ed = pd.to_datetime(str(_earnings_date_raw), errors="coerce")
                        if pd.notna(_ed):
                            _days_to_earnings = (_ed - pd.Timestamp.now()).days
                            _earnings_risk = _days_to_earnings <= 45
                    except Exception:
                        _earnings_risk = True  # parse failure = treat as unknown = risk
                if _earnings_risk:
                    roll_mode = _ROLL_MODE_WEEKLY
                    logger.info(
                        f"[RollEngine] {ticker} → WEEKLY mode: "
                        f"Equity_Integrity=WEAKENING, earnings_risk={_earnings_risk}"
                    )

        # ── Basis-reduction mode: approaching hard stop → roll DOWN ─────────
        # When drift_from_net is between -15% and -20% (approaching hard stop),
        # the current OTM call is too far out of the money to generate meaningful
        # premium. Roll DOWN to a tighter strike — same expiry allowed — to
        # maximize premium collection for basis recovery.
        # McMillan Ch.3: "Roll down to recycle premium when OTM call is dead weight."
        # Overrides NORMAL and PRE_ITM. Emergency/weekly have structural priority.
        if roll_mode in (_ROLL_MODE_NORMAL, _ROLL_MODE_PRE_ITM):
            # Use resolved net_cost_basis (already lifted from stock leg at line 304+)
            if net_cost_basis > 0 and ul_price > 0:
                _drift_for_roll = (ul_price - net_cost_basis) / net_cost_basis
                if -0.20 <= _drift_for_roll <= -0.10:
                    roll_mode = _ROLL_MODE_BASIS_REDUCTION
                    logger.info(
                        f"[RollEngine] {ticker} → BASIS_REDUCTION mode: "
                        f"drift={_drift_for_roll:.1%} approaching hard stop — "
                        f"allow same-expiry roll-down for premium recycling"
                    )

        # ── Income-friendly regimes: prefer same-strike credit rolls ────────
        # Position_Regime is computed by compute_position_trajectory() (Cycle 2.85).
        # When the position is in a range-bound income regime, rolling UP in strike
        # (standard OTM delta targeting) produces debits that erode collected premium.
        # Instead, roll OUT in time at the same or near strike to collect credit.
        # McMillan Ch.3: "Income overlay profits from range-bound behaviour — don't
        # chase strikes when the stock isn't trending."
        # Applies to both SIDEWAYS_INCOME and MEAN_REVERSION — both are income-friendly.
        # Only overrides NORMAL or PRE_ITM — emergency/weekly modes have structural priority.
        if roll_mode in (_ROLL_MODE_NORMAL, _ROLL_MODE_PRE_ITM):
            _pos_regime = str(row.get("Position_Regime", "") or "").strip()
            if _pos_regime in ("SIDEWAYS_INCOME", "MEAN_REVERSION"):
                roll_mode = _ROLL_MODE_INCOME_SAME
                logger.info(
                    f"[RollEngine] {ticker} → INCOME_SAME mode: "
                    f"Position_Regime={_pos_regime} — prefer same-strike credit rolls"
                )

        # Select DTE window and delta range based on mode
        if roll_mode == _ROLL_MODE_EMERGENCY:
            dte_window  = _ROLL_DTE_WINDOWS_EMERGENCY.get(strategy_key,
                            _ROLL_DTE_WINDOWS.get(strategy_key, (45, 150, 90)))
            delta_range = _ROLL_DELTA_TARGETS_EMERGENCY.get(strategy_key,
                            _ROLL_DELTA_TARGETS.get(strategy_key, (0.25, 0.55)))
        elif roll_mode == _ROLL_MODE_PRE_ITM:
            dte_window  = _ROLL_DTE_WINDOWS_PRE_ITM.get(strategy_key,
                            _ROLL_DTE_WINDOWS.get(strategy_key, (21, 75, 45)))
            delta_range = _ROLL_DELTA_TARGETS_PRE_ITM.get(strategy_key,
                            _ROLL_DELTA_TARGETS.get(strategy_key, (0.20, 0.40)))
        elif roll_mode == _ROLL_MODE_BROKEN_RECOVERY:
            # BROKEN + gamma dominant + IV < HV: extend DTE to cut gamma drag.
            # Gamma ∝ 1/√T — doubling DTE from 14→35d cuts gamma by ~√2.5 ≈ 1.6×.
            # Near-ATM delta collects more premium to offset the IV/HV disadvantage.
            dte_window  = _ROLL_DTE_WINDOWS_BROKEN_RECOVERY.get(strategy_key,
                            _ROLL_DTE_WINDOWS.get(strategy_key, (28, 60, 35)))
            delta_range = _ROLL_DELTA_TARGETS_BROKEN_RECOVERY.get(strategy_key,
                            _ROLL_DELTA_TARGETS.get(strategy_key, (0.25, 0.40)))
        elif roll_mode == _ROLL_MODE_WEEKLY:
            dte_window  = _ROLL_DTE_WINDOWS_WEEKLY.get(strategy_key,
                            _ROLL_DTE_WINDOWS.get(strategy_key, (7, 21, 14)))
            delta_range = _ROLL_DELTA_TARGETS_WEEKLY.get(strategy_key,
                            _ROLL_DELTA_TARGETS.get(strategy_key, (0.20, 0.40)))
        elif roll_mode == _ROLL_MODE_INCOME_SAME:
            dte_window  = _ROLL_DTE_WINDOWS_INCOME_SAME.get(strategy_key,
                            _ROLL_DTE_WINDOWS.get(strategy_key, (21, 60, 45)))
            delta_range = _ROLL_DELTA_TARGETS_INCOME_SAME.get(strategy_key,
                            _ROLL_DELTA_TARGETS.get(strategy_key, (0.20, 0.55)))
        elif roll_mode == _ROLL_MODE_BASIS_REDUCTION:
            dte_window  = _ROLL_DTE_WINDOWS_BASIS_REDUCTION.get(strategy_key,
                            _ROLL_DTE_WINDOWS.get(strategy_key, (21, 90, 45)))
            delta_range = _ROLL_DELTA_TARGETS_BASIS_REDUCTION.get(strategy_key,
                            _ROLL_DELTA_TARGETS.get(strategy_key, (0.25, 0.45)))
        elif roll_mode == _ROLL_MODE_RECOVERY_PREMIUM:
            dte_window  = _ROLL_DTE_WINDOWS_RECOVERY_PREMIUM.get(strategy_key,
                            _ROLL_DTE_WINDOWS.get(strategy_key, (14, 45, 30)))
            delta_range = _ROLL_DELTA_TARGETS_RECOVERY_PREMIUM.get(strategy_key,
                            _ROLL_DELTA_TARGETS.get(strategy_key, (0.25, 0.45)))
        else:
            dte_window  = _ROLL_DTE_WINDOWS.get(strategy_key, (30, 120, 60))
            delta_range = _ROLL_DELTA_TARGETS.get(strategy_key, (0.25, 0.50))

        # ── Widen delta band when near breakeven (cross-cutting) ──────────
        # When the underlying is within ~5% of net cost, the position is
        # close to breakeven and ATM/ITM credit rolls become attractive.
        # Standard OTM delta bands (0.20–0.45) reject these higher-delta
        # strikes. Widen to 0.65 so the heuristic shortlist includes credit
        # roll candidates; MC rerank will sort them by EV against debit rolls.
        # Applies to any income mode (NORMAL, RECOVERY_PREMIUM, etc.) — not
        # just RECOVERY_PREMIUM, since near-breakeven positions in NORMAL mode
        # also benefit from seeing credit roll options.
        if net_cost_basis > 0 and ul_price > 0 and strategy_key in (
            "BUY_WRITE", "COVERED_CALL", "SHORT_PUT", "CSP"
        ):
            _basis_proximity = abs(ul_price - net_cost_basis) / net_cost_basis
            if _basis_proximity <= 0.05:
                _lo, _hi = delta_range
                if strategy_key in ("SHORT_PUT", "CSP"):
                    delta_range = (min(_lo, -0.65), _hi)  # widen magnitude
                else:
                    delta_range = (_lo, max(_hi, 0.65))
                logger.info(
                    f"[RollEngine] {ticker} near-breakeven ({roll_mode}): "
                    f"proximity={_basis_proximity:.1%}, widened delta band → {delta_range}"
                )

        # ── DTE window extension when current_dte is near max_dte ──────────
        # Problem: when current_dte is already close to max_dte, the "must roll OUT"
        # filter (actual_dte > current_dte) + max_dte ceiling leaves a window so
        # narrow that no available expiry falls within it.
        # Example: WEEKLY mode (7, 21, 14), current_dte=17 → only DTE 18–21 survive.
        # If the next available weekly expiry is at DTE 24, zero candidates pass.
        # Fix: extend max_dte when the remaining window is < 7 DTE. This preserves
        # the short-cycle INTENT while ensuring at least one cycle is reachable.
        # The target_dte stays the same — scoring still prefers the original target.
        _min_dte_w, _max_dte_w, _target_dte_w = dte_window
        _remaining_window = _max_dte_w - int(dte)
        if _remaining_window < 7 and dte > 0 and dte <= _max_dte_w * 3:
            # Only extend when current DTE is in the same order of magnitude as the
            # window — avoids nonsensical extensions (e.g. LEAPS DTE=689 in WEEKLY).
            # Use +35 days to bridge monthly-only chains (monthlies are ~30d apart).
            _extended_max = int(dte) + 35  # bridge at least one monthly cycle gap
            if _extended_max > _max_dte_w:
                dte_window = (_min_dte_w, _extended_max, _target_dte_w)
                logger.info(
                    f"[RollEngine] {ticker} DTE window extended: current_dte={dte:.0f} "
                    f"near max={_max_dte_w}, remaining window was {_remaining_window}d → "
                    f"extended to ({_min_dte_w}, {_extended_max}, {_target_dte_w})"
                )

        # Timing-aware adjustments to roll target parameters.
        # McMillan Ch.3: in a breakout UP on a BW/CC, roll further OTM and further
        # out in time — give the stock room to run and collect more premium.
        # In a breakdown, roll closer-in (smaller debit) and slightly lower delta.
        # Note: timing adjustments apply on top of roll_mode — they are additive.
        # EXCEPTION: WEEKLY mode skips timing adjustments — the short-cycle window
        # is a hard constraint (fragility). A BREAKOUT_UP signal must not extend it.
        if roll_mode != _ROLL_MODE_WEEKLY:
            timing = _get_roll_timing(row)
            dte_window, delta_range = _adjust_for_timing(
                timing, strategy_key, dte_window, delta_range, ul_price, strike
            )

        # ── Calendar context awareness ─────────────────────────────────────────
        # Wire in expiry_proximity_flag() from scan_engine.calendar_context:
        #
        # 1. PIN_RISK / GAMMA_CRITICAL (≤3 DTE) → WEEKLY override (if not EMERGENCY)
        #    Don't extend DTE into a position that's about to expire with pin risk.
        #    McMillan Ch.7: close or roll before Thursday if within 2% of strike.
        #
        # 2. PRE_HOLIDAY_EXPIRY (≤7 DTE into long weekend) → WEEKLY override + note
        #    Theta acceleration + non-trading gap = roll today, not Monday.
        #
        # 3. THETA_ACCELERATING (≤7 DTE, normal) → note only (no mode change)
        #    Informs candidate rationale: final-week decay is non-linear.
        #
        # 4. Pre-long-weekend with ≤21 DTE current position → theta collection note
        #    Rolling into a position that captures weekend theta is an advantage.
        #    Passarelli Ch.6: Friday rolls collect Sat+Sun theta at no extra risk.
        calendar_note = ""
        try:
            from scan_engine.calendar_context import (
                expiry_proximity_flag, get_calendar_context, is_pre_long_weekend,
            )
            _prox_flag, _prox_note = expiry_proximity_flag(
                dte=dte, strategy=strategy_key,
                ul_last=ul_price, strike=strike,
            )
            _ctx = get_calendar_context()

            if _prox_flag in ("PIN_RISK", "GAMMA_CRITICAL", "PRE_HOLIDAY_EXPIRY"):
                if roll_mode not in (_ROLL_MODE_EMERGENCY,):
                    _prev_mode = roll_mode
                    roll_mode = _ROLL_MODE_WEEKLY
                    dte_window  = _ROLL_DTE_WINDOWS_WEEKLY.get(strategy_key,
                                    _ROLL_DTE_WINDOWS.get(strategy_key, (7, 21, 14)))
                    delta_range = _ROLL_DELTA_TARGETS_WEEKLY.get(strategy_key,
                                    _ROLL_DELTA_TARGETS.get(strategy_key, (0.20, 0.40)))
                    logger.info(
                        f"[RollEngine] {ticker} → WEEKLY mode override: "
                        f"calendar={_prox_flag} (was {_prev_mode})"
                    )
                calendar_note = f" | 📅 {_prox_note}"

            elif _prox_flag == "THETA_ACCELERATING":
                calendar_note = f" | 📅 {_prox_note}"

            elif _ctx.is_pre_long_weekend and dte <= 21:
                # Current position has ≤21 DTE and today is the last session before
                # a long weekend — rolling now captures the extra theta gap.
                _gap = _ctx.theta_bleed_days
                calendar_note = (
                    f" | 📅 Pre-long-weekend roll advantage: "
                    f"+{_gap} extra theta days collected by rolling today "
                    f"(Passarelli Ch.6: Friday rolls collect weekend theta at no added risk)."
                )

            elif _ctx.is_friday and dte <= 21:
                # Normal Friday with near-expiry position — theta advantage note only.
                calendar_note = (
                    f" | 📅 Friday roll: captures Sat+Sun theta decay in new position "
                    f"(Passarelli Ch.6)."
                )

        except Exception as _cal_err:
            logger.debug(f"[RollEngine] calendar_context unavailable for {ticker}: {_cal_err}")

        # Fetch chain (reuse session cache to avoid duplicate calls)
        chain = _get_chain(ticker, schwab_client, session_cache)
        if not chain:
            logger.warning(f"[RollEngine] No chain for {ticker} — skipping roll candidates")
            continue

        if ticker not in processed_tickers:
            processed_tickers.add(ticker)
            time.sleep(_CHAIN_DELAY_SEC)

        # ── Earnings proximity (Phase 1b) ──────────────────────────────────────
        # Compute days_to_earnings for the scoring penalty in _score_candidate().
        # The penalty fires when earnings fall BEFORE this candidate's expiry DTE,
        # meaning the new position would be held through the IV event.
        # Source: "Earnings_Date" column from doctrine CSV (injected by DoctrineAuthority).
        _days_to_earnings: int | None = None
        _earn_raw = row.get("Earnings_Date")
        if _earn_raw not in (None, "", "nan", "N/A") and not (
            isinstance(_earn_raw, float) and pd.isna(_earn_raw)
        ):
            try:
                _ed = pd.to_datetime(str(_earn_raw), errors="coerce")
                if pd.notna(_ed):
                    _days_to_earnings = (_ed.normalize() - pd.Timestamp.now().normalize()).days
            except Exception:
                pass

        # Current option mid (for net-roll economics in scoring)
        _cur_bid = float(row.get("Bid", 0) or 0)
        _cur_ask = float(row.get("Ask", 0) or 0)
        _cur_option_mid = (
            (_cur_bid + _cur_ask) / 2
            if _cur_bid and _cur_ask
            else float(row.get("Last", 0) or 0)
        )

        # Dividend data (for assignment-bait penalty in scoring)
        # McMillan Ch.2: early exercise when extrinsic < dividend
        _days_to_div = float(row.get("Days_To_Dividend", 9999) or 9999)
        _div_amount = float(row.get("Dividend_Amount", 0) or 0)

        # PMCC LEAP strike constraint (short call must stay below LEAP strike)
        _leap_strike = 0.0
        if strategy.upper() == "PMCC":
            _leap_strike = float(row.get("LEAP_Call_Strike", 0) or 0)

        # Churn guard — consecutive debit rolls from position trajectory
        _consec_debit_rolls = int(
            float(row.get("Trajectory_Consecutive_Debit_Rolls", 0) or 0)
        )

        # Roll trigger classification — WHY doctrine is rolling
        # Winning_Gate carries the doctrine gate name that produced the ROLL action.
        # This gives the scorer explicit context for what matters most.
        _winning_gate = str(row.get("Winning_Gate", "") or "")
        _roll_trigger = _classify_roll_trigger(_winning_gate)
        logger.debug(
            f"[RollEngine] {ticker} roll_trigger={_roll_trigger} "
            f"(from Winning_Gate={_winning_gate!r})"
        )

        # Find candidates
        candidates = _select_roll_candidates(
            chain            = chain,
            cp               = cp_normalized,
            ul_price         = ul_price,
            current_strike   = strike,
            current_dte      = dte,
            current_iv       = current_iv,
            dte_window       = dte_window,
            delta_range      = delta_range,
            strategy_key     = strategy_key,
            net_cost_basis   = net_cost_basis,
            hv_20d           = hv_20d,
            roll_mode        = roll_mode,
            days_to_earnings = _days_to_earnings,
            current_option_mid = _cur_option_mid,
            leap_strike      = _leap_strike,
            days_to_dividend = _days_to_div,
            dividend_amount  = _div_amount,
            consecutive_debit_rolls = _consec_debit_rolls,
            roll_trigger     = _roll_trigger,
        )

        # ── No-viable-roll assessment (emergency mode only) ───────────────────
        # If we're in EMERGENCY mode and no candidates survive the above-basis
        # filter, surface an explicit "assignment preferable" verdict as
        # Roll_Candidate_1 so the dashboard and checklist can surface it.
        if roll_mode == _ROLL_MODE_EMERGENCY and not candidates:
            viability = _assess_roll_viability(
                ul_price=ul_price, net_cost_basis=net_cost_basis,
                current_strike=strike, current_delta=current_delta,
                strategy_key=strategy_key,
            )
            df.at[idx, "Roll_Candidate_1"] = json.dumps(viability)
            logger.info(
                f"[RollEngine] {ticker} EMERGENCY mode — no viable above-basis roll found. "
                f"Verdict: {viability.get('verdict')}"
            )
            continue

        # ── RECOVERY_PREMIUM no-viable-roll fallback ─────────────────────────
        # If RECOVERY_PREMIUM mode finds zero candidates (illiquid chain, IV too
        # low for viable premium), surface an explicit verdict so the dashboard
        # shows actionable guidance instead of "could not be parsed".
        if roll_mode == _ROLL_MODE_RECOVERY_PREMIUM and not candidates:
            _rp_gap = net_cost_basis - ul_price if net_cost_basis > 0 else 0
            _rp_verdict = {
                "verdict": "NO_VIABLE_RECOVERY_ROLL",
                "roll_mode": _ROLL_MODE_RECOVERY_PREMIUM,
                "no_viable_roll": True,
                "current_strike": round(strike, 2),
                "net_cost_basis": round(net_cost_basis, 2) if net_cost_basis else None,
                "ul_price": round(ul_price, 2),
                "current_delta": round(current_delta, 3),
                "roll_rationale": (
                    f"[RECOVERY PREMIUM] No viable roll candidates in "
                    f"14-45 DTE at delta 0.25-0.45. "
                    f"Stock ${ul_price:.2f}, basis ${net_cost_basis:.2f}, "
                    f"gap ${_rp_gap:.2f}/share. "
                    f"Options: (1) Wait for IV expansion to generate viable premium; "
                    f"(2) Accept wider delta range; "
                    f"(3) Re-evaluate thesis if IV consistently too low for recovery. "
                    f"(Jabbour Ch.4: don't force uneconomical writes)"
                ),
                "score": 0.0,
            }
            df.at[idx, "Roll_Candidate_1"] = json.dumps(_rp_verdict)
            logger.info(
                f"[RollEngine] {ticker} RECOVERY_PREMIUM mode — no viable candidates. "
                f"Stock=${ul_price:.2f}, basis=${net_cost_basis:.2f}"
            )
            continue

        # ── INCOME_SAME credit-preference re-ranking ──────────────────────────
        # In SIDEWAYS_INCOME, credit rolls are structurally correct — they reduce
        # basis and continue the income cycle. Debit rolls erode collected premium.
        # After scoring, re-sort: credits first (by score), then debits (by score).
        # This ensures the top candidate is always the best credit roll, even if a
        # far-OTM debit roll scored higher on raw delta proximity.
        if roll_mode in (_ROLL_MODE_INCOME_SAME, _ROLL_MODE_RECOVERY_PREMIUM) and candidates:
            # Pre-compute cost for re-ranking
            for _c in candidates:
                _c["_pre_cost"] = _estimate_roll_cost(row, _c)
            # Partition into credits and debits
            _credits = [c for c in candidates if c["_pre_cost"].get("type") == "credit"]
            _debits  = [c for c in candidates if c["_pre_cost"].get("type") != "credit"]
            # Credits first (already sorted by score), then debits
            candidates = _credits + _debits
            # Clean up pre-computed cost (will be re-assigned below)
            for _c in candidates:
                _c.pop("_pre_cost", None)
            if _credits:
                logger.info(
                    f"[RollEngine] {ticker} {roll_mode}: {len(_credits)} credit candidate(s) "
                    f"prioritised over {len(_debits)} debit candidate(s)"
                )

        for rank, cand in enumerate(candidates[:_TOP_N], start=1):
            cost_to_roll = _estimate_roll_cost(row, cand)
            cand["roll_from_strike"]  = strike
            cand["roll_from_dte"]     = int(dte)
            cand["roll_from_iv"]      = round(current_iv, 4)
            cand["cost_to_roll"]      = cost_to_roll
            cand["roll_mode"]         = roll_mode   # surface to dashboard
            # Trader-quality derived fields
            cand.update(_compute_trader_metrics(
                cand, cost_to_roll, net_cost_basis, cum_premium, ul_price, strategy_key
            ))
            # ── Economics vector: decomposed roll analysis ────────────────
            econ_vec = _compute_economics_vector(
                cand, cost_to_roll, row, net_cost_basis, ul_price, strategy_key
            )
            edge_label, edge_summary = _classify_candidate_edge(econ_vec)
            cand["economics"]    = econ_vec
            cand["primary_edge"] = edge_label
            cand["edge_summary"] = edge_summary
            cand["calendar_note"]     = calendar_note  # surfaced in dashboard trade-off line
            cand["roll_rationale"]    = _build_roll_rationale(
                cand, strategy_key, ul_price, net_cost_basis, roll_mode=roll_mode,
                position_signals={
                    "adx_14":                float(row.get("adx_14") or 0),
                    "roc_20":                float(row.get("roc_20") or 0),
                    "momentum_slope":        float(row.get("momentum_slope") or 0),
                    "MomentumVelocity_State": str(row.get("MomentumVelocity_State") or ""),
                    "iv_vs_hv_gap":          float(row.get("IV_vs_HV_Gap") or 0),
                    "drift_direction":       str(row.get("Drift_Direction") or ""),
                    "drift_magnitude":       str(row.get("Drift_Magnitude") or ""),
                },
                calendar_note=calendar_note,
            )
            df.at[idx, f"Roll_Candidate_{rank}"] = json.dumps(cand)

        # ── Split execution suggestion (multi-contract positions) ──────────
        # After all candidates are ranked and enriched, evaluate whether
        # splitting execution produces a better outcome than rolling all
        # contracts to a single candidate.
        _qty = abs(int(float(row.get("Quantity", 1) or 1)))
        if _qty >= 4 and candidates:
            _top_cands = [
                json.loads(df.at[idx, f"Roll_Candidate_{r}"])
                for r in range(1, min(_TOP_N + 1, len(candidates) + 1))
                if pd.notna(df.at[idx, f"Roll_Candidate_{r}"])
            ]
            split_sug = _compute_split_suggestion(
                _top_cands, _qty, strategy_key,
                net_cost_basis=net_cost_basis, ul_price=ul_price,
            )
            if split_sug:
                df.at[idx, "Roll_Split_Suggestion"] = json.dumps(split_sug)
                logger.info(
                    f"[RollEngine] {ticker} split suggestion: {split_sug['type']} "
                    f"({split_sug.get('rationale', '')[:80]}...)"
                )

    rolled_count = roll_mask.sum()
    filled_count = df[roll_mask]["Roll_Candidate_1"].notna().sum()
    logger.info(
        f"[RollEngine] {filled_count}/{rolled_count} ROLL rows populated with candidates."
    )
    return df


# ── Vol surface helpers ────────────────────────────────────────────────────────

def _build_vol_surface(
    chain: dict,
    cp: str,
    ul_price: float,
    min_dte: int,
    max_dte: int,
) -> Dict[str, Dict]:
    """
    Pre-scan the full chain to extract ATM IV and put skew per expiry.

    Returns a dict keyed by expiry_date_str with:
      {
        "atm_iv":    float | None  — IV of the closest-to-ATM strike
        "skew_ratio": float | None — put_otm_iv / atm_iv  (>1 = put skew present)
        "term_ivs":  {exp_str: atm_iv, ...}  — for term structure slope (populated
                                                after all expiries are processed)
      }

    Skew interpretation (Gatheral Ch.1 / Sinclair Ch.4):
      skew_ratio > 1.05 : put wing is bid — market pricing downside risk
      skew_ratio < 0.95 : unusual; call wing bid (rare for most equities)
      skew_ratio = 1.0  : flat smile — no skew at this expiry

    Term structure slope = (far_atm_iv - near_atm_iv) / (far_dte - near_dte)
      positive slope : contango — farther expiries are richer (normal backwardation
                        in equity options). Selling near term = selling cheap vol;
                        rolling to farther DTE picks up the contango premium.
      negative slope : backwardation (event-driven) — near-term IV spike; farther
                        expiries are cheaper. Common before earnings/macro events.
                        Rolling to farther DTE AFTER the event = buying cheap vol.

    Only scans DTE window [min_dte, max_dte] to avoid irrelevant expiries.
    """
    exp_map_key = "callExpDateMap" if cp == "C" else "putExpDateMap"
    exp_map     = chain.get(exp_map_key, {})
    # Also always pull put map for skew (put skew = put OTM IV vs ATM call IV)
    put_map     = chain.get("putExpDateMap", {})

    surface: Dict[str, Dict] = {}

    for exp_key, strikes_map in exp_map.items():
        parts = exp_key.split(":")
        if len(parts) != 2:
            continue
        exp_date_str = parts[0]
        try:
            exp_dte = int(parts[1])
        except ValueError:
            continue
        if exp_dte < min_dte or exp_dte > max_dte:
            continue

        # ── ATM IV — find the strike closest to ul_price ─────────────────
        atm_iv: float | None = None
        best_distance = float("inf")
        for strike_str, contracts in strikes_map.items():
            if not contracts:
                continue
            try:
                s = float(strike_str)
                dist = abs(s - ul_price)
                if dist < best_distance:
                    c = contracts[0]
                    iv_raw = c.get("volatility", c.get("impliedVolatility", None))
                    if iv_raw is not None:
                        iv_val = float(iv_raw) / 100.0
                        if iv_val > 0:
                            atm_iv = iv_val
                            best_distance = dist
            except (TypeError, ValueError):
                continue

        # ── Put skew — OTM put IV / ATM IV ────────────────────────────────
        # OTM put = strike ~85–90% of spot (25-delta proxy). Gatheral Ch.1:
        # "Equity skew arises from the demand for downside protection."
        # We use the put at ~87.5% of spot (midpoint of 80–95% OTM range).
        skew_ratio: float | None = None
        if atm_iv and atm_iv > 0 and ul_price > 0:
            _otm_target = ul_price * 0.875   # ~87.5% of spot = typical 25Δ put region
            _put_strikes_map = put_map.get(exp_key, {})
            _put_best_dist = float("inf")
            _put_otm_iv: float | None = None
            for _ps_str, _pcontracts in _put_strikes_map.items():
                if not _pcontracts:
                    continue
                try:
                    _ps = float(_ps_str)
                    # Only OTM puts (strike < ul_price)
                    if _ps >= ul_price:
                        continue
                    _dist = abs(_ps - _otm_target)
                    if _dist < _put_best_dist:
                        _pc = _pcontracts[0]
                        _piv_raw = _pc.get("volatility", _pc.get("impliedVolatility", None))
                        if _piv_raw is not None:
                            _piv = float(_piv_raw) / 100.0
                            if _piv > 0:
                                _put_otm_iv = _piv
                                _put_best_dist = _dist
                except (TypeError, ValueError):
                    continue
            if _put_otm_iv and _put_otm_iv > 0:
                skew_ratio = _put_otm_iv / atm_iv

        surface[exp_date_str] = {
            "atm_iv":    atm_iv,
            "skew_ratio": skew_ratio,
            "dte":       exp_dte,
        }

    # ── Term structure slope ───────────────────────────────────────────────
    # Computed across all expiries with valid ATM IV in the window.
    # slope = (IV_far - IV_near) / (DTE_far - DTE_near)
    # Injected into each expiry entry so _score_candidate can use it.
    sorted_expiries = sorted(
        [(v["dte"], k, v["atm_iv"]) for k, v in surface.items() if v["atm_iv"] is not None],
        key=lambda x: x[0],
    )
    for exp_date_str, entry in surface.items():
        entry["term_slope"] = None  # default: no slope computed

    if len(sorted_expiries) >= 2:
        near_dte, near_key, near_iv = sorted_expiries[0]
        far_dte,  far_key,  far_iv  = sorted_expiries[-1]
        if far_dte > near_dte and near_iv and far_iv:
            _slope = (far_iv - near_iv) / (far_dte - near_dte)
            for _, entry in surface.items():
                entry["term_slope"] = round(_slope, 6)

    return surface


# ── Candidate selection ────────────────────────────────────────────────────────

def _select_roll_candidates(
    chain: dict,
    cp: str,
    ul_price: float,
    current_strike: float,
    current_dte: float,
    current_iv: float,
    dte_window: Tuple[int, int, int],
    delta_range: Tuple[float, float],
    strategy_key: str,
    net_cost_basis: float = 0.0,
    hv_20d: float = 0.0,
    roll_mode: str = _ROLL_MODE_NORMAL,
    days_to_earnings: int | None = None,
    current_option_mid: float = 0.0,
    leap_strike: float = 0.0,
    days_to_dividend: float = 9999.0,
    dividend_amount: float = 0.0,
    consecutive_debit_rolls: int = 0,
    roll_trigger: str = ROLL_TRIGGER_DISCRETIONARY,
) -> List[Dict[str, Any]]:
    """
    Parse the chain, filter for roll-eligible contracts, score and rank them.
    Returns ranked list of candidate dicts.

    roll_mode controls two mode-specific behaviors:
      PRE_ITM   — above-basis strikes are strongly preferred (scored higher);
                  credit rolls sorted to top; debit rolls included but penalized.
      EMERGENCY — above-basis is a HARD FILTER: any strike at or below net_cost_basis
                  is excluded entirely. Only above-basis rolls surface. If none exist,
                  returns empty list → caller generates no-viable-roll verdict.
      NORMAL    — standard scoring, no basis filter.
    """
    min_dte, max_dte, target_dte = dte_window
    delta_min, delta_max = delta_range
    is_short_vol = strategy_key in ("BUY_WRITE", "COVERED_CALL", "SHORT_PUT", "CSP")

    # BASIS_REDUCTION: prefer same expiry — the intent is roll DOWN (tighter strike),
    # not OUT (longer DTE). Override target_dte so same-expiry candidates score 1.0
    # on DTE instead of being penalized for not matching the default 45d target.
    # McMillan Ch.3: "Roll down to capture extrinsic, not out to buy time."
    if roll_mode == _ROLL_MODE_BASIS_REDUCTION and current_dte > 0:
        target_dte = int(current_dte)

    _emergency_basis_filter = (
        roll_mode == _ROLL_MODE_EMERGENCY
        and is_short_vol
        and net_cost_basis > 0
    )

    # Phase 2 — vol surface pre-scan.
    # Build ATM IV, put skew, and term structure slope before the main scoring loop.
    # This avoids re-scanning the chain per-strike and gives each candidate
    # expiry-level surface data for a richer IV score in _score_candidate().
    vol_surface = _build_vol_surface(
        chain    = chain,
        cp       = cp,
        ul_price = ul_price,
        min_dte  = min_dte,
        max_dte  = max_dte,
    )

    exp_map_key = "callExpDateMap" if cp == "C" else "putExpDateMap"
    exp_map     = chain.get(exp_map_key, {})

    candidates: List[Dict[str, Any]] = []

    # ── Diagnostic: log available expiries vs DTE window ─────────────────
    _diag_expiries = []
    for _ek in exp_map:
        _pts = _ek.split(":")
        if len(_pts) == 2:
            try:
                _diag_expiries.append((_pts[0], int(_pts[1])))
            except ValueError:
                pass
    _diag_expiries.sort(key=lambda x: x[1])
    _diag_in_window = [
        f"{d}(DTE{e})" for d, e in _diag_expiries
        if min_dte <= e <= max_dte and e > current_dte
    ]
    logger.info(
        f"[RollEngine] _select: mode={roll_mode} cp={cp} "
        f"dte_window=({min_dte},{max_dte},{target_dte}) current_dte={current_dte:.0f} "
        f"delta_range=({delta_min:.2f},{delta_max:.2f}) "
        f"chain_expiries={len(_diag_expiries)} in_window={len(_diag_in_window)} "
        f"{_diag_in_window[:5]}"
    )

    _diag_dte_rejected = 0
    _diag_rollout_rejected = 0
    _diag_mid_rejected = 0
    _diag_delta_rejected = 0
    _diag_liq_rejected = 0
    _diag_basis_rejected = 0

    for exp_key, strikes_map in exp_map.items():
        # exp_key format: "2026-04-17:55"
        parts = exp_key.split(":")
        if len(parts) != 2:
            continue
        exp_date_str = parts[0]
        try:
            actual_dte = int(parts[1])
        except ValueError:
            continue

        # Filter by DTE window — must be FURTHER out than current (rolling out)
        # Exception: BASIS_REDUCTION allows same-expiry (roll DOWN to lower strike)
        # but NOT drastically shorter DTE. Floor at 75% of current_dte to prevent
        # yield inflation from short-dated weeklies dominating the score.
        # E.g., at 66 DTE current → floor 49 DTE (allows May 1 but rejects Apr 10).
        if actual_dte < min_dte or actual_dte > max_dte:
            _diag_dte_rejected += len(strikes_map)
            continue
        _is_basis_reduction = roll_mode == _ROLL_MODE_BASIS_REDUCTION
        if _is_basis_reduction:
            _basis_dte_floor = max(min_dte, int(current_dte * 0.75))
            if actual_dte < _basis_dte_floor:
                _diag_rollout_rejected += len(strikes_map)
                continue  # Too short for basis reduction — prefer same horizon
        elif actual_dte <= current_dte:
            _diag_rollout_rejected += len(strikes_map)
            continue  # Cannot roll to same or shorter DTE

        for strike_str, contracts in strikes_map.items():
            if not contracts:
                continue
            c = contracts[0]

            # Extract contract data
            try:
                strike      = float(strike_str)
                delta_raw   = float(c.get("delta", 0) or 0)
                gamma       = float(c.get("gamma", 0) or 0)
                vega        = float(c.get("vega",  0) or 0)
                theta       = float(c.get("theta", 0) or 0)
                bid         = float(c.get("bid",   0) or 0)
                ask         = float(c.get("ask",   0) or 0)
                bid_size    = int(c.get("bidSize",  0) or 0)
                ask_size    = int(c.get("askSize",  0) or 0)
                oi          = int(c.get("openInterest", 0) or 0)
                volume      = int(c.get("totalVolume",  0) or 0)
                iv_raw      = c.get("volatility", c.get("impliedVolatility", None))
                iv          = float(iv_raw) / 100.0 if iv_raw is not None else np.nan
                mid         = (bid + ask) / 2.0 if bid > 0 and ask > 0 else float(c.get("mark", 0) or 0)
            except (TypeError, ValueError):
                continue

            if mid <= 0 or bid <= 0:
                _diag_mid_rejected += 1
                continue

            # Delta filter — calls positive, puts negative
            if cp == "C":
                if not (delta_min <= delta_raw <= delta_max):
                    _diag_delta_rejected += 1
                    continue
            else:
                # Put deltas are negative: delta_range like (-0.35, -0.15)
                if not (delta_range[0] <= delta_raw <= delta_range[1]):
                    _diag_delta_rejected += 1
                    continue

            # Liquidity filter — now includes bid/ask size for book-depth awareness
            spread_pct = ((ask - bid) / mid * 100) if mid > 0 else 999
            liq_grade  = _grade_liquidity(oi, volume, spread_pct, bid_size=bid_size, ask_size=ask_size)
            if liq_grade == "ILLIQUID":
                _diag_liq_rejected += 1
                continue

            # ── PMCC width constraint — short call must stay below LEAP strike ──
            # PMCC = long deep-ITM LEAP + short near-term OTM call. The short call
            # strike MUST be below the LEAP strike to maintain debit spread structure.
            # If short call strike ≥ LEAP strike → width inversion → broken PMCC.
            # Passarelli Ch.5: "The short strike above the long creates unlimited risk."
            if leap_strike > 0 and strike >= leap_strike:
                _diag_basis_rejected += 1  # reuse counter (structural rejection)
                continue

            # ── EMERGENCY mode: hard filter — above net cost basis only ──────
            # McMillan Ch.3: "If no roll above your cost basis exists, assignment
            # is preferable to locking in a debit roll that still realizes a loss."
            # Only apply to short vol strategies where basis comparison makes sense.
            if _emergency_basis_filter and strike <= net_cost_basis:
                _diag_basis_rejected += 1
                continue  # below/at basis — would not rescue position

            # Phase 2 — pull per-expiry surface data for this candidate
            _surf = vol_surface.get(exp_date_str, {})
            _atm_iv    = _surf.get("atm_iv")
            _skew_ratio = _surf.get("skew_ratio")
            _term_slope = _surf.get("term_slope")

            # Score: returns full economics vector (sub-scores + composite)
            score_vector = _score_candidate(
                delta            = abs(delta_raw),
                target_delta     = abs((delta_range[0] + delta_range[1]) / 2),
                actual_dte       = actual_dte,
                target_dte       = target_dte,
                iv               = iv,
                current_iv       = current_iv,
                liq_grade        = liq_grade,
                strategy_key     = strategy_key,
                mid              = mid,
                theta            = theta,
                net_cost_basis   = net_cost_basis,
                hv_20d           = hv_20d,
                ul_price         = ul_price,
                strike           = strike,
                current_option_mid = current_option_mid,
                current_strike   = current_strike,
                days_to_dividend = days_to_dividend,
                dividend_amount  = dividend_amount,
                consecutive_debit_rolls = consecutive_debit_rolls,
                roll_mode        = roll_mode,
                days_to_earnings = days_to_earnings,
                atm_iv           = _atm_iv,
                skew_ratio       = _skew_ratio,
                term_slope       = _term_slope,
                roll_trigger     = roll_trigger,
            )
            score = score_vector["composite"]

            candidates.append({
                "strike":     round(strike, 2),
                "expiry":     exp_date_str,
                "dte":        actual_dte,
                "delta":      round(delta_raw, 3),
                "gamma":      round(gamma, 4),
                "vega":       round(vega, 4),
                "theta":      round(theta, 4),
                "iv":         round(iv, 4) if not np.isnan(iv) else None,
                "bid":        round(bid, 2),
                "ask":        round(ask, 2),
                "mid":        round(mid, 2),
                "bid_size":   bid_size,
                "ask_size":   ask_size,
                "oi":         oi,
                "volume":     volume,
                "spread_pct": round(spread_pct, 1),
                "liq_grade":  liq_grade,
                "score":      round(score, 4),
                "score_vector": score_vector,
                "hv_20d":     round(hv_20d, 4) if hv_20d else None,
                "atm_iv":     round(_atm_iv, 4) if _atm_iv is not None else None,
                "skew_ratio": round(_skew_ratio, 3) if _skew_ratio is not None else None,
                "term_slope": round(_term_slope, 6) if _term_slope is not None else None,
                "roll_trigger": roll_trigger,
            })

    # ── Diagnostic summary ──────────────────────────────────────────────
    logger.info(
        f"[RollEngine] _select filter summary: "
        f"dte_rejected={_diag_dte_rejected} rollout_rejected={_diag_rollout_rejected} "
        f"mid_rejected={_diag_mid_rejected} delta_rejected={_diag_delta_rejected} "
        f"liq_rejected={_diag_liq_rejected} basis_rejected={_diag_basis_rejected} "
        f"→ {len(candidates)} candidates survived"
    )

    # Sort by score descending, then enforce expiry diversity in top N.
    # Without diversity, all 5 candidates can cluster in the same expiry
    # with tiny strike differences — MC rerank then can't discover that a
    # different expiration has better EV.
    candidates.sort(key=lambda x: x["score"], reverse=True)
    candidates = _enforce_expiry_diversity(candidates, _TOP_N)
    return candidates


def _enforce_expiry_diversity(candidates: list, top_n: int) -> list:
    """
    Ensure the top-N shortlist spans at least 2 expirations when possible.

    Algorithm: greedily pick best-scored candidates, but cap any single
    expiration at (top_n - 1) slots so at least one alternate expiry can
    enter. Remaining candidates follow in score order.

    If fewer than 2 distinct expirations exist in the full list, returns
    plain score-sorted order (no diversity possible).
    """
    if len(candidates) <= top_n:
        return candidates

    # Count distinct expirations
    expiries = {c.get("expiry", c.get("actual_dte", "")) for c in candidates}
    if len(expiries) < 2:
        return candidates  # only one expiration available

    max_per_expiry = top_n - 1  # reserve at least 1 slot for diversity
    selected = []
    expiry_counts: dict = {}
    remainder = []

    for c in candidates:
        exp = c.get("expiry", c.get("actual_dte", ""))
        if len(selected) < top_n:
            if expiry_counts.get(exp, 0) < max_per_expiry:
                selected.append(c)
                expiry_counts[exp] = expiry_counts.get(exp, 0) + 1
            else:
                remainder.append(c)
        else:
            remainder.append(c)

    # If we didn't fill all slots (unlikely), backfill from remainder
    for c in remainder:
        if len(selected) >= top_n:
            break
        selected.append(c)
    remainder = [c for c in remainder if c not in selected]

    return selected + remainder


# ── Scoring ───────────────────────────────────────────────────────────────────

def _score_candidate(
    delta: float,
    target_delta: float,
    actual_dte: int,
    target_dte: int,
    iv: float,
    current_iv: float,
    liq_grade: str,
    strategy_key: str,
    mid: float = 0.0,
    theta: float = 0.0,
    net_cost_basis: float = 0.0,
    hv_20d: float = 0.0,
    ul_price: float = 0.0,
    strike: float = 0.0,
    roll_mode: str = _ROLL_MODE_NORMAL,
    days_to_earnings: int | None = None,
    atm_iv: float | None = None,
    skew_ratio: float | None = None,
    term_slope: float | None = None,
    current_option_mid: float = 0.0,
    current_strike: float = 0.0,
    days_to_dividend: float = 9999.0,
    dividend_amount: float = 0.0,
    consecutive_debit_rolls: int = 0,
    roll_trigger: str = ROLL_TRIGGER_DISCRETIONARY,
) -> float:
    """
    Composite score (higher = better roll candidate). Think like a trader.

    McMillan Ch.3 / Passarelli Ch.6 / Natenberg Ch.11 / Gatheral Ch.1:

    Components and weights (default, adjusted by roll_trigger):
      1. Delta proximity  (25%) — strike placement relative to directional thesis
      2. Annualized yield (25%) — premium efficiency on net cost basis (Passarelli Ch.6)
                                   blended 70/30 with theta efficiency (Phase 1c)
      3. DTE fit          (20%) — time horizon alignment with strategy cycle
      4. Liquidity        (20%) — execution feasibility (THIN grade = credit is theoretical)
      5. IV advantage     (10%) — vol edge: IV/HV ratio + vol surface signals (Phase 2)
                                   atm_iv vs strike iv (skew position)
                                   term structure slope (contango vs backwardation)

    roll_trigger adjusts these weights situationally:
      ASSIGNMENT_DEFENSE: delta weight 1.6×, yield 0.6× (rescue mode — delta reduction is critical)
      INCOME_GATE:        yield weight 1.4×, delta 0.8× (income cycle — maximize premium)
      HARD_STOP:          delta 1.4×, dte 1.3×, yield 0.5× (survival over income)
      GAMMA_DANGER:       dte weight 1.6× (extend DTE to cut gamma ∝ 1/√T)
      etc. — see _TRIGGER_WEIGHT_ADJUSTMENTS

    Post-scoring multipliers:
      Earnings-in-window penalty (Phase 1b) — if earnings fall before this contract
      expires, the new position carries the IV collapse event. Natenberg Ch.8:
      "IV collapses post-earnings — all extrinsic value earned by selling the event
      is surrendered on the morning after." Penalty is strategy-conditional:
      higher for short-vol (selling the event is structurally dangerous) than
      long-vol (IV crush hurts but does not guarantee a loss).

    Yield replaces the old DTE-as-primary approach: two strikes at same DTE but
    different premiums should score differently — the one that actually earns
    more per dollar of capital deployed wins (McMillan: "maximize income per cycle").
    """
    is_short_vol = strategy_key in ("BUY_WRITE", "COVERED_CALL", "SHORT_PUT", "CSP")

    # 1. Delta proximity (25%) — closer to target = better strike placement
    max_delta_deviation = 0.30
    delta_dev   = abs(delta - target_delta)
    delta_score = max(0.0, 1.0 - delta_dev / max_delta_deviation)

    # 2. Annualized yield on capital (25%) — the trader's primary filter for income strategies
    # Passarelli Ch.6: "Choose the roll that maximizes premium per day per dollar at risk."
    # Benchmark: must exceed Fidelity's 10.375%/yr margin rate to have positive carry.
    # Excellent = 2× margin rate (~20.75%/yr), neutral = margin rate (just covering carry),
    # poor = below margin rate (negative carry — this roll loses money on financing alone).
    # McMillan Ch.3: "Any roll that fails to cover the financing cost is negative carry."
    from core.shared.finance_utils import annualized_yield as _ann_yield
    yield_score = 0.5  # neutral default
    if actual_dte > 0 and mid > 0:
        if is_short_vol and net_cost_basis > 0:
            # Per-share income annualized on net cost basis
            annualized_yield = _ann_yield(mid, net_cost_basis, actual_dte)
            # Score: 0 at 0%/yr, 0.5 at margin rate (10.375%), 1.0 at 2× margin rate (~20.75%)
            yield_score = min(1.0, annualized_yield / _YIELD_BENCHMARK_EXCELLENT)
        elif is_short_vol and ul_price > 0:
            # Fallback: yield on current stock price
            annualized_yield = _ann_yield(mid, ul_price, actual_dte)
            yield_score = min(1.0, annualized_yield / _YIELD_BENCHMARK_EXCELLENT)
        elif not is_short_vol:
            # Long vol: score by extrinsic value as % of premium paid
            # More extrinsic = more time value = more room for thesis to develop
            intrinsic = max(0.0, ul_price - strike) if ul_price > strike else 0.0
            extrinsic = max(0.0, mid - intrinsic)
            extrinsic_pct = extrinsic / mid if mid > 0 else 0.5
            yield_score = min(1.0, extrinsic_pct * 1.5)  # 67% extrinsic = perfect

    # Phase 1c — theta efficiency blend into yield_score (30% weight within yield component)
    # theta_per_dollar = |theta| / mid: daily theta earned per dollar of premium.
    # Passarelli Ch.6: "The efficient roll maximizes theta collected per dollar committed."
    # A strike with the same premium but higher theta/dollar harvests the carry faster —
    # less exposure to adverse moves per unit of income collected.
    # Benchmark: 0.033/day per dollar at mid = 1.0% daily = perfect (annualizes to ~12%/yr
    # of the premium itself, which exceeds any realistic carry benchmark).
    # Blend: 70% raw yield + 30% theta efficiency — preserves the capital basis anchor
    # while rewarding candidates that harvest carry faster.
    if theta != 0 and mid > 0:
        theta_per_dollar = abs(theta) / mid   # daily decay per $1 of premium
        # Scale: 0.033/day = score 1.0 (1%/day); anything above is capped
        theta_score = min(1.0, theta_per_dollar / 0.033)
        yield_score = 0.70 * yield_score + 0.30 * theta_score

    # ── Net-roll economics adjustment (non-INCOME_SAME short-vol modes) ──────
    # Problem: yield_score uses `mid` (new premium only), ignoring the cost to
    # close the current option. A candidate showing 22%/yr on $0.65 open premium
    # that costs $0.75 to close = net -$0.10 debit. Without this adjustment,
    # the scorer ranks debit rolls as if they were free credits.
    #
    # McMillan Ch.3: "The roll credit or debit is the actual economics of the trade."
    # Passarelli Ch.6: "Net premium received, not gross, determines carry."
    #
    # For INCOME_SAME mode: credit-preference re-ranking (line ~746) handles this
    # via post-scoring partition. All other short-vol modes need it in-score.
    #
    # Components:
    #   1. net_roll_economics: adjusts yield_score based on net credit/debit
    #   2. recovery_improvement: bonus/penalty for strike lift vs basis
    #      - Small debit can win if strike lift materially improves recovery geometry
    #      - Large debit with weak strike improvement → penalty
    net_roll_adj = 0.0
    recovery_score = 0.0
    if (is_short_vol
        and current_option_mid > 0
        and mid > 0
        and roll_mode != _ROLL_MODE_INCOME_SAME):

        # Net roll: positive = credit, negative = debit
        # For short vol: close = buy at ask, open = sell at bid.
        # Conservative slippage model: assume ~2% adverse fill on each leg.
        # Mid-to-mid overstates credit by 2-5%, especially for THIN candidates.
        # Natenberg Ch.11: "Theoretical edge disappears in the spread."
        # Actual cost is computed precisely by _estimate_roll_cost() post-scoring
        # using real bid/ask — this is a scoring-time conservative estimate.
        _close_cost = current_option_mid * 1.02   # buy back at ask (~2% worse)
        _open_proceeds = mid * 0.98               # sell at bid (~2% worse)
        net_roll = _open_proceeds - _close_cost   # positive = net credit roll

        # --- Component 1: Net roll economics on yield ---
        # Replace gross yield with net-aware yield when net_roll is negative (debit).
        # Credit rolls: yield_score already reflects actual economics (good).
        # Debit rolls: penalize yield proportionally to how much debit eats the premium.
        if net_roll < 0:
            # Debit magnitude as fraction of new premium: e.g. -0.10/0.65 = 15% eaten
            debit_fraction = abs(net_roll) / mid
            # Penalty: scale yield down. Full debit (fraction=1.0) → yield_score × 0.20
            # Half debit (fraction=0.5) → yield_score × 0.60. Mild debit (<10%) → minor.
            debit_penalty = max(0.20, 1.0 - debit_fraction * 0.80)
            yield_score *= debit_penalty
        else:
            # Credit roll in non-INCOME_SAME mode: mild bonus (credit = preferred)
            # Bonus proportional to credit size vs premium: 0.15 credit on 0.65 = 23% bonus
            credit_bonus = min(0.15, (net_roll / mid) * 0.30)
            net_roll_adj = credit_bonus

        # --- Component 2: Recovery improvement (strike lift vs basis) ---
        # A debit roll is justified when it materially lifts the strike toward/above basis.
        # McMillan Ch.3: "Pay a small debit to move the strike from deeply underwater to
        # near breakeven — the assignment path improves even if you pay to get there."
        if net_cost_basis > 0 and current_strike > 0:
            # Strike improvement: how much closer to (or above) basis does the new strike get?
            # Old gap: current_strike vs basis (negative = underwater)
            old_gap_pct = (current_strike - net_cost_basis) / net_cost_basis
            new_gap_pct = (strike - net_cost_basis) / net_cost_basis
            strike_improvement = new_gap_pct - old_gap_pct  # positive = closer to basis

            if strike_improvement > 0:
                # Bonus: meaningful strike lift toward basis
                # 5% improvement = +0.10, 10% = +0.15, cap at +0.20
                recovery_score = min(0.20, strike_improvement * 2.0)

                # If debit roll BUT strike crosses above basis → strong recovery signal
                # Guard: crossing must be economically meaningful (>1% above basis)
                # to avoid rewarding symbolic $0.01 crossings that disappear in slippage.
                # Passarelli Ch.6: "Breakeven after friction is the real breakeven."
                if net_roll < 0 and strike > net_cost_basis and current_strike <= net_cost_basis:
                    basis_clearance_pct = (strike - net_cost_basis) / net_cost_basis
                    if basis_clearance_pct >= 0.01:  # ≥1% above basis = meaningful
                        recovery_score = min(0.25, recovery_score + 0.10)
                    # else: symbolic crossing (<1%) — keep normal recovery_score, no extra bonus
            elif strike_improvement < -0.02 and net_roll < 0:
                # Rolling DOWN in strike AND paying a debit → worst case: penalty
                recovery_score = -0.10

        # --- Liquidity gate on bonuses ---
        # Bonuses (credit, recovery) are theoretical if the candidate can't fill.
        # A THIN/ILLIQUID candidate with OI=14 and 30%+ spread cannot execute at mid —
        # the credit is phantom and the recovery geometry is unrealizable.
        # McMillan Ch.3: "A roll that can't fill at a reasonable price is not a roll."
        # Suppress bonuses for poor liquidity; preserve penalties (debit penalty stands
        # regardless of liquidity — a debit roll on a THIN candidate is doubly bad).
        if liq_grade in ("THIN", "ILLIQUID"):
            net_roll_adj = min(net_roll_adj, 0.0)     # suppress credit bonus
            recovery_score = min(recovery_score, 0.0)  # suppress recovery bonus

    # 3. DTE fit (20%) — time horizon match
    max_dte_deviation = 60
    dte_dev   = abs(actual_dte - target_dte)
    dte_score = max(0.0, 1.0 - dte_dev / max_dte_deviation)

    # 4. IV advantage (10%) — volatility edge: IV/HV + vol surface signals (Phase 2)
    #
    # Base signal: IV/HV ratio (Natenberg Ch.11)
    #   Short vol: IV > HV = positive edge (selling rich premium)
    #   Long vol:  IV < HV = positive edge (buying cheap vol)
    #
    # Surface signal 1 — skew position (Gatheral Ch.1 / Sinclair Ch.4):
    #   skew_ratio = OTM put IV / ATM IV. Equity options almost always have skew > 1.
    #   For SHORT PUT / CSP: high skew (>1.15) means you're in the put wing bid —
    #     the market is paying you extra for downside protection. Strong short-vol edge.
    #   For LONG CALL: high skew means farther OTM calls are cheaper relative to ATM.
    #     Rolling to a slightly higher-delta (closer-to-ATM) call is more efficient.
    #   Implementation: skew bonus/penalty on iv_score (capped to avoid dominating)
    #
    # Surface signal 2 — term structure slope (Gatheral Ch.2):
    #   Positive slope (contango): farther expiries carry more IV. For short-vol rolls
    #     to farther DTE, you're selling into richer vol — term structure is favorable.
    #   Negative slope (backwardation): near-term is spiked (event). Rolling farther
    #     = buying the spike at near-term and selling cheap farther out. Avoid for
    #     income strategies; potentially useful for long vol (buy now, vol collapses).
    #   Implementation: slope adjustment added to iv_score
    iv_score = 0.5
    _iv_valid = not (np.isnan(iv) if isinstance(iv, float) else False) and iv > 0

    if _iv_valid:
        if hv_20d > 0:
            iv_hv_ratio = iv / hv_20d
            if is_short_vol:
                iv_score = min(1.0, max(0.0, iv_hv_ratio - 0.5))  # 1.5× = perfect
            else:
                iv_score = min(1.0, max(0.0, 1.5 - iv_hv_ratio))
        elif current_iv > 0:
            iv_ratio = iv / current_iv
            if is_short_vol:
                iv_score = max(0.0, min(1.0, iv_ratio - 0.5))
            else:
                iv_score = max(0.0, min(1.0, 1.5 - iv_ratio))

    # Phase 2a — skew adjustment
    # Applies when we have atm_iv to compare strike IV against the ATM level.
    # This captures the skew position: is this strike in the put wing (bid) or
    # call wing (cheap)? The ratio iv/atm_iv tells us where on the smile we are.
    # For short-vol PUT strategies: a strike with iv > atm_iv is in the put wing —
    #   the market is paying extra for that strike. This is positive edge.
    # For short-vol CALL strategies: iv ≈ atm_iv is ideal (ATM = richest premium/delta).
    #   iv > atm_iv on calls = unusual; may indicate straddle bid or event.
    # For long vol: iv < atm_iv = buying the cheap wing (OTM options cheaper vs ATM).
    if _iv_valid and atm_iv and atm_iv > 0:
        _strike_vs_atm = iv / atm_iv  # >1 = this strike is in the "expensive" wing
        if is_short_vol:
            # Selling expensive wing = positive edge
            _skew_adj = min(0.10, max(-0.10, (_strike_vs_atm - 1.0) * 0.5))
        else:
            # Buying cheap wing = positive edge (lower iv/atm_iv = better for long vol)
            _skew_adj = min(0.10, max(-0.10, (1.0 - _strike_vs_atm) * 0.5))
        iv_score = min(1.0, max(0.0, iv_score + _skew_adj))

    # Phase 2b — put skew ratio awareness (Gatheral Ch.1)
    # skew_ratio = OTM put IV / ATM IV for this expiry.
    # High skew (>1.15) means the market is paying a lot for downside protection.
    # For short put strategies: high skew means you're selling expensive protection —
    #   extra premium per dollar of risk. Positive edge; mild bonus.
    # For long call strategies: high skew means OTM puts are expensive relative to
    #   calls. Irrelevant or slight negative (capital is being pulled to put wing).
    # Adjustment is small (±0.05 max) to inform without dominating the IV score.
    if skew_ratio is not None and skew_ratio > 0:
        if is_short_vol and strategy_key in ("SHORT_PUT", "CSP"):
            # High put skew = market paying more for puts = selling puts has more edge
            _skew_ratio_adj = min(0.05, max(-0.05, (skew_ratio - 1.0) * 0.25))
            iv_score = min(1.0, max(0.0, iv_score + _skew_ratio_adj))

    # Phase 2c — term structure adjustment (Gatheral Ch.2)
    # term_slope > 0 (contango): farther expiries carry more IV.
    #   For short-vol rolling farther: you're rolling into richer premium — favorable.
    #   Magnitude: if slope is large (e.g., 0.001 IV/day = 0.1 vol over 100d), that's
    #   a meaningful premium pickup on a longer roll.
    # term_slope < 0 (backwardation): farther expiries are cheaper.
    #   Rolling farther = selling cheaper vol. Negative for income strategies.
    # Adjustment capped at ±0.08 to prevent term structure dominating the score.
    if term_slope is not None:
        # Normalize: 0.001 IV/day = "moderate" contango for equities
        _slope_adj = min(0.08, max(-0.08, term_slope * 80))
        if is_short_vol:
            # Contango (positive slope) = favorable for short-vol extension rolls
            iv_score = min(1.0, max(0.0, iv_score + _slope_adj))
        else:
            # Long vol: backwardation (negative slope) after event = buy cheap
            iv_score = min(1.0, max(0.0, iv_score - _slope_adj))

    # 5. Liquidity (20%) — raised from 10%
    # A roll candidate with THIN/ILLIQUID grade cannot reliably fill at mid.
    # Theoretical yield is irrelevant if execution cost eats the credit.
    # McMillan Ch.3: "A roll that can't fill at a reasonable price is not a roll."
    liq_map = {"EXCELLENT": 1.0, "GOOD": 0.75, "ACCEPTABLE": 0.5, "THIN": 0.25, "ILLIQUID": 0.0}
    liq_score = liq_map.get(liq_grade, 0.5)

    # Weights: delta 25%, yield 25%, DTE 20%, liquidity 20%, IV 10%
    # Rationale: liquidity raised from 10% to 20% (execution feasibility is binary —
    # a THIN strike with 30%+ spread will not fill at mid, making the credit theoretical).
    # Delta reduced from 30% to 25% (small delta deviation matters less than executability).
    # net_roll_adj and recovery_score are additive bonuses/penalties from net-roll economics.
    #
    # Roll trigger adjusts weights situationally — e.g., ASSIGNMENT_DEFENSE boosts delta
    # weight 1.6× and reduces yield to 0.6× because delta reduction is the goal, not income.
    _tw = _get_trigger_weights(roll_trigger)
    _w_delta = 0.25 * _tw["delta_w"]
    _w_yield = 0.25 * _tw["yield_w"]
    _w_dte   = 0.20 * _tw["dte_w"]
    _w_iv    = 0.10 * _tw["iv_w"]
    _w_liq   = 0.20 * _tw["liq_w"]
    _w_total = _w_delta + _w_yield + _w_dte + _w_iv + _w_liq
    # Renormalize so weights sum to 1.0 (preserve score scale)
    if _w_total > 0:
        _w_delta /= _w_total
        _w_yield /= _w_total
        _w_dte   /= _w_total
        _w_iv    /= _w_total
        _w_liq   /= _w_total

    base_score = (_w_delta * delta_score + _w_yield * yield_score +
                  _w_dte   * dte_score   + _w_iv    * iv_score +
                  _w_liq   * liq_score   + net_roll_adj + recovery_score)

    # ── Mode-specific score adjustments ─────────────────────────────────────
    # PRE_ITM: credit-first prioritization + above-basis bonus.
    # EMERGENCY: above-basis strikes already hard-filtered, so add a bonus for
    # how far ABOVE basis the strike is (more rescue distance = better).
    # INCOME_SAME: same-strike proximity bonus — prefer rolling OUT, not UP.
    mode_bonus = 0.0
    if roll_mode in (_ROLL_MODE_PRE_ITM, _ROLL_MODE_EMERGENCY) and is_short_vol:
        if net_cost_basis > 0 and strike > 0:
            if strike > net_cost_basis:
                # Bonus proportional to how far above basis: e.g. +3% above = +0.06 bonus
                basis_pct_above = (strike - net_cost_basis) / net_cost_basis
                mode_bonus += min(0.15, basis_pct_above * 2.0)  # cap at 0.15
            elif roll_mode == _ROLL_MODE_PRE_ITM and strike <= net_cost_basis:
                # Below-basis: soft penalty in PRE_ITM (not hard-filtered, but deprioritized)
                mode_bonus -= 0.10
    elif roll_mode == _ROLL_MODE_INCOME_SAME and is_short_vol:
        # SIDEWAYS_INCOME: same-strike credit rolls are structurally correct.
        # McMillan Ch.3: in range-bound behaviour, roll OUT in time, not UP in strike.
        # Bonus: strikes near current strike get up to +0.20 (dominant influence).
        # Penalty: strikes far from current (delta <0.30) lose up to -0.10.
        # This flips the scoring from "target OTM delta" to "target same strike".
        if ul_price > 0 and strike > 0:
            # How close is this candidate's strike to the current price?
            # delta ~0.50 = ATM = same strike; delta ~0.20 = far OTM
            # Use delta proximity to ATM as proxy (simpler, no need for current_strike)
            _atm_proximity = 1.0 - abs(delta - 0.50) / 0.30  # 1.0 at delta=0.50, 0 at delta=0.20/0.80
            _atm_proximity = max(0.0, min(1.0, _atm_proximity))
            mode_bonus += _atm_proximity * 0.20  # up to +0.20 for ATM/same-strike
            # Penalize far-OTM strikes that would require a debit to roll UP
            if delta < 0.30:
                mode_bonus -= 0.10

    elif roll_mode == _ROLL_MODE_BASIS_REDUCTION and is_short_vol:
        # Basis reduction: strongly reward strikes closer to ATM (more extrinsic).
        # The whole point is rolling DOWN to a tighter strike for premium recycling.
        # McMillan Ch.3: "Roll down to capture extrinsic, not out to buy time."
        # Previous bonus (max 0.15) was too weak — $145 vs $150 only differed by
        # 0.037, which was noise against the base score. Strengthened to dominate
        # strike selection: up to +0.30 for near-ATM, with penalty for far-OTM
        # (>10% OTM) strikes that defeat the purpose of rolling down.
        if ul_price > 0 and strike > 0:
            _otm_ratio = (strike - ul_price) / ul_price if strike > ul_price else 0.0
            # Strong bonus: ATM → +0.30, 5% OTM → +0.20, 10% OTM → +0.10, 15%+ → 0
            _tightness_bonus = max(0.0, 0.30 - _otm_ratio * 2.0)
            mode_bonus += _tightness_bonus
            # Penalty for strikes >10% OTM — these are still dead weight, not basis
            # reduction. The current $150 at $135 stock = 11.1% OTM should score
            # lower than $145 at 7.4% OTM or $140 at 3.7% OTM.
            if _otm_ratio > 0.10:
                mode_bonus -= 0.08
        # Credit preference: basis reduction exists to collect premium and reduce
        # net cost. A debit roll defeats the purpose. Compare candidate mid to a
        # rough estimate of current option value (use mid if available, else 0).
        # Higher candidate mid → more likely to produce net credit when closing
        # the current (further OTM) call and selling this (tighter) one.
        # Mild bonus only — the DTE floor is the primary guard against bad candidates.
        if mid > 0 and net_cost_basis > 0:
            _premium_yield = mid / net_cost_basis  # raw premium as % of basis
            # Bonus: 2% premium → +0.05, 3% → +0.075, 4%+ → +0.10 (capped)
            mode_bonus += min(0.10, _premium_yield * 2.5)

    elif roll_mode == _ROLL_MODE_RECOVERY_PREMIUM and is_short_vol:
        # Recovery premium: score by basis improvement per cycle.
        # The question is: "How many dollars of basis reduction does this
        # contract deliver per cycle, relative to the current gap?"
        # Jabbour Ch.4: "In repair, premium/cycle captures compounding."
        #
        # 1. Premium density: mid / dte — dollars of premium per day of exposure
        if actual_dte > 0 and mid > 0:
            _prem_per_day = mid / actual_dte
            # Benchmark: $0.10/day = excellent for recovery (high IV scenarios)
            _density_score = min(1.0, _prem_per_day / 0.10)
            mode_bonus += min(0.15, _density_score * 0.15)

        # 2. Strike safety — OTM cushion (avoid assignment at massive loss)
        if ul_price > 0 and strike > 0:
            _otm_pct_rp = (strike - ul_price) / ul_price if strike > ul_price else 0.0
            # Sweet spot: 5-15% OTM. Below 5% = too tight. Above 20% = too cheap.
            if 0.05 <= _otm_pct_rp <= 0.15:
                mode_bonus += 0.10
            elif _otm_pct_rp > 0.20:
                mode_bonus -= 0.05  # premium too thin at far OTM

        # 3. Basis reduction rate — fraction of gap closed per cycle
        if net_cost_basis > 0 and mid > 0 and net_cost_basis > ul_price:
            _rp_gap = net_cost_basis - ul_price
            _reduction_pct = mid / _rp_gap  # e.g. $0.50 / $11 gap = 4.5%
            mode_bonus += min(0.10, _reduction_pct * 2.0)

    # ── Phase 1b — Earnings-in-window penalty ───────────────────────────────
    # If earnings fall BEFORE this contract's expiry, the new position is forced
    # to hold through the IV event. Natenberg Ch.8: "Post-earnings IV crush destroys
    # extrinsic value regardless of direction — the premium collected for selling the
    # event is surrendered in the gap-down (or gap-up) open."
    # Strategy-conditional: short-vol sellers are structurally exposed (they sold the
    # event = short gamma through the uncertainty). Long-vol buyers are hurt by IV
    # crush but the directional move may compensate.
    # Severity tiers mirror practical trading risk horizons:
    #   ≤7d  : earnings THIS week — almost certain IV event inside this contract
    #   ≤21d : earnings within 3 weeks — high probability of IV crush before expiry
    #   >21d : earnings inside window but not imminent — soft warning
    earnings_multiplier = 1.0
    if days_to_earnings is not None and days_to_earnings >= 0 and days_to_earnings < actual_dte:
        if days_to_earnings <= 7:
            # Earnings this week — structural IV collapse virtually guaranteed inside DTE
            earnings_multiplier = 0.70 if is_short_vol else 0.82
        elif days_to_earnings <= 21:
            # Earnings within 3 weeks — high probability of carry disruption
            earnings_multiplier = 0.80 if is_short_vol else 0.88
        else:
            # Earnings inside DTE but >3 weeks — moderate awareness penalty
            earnings_multiplier = 0.90 if is_short_vol else 0.93

    # ── Dividend assignment penalty ──────────────────────────────────────────
    # McMillan Ch.2: "Call owners exercise early to capture the dividend when
    # remaining extrinsic < dividend — the call is worth more dead than alive."
    # A roll candidate where extrinsic < dividend is assignment bait: the new
    # strike will trigger the same early-exercise incentive that forced this roll.
    # Natenberg Ch.15: "The roll must lift the strike beyond the assignment
    # incentive threshold — extrinsic must exceed dividend amount."
    # Only applies to SHORT CALL strategies (BW, CC, PMCC) — puts don't face
    # dividend early exercise. CSP/SHORT_PUT are excluded.
    _is_short_call = strategy_key in ("BUY_WRITE", "COVERED_CALL", "PMCC")
    dividend_multiplier = 1.0
    if (_is_short_call and dividend_amount > 0
            and days_to_dividend < actual_dte and days_to_dividend < 45):
        # Candidate expires AFTER next ex-div → assignment risk exists
        # Compute extrinsic value at candidate strike
        _intrinsic_call = max(0.0, ul_price - strike)  # call intrinsic (ITM amount)
        _extrinsic = max(0.0, mid - _intrinsic_call)
        if _extrinsic < dividend_amount:
            # Assignment bait: extrinsic doesn't compensate for holding through div
            # Severe penalty — this strike WILL get exercised early
            dividend_multiplier = 0.60
        elif _extrinsic < dividend_amount * 1.5:
            # Marginal: extrinsic barely exceeds dividend — at risk of assignment
            # after any adverse move narrows the gap
            dividend_multiplier = 0.80
        elif _extrinsic < dividend_amount * 2.0 and days_to_dividend <= 10:
            # Near-term div + thin extrinsic cushion → mild caution
            dividend_multiplier = 0.90

    # ── Churn guard — consecutive debit roll penalty ─────────────────────────
    # Passarelli Ch.6: "Each roll pays the market-maker's spread — at some point
    # the position is too damaged to keep rolling."
    # If the last 2+ rolls were all net debits, this position is bleeding basis
    # through roll friction. Escalating penalty discourages a 4th debit roll
    # when the position should probably be closed or assigned.
    # Only fires when current_option_mid > 0 AND this candidate would also be a
    # debit (net_roll_adj <= 0 means no credit bonus was earned → debit or neutral).
    churn_multiplier = 1.0
    if consecutive_debit_rolls >= 2 and is_short_vol and current_option_mid > 0:
        # Check if THIS roll would also be a debit
        _would_be_debit = (net_roll_adj <= 0) if current_option_mid > 0 else False
        if _would_be_debit:
            # This would be ANOTHER debit roll on top of 2+ prior debits
            if consecutive_debit_rolls >= 3:
                churn_multiplier = 0.70  # 3+ consecutive debits → strong penalty
            else:
                churn_multiplier = 0.85  # 2 consecutive debits → moderate penalty

    composite = min(1.0, (base_score + mode_bonus) * earnings_multiplier
                    * dividend_multiplier * churn_multiplier)

    return {
        "composite": round(composite, 4),
        # ── Quality sub-scores (0-1 each) ─────────────────────────────
        "delta_score":    round(delta_score, 4),
        "yield_score":    round(yield_score, 4),
        "dte_score":      round(dte_score, 4),
        "iv_score":       round(iv_score, 4),
        "liq_score":      round(liq_score, 4),
        # ── Economics adjustments ──────────────────────────────────────
        "net_roll_adj":   round(net_roll_adj, 4),
        "recovery_score": round(recovery_score, 4),
        "mode_bonus":     round(mode_bonus, 4),
        # ── Multipliers ───────────────────────────────────────────────
        "earnings_mult":  round(earnings_multiplier, 4),
        "dividend_mult":  round(dividend_multiplier, 4),
        "churn_mult":     round(churn_multiplier, 4),
        # ── Weights applied (after trigger renormalization) ────────────
        "w_delta": round(_w_delta, 4),
        "w_yield": round(_w_yield, 4),
        "w_dte":   round(_w_dte, 4),
        "w_iv":    round(_w_iv, 4),
        "w_liq":   round(_w_liq, 4),
    }


# ── Liquidity grading ─────────────────────────────────────────────────────────

def _grade_liquidity(
    oi: int,
    volume: int,
    spread_pct: float,
    bid_size: int = 0,
    ask_size: int = 0,
) -> str:
    """
    Grade execution liquidity. OI is inventory; bid/ask size is liquidity NOW.

    Sinclair (Volatility Trading): realized PnL depends on execution quality,
    not theoretical value. A wide bid/ask or thin book size means mid fills
    are optimistic — the theoretical credit is not the realized credit.

    bid_size / ask_size: number of contracts at the bid/ask right now.
    - < 5 contracts at bid on a sell leg = fragile fill; cap grade at THIN.
    - ≥ 50 contracts = deep book; mild EXCELLENT boost (already at ceiling for high OI).
    """
    # Base grade from OI + spread (existing logic)
    if oi >= 5000 and spread_pct < 10:
        grade = "EXCELLENT"
    elif oi >= 500 and spread_pct < 3:
        grade = "EXCELLENT"
    elif oi >= 1000 and spread_pct < 10:
        grade = "GOOD"
    elif oi >= 100 and spread_pct < 5:
        grade = "GOOD"
    elif oi >= 25 and spread_pct < 10:
        grade = "ACCEPTABLE"
    elif oi >= 5 and volume >= 100:
        grade = "ACCEPTABLE"
    elif oi >= 5:
        grade = "THIN"
    else:
        return "ILLIQUID"

    # ── bid/ask size downgrade (Sinclair: execution quality beats theoretical value) ──
    # The relevant size is the side you're hitting:
    #   - Opening a short (selling): bid_size matters (you're lifting the bid)
    #   - Closing a long (selling): bid_size matters
    #   - Opening a long (buying):  ask_size matters (you're hitting the ask)
    # We use min(bid_size, ask_size) as a conservative proxy when not known directionally.
    _book_size = min(bid_size, ask_size) if bid_size > 0 and ask_size > 0 else max(bid_size, ask_size)
    _GRADE_ORDER = ["ILLIQUID", "THIN", "ACCEPTABLE", "GOOD", "EXCELLENT"]

    if _book_size > 0:
        if _book_size < 5 and spread_pct > 3:
            # Thin book + wide spread: cap at THIN regardless of OI
            # Rationale: OI is stale inventory; a 3-contract book means 3-lot fills max
            # before price moves. McMillan: "always use limit at mid or better" —
            # but if book is < 5 lots, mid may gap on partial fill.
            _target = "THIN"
            if _GRADE_ORDER.index(grade) > _GRADE_ORDER.index(_target):
                grade = _target
        elif _book_size < 10 and spread_pct > 5:
            # Slightly thin + moderately wide: cap at ACCEPTABLE
            _target = "ACCEPTABLE"
            if _GRADE_ORDER.index(grade) > _GRADE_ORDER.index(_target):
                grade = _target

    return grade


# ── Roll economics vector ─────────────────────────────────────────────────────

def _compute_economics_vector(
    cand: Dict[str, Any],
    cost_to_roll: Dict[str, Any],
    current_row: pd.Series,
    net_cost_basis: float,
    ul_price: float,
    strategy_key: str,
) -> Dict[str, Any]:
    """
    Assemble the full roll economics vector from separated components.

    Each candidate gets a decomposed view of what the roll actually does:
    leg economics, strike change value, time change value, post-roll state.
    This replaces the single-score ranking with a multi-dimensional view
    that lets the trader (and doctrine) see WHY a candidate is good or bad.

    McMillan Ch.3 / Passarelli Ch.6: "A roll is not one number — it is a
    collection of economic changes that must each be evaluated."
    """
    is_short_vol = strategy_key in ("BUY_WRITE", "COVERED_CALL", "SHORT_PUT", "CSP")
    score_vec = cand.get("score_vector", {})

    # ── Leg economics ─────────────────────────────────────────────────
    net_per = float(cost_to_roll.get("net_per_contract") or 0)
    net_total = float(cost_to_roll.get("net_total") or 0)
    cost_type = cost_to_roll.get("type", "unknown")
    contracts = int(cost_to_roll.get("contracts", 1) or 1)

    # Slippage warning: spread > 10% on either leg = real fill risk
    cand_spread = float(cand.get("spread_pct", 0) or 0)
    slippage_warning = cand_spread > 10 or cand.get("liq_grade") in ("THIN", "ILLIQUID")

    # ── Strike change analysis ────────────────────────────────────────
    current_strike = float(cand.get("roll_from_strike", 0) or 0)
    new_strike = float(cand.get("strike", 0) or 0)
    strike_change = round(new_strike - current_strike, 2) if current_strike > 0 else None
    strike_change_pct = (
        round((new_strike - current_strike) / current_strike * 100, 1)
        if current_strike > 0 else None
    )

    # Basis improvement: how much closer to or above basis?
    basis_improvement_pct = None
    if net_cost_basis > 0 and current_strike > 0 and new_strike > 0:
        old_gap = (current_strike - net_cost_basis) / net_cost_basis
        new_gap = (new_strike - net_cost_basis) / net_cost_basis
        basis_improvement_pct = round((new_gap - old_gap) * 100, 2)

    # Assignment risk change
    assignment_risk = "UNCHANGED"
    if is_short_vol and current_strike > 0 and new_strike > 0 and ul_price > 0:
        old_otm_pct = (current_strike - ul_price) / ul_price if is_short_vol else 0
        new_otm_pct = (new_strike - ul_price) / ul_price if is_short_vol else 0
        if new_otm_pct > old_otm_pct + 0.01:
            assignment_risk = "IMPROVED"
        elif new_otm_pct < old_otm_pct - 0.01:
            assignment_risk = "WORSENED"

    # ── Time change analysis ──────────────────────────────────────────
    current_dte = int(cand.get("roll_from_dte", 0) or 0)
    new_dte = int(cand.get("dte", 0) or 0)
    dte_extension = new_dte - current_dte if current_dte > 0 else None

    # Gamma reduction estimate: gamma ∝ 1/√T — extending DTE cuts gamma
    gamma_reduction_pct = None
    if current_dte > 0 and new_dte > 0 and new_dte > current_dte:
        import math
        gamma_reduction_pct = round(
            (1.0 - math.sqrt(current_dte / new_dte)) * 100, 1
        )

    # ── Post-roll state ───────────────────────────────────────────────
    # These are already computed by _compute_trader_metrics and embedded in cand
    new_breakeven = cand.get("breakeven_after_roll")
    new_ann_yield = cand.get("annualized_yield_pct")
    new_otm_pct_val = cand.get("otm_pct")
    prob_expire_otm = cand.get("prob_otm_at_expiry")
    theta_per_day = cand.get("theta_per_day_dollars")

    # ── New breakeven from roll credit/debit ───────────────────────────
    # Net cost basis adjusted for the roll credit/debit
    new_basis_after_roll = None
    if net_cost_basis > 0 and is_short_vol:
        new_basis_after_roll = round(net_cost_basis - net_per, 2)

    return {
        # Leg economics
        "close_cost": round(-net_per + float(cand.get("mid", 0) or 0), 2) if is_short_vol else None,
        "open_proceeds": round(float(cand.get("mid", 0) or 0), 2),
        "net_credit_debit": round(net_per, 2),
        "net_total": round(net_total, 2),
        "cost_type": cost_type,
        "contracts": contracts,
        "slippage_warning": slippage_warning,
        # Strike change
        "strike_change": strike_change,
        "strike_change_pct": strike_change_pct,
        "basis_improvement_pct": basis_improvement_pct,
        "assignment_risk_change": assignment_risk,
        # Time change
        "dte_extension": dte_extension,
        "gamma_reduction_pct": gamma_reduction_pct,
        # Post-roll state
        "new_breakeven": new_breakeven,
        "new_basis_after_roll": new_basis_after_roll,
        "new_annualized_yield_pct": new_ann_yield,
        "new_otm_pct": new_otm_pct_val,
        "prob_expire_otm": prob_expire_otm,
        "theta_per_day": theta_per_day,
        # Score decomposition (from _score_candidate vector)
        "delta_score": score_vec.get("delta_score"),
        "yield_score": score_vec.get("yield_score"),
        "dte_score": score_vec.get("dte_score"),
        "iv_score": score_vec.get("iv_score"),
        "liq_score": score_vec.get("liq_score"),
        "net_roll_adj": score_vec.get("net_roll_adj"),
        "recovery_score": score_vec.get("recovery_score"),
        "earnings_mult": score_vec.get("earnings_mult"),
        "dividend_mult": score_vec.get("dividend_mult"),
        "churn_mult": score_vec.get("churn_mult"),
        "composite_score": score_vec.get("composite"),
    }


def _classify_candidate_edge(econ: Dict[str, Any]) -> Tuple[str, str]:
    """
    Classify the primary economic edge of a roll candidate.

    Returns (edge_label, edge_summary) — human-readable classification of
    what this candidate is best at.

    Edge labels:
      INCOME_EXTENSION    — small change, mostly DTE extension for more theta
      STRIKE_IMPROVEMENT  — meaningful strike lift toward/above basis
      RECOVERY_ROLL       — debit roll justified by strong recovery geometry
      INCOME_CREDIT       — credit roll that adds income + optionality
      DEFENSIVE_ROLL      — assignment risk improvement via strike lift
      ASSIGNMENT_PREFERABLE — roll economics don't justify the cost
      WEAK_LIQUIDITY      — apparent edge may be unrealizable due to poor fills

    McMillan Ch.3: "Name what the roll does for you — if you can't, don't roll."
    """
    net = econ.get("net_credit_debit") or 0
    strike_change = econ.get("strike_change") or 0
    basis_imp = econ.get("basis_improvement_pct") or 0
    dte_ext = econ.get("dte_extension") or 0
    assign_risk = econ.get("assignment_risk_change", "UNCHANGED")
    slippage = econ.get("slippage_warning", False)
    liq_score = econ.get("liq_score") or 0
    composite = econ.get("composite_score") or 0
    cost_type = econ.get("cost_type", "unknown")

    # Check for weak liquidity first — it overrides everything
    if slippage and liq_score < 0.30:
        label = "WEAK_LIQUIDITY"
        summary = f"{'credit' if net >= 0 else 'debit'} on paper, but wide spread/thin book — real fill uncertain"
        return label, summary

    # Classify based on what the candidate primarily offers
    parts = []

    if net >= 0:
        parts.append(f"${abs(net):.2f} {'credit' if net > 0.005 else 'flat'}")
    else:
        parts.append(f"${abs(net):.2f} debit")

    if abs(strike_change) > 0.50:
        parts.append(f"${strike_change:+.0f} strike {'up' if strike_change > 0 else 'down'}")

    if dte_ext and dte_ext > 0:
        parts.append(f"+{dte_ext}d DTE")

    # Determine primary edge
    if composite < 0.25:
        label = "ASSIGNMENT_PREFERABLE"
        summary = "roll economics weak — assignment or close may be better"
    elif basis_imp is not None and basis_imp > 3.0 and net < 0:
        label = "RECOVERY_ROLL"
        summary = ", ".join(parts) + " — debit justified by recovery geometry"
    elif basis_imp is not None and basis_imp > 2.0 and assign_risk == "IMPROVED":
        label = "STRIKE_IMPROVEMENT"
        summary = ", ".join(parts) + " — meaningful recovery room gained"
    elif assign_risk == "IMPROVED" and strike_change > 0 and net >= 0:
        label = "DEFENSIVE_ROLL"
        summary = ", ".join(parts) + " — assignment risk reduced at no cost"
    elif dte_ext is not None and dte_ext > 14 and abs(strike_change) < 1.0:
        label = "INCOME_EXTENSION"
        summary = ", ".join(parts) + " — time extension for more theta"
    elif net > 0.05 and abs(strike_change) < 1.0:
        label = "INCOME_CREDIT"
        summary = ", ".join(parts) + " — adds income with similar positioning"
    elif net > 0:
        label = "INCOME_CREDIT"
        summary = ", ".join(parts)
    else:
        label = "INCOME_EXTENSION"
        summary = ", ".join(parts)

    return label, summary


# ── Split execution suggestion ────────────────────────────────────────────────

def _compute_split_suggestion(
    candidates: List[Dict[str, Any]],
    total_contracts: int,
    strategy_key: str,
    net_cost_basis: float = 0.0,
    ul_price: float = 0.0,
) -> Optional[Dict[str, Any]]:
    """
    For multi-contract positions (qty >= 4), evaluate whether splitting
    execution across different paths produces a better outcome than
    rolling all contracts to a single candidate.

    Passarelli Ch.6: "With size, you can split the roll — capture income on
    half, buy recovery on the other half."

    Returns None if split is not recommended (single-contract, or no
    meaningful benefit), or a dict with the suggestion.
    """
    if total_contracts < 4 or not candidates:
        return None

    is_short_vol = strategy_key in ("BUY_WRITE", "COVERED_CALL", "SHORT_PUT", "CSP")

    # Need at least one candidate with economics
    top = candidates[0]
    top_edge = top.get("primary_edge", "")
    top_econ = top.get("economics", {})
    if not top_econ:
        return None

    # Split ratio: default 50/50 rounded to integer contracts
    half_a = total_contracts // 2
    half_b = total_contracts - half_a

    # ── Strategy 1: Stagger across two candidates ─────────────────────
    # When candidate #1 and #2 have different edge types, splitting
    # across both captures two economic benefits simultaneously.
    # Checked FIRST because diversification across candidates is higher
    # value than single-candidate partial execution.
    if len(candidates) >= 2:
        second = candidates[1]
        sec_edge = second.get("primary_edge", "")
        sec_econ = second.get("economics", {})

        # Only suggest if edges are meaningfully different
        _different_edges = (
            top_edge != sec_edge
            and sec_edge not in ("ASSIGNMENT_PREFERABLE", "WEAK_LIQUIDITY")
            and top_edge not in ("ASSIGNMENT_PREFERABLE", "WEAK_LIQUIDITY")
            and sec_econ
        )

        # Different expirations = staggered expiry benefit
        _different_expiry = (
            top.get("expiry") and second.get("expiry")
            and top.get("expiry") != second.get("expiry")
        )

        if _different_edges and _different_expiry:
            return {
                "type": "STAGGER_EXPIRY",
                "tranche_a_contracts": half_a,
                "tranche_b_contracts": half_b,
                "tranche_a": f"${top.get('strike', '?')} {top.get('expiry', '?')}",
                "tranche_b": f"${second.get('strike', '?')} {second.get('expiry', '?')}",
                "rationale": (
                    f"Stagger: {half_a} contracts to "
                    f"#{1} ({top_edge.replace('_', ' ').lower()}: "
                    f"${top.get('strike', '?')} {top.get('expiry', '?')}), "
                    f"{half_b} to "
                    f"#{2} ({sec_edge.replace('_', ' ').lower()}: "
                    f"${second.get('strike', '?')} {second.get('expiry', '?')}). "
                    f"Two different economic profiles reduce concentration risk."
                ),
                "edge_type": f"{top_edge}+{sec_edge}",
            }
        elif _different_edges and not _different_expiry:
            return {
                "type": "SPLIT_STRIKE",
                "tranche_a_contracts": half_a,
                "tranche_b_contracts": half_b,
                "tranche_a": f"${top.get('strike', '?')} {top.get('expiry', '?')}",
                "tranche_b": f"${second.get('strike', '?')} {second.get('expiry', '?')}",
                "rationale": (
                    f"Split: {half_a} contracts to "
                    f"#{1} ({top_edge.replace('_', ' ').lower()}: "
                    f"${top.get('strike', '?')}), "
                    f"{half_b} to "
                    f"#{2} ({sec_edge.replace('_', ' ').lower()}: "
                    f"${second.get('strike', '?')}). "
                    f"Diversifies strike exposure within same expiration."
                ),
                "edge_type": f"{top_edge}+{sec_edge}",
            }

    # ── Strategy 2: Partial close + partial roll ──────────────────────
    # When assignment is close to preferable but roll still has some value,
    # close half and roll half.
    if top_edge == "ASSIGNMENT_PREFERABLE" and is_short_vol and len(candidates) >= 2:
        second = candidates[1]
        sec_edge = second.get("primary_edge", "")
        if sec_edge not in ("ASSIGNMENT_PREFERABLE", "WEAK_LIQUIDITY"):
            return {
                "type": "PARTIAL_CLOSE_PARTIAL_ROLL",
                "close_contracts": half_a,
                "roll_contracts": half_b,
                "roll_to": f"${second.get('strike', '?')} {second.get('expiry', '?')}",
                "rationale": (
                    f"Close/assign {half_a} contracts (roll economics weak), "
                    f"roll {half_b} to #{2} ({sec_edge.replace('_', ' ').lower()}: "
                    f"${second.get('strike', '?')} {second.get('expiry', '?')}). "
                    f"Reduces position size while maintaining partial exposure."
                ),
                "edge_type": f"CLOSE+{sec_edge}",
            }

    # ── Strategy 3: Roll partial + hold ────────────────────────────────
    # Income credit or extension on a short-vol position: roll half to
    # capture income, hold the other half (let remaining theta decay).
    if is_short_vol and top_edge in ("INCOME_CREDIT", "INCOME_EXTENSION"):
        net = top_econ.get("net_credit_debit", 0) or 0
        credit_total = round(abs(net) * half_a * 100, 2)
        return {
            "type": "ROLL_PARTIAL_HOLD",
            "roll_contracts": half_a,
            "hold_contracts": half_b,
            "edge_type": top_edge,
            "rationale": (
                f"Roll {half_a} contracts for "
                f"${credit_total:.0f} credit, hold {half_b} "
                f"for remaining theta. {top_edge.replace('_', ' ').lower()} "
                f"captured on partial position."
            ),
        }

    # ── Strategy 4: Split debit exposure ─────────────────────────────
    # Recovery roll (debit) with meaningful basis improvement: roll half
    # to limit debit commitment while still improving cost basis.
    if top_edge == "RECOVERY_ROLL":
        basis_imp = top_econ.get("basis_improvement_pct", 0) or 0
        if basis_imp > 2.0:
            net = top_econ.get("net_credit_debit", 0) or 0
            debit_total = round(abs(net) * half_a * 100, 2)
            return {
                "type": "SPLIT_DEBIT_EXPOSURE",
                "roll_contracts": half_a,
                "hold_contracts": half_b,
                "edge_type": top_edge,
                "rationale": (
                    f"Roll {half_a} contracts (reduces debit commitment "
                    f"to ${debit_total:.0f} vs ${round(abs(net) * total_contracts * 100, 2):.0f}). "
                    f"Hold {half_b} — basis improvement {basis_imp:.1f}% "
                    f"captured on half the position."
                ),
            }

    return None


# ── Roll cost ─────────────────────────────────────────────────────────────────

def _estimate_roll_cost(current_row: pd.Series, candidate: Dict[str, Any]) -> Dict[str, Any]:
    """
    Estimate the net cost to roll: close current + open candidate.

    For LONG options:
        Close (sell) current at bid → candidate mid = net debit/credit
    For SHORT options (CC, BW, CSP):
        Close (buy) current at ask → sell candidate at bid = net credit/debit
    """
    try:
        strategy = str(current_row.get("Strategy", "") or "")
        qty      = float(current_row.get("Quantity", 1) or 1)
        is_short = qty < 0

        current_bid = float(current_row.get("Bid", 0) or 0)
        current_ask = float(current_row.get("Ask", 0) or 0)
        current_mid = (current_bid + current_ask) / 2 if current_bid and current_ask else float(current_row.get("Last", 0) or 0)

        cand_mid = float(candidate.get("mid", 0) or 0)
        cand_bid = float(candidate.get("bid", 0) or 0)

        if is_short:
            # Short: buy back at ask (close), sell new at bid (open)
            close_cost = current_ask  # pay to close
            open_proceeds = cand_bid  # receive to open
            net = open_proceeds - close_cost  # positive = net credit
            label = "credit" if net >= 0 else "debit"
        else:
            # Long: sell current at bid (close), buy new at ask (open)
            close_proceeds = current_bid
            open_cost = candidate.get("ask", cand_mid)
            net = close_proceeds - open_cost  # negative = net debit
            label = "debit" if net <= 0 else "credit"

        return {
            "net_per_contract":  round(net, 2),
            "net_total":         round(net * abs(qty) * 100, 2),
            "type":              label,
            "contracts":         int(abs(qty)),
        }
    except Exception:
        return {"net_per_contract": None, "net_total": None, "type": "unknown", "contracts": 1}


# ── Roll rationale ────────────────────────────────────────────────────────────

def _compute_trader_metrics(
    cand: Dict[str, Any],
    cost_to_roll: Dict[str, Any],
    net_cost_basis: float,
    cum_premium: float,
    ul_price: float,
    strategy_key: str,
) -> Dict[str, Any]:
    """
    Compute the numbers a trader actually uses to evaluate a roll candidate.

    McMillan Ch.3: before executing a roll, a trader answers:
      1. What is my new breakeven after paying/receiving the roll cost?
      2. What annualized yield does this new strike provide on my capital?
      3. How far OTM is the new strike — what's the probability of it expiring worthless?
      4. Is the theta efficient — how many $/day will I earn while I wait?

    All of these go into Roll_Candidate_* as structured fields so the dashboard
    and doctrine can surface them without re-computing.
    """
    mid        = float(cand.get("mid", 0) or 0)
    strike     = float(cand.get("strike", 0) or 0)
    dte        = int(cand.get("dte", 1) or 1)
    iv         = float(cand.get("iv", 0) or 0)
    theta      = float(cand.get("theta", 0) or 0)  # per-share per-day (negative)
    delta      = abs(float(cand.get("delta", 0) or 0))

    is_short_vol = strategy_key in ("BUY_WRITE", "COVERED_CALL", "SHORT_PUT", "CSP")
    net_roll     = float(cost_to_roll.get("net_per_contract", 0) or 0)  # positive=credit, neg=debit
    contracts    = max(1, int(cost_to_roll.get("contracts", 1) or 1))

    # ── 1. Breakeven after roll ───────────────────────────────────────────────
    # For BW/CC short call: new breakeven = net_cost_basis + roll_credit (or - roll_debit)
    # net_per_contract is already per-share (computed from bid/ask option prices directly).
    # Positive net_roll = credit received → reduces breakeven
    # Negative net_roll = debit paid    → raises breakeven
    breakeven_after_roll = None
    if net_cost_basis > 0 and is_short_vol:
        breakeven_after_roll = round(net_cost_basis - net_roll, 2)

    # ── 2. Annualized yield on capital ───────────────────────────────────────
    # Passarelli Ch.6: the metric that actually tells you if the roll is worth doing
    from core.shared.finance_utils import annualized_yield as _ann_yield
    annualized_yield_pct = None
    capital = net_cost_basis if net_cost_basis > 0 else ul_price
    if is_short_vol and mid > 0 and capital > 0 and dte > 0:
        annualized_yield_pct = round(_ann_yield(mid, capital, dte) * 100, 1)

    # ── 3. OTM percentage ─────────────────────────────────────────────────────
    # McMillan Ch.3: distance from spot tells you how much the stock can move before
    # the call goes ITM. More OTM = more upside captured, less premium.
    otm_pct = None
    if ul_price > 0 and strike > 0:
        if is_short_vol:
            # CC/BW short call: OTM = how far above spot the strike is
            otm_pct = round(((strike - ul_price) / ul_price) * 100, 1)
        else:
            # Long call: ITM/OTM vs spot
            otm_pct = round(((strike - ul_price) / ul_price) * 100, 1)

    # ── 4. Prob OTM at expiry (delta approximation) ───────────────────────────
    # Natenberg Ch.8: delta ≈ N(d1) ≈ probability of expiring ITM for calls.
    # Prob expiring worthless (OTM at expiry) ≈ 1 - delta for short calls.
    prob_otm_at_expiry = None
    if is_short_vol and delta > 0:
        prob_otm_at_expiry = round((1.0 - delta) * 100, 1)  # % probability expires worthless

    # ── 5. Theta per day ($) ─────────────────────────────────────────────────
    # Passarelli Ch.6: daily theta income tells you what you're "earning" while you wait.
    # Theta from chain is per-share per-day (negative). Multiply by 100 for per-contract.
    theta_per_day_dollars = None
    if theta != 0 and contracts > 0:
        # For short vol: we RECEIVE theta (positive income)
        theta_per_day_dollars = round(abs(theta) * 100 * contracts, 2)

    # ── 6. IV vs assignment risk cross-check ─────────────────────────────────
    # If IV of new candidate is much higher than current, that's a warning:
    # high IV often precedes a vol crush that kills premium quickly after entry.
    # Natenberg Ch.11: don't roll into a vol spike without awareness.
    iv_vs_current = None
    current_iv = float(cand.get("roll_from_iv", 0) or 0)
    if current_iv > 0 and iv > 0:
        iv_change_pct = round(((iv - current_iv) / current_iv) * 100, 1)
        iv_vs_current = iv_change_pct  # positive = new strike has higher IV

    return {
        "breakeven_after_roll":    breakeven_after_roll,
        "annualized_yield_pct":    annualized_yield_pct,
        "otm_pct":                 otm_pct,
        "prob_otm_at_expiry":      prob_otm_at_expiry,
        "theta_per_day_dollars":   theta_per_day_dollars,
        "iv_vs_current_pct":       iv_vs_current,
        "cum_premium_collected":   round(cum_premium, 2) if cum_premium else None,
    }


def _build_roll_rationale(
    candidate: Dict[str, Any],
    strategy_key: str,
    ul_price: float,
    net_cost_basis: float = 0.0,
    roll_mode: str = _ROLL_MODE_NORMAL,
    position_signals: Optional[Dict[str, Any]] = None,
    calendar_note: str = "",
) -> str:
    """
    Trader-quality rationale: the numbers that actually drive the roll decision.

    A real trader evaluating a roll asks in this order:
      1. What am I rolling FROM and TO (strike, expiry, moneyness)?
      2. What does this cost/earn?
      3. What is my new breakeven — can I profit if assigned?
      4. What yield does this generate on my capital?
      5. What is the probability this expires worthless?
      6. How much theta income per day while I hold this?
      7. Is there a vol risk I should know about?

    McMillan Ch.3 / Passarelli Ch.6 / Natenberg Ch.11
    """
    strike   = candidate.get("strike")
    expiry   = candidate.get("expiry", "?")
    dte      = candidate.get("dte", "?")
    delta    = candidate.get("delta")
    iv       = candidate.get("iv")
    liq      = candidate.get("liq_grade", "?")
    oi       = candidate.get("oi", 0)
    cost     = candidate.get("cost_to_roll", {})
    net      = cost.get("net_per_contract")
    ctype    = cost.get("type", "")
    contracts = cost.get("contracts", 1)

    # Derived trader metrics (computed by _compute_trader_metrics)
    be_after   = candidate.get("breakeven_after_roll")
    yield_pct  = candidate.get("annualized_yield_pct")
    prob_otm   = candidate.get("prob_otm_at_expiry")
    theta_day  = candidate.get("theta_per_day_dollars")
    iv_chg     = candidate.get("iv_vs_current_pct")
    otm_pct    = candidate.get("otm_pct")

    is_short_vol = strategy_key in ("BUY_WRITE", "COVERED_CALL", "SHORT_PUT", "CSP")

    # ── Moneyness label ───────────────────────────────────────────────────────
    moneyness = ""
    if strike is not None and ul_price > 0:
        pct = (strike - ul_price) / ul_price * 100
        if abs(pct) < 0.5:
            moneyness = "ATM"
        elif pct > 0:
            moneyness = f"{pct:+.1f}% OTM"
        else:
            moneyness = f"{abs(pct):.1f}% ITM"

    # ── Cost string ───────────────────────────────────────────────────────────
    # Use pre-computed net_total (net_per_contract × contracts × 100) from cost_to_roll dict.
    # Do NOT recompute here — net_per_contract × contracts misses the ×100 multiplier.
    if net is not None:
        total_net = cost.get("net_total")  # already includes ×100
        total_str = f"${abs(total_net):.0f}" if total_net is not None else f"${abs(net * contracts * 100):.0f}"
        if ctype == "credit":
            cost_str = f"Net credit ${abs(net):.2f}/share ({total_str} total received)"
        else:
            cost_str = f"Net debit ${abs(net):.2f}/share ({total_str} total paid)"
    else:
        cost_str = "execution cost TBD"

    # ── Format key metrics ────────────────────────────────────────────────────
    iv_str      = f"{iv:.0%}" if iv else "?"
    delta_str   = f"{delta:.3f}" if delta is not None else "?"
    be_str      = f"${be_after:.2f}" if be_after is not None else "?"
    yield_str   = f"{yield_pct:.1f}%/yr" if yield_pct is not None else "?"
    prob_str    = f"{prob_otm:.0f}%" if prob_otm is not None else "?"
    theta_str   = f"${theta_day:.2f}/day" if theta_day is not None else "?"
    cp_label    = "C" if "CALL" in strategy_key or strategy_key in ("BUY_WRITE", "COVERED_CALL") else "P"

    # ── Vol risk warning ──────────────────────────────────────────────────────
    iv_warning = ""
    if iv_chg is not None and is_short_vol and iv_chg > 20:
        iv_warning = (
            f" ⚠️ IV on candidate is {iv_chg:+.0f}% vs current — "
            f"elevated IV may crush quickly after entry (Natenberg Ch.11: vol risk)."
        )
    elif iv_chg is not None and not is_short_vol and iv_chg < -20:
        iv_warning = (
            f" ⚠️ IV on candidate is {iv_chg:+.0f}% vs current — "
            f"buying compressed vol may not recover in time."
        )

    # ── Breakeven context for BW/CC ───────────────────────────────────────────
    be_context = ""
    if be_after is not None and strike is not None and is_short_vol:
        if strike >= be_after:
            be_context = f" If assigned at ${strike:.0f}: +${(strike - be_after):.2f}/share profit."
        else:
            be_context = (
                f" ⚠️ If assigned at ${strike:.0f}: −${(be_after - strike):.2f}/share loss vs net basis. "
                f"Consider rolling higher."
            )

    # ── Margin carry context ───────────────────────────────────────────────────
    margin_note = ""
    if yield_pct is not None and is_short_vol:
        margin_rate_pct = FIDELITY_MARGIN_RATE * 100
        if yield_pct < margin_rate_pct:
            margin_note = (
                f" ⚠️ Yield {yield_pct:.1f}%/yr BELOW Fidelity margin 10.375%/yr — "
                f"negative carry, this roll loses ${(margin_rate_pct - yield_pct):.1f}%/yr on financing."
            )
        else:
            carry_cover = yield_pct / margin_rate_pct
            margin_note = f" Covers margin {carry_cover:.1f}× (10.375%/yr Fidelity rate)."

    # ── Roll mode prefix ──────────────────────────────────────────────────────
    _mode_prefix = ""
    if roll_mode == _ROLL_MODE_WEEKLY:
        _mode_prefix = "🟡 [WEEKLY CYCLE — fragile position] "
    elif roll_mode == _ROLL_MODE_PRE_ITM:
        _mode_prefix = "⚠️ [PRE-ITM WINDOW] "
    elif roll_mode == _ROLL_MODE_EMERGENCY:
        _emg_strike = candidate.get("strike", 0) or 0
        _basis_gap  = (_emg_strike - net_cost_basis) if net_cost_basis > 0 and _emg_strike > 0 else None
        _emg_basis  = (
            f"Strike ${_emg_strike:.2f} is ${_basis_gap:.2f} ABOVE net cost ${net_cost_basis:.2f}. "
            if _basis_gap is not None and _basis_gap > 0
            else ""
        )
        _mode_prefix = f"🚨 [EMERGENCY — extended DTE search] {_emg_basis}"

    elif roll_mode == _ROLL_MODE_RECOVERY_PREMIUM:
        _rp_gap = net_cost_basis - ul_price if net_cost_basis > 0 else 0
        _rp_gap_str = f"Gap to basis: ${_rp_gap:.2f}/share. " if _rp_gap > 0 else ""
        _rp_mid = float(candidate.get("mid", 0) or 0)
        _rp_cycles = round(_rp_gap / _rp_mid, 1) if _rp_mid > 0 and _rp_gap > 0 else "?"
        _mode_prefix = (
            f"💰 [RECOVERY PREMIUM — basis reduction cycle] {_rp_gap_str}"
            f"~{_rp_cycles} cycles at this premium to close gap. "
        )

    # ── Debit roll judgment (BUY_WRITE / COVERED_CALL only) ──────────────────
    # When the roll requires a net debit, the engine answers three questions
    # that determine whether the debit is justified (McMillan Ch.3).
    #
    # Weighted scoring (mirrors professional hierarchy):
    #   Trend strength  40% — ADX is the most durable signal. A confirmed trend
    #                         is the primary justification for paying a debit.
    #                         McMillan Ch.3: "roll the debit only in a confirmed trend."
    #   Momentum        30% — Near-term continuation (ROC, slope, velocity state).
    #                         More reactive than ADX but confirms short-term thesis.
    #   IV fairness     30% — Cost check: is the buy-back / new premium fairly priced?
    #                         Does not affect the thesis, only the cost of the roll.
    #
    # Score interpretation:
    #   ≥ 0.70  → ✅ Justified     — thesis intact, cost fair
    #   0.40–0.69 → ⚠️ Marginal   — proceed only if directional conviction is high
    #   < 0.40  → ❌ Questionable  — consider letting expire and re-selling next cycle
    _debit_judgment = ""
    if ctype == "debit" and net is not None and position_signals:
        _adx        = float(position_signals.get("adx_14") or 0)
        _roc20      = float(position_signals.get("roc_20") or 0)
        _mom_slope  = float(position_signals.get("momentum_slope") or 0)
        _mom_raw    = position_signals.get("MomentumVelocity_State") or ""
        _mom_state  = (getattr(_mom_raw, 'value', None) or str(_mom_raw).split('.')[-1]).upper()
        _iv_hv_gap  = float(position_signals.get("iv_vs_hv_gap") or 0)
        _drift_dir  = str(position_signals.get("drift_direction") or "").upper()
        _drift_mag  = str(position_signals.get("drift_magnitude") or "").upper()

        # Q1 (40%): Trend strength — ADX is the anchor signal.
        # ADX ≥ 25: confirmed trend (full score). 18–24: developing (partial). <18: choppy (zero).
        # McMillan Ch.3 / Natenberg Ch.8: ADX > 25 = directional conviction confirmed.
        if _adx >= 25:
            _q1_score = 1.0
            _q1_label = f"ADX={_adx:.0f} ✅ confirmed trend"
        elif _adx >= 18:
            _q1_score = 0.5
            _q1_label = f"ADX={_adx:.0f} ⚠️ developing trend"
        else:
            _q1_score = 0.0
            _q1_label = f"ADX={_adx:.0f} ❌ no trend / choppy"

        # Q2 (30%): Upside price momentum — ROC20 + velocity state + slope.
        # Uses PRICE momentum signals only. Drift_Direction/Magnitude are Greek drift
        # (delta ROC) — not stock price direction — and must NOT be used here.
        # roc_20 is stored as a raw percentage (e.g. 6.18 = 6.18%), not decimal.
        _roc20_str  = f"{_roc20:+.1f}%"
        _q2_roc_ok  = _roc20 > 2.0   # >+2% price ROC (not Greek ROC)
        _q2_mom_ok  = any(s in _mom_state for s in ("STRONG", "BULLISH", "TRENDING"))
        _q2_slp_ok  = _mom_slope > 0
        if _q2_roc_ok and (_q2_mom_ok or _q2_slp_ok):
            _q2_score = 1.0
            _q2_label = f"ROC20={_roc20_str} ✅ positive momentum"
        elif _q2_roc_ok or _q2_slp_ok:
            _q2_score = 0.5
            _q2_label = f"ROC20={_roc20_str} ⚠️ weak momentum"
        else:
            _q2_score = 0.0
            _q2_label = f"ROC20={_roc20_str} ❌ negative / no momentum"

        # Q3 (30%): IV fairness — is the cost of the roll reasonable?
        # IV < HV (gap < 0): buying back the short at fair/cheap vol. Full score.
        # IV ≈ HV (gap 0–5pt): acceptable. Partial.
        # IV > HV + 5pt: buying back expensive optionality. Score zero.
        if _iv_hv_gap <= 0:
            _q3_score = 1.0
            _q3_label = f"IV-HV={_iv_hv_gap:+.1f}pt ✅ IV cheap/fair"
        elif _iv_hv_gap <= 5.0:
            _q3_score = 0.5
            _q3_label = f"IV-HV={_iv_hv_gap:+.1f}pt ⚠️ IV slightly elevated"
        else:
            _q3_score = 0.0
            _q3_label = f"IV-HV={_iv_hv_gap:+.1f}pt ❌ IV rich — buy-back expensive"

        # Weighted composite score
        _score = round(0.40 * _q1_score + 0.30 * _q2_score + 0.30 * _q3_score, 2)

        # Hard veto: REVERSING momentum state caps verdict at Marginal regardless of score.
        # McMillan Ch.3: "do not pay a debit into a reversal — wait for momentum to stabilise."
        _mom_reversing = any(s in _mom_state for s in ("REVERSING", "LATE_CYCLE", "EXHAUSTED"))
        if _score >= 0.70 and _mom_reversing:
            _verdict = (
                f"⚠️ Marginal (score {_score:.0%}, capped) — "
                f"momentum state={_mom_state.title()} vetoes Justified; "
                "wait for reversal to stabilise before paying debit"
            )
        elif _score >= 0.70:
            _verdict = f"✅ Justified (score {_score:.0%}) — thesis intact, debit defensible"
        elif _score >= 0.40:
            _verdict = f"⚠️ Marginal (score {_score:.0%}) — proceed only with strong directional conviction"
        else:
            _verdict = f"❌ Questionable (score {_score:.0%}) — consider letting expire, re-sell next cycle"

        _debit_judgment = (
            f" | Debit roll judgment: {_verdict}. "
            f"Trend 40%: {_q1_label}. "
            f"Momentum 30%: {_q2_label}. "
            f"IV 30%: {_q3_label}. "
            f"(McMillan Ch.3: pay debit only when bullish thesis confirmed)"
        )

    # ── Assemble rationale by strategy type ──────────────────────────────────
    if strategy_key in ("BUY_WRITE", "COVERED_CALL"):
        return (
            f"{_mode_prefix}Roll to {strike}{cp_label} {expiry} ({dte}d, {moneyness}) | "
            f"δ={delta_str} IV={iv_str} OI={oi:,} liq={liq} | "
            f"{cost_str} | "
            f"Yield: {yield_str} on capital{margin_note} · θ={theta_str} income · "
            f"{prob_str} prob expires worthless · "
            f"New breakeven: {be_str}.{be_context}{iv_warning}"
            f"{_debit_judgment} "
            f"(McMillan Ch.3 / Passarelli Ch.6)"
            f"{calendar_note}"
        )
    elif strategy_key in ("SHORT_PUT", "CSP"):
        return (
            f"Roll to {strike}{cp_label} {expiry} ({dte}d, {moneyness}) | "
            f"δ={delta_str} IV={iv_str} OI={oi:,} liq={liq} | "
            f"{cost_str} | "
            f"Yield: {yield_str}{margin_note} · θ={theta_str}/day · "
            f"{prob_str} prob expires worthless · "
            f"Assignment price: ${strike:.2f}.{iv_warning} "
            f"(McMillan Ch.6 / Passarelli Ch.6)"
            f"{calendar_note}"
        )
    elif strategy_key in ("LONG_CALL", "LONG_PUT", "LEAP"):
        # Time value % of mid — what fraction of the premium you're paying is pure extrinsic
        _mid = candidate.get("mid", 0) or 0
        _intrinsic = max(0.0, (ul_price - strike) if cp_label == "C" else (strike - ul_price)) if strike else 0.0
        _extrinsic = max(0.0, _mid - _intrinsic)
        _tv_pct_str = f"{_extrinsic/_mid:.0%} extrinsic" if _mid > 0 else "extrinsic: ?"

        # IV vs HV edge — complete sentence (Natenberg Ch.11: buy cheap vol when IV < HV)
        _hv = candidate.get("hv_20d")
        if iv and _hv and _hv > 0:
            _iv_hv_ratio = iv / _hv
            if _iv_hv_ratio < 0.85:
                _iv_hv_str = f"IV={iv_str} vs HV={_hv:.0%} — cheap vol edge (IV/HV={_iv_hv_ratio:.2f})"
            elif _iv_hv_ratio > 1.20:
                _iv_hv_str = f"IV={iv_str} vs HV={_hv:.0%} — buying rich vol (IV/HV={_iv_hv_ratio:.2f}) ⚠️"
            else:
                _iv_hv_str = f"IV={iv_str} vs HV={_hv:.0%} — fair vol (IV/HV={_iv_hv_ratio:.2f})"
        elif iv:
            _iv_hv_str = f"IV={iv_str} (HV unavailable)"
        else:
            _iv_hv_str = "IV unavailable"

        return (
            f"Roll to {strike}{cp_label} {expiry} ({dte}d, {moneyness}) | "
            f"δ={delta_str} IV={iv_str} OI={oi:,} liq={liq} | "
            f"{cost_str} | "
            f"{_tv_pct_str} · θ cost={theta_str} · "
            f"{_iv_hv_str}.{iv_warning} "
            f"(McMillan Ch.4 / Natenberg Ch.11)"
            f"{calendar_note}"
        )
    else:
        return (
            f"Roll to {strike} {expiry} ({dte}d, {moneyness}) | "
            f"δ={delta_str} IV={iv_str} liq={liq} | "
            f"{cost_str}.{iv_warning}"
            f"{calendar_note}"
        )


# ── Roll viability assessment ─────────────────────────────────────────────────

def _assess_roll_viability(
    ul_price: float,
    net_cost_basis: float,
    current_strike: float,
    current_delta: float,
    strategy_key: str,
) -> Dict[str, Any]:
    """
    Called when EMERGENCY mode search finds zero viable above-basis candidates.

    Returns a structured verdict dict that Roll_Candidate_1 is set to, so
    the dashboard and checklist can surface an explicit "no viable roll" message
    rather than silently showing no candidates.

    McMillan Ch.3: "If the only rolls available are below your cost basis, you
    face a binary choice: pay a debit to buy time (trading dollars for days), or
    accept assignment and redeploy capital. Only accept the debit if your thesis
    remains intact and the stock is likely to recover above your basis before
    the next expiry."

    Verdict logic:
      - ASSIGNMENT_PREFERABLE : stock price > net_cost_basis → assignment is profitable
      - DEBIT_ROLL_EVALUATE   : stock price < net_cost_basis → assignment locks in loss;
                                evaluate debit roll vs holding stock naked
      - NO_DATA               : insufficient basis data to evaluate
    """
    verdict = "NO_DATA"
    rationale = "Insufficient cost basis data to evaluate. Run pipeline to populate."

    if net_cost_basis > 0 and ul_price > 0:
        loss_at_assignment = net_cost_basis - current_strike  # positive = loss
        if current_strike >= net_cost_basis:
            # Assignment at current strike is already profitable — just take it
            profit = current_strike - net_cost_basis
            verdict   = "ASSIGNMENT_PREFERABLE"
            rationale = (
                f"No above-basis credit roll found in 45–150 DTE range. "
                f"Assignment at ${current_strike:.2f} is PROFITABLE (+${profit:.2f}/share above net cost ${net_cost_basis:.2f}). "
                f"Accept assignment and redeploy capital. "
                f"(McMillan Ch.3: when assignment is profitable, no need to roll)"
            )
        else:
            # Assignment at current strike locks in a loss
            loss = net_cost_basis - current_strike
            stock_recovery_needed = net_cost_basis - ul_price
            verdict   = "DEBIT_ROLL_EVALUATE"
            rationale = (
                f"No viable above-basis roll found in 45–150 DTE extended search. "
                f"Assignment at ${current_strike:.2f} would realize −${loss:.2f}/share loss "
                f"vs net cost ${net_cost_basis:.2f}. "
                f"Stock at ${ul_price:.2f} needs +${stock_recovery_needed:.2f}/share (+{stock_recovery_needed/ul_price:.0%}) "
                f"to reach breakeven. "
                f"Options: (1) Accept assignment and harvest tax loss; "
                f"(2) Pay debit to roll 90+ DTE and hold for recovery — only if thesis intact; "
                f"(3) Buy back call now and hold stock naked if strongly bullish. "
                f"(McMillan Ch.3: no economically rational covered call roll exists at this time)"
            )

    return {
        "verdict":              verdict,
        "roll_mode":            _ROLL_MODE_EMERGENCY,
        "no_viable_roll":       True,
        "current_strike":       round(current_strike, 2),
        "net_cost_basis":       round(net_cost_basis, 2) if net_cost_basis else None,
        "ul_price":             round(ul_price, 2),
        "current_delta":        round(current_delta, 3),
        "roll_rationale":       rationale,
        "score":                0.0,
    }


# ── Strategy key resolution ───────────────────────────────────────────────────

def _strategy_key(strategy: str, dte: float) -> str:
    """Map df Strategy column to our config keys."""
    s = (strategy or "").upper()
    if "BUY_WRITE" in s:
        return "BUY_WRITE"
    if "COVERED_CALL" in s:
        return "COVERED_CALL"
    if "SHORT_PUT" in s or "CSP" in s:
        return "SHORT_PUT"
    if "LONG_CALL" in s or "LONG_PUT" in s:
        return "LEAP" if dte >= 270 else ("LONG_CALL" if "CALL" in s else "LONG_PUT")
    return "LONG_CALL"


# ── Chain fetch ───────────────────────────────────────────────────────────────

def _get_chain(ticker: str, schwab_client, session_cache: dict) -> Optional[dict]:
    """Fetch chain with session cache. Reuses LiveGreeksProvider cache if available."""
    if ticker in session_cache:
        return session_cache[ticker]
    try:
        schwab_client.ensure_valid_token()
        chain = schwab_client.get_chains(
            symbol     = ticker,
            strikeCount= 30,      # Wider range for roll candidates
            range      = "ALL",
            strategy   = "SINGLE",
        )
        if chain:
            session_cache[ticker] = chain
        return chain
    except Exception as e:
        logger.warning(f"[RollEngine] Chain fetch failed for {ticker}: {e}")
        return None


# ── Roll timing intelligence ───────────────────────────────────────────────────

def _get_roll_timing(row: pd.Series) -> str:
    """
    Classify current market timing for roll candidate selection.
    Returns: "BREAKOUT_UP" | "BREAKOUT_DOWN" | "CHOPPY" | "RELEASING" | "NEUTRAL"

    This is a simplified version of DoctrineAuthority._classify_roll_timing(),
    reimplemented here to avoid a circular import. Uses the same signal columns.
    """
    chop     = float(row.get('choppiness_index', 50) or 50)
    ker      = float(row.get('kaufman_efficiency_ratio', 0.5) or 0.5)
    adx      = float(row.get('adx_14', 25) or 25)
    roc_5    = float(row.get('roc_5', 0) or 0)
    roc_10   = float(row.get('roc_10', 0) or 0)
    bb_z     = float(row.get('bb_width_z', 0) or 0)

    def _sn(col):
        v = row.get(col, '') or ''
        return (getattr(v, 'value', None) or str(v).split('.')[-1]).upper()

    range_eff = _sn('RangeEfficiency_State')
    trend_int = _sn('TrendIntegrity_State')
    mom_vel   = _sn('MomentumVelocity_State')
    dir_bal   = _sn('DirectionalBalance_State')
    comp_mat  = _sn('CompressionMaturity_State')

    if (trend_int == 'STRONG_TREND' and dir_bal == 'BUYER_DOMINANT'
            and mom_vel in ('ACCELERATING', 'TRENDING') and ker > 0.55
            and roc_5 > 2.0 and chop < 50):
        return "BREAKOUT_UP"

    if (dir_bal == 'SELLER_DOMINANT' and mom_vel in ('ACCELERATING', 'TRENDING')
            and roc_5 < -2.0 and roc_10 < -4.0 and chop < 55):
        return "BREAKOUT_DOWN"

    if (chop > 61.8 and ker < 0.35
            and range_eff in ('INEFFICIENT_RANGE', 'NOISY')
            and trend_int in ('NO_TREND', 'TREND_EXHAUSTED')
            and adx < 20 and abs(roc_5) < 2.0):
        return "CHOPPY"

    if (comp_mat in ('RELEASING', 'POST_EXPANSION') and bb_z > 0.5 and adx < 25):
        return "RELEASING"

    return "NEUTRAL"


def _adjust_for_timing(
    timing: str,
    strategy_key: str,
    dte_window: Tuple[int, int, int],
    delta_range: Tuple[float, float],
    ul_price: float,
    current_strike: float,
) -> Tuple[Tuple[int, int, int], Tuple[float, float]]:
    """
    Adjust roll search parameters based on market timing.

    McMillan Ch.3 breakout adjustments:
      BREAKOUT_UP  → roll further OTM (higher strike), further DTE.
                     Give the stock room to run; collect more time premium.
                     Use OTM buffer of ~5-8% from spot.
      BREAKOUT_DOWN → roll closer-in (shorter DTE ok, lower debit).
                     Cheap defensive roll — minimize cost, don't over-reach.
      CHOPPY       → widen DTE slightly (wait for better strike to become available)
                     but tighten delta to avoid paying for strikes that whipsaw.
      RELEASING    → neutral parameters — breakout not yet confirmed.
      NEUTRAL      → no adjustment, use defaults.

    Returns adjusted (dte_window, delta_range).
    """
    min_dte, max_dte, target_dte = dte_window
    delta_min, delta_max = delta_range
    is_short_vol = strategy_key in ("BUY_WRITE", "COVERED_CALL", "SHORT_PUT", "CSP")

    if timing == "BREAKOUT_UP" and is_short_vol:
        # Roll further OTM (lower delta = more room for stock to move)
        # Roll further in time (more premium, more room to be wrong on timing)
        return (
            (min_dte, max_dte + 30, target_dte + 15),   # extend DTE window
            (max(0.10, delta_min - 0.08), max(0.30, delta_max - 0.08)),  # shift OTM
        )

    if timing == "BREAKOUT_DOWN" and is_short_vol:
        # Defensive: shorter DTE is fine (cheaper to close current),
        # slightly tighten delta (don't want too far OTM if stock keeps dropping)
        return (
            (min_dte, min(max_dte, target_dte + 15), target_dte),  # tighter window
            (delta_min, min(delta_max, 0.35)),  # moderate delta cap
        )

    if timing == "CHOPPY" and is_short_vol:
        # Extend DTE slightly — wait for better strikes to become meaningful.
        # Tighten delta lower bound to avoid thin OTM strikes that whipsaw.
        return (
            (min_dte + 7, max_dte + 14, target_dte + 7),
            (max(0.15, delta_min), min(0.35, delta_max)),  # tighter range
        )

    # RELEASING, NEUTRAL, long vol strategies — use defaults unchanged
    return dte_window, delta_range
