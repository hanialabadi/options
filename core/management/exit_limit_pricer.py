"""
Exit Limit Pricer — Phase 1: Daily Technical Levels
====================================================
Suggests limit prices for EXIT actions using daily technical levels
(EMA9, SMA20, BB) and delta approximation.  No intraday data needed.

Public API
----------
    compute_exit_limit_prices(df) -> df   # mutates EXIT rows only

Columns written:
    Exit_Limit_Price          float   — suggested limit price for the option
    Exit_Limit_Level          str     — which stock level drives the limit (e.g. "SMA20")
    Exit_Limit_Rationale      str     — human-readable explanation
    Exit_Limit_Patience_Days  int     — how many days to wait before going market
"""

from __future__ import annotations

import logging
from typing import Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ─── Direction classification ───────────────────────────────────────────────

# RALLY = we are SELLING a call-like option → want stock UP for better fill
# DIP   = we are BUYING BACK a short call or SELLING a put → want stock DOWN
# SKIP  = no clear directional preference

_RALLY_STRATEGIES = frozenset({
    "LONG_CALL", "BUY_CALL", "LEAPS_CALL",
    "CSP",       # buying back short put is cheaper when stock rallies
})

_DIP_STRATEGIES = frozenset({
    "LONG_PUT", "BUY_PUT", "LEAPS_PUT",
    "BUY_WRITE", "COVERED_CALL",  # buying back short call is cheaper on dips
})

_SKIP_STRATEGIES = frozenset({
    "STOCK_ONLY", "STOCK_ONLY_IDLE", "STRADDLE", "STRANGLE",
})


def _classify_exit_direction(strategy: str, call_put: str) -> str:
    """Return 'RALLY', 'DIP', or 'SKIP' for the given strategy."""
    s = str(strategy or "").upper().strip()
    if s in _RALLY_STRATEGIES:
        return "RALLY"
    if s in _DIP_STRATEGIES:
        return "DIP"
    if s in _SKIP_STRATEGIES:
        return "SKIP"
    # Fallback: infer from Call/Put if strategy is unrecognised
    cp = str(call_put or "").upper().strip()
    if cp in ("CALL", "C"):
        return "RALLY"
    if cp in ("PUT", "P"):
        return "DIP"
    return "SKIP"


# ─── Favorable level selection ──────────────────────────────────────────────

def _select_favorable_level(
    ul_last: float,
    ema9: float,
    sma20: float,
    sma50: float,
    lower_band: float,
    upper_band: float,
    direction: str,
) -> Tuple[float, str]:
    """
    Pick the nearest technically significant stock level in the favorable
    direction.  Returns (target_stock_price, level_label).

    RALLY → nearest level ABOVE current (EMA9 preferred, then SMA20, then UpperBand)
    DIP   → nearest level BELOW current (EMA9 preferred, then SMA20, then LowerBand)
    """
    if direction == "RALLY":
        candidates = []
        if ema9 > 0 and ema9 > ul_last:
            candidates.append((ema9, "EMA9"))
        if sma20 > 0 and sma20 > ul_last:
            candidates.append((sma20, "SMA20"))
        if upper_band > 0 and upper_band > ul_last:
            candidates.append((upper_band, "UpperBB"))
        # Pick the nearest (smallest distance above current)
        if candidates:
            candidates.sort(key=lambda c: c[0] - ul_last)
            return candidates[0]

    elif direction == "DIP":
        candidates = []
        if ema9 > 0 and ema9 < ul_last:
            candidates.append((ema9, "EMA9"))
        if sma20 > 0 and sma20 < ul_last:
            candidates.append((sma20, "SMA20"))
        if lower_band > 0 and lower_band < ul_last:
            candidates.append((lower_band, "LowerBB"))
        # Pick the nearest (smallest distance below current)
        if candidates:
            candidates.sort(key=lambda c: ul_last - c[0])
            return candidates[0]

    # Fallback: no favorable level found → use current price
    return (ul_last, "Current")


# ─── Theta-to-Move Ratio ──────────────────────────────────────────────────────

_THETA_MOVE_CRITICAL = 0.8  # ratio above this → patience = 0 (theta dominates)

# Short-option strategies where theta works FOR the seller.
# High theta/move is beneficial → do NOT override patience to 0.
_SHORT_OPTION_STRATEGIES = frozenset({"BUY_WRITE", "COVERED_CALL"})


def _compute_theta_to_move_ratio(
    theta: float,
    delta: float,
    atr: float,
) -> float:
    """
    Ratio of daily theta bleed to expected daily P&L from a typical stock move.

        theta_to_move_ratio = |Theta × 100| / (ATR_14 × |Delta| × 100)

    < 0.3: theta negligible vs move → patience OK
    0.3–0.8: theta meaningful → standard urgency
    > 0.8: theta dominates → override patience to 0
    """
    if not (atr > 0 and abs(delta) > 0):
        return np.nan
    return abs(theta * 100.0) / (atr * abs(delta) * 100.0)


# ─── Patience days ──────────────────────────────────────────────────────────

_URGENCY_PATIENCE = {
    "CRITICAL": 0,
    "HIGH": 1,
    "MEDIUM": 2,
    "LOW": 3,
}


def _compute_patience_days(
    urgency: str,
    dte: float,
    momentum_state: str,
    direction: str,
    theta_to_move_ratio: float = np.nan,
    is_short_option: bool = False,
) -> int:
    """
    How many days to hold the limit order before falling back to market.

    Overrides:
    - DTE ≤ 3 → 0 (gamma risk, must exit immediately)
    - Momentum REVERSING against direction → 0
    - theta_to_move_ratio > 0.8 AND long option → 0 (theta eating position)
    - theta_to_move_ratio > 0.8 AND short option → +1 patience (theta works for seller)
    """
    base = _URGENCY_PATIENCE.get(str(urgency or "").upper().strip(), 2)

    # DTE override: gamma risk
    if pd.notna(dte) and dte <= 3:
        return 0

    # Momentum override: if momentum is reversing against our favorable direction,
    # the level may never be reached — go market now.
    mom = str(momentum_state or "").upper().strip()
    if mom == "REVERSING":
        return 0

    # Theta override (direction-aware):
    if pd.notna(theta_to_move_ratio) and theta_to_move_ratio > _THETA_MOVE_CRITICAL:
        if is_short_option:
            # Short option: high theta works FOR the seller.
            # Wait and let theta decay reduce buyback cost. Add +1 patience.
            return min(base + 1, 3)  # cap at 3 days
        else:
            # Long option: theta eating position faster than stock can move → go now.
            return 0

    return base


# ─── Main entry point ───────────────────────────────────────────────────────

def compute_exit_limit_prices(df: pd.DataFrame) -> pd.DataFrame:
    """
    For every EXIT row (and ROLL rows on BUY_WRITE/COVERED_CALL), compute a
    suggested limit price for the buyback leg using:
    1. Direction classification (RALLY/DIP/SKIP)
    2. Nearest favorable daily technical level
    3. Delta approximation of option price at that level
    4. Patience days based on urgency + DTE

    Non-target rows are untouched (columns set to NaN/"").
    """
    # Ensure output columns exist
    for col in ("Exit_Limit_Price", "Exit_Limit_Patience_Days"):
        if col not in df.columns:
            df[col] = np.nan
    for col in ("Exit_Limit_Level", "Exit_Limit_Rationale"):
        if col not in df.columns:
            df[col] = ""

    action_col = df.get("Action", pd.Series(dtype=str))
    strategy_col = df.get("Strategy", pd.Series(dtype=str)).fillna("").str.upper().str.strip()

    # EXIT rows: all strategies
    exit_mask = action_col == "EXIT"
    # ROLL rows: only BUY_WRITE/COVERED_CALL (close-leg is a buyback)
    roll_bw_cc_mask = (action_col.isin(("ROLL", "ROLL_WAIT"))) & (strategy_col.isin(_SHORT_OPTION_STRATEGIES))
    target_mask = exit_mask | roll_bw_cc_mask

    if not target_mask.any():
        return df

    for idx in df.index[target_mask]:
        row = df.loc[idx]
        strategy = str(row.get("Strategy", "") or "")
        call_put = str(row.get("Call/Put", "") or "")

        direction = _classify_exit_direction(strategy, call_put)
        if direction == "SKIP":
            df.at[idx, "Exit_Limit_Level"] = "SKIP"
            df.at[idx, "Exit_Limit_Rationale"] = (
                f"{strategy}: no directional preference — use market price"
            )
            df.at[idx, "Exit_Limit_Patience_Days"] = 0
            continue

        # Read stock price and technical levels
        ul_last = float(row.get("UL Last") or 0)
        if ul_last <= 0:
            continue  # no stock price → can't compute

        ema9 = float(row.get("EMA9") or 0)
        sma20 = float(row.get("SMA20") or 0)
        sma50 = float(row.get("SMA50") or 0)
        lower_band = float(row.get("LowerBand_20") or 0)
        upper_band = float(row.get("UpperBand_20") or 0)

        target_stock, level_label = _select_favorable_level(
            ul_last, ema9, sma20, sma50, lower_band, upper_band, direction,
        )

        # Read option data — for BW/CC use short call fields
        is_bw_cc = strategy.upper() in ("BUY_WRITE", "COVERED_CALL")
        if is_bw_cc:
            option_price = abs(float(row.get("Short_Call_Last") or row.get("Last") or 0))
            delta = abs(float(row.get("Short_Call_Delta") or row.get("Delta") or 0))
            bid = abs(float(row.get("Bid") or 0))  # short side: bid = what we pay to buy back
        else:
            option_price = abs(float(row.get("Last") or 0))
            delta = abs(float(row.get("Delta") or 0))
            bid = abs(float(row.get("Bid") or 0))

        if option_price <= 0 or delta <= 0:
            # No option data → can't compute limit
            continue

        # Delta approximation: estimate option price at target stock level
        stock_move = target_stock - ul_last  # signed

        if direction == "RALLY":
            # Selling a call (or buying back short put on CSP)
            # Call gains value when stock goes up: option_price + delta * move
            exit_limit = option_price + delta * abs(stock_move)
        else:  # DIP
            # Selling a put: put gains value when stock goes down
            # OR buying back short call: call loses value when stock goes down
            if is_bw_cc:
                # Buying back short call: cheaper when stock drops
                exit_limit = option_price - delta * abs(stock_move)
            else:
                # Selling a put: put gains value when stock drops
                exit_limit = option_price + delta * abs(stock_move)

        # Floor guards
        if is_bw_cc:
            # Buying back: floor at $0.01 (can't be negative)
            exit_limit = max(exit_limit, 0.01)
        else:
            # Selling: floor at Bid (can't sell below bid)
            if bid > 0:
                exit_limit = max(exit_limit, bid)
            else:
                exit_limit = max(exit_limit, 0.01)

        # Theta-to-Move Ratio
        theta_raw = pd.to_numeric(row.get("Theta"), errors="coerce")
        atr_raw = pd.to_numeric(row.get("ATR_14") or row.get("atr_14"), errors="coerce")
        tmr = _compute_theta_to_move_ratio(
            float(theta_raw) if pd.notna(theta_raw) else 0.0,
            delta,
            float(atr_raw) if pd.notna(atr_raw) else 0.0,
        )

        # Patience
        urgency = str(row.get("Urgency", "") or "")
        dte = pd.to_numeric(row.get("DTE"), errors="coerce")
        momentum = str(row.get("MomentumVelocity_State", "") or "")
        patience = _compute_patience_days(urgency, dte, momentum, direction, tmr, is_short_option=is_bw_cc)

        # If level is "Current" (no favorable level found), patience = 0
        if level_label == "Current":
            patience = 0

        # Build rationale
        if level_label == "Current":
            rationale = (
                f"No favorable {direction.lower()} level found — "
                f"use market price ${option_price:.2f}"
            )
        else:
            move_str = f"${abs(stock_move):.2f}"
            rationale = (
                f"{level_label} at ${target_stock:.2f} is {move_str} "
                f"{'above' if direction == 'RALLY' else 'below'} current ${ul_last:.2f} — "
                f"Δ≈{delta:.2f} → limit ${exit_limit:.2f}"
            )
            if patience > 0:
                rationale += f", patience {patience}d then market"
            else:
                rationale += ", execute now"
            if pd.notna(tmr) and tmr > 0:
                if is_bw_cc and tmr > _THETA_MOVE_CRITICAL:
                    rationale += f" (θ/move={tmr:.2f} — theta favors seller, wait)"
                else:
                    rationale += f" (θ/move={tmr:.2f})"

        # Write
        df.at[idx, "Exit_Limit_Price"] = round(exit_limit, 2)
        df.at[idx, "Exit_Limit_Level"] = level_label
        df.at[idx, "Exit_Limit_Rationale"] = rationale
        df.at[idx, "Exit_Limit_Patience_Days"] = patience

    return df
