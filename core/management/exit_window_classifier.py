"""
Exit Optimal Window Classifier — Phase 2: Intraday Exit Timing
===============================================================
Classifies the optimal execution window for EXIT HIGH/CRITICAL actions
using momentum, spread quality, volume, and technical level proximity.

Parallel to _classify_roll_timing() + _build_intraday_roll_advisory() in engine.py,
but adapted for exit context.  Reuses the same Intraday_Advisory_JSON column and
dashboard expander that rolls use.

Public API
----------
    classify_exit_windows(df) -> df   # mutates EXIT HIGH/CRITICAL rows only

Columns written:
    Exit_Window_State         str    — MOMENTUM_ALIGNED | FAVORABLE_APPROACHING | SPREAD_WIDE | MOMENTUM_OPPOSING | NEUTRAL
    Exit_Window_Reason        str    — human-readable explanation
    Intraday_Advisory_JSON    str    — JSON blob (same structure as roll advisory)
"""

from __future__ import annotations

import json
import logging
from typing import Dict, Optional

import numpy as np
import pandas as pd

from core.management.exit_limit_pricer import (
    _classify_exit_direction,
    _compute_theta_to_move_ratio,
    _SHORT_OPTION_STRATEGIES,
    _THETA_MOVE_CRITICAL,
)

logger = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────────────

_SPREAD_WIDE_PCT = 0.05       # bid-ask > 5% of option price → SPREAD_WIDE
_SPREAD_TIGHT_PCT = 0.03      # ≤ 3% for proxy signal pass
_VOLUME_OI_THRESHOLD = 0.10   # volume ≥ 10% of OI for proxy signal pass
_VOLUME_OI_LOW = 0.20         # volume < 20% of OI (relative to OI) → SPREAD_WIDE
_INTRADAY_CHG_THRESHOLD = 0.5 # stock moved > 0.5% in favorable direction
_DISTANCE_TARGET_PCT = 0.01   # stock within 1% of Exit_Limit target
_KER_TRENDING = 0.45          # KER above this = trending sufficiently


# ─── Timing classification ────────────────────────────────────────────────────

def _classify_exit_timing(row: pd.Series, direction: str) -> dict:
    """
    Classify the intraday exit window for an EXIT row.

    Returns dict with keys:
        timing            — window state name
        urgency_mod       — upgrade urgency (str) or None
        action_mod        — EXIT_NOW | WAIT | PROCEED
        reason            — human-readable explanation
        intraday_advisory — dict for JSON serialization, or None
    """
    # Read signals from row
    momentum = str(row.get("MomentumVelocity_State", "") or "").upper().strip()
    ker = pd.to_numeric(row.get("kaufman_efficiency_ratio"), errors="coerce")
    bid = pd.to_numeric(row.get("Bid"), errors="coerce")
    ask = pd.to_numeric(row.get("Ask"), errors="coerce")
    last = pd.to_numeric(row.get("Last"), errors="coerce")
    volume = pd.to_numeric(row.get("Volume"), errors="coerce")
    oi = pd.to_numeric(row.get("Open_Int"), errors="coerce")
    ul_last = pd.to_numeric(row.get("UL Last"), errors="coerce")
    ul_prev = pd.to_numeric(row.get("UL_Prev_Close"), errors="coerce")

    # Determine momentum alignment with exit direction
    rally_momentum = momentum in ("ACCELERATING",) and direction == "RALLY"
    dip_momentum = momentum in ("ACCELERATING",) and direction == "DIP"
    momentum_aligned = rally_momentum or dip_momentum

    # Check if momentum opposes exit direction
    # If we want RALLY but stock is ACCELERATING down, or vice versa
    directional_balance = str(row.get("DirectionalBalance_State", "") or "").upper().strip()
    opposing = False
    if direction == "RALLY" and directional_balance == "SELLER_DOMINANT" and momentum == "ACCELERATING":
        opposing = True
    elif direction == "DIP" and directional_balance == "BUYER_DOMINANT" and momentum == "ACCELERATING":
        opposing = True

    # Spread width check
    spread_pct = np.nan
    if pd.notna(bid) and pd.notna(ask) and pd.notna(last) and last > 0:
        spread_pct = (ask - bid) / last
    spread_wide = pd.notna(spread_pct) and spread_pct > _SPREAD_WIDE_PCT

    # Volume vs OI check
    vol_oi_ratio = np.nan
    if pd.notna(volume) and pd.notna(oi) and oi > 0:
        vol_oi_ratio = volume / oi
    volume_low = pd.notna(vol_oi_ratio) and vol_oi_ratio < _VOLUME_OI_LOW

    # Distance to target level
    exit_level = str(row.get("Exit_Limit_Level", "") or "").strip()
    exit_target = pd.to_numeric(row.get("Exit_Limit_Price"), errors="coerce")
    # Compute using EMA9/SMA20 from Exit_Limit_Level (stock-level target stored in rationale)
    ema9 = pd.to_numeric(row.get("EMA9"), errors="coerce")
    sma20 = pd.to_numeric(row.get("SMA20"), errors="coerce")
    atr = pd.to_numeric(row.get("ATR_14") or row.get("atr_14"), errors="coerce")

    # Stock distance to target level
    stock_target = None
    if exit_level == "EMA9" and pd.notna(ema9):
        stock_target = ema9
    elif exit_level == "SMA20" and pd.notna(sma20):
        stock_target = sma20

    near_target = False
    if stock_target is not None and pd.notna(ul_last) and ul_last > 0:
        dist_pct = abs(ul_last - stock_target) / ul_last
        near_target = dist_pct < _DISTANCE_TARGET_PCT
    within_atr = False
    if stock_target is not None and pd.notna(ul_last) and pd.notna(atr) and atr > 0:
        within_atr = abs(ul_last - stock_target) < atr

    # ── Classification priority ──────────────────────────────────────────────
    # 1. MOMENTUM_ALIGNED: momentum favors exit + trending
    ker_ok = pd.notna(ker) and ker > _KER_TRENDING
    if momentum_aligned and ker_ok:
        advisory = _build_intraday_exit_advisory(row, "MOMENTUM_ALIGNED", direction)
        return {
            "timing": "MOMENTUM_ALIGNED",
            "urgency_mod": None,
            "action_mod": "EXIT_NOW",
            "reason": f"Momentum {momentum} in favorable {direction} direction (KER={ker:.2f})",
            "intraday_advisory": advisory,
        }

    # 2. SPREAD_WIDE: illiquid execution conditions
    if spread_wide or volume_low:
        parts = []
        if spread_wide:
            parts.append(f"spread {spread_pct:.1%}")
        if volume_low:
            parts.append(f"vol/OI {vol_oi_ratio:.1%}")
        advisory = _build_intraday_exit_advisory(row, "SPREAD_WIDE", direction)
        return {
            "timing": "SPREAD_WIDE",
            "urgency_mod": None,
            "action_mod": "WAIT",
            "reason": f"Illiquid: {', '.join(parts)} — wait for spread tightening",
            "intraday_advisory": advisory,
        }

    # 3. MOMENTUM_OPPOSING: momentum against exit direction
    if opposing:
        advisory = _build_intraday_exit_advisory(row, "MOMENTUM_OPPOSING", direction)
        return {
            "timing": "MOMENTUM_OPPOSING",
            "urgency_mod": None,
            "action_mod": "WAIT",
            "reason": f"Momentum opposes {direction} — {directional_balance} with {momentum}",
            "intraday_advisory": advisory,
        }

    # 4. FAVORABLE_APPROACHING: technical level within 1 ATR and momentum not opposing
    if within_atr and not opposing:
        advisory = _build_intraday_exit_advisory(row, "FAVORABLE_APPROACHING", direction)
        return {
            "timing": "FAVORABLE_APPROACHING",
            "urgency_mod": None,
            "action_mod": "PROCEED",
            "reason": f"Stock within 1 ATR of {exit_level} target — watch for approach",
            "intraday_advisory": advisory,
        }

    # 5. NEUTRAL: no strong signal
    advisory = _build_intraday_exit_advisory(row, "NEUTRAL", direction)
    return {
        "timing": "NEUTRAL",
        "urgency_mod": None,
        "action_mod": "PROCEED",
        "reason": "No strong intraday signal — proceed with limit order",
        "intraday_advisory": advisory,
    }


# ─── Intraday advisory builder ───────────────────────────────────────────────

def _build_intraday_exit_advisory(
    row: pd.Series,
    timing: str,
    direction: str,
) -> dict:
    """
    Build the intraday advisory dict (same JSON structure as roll advisory).

    6 proxy signals scored 0/1 each:
        1. intraday_chg_pct    — stock moved >0.5% in favorable direction
        2. spread_pct          — bid-ask ≤ 3% of option price
        3. volume_vs_oi        — volume ≥ 10% of OI
        4. distance_to_target  — stock within 1% of Exit_Limit target level
        5. momentum_alignment  — MomentumVelocity matches exit direction
        6. theta_to_move_ratio — theta > 0.8× expected daily move (urgency signal)

    Verdict: 3+ → EXECUTE_NOW | 2 → FAVORABLE_WINDOW | <2 → VERIFY_FIRST
    """
    # Read data
    ul_last = pd.to_numeric(row.get("UL Last"), errors="coerce")
    ul_prev = pd.to_numeric(row.get("UL_Prev_Close"), errors="coerce")
    bid = pd.to_numeric(row.get("Bid"), errors="coerce")
    ask = pd.to_numeric(row.get("Ask"), errors="coerce")
    last = pd.to_numeric(row.get("Last"), errors="coerce")
    volume = pd.to_numeric(row.get("Volume"), errors="coerce")
    oi = pd.to_numeric(row.get("Open_Int"), errors="coerce")
    momentum = str(row.get("MomentumVelocity_State", "") or "").upper().strip()
    exit_level = str(row.get("Exit_Limit_Level", "") or "").strip()
    ema9 = pd.to_numeric(row.get("EMA9"), errors="coerce")
    sma20 = pd.to_numeric(row.get("SMA20"), errors="coerce")

    # ── Signal 1: Intraday change in favorable direction ─────────────────────
    intraday_chg_pct = 0.0
    if pd.notna(ul_last) and pd.notna(ul_prev) and ul_prev > 0:
        intraday_chg_pct = ((ul_last - ul_prev) / ul_prev) * 100.0
    # For DIP, favorable = negative change → negate for scoring
    favorable_chg = intraday_chg_pct if direction == "RALLY" else -intraday_chg_pct
    sig_intraday = favorable_chg > _INTRADAY_CHG_THRESHOLD

    # ── Signal 2: Spread quality ─────────────────────────────────────────────
    spread_pct = np.nan
    if pd.notna(bid) and pd.notna(ask) and pd.notna(last) and last > 0:
        spread_pct = (ask - bid) / last
    sig_spread = pd.notna(spread_pct) and spread_pct <= _SPREAD_TIGHT_PCT

    # ── Signal 3: Volume vs OI ───────────────────────────────────────────────
    vol_oi = np.nan
    if pd.notna(volume) and pd.notna(oi) and oi > 0:
        vol_oi = volume / oi
    sig_volume = pd.notna(vol_oi) and vol_oi >= _VOLUME_OI_THRESHOLD

    # ── Signal 4: Distance to target level ───────────────────────────────────
    stock_target = None
    if exit_level == "EMA9" and pd.notna(ema9):
        stock_target = ema9
    elif exit_level == "SMA20" and pd.notna(sma20):
        stock_target = sma20
    distance_pct = np.nan
    if stock_target is not None and pd.notna(ul_last) and ul_last > 0:
        distance_pct = abs(ul_last - stock_target) / ul_last
    sig_distance = pd.notna(distance_pct) and distance_pct <= _DISTANCE_TARGET_PCT

    # ── Signal 5: Momentum alignment ────────────────────────────────────────
    rally_mom = momentum in ("ACCELERATING",) and direction == "RALLY"
    dip_mom = momentum in ("ACCELERATING",) and direction == "DIP"
    sig_momentum = rally_mom or dip_mom

    # ── Signal 6: Theta-to-Move Ratio ────────────────────────────────────────
    theta_raw = pd.to_numeric(row.get("Theta"), errors="coerce")
    delta_raw = pd.to_numeric(row.get("Delta"), errors="coerce")
    atr_raw = pd.to_numeric(row.get("ATR_14") or row.get("atr_14"), errors="coerce")
    tmr = _compute_theta_to_move_ratio(
        float(theta_raw) if pd.notna(theta_raw) else 0.0,
        float(delta_raw) if pd.notna(delta_raw) else 0.0,
        float(atr_raw) if pd.notna(atr_raw) else 0.0,
    )
    # For short options (BUY_WRITE/COVERED_CALL): high theta/move is GOOD (seller
    # collects theta fast) — NOT an urgency signal.  Urgency fires when theta is
    # negligible (stock moves dominate, so waiting costs money).
    # For long options: high theta/move = urgency to exit before theta eats position.
    strategy_upper = str(row.get("Strategy", "") or "").upper().strip()
    is_short_opt = strategy_upper in _SHORT_OPTION_STRATEGIES
    if is_short_opt:
        sig_theta = pd.notna(tmr) and tmr < 0.3  # theta negligible → stock risk dominates
    else:
        sig_theta = pd.notna(tmr) and tmr > _THETA_MOVE_CRITICAL

    # ── Score and verdict ────────────────────────────────────────────────────
    confirmations = sum([sig_intraday, sig_spread, sig_volume, sig_distance, sig_momentum, sig_theta])

    if confirmations >= 3:
        proxy_verdict = "EXECUTE_NOW"
        proxy_color = "red"
        proxy_summary = (
            f"{confirmations}/6 signals confirm favorable exit window — "
            f"execute {direction.lower()} limit order now."
        )
    elif confirmations == 2:
        proxy_verdict = "FAVORABLE_WINDOW"
        proxy_color = "orange"
        proxy_summary = (
            f"{confirmations}/6 signals — approaching favorable window. "
            f"Monitor for additional confirmation."
        )
    else:
        proxy_verdict = "VERIFY_FIRST"
        proxy_color = "blue"
        proxy_summary = (
            f"Only {confirmations}/6 signals — verify conditions manually "
            f"before sending exit order."
        )

    # ── Signals dict ─────────────────────────────────────────────────────────
    signals = {
        "intraday_chg_pct": round(intraday_chg_pct, 2),
        "spread_pct": round(float(spread_pct) * 100, 2) if pd.notna(spread_pct) else None,
        "volume_vs_oi": round(float(vol_oi), 2) if pd.notna(vol_oi) else None,
        "distance_to_target_pct": round(float(distance_pct) * 100, 2) if pd.notna(distance_pct) else None,
        "momentum_alignment": sig_momentum,
        "theta_to_move_ratio": round(float(tmr), 2) if pd.notna(tmr) else None,
    }

    # ── Notes ────────────────────────────────────────────────────────────────
    notes = []
    dir_label = "rally" if direction == "RALLY" else "dip"
    if timing == "MOMENTUM_ALIGNED":
        notes.append(f"Stock momentum is aligned with {dir_label} direction — favorable for exit execution.")
    elif timing == "SPREAD_WIDE":
        notes.append("Wide spreads or low volume — limit order may not fill at target price.")
        notes.append("Consider using a GTC limit order and waiting for spread compression.")
    elif timing == "MOMENTUM_OPPOSING":
        notes.append(f"Stock momentum is currently opposing {dir_label} — fills will likely be worse.")
        notes.append("If urgency allows, wait for momentum shift before executing.")
    elif timing == "FAVORABLE_APPROACHING":
        notes.append(f"Stock is approaching {exit_level} level — watch for arrival to execute.")
    else:
        notes.append("No strong intraday signal. Use limit order from Phase 1 pricing.")

    if sig_intraday:
        notes.append(f"Intraday move {intraday_chg_pct:+.1f}% supports favorable {dir_label} exit.")
    if sig_theta:
        if is_short_opt:
            notes.append(
                f"Theta-to-move ratio {tmr:.2f} — theta negligible vs stock moves. "
                f"Buyback cost driven by stock direction, not decay. Execute promptly."
            )
        else:
            notes.append(f"Theta-to-move ratio {tmr:.2f} — theta dominates expected daily move. Execute promptly.")

    # ── Checklist ────────────────────────────────────────────────────────────
    checklist = [
        {
            "item": "Spread quality",
            "description": (
                "Check that the bid-ask spread on your option is no wider than 3% "
                "of the option price. Wide spreads erode exit fill quality."
            ),
        },
        {
            "item": "Volume confirmation",
            "description": (
                "Verify today's option volume is above its recent average. "
                "Higher volume = better chance of filling at your limit price."
            ),
        },
        {
            "item": "Stock momentum (5-min chart)",
            "description": (
                f"On the 5-minute chart, confirm stock is moving in the "
                f"{'upward' if direction == 'RALLY' else 'downward'} direction. "
                f"Look for consecutive candles in the favorable direction."
            ),
        },
        {
            "item": "VWAP confirmation",
            "description": (
                f"Check if stock is {'above' if direction == 'RALLY' else 'below'} VWAP. "
                f"{'Above' if direction == 'RALLY' else 'Below'} VWAP confirms "
                f"institutional flow supports your exit direction."
            ),
        },
        {
            "item": "Level proximity",
            "description": (
                f"Check if the stock is near the {exit_level or 'target'} level "
                f"(from Phase 1 limit pricing). Closer = more likely to reach the "
                f"price target for your limit order."
            ),
        },
        {
            "item": "No reversal candle",
            "description": (
                "Scan the last 15 minutes for doji, engulfing, or hammer candles "
                "that might indicate a reversal against your exit direction. "
                "If present, consider tightening or going market."
            ),
        },
    ]

    return {
        "proxy_verdict": proxy_verdict,
        "proxy_color": proxy_color,
        "proxy_summary": proxy_summary,
        "signals": signals,
        "notes": notes,
        "checklist": checklist,
    }


# ─── Main entry point ─────────────────────────────────────────────────────────

def classify_exit_windows(df: pd.DataFrame) -> pd.DataFrame:
    """
    For EXIT HIGH/CRITICAL rows, classify the optimal intraday execution window.

    LOW/MEDIUM EXIT rows use patience days from Phase 1 instead — no intraday
    advisory is generated for them.

    Writes:
        Exit_Window_State      — timing classification
        Exit_Window_Reason     — human-readable reason
        Intraday_Advisory_JSON — JSON blob (reuses roll advisory column)
    """
    # Ensure output columns exist
    for col in ("Exit_Window_State", "Exit_Window_Reason"):
        if col not in df.columns:
            df[col] = ""
    if "Intraday_Advisory_JSON" not in df.columns:
        df["Intraday_Advisory_JSON"] = ""

    # Only EXIT HIGH/CRITICAL rows
    exit_mask = df.get("Action", pd.Series(dtype=str)) == "EXIT"
    urgency = df.get("Urgency", pd.Series(dtype=str)).fillna("").str.upper().str.strip()
    high_urgency = urgency.isin(("HIGH", "CRITICAL"))
    target_mask = exit_mask & high_urgency

    if not target_mask.any():
        return df

    classified = 0
    for idx in df.index[target_mask]:
        row = df.loc[idx]
        strategy = str(row.get("Strategy", "") or "")
        call_put = str(row.get("Call/Put", "") or "")
        direction = _classify_exit_direction(strategy, call_put)

        if direction == "SKIP":
            df.at[idx, "Exit_Window_State"] = "SKIP"
            df.at[idx, "Exit_Window_Reason"] = f"{strategy}: no directional preference"
            continue

        result = _classify_exit_timing(row, direction)

        df.at[idx, "Exit_Window_State"] = result["timing"]
        df.at[idx, "Exit_Window_Reason"] = result["reason"]

        if result.get("intraday_advisory"):
            try:
                df.at[idx, "Intraday_Advisory_JSON"] = json.dumps(result["intraday_advisory"])
            except (TypeError, ValueError):
                pass

        classified += 1

    if classified:
        logger.info(f"[ExitWindow] Classified exit windows for {classified} EXIT HIGH/CRITICAL rows.")

    return df
