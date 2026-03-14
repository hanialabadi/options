"""
Shared income strategy gates — used by both BUY_WRITE and COVERED_CALL.

These gates evaluate conditions common to all short-premium income strategies
(CC, BW) where a short call is written against stock. They are extracted from
the duplicated logic in engine.py's `_buy_write_doctrine` and `_covered_call_doctrine`.

Each gate is a pure function with signature:
    gate_xxx(*, spot, dte, ..., result) -> tuple[bool, Dict]

Returns (True, result) if gate fired (caller should return immediately).
Returns (False, result) if gate did not fire (caller should continue).
"""

import math
from typing import Dict, Any, Tuple

import pandas as pd

from .gate_result import fire_gate, skip_gate, STATE_ACTIONABLE
from .thresholds import (
    GAMMA_ATM_PROXIMITY,
    GAMMA_DANGER_RATIO,
    GAMMA_DOMINANCE_RATIO,
    GAMMA_MONEYNESS_GUARD,
    EXTRINSIC_THETA_EXHAUSTED,
    BREAKOUT_THROUGH_STRIKE,
    DTE_EMERGENCY_ROLL,
    DTE_INCOME_GATE,
    DTE_CUSHION_WINDOW,
    DELTA_DIVIDEND_ASSIGNMENT,
    DELTA_ITM_EMERGENCY,
    DIVIDEND_DAYS_CRITICAL,
    DIVIDEND_DAYS_WARNING,
    ADX_TRENDING,
    CONSECUTIVE_DEBIT_ROLLS_HARD_STOP,
    MFE_SIGNIFICANT,
    MFE_GIVEBACK_EXIT,
    MFE_ROUNDTRIP_PNL,
    CARRY_INVERSION_MILD,
    CARRY_INVERSION_SEVERE,
)

from core.management.cycle1.identity.constants import FIDELITY_MARGIN_RATE_DAILY


def gate_post_buyback_sticky(
    *,
    row: pd.Series,
    spot: float,
    result: Dict[str, Any],
) -> Tuple[bool, Dict[str, Any]]:
    """
    Post-BUYBACK sticky gate — block re-selling premium until equity recovers.

    When prior action was BUYBACK, the short call has been removed.
    Do NOT re-sell (ROLL) until Equity_Integrity = INTACT.
    Jabbour Ch.11: "close and re-evaluate" means wait for confirmation.
    """
    prior_action = str(row.get('Prior_Action', '') or '').upper()
    ei_state = str(row.get('Equity_Integrity_State', '') or '').strip()

    if prior_action != "BUYBACK" or ei_state == "INTACT":
        return skip_gate(result)

    # Prefer pre-computed Daily_Margin_Cost (correct: borrowed portion only)
    _pbc_precomputed = float(row.get('Daily_Margin_Cost') or 0.0)
    margin_cost_daily = _pbc_precomputed if _pbc_precomputed > 0 else (
        spot * FIDELITY_MARGIN_RATE_DAILY * 100 if spot > 0 else 0.0
    )
    return fire_gate(
        result,
        action="HOLD",
        urgency="HIGH",
        rationale=(
            f"Post-BUYBACK hold \u2014 short call was removed, stock held unencumbered. "
            f"Equity Integrity is {ei_state or 'UNKNOWN'} (not yet INTACT). "
            f"Do NOT re-sell premium until structure confirms recovery. "
            f"Stock carries ${margin_cost_daily:.2f}/day margin cost "
            f"with zero theta offset \u2014 accept this cost as the price of decoupling. "
            f"(Jabbour Ch.11: re-evaluate only after structure resolves; "
            f"McMillan Ch.3: stock and call decisions are independent)"
        ),
        doctrine_source="Post-BUYBACK: Equity not INTACT \u2014 hold unencumbered",
    )


def gate_gamma_danger_zone(
    *,
    spot: float,
    strike: float,
    dte: float,
    theta: float,
    gamma: float,
    hv_20d: float,
    gamma_roc_3d: float,
    ei_state: str,
    strategy_label: str,
    result: Dict[str, Any],
) -> Tuple[bool, Dict[str, Any]]:
    """
    Near-ATM + low DTE gamma acceleration detector.

    Fires BEFORE equity breaks. Pre-emptive roll to extend DTE and reduce gamma.
    Natenberg Ch.7 (0.744): "ATM + low DTE + low vol is the maximum-risk
    configuration for short gamma."
    Passarelli Ch.6: "Pre-emptive roll before gamma overwhelms theta."
    """
    hv = hv_20d
    if hv >= 1.0:
        hv /= 100.0
    sigma = spot * (hv / math.sqrt(252)) if spot > 0 else 0.0
    drag = 0.5 * gamma * (sigma ** 2)
    atm_pct = abs(spot - strike) / spot if spot > 0 and strike > 0 else 1.0
    ratio = drag / theta if theta > 0 else 0.0

    fires = (
        atm_pct < GAMMA_ATM_PROXIMITY
        and DTE_EMERGENCY_ROLL < dte <= DTE_INCOME_GATE
        and ratio > GAMMA_DANGER_RATIO
        and ei_state != 'BROKEN'
    )

    if not fires:
        return skip_gate(result)

    urgency = "HIGH" if gamma_roc_3d > 0 else "MEDIUM"
    roc_note = (
        f"Gamma_ROC_3D={gamma_roc_3d:+.4f} (accelerating \u2014 urgency escalated). "
        if gamma_roc_3d > 0 else
        f"Gamma_ROC_3D={gamma_roc_3d:+.4f} (stable/declining). "
    )
    return fire_gate(
        result,
        action="ROLL",
        urgency=urgency,
        rationale=(
            f"Gamma Danger Zone: near-ATM ({atm_pct:.1%} from strike "
            f"${strike:.2f}), DTE={dte:.0f}, gamma/theta ratio={ratio:.1f}x. "
            f"{roc_note}"
            f"Gamma drag ${drag*100:.2f}c/contract/day approaching theta "
            f"${theta*100:.2f}c/contract/day \u2014 short gamma accelerating toward dominance. "
            f"Roll to extend DTE (30-45d target reduces gamma ~40%) or move strike OTM. "
            f"Natenberg Ch.7: 'ATM + low DTE is the maximum-risk configuration for short gamma.' "
            f"Passarelli Ch.6: 'Pre-emptive roll before gamma overwhelms theta.'"
        ),
        doctrine_source=f"Natenberg Ch.7: Gamma danger zone + Passarelli Ch.6: pre-emptive roll ({strategy_label})",
    )


def _compute_gamma_dominance(
    *,
    spot: float,
    strike: float,
    theta: float,
    gamma: float,
    hv_20d: float,
) -> Tuple[float, float, bool]:
    """
    Compute gamma drag and whether gamma dominates theta.

    Returns (gamma_drag, ratio, is_dominant).
    """
    hv = hv_20d
    if hv >= 1.0:
        hv /= 100.0
    sigma = spot * (hv / math.sqrt(252)) if spot > 0 else 0.0
    gamma_drag = 0.5 * gamma * (sigma ** 2)

    # Moneyness sanity guard: gamma dominance only meaningful within 30% of spot
    otm_pct = abs(strike - spot) / spot if strike > 0 and spot > 0 else 0.0
    ratio = gamma_drag / theta if theta > 0 else 0.0

    is_dominant = (
        theta > 0
        and gamma_drag > theta * GAMMA_DOMINANCE_RATIO
        and otm_pct <= GAMMA_MONEYNESS_GUARD
    )
    return gamma_drag, ratio, is_dominant


def gate_equity_broken_gamma_conviction(
    *,
    row: pd.Series,
    spot: float,
    strike: float,
    dte: float,
    theta: float,
    gamma: float,
    hv_20d: float,
    ei_state: str,
    ei_reason: str,
    result: Dict[str, Any],
) -> Tuple[bool, Dict[str, Any]]:
    """
    Equity BROKEN + gamma dominant + buyback conviction → ROLL HIGH.

    Conviction gate (any one fires):
      A. DTE ≤ 7: expiration week
      B. Stock above strike + upward momentum
      C. Extrinsic < $0.20: no theta left to collect

    Passarelli Ch.6: close short premium in expiration week.
    Natenberg Ch.7: short gamma at >2x theta is structurally unprofitable.
    """
    if ei_state != 'BROKEN':
        return skip_gate(result)

    gamma_drag, ratio, is_dominant = _compute_gamma_dominance(
        spot=spot, strike=strike, theta=theta, gamma=gamma, hv_20d=hv_20d,
    )
    if not is_dominant:
        return skip_gate(result)

    # Compute conviction signals
    call_last = abs(float(row.get('Short_Call_Last') or row.get('Last') or 0))
    entry_premium = abs(float(row.get('Short_Call_Premium') or row.get('Premium_Entry') or 0))
    intrinsic = max(0.0, spot - strike) if spot > 0 and strike > 0 else 0.0
    extrinsic_val = max(0.0, call_last - intrinsic)

    adx = float(row.get('adx_14', 0) or 0)
    roc = float(row.get('roc_20', 0) or 0)

    has_conviction = (
        dte <= DTE_EMERGENCY_ROLL
        or (spot > strike * BREAKOUT_THROUGH_STRIKE and roc > 0 and adx > ADX_TRENDING)
        or extrinsic_val < EXTRINSIC_THETA_EXHAUSTED
    )

    if not has_conviction:
        return skip_gate(result)

    close_note = (
        f" Current call at ${call_last:.2f} vs ${entry_premium:.2f} entry "
        f"({'profit' if call_last < entry_premium else 'debit'} to close)."
        if call_last > 0 and entry_premium > 0 else ""
    )
    conv_reason = (
        f"DTE \u2264 {DTE_EMERGENCY_ROLL} (expiration week)" if dte <= DTE_EMERGENCY_ROLL else
        f"stock ${spot:.2f} above strike ${strike:.2f} + momentum (ROC={roc:.1f}, ADX={adx:.0f})"
        if spot > strike * BREAKOUT_THROUGH_STRIKE else
        f"extrinsic only ${extrinsic_val:.2f} (< ${EXTRINSIC_THETA_EXHAUSTED:.2f} \u2014 no theta left)"
    )
    return fire_gate(
        result,
        action="ROLL",
        urgency="HIGH",
        rationale=(
            f"\u26a1 Equity BROKEN + gamma dominant ({ratio:.1f}\u00d7 theta) + "
            f"buyback conviction ({conv_reason}): "
            f"gamma drag ${gamma_drag*100:.2f}/contract/day vs theta "
            f"${theta*100:.2f}/contract/day \u2014 HOLD bleeds ${(gamma_drag - theta)*100:.2f}/contract/day. "
            f"{close_note} "
            f"BUY BACK the short call to stop the gamma bleed and decouple from the stock decision. "
            f"Then evaluate the stock independently: if thesis broken \u2192 sell stock; "
            f"if temporary \u2192 re-sell a 30\u201345 DTE near-ATM call for better theta efficiency. "
            f"(Passarelli Ch.6: close short premium in expiration week; "
            f"Natenberg Ch.7: short gamma at {ratio:.1f}\u00d7 theta is structurally unprofitable)"
        ),
        doctrine_source="Passarelli Ch.6: Expiration week close + Natenberg Ch.7: gamma/theta ratio",
    )


def gate_equity_broken_gamma_no_conviction(
    *,
    row: pd.Series,
    spot: float,
    strike: float,
    dte: float,
    theta: float,
    gamma: float,
    hv_20d: float,
    effective_cost: float,
    cum_premium: float,
    ei_state: str,
    ei_reason: str,
    result: Dict[str, Any],
) -> Tuple[bool, Dict[str, Any]]:
    """
    Equity BROKEN + gamma dominant + NO conviction.

    Sub-gates:
      A. Negative carry + below net cost → EXIT MEDIUM
      B. Negative carry + above net cost (cushion) → HOLD MEDIUM
      C. Positive carry despite gamma → HOLD MEDIUM (informational)
    """
    if ei_state != 'BROKEN':
        return skip_gate(result)

    gamma_drag, ratio, is_dominant = _compute_gamma_dominance(
        spot=spot, strike=strike, theta=theta, gamma=gamma, hv_20d=hv_20d,
    )
    if not is_dominant:
        return skip_gate(result)

    # Check conviction — if conviction present, this gate should not fire
    # (gate_equity_broken_gamma_conviction should have caught it)
    call_last = abs(float(row.get('Short_Call_Last') or row.get('Last') or 0))
    intrinsic = max(0.0, spot - strike) if spot > 0 and strike > 0 else 0.0
    extrinsic_val = max(0.0, call_last - intrinsic)
    adx = float(row.get('adx_14', 0) or 0)
    roc = float(row.get('roc_20', 0) or 0)

    has_conviction = (
        dte <= DTE_EMERGENCY_ROLL
        or (spot > strike * BREAKOUT_THROUGH_STRIKE and roc > 0 and adx > ADX_TRENDING)
        or extrinsic_val < EXTRINSIC_THETA_EXHAUSTED
    )
    if has_conviction:
        return skip_gate(result)

    # No conviction — check carry economics
    margin_daily = effective_cost * FIDELITY_MARGIN_RATE_DAILY if effective_cost > 0 else 0.0
    net_carry = theta - margin_daily - gamma_drag

    above_net_cost = (
        effective_cost > 0
        and spot > effective_cost
        and dte > DTE_CUSHION_WINDOW
    )
    cushion_pct = (
        (spot - effective_cost) / effective_cost
        if effective_cost > 0 and spot > effective_cost else 0.0
    )

    if net_carry < 0 and not above_net_cost:
        # Sub-gate A: negative carry + no cushion → EXIT MEDIUM
        bleed_contract = abs(net_carry) * 100
        bleed_to_exp = bleed_contract * dte
        return fire_gate(
            result,
            action="EXIT",
            urgency="MEDIUM",
            rationale=(
                f"\u26a0\ufe0f Equity BROKEN ({ei_reason}) + negative carry: "
                f"\u03b8 ${theta*100:.2f}/day \u2212 margin ${margin_daily*100:.2f}/day "
                f"\u2212 \u03b3-drag ${gamma_drag*100:.2f}/day = "
                f"net bleed ${bleed_contract:.2f}/contract/day "
                f"(${bleed_to_exp:.0f} to expiry). "
                f"Stock ${spot:.2f} is BELOW net cost basis ${effective_cost:.2f} \u2014 "
                f"no premium cushion to absorb the bleed. "
                f"Gamma dominance ({ratio:.1f}\u00d7 theta) confirms drag exceeds income. "
                f"Close the position or buy back the call "
                f"and evaluate the stock independently. "
                f"(McMillan Ch.3: don't carry a broken structure at negative EV; "
                f"Natenberg Ch.7: negative carry + broken equity = structural loss)"
            ),
            doctrine_source="EquityIntegrity: BROKEN + Negative Carry \u2192 EXIT",
        )

    if net_carry < 0 and above_net_cost:
        # Sub-gate B: negative carry BUT stock above net cost → HOLD with cushion
        bleed_contract = abs(net_carry) * 100
        cushion_days = int(
            (spot - effective_cost) * 100 / bleed_contract
        ) if bleed_contract > 0 else 999
        return fire_gate(
            result,
            action="HOLD",
            urgency="MEDIUM",
            rationale=(
                f"\u26a0\ufe0f Equity BROKEN ({ei_reason}) + negative daily carry: "
                f"\u03b8 ${theta*100:.2f}/day \u2212 margin ${margin_daily*100:.2f}/day "
                f"\u2212 \u03b3-drag ${gamma_drag*100:.2f}/day = "
                f"net bleed ${bleed_contract:.2f}/contract/day. "
                f"BUT stock ${spot:.2f} is {cushion_pct:.1%} ABOVE net cost basis "
                f"${effective_cost:.2f} (cushion from ${cum_premium:.2f}/share collected). "
                f"Premium cushion absorbs ~{cushion_days}d of bleed before breakeven is threatened. "
                f"Monitor for: (A) stock approaching net cost basis \u2192 upgrade to EXIT, "
                f"(B) HV mean-reversion reducing gamma drag, "
                f"(C) DTE \u2264 {DTE_CUSHION_WINDOW} \u2192 roll or close. "
                f"(McMillan Ch.3: cumulative premium income IS the BUY_WRITE edge \u2014 "
                f"don't abandon accumulated cost reduction on a single cycle's carry metric)"
            ),
            doctrine_source="McMillan Ch.3: BUY_WRITE cost-basis cushion \u2014 HOLD with carry warning",
        )

    # Sub-gate C: positive carry — informational hold
    return skip_gate(result)


def gate_dividend_assignment(
    *,
    row: pd.Series,
    delta: float,
    result: Dict[str, Any],
) -> Tuple[bool, Dict[str, Any]]:
    """
    Dividend early exercise risk gate.

    McMillan Ch.2: when delta > 0.50, days_to_div < 5, and dividend > 0,
    early exercise risk is material.
    """
    days_to_div = row.get('Days_To_Dividend')
    div_amt = float(row.get('Dividend_Amount', 0) or 0)

    if days_to_div is None or pd.isna(days_to_div):
        return skip_gate(result)

    days_to_div = float(days_to_div)

    if delta <= DELTA_DIVIDEND_ASSIGNMENT or days_to_div > DIVIDEND_DAYS_WARNING or div_amt <= 0:
        return skip_gate(result)

    urgency = "CRITICAL" if days_to_div < DIVIDEND_DAYS_CRITICAL else "HIGH"
    return fire_gate(
        result,
        action="ROLL",
        urgency=urgency,
        rationale=(
            f"Dividend assignment risk: delta={delta:.2f} > {DELTA_DIVIDEND_ASSIGNMENT}, "
            f"ex-div in {days_to_div:.0f} days, dividend=${div_amt:.2f}. "
            f"Early exercise likely if call is ITM near ex-dividend. "
            f"Roll to next expiration before ex-div date. "
            f"(McMillan Ch.2: early exercise risk)"
        ),
        doctrine_source="McMillan Ch.2: Dividend assignment risk",
    )


def gate_expiration_dte_7(
    *,
    dte: float,
    strike: float,
    spot: float,
    result: Dict[str, Any],
) -> Tuple[bool, Dict[str, Any]]:
    """
    Pin risk at expiry — DTE < 7.

    McMillan Ch.3: gamma acceleration + pin risk in expiration week.
    """
    if dte >= DTE_EMERGENCY_ROLL:
        return skip_gate(result)

    return fire_gate(
        result,
        action="ROLL",
        urgency="HIGH",
        rationale=(
            f"Expiration week: DTE={dte:.0f} < {DTE_EMERGENCY_ROLL}. "
            f"Pin risk active \u2014 gamma acceleration makes delta unpredictable. "
            f"Spot ${spot:.2f} vs strike ${strike:.2f}. "
            f"Roll to 30-45 DTE call to restore theta efficiency and reduce pin risk. "
            f"(McMillan Ch.3: pin risk + gamma acceleration at expiry)"
        ),
        doctrine_source="McMillan Ch.3: Expiration week pin risk",
    )


def gate_itm_defense(
    *,
    delta: float,
    spot: float,
    strike: float,
    effective_cost: float,
    result: Dict[str, Any],
) -> Tuple[bool, Dict[str, Any]]:
    """
    Deep ITM defense — delta > 0.70, assignment imminent.

    Passarelli Ch.5: assignment imminent at delta > 0.70, roll NOW.
    """
    if delta <= DELTA_ITM_EMERGENCY:
        return skip_gate(result)

    # Cost-basis context
    loss_per_share = strike - effective_cost if effective_cost > 0 else 0
    loss_note = (
        f"Assignment at ${strike:.2f} vs cost ${effective_cost:.2f} = "
        f"${loss_per_share:+.2f}/share {'profit' if loss_per_share >= 0 else 'loss'}."
    )

    return fire_gate(
        result,
        action="ROLL",
        urgency="HIGH",
        rationale=(
            f"Assignment imminent: delta={delta:.2f} > {DELTA_ITM_EMERGENCY}. "
            f"Spot ${spot:.2f} deep ITM vs strike ${strike:.2f}. "
            f"{loss_note} "
            f"Income purpose eliminated \u2014 roll up/out to restore OTM positioning. "
            f"(Passarelli Ch.5: assignment imminent at delta > 0.70 \u2014 roll NOW)"
        ),
        doctrine_source="Passarelli Ch.5: ITM Emergency \u2014 ROLL",
    )


def gate_consecutive_debit_roll_stop(
    *,
    row: pd.Series,
    result: Dict[str, Any],
    strategy_label: str = "INCOME",
) -> Tuple[bool, Dict[str, Any]]:
    """
    Hard stop after N consecutive net-debit rolls.

    When a position has been rolled at a net debit N+ times in a row,
    the income thesis is structurally broken \u2014 each roll pays MORE to
    maintain the position than the premium collected.

    McMillan Ch.3: "Recognize when a position is chasing losses."
    Given Ch.6: "A debit roll is a new investment, not a repair."

    Returns (True, result) if gate fires (caller should propose EXIT).
    Returns (False, result) if below threshold.
    """
    consec = int(float(row.get('Trajectory_Consecutive_Debit_Rolls', 0) or 0))
    if consec < CONSECUTIVE_DEBIT_ROLLS_HARD_STOP:
        return skip_gate(result)

    total_cost = float(row.get('Trajectory_Total_Roll_Cost', 0) or 0)
    cum_premium = float(row.get('Cumulative_Premium_Collected', 0) or 0)
    net_income = cum_premium - abs(total_cost)

    return fire_gate(
        result,
        action="EXIT",
        urgency="HIGH",
        rationale=(
            f"Consecutive debit roll hard stop: {consec} consecutive net-debit "
            f"rolls (threshold={CONSECUTIVE_DEBIT_ROLLS_HARD_STOP}). "
            f"Cumulative premium ${cum_premium:.0f} vs total roll cost "
            f"${abs(total_cost):.0f} = net {'profit' if net_income >= 0 else 'loss'} "
            f"${abs(net_income):.0f}. "
            f"Income thesis structurally broken \u2014 each roll compounds losses. "
            f"Close position and redeploy capital to a fresh setup with better "
            f"risk/reward. "
            f"(McMillan Ch.3: strike-chase recognition; "
            f"Given Ch.6: debit roll = new investment, not repair)"
        ),
        doctrine_source=f"McMillan Ch.3 + Given Ch.6: Consecutive debit roll stop ({strategy_label})",
    )


def gate_fading_winner(
    *,
    row: pd.Series,
    pnl_pct: float,
    result: Dict[str, Any],
    strategy_label: str = "INCOME",
) -> Tuple[bool, Dict[str, Any]]:
    """
    Trailing protection for winners that are giving back gains.

    Detects positions that reached significant MFE (peak P&L) but have
    since retraced. Two tiers:
      - Gave back >50% of MFE: EXIT MEDIUM (protect remaining gains)
      - Round-tripped to <5% after 20%+ MFE: EXIT HIGH (urgent protection)

    McMillan Ch.4: "Protect profits \u2014 do not let a winner become a loser."
    Jabbour Ch.11: "Trail stops based on favorable excursion, not entry price."
    """
    _raw_mfe = row.get('Trajectory_MFE', 0)
    if _raw_mfe is None or (isinstance(_raw_mfe, float) and math.isnan(_raw_mfe)):
        return skip_gate(result)
    mfe = float(_raw_mfe or 0)
    if mfe < MFE_SIGNIFICANT:
        return skip_gate(result)

    # Position must still be in profit (if underwater, hard stop handles it)
    if pnl_pct is None or pnl_pct <= 0:
        return skip_gate(result)

    giveback_pct = 1.0 - (pnl_pct / mfe) if mfe > 0 else 0.0

    # Tier 1: Round-trip \u2014 was 20%+ up, now barely positive
    if pnl_pct < MFE_ROUNDTRIP_PNL:
        return fire_gate(
            result,
            action="EXIT",
            urgency="HIGH",
            rationale=(
                f"Round-trip protection: position peaked at +{mfe:.0%} MFE "
                f"but has retraced to +{pnl_pct:.1%} (gave back {giveback_pct:.0%}). "
                f"Exit now to preserve remaining gains before breakeven. "
                f"(McMillan Ch.4: do not let a winner become a loser; "
                f"Jabbour Ch.11: trail stops based on peak excursion)"
            ),
            doctrine_source=f"McMillan Ch.4 + Jabbour Ch.11: Round-trip protection ({strategy_label})",
        )

    # Tier 2: Gave back too much of peak gains — vol-scaled threshold
    # Carver: "Systematic trailing stops should scale with volatility."
    # Low-vol names (HV<15%): tighter 35% giveback — small moves are meaningful
    # High-vol names (HV>40%): wider 60% giveback — large swings are normal
    # Mid-range: standard 50% giveback
    from .thresholds import (
        MFE_GIVEBACK_HV_LOW, MFE_GIVEBACK_HV_HIGH,
        MFE_GIVEBACK_TIGHT, MFE_GIVEBACK_WIDE,
    )
    _hv_raw = row.get('HV_20D')
    _hv = float(_hv_raw) if _hv_raw is not None and pd.notna(_hv_raw) else None
    if _hv is not None and _hv > 1.0:
        _hv /= 100.0  # normalize percentage to decimal

    if _hv is not None and _hv < MFE_GIVEBACK_HV_LOW:
        _giveback_thresh = MFE_GIVEBACK_TIGHT
        _vol_note = f"HV {_hv:.0%} < {MFE_GIVEBACK_HV_LOW:.0%} \u2014 tighter trail"
    elif _hv is not None and _hv > MFE_GIVEBACK_HV_HIGH:
        _giveback_thresh = MFE_GIVEBACK_WIDE
        _vol_note = f"HV {_hv:.0%} > {MFE_GIVEBACK_HV_HIGH:.0%} \u2014 wider trail"
    else:
        _giveback_thresh = MFE_GIVEBACK_EXIT
        _vol_note = f"HV {_hv:.0%} \u2014 standard trail" if _hv is not None else "HV unknown \u2014 standard trail"

    if giveback_pct >= _giveback_thresh:
        return fire_gate(
            result,
            action="EXIT",
            urgency="MEDIUM",
            rationale=(
                f"Fading winner: position peaked at +{mfe:.0%} MFE, "
                f"now +{pnl_pct:.1%} (gave back {giveback_pct:.0%} of gains, "
                f"threshold {_giveback_thresh:.0%}). {_vol_note}. "
                f"Momentum fading \u2014 lock in remaining profit and redeploy. "
                f"(McMillan Ch.4: protect profits; "
                f"Jabbour Ch.11: trail based on favorable excursion; "
                f"Carver: vol-scaled trailing stops)"
            ),
            doctrine_source=f"McMillan Ch.4 + Jabbour Ch.11 + Carver: Fading winner ({strategy_label})",
        )

    return skip_gate(result)


def gate_income_trim(
    *,
    row: pd.Series,
    pnl_pct: float,
    wave_phase: str,
    conviction_status: str,
    quantity: int,
    result: Dict[str, Any],
    strategy_label: str = "INCOME",
) -> Tuple[bool, Dict[str, Any]]:
    """
    Partial-close gate for multi-contract income positions.

    Uses wave phase to determine trim timing:
      - PEAKING + MFE ≥ 20%  → trim 25% (protect extended gains)
      - EXHAUSTED/FADING      → trim 50% (exit fading momentum)

    Quantity must be in option-contract units (not raw shares).

    Rounding rules:
      - trim_contracts = min(max(1, round(qty × fraction)), qty - 1)
      - Never trim to 0 contracts
      - If trim would remove all → escalate to EXIT instead

    McMillan Ch.4: "Protect profits — scale out on strength, not weakness."
    Passarelli Ch.6: "Partial close reduces risk while maintaining exposure."
    """
    from .thresholds import INCOME_TRIM_MIN_QUANTITY, MFE_SIGNIFICANT
    from core.management.cycle2.chart_state.state_extractors.wave_phase import (
        is_trim_eligible,
    )

    if quantity < INCOME_TRIM_MIN_QUANTITY:
        return skip_gate(result)

    mfe = float(row.get('Trajectory_MFE', 0) or 0)
    should_trim, trim_fraction = is_trim_eligible(wave_phase, mfe, pnl_pct)

    if not should_trim:
        return skip_gate(result)

    # ── Integer rounding with guards ──────────────────────────────────────
    trim_contracts = max(1, round(quantity * trim_fraction))
    trim_contracts = min(trim_contracts, quantity - 1)

    # If rounding leaves 0 remaining (shouldn't happen with min guard, but safe)
    if trim_contracts >= quantity:
        return fire_gate(
            result,
            action="EXIT",
            urgency="HIGH",
            rationale=(
                f"Trim escalated to EXIT: {trim_fraction:.0%} trim on {quantity} "
                f"contracts would remove all. Wave phase {wave_phase}, "
                f"conviction {conviction_status}. "
                f"(McMillan Ch.4: full exit when trim leaves no remaining exposure)"
            ),
            doctrine_source=f"McMillan Ch.4: Trim→EXIT escalation ({strategy_label})",
        )

    # ── Determine urgency from wave phase ─────────────────────────────────
    if wave_phase in ("EXHAUSTED", "FADING") and conviction_status == "REVERSING":
        urgency = "HIGH"
    else:
        urgency = "MEDIUM"

    return fire_gate(
        result,
        action="TRIM",
        urgency=urgency,
        rationale=(
            f"Income TRIM: wave phase {wave_phase}, trim {trim_contracts} of "
            f"{quantity} contracts ({trim_fraction:.0%}). "
            f"MFE={mfe:.1%}, P&L={pnl_pct:.1%}, conviction={conviction_status}. "
            f"Remaining {quantity - trim_contracts} contracts maintain exposure "
            f"while locking partial profits. "
            f"(McMillan Ch.4: scale out on strength; "
            f"Passarelli Ch.6: partial close reduces risk)"
        ),
        doctrine_source=f"McMillan Ch.4 + Passarelli Ch.6: Income TRIM ({strategy_label})",
        Trim_Contracts=trim_contracts,
        Trim_Pct=trim_fraction,
    )


# ── Carry Inversion Roll Gate ────────────────────────────────────────────────

def gate_carry_inversion_roll(
    *,
    row: pd.Series,
    result: Dict[str, Any],
) -> Tuple[bool, Dict[str, Any]]:
    """Propose ROLL when margin carry cost exceeds theta income.

    Uses pre-computed ``Carry_Theta_Ratio`` and ``Carry_Classification``
    from ``MarginCarryCalculator`` (Cycle 2, step 2.65).  These columns
    use the corrected borrowed-amount calculation and exclude retirement
    accounts and options (only stock positions carry margin interest).

    Passarelli Ch.6: "Negative carry — yield below financing rate — is a ROLL signal."
    McMillan Ch.3: "The covered writer must earn at least the cost of carrying the stock."

    Returns (True, result) if gate fired, (False, result) otherwise.
    """
    classification = str(row.get("Carry_Classification", "") or "").upper()

    if classification not in ("MILD_INVERSION", "SEVERE_INVERSION"):
        return skip_gate(result)

    ratio = float(row.get("Carry_Theta_Ratio") or 0.0)
    daily_cost = float(row.get("Daily_Margin_Cost") or 0.0)
    cumulative = float(row.get("Cumulative_Margin_Carry") or 0.0)
    days_in_trade = float(row.get("Days_In_Trade") or 0.0)

    if ratio <= 0:
        return skip_gate(result)

    # Severe: margin >= 1.5× theta → HIGH urgency
    if classification == "SEVERE_INVERSION":
        return fire_gate(
            result,
            action="ROLL",
            urgency="HIGH",
            rationale=(
                f"Carry severely inverted: margin cost ${daily_cost:.2f}/day "
                f"is {ratio:.1f}× theta income (threshold: {CARRY_INVERSION_SEVERE}×). "
                f"Cumulative carry: ${cumulative:.2f} over {days_in_trade:.0f} days. "
                f"Theta cannot cover financing — roll to a strike with higher "
                f"premium or shorter DTE to restore positive carry. "
                f"(Passarelli Ch.6: negative carry is a ROLL signal; "
                f"McMillan Ch.3: covered writer must earn carry cost)"
            ),
            doctrine_source="Passarelli Ch.6 + McMillan Ch.3: Severe Carry Inversion → ROLL",
        )

    # Mild: margin >= 1.0× theta but < 1.5× → MEDIUM urgency
    return fire_gate(
        result,
        action="ROLL",
        urgency="MEDIUM",
        rationale=(
            f"Carry mildly inverted: margin cost ${daily_cost:.2f}/day "
            f"is {ratio:.1f}× theta income (threshold: {CARRY_INVERSION_MILD}×). "
            f"Cumulative carry: ${cumulative:.2f} over {days_in_trade:.0f} days. "
            f"Theta barely covers financing — consider rolling closer to ATM "
            f"for higher premium or shorter DTE to improve carry ratio. "
            f"(Passarelli Ch.6: yield below financing = roll signal)"
        ),
        doctrine_source="Passarelli Ch.6: Mild Carry Inversion → ROLL",
    )
