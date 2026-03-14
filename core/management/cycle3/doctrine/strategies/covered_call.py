"""
COVERED_CALL doctrine — short call against long stock.

Gate order (first match returns):
  1.  Underlying collapse (drift High+Down) → EXIT CRITICAL
  1a. Post-BUYBACK sticky → HOLD HIGH (shared gate)
  1b. Gamma Danger Zone → ROLL HIGH/MEDIUM (shared gate)
  1c. Equity BROKEN + gamma conviction → ROLL HIGH (shared gate)
  1d. Equity BROKEN + gamma no conviction → EXIT/HOLD MEDIUM (shared gate)
  1e. Equity BROKEN + LEAPS carry inversion (severe) → BUYBACK HIGH
  1f. Equity BROKEN + LEAPS carry inversion (mild) → HOLD HIGH
  1g. Equity BROKEN standard → HOLD HIGH
  1h. LEAPS carry inversion (non-BROKEN) → ROLL MEDIUM
  2.  Dividend assignment → ROLL CRITICAL/HIGH (shared gate)
  3.  ITM/ATM Late lifecycle → ROLL HIGH
  4.  Dual-stage delta: > 0.70 → ROLL HIGH; > 0.55 → ROLL MEDIUM
  5.  DTE ≤ 7 pin risk → ROLL HIGH (with thesis addendum)
  6.  21-DTE income gate → ROLL MEDIUM/HIGH
  7.  50% premium capture → ROLL MEDIUM
  8.  Thesis regime degradation → ROLL MEDIUM
  9.  EV comparator override → ROLL MEDIUM
  10. Default → HOLD LOW + WEAKENING annotation
"""

import logging
import math
from typing import Dict, Any

import pandas as pd

from ..gate_result import (
    fire_gate,
    skip_gate,
    STATE_ACTIONABLE,
    STATE_NEUTRAL_CONFIDENT,
)
from ..proposal import ProposalCollector, propose_gate
from ..thresholds import (
    DELTA_PRE_ITM_WARNING,
    DELTA_ITM_EMERGENCY,
    DELTA_BEHAVIORAL_ITM,
    DTE_CUSHION_WINDOW,
    DTE_EMERGENCY_ROLL,
    DTE_INCOME_GATE,
    DTE_LEAPS_THRESHOLD,
    CARRY_INVERSION_SEVERE,
    PREMIUM_CAPTURE_TARGET,
    EV_NOISE_FLOOR_INCOME,
)
from ..shared_income_gates import (
    gate_post_buyback_sticky,
    gate_gamma_danger_zone,
    gate_equity_broken_gamma_conviction,
    gate_equity_broken_gamma_no_conviction,
    gate_dividend_assignment,
    gate_expiration_dte_7,
    gate_consecutive_debit_roll_stop,
    gate_fading_winner,
)
from ..helpers import check_thesis_degradation, detect_recovery_state, safe_pnl_pct, safe_row_float
from ..comparators.action_ev_bw import compare_actions_bw, resolve_income_proposals
from core.management.cycle1.identity.constants import FIDELITY_MARGIN_RATE_DAILY

logger = logging.getLogger(__name__)


def covered_call_doctrine(row: pd.Series, result: Dict[str, Any]) -> Dict[str, Any]:
    """Evaluate COVERED_CALL position. Mutates and returns *result*."""
    # ── Extract fields ────────────────────────────────────────────────
    delta = abs(safe_row_float(row, 'Short_Call_Delta', 'Delta'))
    _dte_raw = row.get('Short_Call_DTE')
    if not pd.notna(_dte_raw):
        _dte_raw = row.get('DTE')
    if not pd.notna(_dte_raw):
        _dte_raw = 999
    dte = float(_dte_raw)
    moneyness = row.get('Moneyness_Label', 'OTM')
    lifecycle = row.get('Lifecycle_Phase', 'ACTIVE')
    drift_dir = row.get('Drift_Direction', '')
    drift_mag = row.get('Drift_Magnitude', '')

    spot = abs(safe_row_float(row, 'UL Last', 'Spot'))
    strike = safe_row_float(row, 'Strike')
    ei_state = str(row.get('Equity_Integrity_State', '') or '').strip()
    ei_reason = str(row.get('Equity_Integrity_Reason', '') or '').strip()

    theta = abs(safe_row_float(row, 'Theta'))
    gamma = abs(safe_row_float(row, 'Gamma'))
    hv_20d = safe_row_float(row, 'HV_20D', default=0.20)
    gamma_roc_3d = safe_row_float(row, 'Gamma_ROC_3D')

    # ── Gate 1: Underlying collapse ───────────────────────────────────
    if drift_mag == 'High' and drift_dir == 'Down':
        _, _cc_exit_result = fire_gate(
            result,
            action="EXIT",
            urgency="CRITICAL",
            rationale=(
                "Underlying price collapse — call premium insufficient to offset stock loss. "
                "Exit both legs (McMillan Ch.2: Hard Stop)."
            ),
            doctrine_source="McMillan Ch.2: Hard Stop",
        )
        _cc_exit_result['Exit_Trigger_Type'] = 'CAPITAL'
        return _cc_exit_result

    # ── Gate 1a: Post-BUYBACK sticky (shared) ─────────────────────────
    fired, result = gate_post_buyback_sticky(row=row, spot=spot, result=result)
    if fired:
        return result

    # ── Gate 1b: Gamma Danger Zone (shared) ───────────────────────────
    fired, result = gate_gamma_danger_zone(
        spot=spot, strike=strike, dte=dte, theta=theta, gamma=gamma,
        hv_20d=hv_20d, gamma_roc_3d=gamma_roc_3d, ei_state=ei_state,
        strategy_label="CC", result=result,
    )
    if fired:
        return result

    # ── Gate 1c-1g: Equity BROKEN gates ───────────────────────────────
    if ei_state == 'BROKEN' and result.get('Action') not in ('EXIT', 'HARD_HALT'):
        # 1c: BROKEN + gamma conviction
        fired, result = gate_equity_broken_gamma_conviction(
            row=row, spot=spot, strike=strike, dte=dte, theta=theta,
            gamma=gamma, hv_20d=hv_20d, ei_state=ei_state,
            ei_reason=ei_reason, result=result,
        )
        if fired:
            return result

        # 1d: BROKEN + gamma no conviction
        # Need effective_cost and cum_premium for this gate
        from core.shared.finance_utils import effective_cost_per_share as _ecp
        effective_cost, _, _ = _ecp(row, spot_fallback=spot)
        cum_premium = abs(safe_row_float(row, 'Cumulative_Premium_Collected'))

        fired, result = gate_equity_broken_gamma_no_conviction(
            row=row, spot=spot, strike=strike, dte=dte, theta=theta,
            gamma=gamma, hv_20d=hv_20d, effective_cost=effective_cost,
            cum_premium=cum_premium, ei_state=ei_state,
            ei_reason=ei_reason, result=result,
        )
        if fired:
            return result

        # 1e-1f: BROKEN + LEAPS carry inversion
        if dte > DTE_LEAPS_THRESHOLD:
            result_leaps = _gate_leaps_carry_broken(
                row=row, spot=spot, dte=dte, ei_reason=ei_reason, result=result,
            )
            if result_leaps is not None:
                return result_leaps

        # 1g: Standard BROKEN — no gamma dominance, not LEAPS
        return fire_gate(
            result,
            action="HOLD",
            urgency="HIGH",
            rationale=(
                f"\u26a0\ufe0f Equity Integrity BROKEN — structural deterioration detected "
                f"({ei_reason}). "
                f"Rolling locks in deeper commitment to a structurally declining stock. "
                f"Hold and reassess before next roll "
                f"(McMillan Ch.1: confirm trend context before committing further capital)."
            ),
            doctrine_source="EquityIntegrity: BROKEN structural gate (CC)",
        )[1]

    # ── Gate 1h: LEAPS carry inversion (non-BROKEN) ──────────────────
    if dte > DTE_LEAPS_THRESHOLD:
        result_leaps = _gate_leaps_carry_non_broken(
            row=row, spot=spot, dte=dte, result=result,
        )
        if result_leaps is not None:
            return result_leaps

    # ── Gate 2: Dividend assignment (shared) ──────────────────────────
    fired, result = gate_dividend_assignment(row=row, delta=delta, result=result)
    if fired:
        return result

    # ── Gate 3: ITM/ATM Late lifecycle ────────────────────────────────
    _is_behaviorally_itm = (moneyness == 'ITM') or (moneyness == 'ATM' and delta > DELTA_BEHAVIORAL_ITM)
    if lifecycle == 'TERMINAL' and _is_behaviorally_itm:
        return fire_gate(
            result,
            action="ROLL",
            urgency="HIGH",
            rationale=(
                f"Call ITM/near-ITM (Delta={delta:.2f}) with DTE={dte:.0f} — "
                f"assignment risk imminent. Roll up/out or accept assignment "
                f"if stock thesis intact (McMillan Ch.2)."
            ),
            doctrine_source="McMillan Ch.2: Expiration Management",
        )[1]

    # ── Gate 4: Dual-stage delta guard ────────────────────────────────
    if delta > DELTA_ITM_EMERGENCY:
        return fire_gate(
            result,
            action="ROLL",
            urgency="HIGH",
            rationale=(
                f"\U0001f6d1 Call delta={delta:.2f} > {DELTA_ITM_EMERGENCY} — "
                f"moving nearly dollar-for-dollar with stock. "
                f"Income purpose of covered call is eliminated; assignment highly probable. "
                f"BUY BACK the call and roll up/out immediately to restore income structure "
                f"(Passarelli Ch.5: Uncap Upside; McMillan Ch.2: delta > 0.70 = emergency roll)."
            ),
            doctrine_source="Passarelli Ch.5: Uncap Upside (delta > 0.70 — emergency)",
        )[1]
    if delta > DELTA_PRE_ITM_WARNING:
        return fire_gate(
            result,
            action="ROLL",
            urgency="MEDIUM",
            rationale=(
                f"\u26a0\ufe0f Call delta={delta:.2f} > {DELTA_PRE_ITM_WARNING} — "
                f"upside cap becoming material. "
                f"Early warning: roll up/out now while the debit-to-close is still manageable "
                f"and the roll premium is favorable. Acting at {DELTA_PRE_ITM_WARNING} "
                f"rather than {DELTA_ITM_EMERGENCY} preserves more premium in the new cycle "
                f"(McMillan Ch.2: early roll before deeply ITM; "
                f"Passarelli Ch.5: dual-stage delta gate)."
            ),
            doctrine_source=f"Passarelli Ch.5 + McMillan Ch.2: Early Warning (delta {DELTA_PRE_ITM_WARNING}–{DELTA_ITM_EMERGENCY})",
        )[1]

    # ── Gate 5: DTE ≤ 7 with thesis addendum ─────────────────────────
    try:
        _dte_float = float(dte)
    except (TypeError, ValueError):
        _dte_float = 999.0

    if _dte_float <= DTE_EMERGENCY_ROLL:
        thesis_addendum = ""
        thesis_check = check_thesis_degradation(row)
        if thesis_check:
            thesis_addendum = f" Note: {thesis_check['text']}."
        return fire_gate(
            result,
            action="ROLL",
            urgency="HIGH",
            rationale=(
                f"DTE={_dte_float:.0f} — expiration imminent. "
                f"Pin risk and gamma acceleration require rolling now. "
                f"Roll to next cycle before time value collapses entirely."
                + thesis_addendum
                + " (McMillan Ch.3: Expiration Management)."
            ),
            doctrine_source="McMillan Ch.3: Expiration Management",
        )[1]

    # ── Gate 6: 21-DTE income gate ────────────────────────────────────
    pe_21 = abs(safe_row_float(row, 'Premium_Entry', 'Premium'))
    last_21 = abs(safe_row_float(row, 'Last'))
    captured_21 = (pe_21 - last_21) / pe_21 if pe_21 > 0 else 0.0
    moneyness_21 = str(row.get('Moneyness_Label') or row.get('Short_Call_Moneyness') or 'OTM')

    if (
        dte <= DTE_INCOME_GATE
        and dte >= DTE_EMERGENCY_ROLL       # DTE < 7 handled above
        and captured_21 < PREMIUM_CAPTURE_TARGET
        and moneyness_21 != 'ITM'           # ITM = assignment gate above
    ):
        urgency_21 = 'MEDIUM' if captured_21 >= 0 else 'HIGH'
        return fire_gate(
            result,
            action="ROLL",
            urgency=urgency_21,
            rationale=(
                f"21-DTE income gate: DTE={dte:.0f} \u2264 {DTE_INCOME_GATE} with only "
                f"{captured_21:.0%} profit captured (need \u2265{PREMIUM_CAPTURE_TARGET:.0%}). "
                f"Gamma-theta ratio has degraded — short call edge exhausted. "
                f"Buy back current call and roll out to 30-45 DTE to reset cycle. "
                f"(Given Ch.6: 21-DTE income roll; Passarelli Ch.2: theta/gamma degradation.)"
            ),
            doctrine_source="Given Ch.6 + Passarelli Ch.2: 21-DTE Income Roll Gate (CC)",
        )[1]

    # ── Gate 7: 50% premium capture ───────────────────────────────────
    premium_entry = abs(safe_row_float(row, 'Premium_Entry', 'Premium'))
    current_cost = abs(safe_row_float(row, 'Last'))
    if (
        premium_entry > 0
        and current_cost <= premium_entry * PREMIUM_CAPTURE_TARGET
        and dte > DTE_INCOME_GATE
    ):
        pct_captured = 1 - (current_cost / premium_entry)
        return fire_gate(
            result,
            action="ROLL",
            urgency="MEDIUM",
            rationale=(
                f"50% premium captured ({pct_captured:.0%} of ${premium_entry:.2f} entry credit) "
                f"with {dte:.0f} DTE remaining — close and redeploy into next cycle "
                f"(Passarelli Ch.6: 50% Rule)."
            ),
            doctrine_source="Passarelli Ch.6: 50% Rule",
        )[1]

    # ── Gate 8: Thesis regime degradation ─────────────────────────────
    thesis = check_thesis_degradation(row)
    if thesis:
        return fire_gate(
            result,
            action="ROLL",
            urgency="MEDIUM",
            rationale=(
                f"Entry regime degraded: {thesis['text']}. "
                f"Reassess strike/expiry — original setup no longer intact "
                f"(McMillan Ch.2: Thesis Persistence)."
            ),
            doctrine_source="McMillan Ch.2: Thesis Persistence",
        )[1]

    # ── Gate 9: EV comparator override ────────────────────────────────
    # Build CC default context FIRST (used if EV doesn't override)
    iv_shape = str(row.get('iv_surface_shape', '') or '').upper()
    iv_note = ""
    if iv_shape == 'BACKWARDATION':
        slope = safe_row_float(row, 'iv_ts_slope_30_90')
        iv_note = (
            f" IV BACKWARDATED ({slope:+.1f}pt): collecting elevated near-term IV "
            f"— premium received is above-normal (Natenberg Ch.11)."
        )

    # Forward expectancy context
    _ev_ratio = safe_row_float(row, 'EV_Feasibility_Ratio') if pd.notna(row.get('EV_Feasibility_Ratio')) else float('nan')
    _em = safe_row_float(row, 'Expected_Move_10D')
    _req = safe_row_float(row, 'Required_Move_Breakeven', 'Required_Move')
    ev_note = ""
    if not pd.isna(_ev_ratio) and _ev_ratio > 0 and _em > 0:
        if _ev_ratio < 0.5:
            ev_note = (
                f" \u26a0\ufe0f Strike proximity: stock only ${_req:.1f} away from strike "
                f"({_ev_ratio:.2f}\u00d7 10D expected move ${_em:.1f}). "
                f"Assignment risk is elevated — consider rolling up/out proactively "
                f"(Passarelli Ch.5: roll before ITM, not after)."
            )
        else:
            ev_note = (
                f" Strike ${_req:.1f} away ({_ev_ratio:.2f}\u00d7 10D expected ${_em:.1f}) "
                f"— low assignment probability, theta working as planned."
            )

    ev_override = False
    try:
        qty_ev = abs(safe_row_float(row, 'Quantity', default=1.0))
        dte_ev = max(float(dte), 1)
        # Use same cost hierarchy as BW: Net_Cost_Basis_Per_Share first
        net_cost_ev = safe_row_float(row, 'Net_Cost_Basis_Per_Share')
        broker_basis = abs(safe_row_float(row, 'Basis'))
        qty_abs = abs(safe_row_float(row, 'Quantity', default=1.0))
        broker_cost_per_share = (broker_basis / qty_abs) if qty_abs > 0 and broker_basis > 0 else 0.0
        if net_cost_ev > 0:
            effective_cost_ev = net_cost_ev
        elif broker_cost_per_share > 0:
            effective_cost_ev = broker_cost_per_share
        else:
            effective_cost_ev = safe_row_float(row, 'Underlying_Price_Entry') or spot

        ev = compare_actions_bw(
            row, spot=spot, strike=strike,
            effective_cost=effective_cost_ev, qty=qty_ev, dte=dte_ev,
        )
        # Store EV fields regardless of override
        result["Action_EV_Ranking"] = ev["ranked_actions"]
        result["Action_EV_Winner"] = ev["ev_winner"]
        result["Action_EV_Margin"] = ev["ev_margin"]
        result["Action_EV_Hold"] = ev["ev_hold"]
        result["Action_EV_Roll"] = ev["ev_roll"]
        result["Action_EV_Assign"] = ev["ev_assign"]
        result["Action_EV_Buyback"] = ev["ev_buyback"]
        result["EV_Buyback_Trigger"] = ev["ev_buyback_trigger"]
        result["Gamma_Drag_Daily"] = ev["gamma_drag_daily"]

        ev_winner = ev["ev_winner"]
        ev_margin = ev["ev_margin"]
        ev_summary = ev["ev_summary"]
        thesis_ok = not result.get("_thesis_blocks_roll", False)

        if thesis_ok and ev_margin >= EV_NOISE_FLOOR_INCOME:
            if ev_winner == "ROLL":
                fire_gate(
                    result,
                    action="ROLL",
                    urgency="MEDIUM",
                    rationale=(
                        f"EV decision: ROLL dominates HOLD by ${ev_margin:,.0f} "
                        f"over {dte_ev:.0f}d horizon. "
                        f"Theta carry insufficient vs roll credit opportunity. "
                        f"{ev_summary}  "
                        f"(Passarelli Ch.6: roll when EV favors)"
                    ),
                    doctrine_source="ActionEV: ROLL > HOLD (CC)",
                )
                ev_override = True
    except Exception as ev_err:
        logger.debug(f"CC EV comparator error (non-fatal): {ev_err}")

    # ── Gate 10: Default HOLD LOW ─────────────────────────────────────
    if not ev_override:
        result.update({
            "Action": "HOLD",
            "Urgency": "LOW",
            "Rationale": (
                f"Call OTM ({moneyness}), DTE={dte:.0f} — theta working as intended."
                f"{iv_note}{ev_note} No action required (McMillan Ch.2)."
            ),
            "Doctrine_Source": "McMillan Ch.2: Neutral Maintenance",
            "Decision_State": STATE_NEUTRAL_CONFIDENT,
            "Required_Conditions_Met": True,
        })

    # ── WEAKENING annotation (non-blocking) ───────────────────────────
    if ei_state == 'WEAKENING' and ei_reason:
        result['Rationale'] = (
            result.get('Rationale', '') +
            f"  [\u26a0\ufe0f Equity WEAKENING: {ei_reason} — monitor closely before next roll.]"
        )

    return result


# ── LEAPS carry inversion helpers (CC-specific) ──────────────────────

def _gate_leaps_carry_broken(
    *,
    row: pd.Series,
    spot: float,
    dte: float,
    ei_reason: str,
    result: Dict[str, Any],
) -> "Dict[str, Any] | None":
    """
    LEAPS carry inversion when equity is BROKEN.

    Returns the updated result dict if gate fired, None otherwise.
    Given Ch.6: sell calls within one strike of ATM only.

    Prefers pre-computed ``Daily_Margin_Cost`` from MarginCarryCalculator
    (correct: borrowed portion only, excludes retirement/options).
    Falls back to ``spot × rate`` if column not yet populated.
    """
    # Prefer pre-computed Daily_Margin_Cost (per-contract: value is already $/day)
    _precomputed = float(row.get('Daily_Margin_Cost') or 0.0)
    if _precomputed > 0:
        margin_daily = _precomputed / 100.0  # convert $/day to $/share/day
    else:
        margin_daily = spot * FIDELITY_MARGIN_RATE_DAILY if spot > 0 else 0.0
    theta_daily = abs(safe_row_float(row, 'Theta'))
    _strike = safe_row_float(row, 'Strike') or 0.0
    pct_otm = abs(spot - _strike) / spot if spot > 0 and _strike > 0 else 0.0

    if margin_daily <= 0 or theta_daily <= 0 or margin_daily < theta_daily:
        return None

    ratio = margin_daily / theta_daily

    if ratio >= CARRY_INVERSION_SEVERE:
        return fire_gate(
            result,
            action="BUYBACK",
            urgency="HIGH",
            rationale=(
                f"\u26a0\ufe0f Equity BROKEN + LEAPS carry severely inverted: "
                f"margin cost ${margin_daily*100:.2f}/contract/day vs theta income "
                f"${theta_daily*100:.2f}/contract/day ({ratio:.1f}\u00d7 theta). "
                f"Strike ${_strike:.0f} is {pct_otm:.0%} OTM — theta too weak "
                f"to cover financing. Buy back the short call. "
                f"Do NOT re-sell while equity is BROKEN ({ei_reason}) — hold stock "
                f"unencumbered until structural deterioration resolves. "
                f"(Given Ch.6: 'sell calls within one strike of ATM'; "
                f"Jabbour Ch.11: 'close and re-evaluate rather than rolling a losing structure')"
            ),
            doctrine_source="Given Ch.6: LEAPS Carry Inversion (severe) + EquityIntegrity: BROKEN (CC)",
        )[1]

    # Mild inversion (1.0x - 1.5x)
    net_bleed = (margin_daily - theta_daily) * 100
    return fire_gate(
        result,
        action="HOLD",
        urgency="HIGH",
        rationale=(
            f"\u26a0\ufe0f Equity BROKEN + LEAPS carry mildly inverted: "
            f"margin ${margin_daily*100:.2f}/day vs theta "
            f"${theta_daily*100:.2f}/day ({ratio:.1f}\u00d7 theta, "
            f"net bleed ${net_bleed:.2f}/day). "
            f"Buying back the call would increase bleed to "
            f"${margin_daily*100:.2f}/day with zero income. "
            f"HOLD — the short call still offsets most margin cost. "
            f"Monitor: if ratio exceeds {CARRY_INVERSION_SEVERE}\u00d7 or equity recovers to re-sell "
            f"closer to ATM (Given Ch.6). Strike ${_strike:.0f} "
            f"is {pct_otm:.0%} OTM."
        ),
        doctrine_source="Given Ch.6: LEAPS Carry Mild Inversion + EquityIntegrity: BROKEN (CC)",
    )[1]


def _gate_leaps_carry_non_broken(
    *,
    row: pd.Series,
    spot: float,
    dte: float,
    result: Dict[str, Any],
) -> "Dict[str, Any] | None":
    """
    LEAPS carry inversion when equity is NOT broken.

    Returns the updated result dict if gate fired, None otherwise.
    Given Ch.6: re-sell closer to ATM for efficient carry.

    Prefers pre-computed ``Daily_Margin_Cost`` from MarginCarryCalculator
    (correct: borrowed portion only, excludes retirement/options).
    Falls back to ``spot × rate`` if column not yet populated.
    """
    _precomputed = float(row.get('Daily_Margin_Cost') or 0.0)
    if _precomputed > 0:
        margin_daily = _precomputed / 100.0  # convert $/day to $/share/day
    else:
        margin_daily = spot * FIDELITY_MARGIN_RATE_DAILY if spot > 0 else 0.0
    theta_daily = abs(safe_row_float(row, 'Theta'))
    _strike = safe_row_float(row, 'Strike') or 0.0
    pct_otm = abs(spot - _strike) / spot if spot > 0 and _strike > 0 else 0.0

    if margin_daily <= 0 or theta_daily <= 0 or margin_daily < theta_daily:
        return None

    ratio = margin_daily / theta_daily
    return fire_gate(
        result,
        action="ROLL",
        urgency="MEDIUM",
        rationale=(
            f"\U0001f4ca LEAPS carry inverted: margin ${margin_daily*100:.2f}/contract/day \u2265 "
            f"theta ${theta_daily*100:.2f}/contract/day ({ratio:.1f}\u00d7 theta). "
            f"Strike ${_strike:.0f} is {pct_otm:.0%} OTM — theta decays too slowly "
            f"at this distance to cover financing at 10.375%/yr. "
            f"Buy back and re-sell 30\u201345 DTE closer to ATM for efficient carry "
            f"(Given Ch.6: 'one strike from ATM'; "
            f"Augen: 'roll when the new position has similar dynamics')."
        ),
        doctrine_source="Given Ch.6: LEAPS Carry Inversion — re-sell closer (CC)",
    )[1]


# ── v2: Proposal-based evaluation ────────────────────────────────────────────

def _shared_gate_to_proposal(
    collector: ProposalCollector,
    gate_name: str,
    fired: bool,
    result: Dict[str, Any],
    priority: int,
    is_hard_veto: bool = False,
) -> None:
    """Convert a shared gate's (fired, result) output into a proposal if fired."""
    if not fired:
        return
    propose_gate(
        collector,
        gate_name,
        action=result.get("Action", "HOLD"),
        urgency=result.get("Urgency", "MEDIUM"),
        rationale=result.get("Rationale", ""),
        doctrine_source=result.get("Doctrine_Source", ""),
        priority=priority,
        is_hard_veto=is_hard_veto,
        exit_trigger_type=result.get("Exit_Trigger_Type", ""),
    )


def covered_call_doctrine_v2(row: pd.Series, result: Dict[str, Any]) -> Dict[str, Any]:
    """Proposal-based COVERED_CALL evaluation.

    All gates propose actions into a ProposalCollector instead of returning
    immediately. A resolver picks the best action using deterministic EV
    and MC evidence.

    Original ``covered_call_doctrine()`` is preserved unchanged for A/B testing.
    """
    collector = ProposalCollector()

    # ── Extract fields (same as v1) ──────────────────────────────────────
    delta = abs(safe_row_float(row, 'Short_Call_Delta', 'Delta'))
    _dte_raw_v2 = row.get('Short_Call_DTE')
    if not pd.notna(_dte_raw_v2):
        _dte_raw_v2 = row.get('DTE')
    if not pd.notna(_dte_raw_v2):
        _dte_raw_v2 = 999
    dte = float(_dte_raw_v2)
    moneyness = row.get('Moneyness_Label', 'OTM')
    lifecycle = row.get('Lifecycle_Phase', 'ACTIVE')
    drift_dir = row.get('Drift_Direction', '')
    drift_mag = row.get('Drift_Magnitude', '')

    spot = abs(safe_row_float(row, 'UL Last', 'Spot'))
    strike = safe_row_float(row, 'Strike')
    ei_state = str(row.get('Equity_Integrity_State', '') or '').strip()
    ei_reason = str(row.get('Equity_Integrity_Reason', '') or '').strip()

    theta = abs(safe_row_float(row, 'Theta'))
    gamma = abs(safe_row_float(row, 'Gamma'))
    hv_20d = safe_row_float(row, 'HV_20D', default=0.20)
    gamma_roc_3d = safe_row_float(row, 'Gamma_ROC_3D')

    # ── Gate 1: Underlying collapse (HARD VETO) ─────────────────────────
    if drift_mag == 'High' and drift_dir == 'Down':
        propose_gate(
            collector, "underlying_collapse",
            action="EXIT", urgency="CRITICAL",
            rationale=(
                "Underlying price collapse — call premium insufficient to offset stock loss. "
                "Exit both legs (McMillan Ch.2: Hard Stop)."
            ),
            doctrine_source="McMillan Ch.2: Hard Stop",
            priority=1, is_hard_veto=True, exit_trigger_type="CAPITAL",
        )

    # ── Gate 0b: Consecutive debit roll hard stop (shared) ──────────────
    _r0b = result.copy()
    fired_0b, _r0b = gate_consecutive_debit_roll_stop(
        row=row, result=_r0b, strategy_label="CC",
    )
    _shared_gate_to_proposal(
        collector, "consecutive_debit_roll_stop", fired_0b, _r0b,
        priority=2, is_hard_veto=True,
    )

    # ── Gate 0c: Fading winner trailing protection (shared) ─────────────
    _cc_pnl = safe_pnl_pct(row)
    _r0c = result.copy()
    fired_0c, _r0c = gate_fading_winner(
        row=row, pnl_pct=_cc_pnl if _cc_pnl is not None else 0.0,
        result=_r0c, strategy_label="CC",
    )
    _shared_gate_to_proposal(
        collector, "fading_winner", fired_0c, _r0c,
        priority=3,
    )

    # ── Gate 0d: Income TRIM — wave-phase partial close (shared) ─────────
    from core.management.cycle3.doctrine.shared_income_gates import gate_income_trim
    _wave_cc = str(row.get('WavePhase_State', '') or '').upper()
    _conv_cc = str(row.get('Conviction_Status', '') or '').upper()
    _qty_cc = abs(int(float(
        row.get('Base_Quantity') or row.get('Entry_Quantity') or row.get('Quantity', 1)
    )))
    _r0d = result.copy()
    fired_0d, _r0d = gate_income_trim(
        row=row, pnl_pct=_cc_pnl if _cc_pnl is not None else 0.0,
        wave_phase=_wave_cc, conviction_status=_conv_cc,
        quantity=_qty_cc, result=_r0d, strategy_label="CC",
    )
    _shared_gate_to_proposal(
        collector, "income_trim", fired_0d, _r0d,
        priority=4,
    )

    # ── Gate 1a: Post-BUYBACK sticky (shared) ───────────────────────────
    _r1a = result.copy()
    fired_1a, _r1a = gate_post_buyback_sticky(row=row, spot=spot, result=_r1a)
    _shared_gate_to_proposal(collector, "post_buyback_sticky", fired_1a, _r1a, priority=10)

    # ── Gate 1b: Gamma Danger Zone (shared) ──────────────────────────────
    _r1b = result.copy()
    fired_1b, _r1b = gate_gamma_danger_zone(
        spot=spot, strike=strike, dte=dte, theta=theta, gamma=gamma,
        hv_20d=hv_20d, gamma_roc_3d=gamma_roc_3d, ei_state=ei_state,
        strategy_label="CC", result=_r1b,
    )
    _shared_gate_to_proposal(collector, "gamma_danger_zone", fired_1b, _r1b, priority=20)

    # ── Gate 1c-1g: Equity BROKEN gates ──────────────────────────────────
    if ei_state == 'BROKEN':
        # 1c: BROKEN + gamma conviction
        _r1c = result.copy()
        fired_1c, _r1c = gate_equity_broken_gamma_conviction(
            row=row, spot=spot, strike=strike, dte=dte, theta=theta,
            gamma=gamma, hv_20d=hv_20d, ei_state=ei_state,
            ei_reason=ei_reason, result=_r1c,
        )
        _shared_gate_to_proposal(collector, "equity_broken_gamma_conviction", fired_1c, _r1c, priority=30)

        # 1d: BROKEN + gamma no conviction
        from core.shared.finance_utils import effective_cost_per_share as _ecp
        effective_cost, _, _ = _ecp(row, spot_fallback=spot)
        cum_premium = abs(safe_row_float(row, 'Cumulative_Premium_Collected'))

        _r1d = result.copy()
        fired_1d, _r1d = gate_equity_broken_gamma_no_conviction(
            row=row, spot=spot, strike=strike, dte=dte, theta=theta,
            gamma=gamma, hv_20d=hv_20d, effective_cost=effective_cost,
            cum_premium=cum_premium, ei_state=ei_state,
            ei_reason=ei_reason, result=_r1d,
        )
        _shared_gate_to_proposal(collector, "equity_broken_gamma_no_conviction", fired_1d, _r1d, priority=31)

        # 1e-1f: BROKEN + LEAPS carry inversion
        if dte > DTE_LEAPS_THRESHOLD:
            _r1ef = result.copy()
            result_leaps = _gate_leaps_carry_broken(
                row=row, spot=spot, dte=dte, ei_reason=ei_reason, result=_r1ef,
            )
            if result_leaps is not None:
                propose_gate(
                    collector, "leaps_carry_broken",
                    action=result_leaps.get("Action", "HOLD"),
                    urgency=result_leaps.get("Urgency", "HIGH"),
                    rationale=result_leaps.get("Rationale", ""),
                    doctrine_source=result_leaps.get("Doctrine_Source", ""),
                    priority=32,
                )

        # 1g: Standard BROKEN — no gamma dominance, not LEAPS
        propose_gate(
            collector, "equity_broken_standard",
            action="HOLD", urgency="HIGH",
            rationale=(
                f"⚠️ Equity Integrity BROKEN — structural deterioration detected "
                f"({ei_reason}). "
                f"Rolling locks in deeper commitment to a structurally declining stock. "
                f"Hold and reassess before next roll "
                f"(McMillan Ch.1: confirm trend context before committing further capital)."
            ),
            doctrine_source="EquityIntegrity: BROKEN structural gate (CC)",
            priority=35,
        )

    # ── Gate 1h: LEAPS carry inversion (non-BROKEN) ─────────────────────
    if dte > DTE_LEAPS_THRESHOLD and ei_state != 'BROKEN':
        _r1h = result.copy()
        result_leaps = _gate_leaps_carry_non_broken(
            row=row, spot=spot, dte=dte, result=_r1h,
        )
        if result_leaps is not None:
            propose_gate(
                collector, "leaps_carry_non_broken",
                action=result_leaps.get("Action", "ROLL"),
                urgency=result_leaps.get("Urgency", "MEDIUM"),
                rationale=result_leaps.get("Rationale", ""),
                doctrine_source=result_leaps.get("Doctrine_Source", ""),
                priority=33,
            )

    # ── Gate 1i: Carry inversion (all DTE, pre-computed) ─────────────────
    from core.management.cycle3.doctrine.shared_income_gates import gate_carry_inversion_roll
    _r1i = result.copy()
    fired_1i, _r1i = gate_carry_inversion_roll(row=row, result=_r1i)
    _shared_gate_to_proposal(collector, "carry_inversion_roll", fired_1i, _r1i, priority=34)

    # ── Gate 2: Dividend assignment (shared) ─────────────────────────────
    _r2 = result.copy()
    fired_2, _r2 = gate_dividend_assignment(row=row, delta=delta, result=_r2)
    _shared_gate_to_proposal(collector, "dividend_assignment", fired_2, _r2, priority=15)

    # ── Gate 3: ITM/ATM Late lifecycle ───────────────────────────────────
    _is_behaviorally_itm = (moneyness == 'ITM') or (moneyness == 'ATM' and delta > DELTA_BEHAVIORAL_ITM)
    if lifecycle == 'TERMINAL' and _is_behaviorally_itm:
        propose_gate(
            collector, "itm_late_lifecycle",
            action="ROLL", urgency="HIGH",
            rationale=(
                f"Call ITM/near-ITM (Delta={delta:.2f}) with DTE={dte:.0f} — "
                f"assignment risk imminent. Roll up/out or accept assignment "
                f"if stock thesis intact (McMillan Ch.2)."
            ),
            doctrine_source="McMillan Ch.2: Expiration Management",
            priority=40,
        )

    # ── Gate 4: Dual-stage delta guard ───────────────────────────────────
    if delta > DELTA_ITM_EMERGENCY:
        propose_gate(
            collector, "delta_emergency",
            action="ROLL", urgency="HIGH",
            rationale=(
                f"🛑 Call delta={delta:.2f} > {DELTA_ITM_EMERGENCY} — "
                f"moving nearly dollar-for-dollar with stock. "
                f"Income purpose of covered call is eliminated; assignment highly probable. "
                f"BUY BACK the call and roll up/out immediately to restore income structure "
                f"(Passarelli Ch.5: Uncap Upside; McMillan Ch.2: delta > 0.70 = emergency roll)."
            ),
            doctrine_source="Passarelli Ch.5: Uncap Upside (delta > 0.70 — emergency)",
            priority=42,
        )
    elif delta > DELTA_PRE_ITM_WARNING:
        propose_gate(
            collector, "delta_warning",
            action="ROLL", urgency="MEDIUM",
            rationale=(
                f"⚠️ Call delta={delta:.2f} > {DELTA_PRE_ITM_WARNING} — "
                f"upside cap becoming material. "
                f"Early warning: roll up/out now while the debit-to-close is still manageable "
                f"and the roll premium is favorable. Acting at {DELTA_PRE_ITM_WARNING} "
                f"rather than {DELTA_ITM_EMERGENCY} preserves more premium in the new cycle "
                f"(McMillan Ch.2: early roll before deeply ITM; "
                f"Passarelli Ch.5: dual-stage delta gate)."
            ),
            doctrine_source=f"Passarelli Ch.5 + McMillan Ch.2: Early Warning (delta {DELTA_PRE_ITM_WARNING}–{DELTA_ITM_EMERGENCY})",
            priority=45,
        )

    # ── Gate 5: DTE ≤ 7 pin risk ────────────────────────────────────────
    try:
        _dte_float = float(dte)
    except (TypeError, ValueError):
        _dte_float = 999.0

    if _dte_float <= DTE_EMERGENCY_ROLL:
        thesis_addendum = ""
        thesis_check = check_thesis_degradation(row)
        if thesis_check:
            thesis_addendum = f" Note: {thesis_check['text']}."
        propose_gate(
            collector, "pin_risk_dte7",
            action="ROLL", urgency="HIGH",
            rationale=(
                f"DTE={_dte_float:.0f} — expiration imminent. "
                f"Pin risk and gamma acceleration require rolling now. "
                f"Roll to next cycle before time value collapses entirely."
                + thesis_addendum
                + " (McMillan Ch.3: Expiration Management)."
            ),
            doctrine_source="McMillan Ch.3: Expiration Management",
            priority=25, exit_trigger_type="GAMMA",
        )

    # ── Gate 6: 21-DTE income gate ──────────────────────────────────────
    pe_21 = abs(safe_row_float(row, 'Premium_Entry', 'Premium'))
    last_21 = abs(safe_row_float(row, 'Last'))
    captured_21 = (pe_21 - last_21) / pe_21 if pe_21 > 0 else 0.0
    moneyness_21 = str(row.get('Moneyness_Label') or row.get('Short_Call_Moneyness') or 'OTM')

    # Far-OTM exemption: don't roll far-OTM calls near expiry — let them expire
    _mc_p_assign_cc = safe_row_float(row, 'MC_Assign_P_Expiry')
    _far_otm_cc = (
        delta < 0.30
        and (_mc_p_assign_cc < 0.05 if not (isinstance(_mc_p_assign_cc, float) and _mc_p_assign_cc != _mc_p_assign_cc) else delta < 0.25)
    )

    if (
        dte <= DTE_INCOME_GATE
        and dte >= DTE_EMERGENCY_ROLL
        and captured_21 < PREMIUM_CAPTURE_TARGET
        and moneyness_21 != 'ITM'
        and not _far_otm_cc
    ):
        urgency_21 = 'MEDIUM' if captured_21 >= 0 else 'HIGH'
        propose_gate(
            collector, "income_gate_21dte",
            action="ROLL", urgency=urgency_21,
            rationale=(
                f"21-DTE income gate: DTE={dte:.0f} ≤ {DTE_INCOME_GATE} with only "
                f"{captured_21:.0%} profit captured (need ≥{PREMIUM_CAPTURE_TARGET:.0%}). "
                f"Gamma-theta ratio has degraded — short call edge exhausted. "
                f"Buy back current call and roll out to 30-45 DTE to reset cycle. "
                f"(Given Ch.6: 21-DTE income roll; Passarelli Ch.2: theta/gamma degradation.)"
            ),
            doctrine_source="Given Ch.6 + Passarelli Ch.2: 21-DTE Income Roll Gate (CC)",
            priority=50, exit_trigger_type="INCOME",
        )
    elif (
        _far_otm_cc
        and dte <= DTE_INCOME_GATE
        and dte >= DTE_EMERGENCY_ROLL
        and captured_21 < PREMIUM_CAPTURE_TARGET
        and moneyness_21 != 'ITM'
    ):
        _remaining_theta_cc = abs(safe_row_float(row, 'Theta')) * 100 * dte
        propose_gate(
            collector, "income_gate_21dte_far_otm_hold",
            action="HOLD", urgency="LOW",
            rationale=(
                f"21-DTE income gate: DTE={dte:.0f}, {captured_21:.0%} captured, "
                f"but call is far OTM (Δ {delta:.3f}, P(assign)={_mc_p_assign_cc:.0%}). "
                f"Remaining θ income ${_remaining_theta_cc:.0f} to expiry — "
                f"let call expire worthless and sell new cycle after. "
                f"(McMillan Ch.3: far-OTM expiry is the profit mechanism.)"
            ),
            doctrine_source="McMillan Ch.3: Far-OTM expiry — let theta work (CC)",
            priority=50,
        )

    # ── Gate 7: 50% premium capture ─────────────────────────────────────
    premium_entry = abs(safe_row_float(row, 'Premium_Entry', 'Premium'))
    current_cost = abs(safe_row_float(row, 'Last'))
    if (
        premium_entry > 0
        and current_cost <= premium_entry * PREMIUM_CAPTURE_TARGET
        and dte > DTE_INCOME_GATE
    ):
        pct_captured = 1 - (current_cost / premium_entry)
        propose_gate(
            collector, "premium_capture",
            action="ROLL", urgency="MEDIUM",
            rationale=(
                f"50% premium captured ({pct_captured:.0%} of ${premium_entry:.2f} entry credit) "
                f"with {dte:.0f} DTE remaining — close and redeploy into next cycle "
                f"(Passarelli Ch.6: 50% Rule)."
            ),
            doctrine_source="Passarelli Ch.6: 50% Rule",
            priority=55, exit_trigger_type="INCOME",
        )

    # ── Gate 8: Thesis regime degradation ────────────────────────────────
    thesis = check_thesis_degradation(row)
    if thesis:
        propose_gate(
            collector, "thesis_degradation",
            action="ROLL", urgency="MEDIUM",
            rationale=(
                f"Entry regime degraded: {thesis['text']}. "
                f"Reassess strike/expiry — original setup no longer intact "
                f"(McMillan Ch.2: Thesis Persistence)."
            ),
            doctrine_source="McMillan Ch.2: Thesis Persistence",
            priority=60,
        )

    # ── Gate 9: EV comparator (always evaluates) ────────────────────────
    ev_result = None
    try:
        qty_ev = abs(safe_row_float(row, 'Quantity', default=1.0))
        dte_ev = max(float(dte), 1)
        # Use same cost hierarchy as BW: Net_Cost_Basis_Per_Share first
        net_cost_ev = safe_row_float(row, 'Net_Cost_Basis_Per_Share')
        broker_basis = abs(safe_row_float(row, 'Basis'))
        qty_abs = abs(safe_row_float(row, 'Quantity', default=1.0))
        broker_cost_per_share = (broker_basis / qty_abs) if qty_abs > 0 and broker_basis > 0 else 0.0
        if net_cost_ev > 0:
            effective_cost_ev = net_cost_ev
        elif broker_cost_per_share > 0:
            effective_cost_ev = broker_cost_per_share
        else:
            effective_cost_ev = safe_row_float(row, 'Underlying_Price_Entry') or spot

        ev_result = compare_actions_bw(
            row, spot=spot, strike=strike,
            effective_cost=effective_cost_ev, qty=qty_ev, dte=dte_ev,
        )
        # Store EV fields in result for downstream display
        result["Action_EV_Ranking"] = ev_result["ranked_actions"]
        result["Action_EV_Winner"] = ev_result["ev_winner"]
        result["Action_EV_Margin"] = ev_result["ev_margin"]
        result["Action_EV_Hold"] = ev_result["ev_hold"]
        result["Action_EV_Roll"] = ev_result["ev_roll"]
        result["Action_EV_Assign"] = ev_result["ev_assign"]
        result["Action_EV_Buyback"] = ev_result["ev_buyback"]
        result["EV_Buyback_Trigger"] = ev_result["ev_buyback_trigger"]
        result["Gamma_Drag_Daily"] = ev_result["gamma_drag_daily"]

        # ── Timing quality adjustments ────────────────────────────────────
        _timing_roll_adj = 1.0
        _timing_hold_bonus = 0.0
        _timing_notes = []

        _tq_iv_now = safe_row_float(row, 'IV_Now', 'IV_30D')
        _tq_hv = safe_row_float(row, 'HV_20D')
        if _tq_hv >= 5.0:
            _tq_hv /= 100.0
        if _tq_iv_now >= 5.0:
            _tq_iv_now /= 100.0
        _tq_iv_hv_ratio = _tq_iv_now / _tq_hv if _tq_hv > 0.01 else 1.0
        if _tq_iv_hv_ratio < 0.95:
            _iv_discount = max(0.50, _tq_iv_hv_ratio)
            _timing_roll_adj *= _iv_discount
            _timing_notes.append(
                f"IV depressed ({_tq_iv_now:.0%}/{_tq_hv:.0%}={_tq_iv_hv_ratio:.2f}): "
                f"ROLL EV ×{_iv_discount:.2f}"
            )

        _tq_days_macro = safe_row_float(row, 'Days_To_Macro', default=999.0)
        _tq_macro_type = str(row.get('Macro_Next_Type', '') or '').upper()
        _tq_macro_event = str(row.get('Macro_Next_Event', '') or '')
        _tq_macro_high = _tq_macro_type in ('FOMC', 'CPI', 'NFP')
        if _tq_macro_high and _tq_days_macro <= 3:
            _timing_roll_adj *= 0.70
            _timing_hold_bonus = abs(ev_result.get('ev_roll', 0)) * 0.20
            _timing_notes.append(
                f"Macro {_tq_macro_event} in {_tq_days_macro:.0f}d: "
                f"ROLL EV ×0.70 + HOLD bonus ${_timing_hold_bonus:,.0f}"
            )
        elif _tq_macro_high and _tq_days_macro <= 5:
            _timing_roll_adj *= 0.85
            _timing_notes.append(f"Macro {_tq_macro_event} in {_tq_days_macro:.0f}d: ROLL EV ×0.85")

        _tq_squeeze = bool(row.get('Keltner_Squeeze_On', False))
        if _tq_squeeze:
            _timing_roll_adj *= 0.85
            _timing_notes.append("Keltner squeeze ON: ROLL EV ×0.85")

        # (d) Debit roll history: consecutive debit rolls erode income edge.
        _tq_consec_debits = int(safe_row_float(row, 'Trajectory_Consecutive_Debit_Rolls'))
        _tq_emergency = delta > DELTA_PRE_ITM_WARNING or dte < DTE_CUSHION_WINDOW
        if _tq_consec_debits >= 2 and not _tq_emergency:
            _debit_discount = 0.80 if _tq_consec_debits >= 3 else 0.90
            _timing_roll_adj *= _debit_discount
            _timing_notes.append(
                f"{_tq_consec_debits} consecutive debit roll(s): ROLL EV "
                f"discounted ×{_debit_discount:.2f} — income edge eroding "
                f"(McMillan Ch.3: debit rolls signal structural headwind)"
            )

        if _timing_roll_adj < 1.0:
            ev_result['ev_roll'] = ev_result.get('ev_roll', 0) * _timing_roll_adj
            ev_result['ev_buyback'] = ev_result.get('ev_buyback', 0) * max(_timing_roll_adj, 0.90)
            if _timing_hold_bonus > 0:
                ev_result['ev_hold'] = ev_result.get('ev_hold', 0) + _timing_hold_bonus
            _adj_evs = {
                'HOLD': ev_result.get('ev_hold', 0),
                'ROLL': ev_result.get('ev_roll', 0),
                'ASSIGN': ev_result.get('ev_assign', 0),
                'BUYBACK': ev_result.get('ev_buyback', 0),
            }
            ev_result['ev_winner'] = max(_adj_evs, key=_adj_evs.get)
            ev_result['ev_summary'] = (
                f"Action EV (over {max(dte,1):.0f}d): "
                + " | ".join(f"{a} {'+' if v >= 0 else ''}{v:,.0f}"
                             for a, v in [('HOLD', ev_result['ev_hold']),
                                          ('ROLL', ev_result['ev_roll']),
                                          ('ASSIGN', ev_result['ev_assign']),
                                          ('BUYBACK', ev_result['ev_buyback'])])
                + f" → **{ev_result['ev_winner']}** wins"
                + (f" [Timing: {'; '.join(_timing_notes)}]" if _timing_notes else "")
            )
            logger.debug(f"[CC_v2] Timing quality: roll_adj={_timing_roll_adj:.2f}, new_winner={ev_result['ev_winner']}")

        # Add EV-backed proposals for each action
        _ev_winner_action = ev_result.get("ev_winner", "")

        # ASSIGN guard: don't propose ASSIGN when assignment is improbable
        _p_assign_ev = abs(safe_row_float(row, 'MC_Assign_P_Expiry', 'Short_Call_Delta', 'Delta'))
        if _ev_winner_action == "ASSIGN" and _p_assign_ev < 0.20:
            _ev_winner_action = "HOLD"
            ev_result["ev_winner"] = "HOLD"

        # Map internal ASSIGN to user-facing label based on moneyness
        if _ev_winner_action == "ASSIGN":
            if delta >= 0.50:
                _ev_winner_action = "ACCEPT_CALL_AWAY"
            else:
                _ev_winner_action = "LET_EXPIRE"
            ev_result["ev_winner"] = _ev_winner_action
            # Update summary text so user sees the mapped label, not internal "ASSIGN"
            ev_result["ev_summary"] = ev_result.get("ev_summary", "").replace("**ASSIGN**", f"**{_ev_winner_action}**").replace("ASSIGN ", f"{_ev_winner_action} ")

        # ROLL guard: don't propose ROLL when thesis blocks it.
        # Exception: deep ITM — ROLL is defensive (preventing assignment).
        if (_ev_winner_action == "ROLL"
                and result.get('_thesis_blocks_roll', False)
                and delta < DELTA_ITM_EMERGENCY):
            _ev_winner_action = "HOLD"
            ev_result["ev_winner"] = "HOLD"

        for action_name, ev_key in [("HOLD", "ev_hold"), ("ROLL", "ev_roll"),
                                     ("ASSIGN", "ev_assign"), ("BUYBACK", "ev_buyback")]:
            ev_val = ev_result.get(ev_key, 0.0)
            # Map ASSIGN to user-facing label for the winning proposal
            if action_name == "ASSIGN" and _ev_winner_action in ("LET_EXPIRE", "ACCEPT_CALL_AWAY"):
                propose_gate(
                    collector, f"ev_comparator_{_ev_winner_action.lower()}",
                    action=_ev_winner_action, urgency="MEDIUM",
                    rationale=(
                        f"EV decision: {_ev_winner_action} wins with ${ev_val:,.0f} over "
                        f"{dte_ev:.0f}d. {ev_result['ev_summary']}"
                    ),
                    doctrine_source=f"ActionEV: {_ev_winner_action} winner (CC)",
                    priority=70, ev_estimate=ev_val,
                )
            elif action_name == _ev_winner_action:
                propose_gate(
                    collector, f"ev_comparator_{action_name.lower()}",
                    action=action_name, urgency="MEDIUM",
                    rationale=(
                        f"EV decision: {action_name} wins with ${ev_val:,.0f} over "
                        f"{dte_ev:.0f}d. {ev_result['ev_summary']}"
                    ),
                    doctrine_source=f"ActionEV: {action_name} winner (CC)",
                    priority=70, ev_estimate=ev_val,
                )
    except Exception as ev_err:
        logger.debug(f"CC v2 EV comparator error (non-fatal): {ev_err}")

    # ── Gate 10: Default HOLD LOW (always present) ───────────────────────
    iv_shape = str(row.get('iv_surface_shape', '') or '').upper()
    iv_note = ""
    if iv_shape == 'BACKWARDATION':
        slope = safe_row_float(row, 'iv_ts_slope_30_90')
        iv_note = (
            f" IV BACKWARDATED ({slope:+.1f}pt): collecting elevated near-term IV "
            f"— premium received is above-normal (Natenberg Ch.11)."
        )

    propose_gate(
        collector, "default_hold",
        action="HOLD", urgency="LOW",
        rationale=(
            f"Call OTM ({moneyness}), DTE={dte:.0f} — theta working as intended."
            f"{iv_note} No action required (McMillan Ch.2)."
        ),
        doctrine_source="McMillan Ch.2: Neutral Maintenance",
        priority=100,
    )

    # ── Resolution ───────────────────────────────────────────────────────
    logger.debug(f"[CC_v2] {collector.summary()}")

    if collector.has_hard_veto():
        winner = collector.get_veto()
        return collector.to_result(winner, result, resolution_method="HARD_VETO")

    # Resolve via EV + MC
    if ev_result is not None:
        winner = resolve_income_proposals(collector, ev_result, row)
        resolved = collector.to_result(winner, result, resolution_method="EV_COMPARISON")
    else:
        # No EV data — fall back to highest urgency, lowest priority
        proposals_sorted = sorted(
            collector.proposals,
            key=lambda p: (-p.urgency_rank, p.priority),
        )
        winner = proposals_sorted[0]
        resolved = collector.to_result(winner, result, resolution_method="PRIORITY_FALLBACK")

    # ── Post-resolution recovery mode guard ────────────────────────────────
    # Detect recovery state: if position is deeply underwater but income
    # repair is viable, soften EXIT to HOLD with recovery context.
    from core.shared.finance_utils import effective_cost_per_share as _ecp
    _rc_effective_cost, _, _ = _ecp(row, spot_fallback=spot)
    _recovery = detect_recovery_state(row, spot=spot, effective_cost=_rc_effective_cost)
    _recovery_mode = _recovery["is_recovery"]

    if _recovery_mode and resolved.get('Action') == 'EXIT':
        # Only override non-structural EXITs (underlying_collapse is genuine emergency)
        _exit_gate = resolved.get('Winning_Gate', '')
        if _exit_gate != 'underlying_collapse':
            _rc = _recovery["context"]
            _rc_months = _rc.get("months_to_breakeven", float('inf'))
            _rc_net_mo = _rc.get("net_monthly", 0)
            _rc_prem = _rc.get("premium_collected_per_share", 0)
            _rc_months_str = (
                f" Net income ~${_rc_net_mo:.2f}/mo → ~{_rc_months:.0f} months to close gap."
                if _rc_net_mo > 0 and _rc_months < 999
                else ""
            )
            # Catalyst context
            _catalyst_parts = []
            _rc_days_earn = _rc.get("days_to_earnings", 999)
            if _rc_days_earn < 60:
                _earn_note = f"Earnings in {_rc_days_earn:.0f}d"
                if _rc.get("earnings_beat_rate", 0) > 0:
                    _earn_note += f" (beat {_rc['earnings_beat_rate']:.0%})"
                _catalyst_parts.append(_earn_note)
            _rc_days_macro = _rc.get("days_to_macro", 999)
            if _rc_days_macro < 10 and _rc.get("macro_event"):
                _catalyst_parts.append(f"{_rc['macro_event']} in {_rc_days_macro:.0f}d")
            if _rc.get("stock_basing"):
                _catalyst_parts.append(f"stock basing (ADX {_rc.get('adx', 0):.0f})")
            _rc_catalyst = (f" Next catalyst: {'; '.join(_catalyst_parts)}."
                            if _catalyst_parts else "")

            resolved['Action'] = 'HOLD'
            resolved['Urgency'] = 'MEDIUM'
            resolved['Rationale'] = (
                f"Recovery ladder active: ${_rc_prem:.2f}/sh collected. "
                f"EXIT suppressed — exit locks in {_rc['loss_pct']:.0%} permanent loss. "
                f"Hold call to expiry, reassess stock thesis.{_rc_months_str}"
                f"{_rc_catalyst} "
                f"(Jabbour Ch.4: Repair Strategies; McMillan Ch.3: Basis Reduction)"
            )
            resolved['Resolution_Method'] = 'RECOVERY_LADDER'

    # ── Post-resolution far-OTM guard ──────────────────────────────────────
    _post_delta = delta
    _post_p_assign = abs(safe_row_float(row, 'MC_Assign_P_Expiry'))
    _post_p_assign_valid = not (isinstance(_post_p_assign, float) and _post_p_assign != _post_p_assign)
    _post_far_otm = (
        _post_delta < 0.30
        and (_post_p_assign < 0.05 if _post_p_assign_valid else _post_delta < 0.25)
        and ei_state != 'BROKEN'
        and dte > DTE_INCOME_GATE
    )

    if _post_far_otm and resolved.get('Action') not in ('HOLD', 'EXIT'):
        resolved['Action'] = 'HOLD'
        resolved['Urgency'] = 'LOW'
        resolved['Rationale'] = (
            f"Far-OTM override: short call Δ {_post_delta:.3f} with P(assign) "
            f"{'< 5%' if _post_p_assign_valid else 'negligible (delta proxy)'}. "
            f"Position is collecting θ as designed — no action needed. "
            f"Original proposal ({winner.action} {winner.urgency}) suppressed. "
            f"(McMillan Ch.3: far-OTM covered call is pure income collection; "
            f"only structural capital damage warrants intervention.)"
        )
        resolved['Resolution_Method'] = 'FAR_OTM_OVERRIDE'
    elif _post_far_otm and resolved.get('Action') == 'EXIT' and resolved.get('Exit_Trigger_Type') != 'CAPITAL':
        resolved['Action'] = 'HOLD'
        resolved['Urgency'] = 'LOW'
        resolved['Rationale'] = (
            f"Far-OTM override: EXIT suppressed — call Δ {_post_delta:.3f}, "
            f"P(assign) negligible. Non-CAPITAL EXIT not justified when "
            f"position is structurally sound. (McMillan Ch.3)"
        )
        resolved['Resolution_Method'] = 'FAR_OTM_OVERRIDE'

    # ── Persistence urgency escalation ──────────────────────────────────
    # Exempt far-OTM positions — sustained drift below cost basis doesn't
    # threaten the short call when delta < 0.30 (McMillan Ch.3).
    # Exempt ASSIGN — passive posture, urgency escalation meaningless.
    _persist_far_otm = (
        _post_delta < 0.30
        and (_post_p_assign < 0.05 if _post_p_assign_valid else _post_delta < 0.25)
    )
    _drift_persist = str(row.get('Drift_Persistence', '') or '')
    if (_drift_persist == 'Sustained'
            and resolved.get('Action') not in ('HOLD', 'ASSIGN', 'LET_EXPIRE', 'ACCEPT_CALL_AWAY')
            and not _persist_far_otm):
        _drift_from_net = safe_row_float(row, 'Drift_From_Net_Cost')
        if _drift_from_net > 0:
            resolved['Urgency'] = 'HIGH'
            resolved['Exit_Trigger_Type'] = 'INCOME'
        else:
            resolved['Urgency'] = 'CRITICAL'
            resolved['Exit_Trigger_Type'] = 'CAPITAL'

    # ── WEAKENING annotation (non-blocking, same as v1) ──────────────────
    if ei_state == 'WEAKENING' and ei_reason:
        resolved['Rationale'] = (
            resolved.get('Rationale', '') +
            f"  [⚠️ Equity WEAKENING: {ei_reason} — monitor closely before next roll.]"
        )

    return resolved
