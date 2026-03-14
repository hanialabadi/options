"""
Multi-leg doctrine — straddles/strangles (long-volatility plays).

Extracted from DoctrineAuthority._multi_leg_doctrine (engine.py lines 6692-6934).
"""

import logging
from typing import Dict

import pandas as pd

from core.management.cycle3.doctrine.gate_result import (
    STATE_ACTIONABLE,
    STATE_NEUTRAL_CONFIDENT,
)
from core.management.cycle3.doctrine.helpers import check_thesis_degradation, safe_row_float
from core.management.cycle3.doctrine.proposal import ProposalCollector, propose_gate

logger = logging.getLogger(__name__)


def multi_leg_doctrine(row: pd.Series, result: Dict) -> Dict:
    # ── Calendar gates ──────────────────────────────────────────────────
    try:
        from scan_engine.calendar_context import expiry_proximity_flag
        _dte_ml   = safe_row_float(row, 'DTE', default=999.0)
        _ul_ml    = safe_row_float(row, 'UL Last')
        _strike_ml = safe_row_float(row, 'Strike')
        _strat_ml  = str(row.get('Strategy', '') or '').upper()
        _exp_flag_ml, _exp_note_ml = expiry_proximity_flag(
            dte=_dte_ml, strategy=_strat_ml,
            ul_last=_ul_ml, strike=_strike_ml,
        )
        if _exp_flag_ml == 'PIN_RISK':
            result.update({
                "Action": "EXIT",
                "Urgency": "CRITICAL",
                "Exit_Trigger_Type": "GAMMA",
                "Rationale": _exp_note_ml,
                "Doctrine_Source": "McMillan Ch.7 + Natenberg Ch.15: Pin Risk",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True
            })
            return result
        elif _exp_flag_ml == 'GAMMA_CRITICAL':
            result.update({
                "Action": "ROLL",
                "Urgency": "HIGH",
                "Rationale": _exp_note_ml,
                "Doctrine_Source": "Natenberg Ch.15: Gamma Critical",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True
            })
            return result
    except Exception as _ml_exp_err:
        logger.debug(f"Multi-leg expiration gate skipped: {_ml_exp_err}")

    # ── Thesis-aware routing ───────────────────────────────────────────
    # Straddles/strangles are vol expansion plays — direction-neutral
    vol_state = str(row.get('VolatilityState_State', 'UNKNOWN') or 'UNKNOWN')
    vol_state = vol_state.split('.')[-1].upper()
    price_drift = float(row.get('Price_Drift_Pct', 0) if pd.notna(row.get('Price_Drift_Pct')) else 0)
    dte = row.get('DTE', 999) or 999
    iv_now = safe_row_float(row, 'IV_Now')
    iv_entry = safe_row_float(row, 'IV_Entry')
    hv_20d = safe_row_float(row, 'HV_20D')
    pnl_pct = float(row.get('Total_GL_Decimal', 0) if pd.notna(row.get('Total_GL_Decimal')) else 0)

    # 1. Vol spike management: if vol has expanded significantly, partial profit
    if iv_entry > 0 and iv_now > 0 and iv_now / iv_entry > 1.50:
        result.update({
            "Action": "TRIM",
            "Urgency": "MEDIUM",
            "Rationale": (
                f"Vol expansion captured: IV moved from {iv_entry:.1%} to {iv_now:.1%} "
                f"({iv_now/iv_entry - 1:.0%} increase). Partial profit on vol spike. "
                f"Natenberg Ch.5: Capture vol profits when implied exceeds realized entry level."
            ),
            "Doctrine_Source": "Natenberg Ch.5: Vol Spike Profit Capture",
            "Decision_State": STATE_ACTIONABLE,
            "Required_Conditions_Met": True,
        })
        return result

    # 2. Profit target: 50% of premium paid
    if pnl_pct >= 0.50:
        result.update({
            "Action": "EXIT",
            "Urgency": "MEDIUM",
            "Rationale": (
                f"Profit target reached: {pnl_pct:.0%} gain (≥50%). "
                f"Take profits on vol expansion play (McMillan Ch.4: Profit Target)."
            ),
            "Doctrine_Source": "McMillan Ch.4: Straddle/Strangle Profit Target",
            "Decision_State": STATE_ACTIONABLE,
            "Required_Conditions_Met": True,
        })
        return result

    # 3. Theta bleed rate monitoring
    theta_bleed = safe_row_float(row, 'Theta_Bleed_Daily_Pct')
    if theta_bleed > 5.0 and vol_state not in ('EXPANDING', 'EXTREME'):
        result.update({
            "Action": "EXIT",
            "Urgency": "HIGH",
            "Rationale": (
                f"Theta bleed critical: {theta_bleed:.1f}%/day with vol {vol_state} "
                f"(not expanding). Double-theta on two legs is consuming premium rapidly. "
                f"Exit before theta acceleration destroys remaining value "
                f"(Passarelli Ch.2: straddle theta = 2× single leg)."
            ),
            "Doctrine_Source": "Passarelli Ch.2: Straddle Theta Critical",
            "Decision_State": STATE_ACTIONABLE,
            "Required_Conditions_Met": True,
        })
        return result

    # 4. Asymmetric leg management
    # If one leg is deep ITM and the other is worthless, consider closing the losing leg
    call_delta = abs(safe_row_float(row, 'Call_Delta'))
    put_delta = abs(safe_row_float(row, 'Put_Delta'))
    if call_delta > 0 and put_delta > 0:
        delta_ratio = max(call_delta, put_delta) / min(call_delta, put_delta)
        if delta_ratio > 5.0:
            _winning_leg = "call" if call_delta > put_delta else "put"
            _losing_leg = "put" if _winning_leg == "call" else "call"
            result.update({
                "Action": "TRIM",
                "Urgency": "LOW",
                "Rationale": (
                    f"Asymmetric legs: {_winning_leg} delta={max(call_delta, put_delta):.2f} vs "
                    f"{_losing_leg} delta={min(call_delta, put_delta):.2f} (ratio {delta_ratio:.1f}×). "
                    f"Consider closing {_losing_leg} leg to recover residual value and reduce theta "
                    f"(Natenberg Ch.5: asymmetric straddle management)."
                ),
                "Doctrine_Source": "Natenberg Ch.5: Asymmetric Leg Management",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True,
            })
            return result

    # 5. Time stop: ≤21 DTE for short-dated, theta acceleration destroys both legs
    if dte <= 21 and pnl_pct < 0:
        result.update({
            "Action": "EXIT",
            "Urgency": "HIGH",
            "Rationale": (
                f"Time stop: DTE={dte} ≤ 21, position down {abs(pnl_pct):.0%}. "
                f"Theta acceleration on BOTH legs = 2× decay rate in final weeks. "
                f"Exit to stop the bleed (Passarelli Ch.2: straddle time stop)."
            ),
            "Doctrine_Source": "Passarelli Ch.2: Straddle/Strangle Time Stop",
            "Decision_State": STATE_ACTIONABLE,
            "Required_Conditions_Met": True,
        })
        return result

    # 6. Vol mean-reversion detection: vol was high, now contracting
    if vol_state in ('CONTRACTING', 'NORMAL') and iv_entry > 0 and iv_now > 0 and iv_now < iv_entry * 0.80:
        result.update({
            "Action": "EXIT",
            "Urgency": "MEDIUM",
            "Rationale": (
                f"Vol mean-reversion: IV contracted from {iv_entry:.1%} to {iv_now:.1%} "
                f"({1 - iv_now/iv_entry:.0%} decline). Vol thesis is reversing — "
                f"remaining premium is decaying without vol catalyst "
                f"(Natenberg Ch.5: exit when vol thesis reverses)."
            ),
            "Doctrine_Source": "Natenberg Ch.5: Vol Mean Reversion Exit",
            "Decision_State": STATE_ACTIONABLE,
            "Required_Conditions_Met": True,
        })
        return result

    # 7. Vol collapse exit: extreme IV compression
    if vol_state == 'COMPRESSED' and pnl_pct < -0.30:
        result.update({
            "Action": "EXIT",
            "Urgency": "HIGH",
            "Rationale": (
                f"Vol collapse: volatility state COMPRESSED, position down {abs(pnl_pct):.0%}. "
                f"Vol expansion thesis is structurally broken — no catalyst. "
                f"Cut losses (McMillan Ch.4: exit broken thesis)."
            ),
            "Doctrine_Source": "McMillan Ch.4: Vol Collapse Exit",
            "Decision_State": STATE_ACTIONABLE,
            "Required_Conditions_Met": True,
        })
        return result

    # 8. Thesis regime degradation
    thesis = check_thesis_degradation(row)
    if thesis:
        result.update({
            "Action": "ROLL",
            "Urgency": "MEDIUM",
            "Rationale": (
                f"Entry regime degraded: {thesis['text']}. "
                f"Vol regime may have shifted — reassess or roll "
                f"(McMillan Ch.4: Thesis Persistence)."
            ),
            "Doctrine_Source": "McMillan Ch.4: Thesis Persistence",
            "Decision_State": STATE_ACTIONABLE,
            "Required_Conditions_Met": True,
        })
        return result

    # Default HOLD
    result.update({
        "Action": "HOLD",
        "Urgency": "LOW",
        "Rationale": (
            f"Vol play intact — DTE={dte}, vol state={vol_state}. "
            f"Monitoring for vol expansion catalyst (Natenberg Ch.5)."
        ),
        "Doctrine_Source": "Natenberg Ch.5: Vol Play Maintenance",
        "Decision_State": STATE_NEUTRAL_CONFIDENT,
        "Required_Conditions_Met": True,
    })
    return result


# ── v2: Proposal-based evaluation ────────────────────────────────────────────

def multi_leg_doctrine_v2(row: pd.Series, result: Dict) -> Dict:
    """Proposal-based MULTI_LEG (straddle/strangle) evaluation.

    All gates propose actions into a ProposalCollector instead of returning
    immediately.  A resolver picks the best action using urgency rank and
    priority number (multi-leg has no EV comparator).

    Original ``multi_leg_doctrine()`` is preserved unchanged for A/B testing.
    """
    collector = ProposalCollector()

    # ── Calendar gates ────────────────────────────────────────────────────
    try:
        from scan_engine.calendar_context import expiry_proximity_flag
        _dte_ml = safe_row_float(row, 'DTE', default=999.0)
        _ul_ml = safe_row_float(row, 'UL Last')
        _strike_ml = safe_row_float(row, 'Strike')
        _strat_ml = str(row.get('Strategy', '') or '').upper()
        _exp_flag_ml, _exp_note_ml = expiry_proximity_flag(
            dte=_dte_ml, strategy=_strat_ml,
            ul_last=_ul_ml, strike=_strike_ml,
        )
        if _exp_flag_ml == 'PIN_RISK':
            propose_gate(
                collector, "pin_risk",
                action="EXIT", urgency="CRITICAL",
                rationale=_exp_note_ml,
                doctrine_source="McMillan Ch.7 + Natenberg Ch.15: Pin Risk",
                priority=1, is_hard_veto=True, exit_trigger_type="GAMMA",
            )
        elif _exp_flag_ml == 'GAMMA_CRITICAL':
            propose_gate(
                collector, "gamma_critical",
                action="ROLL", urgency="HIGH",
                rationale=_exp_note_ml,
                doctrine_source="Natenberg Ch.15: Gamma Critical",
                priority=5,
            )
    except Exception as _ml_exp_err:
        logger.debug(f"Multi-leg v2 expiration gate skipped: {_ml_exp_err}")

    # ── Extract fields ────────────────────────────────────────────────────
    vol_state = str(row.get('VolatilityState_State', 'UNKNOWN') or 'UNKNOWN')
    vol_state = vol_state.split('.')[-1].upper()
    price_drift = float(row.get('Price_Drift_Pct', 0) if pd.notna(row.get('Price_Drift_Pct')) else 0)
    dte = row.get('DTE', 999) or 999
    iv_now = safe_row_float(row, 'IV_Now')
    iv_entry = safe_row_float(row, 'IV_Entry')
    hv_20d = safe_row_float(row, 'HV_20D')
    pnl_pct = float(row.get('Total_GL_Decimal', 0) if pd.notna(row.get('Total_GL_Decimal')) else 0)

    # ── Gate 1: Vol spike — partial profit on IV expansion ────────────────
    if iv_entry > 0 and iv_now > 0 and iv_now / iv_entry > 1.50:
        propose_gate(
            collector, "iv_expansion_trim",
            action="TRIM", urgency="MEDIUM",
            rationale=(
                f"Vol expansion captured: IV moved from {iv_entry:.1%} to {iv_now:.1%} "
                f"({iv_now/iv_entry - 1:.0%} increase). Partial profit on vol spike. "
                f"Natenberg Ch.5: Capture vol profits when implied exceeds realized entry level."
            ),
            doctrine_source="Natenberg Ch.5: Vol Spike Profit Capture",
            priority=20,
        )

    # ── Gate 2: Profit target — 50% of premium paid ──────────────────────
    if pnl_pct >= 0.50:
        propose_gate(
            collector, "profit_target_exit",
            action="EXIT", urgency="MEDIUM",
            rationale=(
                f"Profit target reached: {pnl_pct:.0%} gain (>=50%). "
                f"Take profits on vol expansion play (McMillan Ch.4: Profit Target)."
            ),
            doctrine_source="McMillan Ch.4: Straddle/Strangle Profit Target",
            priority=25,
        )

    # ── Gate 3: Theta bleed — double-leg decay critical ───────────────────
    theta_bleed = safe_row_float(row, 'Theta_Bleed_Daily_Pct')
    if theta_bleed > 5.0 and vol_state not in ('EXPANDING', 'EXTREME'):
        propose_gate(
            collector, "theta_bleed_exit",
            action="EXIT", urgency="HIGH",
            rationale=(
                f"Theta bleed critical: {theta_bleed:.1f}%/day with vol {vol_state} "
                f"(not expanding). Double-theta on two legs is consuming premium rapidly. "
                f"Exit before theta acceleration destroys remaining value "
                f"(Passarelli Ch.2: straddle theta = 2x single leg)."
            ),
            doctrine_source="Passarelli Ch.2: Straddle Theta Critical",
            priority=30,
        )

    # ── Gate 4: Delta asymmetry — one leg dominant ────────────────────────
    call_delta = abs(safe_row_float(row, 'Call_Delta'))
    put_delta = abs(safe_row_float(row, 'Put_Delta'))
    if call_delta > 0 and put_delta > 0:
        delta_ratio = max(call_delta, put_delta) / min(call_delta, put_delta)
        if delta_ratio > 5.0:
            _winning_leg = "call" if call_delta > put_delta else "put"
            _losing_leg = "put" if _winning_leg == "call" else "call"
            propose_gate(
                collector, "delta_asymmetry_trim",
                action="TRIM", urgency="LOW",
                rationale=(
                    f"Asymmetric legs: {_winning_leg} delta={max(call_delta, put_delta):.2f} vs "
                    f"{_losing_leg} delta={min(call_delta, put_delta):.2f} (ratio {delta_ratio:.1f}x). "
                    f"Consider closing {_losing_leg} leg to recover residual value and reduce theta "
                    f"(Natenberg Ch.5: asymmetric straddle management)."
                ),
                doctrine_source="Natenberg Ch.5: Asymmetric Leg Management",
                priority=35,
            )

    # ── Gate 5: Time stop — DTE <= 21 with losing position ────────────────
    if dte <= 21 and pnl_pct < 0:
        propose_gate(
            collector, "time_stop_exit",
            action="EXIT", urgency="HIGH",
            rationale=(
                f"Time stop: DTE={dte} <= 21, position down {abs(pnl_pct):.0%}. "
                f"Theta acceleration on BOTH legs = 2x decay rate in final weeks. "
                f"Exit to stop the bleed (Passarelli Ch.2: straddle time stop)."
            ),
            doctrine_source="Passarelli Ch.2: Straddle/Strangle Time Stop",
            priority=40,
        )

    # ── Gate 6: Vol mean-reversion — IV contracting from entry ────────────
    if vol_state in ('CONTRACTING', 'NORMAL') and iv_entry > 0 and iv_now > 0 and iv_now < iv_entry * 0.80:
        propose_gate(
            collector, "vol_mean_reversion_exit",
            action="EXIT", urgency="MEDIUM",
            rationale=(
                f"Vol mean-reversion: IV contracted from {iv_entry:.1%} to {iv_now:.1%} "
                f"({1 - iv_now/iv_entry:.0%} decline). Vol thesis is reversing — "
                f"remaining premium is decaying without vol catalyst "
                f"(Natenberg Ch.5: exit when vol thesis reverses)."
            ),
            doctrine_source="Natenberg Ch.5: Vol Mean Reversion Exit",
            priority=45,
        )

    # ── Gate 7: Vol collapse — extreme IV compression with loss ───────────
    if vol_state == 'COMPRESSED' and pnl_pct < -0.30:
        propose_gate(
            collector, "vol_collapse_exit",
            action="EXIT", urgency="HIGH",
            rationale=(
                f"Vol collapse: volatility state COMPRESSED, position down {abs(pnl_pct):.0%}. "
                f"Vol expansion thesis is structurally broken — no catalyst. "
                f"Cut losses (McMillan Ch.4: exit broken thesis)."
            ),
            doctrine_source="McMillan Ch.4: Vol Collapse Exit",
            priority=42,
        )

    # ── Gate 8: Thesis regime degradation ─────────────────────────────────
    thesis = check_thesis_degradation(row)
    if thesis:
        propose_gate(
            collector, "thesis_degradation_roll",
            action="ROLL", urgency="MEDIUM",
            rationale=(
                f"Entry regime degraded: {thesis['text']}. "
                f"Vol regime may have shifted — reassess or roll "
                f"(McMillan Ch.4: Thesis Persistence)."
            ),
            doctrine_source="McMillan Ch.4: Thesis Persistence",
            priority=50,
        )

    # ── Default HOLD (always present) ─────────────────────────────────────
    propose_gate(
        collector, "default_hold",
        action="HOLD", urgency="LOW",
        rationale=(
            f"Vol play intact — DTE={dte}, vol state={vol_state}. "
            f"Monitoring for vol expansion catalyst (Natenberg Ch.5)."
        ),
        doctrine_source="Natenberg Ch.5: Vol Play Maintenance",
        priority=100,
    )

    # ── Resolution ────────────────────────────────────────────────────────
    logger.debug(f"[ML_v2] {collector.summary()}")

    if collector.has_hard_veto():
        winner = collector.get_veto()
        return collector.to_result(winner, result, resolution_method="HARD_VETO")

    # No EV comparator for multi-leg — resolve by urgency rank desc, priority asc
    proposals_sorted = sorted(
        collector.proposals,
        key=lambda p: (-p.urgency_rank, p.priority),
    )
    winner = proposals_sorted[0]
    return collector.to_result(winner, result, resolution_method="PRIORITY_FALLBACK")
