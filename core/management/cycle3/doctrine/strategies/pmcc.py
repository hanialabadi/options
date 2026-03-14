"""
PMCC (Poor Man's Covered Call) strategy doctrine.

Gate order (proposal-based, all gates evaluated):
  0.  New-position grace period (hard veto)
  1.  Hard stop — LEAP value collapse (-40% from entry)
  1a. Approaching hard stop (-25% to -40%)
  1b. Earnings event risk (binary event inside short-call DTE)
  1c. Underlying health story check (BROKEN/DEGRADED)
  2.  Short call ITM defense (delta > 0.55 — assignment risk on diagonal)
  3.  Short call expiration proximity (DTE < 7 — gamma risk on sold leg)
  3a. Width inversion — short strike below LEAP strike (max loss exceeded)
  4.  LEAP time decay guard (LEAP DTE < 120 — roll LEAP out)
  5.  Premium capture (≥50% of short call premium collected)
  6.  21-DTE income gate (roll short call to next cycle)
  7.  Thesis regime degradation
  8.  EV comparator (roll vs hold)
  9.  Default hold

PMCC is structurally a diagonal call spread:
  - LONG deep-ITM LEAP call (delta 0.70-0.85, DTE 270+)
  - SHORT near-term OTM call (delta 0.25-0.35, DTE 30-45)

Key differences from Buy-Write:
  - No stock ownership → no hard-stop on stock price, but LEAP can lose value
  - LEAP is the "anchor" — its time value erodes slowly but is NOT zero
  - Assignment on short call is DANGEROUS — you don't own shares, must exercise LEAP
  - Width (short_strike - leap_strike) = max per-share gain on short call
  - Net debit = LEAP cost - short call credit = max loss if both expire worthless
  - LEAP must be rolled before it approaches 120 DTE (loses delta stability)

Doctrine sources:
  Passarelli Ch.6: Diagonal Spreads — PMCC structure and management
  McMillan Ch.11: Diagonal Spread Strategies
  Hull Ch.10: LEAP as synthetic stock substitute
  Natenberg Ch.7: roll timing, adjustment frequency
"""

import math
import logging
from typing import Dict, Any

import pandas as pd

from core.management.cycle3.doctrine.gate_result import (
    STATE_ACTIONABLE,
    STATE_NEUTRAL_CONFIDENT,
    STATE_UNCERTAIN,
    fire_gate,
)
from core.management.cycle3.doctrine.thresholds import (
    DTE_EMERGENCY_ROLL,
    DTE_INCOME_GATE,
    PREMIUM_CAPTURE_TARGET,
    GAMMA_ATM_PROXIMITY,
    GAMMA_DANGER_RATIO,
)
from core.management.cycle3.doctrine.helpers import (
    check_thesis_degradation,
    safe_row_float,
)
from core.management.cycle3.doctrine.proposal import ProposalCollector, propose_gate

logger = logging.getLogger(__name__)

# ── PMCC-specific thresholds ──────────────────────────────────────────
PMCC_HARD_STOP = -0.40           # Exit if LEAP value drops 40% from entry
PMCC_APPROACHING_STOP = -0.25    # Alert zone: LEAP down 25-40%
PMCC_LEAP_ROLL_DTE = 120         # Roll LEAP out when DTE drops below this
PMCC_SHORT_DELTA_DEFENSE = 0.55  # Short call ITM defense trigger
PMCC_SHORT_DELTA_CRITICAL = 0.70 # Short call deep ITM — assignment imminent
PMCC_COOLDOWN_DAYS = 3           # Recently-rolled short call cooldown


def pmcc_doctrine(row: pd.Series, result: Dict) -> Dict:
    """Legacy v1 shadow for PMCC — delegates to v2 (PMCC has no v1 history)."""
    return pmcc_doctrine_v2(row, result)


def pmcc_doctrine_v2(row: pd.Series, result: Dict) -> Dict:
    """Proposal-based PMCC doctrine — all gates evaluate, best action wins."""

    collector = ProposalCollector()

    spot = safe_row_float(row, 'UL Last')
    short_delta = abs(safe_row_float(row, 'Short_Call_Delta', 'Delta'))
    _dte_raw = row.get('Short_Call_DTE')
    if not pd.notna(_dte_raw):
        _dte_raw = row.get('DTE')
    if not pd.notna(_dte_raw):
        _dte_raw = 999
    short_dte = float(_dte_raw)
    short_strike = safe_row_float(row, 'Short_Call_Strike', 'Strike')

    # LEAP leg
    leap_delta = abs(safe_row_float(row, 'LEAP_Call_Delta'))
    leap_dte = safe_row_float(row, 'LEAP_Call_DTE')
    leap_strike = safe_row_float(row, 'LEAP_Call_Strike')
    leap_entry_price = safe_row_float(row, 'LEAP_Entry_Price')
    leap_current_price = safe_row_float(row, 'LEAP_Call_Last', 'LEAP_Call_Mid')

    net_debit = safe_row_float(row, 'Net_Cost_Basis_Per_Share')
    cum_premium = safe_row_float(row, 'Cumulative_Premium_Collected')
    leap_pnl_pct = ((leap_current_price - leap_entry_price) / leap_entry_price) if leap_entry_price > 0 else 0.0

    # ── 0. Grace period (hard veto) ──────────────────────────────────
    days_in_trade = safe_row_float(row, 'Days_In_Trade')
    if days_in_trade < 2 and leap_pnl_pct > PMCC_HARD_STOP:
        propose_gate(
            collector, "grace_period",
            action="HOLD", urgency="LOW",
            rationale=f"New PMCC: {days_in_trade:.0f}d old, allow development.",
            doctrine_source="McMillan Ch.4: New Position Grace Period",
            priority=0, is_hard_veto=True,
        )

    # ── 1. LEAP hard stop (hard veto) ────────────────────────────────
    if leap_entry_price > 0 and leap_pnl_pct <= PMCC_HARD_STOP:
        propose_gate(
            collector, "leap_hard_stop",
            action="EXIT", urgency="CRITICAL",
            rationale=(
                f"LEAP collapsed {leap_pnl_pct:.1%} (${leap_entry_price:.2f}→"
                f"${leap_current_price:.2f}). Close both legs."
            ),
            doctrine_source="Passarelli Ch.6: PMCC Hard Stop",
            priority=1, is_hard_veto=True,
            exit_trigger_type="CAPITAL",
        )

    # ── 1a. Approaching hard stop ────────────────────────────────────
    if leap_entry_price > 0 and PMCC_HARD_STOP < leap_pnl_pct <= PMCC_APPROACHING_STOP:
        propose_gate(
            collector, "approaching_stop",
            action="ROLL", urgency="HIGH",
            rationale=(
                f"LEAP at {leap_pnl_pct:+.1%}, approaching hard stop. "
                f"Roll short call down. Premium: ${cum_premium:.2f}/sh."
            ),
            doctrine_source="McMillan Ch.11: PMCC Basis Reduction",
            priority=5,
        )

        # ── 1a-TRIM. Defensive trim at approaching stop ────────────
        from core.management.cycle3.doctrine.thresholds import DEFENSIVE_TRIM_PCT, DEFENSIVE_TRIM_MIN_QUANTITY
        _qty_pmcc = abs(int(float(
            row.get('Base_Quantity') or row.get('Entry_Quantity')
            or row.get('Quantity', 1)
        )))
        if _qty_pmcc >= DEFENSIVE_TRIM_MIN_QUANTITY:
            _def_trim_n = max(1, round(_qty_pmcc * DEFENSIVE_TRIM_PCT))
            _def_trim_n = min(_def_trim_n, _qty_pmcc - 1)
            propose_gate(
                collector, "defensive_trim_approaching_stop",
                action="TRIM", urgency="HIGH",
                rationale=(
                    f"Defensive trim: LEAP at {leap_pnl_pct:+.1%} approaching hard stop "
                    f"with {_qty_pmcc} contracts. Trim {_def_trim_n} ({DEFENSIVE_TRIM_PCT:.0%}) "
                    f"to reduce exposure while preserving recovery on remaining "
                    f"{_qty_pmcc - _def_trim_n}. "
                    f"(Sinclair Ch.7: fractional Kelly; Chan Ch.4: partial exits)"
                ),
                doctrine_source="Sinclair Ch.7 + Chan Ch.4: Defensive Trim",
                priority=5,
                exit_trigger_type="CAPITAL",
                Trim_Contracts=_def_trim_n,
                Trim_Pct=DEFENSIVE_TRIM_PCT,
            )

    # ── 1b. Earnings risk ────────────────────────────────────────────
    earnings_date = row.get('Earnings_Date')
    if earnings_date is not None and short_dte < 90:
        try:
            snap_ts = pd.to_datetime(row.get('Snapshot_TS') or pd.Timestamp.now())
            earn_dt = pd.to_datetime(earnings_date)
            days_to_earnings = (earn_dt.normalize() - snap_ts.normalize()).days
            if 0 <= days_to_earnings <= max(int(short_dte), 7):
                propose_gate(
                    collector, "earnings_risk",
                    action="ROLL", urgency="HIGH",
                    rationale=(
                        f"Earnings in {days_to_earnings}d inside short-call window. "
                        f"Gap risk → forced LEAP exercise. Roll to post-earnings."
                    ),
                    doctrine_source="Natenberg Ch.12: PMCC Earnings Event",
                    priority=8,
                )
        except Exception:
            pass

    # ── 1c. Story check ──────────────────────────────────────────────
    price_struct = str(row.get('PriceStructure_State', '') or '').upper()
    trend_integ = str(row.get('TrendIntegrity_State', '') or '').upper()

    if 'STRUCTURE_BROKEN' in price_struct and trend_integ in ('NO_TREND', 'TREND_EXHAUSTED'):
        _story_action = "EXIT" if leap_pnl_pct < -0.15 else "HOLD"
        _story_urgency = "HIGH" if _story_action == "EXIT" else "MEDIUM"
        propose_gate(
            collector, "story_broken",
            action=_story_action, urgency=_story_urgency,
            rationale=(
                f"Story BROKEN: structure + trend exhausted. LEAP {leap_pnl_pct:+.1%}."
            ),
            doctrine_source="Passarelli Ch.2: Story Check",
            priority=10,
        )

    # ── Cooldown ─────────────────────────────────────────────────────
    days_since_roll = row.get('Days_Since_Last_Roll')
    thesis_state = str(row.get('Thesis_State', '') or '').upper()
    if (
        pd.notna(days_since_roll)
        and float(days_since_roll) < PMCC_COOLDOWN_DAYS
        and thesis_state in ('INTACT', 'UNKNOWN', '')
    ):
        propose_gate(
            collector, "cooldown",
            action="HOLD", urgency="LOW",
            rationale=f"Short call rolled {int(days_since_roll)}d ago, cooldown active.",
            doctrine_source="Natenberg Ch.7: PMCC Cooldown",
            priority=12,
        )

    # ── 2. Short call ITM defense ────────────────────────────────────
    if short_delta > PMCC_SHORT_DELTA_CRITICAL:
        propose_gate(
            collector, "short_itm_critical",
            action="ROLL", urgency="CRITICAL",
            rationale=(
                f"Short call DEEP ITM (delta={short_delta:.2f}). "
                f"Assignment forces LEAP exercise — forfeit time value."
            ),
            doctrine_source="Passarelli Ch.6: PMCC Assignment Defense",
            priority=15, is_hard_veto=True,
        )
    elif short_delta > PMCC_SHORT_DELTA_DEFENSE:
        propose_gate(
            collector, "short_itm_warning",
            action="ROLL", urgency="HIGH",
            rationale=(
                f"Short call approaching ITM (delta={short_delta:.2f}). "
                f"Roll up/out before assignment risk."
            ),
            doctrine_source="Passarelli Ch.6: PMCC Pre-ITM Defense",
            priority=20,
        )

    # ── 3. Short call expiration ─────────────────────────────────────
    if short_dte < DTE_EMERGENCY_ROLL:
        propose_gate(
            collector, "short_expiration",
            action="ROLL", urgency="HIGH",
            rationale=f"Short call DTE={short_dte:.0f} < {DTE_EMERGENCY_ROLL}. Roll to next cycle.",
            doctrine_source="McMillan Ch.11: PMCC Expiration Management",
            priority=25,
        )

    # ── 3a. Width inversion ──────────────────────────────────────────
    if short_strike > 0 and leap_strike > 0 and short_strike <= leap_strike:
        propose_gate(
            collector, "width_inversion",
            action="ROLL", urgency="CRITICAL",
            rationale=(
                f"WIDTH INVERSION: short ${short_strike:.2f} ≤ LEAP ${leap_strike:.2f}. "
                f"Assignment produces spread loss. Roll short above LEAP."
            ),
            doctrine_source="Passarelli Ch.6: PMCC Width Inversion",
            priority=2, is_hard_veto=True,
        )

    # ── 4. LEAP tenor guard ──────────────────────────────────────────
    if 0 < leap_dte < PMCC_LEAP_ROLL_DTE:
        propose_gate(
            collector, "leap_tenor",
            action="ROLL", urgency="HIGH",
            rationale=(
                f"LEAP DTE={leap_dte:.0f} < {PMCC_LEAP_ROLL_DTE}. "
                f"Roll LEAP to 270+ DTE to restore stability."
            ),
            doctrine_source="Hull Ch.10: PMCC LEAP Tenor Guard",
            priority=30,
        )

    # ── 5. Premium capture ───────────────────────────────────────────
    premium_entry = safe_row_float(row, 'Premium_Entry')
    short_last = abs(safe_row_float(row, 'Short_Call_Last', 'Last'))
    if premium_entry > 0:
        captured_pct = 1.0 - (short_last / premium_entry)
        if captured_pct >= PREMIUM_CAPTURE_TARGET:
            propose_gate(
                collector, "premium_capture",
                action="ROLL", urgency="MEDIUM",
                rationale=(
                    f"Short call {captured_pct:.0%} captured "
                    f"(${premium_entry:.2f}→${short_last:.2f}). Roll to next cycle."
                ),
                doctrine_source="McMillan Ch.11: PMCC Premium Capture",
                priority=50,
            )

    # ── 6. 21-DTE income gate ────────────────────────────────────────
    if DTE_EMERGENCY_ROLL <= short_dte <= DTE_INCOME_GATE:
        propose_gate(
            collector, "income_gate_21dte",
            action="ROLL", urgency="MEDIUM",
            rationale=(
                f"Short call DTE={short_dte:.0f} ≤ {DTE_INCOME_GATE}. "
                f"Roll to restart theta income cycle."
            ),
            doctrine_source="McMillan Ch.11: PMCC Income Cycle",
            priority=55,
        )

    # ── 7. Thesis degradation ────────────────────────────────────────
    _thesis_degraded = check_thesis_degradation(row)
    if _thesis_degraded:
        propose_gate(
            collector, "thesis_degradation",
            action="ROLL", urgency="MEDIUM",
            rationale=f"Thesis degraded: {_thesis_degraded}. Reduce directional exposure.",
            doctrine_source="Passarelli Ch.6: PMCC Thesis Adjustment",
            priority=60,
        )

    # ── 8. Carry inversion (pre-computed from MarginCarryCalculator) ──
    from core.management.cycle3.doctrine.shared_income_gates import gate_carry_inversion_roll
    _r_ci = result.copy()
    _ci_fired, _r_ci = gate_carry_inversion_roll(row=row, result=_r_ci)
    if _ci_fired:
        propose_gate(
            collector, "carry_inversion_roll",
            action=_r_ci.get("Action", "ROLL"),
            urgency=_r_ci.get("Urgency", "MEDIUM"),
            rationale=_r_ci.get("Rationale", ""),
            doctrine_source=_r_ci.get("Doctrine_Source", ""),
            priority=55,
        )

    # ── 9. Default hold ──────────────────────────────────────────────
    propose_gate(
        collector, "default_hold",
        action="HOLD", urgency="LOW",
        rationale=(
            f"PMCC OK: LEAP Δ={leap_delta:.2f} DTE={leap_dte:.0f}, "
            f"short Δ={short_delta:.2f} DTE={short_dte:.0f} "
            f"@ ${short_strike:.2f}. Spot=${spot:.2f}."
        ),
        doctrine_source="McMillan Ch.11: PMCC Default Hold",
        priority=100,
    )

    # ── Resolution ───────────────────────────────────────────────────
    if collector.has_hard_veto():
        winner = collector.get_veto()
        return collector.to_result(winner, result, resolution_method="HARD_VETO")

    # No MC-based resolution for PMCC yet — use priority-based fallback
    # (highest urgency wins, lowest priority number as tiebreaker)
    proposals_sorted = sorted(
        collector.proposals,
        key=lambda p: (-p.urgency_rank, p.priority),
    )
    winner = proposals_sorted[0]
    return collector.to_result(winner, result, resolution_method="PRIORITY_FALLBACK")
