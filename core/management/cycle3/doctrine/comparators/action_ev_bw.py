"""
Action Comparator for BUY_WRITE / COVERED_CALL positions.

Computes a deterministic expected value for four competing actions:
  HOLD      — collect remaining theta, pay carry cost, keep position
  ROLL      — estimated credit from extrinsic value, reset DTE cycle
  ASSIGN    — certain: stock called away at strike, P&L is fixed now
  BUYBACK   — certain cost: close short call, hold stock naked

This is NOT a full probability-weighted EV model. It uses:
  - Theta carry as the certain income stream for HOLD
  - Gamma drag estimate (Natenberg Ch.7: ½ × Γ × σ² × S²) as HOLD cost
  - Extrinsic remaining as ROLL credit proxy (lower-bound estimate)
  - Assignment outcome as certain proceeds (no probability needed)
  - Buyback cost from current ask as certain exit cost

All EVs are in dollars over the remaining DTE horizon.
The winner is the action with the highest EV (or least-negative).

Extracted from engine.py `DoctrineAuthority._compare_actions_bw`.
"""

import logging
import math
from typing import Any, Dict, Optional

import pandas as pd

from core.management.cycle1.identity.constants import FIDELITY_MARGIN_RATE_DAILY
from ..proposal import ActionProposal, ProposalCollector
from ..thresholds import (
    ADX_TRENDING,
    ROC5_ACCELERATING_BUYBACK,
    IV_BUYBACK_TRIGGER_CEILING,
    GAMMA_EATING_THETA,
    EV_NOISE_FLOOR_INCOME,
    STANDARD_ROLL_DTE,
    CONTRACT_MULTIPLIER,
)

logger = logging.getLogger(__name__)


def compare_actions_bw(
    row: pd.Series,
    *,
    spot: float,
    strike: float,
    effective_cost: float,
    qty: float,
    dte: float,
) -> Dict[str, Any]:
    """
    Compute deterministic EV for HOLD / ROLL / ASSIGN / BUYBACK.

    Returns a dict with:
      ev_hold, ev_roll, ev_assign, ev_buyback  (floats, $ over DTE)
      ranked_actions                            (list[str], highest to lowest)
      ev_winner                                 (str)
      ev_margin                                 (#1 minus #2, $ — confidence gap)
      ev_summary                                (one-line readable string)
      ev_buyback_trigger                        (bool — gamma conditions favor buyback)
      gamma_drag_daily                          ($ per day)
    """
    n_contracts = max(1, int(abs(qty)))
    multiplier = CONTRACT_MULTIPLIER * n_contracts  # per-contract → total dollars

    # ── Raw inputs ────────────────────────────────────────────────────
    theta_raw = abs(float(row.get('Theta', 0) or 0))          # per share per day
    gamma_raw = abs(float(row.get('Gamma', 0) or 0))          # per share per $1 move
    hv_daily = float(row.get('HV_20D', 0.20) or 0.20)        # annualised; convert below
    if hv_daily >= 1.0:
        hv_daily = hv_daily / 100.0                           # normalise 46.0 → 0.46
    hv_daily_frac = hv_daily / math.sqrt(252)                 # daily σ fraction
    daily_sigma_dollars = spot * hv_daily_frac                # expected daily $ move

    # Option price (to close)
    call_last = abs(float(row.get('Short_Call_Last') or row.get('Last') or 0))
    call_ask = abs(float(row.get('Ask', call_last * 1.02) or call_last * 1.02))
    call_bid = abs(float(row.get('Bid', call_last * 0.98) or call_last * 0.98))

    # Carry cost
    capital = effective_cost if effective_cost > 0 else spot
    daily_carry = capital * FIDELITY_MARGIN_RATE_DAILY        # $ per share per day

    # ── EV_HOLD ───────────────────────────────────────────────────────
    gamma_drag_daily = 0.5 * gamma_raw * (daily_sigma_dollars ** 2)  # $ per share per day
    theta_income_total = theta_raw * dte * multiplier
    carry_cost_total = daily_carry * dte * multiplier
    gamma_drag_total = gamma_drag_daily * dte * multiplier
    ev_hold = theta_income_total - carry_cost_total - gamma_drag_total

    # ── EV_ROLL ───────────────────────────────────────────────────────
    intrinsic_val = max(0.0, spot - strike)                   # per share
    extrinsic_val = max(0.0, call_last - intrinsic_val)       # per share
    roll_slippage = (call_ask - call_bid) / 2.0               # half-spread cost per share
    new_carry = daily_carry * STANDARD_ROLL_DTE * multiplier
    ev_roll = (extrinsic_val * multiplier) - (roll_slippage * multiplier) - new_carry

    # ── EV_ASSIGN ─────────────────────────────────────────────────────
    # Probability-weight assignment EV: assignment only happens if
    # stock is above strike at expiry. Use MC P(assign) if available,
    # otherwise use delta as a proxy for assignment probability.
    # (McMillan Ch.3: assignment outcome is certain IF called, but the
    # probability of being called depends on moneyness at expiry.)
    assign_per_share = strike - effective_cost
    _delta_abs = abs(float(row.get('Short_Call_Delta') or row.get('Delta') or 0))
    # MC_Assign_P_Expiry is the MC-simulated probability of finishing ITM [0-1].
    # Distinguish between "MC didn't run" (NaN/None) vs "MC ran and returned 0%" (0.0).
    _mc_raw = row.get('MC_Assign_P_Expiry')
    _mc_ran = _mc_raw is not None and not (isinstance(_mc_raw, float) and math.isnan(_mc_raw))
    if _mc_ran:
        _p_assign = float(_mc_raw or 0)
        # Ensure [0, 1] range — stored as fraction, not percent
        if _p_assign > 1.0:
            _p_assign = min(1.0, _p_assign / 100.0)
    else:
        # Delta as proxy when MC didn't run
        _p_assign = _delta_abs
    ev_assign = assign_per_share * multiplier * _p_assign

    # ── EV_BUYBACK ────────────────────────────────────────────────────
    buyback_cost = call_ask * multiplier
    ev_buyback = -buyback_cost

    # ── Gamma/breakout buyback trigger ────────────────────────────────
    adx = float(row.get('adx_14', 25) or 25)
    roc_5 = float(row.get('roc_5', 0) or 0)
    _greek_raw = row.get('GreekDominance_State', '') or ''
    greek_dom = (getattr(_greek_raw, 'value', None) or str(_greek_raw).split('.')[-1]).upper()
    iv_norm = float(row.get('IV_30D', row.get('IV_Now', 0.30)) or 0.30)
    if iv_norm >= 5.0:
        iv_norm = iv_norm / 100.0
    ev_buyback_trigger = (
        greek_dom == 'GAMMA_DOMINANT'
        and adx > ADX_TRENDING + 3                            # 28 — trending
        and roc_5 > ROC5_ACCELERATING_BUYBACK                 # accelerating up
        and iv_norm < IV_BUYBACK_TRIGGER_CEILING               # IV low → cheap to close
        and gamma_drag_daily > theta_raw * GAMMA_EATING_THETA  # gamma eating theta
    )

    # ── Rank ──────────────────────────────────────────────────────────
    ev_map = {
        "HOLD": ev_hold,
        "ROLL": ev_roll,
        "ASSIGN": ev_assign,
        "BUYBACK": ev_buyback,
    }
    ranked = sorted(ev_map.items(), key=lambda x: x[1], reverse=True)
    ranked_actions = [a for a, _ in ranked]
    ev_winner = ranked_actions[0]
    ev_margin = ranked[0][1] - ranked[1][1]

    # ── Summary line ──────────────────────────────────────────────────
    def _fmt(v: float) -> str:
        return f"+${v:,.0f}" if v >= 0 else f"-${abs(v):,.0f}"

    _n_evaluated = len(ev_map)  # should always be 4 for BW/CC
    if ev_margin < EV_NOISE_FLOOR_INCOME:
        ev_summary = (
            f"Action EV (over {dte:.0f}d, {_n_evaluated}/{_n_evaluated} actions scored): "
            f"HOLD {_fmt(ev_hold)} | ROLL {_fmt(ev_roll)} | "
            f"ASSIGN {_fmt(ev_assign)} | BUYBACK {_fmt(ev_buyback)} "
            f"→ statistical tie ({ev_winner}/{ranked_actions[1]} within ${ev_margin:.0f} noise floor) — "
            f"doctrine gates take precedence"
        )
    else:
        ev_summary = (
            f"Action EV (over {dte:.0f}d, {_n_evaluated}/{_n_evaluated} actions scored): "
            f"HOLD {_fmt(ev_hold)} | ROLL {_fmt(ev_roll)} | "
            f"ASSIGN {_fmt(ev_assign)} | BUYBACK {_fmt(ev_buyback)} "
            f"→ **{ev_winner}** wins by {_fmt(ev_margin)}"
        )

    if ev_buyback_trigger:
        ev_summary += (
            f" ⚡ Buyback trigger active: gamma drag ${gamma_drag_daily*100:.2f}/day "
            f"vs theta ${theta_raw*100:.2f}/day — breakout underway, IV cheap to close."
        )

    return {
        "ev_hold": round(ev_hold, 2),
        "ev_roll": round(ev_roll, 2),
        "ev_assign": round(ev_assign, 2),
        "ev_buyback": round(ev_buyback, 2),
        "ranked_actions": ranked_actions,
        "ev_winner": ev_winner,
        "ev_margin": round(ev_margin, 2),
        "ev_summary": ev_summary,
        "ev_buyback_trigger": ev_buyback_trigger,
        "gamma_drag_daily": round(gamma_drag_daily * 100, 4),  # per contract per day
    }


# ── EV-to-action mapping for proposal enrichment ────────────────────────────
_ACTION_TO_EV_KEY = {
    "HOLD": "ev_hold",
    "ROLL": "ev_roll",
    "ASSIGN": "ev_assign",
    "LET_EXPIRE": "ev_assign",
    "ACCEPT_CALL_AWAY": "ev_assign",
    "ACCEPT_SHARE_ASSIGNMENT": "ev_assign",
    "BUYBACK": "ev_buyback",
}

# MC column mappings — higher values favor the action
# Canonical names from mc_management.py: MC_Hold_*, MC_TB_*, MC_Assign_*
_MC_HOLD_SIGNALS = ["MC_Hold_P_Recovery", "MC_TB_P_Profit"]
_MC_EXIT_SIGNALS = ["MC_Hold_P_MaxLoss", "MC_TB_P_Stop"]
_MC_ASSIGN_SIGNALS = ["MC_Assign_P_Expiry"]


def resolve_income_proposals(
    collector: ProposalCollector,
    ev_result: Dict[str, Any],
    row: pd.Series,
) -> ActionProposal:
    """Pick best proposal using deterministic EV + MC evidence + urgency.

    Resolution order:
      1. CAPITAL exit proposals always win (hard constraint, even if not hard_veto)
      2. Score each proposal: deterministic EV + MC weight + urgency bonus
      3. Highest total score wins
      4. Tiebreaker: higher urgency, then lower priority number

    Returns the winning ActionProposal.
    """
    proposals = collector.proposals
    if not proposals:
        raise ValueError("No proposals to resolve")

    # 1. CAPITAL exits always win over non-CAPITAL proposals
    capital_exits = [
        p for p in proposals
        if p.exit_trigger_type == "CAPITAL" and p.action == "EXIT"
    ]
    if capital_exits:
        capital_exits.sort(key=lambda p: (-p.urgency_rank, p.priority))
        return capital_exits[0]

    # 2. Score each proposal
    scored: list[tuple[float, ActionProposal]] = []
    ev_winner_action = ev_result.get("ev_winner", "")
    ev_margin = ev_result.get("ev_margin", 0.0)

    for p in proposals:
        score = 0.0

        # Deterministic EV component (normalized to 0-100 scale)
        ev_key = _ACTION_TO_EV_KEY.get(p.action)
        if ev_key and ev_key in ev_result:
            ev_val = ev_result[ev_key]
            # Normalize: map [-5000, +5000] to [-50, +50]
            score += max(-50.0, min(50.0, ev_val / 100.0))

        # EV winner bonus: if this action matches the EV winner, boost
        if p.action == ev_winner_action and ev_margin > EV_NOISE_FLOOR_INCOME:
            score += 15.0

        # MC evidence component (0.4 weight relative to deterministic)
        mc_bonus = _compute_mc_bonus(p.action, row)
        score += mc_bonus * 0.4

        # Urgency bonus: CRITICAL=12, HIGH=8, MEDIUM=4, LOW=0
        score += p.urgency_rank * 4.0

        # Priority bonus: lower priority number = small boost (max 5 pts)
        score += max(0.0, 5.0 - p.priority / 20.0)

        scored.append((score, p))

    # 3. Sort by score descending, then urgency, then priority
    scored.sort(key=lambda x: (-x[0], -x[1].urgency_rank, x[1].priority))

    winner = scored[0][1]
    logger.debug(
        f"[ProposalResolver] {collector.summary()} → winner: "
        f"{winner.action} ({winner.gate_name}) score={scored[0][0]:.1f}"
    )
    return winner


def _compute_mc_bonus(action: str, row: pd.Series) -> float:
    """Extract MC signal strength for a given action from row columns."""
    bonus = 0.0

    if action == "HOLD":
        for col in _MC_HOLD_SIGNALS:
            val = float(row.get(col, 0) or 0)
            if val > 0:
                bonus += val * 20.0  # P(recovery) 0.6 → +12 pts
    elif action == "EXIT":
        for col in _MC_EXIT_SIGNALS:
            val = float(row.get(col, 0) or 0)
            if val > 0:
                bonus += val * 20.0
    elif action in ("ASSIGN", "LET_EXPIRE", "ACCEPT_CALL_AWAY", "ACCEPT_SHARE_ASSIGNMENT"):
        for col in _MC_ASSIGN_SIGNALS:
            val = float(row.get(col, 0) or 0)
            if val > 0.5:  # only boost if P(assign) > 50%
                bonus += val * 15.0
    elif action == "ROLL":
        # Roll benefits from low assignment risk + moderate hold value
        p_assign = float(row.get("MC_Assign_P_Expiry", 0) or 0)
        if p_assign < 0.3:
            bonus += 8.0  # safe to roll — assignment unlikely
        wait_verdict = str(row.get("MC_Wait_Verdict", "") or "")
        if wait_verdict == "WAIT":
            bonus += 5.0  # MC says waiting is fine

    return bonus
