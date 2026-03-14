"""
Action EV comparator for directional long options (LONG_PUT, LONG_CALL, LEAPS).

Mirrors compare_actions_bw() but for the buyer side:
  HOLD  — keep position, pay theta, ride directional move
  ROLL  — pay debit (or collect credit) to extend/adjust
  CLOSE — sell to close at current bid (certain exit)

Extracted from DoctrineAuthority._compare_actions_long_option (engine.py).
"""

import json
import logging
from typing import Any, Dict

import pandas as pd

from ..proposal import ActionProposal, ProposalCollector
from ..thresholds import EV_NOISE_FLOOR_DIRECTIONAL

logger = logging.getLogger(__name__)


def compare_actions_long_option(
    row: pd.Series, dte: float, pnl_pct: float
) -> Dict[str, Any]:
    """
    Action comparator for directional long options.

    Returns dict with:
      ev_hold, ev_roll, ev_close  (floats, $ per contract)
      ranked_actions              (list[str])
      ev_winner                   (str)
      ev_margin                   ($, gap between #1 and #2)
      vol_confidence              (float 0-1)
      capital_impact              ($ additional capital if ROLL)
      ev_summary                  (one-line string)
      mc_used                     (bool)
    """
    qty  = max(1, int(abs(float(row.get('Quantity', 1) or 1))))
    mult = 100 * qty

    # ── Vol confidence weight ─────────────────────────────────────────────
    iv_now = float(row.get('IV_30D', row.get('IV_Now', 0)) or 0)
    hv_20  = float(row.get('HV_20D', row.get('HV_20', 0)) or 0)
    if iv_now >= 5.0:
        iv_now /= 100.0
    if hv_20 >= 5.0:
        hv_20 /= 100.0
    if hv_20 > 0 and iv_now > 0:
        _iv_hv_ratio = iv_now / hv_20
        if 0.85 <= _iv_hv_ratio <= 1.15:
            vol_confidence = 0.85
        elif 0.70 <= _iv_hv_ratio < 0.85 or 1.15 < _iv_hv_ratio <= 1.30:
            vol_confidence = 0.65
        else:
            vol_confidence = 0.45
    else:
        vol_confidence = 0.50

    # ── EV_HOLD ──────────────────────────────────────────────────────────
    mc_hold_ev  = row.get('MC_Hold_EV')
    mc_hold_p50 = row.get('MC_Hold_P50')
    mc_used     = False
    theta_raw   = abs(float(row.get('Theta', 0) or 0))

    if mc_hold_ev is not None and pd.notna(mc_hold_ev):
        _mc_ev = float(mc_hold_ev)
        ev_hold = _mc_ev * vol_confidence + (-theta_raw * dte * mult) * (1 - vol_confidence)
        mc_used = True
    else:
        ev_hold = -theta_raw * dte * mult

    # ── EV_ROLL ───────────────────────────────────────────────────────────
    mc_credit_delta = row.get('MC_Wait_Credit_Delta')
    roll_cost_per_contract = 0.0

    if mc_credit_delta is not None and pd.notna(mc_credit_delta):
        roll_cost_per_contract = float(mc_credit_delta)
    else:
        _rc1 = row.get('Roll_Candidate_1')
        if _rc1 and str(_rc1) not in ('', 'nan', 'None'):
            try:
                _cd = json.loads(str(_rc1)) if isinstance(_rc1, str) else _rc1
                _ctr = _cd.get('cost_to_roll', {})
                if isinstance(_ctr, str):
                    _ctr = json.loads(_ctr)
                _npc = float(_ctr.get('net_per_contract', 0) or 0)
                roll_cost_per_contract = _npc
            except Exception:
                pass

    ev_roll = roll_cost_per_contract * qty
    capital_impact = max(0.0, -roll_cost_per_contract) * qty

    # ── EV_CLOSE ─────────────────────────────────────────────────────────
    last_price = float(row.get('Last', row.get('Mark', 0)) or 0)
    bid_price  = float(row.get('Bid', last_price * 0.98) or last_price * 0.98)
    _raw_entry_prem = row.get('Premium_Entry')
    _entry_prem_missing = pd.isna(_raw_entry_prem)
    entry_prem = float(_raw_entry_prem or last_price) if not _entry_prem_missing else last_price
    ev_close   = (bid_price - entry_prem) * mult

    # ── Rank ─────────────────────────────────────────────────────────────
    ev_map = {"HOLD": ev_hold, "ROLL": ev_roll, "CLOSE": ev_close}
    ranked = sorted(ev_map.items(), key=lambda x: x[1], reverse=True)
    ranked_actions = [a for a, _ in ranked]
    ev_winner      = ranked_actions[0]
    ev_margin      = ranked[0][1] - ranked[1][1]

    def _fmt(v: float) -> str:
        return f"+${v:,.0f}" if v >= 0 else f"-${abs(v):,.0f}"

    _NOISE = 75.0
    _n_evaluated = len(ev_map)  # should always be 3 for long options
    if ev_margin < _NOISE:
        ev_summary = (
            f"[{_n_evaluated}/{_n_evaluated} actions scored] "
            f"Statistical tie: {ev_winner}≈{ranked_actions[1]} "
            f"(margin ${ev_margin:.0f} < ${_NOISE:.0f} noise floor). "
            f"Vol confidence: {vol_confidence:.0%}. "
            f"{'MC-weighted.' if mc_used else 'Static theta fallback — no MC.'}"
        )
    else:
        ev_summary = (
            f"[{_n_evaluated}/{_n_evaluated} actions scored] "
            f"{ev_winner} dominates: {_fmt(ranked[0][1])} vs "
            f"{ranked_actions[1]}: {_fmt(ranked[1][1])} "
            f"(margin {_fmt(ev_margin)}). "
            f"Vol confidence: {vol_confidence:.0%}. "
            f"{'MC-weighted.' if mc_used else 'Static theta fallback.'}"
        )

    if _entry_prem_missing:
        ev_summary += (
            " [WARNING: Premium_Entry missing — ev_close uses Last as proxy, "
            "making CLOSE appear ~neutral. Treat CLOSE EV with low confidence.]"
        )

    return {
        "ev_hold":          ev_hold,
        "ev_roll":          ev_roll,
        "ev_close":         ev_close,
        "ranked_actions":   ranked_actions,
        "ev_winner":        ev_winner,
        "ev_margin":        ev_margin,
        "vol_confidence":   vol_confidence,
        "capital_impact":   capital_impact,
        "ev_summary":       ev_summary,
        "mc_used":          mc_used,
    }


# ── EV-to-action mapping for proposal enrichment ────────────────────────────
_ACTION_TO_EV_KEY = {
    "HOLD": "ev_hold",
    "ROLL": "ev_roll",
    "CLOSE": "ev_close",
    "EXIT": "ev_close",  # EXIT maps to CLOSE EV (sell to close)
}

# MC column mappings — higher values favor the action
# Canonical names from mc_management.py: MC_Hold_*, MC_TB_*
_MC_HOLD_SIGNALS = ["MC_Hold_P_Recovery", "MC_TB_P_Profit"]
_MC_EXIT_SIGNALS = ["MC_Hold_P_MaxLoss", "MC_TB_P_Stop"]


def resolve_directional_proposals(
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
        # Map EXIT → CLOSE for comparison since EV comparator uses CLOSE
        _cmp_action = "CLOSE" if p.action == "EXIT" else p.action
        if _cmp_action == ev_winner_action and ev_margin > EV_NOISE_FLOOR_DIRECTIONAL:
            score += 15.0

        # MC evidence component (0.4 weight relative to deterministic)
        mc_bonus = _compute_mc_bonus_directional(p.action, row)
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
        f"[DirectionalProposalResolver] {collector.summary()} → winner: "
        f"{winner.action} ({winner.gate_name}) score={scored[0][0]:.1f}"
    )
    return winner


def _compute_mc_bonus_directional(action: str, row: pd.Series) -> float:
    """Extract MC signal strength for a given action from row columns."""
    bonus = 0.0

    if action == "HOLD":
        for col in _MC_HOLD_SIGNALS:
            val = float(row.get(col, 0) or 0)
            if val > 0:
                bonus += val * 20.0  # P(recovery) 0.6 → +12 pts
    elif action in ("EXIT", "CLOSE"):
        for col in _MC_EXIT_SIGNALS:
            val = float(row.get(col, 0) or 0)
            if val > 0:
                bonus += val * 20.0
    elif action == "ROLL":
        # Roll benefits from low exit pressure + moderate hold value
        p_max_loss = float(row.get("MC_Hold_P_MaxLoss", 0) or 0)
        if p_max_loss < 0.3:
            bonus += 8.0  # safe to roll — max loss unlikely
        wait_verdict = str(row.get("MC_Wait_Verdict", "") or "")
        if wait_verdict == "WAIT":
            bonus += 5.0  # MC says waiting is fine

    return bonus
