"""
Proposal-based gate evaluation infrastructure.

Instead of fire_gate() returning immediately (first match wins), propose_gate()
collects all gate evaluations into a ProposalCollector. A resolver then picks
the best action using deterministic EV + MC evidence.

Hard vetoes (circuit breaker, hard stop) bypass the resolver and win immediately.
"""

from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .gate_result import STATE_ACTIONABLE, STATE_NEUTRAL_CONFIDENT

# ── Urgency ordering (higher index = more urgent) ────────────────────────────
_URGENCY_RANK = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}


@dataclass
class ActionProposal:
    """A single gate's recommended action."""

    gate_name: str
    action: str             # EXIT | HOLD | ROLL | BUYBACK | LET_EXPIRE | ACCEPT_CALL_AWAY | ACCEPT_SHARE_ASSIGNMENT | CLOSE | REVIEW
    urgency: str            # CRITICAL | HIGH | MEDIUM | LOW
    exit_trigger_type: str  # CAPITAL | INCOME | GAMMA | ""
    rationale: str
    doctrine_source: str
    priority: int           # lower = higher priority (hard stop = 1, default hold = 100)
    is_hard_veto: bool      # True = resolver cannot override
    ev_estimate: float      # deterministic EV from gate logic (NaN if unknown)
    extra_fields: Dict[str, Any] = field(default_factory=dict)

    @property
    def urgency_rank(self) -> int:
        return _URGENCY_RANK.get(self.urgency, 0)


class ProposalCollector:
    """Accumulates proposals from all gates in a strategy function."""

    def __init__(self) -> None:
        self.proposals: List[ActionProposal] = []

    def propose(
        self,
        gate_name: str,
        *,
        action: str,
        urgency: str,
        rationale: str,
        doctrine_source: str,
        priority: int,
        is_hard_veto: bool = False,
        exit_trigger_type: str = "",
        ev_estimate: float = float("nan"),
        **extra: Any,
    ) -> None:
        """Add a proposal from a gate evaluation."""
        self.proposals.append(
            ActionProposal(
                gate_name=gate_name,
                action=action,
                urgency=urgency,
                exit_trigger_type=exit_trigger_type,
                rationale=rationale,
                doctrine_source=doctrine_source,
                priority=priority,
                is_hard_veto=is_hard_veto,
                ev_estimate=ev_estimate,
                extra_fields=dict(extra),
            )
        )

    # ── Query helpers ─────────────────────────────────────────────────────

    def has_hard_veto(self) -> bool:
        return any(p.is_hard_veto for p in self.proposals)

    def get_veto(self) -> ActionProposal:
        """Return the highest-priority (lowest number) hard veto."""
        vetoes = [p for p in self.proposals if p.is_hard_veto]
        if not vetoes:
            raise ValueError("No hard veto proposals found")
        return min(vetoes, key=lambda p: p.priority)

    def get_proposals_by_action(self) -> Dict[str, List[ActionProposal]]:
        """Group proposals by action type."""
        groups: Dict[str, List[ActionProposal]] = {}
        for p in self.proposals:
            groups.setdefault(p.action, []).append(p)
        return groups

    def best_proposal_for_action(self, action: str) -> Optional[ActionProposal]:
        """Return the highest-urgency, lowest-priority proposal for a given action."""
        candidates = [p for p in self.proposals if p.action == action]
        if not candidates:
            return None
        # Sort: highest urgency first, then lowest priority number
        candidates.sort(key=lambda p: (-p.urgency_rank, p.priority))
        return candidates[0]

    def summary(self) -> str:
        """One-line summary: '4 proposals: ROLL×2, HOLD×1, EXIT×1'."""
        counts = Counter(p.action for p in self.proposals)
        parts = [f"{action}×{ct}" for action, ct in counts.most_common()]
        return f"{len(self.proposals)} proposals: {', '.join(parts)}"

    # ── Result conversion ─────────────────────────────────────────────────

    def to_result(
        self,
        winner: ActionProposal,
        result: Dict[str, Any],
        resolution_method: str = "PRIORITY_FALLBACK",
    ) -> Dict[str, Any]:
        """Convert winning proposal to the legacy result dict format.

        Preserves the same shape as fire_gate() output so downstream code
        (run_all.py, dashboard) sees no change.
        """
        result.update(
            {
                "Action": winner.action,
                "Urgency": winner.urgency,
                "Rationale": winner.rationale,
                "Doctrine_Source": winner.doctrine_source,
                "Decision_State": STATE_ACTIONABLE if winner.action != "HOLD" else STATE_NEUTRAL_CONFIDENT,
                "Required_Conditions_Met": True,
                # Proposal metadata
                "Proposals_Considered": len(self.proposals),
                "Proposals_Summary": self.summary(),
                "Resolution_Method": resolution_method,
                "Winning_Gate": winner.gate_name,
            }
        )
        if winner.exit_trigger_type:
            result["Exit_Trigger_Type"] = winner.exit_trigger_type
        if not math.isnan(winner.ev_estimate):
            result["EV_Estimate"] = winner.ev_estimate
        # Merge any extra fields from the winning proposal
        result.update(winner.extra_fields)
        return result


def propose_gate(
    collector: ProposalCollector,
    gate_name: str,
    *,
    action: str,
    urgency: str,
    rationale: str,
    doctrine_source: str,
    priority: int,
    is_hard_veto: bool = False,
    exit_trigger_type: str = "",
    ev_estimate: float = float("nan"),
    **extra: Any,
) -> bool:
    """Add proposal to collector. Returns True (gate evaluated).

    Parallel to fire_gate() but does NOT signal early return.
    Callers should NOT use the return value for control flow.
    """
    collector.propose(
        gate_name,
        action=action,
        urgency=urgency,
        rationale=rationale,
        doctrine_source=doctrine_source,
        priority=priority,
        is_hard_veto=is_hard_veto,
        exit_trigger_type=exit_trigger_type,
        ev_estimate=ev_estimate,
        **extra,
    )
    return True
