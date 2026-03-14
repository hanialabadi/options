"""
Gate result helper — standardizes the Dict mutation pattern used by all
management doctrine gates.

Replaces the 5-line result.update({...}) boilerplate in every gate
with a one-line fire_gate() call that returns (True, result) to
signal early return to the caller.
"""

from typing import Dict, Any, Tuple

# Decision States — single source of truth
STATE_ACTIONABLE = "ACTIONABLE"
STATE_NEUTRAL_CONFIDENT = "NEUTRAL_CONFIDENT"
STATE_UNCERTAIN = "UNCERTAIN"
STATE_BLOCKED_GOVERNANCE = "BLOCKED_GOVERNANCE"
STATE_UNRESOLVED_IDENTITY = "UNRESOLVED_IDENTITY"

# Uncertainty Reasons
REASON_ATTRIBUTION_QUALITY_LOW = "ATTRIBUTION_QUALITY_LOW"
REASON_IV_AUTHORITY_MISSING = "IV_AUTHORITY_MISSING"
REASON_SCHWAB_IV_EXPIRED = "SCHWAB_IV_EXPIRED"
REASON_DELTA_GAMMA_INCOMPLETE = "DELTA_GAMMA_INCOMPLETE"
REASON_STOCK_LEG_NOT_AVAILABLE = "STOCK_LEG_NOT_AVAILABLE"
REASON_CYCLE2_SIGNAL_INCOMPLETE = "CYCLE2_SIGNAL_INCOMPLETE"
REASON_STOCK_AUTHORITY_VIOLATION = "STOCK_AUTHORITY_VIOLATION"
REASON_STRUCTURAL_DATA_INCOMPLETE = "STRUCTURAL_DATA_INCOMPLETE"

# Epistemic Authority Levels
AUTHORITY_REQUIRED = "REQUIRED"
AUTHORITY_CONTEXTUAL = "CONTEXTUAL"
AUTHORITY_SUPPORTIVE = "SUPPORTIVE"
AUTHORITY_NON_AUTHORITATIVE = "NON_AUTHORITATIVE"


def fire_gate(
    result: Dict[str, Any],
    *,
    action: str,
    urgency: str,
    rationale: str,
    doctrine_source: str,
    decision_state: str = STATE_ACTIONABLE,
    **extra_fields: Any,
) -> Tuple[bool, Dict[str, Any]]:
    """
    Standard gate fire helper.

    Updates the result dict with the gate outcome and returns (True, result)
    to signal that the calling doctrine function should return immediately
    (early exit pattern).

    Args:
        result: The mutable result dict to update.
        action: EXIT | HOLD | ROLL | TRIM | SCALE_UP | CLOSE | BUYBACK | AWAITING_SETTLEMENT
        urgency: CRITICAL | HIGH | MEDIUM | LOW
        rationale: Human-readable explanation (may include $ amounts and %).
        doctrine_source: RAG citation (e.g., "McMillan Ch.3: Hard Stop").
        decision_state: One of STATE_* constants. Defaults to STATE_ACTIONABLE.
        **extra_fields: Any additional fields to set (e.g., Doctrine_State, Gamma_Drag_Daily).

    Returns:
        (True, result) — always True, signaling gate fired.
    """
    result.update({
        "Action": action,
        "Urgency": urgency,
        "Rationale": rationale,
        "Doctrine_Source": doctrine_source,
        "Decision_State": decision_state,
        "Required_Conditions_Met": True,
        **extra_fields,
    })
    return True, result


def skip_gate(result: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """
    Convenience: gate did not fire, return (False, result) unchanged.
    """
    return False, result


# ── Proposal-based evaluation (v2) ──────────────────────────────────────────
# Re-export from proposal module so strategy files can import from one place.
from .proposal import ActionProposal, ProposalCollector, propose_gate  # noqa: E402, F401
