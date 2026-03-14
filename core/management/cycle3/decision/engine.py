"""
Backward-compatible facade — all logic moved to doctrine/ package.

Public API (unchanged):
  DoctrineAuthority          — from doctrine/__init__.py
  generate_recommendations   — from doctrine/orchestrator.py
  _apply_execution_readiness — from doctrine/execution_readiness.py
"""

# Re-export public API so existing imports keep working:
#   from core.management.cycle3.decision.engine import DoctrineAuthority
#   from core.management.cycle3.decision.engine import generate_recommendations
#   from core.management.cycle3.decision.engine import _apply_execution_readiness

from core.management.cycle3.doctrine import DoctrineAuthority  # noqa: F401
from core.management.cycle3.doctrine.orchestrator import generate_recommendations  # noqa: F401
from core.management.cycle3.doctrine.execution_readiness import _apply_execution_readiness  # noqa: F401

# Also re-export constants used by tests and external callers
from core.management.cycle3.doctrine import (  # noqa: F401
    STATE_ACTIONABLE,
    STATE_NEUTRAL_CONFIDENT,
    STATE_UNCERTAIN,
    STATE_BLOCKED_GOVERNANCE,
    STATE_UNRESOLVED_IDENTITY,
    REASON_ATTRIBUTION_QUALITY_LOW,
    REASON_IV_AUTHORITY_MISSING,
    REASON_SCHWAB_IV_EXPIRED,
    REASON_DELTA_GAMMA_INCOMPLETE,
    REASON_STOCK_LEG_NOT_AVAILABLE,
    REASON_CYCLE2_SIGNAL_INCOMPLETE,
    REASON_STOCK_AUTHORITY_VIOLATION,
    REASON_STRUCTURAL_DATA_INCOMPLETE,
    AUTHORITY_REQUIRED,
    AUTHORITY_CONTEXTUAL,
    AUTHORITY_SUPPORTIVE,
    AUTHORITY_NON_AUTHORITATIVE,
    STOCK_AUTHORITY_MAP,
    IV_AUTHORITY_MAP,
)
