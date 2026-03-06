"""
Data Requirements Schema - Bias-Free Enrichment System

This module defines the schema for expressing data requirements in a
machine-readable, strategy-agnostic way. Requirements are pure data
descriptions, not policy decisions.

DESIGN PRINCIPLES:
1. Requirements are OBJECTIVE - they describe what data is missing, not why it matters
2. Requirements are MEASURABLE - clear thresholds, not fuzzy judgments
3. Requirements are STRATEGY-AGNOSTIC - no special handling for CSP vs directional
4. Requirements are EXTENSIBLE - new types can be added via configuration
5. Requirements are TESTABLE - deterministic detection, no side effects
"""

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Set
from datetime import datetime, timedelta
import hashlib


class RequirementType(Enum):
    """
    Enumeration of data requirement types.

    These are STRUCTURAL categories, not trading concepts.
    Each type has a clear definition of "satisfied" vs "unsatisfied".
    """
    # Historical Data Requirements
    IV_HISTORY = auto()          # Historical IV term structure depth
    PRICE_HISTORY = auto()       # Historical price data for technicals
    EARNINGS_CALENDAR = auto()   # Earnings date information

    # Real-time Data Requirements
    QUOTE_FRESHNESS = auto()     # Bid/ask quote staleness
    CHAIN_DATA = auto()          # Options chain availability
    GREEKS = auto()              # Delta, gamma, theta, vega availability

    # Derived Data Requirements
    IV_RANK = auto()             # Computed IV percentile
    LIQUIDITY_METRICS = auto()   # Spread %, OI, volume
    TECHNICAL_SIGNALS = auto()   # RSI, ADX, trend indicators


class RequirementPriority(Enum):
    """
    Priority levels for requirements.

    Priority is NOT about trading importance - it's about data dependency order.
    A P1 requirement must be satisfied before P2 requirements can be evaluated.
    """
    P1_BLOCKING = 1      # Cannot proceed without this data
    P2_IMPORTANT = 2     # Significantly impacts decision quality
    P3_ENHANCING = 3     # Nice to have, not blocking


class ResolutionStatus(Enum):
    """
    Status of a requirement's resolution attempts.
    """
    PENDING = auto()           # Not yet attempted
    IN_PROGRESS = auto()       # Currently being resolved
    SATISFIED = auto()         # Requirement met
    PARTIALLY_SATISFIED = auto()  # Some data obtained, below threshold
    UNRESOLVABLE = auto()      # Exhausted all resolvers, max attempts reached
    COOLDOWN = auto()          # Waiting for backoff period


@dataclass(frozen=True)
class DataRequirement:
    """
    A single, machine-readable data requirement.

    This is a VALUE OBJECT - immutable, hashable, comparable.
    It describes WHAT is needed, not HOW to get it.

    Attributes:
        requirement_type: Category of data needed
        entity_id: Ticker or trade ID this requirement applies to
        field_name: Specific column/field that is missing or insufficient
        current_value: Current state of the data (can be None, 0, or partial)
        required_threshold: Minimum value to satisfy requirement
        priority: Dependency ordering (P1 before P2 before P3)
        metadata: Additional context (not used for equality)
    """
    requirement_type: RequirementType
    entity_id: str                    # e.g., "AAPL" or "AAPL_CSP_20260215"
    field_name: str                   # e.g., "iv_history_count", "bid", "IV_Rank_30D"
    current_value: Any                # Current state
    required_threshold: Any           # What's needed
    priority: RequirementPriority = RequirementPriority.P2_IMPORTANT
    metadata: Dict[str, Any] = field(default_factory=dict, compare=False, hash=False)

    @property
    def requirement_id(self) -> str:
        """Unique identifier for this specific requirement instance."""
        key = f"{self.requirement_type.name}:{self.entity_id}:{self.field_name}"
        return hashlib.md5(key.encode()).hexdigest()[:12]

    @property
    def is_satisfied(self) -> bool:
        """Check if current value meets or exceeds threshold."""
        if self.current_value is None:
            return False
        if self.required_threshold is None:
            return self.current_value is not None

        # Numeric comparison
        if isinstance(self.required_threshold, (int, float)):
            try:
                return float(self.current_value) >= float(self.required_threshold)
            except (ValueError, TypeError):
                return False

        # Boolean
        if isinstance(self.required_threshold, bool):
            return bool(self.current_value) == self.required_threshold

        # String match
        if isinstance(self.required_threshold, str):
            return str(self.current_value) == self.required_threshold

        # Default: exact match
        return self.current_value == self.required_threshold

    @property
    def gap(self) -> Optional[float]:
        """Numeric gap between current and required (None if not comparable)."""
        if self.current_value is None or self.required_threshold is None:
            return None
        try:
            return float(self.required_threshold) - float(self.current_value)
        except (ValueError, TypeError):
            return None

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary for storage/logging."""
        return {
            "requirement_id": self.requirement_id,
            "type": self.requirement_type.name,
            "entity_id": self.entity_id,
            "field_name": self.field_name,
            "current_value": self.current_value,
            "required_threshold": self.required_threshold,
            "priority": self.priority.name,
            "is_satisfied": self.is_satisfied,
            "gap": self.gap,
            "metadata": self.metadata
        }


@dataclass
class EnrichmentAttempt:
    """
    Record of an enrichment attempt for a requirement.

    This enables:
    - Tracking what resolvers were tried
    - Implementing backoff/cooldown
    - Detecting infinite loops
    - Audit trail for debugging
    """
    requirement_id: str
    resolver_name: str
    timestamp: datetime
    success: bool
    data_obtained: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    duration_ms: Optional[float] = None

    @property
    def is_recent(self) -> bool:
        """Check if attempt is within cooldown period (default 1 hour)."""
        return datetime.now() - self.timestamp < timedelta(hours=1)


@dataclass
class RequirementResolutionState:
    """
    Tracks the resolution state of a requirement across multiple attempts.

    This is the MUTABLE state that tracks enrichment progress.
    The DataRequirement itself is immutable; this tracks attempts.
    """
    requirement: DataRequirement
    status: ResolutionStatus = ResolutionStatus.PENDING
    attempts: List[EnrichmentAttempt] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)
    last_updated: datetime = field(default_factory=datetime.now)

    # Configurable limits
    max_attempts: int = 3
    cooldown_minutes: int = 60

    @property
    def attempt_count(self) -> int:
        return len(self.attempts)

    @property
    def can_attempt(self) -> bool:
        """Check if we can make another enrichment attempt."""
        if self.status == ResolutionStatus.SATISFIED:
            return False
        if self.status == ResolutionStatus.UNRESOLVABLE:
            return False
        if self.attempt_count >= self.max_attempts:
            return False
        if self.attempts and self.attempts[-1].is_recent:
            return False  # Still in cooldown
        return True

    @property
    def next_attempt_after(self) -> Optional[datetime]:
        """When can the next attempt be made?"""
        if not self.attempts:
            return datetime.now()
        last = self.attempts[-1]
        # Exponential backoff: 1h, 2h, 4h, ...
        backoff = timedelta(minutes=self.cooldown_minutes * (2 ** (self.attempt_count - 1)))
        return last.timestamp + backoff

    def record_attempt(self, resolver_name: str, success: bool,
                       data: Optional[Dict] = None, error: Optional[str] = None,
                       duration_ms: Optional[float] = None):
        """Record an enrichment attempt."""
        attempt = EnrichmentAttempt(
            requirement_id=self.requirement.requirement_id,
            resolver_name=resolver_name,
            timestamp=datetime.now(),
            success=success,
            data_obtained=data,
            error_message=error,
            duration_ms=duration_ms
        )
        self.attempts.append(attempt)
        self.last_updated = datetime.now()

        if success:
            self.status = ResolutionStatus.SATISFIED
        elif self.attempt_count >= self.max_attempts:
            self.status = ResolutionStatus.UNRESOLVABLE
        else:
            self.status = ResolutionStatus.COOLDOWN


@dataclass
class TradeBlockers:
    """
    Complete set of blockers for a single trade.

    This replaces the monolithic "AWAIT_CONFIRMATION" with a structured
    list of specific, measurable blockers.
    """
    trade_id: str
    ticker: str
    strategy_name: str
    requirements: List[DataRequirement] = field(default_factory=list)
    resolution_states: Dict[str, RequirementResolutionState] = field(default_factory=dict)

    @property
    def unsatisfied_requirements(self) -> List[DataRequirement]:
        """Get list of requirements that are not yet met."""
        return [r for r in self.requirements if not r.is_satisfied]

    @property
    def blocking_requirements(self) -> List[DataRequirement]:
        """Get P1 requirements that are not met."""
        return [r for r in self.unsatisfied_requirements
                if r.priority == RequirementPriority.P1_BLOCKING]

    @property
    def actionable_requirements(self) -> List[DataRequirement]:
        """Get requirements that can be actively resolved (not market-dependent)."""
        market_dependent = {RequirementType.LIQUIDITY_METRICS, RequirementType.QUOTE_FRESHNESS}
        return [r for r in self.unsatisfied_requirements
                if r.requirement_type not in market_dependent]

    @property
    def is_ready(self) -> bool:
        """Check if all P1 and P2 requirements are satisfied."""
        critical = [r for r in self.requirements
                    if r.priority in (RequirementPriority.P1_BLOCKING, RequirementPriority.P2_IMPORTANT)]
        return all(r.is_satisfied for r in critical)

    @property
    def blocker_summary(self) -> str:
        """Human-readable summary of blockers."""
        unsatisfied = self.unsatisfied_requirements
        if not unsatisfied:
            return "READY"

        by_type = {}
        for r in unsatisfied:
            t = r.requirement_type.name
            if t not in by_type:
                by_type[t] = []
            by_type[t].append(r.field_name)

        parts = [f"{t}({','.join(fields)})" for t, fields in by_type.items()]
        return "BLOCKED: " + "; ".join(parts)

    def get_resolution_state(self, req: DataRequirement) -> RequirementResolutionState:
        """Get or create resolution state for a requirement."""
        if req.requirement_id not in self.resolution_states:
            self.resolution_states[req.requirement_id] = RequirementResolutionState(req)
        return self.resolution_states[req.requirement_id]

    def to_dict(self) -> Dict[str, Any]:
        """Serialize for storage/logging."""
        return {
            "trade_id": self.trade_id,
            "ticker": self.ticker,
            "strategy_name": self.strategy_name,
            "total_requirements": len(self.requirements),
            "unsatisfied_count": len(self.unsatisfied_requirements),
            "blocking_count": len(self.blocking_requirements),
            "is_ready": self.is_ready,
            "blocker_summary": self.blocker_summary,
            "requirements": [r.to_dict() for r in self.requirements]
        }
