"""
Resolver Registry - Maps Requirements to Resolution Strategies

This module defines the mapping between data requirements and available
resolvers. Resolvers are the actual implementations that fetch/compute data.

DESIGN PRINCIPLES:
1. CONFIGURABLE - Resolver mappings can be loaded from config
2. PRIORITY-ORDERED - Multiple resolvers per requirement, tried in order
3. RATE-LIMITED - Each resolver has defined rate limits
4. STATELESS - Registry is pure configuration, no state
5. EXTENSIBLE - New resolvers can be registered dynamically
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable, Any
from enum import Enum, auto
from datetime import timedelta
import logging

from .data_requirements import RequirementType

logger = logging.getLogger(__name__)


class ResolverType(Enum):
    """
    Types of resolvers available in the system.

    This is an enumeration of ALL possible resolver implementations.
    Not all resolvers are available/configured for all requirements.
    """
    # API-based resolvers
    SCHWAB_API = auto()            # Schwab API for quotes, chains, IV
    FIDELITY_SCRAPER = auto()      # Fidelity web scraper for IV data
    YFINANCE = auto()              # Yahoo Finance for price history
    TRADIER_API = auto()           # Tradier API for options data

    # Database resolvers
    DUCKDB_CACHE = auto()          # Read from local DuckDB cache
    IV_HISTORY_DB = auto()         # Read from iv_history.duckdb

    # Computed resolvers
    COMPUTE_FROM_CHAIN = auto()    # Compute Greeks from options chain
    COMPUTE_IV_RANK = auto()       # Compute IV Rank from history

    # Non-actionable (market-dependent)
    MARKET_WAIT = auto()           # Wait for market conditions to change

    # Manual intervention
    MANUAL_ENTRY = auto()          # Requires human input


@dataclass(frozen=True)
class RateLimit:
    """
    Rate limiting configuration for a resolver.
    """
    requests_per_minute: int = 60
    requests_per_hour: int = 1000
    requests_per_day: int = 10000
    concurrent_requests: int = 5
    cooldown_on_failure: timedelta = field(default_factory=lambda: timedelta(minutes=5))


@dataclass(frozen=True)
class ResolverConfig:
    """
    Configuration for a single resolver.

    This is a VALUE OBJECT - immutable and hashable.
    """
    resolver_type: ResolverType
    name: str                          # Human-readable name
    priority: int                      # Lower = higher priority (tried first)
    rate_limit: RateLimit = field(default_factory=RateLimit)
    enabled: bool = True               # Can be disabled via config
    requires_auth: bool = False        # Needs authentication
    batch_capable: bool = False        # Can process multiple tickers at once
    max_batch_size: int = 1            # Max tickers per batch
    estimated_latency_ms: int = 1000   # Expected latency
    is_actionable: bool = True         # False for MARKET_WAIT
    description: str = ""              # Documentation


@dataclass
class ResolverRegistry:
    """
    Registry of all available resolvers mapped to requirements.

    This is the central configuration for the enrichment system.
    It defines WHAT resolvers are available, not WHEN to use them.
    """
    # Mapping from requirement type to list of resolvers (priority-ordered)
    _registry: Dict[RequirementType, List[ResolverConfig]] = field(default_factory=dict)

    # Global enable/disable for resolver types
    _disabled_resolvers: set = field(default_factory=set)

    def register(self, requirement_type: RequirementType, config: ResolverConfig):
        """Register a resolver for a requirement type."""
        if requirement_type not in self._registry:
            self._registry[requirement_type] = []

        # Insert in priority order
        configs = self._registry[requirement_type]
        inserted = False
        for i, existing in enumerate(configs):
            if config.priority < existing.priority:
                configs.insert(i, config)
                inserted = True
                break
        if not inserted:
            configs.append(config)

        logger.debug(f"Registered resolver {config.name} for {requirement_type.name} "
                     f"at priority {config.priority}")

    def get_resolvers(
        self,
        requirement_type: RequirementType,
        only_enabled: bool = True,
        only_actionable: bool = True
    ) -> List[ResolverConfig]:
        """
        Get resolvers for a requirement type, in priority order.

        Args:
            requirement_type: The type of requirement
            only_enabled: Filter to only enabled resolvers
            only_actionable: Filter out MARKET_WAIT type resolvers

        Returns:
            List of ResolverConfig in priority order
        """
        configs = self._registry.get(requirement_type, [])

        if only_enabled:
            configs = [c for c in configs
                       if c.enabled and c.resolver_type not in self._disabled_resolvers]

        if only_actionable:
            configs = [c for c in configs if c.is_actionable]

        return configs

    def disable_resolver(self, resolver_type: ResolverType):
        """Globally disable a resolver type."""
        self._disabled_resolvers.add(resolver_type)
        logger.info(f"Disabled resolver: {resolver_type.name}")

    def enable_resolver(self, resolver_type: ResolverType):
        """Re-enable a resolver type."""
        self._disabled_resolvers.discard(resolver_type)
        logger.info(f"Enabled resolver: {resolver_type.name}")

    def get_all_requirements(self) -> List[RequirementType]:
        """Get all requirement types that have registered resolvers."""
        return list(self._registry.keys())

    def to_dict(self) -> Dict[str, Any]:
        """Serialize registry for logging/debugging."""
        return {
            req_type.name: [
                {
                    'resolver': c.resolver_type.name,
                    'name': c.name,
                    'priority': c.priority,
                    'enabled': c.enabled and c.resolver_type not in self._disabled_resolvers,
                    'actionable': c.is_actionable
                }
                for c in configs
            ]
            for req_type, configs in self._registry.items()
        }


def create_default_registry() -> ResolverRegistry:
    """
    Create the default resolver registry with standard configurations.

    This is the ONLY place where resolver configurations are defined.
    To customize, modify this function or load from a config file.

    NOTE: This is CONFIGURATION, not POLICY.
    We're defining WHAT resolvers exist, not WHEN to use them.
    """
    registry = ResolverRegistry()

    # =========================================================================
    # IV_HISTORY Resolvers
    # =========================================================================
    registry.register(RequirementType.IV_HISTORY, ResolverConfig(
        resolver_type=ResolverType.IV_HISTORY_DB,
        name="IV History Database (Cache)",
        priority=1,
        rate_limit=RateLimit(requests_per_minute=1000),
        batch_capable=True,
        max_batch_size=500,
        estimated_latency_ms=10,
        description="Read IV history from local iv_history.duckdb cache"
    ))

    registry.register(RequirementType.IV_HISTORY, ResolverConfig(
        resolver_type=ResolverType.SCHWAB_API,
        name="Schwab API Backfill",
        priority=2,
        rate_limit=RateLimit(
            requests_per_minute=120,
            requests_per_hour=1000,
            cooldown_on_failure=timedelta(minutes=15)
        ),
        requires_auth=True,
        batch_capable=False,
        estimated_latency_ms=500,
        description="Fetch historical IV from Schwab API"
    ))

    registry.register(RequirementType.IV_HISTORY, ResolverConfig(
        resolver_type=ResolverType.FIDELITY_SCRAPER,
        name="Fidelity Web Scraper",
        priority=3,
        rate_limit=RateLimit(
            requests_per_minute=10,
            requests_per_hour=100,
            requests_per_day=500,
            cooldown_on_failure=timedelta(hours=1)
        ),
        batch_capable=True,
        max_batch_size=50,
        estimated_latency_ms=3000,
        description="Scrape IV data from Fidelity website"
    ))

    # =========================================================================
    # IV_RANK Resolvers
    # =========================================================================
    registry.register(RequirementType.IV_RANK, ResolverConfig(
        resolver_type=ResolverType.DUCKDB_CACHE,
        name="Pipeline DB Cache",
        priority=1,
        rate_limit=RateLimit(requests_per_minute=1000),
        batch_capable=True,
        max_batch_size=500,
        estimated_latency_ms=10,
        description="Read pre-computed IV Rank from pipeline.duckdb"
    ))

    registry.register(RequirementType.IV_RANK, ResolverConfig(
        resolver_type=ResolverType.COMPUTE_IV_RANK,
        name="Compute from History",
        priority=2,
        rate_limit=RateLimit(requests_per_minute=100),
        batch_capable=True,
        max_batch_size=100,
        estimated_latency_ms=50,
        description="Compute IV Rank from available IV history"
    ))

    registry.register(RequirementType.IV_RANK, ResolverConfig(
        resolver_type=ResolverType.FIDELITY_SCRAPER,
        name="Fidelity IV Rank",
        priority=3,
        rate_limit=RateLimit(
            requests_per_minute=10,
            requests_per_hour=100,
            cooldown_on_failure=timedelta(hours=1)
        ),
        batch_capable=True,
        max_batch_size=50,
        estimated_latency_ms=3000,
        description="Scrape IV Rank directly from Fidelity"
    ))

    # =========================================================================
    # QUOTE_FRESHNESS Resolvers
    # =========================================================================
    registry.register(RequirementType.QUOTE_FRESHNESS, ResolverConfig(
        resolver_type=ResolverType.SCHWAB_API,
        name="Schwab Quote API",
        priority=1,
        rate_limit=RateLimit(
            requests_per_minute=120,
            concurrent_requests=10
        ),
        requires_auth=True,
        batch_capable=True,
        max_batch_size=100,
        estimated_latency_ms=200,
        description="Fetch real-time quotes from Schwab"
    ))

    registry.register(RequirementType.QUOTE_FRESHNESS, ResolverConfig(
        resolver_type=ResolverType.TRADIER_API,
        name="Tradier Quote API",
        priority=2,
        rate_limit=RateLimit(
            requests_per_minute=60,
            concurrent_requests=5
        ),
        requires_auth=True,
        batch_capable=True,
        max_batch_size=50,
        estimated_latency_ms=300,
        description="Fetch quotes from Tradier as fallback"
    ))

    # =========================================================================
    # LIQUIDITY_METRICS Resolvers
    # =========================================================================
    # NOTE: Liquidity is market-dependent - we can only wait
    registry.register(RequirementType.LIQUIDITY_METRICS, ResolverConfig(
        resolver_type=ResolverType.MARKET_WAIT,
        name="Wait for Market Improvement",
        priority=1,
        rate_limit=RateLimit(requests_per_minute=1000),
        is_actionable=False,  # Cannot actively resolve
        description="Liquidity is market-dependent; wait for conditions to improve"
    ))

    # =========================================================================
    # GREEKS Resolvers
    # =========================================================================
    registry.register(RequirementType.GREEKS, ResolverConfig(
        resolver_type=ResolverType.SCHWAB_API,
        name="Schwab Chain API",
        priority=1,
        rate_limit=RateLimit(
            requests_per_minute=120,
            concurrent_requests=5
        ),
        requires_auth=True,
        batch_capable=False,
        estimated_latency_ms=500,
        description="Fetch Greeks from Schwab options chain"
    ))

    registry.register(RequirementType.GREEKS, ResolverConfig(
        resolver_type=ResolverType.COMPUTE_FROM_CHAIN,
        name="Compute from Chain Data",
        priority=2,
        rate_limit=RateLimit(requests_per_minute=100),
        batch_capable=True,
        max_batch_size=100,
        estimated_latency_ms=100,
        description="Compute Greeks from available chain data using Black-Scholes"
    ))

    # =========================================================================
    # PRICE_HISTORY Resolvers
    # =========================================================================
    registry.register(RequirementType.PRICE_HISTORY, ResolverConfig(
        resolver_type=ResolverType.DUCKDB_CACHE,
        name="Price History Cache",
        priority=1,
        rate_limit=RateLimit(requests_per_minute=1000),
        batch_capable=True,
        max_batch_size=500,
        estimated_latency_ms=10,
        description="Read price history from local cache"
    ))

    registry.register(RequirementType.PRICE_HISTORY, ResolverConfig(
        resolver_type=ResolverType.YFINANCE,
        name="Yahoo Finance",
        priority=2,
        rate_limit=RateLimit(
            requests_per_minute=30,
            requests_per_hour=500,
            cooldown_on_failure=timedelta(minutes=10)
        ),
        batch_capable=False,
        estimated_latency_ms=1000,
        description="Fetch price history from Yahoo Finance"
    ))

    registry.register(RequirementType.PRICE_HISTORY, ResolverConfig(
        resolver_type=ResolverType.SCHWAB_API,
        name="Schwab Price History",
        priority=3,
        rate_limit=RateLimit(
            requests_per_minute=60,
            concurrent_requests=5
        ),
        requires_auth=True,
        batch_capable=False,
        estimated_latency_ms=500,
        description="Fetch price history from Schwab"
    ))

    return registry


# Global default registry
DEFAULT_REGISTRY = create_default_registry()


def get_resolver_chain(
    requirement_type: RequirementType,
    registry: ResolverRegistry = DEFAULT_REGISTRY
) -> List[ResolverConfig]:
    """
    Get the resolver chain for a requirement type.

    This is the ordered list of resolvers to try for a given requirement.
    """
    return registry.get_resolvers(requirement_type)


def get_actionable_resolver_chain(
    requirement_type: RequirementType,
    registry: ResolverRegistry = DEFAULT_REGISTRY
) -> List[ResolverConfig]:
    """
    Get only actionable resolvers (excludes MARKET_WAIT).
    """
    return registry.get_resolvers(requirement_type, only_actionable=True)
