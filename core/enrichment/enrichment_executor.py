"""
Enrichment Executor - Executes Resolvers with Rate Limiting and Backoff

This module executes resolvers to satisfy data requirements. It handles:
- Rate limiting per resolver
- Exponential backoff on failures
- Cooldown tracking to prevent infinite loops
- Batch processing where supported
- Clean re-entry into the pipeline

DESIGN PRINCIPLES:
1. DETERMINISTIC - Same inputs produce same outputs (given same external state)
2. RATE-SAFE - Never exceeds configured rate limits
3. IDEMPOTENT - Safe to retry; won't corrupt state
4. OBSERVABLE - Full logging and metrics for debugging
5. EXTENSIBLE - New resolvers can be added via registry
"""

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable, Tuple
from collections import defaultdict
import threading

from .data_requirements import (
    DataRequirement,
    RequirementType,
    RequirementResolutionState,
    ResolutionStatus,
    TradeBlockers,
    EnrichmentAttempt
)
from .resolver_registry import (
    ResolverRegistry,
    ResolverConfig,
    ResolverType,
    DEFAULT_REGISTRY
)

logger = logging.getLogger(__name__)


@dataclass
class RateLimitState:
    """Tracks rate limit state for a resolver."""
    resolver_type: ResolverType
    requests_this_minute: int = 0
    requests_this_hour: int = 0
    requests_today: int = 0
    minute_start: datetime = field(default_factory=datetime.now)
    hour_start: datetime = field(default_factory=datetime.now)
    day_start: datetime = field(default_factory=lambda: datetime.now().replace(hour=0, minute=0, second=0))
    last_request: Optional[datetime] = None
    cooldown_until: Optional[datetime] = None

    def can_request(self, config: ResolverConfig) -> Tuple[bool, Optional[str]]:
        """Check if a request can be made within rate limits."""
        now = datetime.now()

        # Check cooldown
        if self.cooldown_until and now < self.cooldown_until:
            wait_seconds = (self.cooldown_until - now).total_seconds()
            return False, f"In cooldown for {wait_seconds:.0f}s"

        # Reset counters if windows have passed
        if (now - self.minute_start).total_seconds() >= 60:
            self.requests_this_minute = 0
            self.minute_start = now

        if (now - self.hour_start).total_seconds() >= 3600:
            self.requests_this_hour = 0
            self.hour_start = now

        if now.date() > self.day_start.date():
            self.requests_today = 0
            self.day_start = now.replace(hour=0, minute=0, second=0)

        # Check limits
        rl = config.rate_limit
        if self.requests_this_minute >= rl.requests_per_minute:
            return False, f"Minute limit reached ({rl.requests_per_minute})"
        if self.requests_this_hour >= rl.requests_per_hour:
            return False, f"Hour limit reached ({rl.requests_per_hour})"
        if self.requests_today >= rl.requests_per_day:
            return False, f"Day limit reached ({rl.requests_per_day})"

        return True, None

    def record_request(self, success: bool, config: ResolverConfig):
        """Record that a request was made."""
        now = datetime.now()
        self.requests_this_minute += 1
        self.requests_this_hour += 1
        self.requests_today += 1
        self.last_request = now

        if not success:
            self.cooldown_until = now + config.rate_limit.cooldown_on_failure


@dataclass
class EnrichmentExecutor:
    """
    Executes resolvers to satisfy data requirements.

    This is the main orchestrator for data enrichment. It:
    1. Takes a set of requirements
    2. Looks up resolvers for each requirement type
    3. Executes resolvers in priority order with rate limiting
    4. Tracks attempts to prevent infinite loops
    5. Returns enriched data for pipeline re-entry
    """
    registry: ResolverRegistry = field(default_factory=lambda: DEFAULT_REGISTRY)
    rate_limit_states: Dict[ResolverType, RateLimitState] = field(default_factory=dict)
    resolution_states: Dict[str, RequirementResolutionState] = field(default_factory=dict)

    # Resolver implementations (callbacks)
    _resolver_impls: Dict[ResolverType, Callable] = field(default_factory=dict)

    # Execution configuration
    max_attempts_per_requirement: int = 3
    max_total_enrichment_time_seconds: int = 300  # 5 minute timeout
    dry_run: bool = False  # If True, don't actually execute resolvers

    # Thread safety
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def register_resolver_impl(
        self,
        resolver_type: ResolverType,
        impl: Callable[[List[str], ResolverConfig], Dict[str, Any]]
    ):
        """
        Register the actual implementation for a resolver type.

        The implementation function signature:
            impl(tickers: List[str], config: ResolverConfig) -> Dict[str, Any]

        Returns a dictionary mapping ticker -> enriched data
        """
        self._resolver_impls[resolver_type] = impl
        logger.info(f"Registered resolver implementation: {resolver_type.name}")

    def execute_enrichment(
        self,
        blockers: Dict[str, TradeBlockers],
        requirement_types: Optional[List[RequirementType]] = None
    ) -> Dict[str, Any]:
        """
        Execute enrichment for all unsatisfied requirements.

        Args:
            blockers: Dictionary of trade blockers from requirement detector
            requirement_types: Optional filter to only process specific types

        Returns:
            Dictionary of enriched data, keyed by ticker, containing all
            data obtained from resolvers. This can be merged back into
            the pipeline DataFrame.
        """
        start_time = time.time()
        enriched_data = defaultdict(dict)
        stats = {
            'requirements_processed': 0,
            'resolvers_executed': 0,
            'successful_enrichments': 0,
            'failed_enrichments': 0,
            'skipped_rate_limit': 0,
            'skipped_max_attempts': 0
        }

        # Aggregate requirements by type for batch processing
        requirements_by_type = self._aggregate_requirements(blockers, requirement_types)

        logger.info(f"Starting enrichment for {len(requirements_by_type)} requirement types")

        for req_type, reqs_by_ticker in requirements_by_type.items():
            # Check timeout
            elapsed = time.time() - start_time
            if elapsed > self.max_total_enrichment_time_seconds:
                logger.warning(f"Enrichment timeout after {elapsed:.1f}s")
                break

            tickers = list(reqs_by_ticker.keys())
            logger.info(f"Processing {req_type.name}: {len(tickers)} tickers")

            # Get resolver chain for this requirement type
            resolvers = self.registry.get_resolvers(req_type, only_actionable=True)

            if not resolvers:
                logger.debug(f"No actionable resolvers for {req_type.name}")
                continue

            # Try resolvers in priority order
            remaining_tickers = set(tickers)
            for resolver_config in resolvers:
                if not remaining_tickers:
                    break

                # Filter tickers that can still be attempted
                attemptable = self._filter_attemptable(
                    remaining_tickers, reqs_by_ticker, resolver_config
                )

                if not attemptable:
                    logger.debug(f"No attemptable tickers for {resolver_config.name}")
                    continue

                # Execute resolver
                result = self._execute_resolver(
                    list(attemptable),
                    resolver_config,
                    reqs_by_ticker,
                    stats
                )

                # Merge results
                for ticker, data in result.items():
                    enriched_data[ticker].update(data)
                    remaining_tickers.discard(ticker)

            stats['requirements_processed'] += len(tickers)

        # Log summary
        elapsed = time.time() - start_time
        logger.info(f"Enrichment complete in {elapsed:.1f}s:")
        logger.info(f"  Requirements processed: {stats['requirements_processed']}")
        logger.info(f"  Resolvers executed: {stats['resolvers_executed']}")
        logger.info(f"  Successful: {stats['successful_enrichments']}")
        logger.info(f"  Failed: {stats['failed_enrichments']}")
        logger.info(f"  Skipped (rate limit): {stats['skipped_rate_limit']}")
        logger.info(f"  Skipped (max attempts): {stats['skipped_max_attempts']}")

        return dict(enriched_data)

    def _aggregate_requirements(
        self,
        blockers: Dict[str, TradeBlockers],
        filter_types: Optional[List[RequirementType]] = None
    ) -> Dict[RequirementType, Dict[str, DataRequirement]]:
        """Aggregate unsatisfied requirements by type and ticker."""
        aggregated = defaultdict(dict)

        for trade_id, trade_blockers in blockers.items():
            for req in trade_blockers.actionable_requirements:
                if filter_types and req.requirement_type not in filter_types:
                    continue

                if not req.is_satisfied:
                    # Use ticker as key (not trade_id) for deduplication
                    ticker = req.entity_id
                    req_type = req.requirement_type

                    # Keep one requirement per ticker per type
                    if ticker not in aggregated[req_type]:
                        aggregated[req_type][ticker] = req

        return dict(aggregated)

    def _filter_attemptable(
        self,
        tickers: set,
        reqs_by_ticker: Dict[str, DataRequirement],
        resolver_config: ResolverConfig
    ) -> set:
        """Filter tickers to those that can be attempted with this resolver."""
        attemptable = set()

        for ticker in tickers:
            req = reqs_by_ticker.get(ticker)
            if not req:
                continue

            # Get or create resolution state
            state = self._get_resolution_state(req)

            # Check if already satisfied or exhausted
            if state.status in (ResolutionStatus.SATISFIED, ResolutionStatus.UNRESOLVABLE):
                continue

            # Check attempt limit
            if state.attempt_count >= self.max_attempts_per_requirement:
                continue

            # Check cooldown
            if not state.can_attempt:
                continue

            attemptable.add(ticker)

        return attemptable

    def _execute_resolver(
        self,
        tickers: List[str],
        config: ResolverConfig,
        reqs_by_ticker: Dict[str, DataRequirement],
        stats: Dict[str, int]
    ) -> Dict[str, Any]:
        """Execute a single resolver for a batch of tickers."""
        results = {}

        # Check rate limit
        rate_state = self._get_rate_limit_state(config.resolver_type)
        can_request, reason = rate_state.can_request(config)

        if not can_request:
            logger.debug(f"Rate limited for {config.name}: {reason}")
            stats['skipped_rate_limit'] += len(tickers)
            return results

        # Check if we have an implementation
        impl = self._resolver_impls.get(config.resolver_type)
        if impl is None:
            logger.warning(f"No implementation registered for {config.resolver_type.name}")
            return results

        # Dry run mode
        if self.dry_run:
            logger.info(f"[DRY RUN] Would execute {config.name} for {len(tickers)} tickers")
            return results

        # Batch if supported
        batches = self._create_batches(tickers, config.max_batch_size)

        for batch in batches:
            try:
                start = time.time()
                batch_results = impl(batch, config)
                duration_ms = (time.time() - start) * 1000

                # Record success for rate limiting
                rate_state.record_request(True, config)
                stats['resolvers_executed'] += 1

                # Process results
                for ticker in batch:
                    req = reqs_by_ticker.get(ticker)
                    state = self._get_resolution_state(req) if req else None

                    if ticker in batch_results and batch_results[ticker]:
                        results[ticker] = batch_results[ticker]
                        stats['successful_enrichments'] += 1

                        if state:
                            state.record_attempt(
                                config.name, True,
                                data=batch_results[ticker],
                                duration_ms=duration_ms
                            )
                    else:
                        stats['failed_enrichments'] += 1
                        if state:
                            state.record_attempt(
                                config.name, False,
                                error="No data returned",
                                duration_ms=duration_ms
                            )

                logger.debug(f"Executed {config.name} for {len(batch)} tickers "
                             f"in {duration_ms:.0f}ms")

            except Exception as e:
                logger.error(f"Resolver {config.name} failed: {e}")
                rate_state.record_request(False, config)
                stats['failed_enrichments'] += len(batch)

                # Record failures for all tickers in batch
                for ticker in batch:
                    req = reqs_by_ticker.get(ticker)
                    if req:
                        state = self._get_resolution_state(req)
                        state.record_attempt(config.name, False, error=str(e))

        return results

    def _get_rate_limit_state(self, resolver_type: ResolverType) -> RateLimitState:
        """Get or create rate limit state for a resolver."""
        with self._lock:
            if resolver_type not in self.rate_limit_states:
                self.rate_limit_states[resolver_type] = RateLimitState(resolver_type)
            return self.rate_limit_states[resolver_type]

    def _get_resolution_state(self, req: DataRequirement) -> RequirementResolutionState:
        """Get or create resolution state for a requirement."""
        with self._lock:
            if req.requirement_id not in self.resolution_states:
                self.resolution_states[req.requirement_id] = RequirementResolutionState(req)
            return self.resolution_states[req.requirement_id]

    def _create_batches(self, items: List[str], batch_size: int) -> List[List[str]]:
        """Split items into batches."""
        if batch_size <= 1:
            return [[item] for item in items]
        return [items[i:i + batch_size] for i in range(0, len(items), batch_size)]

    def get_stats(self) -> Dict[str, Any]:
        """Get current executor statistics."""
        return {
            'rate_limit_states': {
                rt.name: {
                    'requests_this_minute': state.requests_this_minute,
                    'requests_this_hour': state.requests_this_hour,
                    'requests_today': state.requests_today,
                    'in_cooldown': state.cooldown_until is not None and
                                   datetime.now() < state.cooldown_until
                }
                for rt, state in self.rate_limit_states.items()
            },
            'resolution_states': {
                req_id: {
                    'status': state.status.name,
                    'attempts': state.attempt_count,
                    'can_attempt': state.can_attempt
                }
                for req_id, state in self.resolution_states.items()
            }
        }

    def reset_cooldowns(self):
        """Reset all cooldowns (for testing/debugging)."""
        with self._lock:
            for state in self.rate_limit_states.values():
                state.cooldown_until = None
            for state in self.resolution_states.values():
                state.status = ResolutionStatus.PENDING
                state.attempts = []
        logger.info("All cooldowns reset")


# Global executor instance
_executor: Optional[EnrichmentExecutor] = None


def get_enrichment_executor() -> EnrichmentExecutor:
    """Get or create the global enrichment executor."""
    global _executor
    if _executor is None:
        _executor = EnrichmentExecutor()
    return _executor


def execute_enrichment_cycle(
    blockers: Dict[str, TradeBlockers],
    requirement_types: Optional[List[RequirementType]] = None,
    executor: Optional[EnrichmentExecutor] = None
) -> Dict[str, Any]:
    """
    Convenience function to run one enrichment cycle.

    This is the main entry point for the enrichment system.
    """
    if executor is None:
        executor = get_enrichment_executor()

    return executor.execute_enrichment(blockers, requirement_types)
