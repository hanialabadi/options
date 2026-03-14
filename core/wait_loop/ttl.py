"""
TTL (Time-To-Live) & Expiry Logic

Prevents zombie trades by enforcing maximum wait times and automatic expiry.
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Tuple, Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)


# TTL Configuration by Strategy Type
TTL_CONFIG = {
    "DIRECTIONAL": {
        "max_sessions_wait": 3,          # Max 3 trading sessions
        "max_days_wait": 5,               # Max 5 calendar days
        "invalidate_if_no_progress": True,  # Reject if 0% progress after 2 sessions
        "min_sessions_for_progress_check": 2
    },
    "INCOME": {
        "max_sessions_wait": 5,          # More patience for income strategies
        "max_days_wait": 7,               # Max 7 calendar days
        "invalidate_if_no_progress": False,
        "min_sessions_for_progress_check": 3
    },
    "LEAP": {
        "max_sessions_wait": 10,         # Longer timeframe for LEAPs
        "max_days_wait": 14,              # Max 14 calendar days
        "invalidate_if_no_progress": False,
        "min_sessions_for_progress_check": 5
    },
    "DEFAULT": {
        "max_sessions_wait": 3,
        "max_days_wait": 5,
        "invalidate_if_no_progress": True,
        "min_sessions_for_progress_check": 2
    }
}


def get_ttl_config(strategy_type: str) -> Dict[str, Any]:
    """
    Get TTL configuration for strategy type.

    Args:
        strategy_type: Strategy type (DIRECTIONAL, INCOME, LEAP)

    Returns:
        TTL configuration dictionary
    """
    return TTL_CONFIG.get(strategy_type.upper(), TTL_CONFIG["DEFAULT"])


def _resolve_ttl_type(wait_entry: Dict[str, Any]) -> str:
    """Resolve TTL type from a wait entry, detecting LEAPs from strategy_name.

    LEAPs are classified as DIRECTIONAL for execution-gate routing but need
    the longer LEAP TTL (10 sessions / 14 days instead of 3 / 5).
    """
    strategy_name = str(wait_entry.get("strategy_name", "") or "").lower()
    if 'leap' in strategy_name:
        return 'LEAP'
    return wait_entry.get("strategy_type", "DEFAULT")


def calculate_expiry_deadline(
    start_time: datetime,
    strategy_type: str
) -> datetime:
    """
    Calculate when wait entry should expire.

    Args:
        start_time: When wait period started
        strategy_type: Strategy type

    Returns:
        Expiry deadline timestamp
    """
    config = get_ttl_config(strategy_type)
    max_days = config["max_days_wait"]

    return start_time + timedelta(days=max_days)


def should_expire(wait_entry: Dict[str, Any], now: Optional[datetime] = None) -> Tuple[bool, Optional[str]]:
    """
    Check if wait entry should be expired.

    Args:
        wait_entry: Wait entry dictionary from database
        now: Current timestamp (defaults to datetime.now())

    Returns:
        (should_expire: bool, reason: Optional[str])

    RAG Source: docs/SMART_WAIT_DESIGN.md - TTL & Expiry Logic
    """
    if now is None:
        now = datetime.now()

    _ttl_type = _resolve_ttl_type(wait_entry)
    ttl_config = get_ttl_config(_ttl_type)

    wait_started_at = wait_entry["wait_started_at"]
    wait_expires_at = wait_entry["wait_expires_at"]
    wait_progress = wait_entry.get("wait_progress", 0.0)
    evaluation_count = wait_entry.get("evaluation_count", 0)

    # 1. Hard TTL deadline (calendar days)
    if now > wait_expires_at:
        days_elapsed = (now - wait_started_at).days
        return True, f"TTL_EXPIRED: {days_elapsed} days (max: {ttl_config['max_days_wait']})"

    # 2. Session count exceeded
    sessions_elapsed = count_trading_sessions(wait_started_at, now)
    max_sessions = ttl_config["max_sessions_wait"]

    if sessions_elapsed > max_sessions:
        return True, f"MAX_SESSIONS: {sessions_elapsed}/{max_sessions} sessions"

    # 3. No progress and config requires it
    if ttl_config["invalidate_if_no_progress"]:
        min_sessions = ttl_config["min_sessions_for_progress_check"]

        if wait_progress == 0.0 and sessions_elapsed >= min_sessions:
            return True, f"NO_PROGRESS: 0% progress after {sessions_elapsed} sessions (threshold: {min_sessions})"

    # 4. Not expired
    return False, None


def count_trading_sessions(start_time: datetime, end_time: datetime) -> int:
    """
    Count number of trading sessions *after* creation.

    The creation day is not counted — an entry created Monday and evaluated
    Wednesday has elapsed 2 sessions (Tuesday, Wednesday), not 3.  This
    ensures ``max_sessions_wait=3`` gives 3 re-evaluation opportunities.

    Simplified implementation:
    - Counts weekdays (Mon-Fri) between start and end, excluding start
    - Does not account for market holidays (acceptable approximation)

    Args:
        start_time: Start timestamp
        end_time: End timestamp

    Returns:
        Number of trading sessions elapsed (excluding creation day)
    """
    start_date = start_time.date()
    end_date = end_time.date()

    if end_date <= start_date:
        return 0

    # Count business days from the day *after* creation to end (inclusive)
    next_day = start_date + timedelta(days=1)
    date_range = pd.date_range(
        start=next_day,
        end=end_date,
        freq='B'  # Business days (Mon-Fri)
    )

    return len(date_range)


def get_time_remaining(wait_entry: Dict[str, Any], now: Optional[datetime] = None) -> Dict[str, Any]:
    """
    Calculate time remaining until expiry.

    Args:
        wait_entry: Wait entry dictionary
        now: Current timestamp (defaults to datetime.now())

    Returns:
        Dictionary with:
        - days_remaining
        - hours_remaining
        - sessions_remaining
        - pct_time_used
    """
    if now is None:
        now = datetime.now()

    wait_started_at = wait_entry["wait_started_at"]
    wait_expires_at = wait_entry["wait_expires_at"]
    _ttl_type = _resolve_ttl_type(wait_entry)
    ttl_config = get_ttl_config(_ttl_type)

    # Time deltas
    time_remaining = wait_expires_at - now
    time_elapsed = now - wait_started_at
    total_time = wait_expires_at - wait_started_at

    days_remaining = time_remaining.days
    hours_remaining = time_remaining.total_seconds() / 3600

    # Session counts
    sessions_elapsed = count_trading_sessions(wait_started_at, now)
    max_sessions = ttl_config["max_sessions_wait"]
    sessions_remaining = max(0, max_sessions - sessions_elapsed)

    # Percentage of time used
    if total_time.total_seconds() > 0:
        pct_time_used = time_elapsed.total_seconds() / total_time.total_seconds()
    else:
        pct_time_used = 1.0

    return {
        "days_remaining": days_remaining,
        "hours_remaining": hours_remaining,
        "sessions_remaining": sessions_remaining,
        "sessions_elapsed": sessions_elapsed,
        "max_sessions": max_sessions,
        "pct_time_used": pct_time_used,
        "is_urgent": pct_time_used > 0.75  # Flag if >75% time used
    }


def validate_ttl_config():
    """
    Validate TTL configuration for consistency.

    Raises:
        ValueError: If configuration is invalid
    """
    for strategy_type, config in TTL_CONFIG.items():
        if config["max_days_wait"] < 1:
            raise ValueError(f"{strategy_type}: max_days_wait must be >= 1")

        if config["max_sessions_wait"] < 1:
            raise ValueError(f"{strategy_type}: max_sessions_wait must be >= 1")

        if config["invalidate_if_no_progress"]:
            min_sessions = config["min_sessions_for_progress_check"]
            if min_sessions < 1:
                raise ValueError(f"{strategy_type}: min_sessions_for_progress_check must be >= 1")
            if min_sessions > config["max_sessions_wait"]:
                raise ValueError(
                    f"{strategy_type}: min_sessions_for_progress_check ({min_sessions}) "
                    f"cannot exceed max_sessions_wait ({config['max_sessions_wait']})"
                )

    logger.info("[TTL_CONFIG] Validation passed")


# Validate config on module load
validate_ttl_config()


# ============================================================
# STAGNATION DECAY LOGIC
# Requirement 4: Detect and handle stagnating WAIT entries
# ============================================================

def detect_stagnation(wait_entry: Dict[str, Any], now: Optional[datetime] = None) -> Tuple[bool, Optional[str]]:
    """
    Detect if wait entry is stagnating (no progress toward conditions).

    DETERMINISTIC STAGNATION CRITERIA:
    1. Zero progress after 2+ evaluations
    2. Progress <10% after 50% of TTL elapsed
    3. No conditions met after 3+ evaluations

    Args:
        wait_entry: Wait entry dictionary
        now: Current timestamp (defaults to datetime.now())

    Returns:
        (is_stagnating: bool, reason: Optional[str])
    """
    if now is None:
        now = datetime.now()

    wait_progress = wait_entry.get("wait_progress", 0.0)
    evaluation_count = wait_entry.get("evaluation_count", 0)
    conditions_met = wait_entry.get("conditions_met", [])
    total_conditions = len(wait_entry.get("wait_conditions", []))

    wait_started_at = wait_entry["wait_started_at"]
    wait_expires_at = wait_entry["wait_expires_at"]

    # Calculate time elapsed as % of TTL
    total_ttl = wait_expires_at - wait_started_at
    time_elapsed = now - wait_started_at
    ttl_pct_elapsed = time_elapsed.total_seconds() / total_ttl.total_seconds() if total_ttl.total_seconds() > 0 else 1.0

    # ============================================================
    # STAGNATION CRITERION 1: Zero progress after 2+ evaluations
    # ============================================================
    if evaluation_count >= 2 and wait_progress == 0.0:
        logger.warning(
            f"[STAGNATION] {wait_entry.get('ticker')} {wait_entry.get('strategy_name')}: "
            f"Zero progress after {evaluation_count} evaluations"
        )
        return True, f"STAGNATION_ZERO_PROGRESS: 0% after {evaluation_count} evaluations"

    # ============================================================
    # STAGNATION CRITERION 2: <10% progress after 50% TTL
    # ============================================================
    if ttl_pct_elapsed > 0.5 and wait_progress < 0.1:
        logger.warning(
            f"[STAGNATION] {wait_entry.get('ticker')} {wait_entry.get('strategy_name')}: "
            f"Only {wait_progress:.1%} progress after {ttl_pct_elapsed:.1%} of TTL elapsed"
        )
        return True, f"STAGNATION_SLOW_PROGRESS: {wait_progress:.1%} after {ttl_pct_elapsed:.1%} TTL"

    # ============================================================
    # STAGNATION CRITERION 3: No conditions met after 3+ evaluations
    # ============================================================
    if evaluation_count >= 3 and len(conditions_met) == 0 and total_conditions > 0:
        logger.warning(
            f"[STAGNATION] {wait_entry.get('ticker')} {wait_entry.get('strategy_name')}: "
            f"No conditions met after {evaluation_count} evaluations (0/{total_conditions})"
        )
        return True, f"STAGNATION_NO_CONDITIONS: 0/{total_conditions} met after {evaluation_count} evaluations"

    # Not stagnating
    return False, None


def apply_stagnation_penalty(
    wait_entry: Dict[str, Any],
    now: Optional[datetime] = None
) -> Tuple[bool, Optional[str]]:
    """
    Apply penalty for stagnating entries by accelerating expiry.

    REQUIREMENT 4: Stagnation decay with explicit handling.

    AGGRESSIVE POLICY (updated):
    - Zero progress after 3+ evaluations = IMMEDIATE expiry
    - Zero progress after 2 evaluations = 75% TTL penalty (accelerated)
    - <10% progress after 50% TTL = 50% TTL penalty

    This prevents zombie trades from lingering without progress.

    Args:
        wait_entry: Wait entry dictionary
        now: Current timestamp

    Returns:
        (should_expire: bool, reason: Optional[str])
    """
    if now is None:
        now = datetime.now()

    is_stagnating, stagnation_reason = detect_stagnation(wait_entry, now)

    if not is_stagnating:
        return False, None

    wait_progress = wait_entry.get("wait_progress", 0.0)
    evaluation_count = wait_entry.get("evaluation_count", 0)
    ticker = wait_entry.get('ticker', 'UNKNOWN')
    strategy = wait_entry.get('strategy_name', 'UNKNOWN')

    # ============================================================
    # AGGRESSIVE POLICY: Immediate expiry for hopeless entries
    # ============================================================

    # IMMEDIATE EXPIRY: Zero progress after 3+ evaluations
    # These entries have conditions that cannot be satisfied
    if wait_progress == 0.0 and evaluation_count >= 3:
        logger.warning(
            f"[STAGNATION_PENALTY] {ticker} {strategy}: "
            f"IMMEDIATE EXPIRY - 0% progress after {evaluation_count} evaluations. "
            f"Conditions likely unsatisfiable."
        )
        return True, f"STAGNATION_IMMEDIATE: 0% progress after {evaluation_count} evaluations (conditions unsatisfiable)"

    # Check if TTL penalty should be applied
    wait_expires_at = wait_entry["wait_expires_at"]
    time_remaining = wait_expires_at - now

    # If already expired or negative time, expire now
    if time_remaining.total_seconds() <= 0:
        return True, f"STAGNATION_EXPIRY: TTL already exceeded"

    wait_started_at = wait_entry["wait_started_at"]
    total_ttl = wait_expires_at - wait_started_at
    ttl_pct_remaining = time_remaining.total_seconds() / total_ttl.total_seconds()

    # If already near expiry (<25% TTL remaining), just let it expire normally
    if ttl_pct_remaining < 0.25:
        logger.info(
            f"[STAGNATION_PENALTY] {ticker}: "
            f"Already near expiry ({ttl_pct_remaining:.1%} remaining), allowing natural expiration"
        )
        return False, None

    # ============================================================
    # ACCELERATED PENALTIES
    # ============================================================

    # SEVERE PENALTY (75%): Zero progress after 2 evaluations
    if wait_progress == 0.0 and evaluation_count >= 2:
        penalized_expiry = now + (time_remaining / 4)  # Only 25% of remaining time
        if now > penalized_expiry:
            logger.warning(
                f"[STAGNATION_PENALTY] {ticker} {strategy}: "
                f"EXPIRED (75% penalty). 0% progress after {evaluation_count} evals."
            )
            return True, f"STAGNATION_EXPIRY: {stagnation_reason} + 75% TTL penalty"
        else:
            logger.info(
                f"[STAGNATION_PENALTY] {ticker}: "
                f"75% penalty applied. New expiry: {penalized_expiry}"
            )
            return False, None

    # MODERATE PENALTY (50%): Slow progress
    penalized_expiry = now + (time_remaining / 2)

    if now > penalized_expiry:
        logger.warning(
            f"[STAGNATION_PENALTY] {ticker} {strategy}: "
            f"EXPIRED (50% penalty). {stagnation_reason}"
        )
        return True, f"STAGNATION_EXPIRY: {stagnation_reason} + 50% TTL penalty"

    # Not yet expired even with penalty
    logger.info(
        f"[STAGNATION_PENALTY] {ticker}: "
        f"50% penalty applied. Original: {wait_expires_at}, Penalized: {penalized_expiry}"
    )
    return False, None
