"""
Fidelity IV Trigger Logic: Deterministic Pre-Execution Validator

REQUIREMENT 3: Explicit Fidelity triggering with detailed logging.

Fidelity IV is a VERIFICATION step, not required for discovery.
It provides long-term IV context (252D+) for INCOME strategies and
immature DIRECTIONAL setups to validate edge sustainability.

Design Principle: Fidelity is a quality validator, not a discovery blocker.
"""

from typing import Tuple, Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)


def should_trigger_fidelity(
    strategy_type: str,
    iv_maturity_state: str,
    iv_source: str,
    liquidity_grade: str,
    data_completeness: str,
    ticker: str,
    strategy_name: str
) -> Tuple[bool, str]:
    """
    Determine if Fidelity Long-Term IV enrichment should be triggered.

    DETERMINISTIC RULES (RAG-grounded):
    - R1: INCOME strategies ALWAYS require Fidelity (long-term edge validation)
    - R2: DIRECTIONAL + MATURE Schwab IV → NO Fidelity (sufficient for execution)
    - R3: DIRECTIONAL + IMMATURE Schwab IV → Fidelity RECOMMENDED (but not blocking)
    - R4: Illiquid or Missing data → NO Fidelity (fix structural issues first)

    Args:
        strategy_type: DIRECTIONAL | INCOME | LEAP | VOLATILITY
        iv_maturity_state: MATURE | IMMATURE | MISSING
        iv_source: Schwab | Fidelity | None
        liquidity_grade: Excellent | Good | Acceptable | Thin | Illiquid
        data_completeness: Complete | Partial | Missing
        ticker: Ticker symbol (for logging)
        strategy_name: Strategy name (for logging)

    Returns:
        (should_trigger: bool, reason: str)

    RAG Sources:
    - scan_engine/step12_acceptance.py:307-345 (R0.3, R0.4, R0.5 rules)
    - docs/EXECUTION_SEMANTICS.md:49-67 (Escalation Eligibility)
    """
    # Normalize inputs
    strategy_type = strategy_type.upper() if strategy_type else "UNKNOWN"
    iv_maturity_state = iv_maturity_state.upper() if iv_maturity_state else "MISSING"
    liquidity_grade = liquidity_grade.title() if liquidity_grade else "Unknown"

    # ============================================================
    # R0: Pre-flight checks (structural blockers)
    # ============================================================

    # R0.1: Missing data blocks Fidelity (fix Schwab data first)
    if data_completeness == "Missing":
        logger.info(
            f"[FIDELITY_SKIP] {ticker} {strategy_name}: "
            f"R0.1 - Missing critical data (Schwab data incomplete)"
        )
        return False, "SKIP_R0.1: Missing critical Schwab data"

    # R0.2: Illiquid contracts don't need Fidelity (will be rejected anyway)
    if liquidity_grade == "Illiquid":
        logger.info(
            f"[FIDELITY_SKIP] {ticker} {strategy_name}: "
            f"R0.2 - Illiquid contract (structural rejection)"
        )
        return False, "SKIP_R0.2: Illiquid contract"

    # ============================================================
    # R1: INCOME strategies - Fidelity only if IMMATURE
    # ============================================================
    # Natenberg Ch.4: Premium selling requires IV percentile context
    # If DuckDB has 252+ days (MATURE), Schwab IV is sufficient
    if strategy_type == "INCOME":
        if iv_maturity_state == "MATURE":
            logger.info(
                f"[FIDELITY_SKIP] {ticker} {strategy_name}: "
                f"R1 - INCOME with MATURE DuckDB IV (252+ days sufficient for percentile ranking)"
            )
            return False, "SKIP_R1: INCOME strategy with MATURE DuckDB IV (252+ days, per Natenberg Ch.4)"
        else:
            logger.info(
                f"[FIDELITY_TRIGGER] {ticker} {strategy_name}: "
                f"R1 - INCOME with {iv_maturity_state} IV requires Fidelity validation"
            )
            return True, f"TRIGGER_R1: INCOME strategy with {iv_maturity_state} IV requires Fidelity validation"

    # ============================================================
    # R2: DIRECTIONAL + MATURE Schwab IV → NO Fidelity needed
    # ============================================================
    if strategy_type == "DIRECTIONAL":
        if iv_source == "Schwab" and iv_maturity_state == "MATURE":
            logger.info(
                f"[FIDELITY_SKIP] {ticker} {strategy_name}: "
                f"R2 - DIRECTIONAL + MATURE Schwab IV (sufficient for execution)"
            )
            return False, "SKIP_R2: DIRECTIONAL strategy with MATURE Schwab IV (sufficient)"

        # R3: DIRECTIONAL + IMMATURE → Fidelity RECOMMENDED but not blocking
        elif iv_maturity_state in ["IMMATURE", "MISSING"]:
            logger.info(
                f"[FIDELITY_TRIGGER] {ticker} {strategy_name}: "
                f"R3 - DIRECTIONAL + {iv_maturity_state} IV (Fidelity recommended for maturity validation)"
            )
            return True, f"TRIGGER_R3: DIRECTIONAL strategy with {iv_maturity_state} IV (Fidelity recommended)"

        else:
            # Unknown IV state for DIRECTIONAL
            logger.warning(
                f"[FIDELITY_SKIP] {ticker} {strategy_name}: "
                f"R3.1 - DIRECTIONAL with unknown IV state: {iv_maturity_state}"
            )
            return False, f"SKIP_R3.1: DIRECTIONAL with unknown IV state ({iv_maturity_state})"

    # ============================================================
    # R4: LEAP strategies - Fidelity only if IMMATURE
    # ============================================================
    # Passarelli Ch.8: LEAPs need term structure, not deeper history
    # System already has IV_360D from Schwab constant-maturity interpolation
    if strategy_type == "LEAP":
        if iv_maturity_state == "MATURE":
            logger.info(
                f"[FIDELITY_SKIP] {ticker} {strategy_name}: "
                f"R4 - LEAP with MATURE IV and term structure (360D available from Schwab)"
            )
            return False, "SKIP_R4: LEAP with MATURE IV and term structure (IV_360D available, per Passarelli Ch.8)"
        else:
            logger.info(
                f"[FIDELITY_TRIGGER] {ticker} {strategy_name}: "
                f"R4 - LEAP with {iv_maturity_state} IV requires validation"
            )
            return True, f"TRIGGER_R4: LEAP strategy with {iv_maturity_state} IV requires validation"

    # ============================================================
    # R5: Default fallback (VOLATILITY, UNKNOWN, etc.)
    # ============================================================
    logger.info(
        f"[FIDELITY_SKIP] {ticker} {strategy_name}: "
        f"R5 - Strategy type {strategy_type} does not require Fidelity"
    )
    return False, f"SKIP_R5: Strategy type {strategy_type} does not require Fidelity"


def log_fidelity_decision_summary(
    total_evaluated: int,
    fidelity_triggered: int,
    fidelity_skipped: int,
    skip_reasons: Dict[str, int],
    trigger_reasons: Dict[str, int]
):
    """
    Log comprehensive summary of Fidelity triggering decisions.

    Args:
        total_evaluated: Total strategies evaluated
        fidelity_triggered: Count of Fidelity triggers
        fidelity_skipped: Count of Fidelity skips
        skip_reasons: Dict of skip reason → count
        trigger_reasons: Dict of trigger reason → count
    """
    logger.info("")
    logger.info("📊 FIDELITY TRIGGER DECISION SUMMARY")
    logger.info("─" * 60)
    logger.info(f"   Total strategies evaluated: {total_evaluated}")
    logger.info(f"   Fidelity triggered: {fidelity_triggered} ({fidelity_triggered/total_evaluated*100:.1f}%)")
    logger.info(f"   Fidelity skipped: {fidelity_skipped} ({fidelity_skipped/total_evaluated*100:.1f}%)")

    if skip_reasons:
        logger.info(f"   Skip reasons:")
        for reason, count in sorted(skip_reasons.items(), key=lambda x: -x[1]):
            logger.info(f"      {reason}: {count}")

    if trigger_reasons:
        logger.info(f"   Trigger reasons:")
        for reason, count in sorted(trigger_reasons.items(), key=lambda x: -x[1]):
            logger.info(f"      {reason}: {count}")

    logger.info("─" * 60)
    logger.info("")


def is_fidelity_data_stale(
    last_fidelity_fetch: Optional[str],
    max_age_trading_days: int = 2
) -> Tuple[bool, str]:
    """
    Check if cached Fidelity data is stale and needs refresh.

    Args:
        last_fidelity_fetch: Timestamp of last Fidelity fetch (ISO format)
        max_age_trading_days: Maximum age in trading days before stale

    Returns:
        (is_stale: bool, reason: str)
    """
    if not last_fidelity_fetch:
        return True, "NO_CACHE: No Fidelity data in cache"

    from datetime import datetime
    from core.shared.data_layer.market_time import fidelity_freshness

    try:
        last_fetch = datetime.fromisoformat(last_fidelity_fetch)
        is_fresh, age_td = fidelity_freshness(last_fetch, max_trading_days=max_age_trading_days)
        if not is_fresh:
            return True, f"STALE_CACHE: Last fetch {age_td} trading days ago (max: {max_age_trading_days})"
        return False, f"FRESH_CACHE: Last fetch {age_td} trading days ago"

    except Exception as e:
        logger.warning(f"Could not parse Fidelity fetch timestamp: {e}")
        return True, f"INVALID_TIMESTAMP: {e}"
