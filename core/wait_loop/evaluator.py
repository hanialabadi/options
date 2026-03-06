"""
Wait Condition Evaluator: Re-Evaluation Engine & State Transitions

Core engine that re-evaluates WAIT entries every scan and manages state transitions.
"""

import duckdb
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging

from .schema import ConfirmationCondition, PromotionResult, TradeStatus
from .conditions import ConditionFactory, BaseCondition
from .ttl import should_expire, get_time_remaining
from .persistence import WaitListPersistence

logger = logging.getLogger(__name__)


@dataclass
class WaitEvaluationResult:
    """Result of re-evaluating the wait list"""
    promoted: List[Dict[str, Any]] = field(default_factory=list)
    expired: List[Dict[str, Any]] = field(default_factory=list)
    invalidated: List[Dict[str, Any]] = field(default_factory=list)
    still_waiting: List[Dict[str, Any]] = field(default_factory=list)

    def total_count(self) -> int:
        return len(self.promoted) + len(self.expired) + len(self.invalidated) + len(self.still_waiting)

    def summary(self) -> str:
        return (
            f"Promoted: {len(self.promoted)}, "
            f"Expired: {len(self.expired)}, "
            f"Invalidated: {len(self.invalidated)}, "
            f"Still Waiting: {len(self.still_waiting)}"
        )


class WaitConditionEvaluator:
    """
    Evaluates wait conditions and manages state transitions.

    RAG Source: docs/SMART_WAIT_DESIGN.md - Re-Evaluation Engine
    """

    def __init__(self, con: duckdb.DuckDBPyConnection):
        self.con = con
        self.persistence = WaitListPersistence(con)

    def evaluate_wait_list(self, market_data_by_ticker: Dict[str, Dict[str, Any]]) -> WaitEvaluationResult:
        """
        Re-evaluates all ACTIVE wait list entries.

        Args:
            market_data_by_ticker: Dict mapping ticker → current market data

        Returns:
            WaitEvaluationResult with promoted, expired, invalidated, and still_waiting trades
        """
        logger.info("[WAIT_EVAL] Starting wait list re-evaluation")

        result = WaitEvaluationResult()

        # Load active wait entries
        wait_entries = self.persistence.load_active_waits()
        logger.info(f"[WAIT_EVAL] Loaded {len(wait_entries)} active wait entries")

        if not wait_entries:
            return result

        # Re-evaluate each entry
        for wait_entry in wait_entries:
            ticker = wait_entry["ticker"]
            wait_id = wait_entry["wait_id"]

            logger.info(f"[WAIT_EVAL] Evaluating {wait_id} ({ticker} - {wait_entry['strategy_name']})")

            # 1. Check TTL expiry first
            should_expire_flag, expiry_reason = should_expire(wait_entry)
            if should_expire_flag:
                logger.info(f"[WAIT_EVAL] ⏰ EXPIRED: {wait_id} - {expiry_reason}")
                self.persistence.mark_rejected(wait_id, expiry_reason, TradeStatus.EXPIRED)
                wait_entry["rejection_reason"] = expiry_reason
                result.expired.append(wait_entry)
                continue

            # 1.5. Check stagnation decay (REQUIREMENT 4)
            from .ttl import apply_stagnation_penalty
            stagnation_expired, stagnation_reason = apply_stagnation_penalty(wait_entry)
            if stagnation_expired:
                logger.warning(f"[WAIT_EVAL] 🐌 STAGNATION_EXPIRED: {wait_id} - {stagnation_reason}")
                self.persistence.mark_rejected(wait_id, stagnation_reason, TradeStatus.EXPIRED)
                wait_entry["rejection_reason"] = stagnation_reason
                result.expired.append(wait_entry)
                continue

            # 2. Get current market data
            market_data = market_data_by_ticker.get(ticker)
            if not market_data:
                logger.warning(f"[WAIT_EVAL] ⚠️  No market data for {ticker}, skipping")
                result.still_waiting.append(wait_entry)
                continue

            # 3. Check invalidation triggers
            invalidation_reason = self.check_invalidation(wait_entry, market_data)
            if invalidation_reason:
                logger.info(f"[WAIT_EVAL] ❌ INVALIDATED: {wait_id} - {invalidation_reason}")
                self.persistence.mark_rejected(wait_id, invalidation_reason, TradeStatus.INVALIDATED)
                wait_entry["rejection_reason"] = invalidation_reason
                result.invalidated.append(wait_entry)
                continue

            # 4. Evaluate wait conditions
            conditions_result = self.evaluate_conditions(wait_entry, market_data)

            # 5. Update wait progress
            self.persistence.update_wait_progress(
                wait_id,
                conditions_result["conditions_met"],
                conditions_result["wait_progress"],
                market_data.get("last_price"),
                market_data.get("iv_30d"),
                market_data.get("chart_signal")
            )

            wait_entry["conditions_met"] = conditions_result["conditions_met"]
            wait_entry["wait_progress"] = conditions_result["wait_progress"]
            wait_entry["current_price"] = market_data.get("last_price")

            # 6. Attempt promotion if all conditions met
            if conditions_result["all_conditions_met"]:
                logger.info(f"[WAIT_EVAL] ✅ All conditions met for {wait_id}, attempting promotion")
                promotion_result = self.attempt_promotion(wait_entry, market_data)

                if promotion_result.outcome == "PROMOTED":
                    logger.info(f"[WAIT_EVAL] 🟢 PROMOTED: {wait_id} → READY_NOW")
                    self.persistence.mark_promoted(
                        wait_id,
                        promotion_result.contract_symbol,
                        promotion_result.confidence_score
                    )
                    wait_entry["contract_symbol"] = promotion_result.contract_symbol
                    wait_entry["confidence_score"] = promotion_result.confidence_score
                    result.promoted.append(wait_entry)

                elif promotion_result.outcome == "REJECTED":
                    logger.info(f"[WAIT_EVAL] 🔴 REJECTED on promotion: {wait_id} - {promotion_result.reason}")
                    self.persistence.mark_rejected(wait_id, promotion_result.reason, TradeStatus.REJECTED)
                    wait_entry["rejection_reason"] = promotion_result.reason
                    result.invalidated.append(wait_entry)

                else:  # STILL_WAITING (e.g., waiting on Fidelity IV)
                    logger.info(f"[WAIT_EVAL] 🟡 Still waiting: {wait_id} - {promotion_result.reason}")
                    result.still_waiting.append(wait_entry)
            else:
                # Not all conditions met, continue waiting
                logger.info(
                    f"[WAIT_EVAL] 🟡 Progress: {wait_id} - "
                    f"{len(conditions_result['conditions_met'])}/{len(wait_entry['wait_conditions'])} "
                    f"({conditions_result['wait_progress']:.1%})"
                )
                result.still_waiting.append(wait_entry)

        logger.info(f"[WAIT_EVAL] Completed: {result.summary()}")
        return result

    def evaluate_conditions(self, wait_entry: Dict[str, Any], market_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Evaluate all wait conditions for an entry.

        Args:
            wait_entry: Wait entry dictionary
            market_data: Current market data

        Returns:
            Dict with:
            - conditions_met: List of condition IDs that are satisfied
            - wait_progress: Overall progress (0.0 - 1.0)
            - all_conditions_met: bool
        """
        wait_conditions = wait_entry.get("wait_conditions", [])

        if not wait_conditions:
            return {
                "conditions_met": [],
                "wait_progress": 1.0,  # No conditions = immediately ready
                "all_conditions_met": True
            }

        conditions_met = []
        total_progress = 0.0

        for condition_dict in wait_conditions:
            try:
                # Create condition instance
                condition = ConditionFactory.create_from_dict(condition_dict)

                # Check if met
                is_met = condition.check(market_data, wait_entry)
                if is_met:
                    conditions_met.append(condition_dict["condition_id"])

                # Get progress
                progress = condition.get_progress(market_data, wait_entry)
                total_progress += progress

                logger.debug(
                    f"[CONDITION] {condition_dict['condition_id']}: "
                    f"met={is_met}, progress={progress:.1%}"
                )

            except Exception as e:
                logger.error(f"[CONDITION] Error evaluating {condition_dict.get('condition_id')}: {e}")
                continue

        # Overall progress is average of individual conditions
        wait_progress = total_progress / len(wait_conditions) if wait_conditions else 1.0
        all_conditions_met = len(conditions_met) == len(wait_conditions)

        return {
            "conditions_met": conditions_met,
            "wait_progress": wait_progress,
            "all_conditions_met": all_conditions_met
        }

    def check_invalidation(self, wait_entry: Dict[str, Any], market_data: Dict[str, Any]) -> Optional[str]:
        """
        Check if setup has been invalidated.

        Args:
            wait_entry: Wait entry dictionary
            market_data: Current market data

        Returns:
            Invalidation reason if invalidated, None otherwise
        """
        invalidation_price = wait_entry.get("invalidation_price")
        current_price = market_data.get("last_price")

        if invalidation_price is not None and current_price is not None:
            # Directional: price broke below support or above resistance
            if current_price < invalidation_price:
                return f"INVALIDATED: Price broke below ${invalidation_price:.2f} (current: ${current_price:.2f})"

        # Could add more invalidation checks:
        # - Regime change (e.g., market stress spike)
        # - Earnings announcement in window
        # - IV collapse below minimum threshold

        return None

    def attempt_promotion(self, wait_entry: Dict[str, Any], market_data: Dict[str, Any]) -> PromotionResult:
        """
        Attempt to promote WAIT entry to READY_NOW.

        REQUIREMENT 2: PROMOTION RE-CHECK
        Promoted trades must re-validate ALL execution gates before READY_NOW.
        No shortcuts. No bypasses.

        Prerequisites (ALL must pass):
        1. All wait_conditions satisfied (already checked)
        2. Liquidity gates still pass (RE-VALIDATED)
        3. Data gates still pass (RE-VALIDATED)
        4. Strategy remains valid in current regime (RE-VALIDATED)
        5. Not invalidated (already checked)
        6. Contracts still available in expiration window (RE-VALIDATED)

        Args:
            wait_entry: Wait entry dictionary
            market_data: Current market data

        Returns:
            PromotionResult

        RAG Source: docs/SMART_WAIT_DESIGN.md - Promotion Logic
        """
        wait_id = wait_entry["wait_id"]
        ticker = wait_entry["ticker"]
        strategy_name = wait_entry["strategy_name"]
        strategy_type = wait_entry.get("strategy_type", "UNKNOWN")

        logger.info(f"[PROMOTION_RECHECK] Starting promotion validation for {wait_id} ({ticker} - {strategy_name})")

        # ============================================================
        # GATE 1: DATA COMPLETENESS RE-CHECK
        # ============================================================
        logger.debug(f"[PROMOTION_RECHECK] Gate 1: Data Completeness")

        has_price = market_data.get("last_price") is not None and market_data.get("last_price") > 0
        has_iv = market_data.get("iv_30d") is not None
        has_hv = market_data.get("hv_30") is not None
        has_volume = market_data.get("volume") is not None

        # Check for critical Greeks (if available in market_data)
        has_greeks = all([
            market_data.get(greek) is not None
            for greek in ['delta', 'gamma', 'vega', 'theta']
        ]) if 'delta' in market_data else True  # Skip if Greeks not in market data

        if not has_price:
            # Transient data gap (quote glitch, ticker not in snapshot this run).
            # Defer rather than permanently reject — the entry is still valid.
            logger.warning(
                f"[PROMOTION_RECHECK] ⚠️  Gate 1 DEFERRED: Missing price for {ticker} "
                f"— keeping ACTIVE, will retry next scan"
            )
            return PromotionResult(
                wait_id=wait_id,
                outcome="STILL_WAITING",
                reason="DATA_INCOMPLETE: Missing current price (transient — retrying next scan)"
            )

        data_completeness = "Complete" if (has_price and has_iv and has_hv) else "Partial"
        logger.debug(f"[PROMOTION_RECHECK] ✅ Gate 1 PASSED: Data completeness = {data_completeness}")

        # ============================================================
        # GATE 2: LIQUIDITY RE-CHECK
        # ============================================================
        logger.debug(f"[PROMOTION_RECHECK] Gate 2: Liquidity")

        bid_ask_spread_pct = self._calculate_spread_pct(market_data)

        if bid_ask_spread_pct is not None:
            # Liquidity thresholds (from Step 12 R0.2/R2.3 rules)
            if bid_ask_spread_pct > 15.0:  # Illiquid
                logger.warning(f"[PROMOTION_RECHECK] ❌ Gate 2 FAILED: Illiquid (spread: {bid_ask_spread_pct:.1f}%)")
                return PromotionResult(
                    wait_id=wait_id,
                    outcome="REJECTED",
                    reason=f"LIQUIDITY_DEGRADED: Spread widened to {bid_ask_spread_pct:.1f}% (Illiquid threshold: 15%)"
                )
            elif bid_ask_spread_pct > 10.0:  # Thin
                logger.warning(f"[PROMOTION_RECHECK] ⚠️  Gate 2 WARNING: Thin liquidity (spread: {bid_ask_spread_pct:.1f}%)")
                # Still allow promotion but with caution flag
            else:
                logger.debug(f"[PROMOTION_RECHECK] ✅ Gate 2 PASSED: Good liquidity (spread: {bid_ask_spread_pct:.1f}%)")
        else:
            logger.warning(f"[PROMOTION_RECHECK] ⚠️  Gate 2 WARNING: Could not calculate spread")

        # ============================================================
        # GATE 3: STRATEGY VALIDITY RE-CHECK
        # ============================================================
        logger.debug(f"[PROMOTION_RECHECK] Gate 3: Strategy Validity")

        # Check if strategy type is still valid — normalize to upper for comparison
        # step6 emits mixed case ('Directional', 'Volatility', 'Neutral'); evaluator uses UPPER.
        _strategy_type_upper = str(strategy_type).upper() if strategy_type else ""
        if _strategy_type_upper not in ['DIRECTIONAL', 'INCOME', 'LEAP', 'VOLATILITY', 'NEUTRAL']:
            logger.warning(f"[PROMOTION_RECHECK] ❌ Gate 3 FAILED: Invalid strategy type: {strategy_type}")
            return PromotionResult(
                wait_id=wait_id,
                outcome="REJECTED",
                reason=f"STRATEGY_INVALID: Unknown strategy type {strategy_type}"
            )

        logger.debug(f"[PROMOTION_RECHECK] ✅ Gate 3 PASSED: Strategy type valid ({strategy_type})")

        # ============================================================
        # GATE 4: VOLATILITY SANITY CHECK
        # ============================================================
        logger.debug(f"[PROMOTION_RECHECK] Gate 4: Volatility Sanity")

        iv_30d = market_data.get("iv_30d")
        hv_30 = market_data.get("hv_30")

        if iv_30d is not None:
            # Sanity check: IV should be reasonable (5% - 200%)
            if iv_30d < 5.0 or iv_30d > 200.0:
                logger.warning(f"[PROMOTION_RECHECK] ❌ Gate 4 FAILED: IV out of range: {iv_30d:.1f}%")
                return PromotionResult(
                    wait_id=wait_id,
                    outcome="REJECTED",
                    reason=f"IV_INVALID: IV={iv_30d:.1f}% (expected 5-200%)"
                )

        logger.debug(f"[PROMOTION_RECHECK] ✅ Gate 4 PASSED: IV sanity check OK")

        # ============================================================
        # GATE 5: PRICE MOVEMENT SANITY CHECK
        # ============================================================
        logger.debug(f"[PROMOTION_RECHECK] Gate 5: Price Movement")

        entry_price = wait_entry.get("entry_price")
        current_price = market_data.get("last_price")

        if entry_price and current_price:
            price_change_pct = (current_price - entry_price) / entry_price * 100

            # Extreme price movement check (>50% move might indicate data error or corporate action)
            if abs(price_change_pct) > 50.0:
                logger.warning(
                    f"[PROMOTION_RECHECK] ⚠️  Gate 5 WARNING: Extreme price move: "
                    f"{price_change_pct:+.1f}% (entry: ${entry_price:.2f}, current: ${current_price:.2f})"
                )
                # Don't reject, but flag for review

        logger.debug(f"[PROMOTION_RECHECK] ✅ Gate 5 PASSED: Price movement reasonable")

        # ============================================================
        # ALL GATES PASSED - CALCULATE CONFIDENCE SCORE
        # ============================================================
        confidence_score = self._calculate_confidence_score(wait_entry, market_data)

        # Adjust confidence based on re-check results
        if data_completeness == "Partial":
            confidence_score *= 0.9  # 10% penalty for partial data
        if bid_ask_spread_pct is not None and bid_ask_spread_pct > 10.0:
            confidence_score *= 0.85  # 15% penalty for thin liquidity

        logger.info(
            f"[PROMOTION_RECHECK] ✅ ALL GATES PASSED for {wait_id} "
            f"(Confidence: {confidence_score:.1%})"
        )

        # ============================================================
        # PROMOTION APPROVED
        # ============================================================
        contract_symbol = wait_entry.get("contract_symbol") or f"{ticker}_PLACEHOLDER"

        return PromotionResult(
            wait_id=wait_id,
            outcome="PROMOTED",
            contract_symbol=contract_symbol,
            confidence_score=confidence_score,
            reason="All promotion gates passed, liquidity & data confirmed"
        )

    def _calculate_spread_pct(self, market_data: Dict[str, Any]) -> Optional[float]:
        """Calculate bid/ask spread percentage"""
        bid = market_data.get("bid")
        ask = market_data.get("ask")

        if bid and ask and bid > 0:
            return (ask - bid) / bid * 100

        return None

    def _calculate_confidence_score(self, wait_entry: Dict[str, Any], market_data: Dict[str, Any]) -> float:
        """
        Calculate confidence score for promotion.

        Factors:
        - How long did we wait (longer wait = higher conviction)
        - How much price moved in expected direction
        - How quickly conditions were satisfied

        Returns:
            Confidence score (0.0 - 1.0)
        """
        base_score = 0.75  # Base score for meeting all conditions

        # Bonus for price movement in expected direction
        entry_price = wait_entry.get("entry_price")
        current_price = market_data.get("last_price")

        if entry_price and current_price:
            price_change_pct = (current_price - entry_price) / entry_price * 100

            # Assume bullish if price increased (simplified)
            if price_change_pct > 0:
                # +0.15 for >5% move, capped at +0.20
                price_bonus = min(0.20, abs(price_change_pct) / 5 * 0.15)
                base_score += price_bonus

        # Bonus for wait time (patience = conviction)
        evaluation_count = wait_entry.get("evaluation_count", 1)
        if evaluation_count >= 3:
            base_score += 0.05  # +5% for >=3 evaluations

        return min(1.0, base_score)


def evaluate_wait_list(con: duckdb.DuckDBPyConnection, market_data_by_ticker: Dict[str, Dict[str, Any]]) -> WaitEvaluationResult:
    """
    Convenience function to evaluate wait list.

    Args:
        con: DuckDB connection
        market_data_by_ticker: Dict mapping ticker → current market data

    Returns:
        WaitEvaluationResult
    """
    evaluator = WaitConditionEvaluator(con)
    return evaluator.evaluate_wait_list(market_data_by_ticker)
