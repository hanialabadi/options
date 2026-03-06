"""
Wait Condition Generator: Create explicit, testable confirmation conditions

Analyzes rejection reasons from Step 12 and generates appropriate wait conditions
for AWAIT_CONFIRMATION status.

RAG Source: docs/SMART_WAIT_DESIGN.md - Confirmation Condition Types
"""

from typing import List, Dict, Any, Optional
from core.wait_loop.schema import ConfirmationCondition, ConditionType
import uuid
import logging

logger = logging.getLogger(__name__)


def generate_wait_conditions_for_gate(
    gate_reason: str,
    row_context: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Generate wait conditions based on gate rejection reason.

    Args:
        gate_reason: Gate reason from Step 12 (e.g., "R2.3: Liquidity is Thin")
        row_context: Row data with ticker, price, IV, liquidity metrics, etc.

    Returns:
        List of condition dictionaries ready for JSON storage
    """
    conditions = []

    # Extract gate code
    gate_code = gate_reason.split(":")[0].strip() if ":" in gate_reason else gate_reason

    # R2.2: Directional + immature IV
    if "R2.2" in gate_code:
        # Wait for IV to mature (settle into stable range)
        iv_30d = row_context.get("iv_30d")
        if iv_30d and iv_30d > 30:
            conditions.append(_create_volatility_condition(
                metric="iv_30d",
                operator="less_than",
                threshold=iv_30d * 0.9,  # Wait for IV to drop 10%
                description=f"IV settles below {iv_30d * 0.9:.1f}%"
            ))
        # Also add time delay as fallback
        conditions.append(_create_time_delay_condition(
            delay_sessions=2,
            description="Wait 2 trading sessions for IV maturity"
        ))

    # R2.3: Thin/Acceptable liquidity
    # REQUIREMENT 4: Explicit "liquidity must improve" condition
    elif "R2.3" in gate_code:
        # Wait for liquidity improvement - EXPLICIT and MEASURABLE
        bid = row_context.get("bid")
        ask = row_context.get("ask")

        if bid and ask and bid > 0:
            current_spread_pct = (ask - bid) / bid * 100

            # DETERMINISTIC THRESHOLDS (no discretion):
            # - Thin: 10-15% spread → Target: <7.5% (meaningful improvement)
            # - Acceptable: 7-10% spread → Target: <5.0% (Good liquidity)

            if current_spread_pct > 10.0:  # Thin liquidity
                target_spread = 7.5  # Must improve to below Thin threshold
                logger.info(
                    f"[WAIT_GEN] {row_context.get('ticker')}: "
                    f"R2.3 Thin liquidity ({current_spread_pct:.1f}%) - "
                    f"MUST improve to <{target_spread:.1f}%"
                )
            else:  # Acceptable liquidity (7-10%)
                target_spread = 5.0  # Must improve to Good liquidity
                logger.info(
                    f"[WAIT_GEN] {row_context.get('ticker')}: "
                    f"R2.3 Acceptable liquidity ({current_spread_pct:.1f}%) - "
                    f"MUST improve to <{target_spread:.1f}%"
                )

            # EXPLICIT liquidity improvement condition
            conditions.append(_create_liquidity_condition(
                metric="bid_ask_spread_pct",
                operator="less_than",
                threshold=target_spread,
                description=f"Liquidity MUST improve: spread <{target_spread:.1f}% (current: {current_spread_pct:.1f}%)"
            ))
        else:
            # ARCHITECTURAL GUARD: Missing bid/ask is structural failure, not timing issue
            logger.error(
                f"[WAIT_GEN] {row_context.get('ticker')}: "
                f"R2.3 No bid/ask data - STRUCTURAL failure, must be REJECTED upstream"
            )
            # Return empty conditions - signals should_reject_permanently() to handle it
            return []

        # Also wait for next session (liquidity often improves after market open)
        conditions.append(_create_time_delay_condition(
            next_session=True,
            description="Wait for next trading session"
        ))

    # R3.2.TIMING: TQS-driven entry timing — wait for RSI/price mean reversion
    # Murphy Ch.4: "Wait for the pullback"; Bulkowski: extended entries statistically lose
    elif "R3.2.TIMING" in gate_code or "R3.2.TIMING_AND_PRICE" in gate_code:
        _rsi = row_context.get("RSI") or row_context.get("rsi")
        _sma20 = row_context.get("SMA20") or row_context.get("sma20")
        _last = row_context.get("last_price") or row_context.get("Last")
        _tqs = row_context.get("TQS_Score") or row_context.get("tqs_score")

        # RSI pullback: wait for RSI to drop back to 35-65 range (exit extended zone)
        if _rsi is not None:
            _rsi_f = float(_rsi)
            if _rsi_f > 65:
                # Overbought long setup — wait for RSI to pull back below 65
                conditions.append(_create_technical_condition(
                    metric="RSI",
                    operator="less_than",
                    threshold=65.0,
                    description=f"RSI must pull back below 65 (currently {_rsi_f:.1f}) — Murphy Ch.4: wait for mean reversion"
                ))
            elif _rsi_f < 35:
                # Oversold short setup — wait for RSI to recover above 35
                conditions.append(_create_technical_condition(
                    metric="RSI",
                    operator="greater_than",
                    threshold=35.0,
                    description=f"RSI must recover above 35 (currently {_rsi_f:.1f}) — Murphy Ch.4: wait for stabilization"
                ))

        # Price vs SMA20: wait for price to return within 3% of SMA20
        if _last is not None and _sma20 is not None:
            _last_f  = float(_last)
            _sma20_f = float(_sma20)
            if _last_f > 0 and _sma20_f > 0:
                _ext_pct = (_last_f - _sma20_f) / _sma20_f * 100
                if abs(_ext_pct) > 3.0:
                    # Target: price within 3% of SMA20 (reversion zone)
                    _target = _sma20_f * 1.03 if _ext_pct > 0 else _sma20_f * 0.97
                    _op = "less_than" if _ext_pct > 0 else "greater_than"
                    conditions.append(_create_price_level_condition(
                        operator=_op,
                        threshold=round(_target, 2),
                        timeframe="close",
                        description=(
                            f"Price must return within 3% of SMA20=${_sma20_f:.2f} "
                            f"(currently {_ext_pct:+.1f}% extended) — "
                            f"target ${_target:.2f}"
                        )
                    ))

        # Fallback: next session recheck (minimum wait)
        conditions.append(_create_time_delay_condition(
            next_session=True,
            description="Wait at least one trading session before re-evaluating entry timing"
        ))

    # R3.2.PORTFOLIO: existing same-direction position is underwater
    # McMillan Ch.4: never average into a losing directional long until the
    # original thesis recovers.  Condition: existing position P&L > -15%
    # (extracted from Gate_Reason text since row_context doesn't carry GL).
    elif "R3.2.PORTFOLIO" in gate_code:
        # Extract existing symbol from gate reason text if present
        import re
        _sym_match = re.search(r'Existing (\w+) is', gate_reason)
        _sym = _sym_match.group(1) if _sym_match else "existing position"
        conditions.append(_create_technical_condition(
            metric="portfolio_gl_pct",
            operator="greater_than",
            threshold=-15.0,
            description=(
                f"{_sym} P&L must recover above -15% — "
                f"McMillan Ch.4: wait for original thesis to stabilize before adding"
            )
        ))
        # Minimum: wait 1 full session before re-checking
        conditions.append(_create_time_delay_condition(
            next_session=True,
            description="Wait at least one trading session before re-evaluating portfolio conflict"
        ))

    # R3.2.PRICE: BS fair-value overpay — wait for mid to drop into BS band
    # Natenberg Ch.8: price discipline as important as direction selection
    elif "R3.2.PRICE" in gate_code:
        _bs_upper = row_context.get("Price_Gate_Band_Upper") or row_context.get("Entry_Band_Upper")
        _mid      = row_context.get("Mid_Price") or row_context.get("mid_price")
        _fv_pct   = row_context.get("Price_Gate_FV_Pct") or row_context.get("Premium_vs_FairValue_Pct")

        if _bs_upper is not None and _bs_upper > 0:
            conditions.append(_create_price_level_condition(
                operator="less_than",
                threshold=round(float(_bs_upper), 2),
                timeframe="option_mid",
                description=(
                    f"Option mid must drop to ≤${float(_bs_upper):.2f} (BS fair-value upper band) "
                    f"— currently {float(_fv_pct):.1f}% above fair value. "
                    f"Natenberg Ch.8: enter at or below fair value for long vol."
                )
            ))
        conditions.append(_create_time_delay_condition(
            next_session=True,
            description="Wait for next session — IV compression often reduces option premium next morning"
        ))

    # R2.4: Partial data
    elif "R2.4" in gate_code:
        # Data won't self-fix - this should be REJECTED
        # But if we're generating conditions, add a recheck delay
        conditions.append(_create_time_delay_condition(
            delay_hours=4,
            description="Recheck data completeness in 4 hours"
        ))

    # Generic fallback: if no specific conditions, add a time delay
    if not conditions:
        logger.warning(f"[WAIT_GEN] No specific conditions for {gate_code}, using generic time delay")
        conditions.append(_create_time_delay_condition(
            delay_hours=24,
            description="Wait 24 hours and recheck"
        ))

    logger.info(f"[WAIT_GEN] Generated {len(conditions)} wait conditions for {gate_code}")
    return conditions


def _create_price_level_condition(
    operator: str,
    threshold: float,
    timeframe: str = "close",
    description: Optional[str] = None
) -> Dict[str, Any]:
    """Create price level condition"""
    condition_id = f"price_{operator}_{threshold}_{str(uuid.uuid4())[:8]}"

    return {
        "condition_id": condition_id,
        "type": ConditionType.PRICE_LEVEL.value,
        "description": description or f"Price {timeframe} {operator} ${threshold:.2f}",
        "config": {
            "operator": operator,
            "threshold": threshold,
            "timeframe": timeframe
        }
    }


def _create_liquidity_condition(
    metric: str,
    operator: str,
    threshold: float,
    description: Optional[str] = None
) -> Dict[str, Any]:
    """Create liquidity condition"""
    condition_id = f"liquidity_{metric}_{operator}_{str(uuid.uuid4())[:8]}"

    return {
        "condition_id": condition_id,
        "type": ConditionType.LIQUIDITY.value,
        "description": description or f"{metric} {operator} {threshold}",
        "config": {
            "metric": metric,
            "operator": operator,
            "threshold": threshold
        }
    }


def _create_time_delay_condition(
    delay_hours: Optional[int] = None,
    delay_sessions: Optional[int] = None,
    next_session: bool = False,
    description: Optional[str] = None
) -> Dict[str, Any]:
    """Create time delay condition"""
    condition_id = f"time_delay_{str(uuid.uuid4())[:8]}"

    config = {}
    if delay_hours is not None:
        config["delay_hours"] = delay_hours
    if delay_sessions is not None:
        config["delay_sessions"] = delay_sessions
    if next_session:
        config["next_session"] = next_session

    if description is None:
        if delay_hours:
            description = f"Wait {delay_hours} hours"
        elif delay_sessions:
            description = f"Wait {delay_sessions} trading sessions"
        elif next_session:
            description = "Wait for next trading session"
        else:
            description = "Time delay"

    return {
        "condition_id": condition_id,
        "type": ConditionType.TIME_DELAY.value,
        "description": description,
        "config": config
    }


def _create_volatility_condition(
    metric: str,
    operator: str,
    threshold: float,
    description: Optional[str] = None
) -> Dict[str, Any]:
    """Create volatility condition"""
    condition_id = f"volatility_{metric}_{operator}_{str(uuid.uuid4())[:8]}"

    return {
        "condition_id": condition_id,
        "type": ConditionType.VOLATILITY.value,
        "description": description or f"{metric} {operator} {threshold:.1f}%",
        "config": {
            "metric": metric,
            "operator": operator,
            "threshold": threshold
        }
    }


def _create_candle_pattern_condition(
    pattern: str,
    count: int,
    timeframe: str = "1d",
    description: Optional[str] = None
) -> Dict[str, Any]:
    """Create candle pattern condition"""
    condition_id = f"candle_{pattern}_{count}_{str(uuid.uuid4())[:8]}"

    return {
        "condition_id": condition_id,
        "type": ConditionType.CANDLE_PATTERN.value,
        "description": description or f"{count} {pattern} candles on {timeframe}",
        "config": {
            "pattern": pattern,
            "count": count,
            "timeframe": timeframe
        }
    }


def _create_technical_condition(
    metric: str,
    operator: str,
    threshold: float,
    description: Optional[str] = None
) -> Dict[str, Any]:
    """Create a technical indicator condition (RSI, SMA distance, momentum).
    Used by Extension_Monitor to re-check WAIT_PULLBACK entries each pipeline run.
    """
    condition_id = f"technical_{metric}_{operator}_{str(uuid.uuid4())[:8]}"
    return {
        "condition_id": condition_id,
        "type": ConditionType.TECHNICAL.value,
        "description": description or f"{metric} {operator} {threshold}",
        "config": {
            "metric": metric,
            "operator": operator,
            "threshold": threshold
        }
    }


def should_reject_permanently(gate_reason: str, row_context: Dict[str, Any]) -> bool:
    """
    Determine if a trade should be permanently REJECTED vs AWAIT_CONFIRMATION.

    Args:
        gate_reason: Gate reason from Step 12
        row_context: Row data

    Returns:
        True if should be REJECTED, False if should AWAIT_CONFIRMATION
    """
    gate_code = gate_reason.split(":")[0].strip() if ":" in gate_reason else gate_reason

    # Permanent rejections (structural issues that won't resolve)
    permanent_rejection_codes = [
        "R0.1",  # Critical data missing (won't self-fix)
        "R0.2",  # Illiquid contract (structural)
        "R1.1",  # Market-wide stress (requires manual intervention)
        "R1.2",  # Strategy-specific blocking (structural)
    ]

    # R2.3: Missing bid/ask data is structural (contract selection failure)
    if "R2.3" in gate_code:
        bid = row_context.get("bid")
        ask = row_context.get("ask")
        if not bid or not ask or bid <= 0:
            return True  # Reject if no valid bid/ask data

    # R2.4: Partial data - could be temporary OR permanent
    # Reject if too many gaps
    if "R2.4" in gate_code:
        data_completeness = row_context.get("Data_Completeness_Overall", "Missing")
        if data_completeness == "Missing":
            return True  # Reject if completely missing

    # Check for specific keywords indicating permanent issues
    permanent_keywords = [
        "illiquid",
        "critical.*missing",
        "blocked",
        "unsupported",
        "invalid"
    ]

    import re
    for keyword in permanent_keywords:
        if re.search(keyword, gate_reason.lower()):
            return True

    # Check if in permanent rejection list
    for code in permanent_rejection_codes:
        if code in gate_code:
            return True

    # Default: allow AWAIT_CONFIRMATION (give it a chance to resolve)
    return False
