"""
Confirmation Condition Types & Checkers

Defines testable, binary confirmation conditions for wait loop.
All conditions must be machine-checkable with no discretion.

RAG Source: docs/SMART_WAIT_DESIGN.md - Confirmation Condition Types
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class ConditionType(Enum):
    """Types of testable confirmation conditions"""
    PRICE_LEVEL = "price_level"
    CANDLE_PATTERN = "candle_pattern"
    LIQUIDITY = "liquidity"
    TIME_DELAY = "time_delay"
    VOLATILITY = "volatility"
    TECHNICAL = "technical"   # RSI, price vs SMA, momentum recovery


class BaseCondition(ABC):
    """
    Base class for all confirmation conditions.

    All conditions must implement:
    - check(): Returns bool (condition met or not)
    - get_progress(): Returns float 0.0-1.0 (partial completion)
    - describe(): Returns human-readable description
    """

    def __init__(self, condition_id: str, config: Dict[str, Any]):
        self.condition_id = condition_id
        self.config = config

    @abstractmethod
    def check(self, market_data: Dict[str, Any], wait_entry: Dict[str, Any]) -> bool:
        """
        Check if condition is satisfied.

        Args:
            market_data: Current market data for ticker
            wait_entry: Wait entry state

        Returns:
            True if condition met, False otherwise
        """
        pass

    @abstractmethod
    def get_progress(self, market_data: Dict[str, Any], wait_entry: Dict[str, Any]) -> float:
        """
        Calculate partial completion (0.0 - 1.0).

        Args:
            market_data: Current market data
            wait_entry: Wait entry state

        Returns:
            Progress percentage (0.0 = not started, 1.0 = complete)
        """
        pass

    @abstractmethod
    def describe(self) -> str:
        """Return human-readable description of condition."""
        pass


class PriceLevelCondition(BaseCondition):
    """
    Price level confirmation condition.

    Config schema:
    {
        "operator": "above" | "below" | "between",
        "threshold": float | [low, high],
        "timeframe": "intraday" | "close" | "session_high" | "session_low"
    }

    Examples:
    - Close above $260 resistance
    - Break below $120 support
    - Trade between $250-$270 range
    """

    def check(self, market_data: Dict[str, Any], wait_entry: Dict[str, Any]) -> bool:
        operator = self.config["operator"]
        threshold = self.config["threshold"]
        timeframe = self.config.get("timeframe", "close")

        # Get relevant price
        price = self._get_price(market_data, timeframe)
        if price is None:
            return False

        # Apply operator
        if operator == "above":
            return price > threshold
        elif operator == "below":
            return price < threshold
        elif operator == "between":
            low, high = threshold
            return low <= price <= high
        else:
            logger.warning(f"[CONDITION] Unknown operator: {operator}")
            return False

    def get_progress(self, market_data: Dict[str, Any], wait_entry: Dict[str, Any]) -> float:
        operator = self.config["operator"]
        threshold = self.config["threshold"]
        timeframe = self.config.get("timeframe", "close")

        price = self._get_price(market_data, timeframe)
        if price is None:
            return 0.0

        entry_price = wait_entry.get("entry_price", price)

        # Calculate progress based on movement toward threshold
        if operator == "above":
            if price >= threshold:
                return 1.0
            elif entry_price >= threshold:
                return 0.0  # Already above at entry
            else:
                # Progress = % of distance covered
                distance = threshold - entry_price
                progress = (price - entry_price) / distance if distance > 0 else 0.0
                return max(0.0, min(1.0, progress))

        elif operator == "below":
            if price <= threshold:
                return 1.0
            elif entry_price <= threshold:
                return 0.0  # Already below at entry
            else:
                distance = entry_price - threshold
                progress = (entry_price - price) / distance if distance > 0 else 0.0
                return max(0.0, min(1.0, progress))

        elif operator == "between":
            low, high = threshold
            if low <= price <= high:
                return 1.0
            else:
                # Distance from range
                if price < low:
                    distance = low - price
                else:
                    distance = price - high
                # Progress inversely proportional to distance
                max_distance = entry_price * 0.1  # 10% as reference
                progress = 1.0 - (distance / max_distance)
                return max(0.0, min(1.0, progress))

        return 0.0

    def describe(self) -> str:
        operator = self.config["operator"]
        threshold = self.config["threshold"]
        timeframe = self.config.get("timeframe", "close")

        if operator == "above":
            return f"Price {timeframe} above ${threshold:.2f}"
        elif operator == "below":
            return f"Price {timeframe} below ${threshold:.2f}"
        elif operator == "between":
            low, high = threshold
            return f"Price {timeframe} between ${low:.2f}-${high:.2f}"
        else:
            return f"Price {operator} {threshold}"

    def _get_price(self, market_data: Dict[str, Any], timeframe: str) -> Optional[float]:
        """Extract price based on timeframe"""
        if timeframe == "close":
            return market_data.get("close") or market_data.get("last_price")
        elif timeframe == "intraday":
            return market_data.get("last_price")
        elif timeframe == "session_high":
            return market_data.get("high")
        elif timeframe == "session_low":
            return market_data.get("low")
        else:
            return market_data.get("last_price")


class CandlePatternCondition(BaseCondition):
    """
    Candle pattern confirmation condition.

    Config schema:
    {
        "pattern": "consecutive_green" | "consecutive_red" | "engulfing" | "hammer",
        "count": int,
        "timeframe": "5m" | "15m" | "30m" | "1h" | "1d"
    }

    Examples:
    - Two consecutive green 30m candles
    - Bullish engulfing on daily
    - No red candles for 3 sessions
    """

    def check(self, market_data: Dict[str, Any], wait_entry: Dict[str, Any]) -> bool:
        pattern = self.config["pattern"]
        count = self.config["count"]
        timeframe = self.config.get("timeframe", "1d")

        # Get candle history for timeframe
        candles = market_data.get(f"candles_{timeframe}", [])

        if len(candles) < count:
            return False  # Not enough candle data

        # Check pattern
        if pattern == "consecutive_green":
            return all(c.get("close", 0) > c.get("open", 0) for c in candles[-count:])

        elif pattern == "consecutive_red":
            return all(c.get("close", 0) < c.get("open", 0) for c in candles[-count:])

        elif pattern == "engulfing":
            if len(candles) < 2:
                return False
            prev_candle = candles[-2]
            curr_candle = candles[-1]

            # Bullish engulfing
            return (
                prev_candle.get("close", 0) < prev_candle.get("open", 0) and  # Prev red
                curr_candle.get("close", 0) > curr_candle.get("open", 0) and  # Curr green
                curr_candle.get("close", 0) > prev_candle.get("open", 0) and  # Engulfs top
                curr_candle.get("open", 0) < prev_candle.get("close", 0)      # Engulfs bottom
            )

        elif pattern == "hammer":
            if not candles:
                return False
            candle = candles[-1]
            body = abs(candle.get("close", 0) - candle.get("open", 0))
            lower_wick = min(candle.get("open", 0), candle.get("close", 0)) - candle.get("low", 0)
            upper_wick = candle.get("high", 0) - max(candle.get("open", 0), candle.get("close", 0))

            # Hammer: small body, long lower wick, small upper wick
            return lower_wick > 2 * body and upper_wick < body

        return False

    def get_progress(self, market_data: Dict[str, Any], wait_entry: Dict[str, Any]) -> float:
        pattern = self.config["pattern"]
        count = self.config["count"]
        timeframe = self.config.get("timeframe", "1d")

        candles = market_data.get(f"candles_{timeframe}", [])

        if pattern in ["consecutive_green", "consecutive_red"]:
            # Count matching candles
            matching = 0
            for candle in reversed(candles):
                is_green = candle.get("close", 0) > candle.get("open", 0)
                is_match = is_green if pattern == "consecutive_green" else not is_green

                if is_match:
                    matching += 1
                else:
                    break  # Streak broken

            return min(1.0, matching / count)

        elif pattern in ["engulfing", "hammer"]:
            # Binary patterns: either met or not
            return 1.0 if self.check(market_data, wait_entry) else 0.0

        return 0.0

    def describe(self) -> str:
        pattern = self.config["pattern"]
        count = self.config.get("count", 1)
        timeframe = self.config.get("timeframe", "1d")

        if pattern == "consecutive_green":
            return f"{count} consecutive green {timeframe} candles"
        elif pattern == "consecutive_red":
            return f"{count} consecutive red {timeframe} candles"
        elif pattern == "engulfing":
            return f"Bullish engulfing on {timeframe}"
        elif pattern == "hammer":
            return f"Hammer pattern on {timeframe}"
        else:
            return f"{pattern} pattern ({timeframe})"


class LiquidityCondition(BaseCondition):
    """
    Liquidity improvement confirmation condition.

    Config schema:
    {
        "metric": "bid_ask_spread_pct" | "open_interest" | "volume",
        "operator": "less_than" | "greater_than",
        "threshold": float
    }

    Examples:
    - Bid/ask spread < 5%
    - Open interest > 1000
    - Daily volume > 500k
    """

    def check(self, market_data: Dict[str, Any], wait_entry: Dict[str, Any]) -> bool:
        metric = self.config["metric"]
        operator = self.config["operator"]
        threshold = self.config["threshold"]

        value = self._get_metric_value(market_data, metric)
        if value is None:
            return False

        if operator == "less_than":
            return value < threshold
        elif operator == "greater_than":
            return value > threshold
        else:
            return False

    def get_progress(self, market_data: Dict[str, Any], wait_entry: Dict[str, Any]) -> float:
        metric = self.config["metric"]
        operator = self.config["operator"]
        threshold = self.config["threshold"]

        value = self._get_metric_value(market_data, metric)
        if value is None:
            return 0.0

        entry_value = self._get_metric_value(
            {"bid_ask_spread_pct": wait_entry.get("entry_bid_ask_spread_pct"),
             "open_interest": wait_entry.get("entry_open_interest"),
             "volume": wait_entry.get("entry_volume")},
            metric
        )

        if entry_value is None:
            entry_value = value

        if operator == "less_than":
            if value <= threshold:
                return 1.0
            elif entry_value <= threshold:
                return 0.0
            else:
                distance = entry_value - threshold
                progress = (entry_value - value) / distance if distance > 0 else 0.0
                return max(0.0, min(1.0, progress))

        elif operator == "greater_than":
            if value >= threshold:
                return 1.0
            elif entry_value >= threshold:
                return 0.0
            else:
                distance = threshold - entry_value
                progress = (value - entry_value) / distance if distance > 0 else 0.0
                return max(0.0, min(1.0, progress))

        return 0.0

    def describe(self) -> str:
        metric = self.config["metric"]
        operator = self.config["operator"]
        threshold = self.config["threshold"]

        metric_display = {
            "bid_ask_spread_pct": "Bid/ask spread",
            "open_interest": "Open interest",
            "volume": "Volume"
        }.get(metric, metric)

        op_display = {
            "less_than": "<",
            "greater_than": ">"
        }.get(operator, operator)

        if metric == "bid_ask_spread_pct":
            return f"{metric_display} {op_display} {threshold:.1f}%"
        else:
            return f"{metric_display} {op_display} {threshold:,.0f}"

    def _get_metric_value(self, market_data: Dict[str, Any], metric: str) -> Optional[float]:
        """Extract metric value from market data"""
        if metric == "bid_ask_spread_pct":
            bid = market_data.get("bid")
            ask = market_data.get("ask")
            if bid and ask and bid > 0:
                return (ask - bid) / bid * 100
        elif metric == "open_interest":
            return market_data.get("open_interest")
        elif metric == "volume":
            return market_data.get("volume")

        return None


class TimeDelayCondition(BaseCondition):
    """
    Time-based confirmation condition (wait period).

    Config schema:
    {
        "delay_hours": int,            # Optional
        "delay_sessions": int,         # Optional
        "next_session": bool          # Optional
    }

    Examples:
    - Wait 24 hours for volatility to settle
    - Wait for next trading session
    - Recheck after 3 sessions
    """

    def check(self, market_data: Dict[str, Any], wait_entry: Dict[str, Any]) -> bool:
        delay_hours = self.config.get("delay_hours")
        delay_sessions = self.config.get("delay_sessions")
        next_session = self.config.get("next_session", False)

        wait_started_at = wait_entry["wait_started_at"]
        now = datetime.now()

        if delay_hours is not None:
            required_time = wait_started_at + timedelta(hours=delay_hours)
            if now >= required_time:
                return True

        if delay_sessions is not None:
            from .ttl import count_trading_sessions
            sessions_elapsed = count_trading_sessions(wait_started_at, now)
            if sessions_elapsed >= delay_sessions:
                return True

        if next_session:
            # Check if we're in a new trading session
            # Simplified: check if date changed and it's a weekday
            if now.date() > wait_started_at.date() and now.weekday() < 5:
                return True

        return False

    def get_progress(self, market_data: Dict[str, Any], wait_entry: Dict[str, Any]) -> float:
        delay_hours = self.config.get("delay_hours")
        delay_sessions = self.config.get("delay_sessions")

        wait_started_at = wait_entry["wait_started_at"]
        now = datetime.now()

        if delay_hours is not None:
            elapsed_hours = (now - wait_started_at).total_seconds() / 3600
            return min(1.0, elapsed_hours / delay_hours)

        if delay_sessions is not None:
            from .ttl import count_trading_sessions
            sessions_elapsed = count_trading_sessions(wait_started_at, now)
            return min(1.0, sessions_elapsed / delay_sessions)

        # For next_session, binary progress
        return 1.0 if self.check(market_data, wait_entry) else 0.0

    def describe(self) -> str:
        delay_hours = self.config.get("delay_hours")
        delay_sessions = self.config.get("delay_sessions")
        next_session = self.config.get("next_session", False)

        if delay_hours is not None:
            return f"Wait {delay_hours} hours"
        elif delay_sessions is not None:
            return f"Wait {delay_sessions} trading sessions"
        elif next_session:
            return "Wait for next trading session"
        else:
            return "Time delay condition"


class VolatilityCondition(BaseCondition):
    """
    Volatility-based confirmation condition.

    Config schema:
    {
        "metric": "iv_30d" | "hv_30" | "ivhv_gap",
        "operator": "less_than" | "greater_than" | "between",
        "threshold": float | [low, high]
    }

    Examples:
    - IV settles below 40%
    - HV confirms uptrend (>35%)
    - IV/HV gap expands to >10%
    """

    def check(self, market_data: Dict[str, Any], wait_entry: Dict[str, Any]) -> bool:
        metric = self.config["metric"]
        operator = self.config["operator"]
        threshold = self.config["threshold"]

        value = self._get_metric_value(market_data, metric)
        if value is None:
            return False

        if operator == "less_than":
            return value < threshold
        elif operator == "greater_than":
            return value > threshold
        elif operator == "between":
            low, high = threshold
            return low <= value <= high
        else:
            return False

    def get_progress(self, market_data: Dict[str, Any], wait_entry: Dict[str, Any]) -> float:
        # Similar logic to PriceLevelCondition
        metric = self.config["metric"]
        operator = self.config["operator"]
        threshold = self.config["threshold"]

        value = self._get_metric_value(market_data, metric)
        if value is None:
            return 0.0

        entry_value = {
            "iv_30d": wait_entry.get("entry_iv_30d"),
            "hv_30": wait_entry.get("entry_hv_30"),
            "ivhv_gap": None  # Calculated
        }.get(metric)

        if entry_value is None:
            entry_value = value

        if operator == "less_than":
            if value <= threshold:
                return 1.0
            distance = entry_value - threshold
            progress = (entry_value - value) / distance if distance > 0 else 0.0
            return max(0.0, min(1.0, progress))

        elif operator == "greater_than":
            if value >= threshold:
                return 1.0
            distance = threshold - entry_value
            progress = (value - entry_value) / distance if distance > 0 else 0.0
            return max(0.0, min(1.0, progress))

        return 0.0

    def describe(self) -> str:
        metric = self.config["metric"]
        operator = self.config["operator"]
        threshold = self.config["threshold"]

        metric_display = {
            "iv_30d": "IV",
            "hv_30": "HV",
            "ivhv_gap": "IV/HV gap"
        }.get(metric, metric)

        op_display = {
            "less_than": "<",
            "greater_than": ">",
            "between": "between"
        }.get(operator, operator)

        if operator == "between":
            low, high = threshold
            return f"{metric_display} {op_display} {low:.1f}%-{high:.1f}%"
        else:
            return f"{metric_display} {op_display} {threshold:.1f}%"

    def _get_metric_value(self, market_data: Dict[str, Any], metric: str) -> Optional[float]:
        """Extract volatility metric from market data"""
        if metric == "iv_30d":
            return market_data.get("iv_30d")
        elif metric == "hv_30":
            return market_data.get("hv_30")
        elif metric == "ivhv_gap":
            iv = market_data.get("iv_30d")
            hv = market_data.get("hv_30")
            if iv is not None and hv is not None and hv > 0:
                return (iv - hv) / hv * 100
        return None


class TechnicalCondition(BaseCondition):
    """
    Technical indicator condition for timing recovery.

    Used by Extension_Monitor to re-check WAIT_PULLBACK entries.

    Config schema:
    {
        "metric": "RSI" | "price_vs_sma20_pct",
        "operator": "less_than" | "greater_than",
        "threshold": float
    }

    Examples:
    - RSI < 65 (overbought extension resolved)
    - RSI > 35 (oversold stabilized)
    - price_vs_sma20_pct < 3.0 (price returned near SMA20)
    """

    def check(self, market_data: Dict[str, Any], wait_entry: Dict[str, Any]) -> bool:
        metric   = self.config["metric"]
        operator = self.config["operator"]
        threshold = float(self.config["threshold"])

        value = self._get_value(market_data, metric)
        if value is None:
            return False

        if operator == "less_than":
            return value < threshold
        elif operator == "greater_than":
            return value > threshold
        return False

    def get_progress(self, market_data: Dict[str, Any], wait_entry: Dict[str, Any]) -> float:
        metric    = self.config["metric"]
        operator  = self.config["operator"]
        threshold = float(self.config["threshold"])

        value = self._get_value(market_data, metric)
        if value is None:
            return 0.0

        # Progress = how far along toward the threshold from entry value
        entry_val = self._get_value(
            {k: wait_entry.get(f"entry_{k}", wait_entry.get(k)) for k in [metric]},
            metric
        )
        if entry_val is None:
            entry_val = value

        if operator == "less_than":
            if value <= threshold:
                return 1.0
            dist = entry_val - threshold
            return max(0.0, min(1.0, (entry_val - value) / dist)) if dist > 0 else 0.0
        elif operator == "greater_than":
            if value >= threshold:
                return 1.0
            dist = threshold - entry_val
            return max(0.0, min(1.0, (value - entry_val) / dist)) if dist > 0 else 0.0
        return 0.0

    def describe(self) -> str:
        metric    = self.config["metric"]
        operator  = self.config["operator"]
        threshold = self.config["threshold"]
        op_str = "<" if operator == "less_than" else ">"
        return f"{metric} {op_str} {threshold}"

    def _get_value(self, market_data: Dict[str, Any], metric: str) -> Optional[float]:
        if metric == "RSI":
            v = market_data.get("RSI") or market_data.get("rsi")
            return float(v) if v is not None else None
        elif metric == "price_vs_sma20_pct":
            last  = market_data.get("last_price") or market_data.get("Last")
            sma20 = market_data.get("SMA20") or market_data.get("sma20")
            if last and sma20 and float(sma20) > 0:
                return (float(last) - float(sma20)) / float(sma20) * 100
        return None


class ConditionFactory:
    """
    Factory for creating condition instances from configuration.
    """

    CONDITION_CLASSES = {
        ConditionType.PRICE_LEVEL: PriceLevelCondition,
        ConditionType.CANDLE_PATTERN: CandlePatternCondition,
        ConditionType.LIQUIDITY: LiquidityCondition,
        ConditionType.TIME_DELAY: TimeDelayCondition,
        ConditionType.VOLATILITY: VolatilityCondition,
        ConditionType.TECHNICAL: TechnicalCondition,
    }

    @classmethod
    def create(cls, condition_type: ConditionType, condition_id: str, config: Dict[str, Any]) -> BaseCondition:
        """
        Create condition instance from type and config.

        Args:
            condition_type: Type of condition
            condition_id: Unique identifier for condition
            config: Type-specific configuration

        Returns:
            Condition instance

        Raises:
            ValueError: If condition type is unknown
        """
        condition_class = cls.CONDITION_CLASSES.get(condition_type)
        if not condition_class:
            raise ValueError(f"Unknown condition type: {condition_type}")

        return condition_class(condition_id, config)

    @classmethod
    def create_from_dict(cls, condition_dict: Dict[str, Any]) -> BaseCondition:
        """
        Create condition from dictionary (e.g. from database JSON).

        Args:
            condition_dict: Dictionary with condition_id, type, config

        Returns:
            Condition instance
        """
        condition_id = condition_dict["condition_id"]
        condition_type = ConditionType(condition_dict["type"])
        config = condition_dict["config"]

        return cls.create(condition_type, condition_id, config)
