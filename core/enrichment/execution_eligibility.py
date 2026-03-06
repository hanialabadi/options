"""
Execution Eligibility - Decoupled Evaluation and Execution Status

This module separates two distinct concepts:
1. EVALUATED_OK - All non-volatility gates passed (liquidity, Greeks, price action)
2. EXECUTABLE_NOW - Capital-eligible under current maturity tier

A trade can be EVALUATED_OK but not EXECUTABLE_NOW if volatility maturity is insufficient.

DESIGN PRINCIPLES:
1. Evaluation is strategy-agnostic - same gates for all trade types
2. Execution eligibility depends on strategy + maturity tier
3. Income trades require MATURE tier
4. Directional trades can execute at EARLY or higher
5. Clear human-readable explanations for all states
"""

from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
import pandas as pd
import numpy as np
import logging

from .volatility_maturity import VolatilityMaturityTier, compute_maturity_tier

logger = logging.getLogger(__name__)


class EvaluationStatus(Enum):
    """
    Evaluation status - did the trade pass all non-volatility gates?
    """
    EVALUATED_OK = "EVALUATED_OK"          # All gates passed
    EVALUATION_FAILED = "EVALUATION_FAILED"  # One or more gates failed
    PENDING_EVALUATION = "PENDING_EVALUATION"  # Not yet evaluated


class ExecutionStatus(Enum):
    """
    Execution status - is the trade capital-eligible now?
    """
    EXECUTABLE_NOW = "EXECUTABLE_NOW"      # Ready to execute
    GATED_BY_MATURITY = "GATED_BY_MATURITY"  # Awaiting volatility maturity
    BLOCKED = "BLOCKED"                     # Cannot execute (evaluation failed or other blocker)
    NOT_EVALUATED = "NOT_EVALUATED"         # Must evaluate first


# Strategy classification for execution rules
INCOME_STRATEGIES = frozenset([
    "CSP", "CC", "PCS", "CCS", "IC", "BPS", "BCS",
    "Cash Secured Put", "Covered Call", "Put Credit Spread",
    "Call Credit Spread", "Iron Condor", "Bull Put Spread", "Bear Call Spread",
    "Cash-Secured Put", "Buy-Write",
])

DIRECTIONAL_STRATEGIES = frozenset([
    "LONG_CALL", "LONG_PUT", "CALL_DEBIT", "PUT_DEBIT",
    "Long Call", "Long Put", "Call Debit Spread", "Put Debit Spread"
])

LEAP_STRATEGIES = frozenset([
    "LEAP", "LEAPS", "Long Call LEAP", "Long Put LEAP"
])

VOLATILITY_STRATEGIES = frozenset([
    "Straddle", "Strangle", "Long Straddle", "Long Strangle",
    "STRADDLE", "STRANGLE",
])


@dataclass
class ExecutionGate:
    """
    A single gate that must pass for execution.
    """
    name: str
    passed: bool
    reason: str
    resolution: Optional[str] = None  # What will resolve it (time/action)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "gate": self.name,
            "passed": self.passed,
            "reason": self.reason,
            "resolution": self.resolution,
        }


@dataclass
class EligibilityAssessment:
    """
    Complete eligibility assessment for a trade.

    Separates evaluation (non-volatility) from execution (includes maturity).
    """
    ticker: str
    strategy: str
    strategy_type: str  # "INCOME", "DIRECTIONAL", "LEAP"

    # Evaluation status (non-volatility gates)
    evaluation_status: EvaluationStatus
    evaluation_gates: List[ExecutionGate] = field(default_factory=list)

    # Volatility maturity
    maturity_tier: VolatilityMaturityTier = VolatilityMaturityTier.SPOT_ONLY
    iv_history_count: int = 0
    days_to_mature: int = 120

    # Execution status (combines evaluation + maturity)
    execution_status: ExecutionStatus = ExecutionStatus.NOT_EVALUATED

    # Human-readable summary
    summary: str = ""
    missing_data: List[str] = field(default_factory=list)
    resolution_path: str = ""

    def __post_init__(self):
        """Compute execution status from evaluation + maturity."""
        self._compute_execution_status()
        self._generate_summary()

    def _compute_execution_status(self):
        """Derive execution status from evaluation status and maturity tier."""
        if self.evaluation_status == EvaluationStatus.PENDING_EVALUATION:
            self.execution_status = ExecutionStatus.NOT_EVALUATED
            return

        if self.evaluation_status == EvaluationStatus.EVALUATION_FAILED:
            self.execution_status = ExecutionStatus.BLOCKED
            return

        # Evaluation passed - check maturity requirements
        if self.strategy_type == "INCOME":
            if self.maturity_tier.allows_income_execution:
                self.execution_status = ExecutionStatus.EXECUTABLE_NOW
            else:
                self.execution_status = ExecutionStatus.GATED_BY_MATURITY

        elif self.strategy_type == "DIRECTIONAL":
            if self.maturity_tier.allows_directional_execution:
                self.execution_status = ExecutionStatus.EXECUTABLE_NOW
            else:
                self.execution_status = ExecutionStatus.GATED_BY_MATURITY

        elif self.strategy_type == "VOLATILITY":
            # Vol strategies (Straddle, Strangle) need IV context for RV/IV
            # comparison — IMMATURE (20d+) provides IV_Rank_30D and ZScore
            if self.maturity_tier in (VolatilityMaturityTier.IMMATURE, VolatilityMaturityTier.MATURE):
                self.execution_status = ExecutionStatus.EXECUTABLE_NOW
            else:
                self.execution_status = ExecutionStatus.GATED_BY_MATURITY

        elif self.strategy_type == "LEAP":
            # LEAPs require at least IMMATURE for basic timing
            if self.maturity_tier in (VolatilityMaturityTier.IMMATURE, VolatilityMaturityTier.MATURE):
                self.execution_status = ExecutionStatus.EXECUTABLE_NOW
            else:
                self.execution_status = ExecutionStatus.GATED_BY_MATURITY

        else:
            # Unknown strategy type - require MATURE as fallback
            if self.maturity_tier == VolatilityMaturityTier.MATURE:
                self.execution_status = ExecutionStatus.EXECUTABLE_NOW
            else:
                self.execution_status = ExecutionStatus.GATED_BY_MATURITY

    def _generate_summary(self):
        """Generate human-readable summary."""
        failed_gates = [g for g in self.evaluation_gates if not g.passed]

        if self.execution_status == ExecutionStatus.EXECUTABLE_NOW:
            self.summary = "Trade evaluated and ready for execution"
            self.missing_data = []
            self.resolution_path = "None - ready to execute"

        elif self.execution_status == ExecutionStatus.BLOCKED:
            gate_names = [g.name for g in failed_gates]
            self.summary = f"Trade blocked by: {', '.join(gate_names)}"
            self.missing_data = [g.reason for g in failed_gates]
            self.resolution_path = "; ".join(g.resolution or "Requires data fix" for g in failed_gates if g.resolution)

        elif self.execution_status == ExecutionStatus.GATED_BY_MATURITY:
            self.summary = f"Trade evaluated OK, but gated by volatility maturity ({self.iv_history_count} days collected, need 120 for {self.strategy_type})"
            self.missing_data = [f"IV history: {self.iv_history_count}/120 days"]
            if self.days_to_mature > 0:
                self.resolution_path = f"Fidelity scraping + {self.days_to_mature} more days of data collection"
            else:
                self.resolution_path = "Continue daily Fidelity scraping"

        elif self.execution_status == ExecutionStatus.NOT_EVALUATED:
            self.summary = "Trade pending evaluation"
            self.missing_data = ["Evaluation not yet run"]
            self.resolution_path = "Run pipeline evaluation"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for DataFrame integration."""
        return {
            "Ticker": self.ticker,
            "Strategy": self.strategy,
            "Strategy_Type": self.strategy_type,
            "Evaluation_Status": self.evaluation_status.value,
            "Execution_Status": self.execution_status.value,
            "Volatility_Maturity_Tier": self.maturity_tier.value,
            "iv_history_count": self.iv_history_count,
            "days_to_mature": self.days_to_mature,
            "Summary": self.summary,
            "Missing_Data": "; ".join(self.missing_data) if self.missing_data else "",
            "Resolution_Path": self.resolution_path,
        }


def classify_strategy_type(strategy_name: str) -> str:
    """
    Classify a strategy name into INCOME, DIRECTIONAL, LEAP, or VOLATILITY.

    Returns "INCOME" as default for unknown strategies (conservative — requires full maturity).
    """
    if strategy_name is None or pd.isna(strategy_name):
        return "INCOME"  # Conservative default

    strategy_upper = str(strategy_name).upper()

    # Check VOLATILITY first (distinct set — cannot be mistaken for income)
    for vol_strat in VOLATILITY_STRATEGIES:
        if vol_strat.upper() in strategy_upper:
            return "VOLATILITY"

    # Check LEAP (before DIRECTIONAL — "Long Call LEAP" contains "Long Call")
    for leap_strat in LEAP_STRATEGIES:
        if leap_strat.upper() in strategy_upper:
            return "LEAP"

    # Check DIRECTIONAL
    for dir_strat in DIRECTIONAL_STRATEGIES:
        if dir_strat.upper() in strategy_upper:
            return "DIRECTIONAL"

    # Check INCOME
    for income_strat in INCOME_STRATEGIES:
        if income_strat.upper() in strategy_upper:
            return "INCOME"

    # Default to INCOME (requires full maturity — conservative)
    return "INCOME"


def evaluate_non_volatility_gates(row: pd.Series) -> List[ExecutionGate]:
    """
    Evaluate all non-volatility gates for a trade.

    These are the same regardless of strategy - pure data quality checks.
    """
    gates = []

    # Liquidity gate
    bid_ask_spread = row.get("Bid_Ask_Spread_Pct", row.get("bid_ask_spread_pct"))
    if bid_ask_spread is not None and not pd.isna(bid_ask_spread):
        passed = bid_ask_spread < 15  # 15% max spread
        gates.append(ExecutionGate(
            name="Liquidity",
            passed=passed,
            reason=f"Bid-ask spread: {bid_ask_spread:.1f}%" if not passed else "OK",
            resolution="Wait for better liquidity or choose different strike" if not passed else None
        ))
    else:
        gates.append(ExecutionGate(
            name="Liquidity",
            passed=False,
            reason="Bid-ask spread data missing",
            resolution="Refresh quotes from Schwab"
        ))

    # Open Interest gate
    open_interest = row.get("Open_Interest", row.get("open_interest", row.get("OI")))
    if open_interest is not None and not pd.isna(open_interest):
        passed = open_interest >= 100
        gates.append(ExecutionGate(
            name="Open Interest",
            passed=passed,
            reason=f"OI: {int(open_interest)}" if not passed else "OK",
            resolution="Choose strike with higher open interest" if not passed else None
        ))

    # Quote freshness gate
    quote_time = row.get("Quote_Time", row.get("quote_time"))
    # For now, pass if quote exists
    if quote_time is not None and not pd.isna(quote_time):
        gates.append(ExecutionGate(
            name="Quote Freshness",
            passed=True,
            reason="OK"
        ))
    else:
        gates.append(ExecutionGate(
            name="Quote Freshness",
            passed=False,
            reason="No quote timestamp",
            resolution="Refresh quotes from Schwab"
        ))

    # Greeks validation (delta must exist)
    delta = row.get("Delta", row.get("delta"))
    if delta is not None and not pd.isna(delta):
        gates.append(ExecutionGate(
            name="Greeks",
            passed=True,
            reason="OK"
        ))
    else:
        gates.append(ExecutionGate(
            name="Greeks",
            passed=False,
            reason="Delta missing",
            resolution="Fetch option chain from Schwab"
        ))

    return gates


def assess_eligibility(
    ticker: str,
    strategy: str,
    iv_history_count: int,
    row: Optional[pd.Series] = None
) -> EligibilityAssessment:
    """
    Create a complete eligibility assessment for a trade.

    Args:
        ticker: Stock symbol
        strategy: Strategy name (e.g., "CSP", "LONG_CALL")
        iv_history_count: Days of IV history available
        row: Optional DataFrame row with quote/liquidity data for gate evaluation

    Returns:
        EligibilityAssessment with evaluation and execution status
    """
    strategy_type = classify_strategy_type(strategy)
    maturity_tier = VolatilityMaturityTier.from_history_count(iv_history_count)
    days_to_mature = max(0, 120 - (iv_history_count or 0))

    # Evaluate non-volatility gates if row provided
    if row is not None:
        gates = evaluate_non_volatility_gates(row)
        all_passed = all(g.passed for g in gates)
        eval_status = EvaluationStatus.EVALUATED_OK if all_passed else EvaluationStatus.EVALUATION_FAILED
    else:
        gates = []
        eval_status = EvaluationStatus.PENDING_EVALUATION

    return EligibilityAssessment(
        ticker=ticker,
        strategy=strategy,
        strategy_type=strategy_type,
        evaluation_status=eval_status,
        evaluation_gates=gates,
        maturity_tier=maturity_tier,
        iv_history_count=iv_history_count or 0,
        days_to_mature=days_to_mature,
    )


def compute_eligibility_for_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add eligibility columns to DataFrame.

    Expects columns:
    - Ticker
    - Strategy_Name or Strategy
    - iv_history_count

    Returns DataFrame with added columns:
    - Strategy_Type
    - Evaluation_Status
    - Execution_Status
    - Summary
    - Missing_Data
    - Resolution_Path
    """
    df = df.copy()

    # Ensure required columns — fall back to IV_History_Count from IVEngine if needed
    if "iv_history_count" not in df.columns:
        if "IV_History_Count" in df.columns:
            df["iv_history_count"] = df["IV_History_Count"]
        else:
            df["iv_history_count"] = 0

    strategy_col = "Strategy_Name" if "Strategy_Name" in df.columns else "Strategy"
    if strategy_col not in df.columns:
        df[strategy_col] = "UNKNOWN"

    # Process each row
    results = []
    for idx, row in df.iterrows():
        ticker = row.get("Ticker", "UNKNOWN")
        strategy = row.get(strategy_col, "UNKNOWN")
        iv_count = row.get("iv_history_count", 0)
        if pd.isna(iv_count):
            iv_count = 0

        assessment = assess_eligibility(
            ticker=ticker,
            strategy=strategy,
            iv_history_count=int(iv_count),
            row=row
        )

        results.append({
            "Strategy_Type": assessment.strategy_type,
            "Evaluation_Status": assessment.evaluation_status.value,
            # Write to Maturity_Execution_Status so we don't overwrite Step12's Execution_Status
            "Maturity_Execution_Status": assessment.execution_status.value,
            "Volatility_Maturity_Tier": assessment.maturity_tier.value,
            "Eligibility_Summary": assessment.summary,
            "Missing_Data": "; ".join(assessment.missing_data) if assessment.missing_data else "",
            "Resolution_Path": assessment.resolution_path,
        })

    result_df = pd.DataFrame(results, index=df.index)
    for col in result_df.columns:
        # Never overwrite Execution_Status — Step12 owns that column
        if col == "Execution_Status":
            continue
        # Never overwrite Strategy_Type if already populated — Step6 sets the authoritative value
        if col == "Strategy_Type" and "Strategy_Type" in df.columns and df["Strategy_Type"].notna().any():
            continue
        df[col] = result_df[col]

    # Log summary using the maturity-specific column
    maturity_counts = df["Maturity_Execution_Status"].value_counts().to_dict()
    logger.info(f"Execution eligibility: {maturity_counts}")

    return df


def get_executable_trades(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter to only EXECUTABLE_NOW trades.
    """
    if "Execution_Status" not in df.columns:
        df = compute_eligibility_for_dataframe(df)

    return df[df["Execution_Status"] == "EXECUTABLE_NOW"].copy()


def get_gated_trades(df: pd.DataFrame) -> pd.DataFrame:
    """
    Get trades that are evaluated OK but gated by maturity.

    These are valid trades awaiting volatility data.
    """
    if "Execution_Status" not in df.columns:
        df = compute_eligibility_for_dataframe(df)

    return df[df["Execution_Status"] == "GATED_BY_MATURITY"].copy()
