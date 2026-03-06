"""
Human-Readable Trade Explanation Layer

Every trade must clearly answer:
1. What data is missing?
2. Why does that matter?
3. What will resolve it (time vs scraping)?

This module provides the explanation layer for scan output and dashboard display.

DESIGN PRINCIPLES:
1. TRANSPARENCY - No hidden logic, every decision is explained
2. ACTIONABLE - Explanations tell users what to do next
3. PROGRESSIVE - Shows path from current state to executable
4. HONEST - "No READY trades" is a healthy, correct outcome
"""

import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from enum import Enum
import logging

from .volatility_maturity import VolatilityMaturityTier, compute_maturity_for_dataframe
from .execution_eligibility import (
    ExecutionStatus,
    EvaluationStatus,
    classify_strategy_type,
    compute_eligibility_for_dataframe,
)

logger = logging.getLogger(__name__)


class DataGapType(Enum):
    """Types of data gaps that can block or gate a trade."""
    IV_HISTORY = "IV_HISTORY"
    QUOTE_STALE = "QUOTE_STALE"
    GREEKS_MISSING = "GREEKS_MISSING"
    LIQUIDITY_LOW = "LIQUIDITY_LOW"
    PRICE_HISTORY = "PRICE_HISTORY"
    CHAIN_UNAVAILABLE = "CHAIN_UNAVAILABLE"


@dataclass
class DataGap:
    """
    A single data gap with explanation.
    """
    gap_type: DataGapType
    severity: str  # "BLOCKING", "GATING", "WARNING"
    what_missing: str
    why_matters: str
    resolution: str
    time_to_resolve: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "gap_type": self.gap_type.value,
            "severity": self.severity,
            "what_missing": self.what_missing,
            "why_matters": self.why_matters,
            "resolution": self.resolution,
            "time_to_resolve": self.time_to_resolve,
        }

    def to_human_string(self) -> str:
        """Single-line human-readable explanation."""
        return f"{self.what_missing} → {self.resolution}"


@dataclass
class TradeExplanation:
    """
    Complete explanation for a trade's current status.
    """
    ticker: str
    strategy: str
    strategy_type: str

    # Status
    evaluation_status: str
    execution_status: str
    maturity_tier: str

    # Data gaps
    data_gaps: List[DataGap] = field(default_factory=list)

    # Summary fields
    is_executable: bool = False
    is_healthy_wait: bool = False  # True if gated but progressing
    blocking_reason: Optional[str] = None
    gating_reason: Optional[str] = None
    next_action: str = ""

    def __post_init__(self):
        self._compute_summary()

    def _compute_summary(self):
        """Compute summary fields from data gaps."""
        blocking_gaps = [g for g in self.data_gaps if g.severity == "BLOCKING"]
        gating_gaps = [g for g in self.data_gaps if g.severity == "GATING"]

        self.is_executable = (
            self.execution_status == "EXECUTABLE_NOW" and
            len(blocking_gaps) == 0
        )

        if blocking_gaps:
            self.blocking_reason = "; ".join(g.what_missing for g in blocking_gaps)
            self.next_action = blocking_gaps[0].resolution
        elif gating_gaps:
            self.is_healthy_wait = True
            self.gating_reason = "; ".join(g.what_missing for g in gating_gaps)
            self.next_action = gating_gaps[0].resolution
        elif self.is_executable:
            self.next_action = "Ready to execute"
        else:
            self.next_action = "Run evaluation"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "Ticker": self.ticker,
            "Strategy": self.strategy,
            "Strategy_Type": self.strategy_type,
            "Evaluation_Status": self.evaluation_status,
            "Execution_Status": self.execution_status,
            "Maturity_Tier": self.maturity_tier,
            "Is_Executable": self.is_executable,
            "Is_Healthy_Wait": self.is_healthy_wait,
            "Blocking_Reason": self.blocking_reason or "",
            "Gating_Reason": self.gating_reason or "",
            "Next_Action": self.next_action,
            "Data_Gaps_Count": len(self.data_gaps),
        }

    def to_dashboard_card(self) -> str:
        """Format for dashboard display."""
        lines = [
            f"{'─' * 50}",
            f"  {self.ticker} | {self.strategy}",
            f"{'─' * 50}",
        ]

        if self.is_executable:
            lines.append("  ✓ READY TO EXECUTE")
        elif self.is_healthy_wait:
            lines.append(f"  ⏳ GATED: {self.gating_reason}")
            lines.append(f"     Resolution: {self.next_action}")
        elif self.blocking_reason:
            lines.append(f"  ✗ BLOCKED: {self.blocking_reason}")
            lines.append(f"     Resolution: {self.next_action}")
        else:
            lines.append(f"  ? Status: {self.execution_status}")

        lines.append(f"  Maturity: {self.maturity_tier}")
        lines.append(f"{'─' * 50}")

        return "\n".join(lines)


def detect_data_gaps(row: pd.Series) -> List[DataGap]:
    """
    Detect all data gaps for a trade row.

    Returns list of DataGap objects with explanations.
    """
    gaps = []

    # 1. IV History gap
    iv_history_count = row.get("iv_history_count", 0)
    if pd.isna(iv_history_count):
        iv_history_count = 0
    iv_history_count = int(iv_history_count)

    maturity_tier = row.get("Volatility_Maturity_Tier", "SPOT_ONLY")
    strategy_type = classify_strategy_type(row.get("Strategy_Name", row.get("Strategy", "")))

    if maturity_tier != "MATURE":
        days_needed = 120 - iv_history_count

        # Determine severity based on strategy type
        if strategy_type == "INCOME":
            severity = "GATING"  # Income strategies are gated, not blocked
            why = "Income strategies require IV Rank for proper premium valuation"
        elif strategy_type == "DIRECTIONAL" and iv_history_count < 7:
            severity = "GATING"
            why = "Directional trades need at least 7 days for timing context"
        else:
            severity = "WARNING"
            why = "Limited IV history reduces confidence in volatility assessment"

        gaps.append(DataGap(
            gap_type=DataGapType.IV_HISTORY,
            severity=severity,
            what_missing=f"IV history: {iv_history_count}/120 days",
            why_matters=why,
            resolution=f"Continue daily Fidelity scraping ({days_needed} more days needed)",
            time_to_resolve=f"~{days_needed} days of daily scraping",
        ))

    # 2. Quote freshness gap
    quote_time = row.get("Quote_Time", row.get("quote_time"))
    bid = row.get("Bid", row.get("bid"))
    ask = row.get("Ask", row.get("ask"))

    if pd.isna(bid) or pd.isna(ask):
        gaps.append(DataGap(
            gap_type=DataGapType.QUOTE_STALE,
            severity="BLOCKING",
            what_missing="No bid/ask quotes",
            why_matters="Cannot determine entry price or spread",
            resolution="Refresh quotes from Schwab",
            time_to_resolve="Immediate (API call)",
        ))

    # 3. Greeks gap
    delta = row.get("Delta", row.get("delta"))
    if pd.isna(delta):
        gaps.append(DataGap(
            gap_type=DataGapType.GREEKS_MISSING,
            severity="BLOCKING",
            what_missing="Greeks (delta) missing",
            why_matters="Cannot assess option sensitivity or probability",
            resolution="Fetch option chain from Schwab",
            time_to_resolve="Immediate (API call)",
        ))

    # 4. Liquidity gap
    bid_ask_spread = row.get("Bid_Ask_Spread_Pct", row.get("bid_ask_spread_pct"))
    if pd.notna(bid_ask_spread) and bid_ask_spread > 15:
        gaps.append(DataGap(
            gap_type=DataGapType.LIQUIDITY_LOW,
            severity="BLOCKING",
            what_missing=f"Wide spread: {bid_ask_spread:.1f}%",
            why_matters="Poor execution quality, slippage risk",
            resolution="Wait for better liquidity or choose different strike",
            time_to_resolve="Market hours, liquidity varies",
        ))

    open_interest = row.get("Open_Interest", row.get("open_interest", row.get("OI")))
    if pd.notna(open_interest) and open_interest < 100:
        gaps.append(DataGap(
            gap_type=DataGapType.LIQUIDITY_LOW,
            severity="WARNING",
            what_missing=f"Low OI: {int(open_interest)}",
            why_matters="May have difficulty closing position",
            resolution="Consider strike with higher open interest",
            time_to_resolve="N/A - structural",
        ))

    return gaps


def explain_trade(row: pd.Series) -> TradeExplanation:
    """
    Generate complete explanation for a single trade.
    """
    ticker = row.get("Ticker", "UNKNOWN")
    strategy = row.get("Strategy_Name", row.get("Strategy", "UNKNOWN"))
    strategy_type = classify_strategy_type(strategy)

    # Get status fields (may need to compute if not present)
    eval_status = row.get("Evaluation_Status", "PENDING_EVALUATION")
    exec_status = row.get("Execution_Status", "NOT_EVALUATED")
    maturity_tier = row.get("Volatility_Maturity_Tier", "SPOT_ONLY")

    # Detect data gaps
    data_gaps = detect_data_gaps(row)

    return TradeExplanation(
        ticker=ticker,
        strategy=strategy,
        strategy_type=strategy_type,
        evaluation_status=eval_status,
        execution_status=exec_status,
        maturity_tier=maturity_tier,
        data_gaps=data_gaps,
    )


def add_explanations_to_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add explanation columns to DataFrame.

    Adds:
    - Explanation_Summary: One-line summary
    - Missing_Data: What data is missing
    - Why_Matters: Why the missing data matters
    - Resolution_Path: What will resolve it
    - Is_Healthy_Wait: True if gated but progressing
    """
    df = df.copy()

    # Ensure we have maturity and eligibility computed
    if "Volatility_Maturity_Tier" not in df.columns:
        df = compute_maturity_for_dataframe(df)

    if "Execution_Status" not in df.columns:
        df = compute_eligibility_for_dataframe(df)

    # Generate explanations
    summaries = []
    missing_data = []
    why_matters = []
    resolutions = []
    healthy_waits = []

    for _, row in df.iterrows():
        explanation = explain_trade(row)

        summaries.append(explanation.next_action)

        if explanation.data_gaps:
            missing_data.append("; ".join(g.what_missing for g in explanation.data_gaps))
            why_matters.append("; ".join(g.why_matters for g in explanation.data_gaps))
            resolutions.append("; ".join(g.resolution for g in explanation.data_gaps))
        else:
            missing_data.append("")
            why_matters.append("")
            resolutions.append("")

        healthy_waits.append(explanation.is_healthy_wait)

    df["Explanation_Summary"] = summaries
    df["Missing_Data"] = missing_data
    df["Why_Matters"] = why_matters
    df["Resolution_Path"] = resolutions
    df["Is_Healthy_Wait"] = healthy_waits

    return df


def generate_scan_summary(df: pd.DataFrame) -> str:
    """
    Generate human-readable summary for scan output.

    This is the main output users see after a scan.
    """
    if "Execution_Status" not in df.columns:
        df = compute_eligibility_for_dataframe(df.copy())

    total = len(df)

    # Count by execution status
    exec_counts = df["Execution_Status"].value_counts().to_dict()
    executable = exec_counts.get("EXECUTABLE_NOW", 0)
    gated = exec_counts.get("GATED_BY_MATURITY", 0)
    blocked = exec_counts.get("BLOCKED", 0)
    not_eval = exec_counts.get("NOT_EVALUATED", 0)

    # Count by maturity tier
    if "Volatility_Maturity_Tier" in df.columns:
        tier_counts = df["Volatility_Maturity_Tier"].value_counts().to_dict()
    else:
        tier_counts = {}

    lines = [
        "",
        "=" * 60,
        "SCAN RESULTS SUMMARY",
        "=" * 60,
        "",
        f"Total trades evaluated: {total}",
        "",
        "EXECUTION STATUS:",
        f"  EXECUTABLE_NOW:      {executable:3d}  ← Ready for capital deployment",
        f"  GATED_BY_MATURITY:   {gated:3d}  ← Awaiting IV history",
        f"  BLOCKED:             {blocked:3d}  ← Data quality issues",
        f"  NOT_EVALUATED:       {not_eval:3d}  ← Pending evaluation",
        "",
    ]

    if executable == 0 and gated > 0:
        lines.extend([
            "─" * 60,
            "NOTE: No trades are EXECUTABLE_NOW",
            "",
            "This is a HEALTHY, CORRECT outcome when:",
            "  • IV history is still accumulating (< 120 days)",
            "  • Income strategies require MATURE volatility data",
            "",
            f"Currently {gated} trades are GATED_BY_MATURITY.",
            "These will become executable as IV history accumulates.",
            "─" * 60,
            "",
        ])

    if tier_counts:
        lines.extend([
            "VOLATILITY MATURITY:",
            f"  MATURE (120+ days):  {tier_counts.get('MATURE', 0):3d}",
            f"  IMMATURE (30-119):   {tier_counts.get('IMMATURE', 0):3d}",
            f"  EARLY (7-29):        {tier_counts.get('EARLY', 0):3d}",
            f"  SPOT_ONLY (0-6):     {tier_counts.get('SPOT_ONLY', 0):3d}",
            "",
        ])

    # Show top gated trades
    if gated > 0:
        gated_df = df[df["Execution_Status"] == "GATED_BY_MATURITY"].copy()
        if "days_to_mature" in gated_df.columns:
            gated_df = gated_df.sort_values("days_to_mature")

        lines.extend([
            "TOP GATED TRADES (closest to MATURE):",
            "─" * 60,
        ])

        for i, (_, row) in enumerate(gated_df.head(5).iterrows()):
            ticker = row.get("Ticker", "?")
            strategy = row.get("Strategy_Name", row.get("Strategy", "?"))
            days = row.get("days_to_mature", "?")
            iv_count = row.get("iv_history_count", 0)
            lines.append(f"  {ticker:6s} {strategy:20s} {iv_count:3.0f}/120 days ({days} more needed)")

        lines.append("─" * 60)

    lines.extend([
        "",
        "NEXT STEPS:",
        "  1. Run daily Fidelity scraper to accumulate IV history",
        "  2. Wait for GATED trades to reach 120 days",
        "  3. Re-run scan to see updated EXECUTABLE trades",
        "",
        "=" * 60,
    ])

    return "\n".join(lines)
