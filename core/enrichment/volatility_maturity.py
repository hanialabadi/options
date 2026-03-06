"""
Volatility Maturity Tier - First-Class Data Availability Concept

This module defines the Volatility_Maturity_Tier as an immutable, strategy-agnostic
classification based purely on IV history data availability.

DESIGN PRINCIPLES:
1. Derived ONLY from data availability, never from strategy
2. Immutable once computed for a given data snapshot
3. Used to determine execution eligibility, not trade quality
4. Progressive - tickers move up tiers as data accumulates

TIER SEMANTICS:
- SPOT_ONLY (0-6 days): Only current IV available, no historical context
- EARLY (7-29 days): Short-term patterns visible, directional timing possible
- IMMATURE (30-119 days): IV patterns visible, not statistically reliable for percentiles
- MATURE (>=120 days): Full IV Rank calculation valid, income strategy eligible
"""

from enum import Enum, auto
from dataclasses import dataclass
from typing import Optional, Dict, Any
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


class VolatilityMaturityTier(Enum):
    """
    Immutable volatility data maturity classification.

    Based purely on iv_history_count - the number of days of IV data available.
    """
    SPOT_ONLY = "SPOT_ONLY"    # 0-6 days: Current IV only, no historical context
    EARLY = "EARLY"            # 7-29 days: Short-term patterns, directional timing only
    IMMATURE = "IMMATURE"      # 30-119 days: Patterns visible, not statistically reliable
    MATURE = "MATURE"          # 120+ days: Full IV Rank valid, income eligible

    @classmethod
    def from_history_count(cls, iv_history_count: Optional[int]) -> "VolatilityMaturityTier":
        """
        Derive tier strictly from IV history count.

        This is the ONLY valid way to determine tier.
        """
        if iv_history_count is None or pd.isna(iv_history_count):
            return cls.SPOT_ONLY

        count = int(iv_history_count)

        if count < 7:
            return cls.SPOT_ONLY
        elif count < 30:
            return cls.EARLY
        elif count < 120:
            return cls.IMMATURE
        else:
            return cls.MATURE

    @property
    def min_days(self) -> int:
        """Minimum days of IV history for this tier."""
        return {
            VolatilityMaturityTier.SPOT_ONLY: 0,
            VolatilityMaturityTier.EARLY: 7,
            VolatilityMaturityTier.IMMATURE: 30,
            VolatilityMaturityTier.MATURE: 120,
        }[self]

    @property
    def max_days(self) -> Optional[int]:
        """Maximum days of IV history for this tier (None = no upper bound)."""
        return {
            VolatilityMaturityTier.SPOT_ONLY: 6,
            VolatilityMaturityTier.EARLY: 29,
            VolatilityMaturityTier.IMMATURE: 119,
            VolatilityMaturityTier.MATURE: None,
        }[self]

    @property
    def allows_iv_rank(self) -> bool:
        """Whether IV Rank percentile calculation is valid at this tier."""
        return self == VolatilityMaturityTier.MATURE

    @property
    def allows_income_execution(self) -> bool:
        """Whether income strategies (CSP, CC, spreads) can execute at this tier."""
        return self == VolatilityMaturityTier.MATURE

    @property
    def allows_directional_execution(self) -> bool:
        """Whether directional strategies can execute at this tier."""
        # Directional trades can execute at EARLY or higher
        return self in (
            VolatilityMaturityTier.EARLY,
            VolatilityMaturityTier.IMMATURE,
            VolatilityMaturityTier.MATURE,
        )

    @property
    def human_label(self) -> str:
        """Human-readable label for display."""
        return {
            VolatilityMaturityTier.SPOT_ONLY: "Spot Only (0-6 days)",
            VolatilityMaturityTier.EARLY: "Early (7-29 days)",
            VolatilityMaturityTier.IMMATURE: "Immature (30-119 days)",
            VolatilityMaturityTier.MATURE: "Mature (120+ days)",
        }[self]


@dataclass(frozen=True)
class MaturityAssessment:
    """
    Immutable assessment of a ticker's volatility data maturity.

    Contains the tier plus context for human understanding.
    """
    ticker: str
    tier: VolatilityMaturityTier
    iv_history_count: int
    days_to_next_tier: Optional[int]
    has_spot_iv: bool
    iv_source: str  # "SCHWAB", "FIDELITY", "NONE"

    @property
    def needs_enrichment(self) -> bool:
        """Whether this ticker should be queued for Fidelity scraping."""
        return self.tier != VolatilityMaturityTier.MATURE

    @property
    def enrichment_priority(self) -> int:
        """Priority for enrichment queue (lower = higher priority)."""
        # SPOT_ONLY is highest priority, IMMATURE is lowest
        return {
            VolatilityMaturityTier.SPOT_ONLY: 1,
            VolatilityMaturityTier.EARLY: 2,
            VolatilityMaturityTier.IMMATURE: 3,
            VolatilityMaturityTier.MATURE: 99,
        }[self.tier]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for DataFrame integration."""
        return {
            "Ticker": self.ticker,
            "Volatility_Maturity_Tier": self.tier.value,
            "iv_history_count": self.iv_history_count,
            "days_to_mature": self.days_to_next_tier,
            "has_spot_iv": self.has_spot_iv,
            "iv_source": self.iv_source,
            "needs_fidelity_enrichment": self.needs_enrichment,
        }


def compute_maturity_tier(
    ticker: str,
    iv_history_count: Optional[int],
    has_spot_iv: bool = False,
    iv_source: str = "NONE"
) -> MaturityAssessment:
    """
    Compute the volatility maturity assessment for a ticker.

    Args:
        ticker: Stock symbol
        iv_history_count: Number of days of IV history available
        has_spot_iv: Whether current IV value is available
        iv_source: Source of IV data ("SCHWAB", "FIDELITY", "NONE")

    Returns:
        Immutable MaturityAssessment
    """
    count = 0 if iv_history_count is None or pd.isna(iv_history_count) else int(iv_history_count)
    tier = VolatilityMaturityTier.from_history_count(count)

    # Calculate days to next tier
    if tier == VolatilityMaturityTier.MATURE:
        days_to_next = None
    else:
        next_tier_min = {
            VolatilityMaturityTier.SPOT_ONLY: 7,
            VolatilityMaturityTier.EARLY: 30,
            VolatilityMaturityTier.IMMATURE: 120,
        }[tier]
        days_to_next = next_tier_min - count

    return MaturityAssessment(
        ticker=ticker,
        tier=tier,
        iv_history_count=count,
        days_to_next_tier=days_to_next,
        has_spot_iv=has_spot_iv,
        iv_source=iv_source,
    )


def compute_maturity_for_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add Volatility_Maturity_Tier column to DataFrame.

    Expects columns:
    - Ticker
    - iv_history_count (optional, defaults to 0)
    - IV_30D or similar (optional, for has_spot_iv detection)

    Returns:
        DataFrame with added columns:
        - Volatility_Maturity_Tier
        - days_to_mature
        - needs_fidelity_enrichment
    """
    df = df.copy()

    # Ensure iv_history_count exists — fall back to IVEngine's IV_History_Count if needed
    if "iv_history_count" not in df.columns:
        if "IV_History_Count" in df.columns:
            df["iv_history_count"] = df["IV_History_Count"]
        else:
            df["iv_history_count"] = 0

    # Detect spot IV availability
    iv_cols = [c for c in df.columns if "IV_" in c and "Rank" not in c and "Maturity" not in c]
    if iv_cols:
        df["_has_spot_iv"] = df[iv_cols[0]].notna()
    else:
        df["_has_spot_iv"] = False

    # Compute tier for each row
    def compute_row_tier(row):
        count = row.get("iv_history_count", 0)
        if pd.isna(count):
            count = 0
        return VolatilityMaturityTier.from_history_count(int(count)).value

    def compute_days_to_mature(row):
        count = row.get("iv_history_count", 0)
        if pd.isna(count):
            count = 0
        count = int(count)
        if count >= 120:
            return 0
        return 120 - count

    df["Volatility_Maturity_Tier"] = df.apply(compute_row_tier, axis=1)
    df["days_to_mature"] = df.apply(compute_days_to_mature, axis=1)
    df["needs_fidelity_enrichment"] = df["Volatility_Maturity_Tier"] != "MATURE"

    # Clean up temp column
    df.drop(columns=["_has_spot_iv"], inplace=True, errors="ignore")

    logger.info(f"Computed maturity tiers: {df['Volatility_Maturity_Tier'].value_counts().to_dict()}")

    return df


def get_tickers_needing_enrichment(
    df: pd.DataFrame,
    priority_order: bool = True
) -> pd.DataFrame:
    """
    Extract tickers that need Fidelity enrichment, optionally sorted by priority.

    Args:
        df: DataFrame with Volatility_Maturity_Tier column
        priority_order: If True, sort by enrichment priority (SPOT_ONLY first)

    Returns:
        DataFrame with Ticker, tier, iv_history_count, days_to_mature
    """
    if "Volatility_Maturity_Tier" not in df.columns:
        df = compute_maturity_for_dataframe(df)

    # Filter to non-MATURE
    needs_enrichment = df[df["Volatility_Maturity_Tier"] != "MATURE"].copy()

    if len(needs_enrichment) == 0:
        return pd.DataFrame(columns=["Ticker", "Volatility_Maturity_Tier", "iv_history_count", "days_to_mature"])

    # Deduplicate by ticker
    result = needs_enrichment.groupby("Ticker").agg({
        "Volatility_Maturity_Tier": "first",
        "iv_history_count": "max",
        "days_to_mature": "min",
    }).reset_index()

    if priority_order:
        # Sort by priority: SPOT_ONLY > EARLY > IMMATURE
        tier_priority = {"SPOT_ONLY": 1, "EARLY": 2, "IMMATURE": 3}
        result["_priority"] = result["Volatility_Maturity_Tier"].map(tier_priority)
        result = result.sort_values("_priority").drop(columns=["_priority"])

    return result
