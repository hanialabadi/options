"""
Progressive Fidelity Enrichment Demand Generator

This module generates and manages the queue of tickers that need Fidelity
scraping for IV history collection.

DESIGN PRINCIPLES:
1. PROGRESSIVE - IV history accumulates daily, one scrape at a time
2. TRANSPARENT - Every trade knows why it's gated and what will resolve it
3. NON-BLOCKING - Demand is generated, but never silently blocks trades
4. PRIORITIZED - SPOT_ONLY tickers get scraped before IMMATURE tickers

The scraper is NOT auto-executed - it requires manual login.
This module only manages the demand queue and progress tracking.
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime, date
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field

from .volatility_maturity import (
    VolatilityMaturityTier,
    compute_maturity_for_dataframe,
    get_tickers_needing_enrichment,
)

logger = logging.getLogger(__name__)

# Default paths
DEFAULT_DEMAND_FILE = Path("output/fidelity_iv_demand.csv")
DEFAULT_PROGRESS_FILE = Path("output/fidelity_iv_progress.csv")


@dataclass
class EnrichmentProgress:
    """
    Tracks IV history collection progress for a ticker.
    """
    ticker: str
    current_days: int
    target_days: int = 120
    first_scraped: Optional[date] = None
    last_scraped: Optional[date] = None
    scrape_count: int = 0
    tier: VolatilityMaturityTier = VolatilityMaturityTier.SPOT_ONLY

    @property
    def days_remaining(self) -> int:
        return max(0, self.target_days - self.current_days)

    @property
    def progress_pct(self) -> float:
        return min(100, (self.current_days / self.target_days) * 100)

    @property
    def is_complete(self) -> bool:
        return self.current_days >= self.target_days

    def to_dict(self) -> Dict:
        return {
            "Ticker": self.ticker,
            "IV_History_Days": self.current_days,
            "Target_Days": self.target_days,
            "Days_Remaining": self.days_remaining,
            "Progress_Pct": round(self.progress_pct, 1),
            "Maturity_Tier": self.tier.value,
            "First_Scraped": self.first_scraped.isoformat() if self.first_scraped else None,
            "Last_Scraped": self.last_scraped.isoformat() if self.last_scraped else None,
            "Scrape_Count": self.scrape_count,
            "Complete": self.is_complete,
        }


@dataclass
class DemandReport:
    """
    Summary of Fidelity scraping demand.
    """
    total_tickers: int
    spot_only_count: int
    early_count: int
    immature_count: int
    mature_count: int
    tickers_needing_scrape: List[str]
    priority_queue: List[Tuple[str, str, int]]  # (ticker, tier, days_remaining)
    exported_to: Optional[Path] = None

    def to_summary(self) -> str:
        """Human-readable summary."""
        lines = [
            "=" * 60,
            "FIDELITY IV ENRICHMENT DEMAND",
            "=" * 60,
            "",
            f"Total tickers analyzed: {self.total_tickers}",
            "",
            "Maturity Distribution:",
            f"  MATURE (120+ days):    {self.mature_count:3d}  [Ready for income trades]",
            f"  IMMATURE (30-119):     {self.immature_count:3d}  [Need more data]",
            f"  EARLY (7-29):          {self.early_count:3d}  [Need more data]",
            f"  SPOT_ONLY (0-6):       {self.spot_only_count:3d}  [Need scraping]",
            "",
            f"Tickers needing scrape: {len(self.tickers_needing_scrape)}",
            "",
        ]

        if self.priority_queue:
            lines.append("Priority Queue (top 10):")
            lines.append("-" * 40)
            for i, (ticker, tier, days) in enumerate(self.priority_queue[:10], 1):
                lines.append(f"  {i:2d}. {ticker:6s}  {tier:12s}  {days:3d} days to MATURE")
            lines.append("-" * 40)

        if self.exported_to:
            lines.append("")
            lines.append(f"Demand exported to: {self.exported_to}")
            lines.append("")
            lines.append("To run the scraper:")
            lines.append(f"  python cli/enrich_iv_fidelity.py --input {self.exported_to}")

        lines.append("=" * 60)
        return "\n".join(lines)


def generate_fidelity_demand(
    df: pd.DataFrame,
    output_path: Optional[Path] = None,
    include_all_non_mature: bool = True,
) -> DemandReport:
    """
    Generate Fidelity scraping demand from pipeline data.

    Args:
        df: DataFrame with Ticker and iv_history_count columns
        output_path: Path to write demand CSV (default: output/fidelity_iv_demand.csv)
        include_all_non_mature: If True, include all non-MATURE tickers

    Returns:
        DemandReport with summary and priority queue
    """
    if output_path is None:
        output_path = DEFAULT_DEMAND_FILE

    # Compute maturity tiers
    df_with_tier = compute_maturity_for_dataframe(df.copy())

    # Count by tier
    tier_counts = df_with_tier.groupby("Volatility_Maturity_Tier").size().to_dict()

    spot_only = tier_counts.get("SPOT_ONLY", 0)
    early = tier_counts.get("EARLY", 0)
    immature = tier_counts.get("IMMATURE", 0)
    mature = tier_counts.get("MATURE", 0)

    # Get tickers needing enrichment
    demand_df = get_tickers_needing_enrichment(df_with_tier, priority_order=True)

    tickers_needing_scrape = demand_df["Ticker"].tolist() if not demand_df.empty else []

    # Build priority queue
    priority_queue = []
    if not demand_df.empty:
        for _, row in demand_df.iterrows():
            priority_queue.append((
                row["Ticker"],
                row["Volatility_Maturity_Tier"],
                row.get("days_to_mature", 120),
            ))

    # Export demand file
    exported_to = None
    if tickers_needing_scrape:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        demand_export = pd.DataFrame({
            "Ticker": tickers_needing_scrape,
            "Maturity_Tier": [pq[1] for pq in priority_queue],
            "Days_To_Mature": [pq[2] for pq in priority_queue],
            "Generated_At": datetime.now().isoformat(),
        })
        demand_export.to_csv(output_path, index=False)
        exported_to = output_path
        logger.info(f"Exported {len(tickers_needing_scrape)} tickers to {output_path}")

    return DemandReport(
        total_tickers=len(df_with_tier["Ticker"].unique()),
        spot_only_count=spot_only,
        early_count=early,
        immature_count=immature,
        mature_count=mature,
        tickers_needing_scrape=tickers_needing_scrape,
        priority_queue=priority_queue,
        exported_to=exported_to,
    )


def update_progress_after_scrape(
    scraped_tickers: List[str],
    progress_path: Optional[Path] = None,
) -> pd.DataFrame:
    """
    Update progress tracking after a scrape run.

    Args:
        scraped_tickers: List of tickers that were scraped
        progress_path: Path to progress file

    Returns:
        Updated progress DataFrame
    """
    if progress_path is None:
        progress_path = DEFAULT_PROGRESS_FILE

    # Load existing progress
    if progress_path.exists():
        df_progress = pd.read_csv(progress_path)
    else:
        df_progress = pd.DataFrame(columns=[
            "Ticker", "First_Scraped", "Last_Scraped", "Scrape_Count"
        ])

    today = date.today().isoformat()

    for ticker in scraped_tickers:
        if ticker in df_progress["Ticker"].values:
            mask = df_progress["Ticker"] == ticker
            df_progress.loc[mask, "Last_Scraped"] = today
            df_progress.loc[mask, "Scrape_Count"] = df_progress.loc[mask, "Scrape_Count"] + 1
        else:
            df_progress = pd.concat([df_progress, pd.DataFrame([{
                "Ticker": ticker,
                "First_Scraped": today,
                "Last_Scraped": today,
                "Scrape_Count": 1,
            }])], ignore_index=True)

    df_progress.to_csv(progress_path, index=False)
    logger.info(f"Updated progress for {len(scraped_tickers)} tickers")

    return df_progress


def get_enrichment_summary(df: pd.DataFrame) -> str:
    """
    Get human-readable enrichment summary for dashboard display.

    This answers:
    - What data is missing?
    - Why does that matter?
    - What will resolve it (time vs scraping)?
    """
    df_with_tier = compute_maturity_for_dataframe(df.copy())

    tier_counts = df_with_tier.groupby("Volatility_Maturity_Tier").size().to_dict()
    total = len(df_with_tier)

    mature = tier_counts.get("MATURE", 0)
    needs_data = total - mature

    lines = [
        "IV HISTORY STATUS",
        "-" * 40,
        f"MATURE ({mature}/{total}): Ready for full evaluation",
    ]

    if needs_data > 0:
        lines.append("")
        lines.append(f"AWAITING DATA ({needs_data}/{total}):")

        if tier_counts.get("SPOT_ONLY", 0) > 0:
            lines.append(f"  SPOT_ONLY: {tier_counts['SPOT_ONLY']} tickers need Fidelity scraping")

        if tier_counts.get("EARLY", 0) > 0:
            lines.append(f"  EARLY: {tier_counts['EARLY']} tickers have 7-29 days collected")

        if tier_counts.get("IMMATURE", 0) > 0:
            lines.append(f"  IMMATURE: {tier_counts['IMMATURE']} tickers have 30-119 days collected")

        lines.append("")
        lines.append("Resolution: Daily Fidelity scraping accumulates IV history")
        lines.append("            120 days of history = MATURE tier")

    return "\n".join(lines)


def check_daily_scrape_needed() -> bool:
    """
    Check if daily scrape is needed based on last scrape timestamp.

    Returns True if no scrape happened today.
    """
    progress_path = DEFAULT_PROGRESS_FILE

    if not progress_path.exists():
        return True

    df_progress = pd.read_csv(progress_path)

    if df_progress.empty:
        return True

    # Check if any ticker was scraped today
    today = date.today().isoformat()
    last_scrapes = df_progress["Last_Scraped"].dropna().unique()

    return today not in last_scrapes


def get_daily_scrape_recommendation() -> Dict:
    """
    Get recommendation for today's scrape run.

    Returns dict with:
    - should_scrape: bool
    - reason: str
    - priority_tickers: list
    """
    needs_scrape = check_daily_scrape_needed()

    if not needs_scrape:
        return {
            "should_scrape": False,
            "reason": "Already scraped today",
            "priority_tickers": [],
        }

    # Read demand file for priority
    if DEFAULT_DEMAND_FILE.exists():
        df_demand = pd.read_csv(DEFAULT_DEMAND_FILE)
        priority_tickers = df_demand["Ticker"].tolist()[:20]  # Top 20
    else:
        priority_tickers = []

    return {
        "should_scrape": True,
        "reason": "No scrape recorded today - IV history needs accumulation",
        "priority_tickers": priority_tickers,
    }
