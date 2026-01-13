"""
Phase 3 Enrichment: Confidence Tier Scoring

⚠️  DEPRECATED: This module is now consolidated into pcs_score.py

The PCS (Portfolio Confidence Score) calculation in pcs_score.py now
handles both PCS_Tier and Confidence_Tier using shared constants.

This file is kept for backward compatibility but should not be used directly.
Import calculate_pcs from pcs_score.py instead.
"""

import logging
import pandas as pd

from core.phase3_constants import (
    PCS_TIER1_THRESHOLD,
    PCS_TIER2_THRESHOLD,
    PCS_TIER3_THRESHOLD,
)

logger = logging.getLogger(__name__)


def score_confidence_tier(df: pd.DataFrame) -> pd.DataFrame:
    """
    Score confidence tiers based on PCS.
    
    ⚠️  DEPRECATED: Use calculate_pcs() from pcs_score.py instead.
    
    This function is redundant with pcs_score.py and should not be called separately.
    The tier logic is now part of the main PCS calculation.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with PCS column
    
    Returns
    -------
    pd.DataFrame
        DataFrame with Confidence_Tier column
    """
    logger.warning(
        "⚠️  score_confidence_tier() is deprecated. "
        "Use calculate_pcs() from pcs_score.py instead."
    )
    
    if "PCS" not in df.columns:
        raise ValueError(
            "❌ 'PCS' column required. Run calculate_pcs() first."
        )
    
    # Apply tier logic (now using shared constants)
    df["Confidence_Tier"] = df["PCS"].apply(
        lambda pcs: "Tier 1" if pcs >= PCS_TIER1_THRESHOLD else
                    "Tier 2" if pcs >= PCS_TIER2_THRESHOLD else
                    "Tier 3" if pcs >= PCS_TIER3_THRESHOLD else
                    "Tier 4"
    )
    
    return df
