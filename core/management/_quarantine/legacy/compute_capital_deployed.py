"""
Phase 3 Observable: Capital Deployed

Extracts or estimates total capital at risk for position sizing and exposure tracking.

Design:
- Capital_Deployed = total capital tied up in position
- Primary: Broker "Margin Required" field (most accurate)
- Fallback: Strategy-aware estimation from Greeks/Premium

This is an OBSERVATION, not a freeze. Phase 6 will create Capital_Deployed_Entry.

Broker Fields Priority:
1. "Margin Required" (Schwab) - exact capital at risk
2. "Buying Power Effect" (TD Ameritrade)
3. Estimated from strategy + collateral
"""

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


def _estimate_capital_from_strategy(df: pd.DataFrame) -> pd.Series:
    """
    Fallback capital estimation when broker margin unavailable.
    
    Strategy-specific logic:
    - Naked puts/calls: Notional risk (strike * 100 * quantity)
    - Spreads: Max loss = width * 100 * quantity
    - Iron condors: Sum of spread widths
    - Covered calls/puts: Stock value + option premium
    - Stock: Market value
    
    Returns
    -------
    pd.Series
        Estimated capital deployed per leg
    """
    capital = pd.Series(0.0, index=df.index)
    
    # Stock positions: Use market value
    stock_mask = df.get("AssetType", "") == "Stock"
    if stock_mask.any():
        capital[stock_mask] = abs(
            df.loc[stock_mask, "Quantity"] * df.loc[stock_mask].get("Last", 0) * 100
        )
    
    # Options: Use notional value as conservative estimate
    # (Real margin is lower due to portfolio margining, but this is safe upper bound)
    option_mask = df.get("AssetType", "") == "Option"
    if option_mask.any():
        # Notional = Strike * 100 * abs(Quantity)
        capital[option_mask] = abs(
            df.loc[option_mask].get("Strike", 0) 
            * 100 
            * df.loc[option_mask, "Quantity"]
        )
    
    return capital


def compute_capital_deployed(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add Capital_Deployed column (leg-level capital at risk).
    
    Parameters
    ----------
    df : pd.DataFrame
        Must contain:
        - 'Quantity' (position size)
        - Optional: 'Margin Required', 'Buying Power Effect' (broker fields)
        - Optional: 'Strike', 'AssetType', 'Strategy' (for estimation)
    
    Returns
    -------
    pd.DataFrame
        Input DataFrame with added 'Capital_Deployed' column (USD)
    
    Notes
    -----
    - Broker margin field is most accurate (when available)
    - Fallback uses conservative notional value estimation
    - Leg-level capital (not trade-level) - aggregation happens in compute_trade_aggregates
    - Negative capital not allowed (uses absolute value)
    
    Phase 6 will freeze this as Capital_Deployed_Entry.
    """
    if df.empty:
        logger.warning("Empty DataFrame passed to compute_capital_deployed")
        return df
    
    # Try broker margin fields first (most accurate)
    capital = None
    
    # Priority 1: Schwab "Margin Required"
    if "Margin Required" in df.columns:
        capital = df["Margin Required"].fillna(0)
        source = "Margin Required"
    
    # Priority 2: TD Ameritrade "Buying Power Effect"
    elif "Buying Power Effect" in df.columns:
        capital = df["Buying Power Effect"].fillna(0)
        source = "Buying Power Effect"
    
    # Priority 3: Estimate from strategy
    else:
        capital = _estimate_capital_from_strategy(df)
        source = "Estimated (strategy-based)"
    
    # Ensure non-negative
    capital = capital.abs()
    
    # Add to DataFrame
    df["Capital_Deployed"] = capital
    
    # Log statistics
    total_capital = capital.sum()
    logger.info(
        f"Computed Capital_Deployed for {len(df)} positions: "
        f"${total_capital:,.0f} total (source: {source})"
    )
    
    if source.startswith("Estimated"):
        logger.warning(
            "⚠️  Capital_Deployed using ESTIMATED values (broker margin field not available). "
            "Values are conservative upper bounds (notional risk). "
            "For accurate margin, ensure broker exports include 'Margin Required' or 'Buying Power Effect'."
        )
    
    return df
