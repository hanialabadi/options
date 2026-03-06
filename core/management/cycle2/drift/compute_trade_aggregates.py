"""
Phase 3 Observable: Trade-Level Aggregates

Aggregates leg-level Greeks and premium to trade-level for net exposure tracking.

Design:
- Groups by TradeID
- Sums: Delta, Gamma, Theta, Vega, Premium
- Preserves: TradeID structure (multi-leg trades remain as multiple rows)
- Appends: *_Trade columns alongside leg-level columns

This is an OBSERVATION, not a freeze. Phase 6 will create *_Trade_Entry fields.

Critical: This does NOT collapse rows. Each leg retains its *_Trade value for easy filtering.
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)


def compute_trade_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add trade-level Greek and premium aggregates.
    
    Parameters
    ----------
    df : pd.DataFrame
        Must contain:
        - 'TradeID' (trade identifier)
        - Greek columns: 'Delta', 'Gamma', 'Theta', 'Vega' (optional)
        - 'Premium' (option time value, optional)
    
    Returns
    -------
    pd.DataFrame
        Input DataFrame with added columns:
        - Delta_Trade, Gamma_Trade, Theta_Trade, Vega_Trade, Premium_Trade
    
    Notes
    -----
    - Trade-level values are net exposure (sum of all legs)
    - Each leg row contains the same *_Trade value (denormalized for convenience)
    - Missing Greeks → 0 contribution to trade aggregate
    - Stock legs contribute to Delta_Trade (Delta = quantity for stock)
    
    Example:
    ```
    # Iron Condor (4 legs)
    Leg 1: Delta=-0.30, Delta_Trade=-0.05 (net across all 4 legs)
    Leg 2: Delta=+0.28, Delta_Trade=-0.05 (same value)
    Leg 3: Delta=-0.15, Delta_Trade=-0.05 (same value)
    Leg 4: Delta=+0.12, Delta_Trade=-0.05 (same value)
    ```
    
    Phase 6 will freeze these as *_Trade_Entry for drift analysis.
    """
    if df.empty:
        logger.warning("Empty DataFrame passed to compute_trade_aggregates")
        return df
    
    if "TradeID" not in df.columns:
        logger.error("Missing 'TradeID' column for trade aggregation")
        raise ValueError("Cannot compute trade aggregates: 'TradeID' column missing")
    
    # Columns to aggregate (leg-level → trade-level)
    greek_cols = ["Delta", "Gamma", "Theta", "Vega"]
    other_cols = ["Premium"]
    
    agg_cols = []
    for col in greek_cols + other_cols:
        if col in df.columns:
            agg_cols.append(col)
    
    if not agg_cols:
        logger.warning("No Greek or Premium columns found for trade aggregation")
        # Add placeholder columns with 0 values
        for col in greek_cols + other_cols:
            df[f"{col}_Trade"] = 0.0
        return df
    
    # Fill missing values with 0 for aggregation
    df_agg = df.copy()
    for col in agg_cols:
        df_agg[col] = df_agg[col].fillna(0)
    
    # Aggregate by TradeID
    trade_sums = df_agg.groupby("TradeID")[agg_cols].sum()
    
    # Rename columns to *_Trade
    trade_sums.columns = [f"{col}_Trade" for col in trade_sums.columns]
    
    # Merge back to original DataFrame (denormalized: each leg gets trade-level value)
    df = df.merge(trade_sums, on="TradeID", how="left")
    
    # Fill missing aggregates with 0 (shouldn't happen, but defensive)
    for col in trade_sums.columns:
        df[col] = df[col].fillna(0)
    
    # Add missing columns that weren't in original df
    for col in greek_cols + other_cols:
        trade_col = f"{col}_Trade"
        if trade_col not in df.columns:
            df[trade_col] = 0.0
    
    # Log statistics
    n_trades = df["TradeID"].nunique()
    logger.info(
        f"Computed trade-level aggregates for {len(df)} legs across {n_trades} trades"
    )
    
    # Log sample trade-level exposures
    if "Delta_Trade" in df.columns:
        delta_range = (df["Delta_Trade"].min(), df["Delta_Trade"].max())
        logger.info(f"Delta_Trade range: {delta_range[0]:.3f} to {delta_range[1]:.3f}")
    
    if "Gamma_Trade" in df.columns:
        gamma_range = (df["Gamma_Trade"].min(), df["Gamma_Trade"].max())
        logger.info(f"Gamma_Trade range: {gamma_range[0]:.4f} to {gamma_range[1]:.4f}")
    
    return df
