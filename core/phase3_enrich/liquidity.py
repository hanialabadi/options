"""
Phase 3 Enrichment: Liquidity Screening

Computes liquidity metrics and flags for option positions.
"""

import pandas as pd
import numpy as np
import logging
from core.data_contracts.config import MANAGEMENT_SAFE_MODE

from core.phase3_constants import (
    LIQUIDITY_OI_THRESHOLD,
    LIQUIDITY_SPREAD_PCT_THRESHOLD,
    LIQUIDITY_MIN_DOLLAR_VOLUME,
    LIQUIDITY_MIN_VEGA_EFFICIENCY,
    LIQUIDITY_WIDE_SPREAD_THRESHOLD,
    MIN_SPREAD_FOR_VEGA_EFFICIENCY,
)

logger = logging.getLogger(__name__)
# Column name constants for schema integrity
ASSET_TYPE_COL = "AssetType"
OPEN_INT_COL = "Open Int"
ASK_COL = "Ask"
BID_COL = "Bid"
VOLUME_COL = "Volume"
VEGA_COL = "Vega"

def enrich_liquidity(
    df: pd.DataFrame,
    oi_threshold: int = LIQUIDITY_OI_THRESHOLD,
    spread_pct_threshold: float = LIQUIDITY_SPREAD_PCT_THRESHOLD,
    min_dollar_volume: float = LIQUIDITY_MIN_DOLLAR_VOLUME,
    min_vega_efficiency: float = LIQUIDITY_MIN_VEGA_EFFICIENCY,
    wide_spread_threshold: float = LIQUIDITY_WIDE_SPREAD_THRESHOLD,
) -> pd.DataFrame:
    """
    Enrich options DataFrame with liquidity screening columns.
    """
    df = df.copy()
    
    total_rows = len(df)
    if not MANAGEMENT_SAFE_MODE:
        logger.info(f"Starting liquidity enrichment on {total_rows} rows")
    
    # === Validate required columns ===
    required_cols = [ASSET_TYPE_COL, OPEN_INT_COL, ASK_COL, BID_COL, VOLUME_COL, VEGA_COL]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"❌ Missing required columns for liquidity enrichment: {missing}")
    
    # === Filter to options only ===
    options_mask = df[ASSET_TYPE_COL] == "OPTION"
    stocks_excluded = (~options_mask).sum()
    df_options = df[options_mask].copy()
    
    if len(df_options) == 0:
        if not MANAGEMENT_SAFE_MODE:
            logger.warning(f"⚠️ No option positions found ({stocks_excluded} stocks excluded)")
        # Return original df with empty liquidity columns
        for col in ["OI", "Spread_Pct", "Dollar_Volume", "Vega_Efficiency",
                    "OI_OK", "Spread_OK", "DollarVolume_OK", "Vega_OK", 
                    "WideSpread_Flag", "Liquidity_Measurable", "Liquidity_OK"]:
            df[col] = np.nan if col in ["OI", "Spread_Pct", "Dollar_Volume", "Vega_Efficiency"] else False
        return df
    
    if not MANAGEMENT_SAFE_MODE:
        logger.info(f"Processing {len(df_options)} option positions ({stocks_excluded} stocks excluded)")
    
    # --- Numeric columns ---
    df_options["OI"] = df_options[OPEN_INT_COL]
    mid_price = (df_options[ASK_COL] + df_options[BID_COL]) / 2
    spread_raw = df_options[ASK_COL] - df_options[BID_COL]
    
    measurable = (
        (df_options[ASK_COL] > 0) &
        (df_options[BID_COL] > 0) &
        (df_options[VOLUME_COL] >= 0) &
        (df_options[VEGA_COL] > 0)
    )
    
    df_options["Spread_Pct"] = np.where(measurable & (mid_price > 0), spread_raw / mid_price, np.nan)
    df_options["Dollar_Volume"] = df_options[VOLUME_COL] * mid_price
    safe_spread = np.maximum(spread_raw.abs(), MIN_SPREAD_FOR_VEGA_EFFICIENCY)
    df_options["Vega_Efficiency"] = df_options[VEGA_COL] / safe_spread

    # --- Tag/flag columns ---
    df_options["Liquidity_Measurable"] = measurable
    df_options["OI_OK"] = measurable & (df_options["OI"] >= oi_threshold)
    df_options["Spread_OK"] = measurable & (df_options["Spread_Pct"] <= spread_pct_threshold)
    df_options["WideSpread_Flag"] = measurable & (df_options["Spread_Pct"] > wide_spread_threshold)
    df_options["DollarVolume_OK"] = measurable & (df_options["Dollar_Volume"] >= min_dollar_volume)
    df_options["Vega_OK"] = measurable & (df_options["Vega_Efficiency"] >= min_vega_efficiency)

    df_options["Liquidity_OK"] = (
        measurable &
        df_options["OI_OK"] &
        df_options["Spread_OK"] &
        df_options["DollarVolume_OK"] &
        df_options["Vega_OK"]
    )
    
    # Merge back
    for col in ["OI", "Spread_Pct", "Dollar_Volume", "Vega_Efficiency"]:
        df[col] = np.nan
    for col in ["OI_OK", "Spread_OK", "DollarVolume_OK", "Vega_OK", 
                "WideSpread_Flag", "Liquidity_Measurable", "Liquidity_OK"]:
        df[col] = False
    
    df.loc[options_mask, df_options.columns] = df_options

    return df
