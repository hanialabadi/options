"""
Phase 3 Enrichment: Strategy Metadata Tagging

Tags strategy intent, exit style, edge type, and computes capital deployed.
"""

import pandas as pd
import numpy as np
import logging
from core.data_contracts.config import MANAGEMENT_SAFE_MODE

from core.phase3_constants import (
    STRATEGY_COVERED_CALL,
    STRATEGY_CSP,
    STRATEGY_LONG_STRADDLE,
    STRATEGY_LONG_STRANGLE,
    STRATEGY_BUY_CALL,
    STRATEGY_BUY_PUT,
    ASSET_TYPE_STOCK,
    ASSET_TYPE_OPTION,
    TAG_INTENT_BULLISH_INCOME,
    TAG_INTENT_NEUTRAL_VOL_EDGE,
    TAG_INTENT_DIRECTIONAL_BULLISH,
    TAG_INTENT_DIRECTIONAL_BEARISH,
    TAG_INTENT_YIELD_CAP,
    TAG_INTENT_UNCLASSIFIED,
    TAG_EXIT_TRAIL,
    TAG_EXIT_THETA_HOLD,
    TAG_EXIT_DUAL_LEG,
    TAG_EXIT_MANUAL,
    TAG_EDGE_VOL,
    TAG_EDGE_THETA,
    TAG_EDGE_NONE,
    MIN_VEGA_FOR_VOL_EDGE,
    IV_SPREAD_LIQUIDITY_RISK_THRESHOLD,
    MIN_BASIS_FOR_CALCULATIONS,
    OPTIONS_CONTRACT_MULTIPLIER,
)

logger = logging.getLogger(__name__)

# Column name constants for schema integrity
STRATEGY_COL = "Strategy"
ASSET_TYPE_COL = "AssetType"
VEGA_COL = "Vega"
THETA_COL = "Theta"
QUANTITY_COL = "Quantity"
BASIS_COL = "Basis"
STRIKE_COL = "Strike"
IV_ASK_COL = "IV Ask"
IV_BID_COL = "IV Bid"
LEG_COUNT_COL = "LegCount"
SYMBOL_COL = "Symbol"
TRADE_ID_COL = "TradeID"
CAPITAL_DEPLOYED_COL = "Capital_Deployed"
TAG_INTENT_COL = "Tag_Intent"
TAG_EXIT_STYLE_COL = "Tag_ExitStyle"
TAG_EDGE_TYPE_COL = "Tag_EdgeType"
TAG_LEG_STRUCTURE_COL = "Tag_LegStructure"
IV_SPREAD_COL = "IV_Spread"
IV_LIQUIDITY_RISK_COL = "IV_LiquidityRisk"
VEGA_EFFICIENCY_COL = "Vega_Efficiency"

# Issue 12: Document theta dominance threshold
THETA_DOMINANCE_THRESHOLD = 1.0  # Theta edge if |theta| > |vega| (theta dominant)

# Issue 20: Vega efficiency outlier threshold
MAX_VEGA_EFFICIENCY = 1000.0  # Flag extreme vega efficiency as outlier


def tag_strategy_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """
    Tag strategy metadata: intent, exit style, edge type, capital deployed.
    """
    df = df.copy()
    
    # Issue 2: Validate required columns
    required_cols = [STRATEGY_COL, ASSET_TYPE_COL]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"❌ Missing required columns: {missing}")
    
    # Issue 1: Filter to options only for option-specific tags
    options_mask = df[ASSET_TYPE_COL] == ASSET_TYPE_OPTION
    stocks_excluded = (~options_mask).sum()
    
    if not MANAGEMENT_SAFE_MODE:
        logger.info(f"Processing strategy metadata for {options_mask.sum()} options and {stocks_excluded} non-options")
    
    # Issue 3: Initialize all tag columns to NaN (proper NaN semantics)
    df[TAG_INTENT_COL] = np.nan
    df[TAG_EXIT_STYLE_COL] = np.nan
    df[TAG_EDGE_TYPE_COL] = np.nan
    df[TAG_LEG_STRUCTURE_COL] = np.nan
    
    # Issue 28: Skip option tagging if no options, but continue to capital calculations
    if not options_mask.any():
        if not MANAGEMENT_SAFE_MODE:
            logger.warning("⚠️ No option positions found. Skipping option-specific tags.")
    else:
    
        # Issue 4: Validate Strategy is not NaN for options
        missing_strategy = options_mask & df[STRATEGY_COL].isna()
        if missing_strategy.any() and not MANAGEMENT_SAFE_MODE:
            missing_count = missing_strategy.sum()
            logger.warning(
                f"⚠️ {missing_count} option positions with missing Strategy. "
                f"Tag_Intent/ExitStyle will remain NaN."
            )
        
        # Issue 21, 25: Tag Intent - compute conditions on subset to avoid alignment issues
        options_with_strategy = options_mask & df[STRATEGY_COL].notna()
        
        if options_with_strategy.any():
            df_subset = df[options_with_strategy]
            conditions_intent = [
                df_subset[STRATEGY_COL] == STRATEGY_CSP,
                df_subset[STRATEGY_COL].isin([STRATEGY_LONG_STRADDLE, STRATEGY_LONG_STRANGLE]),
                df_subset[STRATEGY_COL] == STRATEGY_BUY_CALL,
                df_subset[STRATEGY_COL] == STRATEGY_BUY_PUT,
                df_subset[STRATEGY_COL] == STRATEGY_COVERED_CALL,
            ]
            choices_intent = [
                TAG_INTENT_BULLISH_INCOME,
                TAG_INTENT_NEUTRAL_VOL_EDGE,
                TAG_INTENT_DIRECTIONAL_BULLISH,
                TAG_INTENT_DIRECTIONAL_BEARISH,
                TAG_INTENT_YIELD_CAP,
            ]
            df.loc[options_with_strategy, TAG_INTENT_COL] = np.select(
                conditions_intent,
                choices_intent,
                default=TAG_INTENT_UNCLASSIFIED
            )
        
        # Issue 21, 25: Tag Exit Style - compute conditions on subset
        if options_with_strategy.any():
            conditions_exit = [
                df_subset[STRATEGY_COL].isin([STRATEGY_BUY_CALL, STRATEGY_BUY_PUT]),
                df_subset[STRATEGY_COL].isin([STRATEGY_CSP, STRATEGY_COVERED_CALL]),
                df_subset[STRATEGY_COL].isin([STRATEGY_LONG_STRADDLE, STRATEGY_LONG_STRANGLE]),
            ]
            choices_exit = [TAG_EXIT_TRAIL, TAG_EXIT_THETA_HOLD, TAG_EXIT_DUAL_LEG]
            df.loc[options_with_strategy, TAG_EXIT_STYLE_COL] = np.select(
                conditions_exit,
                choices_exit,
                default=TAG_EXIT_MANUAL
            )
    
    # Issue 1, 3, 12: Tag Edge Type - only for options with valid Vega/Theta
    if VEGA_COL in df.columns and THETA_COL in df.columns:
        valid_greeks = options_mask & df[VEGA_COL].notna() & df[THETA_COL].notna()
        
        if valid_greeks.any():
            vega_valid = df.loc[valid_greeks, VEGA_COL]
            theta_valid = df.loc[valid_greeks, THETA_COL]
            
            # Issue 12: Theta edge if |theta| > THETA_DOMINANCE_THRESHOLD * |vega|
            conditions_edge = [
                vega_valid >= MIN_VEGA_FOR_VOL_EDGE,
                np.abs(theta_valid) > (THETA_DOMINANCE_THRESHOLD * np.abs(vega_valid)),
            ]
            choices_edge = [TAG_EDGE_VOL, TAG_EDGE_THETA]
            df.loc[valid_greeks, TAG_EDGE_TYPE_COL] = np.select(conditions_edge, choices_edge, default=TAG_EDGE_NONE)
    
    # Issue 9: Tag Leg Structure - only for options
    if LEG_COUNT_COL in df.columns:
        df.loc[options_mask, TAG_LEG_STRUCTURE_COL] = np.where(
            df.loc[options_mask, LEG_COUNT_COL] > 1,
            "Multi-Leg",
            "Single-Leg"
        )

    
    # === Capital Deployed (Management Exposure) ===
    # Initialize Capital Deployed (per-leg, will aggregate to trade-level later)
    df[CAPITAL_DEPLOYED_COL] = 0.0
    df["Capital_Deployed_Valid"] = pd.Series(False, index=df.index, dtype='boolean')
    
    # Buy Call / Buy Put
    buy_options_mask = df[STRATEGY_COL].isin([STRATEGY_BUY_CALL, STRATEGY_BUY_PUT])
    if buy_options_mask.any():
        df.loc[buy_options_mask, CAPITAL_DEPLOYED_COL] = df.loc[buy_options_mask, BASIS_COL]
        df.loc[buy_options_mask, "Capital_Deployed_Valid"] = True
    
    # Long Straddle / Long Strangle
    straddle_strangle_mask = df[STRATEGY_COL].isin([STRATEGY_LONG_STRADDLE, STRATEGY_LONG_STRANGLE])
    if straddle_strangle_mask.any():
        df.loc[straddle_strangle_mask, CAPITAL_DEPLOYED_COL] = df.loc[straddle_strangle_mask, BASIS_COL]
        df.loc[straddle_strangle_mask, "Capital_Deployed_Valid"] = True
    
    # Covered Call
    covered_call_mask = df[STRATEGY_COL] == STRATEGY_COVERED_CALL
    if covered_call_mask.any():
        stock_leg_mask = covered_call_mask & (df[ASSET_TYPE_COL] == ASSET_TYPE_STOCK)
        option_leg_mask = covered_call_mask & (df[ASSET_TYPE_COL] == ASSET_TYPE_OPTION)
        
        df.loc[stock_leg_mask, CAPITAL_DEPLOYED_COL] = df.loc[stock_leg_mask, BASIS_COL]
        df.loc[stock_leg_mask, "Capital_Deployed_Valid"] = True
        df.loc[option_leg_mask, CAPITAL_DEPLOYED_COL] = df.loc[option_leg_mask, BASIS_COL]
        df.loc[option_leg_mask, "Capital_Deployed_Valid"] = True
    
    # Cash-Secured Put
    csp_mask = df[STRATEGY_COL] == STRATEGY_CSP
    if csp_mask.any():
        if STRIKE_COL in df.columns:
            df.loc[csp_mask, CAPITAL_DEPLOYED_COL] = (
                df.loc[csp_mask, STRIKE_COL] * OPTIONS_CONTRACT_MULTIPLIER * df.loc[csp_mask, QUANTITY_COL].abs()
            )
            df.loc[csp_mask, "Capital_Deployed_Valid"] = True
    
    # Stock (Unknown strategy)
    stock_mask = (df[ASSET_TYPE_COL] == ASSET_TYPE_STOCK) & (~covered_call_mask)
    if stock_mask.any():
        df.loc[stock_mask, CAPITAL_DEPLOYED_COL] = df.loc[stock_mask, BASIS_COL].abs()
        df.loc[stock_mask, "Capital_Deployed_Valid"] = True
    
    # Aggregate per-leg capital to trade-level
    if TRADE_ID_COL in df.columns:
        df["Capital_Deployed_Trade_Level"] = df.groupby(TRADE_ID_COL)[CAPITAL_DEPLOYED_COL].transform('sum')
    
    # IV Spread
    df[IV_SPREAD_COL] = np.nan
    df[IV_LIQUIDITY_RISK_COL] = pd.Series(dtype='boolean')
    df["IV_Spread_Invalid"] = pd.Series(False, index=df.index, dtype='boolean')
    
    if IV_ASK_COL in df.columns and IV_BID_COL in df.columns:
        valid_iv = options_mask & df[IV_ASK_COL].notna() & df[IV_BID_COL].notna()
        if valid_iv.any():
            df.loc[valid_iv, IV_SPREAD_COL] = df.loc[valid_iv, IV_ASK_COL] - df.loc[valid_iv, IV_BID_COL]
            valid_spread = valid_iv & df[IV_SPREAD_COL].notna()
            df.loc[valid_spread, IV_LIQUIDITY_RISK_COL] = (
                df.loc[valid_spread, IV_SPREAD_COL] > IV_SPREAD_LIQUIDITY_RISK_THRESHOLD
            )
    
    # Vega Efficiency
    df[VEGA_EFFICIENCY_COL] = np.nan
    if VEGA_COL in df.columns:
        valid_vega_efficiency = (
            options_mask &
            df[VEGA_COL].notna() &
            df["Capital_Deployed_Trade_Level"].notna() &
            (df["Capital_Deployed_Trade_Level"].abs() >= MIN_BASIS_FOR_CALCULATIONS)
        )
        if valid_vega_efficiency.any():
            df.loc[valid_vega_efficiency, VEGA_EFFICIENCY_COL] = (
                df.loc[valid_vega_efficiency, VEGA_COL] / df.loc[valid_vega_efficiency, "Capital_Deployed_Trade_Level"].abs()
            )
    
    if not MANAGEMENT_SAFE_MODE:
        logger.info(f"✅ Strategy metadata tagged. {df['Capital_Deployed_Valid'].sum()} positions validated.")
    return df
