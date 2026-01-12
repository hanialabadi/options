"""
Phase 3 Enrichment: Skew & Kurtosis Calculation

Calculates distribution metrics for multi-leg strategies.
"""

import pandas as pd
import numpy as np
import logging
from core.data_contracts.config import MANAGEMENT_SAFE_MODE

# Issue 6: Guard scipy dependency for deployment resilience
try:
    from scipy.stats import skew, kurtosis
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False
    # Use basic logging since logger object doesn't exist yet
    logging.getLogger(__name__).error(
        "❌ scipy.stats not available. Skew/Kurtosis calculation will be disabled. "
        "Install with: pip install scipy"
    )

logger = logging.getLogger(__name__)

# Issue 1: IV threshold as documented constant
IV_MID_MAX_THRESHOLD_PCT = 500.0  # Maximum reasonable IV in percentage terms

# Issue 3: IV scale validation
IV_SCALE_VALIDATION_THRESHOLD = 10.0  # If median IV < this, scale mismatch suspected

# Issue 4: Supported AssetType values
SUPPORTED_ASSET_TYPES = {"OPT", "OPTION", "OPTIONS"}  # Normalized to uppercase

# Column name constants for schema integrity
ASSET_TYPE_COL = "AssetType"
TRADE_ID_COL = "TradeID"
IV_MID_COL = "IV Mid"
IV_CROSS_LEG_SKEW_COL = "IV_Cross_Leg_Skew"
IV_CROSS_LEG_KURTOSIS_COL = "IV_Cross_Leg_Kurtosis"
DISTRIBUTION_INVALIDATED_COL = "Distribution_Invalidated"
ORIGINAL_LEG_COUNT_COL = "Original_Leg_Count"
FILTERED_LEG_COUNT_COL = "Filtered_Leg_Count"
ASSET_TYPE_SUPPORTED_COL = "Asset_Type_Supported"


def calculate_skew_and_kurtosis(df: pd.DataFrame, fail_on_missing_scipy: bool = False) -> pd.DataFrame:
    """
    Calculate cross-leg IV dispersion metrics (skew/kurtosis) by TradeID.
    """
    df = df.copy()
    
    # Issue 5: Check scipy availability
    if not SCIPY_AVAILABLE:
        if not MANAGEMENT_SAFE_MODE:
            logger.error("❌ scipy not available, cannot calculate IV Cross-Leg Dispersion.")
        df[IV_CROSS_LEG_SKEW_COL] = np.nan
        df[IV_CROSS_LEG_KURTOSIS_COL] = np.nan
        df[DISTRIBUTION_INVALIDATED_COL] = np.nan
        df[ORIGINAL_LEG_COUNT_COL] = np.nan
        df[FILTERED_LEG_COUNT_COL] = np.nan
        df[ASSET_TYPE_SUPPORTED_COL] = True
        return df
    
    # Validate required columns
    required_cols = [ASSET_TYPE_COL, TRADE_ID_COL, IV_MID_COL]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        if not MANAGEMENT_SAFE_MODE:
            logger.warning(f"⚠️ Missing columns for Skew/Kurtosis: {missing_cols}. Setting all metrics to NaN.")
        df[IV_CROSS_LEG_SKEW_COL] = np.nan
        df[IV_CROSS_LEG_KURTOSIS_COL] = np.nan
        df[DISTRIBUTION_INVALIDATED_COL] = np.nan
        df[ORIGINAL_LEG_COUNT_COL] = np.nan
        df[FILTERED_LEG_COUNT_COL] = np.nan
        df[ASSET_TYPE_SUPPORTED_COL] = np.nan
        return df
    
    # Validate AssetType taxonomy
    asset_type_normalized = df[ASSET_TYPE_COL].astype(str).str.upper().str.strip()
    known_non_option_types = {"STK", "STOCK", "EQUITY", "BOND", "FUT", "FUTURE", "CASH"}
    
    supported_options_mask = asset_type_normalized.isin(SUPPORTED_ASSET_TYPES)
    known_non_options_mask = asset_type_normalized.isin(known_non_option_types)
    
    df[ASSET_TYPE_SUPPORTED_COL] = np.nan
    df.loc[supported_options_mask, ASSET_TYPE_SUPPORTED_COL] = True
    df.loc[~supported_options_mask & ~known_non_options_mask, ASSET_TYPE_SUPPORTED_COL] = False
    
    # Filter to options only
    options_mask = asset_type_normalized.isin(SUPPORTED_ASSET_TYPES)
    stocks_excluded = (~options_mask).sum()
    df_options = df[options_mask].copy()
    
    if len(df_options) == 0:
        if not MANAGEMENT_SAFE_MODE:
            logger.warning(f"⚠️ No option positions found ({stocks_excluded} non-options excluded)")
        df[IV_CROSS_LEG_SKEW_COL] = np.nan
        df[IV_CROSS_LEG_KURTOSIS_COL] = np.nan
        df[DISTRIBUTION_INVALIDATED_COL] = np.nan
        df[ORIGINAL_LEG_COUNT_COL] = np.nan
        df[FILTERED_LEG_COUNT_COL] = np.nan
        return df
    
    if not MANAGEMENT_SAFE_MODE:
        logger.info(f"Processing {len(df_options)} option positions")
    
    # Filter out invalid IV values
    invalid_iv = (
        (df_options[IV_MID_COL].isna()) | 
        (df_options[IV_MID_COL] <= 0) | 
        (df_options[IV_MID_COL] > IV_MID_MAX_THRESHOLD_PCT)
    )
    if invalid_iv.any():
        df_options = df_options[~invalid_iv].copy()
    
    if len(df_options) == 0:
        df[IV_CROSS_LEG_SKEW_COL] = np.nan
        df[IV_CROSS_LEG_KURTOSIS_COL] = np.nan
        df[DISTRIBUTION_INVALIDATED_COL] = np.nan
        df[ORIGINAL_LEG_COUNT_COL] = np.nan
        df[FILTERED_LEG_COUNT_COL] = np.nan
        return df

    def safe_skew(x):
        x_clean = x.dropna()
        if len(x_clean) < 3:
            return np.nan
        try:
            return skew(x_clean)
        except:
            return np.nan

    def safe_kurt(x):
        x_clean = x.dropna()
        if len(x_clean) < 3:
            return np.nan
        try:
            return kurtosis(x_clean)
        except:
            return np.nan

    trade_metrics = df_options.groupby(TRADE_ID_COL)[IV_MID_COL].agg([
        (IV_CROSS_LEG_SKEW_COL, safe_skew),
        (IV_CROSS_LEG_KURTOSIS_COL, safe_kurt)
    ]).reset_index()
    
    # Merge back
    df[IV_CROSS_LEG_SKEW_COL] = np.nan
    df[IV_CROSS_LEG_KURTOSIS_COL] = np.nan
    
    df = df.merge(
        trade_metrics,
        on=TRADE_ID_COL,
        how='left',
        suffixes=('', '_computed')
    )
    
    for col in [IV_CROSS_LEG_SKEW_COL, IV_CROSS_LEG_KURTOSIS_COL]:
        computed_col = f"{col}_computed"
        if computed_col in df.columns:
            df[col] = df[computed_col].combine_first(df[col])
            df.drop(columns=[computed_col], inplace=True)
    
    return df
