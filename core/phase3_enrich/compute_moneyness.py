"""
Phase 3 Enrichment: Moneyness Calculation

Purely descriptive state variable for options management.
Calculates how far in/out of the money an option position is.

This is NOT a decision signal. Phase 3 remains non-decision-making.
"""

import pandas as pd
import numpy as np
import logging

from core.phase3_constants import (
    ASSET_TYPE_OPTION,
    ATM_THRESHOLD,
    MONEYNESS_ITM,
    MONEYNESS_ATM,
    MONEYNESS_OTM,
)
from core.phase2_constants import (
    OPTION_TYPE_CALL,
    OPTION_TYPE_PUT,
)

logger = logging.getLogger(__name__)

# Column name constants (schema integrity)
UL_LAST_COL = "UL Last"  # Canonical name from Phase 1


def compute_moneyness(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute moneyness for option positions (vectorized).
    
    Moneyness is a standard options management metric that describes
    the relationship between strike price and underlying price.
    
    This is a DESCRIPTIVE field only:
    - No strategy logic
    - No rolling logic
    - No decision thresholds
    - No margin assumptions
    
    Prevents Phase 4 from repeatedly recalculating (UL_Last - Strike) / Strike
    for every assignment risk check, roll timing evaluation, or exit decision.
    
    Fixed issues:
    - Vectorized calculation (no apply())
    - Uses shared constants
    - Fixed boundary bug in option type validation
    - Uses logging
    
    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame with columns:
        - AssetType (STOCK/OPTION)
        - Strike (option strike price)
        - OptionType (Call/Put)
        - UL Last (underlying last price)
        - Underlying_Ticker (canonical equity identity)
    
    Returns
    -------
    pandas.DataFrame
        Original DataFrame with appended columns:
        - Moneyness_Pct (float): (UL_Last - Strike) / Strike
        - Moneyness_Label (str): ITM/ATM/OTM categorical label
    
    Notes
    -----
    Moneyness interpretation:
    - For CALLS:
        * Positive moneyness → ITM (underlying above strike)
        * Near-zero moneyness → ATM
        * Negative moneyness → OTM (underlying below strike)
    
    - For PUTS:
        * Negative moneyness → ITM (underlying below strike)
        * Near-zero moneyness → ATM
        * Positive moneyness → OTM (underlying above strike)
    
    Thresholds (constants, no tuning):
    - ATM range: abs(moneyness) < 0.05 (±5%)
    - ITM/OTM: abs(moneyness) ≥ 0.05
    """
    df = df.copy()
    
    # === Validate required columns ===
    required = ["AssetType", "Strike", UL_LAST_COL, "OptionType"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"❌ Missing required columns for moneyness: {missing}")
    
    # === Vectorized Moneyness Calculation ===
    # Only calculate for options
    option_mask = df["AssetType"] == ASSET_TYPE_OPTION
    stock_mask = df["AssetType"] != ASSET_TYPE_OPTION
    
    # Initialize columns
    df["Moneyness_Pct"] = np.nan
    df["Moneyness_Label"] = np.nan  # Use NaN, not None, for consistency
    
    # Explicitly exclude stock rows (Phase 3 contract)
    df.loc[stock_mask, "Moneyness_Label"] = np.nan
    df.loc[stock_mask, "Moneyness_Pct"] = np.nan
    
    if not option_mask.any():
        logger.info("No options in dataset, skipping moneyness calculation")
        return df
    
    # === Validate OptionType BEFORE calculation (fail fast) ===
    option_types = df.loc[option_mask, "OptionType"]
    invalid_types = ~option_types.isin([OPTION_TYPE_CALL, OPTION_TYPE_PUT])
    if invalid_types.any():
        bad_types = option_types[invalid_types].unique()
        bad_count = invalid_types.sum()
        raise ValueError(
            f"❌ {bad_count} options have invalid OptionType values: {list(bad_types)}. "
            f"Must be '{OPTION_TYPE_CALL}' or '{OPTION_TYPE_PUT}'. "
            "Phase 2 should have caught this."
        )
    
    # Calculate moneyness: (UL_Last - Strike) / Strike
    # Only where Strike > 0 and UL Last > 0 to avoid division by zero and negative prices
    valid_calc_mask = (
        option_mask & 
        (df["Strike"] > 0) & 
        df["Strike"].notna() & 
        (df[UL_LAST_COL] > 0) & 
        df[UL_LAST_COL].notna()
    )
    
    if valid_calc_mask.any():
        df.loc[valid_calc_mask, "Moneyness_Pct"] = (
            (df.loc[valid_calc_mask, UL_LAST_COL] - df.loc[valid_calc_mask, "Strike"]) 
            / df.loc[valid_calc_mask, "Strike"]
        )
        
        # === Invariant Assertion: Moneyness sign must be consistent with price direction ===
        # For audit/debug: validate (UL_Last - Strike) sign matches Moneyness_Pct sign
        calculated_moneyness = df.loc[valid_calc_mask, "Moneyness_Pct"]
        ul_minus_strike = df.loc[valid_calc_mask, UL_LAST_COL] - df.loc[valid_calc_mask, "Strike"]
        
        # Signs must match (both positive, both negative, or both zero)
        sign_mismatch = np.sign(calculated_moneyness) != np.sign(ul_minus_strike)
        if sign_mismatch.any():
            mismatch_count = sign_mismatch.sum()
            raise ValueError(
                f"❌ FATAL: {mismatch_count} options have Moneyness_Pct sign mismatch. "
                "This indicates upstream data corruption (inverted UL/Strike mapping)."
            )
    
    # === Assign Categorical Labels (vectorized) ===
    # Only label where moneyness was calculated
    labeled_mask = valid_calc_mask & df["Moneyness_Pct"].notna()
    
    if labeled_mask.any():
        # Get subset for labeling
        labeled_df = df[labeled_mask].copy()
        moneyness_vals = labeled_df["Moneyness_Pct"]
        option_types = labeled_df["OptionType"]
        abs_moneyness = np.abs(moneyness_vals)
        
        # === Classification Logic (TERMINAL: each row gets exactly one label) ===
        
        # 1. ATM: within ±5% of strike (terminal condition)
        # IMPORTANT: This is STRIKE-RELATIVE, not delta-relative.
        # A deep ITM put at 1% of strike is NOT ATM despite low absolute price.
        # This is a volatility-agnostic, strike-distance-only metric.
        # Phase 4+ must NOT treat this as a delta proxy.
        atm_mask = abs_moneyness < ATM_THRESHOLD
        df.loc[labeled_df[atm_mask].index, "Moneyness_Label"] = MONEYNESS_ATM
        
        # 2. For Calls (non-ATM): positive moneyness → ITM, negative → OTM
        call_mask = (option_types == OPTION_TYPE_CALL) & ~atm_mask
        call_itm = call_mask & (moneyness_vals > 0)
        call_otm = call_mask & (moneyness_vals <= 0)
        df.loc[labeled_df[call_itm].index, "Moneyness_Label"] = MONEYNESS_ITM
        df.loc[labeled_df[call_otm].index, "Moneyness_Label"] = MONEYNESS_OTM
        
        # 3. For Puts (non-ATM): negative moneyness → ITM, positive → OTM
        put_mask = (option_types == OPTION_TYPE_PUT) & ~atm_mask
        put_itm = put_mask & (moneyness_vals < 0)
        put_otm = put_mask & (moneyness_vals >= 0)
        df.loc[labeled_df[put_itm].index, "Moneyness_Label"] = MONEYNESS_ITM
        df.loc[labeled_df[put_otm].index, "Moneyness_Label"] = MONEYNESS_OTM
        
        # === Validation: All labeled rows must have a classification ===
        unclassified = df.loc[labeled_mask, "Moneyness_Label"].isna()
        if unclassified.any():
            unclassified_count = unclassified.sum()
            raise ValueError(
                f"❌ LOGIC ERROR: {unclassified_count} options calculated moneyness but not classified. "
                "This indicates incomplete classification logic."
            )
    
    # === Audit-grade logging: all exclusion categories ===
    total_rows = len(df)
    stock_rows = stock_mask.sum()
    option_rows = option_mask.sum()
    calculated = valid_calc_mask.sum()
    skipped = option_rows - calculated
    
    logger.info(
        f"📊 Moneyness Summary: {total_rows} total rows, "
        f"{stock_rows} stocks (excluded), {option_rows} options"
    )
    
    if skipped > 0:
        # Break down skip reasons
        invalid_strike = option_mask & ((df["Strike"] <= 0) | df["Strike"].isna())
        invalid_ul = option_mask & ((df[UL_LAST_COL] <= 0) | df[UL_LAST_COL].isna())
        
        skip_reasons = []
        if invalid_strike.sum() > 0:
            skip_reasons.append(f"{invalid_strike.sum()} invalid Strike (≤0 or NaN)")
        if invalid_ul.sum() > 0:
            skip_reasons.append(f"{invalid_ul.sum()} invalid UL Last (≤0 or NaN)")
        
        logger.warning(
            f"⚠️  Moneyness calculation: {calculated}/{option_rows} options calculated, "
            f"{skipped} skipped. Reasons: {', '.join(skip_reasons) if skip_reasons else 'unknown'}"
        )
    else:
        logger.info(f"✅ Moneyness calculated for all {calculated} option positions")
    
    return df
