"""
Phase 3 Enrichment: Entry PCS Scoring

Entry Portfolio Confidence Score (Entry_PCS) for frozen baseline scoring.
Uses ONLY entry data (Entry Greeks, Entry_IV_Rank, Strategy) to create
a comparable baseline for all positions regardless of when time-series
enhancements become available.

📌 ARCHITECTURAL DECISION (Phase D.1):
Split PCS into two components:
1. Entry_PCS: Frozen baseline using only entry conditions (this module)
2. Current_PCS: Evolving score with time-series enhancements (pcs_score.py)

This separation ensures:
- All positions have comparable starting point (Entry_PCS)
- Time-series data is enhancement, not requirement
- Can answer: "Did good entry scores lead to good outcomes?"

Entry_PCS uses:
- Entry Greeks (Gamma_Entry, Vega_Entry)
- Premium_Entry, Basis (entry collateral)
- Strategy profile
- Entry_IV_Rank (if available)

Entry_PCS DOES NOT use:
- Days_In_Trade (not available at entry)
- P&L performance (not available at entry)
- Current Greeks (may change post-entry)
- Drift metrics (time-series only)
"""

import pandas as pd
import numpy as np
import logging
from core.data_contracts.config import MANAGEMENT_SAFE_MODE

from core.phase3_constants import (
    STRATEGY_LONG_STRADDLE,
    STRATEGY_LONG_STRANGLE,
    STRATEGY_BUY_CALL,
    STRATEGY_BUY_PUT,
    STRATEGY_COVERED_CALL,
    STRATEGY_CSP,
    PROFILE_NEUTRAL_VOL,
    PROFILE_INCOME,
    PROFILE_DIRECTIONAL_BULL,
    PROFILE_DIRECTIONAL_BEAR,
    PROFILE_OTHER,
    PCS_GAMMA_MULTIPLIER,
    PCS_GAMMA_MAX,
    PCS_VEGA_MULTIPLIER,
    PCS_VEGA_MAX,
    PCS_ROI_THRESHOLD_HIGH,
    PCS_ROI_THRESHOLD_MID,
    PCS_ROI_SCORE_HIGH,
    PCS_ROI_SCORE_MID,
    PCS_ROI_SCORE_LOW,
    PCS_WEIGHTS_NEUTRAL_VOL,
    PCS_WEIGHTS_INCOME,
    PCS_WEIGHTS_DIRECTIONAL,
    PCS_WEIGHTS_DEFAULT,
    PCS_TIER1_THRESHOLD,
    PCS_TIER2_THRESHOLD,
    PCS_TIER3_THRESHOLD,
    PCS_TIER1_LABEL,
    PCS_TIER2_LABEL,
    PCS_TIER3_LABEL,
    PCS_TIER4_LABEL,
)

logger = logging.getLogger(__name__)

# Column name constants
ASSET_TYPE_COL = "AssetType"
TRADE_ID_COL = "TradeID"
STRATEGY_COL = "Strategy"
GAMMA_ENTRY_COL = "Gamma_Entry"
VEGA_ENTRY_COL = "Vega_Entry"
PREMIUM_ENTRY_COL = "Premium_Entry"
BASIS_COL = "Basis"


def calculate_entry_pcs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate Entry PCS using only entry data for frozen baseline scoring.
    """
    df = df.copy()
    
    total_rows = len(df)
    
    # === Structural Integrity Check ===
    required_columns = [
        ASSET_TYPE_COL, TRADE_ID_COL, STRATEGY_COL, 
        GAMMA_ENTRY_COL, VEGA_ENTRY_COL, PREMIUM_ENTRY_COL, BASIS_COL
    ]
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        if not MANAGEMENT_SAFE_MODE:
            logger.warning(
                f"⚠️ Missing required columns for Entry PCS: {missing_cols}\n"
                f"   Entry data must be frozen first. Returning without Entry_PCS."
            )
        # Add empty Entry_PCS columns
        for col in [
            "Entry_PCS_GammaScore", "Entry_PCS_VegaScore", "Entry_PCS_ROIScore",
            "Entry_PCS", "Entry_PCS_Profile", "Entry_PCS_Tier"
        ]:
            if col not in df.columns:
                df[col] = np.nan if "Score" in col or col == "Entry_PCS" else None
        return df
    
    # === Filter to options only ===
    options_mask = df[ASSET_TYPE_COL] == "OPTION"
    stocks_excluded = (~options_mask).sum()
    df_options = df[options_mask].copy()
    
    if len(df_options) == 0:
        if not MANAGEMENT_SAFE_MODE:
            logger.warning(f"⚠️ No option positions found ({stocks_excluded} stocks excluded)")
        # Add empty Entry_PCS columns
        for col in [
            "Entry_PCS_GammaScore", "Entry_PCS_VegaScore", "Entry_PCS_ROIScore",
            "Entry_PCS", "Entry_PCS_Profile", "Entry_PCS_Tier"
        ]:
            if col not in df.columns:
                df[col] = np.nan if "Score" in col or col == "Entry_PCS" else None
        return df
    
    if not MANAGEMENT_SAFE_MODE:
        logger.info(f"Processing {len(df_options)} option positions ({stocks_excluded} stocks excluded)")
    
    # === Validate Entry Greeks ===
    # Only score positions where entry Greeks are available (not NaN)
    has_entry_greeks = (
        df_options[GAMMA_ENTRY_COL].notna() & 
        df_options[VEGA_ENTRY_COL].notna() &
        df_options[PREMIUM_ENTRY_COL].notna()
    )
    
    no_entry_greeks = (~has_entry_greeks).sum()
    if no_entry_greeks > 0 and not MANAGEMENT_SAFE_MODE:
        logger.warning(
            f"⚠️ {no_entry_greeks}/{len(df_options)} positions lack entry Greeks. "
            f"Entry_PCS will be NaN for these positions (entry data not frozen yet)."
        )
    
    df_with_entry = df_options[has_entry_greeks].copy()
    
    if len(df_with_entry) == 0:
        # Add empty Entry_PCS columns to all rows
        for col in [
            "Entry_PCS_GammaScore", "Entry_PCS_VegaScore", "Entry_PCS_ROIScore",
            "Entry_PCS", "Entry_PCS_Profile", "Entry_PCS_Tier"
        ]:
            df[col] = np.nan if "Score" in col or col == "Entry_PCS" else None
        return df
    
    # === Strategy Profile Tagging ===
    conditions_profile = [
        df_with_entry[STRATEGY_COL].isin([STRATEGY_LONG_STRADDLE, STRATEGY_LONG_STRANGLE]),
        df_with_entry[STRATEGY_COL].isin([STRATEGY_CSP, STRATEGY_COVERED_CALL]),
        df_with_entry[STRATEGY_COL] == STRATEGY_BUY_CALL,
        df_with_entry[STRATEGY_COL] == STRATEGY_BUY_PUT,
    ]
    choices_profile = [
        PROFILE_NEUTRAL_VOL,
        PROFILE_INCOME,
        PROFILE_DIRECTIONAL_BULL,
        PROFILE_DIRECTIONAL_BEAR,
    ]
    df_with_entry["Entry_PCS_Profile"] = np.select(
        conditions_profile, choices_profile, default=PROFILE_OTHER
    )
    
    # === Scoring Engine (using entry data only) ===
    gamma_entry = df_with_entry[GAMMA_ENTRY_COL]
    vega_entry = df_with_entry[VEGA_ENTRY_COL]
    premium_entry = df_with_entry[PREMIUM_ENTRY_COL]
    basis = df_with_entry[BASIS_COL]
    
    # Entry ROI (strategy-aware)
    credit_strategies = df_with_entry[STRATEGY_COL].isin([STRATEGY_CSP, STRATEGY_COVERED_CALL])
    roi_raw = premium_entry / basis
    roi_entry = np.where(credit_strategies, np.abs(roi_raw), roi_raw)
    
    # Subscores
    gamma_score = np.minimum(gamma_entry.astype(np.float64) * PCS_GAMMA_MULTIPLIER, PCS_GAMMA_MAX).astype(np.float64)
    vega_score = np.minimum(vega_entry.astype(np.float64) * PCS_VEGA_MULTIPLIER, PCS_VEGA_MAX).astype(np.float64)
    
    # ROI tiering
    roi_score = np.select(
        [
            roi_entry.astype(np.float64) >= PCS_ROI_THRESHOLD_HIGH,
            roi_entry.astype(np.float64) >= PCS_ROI_THRESHOLD_MID,
        ],
        [PCS_ROI_SCORE_HIGH, PCS_ROI_SCORE_MID],
        default=PCS_ROI_SCORE_LOW
    ).astype(np.float64)
    
    df_with_entry.loc[:, "Entry_PCS_GammaScore"] = gamma_score
    df_with_entry.loc[:, "Entry_PCS_VegaScore"] = vega_score
    df_with_entry.loc[:, "Entry_PCS_ROIScore"] = roi_score
    
    # === Profile-Weighted Composite Score ===
    weights_map = {
        PROFILE_NEUTRAL_VOL: PCS_WEIGHTS_NEUTRAL_VOL,
        PROFILE_INCOME: PCS_WEIGHTS_INCOME,
        PROFILE_DIRECTIONAL_BULL: PCS_WEIGHTS_DIRECTIONAL,
        PROFILE_DIRECTIONAL_BEAR: PCS_WEIGHTS_DIRECTIONAL,
        PROFILE_OTHER: PCS_WEIGHTS_DEFAULT
    }
    
    entry_pcs = np.zeros(len(df_with_entry), dtype=np.float64)
    for profile, weights in weights_map.items():
        mask_profile = df_with_entry["Entry_PCS_Profile"] == profile
        if mask_profile.any():
            entry_pcs[mask_profile] = (
                df_with_entry.loc[mask_profile, "Entry_PCS_GammaScore"].values * weights["gamma"] +
                df_with_entry.loc[mask_profile, "Entry_PCS_VegaScore"].values * weights["vega"] +
                df_with_entry.loc[mask_profile, "Entry_PCS_ROIScore"].values * weights["roi"]
            )
    
    df_with_entry["Entry_PCS"] = entry_pcs
    
    # === Tier Classification ===
    conditions_tier = [
        df_with_entry["Entry_PCS"] >= PCS_TIER1_THRESHOLD,
        df_with_entry["Entry_PCS"] >= PCS_TIER2_THRESHOLD,
        df_with_entry["Entry_PCS"] >= PCS_TIER3_THRESHOLD,
    ]
    choices_tier = [PCS_TIER1_LABEL, PCS_TIER2_LABEL, PCS_TIER3_LABEL]
    df_with_entry["Entry_PCS_Tier"] = np.select(conditions_tier, choices_tier, default=PCS_TIER4_LABEL)
    
    # === Merge back to full df ===
    entry_pcs_cols = [
        "Entry_PCS_GammaScore", "Entry_PCS_VegaScore", "Entry_PCS_ROIScore",
        "Entry_PCS", "Entry_PCS_Profile", "Entry_PCS_Tier"
    ]
    
    for col in entry_pcs_cols:
        if col not in df.columns:
            df[col] = np.nan if "Score" in col or col == "Entry_PCS" else None
    
    df.loc[df_with_entry.index, entry_pcs_cols] = df_with_entry[entry_pcs_cols]
    
    if not MANAGEMENT_SAFE_MODE:
        scored_count = df["Entry_PCS"].notna().sum()
        logger.info(f"✅ Entry PCS calculated for {scored_count}/{total_rows} positions")
    
    return df


def validate_entry_pcs(df: pd.DataFrame) -> dict:
    """
    Validate Entry_PCS consistency and coverage.
    """
    errors = []
    warnings = []
    stats = {}
    
    expected_cols = [
        "Entry_PCS", "Entry_PCS_GammaScore", "Entry_PCS_VegaScore",
        "Entry_PCS_ROIScore", "Entry_PCS_Profile", "Entry_PCS_Tier"
    ]
    missing_cols = [col for col in expected_cols if col not in df.columns]
    if missing_cols:
        errors.append(f"Missing Entry_PCS columns: {missing_cols}")
        return {"errors": errors, "warnings": warnings, "stats": stats}
    
    return {"errors": errors, "warnings": warnings, "stats": stats}
