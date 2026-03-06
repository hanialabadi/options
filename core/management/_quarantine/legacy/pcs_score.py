"""
Phase 3 Enrichment: Current PCS Scoring (INSTRUMENTED)

Current Portfolio Confidence Score (Current_PCS) for evolving position ranking.
"""

import pandas as pd
import numpy as np
import logging
from core.shared.data_contracts.config import MANAGEMENT_SAFE_MODE

from core.management.cycle1.enrich.constants import (
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
    PCS_REVALIDATION_THRESHOLD,
    VEGA_REVALIDATION_THRESHOLD,
    GAMMA_REVALIDATION_THRESHOLD,
    ROI_REVALIDATION_THRESHOLD,
)

logger = logging.getLogger(__name__)

# Column name constants
ASSET_TYPE_COL = "AssetType"
TRADE_ID_COL = "TradeID"
STRATEGY_COL = "Strategy"
GAMMA_COL = "Gamma"
VEGA_COL = "Vega"
PREMIUM_COL = "Premium"
BASIS_COL = "Basis"

# Coverage Flags
COVERAGE_GREEKS = 0x0001
COVERAGE_PREMIUM = 0x0002
COVERAGE_IV_RANK = 0x0004
COVERAGE_EARNINGS = 0x0008
COVERAGE_DTE = 0x0010
COVERAGE_MONEYNESS = 0x0020
COVERAGE_LIQUIDITY = 0x0040
COVERAGE_IV_SURFACE = 0x0080

# Quality levels
QUALITY_COMPLETE = "COMPLETE"
QUALITY_PARTIAL = "PARTIAL"
QUALITY_MINIMAL = "MINIMAL"
QUALITY_INSUFFICIENT = "INSUFFICIENT"


def _diagnose_input_coverage(df: pd.DataFrame) -> tuple[int, list[str], str, int]:
    coverage = 0
    missing = []
    has_greeks = all(col in df.columns for col in ['Gamma', 'Vega'])
    has_premium = all(col in df.columns for col in ['Premium', 'Basis'])
    if has_greeks: coverage |= COVERAGE_GREEKS
    else: missing.append("Greeks")
    if has_premium: coverage |= COVERAGE_PREMIUM
    else: missing.append("Premium/Basis")
    if not (has_greeks and has_premium):
        return coverage, missing, QUALITY_INSUFFICIENT, 0
    optional_checks = [
        ('IV_Rank', COVERAGE_IV_RANK, 'IV_Rank'),
        ('Days_to_Earnings', COVERAGE_EARNINGS, 'Earnings Calendar'),
        ('DTE', COVERAGE_DTE, 'DTE'),
        ('Moneyness_Pct', COVERAGE_MONEYNESS, 'Moneyness'),
        ('Volume', COVERAGE_LIQUIDITY, 'Liquidity'),
        ('IV_Cross_Leg_Skew', COVERAGE_IV_SURFACE, 'IV Surface'),
    ]
    available_count = 0
    for col_name, flag, display_name in optional_checks:
        if col_name in df.columns and df[col_name].notna().any():
            coverage |= flag
            available_count += 1
        else:
            missing.append(display_name)
    input_score = int((available_count / len(optional_checks)) * 100)
    if available_count == len(optional_checks): quality = QUALITY_COMPLETE
    elif available_count >= len(optional_checks) // 2: quality = QUALITY_PARTIAL
    else: quality = QUALITY_MINIMAL
    return coverage, missing, quality, input_score


def calculate_current_pcs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate Current Portfolio Confidence Score (Current_PCS).
    """
    df = df.copy()
    required_columns = [ASSET_TYPE_COL, TRADE_ID_COL, STRATEGY_COL, GAMMA_COL, VEGA_COL, PREMIUM_COL, BASIS_COL]
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"❌ Missing required columns for PCS calculation: {missing_cols}")
    
    coverage_flags, missing_inputs, quality_level, input_score = _diagnose_input_coverage(df)
    options_mask = df[ASSET_TYPE_COL] == "OPTION"
    df_options = df[options_mask].copy()
    
    if len(df_options) == 0:
        for col in ["PCS_GammaScore", "PCS_VegaScore", "PCS_ROIScore", "ROI_Raw_Calc", "ROI_For_Scoring", "PCS", "PCS_GroupAvg"]:
            df[col] = np.nan
        for col in ["PCS_Profile", "PCS_Tier", "Confidence_Tier"]:
            df[col] = None
        for col in ["Unknown_Strategy", "Low_Quality_Signal", "Needs_Revalidation"]:
            df[col] = None
        df["PCS_Data_Quality"] = quality_level
        df["PCS_Coverage_Flags"] = coverage_flags
        df["PCS_Missing_Inputs"] = ", ".join(missing_inputs) if missing_inputs else None
        df["PCS_Input_Score"] = input_score
        df["PCS_Baseline_Only"] = (input_score == 0)
        return df
    
    df_options.loc[df_options[GAMMA_COL].isna() | (df_options[GAMMA_COL] < 0), GAMMA_COL] = 0
    df_options.loc[df_options[VEGA_COL].isna() | (df_options[VEGA_COL] < 0), VEGA_COL] = 0
    df_options.loc[df_options[BASIS_COL].isna() | (df_options[BASIS_COL] <= 0), BASIS_COL] = 1.0

    conditions_profile = [
        df_options[STRATEGY_COL].isin([STRATEGY_LONG_STRADDLE, STRATEGY_LONG_STRANGLE]),
        df_options[STRATEGY_COL].isin([STRATEGY_CSP, STRATEGY_COVERED_CALL]),
        df_options[STRATEGY_COL] == STRATEGY_BUY_CALL,
        df_options[STRATEGY_COL] == STRATEGY_BUY_PUT,
    ]
    choices_profile = [PROFILE_NEUTRAL_VOL, PROFILE_INCOME, PROFILE_DIRECTIONAL_BULL, PROFILE_DIRECTIONAL_BEAR]
    df_options["PCS_Profile"] = np.select(conditions_profile, choices_profile, default=PROFILE_OTHER)
    df_options["Unknown_Strategy"] = df_options["PCS_Profile"] == PROFILE_OTHER

    gamma = df_options[GAMMA_COL]
    vega = df_options[VEGA_COL]
    premium = df_options[PREMIUM_COL]
    basis = df_options[BASIS_COL]
    
    credit_strategies = df_options[STRATEGY_COL].isin([STRATEGY_CSP, STRATEGY_COVERED_CALL])
    roi_raw_calc = premium / basis
    roi_for_scoring = np.where(credit_strategies, np.abs(roi_raw_calc), roi_raw_calc)
    
    gamma_score = np.minimum(gamma * PCS_GAMMA_MULTIPLIER, PCS_GAMMA_MAX)
    vega_score = np.minimum((vega / basis) * PCS_VEGA_MULTIPLIER, PCS_VEGA_MAX)
    roi_score = np.where(roi_for_scoring >= PCS_ROI_THRESHOLD_HIGH, PCS_ROI_SCORE_HIGH,
                         np.where(roi_for_scoring >= PCS_ROI_THRESHOLD_MID, PCS_ROI_SCORE_MID, PCS_ROI_SCORE_LOW))
    
    pcs = np.zeros(len(df_options))
    for profile, weights in [(PROFILE_NEUTRAL_VOL, PCS_WEIGHTS_NEUTRAL_VOL), (PROFILE_INCOME, PCS_WEIGHTS_INCOME),
                             (PROFILE_DIRECTIONAL_BULL, PCS_WEIGHTS_DIRECTIONAL), (PROFILE_DIRECTIONAL_BEAR, PCS_WEIGHTS_DIRECTIONAL),
                             (PROFILE_OTHER, PCS_WEIGHTS_DEFAULT)]:
        mask = df_options["PCS_Profile"] == profile
        if mask.any():
            pcs[mask] = (weights["gamma"] * gamma_score[mask] + weights["vega"] * vega_score[mask] + weights["roi"] * roi_score[mask])
    
    df_options["PCS_GammaScore"] = gamma_score
    df_options["PCS_VegaScore"] = vega_score
    df_options["PCS_ROIScore"] = roi_score
    df_options["ROI_Raw_Calc"] = roi_raw_calc
    df_options["ROI_For_Scoring"] = roi_for_scoring
    df_options["PCS"] = pcs
    df_options["PCS_GroupAvg"] = df_options.groupby(TRADE_ID_COL)["PCS"].transform("mean")

    df_options["PCS_Tier"] = np.where(df_options["PCS"] >= PCS_TIER1_THRESHOLD, PCS_TIER1_LABEL,
                                     np.where(df_options["PCS"] >= PCS_TIER2_THRESHOLD, PCS_TIER2_LABEL,
                                              np.where(df_options["PCS"] >= PCS_TIER3_THRESHOLD, PCS_TIER3_LABEL, PCS_TIER4_LABEL)))
    df_options["Confidence_Tier"] = df_options["PCS_Tier"]
    
    df_options["Low_Quality_Signal"] = ((df_options["PCS"] < PCS_REVALIDATION_THRESHOLD) | (df_options[VEGA_COL] < VEGA_REVALIDATION_THRESHOLD) |
                                        (df_options[GAMMA_COL] < GAMMA_REVALIDATION_THRESHOLD) | (df_options["ROI_For_Scoring"] < ROI_REVALIDATION_THRESHOLD))
    df_options["Needs_Revalidation"] = df_options["Low_Quality_Signal"] | df_options["Unknown_Strategy"]

    for col in ["PCS_GammaScore", "PCS_VegaScore", "PCS_ROIScore", "ROI_Raw_Calc", "ROI_For_Scoring", "PCS", "PCS_GroupAvg"]:
        df[col] = np.nan
    for col in ["PCS_Profile", "PCS_Tier", "Confidence_Tier"]:
        df[col] = None
    for col in ["Unknown_Strategy", "Low_Quality_Signal", "Needs_Revalidation"]:
        df[col] = None
    
    df["PCS_Data_Quality"] = quality_level
    df["PCS_Coverage_Flags"] = coverage_flags
    df["PCS_Missing_Inputs"] = ", ".join(missing_inputs) if missing_inputs else None
    df["PCS_Input_Score"] = input_score
    df["PCS_Baseline_Only"] = (input_score == 0)
    
    pcs_columns = ["PCS_Profile", "PCS_GammaScore", "PCS_VegaScore", "PCS_ROIScore", "ROI_Raw_Calc", "ROI_For_Scoring", "PCS", "PCS_GroupAvg", "PCS_Tier", "Confidence_Tier", "Unknown_Strategy", "Low_Quality_Signal", "Needs_Revalidation"]
    for col in pcs_columns:
        df.loc[df_options.index, col] = df_options[col].values
    return df

calculate_pcs = calculate_current_pcs
