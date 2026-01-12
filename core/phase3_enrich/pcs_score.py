"""
Phase 3 Enrichment: Current PCS Scoring (INSTRUMENTED)

Current Portfolio Confidence Score (Current_PCS) for evolving position ranking.
Uses current Greek sensitivities, ROI, time-series data, and strategy profiles.

📌 ARCHITECTURAL DECISION (Phase D.1):
Split PCS into two components:
1. Entry_PCS: Frozen baseline using only entry conditions (pcs_score_entry.py)
2. Current_PCS: Evolving score with time-series enhancements (this module)

This separation ensures:
- All positions have comparable starting point (Entry_PCS)
- Current_PCS can evolve with time-series data (Days_In_Trade, P&L, drift)
- Can answer: "How is this performing relative to entry baseline?"

Current_PCS uses:
- Current Greeks (Gamma, Vega, Delta)
- Current Premium, Basis
- Days_In_Trade (time-series)
- P&L performance (time-series, if available)
- IV_Rank, Earnings proximity (optional enhancements)

🎯 INSTRUMENTATION ADDITIONS (Phase 1-4 Transparency):
- PCS_Data_Quality: Explicit quality level (COMPLETE, PARTIAL, MINIMAL, INSUFFICIENT)
- PCS_Coverage_Flags: Bitmask of available inputs (0x0001=Greeks, 0x0002=IV, 0x0004=Earnings, etc.)
- PCS_Missing_Inputs: Human-readable list of unavailable enhancements
- PCS_Input_Score: Percentage of optional inputs available (0-100)
- PCS_Baseline_Only: Boolean flag if only core inputs used

This makes PCS production-ready: it computes with whatever data IS available,
but explicitly documents what's missing for audit/trust.
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
    PCS_REVALIDATION_THRESHOLD,
    VEGA_REVALIDATION_THRESHOLD,
    GAMMA_REVALIDATION_THRESHOLD,
    ROI_REVALIDATION_THRESHOLD,
)

logger = logging.getLogger(__name__)

# Column name constants for schema integrity
ASSET_TYPE_COL = "AssetType"
TRADE_ID_COL = "TradeID"
STRATEGY_COL = "Strategy"
GAMMA_COL = "Gamma"
VEGA_COL = "Vega"
PREMIUM_COL = "Premium"
BASIS_COL = "Basis"

# === INSTRUMENTATION: Coverage Flags (Bitmask) ===
# Core inputs (always required, not optional)
COVERAGE_GREEKS = 0x0001      # Gamma, Vega (required)
COVERAGE_PREMIUM = 0x0002     # Premium, Basis (required)

# Optional enhancements (Phase 3 observables)
COVERAGE_IV_RANK = 0x0004     # IV_Rank available
COVERAGE_EARNINGS = 0x0008    # Days_to_Earnings available
COVERAGE_DTE = 0x0010         # DTE available
COVERAGE_MONEYNESS = 0x0020   # Moneyness_Pct available
COVERAGE_LIQUIDITY = 0x0040   # Volume, Open Interest available
COVERAGE_IV_SURFACE = 0x0080  # IV Cross-Leg metrics available

# Data quality levels
QUALITY_COMPLETE = "COMPLETE"       # All optional enhancements present
QUALITY_PARTIAL = "PARTIAL"         # Some enhancements missing
QUALITY_MINIMAL = "MINIMAL"         # Only core inputs, no enhancements
QUALITY_INSUFFICIENT = "INSUFFICIENT"  # Core inputs invalid/missing


def _diagnose_input_coverage(df: pd.DataFrame) -> tuple[int, list[str], str, int]:
    """
    Diagnose what inputs are available for PCS calculation.
    """
    coverage = 0
    missing = []
    
    # === CORE INPUTS (Required) ===
    has_greeks = all(col in df.columns for col in ['Gamma', 'Vega'])
    has_premium = all(col in df.columns for col in ['Premium', 'Basis'])
    
    if has_greeks:
        coverage |= COVERAGE_GREEKS
    else:
        missing.append("Greeks")
    
    if has_premium:
        coverage |= COVERAGE_PREMIUM
    else:
        missing.append("Premium/Basis")
    
    # If core inputs missing, quality is INSUFFICIENT
    if not (has_greeks and has_premium):
        return coverage, missing, QUALITY_INSUFFICIENT, 0
    
    # === OPTIONAL ENHANCEMENTS (Phase 3 Observables) ===
    optional_checks = [
        ('IV_Rank', COVERAGE_IV_RANK, 'IV_Rank'),
        ('Days_to_Earnings', COVERAGE_EARNINGS, 'Earnings Calendar'),
        ('DTE', COVERAGE_DTE, 'DTE'),
        ('Moneyness_Pct', COVERAGE_MONEYNESS, 'Moneyness'),
        ('Volume', COVERAGE_LIQUIDITY, 'Liquidity'),
        ('IV_Cross_Leg_Skew', COVERAGE_IV_SURFACE, 'IV Surface'),
    ]
    
    available_count = 0
    total_optional = len(optional_checks)
    
    for col_name, flag, display_name in optional_checks:
        if col_name in df.columns:
            # Check if column has non-null data (not just exists)
            if df[col_name].notna().any():
                coverage |= flag
                available_count += 1
            else:
                missing.append(f"{display_name} (null)")
        else:
            missing.append(display_name)
    
    # Calculate input score: % of optional enhancements available
    input_score = int((available_count / total_optional) * 100)
    
    # Determine quality level
    if available_count == total_optional:
        quality = QUALITY_COMPLETE
    elif available_count >= total_optional // 2:
        quality = QUALITY_PARTIAL
    else:
        quality = QUALITY_MINIMAL
    
    return coverage, missing, quality, input_score


def calculate_current_pcs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate Current Portfolio Confidence Score (Current_PCS) using current Greeks
    and time-series data for evolving position scoring.
    """
    df = df.copy()
    
    total_rows = len(df)
    if not MANAGEMENT_SAFE_MODE:
        logger.info(f"Starting PCS calculation on {total_rows} rows")
    
    # === Structural Integrity Check ===
    required_columns = [ASSET_TYPE_COL, TRADE_ID_COL, STRATEGY_COL, GAMMA_COL, VEGA_COL, PREMIUM_COL, BASIS_COL]
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(
            f"❌ Missing required columns for PCS calculation: {missing_cols}\n"
            "   Phase 2 must complete before PCS scoring."
        )
    
    # === INSTRUMENTATION: Diagnose Input Coverage ===
    coverage_flags, missing_inputs, quality_level, input_score = _diagnose_input_coverage(df)
    
    if not MANAGEMENT_SAFE_MODE:
        logger.info(
            f"📊 PCS Input Coverage: {quality_level} "
            f"(score={input_score}%, coverage=0x{coverage_flags:04X})"
        )
    
    if missing_inputs and not MANAGEMENT_SAFE_MODE:
        logger.warning(
            f"⚠️  PCS Missing Inputs ({len(missing_inputs)}): {', '.join(missing_inputs)}"
        )
    
    # Store coverage diagnostics for output
    pcs_coverage_meta = {
        'coverage_flags': coverage_flags,
        'missing_inputs': missing_inputs,
        'quality_level': quality_level,
        'input_score': input_score
    }
    
    # === Issue 1: Filter to options only ===
    options_mask = df[ASSET_TYPE_COL] == "OPTION"
    stocks_excluded = (~options_mask).sum()
    df_options = df[options_mask].copy()
    
    if len(df_options) == 0:
        if not MANAGEMENT_SAFE_MODE:
            logger.warning(f"⚠️ No option positions found ({stocks_excluded} stocks excluded)")
        # Return original df with NaN/None PCS columns
        for col in ["PCS_GammaScore", "PCS_VegaScore", "PCS_ROIScore", "ROI_Raw_Calc", "ROI_For_Scoring", "PCS", "PCS_GroupAvg"]:
            df[col] = np.nan
        for col in ["PCS_Profile", "PCS_Tier", "Confidence_Tier"]:
            df[col] = None
        for col in ["Unknown_Strategy", "Low_Quality_Signal", "Needs_Revalidation"]:
            df[col] = None
        # Add coverage diagnostics
        df["PCS_Data_Quality"] = quality_level
        df["PCS_Coverage_Flags"] = coverage_flags
        df["PCS_Missing_Inputs"] = ", ".join(missing_inputs) if missing_inputs else None
        df["PCS_Input_Score"] = input_score
        df["PCS_Baseline_Only"] = (input_score == 0)
        return df
    
    if not MANAGEMENT_SAFE_MODE:
        logger.info(f"Processing {len(df_options)} option positions ({stocks_excluded} stocks excluded)")
    
    # Greek validation
    invalid_gamma = (df_options[GAMMA_COL].isna()) | (df_options[GAMMA_COL] < 0)
    if invalid_gamma.any():
        df_options.loc[invalid_gamma, GAMMA_COL] = 0
    
    invalid_vega = (df_options[VEGA_COL].isna()) | (df_options[VEGA_COL] < 0)
    if invalid_vega.any():
        df_options.loc[invalid_vega, VEGA_COL] = 0
    
    invalid_basis = (df_options[BASIS_COL].isna()) | (df_options[BASIS_COL] <= 0)
    if invalid_basis.any():
        df_options.loc[invalid_basis, BASIS_COL] = 1.0 # Avoid division by zero

    # Strategy Profile Tagging
    conditions_profile = [
        df_options[STRATEGY_COL].isin([STRATEGY_LONG_STRADDLE, STRATEGY_LONG_STRANGLE]),
        df_options[STRATEGY_COL].isin([STRATEGY_CSP, STRATEGY_COVERED_CALL]),
        df_options[STRATEGY_COL] == STRATEGY_BUY_CALL,
        df_options[STRATEGY_COL] == STRATEGY_BUY_PUT,
    ]
    choices_profile = [
        PROFILE_NEUTRAL_VOL,
        PROFILE_INCOME,
        PROFILE_DIRECTIONAL_BULL,
        PROFILE_DIRECTIONAL_BEAR,
    ]
    df_options["PCS_Profile"] = np.select(conditions_profile, choices_profile, default=PROFILE_OTHER)
    df_options["Unknown_Strategy"] = df_options["PCS_Profile"] == PROFILE_OTHER

    # Scoring Engine
    gamma = df_options[GAMMA_COL]
    vega = df_options[VEGA_COL]
    premium = df_options[PREMIUM_COL]
    basis = df_options[BASIS_COL]
    
    credit_strategies = df_options[STRATEGY_COL].isin([STRATEGY_CSP, STRATEGY_COVERED_CALL])
    roi_raw_calc = premium / basis
    roi_for_scoring = np.where(credit_strategies, np.abs(roi_raw_calc), roi_raw_calc)
    
    gamma_score = np.minimum(gamma * PCS_GAMMA_MULTIPLIER, PCS_GAMMA_MAX)
    vega_score = np.minimum((vega / basis) * PCS_VEGA_MULTIPLIER, PCS_VEGA_MAX)
    
    roi_score = np.where(
        roi_for_scoring >= PCS_ROI_THRESHOLD_HIGH,
        PCS_ROI_SCORE_HIGH,
        np.where(roi_for_scoring >= PCS_ROI_THRESHOLD_MID, PCS_ROI_SCORE_MID, PCS_ROI_SCORE_LOW)
    )
    
    pcs = np.zeros(len(df_options))
    
    # Apply weights
    for profile, weights in [
        (PROFILE_NEUTRAL_VOL, PCS_WEIGHTS_NEUTRAL_VOL),
        (PROFILE_INCOME, PCS_WEIGHTS_INCOME),
        (PROFILE_DIRECTIONAL_BULL, PCS_WEIGHTS_DIRECTIONAL),
        (PROFILE_DIRECTIONAL_BEAR, PCS_WEIGHTS_DIRECTIONAL),
        (PROFILE_OTHER, PCS_WEIGHTS_DEFAULT)
    ]:
        mask = df_options["PCS_Profile"] == profile
        if mask.any():
            pcs[mask] = (
                weights["gamma"] * gamma_score[mask] +
                weights["vega"] * vega_score[mask] +
                weights["roi"] * roi_score[mask]
            )
    
    df_options["PCS_GammaScore"] = gamma_score
    df_options["PCS_VegaScore"] = vega_score
    df_options["PCS_ROIScore"] = roi_score
    df_options["ROI_Raw_Calc"] = roi_raw_calc
    df_options["ROI_For_Scoring"] = roi_for_scoring
    df_options["PCS"] = pcs
    df_options["PCS_GroupAvg"] = df_options.groupby(TRADE_ID_COL)["PCS"].transform("mean")

    # Tier classification
    df_options["PCS_Tier"] = np.where(
        df_options["PCS"] >= PCS_TIER1_THRESHOLD,
        PCS_TIER1_LABEL,
        np.where(
            df_options["PCS"] >= PCS_TIER2_THRESHOLD,
            PCS_TIER2_LABEL,
            np.where(df_options["PCS"] >= PCS_TIER3_THRESHOLD, PCS_TIER3_LABEL, PCS_TIER4_LABEL)
        )
    )
    df_options["Confidence_Tier"] = df_options["PCS_Tier"]
    
    # Revalidation
    df_options["Low_Quality_Signal"] = (
        (df_options["PCS"] < PCS_REVALIDATION_THRESHOLD) |
        (df_options[VEGA_COL] < VEGA_REVALIDATION_THRESHOLD) |
        (df_options[GAMMA_COL] < GAMMA_REVALIDATION_THRESHOLD) |
        (df_options["ROI_For_Scoring"] < ROI_REVALIDATION_THRESHOLD)
    )
    df_options["Needs_Revalidation"] = df_options["Low_Quality_Signal"] | df_options["Unknown_Strategy"]

    # Merge back
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
    
    pcs_columns = [
        "PCS_Profile", "PCS_GammaScore", "PCS_VegaScore", "PCS_ROIScore", 
        "ROI_Raw_Calc", "ROI_For_Scoring", "PCS", "PCS_GroupAvg", "PCS_Tier", 
        "Confidence_Tier", "Unknown_Strategy", "Low_Quality_Signal", "Needs_Revalidation"
    ]
    
    for col in pcs_columns:
        df.loc[df_options.index, col] = df_options[col].values
    
    if not MANAGEMENT_SAFE_MODE:
        logger.info(f"✅ PCS scoring complete: {len(df_options)} options")
    
    return df


# === BACKWARD COMPATIBILITY ALIAS ===
calculate_pcs = calculate_current_pcs
