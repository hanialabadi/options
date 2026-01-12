"""
Step 10: PCS Recalibration and Pre-Filter

PURPOSE:
    Apply neutral, rules-based pre-filter to validate structural trade quality.
    Evaluates liquidity, risk parameters, strategy-specific thresholds, and Greek alignment.
    Filters out poor-risk setups before final execution approval.

DESIGN PRINCIPLE:
    - Neutral scoring (no directional bias)
    - Strategy-specific validation rules (via `calculate_pcs_score_v2`)
    - Greek-based validation (Delta/Vega alignment with strategy, integrated into `calculate_pcs_score_v2`)
    - Conservative risk filters (wide spreads, low liquidity, short DTE, integrated into `calculate_pcs_score_v2`)
    - Outputs Pre_Filter_Status: 'Valid', 'Watch', or 'Rejected'

HARD RULES (from Authoritative Contract):
    - No portfolio awareness.
    - No cross-strategy comparison.
    - Scoring is per-strategy only.

INPUTS (from Step 9B):
    - Ticker, Primary_Strategy, Trade_Bias
    - Actual_DTE, Selected_Strikes, Contract_Symbols
    - Actual_Risk_Per_Contract, Total_Debit, Total_Credit
    - Bid_Ask_Spread_Pct, Open_Interest, Liquidity_Score
    - Risk_Model, Contract_Intent, Structure_Simplified
    - Delta, Vega, Gamma (Greeks extracted from Contract_Symbols JSON)
    - Put_Call_Skew, Probability_Of_Profit (from Step 9B for vol/income strategies)

OUTPUTS (added columns):
    - Pre_Filter_Status: 'Valid', 'Watch', 'Rejected'
    - Filter_Reason: Explanation if Watch/Rejected
    - PCS_Score: 0-100 quality score (from `calculate_pcs_score_v2`)
    - Execution_Ready: True/False (Valid + Contract_Intent promoted)
"""

import pandas as pd
import numpy as np
import logging
import sys
from pathlib import Path
from typing import Dict, Tuple

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from utils.greek_extraction import extract_greeks_to_columns, validate_greek_extraction
from utils.pcs_scoring_v2 import calculate_pcs_score_v2, analyze_pcs_distribution

logger = logging.getLogger(__name__)


def recalibrate_and_filter(
    df: pd.DataFrame,
    min_liquidity_score: float = 30.0,
    max_spread_pct: float = 8.0,
    min_dte: int = 5,
    strict_mode: bool = False
) -> pd.DataFrame:
    """
    Apply PCS recalibration and pre-filter to Step 9B contracts.
    
    Validates structural trade quality using neutral, rules-based scoring.
    Filters out poor-risk setups (wide spreads, low liquidity, weak parameters).
    
    Args:
        df (pd.DataFrame): Step 9B output with contract selections
        min_liquidity_score (float): Minimum acceptable liquidity score. Default 30.
        max_spread_pct (float): Maximum acceptable bid-ask spread %. Default 8%.
        min_dte (int): Minimum DTE for any strategy. Default 5.
        strict_mode (bool): If True, apply stricter thresholds. Default False.
    
    Returns:
        pd.DataFrame: Original df with Pre_Filter_Status, Filter_Reason, PCS_Score, Execution_Ready
    
    Side Effects:
        - Logs summary of filter results
        - Marks simplified structures as Watch
        - Promotes valid contracts to Execution_Candidate
    
    Example:
        >>> df_contracts = fetch_and_select_contracts(df_timeframed)
        >>> df_filtered = recalibrate_and_filter(
        ...     df_contracts,
        ...     min_liquidity_score=40.0,
        ...     max_spread_pct=6.0,
        ...     strict_mode=True
        ... )
        >>> valid_trades = df_filtered[df_filtered['Pre_Filter_Status'] == 'Valid']
    """
    
    # Validate input
    required_cols = [
        'Ticker', 'Primary_Strategy', 'Actual_DTE', 'Bid_Ask_Spread_Pct',
        'Open_Interest', 'Liquidity_Score', 'Contract_Selection_Status'
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns from Step 9B: {missing}")
    
    # Add Risk_Model if missing (backward compatibility)
    if 'Risk_Model' not in df.columns:
        df['Risk_Model'] = 'Unknown'
        logger.info("⚠️ Risk_Model column missing - added default 'Unknown' values")
    
    if df.empty:
        logger.warning("⚠️ Empty DataFrame passed to Step 10")
        return df
    
    logger.info(f"🔍 Step 10: PCS Recalibration for {len(df)} contracts")
    
    # Adjust thresholds for strict mode
    if strict_mode:
        min_liquidity_score = min(min_liquidity_score * 1.5, 100.0)
        max_spread_pct = max_spread_pct * 0.7
        min_dte = min_dte + 2
        logger.info(f"⚡ Strict mode enabled: liquidity≥{min_liquidity_score:.1f}, spread≤{max_spread_pct:.1f}%, DTE≥{min_dte}")
    
    df = df.copy()
    
    # ========================================
    # PHASE 1: EXTRACT GREEKS FROM JSON
    # ========================================
    logger.info("📊 Phase 1: Extracting Greeks from Contract_Symbols JSON...")
    try:
        df = extract_greeks_to_columns(df)
        validation = validate_greek_extraction(df)
        logger.info(f"   ✅ Greek extraction complete")
        logger.info(f"      Coverage: {validation['delta_coverage']}")
        logger.info(f"      Quality: {validation['quality']}")
    except Exception as e:
        logger.warning(f"   ⚠️  Greek extraction failed: {e}")
        logger.warning(f"      Continuing without Greeks (reduced PCS accuracy)")
    
    # ========================================
    # PHASE 2: CALCULATE PCS SCORES V2
    # ========================================
    logger.info("📈 Phase 2: Calculating enhanced PCS scores...")
    try:
        # FIX 3: Disable PCS pre-maturity (Early Exit)
        # PCS is illegal without mature IV.
        if 'IV_Maturity_State' in df.columns:
            immature_mask = df['IV_Maturity_State'] != 'MATURE'
            if immature_mask.all():
                logger.info("⚠️ Skipping PCS execution: All rows have IMMATURE IV")
                df['PCS_Status'] = 'INACTIVE'
                df['PCS_Score_V2'] = np.nan
                df['PCS_Score'] = np.nan
                df['Filter_Reason'] = 'IV_NOT_MATURE'
                return _finalize_step10(df)
            elif immature_mask.any():
                logger.info(f"⚠️ Partial PCS execution: Skipping {immature_mask.sum()} immature IV rows")
                # We will process the whole DF but overwrite immature rows after
        
        df = calculate_pcs_score_v2(df)
        
        if 'IV_Maturity_State' in df.columns:
            immature_mask = df['IV_Maturity_State'] != 'MATURE'
            if immature_mask.any():
                df.loc[immature_mask, 'PCS_Status'] = 'INACTIVE'
                df.loc[immature_mask, 'PCS_Score_V2'] = np.nan
                df.loc[immature_mask, 'PCS_Score'] = np.nan
                df.loc[immature_mask, 'Filter_Reason'] = 'IV_NOT_MATURE'
        
        analysis = analyze_pcs_distribution(df)
        logger.info(f"   ✅ PCS scoring complete")
        logger.info(f"      Mean score: {analysis.get('mean_score', 0):.1f}")
        logger.info(f"      Distribution: {analysis.get('valid_pct', '0%')} Valid, {analysis.get('watch_pct', '0%')} Watch, {analysis.get('rejected_pct', '0%')} Rejected")
    except Exception as e:
        logger.warning(f"   ⚠️  PCS V2 scoring failed: {e}")
        logger.warning(f"      Falling back to legacy scoring...")
        # Fall back to old method
        df['Pre_Filter_Status'] = 'Pending'
        df['Filter_Reason'] = ''
        df['PCS_Score'] = 0.0
    
    # Map PCS_Status to Pre_Filter_Status for compatibility
    if 'PCS_Status' in df.columns:
        df['Pre_Filter_Status'] = df['PCS_Status']
        # Keep both PCS_Score_V2 and legacy PCS_Score
        if 'PCS_Score_V2' in df.columns:
            df['PCS_Score'] = df['PCS_Score_V2']
    else:
        # Legacy mode
        df['Pre_Filter_Status'] = 'Pending'
        df['Filter_Reason'] = ''
        df['PCS_Score'] = 0.0
    
    df['Execution_Ready'] = False
    
    # ========================================
    # PHASE 3: LEGACY VALIDATION (IF NEEDED)
    # ========================================
    # Only apply legacy validation if PCS V2 failed
    if 'PCS_Status' not in df.columns:
        logger.info("⚙️  Phase 3: Applying legacy validation rules...")
        for idx, row in df.iterrows():
            # Skip failed contract selections
            if row['Contract_Selection_Status'] != 'Success':
                df.at[idx, 'Pre_Filter_Status'] = 'Rejected'
                df.at[idx, 'Filter_Reason'] = f"Contract selection failed: {row['Contract_Selection_Status']}"
                continue
            
            # Apply validation rules
            status, reason, score = _apply_validation_rules(
                row,
                min_liquidity_score=min_liquidity_score,
                max_spread_pct=max_spread_pct,
                min_dte=min_dte
            )
            
            df.at[idx, 'Pre_Filter_Status'] = status
            df.at[idx, 'Filter_Reason'] = reason
            df.at[idx, 'PCS_Score'] = score
    
    # ========================================
    # PHASE 4: PROMOTE TO EXECUTION
    # ========================================
    logger.info("🚀 Phase 4: Promoting valid contracts to execution...")
    for idx, row in df.iterrows():
        status = row.get('Pre_Filter_Status')
        # Promote valid contracts to execution candidate
        if status == 'Valid' and row.get('Contract_Intent') == 'Scan':
            df.at[idx, 'Contract_Intent'] = 'Execution_Candidate'
            df.at[idx, 'Execution_Ready'] = True
    
    # Log summary
    _log_filter_summary(df)
    
    return df


def _finalize_step10(df: pd.DataFrame) -> pd.DataFrame:
    """Helper to finalize Step 10 columns and promotion logic."""
    if 'PCS_Status' in df.columns:
        df['Pre_Filter_Status'] = df['PCS_Status']
        if 'PCS_Score_V2' in df.columns:
            df['PCS_Score'] = df['PCS_Score_V2']
    
    df['Execution_Ready'] = False
    for idx, row in df.iterrows():
        status = row.get('Pre_Filter_Status')
        if status == 'Valid' and row.get('Contract_Intent') == 'Scan':
            df.at[idx, 'Contract_Intent'] = 'Execution_Candidate'
            df.at[idx, 'Execution_Ready'] = True
            
    _log_filter_summary(df)
    return df


# Removed legacy _apply_validation_rules, _validate_greek_alignment, _validate_strategy_specific functions.
# These are replaced by calculate_pcs_score_v2.


def _log_filter_summary(df: pd.DataFrame):
    """Log summary of PCS filter results."""
    
    status_counts = df['Pre_Filter_Status'].value_counts().to_dict()
    total = len(df)
    
    valid_count = status_counts.get('Valid', 0)
    watch_count = status_counts.get('Watch', 0)
    rejected_count = status_counts.get('Rejected', 0)
    
    logger.info(f"\n📊 Step 10 PCS Filter Summary:")
    logger.info(f"   ✅ Valid: {valid_count}/{total} ({valid_count/total*100:.1f}%)")
    logger.info(f"   ⚠️  Watch: {watch_count}/{total} ({watch_count/total*100:.1f}%)")
    logger.info(f"   ❌ Rejected: {rejected_count}/{total} ({rejected_count/total*100:.1f}%)")
    
    # Log average PCS score by status
    if valid_count > 0:
        avg_valid_score = df[df['Pre_Filter_Status'] == 'Valid']['PCS_Score'].mean()
        logger.info(f"   Avg Valid PCS Score: {avg_valid_score:.1f}")
    
    # Log top rejection reasons
    if rejected_count > 0:
        rejection_reasons = df[df['Pre_Filter_Status'] == 'Rejected']['Filter_Reason'].value_counts().head(3)
        logger.info(f"\n   Top Rejection Reasons:")
        for reason, count in rejection_reasons.items():
            logger.info(f"     • {reason}: {count}")
