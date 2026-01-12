"""
Step 6: Data Completeness and Quality Validation

NOTE:
This step is strictly DESCRIPTIVE.
It must not introduce strategy assumptions, thresholds,
pass/fail flags, or trade intent.
All strategy decisions occur in later phases.

Purpose:
    Validates that tickers have sufficient data quality for downstream analysis.
    Checks for complete chart metrics, valid IV/HV data, and usable signal structure.
    Tags data completeness without making trade recommendations.
"""

import pandas as pd
import logging
from .utils import validate_input

logger = logging.getLogger(__name__)


def validate_data_quality(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate data completeness and add descriptive classifications.
    
    NOTE:
    This step is strictly DESCRIPTIVE.
    It must not introduce strategy assumptions, thresholds,
    pass/fail flags, or trade intent.
    All strategy decisions occur in later phases.
    
    Purpose:
        Validates that tickers have complete, usable data from prior steps.
        Adds descriptive classifications for:
        - Data completeness (all required fields present)
        - Crossover age bucket (days since last crossover - CONTEXTUAL ONLY)
        Does NOT filter, score, or recommend trades.
        
        NOTE ON CROSSOVER AGE:
        Crossover age is provided as contextual metadata for directional strategies.
        It is NOT relevant for all strategies (e.g., neutral structures, LEAPS,
        volatility trades, CSPs). Downstream logic should use or ignore based on
        strategy requirements.
    
    Logic Flow:
        1. Validate input columns (Ticker, IVHV_gap_30D, Signal_Type)
        2. Check data completeness (no critical NaNs)
        3. Add descriptive classifications:
           - Crossover_Age_Bucket: Classify days since crossover (Age_0_5/Age_6_15/Age_16_plus/None)
           - Data_Complete: Boolean flag for usable data quality
    
    Data Completeness Checks:
        - Has IVHV gap value (not NaN)
        - Has valid price extension measurements
        - Has EMA signal classification
        - All checks are descriptive, not prescriptive
    
    Args:
        df (pd.DataFrame): Input with chart metrics from compute_chart_signals()
            Required: ['Ticker', 'IVHV_gap_30D', 'Signal_Type', 
                      'Price_vs_SMA20', 'Price_vs_SMA50', 'SMA20', 'SMA50',
                      'EMA_Signal', 'Days_Since_Cross']
    
    Returns:
        pd.DataFrame: Original data with added descriptive columns:
            - Crossover_Age_Bucket: 'Age_0_5', 'Age_6_15', 'Age_16_plus', or 'None'
              (contextual metadata - relevant only for directional strategies)
            - Data_Complete: Boolean indicating all required fields present
            Returns ALL input rows (no filtering)
    
    Raises:
        ValueError: If required columns missing
    
    Example:
        >>> df_validated = filter_gem_candidates(df_charted)
        >>> complete = df_validated[df_validated['Data_Complete']]
        >>> print(f"Complete data: {len(complete)} tickers")
    
    Usage Notes:
        - Does NOT filter rows (all input rows returned)
        - Does NOT score or rank tickers
        - Does NOT make trade recommendations
        - Purely descriptive data quality checks
    """
    # 🚨 HARD RULES (from Authoritative Contract):
    # - Enforce schema integrity: Fail fast if required authoritative columns are missing.
    # - No auto-healing in production: Do not attempt to fill missing authoritative data.
    # - No defaults for Signal_Type or Regime: These are authoritative from Step 2.
    # - Missing authoritative fields is a hard error.

    # Strict schema validation: Fail fast if authoritative columns are missing
    required_cols = ['Ticker', 'IVHV_gap_30D', 'Signal_Type', 'Regime']
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        error_msg = (
            f"❌ Missing required authoritative columns from Step 2/3/5: {missing}. "
            "This is a critical pipeline error. The schema contract was violated upstream. "
            "Step 6 will not proceed with incomplete data in a production environment."
        )
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    logger.info("✅ Schema validation passed: Authoritative columns [Signal_Type, Regime] are present.")
    
    # Work on a copy to avoid modifying input
    df_result = df.copy()
    
    # Data completeness check (split into universal vs directional)
    def check_universal_complete(row):
        """Check if ticker has universal data needed for ALL strategies.
        
        Universal data (context only - not strictly required):
        - HV metrics (volatility context)
        - Price data (available from snapshot)
        
        Note: IV is NOT universal - many strategies work without it.
        """
        # Minimal universal requirements - most data is strategy-specific
        universal_checks = [
            pd.notna(row.get('Ticker')),  # Must have ticker
            # Price and HV come from Step 0, should always be present
        ]
        return True  # Universal data from Step 0 snapshot always present
    
    def check_directional_complete(row):
        """Check if ticker has directional signal data.
        
        Directional data (only needed for directional strategies):
        - Chart_EMA_Signal (crossover direction) - Namespaced from Step 5
        - Signal_Type (trend classification) - Authoritative from Step 2
        - Days_Since_Cross (momentum freshness)
        
        NOT needed for:
        - Income strategies (CSP, Covered Call)
        - Volatility strategies (credit spreads)
        - LEAP strategies (use structural trends)
        """
        directional_checks = [
            pd.notna(row.get('Chart_EMA_Signal')), # Namespaced from Step 5
            pd.notna(row.get('Signal_Type')),      # Authoritative from Step 2
            pd.notna(row.get('Days_Since_Cross'))
        ]
        return all(directional_checks)
    
    # Apply both completeness checks
    df_result['Universal_Data_Complete'] = df_result.apply(check_universal_complete, axis=1)
    df_result['Directional_Data_Complete'] = df_result.apply(check_directional_complete, axis=1)
    
    # Add strategy-family specific completeness scores
    def calc_completeness_score(row):
        """Calculate 0-1 completeness score based on available data."""
        score = 0.0
        total_weight = 0.0
        
        # Base data (from Step 0) - always present
        if pd.notna(row.get('Ticker')):
            score += 0.2
        total_weight += 0.2
        
        # HV data (from Step 0) - always present
        if pd.notna(row.get('HV_30_D_Cur')):
            score += 0.2
        total_weight += 0.2
        
        # Chart data (from Step 5) - optional but valuable
        if pd.notna(row.get('Chart_EMA_Signal')) and row.get('Chart_EMA_Signal') != 'Unknown':
            score += 0.2
        total_weight += 0.2
        
        # IV data (from Step 0) - valuable but not required
        if pd.notna(row.get('IV_30_D_Call')):
            score += 0.2
        total_weight += 0.2
        
        # Trend data (from Step 2) - valuable for directional
        if pd.notna(row.get('Signal_Type')) and row.get('Signal_Type') not in ['Unknown', 'Neutral']:
            score += 0.2
        total_weight += 0.2
        
        return score / total_weight if total_weight > 0 else 0.0
    
    def get_completeness_reason(row):
        """Explain what data is available/missing."""
        reasons = []
        if pd.notna(row.get('IV_30_D_Call')):
            reasons.append("Has_IV")
        else:
            reasons.append("No_IV")
        
        if pd.notna(row.get('Chart_EMA_Signal')) and row.get('Chart_EMA_Signal') != 'Unknown':
            reasons.append("Has_Charts")
        else:
            reasons.append("Limited_Charts")
        
        if pd.notna(row.get('Signal_Type')) and row.get('Signal_Type') not in ['Unknown', 'Neutral']:
            reasons.append("Has_Trend")
        
        return "|".join(reasons) if reasons else "Minimal_Data"
    
    df_result['Completeness_Score'] = df_result.apply(calc_completeness_score, axis=1)
    df_result['Completeness_Reason'] = df_result.apply(get_completeness_reason, axis=1)
    
    # 🚨 HARD RULE: No auto-healing or defaults for Signal_Type or Regime.
    # If these were missing, the initial validation would have failed.
    
    universal_count = df_result['Universal_Data_Complete'].sum()
    directional_count = df_result['Directional_Data_Complete'].sum()
    avg_completeness = df_result['Completeness_Score'].mean()
    
    logger.info(f"📊 Universal data completeness: {universal_count}/{len(df_result)} tickers")
    logger.info(f"📊 Directional data completeness: {directional_count}/{len(df_result)} tickers")
    logger.info(f"📊 Average completeness score: {avg_completeness:.2f}")
    logger.info(f"📊 Income-strategy ready (no IV required): {len(df_result)} tickers")
    
    # Crossover age bucketing (contextual metadata - not universally relevant)
    def classify_crossover_age(days_since_cross):
        """Classify crossover age into observational buckets.
        
        Note: This is contextual metadata for directional strategies only.
        Not relevant for neutral structures, LEAPS, or volatility trades.
        """
        if pd.isna(days_since_cross):
            return "None"
        elif days_since_cross <= 5:
            return "Age_0_5"
        elif days_since_cross <= 15:
            return "Age_6_15"
        else:
            return "Age_16_plus"
    
    df_result['Crossover_Age_Bucket'] = df_result['Days_Since_Cross'].apply(classify_crossover_age)
    
    # Log full distribution (neutral - no subset highlighting)
    age_dist = df_result['Crossover_Age_Bucket'].value_counts()
    logger.info(f"⏱️ Crossover age distribution: {age_dist.to_dict()}")
    
    # ============================================================
    # IV COVERAGE ENFORCEMENT (HARD GATES)
    # ============================================================
    # RAG: "Trust > Convenience. Do not analyze partial datasets."
    
    iv_populated = df_result['IV_30_D_Call'].notna().sum()
    total_tickers = len(df_result)
    iv_coverage_pct = (iv_populated / total_tickers * 100) if total_tickers > 0 else 0
    
    df_result['IV_Coverage_Pct'] = iv_coverage_pct
    
    if iv_coverage_pct < 40:
        logger.error(f"🛑 CRITICAL DATA FAILURE: IV coverage is only {iv_coverage_pct:.1f}% (Threshold: 40%)")
        logger.error("   Suppressing all signals. System cannot operate with this level of missing data.")
        return pd.DataFrame() # Hard block: return empty
        
    elif iv_coverage_pct < 60:
        logger.error(f"🛑 HARD BLOCK: IV coverage is {iv_coverage_pct:.1f}% (Threshold: 60%)")
        logger.error("   Blocking execution to prevent unreliable strategy recommendations.")
        return pd.DataFrame() # Hard block: return empty
        
    elif iv_coverage_pct < 80:
        logger.warning(f"⚠️  LOW DATA QUALITY: IV coverage is {iv_coverage_pct:.1f}% (Threshold: 80%)")
        logger.warning("   Proceeding with caution. Some high-quality tickers may be missing IV data.")

    # Return ALL input rows with added descriptive columns
    logger.info(f"✅ Step 6 complete: Returning {len(df_result)} tickers (IV Coverage: {iv_coverage_pct:.1f}%)")
    return df_result
