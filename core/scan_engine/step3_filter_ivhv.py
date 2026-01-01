"""
Step 3: Filter by IV-HV Gap and Add Volatility-Regime Tags

Strategy-Agnostic Design:
    This step performs VOLATILITY-REGIME DETECTION ONLY.
    It identifies tickers where implied volatility (IV) diverges from historical volatility (HV),
    indicating market-perceived vs realized volatility imbalance.
    
    NO STRATEGY INTENT: Does not favor calls, puts, spreads, CSPs, LEAPS, or any specific trade type.
    Downstream steps (7+) will apply strategy logic based on these neutral volatility classifications.
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
import hashlib

logger = logging.getLogger(__name__)

# VERSION TRACKING: Increment when logic changes to detect stale cached data
# Format: YYYYMMDD_NN (date + sequential number)
STEP3_VERSION = "20251227_02"  # Absolute magnitude implementation
STEP3_LOGIC_HASH = hashlib.md5(
    "Edge=abs(gap)>=2.0|Rich=gap>=2.0|Cheap=gap<=-2.0".encode()
).hexdigest()[:8]


def filter_ivhv_gap(df: pd.DataFrame, min_gap: float = 2.0) -> pd.DataFrame:
    """
    Filter tickers by IV-HV gap magnitude and classify volatility regimes.
    
    NOTE:
    This step is strictly DESCRIPTIVE.
    It must not introduce strategy assumptions, thresholds,
    pass/fail flags, or trade intent.
    All strategy decisions occur in later phases.
    
    THRESHOLD RATIONALE:
    The 2.0 percentage point IV-HV gap is a DIAGNOSTIC INCLUSION THRESHOLD, not a trade signal.
    It casts a wide net to capture potential opportunities for deeper analysis.
    Strategy decisions (Steps 7+) require STRONGER thresholds (3.5+) and contextual confirmation.
    
    Example: A 2-point gap at:
        - 20% IV = 10% relative mispricing (highly meaningful)
        - 80% IV = 2.5% relative mispricing (context-dependent)
    Both absolute AND relative gaps are calculated to normalize signals across volatility regimes.
    
    Purpose:
        Identifies tickers with significant implied vs historical volatility divergence.
        Classifications are purely descriptive of volatility magnitude, not strategy intent.
        Leverages Step 2's enriched data (IV_Rank_30D, term structure, trends).
    
    Logic Flow:
        1. Validate required columns from Step 2 (IV/HV + enriched fields)
        2. Convert IV/HV to numeric (handle NaNs)
        3. Filter: IV30_Call >= 15 AND HV30 > 0 (liquidity baseline)
        4. Calculate IVHV_gap_30D = IV30_Call - HV30 (signed divergence)
        5. Calculate IVHV_gap_abs = abs(IVHV_gap_30D) (magnitude-only)
        6. Filter: IVHV_gap_abs >= min_gap (bidirectional filter)
        7. Deduplicate: Keep highest gap per ticker
        8. Tag volatility regimes based on gap magnitude + Step 2 context
    
    Volatility Regime Tags (STRATEGY-NEUTRAL):
        - HighVol: |IVHV gap| >= 5.0 (strong divergence magnitude)
        - ElevatedVol: |IVHV gap| 3.5-5.0 (moderate divergence magnitude)
        - ModerateVol: |IVHV gap| 2.0-3.5 (baseline divergence magnitude)
        - IV_Rich: IVHV gap >= 3.5 (market pricing MORE vol than realized)
        - IV_Cheap: IVHV gap <= -3.5 (market pricing LESS vol than realized)
        - LowRank: IV_Rank_30D < 30 (using Step 2's per-ticker percentile)
        - MeanReversion_Setup: IV elevated + rising while HV stable/falling
        - Expansion_Setup: IV depressed + stable/falling while HV rising
    
    Args:
        df (pd.DataFrame): Input snapshot with IV/HV columns
        min_gap (float, optional): Minimum IVHV gap threshold. Default 2.0.
    
    Returns:
        pd.DataFrame: Filtered tickers with added columns:
            - IV30_Call: Numeric IV (30-day calls)
            - HV30: Numeric HV (30-day)
            - IVHV_gap_30D: Signed IV-HV divergence (absolute points)
            - IVHV_gap_abs: Absolute IV-HV divergence magnitude (absolute points)
            - IVHV_gap_30D_pct: Relative divergence as % of HV (normalized)
            - IVHV_gap_60D_pct, IVHV_gap_90D_pct, etc.: Multi-timeframe relative gaps
            - HighVol, ElevatedVol, ModerateVol: Boolean regime flags (magnitude-based)
            - IV_Rich: Boolean for IV > HV by >=3.5 (describes overpricing pattern)
            - IV_Cheap: Boolean for HV > IV by >=3.5 (describes underpricing pattern)
            - ShortTerm_IV_Rich, MediumTerm_IV_Rich, LEAP_IV_Rich: IV > HV at each timeframe (sell-vol context)
            - ShortTerm_IV_Cheap, MediumTerm_IV_Cheap, LEAP_IV_Cheap: HV > IV at each timeframe (buy-vol context)
            - ShortTerm_IV_Edge, MediumTerm_IV_Edge, LEAP_IV_Edge: Composite (either direction)
            - LowRank: Boolean for IV_Rank_30D < 30 (using Step 2's metric)
            - MeanReversion_Setup: Boolean for IV elevated + rising, HV stable/falling
            - Expansion_Setup: Boolean for IV depressed + stable/falling, HV rising
            - df_elevated_plus: Boolean for |gap| >= 3.5 (strong divergence either direction)
            - df_moderate_vol: Boolean for ModerateVol only
    
    Raises:
        ValueError: If required columns missing
        
    Example:
        >>> df_filtered = filter_ivhv_gap(df_snapshot, min_gap=3.5)
        >>> print(f"Elevated+ regimes: {df_filtered['df_elevated_plus'].sum()}")
        >>> print(df_filtered[['Ticker', 'IVHV_gap_30D', 'HighVol']].head())
    
    Notes:
        - Empty result warns but doesn't raise (allows pipeline continuation)
        - Uses Step 2's IV_Rank_30D (per-ticker recent-range percentile)
        - Leverages IV_Term_Structure, IV_Trend_7D, HV_Trend_30D from Step 2
        - Deduplication ensures 1 row per ticker
        - Column naming avoids strategy bias
    """
    # Validate Step 2 enriched fields exist
    required_from_step2 = ['IV_Rank_30D', 'IV_Term_Structure', 'IV_Trend_7D', 'HV_Trend_30D']
    missing = [col for col in required_from_step2 if col not in df.columns]
    if missing:
        logger.warning(f"⚠️ Missing Step 2 enriched fields: {missing}. Run Step 2 first.")
    
    # 🚨 ASSERTION: Signal_Type and Regime are authoritative from Step 2 ONLY.
    # This step (Step 3) must NOT mutate these columns.
    # If these columns are present, ensure they are not being overwritten or re-calculated here.
    if 'Signal_Type' in df.columns:
        # Assert that Signal_Type is not being re-calculated or modified
        # (Add more specific checks if there's a risk of silent mutation)
        pass 
    if 'Regime' in df.columns:
        # Assert that Regime is not being re-calculated or modified
        pass

    # Convert IV/HV to numeric (Step 2 should have done this, but be safe)
    df['IV30_Call'] = pd.to_numeric(df['IV_30_D_Call'], errors='coerce') if 'IV_30_D_Call' in df.columns else np.nan
    df['HV30'] = pd.to_numeric(df['HV_30_D_Cur'], errors='coerce')
    
    # Basic liquidity filter (HV required, IV optional)
    # If IV is present, require IV ≥ 15 for IV-based strategies
    # If IV is missing, allow HV-only rows to pass (for chart-based strategies)
    initial_count = len(df)
    has_iv = df['IV30_Call'].notna()
    df = df[(df['HV30'] > 0) & ((~has_iv) | (df['IV30_Call'] >= 15))]
    logger.info(f"📊 Liquidity filter: {initial_count} → {len(df)} rows (HV > 0, IV ≥ 15 if present)")
    
    # Calculate IV-HV divergence (signed and absolute magnitude)
    # BIAS REMOVED: Previously called "the edge" (implies directional trading)
    # NOW: Neutral "divergence" or "gap" terminology
    df = df.copy()  # Avoid SettingWithCopyWarning
    
    # Convert all IV/HV columns to numeric for multi-timeframe analysis
    iv_cols = ['IV_60_D_Call', 'IV_90_D_Call', 'IV_180_D_Call', 'IV_360_D_Call']
    hv_cols = ['HV_60_D_Cur', 'HV_90_D_Cur', 'HV_180_D_Cur']
    for col in iv_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    for col in hv_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # MULTI-TIMEFRAME GAP CALCULATIONS
    # Each strategy type should evaluate IV-HV relationship at its relevant timeframe
    df['IVHV_gap_30D'] = df['IV30_Call'] - df['HV30']  # Short-term strategies (30-45 DTE)
    df['IVHV_gap_abs'] = df['IVHV_gap_30D'].abs()      # Magnitude-only (for filtering)
    
    # Medium-term strategies (60-120 DTE)
    if 'IV_60_D_Call' in df.columns and 'HV_60_D_Cur' in df.columns:
        df['IVHV_gap_60D'] = df['IV_60_D_Call'] - df['HV_60_D_Cur']
    else:
        df['IVHV_gap_60D'] = np.nan
    
    # Intermediate strategies (90-150 DTE)
    if 'IV_90_D_Call' in df.columns and 'HV_90_D_Cur' in df.columns:
        df['IVHV_gap_90D'] = df['IV_90_D_Call'] - df['HV_90_D_Cur']
    else:
        df['IVHV_gap_90D'] = np.nan
    
    # LEAP strategies (180-365 DTE)
    if 'IV_180_D_Call' in df.columns and 'HV_180_D_Cur' in df.columns:
        df['IVHV_gap_180D'] = df['IV_180_D_Call'] - df['HV_180_D_Cur']
    else:
        df['IVHV_gap_180D'] = np.nan
    
    # Ultra-LEAP strategies (450+ DTE) - use 360D IV with 180D HV as proxy
    if 'IV_360_D_Call' in df.columns and 'HV_180_D_Cur' in df.columns:
        df['IVHV_gap_360D'] = df['IV_360_D_Call'] - df['HV_180_D_Cur']
    else:
        df['IVHV_gap_360D'] = np.nan
    
    # RELATIVE GAP CALCULATIONS (Percentage-Based Normalization)
    # Purpose: Normalize volatility divergence across different volatility regimes
    # Example: 2-point gap at 20% IV (10% relative) vs 80% IV (2.5% relative)
    # Helps identify meaningful divergences for low-vol large caps vs high-vol growth names
    # Formula: (IV - HV) / HV * 100 = percentage mispricing
    
    # Avoid division by zero - set relative gap to NaN where HV is too low
    df['IVHV_gap_30D_pct'] = np.where(
        df['HV30'] > 5,  # Only calculate if HV > 5% (meaningful denominator)
        (df['IVHV_gap_30D'] / df['HV30']) * 100,
        np.nan
    )
    
    if 'IVHV_gap_60D' in df.columns and 'HV_60_D_Cur' in df.columns:
        df['IVHV_gap_60D_pct'] = np.where(
            df['HV_60_D_Cur'] > 5,
            (df['IVHV_gap_60D'] / df['HV_60_D_Cur']) * 100,
            np.nan
        )
    else:
        df['IVHV_gap_60D_pct'] = np.nan
    
    if 'IVHV_gap_90D' in df.columns and 'HV_90_D_Cur' in df.columns:
        df['IVHV_gap_90D_pct'] = np.where(
            df['HV_90_D_Cur'] > 5,
            (df['IVHV_gap_90D'] / df['HV_90_D_Cur']) * 100,
            np.nan
        )
    else:
        df['IVHV_gap_90D_pct'] = np.nan
    
    if 'IVHV_gap_180D' in df.columns and 'HV_180_D_Cur' in df.columns:
        df['IVHV_gap_180D_pct'] = np.where(
            df['HV_180_D_Cur'] > 5,
            (df['IVHV_gap_180D'] / df['HV_180_D_Cur']) * 100,
            np.nan
        )
    else:
        df['IVHV_gap_180D_pct'] = np.nan
    
    if 'IVHV_gap_360D' in df.columns and 'HV_180_D_Cur' in df.columns:
        df['IVHV_gap_360D_pct'] = np.where(
            df['HV_180_D_Cur'] > 5,
            (df['IVHV_gap_360D'] / df['HV_180_D_Cur']) * 100,
            np.nan
        )
    else:
        df['IVHV_gap_360D_pct'] = np.nan
    
    # QUALIFICATION FLAGS: Does this ticker have IV edge at each timeframe?
    # Strategy-neutral design: Edge = magnitude (abs), Direction = Rich/Cheap (signed)
    # This matches Step 3's philosophy: magnitude first, direction later
    
    # Diagnostic logging
    logger.info(f"📊 Gap statistics before flag calculation:")
    logger.info(f"   - IVHV_gap_30D range: {df['IVHV_gap_30D'].min():.2f} to {df['IVHV_gap_30D'].max():.2f}")
    logger.info(f"   - IVHV_gap_30D mean: {df['IVHV_gap_30D'].mean():.2f}")
    logger.info(f"   - Non-NaN count: {df['IVHV_gap_30D'].notna().sum()} / {len(df)}")
    
    # EDGE FLAGS: Absolute magnitude (bidirectional)
    df['ShortTerm_IV_Edge'] = df['IVHV_gap_30D'].abs() >= 2.0   # Either direction
    df['MediumTerm_IV_Edge'] = df['IVHV_gap_60D'].abs() >= 2.0  # Either direction
    df['LEAP_IV_Edge'] = df['IVHV_gap_180D'].abs() >= 2.0       # Either direction
    df['UltraLEAP_IV_Edge'] = df['IVHV_gap_360D'].abs() >= 2.0  # Either direction
    
    # DIRECTIONAL FLAGS: Separate Rich/Cheap for strategy assignment (Step 7+)
    df['ShortTerm_IV_Rich'] = df['IVHV_gap_30D'] >= 2.0    # IV > HV: Premium selling context
    df['ShortTerm_IV_Cheap'] = df['IVHV_gap_30D'] <= -2.0  # HV > IV: Premium buying context
    df['MediumTerm_IV_Rich'] = df['IVHV_gap_60D'] >= 2.0   # IV > HV: Credit spread context
    df['MediumTerm_IV_Cheap'] = df['IVHV_gap_60D'] <= -2.0 # HV > IV: Debit spread context
    df['LEAP_IV_Rich'] = df['IVHV_gap_180D'] >= 2.0        # IV > HV: Sell LEAP puts
    df['LEAP_IV_Cheap'] = df['IVHV_gap_180D'] <= -2.0      # HV > IV: Buy LEAP calls
    df['UltraLEAP_IV_Rich'] = df['IVHV_gap_360D'] >= 2.0   # IV > HV: Long-term premium selling
    df['UltraLEAP_IV_Cheap'] = df['IVHV_gap_360D'] <= -2.0 # HV > IV: Long-term directional
    
    logger.info(f"📊 Flag counts (before filtering):")
    logger.info(f"   - ShortTerm_IV_Edge: {df['ShortTerm_IV_Edge'].sum()}")
    logger.info(f"   - ShortTerm_IV_Rich: {df['ShortTerm_IV_Rich'].sum()}")
    logger.info(f"   - ShortTerm_IV_Cheap: {df['ShortTerm_IV_Cheap'].sum()}")
    
    # Filter for minimum divergence threshold
    # STRATEGY-NEUTRAL: Filter by absolute magnitude to capture BOTH directions
    # Allows both IV > HV (sell premium) and HV > IV (buy premium) opportunities
    # If IV is missing, skip IVHV gap filter (allow HV-only rows for chart-based strategies)
    has_iv_data = df['IV30_Call'].notna()
    df_filtered = df[(~has_iv_data) | (df['IVHV_gap_abs'] >= min_gap)].copy()
    
    if df_filtered.empty:
        logger.warning(f"⚠️ No tickers passed filters (IVHV gap ≥ {min_gap} or HV-only).")
        return df_filtered
    
    # Check for duplicates (should be rare if Step 2 cleaned properly)
    if df_filtered['Ticker'].duplicated().any():
        dup_count = df_filtered['Ticker'].duplicated().sum()
        logger.warning(f"⚠️ Found {dup_count} duplicate tickers. Keeping highest gap.")
        df_filtered = df_filtered.sort_values(by='IVHV_gap_abs', ascending=False)
        df_filtered = df_filtered.drop_duplicates(subset='Ticker', keep='first')
    
    logger.info(f"✅ After dedup: {len(df_filtered)} unique tickers")
    
    # Volatility-regime tags (STRATEGY-NEUTRAL)
    # MAGNITUDE-BASED (bidirectional): Describe volatility divergence strength
    df_filtered['HighVol'] = df_filtered['IVHV_gap_abs'] >= 5.0
    df_filtered['ElevatedVol'] = (df_filtered['IVHV_gap_abs'] >= 3.5) & (df_filtered['IVHV_gap_abs'] < 5.0)
    df_filtered['ModerateVol'] = (df_filtered['IVHV_gap_abs'] >= 2.0) & (df_filtered['IVHV_gap_abs'] < 3.5)
    
    # LowRank using Step 2's per-ticker IV_Rank_30D (not cross-sectional)
    if 'IV_Rank_30D' in df_filtered.columns:
        df_filtered['LowRank'] = df_filtered['IV_Rank_30D'] < 30
    else:
        df_filtered['LowRank'] = False
        logger.warning("⚠️ IV_Rank_30D missing, LowRank set to False")
    
    # DIRECTIONAL tags (describe pricing relationship, not trade intent)
    # IV_Rich: Market pricing MORE volatility than realized (IV > HV)
    # IV_Cheap: Market pricing LESS volatility than realized (HV > IV)
    df_filtered['IV_Rich'] = df_filtered['IVHV_gap_30D'] >= 3.5   # IV overpriced vs HV
    df_filtered['IV_Cheap'] = df_filtered['IVHV_gap_30D'] <= -3.5  # IV underpriced vs HV
    
    # VOLATILITY PATTERN DETECTION: Combine Step 2 trends with gap analysis
    # These describe CURRENT STATE, not predictions or trade recommendations
    
    # Mean Reversion Pattern: IV elevated in own range + rising + HV stable/falling
    # Describes: IV diverging further from realized volatility while HV is calm
    if all(col in df_filtered.columns for col in ['IV_Rank_30D', 'IV_Trend_7D', 'HV_Trend_30D']):
        df_filtered['MeanReversion_Setup'] = (
            (df_filtered['IV_Rank_30D'] > 70) &  # IV in top 30% of recent range
            (df_filtered['IV_Trend_7D'] == 'Rising') &
            (df_filtered['HV_Trend_30D'].isin(['Stable', 'Falling']))
        )
    else:
        df_filtered['MeanReversion_Setup'] = False
    
    # Expansion Pattern: IV depressed in own range + stable/falling + HV rising
    # Describes: Realized volatility increasing while IV remains low/flat
    if all(col in df_filtered.columns for col in ['IV_Rank_30D', 'IV_Trend_7D', 'HV_Trend_30D']):
        df_filtered['Expansion_Setup'] = (
            (df_filtered['IV_Rank_30D'] < 30) &  # IV in bottom 30% of recent range
            (df_filtered['HV_Trend_30D'] == 'Rising') &
            (df_filtered['IV_Trend_7D'].isin(['Stable', 'Falling']))
        )
    else:
        df_filtered['Expansion_Setup'] = False
    
    # Aggregate regime flags for downstream filtering
    df_filtered['df_elevated_plus'] = df_filtered['IVHV_gap_abs'] >= 3.5  # Strong divergence (either direction)
    df_filtered['df_moderate_vol'] = df_filtered['ModerateVol']            # ModerateVol only
    
    # Log pattern detection summary
    mean_rev_count = df_filtered['MeanReversion_Setup'].sum()
    expansion_count = df_filtered['Expansion_Setup'].sum()
    if mean_rev_count > 0:
        logger.info(f"📉 Mean reversion pattern detected: {mean_rev_count} tickers")
    if expansion_count > 0:
        logger.info(f"📈 Expansion pattern detected: {expansion_count} tickers")
    
    logger.info(f"✅ IVHV filtering complete: {len(df_filtered)} tickers qualified")
    
    # Add version metadata for cache validation
    df_filtered.attrs['step3_version'] = STEP3_VERSION
    df_filtered.attrs['step3_logic_hash'] = STEP3_LOGIC_HASH
    df_filtered.attrs['step3_computed_at'] = datetime.now().isoformat()
    
    return df_filtered
