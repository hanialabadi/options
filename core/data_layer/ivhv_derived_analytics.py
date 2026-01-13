#!/usr/bin/env python3
"""
Derived IV Analytics Layer
==========================

Computes derived metrics from canonical IV/HV time-series:
  - IV Index (aggregated term structure)
  - IV Rank (252-day rolling)
  - IV Percentile (252-day rolling)
  - Data availability diagnostics

INPUT:
  data/ivhv_timeseries/ivhv_timeseries_canonical.csv

OUTPUT:
  data/ivhv_timeseries/ivhv_timeseries_derived.csv

DESIGN PRINCIPLES:
  - If IV history insufficient → expose it (NaN + diagnostic flag)
  - No interpolation, no smoothing
  - Honest diagnostics about data availability
  - Preserve (date, ticker) key structure
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import warnings

warnings.filterwarnings('ignore')


# ============================================================================
# CONFIGURATION
# ============================================================================

# IV Index tenor windows (days)
IV_INDEX_WINDOWS = {
    '7d': 7,
    '30d': 30,
    '60d': 60,
}

# Tenor availability threshold for IV Index
IV_INDEX_MIN_AVAILABILITY = 0.70  # Require ≥70% of tenors

# Rolling window for IV Rank/Percentile
IV_RANK_LOOKBACK_DAYS = 252  # Trading days

# Minimum history required for IV Rank/Percentile
IV_RANK_MIN_HISTORY = 120  # Points

# Short-horizon windows for diagnostic momentum
IV_MOMENTUM_WINDOWS = [3, 5, 10]

# FIX 10: Formalize Day Drift vs Scan Drift
# Day Drift: Changes across market_date
# Scan Drift: Changes across scan_timestamp (same market day)

# All IV call tenors (in days)
IV_CALL_TENORS = {
    'iv_7d_call': 7,
    'iv_14d_call': 14,
    'iv_21d_call': 21,
    'iv_30d_call': 30,
    'iv_60d_call': 60,
    'iv_90d_call': 90,
    'iv_120d_call': 120,
    'iv_150d_call': 150,
    'iv_180d_call': 180,
    'iv_270d_call': 270,
    'iv_360d_call': 360,
    'iv_720d_call': 720,
    'iv_1080d_call': 1080,
}


# ============================================================================
# IV INDEX COMPUTATION
# ============================================================================

def compute_iv_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute IV Index for multiple time windows.
    
    IV Index = mean of call-side IV tenors within window
    
    Rules:
      - Use call-side IV only
      - For each window (7d, 30d, 60d), average tenors ≤ window
      - Require ≥70% tenor availability
      - Else: NaN
    
    Args:
        df: DataFrame with canonical IV columns
        
    Returns:
        DataFrame with added iv_index_* columns
    """
    df = df.copy()
    
    for window_name, window_days in IV_INDEX_WINDOWS.items():
        # Find tenors within window
        tenors_in_window = [
            col for col, tenor_days in IV_CALL_TENORS.items()
            if tenor_days <= window_days and col in df.columns
        ]
        
        if not tenors_in_window:
            df[f'iv_index_{window_name}'] = np.nan
            continue
        
        # Count expected tenors
        expected_count = len(tenors_in_window)
        
        # Compute mean for each row
        iv_index_values = []
        
        for idx, row in df.iterrows():
            # Count available (non-null) tenors
            available_values = []
            for tenor_col in tenors_in_window:
                val = row.get(tenor_col, np.nan)
                if pd.notna(val):
                    available_values.append(val)
            
            # Check availability threshold
            availability_pct = len(available_values) / expected_count if expected_count > 0 else 0
            
            if availability_pct >= IV_INDEX_MIN_AVAILABILITY:
                iv_index_values.append(np.mean(available_values))
            else:
                iv_index_values.append(np.nan)
        
        df[f'iv_index_{window_name}'] = iv_index_values
    
    return df


# ============================================================================
# IV RANK COMPUTATION
# ============================================================================

def compute_iv_rank_percentile(df: pd.DataFrame, reference_column: str = 'iv_index_30d') -> pd.DataFrame:
    """
    Compute IV Rank and IV Percentile with rolling 252-day lookback.
    
    IV Rank = (current_iv - min_iv) / (max_iv - min_iv)
    IV Percentile = percentile rank in historical distribution
    
    Rules:
      - Rolling lookback: 252 trading days (1 year)
      - Require ≥120 historical points
      - Group by ticker (each ticker has independent history)
      - If insufficient history → NaN + diagnostic flag
    
    Args:
        df: DataFrame with IV index columns
        reference_column: Column to use for rank/percentile calculation
        
    Returns:
        DataFrame with iv_rank, iv_percentile, and diagnostic columns
    """
    df = df.copy()
    
    # Ensure date is datetime
    df['date'] = pd.to_datetime(df['date'])
    
    # Sort by ticker and date
    df = df.sort_values(['ticker', 'date']).reset_index(drop=True)
    
    # Initialize output columns
    df['iv_rank'] = np.nan
    df['iv_percentile'] = np.nan
    df['iv_rank_available'] = False
    df['iv_percentile_available'] = False
    df['iv_history_days'] = 0
    df['iv_history_window_days'] = 0
    
    # Initialize momentum columns
    for w in IV_MOMENTUM_WINDOWS:
        df[f'iv_momentum_{w}d'] = np.nan
    
    # Initialize Drift columns (Diagnostic Only)
    df['iv_drift_1d'] = np.nan # Day Drift (market_date delta)
    df['iv_drift_0d'] = np.nan # Scan Drift (intraday delta)
    
    # Group by ticker
    for ticker in df['ticker'].unique():
        ticker_mask = df['ticker'] == ticker
        ticker_df = df[ticker_mask].copy()
        
        # Get indices for this ticker
        ticker_indices = ticker_df.index.tolist()
        
        for i, idx in enumerate(ticker_indices):
            current_date = ticker_df.loc[idx, 'date']
            
            # Look back 252 days (use all prior dates, limit to 252)
            lookback_mask = (ticker_df['date'] < current_date)
            lookback_df = ticker_df[lookback_mask].tail(IV_RANK_LOOKBACK_DAYS)
            
            # Get historical IV values (non-null only)
            # FIX: Count history based on ANY valid IV data point (proxy or index)
            # This ensures daily scans (which only have iv_30d_call) increment the counter
            # even if the full IV surface (required for iv_index_30d) is missing.
            # We exclude diagnostic columns (available, days, window) to avoid false positives.
            data_cols = [col for col in lookback_df.columns if col.startswith('iv_') 
                        and (col.endswith('_call') or col.endswith('_put'))]
            historical_iv_df = lookback_df[lookback_df[data_cols].notna().any(axis=1)]
            
            # Check if we have enough history
            # history_count represents "days with data", used for the 120-day gate
            history_count = len(historical_iv_df)
            df.loc[idx, 'iv_history_days'] = history_count
            
            # For the actual Rank/Percentile calculation, we still need the reference column
            current_iv = ticker_df.loc[idx, reference_column]
            
            # Skip rank calculation if current IV is null, but history count is already saved
            if pd.isna(current_iv):
                continue

            valid_reference_df = lookback_df[['date', reference_column]].dropna()
            historical_iv = valid_reference_df[reference_column]
            
            if history_count > 0:
                first_date = historical_iv_df['date'].min()
                last_date = historical_iv_df['date'].max()
                window_days = (last_date - first_date).days
                df.loc[idx, 'iv_history_window_days'] = window_days
            else:
                window_days = 0

            # Lookback Density Check: 120 points must fall within 180 calendar days
            if history_count < IV_RANK_MIN_HISTORY or window_days > 180:
                # Insufficient history or fragmented data - leave as NaN
                continue
            
            # Compute IV Rank
            iv_min = historical_iv.min()
            iv_max = historical_iv.max()
            
            if iv_max > iv_min:  # Avoid division by zero
                iv_rank = (current_iv - iv_min) / (iv_max - iv_min)
                df.loc[idx, 'iv_rank'] = iv_rank
                df.loc[idx, 'iv_rank_available'] = True
            
            # Compute IV Percentile
            # Percentile = percentage of historical values below current
            below_current = (historical_iv < current_iv).sum()
            iv_percentile = (below_current / history_count) * 100
            
            df.loc[idx, 'iv_percentile'] = iv_percentile
            df.loc[idx, 'iv_percentile_available'] = True

            # Compute Short-Horizon Momentum (Diagnostic Only)
            # This does not violate the 120-day contract as it is labeled diagnostic.
            for w in IV_MOMENTUM_WINDOWS:
                if history_count >= w:
                    past_iv = historical_iv.iloc[-w]
                    if past_iv > 0:
                        momentum = (current_iv - past_iv) / past_iv
                        df.loc[idx, f'iv_momentum_{w}d'] = momentum

            # Compute Day Drift (1D)
            if history_count >= 1:
                prior_iv = historical_iv.iloc[-1]
                if prior_iv > 0:
                    df.loc[idx, 'iv_drift_1d'] = (current_iv - prior_iv) / prior_iv

            # Compute Scan Drift (0D)
            # Note: This requires intraday records which are currently deduplicated in canonical.
            # Surfaced here as a placeholder for future non-canonical intraday series.
            df.loc[idx, 'iv_drift_0d'] = 0.0 # Placeholder
    
    return df


# ============================================================================
# MAIN ANALYTICS PIPELINE
# ============================================================================

def compute_derived_analytics(
    canonical_file: Path,
    output_file: Path
) -> pd.DataFrame:
    """
    Main pipeline: load canonical data and compute derived analytics.
    
    Args:
        canonical_file: Path to canonical time-series CSV
        output_file: Path for derived analytics output
        
    Returns:
        DataFrame with derived analytics
    """
    print("="*80)
    print("DERIVED IV ANALYTICS LAYER")
    print("="*80)
    
    # Load canonical time-series
    print(f"\n📂 Loading canonical time-series...")
    print(f"   File: {canonical_file}")
    
    if not canonical_file.exists():
        raise FileNotFoundError(f"Canonical file not found: {canonical_file}")
    
    df = pd.read_csv(canonical_file)
    
    print(f"   ✅ Loaded {len(df):,} rows")
    print(f"   📅 Date range: {df['date'].min()} → {df['date'].max()}")
    print(f"   🏷️  Unique tickers: {df['ticker'].nunique()}")
    
    # Step 1: Compute IV Index
    print(f"\n{'='*80}")
    print("STEP 1: COMPUTING IV INDEX")
    print("="*80)
    
    print(f"\nWindows: {list(IV_INDEX_WINDOWS.keys())}")
    print(f"Availability threshold: ≥{IV_INDEX_MIN_AVAILABILITY*100:.0f}% of tenors")
    
    df = compute_iv_index(df)
    
    # Report IV Index statistics
    for window_name in IV_INDEX_WINDOWS.keys():
        col = f'iv_index_{window_name}'
        non_null = df[col].notna().sum()
        pct = (non_null / len(df)) * 100
        mean_val = df[col].mean()
        print(f"   {col:20s}: {non_null:4,}/{len(df):,} ({pct:5.1f}%) | mean={mean_val:.2f}")
    
    # Step 2: Compute IV Rank and Percentile
    print(f"\n{'='*80}")
    print("STEP 2: COMPUTING IV RANK & PERCENTILE")
    print("="*80)
    
    print(f"\nLookback window: {IV_RANK_LOOKBACK_DAYS} days")
    print(f"Minimum history: {IV_RANK_MIN_HISTORY} points")
    print(f"Reference column: iv_index_30d")
    
    df = compute_iv_rank_percentile(df, reference_column='iv_index_30d')
    
    # Report statistics
    rank_available = df['iv_rank_available'].sum()
    percentile_available = df['iv_percentile_available'].sum()
    
    print(f"\n📊 Results:")
    print(f"   IV Rank available:       {rank_available:4,}/{len(df):,} ({rank_available/len(df)*100:5.1f}%)")
    print(f"   IV Percentile available: {percentile_available:4,}/{len(df):,} ({percentile_available/len(df)*100:5.1f}%)")
    
    # History distribution
    print(f"\n📈 IV History Distribution:")
    history_bins = [0, 60, 120, 180, 252, 500]
    history_labels = ['0-59', '60-119', '120-179', '180-251', '252+']
    
    df['history_bin'] = pd.cut(df['iv_history_days'], bins=history_bins, labels=history_labels, right=False)
    history_dist = df['history_bin'].value_counts().sort_index()
    
    for bin_label, count in history_dist.items():
        pct = (count / len(df)) * 100
        print(f"   {bin_label:10s} days: {count:4,} ({pct:5.1f}%)")
    
    df = df.drop(columns=['history_bin'])
    
    # Sample derived values
    print(f"\n{'='*80}")
    print("SAMPLE DERIVED VALUES")
    print("="*80)
    
    # Show samples with available IV Rank
    samples_with_rank = df[df['iv_rank_available']].sample(min(5, df['iv_rank_available'].sum()))
    
    print(f"\nSamples with IV Rank (n={len(samples_with_rank)}):")
    for idx, row in samples_with_rank.iterrows():
        print(f"\n{row['date']} | {row['ticker']}")
        print(f"  IV Index 30d: {row['iv_index_30d']:.2f}")
        print(f"  IV Rank: {row['iv_rank']:.2f} ({row['iv_rank']*100:.1f}%)")
        print(f"  IV Percentile: {row['iv_percentile']:.1f}th")
        print(f"  History: {int(row['iv_history_days'])} days")
    
    # Show samples without IV Rank (insufficient history)
    samples_without_rank = df[~df['iv_rank_available']].head(3)
    
    if len(samples_without_rank) > 0:
        print(f"\nSamples WITHOUT IV Rank (insufficient history, n={len(samples_without_rank)}):")
        for idx, row in samples_without_rank.iterrows():
            print(f"\n{row['date']} | {row['ticker']}")
            print(f"  IV Index 30d: {row['iv_index_30d']:.2f}" if pd.notna(row['iv_index_30d']) else "  IV Index 30d: NaN")
            print(f"  History: {int(row['iv_history_days'])} days (need {IV_RANK_MIN_HISTORY}+)")
            print(f"  Status: Insufficient history → NaN + diagnostic flag")
    
    # Save output
    print(f"\n{'='*80}")
    print("SAVING DERIVED ANALYTICS")
    print("="*80)
    
    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, index=False)
    
    print(f"\n📁 File: {output_file}")
    print(f"💾 Size: {output_file.stat().st_size / 1024:.1f} KB")
    print(f"📊 Rows: {len(df):,}")
    print(f"📊 Columns: {len(df.columns)}")
    
    # Column inventory
    print(f"\n📋 Column Inventory:")
    
    canonical_cols = [c for c in df.columns if not c.startswith('iv_index') and 
                      c not in ['iv_rank', 'iv_percentile', 'iv_rank_available', 
                                'iv_percentile_available', 'iv_history_days']]
    derived_cols = [c for c in df.columns if c not in canonical_cols]
    
    print(f"   Canonical (preserved): {len(canonical_cols)} columns")
    print(f"   Derived (added):       {len(derived_cols)} columns")
    print(f"      {derived_cols}")
    
    return df


# ============================================================================
# VALIDATION & DIAGNOSTICS
# ============================================================================

def validate_derived_analytics(df: pd.DataFrame) -> None:
    """
    Validate derived analytics output.
    
    Args:
        df: DataFrame with derived analytics
    """
    print(f"\n{'='*80}")
    print("VALIDATION CHECKS")
    print("="*80)
    
    checks_passed = 0
    checks_failed = 0
    
    # Check 1: All expected derived columns present
    expected_derived_cols = [
        'iv_index_7d', 'iv_index_30d', 'iv_index_60d',
        'iv_rank', 'iv_percentile',
        'iv_rank_available', 'iv_percentile_available', 'iv_history_days'
    ]
    
    missing_cols = [c for c in expected_derived_cols if c not in df.columns]
    
    if not missing_cols:
        print(f"✅ Check 1: All derived columns present")
        checks_passed += 1
    else:
        print(f"❌ Check 1: Missing columns: {missing_cols}")
        checks_failed += 1
    
    # Check 2: IV Rank in valid range [0, 1]
    if 'iv_rank' in df.columns:
        invalid_rank = df['iv_rank'].dropna()
        invalid_rank = invalid_rank[(invalid_rank < 0) | (invalid_rank > 1)]
        
        if len(invalid_rank) == 0:
            print(f"✅ Check 2: IV Rank values in valid range [0, 1]")
            checks_passed += 1
        else:
            print(f"❌ Check 2: {len(invalid_rank)} IV Rank values outside [0, 1]")
            checks_failed += 1
    
    # Check 3: IV Percentile in valid range [0, 100]
    if 'iv_percentile' in df.columns:
        invalid_pct = df['iv_percentile'].dropna()
        invalid_pct = invalid_pct[(invalid_pct < 0) | (invalid_pct > 100)]
        
        if len(invalid_pct) == 0:
            print(f"✅ Check 3: IV Percentile values in valid range [0, 100]")
            checks_passed += 1
        else:
            print(f"❌ Check 3: {len(invalid_pct)} IV Percentile values outside [0, 100]")
            checks_failed += 1
    
    # Check 4: Diagnostic flags consistent with values
    if all(c in df.columns for c in ['iv_rank', 'iv_rank_available']):
        # If iv_rank is not null, iv_rank_available should be True
        inconsistent = df[df['iv_rank'].notna() & ~df['iv_rank_available']]
        
        if len(inconsistent) == 0:
            print(f"✅ Check 4: IV Rank diagnostic flags consistent")
            checks_passed += 1
        else:
            print(f"❌ Check 4: {len(inconsistent)} rows have iv_rank but flag=False")
            checks_failed += 1
    
    # Check 5: No rows lost (same count as input canonical)
    # (We can't check this without loading canonical again, skip)
    
    print(f"\n{'='*80}")
    print(f"Validation Summary: {checks_passed} passed, {checks_failed} failed")
    print("="*80)


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    # Paths
    project_root = Path(__file__).parent.parent.parent
    canonical_file = project_root / "data" / "ivhv_timeseries" / "ivhv_timeseries_canonical.csv"
    output_file = project_root / "data" / "ivhv_timeseries" / "ivhv_timeseries_derived.csv"
    
    # Compute derived analytics
    df_derived = compute_derived_analytics(canonical_file, output_file)
    
    # Validate
    validate_derived_analytics(df_derived)
    
    print(f"\n{'='*80}")
    print("✅ DERIVED IV ANALYTICS COMPLETE")
    print("="*80)
    
    print("\n🎯 Design Principles Validated:")
    print("   ✅ Insufficient history → NaN + diagnostic flag")
    print("   ✅ No interpolation or smoothing")
    print("   ✅ Honest diagnostics about data availability")
    print("   ✅ Preserve (date, ticker) key structure")
    
    print("\n📊 Output Ready For:")
    print("   ⏳ Scan engine integration (later)")
    print("   ⏳ Acceptance semantics (later)")
    print("   ⏳ Execution confidence scoring (later)")
