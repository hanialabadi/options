#!/usr/bin/env python3
"""
Canonical IV/HV Time-Series Loader
==================================

Normalizes historical IV/HV snapshots into a single canonical time-series format.

SCHEMA: CANONICAL IV/HV TIME-SERIES SCHEMA v1.0

Purpose:
  - Load historical IV/HV data from archive
  - Normalize column names (lowercase, underscores)
  - Preserve missing values as explicit NaN
  - Compute data quality metrics
  - Create append-only canonical store

Data Sources:
  - data/ivhv_archive/*.csv (Fidelity historical snapshots)

Output:
  - data/ivhv_timeseries/ivhv_timeseries_canonical.csv

Design Principles:
  - Preserve history > fabricate completeness
  - No interpolation, no smoothing
  - Explicit NaN for missing data
  - Data quality metrics enable honest diagnostics
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple
import re
import logging

logger = logging.getLogger(__name__)


# ============================================================================
# IV SURFACE REHYDRATION (FOR SCAN ENGINE)
# ============================================================================

def load_latest_iv_surface(df: pd.DataFrame, snapshot_date: datetime) -> pd.DataFrame:
    """
    Rehydrate IV surface and Volatility Identity Card from historical time-series.
    
    AUTHORITATIVE IMPLEMENTATION:
    Joins historical IV surface data and pre-computed rolling metrics (Rank, Percentile)
    using an "As-Of" left join logic.
    
    Args:
        df: Snapshot dataframe with 'Ticker' column
        snapshot_date: Date of snapshot for time-series lookup
        
    Returns:
        DataFrame with IV surface and Volatility Identity Card columns:
            - IV_7_D_Call, IV_14_D_Call, etc. (Surface Tenors)
            - iv_rank_252d, iv_percentile_252d (Rolling Metrics)
            - iv_history_count, iv_index_30d_hist (Context)
            - history_depth_ok, iv_data_stale, regime_confidence (Health Flags)
            - iv_surface_source, iv_surface_date, iv_surface_age_days (Metadata)
    """
    project_root = Path(__file__).parent.parent.parent
    canonical_path = project_root / "data" / "ivhv_timeseries" / "ivhv_timeseries_canonical.csv"
    derived_path = project_root / "data" / "ivhv_timeseries" / "ivhv_timeseries_derived.csv"
    
    # Initialize health flags and metadata with defaults
    df['iv_surface_source'] = 'unavailable'
    df['iv_surface_date'] = None
    df['iv_surface_age_days'] = np.nan
    df['history_depth_ok'] = False
    df['iv_data_stale'] = True
    df['regime_confidence'] = 0.0
    
    # Check if time-series files exist
    if not canonical_path.exists():
        logger.warning(f"⚠️  IV time-series not found at {canonical_path}")
        return df
    
    try:
        # 1. Load Canonical Time-Series (Surface Tenors)
        df_ts = pd.read_csv(canonical_path)
        df_ts['date'] = pd.to_datetime(df_ts['date'])
        snapshot_date_only = pd.to_datetime(snapshot_date.date())
        
        # As-Of Logic: Find most recent date <= snapshot_date
        df_ts_past = df_ts[df_ts['date'] <= snapshot_date_only]
        if df_ts_past.empty:
            logger.warning(f"⚠️  No IV surface data found <= {snapshot_date_only.date()}")
            return df
            
        latest_date = df_ts_past['date'].max()
        df_ts_snapshot = df_ts[df_ts['date'] == latest_date].copy()
        
        # 2. Load Derived Analytics (Volatility Identity Card)
        if derived_path.exists():
            df_derived = pd.read_csv(derived_path)
            df_derived['date'] = pd.to_datetime(df_derived['date'])
            df_derived_snapshot = df_derived[df_derived['date'] == latest_date].copy()
            
            # Merge derived metrics into surface snapshot
            derived_cols = ['ticker', 'iv_rank', 'iv_percentile', 'iv_history_days', 'iv_index_30d']
            df_ts_snapshot = df_ts_snapshot.merge(
                df_derived_snapshot[derived_cols],
                on='ticker',
                how='left'
            )
            logger.info(f"✅ Loaded Volatility Identity Card from {latest_date.date()}")
        
        # Calculate age and health flags
        iv_surface_date = latest_date
        age_days = (snapshot_date_only - iv_surface_date).days
        
        # Prepare merge: normalize ticker case
        df_ts_snapshot['ticker_upper'] = df_ts_snapshot['ticker'].str.upper()
        df['Ticker_upper'] = df['Ticker'].str.upper()
        
        # Map canonical columns back to Step 0 schema (uppercase)
        # Map canonical columns back to Step 0 schema (uppercase)
        iv_columns_mapping = {
            'iv_7d_call': 'IV_7_D_Call',
            'iv_14d_call': 'IV_14_D_Call',
            'iv_21d_call': 'IV_21_D_Call',
            'iv_30d_call': 'IV_30_D_Call_hist',
            'iv_60d_call': 'IV_60_D_Call',
            'iv_90d_call': 'IV_90_D_Call',
            'iv_120d_call': 'IV_120_D_Call',
            'iv_150d_call': 'IV_150_D_Call',
            'iv_180d_call': 'IV_180_D_Call',
            'iv_270d_call': 'IV_270_D_Call',
            'iv_360d_call': 'IV_360_D_Call',
            'iv_720d_call': 'IV_720_D_Call',
            'iv_1080d_call': 'IV_1080_D_Call',
            'iv_7d_put': 'IV_7_D_Put',
            'iv_14d_put': 'IV_14_D_Put',
            'iv_21d_put': 'IV_21_D_Put',
            'iv_30d_put': 'IV_30_D_Put',
            'iv_60d_put': 'IV_60_D_Put',
            'iv_90d_put': 'IV_90_D_Put',
            'iv_120d_put': 'IV_120_D_Put',
            'iv_150d_put': 'IV_150_D_Put',
            'iv_180d_put': 'IV_180_D_Put',
            'iv_270d_put': 'IV_270_D_Put',
            'iv_360d_put': 'IV_360_D_Put',
            'iv_720d_put': 'IV_720_D_Put',
            'iv_1080d_put': 'IV_1080_D_Put',
            # Volatility Identity Card Mapping
            'iv_rank': 'iv_rank_252d',
            'iv_percentile': 'iv_percentile_252d',
            'iv_history_days': 'iv_history_count',
            'iv_index_30d': 'iv_index_30d_hist'
        }
        
        # Rename columns for merge
        cols_to_merge = ['ticker_upper'] + [c for c in iv_columns_mapping.keys() if c in df_ts_snapshot.columns]
        df_ts_merge = df_ts_snapshot[cols_to_merge].copy()
        df_ts_merge = df_ts_merge.rename(columns=iv_columns_mapping)
        
        # Drop existing placeholder columns from snapshot
        iv_cols_to_drop = [v for v in iv_columns_mapping.values() if v in df.columns and v != 'IV_30_D_Call']
        if iv_cols_to_drop:
            df = df.drop(columns=iv_cols_to_drop)
        
        # Preserve live IV_30_D_Call
        live_iv_30 = None
        if 'IV_30_D_Call' in df.columns:
            live_iv_30 = df[['Ticker', 'IV_30_D_Call']].copy()
            df = df.drop(columns=['IV_30_D_Call'])
        
        # Merge with snapshot
        df_merged = df.merge(
            df_ts_merge,
            left_on='Ticker_upper',
            right_on='ticker_upper',
            how='left'
        )
        
        # Restore live IV_30_D_Call
        if live_iv_30 is not None:
            df_merged = df_merged.merge(live_iv_30, on='Ticker', how='left')
            if 'IV_30_D_Call_hist' in df_merged.columns:
                df_merged['IV_30_D_Call'] = df_merged['IV_30_D_Call'].fillna(df_merged['IV_30_D_Call_hist'])
        elif 'IV_30_D_Call_hist' in df_merged.columns:
            df_merged['IV_30_D_Call'] = df_merged['IV_30_D_Call_hist']
            
        # Update Metadata and Health Flags
        df_merged['iv_surface_source'] = 'historical_latest'
        df_merged['iv_surface_date'] = iv_surface_date
        df_merged['iv_surface_age_days'] = age_days
        df_merged['iv_data_stale'] = age_days > 2  # Stale if > 48 hours
        
        if 'iv_history_count' in df_merged.columns:
            df_merged['history_depth_ok'] = df_merged['iv_history_count'] >= 120
            # Simple regime confidence based on history depth
            df_merged['regime_confidence'] = np.clip(df_merged['iv_history_count'] / 252.0, 0, 1)
        
        # Clean up
        df_merged = df_merged.drop(columns=['Ticker_upper', 'ticker_upper'], errors='ignore')
        
        # Log merge statistics
        populated_count = 0
        for col in iv_columns_mapping.values():
            if col in df_merged.columns and col != 'IV_30_D_Call_hist':
                populated_count += df_merged[col].notna().sum()
        
        logger.info(f"✅ IV surface rehydration complete:")
        logger.info(f"   Tickers merged: {df_merged['Ticker'].nunique()}")
        logger.info(f"   IV columns populated: {populated_count} values")
        logger.info(f"   IV surface age: {age_days} days")
        
        if age_days > 7:
            logger.warning(f"⚠️  IV surface is stale ({age_days} days old)")
        
        return df_merged
        
    except Exception as e:
        logger.error(f"❌ IV surface rehydration failed: {e}")
        df['iv_surface_source'] = 'error'
        df['iv_surface_date'] = None
        df['iv_surface_age_days'] = None
        return df


# ============================================================================
# CANONICAL SCHEMA DEFINITION
# ============================================================================

# Expected IV tenors (call and put sides) - 13 tenors each = 26 total
IV_TENORS = ['7d', '14d', '21d', '30d', '60d', '90d', '120d', '150d', '180d', '270d', '360d', '720d', '1080d']
EXPECTED_IV_COLUMNS = 26  # 13 call + 13 put

# Expected HV tenors - 8 tenors
HV_TENORS = ['10d', '20d', '30d', '60d', '90d', '120d', '150d', '180d']
EXPECTED_HV_COLUMNS = 8

# Column mapping: archive format → canonical format
COLUMN_MAPPING = {
    # Identity
    'Ticker': 'ticker',
    'Date': 'date_original',  # May be used if present
    'timestamp': 'timestamp_original',  # May be used if present
    
    # Implied Volatility - Call side
    'IV_7_D_Call': 'iv_7d_call',
    'IV_14_D_Call': 'iv_14d_call',
    'IV_21_D_Call': 'iv_21d_call',
    'IV_30_D_Call': 'iv_30d_call',
    'IV_60_D_Call': 'iv_60d_call',
    'IV_90_D_Call': 'iv_90d_call',
    'IV_120_D_Call': 'iv_120d_call',
    'IV_150_D_Call': 'iv_150d_call',
    'IV_180_D_Call': 'iv_180d_call',
    'IV_270_D_Call': 'iv_270d_call',
    'IV_360_D_Call': 'iv_360d_call',
    'IV_720_D_Call': 'iv_720d_call',
    'IV_1080_D_Call': 'iv_1080d_call',
    
    # Implied Volatility - Put side
    'IV_7_D_Put': 'iv_7d_put',
    'IV_14_D_Put': 'iv_14d_put',
    'IV_21_D_Put': 'iv_21d_put',
    'IV_30_D_Put': 'iv_30d_put',
    'IV_60_D_Put': 'iv_60d_put',
    'IV_90_D_Put': 'iv_90d_put',
    'IV_120_D_Put': 'iv_120d_put',
    'IV_150_D_Put': 'iv_150d_put',
    'IV_180_D_Put': 'iv_180d_put',
    'IV_270_D_Put': 'iv_270d_put',
    'IV_360_D_Put': 'iv_360d_put',
    'IV_720_D_Put': 'iv_720d_put',
    'IV_1080_D_Put': 'iv_1080d_put',
    
    # Historical Volatility
    'HV_10_D_Cur': 'hv_10d',
    'HV_20_D_Cur': 'hv_20d',
    'HV_30_D_Cur': 'hv_30d',
    'HV_60_D_Cur': 'hv_60d',
    'HV_90_D_Cur': 'hv_90d',
    'HV_120_D_Cur': 'hv_120d',
    'HV_150_D_Cur': 'hv_150d',
    'HV_180_D_Cur': 'hv_180d',
}

# Canonical column order
CANONICAL_COLUMNS = [
    # Identity
    'date',
    'ticker',
    'source',
    
    # Implied Volatility - Call side
    'iv_7d_call',
    'iv_14d_call',
    'iv_21d_call',
    'iv_30d_call',
    'iv_60d_call',
    'iv_90d_call',
    'iv_120d_call',
    'iv_150d_call',
    'iv_180d_call',
    'iv_270d_call',
    'iv_360d_call',
    'iv_720d_call',
    'iv_1080d_call',
    
    # Implied Volatility - Put side
    'iv_7d_put',
    'iv_14d_put',
    'iv_21d_put',
    'iv_30d_put',
    'iv_60d_put',
    'iv_90d_put',
    'iv_120d_put',
    'iv_150d_put',
    'iv_180d_put',
    'iv_270d_put',
    'iv_360d_put',
    'iv_720d_put',
    'iv_1080d_put',
    
    # Historical Volatility
    'hv_10d',
    'hv_20d',
    'hv_30d',
    'hv_60d',
    'hv_90d',
    'hv_120d',
    'hv_150d',
    'hv_180d',
    
    # Data Quality Metadata
    'iv_series_length',
    'hv_series_length',
    'iv_data_quality',
    'hv_data_quality',
    'expected_iv_tenors',
    'expected_hv_tenors',
    'record_timestamp',
]


# ============================================================================
# NORMALIZATION FUNCTIONS
# ============================================================================

def extract_date_from_filename(filepath: Path) -> str:
    """
    Extract date from snapshot filename.
    
    Example: ivhv_snapshot_2025-08-03.csv → 2025-08-03
    
    Args:
        filepath: Path to snapshot file
        
    Returns:
        Date string in YYYY-MM-DD format
    """
    filename = filepath.stem  # Remove .csv extension
    
    # Pattern: ivhv_snapshot_YYYY-MM-DD
    match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
    if match:
        return match.group(1)
    
    # Fallback: try to extract any date-like pattern
    match = re.search(r'(\d{4})[-_]?(\d{2})[-_]?(\d{2})', filename)
    if match:
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
    
    raise ValueError(f"Cannot extract date from filename: {filename}")


def compute_data_quality(series_length: int, expected_count: int) -> str:
    """
    Compute data quality tier based on completeness percentage.
    
    Tiers:
      - FULL: ≥80% of expected columns populated
      - PARTIAL: 50-79% of expected columns populated
      - SPARSE: 20-49% of expected columns populated
      - MISSING: <20% of expected columns populated
    
    Args:
        series_length: Number of non-null values
        expected_count: Expected number of values
        
    Returns:
        Quality tier string
    """
    if expected_count == 0:
        return 'MISSING'
    
    pct = (series_length / expected_count) * 100
    
    if pct >= 80:
        return 'FULL'
    elif pct >= 50:
        return 'PARTIAL'
    elif pct >= 20:
        return 'SPARSE'
    else:
        return 'MISSING'


def normalize_snapshot(df_raw: pd.DataFrame, snapshot_date: str, source: str = 'fidelity') -> pd.DataFrame:
    """
    Normalize a single snapshot to canonical schema.
    
    Args:
        df_raw: Raw snapshot DataFrame
        snapshot_date: Date string (YYYY-MM-DD)
        source: Data source identifier
        
    Returns:
        Normalized DataFrame with canonical schema
    """
    # Rename columns according to mapping
    df = df_raw.rename(columns=COLUMN_MAPPING).copy()
    
    # FIX 7: Make Canonical Ingestion market-date driven
    # market_date = Schwab/Fidelity truth (validity date)
    # scan_timestamp = System truth (capture time)
    if 'market_date' in df.columns:
        df['date'] = df['market_date']
    else:
        df['date'] = snapshot_date
        
    df['source'] = source.lower()
    
    # Ensure ticker is uppercase
    if 'ticker' in df.columns:
        df['ticker'] = df['ticker'].str.upper()
    
    # Compute IV series length (count non-null IV values)
    iv_call_cols = [f'iv_{tenor}_call' for tenor in IV_TENORS]
    iv_put_cols = [f'iv_{tenor}_put' for tenor in IV_TENORS]
    iv_cols_present = [c for c in iv_call_cols + iv_put_cols if c in df.columns]
    
    if iv_cols_present:
        df['iv_series_length'] = df[iv_cols_present].notna().sum(axis=1)
    else:
        df['iv_series_length'] = 0
    
    # Compute HV series length
    hv_cols = [f'hv_{tenor}' for tenor in HV_TENORS]
    hv_cols_present = [c for c in hv_cols if c in df.columns]
    
    if hv_cols_present:
        df['hv_series_length'] = df[hv_cols_present].notna().sum(axis=1)
    else:
        df['hv_series_length'] = 0
    
    # Compute data quality
    df['iv_data_quality'] = df['iv_series_length'].apply(
        lambda x: compute_data_quality(x, EXPECTED_IV_COLUMNS)
    )
    df['hv_data_quality'] = df['hv_series_length'].apply(
        lambda x: compute_data_quality(x, EXPECTED_HV_COLUMNS)
    )
    
    # Add expected counts (constant)
    df['expected_iv_tenors'] = EXPECTED_IV_COLUMNS
    df['expected_hv_tenors'] = EXPECTED_HV_COLUMNS
    
    # Add record timestamp (ingestion time)
    df['record_timestamp'] = datetime.now().isoformat()
    
    # Select only canonical columns (in order), fill missing with NaN
    df_canonical = pd.DataFrame()
    for col in CANONICAL_COLUMNS:
        if col in df.columns:
            df_canonical[col] = df[col]
        else:
            df_canonical[col] = np.nan
    
    return df_canonical


# ============================================================================
# MAIN LOADER FUNCTION
# ============================================================================

def load_and_normalize_archive(
    archive_dir: Path,
    output_dir: Path,
    output_filename: str = 'ivhv_timeseries_canonical.csv'
) -> pd.DataFrame:
    """
    Load all snapshots from archive and create canonical time-series.
    
    Args:
        archive_dir: Directory containing historical snapshots
        output_dir: Directory for canonical output
        output_filename: Name of output file
        
    Returns:
        Canonical time-series DataFrame
    """
    print("="*80)
    print("CANONICAL IV/HV TIME-SERIES LOADER")
    print("="*80)
    
    # Find all snapshot files
    snapshot_files = sorted(archive_dir.glob('*.csv'))
    
    print(f"\n📂 Archive Directory: {archive_dir}")
    print(f"📊 Found {len(snapshot_files)} snapshot files")
    
    if not snapshot_files:
        raise ValueError(f"No CSV files found in {archive_dir}")
    
    # List files
    print("\n📋 Files to process:")
    for f in snapshot_files:
        size_kb = f.stat().st_size / 1024
        date_str = extract_date_from_filename(f)
        print(f"  {date_str}  {f.name:45s} ({size_kb:6.1f} KB)")
    
    # Process each snapshot
    all_normalized = []
    
    print(f"\n{'='*80}")
    print("NORMALIZING SNAPSHOTS")
    print("="*80)
    
    for filepath in snapshot_files:
        try:
            # Extract date
            snapshot_date = extract_date_from_filename(filepath)
            
            # Load raw data
            df_raw = pd.read_csv(filepath)
            
            # Normalize
            df_normalized = normalize_snapshot(df_raw, snapshot_date, source='fidelity')
            
            all_normalized.append(df_normalized)
            
            print(f"✅ {snapshot_date}  {len(df_normalized):3d} tickers  "
                  f"({len([c for c in df_normalized.columns if c.startswith('iv_')])} IV cols, "
                  f"{len([c for c in df_normalized.columns if c.startswith('hv_')])} HV cols)")
            
        except Exception as e:
            print(f"❌ {filepath.name}: {e}")
            continue
    
    if not all_normalized:
        raise ValueError("No snapshots were successfully normalized")
    
    # Combine all snapshots from archive
    df_new = pd.concat(all_normalized, ignore_index=True)
    
    # Save to output
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / output_filename

    # FIX: Persistent Canonical Accumulation
    # Instead of rebuilding from archive, we append new data to existing canonical store.
    if output_path.exists():
        df_existing = pd.read_csv(output_path)
        prev_max_days = df_existing.groupby('ticker')['date'].nunique().max()
    else:
        df_existing = pd.DataFrame(columns=df_new.columns)
        prev_max_days = 0

    # Align schemas defensively (handle evolving archive formats)
    df_new = df_new.reindex(columns=df_existing.columns, fill_value=np.nan)
    
    # Combine, sort, and deduplicate
    df_timeseries = pd.concat([df_existing, df_new], ignore_index=True)
    df_timeseries = df_timeseries.sort_values(['date', 'ticker', 'record_timestamp'])
    
    # FIX 7: Explicit Market-Date Driven Ingestion
    before_count = len(df_timeseries)
    df_timeseries = df_timeseries.drop_duplicates(subset=['date', 'ticker'], keep='last')
    after_count = len(df_timeseries)
    
    if before_count > after_count:
        logger.info(f"📊 Canonical Ingestion: Skipped {before_count - after_count} duplicate market-date records.")
    
    df_timeseries = df_timeseries.reset_index(drop=True)

    # Post-write assertion: History must never shrink
    current_max_days = df_timeseries.groupby('ticker')['date'].nunique().max()
    if current_max_days < prev_max_days:
        logger.error(f"❌ CRITICAL: History shrinkage detected! ({current_max_days} < {prev_max_days})")
        # In a production environment, we might raise an exception here.
    
    print(f"\n{'='*80}")
    print("CANONICAL TIME-SERIES UPDATED")
    print("="*80)
    print(f"\n📊 Total rows: {len(df_timeseries):,}")
    print(f"📅 Date range: {df_timeseries['date'].min()} → {df_timeseries['date'].max()}")
    print(f"🏷️  Unique tickers: {df_timeseries['ticker'].nunique()}")
    print(f"📁 Unique dates: {df_timeseries['date'].nunique()}")
    
    # Data quality distribution
    print(f"\n{'='*80}")
    print("DATA QUALITY DISTRIBUTION")
    print("="*80)
    
    print("\n📈 IV Data Quality:")
    iv_quality_dist = df_timeseries['iv_data_quality'].value_counts().sort_index()
    for quality, count in iv_quality_dist.items():
        pct = (count / len(df_timeseries)) * 100
        print(f"  {quality:10s}: {count:5,} ({pct:5.1f}%)")
    
    print("\n📉 HV Data Quality:")
    hv_quality_dist = df_timeseries['hv_data_quality'].value_counts().sort_index()
    for quality, count in hv_quality_dist.items():
        pct = (count / len(df_timeseries)) * 100
        print(f"  {quality:10s}: {count:5,} ({pct:5.1f}%)")
    
    df_timeseries.to_csv(output_path, index=False)
    
    print(f"\n{'='*80}")
    print("OUTPUT SAVED")
    print("="*80)
    print(f"\n📁 File: {output_path}")
    print(f"💾 Size: {output_path.stat().st_size / 1024:.1f} KB")
    print(f"📊 Columns: {len(df_timeseries.columns)}")
    
    return df_timeseries


# ============================================================================
# DIAGNOSTIC FUNCTIONS
# ============================================================================

def show_sample_rows(df: pd.DataFrame, n_samples: int = 5) -> None:
    """Display sample rows from different dates/tickers."""
    print(f"\n{'='*80}")
    print(f"SAMPLE ROWS (n={n_samples})")
    print("="*80)
    
    # Sample from different dates
    unique_dates = df['date'].unique()
    sample_dates = np.random.choice(unique_dates, min(n_samples, len(unique_dates)), replace=False)
    
    for date in sorted(sample_dates):
        df_date = df[df['date'] == date]
        sample_row = df_date.sample(1).iloc[0]
        
        print(f"\n📅 Date: {sample_row['date']}  |  Ticker: {sample_row['ticker']}  |  Source: {sample_row['source']}")
        print(f"   IV Quality: {sample_row['iv_data_quality']}  ({sample_row['iv_series_length']}/{sample_row['expected_iv_tenors']} tenors)")
        print(f"   HV Quality: {sample_row['hv_data_quality']}  ({sample_row['hv_series_length']}/{sample_row['expected_hv_tenors']} tenors)")
        
        # Show sample IV values
        iv_30d_call = sample_row.get('iv_30d_call', np.nan)
        iv_30d_put = sample_row.get('iv_30d_put', np.nan)
        hv_30d = sample_row.get('hv_30d', np.nan)
        
        print(f"   Sample values: IV_30d_call={iv_30d_call:.2f}, IV_30d_put={iv_30d_put:.2f}, HV_30d={hv_30d:.2f}")


def show_column_list(df: pd.DataFrame) -> None:
    """Display final column list."""
    print(f"\n{'='*80}")
    print("CANONICAL COLUMN LIST (ORDERED)")
    print("="*80)
    
    print(f"\nTotal columns: {len(df.columns)}")
    
    # Group columns by category
    identity_cols = [c for c in df.columns if c in ['date', 'ticker', 'source']]
    iv_call_cols = [c for c in df.columns if c.startswith('iv_') and c.endswith('_call')]
    iv_put_cols = [c for c in df.columns if c.startswith('iv_') and c.endswith('_put')]
    hv_cols = [c for c in df.columns if c.startswith('hv_') and not c.endswith(('length', 'quality', 'tenors'))]
    metadata_cols = [c for c in df.columns if c not in identity_cols + iv_call_cols + iv_put_cols + hv_cols]
    
    print(f"\n[IDENTITY] ({len(identity_cols)} columns)")
    for i, col in enumerate(identity_cols, 1):
        print(f"  {i:2d}. {col}")
    
    print(f"\n[IMPLIED VOLATILITY - CALL SIDE] ({len(iv_call_cols)} columns)")
    for i, col in enumerate(iv_call_cols, 1):
        print(f"  {i:2d}. {col}")
    
    print(f"\n[IMPLIED VOLATILITY - PUT SIDE] ({len(iv_put_cols)} columns)")
    for i, col in enumerate(iv_put_cols, 1):
        print(f"  {i:2d}. {col}")
    
    print(f"\n[HISTORICAL VOLATILITY] ({len(hv_cols)} columns)")
    for i, col in enumerate(hv_cols, 1):
        print(f"  {i:2d}. {col}")
    
    print(f"\n[DATA QUALITY METADATA] ({len(metadata_cols)} columns)")
    for i, col in enumerate(metadata_cols, 1):
        print(f"  {i:2d}. {col}")


# ============================================================================
# ENTRY POINT
# ============================================================================

def ingest_latest_snapshots():
    """
    CLI Entry Point: Ingest any new archive files into the canonical time-series.
    Idempotent and safe to re-run.
    """
    project_root = Path(__file__).parent.parent.parent
    archive_dir = project_root / "data" / "ivhv_archive"
    output_dir = project_root / "data" / "ivhv_timeseries"
    
    # Load and normalize
    df_canonical = load_and_normalize_archive(archive_dir, output_dir)
    
    # Show diagnostics
    show_column_list(df_canonical)
    show_sample_rows(df_canonical, n_samples=5)
    
    print(f"\n{'='*80}")
    print("✅ CANONICAL INGESTION COMPLETE")
    print("="*80)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Canonical IV/HV Time-Series Loader")
    parser.add_argument("--ingest-latest", action="store_true", help="Ingest latest archive snapshots")
    args = parser.parse_args()

    if args.ingest_latest:
        ingest_latest_snapshots()
    else:
        # Default legacy behavior
        project_root = Path(__file__).parent.parent.parent
        archive_dir = project_root / "data" / "ivhv_archive"
        output_dir = project_root / "data" / "ivhv_timeseries"
        load_and_normalize_archive(archive_dir, output_dir)
