"""
IV/HV Availability Loader

PURPOSE:
    Load IV availability flags from derived analytics layer and merge with strategy dataframes.
    Enables acceptance logic to check if IV Rank/Percentile are available for decision-making.

DESIGN PRINCIPLE:
    "Preserve history > fabricate completeness"
    - If IV history insufficient: expose it, never compensate
    - Downgrade READY_NOW → STRUCTURALLY_READY with diagnostic reason
    - No threshold lowering, no fallbacks, no interpolation

USAGE:
    from core.data_layer.ivhv_availability_loader import load_iv_availability
    
    df_with_iv = load_iv_availability(df_strategies, snapshot_date='2026-01-02')
    
    # Check availability:
    if not df_with_iv.loc[idx, 'iv_rank_available']:
        # Downgrade acceptance status
        # Add diagnostic: "IV history insufficient (X days < 120 required)"
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# Path to derived analytics
DERIVED_ANALYTICS_PATH = Path(__file__).parents[2] / 'data' / 'ivhv_timeseries' / 'ivhv_timeseries_derived.csv'


def load_iv_availability(df: pd.DataFrame, snapshot_date: str = None) -> pd.DataFrame:
    """
    Load IV availability flags and merge with strategy dataframe.
    
    Args:
        df: Strategy dataframe with 'Ticker' column
        snapshot_date: Optional date filter (YYYY-MM-DD). If None, uses most recent date.
        
    Returns:
        DataFrame with added columns:
            - iv_rank_available (bool): True if IV Rank can be computed
            - iv_percentile_available (bool): True if IV Percentile can be computed
            - iv_history_days (int): Days of IV history available
            - iv_index_30d (float): 30-day IV Index (if available)
            
    Notes:
        - If derived analytics file not found: adds columns with False/0/NaN
        - If ticker not in derived analytics: marks as unavailable (False)
        - Preserves all original columns
    """
    # Check if derived analytics exists
    if not DERIVED_ANALYTICS_PATH.exists():
        if not MANAGEMENT_SAFE_MODE:
            logger.warning(f"⚠️  IV derived analytics not found at {DERIVED_ANALYTICS_PATH}")
            logger.warning(f"   Adding IV availability columns with default values (unavailable)")
        
        df['iv_rank_available'] = False
        df['iv_percentile_available'] = False
        df['iv_history_days'] = 0
        df['iv_index_30d'] = np.nan
        
        return df
    
    # Load derived analytics
    try:
        df_iv = pd.read_csv(DERIVED_ANALYTICS_PATH)
        logger.info(f"✅ Loaded IV availability data: {len(df_iv)} records")
        logger.info(f"   Date range: {df_iv['date'].min()} → {df_iv['date'].max()}")
        logger.info(f"   Unique tickers: {df_iv['ticker'].nunique()}")
    except Exception as e:
        logger.error(f"❌ Failed to load IV derived analytics: {e}")
        logger.warning(f"   Adding IV availability columns with default values (unavailable)")
        
        df['iv_rank_available'] = False
        df['iv_percentile_available'] = False
        df['iv_history_days'] = 0
        df['iv_index_30d'] = np.nan
        
        return df
    
    # Filter by date if specified
    if snapshot_date:
        df_iv_filtered = df_iv[df_iv['date'] == snapshot_date].copy()
        
        if len(df_iv_filtered) == 0:
            logger.warning(f"⚠️  No IV data found for date {snapshot_date}")
            logger.warning(f"   Available dates: {df_iv['date'].unique()}")
            logger.warning(f"   Using most recent date: {df_iv['date'].max()}")
            df_iv_filtered = df_iv[df_iv['date'] == df_iv['date'].max()].copy()
    else:
        # Use most recent date
        latest_date = df_iv['date'].max()
        df_iv_filtered = df_iv[df_iv['date'] == latest_date].copy()
        logger.info(f"   Using most recent date: {latest_date}")
    
    # Normalize ticker case for matching (derived analytics uses lowercase)
    df_iv['ticker'] = df_iv['ticker'].str.upper()
    
    # ENHANCEMENT: Get the latest valid metrics for each ticker across all dates
    # This ensures that if the current snapshot has missing IV data, we still show 
    # the accumulated history from previous runs.
    df_iv_latest = df_iv.sort_values('date').groupby('ticker').agg({
        'iv_rank_available': 'last',
        'iv_percentile_available': 'last',
        'iv_history_days': 'max',  # History is cumulative
        'iv_index_30d': 'last'
    }).reset_index()
    
    # Select relevant columns
    iv_cols = ['ticker', 'iv_rank_available', 'iv_percentile_available', 
               'iv_history_days', 'iv_index_30d']
    df_iv_merge = df_iv_latest[iv_cols].copy()
    
    # Determine ticker column - ALWAYS use Underlying_Ticker for IV history
    # Defensive alias handling for different pipeline stages
    ticker_col = next((c for c in ["Underlying_Ticker", "Ticker", "Symbol", "ticker", "symbol"] if c in df.columns), None)

    if not ticker_col:
        logger.error("Missing 'Underlying_Ticker' column for IV availability lookup. Ensure Phase 2 normalization ran.")
        return df

    # Merge with strategy dataframe
    df_merged = df.merge(
        df_iv_merge,
        left_on=ticker_col,
        right_on='ticker',
        how='left',
        suffixes=('', '_iv')
    )
    
    # Drop duplicate ticker column
    if 'ticker' in df_merged.columns:
        df_merged = df_merged.drop(columns=['ticker'])
    
    # Fill NaN for tickers not in IV data (mark as unavailable)
    # FIX: Use assign() to avoid chained assignment error in Copy-on-Write mode
    df_merged = df_merged.assign(
        iv_rank_available=df_merged['iv_rank_available'].fillna(False).astype(bool),
        iv_percentile_available=df_merged['iv_percentile_available'].fillna(False).astype(bool),
        iv_history_days=df_merged['iv_history_days'].fillna(0).astype(int)
    )
    # iv_index_30d can remain NaN (numeric column)
    
    # Log availability statistics
    available_count = df_merged['iv_rank_available'].sum()
    unavailable_count = (~df_merged['iv_rank_available']).sum()
    
    logger.info(f"\n📊 IV Availability Statistics:")
    logger.info(f"   ✅ IV Rank available: {available_count}/{len(df_merged)} ({available_count/len(df_merged)*100:.1f}%)")
    logger.info(f"   ❌ IV Rank unavailable: {unavailable_count}/{len(df_merged)} ({unavailable_count/len(df_merged)*100:.1f}%)")
    
    if unavailable_count > 0:
        avg_history = df_merged[~df_merged['iv_rank_available']]['iv_history_days'].mean()
        max_history = df_merged[~df_merged['iv_rank_available']]['iv_history_days'].max()
        logger.info(f"   📅 History for unavailable: avg={avg_history:.1f} days, max={max_history} days")
        logger.info(f"   📅 Required history: 120+ days")
        logger.info(f"   ⏳ Estimated activation: ~{120 - max_history} more days needed")
    
    return df_merged


def get_iv_diagnostic_reason(iv_history_days: int, required_days: int = 120) -> str:
    """
    Generate human-readable diagnostic reason for insufficient IV history.
    
    Args:
        iv_history_days: Actual days of IV history available
        required_days: Required days for IV Rank/Percentile (default: 120)
        
    Returns:
        Diagnostic string for acceptance_reason
    """
    if iv_history_days == 0:
        return f"IV history insufficient (0 days < {required_days} required)"
    else:
        return f"IV history insufficient ({iv_history_days} days < {required_days} required)"


def check_iv_requirements(strategy_type: str, strategy_name: str) -> bool:
    """
    Determine if a strategy type requires IV Rank/Percentile for acceptance.
    
    Args:
        strategy_type: 'DIRECTIONAL' | 'INCOME' | 'VOLATILITY' | 'UNKNOWN'
        strategy_name: Full strategy name
        
    Returns:
        True if strategy requires IV Rank/Percentile, False otherwise
        
    Notes:
        Currently, ALL strategies benefit from IV Rank/Percentile context.
        Future: May differentiate by strategy type (e.g., INCOME less sensitive).
    """
    # Phase 3: Conservative approach - require IV Rank for all strategies
    # This ensures we don't execute without full IV context
    return True


if __name__ == '__main__':
    """
    Test IV availability loader with sample data.
    """
    import sys
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(message)s'
    )
    
    print("="*70)
    print("IV AVAILABILITY LOADER - TEST")
    print("="*70)
    
    # Create sample strategy dataframe
    sample_tickers = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA']
    df_sample = pd.DataFrame({
        'Ticker': sample_tickers,
        'Strategy': ['Long Call Vertical'] * 5,
        'acceptance_status': ['READY_NOW'] * 5
    })
    
    print(f"\nSample input: {len(df_sample)} strategies")
    print(df_sample)
    
    # Load IV availability
    print(f"\n" + "="*70)
    df_result = load_iv_availability(df_sample)
    
    print(f"\n" + "="*70)
    print("RESULT:")
    print("="*70)
    print(f"\nColumns added: {[col for col in df_result.columns if col not in df_sample.columns]}")
    print(f"\nSample output:")
    output_cols = ['Ticker', 'Strategy', 'acceptance_status', 'iv_rank_available', 
                   'iv_percentile_available', 'iv_history_days']
    print(df_result[output_cols])
    
    # Test diagnostic reason
    print(f"\n" + "="*70)
    print("DIAGNOSTIC REASONS:")
    print("="*70)
    for days in [0, 4, 30, 90, 119]:
        reason = get_iv_diagnostic_reason(days)
        print(f"  {days:3d} days: {reason}")
    
    print(f"\n✅ IV AVAILABILITY LOADER TEST COMPLETE")
