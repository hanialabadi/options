"""
Chart Signal Integration

Loads chart analysis data for recommendation context.
Chart data is ONLY used in Phase 7+ (not in perception loop Phases 1-4).
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path
from typing import Optional
from core.data_contracts.config import MANAGEMENT_SAFE_MODE

logger = logging.getLogger(__name__)


def load_chart_signals(
    symbols: list,
    as_of_date: Optional[pd.Timestamp] = None,
    source: str = 'scan_engine'
) -> pd.DataFrame:
    """
    Load chart signals for given symbols.
    
    Args:
        symbols: List of ticker symbols (Underlying_Ticker)
        as_of_date: Date for historical chart signals (None = latest)
        source: Data source ('scan_engine', 'tradingview', 'cache')
        
    Returns:
        DataFrame with columns:
            - Symbol
            - Chart_Regime (Bullish/Bearish/Sideways/Transition)
            - Signal_Type (Crossover/Reversal/Continuation/Breakdown)
            - EMA_Signal (EMA_20 vs EMA_50 relationship)
            - Days_Since_Cross (days since last crossover)
            - Trend_Slope (directional strength)
            - Signal_Confidence (0-100)
            - Chart_Updated (timestamp of chart data)
    """
    # === Step 4: Fix Chart Signals Safe Mode leak ===
    if MANAGEMENT_SAFE_MODE:
        return pd.DataFrame({
            'Symbol': symbols,
            'Chart_Regime': 'NEUTRAL',
            'Signal_Type': 'NEUTRAL',
            'EMA_Signal': 'NEUTRAL',
            'Days_Since_Cross': np.nan,
            'Trend_Slope': np.nan,
            'Signal_Confidence': 50,
            'Chart_Updated': pd.Timestamp.now()
        })

    if not symbols:
        logger.warning("No symbols provided for chart signal loading")
        return pd.DataFrame()
    
    if source == 'scan_engine':
        return _load_from_scan_engine(symbols, as_of_date)
    elif source == 'cache':
        return _load_from_cache(symbols, as_of_date)
    else:
        logger.warning(f"Unknown chart source: {source}, returning empty")
        return pd.DataFrame()


def _load_from_scan_engine(
    symbols: list,
    as_of_date: Optional[pd.Timestamp] = None
) -> pd.DataFrame:
    """Load chart signals from scan_engine output."""
    try:
        scan_output_dir = Path("data/scan_outputs")

        if not scan_output_dir.exists():
            if not MANAGEMENT_SAFE_MODE:
                logger.warning(f"Scan output directory not found: {scan_output_dir}")
            return _empty_chart_signals(symbols)

        scan_files = list(scan_output_dir.glob("candidates_*.csv"))
        if not scan_files:
            if not MANAGEMENT_SAFE_MODE:
                logger.warning("No scan output files found")
            return _empty_chart_signals(symbols)
        
        latest_scan = max(scan_files, key=lambda p: p.stat().st_mtime)
        logger.info(f"Loading chart signals from: {latest_scan.name}")
        
        df_scan = pd.read_csv(latest_scan)
        
        # Filter to requested symbols
        df_chart = df_scan[df_scan['Symbol'].isin(symbols)].copy()
        
        if len(df_chart) == 0:
            logger.warning(f"No chart signals found for {len(symbols)} symbols")
            return _empty_chart_signals(symbols)
        
        # Normalize chart signal columns
        chart_cols = {
            'Symbol': 'Symbol',
            'Regime': 'Chart_Regime',
            'Signal_Type': 'Signal_Type',
            'EMA_Signal': 'EMA_Signal',
            'Days_Since_Cross': 'Days_Since_Cross',
            'Trend_Slope': 'Trend_Slope',
        }
        
        df_result = pd.DataFrame()
        for scan_col, output_col in chart_cols.items():
            if scan_col in df_chart.columns:
                df_result[output_col] = df_chart[scan_col]
        
        # Add metadata
        df_result['Signal_Confidence'] = 75
        df_result['Chart_Updated'] = pd.Timestamp.now()
        
        logger.info(f"✅ Loaded chart signals for {len(df_result)} symbols")
        return df_result
        
    except Exception as e:
        logger.error(f"Error loading chart signals: {e}", exc_info=True)
        return _empty_chart_signals(symbols)


def _load_from_cache(
    symbols: list,
    as_of_date: Optional[pd.Timestamp] = None
) -> pd.DataFrame:
    """Load cached chart signals."""
    try:
        project_root = Path(__file__).parent.parent.parent.parent
        cache_dir = project_root / "data" / "cache"
        cache_file = cache_dir / "chart_signals.csv"
        
        if not cache_file.exists():
            logger.warning(f"Chart signal cache not found: {cache_file}")
            return _empty_chart_signals(symbols)
        
        df_cache = pd.read_csv(cache_file)
        df_chart = df_cache[df_cache['Symbol'].isin(symbols)].copy()
        
        logger.info(f"✅ Loaded chart signals from cache for {len(df_chart)} symbols")
        return df_chart
        
    except Exception as e:
        logger.error(f"Error loading cached chart signals: {e}", exc_info=True)
        return _empty_chart_signals(symbols)


def _empty_chart_signals(symbols: list) -> pd.DataFrame:
    """Return empty chart signals DataFrame with correct schema."""
    return pd.DataFrame({
        'Symbol': symbols,
        'Chart_Regime': 'Unknown',
        'Signal_Type': 'Unknown',
        'EMA_Signal': 'Unknown',
        'Days_Since_Cross': np.nan,
        'Trend_Slope': np.nan,
        'Signal_Confidence': 0,
        'Chart_Updated': pd.Timestamp.now()
    })


def merge_chart_signals(
    df_positions: pd.DataFrame,
    df_chart: pd.DataFrame
) -> pd.DataFrame:
    """
    Merge chart signals into positions DataFrame.
    """
    if df_chart.empty:
        logger.warning("No chart signals to merge")
        # Add empty chart columns
        for col in ['Chart_Regime', 'Signal_Type', 'EMA_Signal', 'Days_Since_Cross', 'Trend_Slope']:
            if col not in df_positions.columns:
                df_positions[col] = 'Unknown' if 'Signal' in col or 'Regime' in col else np.nan
        return df_positions
    
    # Determine join key - ALWAYS use Underlying_Ticker for chart signals
    # This enforces the canonical symbol identity law.
    if 'Underlying_Ticker' in df_positions.columns:
        join_key = 'Underlying_Ticker'
    else:
        join_key = 'Symbol'
    
    # Ensure df_chart has the correct join key column name
    if 'Symbol' in df_chart.columns and join_key == 'Underlying_Ticker':
        df_chart = df_chart.rename(columns={'Symbol': 'Underlying_Ticker'})

    df_merged = df_positions.merge(
        df_chart,
        on=join_key,
        how='left',
        suffixes=('', '_chart')
    )
    
    # Fill missing chart data with Unknown
    chart_cols = ['Chart_Regime', 'Signal_Type', 'EMA_Signal']
    for col in chart_cols:
        if col in df_merged.columns:
            df_merged[col] = df_merged[col].fillna('Unknown')
    
    logger.info(f"✅ Merged chart signals into {len(df_merged)} positions")
    return df_merged
