"""
IV Rank: 252-Day Per-Ticker Percentile Calculation

Phase 1-4 Compliant Implementation
==================================

Architecture:
- Pure observation (no thresholds, no strategy bias)
- Per-ticker historical analysis (not cross-sectional)
- 252 trading days lookback (industry standard)
- Explicit NaN for insufficient data (no magic defaults)
- Deterministic and replay-safe

Usage:
------
    from core.volatility.compute_iv_rank_252d import compute_iv_rank_252d, compute_iv_rank_batch
    
    # Single ticker
    iv_rank = compute_iv_rank_252d(
        symbol='AAPL',
        current_iv=35.2,
        as_of_date='2026-01-04',
        lookback_days=252
    )
    
    # Batch processing
    df = compute_iv_rank_batch(
        df=positions_df,
        symbol_col='Symbol',
        iv_col='IV Mid',
        date_col='Snapshot_Date',
        lookback_days=252
    )

Design Rationale:
-----------------
1. **Per-Ticker Only**: IV_Rank is ticker's current IV vs its own history
   - NOT universe percentile
   - NOT cross-sectional ranking
   - Enables like-for-like comparison over time

2. **252-Day Window**: One year of trading days (standard in volatility analysis)
   - Captures full cycle (earnings, seasonality)
   - Sufficient for stable percentiles
   - Minimum viable: ~120 days

3. **Explicit NaN**: Return NaN when insufficient data
   - Better than false confidence (magic 50.0)
   - Preserves data integrity
   - Enables downstream quality gates

4. **No Strategy Bias**: Pure percentile calculation
   - No thresholds ("high" vs "low")
   - No tags ("MeanReversion_Setup")
   - No filtering (calculate for ALL)

Data Source:
------------
Fidelity IV/HV snapshots archived in:
    data/ivhv_timeseries/ivhv_timeseries_canonical.csv

Columns used:
- date: Snapshot date
- ticker: Underlying symbol
- iv_30d_call: 30-day ATM call IV (primary proxy for "current IV")

Phase Boundary Protection:
---------------------------
✅ ALLOWED (Phase 1-4):
   - Calculate IV_Rank (observation)
   - Store in snapshots (no interpretation)
   - Return NaN for missing data
   - Track metadata (source, history days)

❌ FORBIDDEN (Phase 1-4):
   - IV-based filtering (remove low IV_Rank)
   - Strategy tagging (tag high IV_Rank)
   - PCS thresholds (adjust PCS by IV_Rank)
   - Quality gates (block if IV_Rank unavailable)

Implementation Notes:
---------------------
- Uses pandas for efficient per-ticker grouping
- Caches canonical time-series for batch operations
- Thread-safe for parallel processing
- Logs data quality warnings (insufficient history)

Author: Options Trading System
Date: 2026-01-04
Phase: Implementation (Architecture Approved)
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path
from typing import Optional, Dict, Tuple
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# Module-level cache for canonical time-series
_CANONICAL_CACHE: Optional[pd.DataFrame] = None


def _load_canonical_timeseries(force_reload: bool = False) -> pd.DataFrame:
    """
    Load canonical IV/HV time-series from Fidelity snapshot archive.
    
    Parameters
    ----------
    force_reload : bool, default False
        If True, reload from disk (ignore cache)
    
    Returns
    -------
    pd.DataFrame
        Canonical time-series with columns:
        - date (datetime64)
        - ticker (str)
        - iv_30d_call (float)
        - iv_30d_put (float)
        - source (str)
        - Other IV/HV tenors
    
    Raises
    ------
    FileNotFoundError
        If canonical CSV not found
    """
    global _CANONICAL_CACHE
    
    if not force_reload and _CANONICAL_CACHE is not None:
        return _CANONICAL_CACHE
    
    # Construct path relative to this module
    module_dir = Path(__file__).parent
    canonical_path = module_dir / "../../data/ivhv_timeseries/ivhv_timeseries_canonical.csv"
    
    if not canonical_path.exists():
        raise FileNotFoundError(
            f"Canonical IV time-series not found: {canonical_path}\n"
            "Expected: data/ivhv_timeseries/ivhv_timeseries_canonical.csv\n"
            "Run historical snapshot collection first."
        )
    
    df = pd.read_csv(canonical_path)
    
    # Parse date column
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
    else:
        raise ValueError("Canonical time-series missing 'date' column")
    
    # Validate required columns
    required_cols = ['date', 'ticker', 'iv_30d_call']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Canonical time-series missing required columns: {missing_cols}")
    
    _CANONICAL_CACHE = df
    logger.info(
        f"Loaded canonical IV time-series: {len(df)} rows, "
        f"{df['ticker'].nunique()} tickers, "
        f"date range {df['date'].min()} to {df['date'].max()}"
    )
    
    return df


def compute_iv_rank_252d(
    symbol: str,
    current_iv: float,
    as_of_date: str,
    lookback_days: int = 252,
    min_history_days: int = 120,
    iv_column: str = "iv_30d_call"
) -> Tuple[Optional[float], Dict[str, any]]:
    """
    Calculate IV_Rank for a single ticker using 252-day historical percentile.
    
    Parameters
    ----------
    symbol : str
        Ticker symbol (e.g., 'AAPL')
    current_iv : float
        Current implied volatility (as decimal, e.g., 0.352 for 35.2%)
    as_of_date : str
        Date for calculation (YYYY-MM-DD format)
    lookback_days : int, default 252
        Historical lookback window (trading days)
    min_history_days : int, default 120
        Minimum viable history to calculate percentile
    iv_column : str, default "iv_30d_call"
        Which IV column to use for historical analysis
    
    Returns
    -------
    iv_rank : float or None
        Percentile rank (0-100 scale), or None if insufficient data
    metadata : dict
        Diagnostic information:
        - 'source': Data source ('historical', 'insufficient_data', 'error')
        - 'history_days': Number of historical observations found
        - 'min_iv': Minimum IV in historical window
        - 'max_iv': Maximum IV in historical window
        - 'window_start': Start date of lookback window
        - 'window_end': End date of lookback window
    
    Examples
    --------
    >>> iv_rank, meta = compute_iv_rank_252d('AAPL', 0.352, '2026-01-04')
    >>> print(f"IV_Rank: {iv_rank:.1f}, History: {meta['history_days']} days")
    IV_Rank: 68.7, History: 152 days
    
    >>> iv_rank, meta = compute_iv_rank_252d('NEWticker', 0.40, '2026-01-04')
    >>> print(f"IV_Rank: {iv_rank}, Reason: {meta['source']}")
    IV_Rank: None, Reason: insufficient_data
    
    Notes
    -----
    - Returns None (not 50.0) when insufficient data
    - Percentile calculation: (count <= current) / total * 100
    - Uses trading days, not calendar days (assumes daily snapshots available)
    - Logs warnings when history < min_history_days
    """
    metadata = {
        'source': 'unknown',
        'history_days': 0,
        'min_iv': None,
        'max_iv': None,
        'window_start': None,
        'window_end': None
    }
    
    try:
        # Load historical data
        df_ts = _load_canonical_timeseries()
        
        # Parse as_of_date
        as_of = pd.to_datetime(as_of_date)
        
        # Define lookback window
        window_start = as_of - timedelta(days=lookback_days * 1.5)  # Buffer for weekends/holidays
        window_end = as_of
        
        # Filter to ticker and date range
        df_ticker = df_ts[
            (df_ts['ticker'] == symbol) &
            (df_ts['date'] >= window_start) &
            (df_ts['date'] <= window_end)
        ].copy()
        
        if df_ticker.empty:
            metadata['source'] = 'insufficient_data'
            logger.warning(
                f"No historical IV data found for {symbol} "
                f"(window: {window_start.date()} to {window_end.date()})"
            )
            return None, metadata
        
        # Extract IV values
        if iv_column not in df_ticker.columns:
            metadata['source'] = 'error'
            logger.error(f"IV column '{iv_column}' not found in canonical time-series")
            return None, metadata
        
        iv_history = df_ticker[iv_column].dropna().values
        
        if len(iv_history) < min_history_days:
            metadata['source'] = 'insufficient_data'
            metadata['history_days'] = len(iv_history)
            logger.warning(
                f"Insufficient IV history for {symbol}: "
                f"{len(iv_history)} days (need {min_history_days})"
            )
            return None, metadata
        
        # Calculate percentile rank
        # Formula: (count of values <= current) / total * 100
        count_lte = np.sum(iv_history <= current_iv)
        total_count = len(iv_history)
        iv_rank = (count_lte / total_count) * 100.0
        
        # Update metadata
        metadata.update({
            'source': 'historical',
            'history_days': total_count,
            'min_iv': float(iv_history.min()),
            'max_iv': float(iv_history.max()),
            'window_start': df_ticker['date'].min().strftime('%Y-%m-%d'),
            'window_end': df_ticker['date'].max().strftime('%Y-%m-%d')
        })
        
        logger.debug(
            f"IV_Rank for {symbol}: {iv_rank:.1f} "
            f"(current={current_iv:.3f}, history={total_count} days, "
            f"range=[{metadata['min_iv']:.3f}, {metadata['max_iv']:.3f}])"
        )
        
        return iv_rank, metadata
        
    except Exception as e:
        metadata['source'] = 'error'
        logger.error(f"Error calculating IV_Rank for {symbol}: {e}", exc_info=True)
        return None, metadata


def compute_iv_rank_batch(
    df: pd.DataFrame,
    symbol_col: str = "Symbol",
    iv_col: str = "IV Mid",
    date_col: Optional[str] = None,
    lookback_days: int = 252,
    min_history_days: int = 120,
    iv_column: str = "iv_30d_call"
) -> pd.DataFrame:
    """
    Calculate IV_Rank for multiple tickers (batch processing).
    
    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame with ticker symbols and current IV values
    symbol_col : str, default "Symbol"
        Column containing ticker symbols
    iv_col : str, default "IV Mid"
        Column containing current IV values (as decimal)
    date_col : str or None, default None
        Column containing observation dates (YYYY-MM-DD)
        If None, uses current date for all rows
    lookback_days : int, default 252
        Historical lookback window (trading days)
    min_history_days : int, default 120
        Minimum viable history to calculate percentile
    iv_column : str, default "iv_30d_call"
        Which IV column to use from canonical time-series
    
    Returns
    -------
    pd.DataFrame
        Input DataFrame with added columns:
        - 'IV_Rank' (float or NaN): Percentile rank (0-100)
        - 'IV_Rank_Source' (str): Data source ('historical', 'insufficient_data', 'error', 'missing_iv')
        - 'IV_Rank_History_Days' (int): Number of historical observations
    
    Examples
    --------
    >>> positions_df = pd.DataFrame({
    ...     'Symbol': ['AAPL', 'MSFT', 'NEWTICKER'],
    ...     'IV Mid': [0.352, 0.281, 0.450]
    ... })
    >>> enriched_df = compute_iv_rank_batch(positions_df)
    >>> enriched_df[['Symbol', 'IV_Rank', 'IV_Rank_Source', 'IV_Rank_History_Days']]
       Symbol  IV_Rank IV_Rank_Source  IV_Rank_History_Days
    0    AAPL     68.7     historical                   152
    1    MSFT     45.2     historical                   148
    2  NEWTICKER    NaN  insufficient_data                35
    
    Notes
    -----
    - NaN in 'IV_Rank' indicates insufficient data (not error)
    - Thread-safe: Can be used in parallel processing
    - Efficient: Loads canonical time-series once, reuses for all tickers
    - Preserves input DataFrame structure (adds columns only)
    """
    if df.empty:
        logger.warning("Empty DataFrame passed to compute_iv_rank_batch")
        df["IV_Rank"] = np.nan
        df["IV_Rank_Source"] = "empty"
        df["IV_Rank_History_Days"] = 0
        return df
    
    # Validate required columns
    if symbol_col not in df.columns:
        logger.error(f"Symbol column '{symbol_col}' not found in DataFrame")
        df["IV_Rank"] = np.nan
        df["IV_Rank_Source"] = "error"
        df["IV_Rank_History_Days"] = 0
        return df
    
    if iv_col not in df.columns:
        logger.warning(f"IV column '{iv_col}' not found in DataFrame, setting IV_Rank to NaN")
        df["IV_Rank"] = np.nan
        df["IV_Rank_Source"] = "missing_iv"
        df["IV_Rank_History_Days"] = 0
        return df
    
    # Use current date if not provided
    if date_col is None or date_col not in df.columns:
        as_of_date = datetime.now().strftime('%Y-%m-%d')
        logger.info(f"No date column provided, using current date: {as_of_date}")
    
    # Pre-load canonical time-series (one load for all tickers)
    try:
        _load_canonical_timeseries()
    except FileNotFoundError as e:
        logger.error(str(e))
        df["IV_Rank"] = np.nan
        df["IV_Rank_Source"] = "error"
        df["IV_Rank_History_Days"] = 0
        return df
    
    # Calculate IV_Rank for each row
    iv_ranks = []
    sources = []
    history_days = []
    
    for idx, row in df.iterrows():
        symbol = row[symbol_col]
        current_iv = row[iv_col]
        
        # Handle missing IV
        if pd.isna(current_iv):
            iv_ranks.append(np.nan)
            sources.append("missing_iv")
            history_days.append(0)
            continue
        
        # Determine as_of_date
        if date_col and date_col in df.columns and not pd.isna(row[date_col]):
            obs_date = pd.to_datetime(row[date_col]).strftime('%Y-%m-%d')
        else:
            obs_date = as_of_date
        
        # Compute IV_Rank
        iv_rank, metadata = compute_iv_rank_252d(
            symbol=symbol,
            current_iv=current_iv,
            as_of_date=obs_date,
            lookback_days=lookback_days,
            min_history_days=min_history_days,
            iv_column=iv_column
        )
        
        iv_ranks.append(iv_rank)
        sources.append(metadata['source'])
        history_days.append(metadata['history_days'])
    
    # Add columns to DataFrame
    df["IV_Rank"] = iv_ranks
    df["IV_Rank_Source"] = sources
    df["IV_Rank_History_Days"] = history_days
    
    # Summary statistics
    valid_count = df["IV_Rank"].notna().sum()
    total_count = len(df)
    logger.info(
        f"IV_Rank batch calculation complete: "
        f"{valid_count}/{total_count} tickers with sufficient history "
        f"({valid_count/total_count*100:.1f}%)"
    )
    
    # Warn if many missing
    if valid_count < total_count * 0.5:
        logger.warning(
            f"⚠️  Low IV_Rank coverage: Only {valid_count}/{total_count} tickers "
            f"have sufficient historical data ({lookback_days} days). "
            f"Consider expanding Fidelity snapshot archive."
        )
    
    return df


def clear_cache():
    """
    Clear the canonical time-series cache.
    
    Use when:
    - Historical data has been updated
    - Running tests that modify canonical CSV
    - Memory optimization in long-running processes
    """
    global _CANONICAL_CACHE
    _CANONICAL_CACHE = None
    logger.info("Canonical time-series cache cleared")
