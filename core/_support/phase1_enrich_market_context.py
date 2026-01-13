"""
Phase 1 Market Context Enrichment - OHLCV Data from Schwab API

PURPOSE:
    Add observable market context (price history) to portfolio positions.
    Reuses scan engine's production Schwab implementation (DRY principle).

PHILOSOPHY:
    "What does the world look like RIGHT NOW?"
    - Fetch recent price action (OHLCV) for each underlying
    - Attach current price context to positions
    - Enable downstream trend/momentum analysis in Phase 3

DESIGN:
    - Reuses: core.scan_engine.step0_schwab_snapshot.fetch_price_history_with_retry()
    - Benefits: Retry logic, 24hr caching, rate limiting already implemented
    - Data source: Schwab API (same as scan engine for consistency)
    - History: 180 trading days (~9 months)

CONTRACT:
    Input:  DataFrame with 'Underlying' column
    Output: Same DataFrame + market context columns:
        - UL_Close (latest close price)
        - UL_High_5D (5-day high)
        - UL_Low_5D (5-day low)
        - UL_Volume_Ratio (today vol / 20-day avg vol)
        - UL_OHLCV_Available (bool: data fetch success)
        - UL_OHLCV_Status (OK/TIMEOUT/RATE_LIMIT/etc)
"""

import logging
import pandas as pd
import numpy as np
from typing import Optional

logger = logging.getLogger(__name__)


def enrich_with_ohlcv(df: pd.DataFrame, client=None) -> pd.DataFrame:
    """
    Enrich positions with underlying market context from Schwab API.
    
    Reuses scan engine's production implementation:
    - core.scan_engine.step0_schwab_snapshot.fetch_price_history_with_retry()
    - Includes retry logic, caching (24hr TTL), error handling
    - Returns 180 days of OHLCV data
    
    Args:
        df: DataFrame with 'Underlying' column
        client: SchwabClient instance (optional, will create if None)
    
    Returns:
        DataFrame with new columns:
        - UL_Close: Latest close price
        - UL_High_5D: 5-day high
        - UL_Low_5D: 5-day low
        - UL_Volume_Ratio: Today volume / 20-day avg (spike detector)
        - UL_OHLCV_Available: True if data fetched successfully
        - UL_OHLCV_Status: API fetch status (OK/TIMEOUT/etc)
    
    Example:
        >>> df = pd.DataFrame({'Underlying': ['AAPL', 'MSFT', 'NVDA']})
        >>> df = enrich_with_ohlcv(df)
        >>> print(df[['Underlying', 'UL_Close', 'UL_OHLCV_Available']])
    """
    # Import scan engine modules (production code reuse)
    try:
        from core.scan_engine.step0_schwab_snapshot import fetch_price_history_with_retry
        from core.scan_engine.schwab_api_client import SchwabClient
    except ImportError as e:
        logger.error(f"Failed to import scan engine modules: {e}")
        logger.warning("⚠️  Skipping OHLCV enrichment (scan engine not available)")
        df['UL_OHLCV_Available'] = False
        df['UL_OHLCV_Status'] = 'IMPORT_ERROR'
        return df
    
    # Initialize Schwab client
    if client is None:
        try:
            client = SchwabClient()
        except Exception as e:
            logger.error(f"Failed to initialize SchwabClient: {e}")
            logger.warning("⚠️  Skipping OHLCV enrichment (Schwab auth failed)")
            df['UL_OHLCV_Available'] = False
            df['UL_OHLCV_Status'] = 'AUTH_ERROR'
            return df
    
    # Get unique underlyings
    underlyings = df['Underlying'].dropna().unique()
    logger.info(f"📊 Fetching OHLCV for {len(underlyings)} unique underlyings...")
    
    # Fetch OHLCV per underlying (reuses scan engine cache!)
    ohlcv_cache = {}
    success_count = 0
    
    for ticker in underlyings:
        try:
            # Reuse scan engine's production fetcher (with caching!)
            hist, status = fetch_price_history_with_retry(client, ticker, use_cache=True)
            
            if hist is not None and len(hist) > 0:
                # Calculate context metrics
                latest_close = hist['close'].iloc[-1]
                high_5d = hist['high'].tail(5).max()
                low_5d = hist['low'].tail(5).min()
                
                # Volume ratio (today vs 20-day avg)
                if len(hist) >= 20:
                    volume_ratio = hist['volume'].iloc[-1] / hist['volume'].tail(20).mean()
                else:
                    volume_ratio = np.nan
                
                ohlcv_cache[ticker] = {
                    'Close': latest_close,
                    'High_5D': high_5d,
                    'Low_5D': low_5d,
                    'Volume_Ratio': volume_ratio,
                    'Status': status,
                    'Available': True
                }
                success_count += 1
                logger.debug(f"✅ {ticker}: Close=${latest_close:.2f}, Vol Ratio={volume_ratio:.2f}x")
            else:
                logger.debug(f"⚠️  {ticker}: No OHLCV data (status: {status})")
                ohlcv_cache[ticker] = {
                    'Close': np.nan,
                    'High_5D': np.nan,
                    'Low_5D': np.nan,
                    'Volume_Ratio': np.nan,
                    'Status': status,
                    'Available': False
                }
        except Exception as e:
            logger.warning(f"⚠️  {ticker}: Exception during OHLCV fetch: {e}")
            ohlcv_cache[ticker] = {
                'Close': np.nan,
                'High_5D': np.nan,
                'Low_5D': np.nan,
                'Volume_Ratio': np.nan,
                'Status': 'EXCEPTION',
                'Available': False
            }
    
    # Attach to positions
    df['UL_Close'] = df['Underlying'].map(lambda t: ohlcv_cache.get(t, {}).get('Close'))
    df['UL_High_5D'] = df['Underlying'].map(lambda t: ohlcv_cache.get(t, {}).get('High_5D'))
    df['UL_Low_5D'] = df['Underlying'].map(lambda t: ohlcv_cache.get(t, {}).get('Low_5D'))
    df['UL_Volume_Ratio'] = df['Underlying'].map(lambda t: ohlcv_cache.get(t, {}).get('Volume_Ratio'))
    df['UL_OHLCV_Available'] = df['Underlying'].map(lambda t: ohlcv_cache.get(t, {}).get('Available', False))
    df['UL_OHLCV_Status'] = df['Underlying'].map(lambda t: ohlcv_cache.get(t, {}).get('Status', 'UNKNOWN'))
    
    coverage_pct = (success_count / len(underlyings) * 100) if len(underlyings) > 0 else 0
    logger.info(f"📊 OHLCV coverage: {success_count}/{len(underlyings)} ({coverage_pct:.1f}%)")
    
    if success_count < len(underlyings):
        logger.warning(f"⚠️  Missing OHLCV for {len(underlyings) - success_count} underlyings")
        # Log failure breakdown
        status_counts = df['UL_OHLCV_Status'].value_counts()
        for status, count in status_counts.items():
            if status != 'OK':
                logger.warning(f"   {status}: {count} underlyings")
    
    return df


# ============================================================
# STANDALONE TEST
# ============================================================

if __name__ == "__main__":
    # Test with sample portfolio
    logging.basicConfig(level=logging.INFO)
    
    test_df = pd.DataFrame({
        'Symbol': ['AAPL250117C195', 'MSFT250221P420', 'NVDA250314C140'],
        'Underlying': ['AAPL', 'MSFT', 'NVDA'],
        'Quantity': [2, -1, 3]
    })
    
    print("📊 Testing OHLCV enrichment...")
    print(f"Input: {len(test_df)} positions")
    
    enriched = enrich_with_ohlcv(test_df)
    
    print("\n✅ Results:")
    print(enriched[[
        'Underlying', 'UL_Close', 'UL_High_5D', 'UL_Low_5D', 
        'UL_Volume_Ratio', 'UL_OHLCV_Available', 'UL_OHLCV_Status'
    ]])
    
    print(f"\n📊 Coverage: {enriched['UL_OHLCV_Available'].sum()}/{len(enriched)} positions")
