"""
HV Bootstrap for IV Rank

When IV history is insufficient (<120 days), we can use Historical Volatility (HV)
computed from price history as a proxy for IV Rank.

RATIONALE:
- IV (Implied Volatility) = market's expectation of future volatility
- HV (Historical Volatility) = realized volatility from price movements
- In normal markets, IV and HV tend to converge over time
- HV Rank is a reasonable proxy for IV Rank when IV history is unavailable

This allows immediate IV Rank estimates while building proper IV history.
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, Optional, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)


def compute_hv_from_prices(prices: pd.Series, window: int = 30) -> pd.Series:
    """
    Compute annualized historical volatility from price series.

    Args:
        prices: Series of closing prices
        window: Rolling window in days (default 30)

    Returns:
        Series of annualized HV values (in percentage)
    """
    returns = np.log(prices / prices.shift(1))
    hv = returns.rolling(window).std() * np.sqrt(252) * 100
    return hv


def compute_hv_rank(
    ticker: str,
    lookback_days: int = 252,
    hv_window: int = 30
) -> Optional[Dict]:
    """
    Compute HV Rank for a ticker using price history.

    Args:
        ticker: Stock ticker symbol
        lookback_days: Days of history for percentile calculation
        hv_window: Window for HV calculation (default 30 days)

    Returns:
        Dict with hv_rank, hv_current, hv_min, hv_max, or None if unavailable
    """
    try:
        from core.shared.data_layer.price_history_loader import load_price_history

        # Load extra days for HV calculation warmup
        df_price, source = load_price_history(ticker, days=lookback_days + hv_window + 30)

        if df_price is None or len(df_price) < hv_window + 30:
            logger.debug(f"Insufficient price history for {ticker}")
            return None

        # Compute HV series
        hv_series = compute_hv_from_prices(df_price['Close'], window=hv_window)
        hv_history = hv_series.dropna().tail(lookback_days)

        if len(hv_history) < 60:  # Minimum 60 days for meaningful rank
            logger.debug(f"Insufficient HV history for {ticker}: {len(hv_history)} days")
            return None

        current_hv = hv_series.iloc[-1]

        # Compute percentile rank
        hv_rank = (hv_history <= current_hv).sum() / len(hv_history) * 100

        return {
            'HV_Rank_Bootstrap': round(hv_rank, 1),
            'HV_30D_Current': round(current_hv, 2),
            'HV_30D_Min_252D': round(hv_history.min(), 2),
            'HV_30D_Max_252D': round(hv_history.max(), 2),
            'HV_History_Days': len(hv_history),
            'HV_Rank_Source': f'HV Bootstrap ({source})'
        }

    except Exception as e:
        logger.error(f"HV rank computation failed for {ticker}: {e}")
        return None


def bootstrap_iv_rank_from_hv(
    df: pd.DataFrame,
    iv_rank_col: str = 'IV_Rank_30D',
    ticker_col: str = 'Ticker'
) -> pd.DataFrame:
    """
    Bootstrap IV Rank from HV for rows where IV Rank is missing.

    This function:
    1. Identifies rows with missing IV_Rank_30D
    2. Computes HV Rank from price history
    3. Uses HV Rank as a proxy for IV Rank
    4. Marks the source as 'HV Bootstrap'

    Args:
        df: DataFrame with Ticker and IV_Rank_30D columns
        iv_rank_col: Column name for IV Rank
        ticker_col: Column name for ticker

    Returns:
        DataFrame with IV Rank filled in where possible
    """
    df = df.copy()

    # Find rows needing bootstrap
    if iv_rank_col not in df.columns:
        df[iv_rank_col] = np.nan

    needs_bootstrap = df[iv_rank_col].isna()
    tickers_to_bootstrap = df.loc[needs_bootstrap, ticker_col].unique()

    if len(tickers_to_bootstrap) == 0:
        logger.info("No tickers need HV bootstrap - all have IV Rank")
        return df

    logger.info(f"Bootstrapping IV Rank from HV for {len(tickers_to_bootstrap)} tickers")

    # Compute HV Rank for each ticker
    bootstrap_count = 0
    for ticker in tickers_to_bootstrap:
        hv_data = compute_hv_rank(ticker)

        if hv_data is not None:
            mask = (df[ticker_col] == ticker) & df[iv_rank_col].isna()
            df.loc[mask, iv_rank_col] = hv_data['HV_Rank_Bootstrap']

            # Add source column if not exists
            if 'IV_Rank_Source' not in df.columns:
                df['IV_Rank_Source'] = np.nan
            df.loc[mask, 'IV_Rank_Source'] = hv_data['HV_Rank_Source']

            bootstrap_count += 1

    logger.info(f"HV Bootstrap complete: {bootstrap_count}/{len(tickers_to_bootstrap)} tickers")

    return df


def get_hv_rank_summary(tickers: list) -> pd.DataFrame:
    """
    Get HV Rank summary for multiple tickers.

    Useful for diagnostics and validation.
    """
    results = []

    for ticker in tickers:
        hv_data = compute_hv_rank(ticker)

        if hv_data:
            results.append({
                'Ticker': ticker,
                **hv_data
            })
        else:
            results.append({
                'Ticker': ticker,
                'HV_Rank_Bootstrap': None,
                'HV_Rank_Source': 'UNAVAILABLE'
            })

    return pd.DataFrame(results)
