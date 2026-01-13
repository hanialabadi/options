"""
Earnings Calendar Module

Provides earnings date lookup for tickers for INFORMATIONAL/EDUCATIONAL purposes only.

Purpose:
- Display days_to_earnings for user awareness
- Educational: Highlight upcoming binary event risk
- NOT a blocking gate - user decides based on their risk tolerance

Design Philosophy:
- Informational: Show data, trust user judgment
- Reliable: Use Yahoo Finance as primary source
- Transparent: Log when data unavailable

Data Sources (in priority order):
1. Yahoo Finance (yfinance library) - primary source
2. Static calendar file (fallback for testing)
3. None (allow trade, no false positives)

Usage:
    from core.data_layer.earnings_calendar import add_earnings_proximity
    
    df = add_earnings_proximity(df, snapshot_date)
    # Adds columns: days_to_earnings, earnings_proximity_flag
    # Flag indicates proximity but does NOT block execution
"""

import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Optional
import os
from core.data_contracts.config import MANAGEMENT_SAFE_MODE

logger = logging.getLogger(__name__)


def get_earnings_date_yfinance(ticker: str) -> Optional[datetime]:
    """
    Fetch next earnings date for ticker from Yahoo Finance.
    
    Args:
        ticker: Stock ticker symbol
        
    Returns:
        datetime of next earnings, or None if unavailable
    """
    # Management Safe Mode: Suppress Yahoo lookups to silence noise
    if MANAGEMENT_SAFE_MODE:
        return None

    # Skip known ETFs to avoid yfinance 404 errors on fundamentals
    etfs = {'SPY', 'QQQ', 'IWM', 'DIA', 'TLT', 'GLD', 'SLV', 'XLE', 'XLF', 'XLI', 'XLK', 'XLU', 'XLP', 'XLV', 'XLY', 'XLB', 'XLC', 'XRE', 'SMH'}
    if ticker.upper() in etfs:
        return None

    try:
        import yfinance as yf
        
        stock = yf.Ticker(ticker)
        
        # Try calendar attribute first (most reliable)
        # NOTE: yfinance throws 404 for ETFs when accessing .calendar
        if hasattr(stock, 'calendar') and stock.calendar is not None:
            earnings_dates = stock.calendar.get('Earnings Date')
            if earnings_dates and len(earnings_dates) > 0:
                # earnings_dates is a list, take the first (next) date
                next_earnings = earnings_dates[0]
                # Convert date to datetime
                if isinstance(next_earnings, datetime):
                    return next_earnings
                else:
                    # It's a datetime.date object
                    return datetime(next_earnings.year, next_earnings.month, next_earnings.day)
                    
    except ImportError:
        logger.debug(f"yfinance not installed - cannot fetch earnings for {ticker}")
    except Exception as e:
        logger.debug(f"Could not fetch earnings date for {ticker} from Yahoo Finance: {e}")
    
    return None


def get_earnings_date_schwab(ticker: str, client) -> Optional[datetime]:
    """
    Fetch next earnings date for ticker from Schwab API.
    
    NOTE: Currently not implemented. Schwab API fundamental data
    may not include earnings dates. Use Yahoo Finance instead.
    
    Args:
        ticker: Stock ticker symbol
        client: Schwab API client instance
        
    Returns:
        datetime of next earnings, or None if unavailable
    """
    # Schwab API does not reliably provide earnings dates
    # Keep this stub for future implementation if Schwab adds this data
    return None


def get_earnings_date_static(ticker: str, calendar_df: pd.DataFrame) -> Optional[datetime]:
    """
    Lookup next earnings date from static calendar file.
    
    Args:
        ticker: Stock ticker symbol
        calendar_df: DataFrame with columns [ticker, earnings_date]
        
    Returns:
        datetime of next earnings, or None if not found
    """
    try:
        matches = calendar_df[calendar_df['ticker'] == ticker]
        if not matches.empty:
            earnings_str = matches.iloc[0]['earnings_date']
            return pd.to_datetime(earnings_str)
    except Exception as e:
        logger.debug(f"Could not lookup earnings for {ticker}: {e}")
    
    return None


def load_static_earnings_calendar() -> Optional[pd.DataFrame]:
    """
    Load static earnings calendar from CSV file.
    
    Expected format:
        ticker,earnings_date
        AAPL,2026-01-28
        MSFT,2026-01-29
        ...
        
    Returns:
        DataFrame or None if file not found
    """
    calendar_path = 'data/earnings_calendar.csv'
    
    if os.path.exists(calendar_path):
        try:
            df = pd.read_csv(calendar_path)
            df['earnings_date'] = pd.to_datetime(df['earnings_date'])
            logger.info(f"Loaded static earnings calendar: {len(df)} tickers")
            return df
        except Exception as e:
            logger.warning(f"Failed to load earnings calendar: {e}")
    
    return None


def compute_days_to_earnings(
    ticker: str,
    snapshot_date: datetime,
    client=None,
    static_calendar: Optional[pd.DataFrame] = None
) -> Optional[int]:
    """
    Compute days from snapshot_date to next earnings date.
    
    Args:
        ticker: Stock ticker symbol
        snapshot_date: Date of market snapshot
        client: Optional Schwab API client (not used, kept for compatibility)
        static_calendar: Optional static calendar DataFrame
        
    Returns:
        Days to earnings (0+ if before earnings, negative if after)
        None if earnings date unknown
        
    Logic:
        1. Try Yahoo Finance (primary source)
        2. Try static calendar (fallback for testing)
        3. Return None (unknown = informational only, no blocking)
    """
    earnings_date = None
    
    # Priority 1: Yahoo Finance
    earnings_date = get_earnings_date_yfinance(ticker)
    
    # Priority 2: Static calendar (fallback)
    if earnings_date is None and static_calendar is not None:
        earnings_date = get_earnings_date_static(ticker, static_calendar)
    
    # Compute days
    if earnings_date:
        delta = (earnings_date.date() - snapshot_date.date()).days
        return delta
    
    return None


def add_earnings_proximity(
    df: pd.DataFrame,
    snapshot_date: datetime,
    client=None
) -> pd.DataFrame:
    """
    Add earnings proximity data to DataFrame for INFORMATIONAL purposes only.
    
    Adds columns:
        - days_to_earnings: int or None (days until next earnings)
        - earnings_proximity_flag: bool (True if within 7 days - INFORMATIONAL ONLY)
        
    NOTE: This is for user awareness/education. Does NOT block trades.
    
    Args:
        df: DataFrame with 'Ticker' column
        snapshot_date: Date of market snapshot
        client: Optional Schwab API client (not used, kept for compatibility)
        
    Returns:
        DataFrame with earnings columns added
    """
    # Load static calendar (fallback for testing only)
    static_calendar = load_static_earnings_calendar()
    
    # Compute days_to_earnings for each ticker
    days_list = []
    for ticker in df['Ticker']:
        days = compute_days_to_earnings(
            ticker,
            snapshot_date,
            client=client,
            static_calendar=static_calendar
        )
        days_list.append(days)
    
    df['days_to_earnings'] = days_list
    
    # Flag: True if within 7 days of earnings (protective gate)
    df['earnings_proximity_flag'] = df['days_to_earnings'].apply(
        lambda x: (x is not None) and (0 <= x <= 7)
    )
    
    if not MANAGEMENT_SAFE_MODE:
        logger.info(
            f"Earnings proximity: {df['earnings_proximity_flag'].sum()} tickers "
            f"within 7 days ({len(df)} total)"
        )
    
    return df


def get_earnings_proximity_summary(df: pd.DataFrame) -> dict:
    """
    Generate summary statistics for earnings proximity.
    
    Args:
        df: DataFrame with earnings columns
        
    Returns:
        dict with summary stats
    """
    within_7_days = df['earnings_proximity_flag'].sum()
    unknown = df['days_to_earnings'].isna().sum()
    
    summary = {
        'total_tickers': len(df),
        'within_7_days': int(within_7_days),
        'earnings_unknown': int(unknown),
        'earnings_known': len(df) - int(unknown)
    }
    
    return summary
