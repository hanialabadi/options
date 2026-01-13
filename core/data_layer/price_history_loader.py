"""
Unified Price History Loader

Authoritative source for fetching and caching price history.
Supports Schwab-first architecture with yfinance fallback.
"""

import pandas as pd
import numpy as np
import logging
import time
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Tuple, Optional

logger = logging.getLogger(__name__)

# Cache Configuration
CACHE_DIR = Path("data/cache/price_history")
CACHE_TTL_HOURS = 24

# Known ETFs to avoid yfinance fundamental 404s
ETFS = {'SPY', 'QQQ', 'IWM', 'DIA', 'TLT', 'GLD', 'SLV', 'XLE', 'XLF', 'XLI', 'XLK', 'XLU', 'XLP', 'XLV', 'XLY', 'XLB', 'XLC', 'XRE', 'SMH'}

def get_cache_path(ticker: str) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"{ticker.upper()}.csv"

def is_cache_valid(ticker: str, ttl_hours: int = CACHE_TTL_HOURS) -> bool:
    path = get_cache_path(ticker)
    if not path.exists():
        return False
    age = (datetime.now() - datetime.fromtimestamp(path.stat().st_mtime)).total_seconds() / 3600
    return age < ttl_hours

def load_price_history(ticker: str, days: int = 180, client=None, use_cache: bool = True) -> Tuple[Optional[pd.DataFrame], str]:
    """
    Authoritative price history loader.
    1. Check cache
    2. Try Schwab (if client provided)
    3. Try yfinance (if not ETF)
    """
    ticker = ticker.upper()
    cache_path = get_cache_path(ticker)
    
    if use_cache and is_cache_valid(ticker):
        try:
            df = pd.read_csv(cache_path, index_col=0, parse_dates=True)
            if len(df) >= 30:
                return df.tail(days), "CACHE"
        except Exception as e:
            logger.debug(f"Cache read failed for {ticker}: {e}")

    # Try Schwab
    if client:
        try:
            # This assumes client has get_price_history matching Schwab API
            response = client.get_price_history(
                symbol=ticker,
                periodType="day",
                period=1,
                frequencyType="daily",
                frequency=1
            )
            if response and 'candles' in response and response['candles']:
                df = pd.DataFrame(response['candles'])
                df = df.rename(columns={
                    'open': 'Open', 'high': 'High', 'low': 'Low', 
                    'close': 'Close', 'volume': 'Volume'
                })
                if 'datetime' in df.columns:
                    df['datetime'] = pd.to_datetime(df['datetime'], unit='ms')
                    df = df.set_index('datetime')
                
                if len(df) >= 30:
                    df.to_csv(cache_path)
                    return df.tail(days), "SCHWAB"
        except Exception as e:
            logger.debug(f"Schwab fetch failed for {ticker}: {e}")

    # Try yfinance (Skip ETFs for fundamentals, but history is usually OK)
    # However, to be safe and avoid 404s in logs, we can use it as last resort
    try:
        import yfinance as yf
        # auto_adjust=True to match Schwab's adjusted prices
        df = yf.download(ticker, period='1y', interval='1d', progress=False, auto_adjust=True)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [col[0] for col in df.columns]
            
        if not df.empty and len(df) >= 30:
            df.to_csv(cache_path)
            return df.tail(days), "YFINANCE"
    except Exception as e:
        logger.debug(f"yfinance fetch failed for {ticker}: {e}")

    return None, "FAILED"
