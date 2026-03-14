"""
Step 0: Generate Live IV/HV Snapshot from Schwab Trader API

PURPOSE:
    Replace manual CSV snapshots with live data from Schwab API.
    Computes Historical Volatility (HV) locally from price history.
    Derives proxy Implied Volatility (IV) from ATM options (30-45 DTE).
    Outputs snapshot matching Step 2's expected schema exactly.

DESIGN PRINCIPLES:
    - Scale to 500+ tickers safely
    - Batch quote requests (100 symbols per call)
    - Cache price history (daily granularity)
    - Throttle IV calls (one lightweight chain fetch per ticker)
    - No full option chain pulls
    - No strategy logic (pure data acquisition)

CONTRACT:
    Input:  core/scraper/tickers.csv (single 'symbol' column)
    Output: data/snapshots/ivhv_snapshot_live_YYYYMMDD.csv
    
    Output schema must match Step 2 expectations:
        - Ticker (not symbol)
        - timestamp
        - last_price, volume (from quotes)
        - iv_30d (proxy from ATM options)
        - hv_10, hv_20, hv_30, hv_60, hv_90 (computed locally)
        - IV_*_Call columns (multi-timeframe IV)
        - HV_*_Cur columns (multi-timeframe HV)

RATE LIMITING:
    - Quotes: Batched (100 symbols/request)
    - Price history: Cached daily per ticker
    - Option chains: Throttled (1 req/sec, skip if unavailable)
    - Total API calls: ~N/100 (quotes) + N (history) + N (chains) for N tickers
    - With caching: ~N/100 (quotes) + N (chains) on subsequent runs

ERROR HANDLING:
    - Missing data: Log warning, continue with partial dataset
    - API errors: Retry once, then skip ticker
    - No silent failures: All errors logged explicitly
    - Output CSV written even with partial results
"""

import os
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
import requests
import shutil

# Add project root to path for imports
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from scan_engine.loaders.schwab_api_client import SchwabClient
from core.shared.data_contracts.config import TICKER_UNIVERSE_PATH, SCAN_SNAPSHOT_DIR, PRICE_CACHE_DIR, PROJECT_ROOT
from scan_engine.iv_collector.rest_collector import IVRestCollector, collect_iv_surface, iv_collected_today
from core.shared.data_layer.duckdb_utils import get_domain_connection, DbDomain

logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURATION
# ============================================================

# API Configuration
SCHWAB_API_BASE = "https://api.schwabapi.com"
BATCH_SIZE_QUOTES = 100  # Schwab allows up to 100 symbols per quotes request
CHAIN_THROTTLE_SECONDS = 1.0  # Rate limit for chain fetches
RETRY_DELAY_SECONDS = 2.0
MAX_RETRIES = 1

# Cache Configuration
CACHE_DIR = PRICE_CACHE_DIR
CACHE_TTL_HOURS = 24  # Reuse cached price history if <24 hours old

# Output Configuration
SNAPSHOT_DIR = SCAN_SNAPSHOT_DIR

# HV Windows (in trading days)
HV_WINDOWS = [10, 20, 30, 60, 90, 120, 150, 180]

# Volatility Regime Thresholds
HV_LOW_THRESHOLD = 15.0  # HV < 15% = Low volatility
HV_HIGH_THRESHOLD = 40.0  # HV > 40% = High volatility
HV_COMPRESSION_THRESHOLD = 5.0  # |hv_10 - hv_30| < 5 = Compression
HV_EXPANSION_THRESHOLD = 10.0  # |hv_10 - hv_30| > 10 = Expansion

# Reliability & Scale Configuration
CHUNK_SIZE = 25         # Process tickers in chunks of 25
CHUNK_SLEEP = 0.5       # Sleep 0.5s between chunks
RETRY_MAX_ATTEMPTS = 2  # Max retries for price history (was 3 — 3×30s = 94s worst case per ticker)
RETRY_BACKOFF = [0.5, 1.0]  # Exponential backoff (matches RETRY_MAX_ATTEMPTS)
CHUNK_WALL_CLOCK_LIMIT = 90  # seconds — abandon chunk if it takes longer than this

# IV Timeframes (in calendar days)
IV_TIMEFRAMES = [7, 14, 21, 30, 60, 90, 120, 150, 180, 270, 360, 720, 1080]

# IV Proxy Parameters
IV_PROXY_MIN_DTE = 30
IV_PROXY_MAX_DTE = 45


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def load_ticker_universe(csv_path: Path) -> List[str]:
    """
    Load ticker symbols from CSV file.
    
    Args:
        csv_path: Path to CSV with 'symbol' or 'Ticker' column
    
    Returns:
        List of ticker symbols (uppercase, deduplicated)
    
    Raises:
        FileNotFoundError: If CSV doesn't exist
        ValueError: If CSV missing required column
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"Ticker universe file not found: {csv_path}")
    
    df = pd.read_csv(csv_path)
    
    # Try 'symbol' first, fallback to 'Ticker'
    if 'symbol' in df.columns:
        col = 'symbol'
    elif 'Ticker' in df.columns:
        col = 'Ticker'
    else:
        raise ValueError(f"CSV must contain 'symbol' or 'Ticker' column. Found: {df.columns.tolist()}")
    
    tickers = df[col].str.upper().unique().tolist()
    logger.info(f"Loaded {len(tickers)} tickers from {csv_path}")
    return tickers


def batch_tickers(tickers: List[str], batch_size: int) -> List[List[str]]:
    """Split tickers into batches for API requests."""
    return [tickers[i:i + batch_size] for i in range(0, len(tickers), batch_size)]


def get_cache_path(ticker: str) -> Path:
    """Get cache file path for ticker's price history."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"{ticker}.json"


def is_cache_valid(cache_path: Path, ttl_hours: int = CACHE_TTL_HOURS) -> bool:
    """Check if cached price history is still fresh."""
    if not cache_path.exists():
        return False
    
    age_seconds = time.time() - cache_path.stat().st_mtime
    age_hours = age_seconds / 3600
    return age_hours < ttl_hours


def save_cache(cache_path: Path, data: dict):
    """Save price history to cache."""
    with open(cache_path, 'w') as f:
        json.dump(data, f)


def load_cache(cache_path: Path) -> Optional[dict]:
    """Load price history from cache."""
    try:
        with open(cache_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Failed to load cache {cache_path}: {e}")
        return None


def calculate_log_returns(prices: pd.Series) -> pd.Series:
    """Calculate log returns from price series."""
    return np.log(prices / prices.shift(1))


def calculate_hv(prices: pd.Series, window: int) -> float:
    """
    Calculate Historical Volatility (HV) for a given window.
    
    HV = std(log returns) * sqrt(252)
    
    Args:
        prices: Price series (Close prices)
        window: Number of trading days
    
    Returns:
        Annualized HV as percentage (e.g., 25.5 for 25.5%)
        Returns NaN if insufficient data
    """
    if len(prices) < window + 1:
        return np.nan
    
    # Get last 'window' days of prices
    recent_prices = prices.tail(window + 1)
    
    # Calculate log returns
    log_returns = calculate_log_returns(recent_prices).dropna()
    
    if len(log_returns) < window:
        return np.nan
    
    # Annualized standard deviation
    std_dev = log_returns.std()
    hv_annualized = std_dev * np.sqrt(252)  # 252 trading days/year
    
    return hv_annualized * 100  # Convert to percentage


def calculate_hv_ewma(prices: pd.Series, lam: float = 0.94) -> float:
    """
    Calculate EWMA (Exponentially Weighted Moving Average) volatility.

    RiskMetrics λ=0.94: recent days weighted more heavily than rolling window.
    Reacts to vol spikes within 2-3 days vs 10-20 day lag of simple rolling HV.
    Sinclair 2020 Ch.3: EWMA is the pragmatic upgrade from rolling HV — not overfitting,
    computationally trivial, interpretable. Preferred over GARCH(1,1) for positional traders.

    Formula:
        σ²_t = λ·σ²_{t-1} + (1-λ)·r²_t
    Annualized: σ_annual = sqrt(σ²_t) * sqrt(252) * 100 (percent)

    Args:
        prices: Close price series (requires ≥ 30 days for stable initialization)
        lam: Decay factor (default 0.94, RiskMetrics standard)

    Returns:
        Current EWMA annualized volatility as percentage (e.g., 28.4 for 28.4%)
        Returns NaN if insufficient data (< 10 prices).
    """
    if len(prices) < 10:
        return np.nan

    log_returns = np.log(prices / prices.shift(1)).dropna().values

    if len(log_returns) < 5:
        return np.nan

    # Initialize with variance of first 5 returns
    variance = float(np.var(log_returns[:5]))
    if variance <= 0:
        variance = 1e-8

    # Roll EWMA forward
    for r in log_returns[5:]:
        variance = lam * variance + (1 - lam) * r * r

    return float(np.sqrt(variance * 252) * 100)


def calculate_hv_slope(hv_10: float, hv_30: float) -> float:
    """
    Calculate HV slope (short-term vs medium-term volatility).
    
    Positive slope = volatility increasing (expansion)
    Negative slope = volatility decreasing (compression)
    
    Args:
        hv_10: 10-day HV
        hv_30: 30-day HV
    
    Returns:
        HV slope (hv_10 - hv_30)
        Returns NaN if either input is NaN
    """
    if np.isnan(hv_10) or np.isnan(hv_30):
        return np.nan
    return hv_10 - hv_30


def classify_volatility_regime(
    hv_30: float,
    hv_slope: float
) -> str:
    """
    Classify volatility regime based on HV level and slope.
    
    Regime Classification:
    - Low: HV < 15% (quiet market)
    - Normal: 15% <= HV <= 40% (typical volatility)
    - High: HV > 40% (elevated volatility)
    - Compression: |slope| < 5 and HV stable (low variance)
    - Expansion: slope > 10 (volatility accelerating)
    
    Args:
        hv_30: 30-day HV (base volatility measure)
        hv_slope: HV slope (hv_10 - hv_30)
    
    Returns:
        Regime string (e.g., "Normal", "High_Expansion")
        Returns "Unknown" if data insufficient
    """
    if np.isnan(hv_30) or np.isnan(hv_slope):
        return "Unknown"
    
    # Base volatility level
    if hv_30 < HV_LOW_THRESHOLD:
        base_regime = "Low"
    elif hv_30 > HV_HIGH_THRESHOLD:
        base_regime = "High"
    else:
        base_regime = "Normal"
    
    # Volatility trend (slope)
    abs_slope = abs(hv_slope)
    
    if abs_slope < HV_COMPRESSION_THRESHOLD:
        trend = "_Compression"
    elif hv_slope > HV_EXPANSION_THRESHOLD:
        trend = "_Expansion"
    elif hv_slope < -HV_EXPANSION_THRESHOLD:
        trend = "_Contraction"
    else:
        trend = ""  # Neutral trend
    
    return base_regime + trend


# ============================================================
# API INTERACTION FUNCTIONS
# ============================================================

def is_market_open_schwab(client: SchwabClient) -> Tuple[bool, str]:
    """
    Check if market is currently open via Schwab market hours endpoint.
    
    Returns:
        Tuple of (is_open: bool, status: str)
        status: "OPEN", "CLOSED", "UNKNOWN"
    """
    try:
        token = client._get_access_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json"
        }
        
        response = requests.get(
            f"{SCHWAB_API_BASE}/marketdata/v1/markets/equity",
            headers=headers,
            timeout=10
        )
        response.raise_for_status()
        
        data = response.json()
        # Schwab returns: {"equity": {"EQ": {"isOpen": true/false}}}
        is_open = data.get('equity', {}).get('EQ', {}).get('isOpen', False)
        status = "OPEN" if is_open else "CLOSED"
        
        logger.info(f"Market status: {status}")
        return is_open, status
        
    except Exception as e:
        logger.warning(f"Failed to check market hours: {e}. Assuming OPEN.")
        return True, "UNKNOWN"  # Default to OPEN fallback order


def extract_best_price(quote_block: dict, is_open: bool) -> Tuple[Optional[float], str]:
    """
    Extract best available price from Schwab quote block with market-hours fallback.
    
    Fallback order:
    - If market OPEN:  lastPrice → mark → bidAskMid → closePrice
    - If market CLOSED: mark → closePrice → lastPrice → bidAskMid
    
    Args:
        quote_block: Schwab 'quote' object (camelCase keys)
        is_open: Whether market is currently open
    
    Returns:
        Tuple of (price: float or None, source: str)
        source: "lastPrice", "mark", "closePrice", "bidAskMid", "regularMarketLastPrice", "none"
    """
    if not quote_block or not isinstance(quote_block, dict):
        return None, "none"
    
    # Helper to check if price is valid
    def is_valid(val):
        return val is not None and not (isinstance(val, float) and np.isnan(val)) and val > 0
    
    # Extract all potential price fields (Schwab uses camelCase)
    last_price = quote_block.get('lastPrice')
    mark = quote_block.get('mark')
    close_price = quote_block.get('closePrice')
    bid = quote_block.get('bidPrice')
    ask = quote_block.get('askPrice')
    regular_last = quote_block.get('regularMarketLastPrice')  # Fallback field
    
    # Compute bid-ask midpoint if both exist
    bid_ask_mid = None
    if is_valid(bid) and is_valid(ask):
        bid_ask_mid = (bid + ask) / 2.0
    
    # Apply fallback cascade based on market status
    if is_open:
        # During market hours: prefer live data
        if is_valid(last_price):
            return float(last_price), "lastPrice"
        if is_valid(mark):
            return float(mark), "mark"
        if bid_ask_mid is not None:
            return float(bid_ask_mid), "bidAskMid"
        if is_valid(close_price):
            return float(close_price), "closePrice"
        if is_valid(regular_last):
            return float(regular_last), "regularMarketLastPrice"
    else:
        # After hours: prefer mark/close over stale lastPrice
        if is_valid(mark):
            return float(mark), "mark"
        if is_valid(close_price):
            return float(close_price), "closePrice"
        if is_valid(last_price):
            return float(last_price), "lastPrice"
        if bid_ask_mid is not None:
            return float(bid_ask_mid), "bidAskMid"
        if is_valid(regular_last):
            return float(regular_last), "regularMarketLastPrice"
    
    return None, "none"


def fetch_batch_quotes(
    client: SchwabClient,
    tickers: List[str],
    is_market_open: bool
) -> Dict[str, Dict]:
    """
    Fetch quotes for a batch of tickers (up to 100) with proper JSON extraction.
    
    Args:
        client: SchwabClient instance
        tickers: List of ticker symbols (max 100)
        is_market_open: Whether market is currently open (for fallback logic)
    
    Returns:
        Dict[ticker -> {
            'last_price': float,
            'volume': float,
            'price_source': str,
            'quote_time': int,
            'trade_time': int,
            'raw_quote': dict  # For debugging
        }]
    """
    if len(tickers) > BATCH_SIZE_QUOTES:
        logger.warning(f"Batch size {len(tickers)} exceeds limit {BATCH_SIZE_QUOTES}. Truncating.")
        tickers = tickers[:BATCH_SIZE_QUOTES]
    
    try:
        token = client._get_access_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json"
        }
        
        # Schwab accepts comma-separated symbols
        symbols = ",".join(tickers)
        
        response = requests.get(
            f"{SCHWAB_API_BASE}/marketdata/v1/quotes",
            headers=headers,
            params={"symbols": symbols, "fields": "quote"},
            timeout=30
        )
        response.raise_for_status()
        
        data = response.json()
        
        # Parse response (Schwab format: {TICKER: {quote: {...}, reference: {...}}})
        results = {}
        for ticker in tickers:
            if ticker not in data:
                logger.warning(f"❌ No quote data returned for {ticker}")
                results[ticker] = {
                    'last_price': None,
                    'volume': None,
                    'price_source': 'none',
                    'quote_time': None,
                    'trade_time': None,
                    'raw_quote': {}
                }
                continue
            
            # Extract quote block (camelCase keys from Schwab)
            quote_block = data[ticker].get('quote', {})
            
            if not quote_block:
                logger.warning(f"❌ Empty quote block for {ticker}")
                results[ticker] = {
                    'last_price': None,
                    'volume': None,
                    'price_source': 'none',
                    'quote_time': None,
                    'trade_time': None,
                    'raw_quote': {}
                }
                continue
            
            # Extract price with fallback logic
            price, source = extract_best_price(quote_block, is_market_open)
            
            # Extract timestamps (milliseconds since epoch)
            quote_time = quote_block.get('quoteTime')
            trade_time = quote_block.get('tradeTime')
            
            # Extract volume
            volume = quote_block.get('totalVolume')
            if volume is not None and not np.isnan(volume):
                volume = float(volume)
            else:
                volume = None
            
            # ENHANCEMENT: Extract additional quote fields for entry quality analysis
            # (Per SCHWAB_API_DATA_INVENTORY.md recommendations)
            high_price = quote_block.get('highPrice')
            low_price = quote_block.get('lowPrice')
            open_price = quote_block.get('openPrice')
            close_price = quote_block.get('closePrice')
            high_52w = quote_block.get('52WeekHigh')
            low_52w = quote_block.get('52WeekLow')
            net_change = quote_block.get('netChange')
            net_pct_change = quote_block.get('netPercentChange')
            dividend_date = quote_block.get('dividendDate')
            dividend_yield = quote_block.get('dividendYield')
            
            results[ticker] = {
                'last_price': price,
                'volume': volume,
                'price_source': source,
                'quote_time': quote_time,
                'trade_time': trade_time,
                # Entry quality fields (for scan-time analysis)
                'highPrice': high_price,
                'lowPrice': low_price,
                'openPrice': open_price,
                'closePrice': close_price,
                '52WeekHigh': high_52w,
                '52WeekLow': low_52w,
                'netChange': net_change,
                'netPercentChange': net_pct_change,
                'dividendDate': dividend_date,
                'dividendYield': dividend_yield,
                'raw_quote': quote_block  # Keep for debugging
            }
            
            if price is None:
                logger.warning(f"⚠️  No valid price for {ticker} (tried all fallbacks)")
        
        valid_count = sum(1 for r in results.values() if r['last_price'] is not None)
        logger.info(f"✅ Fetched quotes for {len(results)} tickers ({valid_count} with valid prices)")
        return results
        
    except Exception as e:
        logger.error(f"❌ Failed to fetch batch quotes: {e}")
        # Return None results for failed tickers
        return {
            ticker: {
                'last_price': None,
                'volume': None,
                'price_source': 'none',
                'quote_time': None,
                'trade_time': None,
                'raw_quote': {}
            } for ticker in tickers
        }


def fetch_all_quotes(
    client: SchwabClient,
    tickers: List[str]
) -> Tuple[Dict[str, Dict], bool, str]:
    """
    Fetch quotes for all tickers (batched) with market-hours detection.
    
    Args:
        client: SchwabClient instance
        tickers: List of all ticker symbols
    
    Returns:
        Tuple of (
            quotes_dict: Dict[ticker -> {last_price, volume, price_source, ...}],
            is_market_open: bool,
            market_status: str ("OPEN", "CLOSED", "UNKNOWN")
        )
    """
    # Check market hours once per run
    is_market_open, market_status = is_market_open_schwab(client)
    
    all_quotes = {}
    batches = batch_tickers(tickers, BATCH_SIZE_QUOTES)
    
    logger.info(f"Fetching quotes for {len(tickers)} tickers in {len(batches)} batches...")
    logger.info(f"Market status: {market_status} (using {'OPEN' if is_market_open else 'CLOSED'} fallback order)")
    
    for i, batch in enumerate(batches, 1):
        logger.info(f"Batch {i}/{len(batches)}: {len(batch)} tickers")
        batch_quotes = fetch_batch_quotes(client, batch, is_market_open)
        all_quotes.update(batch_quotes)
        
        # Small delay between batches
        if i < len(batches):
            time.sleep(0.5)
    
    valid_count = sum(1 for r in all_quotes.values() if r['last_price'] is not None)
    logger.info(f"✅ Completed quote fetching: {len(all_quotes)} tickers ({valid_count} valid, {len(all_quotes) - valid_count} missing)")
    
    return all_quotes, is_market_open, market_status


def fetch_price_history_with_retry(
    client: SchwabClient,
    ticker: str,
    use_cache: bool = True
) -> tuple[Optional[pd.DataFrame], str]:
    """
    Fetch daily price history for a ticker with retry + backoff.
    
    Retry Logic:
    - Max 3 attempts
    - Exponential backoff: 0.5s → 1s → 2s
    - Catches: timeouts, HTTP 429, network errors
    
    Args:
        client: SchwabClient instance
        ticker: Ticker symbol
        use_cache: Whether to use cached data if available
    
    Returns:
        Tuple of (DataFrame or None, status_string)
        Status: OK, TIMEOUT, RATE_LIMIT, AUTH_ERROR, INSUFFICIENT_DATA, UNKNOWN
    """
    cache_path = get_cache_path(ticker)
    
    # Try cache first
    if use_cache and is_cache_valid(cache_path):
        cached_data = load_cache(cache_path)
        if cached_data:
            logger.debug(f"📦 Cache HIT: {ticker}")
            df = pd.DataFrame(cached_data)
            df['date'] = pd.to_datetime(df['date'])
            return df, "OK"
    
    # Fetch from API with retry
    for attempt in range(RETRY_MAX_ATTEMPTS):
        try:
            token = client._get_access_token()
            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json"
            }
            
            # Fetch 180 trading days (~9 months)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=270)  # Extra buffer for weekends/holidays
            
            params = {
                "symbol": ticker,
                "periodType": "year",
                "frequencyType": "daily",
                "frequency": 1,
                "startDate": int(start_date.timestamp() * 1000),  # Schwab expects milliseconds
                "endDate": int(end_date.timestamp() * 1000),
                "needExtendedHoursData": False,
                "needPreviousClose": False
            }
            
            response = requests.get(
                f"{SCHWAB_API_BASE}/marketdata/v1/pricehistory",
                headers=headers,
                params=params,
                timeout=(5, 20)  # (connect_timeout, read_timeout) — was flat 30s
                # connect: 5s to establish TCP; read: 20s for body.
                # A stalled mid-stream response is caught by read_timeout, not connect_timeout.
            )
            response.raise_for_status()
            
            data = response.json()
            
            # Parse Schwab price history format
            candles = data.get('candles', [])
            if not candles:
                logger.debug(f"No price history for {ticker}")
                return None, "INSUFFICIENT_DATA"
            
            # Convert to DataFrame
            df = pd.DataFrame([{
                'date': datetime.fromtimestamp(c['datetime'] / 1000),
                'close': c['close'],
                'high': c['high'],
                'low': c['low'],
                'open': c['open'],
                'volume': c['volume']
            } for c in candles])
            
            # Save to cache (convert datetime to string for JSON serialization)
            cache_data = df.copy()
            cache_data['date'] = cache_data['date'].astype(str)
            save_cache(cache_path, cache_data.to_dict('records'))
            logger.debug(f"✅ Fetched & cached: {ticker} ({len(df)} days)")
            
            return df, "OK"
            
        except requests.exceptions.Timeout:
            status = "TIMEOUT"
            if attempt < RETRY_MAX_ATTEMPTS - 1:
                backoff = RETRY_BACKOFF[attempt]
                logger.debug(f"Timeout for {ticker}, retry {attempt+1}/{RETRY_MAX_ATTEMPTS} after {backoff}s")
                time.sleep(backoff)
            else:
                logger.debug(f"Timeout for {ticker} after {RETRY_MAX_ATTEMPTS} attempts")
                return None, status
                
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                status = "RATE_LIMIT"
                if attempt < RETRY_MAX_ATTEMPTS - 1:
                    backoff = RETRY_BACKOFF[attempt]
                    logger.debug(f"Rate limit for {ticker}, retry {attempt+1}/{RETRY_MAX_ATTEMPTS} after {backoff}s")
                    time.sleep(backoff)
                else:
                    logger.debug(f"Rate limit for {ticker} after {RETRY_MAX_ATTEMPTS} attempts")
                    return None, status
            elif e.response.status_code == 401:
                logger.debug(f"Auth error for {ticker}")
                return None, "AUTH_ERROR"
            else:
                logger.debug(f"HTTP error {e.response.status_code} for {ticker}")
                return None, "UNKNOWN"
                
        except Exception as e:
            status = "UNKNOWN"
            if attempt < RETRY_MAX_ATTEMPTS - 1:
                backoff = RETRY_BACKOFF[attempt]
                logger.debug(f"Error for {ticker}: {e}, retry {attempt+1}/{RETRY_MAX_ATTEMPTS} after {backoff}s")
                time.sleep(backoff)
            else:
                logger.debug(f"Failed to fetch {ticker} after {RETRY_MAX_ATTEMPTS} attempts: {e}")
                return None, status
    
    return None, "UNKNOWN"


def calculate_all_hv(prices: pd.Series) -> Dict[str, float]:
    """
    Calculate HV for all required windows.
    
    Args:
        prices: Close price series
    
    Returns:
        Dict[window -> HV value] (e.g., {10: 25.3, 20: 28.1, ...})
    """
    hv_values = {}
    for window in HV_WINDOWS:
        hv_values[window] = calculate_hv(prices, window)
    return hv_values


def fetch_iv_proxy(
    client: SchwabClient,
    ticker: str,
    last_price: float
) -> Dict[str, float]:
    """
    Fetch proxy IV from ATM options (nearest 30-45 DTE expiry).
    
    Industry-standard proxy method:
    1. Fetch option chain
    2. Find nearest expiry between 30-45 DTE
    3. Find ATM strike (closest to last_price)
    4. Average call IV + put IV at ATM
    5. Result -> iv_30d proxy
    
    Also extracts multi-timeframe IV if available (7D, 60D, 90D, etc.).
    
    Args:
        client: SchwabClient instance
        ticker: Ticker symbol
        last_price: Current stock price (for ATM calculation)
    
    Returns:
        Dict with IV values for multiple timeframes:
        {
            'iv_30d': float,  # Primary proxy (30-45 DTE ATM)
            'iv_7d': float,   # Optional short-term
            'iv_60d': float,  # Optional medium-term
            ...
        }
        Returns NaN for unavailable timeframes
    """
    try:
        token = client._get_access_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json"
        }
        
        # Wide chain fetch: covers all IV timeframes from 7D through 360D.
        # NTM range keeps payload small despite the wider date window.
        # 720D/1080D (LEAPS) are outside this window and remain NaN.
        params = {
            "symbol": ticker,
            "contractType": "ALL",
            "includeQuotes": True,
            "strategy": "SINGLE",
            "range": "NTM",
            "fromDate": (datetime.now() + timedelta(days=5)).strftime("%Y-%m-%d"),
            "toDate": (datetime.now() + timedelta(days=400)).strftime("%Y-%m-%d")
        }

        response = requests.get(
            f"{SCHWAB_API_BASE}/marketdata/v1/chains",
            headers=headers,
            params=params,
            timeout=30
        )
        response.raise_for_status()

        data = response.json()

        call_map = data.get('callExpDateMap', {})
        put_map = data.get('putExpDateMap', {})

        if not call_map and not put_map:
            logger.warning(f"No option data for {ticker}")
            return _empty_iv_dict()

        if last_price is None or (isinstance(last_price, float) and np.isnan(last_price)) or last_price <= 0:
            logger.warning(f"Invalid last_price for {ticker}: {last_price}")
            return _empty_iv_dict()

        # Collect all available expirations and their DTEs (Schwab format: "2025-01-17:30")
        expiry_dte_map: Dict[str, int] = {}
        for date_str in set(list(call_map.keys()) + list(put_map.keys())):
            date_part = date_str.split(':')[0]
            try:
                expiry_date = datetime.strptime(date_part, "%Y-%m-%d")
                dte = (expiry_date - datetime.now()).days
                if dte >= 5:
                    expiry_dte_map[date_str] = dte
            except ValueError:
                continue

        if not expiry_dte_map:
            logger.warning(f"No valid expirations for {ticker}")
            return _empty_iv_dict()

        # Compute ATM strike globally from all available strikes across all expirations
        all_strike_floats: set = set()
        for date_str in expiry_dte_map:
            for strike_str in list(call_map.get(date_str, {}).keys()) + list(put_map.get(date_str, {}).keys()):
                try:
                    all_strike_floats.add(float(strike_str))
                except ValueError:
                    pass

        if not all_strike_floats:
            logger.warning(f"No strikes found for {ticker}")
            return _empty_iv_dict()

        atm_strike = min(all_strike_floats, key=lambda x: abs(x - last_price))

        # Schwab strike keys may be "150", "150.0", or "152.5" — try both formats
        atm_strike_candidates = {f"{atm_strike:.1f}", str(int(atm_strike)) if atm_strike == int(atm_strike) else f"{atm_strike}"}

        def _lookup_strike(strikes_map):
            for key in atm_strike_candidates:
                if key in strikes_map:
                    return strikes_map[key]
            # Last resort: find the key whose float value matches
            for key, val in strikes_map.items():
                try:
                    if float(key) == atm_strike:
                        return val
                except ValueError:
                    pass
            return None

        def _parse_volatility(val):
            """Return NaN for missing, zero, or Schwab sentinel (-999.0) values."""
            if val is None:
                return np.nan
            try:
                v = float(val)
            except (TypeError, ValueError):
                return np.nan
            return np.nan if v <= 0 else v

        def _extract_iv_for_expiry(date_str: str) -> Tuple[float, float]:
            """Return (call_iv, put_iv) at ATM for a single expiration."""
            call_opts = _lookup_strike(call_map.get(date_str, {}))
            put_opts  = _lookup_strike(put_map.get(date_str, {}))
            c_iv = _parse_volatility(call_opts[0].get('volatility') if call_opts else None)
            p_iv = _parse_volatility(put_opts[0].get('volatility') if put_opts else None)
            return c_iv, p_iv

        # For each IV timeframe, find the nearest available expiry within tolerance
        result = _empty_iv_dict()
        for tf_days in IV_TIMEFRAMES:
            best_expiry = min(expiry_dte_map, key=lambda e: abs(expiry_dte_map[e] - tf_days))
            actual_dte = expiry_dte_map[best_expiry]
            # Accept the expiry only if it is within 50% of the target or 14 days, whichever is larger
            tolerance = max(tf_days * 0.5, 14)
            if abs(actual_dte - tf_days) > tolerance:
                continue
            c_iv, p_iv = _extract_iv_for_expiry(best_expiry)
            if not np.isnan(c_iv):
                result[f'iv_{tf_days}d_call'] = c_iv
            if not np.isnan(p_iv):
                result[f'iv_{tf_days}d_put'] = p_iv
            # Average for the primary iv_{tf}d key
            if not np.isnan(c_iv) and not np.isnan(p_iv):
                result[f'iv_{tf_days}d'] = (c_iv + p_iv) / 2
            elif not np.isnan(c_iv):
                result[f'iv_{tf_days}d'] = c_iv
            elif not np.isnan(p_iv):
                result[f'iv_{tf_days}d'] = p_iv
            else:
                continue
            logger.debug(f"  {ticker} iv_{tf_days}d (DTE={actual_dte}): call={c_iv:.2f} put={p_iv:.2f}")

        iv_30d = result.get('iv_30d', np.nan)
        if not np.isnan(iv_30d):
            logger.debug(f"✅ IV proxy for {ticker}: iv_30d={iv_30d:.2f}% (ATM strike={atm_strike})")
        else:
            logger.warning(f"No valid iv_30d for {ticker} (ATM strike={atm_strike})")

        return result
        
    except Exception as e:
        logger.warning(f"Failed to fetch IV for {ticker}: {e}")
        return _empty_iv_dict()


def _empty_iv_dict() -> Dict[str, float]:
    """Return empty IV dict with all timeframes as NaN (average, call leg, put leg)."""
    tfs = [7, 14, 21, 30, 60, 90, 120, 150, 180, 270, 360, 720, 1080]
    d: Dict[str, float] = {}
    for tf in tfs:
        d[f'iv_{tf}d']      = np.nan  # call+put average
        d[f'iv_{tf}d_call'] = np.nan  # call leg only
        d[f'iv_{tf}d_put']  = np.nan  # put leg only
    return d


# ============================================================
# MAIN PIPELINE
# ============================================================

def generate_live_snapshot(
    client: SchwabClient,
    tickers: List[str],
    use_cache: bool = True,
    fetch_iv: bool = True,
    discovery_mode: bool = False,
    skip_iv_if_collected_today: bool = True,
) -> pd.DataFrame:
    """
    Generate live IV/HV snapshot for all tickers.

    Pipeline:
    1. Token pre-flight validation (abort early if expired)
    2. Fetch batch quotes (last_price, volume)
    3. Fetch price history per ticker (chunked, retry + backoff)
    4. Compute HV locally (10D, 20D, 30D, 60D, 90D)
    5. Fetch IV proxy from ATM options (30-45 DTE) — skipped if already
       collected today and skip_iv_if_collected_today=True
    6. Assemble into DataFrame with diagnostic columns

    Args:
        client: SchwabClient instance
        tickers: List of ticker symbols
        use_cache: Whether to use cached price history
        fetch_iv: Whether to fetch IV (slow, can be disabled for testing)
        discovery_mode: If True, prunes IV fetching to high-interest tickers
        skip_iv_if_collected_today: If True (default), skip REST IV collection
            when iv_term_history already has rows for today. Saves ~200-400s
            on second/subsequent same-day pipeline runs. Set False to force
            re-collection (e.g. after market-hours correction).

    Returns:
        DataFrame with Step 2-compatible schema + diagnostic columns
    """
    logger.info("="*80)
    logger.info(f"🚀 STEP 0: Live Snapshot Generation")
    logger.info(f"   Tickers: {len(tickers)}")
    logger.info(f"   Chunking: {CHUNK_SIZE} tickers/chunk")
    logger.info(f"   Retry: {RETRY_MAX_ATTEMPTS} attempts with backoff")
    logger.info("="*80)
    start_time = time.time()
    
    # CRITICAL: Token pre-flight validation
    try:
        client.ensure_valid_token()
    except Exception as e:
        logger.error(f"❌ Token pre-flight validation FAILED: {e}")
        logger.error("Snapshot generation aborted. Please re-authenticate.")
        raise
    
    # Step 1: Fetch all quotes (batched, fast)
    logger.info("\n📊 Step 1/4: Fetching quotes...")
    quotes, is_market_open, market_status = fetch_all_quotes(client, tickers)
    logger.info(f"✅ Quotes fetched: {len(quotes)}/{len(tickers)} | Market: {market_status}")
    
    # Step 2: Fetch price history and compute HV (chunked with retry)
    logger.info(f"\n📈 Step 2/4: Fetching price history & computing HV (chunked)...")
    hv_data = {}
    history_status = {}  # Diagnostic: track fetch status per ticker
    
    # Process in chunks
    chunks = [tickers[i:i+CHUNK_SIZE] for i in range(0, len(tickers), CHUNK_SIZE)]
    total_chunks = len(chunks)
    
    for chunk_idx, chunk in enumerate(chunks, 1):
        logger.info(f"  Chunk {chunk_idx}/{total_chunks}: Processing {len(chunk)} tickers...")
        chunk_start = time.time()

        chunk_failures = []
        for ticker in chunk:
            # Per-chunk wall-clock guard: if this chunk has already burned its budget,
            # skip remaining tickers and mark them as SKIPPED.  Prevents one stuck
            # ticker from blocking all subsequent chunks.
            if time.time() - chunk_start > CHUNK_WALL_CLOCK_LIMIT:
                logger.warning(
                    f"  ⏱️  Chunk {chunk_idx} wall-clock limit ({CHUNK_WALL_CLOCK_LIMIT}s) reached — "
                    f"skipping {ticker} and remaining chunk tickers"
                )
                hv_data[ticker] = {window: np.nan for window in HV_WINDOWS}
                history_status[ticker] = "SKIPPED_TIMEOUT"
                chunk_failures.append(f"{ticker}:SKIPPED_TIMEOUT")
                continue

            price_df, status = fetch_price_history_with_retry(client, ticker, use_cache=use_cache)
            history_status[ticker] = status

            if price_df is None or len(price_df) < 90:
                hv_data[ticker] = {window: np.nan for window in HV_WINDOWS}
                hv_data[ticker]['ewma'] = np.nan
                if status == "OK":
                    history_status[ticker] = "INSUFFICIENT_DATA"
                chunk_failures.append(f"{ticker}:{status}")
            else:
                # Compute HV for all windows + EWMA
                hv_data[ticker] = calculate_all_hv(price_df['close'])
                hv_data[ticker]['ewma'] = calculate_hv_ewma(price_df['close'])

        elapsed = time.time() - chunk_start
        logger.info(f"  Chunk {chunk_idx} done in {elapsed:.1f}s")

        # Log chunk failures
        if chunk_failures:
            logger.debug(f"    Chunk {chunk_idx} failures: {', '.join(chunk_failures[:5])}" +
                        (f" (+{len(chunk_failures)-5} more)" if len(chunk_failures) > 5 else ""))

        # Sleep between chunks (rate limit mitigation)
        if chunk_idx < total_chunks:
            time.sleep(CHUNK_SLEEP)
    
    # Summary statistics
    status_counts = {}
    for status in history_status.values():
        status_counts[status] = status_counts.get(status, 0) + 1
    
    hv_computed = sum(1 for ticker in hv_data if not np.isnan(list(hv_data[ticker].values())[0]))
    logger.info(f"\n✅ HV Processing Complete:")
    logger.info(f"   Computed: {hv_computed}/{len(tickers)} ({100*hv_computed/len(tickers):.1f}%)")
    logger.info(f"   Status breakdown:")
    for status, count in sorted(status_counts.items()):
        logger.info(f"     {status}: {count}")
    
    # Step 3: Fetch IV proxy (slow, throttled)
    iv_data = {ticker: _empty_iv_dict() for ticker in tickers}

    # Layer 1B: Auto-skip IV collection if already persisted today
    _iv_already_collected = False
    if fetch_iv and skip_iv_if_collected_today and iv_collected_today():
        _iv_already_collected = True
        logger.info(
            "[IV_SKIP] IV already collected today — skipping REST collection. "
            "Step 2 will read from iv_term_history as usual. "
            "Use skip_iv_if_collected_today=False to force re-collection."
        )

    if fetch_iv and not _iv_already_collected:
        # Discovery Mode: Prune tickers to high-interest only
        iv_tickers = tickers
        if discovery_mode:
            iv_tickers = []
            for t in tickers:
                hv_30 = hv_data.get(t, {}).get(30, 0)
                net_pct = quotes.get(t, {}).get('netPercentChange', 0)
                if pd.isna(net_pct): net_pct = 0

                # High interest = High Vol (HV30 > 25%) OR High Momentum (|change| > 1.5%)
                if hv_30 > 25.0 or abs(net_pct) > 1.5:
                    iv_tickers.append(t)

            logger.info(f"🔭 Discovery Mode: Pruned IV fetch from {len(tickers)} to {len(iv_tickers)} high-interest tickers")

        logger.info(f"🔍 Step 3/4: Fetching IV surface for {len(iv_tickers)} tickers via IVRestCollector...")

        # Build spot_map from quotes
        spot_map = {
            t: quotes[t]['last_price']
            for t in iv_tickers
            if quotes.get(t, {}).get('last_price') is not None
        }

        collector = IVRestCollector(client, write_to_db=True)
        iv_result = collector.collect(
            iv_tickers,
            spot_map,
            force_run=True,   # step0 already gates on market hours upstream
        )

        # Merge surface results back into iv_data dict (keyed by ticker)
        if not iv_result.df.empty:
            for _, row in iv_result.df.iterrows():
                t = row['ticker']
                entry = _empty_iv_dict()
                for b in [7, 14, 30, 60, 90, 120, 180, 360]:
                    val = row.get(f'iv_{b}d')
                    if val is not None and not (isinstance(val, float) and np.isnan(val)):
                        # Surface uses call-side only — fill both call and average cols
                        entry[f'iv_{b}d']      = val
                        entry[f'iv_{b}d_call'] = val
                iv_data[t] = entry

        logger.info(
            f"✅ IV surface complete: {iv_result.success_count} succeeded, "
            f"{len(iv_result.failed)} failed, {len(iv_result.skipped)} skipped"
        )
    else:
        logger.info("⏭️  Step 3/4: Skipping IV fetch (fetch_iv=False)")
    
    # Step 4: Assemble DataFrame
    logger.info("📦 Step 4/4: Assembling snapshot DataFrame...")
    
    rows = []
    
    # --- CRITICAL FIX: Derive snapshot_ts from broker data, not system time ---
    # Prioritize trade_time, then quote_time from the first available quote
    broker_snapshot_ts = None
    for ticker in tickers:
        quote = quotes.get(ticker, {})
        trade_time_ms = quote.get('trade_time')
        quote_time_ms = quote.get('quote_time')

        if trade_time_ms:
            broker_snapshot_ts = datetime.fromtimestamp(trade_time_ms / 1000)
            break
        elif quote_time_ms:
            broker_snapshot_ts = datetime.fromtimestamp(quote_time_ms / 1000)
            break
    
    if broker_snapshot_ts is None:
        raise ValueError(
            "❌ CRITICAL: Could not derive a deterministic snapshot timestamp from broker data. "
            "No valid trade_time or quote_time found in any fetched quotes. "
            "Step 0 requires a broker-derived canonical timestamp."
        )
    
    snapshot_ts = broker_snapshot_ts
    snapshot_ts_ms = int(snapshot_ts.timestamp() * 1000) # For age calculation
    logger.info(f"✅ Derived canonical snapshot_ts from broker data: {snapshot_ts}")
    # --- END CRITICAL FIX ---
    
    for ticker in tickers:
        quote = quotes[ticker]
        hv = hv_data.get(ticker, {})
        iv = iv_data.get(ticker, _empty_iv_dict())
        
        # Calculate derived volatility metrics
        hv_10 = hv.get(10, np.nan)
        hv_30 = hv.get(30, np.nan)
        hv_slope = calculate_hv_slope(hv_10, hv_30)
        volatility_regime = classify_volatility_regime(hv_30, hv_slope)
        
        # Diagnostic: HV status (COMPUTED if HV present, else FETCH_FAILED)
        hist_status = history_status.get(ticker, "UNKNOWN")
        if hist_status == "OK" and not np.isnan(hv_30):
            hv_status = "COMPUTED"
        elif hist_status == "INSUFFICIENT_DATA":
            hv_status = "INSUFFICIENT_DATA"
        else:
            hv_status = "FETCH_FAILED"
        
        # Calculate quote age (quote_time and trade_time are in milliseconds)
        quote_time = quote.get('quote_time')
        trade_time = quote.get('trade_time')
        
        # Prefer trade_time for age calculation, fallback to quote_time
        if trade_time:
            quote_age_sec = (snapshot_ts_ms - trade_time) / 1000.0
        elif quote_time:
            quote_age_sec = (snapshot_ts_ms - quote_time) / 1000.0
        else:
            quote_age_sec = None
        
        row = {
            # Primary identifier (Step 2 expects 'Ticker', not 'symbol')
            'Ticker': ticker,
            'timestamp': snapshot_ts,
            'Date': snapshot_ts.date(),
            'Error': '',  # Placeholder for compatibility
            'data_source': 'schwab',  # Required for tracking
            
            # Quote data (NEW: price_source, quote_time, trade_time, quote_age_sec, is_market_open)
            'last_price': quote['last_price'],
            'volume': quote['volume'],
            'price_source': quote.get('price_source', 'none'),  # NEW: "lastPrice", "mark", "closePrice", "bidAskMid", "none"
            'quote_time': quote_time,  # NEW: milliseconds epoch
            'trade_time': trade_time,  # NEW: milliseconds epoch
            'quote_age_sec': quote_age_sec,  # NEW: seconds since quote/trade
            'is_market_open': is_market_open,  # NEW: bool from market hours check
            'market_status': market_status,  # NEW: "OPEN", "CLOSED", "UNKNOWN"
            
            # DIAGNOSTIC COLUMNS (observability, not filters)
            'price_history_status': hist_status,  # OK, TIMEOUT, RATE_LIMIT, AUTH_ERROR, INSUFFICIENT_DATA, UNKNOWN
            'hv_status': hv_status,  # COMPUTED, INSUFFICIENT_DATA, FETCH_FAILED
            
            # IV proxy (primary)
            'iv_30d': iv.get('iv_30d', np.nan),
            
            # Multi-timeframe IV (call leg)
            'IV_7_D_Call': iv.get('iv_7d_call', np.nan),
            'IV_14_D_Call': iv.get('iv_14d_call', np.nan),
            'IV_21_D_Call': iv.get('iv_21d_call', np.nan),
            'IV_30_D_Call': iv.get('iv_30d_call', np.nan),
            'IV_60_D_Call': iv.get('iv_60d_call', np.nan),
            'IV_90_D_Call': iv.get('iv_90d_call', np.nan),
            'IV_120_D_Call': iv.get('iv_120d_call', np.nan),
            'IV_150_D_Call': iv.get('iv_150d_call', np.nan),
            'IV_180_D_Call': iv.get('iv_180d_call', np.nan),
            'IV_270_D_Call': iv.get('iv_270d_call', np.nan),
            'IV_360_D_Call': iv.get('iv_360d_call', np.nan),
            'IV_720_D_Call': iv.get('iv_720d_call', np.nan),
            'IV_1080_D_Call': iv.get('iv_1080d_call', np.nan),

            # Multi-timeframe IV (put leg)
            'IV_7_D_Put': iv.get('iv_7d_put', np.nan),
            'IV_14_D_Put': iv.get('iv_14d_put', np.nan),
            'IV_21_D_Put': iv.get('iv_21d_put', np.nan),
            'IV_30_D_Put': iv.get('iv_30d_put', np.nan),
            'IV_60_D_Put': iv.get('iv_60d_put', np.nan),
            'IV_90_D_Put': iv.get('iv_90d_put', np.nan),
            'IV_120_D_Put': iv.get('iv_120d_put', np.nan),
            'IV_150_D_Put': iv.get('iv_150d_put', np.nan),
            'IV_180_D_Put': iv.get('iv_180d_put', np.nan),
            'IV_270_D_Put': iv.get('iv_270d_put', np.nan),
            'IV_360_D_Put': iv.get('iv_360d_put', np.nan),
            'IV_720_D_Put': iv.get('iv_720d_put', np.nan),
            'IV_1080_D_Put': iv.get('iv_1080d_put', np.nan),
            
            # Multi-timeframe HV (computed)
            'hv_10': hv.get(10, np.nan),
            'hv_20': hv.get(20, np.nan),
            'hv_30': hv.get(30, np.nan),
            'hv_60': hv.get(60, np.nan),
            'hv_90': hv.get(90, np.nan),
            'HV_10_D_Cur': hv.get(10, np.nan),  # Duplicate for compatibility
            'HV_20_D_Cur': hv.get(20, np.nan),
            'HV_30_D_Cur': hv.get(30, np.nan),
            'HV_60_D_Cur': hv.get(60, np.nan),
            'HV_90_D_Cur': hv.get(90, np.nan),
            'HV_120_D_Cur': hv.get(120, np.nan),
            'HV_150_D_Cur': hv.get(150, np.nan),
            'HV_180_D_Cur': hv.get(180, np.nan),
            
            # Derived volatility intelligence (Step 0 responsibility)
            'hv_slope': hv_slope,
            'volatility_regime': volatility_regime,
            # EWMA volatility (Sinclair 2020 Ch.3: pragmatic upgrade from rolling HV)
            # λ=0.94 RiskMetrics: reacts to vol spikes in 2-3 days vs 10-20 day lag
            'HV_EWMA': hv.get('ewma', np.nan),
            
            # ENTRY QUALITY FIELDS (NEW - for scan-time analysis)
            # Intraday range & compression (from quotes)
            'highPrice': quote.get('highPrice'),
            'lowPrice': quote.get('lowPrice'),
            'openPrice': quote.get('openPrice'),
            'closePrice': quote.get('closePrice'),
            # 52-week context
            '52WeekHigh': quote.get('52WeekHigh'),
            '52WeekLow': quote.get('52WeekLow'),
            # Daily momentum
            'netChange': quote.get('netChange'),
            'netPercentChange': quote.get('netPercentChange'),
            # Dividend assignment risk
            'dividendDate': quote.get('dividendDate'),
            'dividendYield': quote.get('dividendYield'),
            
            # Snapshot metadata
            'snapshot_ts': snapshot_ts
        }
        
        rows.append(row)
    
    df = pd.DataFrame(rows)
    
    # CRITICAL VALIDATION: Reject snapshots with >30% NaN prices
    nan_count = df['last_price'].isna().sum()
    nan_pct = nan_count / len(df) * 100
    
    if nan_pct > 30:
        error_msg = (
            f"❌ SNAPSHOT QUALITY CHECK FAILED\n"
            f"   NaN prices: {nan_count}/{len(df)} ({nan_pct:.1f}%)\n"
            f"   Threshold: 30%\n"
            f"   This indicates a critical bug in quote extraction or API failure.\n"
            f"   Market status: {market_status}\n"
            f"   Price source breakdown:\n"
        )
        # Add price source stats
        for source in ['none', 'lastPrice', 'mark', 'closePrice', 'bidAskMid', 'regularMarketLastPrice']:
            count = (df['price_source'] == source).sum()
            if count > 0:
                error_msg += f"     {source}: {count} ({count/len(df)*100:.1f}%)\n"
        
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    elif nan_count > 0:
        logger.warning(f"⚠️  {nan_count}/{len(df)} ({nan_pct:.1f}%) tickers have NaN prices (below 30% threshold)")
    else:
        logger.info(f"✅ All {len(df)} tickers have valid prices!")
    
    # GAP 1 FIX: Fetch VVIX (and VIX for SPY stress context) as broadcast values.
    # Natenberg Ch.19 + Sinclair Step 11: VVIX > 130 = vol spike imminent.
    # These are index-level values (not per-ticker) — broadcast to all rows.
    # Schwab supports $VIX.X / $VIX / ^VIX syntax; fallback gracefully if unavailable.
    _vvix_value = np.nan
    _vix_value = np.nan
    _spy_change_pct = np.nan
    try:
        _index_quotes = fetch_batch_quotes(client, ['$VIX.X', '$VVIX.X'], is_market_open)
        for _sym in ('$VIX.X', 'VIX', '^VIX'):
            _q = _index_quotes.get(_sym, {})
            if _q.get('last_price'):
                _vix_value = float(_q['last_price'])
                break
        for _sym in ('$VVIX.X', 'VVIX', '^VVIX'):
            _q = _index_quotes.get(_sym, {})
            if _q.get('last_price'):
                _vvix_value = float(_q['last_price'])
                break
    except Exception as _idx_err:
        logger.debug(f"[GAP1] Schwab index quote fetch failed: {_idx_err}")

    # Fallback: yfinance for VVIX/VIX when Schwab can't serve CBOE indices
    if np.isnan(_vvix_value) or np.isnan(_vix_value):
        try:
            import yfinance as _yf
            if np.isnan(_vvix_value):
                _vvix_hist = _yf.Ticker('^VVIX').history(period='1d')
                if not _vvix_hist.empty:
                    _vvix_value = float(_vvix_hist['Close'].iloc[-1])
                    logger.info(f"[GAP1] VVIX={_vvix_value:.1f} (via yfinance)")
            if np.isnan(_vix_value):
                _vix_hist = _yf.Ticker('^VIX').history(period='1d')
                if not _vix_hist.empty:
                    _vix_value = float(_vix_hist['Close'].iloc[-1])
                    logger.info(f"[GAP1] VIX={_vix_value:.1f} (via yfinance)")
        except Exception as _yf_err:
            logger.debug(f"[GAP1] yfinance fallback failed: {_yf_err}")

    # SPY net change for market stress context
    try:
        _spy_quotes = fetch_batch_quotes(client, ['SPY'], is_market_open)
        _spy_q = _spy_quotes.get('SPY', {})
        if _spy_q.get('last_price') and _spy_q.get('netPercentChange') is not None:
            _spy_change_pct = float(_spy_q.get('netPercentChange', 0))
    except Exception as _spy_err:
        logger.debug(f"[GAP1] SPY change fetch failed: {_spy_err}")

    # Broadcast index values to all rows
    df['VVIX'] = _vvix_value
    df['VIX'] = _vix_value
    df['SPY_Change_Pct'] = _spy_change_pct
    if not np.isnan(_vvix_value):
        logger.info(f"[GAP1] VVIX={_vvix_value:.1f}, VIX={_vix_value:.1f}, SPY_Change={_spy_change_pct:.2f}%")
    else:
        logger.info("[GAP1] VVIX/VIX not available — columns set to NaN (Step 2 will handle gracefully)")

    elapsed = time.time() - start_time

    # Final Summary with Coverage Metrics
    logger.info("\n" + "="*80)
    logger.info("📊 STEP 0 COMPLETE - SUMMARY")
    logger.info("="*80)
    logger.info(f"   Total tickers: {len(df)}")
    logger.info(f"   Runtime: {elapsed:.1f}s")
    logger.info(f"   Throughput: {len(df)/elapsed:.1f} tickers/sec")
    logger.info(f"   Market status: {market_status}")
    
    # Price source breakdown
    logger.info(f"\n   Price Source Coverage:")
    for source in ['lastPrice', 'mark', 'closePrice', 'bidAskMid', 'regularMarketLastPrice', 'none']:
        count = (df['price_source'] == source).sum()
        if count > 0:
            pct = count / len(df) * 100
            logger.info(f"     {source}: {count} ({pct:.1f}%)")
    
    # HV Coverage
    hv_coverage = df[df['hv_status'] == 'COMPUTED']
    logger.info(f"\n   HV Coverage: {len(hv_coverage)}/{len(df)} ({100*len(hv_coverage)/len(df):.1f}%)")
    
    # IV Coverage
    if fetch_iv and not _iv_already_collected:
        iv_coverage = df[df['iv_30d'].notna()]
        logger.info(f"   IV Coverage: {len(iv_coverage)}/{len(df)} ({100*len(iv_coverage)/len(df):.1f}%)")
    elif _iv_already_collected:
        # iv_30d in CSV is NaN because REST was skipped — query DuckDB for truth
        try:
            _iv_con = get_domain_connection(DbDomain.IV_HISTORY, read_only=True)
            _iv_cnt = _iv_con.execute(
                "SELECT COUNT(DISTINCT ticker) FROM iv_term_history "
                "WHERE date = current_date AND iv_30d IS NOT NULL"
            ).fetchone()[0]
            _iv_con.close()
            logger.info(f"   IV Coverage: {_iv_cnt}/{len(df)} ({100*_iv_cnt/len(df):.1f}%) [from iv_term_history — REST skipped]")
        except Exception:
            logger.info("   IV Coverage: skipped (already collected today — see iv_term_history)")
    
    # Failure Breakdown
    logger.info(f"\n   Fetch Status Breakdown:")
    for status in ['OK', 'INSUFFICIENT_DATA', 'TIMEOUT', 'SKIPPED_TIMEOUT', 'RATE_LIMIT', 'AUTH_ERROR', 'UNKNOWN']:
        count = (df['price_history_status'] == status).sum()
        if count > 0:
            logger.info(f"     {status}: {count}")
    
    logger.info("="*80)
    
    return df


def save_snapshot(df: pd.DataFrame, output_dir: Path = SNAPSHOT_DIR) -> Path:
    """
    Save snapshot to CSV with timestamped filename.
    
    Args:
        df: Snapshot DataFrame
        output_dir: Output directory
    
    Returns:
        Path to saved CSV file
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"ivhv_snapshot_live_{timestamp}.csv"
    output_path = output_dir / filename
    
    df.to_csv(output_path, index=False)
    logger.info(f"💾 Snapshot saved: {output_path}")
    
    return output_path


def promote_snapshot_to_archive(snapshot_path: Path):
    """
    Daily Archive Promotion Rule:
    On the first successful scan of a trading day, persist ONE snapshot into ivhv_archive.
    Additional scans the same day do nothing (no duplicates).
    """
    try:
        # 1. Determine market date from snapshot
        df = pd.read_csv(snapshot_path, nrows=1)
        if 'Date' not in df.columns:
            logger.warning(f"⚠️ Cannot promote snapshot: 'Date' column missing in {snapshot_path}")
            return
        
        market_date = df['Date'].iloc[0]
        archive_dir = PROJECT_ROOT / "data" / "ivhv_archive"
        archive_dir.mkdir(parents=True, exist_ok=True)
        
        archive_filename = f"ivhv_snapshot_{market_date}.csv"
        archive_path = archive_dir / archive_filename
        
        # 2. Check if already promoted for this date
        if archive_path.exists():
            logger.info(f"ℹ️ Archive already contains snapshot for {market_date}. Skipping promotion.")
            return
        
        # 3. Promote (Copy)
        shutil.copy2(snapshot_path, archive_path)
        logger.info(f"✅ PROMOTED: {snapshot_path.name} -> {archive_path}")
        
        # 4. Trigger Data Maturity Pipeline (REMOVED: Canonical CSV layer eliminated)
        # The persistence logic is now directly handled in run_snapshot
        print("\n[✅] Data Maturity Pipeline (CSV) removed. IV history updated via DuckDB persistence.")
        
    except Exception as e:
        logger.error(f"❌ Failed to promote snapshot to archive: {e}")


# ============================================================
# MAIN ENTRY POINT
# ============================================================

def run_snapshot(
    test_mode: bool = False,
    test_ticker: str = "AAPL",
    use_cache: bool = True,
    fetch_iv: bool = True,
    discovery_mode: bool = False,
    skip_iv_if_collected_today: bool = True,
):
    """
    Core execution function for Step 0 snapshot generation.

    Callable from both CLI and dashboard. Does NOT configure logging - caller must set up logging.

    Args:
        test_mode: If True, only process test_ticker (for validation)
        test_ticker: Ticker to use in test mode
        use_cache: Whether to use cached price history
        fetch_iv: Whether to fetch IV (can disable for faster testing)
        discovery_mode: If True, prunes IV fetching to high-interest tickers only
        skip_iv_if_collected_today: If True (default), skip REST IV collection when
            iv_term_history already has today's rows. Saves ~200-400s on repeat runs.

    Returns:
        DataFrame: Generated snapshot with IV/HV data
    """
    logger.info("=" * 60)
    logger.info("Step 0: Live IV/HV Snapshot Generation")
    logger.info("=" * 60)
    
    # Initialize Schwab client with strict env var contract
    client_id = os.getenv("SCHWAB_APP_KEY")
    client_secret = os.getenv("SCHWAB_APP_SECRET")
    callback_url = os.getenv("SCHWAB_CALLBACK_URL", "https://127.0.0.1")

    assert client_id, "SCHWAB_APP_KEY not set"
    assert client_secret, "SCHWAB_APP_SECRET not set"

    client = SchwabClient(client_id, client_secret)

    if not client._tokens:
        raise ValueError(
            "No existing tokens found and missing credentials. "
            "Either authenticate first or set SCHWAB_APP_KEY and SCHWAB_APP_SECRET env vars."
        )

    logger.info("✅ SchwabClient initialized (using existing tokens)")
    
    # Load tickers
    if test_mode:
        tickers = [test_ticker]
        logger.info(f"🧪 TEST MODE: Processing single ticker: {test_ticker}")
    else:
        tickers = load_ticker_universe(TICKER_UNIVERSE_PATH)
    
    # Generate snapshot
    df = generate_live_snapshot(
        client,
        tickers,
        use_cache=use_cache,
        fetch_iv=fetch_iv,
        discovery_mode=discovery_mode,
        skip_iv_if_collected_today=skip_iv_if_collected_today,
    )
    
    # Save to CSV
    output_path = save_snapshot(df)
    
    # Promote to archive (Daily Promotion Rule)
    # promote_snapshot_to_archive(output_path) # REMOVED: Canonical CSV layer eliminated

    # IV persistence is now handled inside IVRestCollector (called from generate_live_snapshot).
    # No additional DuckDB writes needed here.

    # Display summary
    logger.info("\n" + "=" * 60)
    logger.info("SNAPSHOT SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total tickers: {len(df)}")
    logger.info(f"Complete IV/HV: {df['iv_30d'].notna().sum()} / {len(df)}")
    logger.info(f"Output file: {output_path}")
    logger.info(f"File size: {output_path.stat().st_size / 1024:.1f} KB")
    
    if test_mode and len(df) > 0:
        logger.info("\n" + "=" * 60)
        logger.info("SAMPLE ROW (TEST MODE)")
        logger.info("=" * 60)
        row = df.iloc[0]
        logger.info(f"Ticker: {row['Ticker']}")
        logger.info(f"Last Price: ${row['last_price']:.2f}")
        logger.info(f"Volume: {row['volume']:,.0f}")
        logger.info(f"IV (30D): {row['iv_30d']:.2f}%")
        logger.info(f"HV (10D): {row['hv_10']:.2f}%")
        logger.info(f"HV (20D): {row['hv_20']:.2f}%")
        logger.info(f"HV (30D): {row['hv_30']:.2f}%")
        logger.info(f"HV (60D): {row['hv_60']:.2f}%")
        logger.info(f"HV (90D): {row['hv_90']:.2f}%")
    
    logger.info("\n✅ Step 0 complete!")
    logger.info("=" * 60)
    
    return df


if __name__ == "__main__":
    # Configure logging for CLI execution
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Run FULL scan with liquid tickers
    df = run_snapshot(
        test_mode=False,
        test_ticker=None,
        use_cache=True,
        fetch_iv=True
    )
