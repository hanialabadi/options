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
import random
from pathlib import Path
from datetime import datetime, timedelta
from typing import Tuple, Optional, Union, Dict, Any
from enum import Enum
from core.shared.data_contracts.config import PRICE_CACHE_DIR, PIPELINE_DB_PATH
import diskcache as dc
# from scan_engine.debug.debug_mode import get_debug_manager # Moved import to function to break circular dependency
import duckdb # Import duckdb globally for type hinting

logger = logging.getLogger(__name__)

class ChartDataStatus(Enum):
    OK = "OK"
    BLOCKED_RATE_LIMIT = "BLOCKED_RATE_LIMIT"
    NO_HISTORY = "NO_HISTORY"
    FAILED = "FAILED"

# Cache Configuration
CACHE_DIR = PRICE_CACHE_DIR
CACHE_TTL_HOURS = 24
PRICE_HISTORY_CACHE = dc.Cache(str(PRICE_CACHE_DIR / "diskcache_price_history"))

# === Smart Persistence Layer (DuckDB) ===
# Explicitly import connection functions and table name
from core.shared.data_layer.duckdb_utils import get_duckdb_connection, get_duckdb_write_connection, PRICE_HISTORY_METADATA_TABLE, initialize_price_history_metadata_table, initialize_price_history_table
from core.shared.data_layer.market_time import price_freshness

def _is_debug_mode() -> bool:
    return os.getenv("PIPELINE_DEBUG") == "1" or os.getenv("DEBUG_TICKER_MODE") == "1"

def _ensure_metadata_table():
    """Initialize both metadata and OHLC cache tables."""
    initialize_price_history_metadata_table()
    initialize_price_history_table()

def _get_metadata(ticker: str, con: Optional[duckdb.DuckDBPyConnection] = None) -> Optional[dict]:
    """
    Retrieves price history metadata for a ticker from DuckDB.
    Uses an existing connection if provided, otherwise opens a new read-only connection.
    """
    _con = con if con is not None else get_duckdb_connection() # Use provided connection or open new
    try:
        result = _con.execute(f"""
            SELECT Ticker, Last_Fetch_TS, Source, Days_History, Backoff_Until
            FROM {PRICE_HISTORY_METADATA_TABLE}
            WHERE Ticker = ?
        """, [ticker.upper()]).fetchdf()
        if not result.empty:
            meta_dict = result.iloc[0].to_dict()
            # Ensure Backoff_Until is a datetime object or None
            if 'Backoff_Until' in meta_dict:
                backoff_val = meta_dict['Backoff_Until']
                if pd.isna(backoff_val): # Handle numpy NaN explicitly
                    meta_dict['Backoff_Until'] = None
                elif isinstance(backoff_val, str):
                    try:
                        meta_dict['Backoff_Until'] = datetime.fromisoformat(backoff_val)
                    except ValueError:
                        logger.warning(f"Could not parse Backoff_Until string '{backoff_val}' for {ticker}. Treating as None.")
                        meta_dict['Backoff_Until'] = None
                elif isinstance(backoff_val, (int, float, np.integer, np.floating)): # Handle numeric timestamps
                    if np.isnan(backoff_val):
                        backoff_dt = None
                    else:
                        try:
                            # Assuming it's a Unix timestamp (seconds)
                            backoff_dt = datetime.fromtimestamp(backoff_val)
                        except (ValueError, OSError):
                            logger.warning(f"Could not convert numeric timestamp '{backoff_val}' to datetime for {ticker}. Treating as None.")
                            backoff_dt = None
                    meta_dict['Backoff_Until'] = backoff_dt
                elif not isinstance(backoff_val, datetime): # If it's not datetime, str, or numeric, treat as None
                    logger.warning(f"Unexpected type for Backoff_Until '{type(backoff_val)}' for {ticker}. Treating as None.")
                    meta_dict['Backoff_Until'] = None
                # If it's already a datetime object, keep it as is
            else:
                meta_dict['Backoff_Until'] = None
            return meta_dict
        return None
    except Exception as e:
        logger.debug(f"Failed to get price metadata for {ticker}: {e}")
        return None
    finally:
        if con is None: # Only close if this function opened the connection
            _con.close()

def _update_metadata(ticker: str, success: bool, source: str = None, backoff_duration: int = 0, days_history: Optional[int] = None, con: Optional[duckdb.DuckDBPyConnection] = None):
    """
    Updates price history metadata for a ticker in DuckDB.
    Uses an existing connection if provided, otherwise opens a new write connection.
    """
    now = datetime.now()
    backoff_until_dt: Optional[datetime] = None
    if backoff_duration > 0:
        backoff_until_dt = now + timedelta(seconds=backoff_duration)
    
    _con = con if con is not None else get_duckdb_write_connection() # Use provided connection or open new
    try:
        if success:
            _con.execute(f"""
                INSERT INTO {PRICE_HISTORY_METADATA_TABLE} (Ticker, Last_Fetch_TS, Source, Days_History, Backoff_Until)
                VALUES (?, ?, ?, ?, NULL)
                ON CONFLICT (Ticker) DO UPDATE SET 
                    Last_Fetch_TS = excluded.Last_Fetch_TS,
                    Source = excluded.Source,
                    Days_History = excluded.Days_History,
                    Backoff_Until = NULL
            """, [ticker.upper(), now, source, days_history])
        else:
            _con.execute(f"""
                INSERT INTO {PRICE_HISTORY_METADATA_TABLE} (Ticker, Last_Fetch_TS, Source, Days_History, Backoff_Until)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (Ticker) DO UPDATE SET 
                    Last_Fetch_TS = excluded.Last_Fetch_TS,
                    Source = excluded.Source,
                    Days_History = excluded.Days_History,
                    Backoff_Until = excluded.Backoff_Until
            """, [ticker.upper(), now, source, days_history, backoff_until_dt])
    except Exception as e:
        logger.error(f"Failed to update price metadata for {ticker}: {e}")
    finally:
        if con is None: # Only close if this function opened the connection
            _con.close()

# Known ETFs to avoid yfinance fundamental 404s
ETFS = {'SPY', 'QQQ', 'IWM', 'DIA', 'TLT', 'GLD', 'SLV', 'XLE', 'XLF', 'XLI', 'XLK', 'XLU', 'XLP', 'XLV', 'XLY', 'XLB', 'XLC', 'XRE', 'SMH'}

def get_cache_path(ticker: str) -> Path:
    # This function is now primarily for the file-based cache fallback,
    # but we keep it for consistency if needed.
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"{ticker.upper()}.csv"

def is_diskcache_valid(ticker: str, ttl_hours: int = CACHE_TTL_HOURS) -> bool:
    """
    Checks if data for a ticker exists in diskcache and is not stale.
    Temporarily disabled due to TypeError issues.
    """
    return False # Temporarily disable diskcache

def safe_yf_history(ticker_symbol: str, period: str = "1y", days_history: Optional[int] = None, con: Optional[duckdb.DuckDBPyConnection] = None) -> Tuple[Optional[pd.DataFrame], ChartDataStatus]:
    """
    Production-grade wrapper for Yahoo Finance OHLC access.
    Uses an existing DuckDB connection if provided for metadata updates.
    """
    import yfinance as yf
    
    # Check smart backoff
    meta = _get_metadata(ticker_symbol, con=con) # Pass connection
    if meta and 'Backoff_Until' in meta and meta['Backoff_Until'] is not None:
        backoff_val = meta['Backoff_Until']
        backoff_dt: Optional[datetime] = None

        # Ensure backoff_dt is always a datetime object or None
        if isinstance(backoff_val, datetime):
            backoff_dt = backoff_val
        elif isinstance(backoff_val, str):
            try:
                backoff_dt = datetime.fromisoformat(backoff_val)
            except ValueError:
                logger.warning(f"Could not parse Backoff_Until string '{backoff_val}' for {ticker_symbol}. Treating as None.")
                backoff_dt = None
        elif isinstance(backoff_val, (int, float, np.integer, np.floating)):
            if np.isnan(backoff_val):
                backoff_dt = None
            else:
                try:
                    # Assuming it's a Unix timestamp (seconds)
                    backoff_dt = datetime.fromtimestamp(backoff_val)
                except (ValueError, OSError):
                    logger.warning(f"Could not convert numeric timestamp '{backoff_val}' to datetime for {ticker_symbol}. Treating as None.")
                    backoff_dt = None
        else:
            logger.warning(f"Unexpected type for Backoff_Until '{type(backoff_val)}' for {ticker_symbol}. Treating as None.")
            backoff_dt = None

        if backoff_dt and backoff_dt > datetime.now():
            return None, ChartDataStatus.BLOCKED_RATE_LIMIT

    max_retries = 3
    base_delay = 2.0
    
    for attempt in range(max_retries):
        try:
            ticker = yf.Ticker(ticker_symbol)
            df = ticker.history(period=period)
            
            if df.empty:
                logger.debug(f"Yahoo fetch failed for {ticker_symbol}: NO_HISTORY")
                _update_metadata(ticker_symbol, False, days_history=days_history, con=con) # Pass connection
                return None, ChartDataStatus.NO_HISTORY
                
            _update_metadata(ticker_symbol, True, source="YFINANCE", days_history=len(df), con=con) # Pass connection
            return df, ChartDataStatus.OK
            
        except Exception as e:
            err_str = str(e)
            if "Rate limited" in err_str or "429" in err_str:
                delay = (base_delay * (2 ** attempt)) + random.uniform(0, 1)
                logger.warning(f"[YF] Rate limit for {ticker_symbol}. Attempt {attempt + 1}/{max_retries}.")
                
                if attempt == max_retries - 1:
                    _update_metadata(ticker_symbol, False, backoff_duration=3600, days_history=days_history, con=con) # Pass connection
                    return None, ChartDataStatus.BLOCKED_RATE_LIMIT
                
                time.sleep(delay)
            else:
                logger.debug(f"Yahoo fetch failed for {ticker_symbol}: {e}")
                _update_metadata(ticker_symbol, False, days_history=days_history, con=con) # Pass connection
                return None, ChartDataStatus.FAILED
                
    return None, ChartDataStatus.FAILED

def _load_from_duckdb_cache(ticker: str, days: int, con: Optional[duckdb.DuckDBPyConnection] = None) -> Optional[pd.DataFrame]:
    """
    Load OHLC from DuckDB price_history table (cached from yf_fetch.py).

    Args:
        ticker: Stock symbol
        days: Number of days to fetch
        con: Optional DuckDB connection

    Returns:
        DataFrame with OHLC data or None if not in cache
    """
    _con = con if con is not None else get_duckdb_connection()
    owns_connection = con is None

    try:
        # Query price_history table
        result = _con.execute("""
            SELECT date, open_price, high_price, low_price, close_price, volume
            FROM price_history
            WHERE UPPER(ticker) = UPPER(?)
            ORDER BY date DESC
            LIMIT ?
        """, [ticker, days]).fetchdf()

        if result.empty:
            return None

        # Convert to expected format
        df = result.rename(columns={
            'open_price': 'Open',
            'high_price': 'High',
            'low_price': 'Low',
            'close_price': 'Close',
            'volume': 'Volume'
        })

        # Set date as index
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')

        # Sort chronologically
        df = df.sort_index()

        return df

    except Exception as e:
        logger.debug(f"DuckDB price_history query failed for {ticker}: {e}")
        return None

    finally:
        if owns_connection and _con is not None:
            try:
                _con.close()
            except:
                pass


def check_ohlc_availability(ticker: str, min_bars: int = 30,
                            con: Optional[duckdb.DuckDBPyConnection] = None) -> Dict[str, Any]:
    """
    Check if ticker has sufficient OHLC in DuckDB cache.

    Args:
        ticker: Stock symbol
        min_bars: Minimum number of bars required (default: 30)
        con: Optional DuckDB connection

    Returns:
        {
            'available': bool,
            'bar_count': int,
            'last_date': datetime or None,
            'staleness_hours': float or None
        }
    """
    _con = con if con is not None else get_duckdb_connection()
    owns_connection = con is None

    try:
        result = _con.execute("""
            SELECT COUNT(*) as bar_count, MAX(date) as last_date
            FROM price_history
            WHERE UPPER(ticker) = UPPER(?)
        """, [ticker]).fetchone()

        if not result or result[0] < min_bars:
            return {
                'available': False,
                'bar_count': result[0] if result else 0,
                'last_date': None,
                'staleness_hours': None
            }

        last_date = pd.to_datetime(result[1])
        staleness_hours = (datetime.now() - last_date).total_seconds() / 3600

        return {
            'available': True,
            'bar_count': result[0],
            'last_date': last_date,
            'staleness_hours': staleness_hours
        }

    finally:
        if owns_connection and _con is not None:
            _con.close()


def load_price_history(ticker: str, days: int = 180, client=None, use_cache: bool = True, skip_auto_fetch: bool = False, con: Optional[duckdb.DuckDBPyConnection] = None) -> Tuple[Optional[pd.DataFrame], Union[str, ChartDataStatus]]:
    """
    Authoritative price history loader with smart persistence.
    Hierarchy:
    1. Valid DiskCache
    2. Schwab API (Authoritative)
    3. DuckDB price_history cache (from yf_fetch.py)
    4. Yahoo Finance Auto-Fetch (only if skip_auto_fetch=False)
    5. Stale DiskCache (Emergency)
    6. File-based Cache (Legacy Fallback)

    Args:
        ticker: Stock symbol
        days: Number of days to fetch
        client: Optional Schwab API client
        use_cache: Enable cache lookup
        skip_auto_fetch: If True, do NOT auto-fetch from Yahoo Finance (demand-driven mode)
        con: Optional DuckDB connection for metadata updates

    Uses an existing DuckDB connection if provided for metadata updates.
    """
    ticker = ticker.upper()

    _ensure_metadata_table()

    # 1. Check DiskCache
    if use_cache and is_diskcache_valid(ticker):
        try:
            df = PRICE_HISTORY_CACHE.get(ticker)
            if df is not None and not df.empty and len(df) >= 30:
                return df.tail(days), "DISKCACHE"
        except Exception as e:
            logger.debug(f"Diskcache read failed for {ticker}: {e}")

    # 2. Try Schwab (Priority Authority)
    if client:
        try:
            # Use periodType=month with period=6 (180 days ≈ 126 trading days) to get enough
            # history for SMA50 calculation. startDate/endDate without frequencyType only
            # returns ~1 month; period-based requests reliably return full daily history.
            response_raw = client.get_price_history( # This returns the requests.Response object
                symbol=ticker,
                periodType="month",
                period=6,
                frequencyType="daily",
                frequency=1
            )
            # CRITICAL FIX: Handle empty response body from Schwab API (200 None)
            # response_raw is now a dict, not a requests.Response object
            if not response_raw or 'candles' not in response_raw or not response_raw['candles']:
                logger.warning(f"⚠️ Schwab fetch for {ticker} returned empty response body or no candles data. Skipping Schwab data.")
                return None, ChartDataStatus.NO_HISTORY # Fallback to yfinance
            
            # response_json is already response_raw since schwab_api_client now returns json()
            response_json = response_raw 
            
            if response_json and 'candles' in response_json and response_json['candles']:
                df = pd.DataFrame(response_json['candles'])
                df = df.rename(columns={
                    'open': 'Open', 'high': 'High', 'low': 'Low', 
                    'close': 'Close', 'volume': 'Volume'
                })
                
                if 'datetime' not in df.columns:
                    logger.warning(f"⚠️ Schwab fetch for {ticker} returned no 'datetime' column in candles. Skipping Schwab data.")
                    return None, ChartDataStatus.NO_HISTORY # Fallback to yfinance

                # Ensure datetime column is numeric before conversion
                df['datetime'] = pd.to_numeric(df['datetime'], errors='coerce')
                df['datetime'] = pd.to_datetime(df['datetime'], unit='ms', errors='coerce')
                
                # CRITICAL: Ensure there are valid datetime values before setting index
                if df['datetime'].notna().any():
                    df = df.set_index('datetime')
                else:
                    logger.warning(f"⚠️ Schwab fetch for {ticker} returned no valid datetime values. Skipping Schwab data.")
                    return None, ChartDataStatus.NO_HISTORY # Fallback to yfinance
                
                if len(df) >= 30:
                    # Always persist to DuckDB for SMA/Murphy indicator use regardless of freshness.
                    # Historical bars are needed even if the latest bar is T-1.
                    if con is not None:
                        try:
                            persist_df = df.reset_index().rename(columns={
                                'datetime': 'date', 'Open': 'open_price', 'High': 'high_price',
                                'Low': 'low_price', 'Close': 'close_price', 'Volume': 'volume'
                            })
                            persist_df['ticker'] = ticker
                            persist_df['date'] = persist_df['date'].dt.date
                            # Keep last candle per day (Schwab returns intraday timestamps)
                            persist_df = persist_df.drop_duplicates(subset=['date'], keep='last')
                            con.execute("DELETE FROM price_history WHERE UPPER(ticker) = UPPER(?)", [ticker])
                            con.execute("""
                                INSERT INTO price_history (ticker, date, open_price, high_price, low_price, close_price, volume, source)
                                SELECT ticker, date, open_price, high_price, low_price, close_price, volume, 'SCHWAB'
                                FROM persist_df
                            """)
                            logger.debug(f"{ticker}: Persisted {len(persist_df)} Schwab OHLC bars to DuckDB")
                        except Exception as db_e:
                            logger.warning(f"⚠️ {ticker}: Failed to persist Schwab OHLC to DuckDB: {db_e}")
                    # Freshness check gates source label but NOT data availability.
                    # Chart primitives (ROC, EMA, momentum) need historical bars —
                    # T-1 data is perfectly valid for computing slopes and rate-of-change.
                    # Previous code returned (None, NO_HISTORY) here, which caused
                    # compute_chart_primitives to skip ALL tickers during after-hours
                    # runs, leaving every momentum/scale-up gate blind.
                    _is_fresh = price_freshness(df.index[-1])
                    _source_label = "SCHWAB" if _is_fresh else "SCHWAB_STALE"
                    if not _is_fresh:
                        logger.info(f"📊 {ticker}: Schwab OHLC stale (last bar {df.index[-1]}), "
                                    f"returning {len(df)} bars for chart primitives")
                    try:
                        _update_metadata(ticker, True, source=_source_label, days_history=len(df), con=con)
                        return df.tail(days), _source_label
                    except TypeError as cache_te:
                        logger.error(f"❌ Diskcache.set failed for {ticker} with TypeError: {cache_te}. Skipping cache.", exc_info=True)
                        _update_metadata(ticker, True, source=f"{_source_label}_NO_CACHE", days_history=len(df), con=con)
                        return df.tail(days), f"{_source_label}_NO_CACHE"
                else:
                    logger.warning(f"⚠️ Schwab fetch for {ticker} returned insufficient data ({len(df)} rows). Skipping Schwab data.")
                    return None, ChartDataStatus.NO_HISTORY # Fallback to yfinance
            else:
                logger.warning(f"⚠️ Schwab fetch for {ticker} returned no candles data. Skipping Schwab data.")
                return None, ChartDataStatus.NO_HISTORY # Fallback to yfinance
        except Exception as e: # Catch all exceptions here, including potential KeyError if 'candles' is missing
            logger.error(f"❌ Schwab fetch for {ticker} failed with unexpected error: {e}", exc_info=True)
            return None, ChartDataStatus.FAILED # Explicitly return FAILED status on exception

    # 3. Check DuckDB price_history cache (from yf_fetch.py)
    if use_cache:
        cached_df = _load_from_duckdb_cache(ticker, days, con=con)
        if cached_df is not None and not cached_df.empty and len(cached_df) >= 30:
            # Freshness check (market-aware)
            if price_freshness(cached_df.index[-1]):
                logger.debug(f"{ticker}: Using cached OHLC from DuckDB ({len(cached_df)} bars)")
                return cached_df.tail(days), "DUCKDB_CACHE"
            else:
                logger.warning(f"⚠️ Stale cached OHLC for {ticker} in DuckDB (last bar {cached_df.index[-1]})")

    # 4. Try yfinance Auto-Fetch (only if not demand-driven mode)
    if skip_auto_fetch:
        # Demand-driven mode: Do NOT auto-fetch from Yahoo Finance
        logger.debug(f"{ticker}: No cached OHLC, skip_auto_fetch=True - will not auto-fetch from YF")
        return None, ChartDataStatus.NO_HISTORY

    df, status = safe_yf_history(ticker, days_history=days, con=con) # Pass days_history and connection
    if status == ChartDataStatus.OK and df is not None:
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [col[0] for col in df.columns]
        
            if len(df) >= 30:
                if not price_freshness(df.index[-1]):
                    logger.warning(f"⚠️ Stale price history for {ticker} (last bar {df.index[-1]})")
                    from scan_engine.debug.debug_mode import get_debug_manager
                    get_debug_manager().log_event(
                        "price_history",
                        "WARN",
                        "STALE_PRICE_DATA",
                        f"Stale price history for {ticker}",
                        {"last_bar_ts": str(df.index[-1]), "source": "YFINANCE"}
                    )
                    return None, ChartDataStatus.NO_HISTORY
                # PRICE_HISTORY_CACHE.set(ticker, df, expire=timedelta(hours=CACHE_TTL_HOURS).total_seconds()) # Temporarily disabled
                return df.tail(days), "YFINANCE"

    # 4. Emergency Fallback: Stale DiskCache (disabled in debug)
    if not _is_debug_mode():
        if ticker in PRICE_HISTORY_CACHE:
            try:
                df = PRICE_HISTORY_CACHE.get(ticker)
                if df is not None and not df.empty:
                    logger.warning(f"[DISKCACHE] Using STALE price history for {ticker} as last resort.")
                    return df.tail(days), "STALE_DISKCACHE"
            except Exception:
                pass

    # 5. Legacy Fallback: File-based Cache (disabled in debug)
    if not _is_debug_mode():
        cache_path = get_cache_path(ticker)
        if cache_path.exists():
            try:
                df = pd.read_csv(cache_path, index_col=0, parse_dates=True)
                if not df.empty:
                    logger.warning(f"[FILE_CACHE] Using LEGACY FILE-BASED price history for {ticker} as last resort.")
                    return df.tail(days), "FILE_CACHE"
            except Exception:
                pass

    from scan_engine.debug.debug_mode import get_debug_manager
    get_debug_manager().log_event(
        "price_history",
        "WARN",
        "INSUFFICIENT_HISTORY",
        f"No usable price history for {ticker}",
        {"ticker": ticker, "last_status": str(status)}
    )
    return None, status
