"""
Step 5: Compute Chart Signals and Market Regime Classification

NOTE:
This step is strictly DESCRIPTIVE.
It must not introduce strategy assumptions, thresholds,
pass/fail flags, or trade intent.
All strategy decisions occur in later phases.

Purpose:
    Describes price structure, momentum, and volatility patterns using technical indicators.
    Provides objective measurements of trend, volatility, and price position relative to averages.
"""

import pandas as pd
import numpy as np
import logging
import time
import requests
import os
from .utils import validate_input

logger = logging.getLogger(__name__)

# Import Schwab client for price history (Schwab-first migration)
try:
    from .schwab_api_client import SchwabClient
    SCHWAB_AVAILABLE = True
except ImportError:
    SCHWAB_AVAILABLE = False
    logger.warning("⚠️ Schwab API client not available, falling back to yfinance")
    
# Fallback to yfinance if Schwab unavailable (defensive)
try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False
    logger.warning("⚠️ yfinance not available")


def fetch_schwab_price_history(client: 'SchwabClient', ticker: str, days: int = 180) -> tuple[pd.DataFrame | None, str]:
    """
    Fetch price history from Schwab with retry logic.
    
    This mirrors the logic from step0_schwab_snapshot.py for consistency.
    
    Args:
        client: SchwabClient instance
        ticker: Stock ticker symbol
        days: Number of days of history (default 180 for chart signals)
    
    Returns:
        Tuple of (DataFrame with OHLC data, status string)
        Status: "OK", "TIMEOUT", "RATE_LIMIT", "AUTH_ERROR", "INSUFFICIENT_DATA", "UNKNOWN"
    """
    max_attempts = 2
    backoff = [0.5, 1.0]
    
    for attempt in range(max_attempts):
        try:
            # Schwab API call
            response = client.get_price_history(
                symbol=ticker,
                periodType="day",
                period=1,  # Get all available data
                frequencyType="daily",
                frequency=1
            )
            
            if not response or 'candles' not in response or not response['candles']:
                return None, "INSUFFICIENT_DATA"
            
            # Convert to DataFrame
            candles = response['candles']
            df = pd.DataFrame(candles)
            
            # Rename columns to match yfinance format
            df = df.rename(columns={
                'open': 'Open',
                'high': 'High',
                'low': 'Low',
                'close': 'Close',
                'volume': 'Volume'
            })
            
            # Convert datetime
            if 'datetime' in df.columns:
                df['datetime'] = pd.to_datetime(df['datetime'], unit='ms')
                df = df.set_index('datetime')
            
            # Filter to requested days
            if len(df) > days:
                df = df.tail(days)
            
            if len(df) < 30:
                return None, "INSUFFICIENT_DATA"
            
            return df, "OK"
            
        except requests.exceptions.Timeout:
            if attempt < max_attempts - 1:
                time.sleep(backoff[attempt])
            else:
                return None, "TIMEOUT"
                
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                if attempt < max_attempts - 1:
                    time.sleep(backoff[attempt])
                else:
                    return None, "RATE_LIMIT"
            elif e.response.status_code == 401:
                return None, "AUTH_ERROR"
            else:
                return None, "UNKNOWN"
                
        except Exception as e:
            logger.debug(f"Schwab price history error for {ticker}: {e}")
            return None, "UNKNOWN"
    
    return None, "UNKNOWN"


def classify_regime(row: dict) -> str:
    """
    Classify market environment based on price structure.
    
    NOTE: This is DESCRIPTIVE classification, not prescriptive.
    Separates "what the market is doing" from "what we should do".
    
    Returns one of: Trending, Ranging, Compressed, Overextended, Neutral
    
    Args:
        row (dict): Dict with keys: Trend_Slope, Atr_Pct, Price_vs_SMA20, SMA20
    
    Returns:
        str: Regime classification
    
    Example:
        >>> regime = classify_regime({
        ...     'Trend_Slope': 2.5,
        ...     'Atr_Pct': 1.8,
        ...     'Price_vs_SMA20': 10.0,
        ...     'SMA20': 150.0
        ... })
        >>> print(regime)  # "Trending"
    """
    trend_slope = row.get('Trend_Slope', 0)
    atr_pct = row.get('Atr_Pct')
    price_vs_sma20 = row.get('Price_vs_SMA20', 0)
    sma20 = row.get('SMA20', 1)
    
    if pd.isna(sma20) or sma20 == 0:
        return "Neutral"
    
    overextension_pct = abs(price_vs_sma20) / sma20
    
    if overextension_pct > 0.40:
        return "Overextended"
    elif atr_pct is not None and atr_pct < 1.0:
        return "Compressed"
    elif abs(trend_slope) > 2.0:
        return "Trending"
    elif abs(trend_slope) < 0.5 and overextension_pct < 0.10:
        return "Ranging"
    else:
        return "Neutral"


def compute_chart_signals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute technical indicators: EMA crossovers, ATR, trend slope, and market regime.
    
    NOTE:
    This step is strictly DESCRIPTIVE.
    It must not introduce strategy assumptions, thresholds,
    pass/fail flags, or trade intent.
    All strategy decisions occur in later phases.
    
    Purpose:
        Describes price structure, momentum, and volatility using technical indicators.
        Fetches 90-day price history and computes moving averages, volatility metrics,
        and regime classification. All outputs are descriptive measurements, not recommendations.
    
    Logic Flow (per ticker):
        1. Fetch 90-day price history via yfinance
        2. Calculate ATR (14-period Average True Range) as volatility measure
        3. Compute moving averages: EMA9, EMA21, SMA20, SMA50
        4. Detect EMA9/EMA21 crossovers and days since last cross
        5. Calculate trend slope (EMA9 delta over 5 days)
        6. Measure price distance from SMAs (extension measurement)
        7. Classify market regime (Trending, Ranging, Compressed, Overextended, Neutral)
    
    Regime Classification (DESCRIPTIVE ONLY):
        - Overextended: Price >40% from SMA20 (extended from average)
        - Compressed: ATR < 1.0% (low volatility, tight range)
        - Trending: Trend slope > 2.0 (strong directional movement)
        - Ranging: Trend slope < 0.5, price near SMAs (sideways pattern)
        - Neutral: Mixed or unclear signals
    
    Args:
        df (pd.DataFrame): Input with at least ['Ticker', 'IVHV_gap_30D']
    
    Returns:
        pd.DataFrame: Original data merged with chart metrics:
            - Chart_Regime: Market environment classification (descriptive)
            - EMA_Signal: "Bullish" or "Bearish" (EMA9 vs EMA21 position)
            - Chart_Signal_Type: Crossover type ("Bullish", "Bearish", or "None")
            - Days_Since_Cross: Days since last EMA crossover (NaN if none)
            - Has_Crossover: Boolean, whether crossover detected
            - Trend_Slope: EMA9 5-day delta (momentum measure)
            - Price_vs_SMA20, Price_vs_SMA50: Distance from moving averages
            - SMA20, SMA50: Moving average values
            - Atr_Pct: ATR as % of price (volatility measure)
    
    Rate Limiting:
        - Sleeps 0.5s every 10 tickers to avoid yfinance throttling
        - For 100+ tickers, consider adding caching (see TODO #3)
    
    Error Handling:
        - Skips tickers with <30 days data (logs warning)
        - Catches yfinance errors per ticker (doesn't fail entire batch)
        - Returns empty DataFrame if all tickers fail
    
    Raises:
        ValueError: If required input columns missing
    
    Example:
        >>> df_charted = compute_chart_signals(df_filtered)
        >>> print(df_charted[['Ticker', 'Regime', 'Signal_Type', 'Atr_Pct']].head())
        >>> trending = df_charted[df_charted['Regime'] == 'Trending']
    
    Performance:
        - ~1 second per ticker (yfinance fetch + calculations)
        - 50 tickers ≈ 55 seconds with rate limiting
        - Consider parallel processing for >200 tickers (future enhancement)
    """
    # 🚨 HARD RULE: This step must NOT overwrite or infer 'Signal_Type' or 'Regime'.
    # These columns are authoritative outputs of Step 2 ONLY.
    # Any chart-derived signal must be namespaced (e.g., 'Chart_Signal_Type', 'Chart_Regime').
    
    validate_input(df, ['Ticker', 'IVHV_gap_30D', 'Signal_Type', 'Regime'], 'Step 5')
    
    chart_results = []
    skipped_count = 0
    
    # Initialize Schwab client if available
    schwab_client = None
    if SCHWAB_AVAILABLE:
        try:
            # Load credentials from env (same as Step 0)
            client_id = os.getenv("SCHWAB_APP_KEY")
            client_secret = os.getenv("SCHWAB_APP_SECRET")
            
            if client_id and client_secret:
                schwab_client = SchwabClient(client_id, client_secret)
                logger.info("✅ Using Schwab for price history (Schwab-only mode)")
            else:
                logger.warning("⚠️ Schwab credentials not found - chart signals will use snapshot data only")
        except Exception as e:
            logger.warning(f"⚠️ Schwab client initialization failed: {e} - using snapshot data only")
            schwab_client = None
    else:
        logger.warning("⚠️ Schwab API not available - using snapshot data only")
    
    for idx, (_, row) in enumerate(df.iterrows()):
        ticker = row['Ticker']
        
        # Rate limiting
        if idx > 0 and idx % 10 == 0:
            time.sleep(0.5)
        
        try:
            # Fetch price history: Schwab-only
            hist = None
            chart_source = "Unknown"
            
            if schwab_client is not None:
                hist, status = fetch_schwab_price_history(schwab_client, ticker, days=180)
                if status == "OK" and hist is not None:
                    logger.debug(f"✅ {ticker}: Schwab price history ({len(hist)} days)")
                    chart_source = "Schwab"
                else:
                    logger.debug(f"⚠️ {ticker}: Schwab status {status} - using snapshot-derived data")
                    hist = None
                    chart_source = "Snapshot"
            else:
                # No Schwab client - use snapshot data with minimal chart signals
                chart_source = "Snapshot"
                logger.debug(f"📊 {ticker}: Using snapshot-derived signals only")
            
            # Skip if no data available and cannot derive from snapshot
            if hist is None or len(hist) < 30:
                # Instead of skipping, mark as snapshot-derived with limited chart data
                logger.debug(f"[LIMITED] {ticker}: Using snapshot data only (no price history)")
                chart_results.append({
                    "Ticker": ticker,
                    "Chart_Regime": "Unknown",
                    "Chart_EMA_Signal": "Unknown",
                    "Chart_Signal_Type": "None",
                    "Days_Since_Cross": float('nan'),
                    "Has_Crossover": False,
                    "Trend_Slope": float('nan'),
                    "Price_vs_SMA20": float('nan'),
                    "Price_vs_SMA50": float('nan'),
                    "SMA20": float('nan'),
                    "SMA50": float('nan'),
                    "Atr_Pct": float('nan'),
                    "Chart_Source": chart_source
                })
                continue
            
            close_prices = hist['Close']
            
            if len(close_prices) < 30:
                logger.warning(f"[SKIP] {ticker}: insufficient data ({len(close_prices)} days)")
                skipped_count += 1
                continue
            
            # ATR Calculation
            high = hist['High']
            low = hist['Low']
            prev_close = hist['Close'].shift(1)
            tr = pd.concat([
                high - low,
                (high - prev_close).abs(),
                (low - prev_close).abs()
            ], axis=1).max(axis=1)
            atr = tr.rolling(window=14).mean()
            atr_pct = atr.iloc[-1] / close_prices.iloc[-1] if close_prices.iloc[-1] != 0 else np.nan
            atr_value = round(atr_pct * 100, 2) if not pd.isna(atr_pct) else np.nan
            
            # Moving Averages
            ema9 = close_prices.ewm(span=9, adjust=False).mean()
            ema21 = close_prices.ewm(span=21, adjust=False).mean()
            sma20 = close_prices.rolling(window=20).mean()
            sma50 = close_prices.rolling(window=50).mean()
            
            # Trend Slope
            trend_slope = round(ema9.iloc[-1] - ema9.iloc[-5], 4) if len(ema9) >= 5 else np.nan
            
            # Price vs SMA
            price_vs_sma20 = close_prices.iloc[-1] - sma20.iloc[-1] if not np.isnan(sma20.iloc[-1]) else np.nan
            price_vs_sma50 = close_prices.iloc[-1] - sma50.iloc[-1] if not np.isnan(sma50.iloc[-1]) else np.nan
            
            # EMA Signal
            ema_signal = "Bullish" if ema9.iloc[-1] > ema21.iloc[-1] else "Bearish"
            
            # Crossover detection
            signal_series = (ema9 > ema21).astype(int)
            cross_diff = signal_series.diff()
            cross_dates = cross_diff[cross_diff != 0].index
            
            latest_date = close_prices.index[-1]
            valid_cross_dates = [d for d in cross_dates if d <= latest_date]
            
            if len(valid_cross_dates) > 0:
                last_cross = valid_cross_dates[-1]
                days_since_cross = (latest_date - last_cross).days
                chart_crossover_type = "Bullish" if ema9[last_cross] > ema21[last_cross] else "Bearish"
                has_crossover = True
            else:
                days_since_cross = float('nan')
                chart_crossover_type = "None"
                has_crossover = False
            
            # Chart Regime classification (namespaced)
            chart_regime = classify_regime({
                'Trend_Slope': trend_slope,
                'Atr_Pct': atr_value,
                'Price_vs_SMA20': price_vs_sma20,
                'SMA20': sma20.iloc[-1] if not np.isnan(sma20.iloc[-1]) else np.nan
            })
            
            chart_results.append({
                "Ticker": ticker,
                "Chart_Regime": chart_regime, # Namespaced
                "Chart_EMA_Signal": ema_signal, # Namespaced
                "Chart_Signal_Type": chart_crossover_type, # Namespaced
                "Days_Since_Cross": days_since_cross,
                "Has_Crossover": has_crossover,
                "Trend_Slope": trend_slope,
                "Price_vs_SMA20": round(price_vs_sma20, 2),
                "Price_vs_SMA50": round(price_vs_sma50, 2),
                "SMA20": round(sma20.iloc[-1], 2) if not np.isnan(sma20.iloc[-1]) else np.nan,
                "SMA50": round(sma50.iloc[-1], 2) if not np.isnan(sma50.iloc[-1]) else np.nan,
                "Atr_Pct": atr_value,
                "Chart_Source": chart_source
            })
            
        except Exception as e:
            logger.error(f"[ERROR] {ticker}: {type(e).__name__}: {str(e)}")
            skipped_count += 1
    
    if not chart_results:
        logger.error("❌ No chart results generated")
        return pd.DataFrame()
    
    chart_df = pd.DataFrame(chart_results)
    chart_df = chart_df.drop_duplicates(subset="Ticker", keep="last")
    logger.info(f"✅ Chart processing: {len(chart_df)} tickers charted ({skipped_count} skipped)")
    
    # Merge with original data
    # 🚨 ASSERTION: Ensure original 'Signal_Type' and 'Regime' are NOT overwritten.
    # They are authoritative from Step 2.
    df_charted = pd.merge(df, chart_df, on="Ticker", how="inner", suffixes=('_original', '_chart'))
    
    # Explicitly drop any potentially conflicting columns from the merge if they were not namespaced
    # (This is a defensive measure, as the above code should already namespace them)
    for col in ['Regime_chart', 'Signal_Type_chart']:
        if col in df_charted.columns:
            df_charted = df_charted.drop(columns=[col])
    
    logger.info(f"✅ Merge complete: {len(df_charted)} rows")
    
    return df_charted
