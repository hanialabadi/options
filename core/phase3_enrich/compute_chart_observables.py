"""
Phase 3 Chart Observables - Trend & Momentum Indicators

PURPOSE:
    Compute observable market context from OHLCV data.
    Answer: "What is the trend/momentum RIGHT NOW?"

PHILOSOPHY:
    Phase 1-4: Pure observation (no trader decisions)
    - Phase 3: "RSI is 72" (observable)
    - Phase 8+: "Exit because RSI > 70" (decision)

DESIGN:
    - Reuses Schwab OHLCV from scan engine (cached, no re-fetch)
    - Computes: SMA, RSI, MACD, Support/Resistance
    - Outputs: Observable indicators only (no thresholds/signals)

CONTRACT:
    Input:  DataFrame with 'Underlying_Ticker' column
    Output: Same DataFrame + chart observable columns:
        - UL_Trend (Uptrend/Downtrend/Range/Unknown)
        - UL_RSI (0-100, current RSI value)
        - UL_MACD_Signal (Bullish/Bearish/Neutral)
        - UL_Price_vs_SMA20 (% above/below SMA20)
        - UL_Support (nearest support level)
        - UL_Resistance (nearest resistance level)
        - UL_Chart_Available (bool: data computed successfully)
"""

import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def compute_chart_observables(df: pd.DataFrame, client=None) -> pd.DataFrame:
    """
    Compute trend and momentum indicators from Schwab OHLCV data.
    """
    # Import dependencies
    try:
        import pandas_ta as ta
        from core.scan_engine.step0_schwab_snapshot import fetch_price_history_with_retry
        from core.scan_engine.schwab_api_client import SchwabClient
    except ImportError as e:
        logger.error(f"Failed to import dependencies: {e}")
        logger.warning("⚠️  Skipping chart observables (missing: pandas_ta or scan_engine)")
        df['UL_Chart_Available'] = False
        return df
    
    # Initialize Schwab client
    if client is None:
        try:
            client = SchwabClient()
        except Exception as e:
            logger.error(f"Failed to initialize SchwabClient: {e}")
            logger.warning("⚠️  Skipping chart observables (Schwab auth failed)")
            df['UL_Chart_Available'] = False
            return df
    
    # Determine ticker column - ALWAYS use Underlying_Ticker for chart signals
    # This enforces the canonical symbol identity law.
    if "Underlying_Ticker" in df.columns:
        ticker_col = "Underlying_Ticker"
    elif "Underlying" in df.columns:
        ticker_col = "Underlying"
    else:
        logger.error("Missing 'Underlying_Ticker' or 'Underlying' column for chart observables")
        df['UL_Chart_Available'] = False
        return df

    # Get unique underlyings
    underlyings = df[ticker_col].dropna().unique()
    logger.info(f"📈 Computing chart observables for {len(underlyings)} underlyings...")
    
    # Compute indicators per underlying
    chart_cache = {}
    success_count = 0
    
    for ticker in underlyings:
        try:
            # Reuse cached Schwab price history
            hist, status = fetch_price_history_with_retry(client, ticker, use_cache=True)
            
            if hist is None or len(hist) < 20:
                logger.debug(f"⚠️  {ticker}: Insufficient data for indicators (need 20+ bars)")
                chart_cache[ticker] = _create_empty_chart_data()
                continue
            
            # Compute indicators
            hist['SMA20'] = hist['close'].rolling(20).mean()
            hist['SMA50'] = hist['close'].rolling(50).mean() if len(hist) >= 50 else np.nan
            hist['RSI'] = ta.rsi(hist['close'], length=14)
            
            # MACD
            macd = ta.macd(hist['close'])
            if macd is not None and len(macd.columns) >= 2:
                hist['MACD'] = macd.iloc[:, 0]
                hist['MACD_Signal'] = macd.iloc[:, 1]
            else:
                hist['MACD'] = np.nan
                hist['MACD_Signal'] = np.nan
            
            latest = hist.iloc[-1]
            
            # Trend classification
            trend = _classify_trend(latest['close'], latest.get('SMA20'), latest.get('SMA50'))
            
            # MACD signal
            macd_signal = _classify_macd(latest.get('MACD'), latest.get('MACD_Signal'))
            
            # Price vs SMA20
            if pd.notna(latest.get('SMA20')) and latest['SMA20'] > 0:
                price_vs_sma20 = ((latest['close'] / latest['SMA20']) - 1) * 100
            else:
                price_vs_sma20 = np.nan
            
            # Support/Resistance
            support = hist['low'].tail(20).min()
            resistance = hist['high'].tail(20).max()
            
            chart_cache[ticker] = {
                'Trend': trend,
                'RSI': latest.get('RSI'),
                'MACD_Signal': macd_signal,
                'Price_vs_SMA20': price_vs_sma20,
                'Support': support,
                'Resistance': resistance,
                'Available': True
            }
            success_count += 1
            
        except Exception as e:
            logger.warning(f"⚠️  {ticker}: Exception during chart computation: {e}")
            chart_cache[ticker] = _create_empty_chart_data()
    
    # Attach to positions
    df['UL_Trend'] = df[ticker_col].map(lambda t: chart_cache.get(t, {}).get('Trend'))
    df['UL_RSI'] = df[ticker_col].map(lambda t: chart_cache.get(t, {}).get('RSI'))
    df['UL_MACD_Signal'] = df[ticker_col].map(lambda t: chart_cache.get(t, {}).get('MACD_Signal'))
    df['UL_Price_vs_SMA20'] = df[ticker_col].map(lambda t: chart_cache.get(t, {}).get('Price_vs_SMA20'))
    df['UL_Support'] = df[ticker_col].map(lambda t: chart_cache.get(t, {}).get('Support'))
    df['UL_Resistance'] = df[ticker_col].map(lambda t: chart_cache.get(t, {}).get('Resistance'))
    df['UL_Chart_Available'] = df[ticker_col].map(lambda t: chart_cache.get(t, {}).get('Available', False))
    
    return df


def _classify_trend(close: float, sma20: float, sma50: float) -> str:
    if pd.isna(close) or pd.isna(sma20):
        return "Unknown"
    if pd.isna(sma50):
        if close > sma20: return "Uptrend"
        elif close < sma20: return "Downtrend"
        else: return "Range"
    if close > sma20 and sma20 > sma50: return "Uptrend"
    elif close < sma20 and sma20 < sma50: return "Downtrend"
    else: return "Range"


def _classify_macd(macd: float, macd_signal: float) -> str:
    if pd.isna(macd) or pd.isna(macd_signal): return "Neutral"
    if macd > macd_signal: return "Bullish"
    else: return "Bearish"


def _create_empty_chart_data() -> dict:
    return {
        'Trend': "Unknown",
        'RSI': np.nan,
        'MACD_Signal': "Neutral",
        'Price_vs_SMA20': np.nan,
        'Support': np.nan,
        'Resistance': np.nan,
        'Available': False
    }
