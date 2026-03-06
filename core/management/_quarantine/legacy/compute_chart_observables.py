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
from utils.ta_lib_utils import calculate_sma, calculate_rsi, calculate_macd
from core.shared.data_layer.technical_data_repository import insert_technical_indicators
from datetime import datetime # Ensure datetime is imported for datetime.now()
from config.indicator_settings import SMA_SETTINGS, RSI_SETTINGS, MACD_SETTINGS

logger = logging.getLogger(__name__)


def compute_chart_observables(df: pd.DataFrame, snapshot_ts: datetime, client=None) -> pd.DataFrame:
    """
    Compute trend and momentum indicators from Schwab OHLCV data.
    """
    # Import dependencies
    try:
        from scan_engine.step0_schwab_snapshot import fetch_price_history_with_retry
        from scan_engine.schwab_api_client import SchwabClient
    except ImportError as e:
        logger.error(f"Failed to import dependencies: {e}")
        logger.warning("⚠️  Skipping chart observables (missing: scan_engine dependencies)")
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
            
            # Compute indicators using ta_lib_utils with configurable parameters
            sma20_series = calculate_sma(hist['close'], timeperiod=SMA_SETTINGS["timeperiod_20"])
            sma50_series = calculate_sma(hist['close'], timeperiod=SMA_SETTINGS["timeperiod_50"])
            rsi_series = calculate_rsi(hist['close'], timeperiod=RSI_SETTINGS["timeperiod"])
            
            # MACD using ta_lib_utils with configurable parameters
            macd_series, macdsignal_series, macdhist_series = calculate_macd(
                hist['close'],
                fastperiod=MACD_SETTINGS["fastperiod"],
                slowperiod=MACD_SETTINGS["slowperiod"],
                signalperiod=MACD_SETTINGS["signalperiod"]
            )
            
            latest = hist.iloc[-1]
            
            # Trend classification
            trend = _classify_trend(latest['close'], sma20_series.iloc[-1], sma50_series.iloc[-1])
            
            # MACD signal
            macd_signal = _classify_macd(macd_series.iloc[-1], macdsignal_series.iloc[-1])
            
            # Price vs SMA20
            if pd.notna(sma20_series.iloc[-1]) and sma20_series.iloc[-1] > 0:
                price_vs_sma20 = ((latest['close'] / sma20_series.iloc[-1]) - 1) * 100
            else:
                price_vs_sma20 = np.nan
            
            # Support/Resistance
            support = hist['low'].tail(20).min()
            resistance = hist['high'].tail(20).max()
            
            chart_cache[ticker] = {
                'Trend': trend,
                'RSI': rsi_series.iloc[-1],
                'MACD_Signal': macd_signal,
                'Price_vs_SMA20': price_vs_sma20,
                'Support': support,
                'Resistance': resistance,
                'Available': True
            }
            success_count += 1

            # Prepare data for ingestion into technical_indicators repository
            indicators_df = pd.DataFrame([{
                "Ticker": ticker,
                "Snapshot_TS": snapshot_ts, # Use current snapshot timestamp
                "RSI_14": rsi_series.iloc[-1] if not np.isnan(rsi_series.iloc[-1]) else np.nan,
                "ADX_14": np.nan,
                "SMA_20": sma20_series.iloc[-1] if not np.isnan(sma20_series.iloc[-1]) else np.nan,
                "SMA_50": sma50_series.iloc[-1] if not np.isnan(sma50_series.iloc[-1]) else np.nan,
                "EMA_9": np.nan,
                "EMA_21": np.nan,
                "ATR_14": np.nan,
                "MACD": macd_series.iloc[-1] if not np.isnan(macd_series.iloc[-1]) else np.nan,
                "MACD_Signal": macdsignal_series.iloc[-1] if not np.isnan(macdsignal_series.iloc[-1]) else np.nan,
                "UpperBand_20": np.nan,
                "MiddleBand_20": np.nan,
                "LowerBand_20": np.nan,
                "SlowK_5_3": np.nan,
                "SlowD_5_3": np.nan
            }])
            insert_technical_indicators(indicators_df)
            
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
        'Available': False,
        'UpperBand_20': np.nan,
        'MiddleBand_20': np.nan,
        'LowerBand_20': np.nan,
        'SlowK_5_3': np.nan,
        'SlowD_5_3': np.nan
    }
