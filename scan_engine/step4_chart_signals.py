"""
Step 4: Chart Signals - 180-Day Price History Analysis

NAMESPACING RULE: This step creates Chart_Signal_Type and Chart_Regime (namespaced).
- Chart_Signal_Type: Based on EMA9/EMA21 crossovers
- Chart_Regime: Based on ATR + Trend_Slope + Price position

CRITICAL: This step does NOT overwrite authoritative Signal_Type or Regime from Step 2.
The original Signal_Type/Regime flow through unchanged to downstream steps.

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
from utils.ta_lib_utils import calculate_sma, calculate_ema, calculate_atr, calculate_bbands, calculate_stoch, calculate_rsi, calculate_adx, calculate_macd
import config.indicator_settings as indicator_settings # Import as module

# Access settings directly from the imported module
REGIME_CLASSIFICATION_THRESHOLDS = indicator_settings.REGIME_CLASSIFICATION_THRESHOLDS
EMA_SETTINGS = indicator_settings.EMA_SETTINGS
SMA_SETTINGS = indicator_settings.SMA_SETTINGS
ATR_SETTINGS = indicator_settings.ATR_SETTINGS
BBANDS_SETTINGS = indicator_settings.BBANDS_SETTINGS
STOCH_SETTINGS = indicator_settings.STOCH_SETTINGS
MACD_SETTINGS = indicator_settings.MACD_SETTINGS # Import MACD settings
from core.shared.data_layer.technical_data_repository import insert_technical_indicators
from datetime import datetime

logger = logging.getLogger(__name__)

from core.shared.data_layer.price_history_loader import load_price_history
from .loaders.schwab_api_client import SchwabClient


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
    
    if overextension_pct > REGIME_CLASSIFICATION_THRESHOLDS["overextension_pct"]:
        return "Overextended"
    elif atr_pct is not None and atr_pct < REGIME_CLASSIFICATION_THRESHOLDS["atr_compressed_pct"]:
        return "Compressed"
    elif abs(trend_slope) > REGIME_CLASSIFICATION_THRESHOLDS["trend_slope_strong"]:
        return "Trending"
    elif abs(trend_slope) < REGIME_CLASSIFICATION_THRESHOLDS["trend_slope_weak"] and overextension_pct < REGIME_CLASSIFICATION_THRESHOLDS["price_near_sma_pct"]:
        return "Ranging"
    else:
        return "Neutral"


def compute_chart_signals(df: pd.DataFrame, snapshot_ts: datetime) -> pd.DataFrame:
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
    try:
        # Load credentials from env (same as Step 0)
        client_id = os.getenv("SCHWAB_APP_KEY")
        client_secret = os.getenv("SCHWAB_APP_SECRET")
        
        if client_id and client_secret:
            schwab_client = SchwabClient(client_id, client_secret)
            logger.info("✅ Using Schwab for price history (Schwab-first mode)")
    except Exception as e:
        logger.warning(f"⚠️ Schwab client initialization failed: {e} - falling back to cache/yfinance")
    
    for idx, (_, row) in enumerate(df.iterrows()):
        ticker = row['Ticker']
        
        try:
            # Fetch price history using unified loader
            hist, chart_source = load_price_history(ticker, days=180, client=schwab_client)
            
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
            
            # ATR Calculation using ta_lib_utils with configurable parameters
            atr_series = calculate_atr(hist['High'], hist['Low'], hist['Close'])
            atr_pct = atr_series.iloc[-1] / close_prices.iloc[-1] if close_prices.iloc[-1] != 0 else np.nan
            atr_value = round(atr_pct * 100, 2) if not pd.isna(atr_pct) else np.nan
            
            # Moving Averages using ta_lib_utils with configurable parameters
            ema9_series = calculate_ema(close_prices, timeperiod=EMA_SETTINGS["timeperiod_9"])
            ema21_series = calculate_ema(close_prices, timeperiod=EMA_SETTINGS["timeperiod_21"])
            sma20_series = calculate_sma(close_prices, timeperiod=SMA_SETTINGS["timeperiod_20"])
            sma50_series = calculate_sma(close_prices, timeperiod=SMA_SETTINGS["timeperiod_50"])

            # Bollinger Bands
            upperband, middleband, lowerband = calculate_bbands(close_prices, timeperiod=BBANDS_SETTINGS["timeperiod"], nbdevup=BBANDS_SETTINGS["nbdevup"], nbdevdn=BBANDS_SETTINGS["nbdevdn"], matype=BBANDS_SETTINGS["matype"])

            # Bollinger Bands
            upperband, middleband, lowerband = calculate_bbands(close_prices, timeperiod=BBANDS_SETTINGS["timeperiod"], nbdevup=BBANDS_SETTINGS["nbdevup"], nbdevdn=BBANDS_SETTINGS["nbdevdn"], matype=BBANDS_SETTINGS["matype"])

            # Stochastic Oscillator
            slowk, slowd = calculate_stoch(hist['High'], hist['Low'], hist['Close'], fastk_period=STOCH_SETTINGS["fastk_period"], slowk_period=STOCH_SETTINGS["slowk_period"], slowk_matype=STOCH_SETTINGS["slowk_matype"], slowd_period=STOCH_SETTINGS["slowd_period"], slowd_matype=STOCH_SETTINGS["slowd_matype"])
            
            # RSI and ADX Calculations
            rsi_series = calculate_rsi(close_prices, timeperiod=indicator_settings.RSI_SETTINGS["timeperiod"])
            adx_series = calculate_adx(hist['High'], hist['Low'], close_prices, timeperiod=indicator_settings.ADX_SETTINGS["timeperiod"])

            # MACD Calculation
            macd, macdsignal, macdhist = calculate_macd(
                close_prices,
                fastperiod=MACD_SETTINGS["fastperiod"],
                slowperiod=MACD_SETTINGS["slowperiod"],
                signalperiod=MACD_SETTINGS["signalperiod"]
            )

            # Trend Slope
            trend_slope = round(ema9_series.iloc[-1] - ema9_series.iloc[-5], 4) if len(ema9_series) >= 5 else np.nan
            
            # Price vs SMA
            price_vs_sma20 = close_prices.iloc[-1] - sma20_series.iloc[-1] if not np.isnan(sma20_series.iloc[-1]) else np.nan
            price_vs_sma50 = close_prices.iloc[-1] - sma50_series.iloc[-1] if not np.isnan(sma50_series.iloc[-1]) else np.nan
            
            # EMA Signal
            ema_signal = "Bullish" if ema9_series.iloc[-1] > ema21_series.iloc[-1] else "Bearish"
            
            # Crossover detection
            signal_series = (ema9_series > ema21_series).astype(int)
            cross_diff = signal_series.diff()
            cross_dates = cross_diff[cross_diff != 0].index
            
            latest_date = close_prices.index[-1]
            valid_cross_dates = [d for d in cross_dates if d <= latest_date]
            
            if len(valid_cross_dates) > 0:
                last_cross = valid_cross_dates[-1]
                days_since_cross = (latest_date - last_cross).days
                chart_crossover_type = "Bullish" if ema9_series[last_cross] > ema21_series[last_cross] else "Bearish"
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
                'SMA20': sma20_series.iloc[-1] if not np.isnan(sma20_series.iloc[-1]) else np.nan
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
                "SMA20": round(sma20_series.iloc[-1], 2) if not np.isnan(sma20_series.iloc[-1]) else np.nan,
                "SMA50": round(sma50_series.iloc[-1], 2) if not np.isnan(sma50_series.iloc[-1]) else np.nan,
                "Atr_Pct": atr_value,
                "Chart_Source": chart_source,
                # Add individual indicator values to chart_results
                "RSI": rsi_series.iloc[-1] if not np.isnan(rsi_series.iloc[-1]) else np.nan,
                "ADX": adx_series.iloc[-1] if not np.isnan(adx_series.iloc[-1]) else np.nan,
                "EMA9": ema9_series.iloc[-1] if not np.isnan(ema9_series.iloc[-1]) else np.nan,
                "EMA21": ema21_series.iloc[-1] if not np.isnan(ema21_series.iloc[-1]) else np.nan,
                "MACD": macd.iloc[-1] if not np.isnan(macd.iloc[-1]) else np.nan,
                "MACD_Signal": macdsignal.iloc[-1] if not np.isnan(macdsignal.iloc[-1]) else np.nan,
                "UpperBand_20": upperband.iloc[-1] if not np.isnan(upperband.iloc[-1]) else np.nan,
                "MiddleBand_20": middleband.iloc[-1] if not np.isnan(middleband.iloc[-1]) else np.nan,
                "LowerBand_20": lowerband.iloc[-1] if not np.isnan(lowerband.iloc[-1]) else np.nan,
                "SlowK_5_3": slowk.iloc[-1] if not np.isnan(slowk.iloc[-1]) else np.nan,
                "SlowD_5_3": slowd.iloc[-1] if not np.isnan(slowd.iloc[-1]) else np.nan,
                "IV_Rank_30D": row.get('IV_Rank_30D', np.nan), # From snapshot
                "PCS_Score_V2": row.get('PCS_Score_V2', np.nan) # From snapshot
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
    df_charted = pd.merge(df, chart_df, on="Ticker", how="inner", suffixes=('', '_chart'))
    
    # Drop chart-suffixed duplicates to preserve original columns
    # This keeps Price_vs_SMA20, Price_vs_SMA50 etc from Step 2
    chart_suffix_cols = [col for col in df_charted.columns if col.endswith('_chart')]
    if chart_suffix_cols:
        logger.debug(f"Dropping {len(chart_suffix_cols)} chart-suffixed duplicates: {chart_suffix_cols[:5]}")
        df_charted = df_charted.drop(columns=chart_suffix_cols)
    
    logger.info(f"✅ Merge complete: {len(df_charted)} rows, {len(df_charted.columns)} columns")
    
    # 🛡️ GOVERNANCE: Lock Phase 3 Hard Gate (Technicals)
    from core.shared.governance.contracts import validate_phase_output
    validate_phase_output(
        df_charted, 
        phase="P3",
        required_cols=['Signal_Type', 'Regime'],
        enum_checks={
            'Signal_Type': ['Bullish', 'Bearish', 'Bidirectional']
        }
    )
    
    return df_charted
