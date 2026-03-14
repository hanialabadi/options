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
    Classify market environment — ADX-primary with Murphy's tiered framework.

    NOTE: This is DESCRIPTIVE classification, not prescriptive.
    Separates "what the market is doing" from "what we should do".

    Decision tree:
        1. Overextended: price > 40% from SMA20 (structural concern, overrides ADX)
        2. Compressed: ATR_Rank < 20th percentile of own history AND ADX < 20
           (unusually quiet for THIS stock, not a universal threshold)
        3. ADX-driven tiers (Murphy: "ADX drop from >40 = trend weakening.
           Rise back above 20 = new trend starting."):
           - ADX < 20   → Ranging
           - 20 ≤ ADX < 30 → Emerging_Trend
           - 30 ≤ ADX < 40 → Trending
           - ADX ≥ 40      → Strong_Trend

    Returns one of: Strong_Trend, Trending, Emerging_Trend, Ranging,
                     Compressed, Overextended

    Args:
        row (dict): Dict with keys: Price_vs_SMA20, SMA20, ADX,
                     ATR_Rank (optional, 0-100 percentile of own history)
    """
    price_vs_sma20 = row.get('Price_vs_SMA20')
    sma20 = row.get('SMA20')

    if pd.isna(sma20) or pd.isna(price_vs_sma20) or sma20 == 0:
        return "Ranging"

    overextension_pct = abs(price_vs_sma20) / sma20

    _adx_raw = pd.to_numeric(row.get('ADX'), errors='coerce')
    _adx = float(_adx_raw) if pd.notna(_adx_raw) else 0
    _adx = float(_adx)
    _atr_rank = pd.to_numeric(row.get('ATR_Rank'), errors='coerce')

    # 1. Overextended — structural override (price far from mean)
    if overextension_pct > REGIME_CLASSIFICATION_THRESHOLDS["overextension_pct"]:
        return "Overextended"

    # 2. Compressed — ATR in bottom percentile of own history AND no trend
    _compressed_pctl = REGIME_CLASSIFICATION_THRESHOLDS["atr_compressed_percentile"]
    if (_atr_rank is not None and not pd.isna(_atr_rank)
            and _atr_rank < _compressed_pctl
            and _adx < REGIME_CLASSIFICATION_THRESHOLDS["adx_range_bound"]):
        return "Compressed"

    # 3. ADX-driven tiers (Murphy's framework)
    if _adx >= REGIME_CLASSIFICATION_THRESHOLDS["adx_trending"]:        # ≥ 40
        return "Strong_Trend"
    elif _adx >= REGIME_CLASSIFICATION_THRESHOLDS["adx_emerging"]:      # 30–39
        return "Trending"
    elif _adx >= REGIME_CLASSIFICATION_THRESHOLDS["adx_range_bound"]:   # 20–29
        return "Emerging_Trend"
    else:                                                                # < 20
        return "Ranging"


# ── Institutional-grade signal helpers (Murphy, Bulkowski, Raschke) ──────

def _classify_market_structure(highs: pd.Series, lows: pd.Series, lookback: int = 5) -> str:
    """
    Detect HH/HL or LH/LL swing point structure.
    Murphy Ch.4: "An uptrend is a succession of higher highs and higher lows."

    Returns: 'Uptrend', 'Downtrend', 'Consolidation', 'Unknown'
    """
    n = len(highs)
    if n < lookback * 4:
        return 'Unknown'

    swing_highs = []
    swing_lows = []

    for i in range(lookback, n - lookback):
        window_h = highs.iloc[i - lookback: i + lookback + 1]
        if highs.iloc[i] >= window_h.max():
            swing_highs.append((i, float(highs.iloc[i])))
        window_l = lows.iloc[i - lookback: i + lookback + 1]
        if lows.iloc[i] <= window_l.min():
            swing_lows.append((i, float(lows.iloc[i])))

    if len(swing_highs) < 2 or len(swing_lows) < 2:
        return 'Unknown'

    sh1, sh2 = swing_highs[-2][1], swing_highs[-1][1]
    sl1, sl2 = swing_lows[-2][1], swing_lows[-1][1]

    hh = sh2 > sh1 * 1.001  # tolerance for noise
    hl = sl2 > sl1 * 1.001
    lh = sh2 < sh1 * 0.999
    ll = sl2 < sl1 * 0.999

    if hh and hl:
        return 'Uptrend'
    elif lh and ll:
        return 'Downtrend'
    else:
        return 'Consolidation'


def _compute_obv_metrics(close: pd.Series, volume: pd.Series, period: int = 20) -> dict:
    """
    OBV slope + breakout volume ratio.
    Murphy Ch.7: "OBV determines if smart money is accumulating or distributing."
    Bulkowski (0.712): "Volume above average on breakout day = larger move."
    """
    if volume is None or len(volume) < period or volume.sum() == 0:
        return {'OBV_Slope': np.nan, 'Volume_Ratio': np.nan}

    price_change = close.diff()
    obv = (np.sign(price_change) * volume).fillna(0).cumsum()

    # OBV slope: percentage change over period (clamped to ±500% to prevent outliers)
    if len(obv) >= period and abs(obv.iloc[-period]) > 1e-6:
        obv_slope = (obv.iloc[-1] - obv.iloc[-period]) / abs(obv.iloc[-period]) * 100
        obv_slope = max(-500.0, min(500.0, obv_slope))
    else:
        obv_slope = np.nan

    # Volume ratio: current vs 20-day average (Bulkowski breakout confirmation)
    vol_sma = volume.rolling(period).mean()
    if not np.isnan(vol_sma.iloc[-1]) and vol_sma.iloc[-1] > 0:
        volume_ratio = float(volume.iloc[-1]) / float(vol_sma.iloc[-1])
    else:
        volume_ratio = np.nan

    return {
        'OBV_Slope': round(obv_slope, 2) if not pd.isna(obv_slope) else np.nan,
        'Volume_Ratio': round(volume_ratio, 2) if not pd.isna(volume_ratio) else np.nan,
    }


def _detect_divergence(price: pd.Series, indicator: pd.Series, lookback: int = 14) -> str:
    """
    Detect classical price/indicator divergence.
    Murphy (0.691): "Divergence between RSI and price when RSI is above 70 or
    below 30 is a serious warning that should be heeded."

    Returns: 'Bullish_Divergence', 'Bearish_Divergence', 'None'
    """
    if len(price) < lookback + 4 or len(indicator) < lookback + 4:
        return 'None'

    p = price.iloc[-(lookback + 4):]
    ind = indicator.iloc[-(lookback + 4):]

    mask = ~(p.isna() | ind.isna())
    p = p[mask]
    ind = ind[mask]
    if len(p) < 10:
        return 'None'

    peaks = []
    troughs = []
    for i in range(2, len(p) - 2):
        if p.iloc[i] > p.iloc[i - 1] and p.iloc[i] > p.iloc[i + 1] and p.iloc[i] > p.iloc[i - 2]:
            peaks.append(i)
        if p.iloc[i] < p.iloc[i - 1] and p.iloc[i] < p.iloc[i + 1] and p.iloc[i] < p.iloc[i - 2]:
            troughs.append(i)

    # Bearish divergence: price HH but indicator LH
    if len(peaks) >= 2:
        p1, p2 = peaks[-2], peaks[-1]
        if p.iloc[p2] > p.iloc[p1] and ind.iloc[p2] < ind.iloc[p1]:
            return 'Bearish_Divergence'

    # Bullish divergence: price LL but indicator HL
    if len(troughs) >= 2:
        t1, t2 = troughs[-2], troughs[-1]
        if p.iloc[t2] < p.iloc[t1] and ind.iloc[t2] > ind.iloc[t1]:
            return 'Bullish_Divergence'

    return 'None'


def _compute_weekly_bias(hist: pd.DataFrame, daily_ema_signal: str) -> str:
    """
    Multi-timeframe trend filter: resample daily to weekly, compare.
    Murphy (0.634): "weekly signals become trend filters for daily signals."

    Returns: 'ALIGNED', 'CONFLICTING', 'Unknown'
    """
    if len(hist) < 60:
        return 'Unknown'

    weekly = hist.resample('W').agg({
        'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'
    }).dropna()

    if len(weekly) < 12:
        return 'Unknown'

    w_ema9 = calculate_ema(weekly['Close'], timeperiod=9)
    w_ema21 = calculate_ema(weekly['Close'], timeperiod=21) if len(weekly) >= 21 else pd.Series([np.nan])

    if pd.isna(w_ema9.iloc[-1]):
        return 'Unknown'

    if not pd.isna(w_ema21.iloc[-1]):
        weekly_signal = 'Bullish' if w_ema9.iloc[-1] > w_ema21.iloc[-1] else 'Bearish'
    else:
        if len(w_ema9) >= 3 and not pd.isna(w_ema9.iloc[-3]):
            weekly_signal = 'Bullish' if w_ema9.iloc[-1] > w_ema9.iloc[-3] else 'Bearish'
        else:
            return 'Unknown'

    if daily_ema_signal == weekly_signal:
        return 'ALIGNED'
    else:
        return 'CONFLICTING'


def _detect_keltner_squeeze(upper_bb: pd.Series, lower_bb: pd.Series,
                             ema21: pd.Series, atr: pd.Series,
                             keltner_mult: float = 1.5) -> dict:
    """
    Keltner Channel squeeze detection (Raschke / Murphy 0.739).
    Squeeze: Bollinger Bands inside Keltner Bands = low vol compression.
    Fire: was in squeeze, now released = breakout imminent.
    """
    result = {'Squeeze_On': False, 'Squeeze_Fired': False}

    if (pd.isna(upper_bb.iloc[-1]) or pd.isna(lower_bb.iloc[-1])
            or pd.isna(ema21.iloc[-1]) or pd.isna(atr.iloc[-1])):
        return result

    k_upper = ema21 + keltner_mult * atr
    k_lower = ema21 - keltner_mult * atr

    in_squeeze = (upper_bb.iloc[-1] < k_upper.iloc[-1]) and (lower_bb.iloc[-1] > k_lower.iloc[-1])
    result['Squeeze_On'] = bool(in_squeeze)

    if len(upper_bb) >= 2 and len(k_upper) >= 2:
        was_squeeze = (upper_bb.iloc[-2] < k_upper.iloc[-2]) and (lower_bb.iloc[-2] > k_lower.iloc[-2])
        if was_squeeze and not in_squeeze:
            result['Squeeze_Fired'] = True

    return result


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
    
    Regime Classification (ADX-primary, Murphy's framework):
        - Overextended: Price >40% from SMA20 (structural override)
        - Compressed: ATR_Rank < 20th percentile of own history + ADX < 20
        - Strong_Trend: ADX ≥ 40 (Murphy: trend at full strength)
        - Trending: 30 ≤ ADX < 40 (confirmed trend)
        - Emerging_Trend: 20 ≤ ADX < 30 (new trend starting — Murphy)
        - Ranging: ADX < 20 (no trend persistence)
    
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

    # Pre-fetch SPY for relative strength computation (Murphy 0.740)
    _spy_close = None
    try:
        _spy_hist, _spy_src = load_price_history('SPY', days=180, client=schwab_client)
        if _spy_hist is not None and len(_spy_hist) >= 30:
            _spy_close = _spy_hist['Close']
            logger.info(f"✅ SPY loaded for relative strength ({len(_spy_hist)} bars, {_spy_src})")
        else:
            logger.warning("⚠️ SPY data unavailable — relative strength skipped")
    except Exception as e:
        logger.warning(f"⚠️ SPY fetch failed: {e} — relative strength skipped")

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
                    "Chart_Source": chart_source,
                    # Institutional-grade signals (defaults for limited data)
                    "Market_Structure": "Unknown",
                    "OBV_Slope": float('nan'),
                    "Volume_Ratio": float('nan'),
                    "RSI_Divergence": "None",
                    "MACD_Divergence": "None",
                    "Weekly_Trend_Bias": "Unknown",
                    "Keltner_Squeeze_On": False,
                    "Keltner_Squeeze_Fired": False,
                    "RS_vs_SPY_20d": float('nan'),
                })
                continue
            
            close_prices = hist['Close']
            
            if len(close_prices) < 30:
                logger.warning(f"[SKIP] {ticker}: insufficient data ({len(close_prices)} days)")
                skipped_count += 1
                continue
            
            # ATR Calculation using ta_lib_utils with configurable parameters
            atr_series = calculate_atr(hist['High'], hist['Low'], hist['Close'], timeperiod=ATR_SETTINGS["timeperiod"])
            atr_pct = atr_series.iloc[-1] / close_prices.iloc[-1] if close_prices.iloc[-1] != 0 else np.nan
            atr_value = round(atr_pct * 100, 2) if not pd.isna(atr_pct) else np.nan

            # ATR_Rank: current ATR% percentile vs own history (like IV_Rank for IV)
            atr_pct_history = (atr_series / close_prices * 100).dropna()
            if len(atr_pct_history) >= 20 and not pd.isna(atr_value):
                atr_rank = round((atr_pct_history < atr_value).sum() / len(atr_pct_history) * 100, 1)
            else:
                atr_rank = np.nan
            
            # Moving Averages using ta_lib_utils with configurable parameters
            ema9_series = calculate_ema(close_prices, timeperiod=EMA_SETTINGS["timeperiod_9"])
            ema21_series = calculate_ema(close_prices, timeperiod=EMA_SETTINGS["timeperiod_21"])
            sma20_series = calculate_sma(close_prices, timeperiod=SMA_SETTINGS["timeperiod_20"])
            sma50_series = calculate_sma(close_prices, timeperiod=SMA_SETTINGS["timeperiod_50"])

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

            # Trend Slope — normalized as % change in EMA9 over 5 days
            if len(ema9_series) >= 5 and ema9_series.iloc[-5] != 0:
                trend_slope = round((ema9_series.iloc[-1] - ema9_series.iloc[-5]) / ema9_series.iloc[-5] * 100, 4)
            else:
                trend_slope = np.nan
            
            # BB_Position: 0 = at lower band, 100 = at upper band (Murphy: band touch = overextended)
            _bb_width = upperband.iloc[-1] - lowerband.iloc[-1]
            if not np.isnan(_bb_width) and _bb_width > 0:
                bb_position = round((close_prices.iloc[-1] - lowerband.iloc[-1]) / _bb_width * 100, 1)
            else:
                bb_position = np.nan

            # MACD histogram value (Murphy: "most reliable MACD signal")
            macd_hist_val = round(macdhist.iloc[-1], 4) if not np.isnan(macdhist.iloc[-1]) else np.nan

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
            
            # Chart Regime classification (ADX-primary, Murphy's framework)
            chart_regime = classify_regime({
                'Price_vs_SMA20': price_vs_sma20,
                'SMA20': sma20_series.iloc[-1] if not np.isnan(sma20_series.iloc[-1]) else np.nan,
                'ADX': adx_series.iloc[-1] if not np.isnan(adx_series.iloc[-1]) else 0,
                'ATR_Rank': atr_rank,
            })

            # ── Institutional-grade signals ──────────────────────────
            # 1. Market Structure: HH/HL swing point detection (Murphy Ch.4)
            market_structure = _classify_market_structure(hist['High'], hist['Low'])

            # 2. OBV metrics: accumulation/distribution + breakout volume (Murphy Ch.7, Bulkowski)
            _volume = hist['Volume'] if 'Volume' in hist.columns else pd.Series(dtype=float)
            obv_metrics = _compute_obv_metrics(close_prices, _volume)

            # 3. Divergence detection (Murphy 0.691: "serious warning")
            rsi_div = _detect_divergence(close_prices, rsi_series, lookback=14)
            macd_div = _detect_divergence(close_prices, macdhist, lookback=14)

            # 4. Multi-timeframe weekly bias (Murphy 0.634: "weekly filters daily")
            weekly_bias = _compute_weekly_bias(hist, ema_signal)

            # 5. Keltner squeeze detection (Raschke / Murphy 0.739)
            squeeze = _detect_keltner_squeeze(upperband, lowerband, ema21_series, atr_series)

            # 6. Relative strength vs SPY (Murphy 0.740: intermarket analysis)
            rs_20d = np.nan
            if _spy_close is not None and len(close_prices) >= 20 and len(_spy_close) >= 20:
                _t_ret = (close_prices.iloc[-1] / close_prices.iloc[-20]) - 1
                _s_ret = (_spy_close.iloc[-1] / _spy_close.iloc[-20]) - 1
                rs_20d = round((_t_ret - _s_ret) * 100, 2)

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
                "ATR_Rank": atr_rank,
                "BB_Position": bb_position,
                "MACD_Histogram": macd_hist_val,
                "Chart_Source": chart_source,
                # Individual indicator values
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
                "PCS_Score_V2": row.get('PCS_Score_V2', np.nan), # From snapshot
                # Institutional-grade signals
                "Market_Structure": market_structure,
                "OBV_Slope": obv_metrics['OBV_Slope'],
                "Volume_Ratio": obv_metrics['Volume_Ratio'],
                "RSI_Divergence": rsi_div,
                "MACD_Divergence": macd_div,
                "Weekly_Trend_Bias": weekly_bias,
                "Keltner_Squeeze_On": squeeze['Squeeze_On'],
                "Keltner_Squeeze_Fired": squeeze['Squeeze_Fired'],
                "RS_vs_SPY_20d": rs_20d,
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
