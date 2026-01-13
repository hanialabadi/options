import numpy as np
import pandas as pd

# --- 1. Squeeze (Bollinger inside Keltner) ---
def compute_squeeze(df, bb_window=20, bb_std=2, kc_window=20, kc_mult=1.5):
    # Bollinger Bands
    bb_mean = df['Close'].rolling(bb_window).mean()
    bb_stddev = df['Close'].rolling(bb_window).std()
    df['BB_Upper'] = bb_mean + bb_std * bb_stddev
    df['BB_Lower'] = bb_mean - bb_std * bb_stddev
    # Keltner Channel
    ema = df['Close'].ewm(span=kc_window).mean()
    tr = df['High'] - df['Low']
    atr = tr.rolling(kc_window).mean()
    df['KC_Upper'] = ema + kc_mult * atr
    df['KC_Lower'] = ema - kc_mult * atr
    # Squeeze condition
    df['Squeeze_On'] = (df['BB_Upper'] < df['KC_Upper']) & (df['BB_Lower'] > df['KC_Lower'])
    return df['Squeeze_On'].iloc[-1]

# --- 2. Multi-Timeframe Trend Alignment (daily/weekly) ---
def compute_multitimeframe_trend(ticker):
    import yfinance as yf
    # Daily
    df_daily = yf.Ticker(ticker).history(period='6mo', interval='1d')
    ema9_d, ema21_d = df_daily['Close'].ewm(span=9).mean(), df_daily['Close'].ewm(span=21).mean()
    daily_trend = "Up" if ema9_d.iloc[-1] > ema21_d.iloc[-1] else "Down"
    # Weekly
    df_week = yf.Ticker(ticker).history(period='2y', interval='1wk')
    ema9_w, ema21_w = df_week['Close'].ewm(span=9).mean(), df_week['Close'].ewm(span=21).mean()
    weekly_trend = "Up" if ema9_w.iloc[-1] > ema21_w.iloc[-1] else "Down"
    trend_align = (daily_trend == weekly_trend)
    return daily_trend, weekly_trend, trend_align

# --- 3. Trend Age / Persistence (days EMA9 above EMA21) ---
def compute_trend_age(df):
    ema9 = df['Close'].ewm(span=9).mean()
    ema21 = df['Close'].ewm(span=21).mean()
    bullish = ema9 > ema21
    # Count consecutive days EMA9 > EMA21 (from end backwards)
    count = 0
    for val in reversed(bullish.tolist()):
        if val:
            count += 1
        else:
            break
    return count

# --- 4. Price Z-Score vs. SMA20 ---
def compute_price_zscore(df, window=20):
    sma = df['Close'].rolling(window).mean()
    std = df['Close'].rolling(window).std()
    zscore = (df['Close'] - sma) / std
    return zscore.iloc[-1]

# --- 5. Distance from 52-Week High/Low (as % of high/low) ---
def compute_distance_52w(df):
    price = df['Close'].iloc[-1]
    hi = df['Close'].rolling(252).max().iloc[-1]
    lo = df['Close'].rolling(252).min().iloc[-1]
    dist_hi = (price - hi) / hi if hi != 0 else np.nan
    dist_lo = (price - lo) / lo if lo != 0 else np.nan
    return dist_hi, dist_lo

# --- 6. Regime Classifier ---
def classify_regime(df):
    # Simple: ratio of ATR to SMA20, and how close price is to SMA20
    atr = (df['High'] - df['Low']).rolling(14).mean()
    sma20 = df['Close'].rolling(20).mean()
    ratio = atr.iloc[-1] / sma20.iloc[-1] if sma20.iloc[-1] != 0 else np.nan
    price_sma = abs(df['Close'].iloc[-1] - sma20.iloc[-1]) / sma20.iloc[-1] if sma20.iloc[-1] != 0 else np.nan
    if pd.isna(ratio) or pd.isna(price_sma):
        return "Unknown"
    if ratio < 0.01 and price_sma < 0.01:
        return "Range"
    elif ratio > 0.02:
        return "Trending"
    else:
        return "Choppy"

# --- 7. Composite: Compute all features for a DataFrame (OHLCV, e.g. yfinance history) ---
def compute_all_features(df, ticker=None):
    features = {}
    try:
        features['Squeeze_On'] = compute_squeeze(df)
    except Exception as e:
        features['Squeeze_On'] = np.nan
    try:
        features['Trend_Age'] = compute_trend_age(df)
    except Exception as e:
        features['Trend_Age'] = np.nan
    try:
        features['Price_ZScore_SMA20'] = compute_price_zscore(df)
    except Exception as e:
        features['Price_ZScore_SMA20'] = np.nan
    try:
        hi, lo = compute_distance_52w(df)
        features['Dist_52w_High'] = hi
        features['Dist_52w_Low'] = lo
    except Exception as e:
        features['Dist_52w_High'] = np.nan
        features['Dist_52w_Low'] = np.nan
    try:
        features['Regime'] = classify_regime(df)
    except Exception as e:
        features['Regime'] = "Unknown"
    if ticker:
        try:
            daily_trend, weekly_trend, trend_align = compute_multitimeframe_trend(ticker)
            features['Daily_Trend'] = daily_trend
            features['Weekly_Trend'] = weekly_trend
            features['Trend_Aligned'] = trend_align
        except Exception as e:
            features['Daily_Trend'] = "Unknown"
            features['Weekly_Trend'] = "Unknown"
            features['Trend_Aligned'] = np.nan
    return features
