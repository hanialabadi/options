import numpy as np
import pandas as pd



# === Trend Structure ===
def get_chart_trend_state(df):
    ema9 = df['Close'].ewm(span=9).mean()
    ema21 = df['Close'].ewm(span=21).mean()
    sma20 = df['Close'].rolling(20).mean()
    sma50 = df['Close'].rolling(50).mean()

    trend_data = {
        "EMA9": ema9.iloc[-1],
        "EMA21": ema21.iloc[-1],
        "SMA20": sma20.iloc[-1],
        "SMA50": sma50.iloc[-1]
    }

    overextended = df['Close'].iloc[-1] > 1.4 * sma20.iloc[-1] or df['Close'].iloc[-1] > 1.4 * sma50.iloc[-1]
    if ema9.iloc[-1] > ema21.iloc[-1]:
        trend_tag = 'Sustained Bullish' if not overextended else 'Overextended'
        score = 0.9
    elif ema9.iloc[-1] < ema21.iloc[-1]:
        trend_tag = 'Downtrend'
        score = 0.9
    else:
        trend_tag = 'Neutral'
        score = 0.5

    return trend_tag, score, trend_data
