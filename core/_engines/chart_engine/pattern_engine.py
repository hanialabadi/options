import numpy as np
import pandas as pd


# === 🕯️ Candlestick Pattern Detection ===
def detect_candlestick_patterns(df):
    patterns = []
    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else last

    body = abs(last['Close'] - last['Open'])
    upper_shadow = last['High'] - max(last['Close'], last['Open'])
    lower_shadow = min(last['Close'], last['Open']) - last['Low']
    full_range = last['High'] - last['Low']

    if upper_shadow > 2 * body and lower_shadow < 0.1 * body and last['Close'] < last['Open']:
        patterns.append({"tag": "ShootingStar", "confidence": 0.9, "exit_flag": True})
    if abs(last['Open'] - last['Close']) <= 0.05 * full_range:
        patterns.append({"tag": "Doji", "confidence": 0.8, "exit_flag": True})
    if lower_shadow > 2 * body and upper_shadow < 0.1 * body and last['Close'] > last['Open']:
        patterns.append({"tag": "Hammer", "confidence": 0.85, "exit_flag": False})
    if last['Open'] < last['Close'] < prev['Open'] and last['Open'] > prev['Close']:
        patterns.append({"tag": "BullishEngulfing", "confidence": 0.8, "exit_flag": False})
    if last['Open'] > last['Close'] > prev['Open'] and last['Close'] < prev['Close']:
        patterns.append({"tag": "BearishEngulfing", "confidence": 0.8, "exit_flag": True})

    return patterns, {
        "Candle_Body": body,
        "Candle_UpperShadow": upper_shadow,
        "Candle_LowerShadow": lower_shadow,
        "Candle_FullRange": full_range
    }
