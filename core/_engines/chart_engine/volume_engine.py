import numpy as np
import pandas as pd

# === 🔊 Volume Overlay Computation ===
def compute_volume_overlays(df):
    df["Volume"] = df["Volume"].fillna(0)  # ⛑ Prevent crash in OBV
    df['OBV'] = (np.sign(df['Close'].diff()) * df['Volume']).fillna(0).cumsum()
    df['ATR'] = df['High'].rolling(14).max() - df['Low'].rolling(14).min()
    df['Volume_Trend'] = df['Volume'].rolling(3).mean().iloc[-1] < df['Volume'].rolling(10).mean().iloc[-1]
    return df
