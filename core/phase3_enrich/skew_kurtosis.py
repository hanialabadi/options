import pandas as pd
import numpy as np
from scipy.stats import skew, kurtosis

# === Skew & Kurtosis Calculation (by TradeID) ===
def calculate_skew_and_kurtosis(df: pd.DataFrame) -> pd.DataFrame:
    if "TradeID" not in df.columns:
        raise ValueError("❌ 'TradeID' column is required for Skew/Kurtosis calculation.")

    def safe_skew(x):
        x_clean = x.dropna()
        return skew(x_clean) if len(x_clean) > 1 else np.nan

    def safe_kurt(x):
        x_clean = x.dropna()
        return kurtosis(x_clean) if len(x_clean) > 1 else np.nan

    df["Skew"] = df.groupby("TradeID")["IV Mid"].transform(safe_skew)
    df["Kurtosis"] = df.groupby("TradeID")["IV Mid"].transform(safe_kurt)

    print(f"📉 Skew nulls: {df['Skew'].isnull().sum()} | TradeIDs: {df['TradeID'].nunique()}")

    # 🩹 Fallback for single-leg trades
    df["Skew"] = df["Skew"].fillna(0.0)
    df["Kurtosis"] = df["Kurtosis"].fillna(0.0)
    print("🩹 Applied fallback: Skew/Kurtosis set to 0 where missing")
    return df
