
# core/phase3_pcs_score.py (patched)

import pandas as pd
import numpy as np
from datetime import datetime
from scipy.stats import skew, kurtosis
import yfinance as yf

# === IV–HV Gap Calculation ===
def calculate_ivhv_gap(df: pd.DataFrame) -> pd.DataFrame:
    def fetch_hv(symbol, period="30d"):
        try:
            data = yf.download(symbol, period=period, interval='1d', progress=False)
            if 'Close' not in data.columns:
                return np.nan
            data['Returns'] = data['Close'].pct_change()
            return data['Returns'].std() * np.sqrt(252)
        except Exception as e:
            print(f"⚠️ Error fetching HV for {symbol}: {e}")
            return np.nan

    if "HV" not in df.columns or df["HV"].isnull().all():
        df['HV'] = df['Underlying'].apply(fetch_hv)

    df['IV Mid'] = pd.to_numeric(df['IV Mid'], errors='coerce')
    df['HV'] = pd.to_numeric(df['HV'], errors='coerce')
    df['IVHV_Gap_Entry'] = df['IV Mid'] - df['HV']
    return df

# === Skew & Kurtosis Calculation ===
def calculate_skew_and_kurtosis(df: pd.DataFrame) -> pd.DataFrame:
    if "GroupKey" in df.columns:
        def safe_skew(x):
            x_clean = x.dropna()
            return skew(x_clean) if len(x_clean) > 1 else np.nan

        def safe_kurt(x):
            x_clean = x.dropna()
            return kurtosis(x_clean) if len(x_clean) > 1 else np.nan

        df["Skew_Entry"] = df.groupby("GroupKey")["IV Mid"].transform(safe_skew)
        df["Kurtosis_Entry"] = df.groupby("GroupKey")["IV Mid"].transform(safe_kurt)

        print(f"📉 Skew nulls: {df['Skew_Entry'].isnull().sum()} | GroupKey count: {df['GroupKey'].nunique()}")
    else:
        val_skew = skew(df["IV Mid"].dropna())
        val_kurt = kurtosis(df["IV Mid"].dropna())
        df["Skew_Entry"] = val_skew
        df["Kurtosis_Entry"] = val_kurt

    # 🩹 Fallback for directional or small-group trades
    df["Skew_Entry"] = df["Skew_Entry"].fillna(0.0)
    df["Kurtosis_Entry"] = df["Kurtosis_Entry"].fillna(0.0)
    print("🩹 Applied fallback: Skew/Kurtosis set to 0 where missing")
    return df

# === PCS Scoring ===
def calculate_pcs(df: pd.DataFrame) -> pd.DataFrame:
    # Ensure IVHV Gap is present
    if "IVHV_Gap_Entry" not in df.columns or df["IVHV_Gap_Entry"].isnull().all():
        df = calculate_ivhv_gap(df)

    def calculate_row_subscores(row):
        gamma = np.nan_to_num(pd.to_numeric(row.get("Gamma", 0)), nan=0)
        vega = np.nan_to_num(pd.to_numeric(row.get("Vega", 0)), nan=0)
        roi = np.nan_to_num(pd.to_numeric(row.get("% Total G/L", 0)), nan=0)
        ivhv_gap = np.nan_to_num(pd.to_numeric(row.get("IVHV_Gap_Entry", 0)), nan=0)

        gamma_score = min(gamma * 1000, 25)
        vega_score = min(vega * 80, 25)
        roi_score = 15 if roi >= 3.0 else 10 if roi >= 2.0 else 5
        ivhv_score = 5 if ivhv_gap >= 5.0 else 0

        return pd.Series([gamma_score, vega_score, roi_score, ivhv_score])

    today = pd.to_datetime(datetime.now().date())
    if "Expiration" in df.columns:
        df['DTE'] = (pd.to_datetime(df['Expiration']) - today).dt.days

    df[["PCS_GammaScore", "PCS_VegaScore", "PCS_ROIScore", "PCS_IVHVBonus"]] = df.apply(
        calculate_row_subscores, axis=1
    )
    df["PCS"] = df[["PCS_GammaScore", "PCS_VegaScore", "PCS_ROIScore", "PCS_IVHVBonus"]].sum(axis=1)

    if "GroupKey" in df.columns:
        df["PCS_GroupAvg"] = df.groupby("GroupKey")["PCS"].transform("mean")

    df["PCS_Tier"] = df["PCS"].apply(
        lambda pcs: "Tier 1" if pcs >= 80 else "Tier 2" if pcs >= 70 else "Tier 3" if pcs >= 60 else "Tier 4"
    )

    df["Needs_Revalidation"] = (df["PCS"] < 65) | (df["Vega"] < 0.25)
    df["PCS_Drift"] = df["PCS"] - df.get("PCS_Entry", df["PCS"])
    df["Vega_ROC"] = df["Vega"] - df.get("Vega_Entry", df["Vega"])
    df["IV_Drift"] = df["IV Mid"] - df.get("IV_Entry", df["IV Mid"])

    return df
