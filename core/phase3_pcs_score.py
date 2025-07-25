# core/phase3_pcs_score.py

# %% 📚 Imports
import pandas as pd
import numpy as np
from datetime import datetime
from scipy.stats import skew, kurtosis
import yfinance as yf

# %% 🔢 Sub-phase 1: PCS Scoring
def calculate_pcs(df: pd.DataFrame) -> pd.DataFrame:
    def calculate_row_score(row):
        gamma = np.nan_to_num(pd.to_numeric(row.get("Gamma", 0)), nan=0)
        vega = np.nan_to_num(pd.to_numeric(row.get("Vega", 0)), nan=0)
        roi = np.nan_to_num(pd.to_numeric(row.get("% Total G/L", 0)), nan=0)
        score = min(gamma * 1000, 25) + min(vega * 80, 25)
        score += 15 if roi >= 3.0 else 10 if roi >= 2.0 else 5
        return score

    today = pd.to_datetime(datetime.now().date())
    if "Expiration" in df.columns:
        df['DTE'] = (df['Expiration'] - today).dt.days
    df['PCS'] = df.apply(calculate_row_score, axis=1)
    print("✅ Sub-phase 1: PCS calculated.")
    return df

# %% 📉 Sub-phase 2: IVHV Gap
def fetch_historical_volatility(symbol, period="30d"):
    try:
        data = yf.download(symbol, period=period, interval='1d', progress=False)
        if 'Close' not in data.columns:
            return None
        data['Returns'] = data['Close'].pct_change()
        return data['Returns'].std() * (252 ** 0.5)
    except Exception as e:
        print(f"⚠️ Error fetching HV for {symbol}: {e}")
        return None

def calculate_ivhv_gap(df: pd.DataFrame) -> pd.DataFrame:
    if 'Underlying' not in df.columns:
        raise ValueError("❌ Missing 'Underlying' column in DataFrame.")

    print("⏳ Fetching HV for underlyings...")
    df['HV'] = df['Underlying'].apply(fetch_historical_volatility)

    df['IV Mid'] = pd.to_numeric(df['IV Mid'], errors='coerce')
    df['HV'] = pd.to_numeric(df['HV'], errors='coerce')
    df['IVHV_Gap_Entry'] = df['IV Mid'] - df['HV']
    print("✅ Sub-phase 2: IVHV Gap calculated.")
    return df

# %% 📊 Sub-phase 3: Skew & Kurtosis
def calculate_skew_and_kurtosis(df: pd.DataFrame) -> pd.DataFrame:
    ivs = df['IV Mid'].dropna()
    df['Skew_Entry'] = skew(ivs)
    df['Kurtosis_Entry'] = kurtosis(ivs)
    print("✅ Sub-phase 3: Skew and Kurtosis calculated.")
    return df

# %% 🔁 Phase 3 Main Entry Point
def phase3_score_pcs(df: pd.DataFrame) -> pd.DataFrame:
    df = calculate_pcs(df)
    df = calculate_ivhv_gap(df)
    df = calculate_skew_and_kurtosis(df)
    print("✅ Phase 3 complete.")
    return df

# %% 🧪 Standalone test
if __name__ == "__main__":
    df = pd.read_csv("/path/to/sample_input.csv")  # Replace with real file
    df = phase3_score_pcs(df)
    print(df[["TradeID", "PCS", "IVHV_Gap_Entry", "Skew_Entry", "Kurtosis_Entry"]].head())
