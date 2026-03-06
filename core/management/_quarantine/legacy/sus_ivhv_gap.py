import pandas as pd
import numpy as np
import yfinance as yf

# === IV–HV Gap Calculation (DISABLED – No longer used in PCS) ===
def calculate_ivhv_gap(df: pd.DataFrame) -> pd.DataFrame:
    # def fetch_hv(symbol: str, period: str = "30d") -> float:
    #     try:
    #         data = yf.download(symbol, period=period, interval='1d', progress=False)
    #         if 'Close' not in data.columns or data.empty:
    #             return np.nan
    #         data['Returns'] = data['Close'].pct_change()
    #         return data['Returns'].std() * np.sqrt(252)  # Annualized
    #     except Exception as e:
    #         print(f"⚠️ Error fetching HV for {symbol}: {e}")
    #         return np.nan

    # # Compute HV if missing
    # if "HV" not in df.columns or df["HV"].isnull().all():
    #     df["HV"] = df["Underlying"].apply(fetch_hv)

    # # ✅ Strip % and convert IV columns
    # iv_cols = ["IV Mid", "IV Bid", "IV Ask"]
    # for col in iv_cols:
    #     if col in df.columns and df[col].dtype == "object":
    #         df[col] = (
    #             df[col].str.replace('%', '', regex=False)
    #                     .astype(float)
    #         )

    # # Ensure HV is numeric
    # df["HV"] = pd.to_numeric(df["HV"], errors="coerce")
    # df["HV30"] = df["HV"] * 100

    # # ✅ Final IVHV Gap (in percentage points)
    # df["IVHV_Gap"] = df["IV Mid"] - df["HV30"]

    return df  # Return unchanged
