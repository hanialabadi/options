import pandas as pd
from datetime import datetime

def load_input_df(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)

    # Normalize date columns
    if "TradeDate" in df.columns:
        df["TradeDate"] = pd.to_datetime(df["TradeDate"], errors="coerce")
    if "Expiration" in df.columns:
        df["Expiration"] = pd.to_datetime(df["Expiration"], errors="coerce")

    # Fill missing TradeID or Strategy if applicable
    df["TradeID"] = df.get("TradeID", "MISSING")
    df["Strategy"] = df.get("Strategy", "Unknown")

    # Type enforcement for Greeks
    greek_cols = ["Delta", "Gamma", "Theta", "Vega", "PCS", "PCS_Entry"]
    for col in greek_cols:
        df[col] = pd.to_numeric(df.get(col, 0), errors="coerce").fillna(0)

    return df
