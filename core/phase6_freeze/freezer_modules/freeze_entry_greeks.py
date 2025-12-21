# core/phase6_freeze/freezer_modules/freeze_entry_greeks.py

import pandas as pd

def freeze_entry_greeks(df: pd.DataFrame) -> pd.DataFrame:
    """
    Freezes Delta, Gamma, Vega, Theta as *_Entry fields.
    Assumes df only includes new trades.
    """
    df = df.copy()
    df["Delta_Entry"]  = df["Delta"]
    df["Gamma_Entry"]  = df["Gamma"]
    df["Vega_Entry"]   = df["Vega"]
    df["Theta_Entry"]  = df["Theta"]
    return df

