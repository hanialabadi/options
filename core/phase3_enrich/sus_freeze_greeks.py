import numpy as np
import pandas as pd

def freeze_core_greeks(df):
    for col in ["Delta", "Gamma", "Theta", "Vega", "PCS"]:
        entry_col = f"{col}_Entry"
        print(f"🔍 Checking {col} → entry_col = {entry_col}")
        print("    In df:", col in df.columns, "/", entry_col in df.columns)

        # Ensure entry_col exists before referencing it
        if entry_col not in df.columns:
            df[entry_col] = np.nan

        # Perform safe combine
        df[entry_col] = df[entry_col].combine_first(df[col])

        # ✅ Now it's safe to print both
        print("    Head:", df[[col, entry_col]].head())

    return df
