import numpy as np
import pandas as pd

def freeze_additional_fields(df: pd.DataFrame) -> pd.DataFrame:
    # Fallback to Last if Premium is missing
    if "Premium" not in df.columns or df["Premium"].isnull().all():
        if "Last" in df.columns:
            df["Premium"] = df["Last"]

    freeze_map = {
        "Premium_Entry": "Premium",
        "Time_Val_Entry": "Time Val",
        "Intrinsic_Val_Entry": "Intrinsic Val",
        "IVHV_Gap_Entry": "IVHV_Gap",
        "Skew_Entry": "Skew",
        "Kurtosis_Entry": "Kurtosis",
        "BreakEven": "BreakEven",  # May be overwritten later
        "CostBasis_Entry": "Basis",
        "Entry_Price": "Last"
    }

    for freeze_col, live_col in freeze_map.items():
        if freeze_col not in df.columns:
            df[freeze_col] = np.nan
        if live_col in df.columns:
            df[freeze_col] = df[freeze_col].combine_first(df[live_col])

    return df