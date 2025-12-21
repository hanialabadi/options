# core/phase6_freeze/freezer_modules/freeze_entry_metadata.py

import pandas as pd

def freeze_entry_metadata(df_new, mode="flat"):
    """
    Freezes strategic metadata fields at entry for both flat and leg-level data.

    Parameters:
    -----------
    df_new : pd.DataFrame
        New trades to freeze metadata for

    mode : str
        'flat' → df_flat
        'legs' → legs_df

    Returns:
    --------
    pd.DataFrame with frozen metadata fields
    """

    df = df_new.copy()

    try:
        # Core metadata fields
        meta_fields = {
            "Strategy": "Strategy_Entry",
            "PCS": "PCS_Entry",
            "ConfidenceTier": "ConfidenceTier_Entry",
            "DTE": "DTE_Entry",
            "TradeDate": "TradeDate_Entry",
        }

        # Freeze basic metadata
        for src, tgt in meta_fields.items():
            if src in df.columns:
                df[tgt] = df[src]

        # Optional: earnings-related tags
        if "HasEarnings" in df.columns:
            df["HasEarnings_Entry"] = df["HasEarnings"]
        if "DaysToEarnings" in df.columns:
            df["EarningsDaysOut_Entry"] = df["DaysToEarnings"]

    except Exception as e:
        print(f"[❌ freeze_entry_metadata] Error: {e}")

    return df

