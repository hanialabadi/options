# core/phase6_freeze/freezer_modules/freeze_entry_ivhv.py

import pandas as pd
from pathlib import Path
from datetime import datetime

def freeze_entry_ivhv(df_new, mode="flat"):
    """
    Freezes IV/HV entry data for both flat and leg-level trade DataFrames.

    Parameters:
    -----------
    df_new : pd.DataFrame
        Flattened or leg-level trades (must include 'Ticker')

    mode : str
        'flat' → freezes fields for df_flat (TradeID-level)
        'legs' → freezes fields for legs_df (LegID-level)

    Returns:
    --------
    pd.DataFrame with frozen IV, HV, and IVHV gap fields
    """

    df = df_new.copy()

    # === Load today's snapshot ===
    TODAY = datetime.today().strftime("%Y-%m-%d")
    archive_path = Path(f"/Users/haniabadi/Documents/Github/options/data/ivhv_archive/ivhv_snapshot_{TODAY}.csv")

    try:
        df_snap = pd.read_csv(archive_path)

        # Columns needed in all cases
        required = {"Ticker", "IV_30_D_Call", "HV_30_D_Cur"}
        if not required.issubset(df_snap.columns):
            raise ValueError(f"[❌ freeze_entry_ivhv] Missing columns in IVHV snapshot: {required - set(df_snap.columns)}")

        # === Merge on Ticker ===
        df = df.merge(df_snap[["Ticker", "IV_30_D_Call", "HV_30_D_Cur"]], on="Ticker", how="left")

        if mode == "flat":
            df["IV_Entry"] = df["IV_30_D_Call"]
            df["HV30_Entry"] = df["HV_30_D_Cur"]
            df["IVHV_EntryGap"] = df["IV_Entry"] - df["HV30_Entry"]

        elif mode == "legs":
            df["IV_Leg_Entry"] = df["IV_30_D_Call"]
            df["HV30_Leg_Entry"] = df["HV_30_D_Cur"]
            df["IVHV_Leg_EntryGap"] = df["IV_Leg_Entry"] - df["HV30_Leg_Entry"]

        else:
            raise ValueError(f"[❌ freeze_entry_ivhv] Invalid mode: {mode}")

        # Clean up temp cols
        df.drop(columns=["IV_30_D_Call", "HV_30_D_Cur"], inplace=True)

    except Exception as e:
        print(f"[❌ freeze_entry_ivhv] Failed to load IVHV snapshot: {e}")
        if mode == "flat":
            df["IV_Entry"] = None
            df["HV30_Entry"] = None
            df["IVHV_EntryGap"] = None
        else:
            df["IV_Leg_Entry"] = None
            df["HV30_Leg_Entry"] = None
            df["IVHV_Leg_EntryGap"] = None

    return df
