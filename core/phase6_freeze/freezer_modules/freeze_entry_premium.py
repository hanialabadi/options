# core/phase6_freeze/freezer_modules/freeze_entry_premium.py

import pandas as pd

def freeze_entry_premium(df_new, mode="flat"):
    """
    Freezes premium-related fields at entry for both flat and leg-level data.

    Parameters:
    -----------
    df_new : pd.DataFrame
        Flattened or leg-level trades (must include premium, strike, etc.)

    mode : str
        'flat' → for df_flat (TradeID-level)
        'legs' → for legs_df (LegID-level)

    Returns:
    --------
    pd.DataFrame with frozen premium-related fields
    """

    df = df_new.copy()

    try:
        if mode == "flat":
            if "Premium" in df.columns:
                df["Premium_Entry"] = df["Premium"]
            else:
                df["Premium_Entry"] = None

            # Optional: freeze breakeven or cost basis if present
            if "Breakeven" in df.columns:
                df["Breakeven_Entry"] = df["Breakeven"]
            if "CostBasis" in df.columns:
                df["CostBasis_Entry"] = df["CostBasis"]

        elif mode == "legs":
            if "Premium_Leg" in df.columns:
                df["Premium_Leg_Entry"] = df["Premium_Leg"]
            else:
                df["Premium_Leg_Entry"] = None

            # Compute breakeven per leg if fields are present
            if {"Strike", "Premium_Leg", "LegType"}.issubset(df.columns):
                df["Breakeven_Leg_Entry"] = df.apply(
                    lambda row: row["Strike"] + row["Premium_Leg"]
                    if row["LegType"] == "Call"
                    else row["Strike"] - row["Premium_Leg"],
                    axis=1
                )

            # Freeze option type for the leg
            option_col = None
            for alt in ["OptionType", "LegType", "Right"]:
                if alt in df.columns:
                    option_col = alt
                    break

            if option_col:
                df["OptionType_Entry"] = df[option_col]

        else:
            raise ValueError(f"[❌ freeze_entry_premium] Invalid mode: {mode}")

    except Exception as e:
        print(f"[❌ freeze_entry_premium] Error: {e}")

    return df
