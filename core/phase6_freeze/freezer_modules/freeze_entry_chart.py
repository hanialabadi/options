# core/phase6_freeze/freezer_modules/freeze_entry_chart.py

import pandas as pd

def freeze_entry_chart(df_new, mode="flat"):
    """
    Freezes chart signal fields at entry.
    
    Parameters:
    -----------
    df_new : pd.DataFrame
        DataFrame containing only new trades (IsNewTrade == True)
        
    mode : str
        'flat' (default) → use df_flat format

    Returns:
    --------
    pd.DataFrame with frozen chart signal fields
    """
    df = df_new.copy()

    if mode != "flat":
        print("⚠️ freeze_entry_chart only supports 'flat' mode for now.")
        return df

    try:
        field_map = {
            "ChartVerdict": "ChartVerdict_Entry",
            "BreakoutConfirmed": "BreakoutConfirmed_Entry",
            "Overextended": "Overextended_Entry",
            "SqueezeActive": "Squeeze_Entry"
        }

        for src, tgt in field_map.items():
            if src in df.columns:
                df[tgt] = df[src]

    except Exception as e:
        print(f"[❌ freeze_entry_chart] Error: {e}")

    return df
