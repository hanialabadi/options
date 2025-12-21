# core/phase6_freeze/freeze_all_entry_fields.py

from core.phase6_freeze.freezer_modules.freeze_entry_greeks import freeze_entry_greeks
from core.phase6_freeze.freezer_modules.freeze_entry_ivhv import freeze_entry_ivhv
from core.phase6_freeze.freezer_modules.freeze_entry_premium import freeze_entry_premium
from core.phase6_freeze.freezer_modules.freeze_entry_metadata import freeze_entry_metadata
from core.phase6_freeze.freezer_modules.freeze_entry_chart import freeze_entry_chart
# from core.phase6_freeze.freezer_modules.freeze_entry_leg_meta import freeze_entry_leg_meta  ← optional

def freeze_all_entry_fields(df_raw, mode="flat"):
    """
    Main controller to freeze all entry-related fields.
    
    Parameters:
    -----------
    df_raw : pd.DataFrame
        Full snapshot DataFrame (df_flat or legs_df)

    mode : str
        'flat' or 'legs' — determines which freezer modules to apply

    Returns:
    --------
    pd.DataFrame with frozen entry fields
    """

    df = df_raw.copy()

    if "IsNewTrade" not in df.columns:
        print("[⚠️ freeze_all_entry_fields] No 'IsNewTrade' column found. Skipping freeze.")
        return df

    df_new = df[df["IsNewTrade"] == True].copy()

    if df_new.empty:
        print("✅ No new trades to freeze.")
        return df

    print(f"🧊 Freezing {len(df_new)} new trade(s) in mode: {mode}")

    # Apply freezer modules (flat or legs)
    if mode == "flat":
        df_new = freeze_entry_greeks(df_new)
        df_new = freeze_entry_ivhv(df_new)
        df_new = freeze_entry_premium(df_new, mode="flat")
        df_new = freeze_entry_metadata(df_new, mode="flat")
        df_new = freeze_entry_chart(df_new)

    elif mode == "legs":
        df_new = freeze_entry_premium(df_new, mode="legs")
        df_new = freeze_entry_metadata(df_new, mode="legs")
        # df_new = freeze_entry_leg_meta(df_new)  # Optional leg-level metadata
        df_new = freeze_entry_ivhv(df_new, mode="legs")

    else:
        raise ValueError(f"[❌ freeze_all_entry_fields] Invalid mode: {mode}")

    # Merge frozen columns back into full DataFrame
    frozen_cols = [col for col in df_new.columns if col.endswith("_Entry")]
    df.update(df_new[["TradeID"] + frozen_cols].set_index("TradeID"))

    print(f"✅ Freeze complete. Fields frozen: {frozen_cols}")
    return df
