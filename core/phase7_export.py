# %% 📚 Imports
import pandas as pd
import numpy as np
import os
from datetime import datetime

# %% 🧊 Phase 7: Freeze + Archive
def patched_phase7_freeze_and_archive(
    df: pd.DataFrame,
    df_master: pd.DataFrame = None,
    master_path: str = "/Users/haniabadi/Documents/Windows/Optionrec/active_master.csv",
    closed_path: str = "/Users/haniabadi/Documents/Windows/Optionrec/closed_log.csv",
    debug: bool = False
) -> pd.DataFrame:
    print("⏳ Starting Phase 7: Freeze, Drift, Archive...")

    now_iso = datetime.now().isoformat()
    today = pd.to_datetime(datetime.now().date())

    # === 🧱 Required columns
    REQUIRED_COLS = ['TradeID', 'OptionType', 'Strike', 'Expiration', 'DTE', 'Delta', 'Gamma']
    missing_cols = [col for col in REQUIRED_COLS if col not in df.columns]
    if missing_cols:
        raise ValueError(f"❌ Missing required columns: {missing_cols}")

    if df_master is None or not isinstance(df_master, pd.DataFrame) or df_master.empty:
        print("📂 No existing master detected. Initializing empty one.")
        df_master = pd.DataFrame()

    df["TradeID"] = df["TradeID"].astype(str).str.strip()
    df_master["TradeID"] = df_master.get("TradeID", pd.Series(dtype=str)).astype(str).str.strip()

    # === 🔍 Identify New + Closed
    known_ids = set(df_master["TradeID"])
    df_new = df[~df["TradeID"].isin(known_ids)].copy()
    df_existing = df[df["TradeID"].isin(known_ids)].copy()
    closed_trades = df_master[~df_master["TradeID"].isin(df["TradeID"])]

    print(f"🆕 New trades: {len(df_new)}")
    print(f"📤 Closed trades: {len(closed_trades)}")

    # === 🧊 Freeze Map
    freeze_map = [
        ("Delta", "Delta_Entry"), ("Gamma", "Gamma_Entry"), ("Vega", "Vega_Entry"), ("Theta", "Theta_Entry"),
        ("IV_Now", "IV_Entry"), ("Skew", "Skew_Entry"), ("Kurtosis", "Kurtosis_Entry"), ("IVHV_Gap", "IVHV_Gap_Entry"),
        ("PCS", "PCS_Entry"), ("Confidence", "Confidence_Entry"),
        ("Basis", "Premium_Entry"), ("Basis", "CostBasis_Entry"),
        ("Last", "Entry_Price")
    ]
    frozen_cols = [dst for _, dst in freeze_map] + ["Entry_Timestamp", "TradeDate"]

    for col in frozen_cols:
        if col not in df_master.columns:
            df_master[col] = np.nan if "Timestamp" not in col else ""

    # === ⛓️ Freeze New Trades
    for src, dst in freeze_map:
        df_new[dst] = df_new.get(src, np.nan)
    df_new["Entry_Timestamp"] = now_iso
    df_new["TradeDate"] = today

    # === 🧩 Merge Existing Trades
    merge_cols = ["TradeID"] + frozen_cols
    for col in merge_cols:
        if col not in df_existing.columns:
            df_existing[col] = np.nan

    df_existing = df_existing.merge(
        df_master[merge_cols], on="TradeID", how="left", suffixes=("", "_master")
    )

    for col in frozen_cols:
        col_m = f"{col}_master"
        if col_m in df_existing.columns:
         df_existing[col] = df_existing[col_m]  # ✅ Always use frozen value
        df_existing.drop(columns=[col_m], inplace=True)


    # === 🧬 Recombine
    df_master_updated = pd.concat([df_existing, df_new], ignore_index=True)
    df_master_updated.drop_duplicates(subset=["TradeID"], keep="first", inplace=True)

    # === 📉 Drift Calculations
    for src, dst in freeze_map:
        if src in df_master_updated.columns and dst in df_master_updated.columns:
            df_master_updated[f"{src}_Drift"] = df_master_updated[src] - df_master_updated[dst]
    if "PCS" in df_master_updated.columns and "PCS_Entry" in df_master_updated.columns:
        df_master_updated["PCS_Drift"] = df_master_updated["PCS"] - df_master_updated["PCS_Entry"]

    # === 📌 Default Tags
    if "SignalTag" not in df_master_updated.columns:
        df_master_updated["SignalTag"] = "Seeded_Active"
    if "OutcomeTag" not in df_master_updated.columns:
        df_master_updated["OutcomeTag"] = ""

    # === 📥 Archive Closed Trades
    if not closed_trades.empty:
        closed_trades["Status"] = "Closed"
        closed_trades["Closed_Timestamp"] = now_iso
        try:
            os.makedirs(os.path.dirname(closed_path), exist_ok=True)
            if os.path.exists(closed_path):
                old = pd.read_csv(closed_path)
                combined = pd.concat([old, closed_trades], ignore_index=True).drop_duplicates()
                combined.to_csv(closed_path, index=False)
            else:
                closed_trades.to_csv(closed_path, index=False)
            print(f"🗂️ Archived {len(closed_trades)} trades.")
        except Exception as e:
            print(f"⚠️ Error archiving trades: {e}")

    # === 💾 Save Updated Master
    try:
        os.makedirs(os.path.dirname(master_path), exist_ok=True)
        df_master_updated.to_csv(master_path, index=False)
        print(f"✅ Master file saved to: {master_path}")
    except Exception as e:
        print(f"❌ Error saving master file: {e}")

    # === 🐞 Debug Output
    if debug:
        df_new.to_csv("debug_new_trades.csv", index=False)
        df_master_updated.to_csv("debug_master_postmerge.csv", index=False)
        print("🐞 Debug files written.")

    # === 🧾 Summary
    entry_cols = [dst for _, dst in freeze_map] + ["TradeDate", "Entry_Timestamp"]
    print("\n🧾 Phase 7 Summary:")
    print(f"Total trades in master: {len(df_master_updated)}")
    print("🧊 Frozen column null counts:")
    print(df_master_updated[entry_cols].isnull().sum())
    print("\n🔍 df.head() after Phase 7:")
    print(df_master_updated.head())
    print("✅ Phase 7 freeze and archive complete.\n")

    return df_master_updated[df_master_updated["TradeID"].isin(df["TradeID"])]
