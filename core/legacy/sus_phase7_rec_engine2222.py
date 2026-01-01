import os
import pandas as pd
import numpy as np
from datetime import datetime

def phase6_freeze_and_archive(
    df: pd.DataFrame,
    df_master: pd.DataFrame,
    master_path: str = "/Users/haniabadi/Documents/Windows/Optionrec/active_master.csv",
    closed_path: str = "/Users/haniabadi/Documents/Windows/Optionrec/closed_log.csv",
    debug: bool = False
) -> pd.DataFrame:
    print("⏳ Starting Phase 6: Freeze, Drift, Archive...")

    now_iso = datetime.now().isoformat()
    today = pd.to_datetime(datetime.now().date())

    if "Skew_Entry" not in df.columns and "IV Mid" in df.columns:
        from core.phase3_pcs_score import calculate_skew_and_kurtosis
        df = calculate_skew_and_kurtosis(df)
        print("📌 Skew and Kurtosis recomputed on-the-fly.")

    REQUIRED_COLS = ['TradeID', 'OptionType', 'Strike', 'Expiration', 'DTE', 'Delta', 'Gamma']
    missing_cols = [col for col in REQUIRED_COLS if col not in df.columns]
    if missing_cols:
        raise ValueError(f"❌ Missing required columns: {missing_cols}")

    if df_master is None or not isinstance(df_master, pd.DataFrame) or df_master.empty:
        raise ValueError("❌ df_master must be explicitly passed to phase6 — auto-load not allowed.")

    df["TradeID"] = df["TradeID"].astype(str).str.strip()
    df_master["TradeID"] = df_master.get("TradeID", pd.Series(dtype=str)).astype(str).str.strip()

    known_ids = set(df_master["TradeID"])
    df_new = df[~df["TradeID"].isin(known_ids)].copy()
    df_existing = df[df["TradeID"].isin(known_ids)].copy()
    closed_trades = df_master[~df_master["TradeID"].isin(df["TradeID"])]

    print(f"🆕 New trades: {len(df_new)}")
    print(f"📤 Closed trades: {len(closed_trades)}")

    freeze_map = [
        ("Delta", "Delta_Entry"), ("Gamma", "Gamma_Entry"),
        ("Vega", "Vega_Entry"), ("Theta", "Theta_Entry"),
        ("IV Mid", "IV_Entry"), 
        ("Skew_Entry", "Skew_Entry"), 
        ("Kurtosis_Entry", "Kurtosis_Entry"), 
        ("IVHV_Gap_Entry", "IVHV_Gap_Entry"), 
        ("PCS", "PCS_Entry"), 
        ("Confidence Tier", "Confidence_Entry"),
        ("Basis", "Premium_Entry"), 
        ("Basis", "CostBasis_Entry"),
        ("Last", "Entry_Price")
    ]
    frozen_cols = [dst for _, dst in freeze_map] + ["Entry_Timestamp"]

    for col in frozen_cols:
        if col not in df_master.columns:
            df_master[col] = np.nan if "Timestamp" not in col else ""
    if "TradeDate" not in df_master.columns:
        df_master["TradeDate"] = pd.NaT

    for src, dst in freeze_map:
        df_new[dst] = df_new.get(src, np.nan)
    df_new["Entry_Timestamp"] = now_iso
    df_new["TradeDate"] = today

    merge_cols = ["TradeID"] + frozen_cols
    if "TradeDate" not in df_existing.columns:
        df_existing["TradeDate"] = pd.NaT

    df_existing = df_existing.merge(
        df_master[merge_cols + ["TradeDate"]], on="TradeID", how="left", suffixes=("", "_master")
    )
    for col in frozen_cols:
        col_m = f"{col}_master"
        if col_m in df_existing.columns:
            df_existing[col] = df_existing[col_m]
            df_existing.drop(columns=[col_m], inplace=True)

    if "TradeDate_master" in df_existing.columns:
        df_existing["TradeDate"] = df_existing["TradeDate_master"].combine_first(df_existing["TradeDate"])
        df_existing.drop(columns=["TradeDate_master"], inplace=True)

    df_master_updated = pd.concat([df_existing, df_new], ignore_index=True)
    df_master_updated.drop_duplicates(subset=["TradeID"], keep="first", inplace=True)

    for src, dst in freeze_map:
        if src in df_master_updated.columns and dst in df_master_updated.columns:
            df_master_updated[f"{src}_Drift"] = df_master_updated[src] - df_master_updated[dst]
    if "PCS" in df_master_updated.columns and "PCS_Entry" in df_master_updated.columns:
        df_master_updated["PCS_Drift"] = df_master_updated["PCS"] - df_master_updated["PCS_Entry"]

    if "SignalTag" not in df_master_updated.columns:
        df_master_updated["SignalTag"] = "Seeded_Active"
    if "OutcomeTag" not in df_master_updated.columns:
        df_master_updated["OutcomeTag"] = ""

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

    if "TradeDate" in df_master_updated.columns:
        df_master_updated["TradeDate"] = pd.to_datetime(df_master_updated["TradeDate"], errors="coerce")
        df_master_updated["Days_Held"] = (today - df_master_updated["TradeDate"]).dt.days
        print("🧮 Days_Held injected:", df_master_updated["Days_Held"].notna().sum())

    if "% Total G/L" in df_master_updated.columns:
        df_master_updated["Held_ROI%"] = df_master_updated["% Total G/L"]

    missing_derived = [col for col in ["Days_Held", "Held_ROI%"] if col not in df_master_updated.columns]
    if missing_derived:
        print(f"❌ Missing derived columns: {missing_derived}. Aborting save.")
        return df_master_updated[df_master_updated["TradeID"].isin(df["TradeID"])]

    try:
        os.makedirs(os.path.dirname(master_path), exist_ok=True)
        df_master_updated.to_csv(master_path, index=False)
        print(f"✅ Master file saved to: {master_path}")
    except Exception as e:
        print(f"❌ Error saving master file: {e}")

    print("\n🧾 Phase 6 Summary:")
    print(f"Total trades in master: {len(df_master_updated)}")
    print("🧊 Frozen column null counts:")
    print(df_master_updated[[*frozen_cols, "TradeDate"]].isnull().sum())
    print("✅ Phase 6 freeze and archive complete.\n")

    return df_master_updated[df_master_updated["TradeID"].isin(df["TradeID"])]
