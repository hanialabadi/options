
import os
import pandas as pd
import numpy as np
from datetime import datetime
from core.data_contracts import get_active_trade_ids, load_snapshot_timeseries


def load_drift_timeseries(drift_dir: str) -> pd.DataFrame:
    # Use data contract instead of hardcoded path
    active_ids = get_active_trade_ids()

    files = [f for f in os.listdir(drift_dir) if f.startswith("positions_") and f.endswith(".csv")]
    data = []

    for file in sorted(files):
        try:
            timestamp_str = file.replace("positions_", "").replace(".csv", "")
            ts = datetime.strptime(timestamp_str, "%Y-%m-%d_%H-%M-%S")
            df = pd.read_csv(os.path.join(drift_dir, file))
            df = df[df["TradeID"].isin(active_ids)]
            df["Snapshot_TS"] = ts
            data.append(df)
        except Exception as e:
            print(f"⚠️ Skipping {file}: {e}")

    df_all = pd.concat(data, ignore_index=True)
    return df_all


def build_drift_timeseries(df_all: pd.DataFrame) -> pd.DataFrame:
    required_cols = ["TradeID", "Snapshot_TS", "Delta", "Gamma", "Vega", "Theta", "PCS", "IVHV_Gap"]
    missing = [col for col in required_cols if col not in df_all.columns]
    if missing:
        raise ValueError(f"❌ Missing required columns: {missing}")

    df_long = df_all[required_cols].copy()
    df_long = df_long.sort_values(["TradeID", "Snapshot_TS"]).reset_index(drop=True)
    return df_long


def calculate_drift_metrics(df_long: pd.DataFrame, window: int = 3) -> pd.DataFrame:
    def apply_rolling(group):
        group = group.sort_values("Snapshot_TS")

        # === 1D, 3D, 5D Change
        for greek in ["Delta", "Gamma", "Vega", "Theta"]:
            group[f"{greek}_1D"] = group[greek] - group[greek].shift(1)
            group[f"{greek}_3D"] = group[greek] - group[greek].shift(3)
            group[f"{greek}_5D"] = group[greek] - group[greek].shift(5)

        # === ROC
        for greek in ["Delta", "Gamma", "Vega", "Theta", "PCS"]:
            group[f"{greek}_ROC"] = group[greek].diff()

        # === SMA + STD
        group["PCS_SMA"] = group["PCS"].rolling(window).mean()
        group["PCS_STD"] = group["PCS"].rolling(window).std()
        group["Vega_SMA"] = group["Vega"].rolling(window).mean()
        group["Vega_STD"] = group["Vega"].rolling(window).std()

        # === IVHV Collapse
        group["IVHV_Drop"] = group["IVHV_Gap"].diff().fillna(0)

        # === HH Detection
        group["HH_Count_Last_2"] = (group["PCS"] > group["PCS"].rolling(2).max().shift(1)).astype(int).rolling(2).sum()
        group["HH_Count_Last_3"] = (group["PCS"] > group["PCS"].rolling(3).max().shift(1)).astype(int).rolling(3).sum()

        return group

    df_drift = df_long.groupby("TradeID", group_keys=False).apply(apply_rolling)
    return df_drift


def flag_drift_signals(df: pd.DataFrame) -> pd.DataFrame:
    if "PCS_ROC" in df.columns:
        df["Flag_PCS_Drift"] = df["PCS_ROC"] < -15
    else:
        df["Flag_PCS_Drift"] = False
    if "Vega_ROC" in df.columns:
        df["Flag_Vega_Flat"] = df["Vega_ROC"] < 0
    else:
        df["Flag_Vega_Flat"] = False
    if "Gamma_ROC" in df.columns:
        df["Flag_Gamma_Risk"] = df["Gamma_ROC"] < -0.05
    else:
        df["Flag_Gamma_Risk"] = False
    if "IVHV_Drop" in df.columns:
        df["Flag_IVHV_Collapse"] = df["IVHV_Drop"] > 3
    else:
        df["Flag_IVHV_Collapse"] = False
        print("⚠️ 'IVHV_Drop' not found — Flag_IVHV_Collapse set to False.")
    # Flag if no higher highs in last 2 days
    if "HH_Count_Last_2" in df.columns:
        df["Flag_No_HH"] = df["HH_Count_Last_2"] == 0
    else:
        df["Flag_No_HH"] = False
        print("⚠️ 'HH_Count_Last_2' missing — Flag_No_HH set to False.")

    # Flag if no higher highs in last 3 days
    if "HH_Count_Last_3" in df.columns:
        df["Flag_No_HH_3D"] = df["HH_Count_Last_3"] == 0
    else:
        df["Flag_No_HH_3D"] = False
        print("⚠️ 'HH_Count_Last_3' missing — Flag_No_HH_3D set to False.")

    # Flag if Delta is under 0.35
    if "Delta" in df.columns:
        df["Flag_Delta_Under35"] = df["Delta"] < 0.35
    else:
        df["Flag_Delta_Under35"] = False
        print("⚠️ 'Delta' missing — Flag_Delta_Under35 set to False.")


    if "Vega_Entry" in df.columns:
        df["Flag_Vega_Below_Entry"] = df["Vega"] < df["Vega_Entry"]

    return df


def update_master_with_drift_tail(df_drift: pd.DataFrame, master_path: str = "/Users/haniabadi/Documents/Windows/Optionrec/active_master.csv"):
    df_tail = df_drift.sort_values("Snapshot_TS").groupby("TradeID").tail(1)
    df_master = pd.read_csv(master_path)

    if "PCS_Entry" in df_master.columns:
        df_tail = df_tail.drop(columns=["PCS_Entry"], errors="ignore")  # avoid merge overwrite
        pcs_entry_map = df_master[["TradeID", "PCS_Entry"]].dropna()
        df_tail = df_tail.merge(pcs_entry_map, on="TradeID", how="left")
        df_tail["PCS_Drift"] = df_tail["PCS"] - df_tail["PCS_Entry"]


    drift_fields = [col for col in df_tail.columns if any(x in col for x in ["Drift", "ROC", "SMA", "STD", "1D", "3D", "5D"])]
    # ✅ Ensure critical raw drift fields are included
    required_drift_fields = ["Delta_1D", "Gamma_3D", "Vega_5D"]
    for field in required_drift_fields:
        if field in df_tail.columns and f"{field}_DriftTail" not in drift_fields:
            drift_fields.append(field)

    df_tail.rename(columns={field: f"{field}_DriftTail" for field in drift_fields}, inplace=True)

    df_master = df_master.merge(
        df_tail[["TradeID"] + [f"{f}_DriftTail" for f in drift_fields] + (["PCS_Drift"] if "PCS_Drift" in df_tail else [])],
        on="TradeID", how="left"
    )

    df_master.to_csv(master_path, index=False)
    print(f"✅ active_master.csv updated with full drift tail snapshot.")


def run_phase7_drift_engine(drift_dir: str, export_csv: str = None, update_master: bool = True) -> pd.DataFrame:
    df_all = load_drift_timeseries(drift_dir)
    df_long = build_drift_timeseries(df_all)
    df_drift = calculate_drift_metrics(df_long)
    df_flagged = flag_drift_signals(df_drift)

    if export_csv:
        df_flagged.to_csv(export_csv, index=False)
        print(f"✅ Drift audit saved to {export_csv}")

    if update_master:
        update_master_with_drift_tail(df_flagged)

    return df_flagged
