import pandas as pd
import os
from datetime import datetime

def evaluate_leg_status(df: pd.DataFrame, legs_dir: str = None) -> pd.DataFrame:
    """
    Compares the current df (by TradeID) to the most recent legs snapshot.
    Adds LegStatus and Structure_Intact columns.
    
    Note: legs_dir should come from data_contracts in production.
    Falls back to a default path if not provided.
    """
    print("\n🧠 Phase 6: Evaluating Leg Structure Status...")
    
    # Use data_contracts if available, otherwise fall back
    if legs_dir is None:
        try:
            from core.data_contracts import SNAPSHOT_DIR
            legs_dir = os.path.join(SNAPSHOT_DIR, "legs")
        except ImportError:
            # Fallback for legacy compatibility
            legs_dir = "data/snapshots/legs"
    
    if not os.path.exists(legs_dir):
        print(f"❌ No legs directory found at {legs_dir}")
        df["LegStatus"] = "Unknown"
        df["Structure_Intact"] = True
        return df

    # Get latest legs_*.csv file
    leg_files = sorted(
        [f for f in os.listdir(legs_dir) if f.startswith("legs_") and f.endswith(".csv")],
        reverse=True
    )
    if not leg_files:
        print("❌ No previous legs snapshots found.")
        df["LegStatus"] = "Unknown"
        df["Structure_Intact"] = True
        return df

    latest_file = os.path.join(legs_dir, leg_files[0])
    print(f"📂 Loaded previous leg snapshot: {latest_file}")
    df_prev_legs = pd.read_csv(latest_file)

    # Build leg count dictionary from prior snapshot
    prev_counts = df_prev_legs.groupby("TradeID")["Symbol"].count().to_dict()

    # Count current legs by TradeID
    current_counts = df[df["OptionType"].isin(["Call", "Put"])].groupby("TradeID")["Symbol"].count()

    # Status inference
    def infer_status(tid):
        prev = prev_counts.get(tid, 0)
        curr = current_counts.get(tid, 0)
        if prev >= 2 and curr == 2:
            return "Active"
        elif prev == 2 and curr == 1:
            return "Broken"
        elif prev == 1 and curr == 2:
            return "Reentered"
        elif curr == 0:
            return "Closed"
        elif prev == 1 and curr == 1:
            return "Partially Active"
        else:
            return "Unknown"

    df["LegStatus"] = df["TradeID"].apply(infer_status)
    df["Structure_Intact"] = df["LegStatus"].isin(["Active", "Reentered"])

    # Print summary
    print(df["LegStatus"].value_counts())

    return df
