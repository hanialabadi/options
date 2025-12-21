# core/freeze_leg_status.py
import os
import pandas as pd

def evaluate_leg_status(df, legs_dir):
    """
    Evaluate structure of multi-leg trades using the latest snapshot in legs_dir.
    Returns df with Structure_Intact and LegStatus columns.
    """
    try:
        leg_files = sorted(
            [f for f in os.listdir(legs_dir) if f.startswith("legs_") and f.endswith(".csv")],
            reverse=True
        )
        if not leg_files:
            print("⚠️ No legs snapshot files found.")
            df["Structure_Intact"] = None
            df["LegStatus"] = "Unknown"
            return df

        latest_path = os.path.join(legs_dir, leg_files[0])
        latest_legs_df = pd.read_csv(latest_path)

        # Count legs by TradeID
        prev_counts = latest_legs_df["TradeID"].value_counts().to_dict()
        curr_counts = df["TradeID"].value_counts().to_dict()

        status_map = {}
        intact_map = {}

        for tid in set(prev_counts) | set(curr_counts):
            prev = prev_counts.get(tid, 0)
            curr = curr_counts.get(tid, 0)

            if curr == 2:
                status = "Active" if prev == 2 else "Reentered"
                intact = True
            elif curr == 1:
                status = "Broken" if prev == 2 else "Active"
                intact = False
            elif curr == 0:
                status = "Closed"
                intact = False
            else:
                status = "Unknown"
                intact = None

            status_map[tid] = status
            intact_map[tid] = intact

        df["LegStatus"] = df["TradeID"].map(status_map).fillna("Unknown")
        df["Structure_Intact"] = df["TradeID"].map(intact_map)

        return df

    except Exception as e:
        print(f"❌ Error in evaluate_leg_status: {e}")
        df["Structure_Intact"] = None
        df["LegStatus"] = "Unknown"
        return df
