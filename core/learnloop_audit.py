# learnloop_audit.py
# ✅ LearnLoop: Classify Trade Outcomes, Detect PCS Mismatches, Update Logs

import pandas as pd
import numpy as np
import os
from datetime import datetime

# === Load Closed Trade Log ===
def load_closed_trades(path="/Users/haniabadi/Documents/Windows/Optionrec/closed_log.csv"):
    if not os.path.exists(path):
        print("❌ No closed_log.csv found — skipping LearnLoop.")
        return pd.DataFrame()
    df = pd.read_csv(path)
    df = df[df["Held_ROI%"].notna() & df["PCS_Entry"].notna()]
    return df

# === Tag Outcome Labels ===
def tag_outcome(df):
    df["OutcomeTag"] = df["Held_ROI%"].apply(
        lambda x: "✅ Full" if x >= 50 else "❌ False" if x < 0 else "⚠️ Premature"
    )
    df["Expected_Win"] = df["PCS_Entry"] >= 75
    df["Actual_Win"] = df["Held_ROI%"] >= 50

    df["LearnLoop_Tag"] = np.select(
        [
            (df["Expected_Win"] == True) & (df["Actual_Win"] == False),
            (df["Expected_Win"] == False) & (df["Actual_Win"] == True)
        ],
        ["❌ False GEM", "❓ Missed Win"],
        default="✅ Correct"
    )
    return df

# === Append to Learn Log ===
def update_learn_log(df, out_path="/Users/haniabadi/Documents/Windows/Optionrec/learn_log.csv"):
    df["LearnLog_Timestamp"] = datetime.now().isoformat()
    cols = [
        "TradeID", "Underlying", "Strategy", "PCS_Entry", "Held_ROI%", "OutcomeTag",
        "Expected_Win", "Actual_Win", "LearnLoop_Tag", "LearnLog_Timestamp"
    ]
    df_out = df[cols].copy()
    df_out.to_csv(out_path, mode="a", index=False, header=not os.path.exists(out_path))
    print(f"✅ LearnLoop log updated: {len(df_out)} trades written to {out_path}")

if __name__ == "__main__":
    df_closed = load_closed_trades()
    if not df_closed.empty:
        df_tagged = tag_outcome(df_closed)
        update_learn_log(df_tagged)
