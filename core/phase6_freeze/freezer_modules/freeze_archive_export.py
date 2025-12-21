import pandas as pd
import os
from datetime import datetime

def save_master(df: pd.DataFrame, path: str = "/Users/haniabadi/Documents/Windows/Optionrec/active_master.csv") -> str:
    """
    Final export of updated master DataFrame after Phase 6 merge and leg evaluation.
    """
    print("\n📦 Phase 6.3: Saving updated master...")

    # Ensure directory exists
    os.makedirs(os.path.dirname(path), exist_ok=True)

    # Inject Days_Held if TradeDate present
    if "TradeDate" in df.columns:
        df["Days_Held"] = (pd.Timestamp.today() - pd.to_datetime(df["TradeDate"])).dt.days
        print("🧮 Days_Held injected for", df.shape[0], "rows")

    # Inject ROI if possible
    if "% Total G/L" in df.columns:
        df["Held_ROI%"] = df["% Total G/L"]
        print("💹 Held_ROI% injected from % Total G/L")

    # Save to active_master.csv
    df.to_csv(path, index=False)
    print(f"✅ Saved updated DataFrame to {path}")

    return path
