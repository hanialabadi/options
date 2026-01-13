import pandas as pd
from datetime import datetime
import os

def log_recommendations(df: pd.DataFrame, save_dir: str = "audit_logs") -> str:
    """
    Saves key recommendation and signal fields to a timestamped CSV log.
    Ensures all expected columns exist or fills with default values.
    """
    os.makedirs(save_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"rec_log_{ts}.csv"
    path = os.path.join(save_dir, filename)

    log_cols = [
        "TradeID", "Underlying", "Strategy", "PCS", "PCS_Entry", "PCS_Drift",
        "Gamma", "Vega", "Vega_ROC", "Exit_Flag",
        "Rec_Action", "Rec_Tier", "Confidence", "OutcomeTag"
    ]

    # Add fallback columns if missing
    for col in log_cols:
        if col not in df.columns:
            df[col] = None

    try:
        df[log_cols].to_csv(path, index=False)
        print(f"📋 Rec audit log saved to: {path}")
    except Exception as e:
        print(f"❌ Failed to save rec audit log → {e}")
        return ""

    return path
