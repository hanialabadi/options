import pandas as pd
from datetime import datetime
import os

def phase6_5_inject_derived_fields(df: pd.DataFrame, save_path: str = None) -> pd.DataFrame:
    today = pd.to_datetime(datetime.now().date())

    # === Days Held ===
    if "TradeDate" in df.columns:
        df["TradeDate"] = pd.to_datetime(df["TradeDate"], errors="coerce")
        df["Days_Held"] = (today - df["TradeDate"]).dt.days
        print(f"🧮 Days_Held injected for {df['Days_Held'].notna().sum()} rows")

    # === ROI Tracking ===
    if "% Total G/L" in df.columns:
        df["Held_ROI%"] = df["% Total G/L"]
        print(f"💹 Held_ROI% injected from % Total G/L")

    # === PCS Drift ===
    if "PCS" in df.columns and "PCS_Entry" in df.columns:
        df["PCS_Drift"] = df["PCS"] - df["PCS_Entry"]
        df["Flag_PCS_Drift"] = df["PCS_Drift"].abs() > 15
    else:
        df["PCS_Drift"] = None
        df["Flag_PCS_Drift"] = False

    # === Vega ROC ===
    if all(col in df.columns for col in ["Vega", "Vega_Entry", "Days_Held"]):
        df["Vega_ROC"] = (df["Vega"] - df["Vega_Entry"]) / df["Days_Held"].replace(0, 1)
        df["Flag_Vega_Flat"] = df["Vega_ROC"] < 0
    else:
        df["Vega_ROC"] = None
        df["Flag_Vega_Flat"] = False

    # === Outcome Tag ===
    def assign_outcome(row):
        try:
            pcs_drift = row.get("PCS_Drift", 0)
            vega_roc = row.get("Vega_ROC", 0)

            pcs_drift = 0 if pd.isna(pcs_drift) else pcs_drift
            vega_roc = 0 if pd.isna(vega_roc) else vega_roc

            if pcs_drift < -15 and vega_roc < 0:
                return "⚠️ Drift"
            elif row.get("PCS_Entry", 0) >= 75 and row.get("Held_ROI%", 0) > 60:
                return "✅ Full"
            elif row.get("PCS_Entry", 0) >= 70 and row.get("Held_ROI%", 0) < 0:
                return "❌ False"
            else:
                return "✔️ Neutral"
        except Exception:
            return "❓Error"

    df["OutcomeTag"] = df.apply(assign_outcome, axis=1)

    if save_path:
        try:
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            df.to_csv(save_path, index=False)
            print(f"✅ Saved updated DataFrame to {save_path}")
        except Exception as e:
            print(f"❌ Failed to save DataFrame to {save_path} → {e}")

    return df
