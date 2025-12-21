# core/rec_engine_v6/rec_infer_rules.py

import pandas as pd

def infer_recommendation_rules(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rule-based inference layer for Rec_Tier and Rec_Action based on PCS, Vega, Gamma, ROI, and structure.
    """

    def decide_action(row):
        pcs = row.get("PCS", 0)
        drift = row.get("PCS_Drift", 0) or 0
        vega = row.get("Vega", 0) or 0
        gamma = row.get("Gamma", 0) or 0
        roi = row.get("Held_ROI%", 0) or 0
        leg_status = row.get("LegStatus", "Unknown")

        if pcs < 65 or drift < -15:
            return "EXIT"
        elif vega < 0.15 and gamma < 0.03:
            return "TRIM"
        elif leg_status in ["Broken", "Partially Active"]:
            return "REVALIDATE"
        elif pcs >= 75 and roi > 40:
            return "SCALE"
        else:
            return "HOLD"

    def assign_tier(pcs):
        if pcs >= 80:
            return 1
        elif pcs >= 70:
            return 2
        else:
            return 3

    df["Rec_Action"] = df.apply(decide_action, axis=1)
    df["Rec_Tier"] = df["PCS"].apply(assign_tier)
    df["Confidence"] = df["Rec_Tier"].map({1: "High", 2: "Medium", 3: "Low"})
    
    return df
