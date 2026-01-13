# rec_tags.py

import pandas as pd

def assign_confidence(row):
    pcs = row.get("PCS", 0)
    vega = row.get("Vega", 0)
    gamma = row.get("Gamma", 0)

    if pcs >= 80 and vega >= 0.25 and gamma >= 0.05:
        return "Tier 1"
    elif pcs >= 75 and vega >= 0.20:
        return "Tier 2"
    elif pcs >= 70:
        return "Tier 3"
    else:
        return "Low"

def assign_recommendation(row):
    pcs = row.get("PCS", 0)
    pcs_drift = row.get("PCS_Drift", 0)
    vega_roc = row.get("Vega_ROC", 0)
    exit_flag = row.get("Exit_Flag", False)

    if pcs < 65 or pcs_drift < -15 or vega_roc < 0 or exit_flag:
        return "EXIT"
    elif pcs >= 80:
        return "HOLD"
    elif pcs >= 75:
        return "TRIM"
    else:
        return "REVALIDATE"

def tag_rec_and_confidence(df: pd.DataFrame) -> pd.DataFrame:
    df["Confidence"] = df.apply(assign_confidence, axis=1)
    df["Rec_Action"] = df.apply(assign_recommendation, axis=1)
    df["Rec_Tier"] = df["Confidence"].map({
        "Tier 1": 1, "Tier 2": 2, "Tier 3": 3, "Low": 4
    }).fillna(4).astype(int)
    return df
