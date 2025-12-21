# core/rec_engine_v6/rec_outcome_tags.py

import pandas as pd

def compute_outcome_tag(row):
    pcs_drift = row.get("PCS_Drift", 0) or 0
    vega_roc = row.get("Vega_ROC", 0) or 0
    roi = row.get("Held_ROI%", 0) or 0
    pcs_entry = row.get("PCS_Entry", 0) or 0

    if pcs_drift < -15 and vega_roc < 0:
        return "⚠️ Drift"
    elif pcs_entry >= 75 and roi > 60:
        return "✅ Full"
    elif pcs_entry >= 70 and roi < 0:
        return "❌ False"
    else:
        return "✔️ Neutral"

def assign_outcome_tags(df: pd.DataFrame) -> pd.DataFrame:
    df["OutcomeTag"] = df.apply(compute_outcome_tag, axis=1)
    return df
