import pandas as pd

def assign_confidence(row):
    pcs = row.get("PCS", 0)
    vega = row.get("Vega", 0)

    if pcs >= 80 and vega >= 0.3:
        return "Tier 1"
    elif pcs >= 75:
        return "Tier 2"
    elif pcs >= 70:
        return "Tier 3"
    else:
        return "Low"

def assign_recommendation(row):
    pcs = row.get("PCS", 0)
    vega = row.get("Vega", 0)
    action = "HOLD"

    if pcs >= 80 and vega > 0.25:
        action = "ENTER"
    elif pcs < 65 or vega < 0.10:
        action = "EXIT"

    return action

def patch_overlay_fields(df: pd.DataFrame) -> pd.DataFrame:
    df["Confidence"] = df.apply(assign_confidence, axis=1)

    df["Rec_Tier"] = df["Confidence"].map({
        "Tier 1": 1,
        "Tier 2": 2,
        "Tier 3": 3,
        "Low": 4
    })

    df["Rec_Action"] = df.apply(assign_recommendation, axis=1)
    df["Rec_V6"] = df["Rec_Action"] + " (" + df["Confidence"] + ")"

    return df

def final_patch_overlay(df: pd.DataFrame) -> pd.DataFrame:
    return patch_overlay_fields(df)
