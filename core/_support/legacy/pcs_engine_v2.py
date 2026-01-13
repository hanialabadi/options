import pandas as pd


def pcs_engine_v2_1(df):
    df = df.copy()

    # === ROI Rate Safety ===
    days_held = df.get("Days_Held", 1).replace({0: 1})  # ✅ Avoid division by zero
    df["ROI_Rate"] = df.get("Held_ROI%", 0) / days_held

    # === Weighted Signal Score (PCS_SignalScore) ===
    df["PCS_SignalScore"] = (
        0.3 * df.get("Gamma", 0) +
        0.3 * df.get("Vega", 0) +
        0.2 * df.get("Chart_CompositeScore", 0) +
        0.2 * df.get("ROI_Rate", 0)
    )

    # === Chart Trend Support ===
    df["Chart_Support"] = ~df.get("Exit_Flag", False) & (df.get("Chart_Trend", "Unknown") != "Broken")

    # === Updated Recommendation Logic ===
    def recommend(row):
        if row.get("PCS", 0) < 65 or row.get("PCS_Drift", 0) > 15:
            return "EXIT"
        if not row.get("Chart_Support", True):
            return "REVALIDATE"
        if row.get("PCS_SignalScore", 0) < 0.25:
            return "REVALIDATE"
        if row.get("Gamma", 0) < 0.02:
            return "TRIM"
        return "HOLD"

    df["Rec_Action"] = df.apply(recommend, axis=1)

    # === Explanation per recommendation
    def rationale(row):
        if row["Rec_Action"] == "EXIT":
            return "PCS < 65 or drift > 15"
        elif row["Rec_Action"] == "REVALIDATE":
            return "Weak signal or failed chart trend"
        elif row["Rec_Action"] == "TRIM":
            return "Gamma collapse"
        return "Signal intact"

    df["Rec_Rationale"] = df.apply(rationale, axis=1)

    # === Tiers
    def tier(row):
        return {"EXIT": 1, "REVALIDATE": 2, "TRIM": 3, "HOLD": 4}.get(row["Rec_Action"], 4)

    df["Rec_Tier"] = df.apply(tier, axis=1)

    # === Flag Conflicting Greeks
    df["Greek_Conflict"] = (
        ((df["Delta"] < 0.35) & (df["Gamma"] < 0.03)) |
        ((df["Theta"] > df["Vega"]) & (df["Vega"] < 0.2))
    )

    # === Repair Flag
    df["Repair_Trigger"] = (
        (df["PCS"] < 70) & (df["Vega"] < 0.25) & (df["Gamma"] < 0.03)
    )

    # ✅ Replace inf values with 0 in PCS_SignalScore
    if "PCS_SignalScore" in df.columns:
        df["PCS_SignalScore"] = df["PCS_SignalScore"].replace([float("inf"), -float("inf")], 0)

    return df
