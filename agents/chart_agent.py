
import pandas as pd

def pcs_engine_v3_unified(df):
    df = df.copy()

    # === Safety Defaults ===
    df["ROI_Rate"] = df.get("Held_ROI%", 0) / df.get("Days_Held", 1)
    df["Chart_Support"] = ~df.get("Exit_Flag", False) & (df.get("Chart_Trend", "Unknown") != "Broken")

    # === 📊 Composite Signal Score ===
    df["PCS_SignalScore"] = (
        0.3 * df.get("Vega", 0) +
        0.2 * df.get("Gamma", 0) +
        0.2 * df.get("ROI_Rate", 0) +
        0.2 * df.get("Chart_CompositeScore", 0) +
        0.1 * df.get("Delta", 0)
    )

    # === 🎯 Unified Health Score (PCS_UnifiedScore) ===
    df["PCS_UnifiedScore"] = (
        0.5 * df.get("PCS", 0) +
        0.3 * df.get("PCS_SignalScore", 0) +
        0.2 * df.get("Chart_CompositeScore", 0)
    )

    # === 🚦 Persona Violation Flag
    df["Persona_Violation"] = (
        ((df.get("Strategy", "").str.lower().str.contains("call|directional")) & (df["Vega"] < 0.2)) |
        ((df.get("Strategy", "").str.lower().str.contains("csp|income")) & (df["Theta"] < df["Vega"]))
    )

    # === 🔄 Recovery Bias Tag
    df["Recovery_Bias"] = (
        (df["Vega"] > 0.2) &
        (df["PCS_Drift"] < 10) &
        (df["Chart_Support"])
    )

    # === 📌 Recommendation Logic ===
    def decide(row):
        if row["PCS_UnifiedScore"] < 60 or row["PCS_Drift"] > 20:
            return "EXIT"
        if not row["Chart_Support"]:
            return "REVALIDATE"
        if row["Persona_Violation"]:
            return "REVALIDATE"
        if row["PCS_UnifiedScore"] < 70:
            return "TRIM"
        return "HOLD"

    df["Rec_Action"] = df.apply(decide, axis=1)

    # === 📣 Composite Rationale
    def explain(row):
        if row["Rec_Action"] == "EXIT":
            return "PCS breakdown or heavy drift"
        if row["Rec_Action"] == "REVALIDATE":
            if row["Persona_Violation"]:
                return "Strategy mismatch or chart failure"
            return "Signal drift or uncertain trend"
        if row["Rec_Action"] == "TRIM":
            return "Signal weakening below Tier 1"
        return "Edge intact"

    df["Rationale_Composite"] = df.apply(explain, axis=1)

    # === Tiers
    tier_map = {"EXIT": 1, "REVALIDATE": 2, "TRIM": 3, "HOLD": 4}
    df["Rec_Tier"] = df["Rec_Action"].map(tier_map)

    # === Health Buckets
    def health_tier(score):
        if score < 60: return "Broken"
        elif score < 70: return "At Risk"
        elif score < 80: return "Valid"
        return "Strong"

    df["Trade_Health_Tier"] = df["PCS_UnifiedScore"].apply(health_tier)

    return df
