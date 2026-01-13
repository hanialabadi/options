import pandas as pd
from core.data_contracts import load_active_master, save_active_master
from core.data_contracts.config import MANAGEMENT_SAFE_MODE


def pcs_engine_v3_2_strategy_aware(df):
    df = df.copy()

    # === ROI Rate Guard ===
    days_held = pd.to_numeric(df["Days_Held"], errors="coerce").fillna(1) if "Days_Held" in df.columns else pd.Series(1, index=df.index)
    df["ROI_Rate"] = df.get("Held_ROI%", 0) / days_held

    df["Chart_Support"] = ~df.get("Exit_Flag", False) & (df.get("Chart_Trend", "Unknown") != "Broken")

    # === Strategy Tier Assignment ===
    def get_strategy_tier(strategy):
        s = str(strategy).lower()
        if "straddle" in s or "strangle" in s or "neutral" in s:
            return "Tier2_Neutral"
        elif "condor" in s or "butterfly" in s:
            return "Tier2_Neutral"
        elif "csp" in s or "income" in s or "cc" in s:
            return "Tier3_Income"
        elif "call" in s or "put" in s or "directional" in s:
            return "Tier1_Directional"
        return "Unknown"

    df["Strategy_Tier"] = df["Strategy"].apply(get_strategy_tier)

    # === Persona Score by Tier ===
    def score_persona(row):
        tier = row["Strategy_Tier"]
        if tier == "Tier1_Directional":
            return (row["Vega"] * 100) if row["Vega"] >= 0.25 else (row["Vega"] * 50)
        elif tier == "Tier2_Neutral":
            score = 0
            if row["Vega"] >= 0.25:
                score += 50
            if 0.02 <= row["Gamma"] <= 0.05:
                score += 30
            if -0.25 < row["Delta"] < 0.25:
                score += 20
            return score
        elif tier == "Tier3_Income":
            return (row["Theta"] * 150) if row["Theta"] > row["Vega"] else 30
        return 0

    df["PCS_PersonaScore"] = df.apply(score_persona, axis=1)

    def match_rank(score):
        if score >= 80: return "Strong"
        elif score >= 60: return "Moderate"
        return "Violated"

    df["Strategy_Match_Rank"] = df["PCS_PersonaScore"].apply(match_rank)
    df["Persona_Violation"] = df["Strategy_Match_Rank"] == "Violated"

    # Management Safe Mode: Disable violations
    if MANAGEMENT_SAFE_MODE:
        df["Persona_Violation"] = False
        df.loc[df["Strategy_Match_Rank"] == "Violated", "Strategy_Match_Rank"] = "Moderate"

    # === Signal and Unified Scoring ===
    df["PCS_SignalScore"] = (
        0.3 * df.get("Vega", 0) +
        0.2 * df.get("Gamma", 0) +
        0.2 * df["ROI_Rate"] +
        0.2 * df.get("Chart_CompositeScore", 0) +
        0.1 * df.get("Delta", 0) +
        0.1 * df.get("Skew_Entry", 0)
    )

    df["PCS_UnifiedScore"] = (
        0.5 * df.get("PCS", 0) +
        0.3 * df["PCS_SignalScore"] +
        0.2 * df.get("Chart_CompositeScore", 0)
    )

    df["Recovery_Bias"] = (
        (df["Vega"] > 0.2) &
        (df["PCS_Drift"] < 10) &
        (df["Chart_Support"])
    )

    # === Forgiveness logic for strong skew + PCS
    df["Forgive_Skew_Strong"] = (
        df.get("Skew_Entry", 0) > 0.3
    ) & (df["PCS_UnifiedScore"] > 70) & (df["Strategy_Tier"] == "Tier1_Directional")

    df.loc[df["Forgive_Skew_Strong"], "Persona_Violation"] = False
    df.loc[df["Forgive_Skew_Strong"], "Strategy_Match_Rank"] = "Forgiven"

    def decide(row):
        if row["PCS_UnifiedScore"] < 60 or row["PCS_Drift"] > 20:
            return "EXIT"
        if not row["Chart_Support"] or row["Persona_Violation"]:
            return "REVALIDATE"
        if row["PCS_UnifiedScore"] < 70:
            return "TRIM"
        return "HOLD"

    df["Rec_Action"] = df.apply(decide, axis=1)

    def explain(row):
        if row["Rec_Action"] == "EXIT":
            return "PCS breakdown or drift"
        if row["Rec_Action"] == "REVALIDATE":
            if row["Persona_Violation"]: return "Strategy mismatch"
            return "Chart failed or weak signal"
        if row["Rec_Action"] == "TRIM":
            return "Moderate degradation"
        return "Edge intact"

    df["Rationale_Composite"] = df.apply(explain, axis=1)
    df["Rec_Tier"] = df["Rec_Action"].map({"EXIT": 1, "REVALIDATE": 2, "TRIM": 3, "HOLD": 4})

    def health_bucket(score):
        if score < 60: return "Broken"
        elif score < 70: return "At Risk"
        elif score < 80: return "Valid"
        return "Strong"

    df["Trade_Health_Tier"] = df["PCS_UnifiedScore"].apply(health_bucket)

    if "PCS_Entry" not in df.columns or df["PCS_Entry"].isnull().any():
        df["PCS_Entry"] = df["PCS"]

    df["PCS_Live"] = df["PCS_UnifiedScore"]

    def pcs_tier(score):
        if score >= 80: return "Tier 1"
        elif score >= 70: return "Tier 2"
        elif score >= 65: return "Tier 3"
        return "Outlier"

    df["PCS_Live_Tier"] = df["PCS_Live"].apply(pcs_tier)

    df["Can_Scale"] = (
        (df["PCS_Live"] >= 80) &
        (df.get("Gamma", 0) > 0.05) &
        (df.get("Vega", 0) > 0.25) &
        (df.get("Chart_Trend", "") == "Sustained Bullish")
    )

    for col in ["PCS_SignalScore", "PCS_UnifiedScore"]:
        if col in df.columns:
            df[col] = df[col].replace([float("inf"), -float("inf")], 0)

    return df


def score_pcs_batch(master_path=None):
    """
    Load active master, run PCS v3, save back.
    master_path parameter deprecated - uses data_contracts.
    """
    print("🔁 Running PCS batch scoring...")
    df = load_active_master()
    if df.empty:
        print("⚠️ No trades to score")
        return df
    
    df = pcs_engine_v3_2_strategy_aware(df)
    save_active_master(df)
    print("✅ PCS scores updated and saved via data_contracts.")
    return df
