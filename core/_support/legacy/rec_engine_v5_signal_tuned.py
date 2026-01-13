import pandas as pd
import numpy as np


def run_rec_engine_v5(df):
    df = df.copy()

    # === Normalize core fields ===
    for col in ["Vega", "Gamma", "Delta", "PCS", "Held_ROI%", "Days_Held", "Chart_CompositeScore",
                "Gamma_ROC", "PCS_ROC", "IVHV_Drop", "HH_Count_Last_3", "DTE", "OVI",
                "OBV", "Chart_MTF_Aligned", "PCS_SMA_3"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        else:
            df[col] = 0

    df["ROI_Rate"] = df["Held_ROI%"] / df["Days_Held"].replace(0, 1)
    df["PCS_Drift"] = df.get("PCS_Drift", 0).fillna(0)
    df["PCS_Momentum"] = df["PCS"] - df.get("PCS_SMA_3", 0)

    # === Parse Chart_Tags ===
    def tag_score(tags):
        if isinstance(tags, str):
            tags = tags.replace("[", "").replace("]", "").replace("'", "").split(", ")
        score = 0
        for tag in tags:
            if "Doji" in tag: score -= 7
            if "ShootingStar" in tag: score -= 10
            if "Hammer" in tag: score += 5
            if "Bullish" in tag: score += 7
            if "Bearish" in tag: score -= 5
        return score

    df["Pattern_Score"] = df["Chart_Tags"].apply(tag_score)

    # === Composite Momentum Score ===
    df["MACD_Cross"] = df.get("MACD_Line", 0) > df.get("MACD_Signal", 0)
    df["Momentum_Score"] = (
        df["MACD_Cross"].astype(int) * 10 +
        df.get("RSI", 50).apply(lambda x: 10 if 45 <= x <= 65 else -5) +
        df.get("ADX", 0).apply(lambda x: 5 if x > 20 else 0) +
        df.get("MFI", 50).apply(lambda x: 5 if 40 <= x <= 70 else -5) +
        df.get("CCI", 0).apply(lambda x: 5 if -100 < x < 100 else -5)
    )

    # === Volume & Volatility Risk Tags ===
    df["Volume_Divergence"] = df.get("Volume_Divergence", False).astype(bool)
    df["ATR_Spike"] = df.get("ATR_Spike", False).astype(bool)

    df["Risk_Penalty"] = 0
    df.loc[df["Volume_Divergence"], "Risk_Penalty"] -= 5
    df.loc[df["ATR_Spike"], "Risk_Penalty"] -= 5
    df.loc[df["IVHV_Drop"] > 5, "Risk_Penalty"] -= 5

    # === Strategy Tier + Event Context Awareness ===
    def strat_tier(strategy):
        s = str(strategy).lower()
        if any(x in s for x in ["straddle", "strangle", "neutral"]): return "Tier2_Neutral"
        if any(x in s for x in ["csp", "income", "cc"]): return "Tier3_Income"
        if any(x in s for x in ["call", "put", "directional"]): return "Tier1_Directional"
        return "Unknown"

    df["Strategy_Tier"] = df["Strategy"].apply(strat_tier)
    df["Earnings_Days"] = pd.to_numeric(df.get("Days_to_Earnings", 99), errors="coerce").fillna(99)
    df["Event_Context"] = (df["Strategy_Tier"] == "Tier2_Neutral") & (df["Earnings_Days"] <= 5)

    # === Drift & Pressure Scoring Adjustments ===
    df["Decay_Penalty"] = 0
    df.loc[df["Gamma_ROC"] < -0.05, "Decay_Penalty"] -= 5
    df.loc[df["PCS_ROC"] < -10, "Decay_Penalty"] -= 5
    df.loc[df["HH_Count_Last_3"] == 0, "Decay_Penalty"] -= 5
    df.loc[df["DTE"] < 5, "Decay_Penalty"] -= 5

    # === Human-style override rules (mimic pro trader logic) ===
    df["HH_Failure"] = (df["HH_Count_Last_3"] == 0) & (df["PCS_Drift"] > 10)
    df["Gamma_Collapse"] = df["Gamma_ROC"] < -0.08
    df["Time_Pressure"] = df["DTE"] < 3
    df["Human_Concern"] = df["HH_Failure"] | df["Gamma_Collapse"] | df["Time_Pressure"]

    # === Composite Sentiment Enhancements ===
    df["Skew_Failure"] = (df.get("Skew_Entry", 0) < 0) | (df.get("Kurtosis_Entry", 0) < 0)
    df["Risk_Penalty"] += df["Skew_Failure"].astype(int) * -5

    df["Post_Earnings_Lag"] = (df["Earnings_Days"] < 0) & (df["Strategy_Tier"] == "Tier2_Neutral")
    df["Post_EPS_Stall"] = df["Post_Earnings_Lag"] & (df["Vega"] < 0.25) & (df["Gamma"] < 0.02)
    df["Human_Concern"] |= df["Post_EPS_Stall"]

    df["OBV_Slope"] = df["OBV"] - df["OBV"].rolling(3).mean()
    df["OBV_Risk"] = df["OBV_Slope"] < 0
    df["Risk_Penalty"] += df["OBV_Risk"].astype(int) * -3
    df["OVI_Score"] = df.get("OVI", 0).apply(lambda x: 5 if x > 0 else -5)
    df["MTF_Alignment"] = df.get("Chart_MTF_Aligned", False).astype(bool).apply(lambda x: 5 if x else -5)
    df["PCS_Momentum_Score"] = df.get("PCS_Momentum", 0).apply(lambda x: 5 if x > 0 else -5)

    # === Unified Signal Score ===
    df["PCS_SignalScore"] = (
        0.2 * df["Vega"] +
        0.1 * df["Gamma"] +
        0.1 * df["Delta"] +
        0.15 * df["ROI_Rate"] +
        0.15 * df["Chart_CompositeScore"] +
        0.10 * df["Pattern_Score"] / 10 +
        0.10 * df["Momentum_Score"] / 25 +
        0.05 * (df["OVI_Score"] + df["MTF_Alignment"] + df["PCS_Momentum_Score"]) +
        0.10 * (df["Risk_Penalty"] + df["Decay_Penalty"])
    ) * 100

    df["PCS_SignalScore"] = df["PCS_SignalScore"].clip(0, 100)

    # === Drift Slope Awareness
    df["PCS_Drift_ROC"] = df["PCS_Drift"] - df["PCS_Drift"].rolling(3).mean().fillna(0)
    df["Drift_Accelerating"] = df["PCS_Drift_ROC"] > 5

    # === Recovery Bias
    df["Recovery_Bias"] = (
        (df["PCS_Drift"] < 10) &
        (df.get("Vega_5D_DriftTail", 0) > -0.3) &
        (~df.get("Exit_Flag", False))
    )

    # === Monitoring Tags and Drift Triggers ===
    df["Alert_PCS_Drift"] = df["PCS_Drift"] > 15
    df["Alert_Action_Change"] = False  # placeholder for monitoring diff

    # === Market Open Awareness ===
    if "Snapshot_TS" in df.columns:
        ts = pd.to_datetime(df["Snapshot_TS"], errors="coerce")
        df["Snapshot_Hour"] = ts.dt.hour + ts.dt.minute / 60
        df["OpenNoise_Risk"] = df["Snapshot_Hour"] < 7.5  # Before 7:30 AM PST
    else:
        df["OpenNoise_Risk"] = False

    # === Event-Aware + Human-Mimic Recommendation Logic ===
    def decide(row):
        # Final Rec_Action logic, avoids referencing pre-existing Rec_Action
        if row["OpenNoise_Risk"] and (row["PCS_SignalScore"] < 55 or row["PCS_Drift"] > 20):
            return "TRIM"
        if row["Event_Context"] and (row["Vega"] < 0.25 or row["Gamma"] < 0.02):
            return "REVALIDATE"
        if row["PCS_SignalScore"] < 55 or row["PCS_Drift"] > 20:
            return "EXIT"
        if not row["Recovery_Bias"] or row["Human_Concern"]:
            return "REVALIDATE"
        if row.get("Drift_Accelerating", False):
            return "REVALIDATE"
        if row.get("PCS_Momentum", 0) > 0 and row["PCS_SignalScore"] < 70:
            return "TRIM"
        if row["PCS_SignalScore"] < 70:
            return "TRIM"
        return "HOLD"

    df["Rec_Action"] = df.apply(decide, axis=1)

    df["Rec_Rationale"] = df["Rec_Action"].map({
        "EXIT": "Signal or drift failure",
        "REVALIDATE": "No recovery / HH fail / Gamma collapse / Time pressure / sentiment drop",
        "TRIM": "Moderate degradation",
        "HOLD": "Edge intact"
    })

    df["Rec_Tier"] = df["Rec_Action"].map({"EXIT": 1, "REVALIDATE": 2, "TRIM": 3, "HOLD": 4})
    # === Final: Tag alerts and return ===
    return df
