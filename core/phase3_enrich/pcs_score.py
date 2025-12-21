import pandas as pd
import numpy as np
from datetime import datetime

# === PCS Scoring ===
def calculate_pcs(df: pd.DataFrame) -> pd.DataFrame:
    # === Structural Integrity Check ===
    required_columns = ["TradeID", "Strategy", "Structure", "LegType", "LegCount"]
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        print(f"⚠️ Warning: Missing strategy context columns: {missing_cols}")
        df["PCS_Warning"] = "Missing strategy tags"
        return df

    # === Strategy Profile Tagging ===
    def assign_profile(row):
        strat = str(row.get("Strategy", ""))
        if "Straddle" in strat or "Strangle" in strat:
            return "Neutral_Vol"
        elif "CSP" in strat or "CC" in strat:
            return "Income"
        elif "Buy Call" in strat:
            return "Directional_Bull"
        elif "Buy Put" in strat:
            return "Directional_Bear"
        else:
            return "Other"

    df["PCS_Profile"] = df.apply(assign_profile, axis=1)

    # === Scoring Engine
    def calculate_row_subscores(row):
        gamma = np.nan_to_num(pd.to_numeric(row.get("Gamma", 0)), nan=0)
        vega = np.nan_to_num(pd.to_numeric(row.get("Vega", 0)), nan=0)
        premium = np.nan_to_num(pd.to_numeric(row.get("Premium", 0)), nan=0)
        basis = np.nan_to_num(pd.to_numeric(row.get("Basis", premium)), nan=1e-3)
        roi = premium / basis
        profile = row.get("PCS_Profile", "Other")

        # Raw subscores
        gamma_score = min(gamma * 1500, 25)
        vega_score = min((vega / max(basis, 1e-3)) * 5000, 25)
        roi_score = 15 if roi >= 0.30 else 10 if roi >= 0.20 else 5

        # Profile-based weighting
        if profile == "Neutral_Vol":
            pcs = 0.6 * vega_score + 0.25 * gamma_score + 0.15 * roi_score
        elif profile == "Income":
            pcs = 0.5 * roi_score + 0.3 * vega_score + 0.2 * gamma_score
        elif profile in ["Directional_Bull", "Directional_Bear"]:
            pcs = 0.5 * gamma_score + 0.3 * vega_score + 0.2 * roi_score
        else:
            pcs = 0.4 * gamma_score + 0.4 * vega_score + 0.2 * roi_score

        return pd.Series([gamma_score, vega_score, roi_score, roi, pcs])

    # Optional: Compute Days to Expiration
    if "Expiration" in df.columns:
        today = pd.to_datetime(datetime.now().date())
        df["DTE"] = (pd.to_datetime(df["Expiration"]) - today).dt.days

    df[["PCS_GammaScore", "PCS_VegaScore", "PCS_ROIScore", "Raw_ROI", "PCS"]] = df.apply(
        calculate_row_subscores, axis=1
    )

    # Grouping PCS by TradeID
    if "TradeID" in df.columns:
        df["PCS_GroupAvg"] = df.groupby("TradeID")["PCS"].transform("mean")

    # Tier classification
    df["PCS_Tier"] = df["PCS"].apply(
        lambda pcs: "Tier 1" if pcs >= 80 else "Tier 2" if pcs >= 70 else "Tier 3" if pcs >= 60 else "Tier 4"
    )

    # Revalidation logic
    df["Needs_Revalidation"] = (df["PCS"] < 65) | (df["Vega"] < 0.25)

    print("✅ [PCS] strategy-aware scoring applied.")
    return df
