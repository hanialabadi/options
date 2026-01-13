import pandas as pd
import numpy as np


def run_rec_engine_v4(df):
    df = df.copy()

    # === Step 1: Normalize missing values ===
    for col in ["Vega", "Gamma", "Delta", "PCS", "Chart_CompositeScore", "Held_ROI%", "Days_Held"]:
        df[col] = pd.to_numeric(df.get(col), errors="coerce").fillna(0)

    df["ROI_Rate"] = df["Held_ROI%"] / df["Days_Held"].replace(0, 1)

    # === Step 2: Parse chart tags into binary weights ===
    def parse_tags(tags):
        if isinstance(tags, str):
            tags = tags.replace("[", "").replace("]", "").replace("'", "").split(", ")
        elif isinstance(tags, list):
            tags = tags
        else:
            return 0

        score = 0
        for tag in tags:
            if "Doji" in tag: score -= 7
            if "ShootingStar" in tag: score -= 10
            if "Hammer" in tag: score += 5
            if "Bullish" in tag: score += 7
            if "Bearish" in tag: score -= 5
        return score

    df["Pattern_Score"] = df["Chart_Tags"].apply(parse_tags)

    # === Step 3: Holistic Signal Score ===
    df["PCS_HolisticScore"] = (
        0.25 * df["Vega"] +
        0.15 * df["Gamma"] +
        0.10 * df["Delta"] +
        0.15 * df["ROI_Rate"] +
        0.15 * df["Chart_CompositeScore"] +
        0.10 * df.get("RSI", 50).apply(lambda x: 1 if 45 <= x <= 65 else 0) +
        0.10 * df["Pattern_Score"] / 10  # Normalize pattern impact
    ) * 100

    df["PCS_HolisticScore"] = df["PCS_HolisticScore"].clip(0, 100)

    # === Step 4: Drift and Chart Flags ===
    df["Recovery_Flag"] = (
        (df.get("PCS_Drift", 0) < 10) &
        (df.get("Vega_5D_DriftTail", 0) > -0.3) &
        (~df.get("Exit_Flag", False))
    )

    # === Step 5: Recommendation ===
    def decide(row):
        if row["PCS_HolisticScore"] < 55 or row.get("PCS_Drift", 0) > 20:
            return "EXIT"
        if not row["Recovery_Flag"]:
            return "REVALIDATE"
        if row["PCS_HolisticScore"] < 70:
            return "TRIM"
        return "HOLD"

    df["Rec_Action"] = df.apply(decide, axis=1)

    def rationale(row):
        if row["Rec_Action"] == "EXIT": return "Signal breakdown or drift"
        if row["Rec_Action"] == "REVALIDATE": return "No recovery / exit flag"
        if row["Rec_Action"] == "TRIM": return "Moderate weakening"
        return "Edge intact"

    df["Rec_Rationale"] = df.apply(rationale, axis=1)
    df["Rec_Tier"] = df["Rec_Action"].map({"EXIT": 1, "REVALIDATE": 2, "TRIM": 3, "HOLD": 4})

    return df
