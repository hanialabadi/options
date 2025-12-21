def score_confidence_tier(df):
    df["Confidence_Tier"] = df["PCS"].apply(
        lambda pcs: "Tier 1" if pcs >= 80 else
                    "Tier 2" if pcs >= 70 else
                    "Tier 3" if pcs >= 65 else
                    "Tier 4"
    )
    return df
