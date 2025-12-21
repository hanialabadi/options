import pandas as pd

def tag_persona_and_confidence(df: pd.DataFrame) -> pd.DataFrame:
    """
    Tags each trade with Rec_Tier, Persona, and Confidence based on PCS, strategy, and outcome tags.
    Used for system-tiered trust levels and historical analysis in LearnLoop.
    """

    def tag_confidence(pcs):
        if pcs >= 80:
            return ("Tier 1", "High")
        elif pcs >= 70:
            return ("Tier 2", "Medium")
        elif pcs >= 65:
            return ("Tier 3", "Low")
        else:
            return ("Tier 4", "Reject")

    def tag_persona(row):
        strat = row.get("Strategy", "")
        if strat in ["CSP", "Covered Call"]:
            return "Freeman"
        elif strat in ["Buy Call", "Buy Put"]:
            return "Passarelli"
        elif strat in ["Straddle", "Strangle"]:
            return "Natenberg"
        else:
            return "Mixed"

    tiers, confidence = zip(*df["PCS"].fillna(0).apply(tag_confidence))
    df["Rec_Tier"] = tiers
    df["Confidence"] = confidence
    df["Persona"] = df.apply(tag_persona, axis=1)
    return df
