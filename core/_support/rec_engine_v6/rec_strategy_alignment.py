# core/rec_engine_v6/rec_strategy_alignment.py

import pandas as pd

def align_strategy_signals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adjusts Rec_Action and PCS if strategy structure or persona rules are violated.
    Applies exit or revalidation logic to straddles, CSPs, and directional trades.
    """

    def enforce_alignment(row):
        strategy = row.get("Strategy", "")
        leg_status = row.get("LegStatus", "Active")
        pcs = row.get("PCS", 0)
        action = row.get("Rec_Action", "HOLD")

        if strategy in ["Straddle", "Strangle"] and leg_status != "Active":
            return "REVALIDATE"
        if strategy in ["Buy Call", "Buy Put"] and pcs < 65:
            return "EXIT"
        if strategy in ["CSP", "Covered Call"] and pcs < 70:
            return "HOLD ONLY"
        return action

    df["Rec_Action"] = df.apply(enforce_alignment, axis=1)
    return df
