# core/pcs_engine/strategy_score.py

import pandas as pd

def compute_strategy_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds/returns a 'PCS_StrategyAdj' column for strategy-specific bonuses/penalties.
    """
    df = df.copy()
    # Simple logic: adjust by structure
    adj = []
    for _, row in df.iterrows():
        strat = row.get("Strategy", "")
        if "Straddle" in strat or "Strangle" in strat:
            adj.append(5)
        elif "Covered" in strat or "CSP" in strat:
            adj.append(2)
        else:
            adj.append(0)
    df["PCS_StrategyAdj"] = adj
    return df
