import pandas as pd
import numpy as np

def tag_strategy_metadata(df: pd.DataFrame) -> pd.DataFrame:
    def tag_intent(row):
        strat = str(row.get("Strategy", ""))
        if "CSP" in strat:
            return "Bullish Income"
        elif "Straddle" in strat or "Strangle" in strat:
            return "Neutral Vol Edge"
        elif "Buy Call" in strat:
            return "Directional Bullish"
        elif "Buy Put" in strat:
            return "Directional Bearish"
        elif "CC" in strat:
            return "Yield + Cap"
        return "Unclassified"
    df["Tag_Intent"] = df.apply(tag_intent, axis=1)

    df["Tag_ExitStyle"] = df["Strategy"].apply(lambda x:
        "Trail Exit" if "Buy" in str(x) else
        "Theta Hold" if "CSP" in str(x) or "CC" in str(x) else
        "Dual Leg Exit" if "Straddle" in str(x) or "Strangle" in str(x) else
        "Manual")

    def tag_edge(row):
        vega = row.get("Vega", 0)
        theta = row.get("Theta", 0)
        if vega >= 0.25:
            return "Vol Edge"
        elif theta > abs(vega):
            return "Theta Edge"
        else:
            return "No Edge"
    df["Tag_EdgeType"] = df.apply(tag_edge, axis=1)

    if "Tag_IVHV_Tier" in df.columns:
        df.drop(columns=["Tag_IVHV_Tier"], inplace=True)

    if "Structure" not in df.columns:
        df["Structure"] = np.where(
            df["Strategy"].str.contains("Straddle|Strangle", na=False),
            "Neutral", "Directional"
        )

    if "LegCount" in df.columns:
        df["Tag_LegStructure"] = df["LegCount"].apply(lambda x: "Multi-Leg" if x > 1 else "Single-Leg")

    def compute_capital(row):
        strat = str(row.get("Strategy", ""))
        if "CSP" in strat:
            return row.get("Strike", 0) * 100
        else:
            return row.get("Basis", 0) * row.get("Quantity", 1)

    df["Capital Deployed"] = df.apply(compute_capital, axis=1)

    # ✅ NEW: Add IV spread, liquidity risk, vega efficiency
    if "IV Ask" in df.columns and "IV Bid" in df.columns:
        df["IV_Spread"] = df["IV Ask"] - df["IV Bid"]
        df["IV_LiquidityRisk"] = df["IV_Spread"] > 1.0

    if "Vega" in df.columns and "Basis" in df.columns:
        df["Vega_Efficiency"] = df["Vega"] / df["Basis"]

    print("✅ [tag_strategy_metadata] applied — Capital Deployed and IV metrics computed.")
    return df
