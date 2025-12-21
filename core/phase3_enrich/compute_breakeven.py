import numpy as np
import pandas as pd

def compute_breakeven(df: pd.DataFrame) -> pd.DataFrame:
    required = ["Strategy", "Premium", "Strike", "TradeID"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise KeyError(f"❌ Missing required columns for breakeven calculation: {missing}")

    def compute_group_breakeven(group):
        strategy = str(group["Strategy"].iloc[0])
        strikes = group["Strike"].dropna().tolist()
        premiums = group["Premium"].dropna().tolist()
        total_premium = sum(premiums)

        if "Straddle" in strategy or "Strangle" in strategy:
            if len(strikes) == 2:
                lower = min(strikes) - total_premium
                upper = max(strikes) + total_premium
                return pd.Series([lower, upper, f"{lower:.2f} / {upper:.2f}", "Straddle/Strangle"])
        elif "Put" in strategy:
            if strikes and premiums:
                return pd.Series([strikes[0] - premiums[0], np.nan, strikes[0] - premiums[0], "Put"])
        elif "Call" in strategy:
            if strikes and premiums:
                return pd.Series([np.nan, strikes[0] + premiums[0], strikes[0] + premiums[0], "Call"])

        return pd.Series([np.nan, np.nan, np.nan, "Unknown"])

    try:
        breakeven_df = (
            df.drop(columns=["TradeID"])
            .groupby(df["TradeID"], group_keys=False)
            .apply(compute_group_breakeven)
            .rename(columns={0: "BreakEven_Lower", 1: "BreakEven_Upper", 2: "BreakEven", 3: "BreakEven_Type"})
        )

        df = df.merge(breakeven_df, left_on="TradeID", right_index=True, how="left")
    except Exception as e:
        print(f"⚠️ Error during BreakEven calculation: {e}")
        df["BreakEven_Lower"] = np.nan
        df["BreakEven_Upper"] = np.nan
        df["BreakEven"] = np.nan
        df["BreakEven_Type"] = "Error"

    return df
