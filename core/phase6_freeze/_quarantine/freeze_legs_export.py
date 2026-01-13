import os
import pandas as pd
from datetime import datetime

def collapse_strategy_rows(df: pd.DataFrame) -> pd.DataFrame:
    grouped = df.groupby("TradeID", group_keys=False)
    collapsed = []

    for tid, group in grouped:
        if group.shape[0] == 1:
            collapsed.append(group.iloc[0])
            continue

        # Multi-leg: Pick leg with highest Vega (fallback to Delta)
        sort_cols = ["Vega", "Delta"] if "Vega" in group.columns else ["Delta"]
        best_leg = group.sort_values(sort_cols, ascending=False).iloc[0].copy()

        # Optional: recompute BreakEven using both legs
        if "Call" in group["OptionType"].values and "Put" in group["OptionType"].values:
            c_leg = group[group["OptionType"] == "Call"].iloc[0]
            p_leg = group[group["OptionType"] == "Put"].iloc[0]
            best_leg["BreakEven"] = f"{p_leg['Strike'] - p_leg['Premium']:.2f} / {c_leg['Strike'] + c_leg['Premium']:.2f}"

        best_leg["Structure"] = "Multi-leg"
        collapsed.append(best_leg)

    df_collapsed = pd.DataFrame(collapsed)
    return df_collapsed

def generate_legs_df(df: pd.DataFrame, save_dir: str = "/Users/haniabadi/Documents/Windows/Optionrec/legs") -> tuple[pd.DataFrame, str]:
    os.makedirs(save_dir, exist_ok=True)

    leg_cols = [
        "TradeID", "Symbol", "OptionType", "Strike", "Expiration",
        "Delta", "Vega", "Theta", "Gamma", "IV Mid", "Premium",
        "PCS", "Strategy"
    ]

    legs_df = df[df["OptionType"].isin(["Call", "Put"])].copy()
    legs_df = legs_df[leg_cols]
    legs_df["LegStatus"] = "Active"
    legs_df["Snapshot_TS"] = datetime.now().isoformat()

    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"legs_{ts}.csv"
    path = os.path.join(save_dir, filename)
    legs_df.to_csv(path, index=False)

    print(f"✅ legs_df saved to: {path}")

    # ✅ FIXED: Use the actual collapse function
    df_collapsed = collapse_strategy_rows(df)
    return df_collapsed, path
