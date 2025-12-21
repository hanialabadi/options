import pandas as pd
import numpy as np

def flatten_snapshot_per_tradeid(df_snapshot: pd.DataFrame, return_legs_df: bool = False):
    """
    Flatten per-leg snapshot into one row per TradeID with column-suffixed leg data (_Call, _Put).
    Adds _Combined metrics and reduces redundancy.
    Optionally returns df_legs (per-leg format) if return_legs_df=True
    """
    # Ensure TradeID is present
    if "TradeID" not in df_snapshot.columns:
        raise ValueError("Missing TradeID column in input snapshot")

    # Split calls and puts
    call_legs = df_snapshot[df_snapshot["OptionType"] == "Call"].copy()
    put_legs = df_snapshot[df_snapshot["OptionType"] == "Put"].copy()

    # Preserve TradeID before suffixing
    call_legs["TradeID"] = call_legs["TradeID"]
    put_legs["TradeID"] = put_legs["TradeID"]

    # Tag each with consistent suffix
    call_legs = call_legs.add_suffix("_Call")
    put_legs = put_legs.add_suffix("_Put")

    # Restore TradeID column for merging
    call_legs.rename(columns={"TradeID_Call": "TradeID"}, inplace=True)
    put_legs.rename(columns={"TradeID_Put": "TradeID"}, inplace=True)

    # Merge left/right with outer to support single legs
    df_flat = pd.merge(call_legs, put_legs, on="TradeID", how="outer")

    # Strategy from either leg
    df_flat["Strategy"] = df_flat.get("Strategy_Call").combine_first(df_flat.get("Strategy_Put"))

    # OptionTypes construction
    call_type = df_flat.get("OptionType_Call", pd.Series("", index=df_flat.index)).fillna("")
    put_type = df_flat.get("OptionType_Put", pd.Series("", index=df_flat.index)).fillna("")
    df_flat["OptionTypes"] = np.where(
        (call_type != "") & (put_type != ""),
        call_type + "/" + put_type,
        call_type + put_type
    )

    # Strike formatting
    strike_call_str = df_flat.get("Strike_Call", pd.Series("", index=df_flat.index)).fillna("").astype(str)
    strike_put_str = df_flat.get("Strike_Put", pd.Series("", index=df_flat.index)).fillna("").astype(str)
    df_flat["Strikes"] = np.where(
        (strike_put_str != "") & (strike_call_str != ""),
        strike_put_str + "P/" + strike_call_str,
        strike_call_str.where(strike_call_str != "", strike_put_str)
    )

    # Premiums
    df_flat["Premium_Call"] = df_flat.get("Premium_Call", 0).fillna(0)
    df_flat["Premium_Put"] = df_flat.get("Premium_Put", 0).fillna(0)
    df_flat["Premium_Total"] = df_flat["Premium_Call"] + df_flat["Premium_Put"]

    # Combined Greeks (na-friendly sum)
    df_flat["Delta_Combined"] = df_flat[["Delta_Call", "Delta_Put"]].sum(axis=1, skipna=True)
    df_flat["Gamma_Combined"] = df_flat[["Gamma_Call", "Gamma_Put"]].sum(axis=1, skipna=True)
    df_flat["Vega_Combined"] = df_flat[["Vega_Call", "Vega_Put"]].sum(axis=1, skipna=True)
    df_flat["Theta_Combined"] = df_flat[["Theta_Call", "Theta_Put"]].sum(axis=1, skipna=True)

    # Combined Basis (na-friendly sum)
    df_flat["Basis_Call"] = df_flat.get("Basis_Call", 0).fillna(0)
    df_flat["Basis_Put"] = df_flat.get("Basis_Put", 0).fillna(0)
    df_flat["Basis_Combined"] = df_flat["Basis_Call"] + df_flat["Basis_Put"]

    # Final clean ordering
    front_cols = [
        # Core identifiers
        "TradeID", "Strategy", "OptionTypes", "Strikes",

        # Combined metrics
        "Premium_Total", "Basis_Combined",
        "Delta_Combined", "Gamma_Combined", "Vega_Combined", "Theta_Combined",

        # Raw symbols (optional)
        "Symbol_Call", "Symbol_Put",

        # Leg Greeks
        "Delta_Call", "Delta_Put", "Gamma_Call", "Gamma_Put",
        "Vega_Call", "Vega_Put", "Theta_Call", "Theta_Put",
        "IV_Mid_Call", "IV_Mid_Put",

        # Premium and Basis
        "Premium_Call", "Premium_Put", "Basis_Call", "Basis_Put"
    ]

    # Preserve column order and drop any snapshot clutter
    front_cols_final = [col for col in front_cols if col in df_flat.columns]
    remaining_cols = [col for col in df_flat.columns if col not in front_cols_final]
    drop_cols = [c for c in df_flat.columns if any(x in c for x in ["Snapshot", "Volume", "Ask", "Bid"])]

    df_flat = df_flat[front_cols_final + remaining_cols]
    df_flat.drop(columns=drop_cols, inplace=True, errors="ignore")
    df_flat.reset_index(drop=True, inplace=True)

    if return_legs_df:
        # === Rebuild df_legs from original snapshot
        legs = []
        for leg_type in ["Call", "Put"]:
            temp = df_snapshot[df_snapshot["OptionType"] == leg_type].copy()
            temp["LegType"] = leg_type
            legs.append(temp)
        df_legs = pd.concat(legs, axis=0).reset_index(drop=True)
        return df_flat, df_legs

    return df_flat
