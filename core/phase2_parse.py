
import pandas as pd
import re

def generate_trade_id(row):
    expiration_fmt = pd.to_datetime(row["Expiration"]).strftime("%y%m%d")
    strategy = row["Strategy"].replace(" ", "")
    return f"{row['Underlying']}{expiration_fmt}_{strategy}"

def phase2_parse_symbols(df):
    if "Symbol" not in df.columns:
        raise ValueError("❌ Missing 'Symbol' column in DataFrame.")

    pattern = re.compile(r"^([A-Z]+)(\d{2})(\d{2})(\d{2})([CP])(\d+(\.\d{1,2})?)$")

    def parse_symbol(sym):
        sym = str(sym).strip() if pd.notnull(sym) else ""
        match = pattern.match(sym)
        if match:
            ticker = match.group(1)
            year = "20" + match.group(2)
            month = match.group(3)
            day = match.group(4)
            opt_type = "Call" if match.group(5) == "C" else "Put"
            strike = float(match.group(6))
            expiration = pd.to_datetime(f"{year}-{month}-{day}", errors='coerce')
            return pd.Series([ticker, opt_type, strike, expiration])
        else:
            return pd.Series([sym, None, None, None])

    parsed = df["Symbol"].apply(parse_symbol)
    parsed.columns = ["Underlying", "OptionType", "Strike", "Expiration"]
    df = pd.concat([df, parsed], axis=1)
    return df

def phase21_strategy_tagging(df):
    df["Strategy"] = "Unknown"
    df["Type"] = "Single-leg"

    grouped = df.groupby(["Underlying", "Expiration"])
    for (underlying, expiry), group in grouped:
        calls = group[group["OptionType"] == "Call"]
        puts = group[group["OptionType"] == "Put"]

        for strike in group["Strike"].unique():
            call = calls[calls["Strike"] == strike]
            put = puts[puts["Strike"] == strike]
            if not call.empty and not put.empty:
                indices = pd.concat([call, put]).index
                df.loc[indices, "Strategy"] = "Long Straddle"
                df.loc[indices, "Type"] = "Multi-leg"

        if len(calls) == 1 and len(puts) == 1 and calls["Strike"].iloc[0] != puts["Strike"].iloc[0]:
            indices = pd.concat([calls, puts]).index
            df.loc[indices, "Strategy"] = "Long Strangle"
            df.loc[indices, "Type"] = "Multi-leg"

    for idx, row in df[df["Strategy"] == "Unknown"].iterrows():
        if row["OptionType"] == "Call":
            df.at[idx, "Strategy"] = "Buy Call"
        elif row["OptionType"] == "Put":
            df.at[idx, "Strategy"] = "Buy Put"

    df["Structure"] = df["Strategy"].apply(lambda x: "Multi-leg" if "Straddle" in x or "Strangle" in x else "Single-leg")
    df["TradeID"] = df.apply(generate_trade_id, axis=1)

    # Final CSP/CC tagging
    csp_mask = (df["OptionType"] == "Put") & (df["Quantity"] < 0)
    df.loc[csp_mask, "Strategy"] = "Cash-Secured Put"

    cc_mask = (df["OptionType"] == "Call") & (df["Quantity"] < 0)
    df.loc[cc_mask, "Strategy"] = "Covered Call"

    # === Leg Metadata ===
    df["LegType"] = df["OptionType"].map({"Call": "Call-Leg", "Put": "Put-Leg"})
    df["LegCount"] = df.groupby("TradeID")["Symbol"].transform("count")

    # === Estimate Premium if not in raw data
    if "Premium" not in df.columns:
        df["Premium"] = (df["Bid"] + df["Ask"]) / 2
        df["Premium_Estimated"] = True
        print("⚠️ 'Premium' not found in raw data — estimated from Bid/Ask.")
    else:
        df["Premium_Estimated"] = False


    return df

def phase2_run_all(df):
    df = phase2_parse_symbols(df)
    df = phase21_strategy_tagging(df)
    return df
