import pandas as pd
import requests
import os

# === ⚙️ Settings ===
TRADIER_TOKEN = "VDdi8tjNjzprxDVXu8rV0hBLQzuV"
TRADIER_ENDPOINT = "https://api.tradier.com/v1/markets/options/chains"

# === 1. Pull Tradier chain with Greeks for a symbol + expiry ===
def get_tradier_greeks(ticker, expiry, token=TRADIER_TOKEN):
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    params = {"symbol": ticker, "expiration": expiry, "greeks": "true"}
    try:
        resp = requests.get(TRADIER_ENDPOINT, headers=headers, params=params)
        if resp.status_code != 200:
            print(f"❌ Tradier API error: {resp.status_code}: {resp.text}")
            return pd.DataFrame()
        data = resp.json().get("options", {}).get("option", [])
        if not data:
            print(f"⚠️ No option data for {ticker} {expiry}")
            return pd.DataFrame()

        df_chain = pd.DataFrame(data)
        df_chain["symbol"] = df_chain["symbol"].str.upper()
        df_chain["Underlying"] = ticker.upper()

        if "greeks" in df_chain.columns:
            greeks_df = pd.json_normalize(df_chain["greeks"])
            df_chain = pd.concat([df_chain.drop(columns=["greeks"]), greeks_df], axis=1)

        return df_chain

    except Exception as e:
        print(f"⚠️ Tradier fetch error: {e}")
        return pd.DataFrame()

# === 2. Match Tradier chain to your master trades ===
def match_chain_to_master(df_chain, df_master):
    df_chain["OptionType"] = df_chain["option_type"].str.upper()
    df_chain["Strike"] = df_chain["strike"]
    df_chain["Expiration"] = pd.to_datetime(df_chain["expiration_date"]).dt.strftime("%Y-%m-%d")

    df_master["Expiration"] = pd.to_datetime(df_master["Expiration"]).dt.strftime("%Y-%m-%d")
    df_master["Strike"] = df_master["Strike"].astype(float)
    df_master["OptionType"] = df_master["OptionType"].str.upper()

    df_merge = df_master.merge(
        df_chain,
        on=["Underlying", "Strike", "Expiration", "OptionType"],
        how="left",
        suffixes=("", "_chain")
    )
    return df_merge

# === 3. Batch updater to refresh Greeks/IV from Tradier ===
def update_active_master_with_tradier(master_path="/Users/haniabadi/Documents/Windows/Optionrec/active_master.csv"):
    df_master = pd.read_csv(master_path)
    df_master["Underlying"] = df_master["Underlying"].astype(str).str.upper()

    tickers = df_master["Underlying"].dropna().unique()
    expiries = df_master["Expiration"].dropna().unique()

    updated_frames = []
    for ticker in tickers:
        for expiry in expiries:
            df_subset = df_master[
                (df_master["Underlying"] == ticker) & 
                (df_master["Expiration"] == expiry)
            ]
            if df_subset.empty:
                continue

            df_chain = get_tradier_greeks(ticker, expiry)
            if df_chain.empty:
                continue

            df_merged = match_chain_to_master(df_chain, df_subset)
            updated_frames.append(df_merged)

    if updated_frames:
        df_updated = pd.concat(updated_frames, ignore_index=True)

        for g in ["delta", "gamma", "vega", "theta"]:
            entry_col = g.upper() + "_Entry"
            live_col = g
            if entry_col not in df_updated.columns and live_col in df_updated.columns:
                df_updated[entry_col] = df_updated[live_col]

        df_updated.to_csv(master_path, index=False)
        print("✅ active_master.csv updated with Tradier chain data.")
        return df_updated
    else:
        print("❌ No updates applied — no matching chains found.")
        return df_master

# === 4. Minimal wrapper for revalidation use ===
def update_greeks_for_active():
    return update_active_master_with_tradier()
