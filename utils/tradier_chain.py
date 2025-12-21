# %% 📚 Imports
import requests
import pandas as pd

# %% 🌐 Tradier API Config
TRADIER_TOKEN = "VDdi8tjNjzprxDVXu8rV0hBLQzuV"  # Replace with .env or secure secret manager in production
TRADIER_ENDPOINT = "https://api.tradier.com/v1/markets/options/chains"

# %% 🔗 Fetch Single Option Chain
def fetch_chain(symbol: str, expiry: str) -> pd.DataFrame:
    """
    Fetch full option chain (including Greeks) for a given ticker and expiration date.
    """
    headers = {
        "Authorization": f"Bearer {TRADIER_TOKEN}",
        "Accept": "application/json"
    }
    params = {"symbol": symbol, "expiration": expiry, "greeks": "true"}

    try:
        resp = requests.get(TRADIER_ENDPOINT, headers=headers, params=params)
        if resp.status_code != 200:
            print(f"⚠️ Tradier error for {symbol} {expiry}: HTTP {resp.status_code}, {resp.text}")
            return pd.DataFrame()

        data = resp.json()
        options = data.get("options", {}).get("option", [])
        if not options:
            print(f"⚠️ No options found for {symbol} {expiry}")
            return pd.DataFrame()

        df = pd.DataFrame(options)

        # Expand nested greeks
        if "greeks" in df.columns:
            greeks_df = df["greeks"].apply(pd.Series)
            greeks_df = greeks_df.rename(columns={k: f"Live_{k.capitalize()}" for k in greeks_df.columns})
            df = pd.concat([df, greeks_df], axis=1)

        # Normalize fields
        df["Strike"] = pd.to_numeric(df["strike"], errors="coerce")
        df["OptionType"] = df["option_type"].str.capitalize()
        df["Expiration"] = pd.to_datetime(df["expiration_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        df["Symbol"] = symbol

        return df

    except requests.exceptions.RequestException as e:
        print(f"⚠️ Request error for {symbol} {expiry}: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"⚠️ Unexpected error for {symbol} {expiry}: {e}")
        return pd.DataFrame()

# %% 🔁 Fetch Multiple Chains from DataFrame
def fetch_chains_from_master(df_master: pd.DataFrame) -> pd.DataFrame:
    """
    Loop through df_master (must include Symbol + Expiration), fetch chains.
    Returns concatenated DataFrame with all options.
    """
    chains = []
    for _, row in df_master[['Symbol', 'Expiration']].drop_duplicates().iterrows():
        chain = fetch_chain(row['Symbol'], row['Expiration'])
        if not chain.empty:
            chains.append(chain)
    if chains:
        df_chain = pd.concat(chains, ignore_index=True)
        print(f"✅ Total options collected: {len(df_chain)}")
        return df_chain
    else:
        print("⚠️ No chains fetched.")
        return pd.DataFrame()

# %% 🧪 Standalone Run Example
if __name__ == "__main__":
    df_master = pd.DataFrame({
        "Symbol": ["META", "AAPL", "GOOGL"],
        "Expiration": ["2025-07-25", "2025-08-01", "2025-07-25"]
    })
    df_chain_full = fetch_chains_from_master(df_master)
    print(df_chain_full.head())
