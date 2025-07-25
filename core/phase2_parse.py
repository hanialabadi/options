# core/phase2_parse_symbols.py

# %% 📚 Imports
import pandas as pd
import re

# %% 🔍 Phase 2: Parse Option Symbols
def phase2_parse_symbols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parses OCC-style option symbols into Underlying, OptionType, Strike, Expiration.
    Ensures TradeID exists and matches Symbol if missing.
    """
    if "Symbol" not in df.columns:
        raise ValueError("❌ Missing 'Symbol' column in DataFrame.")

    # OCC-style pattern: TICKERYYMMDDCPSTRIKE (e.g. AAPL250816C210)
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

    if "TradeID" not in df.columns or df["TradeID"].isnull().any():
        df["TradeID"] = df["Symbol"].astype(str).str.strip()

    print("✅ Phase 2 complete: Parsed option symbols")
    print(df[["Symbol", "TradeID", "OptionType", "Strike", "Expiration"]].head())

    return df

# %% 🧪 Run standalone test
if __name__ == "__main__":
    df_sample = pd.DataFrame({"Symbol": ["AAPL250816C210", "TSLA250816P180", "INVALID"]})
    df_sample = phase2_parse_symbols(df_sample)
    print(df_sample)
