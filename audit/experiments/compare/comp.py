import pandas as pd

def get_symbol_col(df):
    for c in df.columns:
        if c.lower() in {"symbol", "ticker", "tickers"}:
            return c
    raise ValueError("No ticker column found")

# ---- load your universe ----
your_df = pd.read_csv("tickers.csv")
your_col = get_symbol_col(your_df)
your_symbols = set(your_df[your_col].astype(str).str.upper().str.strip())

# ---- load S&P 500 ----
sp_df = pd.read_csv("constituents.csv")
sp_symbols = set(sp_df["Symbol"].astype(str).str.upper().str.strip())

# ---- load Russell 2000 ----
r2k_df = pd.read_csv("russell_2000_cik_to_ticker.csv")
r2k_col = get_symbol_col(r2k_df)
r2k_symbols = set(r2k_df[r2k_col].astype(str).str.upper().str.strip())

# ---- overlaps ----
overlap_sp = your_symbols & sp_symbols
overlap_r2k = your_symbols & r2k_symbols
overlap_both = your_symbols & sp_symbols & r2k_symbols

only_yours = your_symbols - (sp_symbols | r2k_symbols)

print("==== UNIVERSE SIZES ====")
print("Your universe:", len(your_symbols))
print("S&P 500:", len(sp_symbols))
print("Russell 2000:", len(r2k_symbols))

print("\n==== OVERLAPS (counts) ====")
print("Overlap with S&P 500:", len(overlap_sp))
print("Overlap with Russell 2000:", len(overlap_r2k))
print("Overlap with BOTH:", len(overlap_both))
print("Only in your list (neither index):", len(only_yours))

print("\n==== COVERAGE % OF YOUR UNIVERSE ====")
print("Covered by S&P 500:", round(len(overlap_sp) / len(your_symbols) * 100, 2), "%")
print("Covered by Russell 2000:", round(len(overlap_r2k) / len(your_symbols) * 100, 2), "%")

# ---- optional exports (for inspection) ----
pd.Series(sorted(overlap_r2k)).to_csv("overlap_r2k.csv", index=False)
pd.Series(sorted(only_yours)).to_csv("only_yours_neither_index.csv", index=False)