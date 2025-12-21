import os
import pandas as pd

try:
    from IPython.display import display
except ImportError:
    display = print

def load_master_snapshot(path: str = "/Users/haniabadi/Documents/Windows/Optionrec/active_master.csv") -> pd.DataFrame:
    print("🔒 Read-only load from active_master.csv")

    if not os.path.exists(path):
        print(f"⚠️ Master file not found at: {path}")
        return pd.DataFrame()

    try:
        df = pd.read_csv(path)
        print(f"✅ Loaded master with {len(df)} trades.")
    except Exception as e:
        print(f"❌ Failed to read CSV: {e}")
        return pd.DataFrame()

    # 🔐 Inject display-only derived fields (do not persist)
    if "TradeDate" in df.columns and "Days_Held" not in df.columns:
        df["TradeDate"] = pd.to_datetime(df["TradeDate"], errors="coerce")
        df["Days_Held"] = (pd.to_datetime("today") - df["TradeDate"]).dt.days
        print(f"🧮 Days_Held injected for {df['Days_Held'].notna().sum()} rows.")

    if "% Total G/L" in df.columns and "Held_ROI%" not in df.columns:
        df["Held_ROI%"] = df["% Total G/L"]
        print(f"💹 Held_ROI% injected from % Total G/L.")

    required = ['Delta_Entry', 'Gamma_Entry', 'Vega_Entry', 'Theta_Entry', 'IV_Entry']
    missing = [col for col in required if col not in df.columns]

    if not missing:
        print("🔎 Sample frozen Greeks:")
        display(df[['TradeID'] + required].head(5))
        if df['Delta_Entry'].abs().sum() == 0:
            print("⚠️ All frozen Greeks are zero! Check for accidental reset.")
    else:
        print(f"⚠️ Missing columns: {missing}")
        print("Skipping Greek display due to missing data.")

    return df

if __name__ == "__main__":
    df = load_master_snapshot()
    print("📊 Snapshot Head:")
    print(df.head())