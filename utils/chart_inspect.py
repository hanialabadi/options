# %% 📚 Imports
import pandas as pd
import yfinance as yf
import pandas_ta as ta

# %% 📈 Fetch Underlying Chart + Indicators
def fetch_underlying_data(ticker: str, period="60d", interval="1h") -> pd.DataFrame:
    """
    Fetch historical price and indicator data for an underlying ticker.
    Adds EMA9, EMA21, and RSI to the DataFrame.
    """
    df = yf.download(ticker, period=period, interval=interval, auto_adjust=True, progress=False)
    if df.empty:
        raise ValueError(f"❌ No data returned for {ticker} at {interval} interval.")

    df['EMA9'] = ta.ema(df['Close'], 9)
    df['EMA21'] = ta.ema(df['Close'], 21)
    df['RSI'] = ta.rsi(df['Close'], 14)
    return df

# %% 🧪 Utility: Get Latest Snapshot
def get_latest_chart_snapshot(ticker: str) -> pd.Series:
    """
    Return the most recent row of chart data for a given underlying ticker.
    """
    df = fetch_underlying_data(ticker)
    return df.tail(1).squeeze()

# %% 🔁 Batch Fetch Chart Data (Optional)
def attach_latest_chart_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add EMA/RSI snapshot columns to the options DataFrame for each Underlying.
    """
    snapshots = []
    for _, row in df.iterrows():
        try:
            snap = get_latest_chart_snapshot(row["Underlying"])
            snapshots.append({
                "Underlying": row["Underlying"],
                "EMA9": snap["EMA9"],
                "EMA21": snap["EMA21"],
                "RSI": snap["RSI"]
            })
        except Exception as e:
            print(f"⚠️ Failed for {row['Underlying']}: {e}")
            snapshots.append({
                "Underlying": row["Underlying"],
                "EMA9": None,
                "EMA21": None,
                "RSI": None
            })

    snapshot_df = pd.DataFrame(snapshots)
    return df.merge(snapshot_df, on="Underlying", how="left")

# %% 🧪 Run Standalone
if __name__ == "__main__":
    df = pd.DataFrame({
        'Symbol': ['META250725C295', 'AAPL250801P210', 'GOOGL250725C275'],
        'Underlying': ['META', 'AAPL', 'GOOGL']
    })
    df = attach_latest_chart_snapshot(df)
    print(df[["Underlying", "EMA9", "EMA21", "RSI"]])
