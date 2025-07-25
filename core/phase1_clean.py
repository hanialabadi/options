# %% 📚 Imports
import pandas as pd
import time
import os
from typing import Optional, Tuple

# %% 🧼 Phase 1: Load and Clean Raw Trade Data
def phase1_load_and_clean_raw_v2(
    input_path: str = "/Users/haniabadi/Documents/Windows/Positions_Account_.csv",
    snapshot_dir: Optional[str] = "/Users/haniabadi/Documents/Windows/Optionrec/drift"
) -> Tuple[pd.DataFrame, str]:
    start = time.time()

    try:
        print(f"⏳ Loading file: {input_path}")
        df = pd.read_csv(input_path)
        print("🟢 File loaded!")
    except FileNotFoundError:
        print(f"❌ File not found: {input_path}")
        return pd.DataFrame(), ""
    except Exception as e:
        print(f"❌ Error loading file: {e}")
        return pd.DataFrame(), ""

    df.columns = df.columns.str.strip()

    # === Cleaners
    def clean_percent(col): return pd.to_numeric(
        df[col].astype(str).str.replace('%', '', regex=False).str.replace(',', ''),
        errors='coerce'
    )

    def clean_money(col): return pd.to_numeric(
        df[col].astype(str).str.replace(',', ''),
        errors='coerce'
    )

    def clean_integer(col): return pd.to_numeric(
        df[col].astype(str).str.replace(',', '').fillna('0'),
        errors='coerce'
    ).astype('Int64')

    # === Apply Cleaners
    for col in ['% Total G/L', 'IV Mid', 'IV Bid', 'IV Ask']:
        if col in df.columns:
            df[col] = clean_percent(col)

    for col in ['$ Total G/L']:
        if col in df.columns:
            df[col] = clean_money(col)

    for col in ['Volume', 'Open Int']:
        if col in df.columns:
            df[col] = clean_integer(col)

    # === Entry Greeks
    greeks_cols = ['Delta', 'Gamma', 'Theta', 'Vega']
    for col in greeks_cols:
        if col in df.columns:
            df[f'{col}_Entry'] = df[col]

    # === Drift Metrics
    if all(f'{col}_Entry' in df.columns for col in greeks_cols):
        df['Delta_1D'] = df['Delta'] - df['Delta_Entry']
        df['Gamma_3D'] = df['Gamma'] - df['Gamma_Entry']
        df['Vega_5D'] = df['Vega'] - df['Vega_Entry']

        for col in greeks_cols:
            df[f'{col}_ROC'] = df[col].diff()
            df[f'{col}_ROC_percent'] = (df[f'{col}_ROC'] / df[f'{col}_Entry']) * 100

        df['Delta_1D_SMA'] = df['Delta_1D'].rolling(window=10).mean()
        df['Gamma_3D_SMA'] = df['Gamma_3D'].rolling(window=10).mean()
        df['Vega_5D_SMA'] = df['Vega_5D'].rolling(window=10).mean()

    # === Drop duplicate options
    df = df.drop_duplicates(subset=["Symbol"], keep="first")

    # === Save snapshot
    timestamp = pd.to_datetime('today').strftime('%Y-%m-%d_%H-%M-%S')
    filename = f"master_with_drift_{timestamp}.csv"
    snapshot_path = os.path.join(snapshot_dir, filename)
    os.makedirs(snapshot_dir, exist_ok=True)
    df.to_csv(snapshot_path, index=False)

    print("✅ Phase 1 complete.")
    print(f"📊 Rows: {df.shape[0]}, Columns: {df.shape[1]}")
    print(f"💾 Saved snapshot: {snapshot_path}")
    print(f"⏱ Duration: {round(time.time() - start, 2)}s")

    return df, snapshot_path

# %% 🧪 Optional: Run interactively (Jupyter or CLI)
if __name__ == "__main__":
    df, path = phase1_load_and_clean_raw_v2()
    print(f"✅ Loaded DataFrame: {df.shape}")
    print(f"📁 Snapshot saved to: {path}")
    try:
        from IPython.display import display
        display(df.head())
    except ImportError:
        print(df.head())
