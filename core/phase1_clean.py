import pandas as pd
import time
import os
from typing import Optional, Tuple

def phase1_load_and_clean_raw_v2(
    input_path: str = "/Users/haniabadi/Documents/Windows/Positions_Account_.csv",
    snapshot_dir: Optional[str] = "/Users/haniabadi/Documents/Windows/Optionrec/drift",
    save_snapshot: bool = False
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

    # === Clean column names ===
    df.columns = df.columns.str.strip().str.replace(r'[\s]+', ' ', regex=True)

    # === Type conversions ===
    def clean_percent(col): return pd.to_numeric(
        df[col].astype(str).str.replace('%', '', regex=False).str.replace(',', ''),
        errors='coerce'
    )
    def clean_money(col): return pd.to_numeric(
        df[col].astype(str).str.replace(',', ''), errors='coerce'
    )
    def clean_integer(col): return pd.to_numeric(
        df[col].astype(str).str.replace(',', '').fillna('0'), errors='coerce'
    ).astype('Int64')

    for col in ['% Total G/L', 'IV Mid', 'IV Bid', 'IV Ask']:
        if col in df.columns:
            df[col] = clean_percent(col)
    for col in ['$ Total G/L']:
        if col in df.columns:
            df[col] = clean_money(col)
    for col in ['Volume', 'Open Int']:
        if col in df.columns:
            df[col] = clean_integer(col)

    # # === Add placeholder columns ===
    # required_cols = ['PCS', 'PCS_Entry', 'IVHV_Gap_Entry', 'Snapshot_TS']
    # for col in required_cols:
    #     if col not in df.columns:
    #         df[col] = None

    # === Add actual snapshot timestamp ===
    timestamp = pd.to_datetime('today')
    df['Snapshot_TS'] = timestamp

    snapshot_path = ""
    if save_snapshot:
        filename = f"phase1_cleaned_{timestamp.strftime('%Y-%m-%d_%H-%M-%S')}.csv"
        snapshot_path = os.path.join(snapshot_dir, filename)
        os.makedirs(snapshot_dir, exist_ok=True)
        df.to_csv(snapshot_path, index=False)
        print(f"💾 Saved snapshot: {snapshot_path}")

    print("✅ Phase 1 complete.")
    print(f"📊 Rows: {df.shape[0]}, Columns: {df.shape[1]}")
    print(f"⏱ Duration: {round(time.time() - start, 2)}s")

    return df, snapshot_path
