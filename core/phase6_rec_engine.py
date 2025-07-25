# %% 📚 Imports
import os
from datetime import datetime
import pandas as pd

# %% 💾 Phase 4: Save Snapshot
def phase4_save_snapshot(
    df: pd.DataFrame,
    snapshot_dir: str = "/Users/haniabadi/Documents/Windows/Optionrec/drift",
) -> str:
    """
    Phase 4: Save a timestamped snapshot of the DataFrame for drift tracking.
    Verifies critical columns and returns the saved file path.
    """
    # === 🔍 Preview Before Save ===
    print("🧩 Columns in df:")
    print(df.columns.tolist())

    print("🔎 First 5 rows:")
    print(df.head())

    # === 🕳️ Check Important Columns
    important_columns = ['PCS', 'Skew_Entry', 'Kurtosis_Entry', 'IVHV_Gap_Entry']
    print("🕳️ Null values in key columns:")
    for col in important_columns:
        if col in df.columns:
            print(f"{col}: {df[col].isna().sum()} missing")
        else:
            print(f"{col}: ❌ column missing!")

    # === ✅ Check Skew Entry Logic
    if "Skew_Entry" in df.columns and df["Skew_Entry"].isna().sum() == 0:
        print("✅ Skew_Entry is populated correctly.")
    else:
        print("⚠️ Skew_Entry still has missing values.")

    # === 🗂 Prepare Snapshot Path
    now_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    snapshot_path = os.path.join(snapshot_dir, f"positions_{now_str}.csv")

    os.makedirs(snapshot_dir, exist_ok=True)

    # === 💾 Save CSV
    try:
        df.to_csv(snapshot_path, index=False)
        print(f"✅ Snapshot saved successfully at: {snapshot_path}")
    except Exception as e:
        print(f"❌ Error saving snapshot: {e}")
        snapshot_path = ""

    return snapshot_path
