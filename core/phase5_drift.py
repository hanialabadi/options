# %% 📚 Imports
import os
from datetime import datetime
import pandas as pd

# %% 💾 Phase 4: Save Snapshot
def phase4_save_snapshot(
    df: pd.DataFrame,
    snapshot_dir: str = "/Users/haniabadi/Documents/Windows/Optionrec/drift"
) -> tuple[pd.DataFrame, str]:
    """
    Phase 4: Save a timestamped snapshot of the DataFrame for drift tracking.
    Returns the same DataFrame and the saved file path.
    """

    # === 💡 Preview Before Save
    print("🧩 Columns in DataFrame:")
    print(df.columns.tolist())
    print("🔎 First 5 rows preview:")
    print(df.head())

    # === 🧪 Null Check for Required Columns
    required_columns = ['PCS', 'Skew_Entry', 'Kurtosis_Entry', 'IVHV_Gap_Entry']
    print("🕳️ Null check in critical columns:")
    for col in required_columns:
        if col in df.columns:
            null_count = df[col].isna().sum()
            print(f"{col}: {null_count} missing")
        else:
            print(f"{col}: ❌ column missing!")

    if "Skew_Entry" in df.columns:
        if df["Skew_Entry"].isna().sum() == 0:
            print("✅ Skew_Entry is fully populated.")
        else:
            print("⚠️ Skew_Entry still has missing values.")

    # === 📁 Path Setup
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"positions_{timestamp}.csv"
    snapshot_path = os.path.join(snapshot_dir, filename)
    os.makedirs(snapshot_dir, exist_ok=True)

    # === 💾 Save CSV
    try:
        df.to_csv(snapshot_path, index=False)
        print(f"✅ Snapshot saved at: {snapshot_path}")
    except Exception as e:
        print(f"❌ Failed to save snapshot: {e}")
        snapshot_path = ""

    # === 🔍 Final Preview
    print("🔍 df.head() after save:")
    print(df.head())

    return df, snapshot_path

# %% 🧪 Run standalone (optional test)
if __name__ == "__main__":
    df = pd.read_csv("/path/to/sample_post_pcs.csv")  # Adjust path if testing
    df, path = phase4_save_snapshot(df)
    print("✅ Snapshot path:", path)
