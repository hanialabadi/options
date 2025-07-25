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
    Returns the updated DataFrame and the saved file path.
    """
    print("🧩 Columns in df before saving snapshot:")
    print(df.columns.tolist())
    print("🔎 Preview of first 5 rows in df before saving:")
    print(df.head())

    # === 🔍 Validate Key Columns
    important_columns = ['PCS', 'Skew_Entry', 'Kurtosis_Entry', 'IVHV_Gap_Entry']
    print("🕳️ Null check:")
    for col in important_columns:
        if col in df.columns:
            print(f"{col}: {df[col].isna().sum()} missing")
        else:
            print(f"{col}: ❌ column missing!")

    if "Skew_Entry" in df.columns and df['Skew_Entry'].isna().sum() > 0:
        print("⚠️ Skew_Entry still has missing values.")
    else:
        print("✅ Skew_Entry is populated correctly.")

    # === 🗂 File Save Path
    now_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    snapshot_path = os.path.join(snapshot_dir, f"positions_{now_str}.csv")
    os.makedirs(snapshot_dir, exist_ok=True)

    # === 💾 Save
    try:
        df.to_csv(snapshot_path, index=False)
        print(f"✅ Snapshot saved at: {snapshot_path}")
    except Exception as e:
        print(f"❌ Failed to save snapshot: {e}")
        snapshot_path = ""

    print("🔍 df.head() after save:")
    print(df.head())

    return df, snapshot_path

# %% 🧪 Interactive test
if __name__ == "__main__":
    df = pd.read_csv("/path/to/sample_cleaned_df.csv")  # Replace with actual file if testing
    df, path = phase4_save_snapshot(df)
    print("✅ Saved to:", path)
