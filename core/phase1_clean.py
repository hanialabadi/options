import pandas as pd
import time
import os
from typing import Optional, Tuple


def phase1_load_and_clean_raw_v2(
    input_path: str = "/Users/haniabadi/Documents/Windows/Positions_Account_.csv",
    snapshot_dir: Optional[str] = "/Users/haniabadi/Documents/Windows/Optionrec/drift",
    save_snapshot: bool = False
) -> Tuple[pd.DataFrame, str]:
    """
    Phase 1: Load and clean raw brokerage position data.

    This function performs the **first stage** of the options analytics pipeline.
    It is responsible for:
      1. Loading the raw CSV exported from the brokerage platform
      2. Normalizing column names
      3. Converting string-based numeric fields into usable numeric types
      4. Attaching a snapshot timestamp
      5. (Optionally) persisting a cleaned snapshot to disk

    No scoring, filtering, or strategy logic occurs here.
    This phase is **purely structural and type-safe preparation**.

    Parameters
    ----------
    input_path : str, optional
        Absolute path to the raw CSV file containing position data.
        Expected to be a brokerage export with percentage, currency,
        volume, and IV-related columns.

    snapshot_dir : Optional[str], optional
        Directory where cleaned snapshot CSVs will be saved if
        `save_snapshot=True`. The directory is created if it does not exist.

    save_snapshot : bool, default False
        If True, writes a timestamped cleaned CSV snapshot to `snapshot_dir`.
        If False, cleaning is performed in-memory only.

    Returns
    -------
    Tuple[pandas.DataFrame, str]
        - DataFrame: cleaned and type-normalized position data
        - str: full path to the saved snapshot file (empty string if not saved)

    Failure Behavior
    ----------------
    - If the input file is not found, returns (empty DataFrame, "")
    - If any unexpected exception occurs during load, returns (empty DataFrame, "")

    Side Effects
    ------------
    - Prints progress and timing information to stdout
    - Optionally writes a CSV snapshot to disk

    Notes
    -----
    - Percentage columns are converted to numeric values WITHOUT the '%' sign
      (e.g., "12.5%" → 12.5)
    - Currency columns are converted to floats with commas removed
    - Volume and Open Interest columns are coerced to nullable integers
    - A `Snapshot_TS` column is always added using the current timestamp

    This function is intentionally verbose and defensive because it serves
    as the foundation for all downstream PCS, IV/HV, drift, and strategy logic.
    """

    start = time.time()

    # =========================
    # Load raw CSV
    # =========================
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

    # =========================
    # Normalize column names
    # =========================
    # - Strip leading/trailing spaces
    # - Collapse multiple spaces into one
    df.columns = df.columns.str.strip().str.replace(r'[\s]+', ' ', regex=True)

    # =========================
    # Type cleaning helpers
    # =========================
    def clean_percent(col: str) -> pd.Series:
        """Convert percentage strings (e.g. '12.3%') to numeric floats."""
        return pd.to_numeric(
            df[col]
            .astype(str)
            .str.replace('%', '', regex=False)
            .str.replace(',', ''),
            errors='coerce'
        )

    def clean_money(col: str) -> pd.Series:
        """Convert currency-like strings to numeric floats."""
        return pd.to_numeric(
            df[col].astype(str).str.replace(',', ''),
            errors='coerce'
        )

    def clean_integer(col: str) -> pd.Series:
        """Convert integer-like strings to nullable integers."""
        return pd.to_numeric(
            df[col].astype(str).str.replace(',', '').fillna('0'),
            errors='coerce'
        ).astype('Int64')

    # =========================
    # Apply type conversions
    # =========================
    for col in ['% Total G/L', 'IV Mid', 'IV Bid', 'IV Ask']:
        if col in df.columns:
            df[col] = clean_percent(col)

    for col in ['$ Total G/L']:
        if col in df.columns:
            df[col] = clean_money(col)

    for col in ['Volume', 'Open Int']:
        if col in df.columns:
            df[col] = clean_integer(col)

    # =========================
    # Attach snapshot timestamp
    # =========================
    timestamp = pd.to_datetime('now')
    df['Snapshot_TS'] = timestamp

    # =========================
    # Optional snapshot export
    # =========================
    snapshot_path = ""
    if save_snapshot:
        if snapshot_dir is None:
            print("⚠️ snapshot_dir is None, skipping snapshot save")
        else:
            filename = f"phase1_cleaned_{timestamp.strftime('%Y-%m-%d_%H-%M-%S')}.csv"
            snapshot_path = os.path.join(snapshot_dir, filename)
            os.makedirs(snapshot_dir, exist_ok=True)
            df.to_csv(snapshot_path, index=False)
            print(f"💾 Saved snapshot: {snapshot_path}")

    # =========================
    # Completion summary
    # =========================
    print("✅ Phase 1 complete.")
    print(f"📊 Rows: {df.shape[0]}, Columns: {df.shape[1]}")
    print(f"⏱ Duration: {round(time.time() - start, 2)}s")

    return df, snapshot_path