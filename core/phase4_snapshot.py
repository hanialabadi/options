import os
from datetime import datetime
import pandas as pd

def save_clean_snapshot(
    df: pd.DataFrame,
    snapshot_dir: str = "/Users/haniabadi/Documents/Windows/Optionrec/drift",
    db_path: str = "data/pipeline.duckdb",
    to_csv: bool = True,
    to_db: bool = True,
) -> tuple[pd.DataFrame, str, str]:
    """
    Saves a snapshot of clean live trades with a timestamped filename (CSV and/or DuckDB).
    Appends every snapshot to DuckDB (never overwrites) for full historical time-series storage.
    Returns: clean_df, snapshot_path (CSV or ""), run_id
    """
    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H-%M-%S")
    run_id = f"{date_str}_{time_str}"
    folder_path = os.path.join(snapshot_dir, date_str)
    os.makedirs(folder_path, exist_ok=True)
    snapshot_path = os.path.join(folder_path, f"positions_{run_id}.csv")

    df = df.copy()
    df["Snapshot_TS"] = now.isoformat()
    df["run_id"] = run_id  # Ensure run_id is present in every row

    # --- Keep only the desired clean columns (plus run_id) ---
    clean_cols = [
        'run_id', 'Symbol', 'Quantity', 'Last', 'Bid', 'Ask', 'Volume', '$ Total G/L', '% Total G/L',
        'Basis', 'Earnings Date', 'Theta', 'Vega', 'Delta', 'Gamma', 'IV Mid', 'IV Bid', 'IV Ask',
        'Open Int', 'Time Val', 'Intrinsic Val', 'Snapshot_TS', 'Underlying', 'OptionType', 'Strike',
        'Expiration', 'Strategy', 'Type', 'Structure', 'TradeID', 'LegType', 'LegCount', 'Premium',
        'Premium_Estimated'
    ]
    # Add any missing columns as None for schema consistency
    missing_cols = [c for c in clean_cols if c not in df.columns]
    for c in missing_cols:
        df[c] = None

    df_clean = df[clean_cols].copy()

    # --- Save CSV ---
    if to_csv:
        try:
            df_clean.to_csv(snapshot_path, index=False)
            print(f"\n✅ Snapshot saved successfully → {snapshot_path}")
        except Exception as e:
            print(f"\n❌ Failed to save snapshot: {e}")
            snapshot_path = ""

    # --- Save/Append to DuckDB ---
# --- Save/Append to DuckDB with Schema Sync ---
    if to_db:
        try:
            import duckdb
            os.makedirs(os.path.dirname(db_path), exist_ok=True)
            con = duckdb.connect(db_path)
            # --- Check if table exists and schema matches ---
            table_exists = con.execute("""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = 'clean_legs'
            """).fetchone()[0] > 0

            if table_exists:
                # Get column names from DuckDB table
                db_cols = [row[0] for row in con.execute("DESCRIBE clean_legs").fetchall()]
                df_cols = list(df_clean.columns)
                if db_cols != df_cols:
                    print("\n⚠️ Schema change detected. Dropping and recreating 'clean_legs'.")
                    con.execute("DROP TABLE clean_legs")
                    con.execute("CREATE TABLE clean_legs AS SELECT * FROM df_clean LIMIT 0")
            else:
                # Table doesn't exist, create with new schema
                con.execute("CREATE TABLE clean_legs AS SELECT * FROM df_clean LIMIT 0")

            # --- Now safe to append ---
            con.execute("INSERT INTO clean_legs SELECT * FROM df_clean")
            con.close()
            print(f"\n✅ DuckDB updated (appended) → {db_path} [run_id={run_id}]")
        except Exception as e:
            print(f"\n❌ Failed to update DuckDB: {e}")

    return df_clean, snapshot_path, run_id
