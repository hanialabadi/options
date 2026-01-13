import sys
import os
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime

# Add parent directory to Python path to allow imports from core.scan_engine
parent_dir = Path(__file__).parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

from core.scan_engine.step0_resolve_snapshot import resolve_snapshot_path
from core.scan_engine.step2_load_snapshot import load_ivhv_snapshot

# Configure logging to capture all output
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def run_cli_diagnostic(explicit_path: str | None = None):
    """
    Runs Step 0 and ONLY Step 2 of the pipeline for diagnostic purposes.
    """
    print("\n--- CLI Pipeline Diagnostic Run (Step 0 -> Step 2 Only) ---")

    resolved_snapshot_path = None
    try:
        # Step 0: Resolve snapshot path using CLI-specific logic
        logger.info("🔍 Step 0: Resolving snapshot path...")
        resolved_snapshot_path = resolve_snapshot_path(
            explicit_path=explicit_path,
            snapshots_dir="data/snapshots" # CLI-specific archive directory
        )
        
        # Log required provenance information
        file_path_obj = Path(resolved_snapshot_path)
        file_stat = file_path_obj.stat()
        mod_time = datetime.fromtimestamp(file_stat.st_mtime)
        
        print(f"\nResolved Snapshot Path: {resolved_snapshot_path}")
        print(f"Filename: {file_path_obj.name}")
        print(f"File modification time: {mod_time.strftime('%Y-%m-%d %H:%M:%S')}")

    except FileNotFoundError as e:
        logger.error(f"❌ Step 0 failed: {e}")
        print(f"Error: {e}")
        return

    # Step 2: Load snapshot
    logger.info("📊 Step 2: Loading IV/HV snapshot...")
    df_snapshot = load_ivhv_snapshot(resolved_snapshot_path)
    logger.info("✅ Step 2 complete.")

    # Log required schema information
    print(f"Row count: {len(df_snapshot)}")
    print(f"Exact ordered column list: {df_snapshot.columns.tolist()}")
    
    timestamp_columns = [col for col in df_snapshot.columns if 'timestamp' in col.lower() or 'date' in col.lower()]
    print(f"Detected timestamp-like columns: {timestamp_columns}")

    print("\n--- Step 2 Output (First 5 Rows) ---")
    print(df_snapshot.head().to_markdown(index=False))

    print("\nSnapshot_Age_Hours statistics:")
    if 'Snapshot_Age_Hours' in df_snapshot.columns:
        min_age = df_snapshot['Snapshot_Age_Hours'].min()
        max_age = df_snapshot['Snapshot_Age_Hours'].max()
        print(f"  Min Snapshot_Age_Hours: {min_age:.2f} hours")
        print(f"  Max Snapshot_Age_Hours: {max_age:.2f} hours")
    else:
        print("  'Snapshot_Age_Hours' column not found in DataFrame.")
        print("  This typically means the input CSV did not have a 'timestamp' column for freshness validation.")

    print("\nRow Integrity (based on Step 2's internal logic):")
    id_col = None
    if 'Symbol' in df_snapshot.columns:
        id_col = 'Symbol'
    elif 'Ticker' in df_snapshot.columns:
        id_col = 'Ticker'

    if id_col:
        if df_snapshot[id_col].duplicated().any():
            print(f"  Step 2 detected and handled duplicate {id_col.lower()}s (keeping first occurrence).")
        else:
            print(f"  Step 2 did not detect duplicate {id_col.lower()}s.")
    else:
        print("  Neither 'Symbol' nor 'Ticker' column found, cannot assess duplicate rows.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run Step 0 and Step 2 of the scan pipeline for diagnostic purposes.")
    parser.add_argument('--path', type=str, help="Explicit path to the IV/HV snapshot CSV file.")
    args = parser.parse_args()

    run_cli_diagnostic(explicit_path=args.path)
