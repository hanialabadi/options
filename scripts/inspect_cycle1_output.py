import duckdb
import pandas as pd
import os
import sys
from core.shared.data_contracts.config import POSITIONS_HISTORY_DB_PATH

# Ensure full visibility in terminal
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', None)

DB_PATH = str(POSITIONS_HISTORY_DB_PATH)

def inspect():
    print(f"Connecting to {DB_PATH}...")
    if not os.path.exists(DB_PATH):
        print(f"ERROR: Database file not found at {DB_PATH}")
        sys.exit(1)

    try:
        # Connect in read-only mode as per STRICT scope
        con = duckdb.connect(DB_PATH, read_only=True)
        
        # 1. List all tables
        print("\n=== TABLES IN DATABASE ===")
        tables_df = con.execute("SHOW TABLES").df()
        if tables_df.empty:
            print("No tables found.")
            con.close()
            return
        print(tables_df)

        # 2. Row count per table
        print("\n=== ROW COUNTS ===")
        for table_name in tables_df['name']:
            count = con.execute(f"SELECT COUNT(*) FROM \"{table_name}\"").fetchone()[0]
            print(f"Table '{table_name}': {count} rows")

        # The task requested 'positions_history' table, but Cycle 1 uses 'clean_legs'
        target_table = "positions_history"
        if target_table not in tables_df['name'].values and "clean_legs" in tables_df['name'].values:
            print(f"\nNOTE: Table '{target_table}' not found. Using 'clean_legs' as the primary Cycle 1 ledger.")
            target_table = "clean_legs"

        if target_table in tables_df['name'].values:
            # 3. Column list for the target table
            print(f"\n=== COLUMNS IN '{target_table}' ===")
            cols_df = con.execute(f"PRAGMA table_info('{target_table}')").df()
            columns = cols_df['name'].tolist()
            for col in columns:
                print(f"- {col}")

            # 4. Latest snapshot (ORDER BY Snapshot_TS DESC LIMIT 5)
            print(f"\n=== LATEST 5 SNAPSHOTS FROM '{target_table}' ===")
            # Check if Snapshot_TS exists before ordering
            if "Snapshot_TS" in columns:
                latest_df = con.execute(f"SELECT * FROM \"{target_table}\" ORDER BY Snapshot_TS DESC LIMIT 5").df()
                print(latest_df.to_string(index=False))
            else:
                print("ERROR: 'Snapshot_TS' column missing, cannot order by Snapshot_TS.")

            # 5. Validation Checklist
            expected_columns = [
                "Symbol", "Account", "Quantity", "Basis", "UL Last", "Last",
                "Delta", "Gamma", "Vega", "Theta", "Rho", "Snapshot_TS", "Schema_Hash"
            ]
            
            print("\n=== VALIDATION CHECKLIST ===")
            present = []
            missing = []
            
            # Note: "Symbol (OCC)" in task likely refers to the Symbol column containing OCC format strings
            for col in expected_columns:
                if col in columns:
                    present.append(col)
                    print(f"[x] {col}: PRESENT")
                else:
                    missing.append(col)
                    print(f"[ ] {col}: MISSING")
            
            print("\n=== SUMMARY ===")
            print(f"Which expected Cycle-1 columns are present: {', '.join(present) if present else 'None'}")
            print(f"Which (if any) are missing: {', '.join(missing) if missing else 'None'}")
            
        else:
            print(f"\nERROR: Table '{target_table}' not found.")

        con.close()
    except Exception as e:
        print(f"ERROR during inspection: {e}")
        sys.exit(1)

if __name__ == "__main__":
    inspect()
