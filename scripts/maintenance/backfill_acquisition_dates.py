import duckdb
import pandas as pd
from pathlib import Path
import sys
import os

# Add project root to path
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from core.shared.data_contracts.config import PIPELINE_DB_PATH

# Authoritative Backfill Source (Manual Truth)
BACKFILL_DATA = [
    {"Symbol": "PYPL260123C60", "Date_Acquired": "2026-01-08"},
    {"Symbol": "SOFI260123P25", "Date_Acquired": "2026-01-06"},
    {"Symbol": "CVX260123C165", "Date_Acquired": "2026-01-10"},
    {"Symbol": "SOFI260130C29", "Date_Acquired": "2025-12-29"},
    {"Symbol": "AAPL260130C275", "Date_Acquired": "2025-12-29"},
    {"Symbol": "UUUU260206P14", "Date_Acquired": "2025-12-30"},
    {"Symbol": "SHOP260220C165", "Date_Acquired": "2025-12-29"},
    {"Symbol": "MSCI260220C580", "Date_Acquired": "2025-12-29"},
    {"Symbol": "TXN260220P185", "Date_Acquired": "2026-01-06"},
    {"Symbol": "INTC260220C38", "Date_Acquired": "2025-12-29"},
    {"Symbol": "QCOM260220P175", "Date_Acquired": "2026-01-06"},
    {"Symbol": "AAPL260220C280", "Date_Acquired": "2025-12-29"},
    {"Symbol": "VZ260227C40", "Date_Acquired": "2026-01-15"},
    {"Symbol": "UUUU270115C17", "Date_Acquired": "2025-12-04"},
    {"Symbol": "AAPL270115C260", "Date_Acquired": "2025-12-29"},
    {"Symbol": "PLTR280121C250", "Date_Acquired": "2025-12-10"},
    {"Symbol": "AMZN280121C220", "Date_Acquired": "2025-12-15"},
]

def backfill():
    print("🔍 Starting Option Acquisition Date backfill...")
    
    if not PIPELINE_DB_PATH.exists():
        print(f"❌ Database not found at {PIPELINE_DB_PATH}")
        return

    with duckdb.connect(str(PIPELINE_DB_PATH)) as con:
        success_count = 0
        failed_symbols = []

        for entry in BACKFILL_DATA:
            symbol = entry["Symbol"]
            new_date = pd.to_datetime(entry["Date_Acquired"])
            
            # Find the LegID in entry_anchors that matches this symbol
            # Note: LegID usually starts with the symbol
            leg_id_df = con.execute("""
                SELECT LegID 
                FROM entry_anchors 
                WHERE Symbol = ? AND Is_Active = TRUE
            """, [symbol]).df()

            if leg_id_df.empty:
                # Try matching by LegID prefix if Symbol column is unreliable
                leg_id_df = con.execute("""
                    SELECT LegID 
                    FROM entry_anchors 
                    WHERE LegID LIKE ? AND Is_Active = TRUE
                """, [f"{symbol}%"]).df()

            if not leg_id_df.empty:
                leg_id = leg_id_df['LegID'].iloc[0]
                print(f"  ✅ Mapping {symbol} -> {leg_id} | New Date: {entry['Date_Acquired']}")
                
                con.execute("""
                    UPDATE entry_anchors 
                    SET Entry_Snapshot_TS = ?, Entry_Timestamp = ?
                    WHERE LegID = ?
                """, [new_date, new_date, leg_id])
                success_count += 1
            else:
                print(f"  ❌ Could not map symbol: {symbol}")
                failed_symbols.append(symbol)

        print(f"\n✅ Backfill complete. Successfully updated {success_count} positions.")
        if failed_symbols:
            print(f"⚠️  Failed to map {len(failed_symbols)} symbols: {failed_symbols}")

if __name__ == "__main__":
    backfill()
