import duckdb
import pandas as pd
from pathlib import Path
import sys
import os

# Add project root to path
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from core.shared.data_contracts.config import PIPELINE_DB_PATH

# Authoritative Stock Backfill Table
STOCK_BACKFILL_DATA = [
    {"Symbol": "UUUU", "Avg_Cost": 17.30, "Entry_Date": "2025-12-30"},
    {"Symbol": "AAPL", "Avg_Cost": 273.37, "Entry_Date": "2025-12-29"},
    {"Symbol": "SOFI", "Avg_Cost": 26.86, "Entry_Date": "2025-12-29"},
    {"Symbol": "PYPL", "Avg_Cost": 67.9384, "Entry_Date": "2024-03-01"}, # Derived from 6,793.84 / 100
    {"Symbol": "PLTR", "Avg_Cost": 191.79, "Entry_Date": "2025-11-10"},
    {"Symbol": "CVX", "Avg_Cost": 163.72, "Entry_Date": "2026-01-05"},
    {"Symbol": "INTC", "Avg_Cost": 36.32, "Entry_Date": "2024-12-24"},
]

def backfill():
    print("🔍 Starting Stock Acquisition Anchor backfill...")
    
    if not PIPELINE_DB_PATH.exists():
        print(f"❌ Database not found at {PIPELINE_DB_PATH}")
        return

    with duckdb.connect(str(PIPELINE_DB_PATH)) as con:
        success_count = 0
        failed_symbols = []

        for entry in STOCK_BACKFILL_DATA:
            symbol = entry["Symbol"]
            new_date = pd.to_datetime(entry["Entry_Date"])
            avg_cost = entry["Avg_Cost"]
            
            # Find the LegID in entry_anchors that matches this stock symbol
            # RAG: Identity. We target AssetType='STOCK' to avoid touching option legs.
            leg_id_df = con.execute("""
                SELECT LegID 
                FROM entry_anchors 
                WHERE Symbol = ? AND AssetType = 'STOCK' AND Is_Active = TRUE
            """, [symbol]).df()

            if not leg_id_df.empty:
                # Handle potential multiple stock legs (e.g. different accounts)
                for leg_id in leg_id_df['LegID']:
                    print(f"  ✅ Updating {symbol} -> {leg_id} | Date: {entry['Entry_Date']} | Price: {avg_cost}")
                    
                    con.execute("""
                        UPDATE entry_anchors 
                        SET Entry_Snapshot_TS = ?, 
                            Entry_Timestamp = ?,
                            Underlying_Price_Entry = ?
                        WHERE LegID = ?
                    """, [new_date, new_date, avg_cost, leg_id])
                success_count += 1
            else:
                print(f"  ❌ Could not find active stock leg for symbol: {symbol}")
                failed_symbols.append(symbol)

        print(f"\n✅ Backfill complete. Successfully updated {success_count} symbols.")
        if failed_symbols:
            print(f"⚠️  Failed to map {len(failed_symbols)} symbols: {failed_symbols}")

if __name__ == "__main__":
    backfill()
