import duckdb
import pandas as pd
import numpy as np
from pathlib import Path
import sys
import os
import re

# Add project root to path
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from core.shared.data_contracts.config import PIPELINE_DB_PATH

def backfill():
    print("🔍 Starting Entry_Structure backfill...")
    
    if not PIPELINE_DB_PATH.exists():
        print(f"❌ Database not found at {PIPELINE_DB_PATH}")
        return

    with duckdb.connect(str(PIPELINE_DB_PATH)) as con:
        # 1. Ensure columns exist
        cols = con.execute("PRAGMA table_info('entry_anchors')").df()
        if 'Entry_Structure' not in cols['name'].values:
            print("➕ Adding Entry_Structure column to entry_anchors...")
            con.execute("ALTER TABLE entry_anchors ADD COLUMN Entry_Structure VARCHAR")
        
        if 'Is_Active' not in cols['name'].values:
            print("➕ Adding Is_Active column to entry_anchors...")
            con.execute("ALTER TABLE entry_anchors ADD COLUMN Is_Active BOOLEAN DEFAULT TRUE")
            
        if 'Closed_TS' not in cols['name'].values:
            print("➕ Adding Closed_TS column to entry_anchors...")
            con.execute("ALTER TABLE entry_anchors ADD COLUMN Closed_TS TIMESTAMP")

        # 2. Get active anchors
        anchors = con.execute("SELECT LegID, Entry_Snapshot_TS FROM entry_anchors WHERE Is_Active = TRUE").df()
        print(f"📋 Found {len(anchors)} active anchors to evaluate.")

        for _, row in anchors.iterrows():
            leg_id = row['LegID']
            entry_ts = row['Entry_Snapshot_TS']
            
            # Find the first run_id and Ticker for this leg
            first_run_df = con.execute("""
                SELECT run_id, Underlying_Ticker, "Call/Put", Quantity, AssetType
                FROM clean_legs_v2 
                WHERE LegID = ? 
                ORDER BY Snapshot_TS ASC 
                LIMIT 1
            """, [leg_id]).df()
            
            if first_run_df.empty:
                # Try to extract ticker from LegID
                ticker_match = re.match(r"^([A-Z]+)", leg_id)
                ticker = ticker_match.group(1) if ticker_match else None
                if ticker:
                    first_run_df = con.execute("""
                        SELECT run_id, Underlying_Ticker, "Call/Put", Quantity, AssetType
                        FROM clean_legs_v2 
                        WHERE Underlying_Ticker = ? 
                        ORDER BY Snapshot_TS ASC 
                        LIMIT 1
                    """, [ticker]).df()

            if first_run_df.empty:
                print(f"⚠️  No historical snapshots found for {leg_id}. Skipping.")
                continue
                
            first_run_id = first_run_df['run_id'].iloc[0]
            ticker = first_run_df['Underlying_Ticker'].iloc[0]
            asset_type = first_run_df['AssetType'].iloc[0]
            qty = first_run_df['Quantity'].iloc[0]
            side = first_run_df['Call/Put'].iloc[0]
            
            # RAG: Ground Truth Classification
            # 1. Check for stock in the SAME run_id (regardless of TradeID)
            stock_present = False
            if ticker and ticker != 'None':
                # Check for stock in the same run_id
                stock_present = con.execute("""
                    SELECT COUNT(*) 
                    FROM clean_legs_v2 
                    WHERE run_id = ? AND Underlying_Ticker = ? AND AssetType = 'STOCK'
                """, [first_run_id, ticker]).fetchone()[0] > 0
                
                # Fallback: Check if stock exists in ANY run_id for this ticker
                # (If it was previously filtered out of some snapshots)
                if not stock_present:
                    stock_present = con.execute("""
                        SELECT COUNT(*) 
                        FROM clean_legs_v2 
                        WHERE Underlying_Ticker = ? AND AssetType = 'STOCK'
                    """, [ticker]).fetchone()[0] > 0
            
            new_structure = 'Unknown'
            
            if asset_type == 'OPTION':
                if side == 'Call' and qty < 0:
                    if stock_present:
                        new_structure = 'BUY_WRITE'
                    else:
                        new_structure = 'COVERED_CALL'
                elif side == 'Put' and qty < 0:
                    new_structure = 'CSP'
                else:
                    new_structure = 'LONG_OPTION'
            elif asset_type == 'STOCK':
                new_structure = 'STOCK'
            
            print(f"  Leg: {leg_id} | Ticker: {ticker} | Stock Present: {stock_present} -> {new_structure}")
            
            con.execute("""
                UPDATE entry_anchors 
                SET Entry_Structure = ? 
                WHERE LegID = ? AND Entry_Snapshot_TS = ?
            """, [new_structure, leg_id, entry_ts])

        print("\n✅ Backfill complete.")

if __name__ == "__main__":
    backfill()
