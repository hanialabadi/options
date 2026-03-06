import duckdb
import pandas as pd
import numpy as np
import argparse
import os
import sys
from datetime import datetime

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from core.management.cycle2.providers.schwab_iv_provider import fetch_iv_snapshot

# Ensure full visibility in terminal
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', None)

DB_PATH = "data/pipeline.duckdb"

def inspect_drift(symbol, limit=None):
    if not os.path.exists(DB_PATH):
        print(f"ERROR: Database not found at {DB_PATH}")
        return

    print(f"--- Cycle 2 Audit: {symbol} ---")
    print(f"Source: {DB_PATH} (Read-Only)")
    
    try:
        con = duckdb.connect(DB_PATH, read_only=True)
        
        # 1. Load all snapshots for the symbol, ordered by time
        # RAG: Identity Hygiene. Use canonical_anchors for entry and clean_legs for history.
        query = f"SELECT * FROM clean_legs WHERE Symbol = ? AND LegID IS NOT NULL ORDER BY Snapshot_TS ASC"
        df = con.execute(query, [symbol]).df()
        
        if df.empty:
            print(f"No data found for symbol: {symbol}")
            con.close()
            return

        # 2. Identify Entry Anchor (RAG-Correct: Use Canonical Anchors View)
        anchor_query = "SELECT * FROM canonical_anchors WHERE Symbol = ?"
        anchor_df = con.execute(anchor_query, [symbol]).df()
        
        if anchor_df.empty:
            print(f"No canonical anchor found for symbol: {symbol}")
            con.close()
            return
            
        entry_row = anchor_df.iloc[0]
        entry_ts = pd.to_datetime(entry_row['Snapshot_TS'])
        entry_ul = entry_row['UL Last']
        entry_last = entry_row['Last']
        entry_delta = entry_row['Delta']
        entry_vega = entry_row['Vega']
        entry_theta = entry_row['Theta']
        # Cycle-2: Use frozen IV_Entry anchor
        entry_iv = entry_row['IV_Entry'] if 'IV_Entry' in entry_row else np.nan
        quantity = entry_row['Quantity']

        print(f"Entry Anchor Detected: {entry_ts}")
        print(f"Entry UL: {entry_ul:.2f} | Entry Last: {entry_last:.4f}")
        print(f"Entry Greeks: Δ={entry_delta:.4f}, ν={entry_vega:.4f}, θ={entry_theta:.4f}")
        print("-" * 80)

        # 3. Compute Drift & Attribution (Transient Only)
        df['Snapshot_TS'] = pd.to_datetime(df['Snapshot_TS'])
        df['Days_Elapsed'] = (df['Snapshot_TS'] - entry_ts).dt.total_seconds() / 86400.0
        
        # Drift Components
        df['Price_Delta'] = df['UL Last'] - entry_ul
        
        # Cycle 2: Fetch current IV from Schwab for the LATEST snapshot only
        # (Since we don't persist IV, we can only audit the current state's Vega PnL)
        print("Fetching current IV from Schwab...")
        iv_map = fetch_iv_snapshot([symbol], datetime.now())
        current_iv = iv_map.get(symbol)
        
        # PnL Attribution (Passarelli/Natenberg)
        # Formula: Greek_Entry * Change_in_Factor * Multiplier * Quantity
        multiplier = 100.0 # Standard options multiplier
        
        df['Delta_PnL'] = entry_delta * df['Price_Delta'] * multiplier * quantity
        df['Theta_PnL'] = entry_theta * df['Days_Elapsed'] * multiplier * quantity
        
        # Vega PnL: Only possible for the latest row where we just fetched IV
        df['Vega_PnL'] = np.nan
        df['IV_Now'] = np.nan
        if current_iv is not None and pd.notna(entry_iv):
            # Unit: Decimal Volatility (Canonical Standard)
            # current_iv is already decimal (e.g. 0.166)
            # entry_iv might be percent (e.g. 22.27)
            iv_entry_val = entry_iv / 100.0 if entry_iv > 2.0 else entry_iv
            
            # Vega is price change per 1% (0.01) change in IV
            iv_change_pct = (current_iv - iv_entry_val) * 100.0
            vega_pnl = entry_vega * iv_change_pct * multiplier * quantity
            df.loc[df.index[-1], 'Vega_PnL'] = vega_pnl
            df.loc[df.index[-1], 'IV_Now'] = current_iv
            
        # Total PnL relative to Entry Snapshot
        df['Total_PnL'] = (df['Last'] - entry_last) * multiplier * quantity
        
        # Attribution Quality
        df['Attr_Quality'] = "FULL"
        for idx in df.index:
            if pd.isna(df.at[idx, 'Vega_PnL']):
                df.at[idx, 'Attr_Quality'] = "PARTIAL_NO_IV"
            else:
                df.at[idx, 'Attr_Quality'] = "FULL"

        # 4. Smoothing (SMA3)
        df['SMA3_Total_PnL'] = df['Total_PnL'].rolling(window=3).mean()

        # 5. Handle NaNs for summary
        final_delta_pnl = df['Delta_PnL'].iloc[-1] if pd.notna(df['Delta_PnL'].iloc[-1]) else 0.0
        final_theta_pnl = df['Theta_PnL'].iloc[-1] if pd.notna(df['Theta_PnL'].iloc[-1]) else 0.0
        final_vega_pnl = df['Vega_PnL'].iloc[-1] if pd.notna(df['Vega_PnL'].iloc[-1]) else 0.0
        # 6. Format for Display
        display_cols = [
            'Snapshot_TS', 'UL Last', 'Last', 
            'Price_Delta', 'Delta_PnL', 'Theta_PnL', 'Vega_PnL', 'IV_Now',
            'Total_PnL', 'Attr_Quality'
        ]
        
        # Rename for audit clarity
        rename_map = {
            'UL Last': 'UL_Last',
            'Price_Delta': 'Price_Δ',
            'Delta_PnL': 'Delta_PnL',
            'Theta_PnL': 'Theta_PnL',
            'Vega_PnL': 'Vega_PnL',
            'Total_PnL': 'Total_PnL',
            'SMA3_Total_PnL': 'SMA3_PnL',
            'Attr_Quality': 'Quality'
        }
        
        output_df = df[display_cols].rename(columns=rename_map)
        
        if limit:
            output_df = output_df.tail(limit)
            
        print(output_df.to_string(index=False))
        
        print("-" * 80)
        print("Attribution Reconciliation (Latest Snapshot):")
        total_pnl = df['Total_PnL'].iloc[-1]
        unexplained = total_pnl - (final_delta_pnl + final_theta_pnl + final_vega_pnl)
        
        print(f"  Total_PnL:       {total_pnl:10.2f}")
        print(f"  Delta_Explained: {final_delta_pnl:10.2f}")
        print(f"  Theta_Explained: {final_theta_pnl:10.2f}")
        print(f"  Vega_Explained:  {final_vega_pnl:10.2f}")
        print(f"  PnL_Unexplained: {unexplained:10.2f}")
        print("-" * 80)
        print(f"Audit Summary: {len(df)} snapshots inspected. Cycle 2 Measurement Locked.")
        
        con.close()
    except Exception as e:
        print(f"ERROR during inspection: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cycle 2 Drift Inspector (Audit-Grade)")
    parser.add_argument("--symbol", required=True, help="OCC Symbol to inspect")
    parser.add_argument("--limit", type=int, help="Limit output to last N snapshots")
    
    args = parser.parse_args()
    inspect_drift(args.symbol, args.limit)
