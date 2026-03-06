import argparse
import duckdb
import pandas as pd
from pathlib import Path
import os
from core.shared.data_contracts.config import SENSORS_DB_PATH

PROJECT_ROOT = Path(__file__).parent.parent.parent
PIPELINE_DB = PROJECT_ROOT / "data" / "pipeline.duckdb"
SENSOR_DB = SENSORS_DB_PATH

def inspect_symbol(symbol: str):
    print(f"🔍 Inspecting Symbol: {symbol}")
    
    if not os.path.exists(PIPELINE_DB):
        print(f"❌ Pipeline DB not found at {PIPELINE_DB}")
        return
    
    # 1. Get Cycle 1 Anchor from Canonical View
    with duckdb.connect(str(PIPELINE_DB)) as con:
        anchor_query = """
            SELECT TradeID, LegID, Symbol, Underlying_Price_Entry, Strike_Entry, Expiration_Entry, Snapshot_TS
            FROM canonical_anchors
            WHERE Symbol = ?
        """
        anchor_df = con.execute(anchor_query, [symbol]).df()
    
    if anchor_df.empty:
        print(f"❌ No Cycle 1 anchor found for {symbol}")
        return
    
    print("\n--- Cycle 1 Anchor (Fidelity) ---")
    print(anchor_df.to_string(index=False))
    
    # 2. Get Latest Cycle 2 Sensor Reading
    if not os.path.exists(SENSOR_DB):
        print(f"\n❌ Sensor DB not found at {SENSOR_DB}")
        return
        
    with duckdb.connect(str(SENSOR_DB)) as con:
        sensor_query = """
            SELECT LegID, Sensor_TS, UL_Last, Opt_Last, IV, Delta, Gamma, Vega, Theta
            FROM sensor_readings
            WHERE LegID IN (SELECT LegID FROM anchor_df)
            QUALIFY ROW_NUMBER() OVER (PARTITION BY LegID ORDER BY Sensor_TS DESC) = 1
        """
        # We need to pass the LegIDs from anchor_df
        leg_ids = anchor_df['LegID'].tolist()
        placeholders = ', '.join(['?' for _ in leg_ids])
        sensor_df = con.execute(f"""
            SELECT LegID, Sensor_TS, UL_Last, Opt_Last, IV, Delta, Gamma, Vega, Theta
            FROM sensor_readings
            WHERE LegID IN ({placeholders})
            QUALIFY ROW_NUMBER() OVER (PARTITION BY LegID ORDER BY Sensor_TS DESC) = 1
        """, leg_ids).df()
        
    if sensor_df.empty:
        print(f"\n❌ No Cycle 2 sensor readings found for {symbol}")
        return
        
    print("\n--- Cycle 2 Latest Sensor (Schwab) ---")
    print(sensor_df.to_string(index=False))
    
    # 3. Compute Drift Comparison
    comparison = anchor_df.merge(sensor_df, on='LegID')
    comparison['UL_Drift'] = comparison['UL_Last'] - comparison['Underlying_Price_Entry']
    
    print("\n--- Drift Comparison (Entry vs Schwab) ---")
    for _, row in comparison.iterrows():
        print(f"LegID: {row['LegID']}")
        print(f"  Entry UL: {row['Underlying_Price_Entry']:.2f}")
        print(f"  Sensor UL: {row['UL_Last']:.2f}")
        print(f"  UL Drift: {row['UL_Drift']:.2f}")
        print(f"  Sensor IV: {row['IV']:.4f}")
        print(f"  Sensor Delta: {row['Delta']:.4f}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inspect Sensor vs Anchor")
    parser.add_argument("--symbol", required=True, help="OCC Symbol to inspect")
    args = parser.parse_args()
    inspect_symbol(args.symbol)
