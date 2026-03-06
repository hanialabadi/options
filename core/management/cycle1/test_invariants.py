import pandas as pd
import numpy as np
from pathlib import Path
import io
import os
from core.management.cycle1.ingest.clean import phase1_load_and_clean_positions
from core.management.cycle1.identity.parse import phase2_run_all
from core.management.cycle1.snapshot.snapshot import save_clean_snapshot

def test_cycle1_determinism():
    """
    HARD INVARIANT TEST: Determinism & Broker Faithfulness
    
    1. Runs Cycle 1 twice on the same CSV.
    2. Asserts byte-for-byte identical output (excluding run_id and system metadata).
    3. Fails if Snapshot_TS is not broker-derived.
    4. Fails if symbols are mutated.
    """
    # 1. Create Mock Fidelity CSV (Broker Truth)
    # Must include all columns in CYCLE1_WHITELIST
    headers = "Account,Symbol,Quantity,Basis,Last,Bid,Ask,UL Last,Time Val,$ Total G/L,% Total G/L,Earnings Date,Theta,Vega,Gamma,Delta,Rho,Strike,Call/Put,Expiration,Open Int,Volume,% of Acct,As of Date/Time,Type"
    row1 = "Individual,AAPL  260116C240,10,150.0,25.0,24.0,26.0,235.0,2.5,100.0,5.0%,01/20/2026,-0.02,0.05,0.01,0.6,0.01,240.0,Call,01/16/2026,1000,500,1.0%,01/15/2026 12:16:28 PM,Margin"
    # row2 must have exactly 25 columns. MSFT is a stock, so Strike, Call/Put, Expiration are empty.
    row2 = "Individual,MSFT,100,300.0,400.0,399.0,401.0,400.0,0.0,10000.0,33.3%,01/25/2026,0.0,0.0,0.0,0.0,0.0,,,,0,0,10.0%,01/15/2026 12:16:28 PM,Cash"
    csv_content = f"{headers}\n{row1}\n{row2}"
    # Fidelity has 2 header rows to skip
    full_csv = "Fidelity Positions\nGenerated at...\n" + csv_content
    
    mock_path = Path("test_fidelity_mock.csv")
    mock_path.write_text(full_csv)
    
    try:
        # Run 1
        df1, _ = phase1_load_and_clean_positions(mock_path, save_snapshot=False)
        df1 = phase2_run_all(df1)
        
        # Run 2
        df2, _ = phase1_load_and_clean_positions(mock_path, save_snapshot=False)
        df2 = phase2_run_all(df2)
        
        # --- INVARIANT 1: Determinism ---
        # Exclude run_id and system-derived metadata that might vary by milliseconds
        cols_to_compare = [c for c in df1.columns if c not in ['run_id', 'Snapshot_TS']]
        pd.testing.assert_frame_equal(df1[cols_to_compare], df2[cols_to_compare])
        print("✅ INVARIANT: Determinism verified.")
        
        # --- INVARIANT 2: Temporal Authority ---
        expected_ts = pd.to_datetime("01/15/2026 12:16:28 PM")
        if not (df1['Snapshot_TS'] == expected_ts).all():
            raise AssertionError(f"❌ TEMPORAL VIOLATION: Snapshot_TS ({df1['Snapshot_TS'].iloc[0]}) != Broker Truth ({expected_ts})")
        print("✅ INVARIANT: Temporal Authority verified.")
        
        # --- INVARIANT 3: Symbol Integrity ---
        # Fidelity symbol has 2 spaces: "AAPL  260116C240"
        original_symbol = "AAPL  260116C240"
        if df1.loc[df1['AssetType'] == 'OPTION', 'Symbol'].iloc[0] != original_symbol:
            actual = df1.loc[df1['AssetType'] == 'OPTION', 'Symbol'].iloc[0]
            raise AssertionError(f"❌ IDENTITY VIOLATION: Symbol mutated. Expected '{original_symbol}', got '{actual}'")
        print("✅ INVARIANT: Symbol Integrity verified.")
        
        # --- INVARIANT 4: No Enrichment ---
        forbidden_cols = ['Is_Optionable', 'HV_20D', 'IV_Rank']
        found_forbidden = [c for c in forbidden_cols if c in df1.columns]
        if found_forbidden:
            raise AssertionError(f"❌ BOUNDARY VIOLATION: Cycle 1 contains enriched columns: {found_forbidden}")
        print("✅ INVARIANT: Boundary Integrity verified.")

    finally:
        if mock_path.exists():
            os.remove(mock_path)

if __name__ == "__main__":
    try:
        test_cycle1_determinism()
        print("\n🏆 ALL CYCLE 1 INVARIANTS PASSED.")
    except Exception as e:
        print(f"\n🔥 INVARIANT TEST FAILED: {e}")
        exit(1)
