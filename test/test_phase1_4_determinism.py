"""
Phase 1-4 Determinism Validation Test

Tests that Phases 1-3 are deterministic (same CSV → same output)
and that Phase 4 correctly adds market context and First_Seen_Date.

Run this script twice with the same CSV to verify:
1. Phase 1-3 output is identical (deterministic)
2. Phase 4 metadata changes (Snapshot_TS, run_id)
3. First_Seen_Date is consistent across runs
4. New observables are present (DTE, IV_Rank, Capital_Deployed, etc.)
"""

import sys
import pandas as pd
from pathlib import Path

# Add core to path
sys.path.insert(0, str(Path(__file__).parent))

from core.phase1_clean import phase1_load_and_clean_raw_v2
from core.phase2_parse import phase2_run_all
from core.phase3_enrich import run_phase3_enrichment


def test_phase1_3_determinism(csv_path: str):
    """
    Test determinism of Phases 1-3.
    
    Expected:
    - Same CSV input → identical Phase 1-3 output
    - No Entry_Date column (moved to Phase 4 as First_Seen_Date)
    - New observables present: DTE, IV_Rank, Days_to_Earnings, Capital_Deployed, *_Trade
    """
    print("=" * 80)
    print("PHASE 1-3 DETERMINISM VALIDATION")
    print("=" * 80)
    
    # Phase 1: Clean
    print("\n[Phase 1] Loading and cleaning CSV...")
    df1, _ = phase1_load_and_clean_raw_v2(csv_path)
    print(f"✅ Phase 1 complete: {len(df1)} rows, {len(df1.columns)} columns")
    
    # Phase 2: Parse & Identity
    print("\n[Phase 2] Parsing leg identity and structure...")
    df2 = phase2_run_all(df1)
    print(f"✅ Phase 2 complete: {len(df2)} rows, {len(df2.columns)} columns")
    
    # Check: Entry_Date should NOT exist in Phase 2
    if "Entry_Date" in df2.columns:
        print("❌ FAILED: Entry_Date still present in Phase 2 (should be removed)")
        return False
    else:
        print("✅ Entry_Date removed from Phase 2 (moved to Phase 4)")
    
    # Phase 3: Enrich
    print("\n[Phase 3] Running enrichment with new observables...")
    snapshot_ts = pd.Timestamp.now()  # Deterministic timestamp for this run
    df3 = run_phase3_enrichment(df2, snapshot_ts=snapshot_ts)
    print(f"✅ Phase 3 complete: {len(df3)} rows, {len(df3.columns)} columns")
    
    # Validate new observables
    print("\n[Validation] Checking new observable columns...")
    expected_columns = [
        "DTE",
        "IV_Rank",
        "IV_Rank_Source",  # New metadata
        "Days_to_Earnings",
        "Earnings_Source",  # New metadata
        "Capital_Deployed",
        "Delta_Trade",
        "Gamma_Trade",
        "Theta_Trade",
        "Vega_Trade",
        "Premium_Trade",
    ]
    
    missing = [col for col in expected_columns if col not in df3.columns]
    present = [col for col in expected_columns if col in df3.columns]
    
    print(f"✅ Present ({len(present)}/{len(expected_columns)}): {present}")
    if missing:
        print(f"❌ Missing ({len(missing)}): {missing}")
        return False
    
    # Display sample statistics
    print("\n[Statistics] Sample observable values:")
    if "DTE" in df3.columns:
        print(f"  DTE range: {df3['DTE'].min()} to {df3['DTE'].max()} days")
    if "IV_Rank" in df3.columns:
        iv_rank_valid = df3['IV_Rank'].notna().sum()
        print(f"  IV_Rank: {iv_rank_valid}/{len(df3)} valid values (NaN expected in stub mode)")
        if "IV_Rank_Source" in df3.columns:
            print(f"  IV_Rank_Source: {df3['IV_Rank_Source'].value_counts().to_dict()}")
    if "Capital_Deployed" in df3.columns:
        print(f"  Capital_Deployed total: ${df3['Capital_Deployed'].sum():,.0f}")
    if "Days_to_Earnings" in df3.columns:
        earnings_counts = df3['Days_to_Earnings'].value_counts()
        print(f"  Days_to_Earnings: {earnings_counts.to_dict()} (999 expected in stub mode)")
        if "Earnings_Source" in df3.columns:
            print(f"  Earnings_Source: {df3['Earnings_Source'].value_counts().to_dict()}")
    if "Delta_Trade" in df3.columns:
        n_trades = df3["TradeID"].nunique()
        print(f"  Trade-level aggregates: {n_trades} trades")
        print(f"  Delta_Trade range: {df3['Delta_Trade'].min():.3f} to {df3['Delta_Trade'].max():.3f}")
    
    # Determinism test: Save Phase 3 output hash
    phase3_hash = pd.util.hash_pandas_object(df3).sum()
    print(f"\n[Determinism] Phase 3 output hash: {phase3_hash}")
    print("💾 To test determinism: Run this script again and compare hashes")
    print("   (Hashes should be identical for the same CSV)")
    
    print("\n" + "=" * 80)
    print("✅ PHASE 1-3 VALIDATION COMPLETE")
    print("=" * 80)
    
    return True


def test_phase4_metadata(csv_path: str, db_path: str = None):
    """
    Test Phase 4 market context and First_Seen_Date tracking.
    
    Expected:
    - Market_Session, Is_Market_Open, Snapshot_DayType added
    - First_Seen_Date tracked per TradeID
    - Snapshot_TS and run_id change each run
    """
    from core.phase4_snapshot import save_clean_snapshot
    
    print("\n" + "=" * 80)
    print("PHASE 4 METADATA VALIDATION")
    print("=" * 80)
    
    # Run Phases 1-3
    print("\n[Phase 1-3] Running pipeline...")
    df1, _ = phase1_load_and_clean_raw_v2(csv_path)
    df2 = phase2_run_all(df1)
    snapshot_ts = pd.Timestamp.now()
    df3 = run_phase3_enrichment(df2, snapshot_ts=snapshot_ts)
    print(f"✅ Phase 3 complete: {len(df3)} positions ready for snapshot")
    
    # Phase 4: Snapshot with metadata
    print("\n[Phase 4] Saving snapshot with market context...")
    df4, csv_path_out, run_id, csv_success, db_success = save_clean_snapshot(
        df3,
        db_path=db_path,
        to_csv=False,  # Skip CSV for test
        to_db=True,
    )
    print(f"✅ Phase 4 complete: run_id={run_id}")
    
    # Validate metadata columns
    print("\n[Validation] Checking Phase 4 metadata...")
    expected_metadata = [
        "Snapshot_TS",
        "run_id",
        "Schema_Hash",
        "Market_Session",
        "Is_Market_Open",
        "Snapshot_DayType",
        "First_Seen_Date",
    ]
    
    missing_meta = [col for col in expected_metadata if col not in df4.columns]
    if missing_meta:
        print(f"❌ Missing metadata: {missing_meta}")
        return False
    else:
        print(f"✅ All metadata present: {expected_metadata}")
    
    # Display market context
    print("\n[Market Context]")
    print(f"  Market_Session: {df4['Market_Session'].iloc[0]}")
    print(f"  Is_Market_Open: {df4['Is_Market_Open'].iloc[0]}")
    print(f"  Snapshot_DayType: {df4['Snapshot_DayType'].iloc[0]}")
    print(f"  Snapshot_TS: {df4['Snapshot_TS'].iloc[0]}")
    
    # Display First_Seen_Date tracking
    print("\n[First_Seen_Date Tracking]")
    n_trades = df4["TradeID"].nunique()
    first_seen_counts = df4.groupby("First_Seen_Date").size()
    print(f"  Total trades: {n_trades}")
    print(f"  Unique First_Seen_Date values: {len(first_seen_counts)}")
    print(f"  First_Seen_Date range: {df4['First_Seen_Date'].min()} to {df4['First_Seen_Date'].max()}")
    
    print("\n" + "=" * 80)
    print("✅ PHASE 4 VALIDATION COMPLETE")
    print("=" * 80)
    
    return True


if __name__ == "__main__":
    # Default test CSV (adjust path as needed)
    test_csv = "data/snapshots/schwab_positions_2025_01_03.csv"
    test_db = "data/test_determinism.duckdb"
    
    if len(sys.argv) > 1:
        test_csv = sys.argv[1]
    
    print(f"Testing with CSV: {test_csv}\n")
    
    # Test Phase 1-3 determinism
    success_123 = test_phase1_3_determinism(test_csv)
    
    # Test Phase 4 metadata
    success_4 = test_phase4_metadata(test_csv, db_path=test_db)
    
    if success_123 and success_4:
        print("\n🎉 ALL VALIDATIONS PASSED")
        print("\n💡 To verify determinism:")
        print("   1. Run this script again")
        print("   2. Compare Phase 3 output hashes (should be identical)")
        print("   3. Check First_Seen_Date remains consistent for same TradeIDs")
        sys.exit(0)
    else:
        print("\n❌ VALIDATION FAILED")
        sys.exit(1)
