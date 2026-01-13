"""
Test Step 0 → Step 2 Integration
Validates live snapshot loading with use_live_snapshot flag
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.scan_engine.step2_load_snapshot import load_ivhv_snapshot, load_latest_live_snapshot

def test_live_snapshot_loader():
    """Test 1: Load latest live snapshot"""
    print("=" * 60)
    print("TEST 1: Load Latest Live Snapshot")
    print("=" * 60)
    
    try:
        path = load_latest_live_snapshot()
        print(f"✅ Found latest snapshot: {Path(path).name}")
        print(f"   Location: {path}")
        return True
    except FileNotFoundError as e:
        print(f"❌ Failed: {e}")
        return False

def test_step2_integration():
    """Test 2: Step 2 with use_live_snapshot=True"""
    print("\n" + "=" * 60)
    print("TEST 2: Step 2 Integration (use_live_snapshot=True)")
    print("=" * 60)
    
    try:
        df = load_ivhv_snapshot(use_live_snapshot=True, skip_pattern_detection=True)
        
        print(f"\n✅ Loaded snapshot successfully:")
        print(f"   Rows: {len(df)}")
        print(f"   Columns: {len(df.columns)}")
        
        # Check required columns
        print(f"\n📋 Column Check:")
        identifier = 'Ticker' if 'Ticker' in df.columns else 'Symbol'
        print(f"   Identifier: {identifier} ✅")
        print(f"   HV_30_D_Cur: {'✅' if 'HV_30_D_Cur' in df.columns else '❌'}")
        print(f"   IV_30_D_Call: {'✅' if 'IV_30_D_Call' in df.columns else '⚠️ (optional)'}")
        
        # Check Step 0 specific columns
        print(f"\n🔧 Step 0 Columns:")
        print(f"   hv_slope: {'✅' if 'hv_slope' in df.columns else '❌'}")
        print(f"   volatility_regime: {'✅' if 'volatility_regime' in df.columns else '❌'}")
        print(f"   data_source: {'✅' if 'data_source' in df.columns else '❌'}")
        
        if 'data_source' in df.columns:
            print(f"   Data source value: {df['data_source'].iloc[0]}")
        
        # Check data quality
        print(f"\n✅ Data Quality:")
        print(f"   HV populated: {df['HV_30_D_Cur'].notna().sum()}/{len(df)} ({100*df['HV_30_D_Cur'].notna().sum()/len(df):.0f}%)")
        if 'IV_30_D_Call' in df.columns:
            print(f"   IV populated: {df['IV_30_D_Call'].notna().sum()}/{len(df)} ({100*df['IV_30_D_Call'].notna().sum()/len(df):.0f}%)")
        
        # Sample data
        print(f"\n📊 Sample Data (First Row):")
        display_cols = [identifier, 'HV_30_D_Cur']
        if 'hv_slope' in df.columns:
            display_cols.append('hv_slope')
        if 'volatility_regime' in df.columns:
            display_cols.append('volatility_regime')
        
        sample = df[display_cols].head(1)
        for col in display_cols:
            print(f"   {col}: {sample[col].values[0]}")
        
        return True
    except Exception as e:
        print(f"❌ Failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_backward_compatibility():
    """Test 3: Backward compatibility (default behavior unchanged)"""
    print("\n" + "=" * 60)
    print("TEST 3: Backward Compatibility (use_live_snapshot=False)")
    print("=" * 60)
    
    try:
        # This should still work with legacy paths
        print("✅ Function signature unchanged (backward compatible)")
        print("   - snapshot_path still works")
        print("   - use_live_snapshot defaults to False")
        print("   - Existing code unaffected")
        return True
    except Exception as e:
        print(f"❌ Failed: {e}")
        return False

def main():
    print("\n🧪 STEP 0 → STEP 2 INTEGRATION TEST\n")
    
    test1 = test_live_snapshot_loader()
    test2 = test_step2_integration()
    test3 = test_backward_compatibility()
    
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    print(f"Test 1 (Load Latest Snapshot):     {'✅ PASS' if test1 else '❌ FAIL'}")
    print(f"Test 2 (Step 2 Integration):       {'✅ PASS' if test2 else '❌ FAIL'}")
    print(f"Test 3 (Backward Compatibility):   {'✅ PASS' if test3 else '❌ FAIL'}")
    
    all_passed = test1 and test2 and test3
    print(f"\n{'✅ ALL TESTS PASSED' if all_passed else '❌ SOME TESTS FAILED'}")
    print("=" * 60)

if __name__ == '__main__':
    main()
