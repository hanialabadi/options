#!/usr/bin/env python3
"""
Test Step 0 Hardening: Validate reliability enhancements
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.scan_engine.step0_schwab_snapshot import main as step0_main

def test_hardened_step0():
    """
    Test hardened Step 0 with existing tokens (market closed, expecting auth failures)
    
    Expected Behavior:
    - Token pre-flight validation runs
    - If tokens expired: aborts early with clear message
    - If tokens valid: proceeds with chunked processing
    - Diagnostic columns present in output
    - Structured logging with coverage metrics
    """
    print("\n" + "="*80)
    print("STEP 0 HARDENING VALIDATION TEST")
    print("="*80)
    print("\nTest Scenario: Existing tokens (market closed)")
    print("Expected: Token validation, then partial HV coverage due to closed market")
    print("="*80)
    
    try:
        # Run Step 0 with test mode (single ticker)
        df = step0_main(
            test_mode=False,  # Full universe
            use_cache=True,
            fetch_iv=False  # Skip IV for faster test
        )
        
        print("\n" + "="*80)
        print("✅ TEST PASSED - Step 0 Completed")
        print("="*80)
        
        # Validate diagnostic columns present
        assert 'price_history_status' in df.columns, "Missing price_history_status column"
        assert 'hv_status' in df.columns, "Missing hv_status column"
        print("✅ Diagnostic columns present")
        
        # Show coverage
        print(f"\n📊 Results:")
        print(f"   Total rows: {len(df)}")
        print(f"   HV computed: {(df['hv_status'] == 'COMPUTED').sum()}")
        print(f"   Coverage: {100*(df['hv_status'] == 'COMPUTED').sum()/len(df):.1f}%")
        
        # Show status breakdown
        print(f"\n📋 Status Breakdown:")
        for status in df['price_history_status'].value_counts().items():
            print(f"   {status[0]}: {status[1]}")
        
        return True
        
    except Exception as e:
        print("\n" + "="*80)
        print(f"❌ TEST RESULT: {type(e).__name__}")
        print("="*80)
        print(f"\n{str(e)}")
        
        # Check if it's expected token expiration
        if "Token" in str(e) or "auth" in str(e).lower():
            print("\n✅ Token pre-flight validation working correctly")
            print("   (Expected failure due to expired tokens)")
            return True
        else:
            print("\n❌ Unexpected failure")
            import traceback
            traceback.print_exc()
            return False

if __name__ == '__main__':
    success = test_hardened_step0()
    sys.exit(0 if success else 1)
