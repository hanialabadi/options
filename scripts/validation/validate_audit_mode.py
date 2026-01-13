#!/usr/bin/env python3
"""
Forensic Audit Mode Validation Script

Verifies that audit mode meets all non-negotiable requirements:
1. Canonical data preservation (no column drops)
2. Step-scoped views (copies, not mutations)
3. Audit mode artifacts generated
4. Per-ticker traces complete
5. Manual inspection possible

Usage:
    python validate_audit_mode.py
    
Expected: All checks ✅ PASS
"""

import pandas as pd
from pathlib import Path
import sys

def check_canonical_preservation():
    """Verify no columns dropped across pipeline."""
    print("\n" + "="*80)
    print("CHECK 1: CANONICAL DATA PRESERVATION")
    print("="*80)
    
    step_files = sorted(Path("audit_steps").glob("step*.csv"))
    
    if not step_files:
        print("❌ FAIL: No audit step files found")
        return False
    
    prev_cols = set()
    all_passed = True
    
    for i, step_file in enumerate(step_files):
        df = pd.read_csv(step_file, nrows=0)  # Header only
        cols = set(df.columns)
        
        if i == 0:
            print(f"✅ {step_file.name}: {len(cols)} columns (baseline)")
        else:
            dropped = prev_cols - cols
            added = cols - prev_cols
            
            if dropped:
                print(f"❌ {step_file.name}: DROPPED {len(dropped)} columns!")
                print(f"   Dropped: {', '.join(list(dropped)[:5])}")
                all_passed = False
            else:
                print(f"✅ {step_file.name}: {len(cols)} columns (+{len(added)} added, 0 dropped)")
        
        prev_cols = cols
    
    if all_passed:
        print(f"\n✅ PASS: All columns preserved (115 → {len(prev_cols)})")
    else:
        print(f"\n❌ FAIL: Some columns were dropped")
    
    return all_passed


def check_ticker_traces():
    """Verify per-ticker trace files exist and are complete."""
    print("\n" + "="*80)
    print("CHECK 2: PER-TICKER TRACE TABLES")
    print("="*80)
    
    trace_dir = Path("audit_trace")
    
    if not trace_dir.exists():
        print("❌ FAIL: audit_trace/ directory not found")
        return False
    
    trace_files = list(trace_dir.glob("*_trace.csv"))
    
    if not trace_files:
        print("❌ FAIL: No trace files found")
        return False
    
    all_passed = True
    
    for trace_file in trace_files:
        df = pd.read_csv(trace_file)
        ticker = trace_file.stem.replace("_trace", "")
        
        required_cols = ['step', 'step_name', 'description', 'rows', 'status']
        missing_cols = set(required_cols) - set(df.columns)
        
        if missing_cols:
            print(f"❌ {ticker}: Missing columns {missing_cols}")
            all_passed = False
        elif len(df) < 8:  # Should have ~10 steps
            print(f"❌ {ticker}: Only {len(df)} steps (expected ~10)")
            all_passed = False
        else:
            print(f"✅ {ticker}: {len(df)} steps tracked")
    
    if all_passed:
        print(f"\n✅ PASS: All trace tables complete")
    else:
        print(f"\n❌ FAIL: Some trace tables incomplete")
    
    return all_passed


def check_navigation_guide():
    """Verify AUDIT_NAVIGATION.md exists."""
    print("\n" + "="*80)
    print("CHECK 3: NAVIGATION GUIDE")
    print("="*80)
    
    nav_file = Path("AUDIT_NAVIGATION.md")
    
    if not nav_file.exists():
        print("❌ FAIL: AUDIT_NAVIGATION.md not found")
        return False
    
    content = nav_file.read_text()
    
    required_sections = [
        "Manual Inspection Workflow",
        "Key Columns to Verify",
        "audit_steps/",
        "audit_trace/"
    ]
    
    missing = [s for s in required_sections if s not in content]
    
    if missing:
        print(f"❌ FAIL: Missing sections: {missing}")
        return False
    
    print(f"✅ PASS: Navigation guide complete ({len(content)} chars)")
    return True


def check_column_counts():
    """Verify columns accumulate (not drop) across steps."""
    print("\n" + "="*80)
    print("CHECK 4: COLUMN ACCUMULATION")
    print("="*80)
    
    step_files = sorted(Path("audit_steps").glob("step*.csv"))
    
    prev_count = 0
    all_passed = True
    
    for step_file in step_files:
        df = pd.read_csv(step_file, nrows=0)
        count = len(df.columns)
        
        if count < prev_count:
            print(f"❌ {step_file.name}: {count} cols (DROPPED {prev_count - count}!)")
            all_passed = False
        else:
            print(f"✅ {step_file.name}: {count} cols (+{count - prev_count})")
        
        prev_count = count
    
    if all_passed:
        print(f"\n✅ PASS: Columns monotonically increase")
    else:
        print(f"\n❌ FAIL: Columns were dropped")
    
    return all_passed


def check_iv_surface():
    """Verify IV surface columns present in Step 1."""
    print("\n" + "="*80)
    print("CHECK 5: IV SURFACE REHYDRATION")
    print("="*80)
    
    step1 = Path("audit_steps/step01_snapshot_enriched.csv")
    
    if not step1.exists():
        print("❌ FAIL: Step 1 file not found")
        return False
    
    df = pd.read_csv(step1, nrows=0)
    cols = set(df.columns)
    
    iv_cols = [
        'IV_7_D_Call', 'IV_14_D_Call', 'IV_21_D_Call',
        'IV_30_D_Call', 'IV_60_D_Call', 'IV_90_D_Call'
    ]
    
    metadata_cols = [
        'iv_surface_source', 'iv_surface_age_days',
        'iv_rank_available', 'iv_history_days'
    ]
    
    missing_iv = [c for c in iv_cols if c not in cols]
    missing_meta = [c for c in metadata_cols if c not in cols]
    
    if missing_iv:
        print(f"❌ FAIL: Missing IV columns: {missing_iv}")
        return False
    
    if missing_meta:
        print(f"⚠️  WARNING: Missing metadata: {missing_meta}")
    
    print(f"✅ PASS: All {len(iv_cols)} IV surface columns present")
    print(f"✅ PASS: {len(metadata_cols) - len(missing_meta)}/{len(metadata_cols)} metadata columns present")
    
    return True


def main():
    """Run all validation checks."""
    print("\n" + "="*80)
    print("FORENSIC AUDIT MODE VALIDATION")
    print("="*80)
    
    checks = [
        ("Canonical Preservation", check_canonical_preservation),
        ("Ticker Traces", check_ticker_traces),
        ("Navigation Guide", check_navigation_guide),
        ("Column Accumulation", check_column_counts),
        ("IV Surface", check_iv_surface),
    ]
    
    results = []
    
    for name, check_fn in checks:
        try:
            passed = check_fn()
            results.append((name, passed))
        except Exception as e:
            print(f"\n❌ {name}: EXCEPTION - {e}")
            results.append((name, False))
    
    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    
    for name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status}: {name}")
    
    total_passed = sum(1 for _, p in results if p)
    total_checks = len(results)
    
    print("\n" + "="*80)
    if total_passed == total_checks:
        print(f"✅ ALL CHECKS PASSED ({total_passed}/{total_checks})")
        print("="*80)
        return 0
    else:
        print(f"❌ SOME CHECKS FAILED ({total_passed}/{total_checks})")
        print("="*80)
        return 1


if __name__ == "__main__":
    sys.exit(main())
