"""
Status Distribution Audit

Validates Phase 1 fixes impact by analyzing:
1. Status distribution (Explored_* vs hard failures)
2. LEAP presence and annotation quality
3. Liquidity grade distribution
4. Candidate contract preservation

Run AFTER enabling chain cache:
    export DEBUG_CACHE_CHAINS=1
    python run_pipeline.py  # First run - builds cache
    python audit_status_distribution.py  # Analyze results
"""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime
import sys


def load_latest_output():
    """Load most recent Step 9B output"""
    output_dir = Path('output')
    
    # Look for Step 9B output files
    patterns = [
        'step9b_*.csv',
        '*_step9b.csv',
        'option_contracts_*.csv'
    ]
    
    all_files = []
    for pattern in patterns:
        all_files.extend(output_dir.glob(pattern))
    
    if not all_files:
        print("❌ No Step 9B output files found in output/")
        print("   Run pipeline first: python run_pipeline.py")
        return None
    
    # Get most recent file
    latest = max(all_files, key=lambda p: p.stat().st_mtime)
    print(f"📂 Loading: {latest}")
    
    df = pd.read_csv(latest)
    print(f"📊 Loaded {len(df)} strategies")
    
    return df


def audit_status_distribution(df):
    """Analyze Contract_Selection_Status distribution"""
    print("\n" + "="*70)
    print("STATUS DISTRIBUTION ANALYSIS")
    print("="*70)
    
    if 'Contract_Selection_Status' not in df.columns:
        print("⚠️  Contract_Selection_Status column not found")
        return
    
    status_counts = df['Contract_Selection_Status'].value_counts()
    total = len(df)
    
    print(f"\n📊 Status Breakdown (Total: {total} strategies):\n")
    
    # Categorize statuses
    exploratory = []
    failures = []
    success = []
    
    for status, count in status_counts.items():
        pct = (count / total) * 100
        
        # Categorize
        if status.startswith('Explored_'):
            exploratory.append((status, count, pct))
            emoji = "🔍"
        elif status in ['Selected', 'Executable', 'Ready']:
            success.append((status, count, pct))
            emoji = "✅"
        else:
            failures.append((status, count, pct))
            emoji = "❌"
        
        print(f"{emoji} {status:30s} {count:4d} ({pct:5.1f}%)")
    
    # Summary
    print(f"\n📈 Category Summary:")
    print(f"  Exploratory (Explored_*): {sum(c for _, c, _ in exploratory):4d} ({sum(p for _, _, p in exploratory):5.1f}%)")
    print(f"  Success:                  {sum(c for _, c, _ in success):4d} ({sum(p for _, _, p in success):5.1f}%)")
    print(f"  Hard Failures:            {sum(c for _, c, _ in failures):4d} ({sum(p for _, _, p in failures):5.1f}%)")
    
    # Validation
    exploratory_pct = sum(p for _, _, p in exploratory)
    failure_pct = sum(p for _, _, p in failures)
    
    print(f"\n🎯 Phase 1 Fix Validation:")
    if exploratory_pct > failure_pct:
        print(f"  ✅ PASS: More exploratory ({exploratory_pct:.1f}%) than failures ({failure_pct:.1f}%)")
        print(f"  Strategies preserved with context, not rejected blindly")
    else:
        print(f"  ⚠️  CONCERN: More failures ({failure_pct:.1f}%) than exploratory ({exploratory_pct:.1f}%)")
        print(f"  Phase 1 fixes may not be fully applied")


def audit_leap_presence(df):
    """Validate LEAP strategies are present and annotated"""
    print("\n" + "="*70)
    print("LEAP PRESENCE AUDIT")
    print("="*70)
    
    # Check for LEAP column
    if 'Recommended_Strategy' in df.columns:
        leap_strategies = df[df['Recommended_Strategy'].str.contains('LEAP', case=False, na=False)]
    elif 'Strategy_Name' in df.columns:
        leap_strategies = df[df['Strategy_Name'].str.contains('LEAP', case=False, na=False)]
    else:
        print("⚠️  No strategy name column found")
        return
    
    leap_count = len(leap_strategies)
    total = len(df)
    
    print(f"\n📊 LEAP Statistics:")
    print(f"  Total strategies: {total}")
    print(f"  LEAP strategies:  {leap_count} ({(leap_count/total*100):.1f}%)")
    
    if leap_count == 0:
        print("\n❌ FAIL: No LEAP strategies found")
        print("   Expected: 2+ LEAPs (from LEAP validators in Step 7)")
        return
    
    print(f"\n✅ PASS: LEAPs present in output")
    
    # Check LEAP annotations
    print(f"\n🔍 LEAP Annotation Quality:")
    
    if 'Actual_DTE' in leap_strategies.columns:
        dte_check = leap_strategies['Actual_DTE'] >= 365
        print(f"  DTE ≥ 365: {dte_check.sum()}/{leap_count} strategies")
    
    if 'Liquidity_Context' in leap_strategies.columns:
        has_context = leap_strategies['Liquidity_Context'].str.len() > 0
        print(f"  Has context: {has_context.sum()}/{leap_count} strategies")
    
    if 'Bid_Ask_Spread_Pct' in leap_strategies.columns:
        spreads = leap_strategies['Bid_Ask_Spread_Pct'].dropna()
        if len(spreads) > 0:
            print(f"  Avg spread: {spreads.mean():.1f}% (12-25% is normal for LEAPs)")
    
    # Show sample LEAP
    print(f"\n📋 Sample LEAP Strategy:")
    sample_cols = ['Ticker', 'Recommended_Strategy', 'Actual_DTE', 'Bid_Ask_Spread_Pct', 
                   'Contract_Selection_Status', 'Liquidity_Context']
    available_cols = [col for col in sample_cols if col in leap_strategies.columns]
    
    if available_cols:
        print(leap_strategies[available_cols].head(1).to_string(index=False))


def audit_candidate_preservation(df):
    """Check candidate contract preservation"""
    print("\n" + "="*70)
    print("CANDIDATE CONTRACT PRESERVATION AUDIT")
    print("="*70)
    
    if 'Candidate_Contracts' not in df.columns:
        print("⚠️  Candidate_Contracts column not found")
        return
    
    # Parse JSON candidates
    def parse_candidates(json_str):
        try:
            if pd.isna(json_str) or json_str == '[]':
                return []
            return json.loads(json_str)
        except:
            return []
    
    df['_candidates_parsed'] = df['Candidate_Contracts'].apply(parse_candidates)
    df['_num_candidates'] = df['_candidates_parsed'].apply(len)
    
    has_candidates = df['_num_candidates'] > 0
    candidate_count = has_candidates.sum()
    total = len(df)
    
    print(f"\n📊 Candidate Statistics:")
    print(f"  Strategies with candidates: {candidate_count}/{total} ({(candidate_count/total*100):.1f}%)")
    
    if candidate_count == 0:
        print("\n⚠️  No candidates preserved")
        print("   This is OK if all strategies found ideal contracts")
        return
    
    # Analyze candidate quality
    avg_candidates = df.loc[has_candidates, '_num_candidates'].mean()
    print(f"  Avg candidates per strategy: {avg_candidates:.1f}")
    
    # Show sample candidate
    print(f"\n📋 Sample Candidate Contract:")
    sample_row = df[has_candidates].iloc[0]
    candidates = sample_row['_candidates_parsed']
    
    if candidates:
        candidate = candidates[0]
        print(f"  Ticker: {sample_row.get('Ticker', 'N/A')}")
        print(f"  Strike: ${candidate.get('strike', 'N/A')}")
        print(f"  Type: {candidate.get('option_type', 'N/A')}")
        print(f"  Spread: {candidate.get('spread_pct', 'N/A')}%")
        print(f"  OI: {candidate.get('open_interest', 'N/A')}")
        print(f"  Reason: {candidate.get('reason', 'N/A')}")
    
    print(f"\n✅ PASS: Candidates preserved for downstream PCS evaluation")


def audit_liquidity_grades(df):
    """Analyze liquidity grade distribution"""
    print("\n" + "="*70)
    print("LIQUIDITY GRADE DISTRIBUTION")
    print("="*70)
    
    if 'Liquidity_Grade' not in df.columns and 'liquidity_class' not in df.columns:
        print("⚠️  No liquidity grade column found")
        return
    
    grade_col = 'Liquidity_Grade' if 'Liquidity_Grade' in df.columns else 'liquidity_class'
    grades = df[grade_col].value_counts()
    total = len(df)
    
    print(f"\n📊 Liquidity Distribution:")
    for grade, count in grades.items():
        pct = (count / total) * 100
        print(f"  {grade:20s} {count:4d} ({pct:5.1f}%)")
    
    # Validate diversity
    if len(grades) >= 3:
        print(f"\n✅ PASS: Diverse liquidity grades (not binary pass/fail)")
    else:
        print(f"\n⚠️  Limited liquidity diversity")


def audit_output_preservation(df):
    """Validate 180-240/266 expectation from Phase 1 fixes"""
    print("\n" + "="*70)
    print("OUTPUT PRESERVATION AUDIT")
    print("="*70)
    
    total = len(df)
    
    # Count rows with meaningful data
    has_data = pd.Series(False, index=df.index)
    
    checks = [
        ('Actual_DTE', lambda x: x > 0),
        ('Bid_Ask_Spread_Pct', lambda x: x > 0),
        ('Candidate_Contracts', lambda x: x != '[]'),
        ('Liquidity_Context', lambda x: len(str(x)) > 0)
    ]
    
    for col, check_fn in checks:
        if col in df.columns:
            has_data |= df[col].apply(check_fn)
    
    preserved_count = has_data.sum()
    preserved_pct = (preserved_count / total) * 100
    
    print(f"\n📊 Preservation Statistics:")
    print(f"  Total strategies: {total}")
    print(f"  With data: {preserved_count} ({preserved_pct:.1f}%)")
    print(f"  Blank/rejected: {total - preserved_count} ({100-preserved_pct:.1f}%)")
    
    # Validate against expectations
    print(f"\n🎯 Phase 1 Fix Expectations:")
    print(f"  Before: 58/266 (21.8%) - too many hard rejections")
    print(f"  Target: 180-240/266 (67-90%) - preserve with annotations")
    print(f"  Actual: {preserved_count}/{total} ({preserved_pct:.1f}%)")
    
    if preserved_pct >= 67:
        print(f"\n✅ PASS: Output preservation meets target")
        print(f"  Phase 1 fixes working as designed")
    elif preserved_pct >= 50:
        print(f"\n⚠️  PARTIAL: Better than before, but below target")
        print(f"  Some strategies still being rejected prematurely")
    else:
        print(f"\n❌ FAIL: Output preservation below expectations")
        print(f"  Phase 1 fixes may not be fully effective")


if __name__ == '__main__':
    print("\n" + "="*70)
    print("STEP 9B STATUS DISTRIBUTION AUDIT")
    print("="*70)
    print(f"\nPurpose: Validate Phase 1 fixes impact")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Load data
    df = load_latest_output()
    if df is None:
        sys.exit(1)
    
    # Run audits
    audit_status_distribution(df)
    audit_leap_presence(df)
    audit_candidate_preservation(df)
    audit_liquidity_grades(df)
    audit_output_preservation(df)
    
    print("\n" + "="*70)
    print("AUDIT COMPLETE")
    print("="*70)
    print("\nNext Steps:")
    print("  1. If status distribution looks good → proceed to PCS (Step 10)")
    print("  2. If LEAPs missing → check Step 7 validators")
    print("  3. If candidates not preserved → check Step 9B extraction logic")
    print("  4. If output < 67% → review Phase 1 fix deployment")
