#!/usr/bin/env python3
"""
IV SURFACE AUDIT - Controlled 10-Ticker Analysis

Purpose: Track exactly where IV data appears/disappears through the pipeline.
Method: Dump CSV at EVERY step, inspect manually.

Rule: Never audit from logs alone. Always audit from DataFrames written to disk.
"""

import pandas as pd
import numpy as np
import sys
from pathlib import Path

sys.path.insert(0, '.')

from core.scan_engine.step2_load_snapshot import load_ivhv_snapshot
from core.scan_engine.step3_filter_ivhv import filter_ivhv_gap
from core.scan_engine.step5_chart_signals import compute_chart_signals
from core.scan_engine.step6_gem_filter import validate_data_quality
from core.scan_engine.step7_strategy_recommendation import recommend_strategies
from core.scan_engine.step9a_select_timeframes import select_timeframes
from core.scan_engine.step11_evaluation import evaluate_strategies
from core.scan_engine.step12_acceptance import compute_acceptance

# ============================================================================
# STEP 0: LOCK A SMALL, CONTROLLED UNIVERSE
# ============================================================================

DEBUG_TICKERS = [
    "AAPL", "MSFT", "TSLA", "NVDA", "GOOGL",
    "META", "AMZN", "AMD", "NFLX", "INTC"
]

print("="*80)
print("IV SURFACE AUDIT - CONTROLLED 10-TICKER ANALYSIS")
print("="*80)
print(f"\nFixed ticker set: {DEBUG_TICKERS}")
print(f"Audit dumps will be written to: audit/")

# Create audit directory
audit_dir = Path("audit")
audit_dir.mkdir(exist_ok=True)

# ============================================================================
# AUDIT HELPER
# ============================================================================

def audit_dump(df, step_name, focus_ticker="AAPL"):
    """
    Dump DataFrame to CSV and print key statistics.
    
    For each step, we want to know:
    - How many tickers survived
    - Which IV columns exist
    - Which IV columns have non-NaN values
    - AAPL-specific data (transposed for readability)
    """
    out_file = audit_dir / f"{step_name}.csv"
    df.to_csv(out_file, index=False)
    
    print(f"\n{'='*80}")
    print(f"[AUDIT] {step_name}")
    print(f"{'='*80}")
    print(f"File: {out_file}")
    print(f"Rows: {len(df)} tickers")
    print(f"Columns: {len(df.columns)}")
    
    # Find IV columns
    iv_cols = sorted([c for c in df.columns if 'IV_' in c and ('Call' in c or 'Put' in c)])
    if iv_cols:
        print(f"\nIV Columns ({len(iv_cols)}):")
        for col in iv_cols:
            non_null = df[col].notna().sum()
            pct = (non_null / len(df) * 100) if len(df) > 0 else 0
            status = "✅" if non_null > 0 else "❌"
            print(f"  {status} {col:25s}: {non_null:3d}/{len(df)} populated ({pct:5.1f}%)")
    
    # Find earnings columns
    earnings_cols = [c for c in df.columns if 'earning' in c.lower()]
    if earnings_cols:
        print(f"\nEarnings Columns: {earnings_cols}")
    
    # Find IV surface metadata
    metadata_cols = [c for c in df.columns if 'iv_surface' in c.lower()]
    if metadata_cols:
        print(f"\nIV Surface Metadata: {metadata_cols}")
        for col in metadata_cols:
            if col in df.columns:
                print(f"  {col}: {df[col].value_counts().to_dict()}")
    
    # Focus ticker detail
    if focus_ticker in df['Ticker'].values:
        print(f"\n{focus_ticker} DATA:")
        ticker_row = df[df['Ticker'] == focus_ticker].iloc[0]
        
        # Show all IV columns for this ticker
        if iv_cols:
            print(f"  IV Surface:")
            for col in iv_cols[:15]:  # First 15 IV columns
                val = ticker_row[col]
                status = "✅" if pd.notna(val) else "❌ NaN"
                print(f"    {col:25s}: {str(val):>10s}  {status}")
        
        # Show earnings data
        if earnings_cols:
            print(f"  Earnings Data:")
            for col in earnings_cols:
                print(f"    {col}: {ticker_row[col]}")
        
        # Show metadata
        if metadata_cols:
            print(f"  IV Surface Metadata:")
            for col in metadata_cols:
                print(f"    {col}: {ticker_row[col]}")
    
    print(f"{'='*80}")


# ============================================================================
# PIPELINE EXECUTION WITH AUDIT DUMPS
# ============================================================================

# Step 0: Raw Snapshot (already has IV columns with NaN)
print("\n\n🔄 Loading raw snapshot...")
df = pd.read_csv('data/snapshots/ivhv_snapshot_live_20260102_124337.csv')
df = df[df['Ticker'].isin(DEBUG_TICKERS)]
audit_dump(df, "step0_raw_snapshot")

# Step 2: Enrichment (earnings + regime + IV SURFACE REHYDRATION)
print("\n\n🔄 Step 2: Loading with enrichment (INCLUDING IV SURFACE)...")
df_step2 = load_ivhv_snapshot('data/snapshots/ivhv_snapshot_live_20260102_124337.csv')
df_step2 = df_step2[df_step2['Ticker'].isin(DEBUG_TICKERS)]
audit_dump(df_step2, "step2_enriched_snapshot")

# Step 3: IVHV Filter
print("\n\n🔄 Step 3: IVHV filter...")
df_step3 = filter_ivhv_gap(df_step2)
df_step3 = df_step3[df_step3['Ticker'].isin(DEBUG_TICKERS)]
audit_dump(df_step3, "step3_ivhv_filter")

# Step 5: Chart Signals
print("\n\n🔄 Step 5: Chart signals...")
try:
    df_step5 = compute_chart_signals(df_step3, use_cache=True)
    df_step5 = df_step5[df_step5['Ticker'].isin(DEBUG_TICKERS)]
    audit_dump(df_step5, "step5_chart_signals")
except Exception as e:
    print(f"⚠️  Step 5 failed: {e}")
    df_step5 = df_step3

# Step 6: Data Quality Validation
print("\n\n🔄 Step 6: Data quality validation...")
try:
    df_step6 = validate_data_quality(df_step5)
    df_step6 = df_step6[df_step6['Ticker'].isin(DEBUG_TICKERS)]
    audit_dump(df_step6, "step6_data_quality")
except Exception as e:
    print(f"⚠️  Step 6 failed: {e}")
    df_step6 = df_step5

# Step 7: Strategy Recommendation
print("\n\n🔄 Step 7: Strategy recommendation...")
try:
    df_step7 = recommend_strategies(df_step6)
    df_step7 = df_step7[df_step7['Ticker'].isin(DEBUG_TICKERS)]
    audit_dump(df_step7, "step7_strategies")
except Exception as e:
    print(f"⚠️  Step 7 failed: {e}")
    df_step7 = df_step6

# Step 9A: Timeframe Selection
print("\n\n🔄 Step 9A: Timeframe selection...")
try:
    df_step9a = select_timeframes(df_step7)
    df_step9a = df_step9a[df_step9a['Ticker'].isin(DEBUG_TICKERS)]
    audit_dump(df_step9a, "step9a_timeframes")
except Exception as e:
    print(f"⚠️  Step 9A failed: {e}")
    df_step9a = df_step7

# Step 9B: Contract Fetch (SKIPPED - requires live API)
print("\n\n⏭️  Step 9B: Skipped (requires live API)")

# Step 11: Evaluation (using Step 9A output as proxy)
print("\n\n🔄 Step 11: Evaluation...")
try:
    df_step11 = evaluate_strategies(df_step9a, df_step9a)  # Using 9A as both inputs
    df_step11 = df_step11[df_step11['Ticker'].isin(DEBUG_TICKERS)]
    audit_dump(df_step11, "step11_evaluated")
except Exception as e:
    print(f"⚠️  Step 11 failed: {e}")
    df_step11 = df_step9a

# Step 12: Acceptance (THE CRITICAL GATE)
print("\n\n🔄 Step 12: Acceptance gate...")
try:
    df_step12 = compute_acceptance(df_step11)
    df_step12 = df_step12[df_step12['Ticker'].isin(DEBUG_TICKERS)]
    audit_dump(df_step12, "step12_acceptance")
    
    # Final truth table
    print(f"\n{'='*80}")
    print("FINAL ACCEPTANCE TRUTH TABLE")
    print(f"{'='*80}")
    cols = ['Ticker', 'acceptance_status', 'acceptance_reason']
    cols = [c for c in cols if c in df_step12.columns]
    if cols:
        print(df_step12[cols].to_string(index=False))
    else:
        print("⚠️  Acceptance columns not found")
    
except Exception as e:
    print(f"⚠️  Step 12 failed: {e}")
    import traceback
    traceback.print_exc()

print(f"\n{'='*80}")
print("✅ AUDIT COMPLETE")
print(f"{'='*80}")
print(f"\nAll CSV files written to: {audit_dir.resolve()}")
print(f"\nKey files to manually inspect:")
print(f"  1. audit/step0_raw_snapshot.csv         - Baseline (IV should be NaN)")
print(f"  2. audit/step2_enriched_snapshot.csv    - After IV rehydration (IV should be populated)")
print(f"  3. audit/step3_ivhv_filter.csv          - After filtering (survivors)")
print(f"  4. audit/step12_acceptance.csv          - Final gate (rejection reasons)")
print(f"\nNext: Open these CSVs and verify:")
print(f"  - Does IV surface become non-NaN at Step 2?")
print(f"  - Are rejection reasons consistent with missing/stale data?")
print(f"  - Is anything silently defaulted or fabricated?")
