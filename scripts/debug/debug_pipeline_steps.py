#!/usr/bin/env python3
"""
AUTHORITATIVE PIPELINE DEBUG SCRIPT

Purpose: Materialize EVERY step to CSV for manual audit.
Method: Fixed 10-ticker set, no logs, just DataFrames.

Run: venv/bin/python debug_pipeline_steps.py

Output: debug_steps/*.csv (one file per step)
"""

import sys
import pandas as pd
from pathlib import Path

sys.path.insert(0, ".")

# Import actual pipeline steps
from core.scan_engine.step2_load_snapshot import load_ivhv_snapshot
from core.scan_engine.step3_filter_ivhv import filter_ivhv_gap
from core.scan_engine.step5_chart_signals import compute_chart_signals
from core.scan_engine.step6_gem_filter import validate_data_quality
from core.scan_engine.step7_strategy_recommendation import recommend_strategies
from core.scan_engine.step9a_determine_timeframe import determine_timeframe
from core.scan_engine.step11_independent_evaluation import evaluate_strategies_independently
from core.scan_engine.step12_acceptance import apply_acceptance_logic

# Configuration
SNAPSHOT = "data/snapshots/ivhv_snapshot_live_20260102_124337.csv"
OUTDIR = Path("debug_steps")
OUTDIR.mkdir(exist_ok=True)

# Fixed ticker set (controlled universe)
TICKERS = [
    "AAPL", "MSFT", "NVDA", "TSLA", "GOOGL",
    "AMZN", "META", "NFLX", "AMD", "INTC"
]

print("="*80)
print("AUTHORITATIVE PIPELINE DEBUG")
print("="*80)
print(f"Snapshot: {SNAPSHOT}")
print(f"Tickers: {TICKERS}")
print(f"Output: {OUTDIR}/")
print()


def dump(df, step_name, focus_cols=None):
    """
    Save DataFrame to CSV and print summary.
    
    Args:
        df: DataFrame to dump
        step_name: Name of the step (used for filename)
        focus_cols: Optional list of columns to highlight in summary
    """
    # Save full CSV
    path = OUTDIR / f"{step_name}.csv"
    df.to_csv(path, index=False)
    
    # Print summary
    print(f"\n{'='*80}")
    print(f"[{step_name.upper()}]")
    print(f"{'='*80}")
    print(f"File: {path}")
    print(f"Total rows: {len(df)}")
    print(f"Total columns: {len(df.columns)}")
    
    # Filter to our controlled ticker set
    if 'Ticker' in df.columns:
        df_subset = df[df['Ticker'].isin(TICKERS)]
        print(f"Debug tickers: {len(df_subset)} rows")
        
        if len(df_subset) > 0:
            # Show key columns if specified
            if focus_cols:
                available_cols = ['Ticker'] + [c for c in focus_cols if c in df_subset.columns]
                print(f"\nKey columns: {available_cols}")
                print(df_subset[available_cols].head(10).to_string(index=False))
            else:
                # Show all columns for first 3 tickers
                print(f"\nSample (first 3 tickers, first 20 columns):")
                cols_to_show = df_subset.columns[:20].tolist()
                print(df_subset[cols_to_show].head(3).to_string(index=False))
    else:
        print("⚠️  No 'Ticker' column found")
    
    print(f"{'='*80}")


# ============================================================================
# STEP 0: RAW SNAPSHOT
# ============================================================================
print("\n🔄 STEP 0: Loading raw snapshot...")
df0 = pd.read_csv(SNAPSHOT)
df0 = df0[df0['Ticker'].isin(TICKERS)]  # Filter immediately
dump(df0, "step0_raw_snapshot", focus_cols=[
    'last_price', 'IV_30_D_Call', 'IV_7_D_Call', 'IV_14_D_Call', 
    'HV_30_D_Cur', 'volatility_regime'
])


# ============================================================================
# STEP 2: ENRICHED SNAPSHOT (WITH IV SURFACE REHYDRATION)
# ============================================================================
print("\n🔄 STEP 2: Loading with enrichment + IV surface...")
df2 = load_ivhv_snapshot(SNAPSHOT)
df2 = df2[df2['Ticker'].isin(TICKERS)]
dump(df2, "step2_enriched_snapshot", focus_cols=[
    'IV_7_D_Call', 'IV_14_D_Call', 'IV_21_D_Call', 'IV_30_D_Call', 'IV_60_D_Call',
    'iv_surface_source', 'iv_surface_age_days', 'days_to_earnings', 'earnings_proximity_flag'
])


# ============================================================================
# STEP 3: IVHV FILTER
# ============================================================================
print("\n🔄 STEP 3: IVHV filter...")
df3 = filter_ivhv_gap(df2)
df3 = df3[df3['Ticker'].isin(TICKERS)]
dump(df3, "step3_ivhv_filter", focus_cols=[
    'IVHV_gap_30D', 'IV_Cheap', 'IV_Rich', 'ElevatedVol'
])


# ============================================================================
# STEP 5: CHART SIGNALS
# ============================================================================
print("\n🔄 STEP 5: Chart signals...")
try:
    df5 = compute_chart_signals(df3, use_cache=True)
    df5 = df5[df5['Ticker'].isin(TICKERS)]
    dump(df5, "step5_chart_signals", focus_cols=[
        'compression_tag', 'gap_tag', '52w_regime_tag', 'momentum_tag'
    ])
except Exception as e:
    print(f"❌ Step 5 failed: {e}")
    df5 = df3


# ============================================================================
# STEP 6: DATA QUALITY VALIDATION
# ============================================================================
print("\n🔄 STEP 6: Data quality validation...")
try:
    df6 = validate_data_quality(df5)
    df6 = df6[df6['Ticker'].isin(TICKERS)]
    dump(df6, "step6_data_quality", focus_cols=[
        'data_quality_score', 'critical_issues'
    ])
except Exception as e:
    print(f"❌ Step 6 failed: {e}")
    df6 = df5


# ============================================================================
# STEP 7: STRATEGY RECOMMENDATION
# ============================================================================
print("\n🔄 STEP 7: Strategy recommendation...")
try:
    df7 = recommend_strategies(df6)
    df7 = df7[df7['Ticker'].isin(TICKERS)]
    dump(df7, "step7_strategies", focus_cols=[
        'Strategy_Name', 'Strategy_Type', 'Rationale', 'Confidence'
    ])
except Exception as e:
    print(f"❌ Step 7 failed: {e}")
    df7 = df6


# ============================================================================
# STEP 9A: TIMEFRAME SELECTION
# ============================================================================
print("\n🔄 STEP 9A: Timeframe selection...")
try:
    df9a = determine_timeframe(df7)
    df9a = df9a[df9a['Ticker'].isin(TICKERS)]
    dump(df9a, "step9a_timeframes", focus_cols=[
        'Recommended_DTE', 'DTE_Min', 'DTE_Max', 'Timeframe_Rationale'
    ])
except Exception as e:
    print(f"❌ Step 9A failed: {e}")
    df9a = df7


# ============================================================================
# STEP 9B: CONTRACT FETCH (SKIPPED - REQUIRES LIVE API)
# ============================================================================
print("\n⏭️  STEP 9B: Skipped (requires live API)")


# ============================================================================
# STEP 11: EVALUATION
# ============================================================================
print("\n🔄 STEP 11: Independent evaluation...")
try:
    # Using 9A as both inputs (no contracts available)
    df11 = evaluate_strategies_independently(df9a, df9a)
    df11 = df11[df11['Ticker'].isin(TICKERS)]
    dump(df11, "step11_evaluation", focus_cols=[
        'Total_Score', 'Score_Tier', 'Evaluation_Type', 'Strategy_Rank_Within_Ticker'
    ])
except Exception as e:
    print(f"❌ Step 11 failed: {e}")
    import traceback
    traceback.print_exc()
    df11 = df9a


# ============================================================================
# STEP 12: ACCEPTANCE GATE
# ============================================================================
print("\n🔄 STEP 12: Acceptance gate...")
try:
    df12 = apply_acceptance_logic(df11, snapshot_date="2026-01-02")
    df12 = df12[df12['Ticker'].isin(TICKERS)]
    dump(df12, "step12_acceptance", focus_cols=[
        'acceptance_status', 'acceptance_reason', 'iv_rank_available', 'iv_history_days'
    ])
    
    # Final truth table
    print(f"\n{'='*80}")
    print("FINAL ACCEPTANCE TRUTH TABLE")
    print(f"{'='*80}")
    if len(df12) > 0:
        cols = ['Ticker', 'acceptance_status', 'acceptance_reason', 'iv_rank_available', 'iv_history_days']
        cols = [c for c in cols if c in df12.columns]
        print(df12[cols].to_string(index=False))
    else:
        print("⚠️  No rows in Step 12 output")
    
except Exception as e:
    print(f"❌ Step 12 failed: {e}")
    import traceback
    traceback.print_exc()


print(f"\n{'='*80}")
print("✅ DEBUG COMPLETE")
print(f"{'='*80}")
print(f"\nAll CSV files written to: {OUTDIR.resolve()}")
print(f"\n📋 AUDIT CHECKLIST:")
print(f"  1. Open step2_enriched_snapshot.csv")
print(f"     → Check: IV_7_D_Call, IV_14_D_Call populated?")
print(f"     → Check: iv_surface_age_days = 4?")
print(f"     → Check: days_to_earnings present?")
print(f"")
print(f"  2. Open step3_ivhv_filter.csv")
print(f"     → Check: Which tickers survived?")
print(f"     → Check: IVHV_gap_30D values?")
print(f"")
print(f"  3. Open step7_strategies.csv")
print(f"     → Check: Are strategies generated?")
print(f"     → Check: Confidence levels?")
print(f"")
print(f"  4. Open step12_acceptance.csv")
print(f"     → Check: acceptance_status (READY_NOW vs STRUCTURALLY_READY)")
print(f"     → Check: acceptance_reason diagnostics")
print(f"     → Check: iv_rank_available = False?")
print(f"     → Check: iv_history_days = 4?")
print(f"")
print(f"Expected result:")
print(f"  - All tickers: STRUCTURALLY_READY (not READY_NOW)")
print(f"  - Reason: 'IV surface available (stale: 4 days, need fresh data)'")
print(f"  - iv_rank_available: False (only 4 days history, need 120+)")
