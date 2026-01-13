"""
IV Column Tracking Debug Script

Traces IV columns through the entire pipeline (Step 0 → Step 12)
for a controlled set of 10 tickers, printing exact schema and data
at each step to identify where IV surface data is lost or never loaded.
"""

import sys
sys.path.insert(0, '.')

import pandas as pd
from datetime import datetime
import logging

# Silence noisy logs
logging.basicConfig(level=logging.WARNING)

# Fixed 10-ticker subset
TICKERS = ['AAPL', 'MSFT', 'NVDA', 'TSLA', 'GOOGL',
           'AMZN', 'META', 'AMD', 'NFLX', 'JPM']

FOCUS_TICKER = 'AAPL'


def debug_snapshot(df, step_name, ticker=FOCUS_TICKER):
    """Print detailed schema and IV data for debugging"""
    iv_cols = sorted([c for c in df.columns if 'iv_' in c.lower() or c.lower().startswith('iv')])
    earnings_cols = [c for c in df.columns if 'earning' in c.lower()]
    acceptance_cols = [c for c in df.columns if 'acceptance' in c.lower()]
    
    print(f"\n{'='*70}")
    print(f"{step_name}")
    print(f"{'='*70}")
    print(f"Rows: {len(df)} | Columns: {len(df.columns)}")
    print(f"\nIV Columns ({len(iv_cols)}): {iv_cols if iv_cols else 'NONE'}")
    print(f"Earnings Columns ({len(earnings_cols)}): {earnings_cols if earnings_cols else 'NONE'}")
    print(f"Acceptance Columns ({len(acceptance_cols)}): {acceptance_cols if acceptance_cols else 'NONE'}")
    
    if ticker in df['Ticker'].values:
        print(f"\n{ticker} Data:")
        
        # Show all relevant columns
        cols_to_show = ['Ticker']
        cols_to_show.extend(iv_cols)
        cols_to_show.extend(earnings_cols)
        cols_to_show.extend(acceptance_cols)
        
        # Filter to only columns that exist
        cols_to_show = [c for c in cols_to_show if c in df.columns]
        
        row = df[df['Ticker'] == ticker][cols_to_show]
        
        if len(cols_to_show) > 1:
            # Transpose for readability
            print(row.T.to_string())
        else:
            print(f"  No IV/earnings/acceptance columns found")
    else:
        print(f"\n⚠️ {ticker} not in dataset")


def diff_columns(df_before, df_after, step_name):
    """Show exact column changes between steps"""
    before_cols = set(df_before.columns)
    after_cols = set(df_after.columns)
    
    added = sorted(after_cols - before_cols)
    removed = sorted(before_cols - after_cols)
    kept = sorted(before_cols & after_cols)
    
    print(f"\n📊 Column Changes at {step_name}:")
    print(f"   ➕ Added ({len(added)}): {added if added else 'NONE'}")
    print(f"   ➖ Removed ({len(removed)}): {removed if removed else 'NONE'}")
    print(f"   ➡️  Kept: {len(kept)} columns")


print("="*70)
print("IV COLUMN TRACKING - CONTROLLED 10-TICKER SCAN")
print("="*70)
print(f"Tickers: {TICKERS}")
print(f"Focus ticker: {FOCUS_TICKER}")

# ============================================================
# STEP 0: Raw Snapshot Load
# ============================================================
print("\n\n🔵 STEP 0: Loading raw snapshot...")

from core.scan_engine.step2_load_snapshot import load_ivhv_snapshot

df_step0 = load_ivhv_snapshot('data/snapshots/ivhv_snapshot_live_20260102_124337.csv')

# Filter to 10 tickers
df_step0 = df_step0[df_step0['Ticker'].isin(TICKERS)].copy()

debug_snapshot(df_step0, "STEP 0: Raw Snapshot (After load_ivhv_snapshot)")

# ============================================================
# STEP 2: Post-Enrichment (Earnings, etc)
# ============================================================
# Note: load_ivhv_snapshot already includes earnings enrichment
# So Step 0 output IS Step 2 output

debug_snapshot(df_step0, "STEP 2: Post-Enrichment (Same as Step 0, includes earnings)")

# ============================================================
# STEP 3: IVHV Filter
# ============================================================
print("\n\n🔵 STEP 3: IVHV Filter...")

from core.scan_engine.step3_filter_ivhv import filter_ivhv_gap

df_before_step3 = df_step0.copy()
df_step3 = filter_ivhv_gap(df_step0)

debug_snapshot(df_step3, "STEP 3: After IVHV Filter")
diff_columns(df_before_step3, df_step3, "Step 3")

# ============================================================
# STEP 5: Chart Signals
# ============================================================
print("\n\n🔵 STEP 5: Chart Signals...")

from core.scan_engine.step5_chart_signals import compute_chart_signals_batch

df_before_step5 = df_step3.copy()
df_step5 = compute_chart_signals_batch(df_step3, use_cache=True)

debug_snapshot(df_step5, "STEP 5: After Chart Signals")
diff_columns(df_before_step5, df_step5, "Step 5")

# ============================================================
# STEP 6: Pattern Validation
# ============================================================
print("\n\n🔵 STEP 6: Pattern Validation...")

from core.scan_engine.step6_validate_patterns import validate_patterns

df_before_step6 = df_step5.copy()
df_step6 = validate_patterns(df_step5)

debug_snapshot(df_step6, "STEP 6: After Pattern Validation")
diff_columns(df_before_step6, df_step6, "Step 6")

# ============================================================
# STEP 7: Strategy Recommendation
# ============================================================
print("\n\n🔵 STEP 7: Strategy Recommendation...")

from core.scan_engine.step7_recommend_strategy import recommend_strategies

df_before_step7 = df_step6.copy()
df_step7 = recommend_strategies(df_step6)

debug_snapshot(df_step7, "STEP 7: After Strategy Recommendation")
diff_columns(df_before_step7, df_step7, "Step 7")

# ============================================================
# STEP 9A: Timeframe Selection
# ============================================================
print("\n\n🔵 STEP 9A: Timeframe Selection...")

from core.scan_engine.step9a_select_timeframes import select_option_timeframes

df_before_step9a = df_step7.copy()
df_step9a = select_option_timeframes(df_step7)

if df_step9a is not None and len(df_step9a) > 0:
    debug_snapshot(df_step9a, "STEP 9A: After Timeframe Selection")
    diff_columns(df_before_step7, df_step9a, "Step 9A")
else:
    print("\n⚠️ Step 9A returned empty DataFrame")
    df_step9a = pd.DataFrame()

# ============================================================
# STEP 9B: Contract Fetch
# ============================================================
print("\n\n🔵 STEP 9B: Contract Fetch (Schwab API)...")

if len(df_step9a) > 0 and len(df_step7) > 0:
    from core.scan_engine.step9b_fetch_contracts_schwab import fetch_and_select_contracts_schwab
    
    df_before_step9b = df_step9a.copy()
    
    # Take only 3 tickers to avoid long API calls
    sample_tickers = df_step9a['Ticker'].unique()[:3]
    df_step7_sample = df_step7[df_step7['Ticker'].isin(sample_tickers)]
    df_step9a_sample = df_step9a[df_step9a['Ticker'].isin(sample_tickers)]
    
    print(f"   (Using {len(sample_tickers)} tickers to avoid timeout: {sample_tickers.tolist()})")
    
    df_step9b = fetch_and_select_contracts_schwab(df_step7_sample, df_step9a_sample)
    
    if df_step9b is not None and len(df_step9b) > 0:
        debug_snapshot(df_step9b, "STEP 9B: After Contract Fetch")
        diff_columns(df_before_step9b, df_step9b, "Step 9B")
    else:
        print("\n⚠️ Step 9B returned empty DataFrame")
        df_step9b = pd.DataFrame()
else:
    print("\n⚠️ Skipping Step 9B (no data from 9A)")
    df_step9b = pd.DataFrame()

# ============================================================
# STEP 11: Evaluation
# ============================================================
print("\n\n🔵 STEP 11: Evaluation...")

if len(df_step9b) > 0:
    from core.scan_engine.step11_evaluate import evaluate_strategies
    
    df_before_step11 = df_step9b.copy()
    df_step11 = evaluate_strategies(df_step9b)
    
    if df_step11 is not None and len(df_step11) > 0:
        debug_snapshot(df_step11, "STEP 11: After Evaluation")
        diff_columns(df_before_step11, df_step11, "Step 11")
    else:
        print("\n⚠️ Step 11 returned empty DataFrame")
        df_step11 = pd.DataFrame()
else:
    print("\n⚠️ Skipping Step 11 (no data from 9B)")
    df_step11 = pd.DataFrame()

# ============================================================
# STEP 12: Acceptance
# ============================================================
print("\n\n🔵 STEP 12: Acceptance...")

if len(df_step11) > 0:
    from core.scan_engine.step12_acceptance import acceptance_logic
    
    df_before_step12 = df_step11.copy()
    df_step12 = acceptance_logic(df_step11)
    
    if df_step12 is not None and len(df_step12) > 0:
        debug_snapshot(df_step12, "STEP 12: After Acceptance")
        diff_columns(df_before_step11, df_step12, "Step 12")
        
        # Show final acceptance statuses
        print("\n📊 Final Acceptance Summary:")
        status_counts = df_step12['acceptance_status'].value_counts()
        print(status_counts.to_string())
    else:
        print("\n⚠️ Step 12 returned empty DataFrame")
        df_step12 = pd.DataFrame()
else:
    print("\n⚠️ Skipping Step 12 (no data from 11)")
    df_step12 = pd.DataFrame()

# ============================================================
# FINAL SUMMARY
# ============================================================
print("\n\n" + "="*70)
print("SUMMARY: IV COLUMNS THROUGH PIPELINE")
print("="*70)

steps = [
    ("Step 0 (Raw)", df_step0),
    ("Step 3 (IVHV)", df_step3),
    ("Step 5 (Charts)", df_step5),
    ("Step 6 (Validation)", df_step6),
    ("Step 7 (Strategy)", df_step7),
    ("Step 9A (Timeframes)", df_step9a),
    ("Step 9B (Contracts)", df_step9b),
    ("Step 11 (Evaluation)", df_step11),
    ("Step 12 (Acceptance)", df_step12),
]

for step_name, df in steps:
    if len(df) > 0:
        iv_cols = sorted([c for c in df.columns if 'iv_' in c.lower() or c.lower().startswith('iv')])
        print(f"\n{step_name:25s}: {len(df):3d} rows, {len(iv_cols):2d} IV columns")
        if iv_cols:
            print(f"  {iv_cols}")
    else:
        print(f"\n{step_name:25s}: EMPTY")

print("\n" + "="*70)
print("✅ DEBUG SCAN COMPLETE")
print("="*70)
