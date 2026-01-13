#!/usr/bin/env python3
"""
Debug Step 3 Edge Flag Calculations
"""

import sys
import pandas as pd
from pathlib import Path

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent))

from core.scan_engine.step2_load_snapshot import load_ivhv_snapshot
from core.scan_engine.step3_filter_ivhv import filter_ivhv_gap

print("=" * 80)
print("STEP 3 EDGE FLAG DIAGNOSTIC")
print("=" * 80)

# Load snapshot
print("\n1. Loading snapshot...")
try:
    df_snapshot = load_ivhv_snapshot()
    print(f"   ✅ Loaded {len(df_snapshot)} rows")
except Exception as e:
    print(f"   ❌ Failed to load: {e}")
    sys.exit(1)

# Run Step 3
print("\n2. Running Step 3 filter...")
try:
    df_filtered = filter_ivhv_gap(df_snapshot, min_gap=2.0)
    print(f"   ✅ Filtered to {len(df_filtered)} tickers")
except Exception as e:
    print(f"   ❌ Step 3 failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Detailed diagnostics
print("\n3. GAP STATISTICS:")
print(f"   IVHV_gap_30D range: {df_filtered['IVHV_gap_30D'].min():.2f} to {df_filtered['IVHV_gap_30D'].max():.2f}")
print(f"   IVHV_gap_30D mean: {df_filtered['IVHV_gap_30D'].mean():.2f}")
print(f"   IVHV_gap_30D median: {df_filtered['IVHV_gap_30D'].median():.2f}")
print(f"   IVHV_gap_30D std: {df_filtered['IVHV_gap_30D'].std():.2f}")

print("\n4. ABSOLUTE GAP STATISTICS:")
print(f"   IVHV_gap_abs range: {df_filtered['IVHV_gap_abs'].min():.2f} to {df_filtered['IVHV_gap_abs'].max():.2f}")
print(f"   IVHV_gap_abs mean: {df_filtered['IVHV_gap_abs'].mean():.2f}")

print("\n5. GAP DISTRIBUTION:")
positive_gaps = (df_filtered['IVHV_gap_30D'] >= 2.0).sum()
negative_gaps = (df_filtered['IVHV_gap_30D'] <= -2.0).sum()
small_gaps = ((df_filtered['IVHV_gap_30D'] > -2.0) & (df_filtered['IVHV_gap_30D'] < 2.0)).sum()
print(f"   Positive gaps (≥ 2.0): {positive_gaps}")
print(f"   Negative gaps (≤ -2.0): {negative_gaps}")
print(f"   Small gaps (-2.0 to 2.0): {small_gaps}")

print("\n6. EDGE FLAG COUNTS:")
if 'ShortTerm_IV_Edge' in df_filtered.columns:
    edge_count = df_filtered['ShortTerm_IV_Edge'].sum()
    print(f"   ShortTerm_IV_Edge: {edge_count}")
    print(f"   ShortTerm_IV_Edge type: {type(df_filtered['ShortTerm_IV_Edge'].iloc[0]) if len(df_filtered) > 0 else 'N/A'}")
else:
    print("   ❌ ShortTerm_IV_Edge column missing!")

if 'ShortTerm_IV_Rich' in df_filtered.columns:
    rich_count = df_filtered['ShortTerm_IV_Rich'].sum()
    print(f"   ShortTerm_IV_Rich: {rich_count}")
else:
    print("   ❌ ShortTerm_IV_Rich column missing!")

if 'ShortTerm_IV_Cheap' in df_filtered.columns:
    cheap_count = df_filtered['ShortTerm_IV_Cheap'].sum()
    print(f"   ShortTerm_IV_Cheap: {cheap_count}")
else:
    print("   ❌ ShortTerm_IV_Cheap column missing!")

print("\n7. SAMPLE DATA (first 10 tickers):")
display_cols = ['Ticker', 'IVHV_gap_30D', 'IVHV_gap_abs', 'ShortTerm_IV_Edge', 'ShortTerm_IV_Rich', 'ShortTerm_IV_Cheap']
display_cols = [col for col in display_cols if col in df_filtered.columns]
print(df_filtered[display_cols].head(10).to_string(index=False))

print("\n8. EDGE FLAG CALCULATION TEST:")
# Manually calculate to verify logic
test_edge = (df_filtered['IVHV_gap_30D'].abs() >= 2.0)
manual_edge_count = test_edge.sum()
print(f"   Manual calculation: abs(gap) >= 2.0 → {manual_edge_count} tickers")
print(f"   Stored flag count: {edge_count if 'ShortTerm_IV_Edge' in df_filtered.columns else 'N/A'}")
print(f"   Match: {manual_edge_count == edge_count if 'ShortTerm_IV_Edge' in df_filtered.columns else 'Cannot verify'}")

print("\n9. NaN CHECK:")
print(f"   IVHV_gap_30D NaN count: {df_filtered['IVHV_gap_30D'].isna().sum()}")
if 'ShortTerm_IV_Edge' in df_filtered.columns:
    print(f"   ShortTerm_IV_Edge NaN count: {df_filtered['ShortTerm_IV_Edge'].isna().sum()}")

print("\n10. EXTREME CASES:")
# Show tickers with largest positive gap
print("\n   Top 5 POSITIVE gaps:")
top_pos = df_filtered.nlargest(5, 'IVHV_gap_30D')[display_cols]
print(top_pos.to_string(index=False))

print("\n   Top 5 NEGATIVE gaps (most negative):")
top_neg = df_filtered.nsmallest(5, 'IVHV_gap_30D')[display_cols]
print(top_neg.to_string(index=False))

print("\n" + "=" * 80)
print("DIAGNOSTIC COMPLETE")
print("=" * 80)
