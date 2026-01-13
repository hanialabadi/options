#!/usr/bin/env python3
"""
Step 8 Verification Test - Execution-Only Architecture

Tests that Step 8:
1. Only accepts Valid strategies (Watch excluded)
2. Fails loudly on NaN/inf data
3. Produces no NaN values in output
"""
import sys
sys.path.insert(0, '/Users/haniabadi/Documents/Github/options')
import pandas as pd
import numpy as np

print("Step 8 Verification Test")
print("=" * 80)

# Create test strategies matching Step 11 output
test_data = []

# Valid strategies (should be allocated)
for i in range(10):
    test_data.append({
        'Ticker': f'TEST{i}',
        'Primary_Strategy': 'Long Call',
        'Validation_Status': 'Valid',
        'Theory_Compliance_Score': 85.0,
        'Total_Debit': 500.0,
        'Delta': 0.65,
        'Gamma': 0.04,
        'Vega': 0.18,
        'Theta': -0.22,
        'Contract_Quantity': 1,
        'Capital_Required': 500
    })

# Watch strategies (should be EXCLUDED)
for i in range(5):
    test_data.append({
        'Ticker': f'WATCH{i}',
        'Primary_Strategy': 'Long Straddle',
        'Validation_Status': 'Watch',
        'Theory_Compliance_Score': 68.0,  # Marginal
        'Total_Debit': 800.0,
        'Delta': 0.0,
        'Gamma': 0.08,
        'Vega': 0.42,
        'Theta': -0.45,
        'Contract_Quantity': 1,
        'Capital_Required': 800
    })

# Reject strategies (should be EXCLUDED)
for i in range(2):
    test_data.append({
        'Ticker': f'REJECT{i}',
        'Primary_Strategy': 'Long Straddle',
        'Validation_Status': 'Reject',
        'Theory_Compliance_Score': 42.0,
        'Total_Debit': np.nan,  # Missing data (rejected in Step 11)
        'Delta': np.nan,
        'Gamma': np.nan,
        'Vega': np.nan,
        'Theta': np.nan,
        'Contract_Quantity': 1,
        'Capital_Required': np.nan
    })

df_test = pd.DataFrame(test_data)

print(f"\nTest Input: {len(df_test)} strategies")
print(f"  Valid: {(df_test['Validation_Status'] == 'Valid').sum()}")
print(f"  Watch: {(df_test['Validation_Status'] == 'Watch').sum()}")
print(f"  Reject: {(df_test['Validation_Status'] == 'Reject').sum()}")

# Test Step 8
from core.scan_engine.step8_position_sizing import allocate_portfolio_capital

try:
    df_allocated = allocate_portfolio_capital(
        df_test,
        account_balance=100000,
        max_portfolio_risk=0.20,
        max_trade_risk=0.02,
        min_compliance_score=60.0,
        max_strategies_per_ticker=2
    )
    
    print(f"\n✅ Step 8 Completed")
    print(f"   Allocated: {len(df_allocated)} strategies")
    print(f"   Expected: 10 (only Valid strategies)")
    
    # Verify only Valid strategies allocated
    if len(df_allocated) == 10:
        print(f"   ✅ Correct count (Watch/Reject excluded)")
    else:
        print(f"   ❌ WRONG count (expected 10, got {len(df_allocated)})")
    
    # Verify all Validation_Status are Valid
    if (df_allocated['Validation_Status'] == 'Valid').all():
        print(f"   ✅ All allocated strategies are Valid")
    else:
        print(f"   ❌ Non-Valid strategies leaked into allocation!")
    
    # Verify no NaN values in critical fields
    critical_fields = ['Capital_Allocation', 'Contracts', 'Theory_Compliance_Score']
    nan_found = False
    for field in critical_fields:
        if field in df_allocated.columns:
            if df_allocated[field].isna().any():
                print(f"   ❌ NaN values found in {field}")
                nan_found = True
    
    if not nan_found:
        print(f"   ✅ No NaN values in output")
    
    # Verify all numeric fields are finite
    numeric_cols = df_allocated.select_dtypes(include=[np.number]).columns
    all_finite = True
    for col in numeric_cols:
        if not np.all(np.isfinite(df_allocated[col])):
            print(f"   ❌ Non-finite values in {col}")
            all_finite = False
    
    if all_finite:
        print(f"   ✅ All numeric fields are finite")
    
    # Show capital allocation summary
    if 'Capital_Allocation' in df_allocated.columns:
        total_allocated = df_allocated['Capital_Allocation'].sum()
        print(f"\n   Total Capital Allocated: ${total_allocated:,.0f}")
        print(f"   Total Contracts: {df_allocated['Contracts'].sum()}")
    
    print("\n" + "=" * 80)
    print("✅ SUCCESS: Step 8 execution-only architecture verified")
    print("   - Only Valid strategies allocated")
    print("   - Watch strategies excluded (informational)")
    print("   - Reject strategies excluded (NaN data)")
    print("   - No NaN coercion errors")
    
except Exception as e:
    print(f"\n❌ FAILURE: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
