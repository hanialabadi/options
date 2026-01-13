#!/usr/bin/env python
"""
Manual Step 9B test to verify Phase 2 enrichment
"""
import sys
import pandas as pd
sys.path.insert(0, '.')

from core.scan_engine.step9b_fetch_contracts_schwab import fetch_and_select_contracts_schwab

print('='*70)
print('MANUAL STEP 9B RUN FOR PHASE 2 VALIDATION')
print('='*70)

# Load Step 11 and Step 9A outputs from earlier run
df_11 = pd.read_csv('output/Step11_Evaluated_20260102_092132.csv')
df_9a = pd.read_csv('output/Step9A_Timeframes_20260102_092132.csv')

print(f'\n✅ Loaded Step 11: {len(df_11)} rows')
print(f'✅ Loaded Step 9A: {len(df_9a)} rows')

# Take first 5 tickers for faster run
sample_tickers = df_9a['Ticker'].unique()[:5]
df_11_sample = df_11[df_11['Ticker'].isin(sample_tickers)]
df_9a_sample = df_9a[df_9a['Ticker'].isin(sample_tickers)]

print(f'\n📋 Using sample: {sample_tickers.tolist()}')
print(f'   Step 11 rows: {len(df_11_sample)}')
print(f'   Step 9A rows: {len(df_9a_sample)}')

# Run Step 9B
print('\n🔄 Running Step 9B with Phase 2 enrichment...')
result_df = fetch_and_select_contracts_schwab(df_11_sample, df_9a_sample)

if result_df is not None and len(result_df) > 0:
    print(f'\n✅ Step 9B completed: {len(result_df)} contracts')
    
    # Save output
    output_file = 'output/Step9B_PHASE2_VALIDATION.csv'
    result_df.to_csv(output_file, index=False)
    print(f'✅ Saved: {output_file}')
    
    # Check Phase 2 columns
    phase2_cols = ['bidSize', 'askSize', 'depth_tag', 'balance_tag', 'execution_quality', 'dividend_risk']
    present = [c for c in phase2_cols if c in result_df.columns]
    missing = [c for c in phase2_cols if c not in result_df.columns]
    
    print(f'\n📊 Phase 2 columns PRESENT: {present}')
    print(f'📊 Phase 2 columns MISSING: {missing}')
    
    if present:
        print(f'\n✅ PHASE 2 ENRICHMENT VERIFIED!')
        print(f'\nRow count: {len(result_df)}')
        print(f'Column count: {len(result_df.columns)}')
    else:
        print(f'\n❌ Phase 2 enrichment NOT applied')
else:
    print('\n❌ No contracts returned from Step 9B')
