#!/usr/bin/env python
"""
Test Step 12 Acceptance Logic

Validates Phase 3 implementation using Phase 2 validation output.
"""
import sys
import pandas as pd
sys.path.insert(0, '.')

from core.scan_engine.step12_acceptance import apply_acceptance_logic, filter_ready_contracts, sort_by_confidence

print('='*80)
print('PHASE 3: ACCEPTANCE LOGIC TEST')
print('='*80)

# Load Phase 2 validation output
input_file = 'output/Step9B_PHASE2_VALIDATION.csv'
print(f'\n📂 Loading: {input_file}')
df_step9b = pd.read_csv(input_file)
print(f'✅ Loaded {len(df_step9b)} contracts')

# Apply acceptance logic
print('\n🎯 Applying acceptance logic (Step 12)...')
df_step12 = apply_acceptance_logic(df_step9b)

# Save results
output_file = 'output/Step12_Acceptance_TEST.csv'
df_step12.to_csv(output_file, index=False)
print(f'\n✅ Saved: {output_file}')

# Display summary
print('\n' + '='*80)
print('ACCEPTANCE SUMMARY')
print('='*80)

status_counts = df_step12['acceptance_status'].value_counts().to_dict()
confidence_counts = df_step12['confidence_band'].value_counts().to_dict()

print('\n📊 Acceptance Status:')
for status in ['READY_NOW', 'WAIT', 'AVOID']:
    count = status_counts.get(status, 0)
    pct = count / len(df_step12) * 100 if len(df_step12) > 0 else 0
    print(f'   {status:15s}: {count:3d} ({pct:5.1f}%)')

print('\n📊 Confidence Bands:')
for conf in ['HIGH', 'MEDIUM', 'LOW']:
    count = confidence_counts.get(conf, 0)
    pct = count / len(df_step12) * 100 if len(df_step12) > 0 else 0
    print(f'   {conf:15s}: {count:3d} ({pct:5.1f}%)')

# Show sample decisions
print('\n' + '='*80)
print('SAMPLE DECISIONS (First 5 contracts)')
print('='*80)

display_cols = [
    'Ticker', 'Strategy_Name', 'acceptance_status', 'acceptance_reason', 
    'confidence_band', 'directional_bias', 'structure_bias', 'execution_adjustment'
]

existing_cols = [c for c in display_cols if c in df_step12.columns]
print(df_step12[existing_cols].head(5).to_string(index=False))

# Show READY_NOW contracts
df_ready = df_step12[df_step12['acceptance_status'] == 'READY_NOW']
print(f'\n' + '='*80)
print(f'READY_NOW CONTRACTS ({len(df_ready)} total)')
print('='*80)

if len(df_ready) > 0:
    df_ready_sorted = sort_by_confidence(df_ready)
    print(df_ready_sorted[existing_cols].to_string(index=False))
else:
    print('⚠️  No READY_NOW contracts in test data')
    print('\nThis is expected if:')
    print('  - Test data has UNKNOWN Phase 1 enrichment')
    print('  - Market conditions not favorable')
    print('  - GEM evaluation already filtered aggressively')

# Show Phase 1 enrichment status
print(f'\n' + '='*80)
print('PHASE 1 ENRICHMENT STATUS')
print('='*80)

phase1_cols = ['compression_tag', 'gap_tag', 'momentum_tag', '52w_regime_tag', 'entry_timing_context']
for col in phase1_cols:
    if col in df_step12.columns:
        values = df_step12[col].value_counts().to_dict()
        print(f'\n{col}:')
        for k, v in values.items():
            print(f'   {k}: {v}')
    else:
        print(f'\n{col}: MISSING')

print('\n' + '='*80)
print('✅ PHASE 3 TEST COMPLETE')
print('='*80)
print(f'\nOutput saved: {output_file}')
print('Review the acceptance decisions to validate rule logic.')
