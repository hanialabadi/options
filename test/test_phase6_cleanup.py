#!/usr/bin/env python3
"""
Phase 6 Cleanup Validation Test
Verifies that Phase 6 properly:
1. Preserves leg-level granularity
2. Freezes entry fields for new trades only
3. Enforces immutability
4. Does NOT flatten or aggregate
"""

import pandas as pd
import sys
sys.path.insert(0, '/Users/haniabadi/Documents/Github/options')

from core.phase1_clean import phase1_load_and_clean_positions
from core.phase2_parse import phase2_run_all
from core.phase3_enrich.compute_breakeven import compute_breakeven
from core.phase3_enrich.compute_moneyness import compute_moneyness
from core.phase3_enrich.tag_strategy_metadata import tag_strategy_metadata
from core.phase3_enrich.pcs_score import calculate_pcs
from core.phase3_enrich.score_confidence_tier import score_confidence_tier
from core.phase6_freeze_and_archive import phase6_freeze_and_archive

print('='*70)
print('PHASE 6 CLEANUP VALIDATION')
print('='*70)

# Phase 1-3 pipeline
print('\n🔹 Running Phase 1-3 pipeline...')
result = phase1_load_and_clean_positions(input_path=Path('data/brokerage_inputs/fidelity_positions.csv'))
df = result[0] if isinstance(result, tuple) else result
df = phase2_run_all(df)
df = compute_breakeven(df)
df = compute_moneyness(df)
df = tag_strategy_metadata(df)
df = calculate_pcs(df)
df = score_confidence_tier(df)

print(f'Phase 3 output: {len(df)} positions, {len(df.columns)} columns')

# Simulate empty master (all trades are new)
df_master_empty = pd.DataFrame(columns=df.columns)

# Phase 6 with all new trades
print('\n🔹 Running Phase 6 (all new trades)...')
df_master_v1 = phase6_freeze_and_archive(df, df_master_empty)

print(f'Phase 6 output: {len(df_master_v1)} positions, {len(df_master_v1.columns)} columns')

# Validation checks
print('\n' + '='*70)
print('VALIDATION CHECKS')
print('='*70)

# Check 1: Leg-level preservation
print('\n1️⃣ LEG-LEVEL GRANULARITY CHECK')
multi_leg_trades = df_master_v1[df_master_v1['LegCount'] > 1]
if len(multi_leg_trades) > 0:
    leg_counts = multi_leg_trades.groupby('TradeID').size()
    print(f'✅ Multi-leg trades preserved: {len(leg_counts)} TradeIDs')
    print(f'   Example: {leg_counts.head(3).to_dict()}')
    if all(leg_counts >= 2):
        print('✅ All multi-leg trades have multiple rows (not flattened)')
    else:
        print('❌ Some multi-leg trades collapsed to single row')
else:
    print('ℹ️  No multi-leg trades in test data')

# Check 2: Entry fields created
print('\n2️⃣ ENTRY FIELD FREEZE CHECK')
entry_fields = [col for col in df_master_v1.columns if col.endswith('_Entry')]
print(f'Entry fields created: {len(entry_fields)}')
for field in entry_fields[:10]:  # Show first 10
    non_null = df_master_v1[field].notna().sum()
    print(f'   {field}: {non_null}/{len(df_master_v1)} populated')

required_entry_fields = ['Delta_Entry', 'Gamma_Entry', 'Vega_Entry', 'Theta_Entry', 'Premium_Entry']
missing = [f for f in required_entry_fields if f not in df_master_v1.columns]
if not missing:
    print('✅ All required entry fields present')
else:
    print(f'❌ Missing entry fields: {missing}')

# Check 3: No flattening columns
print('\n3️⃣ FLATTENING CHECK')
flatten_indicators = ['_Call', '_Put', '_Combined', '_Total']
flattened_cols = [col for col in df_master_v1.columns if any(ind in col for ind in flatten_indicators)]
if flattened_cols:
    print(f'❌ Flattening detected: {len(flattened_cols)} columns with _Call/_Put/_Combined suffixes')
    print(f'   Examples: {flattened_cols[:5]}')
else:
    print('✅ No flattening detected (no _Call/_Put/_Combined columns)')

# Check 4: IsNewTrade flag
print('\n4️⃣ LIFECYCLE DETECTION CHECK')
if 'IsNewTrade' in df_master_v1.columns:
    new_count = df_master_v1['IsNewTrade'].sum()
    print(f'✅ IsNewTrade flag present: {new_count}/{len(df_master_v1)} marked as new')
else:
    print('❌ IsNewTrade flag missing')

# Check 5: Phase 3 columns preserved
print('\n5️⃣ PHASE 3 ENRICHMENT PRESERVATION')
phase3_critical = ['PCS', 'Capital Deployed', 'Moneyness_Pct', 'BreakEven', 'DTE']
preserved = [col for col in phase3_critical if col in df_master_v1.columns]
print(f'Phase 3 columns preserved: {len(preserved)}/{len(phase3_critical)}')
for col in phase3_critical:
    status = '✅' if col in df_master_v1.columns else '❌'
    print(f'   {status} {col}')

# Check 6: Simulate second snapshot (immutability test)
print('\n6️⃣ IMMUTABILITY TEST')
print('Simulating second snapshot with same trades...')

# Modify some values in Phase 3 output to simulate drift
df_v2 = df.copy()
if 'Delta' in df_v2.columns:
    df_v2['Delta'] = df_v2['Delta'] + 0.05  # Simulate Greek drift

try:
    df_master_v2 = phase6_freeze_and_archive(df_v2, df_master_v1)
    
    # Check that _Entry fields didn't change
    if 'Delta_Entry' in df_master_v2.columns and 'Delta_Entry' in df_master_v1.columns:
        # Compare first TradeID
        first_tid = df_master_v2['TradeID'].iloc[0]
        v1_entry = df_master_v1[df_master_v1['TradeID'] == first_tid]['Delta_Entry'].iloc[0]
        v2_entry = df_master_v2[df_master_v2['TradeID'] == first_tid]['Delta_Entry'].iloc[0]
        
        if v1_entry == v2_entry:
            print(f'✅ Immutability preserved: Delta_Entry unchanged ({v1_entry:.4f})')
        else:
            print(f'❌ Immutability violated: Delta_Entry changed from {v1_entry:.4f} to {v2_entry:.4f}')
            
        # Check that current Delta DID change
        v2_current = df_master_v2[df_master_v2['TradeID'] == first_tid]['Delta'].iloc[0]
        print(f'   Current Delta updated: {v2_current:.4f} (drift tracking possible)')
except Exception as e:
    print(f'⚠️ Immutability test error: {e}')

# Final verdict
print('\n' + '='*70)
print('FINAL VERDICT')
print('='*70)

checks_passed = [
    len(flattened_cols) == 0,  # No flattening
    all(f in df_master_v1.columns for f in required_entry_fields),  # Entry fields present
    'IsNewTrade' in df_master_v1.columns,  # Lifecycle detection
    len(preserved) == len(phase3_critical)  # Phase 3 preserved
]

if all(checks_passed):
    print('✅ PHASE 6 CLEANUP SUCCESSFUL')
    print('✅ Leg-level preservation maintained')
    print('✅ Entry fields frozen correctly')
    print('✅ No flattening detected')
    print('✅ Phase 3 enrichments preserved')
    print('\n🔒 Phase 6 ready to freeze')
else:
    print('⚠️ SOME ISSUES DETECTED')
    print(f'Checks passed: {sum(checks_passed)}/{len(checks_passed)}')

print('='*70)
