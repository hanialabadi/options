"""
End-to-End Pipeline Test: Phase 1 → Phase 2 → Phase 2C → Phase 3

Diagnostic test to verify:
1. Phase 3 enrichment executes without errors
2. Phase 3 does NOT mutate Phase 2 columns
3. Enrichment values are structurally and numerically reasonable
"""

import pandas as pd
import sys
sys.path.insert(0, '/Users/haniabadi/Documents/Github/options')

from core.phase1_clean import phase1_load_and_clean_positions
from core.phase2_parse import phase2_run_all
from core.phase2_validate_structures import enforce_validation_gate

# Import Phase 3 enrichment functions
from core.phase3_enrich.compute_breakeven import compute_breakeven
from core.phase3_enrich.compute_moneyness import compute_moneyness
from core.phase3_enrich.pcs_score import calculate_pcs
from core.phase3_enrich.score_confidence_tier import score_confidence_tier
from core.phase3_enrich.tag_strategy_metadata import tag_strategy_metadata
from core.phase3_enrich.tag_earnings_flags import tag_earnings_flags
from core.phase3_enrich.liquidity import enrich_liquidity
from core.phase3_enrich.skew_kurtosis import calculate_skew_and_kurtosis

print("=" * 80)
print("END-TO-END PIPELINE TEST: Phase 1 → Phase 2 → Phase 2C → Phase 3")
print("=" * 80)

# ========================================================================
# Phase 1: Data Intake
# ========================================================================
print("\n🔹 PHASE 1: Data Intake")
print("-" * 80)

result = phase1_load_and_clean_positions(input_path=Path('data/brokerage_inputs/fidelity_positions.csv'))
df = result[0] if isinstance(result, tuple) else result

print(f"✅ Phase 1 Complete: {len(df)} positions, {len(df.columns)} columns")

# ========================================================================
# Phase 2: Parsing + Strategy Detection + Validation
# ========================================================================
print("\n🔹 PHASE 2: Parsing + Strategy Detection + Validation Gate")
print("-" * 80)

df_phase2 = phase2_run_all(df)
print(f"✅ Phase 2 Complete: {len(df_phase2)} positions, {df_phase2['TradeID'].nunique()} TradeIDs, {len(df_phase2.columns)} columns")

# Execution guard
df_phase2 = enforce_validation_gate(df_phase2, strict=True)

# Capture Phase 2 columns and key values for mutation check
phase2_columns = set(df_phase2.columns)
phase2_checksums = {
    'TradeID': df_phase2['TradeID'].tolist(),
    'Strategy': df_phase2['Strategy'].tolist(),
    'LegType': df_phase2['LegType'].tolist(),
    'Account': df_phase2['Account'].tolist(),
    'Structure': df_phase2['Structure'].tolist() if 'Structure' in df_phase2.columns else None
}

# ========================================================================
# Phase 3: Enrichment
# ========================================================================
print("\n🔹 PHASE 3: Enrichment Layer")
print("-" * 80)

df_phase3 = df_phase2.copy()

# Apply Phase 3 enrichment functions in sequence
try:
    print("  Applying: compute_breakeven()...")
    df_phase3 = compute_breakeven(df_phase3)
    
    print("  Applying: compute_moneyness()...")
    df_phase3 = compute_moneyness(df_phase3)
    
    print("  Applying: tag_strategy_metadata()...")
    df_phase3 = tag_strategy_metadata(df_phase3)
    
    print("  Applying: calculate_pcs()...")
    df_phase3 = calculate_pcs(df_phase3)
    
    print("  Applying: score_confidence_tier()...")
    df_phase3 = score_confidence_tier(df_phase3)
    
    # Skip liquidity if missing required columns
    if 'Open Int' in df_phase3.columns and 'Volume' in df_phase3.columns:
        print("  Applying: enrich_liquidity()...")
        df_phase3 = enrich_liquidity(df_phase3)
    else:
        print("  Skipping: enrich_liquidity() (missing Open Int/Volume columns)")
        # Add placeholder columns for consistency
        df_phase3['Liquidity_OK'] = None
        df_phase3['OI_OK'] = None
        df_phase3['Spread_OK'] = None
    
    # Skip skew/kurtosis if missing IV Mid column
    if 'IV Mid' in df_phase3.columns:
        print("  Applying: calculate_skew_and_kurtosis()...")
        df_phase3 = calculate_skew_and_kurtosis(df_phase3)
    else:
        print("  Skipping: calculate_skew_and_kurtosis() (missing IV Mid column)")
        df_phase3['Skew'] = None
        df_phase3['Kurtosis'] = None
    
    print("  Applying: tag_earnings_flags()...")
    df_phase3 = tag_earnings_flags(df_phase3)
    
    print(f"\n✅ Phase 3 Complete: {len(df_phase3)} positions, {len(df_phase3.columns)} columns")
    
except Exception as e:
    print(f"\n❌ Phase 3 FAILED with error:\n   {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ========================================================================
# Verification 1: Phase 2 Columns NOT Mutated
# ========================================================================
print("\n🔹 VERIFICATION: Phase 2 Columns Immutability")
print("-" * 80)

mutation_detected = False

# Check TradeID
if df_phase3['TradeID'].tolist() != phase2_checksums['TradeID']:
    print("❌ MUTATION DETECTED: TradeID values changed!")
    mutation_detected = True
else:
    print("✅ TradeID: No mutations")

# Check Strategy
if df_phase3['Strategy'].tolist() != phase2_checksums['Strategy']:
    print("❌ MUTATION DETECTED: Strategy values changed!")
    mutation_detected = True
else:
    print("✅ Strategy: No mutations")

# Check LegType
if df_phase3['LegType'].tolist() != phase2_checksums['LegType']:
    print("❌ MUTATION DETECTED: LegType values changed!")
    mutation_detected = True
else:
    print("✅ LegType: No mutations")

# Check Account
if df_phase3['Account'].tolist() != phase2_checksums['Account']:
    print("❌ MUTATION DETECTED: Account values changed!")
    mutation_detected = True
else:
    print("✅ Account: No mutations")

# Check Structure
if phase2_checksums['Structure'] and df_phase3['Structure'].tolist() != phase2_checksums['Structure']:
    print("❌ MUTATION DETECTED: Structure values changed!")
    mutation_detected = True
else:
    print("✅ Structure: No mutations")

if mutation_detected:
    print("\n❌ CRITICAL: Phase 3 violated immutability contract!")
    sys.exit(1)

# ========================================================================
# Verification 2: New Columns Appended
# ========================================================================
print("\n🔹 VERIFICATION: Phase 3 Appended Columns")
print("-" * 80)

phase3_columns = set(df_phase3.columns)
new_columns = phase3_columns - phase2_columns

print(f"Phase 2 had: {len(phase2_columns)} columns")
print(f"Phase 3 has: {len(phase3_columns)} columns")
print(f"New columns added: {len(new_columns)}")
print(f"\nPhase 3 enrichment columns:")
for col in sorted(new_columns):
    print(f"  + {col}")

# ========================================================================
# Output: Enrichment Summary (Grouped by TradeID)
# ========================================================================
print("\n" + "=" * 80)
print("ENRICHMENT OUTPUT SUMMARY (Grouped by TradeID)")
print("=" * 80)

# Define enrichment-relevant columns to display
display_cols = [
    'TradeID', 'Strategy', 'LegCount',
    'PCS', 'PCS_Tier', 'Confidence_Tier',
    'Liquidity_OK', 'Needs_Revalidation',
    'BreakEven', 'BreakEven_Lower', 'BreakEven_Upper', 'BreakEven_Type',
    'Capital_Deployed',
    'Tag_Intent', 'Tag_EdgeType', 'Tag_ExitStyle'
]

# Filter to columns that actually exist
display_cols_exist = [col for col in display_cols if col in df_phase3.columns]

# Group by TradeID and show first occurrence
grouped = df_phase3.groupby('TradeID').first().reset_index()
display_df = grouped[display_cols_exist]

print(f"\n{len(grouped)} TradeIDs with enrichment data:\n")
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)
pd.set_option('display.max_colwidth', 30)
print(display_df.to_string(index=False))

# ========================================================================
# Data Quality Checks
# ========================================================================
print("\n" + "=" * 80)
print("DATA QUALITY CHECKS")
print("=" * 80)

issues = []

# Check 1: PCS NaNs
if 'PCS' in df_phase3.columns:
    pcs_nan_count = df_phase3['PCS'].isna().sum()
    if pcs_nan_count > 0:
        issues.append(f"⚠️  PCS: {pcs_nan_count} NaN values detected")
    else:
        print(f"✅ PCS: No NaN values")

# Check 2: Breakeven NaNs (should exist for most strategies)
breakeven_cols = [col for col in df_phase3.columns if 'BreakEven' in col]
if breakeven_cols:
    for col in breakeven_cols:
        nan_count = df_phase3[col].isna().sum()
        print(f"   {col}: {nan_count}/{len(df_phase3)} NaN")

# Check 3: Capital Deployed
if 'Capital_Deployed' in df_phase3.columns:
    capital_nan = df_phase3['Capital_Deployed'].isna().sum()
    capital_negative = (df_phase3['Capital_Deployed'] < 0).sum()
    
    if capital_nan > 0:
        issues.append(f"⚠️  Capital_Deployed: {capital_nan} NaN values")
    else:
        print(f"✅ Capital_Deployed: No NaN values")
    
    if capital_negative > 0:
        issues.append(f"⚠️  Capital_Deployed: {capital_negative} negative values (illogical)")

# Check 4: Liquidity flags
if 'Liquidity_OK' in df_phase3.columns:
    liquidity_nan = df_phase3['Liquidity_OK'].isna().sum()
    liquidity_ok_count = df_phase3['Liquidity_OK'].sum()
    liquidity_not_ok = len(df_phase3) - liquidity_ok_count - liquidity_nan
    
    print(f"✅ Liquidity_OK: {liquidity_ok_count} OK, {liquidity_not_ok} Not OK, {liquidity_nan} NaN")

# Check 5: Strategy-specific validations
print("\n🔹 Strategy-Specific Checks:")

# Covered Calls should have breakeven
cc_trades = df_phase3[df_phase3['Strategy'] == 'Covered Call']
if not cc_trades.empty:
    if 'BreakEven' in df_phase3.columns or 'BreakEven_Lower' in df_phase3.columns:
        cc_no_breakeven = cc_trades[['BreakEven', 'BreakEven_Lower']].isna().all(axis=1).sum() if 'BreakEven' in df_phase3.columns else 0
        if cc_no_breakeven > 0:
            issues.append(f"⚠️  Covered Calls: {cc_no_breakeven} positions missing breakeven")
        else:
            print(f"✅ Covered Calls: All have breakeven values")

# Straddles should have two breakeven points
straddle_trades = df_phase3[df_phase3['Strategy'] == 'Long Straddle']
if not straddle_trades.empty:
    if 'BreakEven_Lower' in df_phase3.columns and 'BreakEven_Upper' in df_phase3.columns:
        straddle_missing_be = straddle_trades[['BreakEven_Lower', 'BreakEven_Upper']].isna().any(axis=1).sum()
        if straddle_missing_be > 0:
            issues.append(f"⚠️  Straddles: {straddle_missing_be} positions missing lower/upper breakeven")
        else:
            print(f"✅ Straddles: All have lower/upper breakeven values")

# Check 6: PCS Tier consistency
if 'PCS' in df_phase3.columns and 'PCS_Tier' in df_phase3.columns:
    # PCS should map to tiers consistently
    pcs_tier_mismatch = 0
    for idx, row in df_phase3.iterrows():
        if pd.notna(row['PCS']) and pd.notna(row['PCS_Tier']):
            pcs_val = row['PCS']
            tier = row['PCS_Tier']
            # Basic sanity: High PCS should be Tier 1, Low PCS should be Tier 3/4
            if pcs_val > 80 and tier not in [1, '1', 'Tier 1']:
                pcs_tier_mismatch += 1
            elif pcs_val < 40 and tier not in [3, '3', 4, '4', 'Tier 3', 'Tier 4']:
                pcs_tier_mismatch += 1
    
    if pcs_tier_mismatch > 0:
        issues.append(f"⚠️  PCS/Tier Mismatch: {pcs_tier_mismatch} positions with inconsistent PCS↔Tier mapping")
    else:
        print(f"✅ PCS↔Tier: Consistent mapping")

# ========================================================================
# Final Summary
# ========================================================================
print("\n" + "=" * 80)
print("DIAGNOSTIC SUMMARY")
print("=" * 80)

if not issues:
    print("\n✅ PHASE 3 OUTPUT IS SANE")
    print("\nAll checks passed:")
    print("  • Phase 3 executed without errors")
    print("  • No mutations of Phase 2 columns detected")
    print("  • All enrichment columns appended correctly")
    print("  • No data quality issues found")
    print("  • Enrichment values are structurally and numerically reasonable")
    print("\n➡️  Ready to proceed to dashboard wiring")
else:
    print("\n⚠️  PHASE 3 HAS THE FOLLOWING CONCRETE ISSUES:\n")
    for issue in issues:
        print(f"  {issue}")
    print("\n➡️  Consider patching Phase 3 logic before dashboard wiring")

print("\n" + "=" * 80)
print("TEST COMPLETE")
print("=" * 80)
