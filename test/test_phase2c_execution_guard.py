"""
Test Phase 2C Execution Guard

Demonstrates how to use enforce_validation_gate() to block Phase 3 execution.
"""

import pandas as pd
import sys
sys.path.insert(0, '/Users/haniabadi/Documents/Github/options')

from core.phase2_validate_structures import validate_structures, enforce_validation_gate

print("=" * 70)
print("Phase 2C Execution Guard - Demo")
print("=" * 70)

# === Test 1: Valid structure (should pass) ===
print("\n✅ Test 1: Valid Structure (Strict Mode)")
df_valid = pd.DataFrame({
    "TradeID": ["AAPL_260220_CoveredCall_5376"] * 2,
    "Strategy": ["Covered Call"] * 2,
    "Account": ["Individual - TOD *5376"] * 2,
    "Underlying": ["AAPL"] * 2,
    "AssetType": ["STOCK", "OPTION"],
    "OptionType": [None, "Call"],
    "LegType": ["STOCK", "SHORT_CALL"],
    "Strike": [None, 150.0],
    "Expiration": [None, "2026-02-20"],
    "Quantity": [100, -1],
    "Symbol": ["AAPL", "AAPL260220C150"]
})

df_valid = validate_structures(df_valid)

try:
    df_valid = enforce_validation_gate(df_valid, strict=True)
    print("   ✅ Gate passed - Phase 3 allowed to proceed\n")
except ValueError as e:
    print(f"   ❌ Gate blocked: {e}\n")

# === Test 2: Invalid structure (strict mode - should block) ===
print("=" * 70)
print("❌ Test 2: Invalid Structure (Strict Mode - Should Block)")
print("=" * 70)
df_invalid = pd.DataFrame({
    "TradeID": ["TSLA_260220_CoveredCall_5376"],
    "Strategy": ["Covered Call"],
    "Account": ["Individual - TOD *5376"],
    "Underlying": ["TSLA"],
    "AssetType": ["OPTION"],
    "OptionType": ["Call"],
    "LegType": ["SHORT_CALL"],
    "Strike": [250.0],
    "Expiration": ["2026-02-20"],
    "Quantity": [-1],
    "Symbol": ["TSLA260220C250"]
})

df_invalid = validate_structures(df_invalid)

try:
    df_invalid = enforce_validation_gate(df_invalid, strict=True)
    print("   ⚠️ Gate should have blocked but didn't!\n")
except ValueError as e:
    print("   ✅ Gate correctly blocked Phase 3 execution\n")

# === Test 3: Invalid structure (warning mode - should allow) ===
print("=" * 70)
print("⚠️  Test 3: Invalid Structure (Warning Mode - Should Allow)")
print("=" * 70)
df_invalid2 = pd.DataFrame({
    "TradeID": ["TSLA_260220_CoveredCall_5376"],
    "Strategy": ["Covered Call"],
    "Account": ["Individual - TOD *5376"],
    "Underlying": ["TSLA"],
    "AssetType": ["OPTION"],
    "OptionType": ["Call"],
    "LegType": ["SHORT_CALL"],
    "Strike": [250.0],
    "Expiration": ["2026-02-20"],
    "Quantity": [-1],
    "Symbol": ["TSLA260220C250"]
})

df_invalid2 = validate_structures(df_invalid2)

try:
    df_invalid2 = enforce_validation_gate(df_invalid2, strict=False)
    print("   ✅ Warning mode allowed continuation\n")
except ValueError as e:
    print(f"   ❌ Unexpected block: {e}\n")

print("=" * 70)
print("Demo Complete")
print("=" * 70)
print("\n📋 Usage Pattern:\n")
print("   # Strict enforcement (recommended for production)")
print("   df = phase2_run_all(df)")
print("   df = enforce_validation_gate(df, strict=True)  # Blocks if invalid")
print("   df = phase3_enrich(df)  # Only runs if validation passed\n")
print("   # Warning mode (for debugging)")
print("   df = phase2_run_all(df)")
print("   df = enforce_validation_gate(df, strict=False)  # Warns but continues")
print("   df = phase3_enrich(df)  # Runs even with issues")
