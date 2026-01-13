"""
Test Phase 2C Structural Validation

This script demonstrates the validation gate catching various structural violations.
"""

import pandas as pd
import sys
sys.path.insert(0, '/Users/haniabadi/Documents/Github/options')

from core.phase2_validate_structures import validate_structures

print("=" * 70)
print("Phase 2C Validation Gate - Test Cases")
print("=" * 70)

# === Test Case 1: Valid Covered Call ===
print("\n✅ Test 1: Valid Covered Call")
df1 = pd.DataFrame({
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

result1 = validate_structures(df1)
print(f"   Structure Valid: {result1['Structure_Valid'].all()}")
print(f"   Errors: {result1['Validation_Errors'].iloc[0] or 'None'}")

# === Test Case 2: Covered Call Missing Stock ===
print("\n❌ Test 2: Covered Call Missing Stock")
df2 = pd.DataFrame({
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

result2 = validate_structures(df2)
print(f"   Structure Valid: {result2['Structure_Valid'].all()}")
print(f"   Errors: {result2['Validation_Errors'].iloc[0]}")
print(f"   Needs Fix: {result2['Needs_Structural_Fix'].iloc[0]}")

# === Test Case 3: Cross-Account Trade ===
print("\n❌ Test 3: Cross-Account TradeID")
df3 = pd.DataFrame({
    "TradeID": ["INTC_260220_CoveredCall_MIXED"] * 2,
    "Strategy": ["Covered Call"] * 2,
    "Account": ["Individual - TOD *5376", "ROTH IRA *4854"],  # Different accounts!
    "Underlying": ["INTC"] * 2,
    "AssetType": ["STOCK", "OPTION"],
    "OptionType": [None, "Call"],
    "LegType": ["STOCK", "SHORT_CALL"],
    "Strike": [None, 30.0],
    "Expiration": [None, "2026-02-20"],
    "Quantity": [100, -1],
    "Symbol": ["INTC", "INTC260220C30"]
})

result3 = validate_structures(df3)
print(f"   Structure Valid: {result3['Structure_Valid'].all()}")
print(f"   Errors: {result3['Validation_Errors'].iloc[0]}")
print(f"   Needs Fix: {result3['Needs_Structural_Fix'].iloc[0]}")

# === Test Case 4: Straddle with Different Strikes ===
print("\n❌ Test 4: Straddle with Mismatched Strikes")
df4 = pd.DataFrame({
    "TradeID": ["SHOP_250117_LongStraddle_5376"] * 2,
    "Strategy": ["Long Straddle"] * 2,
    "Account": ["Individual - TOD *5376"] * 2,
    "Underlying": ["SHOP"] * 2,
    "AssetType": ["OPTION"] * 2,
    "OptionType": ["Call", "Put"],
    "LegType": ["Call-Leg", "Put-Leg"],
    "Strike": [165.0, 160.0],  # Different strikes!
    "Expiration": ["2025-01-17"] * 2,
    "Quantity": [1, 1],
    "Symbol": ["SHOP250117C165", "SHOP250117P160"]
})

result4 = validate_structures(df4)
print(f"   Structure Valid: {result4['Structure_Valid'].all()}")
print(f"   Errors: {result4['Validation_Errors'].iloc[0]}")
print(f"   Needs Fix: {result4['Needs_Structural_Fix'].iloc[0]}")

# === Test Case 5: CSP with Extra Stock ===
print("\n❌ Test 5: Cash-Secured Put with Stock")
df5 = pd.DataFrame({
    "TradeID": ["UUUU_260206_CSP_4854"] * 2,
    "Strategy": ["Cash-Secured Put"] * 2,
    "Account": ["ROTH IRA *4854"] * 2,
    "Underlying": ["UUUU"] * 2,
    "AssetType": ["OPTION", "STOCK"],  # Should not have stock!
    "OptionType": ["Put", None],
    "LegType": ["Put-Leg", "STOCK"],
    "Strike": [14.0, None],
    "Expiration": ["2026-02-06", None],
    "Quantity": [-1, 100],
    "Symbol": ["UUUU260206P14", "UUUU"]
})

result5 = validate_structures(df5)
print(f"   Structure Valid: {result5['Structure_Valid'].all()}")
print(f"   Errors: {result5['Validation_Errors'].iloc[0]}")
print(f"   Needs Fix: {result5['Needs_Structural_Fix'].iloc[0]}")

# === Test Case 6: Duplicate Symbol ===
print("\n❌ Test 6: Duplicate Symbol in TradeID")
df6 = pd.DataFrame({
    "TradeID": ["NVDA_280121_BuyCall_5376"] * 2,
    "Strategy": ["Buy Call"] * 2,
    "Account": ["Individual - TOD *5376"] * 2,
    "Underlying": ["NVDA"] * 2,
    "AssetType": ["OPTION"] * 2,
    "OptionType": ["Call"] * 2,
    "LegType": ["Call-Leg"] * 2,
    "Strike": [800.0] * 2,
    "Expiration": ["2028-01-21"] * 2,
    "Quantity": [1, 1],
    "Symbol": ["NVDA280121C800", "NVDA280121C800"]  # Duplicate!
})

result6 = validate_structures(df6)
print(f"   Structure Valid: {result6['Structure_Valid'].all()}")
print(f"   Errors: {result6['Validation_Errors'].iloc[0]}")
print(f"   Needs Fix: {result6['Needs_Structural_Fix'].iloc[0]}")

print("\n" + "=" * 70)
print("Test Suite Complete")
print("=" * 70)
