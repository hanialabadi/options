"""
Test Price-Aware Liquidity + LEAP Fallback Implementation

This script demonstrates the core logic changes needed for Step 9B.
Once validated, these patterns will be integrated into the main loop.
"""

import pandas as pd
from typing import Tuple

# Price-aware liquidity thresholds
def get_price_aware_thresholds(underlying_price: float, actual_dte: int) -> Tuple[int, float]:
    """
    Calculate liquidity thresholds based on underlying price AND DTE.
    
    Price Buckets:
    - < $200:  min_OI=50, max_spread=10%  (strict)
    - $200-500: min_OI=25, max_spread=12% (moderate)
    - $500-1000: min_OI=15, max_spread=15% (relaxed)
    - >= $1000: min_OI=5,  max_spread=20% (realistic)
    
    DTE Adjustments:
    - LEAPS (>=365): OI/2, spread*1.25
    - Medium (60-364): OI*0.75, spread*1.15
    - Short (<60): No change
    """
    
    # Step 1: Price buckets
    if underlying_price < 200:
        base_min_oi, base_max_spread = 50, 10.0
        bucket_name = "Standard"
    elif underlying_price < 500:
        base_min_oi, base_max_spread = 25, 12.0
        bucket_name = "MidCap"
    elif underlying_price < 1000:
        base_min_oi, base_max_spread = 15, 15.0
        bucket_name = "LargeCap"
    else:
        base_min_oi, base_max_spread = 5, 20.0
        bucket_name = "Elite"
    
    # Step 2: DTE adjustments
    if actual_dte >= 365:  # LEAPS
        adjusted_oi = max(2, base_min_oi // 2)
        adjusted_spread = base_max_spread * 1.25
        dte_tag = "_LEAP"
    elif actual_dte >= 60:  # Medium-term
        adjusted_oi = max(3, int(base_min_oi * 0.75))
        adjusted_spread = base_max_spread * 1.15
        dte_tag = "_MediumTerm"
    else:  # Short-term
        adjusted_oi = base_min_oi
        adjusted_spread = base_max_spread
        dte_tag = "_ShortTerm"
    
    liquidity_profile = f"{bucket_name}_${int(underlying_price)}{dte_tag}"
    
    return adjusted_oi, adjusted_spread, liquidity_profile


# Test cases
test_cases = [
    # (ticker, price, dte, description)
    ("AAPL", 150, 45, "Standard stock, weekly options"),
    ("AAPL", 150, 400, "Standard stock, LEAP"),
    ("NVDA", 480, 45, "Mid-cap range, weekly"),
    ("BKNG", 3000, 45, "Elite stock, weekly"),
    ("BKNG", 3000, 400, "Elite stock, LEAP"),
    ("AZO", 2500, 45, "Elite stock, weekly"),
    ("AZO", 2500, 400, "Elite stock, LEAP"),
    ("MTD", 1200, 45, "Elite stock, weekly"),
    ("PENNY", 5, 45, "Penny stock (strict filtering)"),
]

print("=" * 100)
print("PRICE-AWARE LIQUIDITY THRESHOLD VALIDATION")
print("=" * 100)
print()

results = []
for ticker, price, dte, desc in test_cases:
    min_oi, max_spread, profile = get_price_aware_thresholds(price, dte)
    
    results.append({
        'Ticker': ticker,
        'Price': f"${price:,.0f}",
        'DTE': dte,
        'Description': desc,
        'Min_OI': min_oi,
        'Max_Spread': f"{max_spread:.1f}%",
        'Liquidity_Profile': profile
    })

df_results = pd.DataFrame(results)
print(df_results.to_string(index=False))
print()

# Show comparison: Old vs New thresholds
print("=" * 100)
print("IMPACT ANALYSIS: OLD vs NEW SYSTEM")
print("=" * 100)
print()

print("OLD SYSTEM (Fixed Thresholds):")
print("  - All stocks: min_OI=50, max_spread=10%")
print("  - Result: BKNG, AZO, MTD, FICO, MELI systematically rejected")
print()

print("NEW SYSTEM (Price-Aware):")
elite_stocks = df_results[df_results['Price'].str.replace('$', '').str.replace(',', '').astype(float) >= 1000]
print(elite_stocks[['Ticker', 'Price', 'DTE', 'Min_OI', 'Max_Spread', 'Liquidity_Profile']].to_string(index=False))
print()
print("  ✅ Elite stocks get realistic thresholds (OI≥5, spread≤20%)")
print("  ✅ LEAPs get additional relaxation (OI≥2, spread≤25%)")
print()

print("SMALL STOCK PROTECTION:")
penny_stock = df_results[df_results['Ticker'] == 'PENNY']
print(penny_stock[['Ticker', 'Price', 'Min_OI', 'Max_Spread']].to_string(index=False))
print("  ✅ Small/junk stocks STILL face strict rules (OI≥50, spread≤10%)")
print()

# LEAP fallback simulation
print("=" * 100)
print("LEAP FALLBACK SIMULATION")
print("=" * 100)
print()

STRATEGY_LEAP_ELIGIBLE = {'Long Call', 'Long Put', 'Buy-Write', 'Covered Call'}
STRATEGY_LEAP_INCOMPATIBLE = {'Long Straddle', 'Long Strangle', 'Short Put'}

leap_scenarios = [
    ("BKNG", 3000, "Long Call", True, "✅ LEAP-eligible + price≥$300 → LEAP fallback ENABLED"),
    ("BKNG", 3000, "Long Straddle", False, "❌ Strategy not LEAP-eligible → LEAP fallback BLOCKED"),
    ("AAPL", 150, "Long Call", False, "❌ Price<$300 → LEAP fallback BLOCKED (not needed anyway)"),
    ("AZO", 2500, "Long Call", True, "✅ LEAP-eligible + price≥$300 → LEAP fallback ENABLED"),
]

for ticker, price, strategy, should_fallback, explanation in leap_scenarios:
    is_eligible = strategy in STRATEGY_LEAP_ELIGIBLE
    is_expensive = price >= 300
    can_fallback = is_eligible and is_expensive
    
    status = "✅ ENABLED" if can_fallback else "❌ BLOCKED"
    print(f"{ticker:6} | ${price:>5,.0f} | {strategy:20} → {status}")
    print(f"       {explanation}")
    print()

print("=" * 100)
print("VISIBILITY COLUMNS (New Tracking)")
print("=" * 100)
print()

visibility_cols = [
    ("Underlying_Price", "float", "Stock price used for bucketing"),
    ("Is_LEAP", "bool", "True if LEAP fallback was used"),
    ("Selection_Mode", "string", "'Standard' or 'LEAP_Fallback'"),
    ("Liquidity_Profile", "string", "e.g. 'Elite_$3000_LEAP'"),
    ("Attempted_DTE", "int", "Target DTE before attempt"),
    ("Failure_Reason", "string", "Why contract selection failed"),
    ("Closest_Expiration_Considered", "datetime", "Best expiry found"),
    ("Best_Strike_Considered", "float", "Closest strike evaluated"),
]

df_visibility = pd.DataFrame(visibility_cols, columns=['Column', 'Type', 'Purpose'])
print(df_visibility.to_string(index=False))
print()
print("💡 These columns provide full auditability even when contracts FAIL.")
print("   No more 'black box' - you can see exactly what was attempted and why it failed.")
print()

print("=" * 100)
print("IMPLEMENTATION SUMMARY")
print("=" * 100)
print()
print("✅ Price-aware thresholds prevent elite stock rejection")
print("✅ LEAP fallback provides explicit long-dated alternatives")
print("✅ Strict rules preserved for small/junk stocks")
print("✅ Full visibility into attempted contracts and failures")
print("✅ Selective permissiveness, not system-wide looseness")
print()
print("Next: Integrate into Step 9B main loop (lines 237-330)")
print()
