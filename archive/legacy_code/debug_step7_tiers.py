#!/usr/bin/env python3
"""
CLI Diagnostic Tool for Step 7 Tier Enforcement
Run this to see exactly what's happening with strategy generation and tier filtering
"""
import pandas as pd
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

print("=" * 80)
print("STEP 7 TIER ENFORCEMENT DIAGNOSTIC")
print("=" * 80)
print()

# Step 1: Load test data
print("📂 Step 1: Loading Step 6 GEM data...")
try:
    from core.scan_engine.step2_clean import load_and_enrich_snapshot
    from core.scan_engine.step3_enrich import enrich_with_pcs
    from core.scan_engine.step5_chart import add_chart_scoring
    from core.scan_engine.step6_gem import apply_gem_filter
    
    # Load pipeline up to Step 6
    df = load_and_enrich_snapshot()
    print(f"   ✅ Step 2: {len(df)} tickers loaded")
    
    df = enrich_with_pcs(df)
    print(f"   ✅ Step 3: {len(df)} tickers enriched")
    
    df = add_chart_scoring(df)
    print(f"   ✅ Step 5: {len(df)} tickers charted")
    
    df = apply_gem_filter(df)
    print(f"   ✅ Step 6: {len(df)} tickers passed GEM filter")
    print()
    
except Exception as e:
    print(f"   ❌ Failed to load data: {e}")
    print()
    print("Creating minimal test data instead...")
    df = pd.DataFrame({
        'Ticker': ['AAPL', 'MSFT', 'GOOGL'],
        'Signal_Type': ['Bullish', 'Bearish', 'Neutral'],
        'IV_Rank_30D': [65, 45, 70],
        'IVHV_gap_30D': [5.2, -2.1, 8.3],
        'ShortTerm_IV_Edge': [True, False, True],
        'Regime': ['Trending', 'Ranging', 'Ranging'],
        'Crossover_Age_Bucket': ['Age_0_5', 'Age_6_15', 'Age_16_plus']
    })
    print(f"   ⚠️ Using test data: {len(df)} tickers")
    print()

# Step 2: Test get_strategy_tier function
print("🔍 Step 2: Testing get_strategy_tier() function...")
try:
    from core.strategy_tiers import get_strategy_tier
    
    test_strategies = [
        'Long Call',
        'Long Put', 
        'Put Credit Spread',
        'Call Debit Spread',
        'Iron Condor',
        'LEAP Call Debit Spread',
        'Calendar Spread',
        'Invalid Strategy Name'
    ]
    
    for strategy in test_strategies:
        result = get_strategy_tier(strategy)
        tier = result.get('tier', 'ERROR')
        exec_ready = result.get('execution_ready', False)
        print(f"   {strategy:30s} → Tier {tier:3s} | Executable: {exec_ready}")
    print()
    
except Exception as e:
    print(f"   ❌ get_strategy_tier() failed: {e}")
    import traceback
    traceback.print_exc()
    print()

# Step 3: Run Step 7 in DEFAULT mode (Tier-1 only)
print("🎯 Step 3: Running Step 7 in DEFAULT mode (Tier-1 only)...")
try:
    from core.scan_engine.step7_strategy_recommendation import recommend_strategies
    
    df_tier1 = recommend_strategies(
        df.copy(),
        tier_filter='tier1_only',
        exploration_mode=False
    )
    
    print(f"   ✅ Step 7 completed")
    print(f"   Input: {len(df)} tickers")
    print(f"   Output: {len(df_tier1)} strategies")
    print()
    
    if len(df_tier1) > 0:
        print("   📊 Strategy Tier Distribution:")
        if 'Strategy_Tier' in df_tier1.columns:
            tier_counts = df_tier1['Strategy_Tier'].value_counts().sort_index()
            for tier, count in tier_counts.items():
                print(f"      Tier {tier}: {count} strategies")
        else:
            print("      ⚠️ Strategy_Tier column missing!")
        print()
        
        print("   🏷️ EXECUTABLE Flag Distribution:")
        if 'EXECUTABLE' in df_tier1.columns:
            exec_counts = df_tier1['EXECUTABLE'].value_counts()
            for flag, count in exec_counts.items():
                print(f"      EXECUTABLE={flag}: {count} strategies")
        else:
            print("      ⚠️ EXECUTABLE column missing!")
        print()
        
        print("   📋 Sample Strategies (first 5):")
        display_cols = ['Ticker', 'Primary_Strategy', 'Strategy_Tier', 'EXECUTABLE', 'Confidence']
        display_cols = [c for c in display_cols if c in df_tier1.columns]
        print(df_tier1[display_cols].head(5).to_string(index=False))
        print()
        
        print("   🔍 Primary_Strategy Value Counts:")
        if 'Primary_Strategy' in df_tier1.columns:
            strat_counts = df_tier1['Primary_Strategy'].value_counts()
            for strat, count in strat_counts.items():
                print(f"      {strat}: {count}")
        print()
    else:
        print("   ⚠️ NO STRATEGIES OUTPUT! This is the problem.")
        print()
        
except Exception as e:
    print(f"   ❌ Step 7 DEFAULT mode failed: {e}")
    import traceback
    traceback.print_exc()
    print()

# Step 4: Run Step 7 in EXPLORATION mode (all tiers)
print("🔍 Step 4: Running Step 7 in EXPLORATION mode (all tiers)...")
try:
    df_all_tiers = recommend_strategies(
        df.copy(),
        tier_filter='all_tiers',
        exploration_mode=True
    )
    
    print(f"   ✅ Step 7 completed")
    print(f"   Input: {len(df)} tickers")
    print(f"   Output: {len(df_all_tiers)} strategies")
    print()
    
    if len(df_all_tiers) > 0:
        print("   📊 Strategy Tier Distribution:")
        if 'Strategy_Tier' in df_all_tiers.columns:
            tier_counts = df_all_tiers['Strategy_Tier'].value_counts().sort_index()
            for tier, count in tier_counts.items():
                print(f"      Tier {tier}: {count} strategies")
        else:
            print("      ⚠️ Strategy_Tier column missing!")
        print()
        
        print("   🏷️ EXECUTABLE Flag Distribution:")
        if 'EXECUTABLE' in df_all_tiers.columns:
            exec_counts = df_all_tiers['EXECUTABLE'].value_counts()
            for flag, count in exec_counts.items():
                print(f"      EXECUTABLE={flag}: {count} strategies")
        else:
            print("      ⚠️ EXECUTABLE column missing!")
        print()
        
        print("   📋 Sample Strategies (first 5):")
        display_cols = ['Ticker', 'Primary_Strategy', 'Strategy_Tier', 'EXECUTABLE', 'Confidence']
        display_cols = [c for c in display_cols if c in df_all_tiers.columns]
        print(df_all_tiers[display_cols].head(5).to_string(index=False))
        print()
        
        # Compare tier distributions
        if 'Strategy_Tier' in df_all_tiers.columns:
            tier1_in_all = (df_all_tiers['Strategy_Tier'] == 1).sum()
            tier2_in_all = (df_all_tiers['Strategy_Tier'] == 2).sum()
            tier3_in_all = (df_all_tiers['Strategy_Tier'] == 3).sum()
            tier999_in_all = (df_all_tiers['Strategy_Tier'] == 999).sum()
            
            print("   🔢 Comparison:")
            print(f"      Tier-1 only mode output: {len(df_tier1)} strategies")
            print(f"      All tiers mode output: {len(df_all_tiers)} strategies")
            print(f"         → Tier-1: {tier1_in_all}")
            print(f"         → Tier-2: {tier2_in_all}")
            print(f"         → Tier-3: {tier3_in_all}")
            print(f"         → Tier-999 (unknown): {tier999_in_all}")
            print()
    else:
        print("   ⚠️ NO STRATEGIES OUTPUT in exploration mode either!")
        print()
        
except Exception as e:
    print(f"   ❌ Step 7 EXPLORATION mode failed: {e}")
    import traceback
    traceback.print_exc()
    print()

# Step 5: Check dtypes
print("🔍 Step 5: Checking DataFrame dtypes...")
try:
    if len(df_tier1) > 0:
        print("   DEFAULT mode dtypes:")
        strategy_cols = ['Strategy_Tier', 'EXECUTABLE', 'Primary_Strategy', 'Confidence']
        for col in strategy_cols:
            if col in df_tier1.columns:
                dtype = df_tier1[col].dtype
                print(f"      {col:20s}: {dtype}")
            else:
                print(f"      {col:20s}: MISSING")
        print()
    
    if len(df_all_tiers) > 0:
        print("   EXPLORATION mode dtypes:")
        for col in strategy_cols:
            if col in df_all_tiers.columns:
                dtype = df_all_tiers[col].dtype
                print(f"      {col:20s}: {dtype}")
            else:
                print(f"      {col:20s}: MISSING")
        print()
        
except Exception as e:
    print(f"   ⚠️ Could not check dtypes: {e}")
    print()

# Step 6: Summary
print("=" * 80)
print("DIAGNOSTIC SUMMARY")
print("=" * 80)

if len(df_tier1) == 0 and len(df_all_tiers) == 0:
    print("❌ CRITICAL ISSUE: No strategies generated in either mode")
    print()
    print("Possible causes:")
    print("  1. Input data missing required columns")
    print("  2. Strategy generation logic not matching any conditions")
    print("  3. All strategies filtered out before tier assignment")
    print()
    print("Check the logs above for errors in strategy generation")
    
elif len(df_tier1) == 0 and len(df_all_tiers) > 0:
    tier1_count = (df_all_tiers['Strategy_Tier'] == 1).sum() if 'Strategy_Tier' in df_all_tiers.columns else 0
    if tier1_count == 0:
        print("⚠️ ISSUE: No Tier-1 strategies exist")
        print()
        print("All generated strategies are Tier-2/3 (non-executable)")
        print("This means:")
        print("  - Strategy recommendations are working")
        print("  - But all recommended strategies require broker approval or system development")
        print()
        print("Solutions:")
        print("  1. Use exploration mode to see all strategies")
        print("  2. Check why no Tier-1 strategies are being recommended")
        print("  3. Verify strategy_tiers.py has correct tier assignments")
    else:
        print("❌ BUG: Tier-1 filtering is broken")
        print()
        print(f"Exploration mode shows {tier1_count} Tier-1 strategies exist")
        print(f"But default mode output is empty")
        print("This is a filtering bug in the tier enforcement code")
        
else:
    print("✅ SUCCESS: Tier enforcement is working correctly")
    print()
    tier1_in_default = len(df_tier1)
    tier1_in_all = (df_all_tiers['Strategy_Tier'] == 1).sum() if 'Strategy_Tier' in df_all_tiers.columns else 0
    
    if tier1_in_default == tier1_in_all:
        print(f"Default mode: {tier1_in_default} Tier-1 strategies (correct)")
        print(f"Exploration mode: {len(df_all_tiers)} total strategies")
        print()
        print("The tier filtering is working as expected!")
    else:
        print(f"⚠️ Mismatch: Default={tier1_in_default}, Tier-1 in exploration={tier1_in_all}")

print()
print("=" * 80)
print("Run this script to diagnose tier enforcement issues")
print("=" * 80)
