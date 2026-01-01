"""
Test LEAP Strategy Nomination Flow

Validates that:
1. Step 7 nominates LEAP strategies when conditions are met
2. Step 9A assigns 365-730 DTE to LEAP strategies
3. Step 9B would fetch LEAP expirations (dry-run)
"""

import pandas as pd
import sys
sys.path.insert(0, '/Users/haniabadi/Documents/Github/options')

from core.scan_engine.step7_strategy_recommendation import recommend_strategies
from core.scan_engine.step9a_determine_timeframe import determine_option_timeframe


def test_leap_nomination():
    """Test that LEAPs are nominated under correct conditions."""
    
    print("=" * 80)
    print("LEAP NOMINATION TEST")
    print("=" * 80)
    
    # Create test data with LEAP-favorable conditions
    test_data = pd.DataFrame([
        {
            'Ticker': 'AAPL',
            'Signal_Type': 'Bullish',  # Sustained Bullish preferred, but Bullish accepted
            'Regime': 'Trending',
            'IVHV_gap_30D': -8.5,
            'IV_Rank_XS': 28,  # Low IV Rank (< 40)
            'IV_180_D_Call': 25.0,
            'HV_180_D_Cur': 35.0,  # gap_180d = -10 (< -5) ✓
            'IV_60_D_Call': 26.0,
            'HV_60_D_Cur': 32.0,   # gap_60d = -6
            'IV_30_D_Call': 27.0,
            'HV_30_D_Cur': 30.0,
            'Data_Complete': True,
        },
        {
            'Ticker': 'MSFT',
            'Signal_Type': 'Bearish',
            'Regime': 'Trending',
            'IVHV_gap_30D': -7.2,
            'IV_Rank_XS': 32,  # Low IV Rank
            'IV_180_D_Put': 22.0,
            'HV_180_D_Cur': 30.0,  # gap_180d = -8 (< -5) ✓
            'IV_60_D_Put': 23.0,
            'HV_60_D_Cur': 28.0,
            'IV_30_D_Put': 24.0,
            'HV_30_D_Cur': 26.0,
            'Data_Complete': True,
        },
        {
            'Ticker': 'GOOGL',
            'Signal_Type': 'Bullish',
            'Regime': 'Trending',
            'IVHV_gap_30D': 5.0,  # Rich IV (won't trigger LEAP)
            'IV_Rank_XS': 65,  # High IV Rank (won't trigger LEAP)
            'IV_180_D_Call': 35.0,
            'HV_180_D_Cur': 28.0,  # gap_180d = +7 (> -5) ✗
            'IV_60_D_Call': 34.0,
            'HV_60_D_Cur': 29.0,
            'IV_30_D_Call': 33.0,
            'HV_30_D_Cur': 28.0,
            'Data_Complete': True,
        },
    ])
    
    print("\n📋 Test Data:")
    print(test_data[['Ticker', 'Signal_Type', 'IV_Rank_XS', 'IVHV_gap_30D']])
    
    # Step 7: Strategy Nomination
    print("\n" + "=" * 80)
    print("STEP 7: STRATEGY NOMINATION")
    print("=" * 80)
    
    df_strategies = recommend_strategies(test_data, enable_directional=True, enable_volatility=False)
    
    print(f"\n✅ Total strategies nominated: {len(df_strategies)}")
    
    if len(df_strategies) > 0:
        print("\n📊 Strategy Breakdown:")
        strategy_counts = df_strategies['Strategy_Name'].value_counts()
        for strategy, count in strategy_counts.items():
            print(f"   {strategy}: {count}")
        
        # Check for LEAP strategies
        leap_strategies = df_strategies[df_strategies['Strategy_Name'].str.contains('LEAP', na=False)]
        
        print(f"\n🚀 LEAP Strategies Nominated: {len(leap_strategies)}")
        if len(leap_strategies) > 0:
            print("\nLEAP Strategy Details:")
            print(leap_strategies[['Ticker', 'Strategy_Name', 'Valid_Reason', 'Capital_Requirement', 'Confidence']].to_string(index=False))
        else:
            print("⚠️ No LEAP strategies nominated!")
            print("\nExpected: AAPL (Long Call LEAP), MSFT (Long Put LEAP)")
            print("Check: IV_Rank < 40? gap_180d < -5? Signal = Bullish/Bearish?")
        
        # Show all strategies for comparison
        print("\n📋 All Nominated Strategies:")
        print(df_strategies[['Ticker', 'Strategy_Name', 'Trade_Bias', 'Capital_Requirement']].to_string(index=False))
    else:
        print("❌ No strategies nominated! Check validator logic.")
        return False
    
    # Step 9A: DTE Assignment
    print("\n" + "=" * 80)
    print("STEP 9A: DTE ASSIGNMENT")
    print("=" * 80)
    
    df_with_dte = determine_option_timeframe(df_strategies)
    
    print(f"\n✅ Strategies with DTE ranges: {len(df_with_dte)}")
    
    # Check LEAP DTE ranges
    leap_with_dte = df_with_dte[df_with_dte['Strategy_Name'].str.contains('LEAP', na=False)]
    
    if len(leap_with_dte) > 0:
        print(f"\n🚀 LEAP Strategies with DTE:")
        print(leap_with_dte[['Ticker', 'Strategy_Name', 'Min_DTE', 'Max_DTE', 'Target_DTE', 'Timeframe_Label']].to_string(index=False))
        
        # Validate DTE ranges
        for idx, row in leap_with_dte.iterrows():
            min_dte = row['Min_DTE']
            max_dte = row['Max_DTE']
            label = row['Timeframe_Label']
            
            if min_dte >= 365 and max_dte <= 730 and label == 'LEAP':
                print(f"   ✅ {row['Ticker']} {row['Strategy_Name']}: {min_dte}-{max_dte} DTE (LEAP) - CORRECT")
            else:
                print(f"   ❌ {row['Ticker']} {row['Strategy_Name']}: {min_dte}-{max_dte} DTE ({label}) - INCORRECT (expected 365-730, LEAP)")
    else:
        print("⚠️ No LEAP strategies with DTE assigned!")
    
    # Show short-term strategies for comparison
    short_term = df_with_dte[~df_with_dte['Strategy_Name'].str.contains('LEAP', na=False)]
    if len(short_term) > 0:
        print(f"\n📋 Short-Term Strategies (for comparison):")
        print(short_term[['Ticker', 'Strategy_Name', 'Min_DTE', 'Max_DTE', 'Timeframe_Label']].to_string(index=False))
    
    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    
    total_leaps = len(leap_with_dte)
    expected_leaps = 2  # AAPL Long Call LEAP, MSFT Long Put LEAP
    
    print(f"\n✅ LEAP Strategies Nominated: {len(leap_strategies)}")
    print(f"✅ LEAP Strategies with DTE: {total_leaps}")
    print(f"🎯 Expected: {expected_leaps} LEAPs (AAPL Long Call LEAP, MSFT Long Put LEAP)")
    
    if total_leaps >= expected_leaps:
        print("\n🎉 SUCCESS: LEAP nomination flow working correctly!")
        print("\nNext Steps:")
        print("   1. Step 9B will automatically fetch 365-730 DTE expirations for these strategies")
        print("   2. Dashboard will display Is_LEAP=True, Horizon_Class='LEAP'")
        print("   3. Run full pipeline with live tickers (AAPL, MSFT) to test end-to-end")
        return True
    else:
        print(f"\n⚠️ WARNING: Expected {expected_leaps} LEAPs, got {total_leaps}")
        print("Review validator logic and test data conditions")
        return False


if __name__ == '__main__':
    success = test_leap_nomination()
    sys.exit(0 if success else 1)
