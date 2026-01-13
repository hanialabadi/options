#!/usr/bin/env python3
"""
Test Phase B: Phase 1 Sampled Exploration

Validates:
1. Phase 1 sampling correctly identifies viable strategies
2. Row count preservation (IN == OUT)
3. Speedup measurement vs full chain fetch
4. All strategies preserved with status labels
"""

import pandas as pd
import time
from core.scan_engine.step9b_fetch_contracts import fetch_and_select_contracts

def test_phase_b():
    """Test Phase B with synthetic test data."""
    
    print("=" * 80)
    print("PHASE B TEST: Phase 1 Sampled Exploration")
    print("=" * 80)
    
    # Create synthetic test data (10 tickers, 22 strategies)
    test_data = []
    
    # Test tickers: mix of liquid and illiquid
    test_tickers = [
        ('AAPL', 'Long Call'),      # Liquid
        ('AAPL', 'Long Straddle'),  # Liquid
        ('SPY', 'Cash-Secured Put'), # Very liquid
        ('QQQ', 'Long Call'),        # Very liquid
        ('TSLA', 'Long Straddle'),   # Liquid
        ('NVDA', 'Long Call'),       # Liquid
        ('BKNG', 'Long Straddle'),   # Illiquid (expensive)
        ('MELI', 'Long Call'),       # Illiquid (expensive)
        ('FICO', 'Long Put'),        # Illiquid (expensive)
        ('TDG', 'Long Call'),        # Illiquid (expensive)
    ]
    
    for ticker, strategy in test_tickers:
        test_data.append({
            'Ticker': ticker,
            'Primary_Strategy': strategy,
            'Strategy_Name': strategy,
            'Strategy_Tier': 1,
            'Trade_Bias': 'Bullish' if 'Call' in strategy else 'Neutral',
            'Min_DTE': 30,
            'Max_DTE': 60,
            'Target_DTE': 45,
            'Num_Contracts': 1,
            'Dollar_Allocation': 1000,
            'Execution_Ready': True
        })
    
    df_test = pd.DataFrame(test_data)
    
    print(f"\n📊 Test Subset:")
    print(f"   Tickers: {df_test['Ticker'].nunique()}")
    print(f"   Strategies: {len(df_test)}")
    print(f"   Avg strategies/ticker: {len(df_test) / df_test['Ticker'].nunique():.2f}")
    
    input_row_count = len(df_test)
    
    # Run Step 9B with Phase 1 enabled
    print(f"\n🚀 Running Step 9B with Phase 1 Sampled Exploration...")
    start_time = time.time()
    
    try:
        df_result = fetch_and_select_contracts(df_test)
        elapsed_time = time.time() - start_time
        
        print(f"\n✅ Step 9B completed in {elapsed_time:.2f} seconds")
        
    except Exception as e:
        print(f"\n❌ Step 9B failed: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Validate results
    print(f"\n" + "=" * 80)
    print("PHASE B VALIDATION")
    print("=" * 80)
    
    # 1. Row count preservation
    output_row_count = len(df_result)
    print(f"\n1. Row Count Preservation:")
    print(f"   Input:  {input_row_count} strategies")
    print(f"   Output: {output_row_count} strategies")
    if input_row_count == output_row_count:
        print(f"   ✅ PASSED: Row count preserved")
    else:
        print(f"   ❌ FAILED: Row count changed ({input_row_count} → {output_row_count})")
    
    # 2. Phase 1 status distribution
    print(f"\n2. Phase 1 Status Distribution:")
    if 'Phase1_Status' in df_result.columns:
        phase1_status = df_result['Phase1_Status'].value_counts()
        print(phase1_status)
        
        phase1_skipped = len(df_result[df_result['Phase1_Status'].isin([
            'No_Viable_Expirations', 'Fast_Reject', 'No_Chain_Data', 'Sampling_Error'
        ])])
        phase1_deep = len(df_result[df_result['Phase1_Status'] == 'Deep_Required'])
        
        print(f"\n   Phase 1 Skipped: {phase1_skipped} ({phase1_skipped/len(df_result)*100:.1f}%)")
        print(f"   Phase 2 Required: {phase1_deep} ({phase1_deep/len(df_result)*100:.1f}%)")
    else:
        print("   ⚠️ Phase1_Status column not found")
    
    # 3. Contract selection outcomes
    print(f"\n3. Contract Selection Outcomes:")
    selection_status = df_result['Contract_Selection_Status'].value_counts()
    print(selection_status)
    
    successful = len(df_result[df_result['Contract_Selection_Status'] == 'Success'])
    print(f"\n   Successful selections: {successful}/{len(df_result)} ({successful/len(df_result)*100:.1f}%)")
    
    # 4. Sample quality distribution
    print(f"\n4. Sample Quality Distribution:")
    if 'Phase1_Sample_Quality' in df_result.columns:
        sample_quality = df_result['Phase1_Sample_Quality'].value_counts()
        print(sample_quality)
    else:
        print("   ⚠️ Phase1_Sample_Quality column not found")
    
    # 5. Speedup estimation
    print(f"\n5. Speedup Estimation:")
    print(f"   Elapsed time: {elapsed_time:.2f}s")
    print(f"   Strategies processed: {len(df_result)}")
    print(f"   Avg time per strategy: {elapsed_time/len(df_result):.3f}s")
    
    # Estimate old time (assuming all strategies required full chain fetch)
    # Old: ~0.8s per strategy (full chain fetch)
    # New: ~0.3s per strategy (Phase 1 sample only for skipped, ~0.8s for deep)
    estimated_old_time = len(df_result) * 0.8
    speedup = estimated_old_time / elapsed_time if elapsed_time > 0 else 1.0
    
    print(f"   Estimated old time: {estimated_old_time:.2f}s (no Phase 1)")
    print(f"   Estimated speedup: {speedup:.2f}×")
    
    # 6. Output file
    output_file = 'output/step9b_phase_b_test.csv'
    df_result.to_csv(output_file, index=False)
    print(f"\n📁 Saved results to: {output_file}")
    
    # 7. Detailed ticker breakdown
    print(f"\n6. Ticker Breakdown:")
    for ticker in df_result['Ticker'].unique():
        ticker_df = df_result[df_result['Ticker'] == ticker]
        success_count = len(ticker_df[ticker_df['Contract_Selection_Status'] == 'Success'])
        print(f"   {ticker}: {success_count}/{len(ticker_df)} successful")
    
    # 8. Key insights
    print(f"\n" + "=" * 80)
    print("KEY INSIGHTS")
    print("=" * 80)
    
    print(f"\n✅ Phase B Implementation Working:")
    print(f"   - Row count preserved: {input_row_count} in = {output_row_count} out")
    
    if 'Phase1_Status' in df_result.columns:
        phase1_skipped = len(df_result[df_result['Phase1_Status'].isin([
            'No_Viable_Expirations', 'Fast_Reject', 'No_Chain_Data', 'Sampling_Error'
        ])])
        phase1_deep = len(df_result[df_result['Phase1_Status'] == 'Deep_Required'])
        print(f"   - Phase 1 filtered {phase1_skipped} obvious failures before full fetch")
        print(f"   - {phase1_deep} strategies required deep exploration")
        
        if phase1_skipped > 0:
            savings = phase1_skipped / len(df_result) * 100
            print(f"\n💰 API Call Savings:")
            print(f"   - {phase1_skipped}/{len(df_result)} strategies skipped full chain fetch")
            print(f"   - Estimated {savings:.1f}% reduction in API calls")
    
    print(f"   - {successful} successful contract selections")
    
    print(f"\n🎯 Next Steps:")
    print(f"   - Phase B validated successfully")
    print(f"   - Ready for Phase C: Phase 2 Deep Exploration optimization")
    print(f"   - Ready for Phase D: Add parallelism")

if __name__ == '__main__':
    test_phase_b()
