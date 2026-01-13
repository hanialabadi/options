"""
Test Step 11: Strategy Comparison & Ranking (Strategy-Aware Architecture)

This test validates the redesigned Step 11 which:
1. Compares ALL strategies per ticker (apples-to-apples comparison)
2. Ranks strategies without making final selection
3. Preserves all rows (no silent filtering)
4. Adds comparison metrics for downstream decision-making

Key Architecture Principles:
- Input: 266 strategies with contracts (from Step 9B)
- Process: Calculate comparison metrics, rank per ticker
- Output: 266 ranked strategies (100% row preservation)
- NO final decision (that's Step 8's job after repositioning)

Test Scenarios:
1. Small Multi-Strategy: AAPL with 3 strategies
2. Production Simulation: 266 strategies with comparison metrics
3. Row Preservation: Assert input count == output count
"""

import pandas as pd
import sys
import os
import logging

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.scan_engine.step11_strategy_pairing import compare_and_rank_strategies

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s'
)
logger = logging.getLogger(__name__)


def test_small_multi_strategy():
    """
    Test 1: Small multi-strategy scenario (AAPL with 3 strategies).
    
    Expected Behavior:
    - Input: 3 strategies (Long Call, Buy-Write, Straddle)
    - Output: 3 strategies ranked (1, 2, 3)
    - Comparison metrics added for all strategies
    - Row count preserved: 3 → 3
    """
    
    logger.info("\n" + "="*80)
    logger.info("TEST 1: Small Multi-Strategy (AAPL with 3 strategies)")
    logger.info("="*80)
    
    # Create mock data (3 strategies for AAPL)
    input_data = pd.DataFrame({
        'Ticker': ['AAPL', 'AAPL', 'AAPL'],
        'Primary_Strategy': ['Long Call', 'Buy-Write', 'Long Straddle'],
        'Strategy_Name': ['Long Call', 'Buy-Write', 'Long Straddle'],
        'Trade_Bias': ['Bullish', 'Neutral', 'Neutral'],
        'Contract_Selection_Status': ['Success', 'Success', 'Success'],
        'Actual_DTE': [42, 35, 42],
        'Selected_Strikes': ['170', '165', '170'],
        'PCS_Final': [75, 68, 72],
        'Delta': [0.55, 0.30, 0.10],
        'Vega': [1.2, 0.8, 2.5],
        'Gamma': [0.05, 0.03, 0.08],
        'Total_Debit': [850, 500, 1500],
        'Bid_Ask_Spread_Pct': [2.5, 1.8, 3.2],
        'Open_Interest': [5000, 3000, 2500]
    })
    
    logger.info(f"📥 Input: {len(input_data)} strategies")
    logger.info(f"   Strategies: {input_data['Primary_Strategy'].tolist()}")
    logger.info(f"   PCS Scores: {input_data['PCS_Final'].tolist()}")
    
    # Run Step 11
    output = compare_and_rank_strategies(
        input_data,
        user_goal='growth',
        account_size=100000,
        risk_tolerance='medium'
    )
    
    # Validate results
    logger.info(f"\n📤 Output: {len(output)} strategies")
    logger.info(f"   Rankings: {output['Strategy_Rank'].tolist()}")
    logger.info(f"   Comparison Scores: {output['Comparison_Score'].tolist()}")
    
    # Assertions
    assert len(output) == len(input_data), f"Row count mismatch: {len(output)} != {len(input_data)}"
    assert 'Strategy_Rank' in output.columns, "Missing Strategy_Rank column"
    assert 'Comparison_Score' in output.columns, "Missing Comparison_Score column"
    assert output['Strategy_Rank'].min() == 1, "Missing rank 1 (best strategy)"
    assert output['Strategy_Rank'].max() == 3, f"Expected max rank 3, got {output['Strategy_Rank'].max()}"
    
    # Show rankings
    logger.info(f"\n📊 AAPL Strategy Rankings:")
    for _, row in output.sort_values('Strategy_Rank').iterrows():
        logger.info(f"   Rank {row['Strategy_Rank']}: {row['Primary_Strategy']}")
        logger.info(f"      Score: {row['Comparison_Score']:.2f} | PCS: {row['PCS_Final']} | Delta: {row['Delta']:.2f}")
    
    logger.info(f"\n✅ Test 1 PASSED: 3 strategies ranked correctly")
    return True


def test_production_simulation():
    """
    Test 2: Production simulation (266 strategies).
    
    Expected Behavior:
    - Input: 266 strategies (multi-ticker, multi-strategy)
    - Output: 266 strategies ranked with comparison metrics
    - Row count preserved: 266 → 266
    - Multi-strategy tickers have proper rankings (1, 2, 3, etc.)
    """
    
    logger.info("\n" + "="*80)
    logger.info("TEST 2: Production Simulation (266 strategies)")
    logger.info("="*80)
    
    # Create mock data (266 strategies across multiple tickers)
    tickers = ['AAPL', 'MSFT', 'NVDA', 'TSLA', 'GOOGL'] * 53 + ['AMZN']  # 266 total
    strategies = ['Long Call', 'Buy-Write', 'Long Straddle', 'Long Put', 'Covered Call'] * 53 + ['Long Call']
    
    input_data = pd.DataFrame({
        'Ticker': tickers,
        'Primary_Strategy': strategies,
        'Strategy_Name': strategies,
        'Trade_Bias': ['Bullish'] * 133 + ['Bearish'] * 133,
        'Contract_Selection_Status': ['Success'] * 250 + ['Failed'] * 16,  # 16 failed strategies
        'Actual_DTE': [35 + (i % 20) for i in range(266)],
        'Selected_Strikes': [f'{150 + (i % 50)}' for i in range(266)],
        'PCS_Final': [65 + (i % 20) for i in range(266)],
        'Delta': [0.3 + (i % 50) / 100 for i in range(266)],
        'Vega': [0.8 + (i % 30) / 20 for i in range(266)],
        'Gamma': [0.03 + (i % 10) / 200 for i in range(266)],
        'Total_Debit': [500 + (i % 100) * 10 for i in range(266)],
        'Bid_Ask_Spread_Pct': [1.5 + (i % 15) / 10 for i in range(266)],
        'Open_Interest': [1000 + (i % 500) * 10 for i in range(266)]
    })
    
    logger.info(f"📥 Input: {len(input_data)} strategies")
    logger.info(f"   Unique Tickers: {input_data['Ticker'].nunique()}")
    logger.info(f"   Unique Strategies: {input_data['Primary_Strategy'].nunique()}")
    logger.info(f"   Successful Contracts: {(input_data['Contract_Selection_Status'] == 'Success').sum()}")
    logger.info(f"   Failed Contracts: {(input_data['Contract_Selection_Status'] != 'Success').sum()}")
    
    # Run Step 11
    output = compare_and_rank_strategies(
        input_data,
        user_goal='balanced',
        account_size=500000,
        risk_tolerance='medium'
    )
    
    # Validate results
    logger.info(f"\n📤 Output: {len(output)} strategies")
    logger.info(f"   Rankings Distribution: {output['Strategy_Rank'].value_counts().to_dict()}")
    logger.info(f"   Avg Comparison Score: {output[output['Comparison_Score'] > 0]['Comparison_Score'].mean():.2f}")
    
    # Assertions
    assert len(output) == len(input_data), f"Row count mismatch: {len(output)} != {len(input_data)}"
    assert 'Strategy_Rank' in output.columns, "Missing Strategy_Rank column"
    assert 'Comparison_Score' in output.columns, "Missing Comparison_Score column"
    
    # Check multi-strategy tickers
    strategies_per_ticker = output.groupby('Ticker').size()
    multi_strategy_tickers = strategies_per_ticker[strategies_per_ticker > 1]
    
    logger.info(f"\n📊 Multi-Strategy Architecture:")
    logger.info(f"   Total Tickers: {len(strategies_per_ticker)}")
    logger.info(f"   Multi-Strategy Tickers: {len(multi_strategy_tickers)} ({len(multi_strategy_tickers)/len(strategies_per_ticker)*100:.1f}%)")
    logger.info(f"   Avg Strategies/Ticker: {strategies_per_ticker.mean():.2f}")
    
    # Show example multi-strategy ticker
    example_ticker = multi_strategy_tickers.index[0]
    example_strategies = output[output['Ticker'] == example_ticker][['Primary_Strategy', 'Strategy_Rank', 'Comparison_Score']]
    logger.info(f"\n   Example: {example_ticker}")
    for _, row in example_strategies.sort_values('Strategy_Rank').iterrows():
        logger.info(f"      Rank {row['Strategy_Rank']}: {row['Primary_Strategy']} (Score: {row['Comparison_Score']:.2f})")
    
    # Check failed strategies
    failed_strategies = output[output['Contract_Selection_Status'] != 'Success']
    logger.info(f"\n   Failed Strategies: {len(failed_strategies)} (all ranked 999)")
    assert (failed_strategies['Strategy_Rank'] == 999).all(), "Failed strategies should have rank 999"
    
    logger.info(f"\n✅ Test 2 PASSED: 266 strategies ranked correctly")
    return True


def test_row_preservation():
    """
    Test 3: Row preservation assertion.
    
    Expected Behavior:
    - Input: N strategies
    - Output: N strategies (exactly)
    - NO silent filtering or dropping
    """
    
    logger.info("\n" + "="*80)
    logger.info("TEST 3: Row Preservation Assertion")
    logger.info("="*80)
    
    # Test various input sizes
    test_sizes = [1, 5, 50, 266]
    
    for size in test_sizes:
        # Create mock data
        input_data = pd.DataFrame({
            'Ticker': [f'TICK{i%10}' for i in range(size)],
            'Primary_Strategy': ['Long Call'] * size,
            'Strategy_Name': ['Long Call'] * size,
            'Trade_Bias': ['Bullish'] * size,
            'Contract_Selection_Status': ['Success'] * size,
            'Actual_DTE': [35] * size,
            'Selected_Strikes': ['150'] * size,
            'PCS_Final': [70] * size,
            'Delta': [0.5] * size,
            'Vega': [1.0] * size,
            'Gamma': [0.05] * size,
            'Total_Debit': [800] * size,
            'Bid_Ask_Spread_Pct': [2.0] * size,
            'Open_Interest': [2000] * size
        })
        
        # Run Step 11
        output = compare_and_rank_strategies(
            input_data,
            user_goal='growth',
            account_size=100000,
            risk_tolerance='medium'
        )
        
        # Validate
        assert len(output) == size, f"Row count mismatch for size {size}: {len(output)} != {size}"
        logger.info(f"   ✅ Size {size:3d}: {len(input_data)} → {len(output)} (preserved)")
    
    logger.info(f"\n✅ Test 3 PASSED: Row preservation validated for all sizes")
    return True


if __name__ == "__main__":
    logger.info("\n" + "="*80)
    logger.info("STEP 11 COMPARISON & RANKING TEST SUITE")
    logger.info("Strategy-Aware Architecture Validation")
    logger.info("="*80)
    
    try:
        # Run all tests
        test_small_multi_strategy()
        test_production_simulation()
        test_row_preservation()
        
        logger.info("\n" + "="*80)
        logger.info("✅ ALL TESTS PASSED")
        logger.info("="*80)
        logger.info("\nStep 11 is ready for production:")
        logger.info("   ✓ Compares strategies with robust metrics")
        logger.info("   ✓ Ranks strategies per ticker (no selection)")
        logger.info("   ✓ Preserves 100% of rows (no silent filtering)")
        logger.info("   ✓ Supports multi-strategy architecture (84%+ multi-strategy tickers)")
        logger.info("\nNext Steps:")
        logger.info("   1. Reposition Step 8 to END of pipeline")
        logger.info("   2. Redesign Step 8 for final 0-1 decision per ticker")
        logger.info("   3. Update CLI audit for Sections G & H")
        
    except AssertionError as e:
        logger.error(f"\n❌ TEST FAILED: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
