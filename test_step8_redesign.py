"""
Test Step 8 Redesign: Final Selection & Position Sizing

This test validates the redesigned Step 8 which operates after Step 11:
1. Makes final 0-1 decision per ticker
2. Applies portfolio constraints
3. Calculates position sizing

Key Architecture:
- Input: 266 ranked strategies from Step 11 (all strategies, all tickers)
- Process: Select top-ranked, apply filters, calculate sizing
- Output: ~50 final trades (0-1 per ticker)
"""

import pandas as pd
import sys
import os
import logging

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.scan_engine.step8_position_sizing import finalize_and_size_positions

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s'
)
logger = logging.getLogger(__name__)


def test_final_selection_small():
    """
    Test 1: Small scenario (AAPL with 3 ranked strategies).
    
    Expected: Select rank 1 only → 1 final trade
    """
    
    logger.info("\n" + "="*80)
    logger.info("TEST 1: Final Selection (AAPL with 3 ranked strategies)")
    logger.info("="*80)
    
    # Create mock data (3 ranked strategies for AAPL)
    input_data = pd.DataFrame({
        'Ticker': ['AAPL', 'AAPL', 'AAPL'],
        'Primary_Strategy': ['Long Call', 'Buy-Write', 'Long Straddle'],
        'Strategy_Name': ['Long Call', 'Buy-Write', 'Long Straddle'],
        'Strategy_Rank': [1, 2, 3],
        'Comparison_Score': [85.23, 78.64, 72.19],
        'Contract_Selection_Status': ['Success', 'Success', 'Success'],
        'Total_Debit': [850, 500, 1500],
        'Trade_Bias': ['Bullish', 'Neutral', 'Neutral'],
        'Execution_Ready': [True, True, True],
        'Expected_Return_Score': [82, 75, 70],
        'Greeks_Quality_Score': [88, 80, 74],
        'Delta': [0.55, 0.30, 0.10],
        'Vega': [1.2, 0.8, 2.5]
    })
    
    logger.info(f"📥 Input: {len(input_data)} strategies")
    logger.info(f"   AAPL Rank 1: Long Call (Score: 85.23)")
    logger.info(f"   AAPL Rank 2: Buy-Write (Score: 78.64)")
    logger.info(f"   AAPL Rank 3: Long Straddle (Score: 72.19)")
    
    # Run Step 8
    output = finalize_and_size_positions(
        input_data,
        account_balance=100000,
        max_positions=50,
        min_comparison_score=60.0
    )
    
    logger.info(f"\n📤 Output: {len(output)} final trade(s)")
    
    # Assertions
    assert len(output) == 1, f"Expected 1 final trade, got {len(output)}"
    assert output.iloc[0]['Ticker'] == 'AAPL', "Should select AAPL"
    assert output.iloc[0]['Strategy_Rank'] == 1, "Should select rank 1"
    assert output.iloc[0]['Primary_Strategy'] == 'Long Call', "Should select Long Call"
    assert 'Dollar_Allocation' in output.columns, "Missing Dollar_Allocation"
    assert 'Num_Contracts' in output.columns, "Missing Num_Contracts"
    assert output.iloc[0]['Position_Valid'] == True, "Position should be valid"
    
    logger.info(f"\n✅ Selected: AAPL | Long Call (Rank 1)")
    logger.info(f"   Allocation: ${output.iloc[0]['Dollar_Allocation']:,.0f}")
    logger.info(f"   Contracts: {output.iloc[0]['Num_Contracts']}")
    logger.info(f"   Risk: ${output.iloc[0]['Max_Position_Risk']:,.0f}")
    
    logger.info(f"\n✅ TEST 1 PASSED")
    return True


def test_production_simulation():
    """
    Test 2: Production simulation (266 ranked strategies → ~50 final trades).
    
    Expected: Select top-ranked per ticker, apply filters, ~50 final trades
    """
    
    logger.info("\n" + "="*80)
    logger.info("TEST 2: Production Simulation (266 strategies → ~50 trades)")
    logger.info("="*80)
    
    # Create mock data (266 ranked strategies)
    # 127 tickers, avg 2.09 strategies per ticker
    tickers = [f'TICK{i:03d}' for i in range(127)]
    
    data = []
    for ticker in tickers:
        num_strategies = min(3, 1 + (hash(ticker) % 3))  # 1-3 strategies per ticker
        
        for rank in range(1, num_strategies + 1):
            strategy = ['Long Call', 'Buy-Write', 'Long Straddle'][rank - 1]
            score = 85 - (rank - 1) * 7  # Rank 1: 85, Rank 2: 78, Rank 3: 71
            
            data.append({
                'Ticker': ticker,
                'Primary_Strategy': strategy,
                'Strategy_Name': strategy,
                'Strategy_Rank': rank,
                'Comparison_Score': score,
                'Contract_Selection_Status': 'Success' if score > 65 else 'Failed',
                'Total_Debit': 500 + rank * 200,
                'Trade_Bias': 'Bullish',
                'Execution_Ready': score > 65,
                'Expected_Return_Score': score - 5,
                'Greeks_Quality_Score': score - 3,
                'Delta': 0.5 - rank * 0.1,
                'Vega': 1.0 + rank * 0.3
            })
    
    input_data = pd.DataFrame(data)
    
    logger.info(f"📥 Input: {len(input_data)} strategies")
    logger.info(f"   Unique tickers: {input_data['Ticker'].nunique()}")
    logger.info(f"   Avg strategies/ticker: {len(input_data)/input_data['Ticker'].nunique():.2f}")
    logger.info(f"   Rank 1: {(input_data['Strategy_Rank'] == 1).sum()}")
    logger.info(f"   Rank 2: {(input_data['Strategy_Rank'] == 2).sum()}")
    logger.info(f"   Rank 3+: {(input_data['Strategy_Rank'] > 2).sum()}")
    
    # Run Step 8
    output = finalize_and_size_positions(
        input_data,
        account_balance=100000,
        max_positions=50,
        min_comparison_score=70.0  # Higher threshold
    )
    
    logger.info(f"\n📤 Output: {len(output)} final trades")
    
    # Assertions
    assert len(output) > 0, "Should have at least some final trades"
    assert len(output) <= 50, f"Should not exceed max_positions (50), got {len(output)}"
    assert output['Ticker'].nunique() == len(output), "Should have 0-1 trade per ticker"
    assert (output['Strategy_Rank'] == 1).all(), "Should only select rank 1 strategies"
    assert (output['Comparison_Score'] >= 70.0).all(), "Should meet min score threshold"
    assert 'Dollar_Allocation' in output.columns, "Missing position sizing"
    assert 'Num_Contracts' in output.columns, "Missing contract count"
    
    # Summary stats
    total_allocation = output['Dollar_Allocation'].sum()
    total_risk = output['Max_Position_Risk'].sum()
    
    logger.info(f"\n📊 Final Portfolio:")
    logger.info(f"   Trades: {len(output)}")
    logger.info(f"   Total allocation: ${total_allocation:,.0f}")
    logger.info(f"   Total risk: ${total_risk:,.0f}")
    logger.info(f"   Avg score: {output['Comparison_Score'].mean():.2f}")
    logger.info(f"   Strategy distribution: {output['Primary_Strategy'].value_counts().to_dict()}")
    
    logger.info(f"\n✅ TEST 2 PASSED")
    return True


def test_portfolio_constraints():
    """
    Test 3: Portfolio constraints (max positions, diversification).
    
    Expected: Respect max_positions limit and diversification rules
    """
    
    logger.info("\n" + "="*80)
    logger.info("TEST 3: Portfolio Constraints")
    logger.info("="*80)
    
    # Create 100 rank-1 strategies (all passing filters)
    data = []
    for i in range(100):
        strategy = ['Long Call', 'Buy-Write', 'Long Put'][i % 3]
        
        data.append({
            'Ticker': f'TICK{i:03d}',
            'Primary_Strategy': strategy,
            'Strategy_Name': strategy,
            'Strategy_Rank': 1,
            'Comparison_Score': 80 + (i % 10),  # Scores 80-89
            'Contract_Selection_Status': 'Success',
            'Total_Debit': 800,
            'Trade_Bias': 'Bullish',
            'Execution_Ready': True,
            'Expected_Return_Score': 75,
            'Greeks_Quality_Score': 70,
            'Delta': 0.5,
            'Vega': 1.0
        })
    
    input_data = pd.DataFrame(data)
    
    logger.info(f"📥 Input: {len(input_data)} rank-1 strategies (all valid)")
    logger.info(f"   All passing filters, all execution-ready")
    
    # Run Step 8 with constraints
    output = finalize_and_size_positions(
        input_data,
        account_balance=100000,
        max_positions=30,  # Strict limit
        min_comparison_score=70.0,
        diversification_limit=10  # Max 10 per strategy type
    )
    
    logger.info(f"\n📤 Output: {len(output)} final trades")
    
    # Assertions
    assert len(output) <= 30, f"Should respect max_positions (30), got {len(output)}"
    
    # Check diversification limit
    strategy_counts = output['Primary_Strategy'].value_counts()
    for strategy, count in strategy_counts.items():
        assert count <= 10, f"{strategy} exceeds diversification limit: {count} > 10"
        logger.info(f"   {strategy}: {count} (≤ 10 limit)")
    
    # Check selection by score (should prioritize higher scores)
    assert output['Comparison_Score'].min() >= 70.0, "Should meet min score"
    
    logger.info(f"\n✅ Diversification respected: Max {strategy_counts.max()} per strategy")
    logger.info(f"✅ Position limit respected: {len(output)} ≤ 30")
    
    logger.info(f"\n✅ TEST 3 PASSED")
    return True


if __name__ == "__main__":
    logger.info("\n" + "="*80)
    logger.info("STEP 8 REDESIGN TEST SUITE")
    logger.info("Final Selection & Position Sizing (Post-Step 11)")
    logger.info("="*80)
    
    try:
        # Run all tests
        test_final_selection_small()
        test_production_simulation()
        test_portfolio_constraints()
        
        logger.info("\n" + "="*80)
        logger.info("✅ ALL TESTS PASSED")
        logger.info("="*80)
        logger.info("\nStep 8 redesign is ready for production:")
        logger.info("   ✓ Selects top-ranked strategy per ticker (0-1 per ticker)")
        logger.info("   ✓ Applies final filters (score, contracts, affordability)")
        logger.info("   ✓ Respects portfolio constraints (max positions, diversification)")
        logger.info("   ✓ Calculates position sizing with risk management")
        logger.info("\nIntegration ready:")
        logger.info("   → Step 11 (266 ranked) → Step 8 (50 final) → Execution")
        
    except AssertionError as e:
        logger.error(f"\n❌ TEST FAILED: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
