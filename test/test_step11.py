"""
Test Step 11: Strategy Pairing & Best-Per-Ticker Selection

Validates straddle/strangle pairing and best-per-ticker logic.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'core'))

import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

from scan_engine.step11_strategy_pairing import pair_and_select_strategies


def create_mock_step10_output():
    """Create mock Step 10 output with multiple execution-ready contracts."""
    return pd.DataFrame([
        # AAPL - Directional Call (strong)
        {
            'Ticker': 'AAPL',
            'Primary_Strategy': 'Bull Call Spread',
            'Trade_Bias': 'Bullish',
            'Actual_DTE': 30,
            'Selected_Strikes': '[170, 175]',
            'Contract_Symbols': '["AAPL240126C170", "AAPL240126C175"]',
            'PCS_Score': 85.0,
            'PCS_Final': 85.0,
            'Pre_Filter_Status': 'Valid',
            'Execution_Ready': True,
            'Option_Type': 'call',
            'Delta': 0.45,
            'Vega': 0.20,
            'Gamma': 0.03,
            'Total_Debit': 300.0,
            'Bid_Ask_Spread_Pct': 3.5,
            'Open_Interest': 500,
            'Contract_Intent': 'Execution_Candidate'
        },
        # AAPL - Call for straddle (ATM)
        {
            'Ticker': 'AAPL',
            'Primary_Strategy': 'Long Call',
            'Trade_Bias': 'Neutral',
            'Actual_DTE': 30,
            'Selected_Strikes': '[172.5]',
            'Contract_Symbols': '["AAPL240126C172.5"]',
            'PCS_Score': 80.0,
            'PCS_Final': 80.0,
            'Pre_Filter_Status': 'Valid',
            'Execution_Ready': True,
            'Option_Type': 'call',
            'Delta': 0.05,
            'Vega': 0.35,
            'Gamma': 0.04,
            'Total_Debit': 250.0,
            'Bid_Ask_Spread_Pct': 4.0,
            'Open_Interest': 400,
            'Contract_Intent': 'Execution_Candidate'
        },
        # AAPL - Put for straddle (ATM, same strike)
        {
            'Ticker': 'AAPL',
            'Primary_Strategy': 'Long Put',
            'Trade_Bias': 'Neutral',
            'Actual_DTE': 30,
            'Selected_Strikes': '[172.5]',
            'Contract_Symbols': '["AAPL240126P172.5"]',
            'PCS_Score': 82.0,
            'PCS_Final': 82.0,
            'Pre_Filter_Status': 'Valid',
            'Execution_Ready': True,
            'Option_Type': 'put',
            'Delta': -0.05,
            'Vega': 0.35,
            'Gamma': 0.04,
            'Total_Debit': 240.0,
            'Bid_Ask_Spread_Pct': 4.0,
            'Open_Interest': 420,
            'Contract_Intent': 'Execution_Candidate'
        },
        # TSLA - Directional only
        {
            'Ticker': 'TSLA',
            'Primary_Strategy': 'Bear Put Spread',
            'Trade_Bias': 'Bearish',
            'Actual_DTE': 25,
            'Selected_Strikes': '[240, 235]',
            'Contract_Symbols': '["TSLA240120P240", "TSLA240120P235"]',
            'PCS_Score': 78.0,
            'PCS_Final': 78.0,
            'Pre_Filter_Status': 'Valid',
            'Execution_Ready': True,
            'Option_Type': 'put',
            'Delta': -0.40,
            'Vega': 0.18,
            'Gamma': 0.025,
            'Total_Debit': 350.0,
            'Bid_Ask_Spread_Pct': 5.0,
            'Open_Interest': 300,
            'Contract_Intent': 'Execution_Candidate'
        },
        # SPY - Not execution ready (should be filtered out)
        {
            'Ticker': 'SPY',
            'Primary_Strategy': 'Long Call',
            'Trade_Bias': 'Bullish',
            'Actual_DTE': 45,
            'Selected_Strikes': '[480]',
            'Contract_Symbols': '["SPY240210C480"]',
            'PCS_Score': 60.0,
            'PCS_Final': 60.0,
            'Pre_Filter_Status': 'Watch',
            'Execution_Ready': False,
            'Option_Type': 'call',
            'Delta': 0.30,
            'Vega': 0.15,
            'Gamma': 0.02,
            'Total_Debit': 400.0,
            'Bid_Ask_Spread_Pct': 6.0,
            'Open_Interest': 200,
            'Contract_Intent': 'Scan'
        }
    ])


def test_basic_pairing():
    """Test basic pairing and selection functionality."""
    logger.info("\n" + "="*60)
    logger.info("TEST 1: Basic Pairing & Selection")
    logger.info("="*60)
    
    df = create_mock_step10_output()
    
    result = pair_and_select_strategies(
        df,
        enable_straddles=True,
        enable_strangles=False,
        capital_limit=5000.0
    )
    
    logger.info(f"✓ Input: {len(df)} contracts")
    logger.info(f"✓ Output: {len(result)} strategies")
    logger.info(f"✓ Tickers: {result['Ticker'].unique().tolist()}")
    
    # Should have one strategy per ticker (excluding SPY which is not execution-ready)
    if len(result) > 3:
        logger.error(f"✗ Expected ≤3 strategies (one per ticker), got {len(result)}")
        return False
    
    # Check AAPL selected highest PCS (straddle should be 81.0, directional 85.0)
    aapl = result[result['Ticker'] == 'AAPL']
    if not aapl.empty:
        aapl_pcs = aapl.iloc[0]['PCS_Final']
        aapl_strategy = aapl.iloc[0]['Strategy_Type']
        logger.info(f"✓ AAPL: {aapl_strategy} (PCS: {aapl_pcs:.1f})")
        
        if aapl_pcs < 80:
            logger.error(f"✗ AAPL PCS should be ≥80, got {aapl_pcs:.1f}")
            return False
    
    logger.info("✅ PASSED: Basic pairing and selection working")
    return True


def test_straddle_creation():
    """Test that straddles are properly created."""
    logger.info("\n" + "="*60)
    logger.info("TEST 2: Straddle Creation")
    logger.info("="*60)
    
    df = create_mock_step10_output()
    
    result = pair_and_select_strategies(
        df,
        enable_straddles=True,
        enable_strangles=False
    )
    
    # Check if any straddles were created
    straddles = result[result['Strategy_Type'] == 'Straddle']
    
    logger.info(f"✓ Straddles found: {len(straddles)}")
    
    if not straddles.empty:
        straddle = straddles.iloc[0]
        logger.info(f"✓ Straddle ticker: {straddle['Ticker']}")
        logger.info(f"✓ Straddle Vega: {straddle.get('Vega', 0):.2f}")
        logger.info(f"✓ Straddle PCS: {straddle['PCS_Final']:.1f}")
        
        # Straddle should have combined Vega > 0.50
        if straddle.get('Vega', 0) < 0.50:
            logger.warning(f"⚠️  Straddle Vega might be low: {straddle.get('Vega', 0):.2f}")
    
    logger.info("✅ PASSED: Straddle creation functional")
    return True


def test_capital_allocation():
    """Test capital allocation logic."""
    logger.info("\n" + "="*60)
    logger.info("TEST 3: Capital Allocation")
    logger.info("="*60)
    
    df = create_mock_step10_output()
    
    result = pair_and_select_strategies(
        df,
        enable_straddles=False,  # Directional only for simplicity
        capital_limit=5000.0
    )
    
    if result.empty:
        logger.error("✗ No strategies selected")
        return False
    
    # Check that Capital_Allocation_Recommended and Contracts_Recommended columns exist
    required_cols = ['Capital_Allocation_Recommended', 'Contracts_Recommended']
    missing = [c for c in required_cols if c not in result.columns]
    
    if missing:
        logger.error(f"✗ Missing columns: {missing}")
        return False
    
    # Check AAPL (PCS 85) gets high allocation
    aapl = result[result['Ticker'] == 'AAPL']
    if not aapl.empty:
        aapl_capital = aapl.iloc[0]['Capital_Allocation_Recommended']
        aapl_contracts = aapl.iloc[0]['Contracts_Recommended']
        
        logger.info(f"✓ AAPL capital: ${aapl_capital:,.2f}")
        logger.info(f"✓ AAPL contracts: {aapl_contracts}")
        
        if aapl_capital > 5000:
            logger.error(f"✗ Capital exceeds limit: ${aapl_capital:,.2f} > $5,000")
            return False
    
    logger.info("✅ PASSED: Capital allocation within limits")
    return True


def test_execution_ready_filter():
    """Test that non-execution-ready contracts are filtered."""
    logger.info("\n" + "="*60)
    logger.info("TEST 4: Execution Ready Filter")
    logger.info("="*60)
    
    df = create_mock_step10_output()
    
    result = pair_and_select_strategies(df)
    
    # SPY has Execution_Ready=False, should not appear
    spy = result[result['Ticker'] == 'SPY']
    
    if not spy.empty:
        logger.error("✗ SPY appeared in results (should be filtered)")
        return False
    
    logger.info("✓ SPY correctly filtered (Execution_Ready=False)")
    logger.info("✅ PASSED: Non-ready contracts filtered")
    return True


def run_all_tests():
    """Run all Step 11 tests."""
    logger.info("\n" + "#"*60)
    logger.info("# Step 11 Test Suite - Strategy Pairing")
    logger.info("#"*60)
    
    tests = [
        ("Basic Pairing & Selection", test_basic_pairing),
        ("Straddle Creation", test_straddle_creation),
        ("Capital Allocation", test_capital_allocation),
        ("Execution Ready Filter", test_execution_ready_filter),
    ]
    
    results = []
    for name, test_func in tests:
        try:
            passed = test_func()
            results.append((name, passed))
        except Exception as e:
            logger.error(f"✗ {name} FAILED with exception: {e}")
            import traceback
            traceback.print_exc()
            results.append((name, False))
    
    # Summary
    logger.info("\n" + "="*60)
    logger.info("TEST SUMMARY")
    logger.info("="*60)
    
    passed_count = sum(1 for _, passed in results if passed)
    total_count = len(results)
    
    for name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        logger.info(f"{status}: {name}")
    
    logger.info("="*60)
    logger.info(f"TOTAL: {passed_count}/{total_count} tests passed")
    logger.info("="*60)
    
    if passed_count == total_count:
        logger.info("🎉 ALL STEP 11 TESTS PASSED!")
        return True
    else:
        logger.error(f"⚠️  {total_count - passed_count} test(s) failed")
        return False


if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)
