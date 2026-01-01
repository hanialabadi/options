#!/usr/bin/env python3
"""
Integration Test Suite for Redesigned Pipeline
================================================

Tests the complete pipeline integration with Steps 9A, 9B, 11, and 8 redesigned.

Pipeline Flow:
    Step 7: Strategy recommendations → 266 strategies
    Step 9A: DTE determination → 266 with DTEs  
    Step 9B: Contract fetching → 266 with contracts
    Step 10: PCS filtering → 262 validated
    Step 11: Comparison & ranking → 262 ranked
    Step 8: Final selection → ~50 final trades

Test Scenarios:
    1. Mock data flow: Verify step-by-step data preservation
    2. Production simulation: Test with real-like data sizes
    3. End-to-end validation: Verify full pipeline integrity
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
import numpy as np
from datetime import datetime

# Import redesigned functions
from core.scan_engine.step9a_determine_timeframe import determine_option_timeframe
from core.scan_engine.step9b_fetch_contracts import fetch_and_select_contracts
from core.scan_engine.step10_pcs_recalibration import recalibrate_and_filter
from core.scan_engine.step11_strategy_pairing import compare_and_rank_strategies
from core.scan_engine.step8_position_sizing import finalize_and_size_positions


def create_mock_step7_output(n_tickers=3, strategies_per_ticker=3):
    """
    Create mock Step 7 output (multi-strategy ledger).
    
    Returns DataFrame with (n_tickers * strategies_per_ticker) rows.
    """
    tickers = [f"TICK{i}" for i in range(n_tickers)]
    strategies = [
        ('Bull Put Spread', 'Directional'),
        ('Iron Condor', 'Income'),
        ('Cash-Secured Put', 'Directional')
    ]
    
    data = []
    for ticker in tickers:
        for i, (strategy_name, strategy_type) in enumerate(strategies[:strategies_per_ticker]):
            data.append({
                'Ticker': ticker,
                'Strategy': strategy_name.lower().replace(' ', '_').replace('-', '_'),
                'Strategy_Name': strategy_name,
                'Strategy_Type': strategy_type,
                'Regime': 'Bearish' if strategy_type == 'Directional' else 'Neutral',
                'Sector': 'Technology',
                'IVR': 65.0,
                'IV_HV_Spread': 12.0,
                'Suitability': 85.0,
                'Price': 150.0,
                'Expected_Return': 2.5,
                'Win_Rate': 0.65,
                'Max_Loss': 300.0,
                'Rank': i + 1
            })
    
    return pd.DataFrame(data)


def test_mock_data_flow():
    """
    Test 1: Mock data flow through all steps.
    
    Validates:
    - Row preservation: 9 → 9 → 9 → 9 → 9 → ~3
    - Column additions at each step
    - Final selection produces 1 trade per ticker
    """
    print("\n" + "="*80)
    print("Test 1: Mock Data Flow (9 strategies → ~3 final trades)")
    print("="*80)
    
    # Step 7: Create mock recommendations (3 tickers × 3 strategies = 9 rows)
    df7 = create_mock_step7_output(n_tickers=3, strategies_per_ticker=3)
    print(f"✅ Step 7: {len(df7)} strategies")
    assert len(df7) == 9, f"Expected 9 rows, got {len(df7)}"
    
    # Step 9A: DTE determination (strategy-aware)
    df9a = determine_option_timeframe(df7)
    print(f"✅ Step 9A: {len(df9a)} strategies (with DTEs)")
    assert len(df9a) == 9, f"Row count changed: {len(df7)} → {len(df9a)}"
    assert 'Min_DTE' in df9a.columns, "Missing DTE columns"
    assert 'Max_DTE' in df9a.columns, "Missing DTE columns"
    assert 'Target_DTE' in df9a.columns, "Missing DTE columns"
    
    # Step 9B: Contract fetching (MOCK - just add contract columns)
    # In real pipeline, this calls Tradier API
    df9b = df9a.copy()
    df9b['Strike'] = 150.0
    df9b['Contract_Type'] = 'put'
    df9b['DTE'] = 35
    df9b['contracts_exist'] = True
    df9b['Contract_Selection_Status'] = 'Success'
    df9b['PCS_Final'] = 70.0
    print(f"✅ Step 9B: {len(df9b)} strategies (with contracts - MOCKED)")
    assert len(df9b) == 9, f"Row count changed: {len(df9a)} → {len(df9b)}"
    
    # Step 10: PCS recalibration (MOCK - just add greek columns)
    # In real pipeline, this filters by liquidity/spread
    df10 = df9b.copy()
    df10['Delta'] = -0.25
    df10['Theta'] = 0.15
    df10['Gamma'] = 0.02
    df10['Vega'] = 0.10
    df10['IV'] = 0.35
    df10['Liquidity_Score'] = 65.0
    df10['Spread_Pct'] = 3.5
    df10['Total_Debit'] = 250.0
    df10['Bid_Ask_Spread_Pct'] = 2.5
    df10['Open_Interest'] = 500
    df10['Trade_Type'] = 'Credit Spread'
    print(f"✅ Step 10: {len(df10)} strategies (PCS validated - MOCKED)")
    assert len(df10) == 9, f"Row count changed: {len(df9b)} → {len(df10)}"
    
    # Step 11: Strategy comparison & ranking (100% row preservation)
    df11 = compare_and_rank_strategies(
        df10,
        user_goal='income',
        account_size=100000,
        risk_tolerance='medium'
    )
    print(f"✅ Step 11: {len(df11)} strategies (ranked)")
    assert len(df11) == 9, f"Row count changed: {len(df10)} → {len(df11)}"
    assert 'Strategy_Rank' in df11.columns, "Missing Strategy_Rank column"
    assert 'Comparison_Score' in df11.columns, "Missing Comparison_Score column"
    
    # Verify ranking: Each ticker should have ranks 1, 2, 3
    for ticker in df11['Ticker'].unique():
        ticker_df = df11[df11['Ticker'] == ticker]
        ranks = sorted(ticker_df['Strategy_Rank'].values)
        assert ranks == [1, 2, 3], f"{ticker}: Expected ranks [1,2,3], got {ranks}"
    
    # Step 8: Final selection & position sizing (select rank 1 only)
    df8 = finalize_and_size_positions(
        df11,
        account_balance=100000,
        max_portfolio_risk=0.20,
        max_trade_risk=0.02,
        min_comparison_score=60.0,
        max_positions=50,
        sizing_method='volatility_scaled'
    )
    print(f"✅ Step 8: {len(df8)} final trades (selected & sized)")
    assert len(df8) <= 3, f"Expected ≤3 final trades (1 per ticker), got {len(df8)}"
    assert len(df8) > 0, "Expected at least 1 final trade"
    
    # Verify final selection: Only rank 1 strategies should be present
    assert all(df8['Strategy_Rank'] == 1), "Non-rank-1 strategies in final output"
    assert 'Position_Size' in df8.columns, "Missing Position_Size column"
    assert 'Capital_Required' in df8.columns, "Missing Capital_Required column"
    
    print(f"\n✅ Test 1 PASSED: 9 → 9 → 9 → 9 → 9 → {len(df8)} (selection rate: {len(df8)/9*100:.1f}%)")
    return True


def test_production_simulation():
    """
    Test 2: Production-scale simulation.
    
    Simulates real pipeline with 266 strategies → ~50 final trades.
    """
    print("\n" + "="*80)
    print("Test 2: Production Simulation (266 strategies → ~50 final trades)")
    print("="*80)
    
    # Step 7: Create production-scale mock (127 tickers, avg 2.09 strategies/ticker)
    np.random.seed(42)
    tickers = [f"TICK{i:03d}" for i in range(127)]
    strategies = [
        ('Bull Put Spread', 'Directional'),
        ('Iron Condor', 'Income'),
        ('Cash-Secured Put', 'Directional'),
        ('Short Strangle', 'Volatility')
    ]
    
    data = []
    for ticker in tickers:
        # Randomly assign 1-3 strategies per ticker (weighted toward 2)
        n_strategies = np.random.choice([1, 2, 3], p=[0.16, 0.68, 0.16])
        selected_strategies = np.random.choice(len(strategies), size=n_strategies, replace=False)
        
        for i, strat_idx in enumerate(selected_strategies):
            strategy_name, strategy_type = strategies[strat_idx]
            data.append({
                'Ticker': ticker,
                'Strategy': strategy_name.lower().replace(' ', '_').replace('-', '_'),
                'Strategy_Name': strategy_name,
                'Strategy_Type': strategy_type,
                'Regime': np.random.choice(['Bullish', 'Bearish', 'Neutral']),
                'Sector': np.random.choice(['Technology', 'Finance', 'Healthcare']),
                'IVR': np.random.uniform(50, 80),
                'IV_HV_Spread': np.random.uniform(8, 15),
                'Suitability': np.random.uniform(70, 95),
                'Price': np.random.uniform(50, 300),
                'Expected_Return': np.random.uniform(1.5, 4.0),
                'Win_Rate': np.random.uniform(0.60, 0.75),
                'Max_Loss': np.random.uniform(200, 500),
                'Rank': i + 1
            })
    
    df7 = pd.DataFrame(data)
    print(f"✅ Step 7: {len(df7)} strategies ({len(df7['Ticker'].unique())} tickers, avg {len(df7)/len(df7['Ticker'].unique()):.2f} strategies/ticker)")
    
    # Step 9A: DTE determination
    df9a = determine_option_timeframe(df7)
    print(f"✅ Step 9A: {len(df9a)} strategies")
    assert len(df9a) == len(df7), f"Row count changed: {len(df7)} → {len(df9a)}"
    
    # Step 9B, 10: Mock contract and greek data
    df10 = df9a.copy()
    df10['Strike'] = df10['Price'] * 0.95
    df10['Contract_Type'] = 'put'
    df10['DTE'] = np.random.randint(30, 45, size=len(df10))
    df10['contracts_exist'] = True
    df10['Contract_Selection_Status'] = 'Success'
    df10['PCS_Final'] = np.random.uniform(60, 80, size=len(df10))
    df10['Delta'] = np.random.uniform(-0.30, -0.20, size=len(df10))
    df10['Theta'] = np.random.uniform(0.10, 0.20, size=len(df10))
    df10['Gamma'] = np.random.uniform(0.01, 0.03, size=len(df10))
    df10['Vega'] = np.random.uniform(0.08, 0.12, size=len(df10))
    df10['IV'] = np.random.uniform(0.30, 0.40, size=len(df10))
    df10['Liquidity_Score'] = np.random.uniform(50, 80, size=len(df10))
    df10['Spread_Pct'] = np.random.uniform(2, 6, size=len(df10))
    df10['Total_Debit'] = np.random.uniform(200, 400, size=len(df10))
    df10['Bid_Ask_Spread_Pct'] = np.random.uniform(2, 5, size=len(df10))
    df10['Open_Interest'] = np.random.randint(100, 1000, size=len(df10))
    df10['Trade_Type'] = 'Credit Spread'
    print(f"✅ Step 9B/10: {len(df10)} strategies (MOCKED)")
    
    # Step 11: Comparison & ranking
    df11 = compare_and_rank_strategies(
        df10,
        user_goal='income',
        account_size=100000,
        risk_tolerance='medium'
    )
    print(f"✅ Step 11: {len(df11)} strategies (ranked)")
    assert len(df11) == len(df10), f"Row count changed: {len(df10)} → {len(df11)}"
    
    # Step 8: Final selection
    df8 = finalize_and_size_positions(
        df11,
        account_balance=100000,
        max_portfolio_risk=0.20,
        max_trade_risk=0.02,
        min_comparison_score=60.0,
        max_positions=50,
        sizing_method='volatility_scaled'
    )
    print(f"✅ Step 8: {len(df8)} final trades")
    
    # Validation
    assert 20 <= len(df8) <= 80, f"Expected 20-80 final trades, got {len(df8)}"
    assert all(df8['Strategy_Rank'] == 1), "Non-rank-1 strategies in final output"
    assert len(df8['Ticker'].unique()) == len(df8), "Duplicate tickers in final output"
    
    print(f"\n✅ Test 2 PASSED: {len(df7)} → {len(df11)} → {len(df8)} (selection rate: {len(df8)/len(df11)*100:.1f}%)")
    return True


def test_end_to_end_validation():
    """
    Test 3: End-to-end data integrity validation.
    
    Validates:
    - Required columns present at each step
    - No data corruption during transformations
    - Final output ready for broker submission
    """
    print("\n" + "="*80)
    print("Test 3: End-to-End Data Integrity Validation")
    print("="*80)
    
    # Create small test dataset
    df7 = create_mock_step7_output(n_tickers=5, strategies_per_ticker=2)
    print(f"✅ Step 7: {len(df7)} strategies")
    
    # Validate Step 7 output
    required_step7 = ['Ticker', 'Strategy_Name', 'Strategy_Type', 'Expected_Return', 'Win_Rate']
    assert all(col in df7.columns for col in required_step7), "Missing Step 7 columns"
    
    # Step 9A
    df9a = determine_option_timeframe(df7)
    print(f"✅ Step 9A: {len(df9a)} strategies")
    required_step9a = ['Min_DTE', 'Max_DTE', 'Target_DTE']
    assert all(col in df9a.columns for col in required_step9a), "Missing Step 9A columns"
    
    # Mock Steps 9B, 10
    df10 = df9a.copy()
    df10['Strike'] = 150.0
    df10['DTE'] = 35
    df10['Delta'] = -0.25
    df10['Theta'] = 0.15
    df10['Gamma'] = 0.02
    df10['Vega'] = 0.10
    df10['IV'] = 0.35
    df10['Liquidity_Score'] = 65.0
    df10['Spread_Pct'] = 3.5
    df10['Total_Debit'] = 250.0
    df10['Bid_Ask_Spread_Pct'] = 2.5
    df10['Open_Interest'] = 500
    df10['contracts_exist'] = True
    df10['Contract_Selection_Status'] = 'Success'
    df10['PCS_Final'] = 70.0
    df10['Trade_Type'] = 'Credit Spread'
    print(f"✅ Step 9B/10: {len(df10)} strategies (MOCKED)")
    
    # Step 11
    df11 = compare_and_rank_strategies(df10, user_goal='income', account_size=100000)
    print(f"✅ Step 11: {len(df11)} strategies")
    required_step11 = ['Strategy_Rank', 'Comparison_Score']
    assert all(col in df11.columns for col in required_step11), "Missing Step 11 columns"
    
    # Step 8
    df8 = finalize_and_size_positions(
        df11,
        account_balance=100000,
        max_portfolio_risk=0.20,
        max_trade_risk=0.02,
        min_comparison_score=60.0,
        max_positions=50,
        sizing_method='volatility_scaled'
    )
    print(f"✅ Step 8: {len(df8)} final trades")
    
    # Validate final output
    required_step8 = ['Position_Size', 'Capital_Required', 'Strategy_Rank']
    assert all(col in df8.columns for col in required_step8), "Missing Step 8 columns"
    
    # Validate data integrity
    assert all(df8['Position_Size'] > 0), "Invalid position sizes"
    assert all(df8['Capital_Required'] > 0), "Invalid capital requirements"
    assert all(df8['Strategy_Rank'] == 1), "Non-rank-1 strategies present"
    assert df8['Capital_Required'].sum() <= 100000, "Portfolio exceeds account balance"
    
    print(f"\n✅ Test 3 PASSED: All data integrity checks passed")
    return True


def run_all_tests():
    """Run all integration tests."""
    print("\n" + "="*80)
    print("PIPELINE INTEGRATION TEST SUITE")
    print("="*80)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    tests = [
        ("Mock Data Flow", test_mock_data_flow),
        ("Production Simulation", test_production_simulation),
        ("End-to-End Validation", test_end_to_end_validation)
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            success = test_func()
            results.append((test_name, "✅ PASSED"))
        except Exception as e:
            results.append((test_name, f"❌ FAILED: {e}"))
            print(f"\n❌ {test_name} FAILED: {e}")
    
    # Summary
    print("\n" + "="*80)
    print("TEST SUMMARY")
    print("="*80)
    for test_name, result in results:
        print(f"{result:20} - {test_name}")
    
    passed = sum(1 for _, r in results if "✅" in r)
    total = len(results)
    print(f"\n{'='*80}")
    print(f"Results: {passed}/{total} tests passed")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    return all("✅" in r for _, r in results)


if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)
