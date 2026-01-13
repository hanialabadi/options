"""
Test Step 10 Integration with PCS V2

Validates that Step 10 correctly integrates Greek extraction and PCS V2 scoring.

Run:
    python test_step10_integration.py
"""

import sys
sys.path.insert(0, '/Users/haniabadi/Documents/Github/options')

import pandas as pd
import json
from core.scan_engine.step10_pcs_recalibration import recalibrate_and_filter


def test_step10_with_greeks():
    """Test 1: Step 10 extracts Greeks and scores correctly"""
    
    print("="*70)
    print("TEST 1: STEP 10 INTEGRATION - GREEKS + PCS V2")
    print("="*70)
    print()
    
    # Simulate Step 9B output with Contract_Symbols JSON
    data = {
        'Ticker': ['AAPL', 'AAPL', 'AAPL', 'AAPL'],
        'Primary_Strategy': ['Long Call', 'Long Straddle', 'Long Strangle', 'Bull Call Spread'],
        'Trade_Bias': ['Bullish', 'Bidirectional', 'Bidirectional', 'Bullish'],
        'Actual_DTE': [45, 45, 45, 45],
        'Selected_Strikes': ['[180.0]', '[180.0, 180.0]', '[175.0, 185.0]', '[180.0, 185.0]'],
        'Contract_Symbols': [
            # Good Long Call
            '[{"symbol": "AAPL250214C180", "delta": 0.52, "gamma": 0.03, "vega": 0.25, "theta": -0.15, "rho": 0.08, "mid_iv": 0.30}]',
            # Good Straddle
            '[{"symbol": "AAPL250214C180", "delta": 0.52, "gamma": 0.03, "vega": 0.25, "theta": -0.15, "rho": 0.08, "mid_iv": 0.30}, {"symbol": "AAPL250214P180", "delta": -0.48, "gamma": 0.03, "vega": 0.25, "theta": -0.15, "rho": -0.07, "mid_iv": 0.32}]',
            # Wide spread Strangle
            '[{"symbol": "AAPL250214C185", "delta": 0.35, "gamma": 0.02, "vega": 0.40, "theta": -0.10, "rho": 0.05, "mid_iv": 0.28}, {"symbol": "AAPL250214P175", "delta": -0.32, "gamma": 0.02, "vega": 0.40, "theta": -0.10, "rho": -0.05, "mid_iv": 0.29}]',
            # Low delta spread
            '[{"symbol": "AAPL250214C180", "delta": 0.25, "gamma": 0.02, "vega": 0.15, "theta": -0.08, "rho": 0.04, "mid_iv": 0.28}, {"symbol": "AAPL250214C185", "delta": -0.18, "gamma": -0.01, "vega": -0.12, "theta": 0.06, "rho": -0.03, "mid_iv": 0.26}]'
        ],
        'Bid_Ask_Spread_Pct': [5.0, 6.0, 18.0, 7.0],  # Strangle has 18% spread → Watch
        'Open_Interest': [1000, 500, 20, 800],  # Strangle has low OI too
        'Liquidity_Score': [85.0, 75.0, 45.0, 70.0],
        'Actual_Risk_Per_Contract': [500, 1000, 800, 300],
        'Total_Debit': [500, 1000, 800, 300],
        'Total_Credit': [0, 0, 0, 0],
        'Risk_Model': ['Debit_Max', 'Debit_Max', 'Debit_Max', 'Debit_Max'],
        'Contract_Selection_Status': ['Success', 'Success', 'Success', 'Success'],
        'Contract_Intent': ['Scan', 'Scan', 'Scan', 'Scan']
    }
    
    df = pd.DataFrame(data)
    
    print("Input (Step 9B output):")
    print(df[['Ticker', 'Primary_Strategy', 'Bid_Ask_Spread_Pct', 'Open_Interest']].to_string(index=False))
    print()
    
    # Run Step 10
    result = recalibrate_and_filter(df)
    
    print("Output (Step 10 filtered):")
    display_cols = ['Primary_Strategy', 'Pre_Filter_Status', 'PCS_Score', 'Filter_Reason']
    print(result[display_cols].to_string(index=False))
    print()
    
    # Assertions
    assert 'Delta' in result.columns, "Delta column should be extracted"
    assert 'Vega' in result.columns, "Vega column should be extracted"
    assert 'PCS_Score' in result.columns, "PCS_Score should be present"
    assert 'Pre_Filter_Status' in result.columns, "Pre_Filter_Status should be present"
    
    # Check Greek extraction
    assert result.iloc[0]['Delta'] == 0.52, "Long Call delta should be 0.52"
    assert abs(result.iloc[1]['Delta'] - 0.04) < 0.01, "Straddle net delta should be ~0.04"
    assert result.iloc[1]['Vega'] == 0.50, "Straddle net vega should be 0.50"
    
    # Check status classification
    status_counts = result['Pre_Filter_Status'].value_counts().to_dict()
    assert status_counts.get('Valid', 0) >= 1, "Should have at least 1 Valid strategy"
    assert status_counts.get('Watch', 0) >= 1, "Should have at least 1 Watch strategy"
    
    print("✅ Step 10 integration working - Greeks extracted, PCS V2 applied")
    print()


def test_step10_backward_compatibility():
    """Test 2: Step 10 handles missing Greeks gracefully"""
    
    print("="*70)
    print("TEST 2: BACKWARD COMPATIBILITY (NO CONTRACT_SYMBOLS)")
    print("="*70)
    print()
    
    # Simulate old Step 9B output WITHOUT Contract_Symbols
    data = {
        'Ticker': ['AAPL', 'AAPL'],
        'Primary_Strategy': ['Long Call', 'Long Put'],
        'Trade_Bias': ['Bullish', 'Bearish'],
        'Actual_DTE': [45, 45],
        'Selected_Strikes': ['[180.0]', '[175.0]'],
        'Bid_Ask_Spread_Pct': [5.0, 6.0],
        'Open_Interest': [1000, 900],
        'Liquidity_Score': [85.0, 80.0],
        'Actual_Risk_Per_Contract': [500, 500],
        'Total_Debit': [500, 500],
        'Total_Credit': [0, 0],
        'Risk_Model': ['Debit_Max', 'Debit_Max'],
        'Contract_Selection_Status': ['Success', 'Success'],
        'Contract_Intent': ['Scan', 'Scan']
        # NO Contract_Symbols column
    }
    
    df = pd.DataFrame(data)
    
    print("Input (legacy Step 9B output - no Contract_Symbols):")
    print(df[['Ticker', 'Primary_Strategy', 'Liquidity_Score']].to_string(index=False))
    print()
    
    # Should not crash
    try:
        result = recalibrate_and_filter(df)
        print("Output:")
        print(result[['Primary_Strategy', 'Pre_Filter_Status', 'PCS_Score']].to_string(index=False))
        print()
        print("✅ Backward compatibility maintained - no crash with missing Greeks")
        print()
    except Exception as e:
        print(f"❌ Failed with error: {e}")
        raise


def test_step10_promotion_to_execution():
    """Test 3: Valid contracts promoted to Execution_Candidate"""
    
    print("="*70)
    print("TEST 3: PROMOTION TO EXECUTION")
    print("="*70)
    print()
    
    data = {
        'Ticker': ['AAPL', 'AAPL'],
        'Primary_Strategy': ['Long Call', 'Long Call'],
        'Trade_Bias': ['Bullish', 'Bullish'],
        'Actual_DTE': [45, 45],
        'Selected_Strikes': ['[180.0]', '[180.0]'],
        'Contract_Symbols': [
            '[{"symbol": "AAPL250214C180", "delta": 0.52, "gamma": 0.03, "vega": 0.25, "theta": -0.15, "rho": 0.08, "mid_iv": 0.30}]',
            '[{"symbol": "AAPL250214C180", "delta": 0.52, "gamma": 0.03, "vega": 0.25, "theta": -0.15, "rho": 0.08, "mid_iv": 0.30}]'
        ],
        'Bid_Ask_Spread_Pct': [5.0, 15.0],  # One good, one wide
        'Open_Interest': [1000, 1000],
        'Liquidity_Score': [85.0, 65.0],
        'Actual_Risk_Per_Contract': [500, 500],
        'Total_Debit': [500, 500],
        'Total_Credit': [0, 0],
        'Risk_Model': ['Debit_Max', 'Debit_Max'],
        'Contract_Selection_Status': ['Success', 'Success'],
        'Contract_Intent': ['Scan', 'Scan']
    }
    
    df = pd.DataFrame(data)
    result = recalibrate_and_filter(df)
    
    print("Results:")
    print(result[['Primary_Strategy', 'Pre_Filter_Status', 'Execution_Ready', 'Contract_Intent']].to_string(index=False))
    print()
    
    # Valid contract should be promoted
    valid_rows = result[result['Pre_Filter_Status'] == 'Valid']
    if len(valid_rows) > 0:
        assert valid_rows.iloc[0]['Execution_Ready'] == True, "Valid contract should be execution ready"
        assert valid_rows.iloc[0]['Contract_Intent'] == 'Execution_Candidate', "Should be promoted to Execution_Candidate"
    
    print("✅ Valid contracts correctly promoted to Execution_Candidate")
    print()


def main():
    """Run all Step 10 integration tests"""
    
    print()
    print("="*70)
    print("STEP 10 INTEGRATION TEST SUITE")
    print("="*70)
    print()
    
    try:
        test_step10_with_greeks()
        test_step10_backward_compatibility()
        test_step10_promotion_to_execution()
        
        print("="*70)
        print("🎉 ALL STEP 10 INTEGRATION TESTS PASSED")
        print("="*70)
        print()
        print("Phase 3 Integration Complete!")
        print()
        print("✅ Greek extraction integrated into Step 10")
        print("✅ PCS V2 scoring active")
        print("✅ Backward compatibility maintained")
        print("✅ Promotion to execution working")
        print()
        print("Next: Run full pipeline to validate end-to-end")
        print("  export DEBUG_CACHE_CHAINS=1")
        print("  python cli/run_pipeline_debug_simple.py")
        print()
        
    except AssertionError as e:
        print()
        print("="*70)
        print("❌ TEST FAILED")
        print("="*70)
        print(f"Error: {e}")
        print()
        sys.exit(1)
    
    except Exception as e:
        print()
        print("="*70)
        print("❌ UNEXPECTED ERROR")
        print("="*70)
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        print()
        sys.exit(1)


if __name__ == '__main__':
    main()
