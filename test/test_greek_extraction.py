"""
Test Greek Extraction

Validates extract_greeks_to_columns() function:
1. Single-leg strategies (direct Greek assignment)
2. Multi-leg strategies (net Greeks)
3. Missing/invalid data handling
4. Validation metrics

Run:
    python test_greek_extraction.py
"""

import sys
import pandas as pd
import json
import numpy as np

# Add project root to path
sys.path.insert(0, '/Users/haniabadi/Documents/Github/options')

from utils.greek_extraction import (
    extract_greeks_to_columns,
    validate_greek_extraction
)


def test_single_leg_extraction():
    """Test 1: Single-leg strategies"""
    
    print("="*70)
    print("TEST 1: SINGLE-LEG STRATEGIES")
    print("="*70)
    print()
    
    data = {
        'Ticker': ['AAPL', 'AAPL'],
        'Strategy': ['Long Call', 'Long Put'],
        'Contract_Symbols': [
            '[{"symbol": "AAPL250214C180", "delta": 0.52, "gamma": 0.03, "vega": 0.25, "theta": -0.15, "rho": 0.08, "mid_iv": 0.30}]',
            '[{"symbol": "AAPL250214P180", "delta": -0.48, "gamma": 0.03, "vega": 0.25, "theta": -0.15, "rho": -0.07, "mid_iv": 0.32}]'
        ]
    }
    
    df = pd.DataFrame(data)
    df = extract_greeks_to_columns(df)
    
    print("Results:")
    print(df[['Strategy', 'Delta', 'Gamma', 'Vega', 'Theta', 'Rho', 'IV_Mid']].to_string(index=False))
    print()
    
    # Assertions
    assert df.iloc[0]['Delta'] == 0.52, "Long Call delta should be 0.52"
    assert df.iloc[1]['Delta'] == -0.48, "Long Put delta should be -0.48"
    assert df.iloc[0]['Vega'] == 0.25, "Vega should be 0.25"
    assert df.iloc[0]['IV_Mid'] == 0.30, "IV should be 0.30"
    
    print("✅ Single-leg extraction working")
    print()


def test_multi_leg_extraction():
    """Test 2: Multi-leg strategies (net Greeks)"""
    
    print("="*70)
    print("TEST 2: MULTI-LEG STRATEGIES")
    print("="*70)
    print()
    
    data = {
        'Ticker': ['AAPL', 'AAPL'],
        'Strategy': ['Long Straddle', 'Long Strangle'],
        'Contract_Symbols': [
            # Straddle: ATM call + ATM put
            '[{"symbol": "AAPL250214C180", "delta": 0.52, "gamma": 0.03, "vega": 0.25, "theta": -0.15, "rho": 0.08, "mid_iv": 0.30},'
            ' {"symbol": "AAPL250214P180", "delta": -0.48, "gamma": 0.03, "vega": 0.25, "theta": -0.15, "rho": -0.07, "mid_iv": 0.32}]',
            # Strangle: OTM call + OTM put
            '[{"symbol": "AAPL250214C185", "delta": 0.35, "gamma": 0.02, "vega": 0.20, "theta": -0.10, "rho": 0.05, "mid_iv": 0.28},'
            ' {"symbol": "AAPL250214P175", "delta": -0.32, "gamma": 0.02, "vega": 0.20, "theta": -0.10, "rho": -0.05, "mid_iv": 0.29}]'
        ]
    }
    
    df = pd.DataFrame(data)
    df = extract_greeks_to_columns(df)
    
    print("Results:")
    print(df[['Strategy', 'Delta', 'Gamma', 'Vega', 'Theta', 'IV_Mid']].to_string(index=False))
    print()
    
    # Assertions
    # Straddle: net delta ≈ 0 (0.52 - 0.48), net vega = 0.50 (0.25 + 0.25)
    assert abs(df.iloc[0]['Delta'] - 0.04) < 0.01, f"Straddle net delta should be ~0.04, got {df.iloc[0]['Delta']}"
    assert abs(df.iloc[0]['Vega'] - 0.50) < 0.01, f"Straddle net vega should be 0.50, got {df.iloc[0]['Vega']}"
    assert abs(df.iloc[0]['Gamma'] - 0.06) < 0.01, f"Straddle net gamma should be 0.06, got {df.iloc[0]['Gamma']}"
    
    # Strangle: net delta ≈ 0 (0.35 - 0.32), net vega = 0.40 (0.20 + 0.20)
    assert abs(df.iloc[1]['Delta'] - 0.03) < 0.01, f"Strangle net delta should be ~0.03, got {df.iloc[1]['Delta']}"
    assert abs(df.iloc[1]['Vega'] - 0.40) < 0.01, f"Strangle net vega should be 0.40, got {df.iloc[1]['Vega']}"
    
    print("✅ Multi-leg extraction working")
    print()


def test_missing_data_handling():
    """Test 3: Missing/invalid data handling"""
    
    print("="*70)
    print("TEST 3: MISSING/INVALID DATA")
    print("="*70)
    print()
    
    data = {
        'Ticker': ['AAPL', 'AAPL', 'AAPL', 'AAPL'],
        'Strategy': ['Test1', 'Test2', 'Test3', 'Test4'],
        'Contract_Symbols': [
            None,  # Missing
            '',    # Empty string
            '[]',  # Empty array
            'invalid json'  # Invalid JSON
        ]
    }
    
    df = pd.DataFrame(data)
    df = extract_greeks_to_columns(df)
    
    print("Results:")
    print(df[['Strategy', 'Delta', 'Vega']].to_string(index=False))
    print()
    
    # All should be NaN
    assert pd.isna(df.iloc[0]['Delta']), "Missing data should result in NaN"
    assert pd.isna(df.iloc[1]['Delta']), "Empty string should result in NaN"
    assert pd.isna(df.iloc[2]['Delta']), "Empty array should result in NaN"
    assert pd.isna(df.iloc[3]['Delta']), "Invalid JSON should result in NaN"
    
    print("✅ Missing data handling working")
    print()


def test_validation_metrics():
    """Test 4: Validation metrics"""
    
    print("="*70)
    print("TEST 4: VALIDATION METRICS")
    print("="*70)
    print()
    
    data = {
        'Ticker': ['AAPL'] * 10,
        'Strategy': ['Long Call'] * 10,
        'Contract_Symbols': [
            '[{"symbol": "AAPL250214C180", "delta": 0.52, "gamma": 0.03, "vega": 0.25, "theta": -0.15, "rho": 0.08, "mid_iv": 0.30}]'
            if i < 8 else None
            for i in range(10)
        ]
    }
    
    df = pd.DataFrame(data)
    df = extract_greeks_to_columns(df)
    
    validation = validate_greek_extraction(df)
    
    print("Validation metrics:")
    for key, value in validation.items():
        print(f"  {key}: {value}")
    print()
    
    # Assertions
    assert validation['total_rows'] == 10, "Should have 10 rows"
    assert validation['rows_with_delta'] == 8, "Should have 8 rows with Delta"
    assert validation['quality'] == 'GOOD', "Quality should be GOOD (>80% coverage)"
    
    print("✅ Validation metrics working")
    print()


def test_real_world_scenario():
    """Test 5: Real-world mixed scenario"""
    
    print("="*70)
    print("TEST 5: REAL-WORLD SCENARIO")
    print("="*70)
    print()
    
    data = {
        'Ticker': ['AAPL', 'AAPL', 'AAPL', 'AAPL', 'AAPL'],
        'Strategy': ['Long Call', 'Long Put', 'Long Straddle', 'Long Strangle', 'Bull Call Spread'],
        'Contract_Symbols': [
            # Single legs
            '[{"symbol": "AAPL250214C180", "delta": 0.52, "gamma": 0.03, "vega": 0.25, "theta": -0.15, "rho": 0.08, "mid_iv": 0.30}]',
            '[{"symbol": "AAPL250214P180", "delta": -0.48, "gamma": 0.03, "vega": 0.25, "theta": -0.15, "rho": -0.07, "mid_iv": 0.32}]',
            # Multi-leg volatility
            '[{"symbol": "AAPL250214C180", "delta": 0.52, "gamma": 0.03, "vega": 0.25, "theta": -0.15, "rho": 0.08, "mid_iv": 0.30},'
            ' {"symbol": "AAPL250214P180", "delta": -0.48, "gamma": 0.03, "vega": 0.25, "theta": -0.15, "rho": -0.07, "mid_iv": 0.32}]',
            '[{"symbol": "AAPL250214C185", "delta": 0.35, "gamma": 0.02, "vega": 0.20, "theta": -0.10, "rho": 0.05, "mid_iv": 0.28},'
            ' {"symbol": "AAPL250214P175", "delta": -0.32, "gamma": 0.02, "vega": 0.20, "theta": -0.10, "rho": -0.05, "mid_iv": 0.29}]',
            # Spread
            '[{"symbol": "AAPL250214C180", "delta": 0.52, "gamma": 0.03, "vega": 0.25, "theta": -0.15, "rho": 0.08, "mid_iv": 0.30},'
            ' {"symbol": "AAPL250214C185", "delta": -0.35, "gamma": -0.02, "vega": -0.20, "theta": 0.10, "rho": -0.05, "mid_iv": 0.28}]'
        ]
    }
    
    df = pd.DataFrame(data)
    df = extract_greeks_to_columns(df)
    
    print("Strategy Classification:")
    print()
    
    for idx, row in df.iterrows():
        delta = row['Delta']
        vega = row['Vega']
        strategy = row['Strategy']
        
        # Classify
        if abs(delta) > 0.35:
            classification = "DIRECTIONAL"
        elif vega > 0.30:
            classification = "VOLATILITY"
        else:
            classification = "NEUTRAL/SPREAD"
        
        print(f"{strategy:20s} | Delta: {delta:6.2f} | Vega: {vega:5.2f} | → {classification}")
    
    print()
    
    # Assertions
    assert abs(df.iloc[0]['Delta']) > 0.35, "Long Call should be directional"
    assert abs(df.iloc[1]['Delta']) > 0.35, "Long Put should be directional"
    assert df.iloc[2]['Vega'] > 0.30, "Straddle should be volatility"
    assert df.iloc[3]['Vega'] > 0.30, "Strangle should be volatility"
    assert abs(df.iloc[4]['Delta']) < 0.30, "Bull Call Spread should have reduced delta"
    
    print("✅ Real-world scenario working")
    print()


def main():
    """Run all tests"""
    
    print()
    print("="*70)
    print("GREEK EXTRACTION TEST SUITE")
    print("="*70)
    print()
    
    try:
        test_single_leg_extraction()
        test_multi_leg_extraction()
        test_missing_data_handling()
        test_validation_metrics()
        test_real_world_scenario()
        
        print("="*70)
        print("🎉 ALL TESTS PASSED")
        print("="*70)
        print()
        print("Next steps:")
        print("  1. Integrate into Step 10 entry point")
        print("  2. Implement calculate_pcs_score_v2() with Greek validation")
        print("  3. Run full pipeline to validate impact")
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
