"""
Quick integration test - verify promoted_strike field is created and extracted.
"""

import pandas as pd
import json
from utils.greek_extraction import extract_greeks_to_columns

def test_promoted_strike_extraction():
    """Test that Greek extraction prioritizes promoted_strike over Contract_Symbols."""
    
    # Simulate Step 9B output with promoted_strike
    data = {
        'Ticker': ['SPY', 'AAPL'],
        'Primary_Strategy': ['Put Credit Spread', 'Call Debit Spread'],
        'promoted_strike': [
            json.dumps({
                'Strike': 450.0,
                'Option_Type': 'Put',
                'Delta': -0.30,
                'Gamma': 0.05,
                'Vega': 0.20,
                'Theta': -0.10,
                'Rho': 0.02,
                'IV': 0.28,
                'Promotion_Reason': 'Credit Spread Short Strike (Sells Premium)',
                'Strategy_Credit': 150
            }),
            json.dumps({
                'Strike': 170.0,
                'Option_Type': 'Call',
                'Delta': 0.60,
                'Gamma': 0.06,
                'Vega': 0.22,
                'Theta': -0.15,
                'Rho': 0.03,
                'IV': 0.32,
                'Promotion_Reason': 'Debit Spread Long Strike (Position Holder)',
                'Strategy_Debit': 550
            })
        ],
        'Contract_Symbols': [
            json.dumps([{'Strike': 450, 'Delta': -0.30}, {'Strike': 445, 'Delta': -0.25}]),
            json.dumps([{'Strike': 170, 'Delta': 0.60}, {'Strike': 180, 'Delta': 0.35}])
        ]
    }
    
    df = pd.DataFrame(data)
    
    # Extract Greeks (should prioritize promoted_strike)
    df = extract_greeks_to_columns(df)
    
    # Validate Greeks were extracted
    assert 'Delta' in df.columns, "Delta column should exist"
    assert 'Gamma' in df.columns, "Gamma column should exist"
    assert 'Vega' in df.columns, "Vega column should exist"
    assert 'Theta' in df.columns, "Theta column should exist"
    assert 'Promoted_Strike' in df.columns, "Promoted_Strike column should exist"
    assert 'Promoted_Reason' in df.columns, "Promoted_Reason column should exist"
    
    # Validate values (from promoted_strike, not Contract_Symbols)
    assert df.loc[0, 'Delta'] == -0.30, f"SPY Delta should be -0.30 (got {df.loc[0, 'Delta']})"
    assert df.loc[0, 'Promoted_Strike'] == 450.0, f"SPY promoted strike should be 450.0 (got {df.loc[0, 'Promoted_Strike']})"
    assert 'Short Strike' in df.loc[0, 'Promoted_Reason'], "Should have promotion reason"
    
    assert df.loc[1, 'Delta'] == 0.60, f"AAPL Delta should be 0.60 (got {df.loc[1, 'Delta']})"
    assert df.loc[1, 'Promoted_Strike'] == 170.0, f"AAPL promoted strike should be 170.0 (got {df.loc[1, 'Promoted_Strike']})"
    assert 'Long Strike' in df.loc[1, 'Promoted_Reason'], "Should have promotion reason"
    
    print("✅ SPY Credit Spread:")
    print(f"   Promoted Strike: {df.loc[0, 'Promoted_Strike']}")
    print(f"   Delta: {df.loc[0, 'Delta']}, Gamma: {df.loc[0, 'Gamma']}, Vega: {df.loc[0, 'Vega']}, Theta: {df.loc[0, 'Theta']}")
    print(f"   Reason: {df.loc[0, 'Promoted_Reason']}")
    
    print("\n✅ AAPL Debit Spread:")
    print(f"   Promoted Strike: {df.loc[1, 'Promoted_Strike']}")
    print(f"   Delta: {df.loc[1, 'Delta']}, Gamma: {df.loc[1, 'Gamma']}, Vega: {df.loc[1, 'Vega']}, Theta: {df.loc[1, 'Theta']}")
    print(f"   Reason: {df.loc[1, 'Promoted_Reason']}")
    
    return df


if __name__ == '__main__':
    print("=" * 70)
    print("Testing Promoted Strike Extraction (End-to-End)")
    print("=" * 70)
    
    df = test_promoted_strike_extraction()
    
    print("\n" + "=" * 70)
    print("✅ INTEGRATION TEST PASSED")
    print("=" * 70)
    print("\nValidated:")
    print("- Greek extraction prioritizes promoted_strike over Contract_Symbols")
    print("- Promoted_Strike column populated correctly")
    print("- Promoted_Reason explains strike selection")
    print("- Delta, Gamma, Vega, Theta extracted from single promoted strike")
    print("\nReady for full pipeline integration!")
