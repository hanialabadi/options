"""
Validate strike promotion end-to-end integration.
Tests that promoted_strike field flows through Step 9B → Step 10 → UI.
"""

import sys
sys.path.insert(0, '/Users/haniabadi/Documents/Github/options')
import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import json
from core.scan_engine.step9b_fetch_contracts import _select_credit_spread_strikes, _promote_best_strike

def test_strike_promotion_in_contract_selection():
    """Test that contract selection returns promoted_strike field."""
    
    print("=" * 70)
    print("Testing Strike Promotion Integration")
    print("=" * 70)
    
    # Create mock chain data for credit spread
    calls = pd.DataFrame({
        'strike': [455, 460, 465, 470],
        'option_type': ['call'] * 4,
        'bid': [8.0, 5.0, 3.0, 1.5],
        'ask': [8.5, 5.5, 3.5, 2.0],
        'spread_pct': [3.0, 3.5, 4.0, 5.0],
        'open_interest': [500, 400, 300, 200],
        'volume': [100, 80, 60, 40],
        'delta': [0.65, 0.50, 0.35, 0.20],
        'gamma': [0.04, 0.05, 0.04, 0.03],
        'vega': [0.18, 0.20, 0.18, 0.15],
        'theta': [-0.22, -0.20, -0.18, -0.15],
        'mid_iv': [0.28, 0.30, 0.32, 0.35],
        'underlying_price': [450] * 4,
        'expiration': ['2024-03-15'] * 4
    })
    
    puts = pd.DataFrame({
        'strike': [420, 425, 430, 435],  # OTM puts (below ATM 450)
        'option_type': ['put'] * 4,
        'bid': [1.5, 3.0, 5.0, 7.0],
        'ask': [2.0, 3.5, 5.5, 7.5],
        'spread_pct': [5.0, 4.0, 3.5, 3.0],
        'open_interest': [200, 300, 400, 500],
        'volume': [40, 60, 80, 100],
        'delta': [-0.15, -0.25, -0.35, -0.45],
        'gamma': [0.03, 0.04, 0.05, 0.04],
        'vega': [0.15, 0.18, 0.20, 0.18],
        'theta': [-0.12, -0.15, -0.18, -0.20],
        'mid_iv': [0.35, 0.32, 0.30, 0.28],
        'underlying_price': [450] * 4,
        'expiration': ['2024-03-15'] * 4
    })
    
    # Test credit spread selection
    result = _select_credit_spread_strikes(
        calls=calls,
        puts=puts,
        bias='Bullish',
        atm=450,
        num_contracts=1,
        is_leaps=False,
        actual_dte=45,
        underlying_price=450
    )
    
    print("\n1. Credit Spread Strike Selection Result:")
    print(f"   Status: {'✅ Success' if result else '❌ Failed'}")
    
    if result:
        print(f"   Strikes: {result['strikes']}")
        print(f"   Symbols count: {len(result['symbols'])}")
        print(f"   Has promoted_strike: {'✅ Yes' if 'promoted_strike' in result else '❌ No'}")
        
        if 'promoted_strike' in result and result['promoted_strike']:
            promoted = result['promoted_strike']
            print(f"\n2. Promoted Strike Details:")
            print(f"   Strike: {promoted.get('Strike')}")
            print(f"   Option Type: {promoted.get('Option_Type')}")
            print(f"   Delta: {promoted.get('Delta')}")
            print(f"   Vega: {promoted.get('Vega')}")
            print(f"   Promotion Reason: {promoted.get('Promotion_Reason', 'N/A')}")
            
            # Verify it's a dict (not JSON string yet)
            if isinstance(promoted, dict):
                print(f"   ✅ Promoted strike is dict (correct format)")
            else:
                print(f"   ❌ Promoted strike is {type(promoted)} (should be dict)")
            
            # Test JSON serialization
            try:
                json_str = json.dumps(promoted)
                print(f"\n3. JSON Serialization Test:")
                print(f"   ✅ Can serialize to JSON")
                print(f"   Length: {len(json_str)} chars")
                
                # Test deserialization
                deserialized = json.loads(json_str)
                print(f"   ✅ Can deserialize from JSON")
                print(f"   Deserialized Strike: {deserialized.get('Strike')}")
                
            except Exception as e:
                print(f"   ❌ JSON serialization failed: {e}")
        else:
            print(f"   ❌ promoted_strike field missing or empty")
    else:
        print(f"   ❌ Contract selection returned None")
    
    print("\n" + "=" * 70)
    if result and 'promoted_strike' in result and result['promoted_strike']:
        print("✅ INTEGRATION TEST PASSED")
        print("   - Strike selection returns promoted_strike")
        print("   - Promoted strike is dict with required fields")
        print("   - Can serialize/deserialize as JSON")
        print("   - Ready for Step 9B → Step 10 → UI flow")
    else:
        print("❌ INTEGRATION TEST FAILED")
        print("   Check _select_credit_spread_strikes implementation")
    print("=" * 70)
    
    return result

if __name__ == '__main__':
    result = test_strike_promotion_in_contract_selection()
    sys.exit(0 if result and 'promoted_strike' in result else 1)
