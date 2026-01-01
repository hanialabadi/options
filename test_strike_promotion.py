"""
Test strike promotion logic - verify exactly one strike is promoted per strategy.

RAG: "Strike selection should be range-based internally (delta bands / ATM proximity),
but the engine must promote exactly one strike per strategy to execution."
"""

import pandas as pd
import numpy as np
from core.scan_engine.step9b_fetch_contracts import _promote_best_strike

def test_credit_spread_promotion():
    """Test credit spread promotes SHORT strike (sells premium)."""
    short_put = {
        'Contract_Symbol': 'SPY_240315P450',
        'Strike': 450,
        'Option_Type': 'Put',
        'Delta': -0.30,
        'Gamma': 0.05,
        'Vega': 0.20,
        'Theta': -0.10,
        'Mid_Price': 5.50
    }
    long_put = {
        'Contract_Symbol': 'SPY_240315P445',
        'Strike': 445,
        'Option_Type': 'Put',
        'Delta': -0.25,
        'Gamma': 0.04,
        'Vega': 0.18,
        'Theta': -0.08,
        'Mid_Price': 4.00
    }
    
    promoted = _promote_best_strike(
        symbols=[short_put, long_put],
        strategy='Credit Spread',
        bias='Bullish',
        underlying_price=455,
        total_credit=150,
        risk_per_contract=350
    )
    
    assert promoted is not None, "Should return promoted strike"
    assert promoted['Strike'] == 450, f"Should promote SHORT strike (450), got {promoted['Strike']}"
    assert 'Promotion_Reason' in promoted, "Should include promotion reason"
    assert 'Strategy_Credit' in promoted, "Should include strategy credit"
    print(f"✅ Credit Spread: Promoted Strike {promoted['Strike']} - {promoted['Promotion_Reason']}")


def test_debit_spread_promotion():
    """Test debit spread promotes LONG strike (position holder)."""
    long_call = {
        'Contract_Symbol': 'AAPL_240315C170',
        'Strike': 170,
        'Option_Type': 'Call',
        'Delta': 0.60,
        'Gamma': 0.06,
        'Vega': 0.22,
        'Theta': -0.15,
        'Mid_Price': 8.50
    }
    short_call = {
        'Contract_Symbol': 'AAPL_240315C180',
        'Strike': 180,
        'Option_Type': 'Call',
        'Delta': 0.35,
        'Gamma': 0.05,
        'Vega': 0.20,
        'Theta': -0.12,
        'Mid_Price': 3.00
    }
    
    promoted = _promote_best_strike(
        symbols=[long_call, short_call],
        strategy='Debit Spread',
        bias='Bullish',
        underlying_price=175,
        total_debit=550,
        risk_per_contract=550
    )
    
    assert promoted is not None
    assert promoted['Strike'] == 170, f"Should promote LONG strike (170), got {promoted['Strike']}"
    assert 'Strategy_Debit' in promoted
    print(f"✅ Debit Spread: Promoted Strike {promoted['Strike']} - {promoted['Promotion_Reason']}")


def test_iron_condor_promotion():
    """Test iron condor promotes SHORT PUT (credit center)."""
    long_put = {'Strike': 440, 'Option_Type': 'Put', 'Delta': -0.15, 'Vega': 0.15}
    short_put = {'Strike': 445, 'Option_Type': 'Put', 'Delta': -0.20, 'Vega': 0.18}
    short_call = {'Strike': 465, 'Option_Type': 'Call', 'Delta': 0.20, 'Vega': 0.18}
    long_call = {'Strike': 470, 'Option_Type': 'Call', 'Delta': 0.15, 'Vega': 0.15}
    
    promoted = _promote_best_strike(
        symbols=[long_put, short_put, short_call, long_call],
        strategy='Iron Condor',
        bias='Neutral',
        underlying_price=455,
        total_credit=200,
        risk_per_contract=300
    )
    
    assert promoted is not None
    assert promoted['Strike'] == 445, f"Should promote SHORT PUT (445), got {promoted['Strike']}"
    assert promoted['Option_Type'].lower() == 'put', "Should promote put side"
    print(f"✅ Iron Condor: Promoted Strike {promoted['Strike']} - {promoted['Promotion_Reason']}")


def test_straddle_promotion():
    """Test straddle promotes highest VEGA strike (vol exposure)."""
    call = {'Strike': 455, 'Option_Type': 'Call', 'Delta': 0.52, 'Vega': 0.28, 'Theta': -0.20}
    put = {'Strike': 455, 'Option_Type': 'Put', 'Delta': -0.48, 'Vega': 0.30, 'Theta': -0.18}
    
    promoted = _promote_best_strike(
        symbols=[call, put],
        strategy='Straddle',
        bias='Neutral',
        underlying_price=455,
        total_debit=1200
    )
    
    assert promoted is not None
    assert promoted['Vega'] == 0.30, f"Should promote highest Vega (0.30), got {promoted['Vega']}"
    assert promoted['Option_Type'].lower() == 'put', "Should promote put (higher Vega)"
    print(f"✅ Straddle: Promoted Strike {promoted['Strike']} - {promoted['Promotion_Reason']}")


def test_single_leg_promotion():
    """Test single leg promotes the only strike (pass-through)."""
    call = {'Strike': 175, 'Option_Type': 'Call', 'Delta': 0.70, 'Vega': 0.22, 'Theta': -0.18}
    
    promoted = _promote_best_strike(
        symbols=[call],
        strategy='Long Call',
        bias='Bullish',
        underlying_price=170,
        total_debit=850
    )
    
    assert promoted is not None
    assert promoted['Strike'] == 175, f"Should promote only strike (175), got {promoted['Strike']}"
    assert 'Promotion_Reason' in promoted
    print(f"✅ Single Leg: Promoted Strike {promoted['Strike']} - {promoted['Promotion_Reason']}")


if __name__ == '__main__':
    print("=" * 70)
    print("Testing Strike Promotion Logic")
    print("=" * 70)
    
    test_credit_spread_promotion()
    test_debit_spread_promotion()
    test_iron_condor_promotion()
    test_straddle_promotion()
    test_single_leg_promotion()
    
    print("\n" + "=" * 70)
    print("✅ ALL TESTS PASSED - Strike promotion working correctly")
    print("=" * 70)
    print("\nExpected Outcomes:")
    print("- Credit Spreads: Promote SHORT strike (sells premium, defines POP)")
    print("- Debit Spreads: Promote LONG strike (position holder, directional)")
    print("- Iron Condors: Promote SHORT PUT (credit center, liquidity)")
    print("- Straddles: Promote highest VEGA strike (volatility exposure)")
    print("- Single Legs: Promote only strike (pass-through)")
