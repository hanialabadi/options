"""
Test Step 9B: Validate all ChatGPT fixes and core functionality

Tests without requiring live Tradier API by mocking data.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'core'))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Import Step 9B functions
from scan_engine.step9b_fetch_contracts import (
    _calculate_liquidity_score,
    _select_calendar_strikes,
    _select_covered_call_strikes,
    _select_single_leg_strikes,
    _select_credit_spread_strikes,
    _select_debit_spread_strikes,
)

def create_mock_option(strike, option_type, bid, ask, oi, volume, symbol=None):
    """Create mock option data."""
    mid = (bid + ask) / 2
    spread_pct = ((ask - bid) / mid * 100) if mid > 0 else 100.0
    return {
        'strike': strike,
        'option_type': option_type,
        'bid': bid,
        'ask': ask,
        'open_interest': oi,
        'volume': volume,
        'mid_price': mid,
        'spread_pct': spread_pct,
        'symbol': symbol or f"TEST{strike}{option_type[0].upper()}"
    }

def test_liquidity_score():
    """Test ISSUE 3: Multi-factor liquidity score."""
    logger.info("\n" + "="*60)
    logger.info("TEST 1: Liquidity Score Calculation (ISSUE 3)")
    logger.info("="*60)
    
    # Test case 1: High liquidity short-term option
    score1 = _calculate_liquidity_score(
        open_interest=5000,
        spread_pct=2.0,
        volume=1000,
        dte=30
    )
    logger.info(f"✓ High liquidity short-term (OI=5000, spread=2%, vol=1000, DTE=30): {score1:.1f}")
    assert score1 > 80, "High liquidity should score >80"
    
    # Test case 2: LEAPS with zero volume (should still score decent)
    score2 = _calculate_liquidity_score(
        open_interest=500,
        spread_pct=5.0,
        volume=0,
        dte=120
    )
    logger.info(f"✓ LEAPS zero volume (OI=500, spread=5%, vol=0, DTE=120): {score2:.1f}")
    assert score2 > 40, "LEAPS with zero volume should get neutral score >40"
    
    # Test case 3: Short-term with zero volume (should score low)
    score3 = _calculate_liquidity_score(
        open_interest=500,
        spread_pct=5.0,
        volume=0,
        dte=30
    )
    logger.info(f"✓ Short-term zero volume (OI=500, spread=5%, vol=0, DTE=30): {score3:.1f}")
    assert score3 < score2, "Short-term zero volume should score lower than LEAPS zero volume"
    
    # Test case 4: Wide spread should hurt score
    score4 = _calculate_liquidity_score(
        open_interest=5000,
        spread_pct=15.0,
        volume=1000,
        dte=30
    )
    logger.info(f"✓ Wide spread (OI=5000, spread=15%, vol=1000, DTE=30): {score4:.1f}")
    assert score4 < score1, "Wide spread should lower liquidity score"
    
    logger.info("✅ PASSED: Liquidity score correctly weights OI, spread, and volume")
    return True

def test_calendar_rejection():
    """Test ISSUE 1: Calendar/Diagonal strategies should be rejected."""
    logger.info("\n" + "="*60)
    logger.info("TEST 2: Calendar/Diagonal Rejection (ISSUE 1)")
    logger.info("="*60)
    
    # Create mock calls and puts with proper ITM/OTM distribution
    calls = pd.DataFrame([
        create_mock_option(95, 'call', 6.0, 6.5, 150, 50),   # ITM
        create_mock_option(105, 'call', 2.0, 2.2, 100, 50),  # OTM
        create_mock_option(110, 'call', 1.0, 1.2, 200, 100), # OTM
    ])
    puts = pd.DataFrame([
        create_mock_option(95, 'put', 1.0, 1.2, 150, 75),
        create_mock_option(90, 'put', 2.0, 2.2, 100, 50),
    ])
    
    # Test rejection without approval
    result = _select_calendar_strikes(
        calls, puts, 'Bullish', atm=100, num_contracts=1,
        is_leaps=False, allow_multi_expiry=False
    )
    
    if result is None:
        logger.info("✓ Calendar strategy REJECTED (allow_multi_expiry=False)")
    else:
        logger.error("✗ Calendar strategy should return None when not approved")
        return False
    
    # Test approval pathway
    result_approved = _select_calendar_strikes(
        calls, puts, 'Bullish', atm=100, num_contracts=1,
        is_leaps=False, allow_multi_expiry=True
    )
    
    logger.info(f"DEBUG: result_approved = {result_approved}")
    
    if result_approved is not None and result_approved.get('structure_simplified') == True:
        logger.info("✓ Calendar strategy APPROVED with Structure_Simplified flag")
    else:
        logger.error(f"✗ Approved calendar should return result with structure_simplified=True")
        logger.error(f"   Got structure_simplified={result_approved.get('structure_simplified') if result_approved else 'N/A'}")
        return False
    
    logger.info("✅ PASSED: Calendar/Diagonal correctly rejected unless explicitly approved")
    return True

def test_covered_call_risk():
    """Test ISSUE 2: Covered call risk should be Stock_Dependent."""
    logger.info("\n" + "="*60)
    logger.info("TEST 3: Covered Call Risk Model (ISSUE 2)")
    logger.info("="*60)
    
    calls = pd.DataFrame([
        create_mock_option(105, 'call', 1.5, 1.7, 500, 100),
        create_mock_option(110, 'call', 0.8, 1.0, 300, 50),
    ])
    
    result = _select_covered_call_strikes(
        calls, atm=100, num_contracts=10, is_leaps=False, actual_dte=45
    )
    
    if result is None:
        logger.error("✗ Covered call should return a result")
        return False
    
    # Check risk_per_contract is None (stock-dependent)
    if result.get('risk_per_contract') is not None:
        logger.error(f"✗ risk_per_contract should be None, got: {result.get('risk_per_contract')}")
        return False
    logger.info("✓ risk_per_contract = None (stock-dependent)")
    
    # Check risk_model is Stock_Dependent
    if result.get('risk_model') != 'Stock_Dependent':
        logger.error(f"✗ risk_model should be 'Stock_Dependent', got: {result.get('risk_model')}")
        return False
    logger.info("✓ risk_model = 'Stock_Dependent'")
    
    logger.info("✅ PASSED: Covered call correctly marked as Stock_Dependent risk")
    return True

def test_leaps_strike_selection():
    """Test ISSUE 4: LEAPS should prefer deeper ITM strikes."""
    logger.info("\n" + "="*60)
    logger.info("TEST 4: LEAPS Deeper ITM Preference (ISSUE 4)")
    logger.info("="*60)
    
    # Create calls with various strikes around ATM=100
    calls = pd.DataFrame([
        create_mock_option(85, 'call', 17.0, 17.5, 200, 10),  # Deep ITM
        create_mock_option(92, 'call', 10.0, 10.5, 300, 20),  # ITM (8% ITM)
        create_mock_option(98, 'call', 4.0, 4.5, 500, 100),   # Near ATM
        create_mock_option(102, 'call', 2.0, 2.5, 400, 80),   # Slightly OTM
    ])
    puts = pd.DataFrame([
        create_mock_option(108, 'put', 10.0, 10.5, 300, 20),  # Deep ITM
        create_mock_option(102, 'put', 4.0, 4.5, 400, 50),    # Near ATM
        create_mock_option(98, 'put', 2.0, 2.5, 500, 100),    # Slightly OTM
    ])
    
    # Test LEAPS bullish (should select deeper ITM)
    result_leaps = _select_single_leg_strikes(
        calls, puts, 'Bullish', atm=100, num_contracts=1,
        is_leaps=True, actual_dte=150
    )
    
    if result_leaps and result_leaps['strikes'][0] <= 92:
        logger.info(f"✓ LEAPS bullish selected ITM strike: {result_leaps['strikes'][0]} (≤ 92)")
    else:
        logger.error(f"✗ LEAPS should select ITM strike ≤92, got: {result_leaps['strikes'][0] if result_leaps else None}")
        return False
    
    # Test short-term bullish (should select near ATM)
    result_short = _select_single_leg_strikes(
        calls, puts, 'Bullish', atm=100, num_contracts=1,
        is_leaps=False, actual_dte=30
    )
    
    if result_short and result_short['strikes'][0] >= 98:
        logger.info(f"✓ Short-term bullish selected near-ATM: {result_short['strikes'][0]} (≥ 98)")
    else:
        logger.error(f"✗ Short-term should select ATM strike ≥98, got: {result_short['strikes'][0] if result_short else None}")
        return False
    
    # Test LEAPS bearish (should select deeper ITM put)
    result_leaps_bear = _select_single_leg_strikes(
        calls, puts, 'Bearish', atm=100, num_contracts=1,
        is_leaps=True, actual_dte=150
    )
    
    if result_leaps_bear and result_leaps_bear['strikes'][0] >= 108:
        logger.info(f"✓ LEAPS bearish selected ITM put: {result_leaps_bear['strikes'][0]} (≥ 108)")
    else:
        logger.error(f"✗ LEAPS bearish should select ITM put ≥108, got: {result_leaps_bear['strikes'][0] if result_leaps_bear else None}")
        return False
    
    logger.info("✅ PASSED: LEAPS correctly prefer deeper ITM strikes (DTE ≥ 120)")
    return True

def test_credit_spread_liquidity():
    """Test that credit spreads use new liquidity score."""
    logger.info("\n" + "="*60)
    logger.info("TEST 5: Credit Spread Liquidity Score")
    logger.info("="*60)
    
    puts = pd.DataFrame([
        create_mock_option(95, 'put', 2.0, 2.2, 500, 100),
        create_mock_option(90, 'put', 1.0, 1.2, 400, 50),
        create_mock_option(85, 'put', 0.5, 0.7, 300, 25),
    ])
    calls = pd.DataFrame([
        create_mock_option(105, 'call', 2.0, 2.2, 500, 100),
    ])
    
    result = _select_credit_spread_strikes(
        calls, puts, 'Bullish', atm=100, num_contracts=1,
        is_leaps=False, actual_dte=45
    )
    
    if result is None:
        logger.error("✗ Credit spread should return result")
        return False
    
    liquidity_score = result.get('liquidity_score')
    if liquidity_score is None:
        logger.error("✗ liquidity_score should not be None")
        return False
    
    # Score should be 0-100
    if not (0 <= liquidity_score <= 100):
        logger.error(f"✗ liquidity_score should be 0-100, got: {liquidity_score}")
        return False
    
    logger.info(f"✓ Credit spread liquidity_score: {liquidity_score:.1f} (valid range 0-100)")
    logger.info("✅ PASSED: Credit spread uses multi-factor liquidity score")
    return True

def test_debit_spread():
    """Test debit spread with LEAPS adjustment."""
    logger.info("\n" + "="*60)
    logger.info("TEST 6: Debit Spread with LEAPS Adjustment")
    logger.info("="*60)
    
    calls = pd.DataFrame([
        create_mock_option(92, 'call', 10.0, 10.5, 300, 20),
        create_mock_option(98, 'call', 5.0, 5.5, 500, 50),
        create_mock_option(102, 'call', 2.0, 2.5, 400, 100),
    ])
    puts = pd.DataFrame([
        create_mock_option(98, 'put', 2.0, 2.5, 400, 100),
    ])
    
    # Test LEAPS debit spread
    result = _select_debit_spread_strikes(
        calls, puts, 'Bullish', atm=100, num_contracts=1,
        is_leaps=True, actual_dte=120
    )
    
    if result is None:
        logger.error("✗ Debit spread should return result")
        return False
    
    if result.get('risk_model') != 'Debit_Max':
        logger.error(f"✗ risk_model should be 'Debit_Max', got: {result.get('risk_model')}")
        return False
    
    logger.info(f"✓ Debit spread strikes: {result['strikes']}")
    logger.info(f"✓ Risk model: {result['risk_model']}")
    logger.info(f"✓ Liquidity score: {result.get('liquidity_score', 0):.1f}")
    logger.info("✅ PASSED: Debit spread correctly configured")
    return True

def run_all_tests():
    """Run all Step 9B tests."""
    logger.info("\n" + "#"*60)
    logger.info("# Step 9B Test Suite - All ChatGPT Fixes")
    logger.info("#"*60)
    
    tests = [
        ("Liquidity Score (ISSUE 3)", test_liquidity_score),
        ("Calendar Rejection (ISSUE 1)", test_calendar_rejection),
        ("Covered Call Risk (ISSUE 2)", test_covered_call_risk),
        ("LEAPS ITM Preference (ISSUE 4)", test_leaps_strike_selection),
        ("Credit Spread Liquidity", test_credit_spread_liquidity),
        ("Debit Spread", test_debit_spread),
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
        logger.info("🎉 ALL TESTS PASSED - Step 9B is production-ready!")
        return True
    else:
        logger.error(f"⚠️  {total_count - passed_count} test(s) failed")
        return False

if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)
