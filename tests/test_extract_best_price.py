#!/usr/bin/env python3
"""
Unit Test: Extract Best Price Function

Tests the extract_best_price() function with mock Schwab quote data
to verify the fallback cascade logic works correctly.

This test can run without Schwab API credentials.
"""

import sys
from pathlib import Path

# Add core module to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.scan_engine.step0_schwab_snapshot import extract_best_price


def test_market_open_lastprice_available():
    """Test: Market open, lastPrice available (highest priority)."""
    quote = {
        'lastPrice': 235.50,
        'mark': 235.45,
        'closePrice': 232.15,
        'bidPrice': 235.48,
        'askPrice': 235.52
    }
    
    price, source = extract_best_price(quote, is_open=True)
    
    assert price == 235.50, f"Expected 235.50, got {price}"
    assert source == "lastPrice", f"Expected 'lastPrice', got {source}"
    print("✅ Test 1 PASSED: Market open, lastPrice available")


def test_market_open_no_lastprice():
    """Test: Market open, lastPrice missing, fall back to mark."""
    quote = {
        'mark': 235.45,
        'closePrice': 232.15,
        'bidPrice': 235.48,
        'askPrice': 235.52
    }
    
    price, source = extract_best_price(quote, is_open=True)
    
    assert price == 235.45, f"Expected 235.45, got {price}"
    assert source == "mark", f"Expected 'mark', got {source}"
    print("✅ Test 2 PASSED: Market open, no lastPrice, falls back to mark")


def test_market_open_only_bid_ask():
    """Test: Market open, only bid/ask available, compute midpoint."""
    quote = {
        'bidPrice': 235.48,
        'askPrice': 235.52,
        'closePrice': 232.15
    }
    
    price, source = extract_best_price(quote, is_open=True)
    
    expected = (235.48 + 235.52) / 2.0
    assert abs(price - expected) < 0.01, f"Expected {expected}, got {price}"
    assert source == "bidAskMid", f"Expected 'bidAskMid', got {source}"
    print("✅ Test 3 PASSED: Market open, falls back to bid-ask midpoint")


def test_market_closed_prefer_mark():
    """Test: Market closed, prefer mark over lastPrice (stale)."""
    quote = {
        'lastPrice': 235.50,
        'mark': 232.88,
        'closePrice': 232.15,
        'bidPrice': 232.85,
        'askPrice': 232.91
    }
    
    price, source = extract_best_price(quote, is_open=False)
    
    assert price == 232.88, f"Expected 232.88, got {price}"
    assert source == "mark", f"Expected 'mark', got {source}"
    print("✅ Test 4 PASSED: Market closed, prefers mark over stale lastPrice")


def test_market_closed_closePrice_fallback():
    """Test: Market closed, no mark, fall back to closePrice."""
    quote = {
        'lastPrice': 235.50,
        'closePrice': 232.15,
        'bidPrice': 232.85,
        'askPrice': 232.91
    }
    
    price, source = extract_best_price(quote, is_open=False)
    
    assert price == 232.15, f"Expected 232.15, got {price}"
    assert source == "closePrice", f"Expected 'closePrice', got {source}"
    print("✅ Test 5 PASSED: Market closed, falls back to closePrice")


def test_empty_quote():
    """Test: Empty quote block returns None."""
    quote = {}
    
    price, source = extract_best_price(quote, is_open=True)
    
    assert price is None, f"Expected None, got {price}"
    assert source == "none", f"Expected 'none', got {source}"
    print("✅ Test 6 PASSED: Empty quote returns None")


def test_all_nan():
    """Test: All fields are NaN/None."""
    quote = {
        'lastPrice': None,
        'mark': None,
        'closePrice': None,
        'bidPrice': None,
        'askPrice': None
    }
    
    price, source = extract_best_price(quote, is_open=True)
    
    assert price is None, f"Expected None, got {price}"
    assert source == "none", f"Expected 'none', got {source}"
    print("✅ Test 7 PASSED: All NaN fields returns None")


def test_regularMarketLastPrice_fallback():
    """Test: Falls back to regularMarketLastPrice when all else fails."""
    quote = {
        'regularMarketLastPrice': 234.99
    }
    
    price, source = extract_best_price(quote, is_open=True)
    
    assert price == 234.99, f"Expected 234.99, got {price}"
    assert source == "regularMarketLastPrice", f"Expected 'regularMarketLastPrice', got {source}"
    print("✅ Test 8 PASSED: Falls back to regularMarketLastPrice")


def main():
    """Run all tests."""
    print("="*80)
    print("  UNIT TESTS: extract_best_price() Fallback Logic")
    print("="*80)
    print()
    
    tests = [
        test_market_open_lastprice_available,
        test_market_open_no_lastprice,
        test_market_open_only_bid_ask,
        test_market_closed_prefer_mark,
        test_market_closed_closePrice_fallback,
        test_empty_quote,
        test_all_nan,
        test_regularMarketLastPrice_fallback
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            test()
            passed += 1
        except AssertionError as e:
            print(f"❌ {test.__name__} FAILED: {e}")
            failed += 1
        except Exception as e:
            print(f"❌ {test.__name__} ERROR: {e}")
            failed += 1
    
    print()
    print("="*80)
    print(f"  RESULTS: {passed}/{len(tests)} tests passed")
    print("="*80)
    
    if failed > 0:
        print(f"\n❌ {failed} test(s) failed")
        sys.exit(1)
    else:
        print(f"\n✅ All tests passed!")
        sys.exit(0)


if __name__ == "__main__":
    main()
