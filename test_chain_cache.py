"""
Test Chain Caching Infrastructure

Validates:
1. Cache key generation: (Ticker, Expiration, AsOfDate)
2. Cache write/read round-trip
3. Cache hit performance (milliseconds vs seconds)
4. Cache stats and management
"""

import os
import time
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# Enable caching for this test
os.environ['DEBUG_CACHE_CHAINS'] = '1'

from core.scan_engine.step9b_fetch_contracts import ChainCache, _fetch_chain_with_greeks, TRADIER_TOKEN


def test_cache_infrastructure():
    """Test cache initialization and key generation"""
    print("\n" + "="*60)
    print("TEST 1: CACHE INFRASTRUCTURE")
    print("="*60)
    
    cache = ChainCache(enabled=True)
    
    # Test key generation
    key1 = cache._build_cache_key('AAPL', '2025-02-14')
    key2 = cache._build_cache_key('AAPL', '2025-02-14', '2025-01-15')
    
    print(f"\n📝 Cache Key Examples:")
    print(f"  Today:    {key1}")
    print(f"  Specific: {key2}")
    
    assert key1.startswith('AAPL_2025-02-14_')
    assert key2 == 'AAPL_2025-02-14_2025-01-15.pkl'
    
    print("\n✅ Cache infrastructure working")
    return cache


def test_cache_round_trip():
    """Test cache write and read"""
    print("\n" + "="*60)
    print("TEST 2: CACHE ROUND-TRIP")
    print("="*60)
    
    cache = ChainCache(enabled=True)
    
    # Create sample chain data
    test_chain = pd.DataFrame({
        'strike': [175.0, 180.0, 185.0],
        'option_type': ['call', 'call', 'call'],
        'bid': [5.0, 3.0, 1.5],
        'ask': [5.5, 3.5, 2.0],
        'open_interest': [1000, 500, 250],
        'underlying_price': [180.0, 180.0, 180.0]
    })
    
    ticker = 'AAPL'
    expiration = '2025-02-14'
    
    # Write to cache
    print(f"\n💾 Writing test data to cache...")
    success = cache.set(
        ticker=ticker,
        expiration=expiration,
        chain=test_chain,
        underlying_price=180.0,
        expirations=[expiration],
        dte=47
    )
    
    assert success, "Cache write failed"
    print(f"✅ Cache write successful")
    
    # Read from cache
    print(f"\n📦 Reading from cache...")
    cached_data = cache.get(ticker, expiration)
    
    assert cached_data is not None, "Cache read failed"
    assert len(cached_data['chain']) == 3, "Wrong number of rows"
    assert cached_data['underlying_price'] == 180.0, "Wrong price"
    assert cached_data['dte'] == 47, "Wrong DTE"
    
    print(f"✅ Cache read successful:")
    print(f"  Rows: {len(cached_data['chain'])}")
    print(f"  Price: ${cached_data['underlying_price']}")
    print(f"  DTE: {cached_data['dte']} days")
    
    return cache


def test_cache_performance():
    """Test cache hit performance improvement"""
    print("\n" + "="*60)
    print("TEST 3: CACHE PERFORMANCE")
    print("="*60)
    
    ticker = 'AAPL'
    expiration = '2025-03-21'  # Use a future expiration
    
    # Clear cache for this ticker to force API call
    cache = ChainCache(enabled=True)
    cache.clear(ticker)
    
    # First call - cache miss (API call)
    print(f"\n🌐 First call (API + cache write):")
    start = time.time()
    chain1 = _fetch_chain_with_greeks(ticker, expiration, TRADIER_TOKEN)
    time_api = time.time() - start
    
    if chain1.empty:
        print(f"⚠️  No data for {ticker} {expiration} - skipping performance test")
        return
    
    print(f"  Time: {time_api:.3f} seconds")
    print(f"  Rows: {len(chain1)}")
    
    # Second call - cache hit (disk read)
    print(f"\n📦 Second call (cache hit):")
    start = time.time()
    chain2 = _fetch_chain_with_greeks(ticker, expiration, TRADIER_TOKEN)
    time_cached = time.time() - start
    
    print(f"  Time: {time_cached:.3f} seconds")
    print(f"  Rows: {len(chain2)}")
    
    # Performance comparison
    speedup = time_api / time_cached if time_cached > 0 else float('inf')
    
    print(f"\n⚡ Performance Impact:")
    print(f"  API call:   {time_api:.3f}s")
    print(f"  Cache hit:  {time_cached:.3f}s")
    print(f"  Speedup:    {speedup:.1f}x faster")
    print(f"  Time saved: {(time_api - time_cached)*1000:.0f}ms")
    
    assert len(chain1) == len(chain2), "Cache data mismatch"
    assert time_cached < time_api, "Cache should be faster"
    
    print(f"\n✅ Cache provides significant speedup")


def test_cache_stats():
    """Test cache statistics and management"""
    print("\n" + "="*60)
    print("TEST 4: CACHE STATS & MANAGEMENT")
    print("="*60)
    
    cache = ChainCache(enabled=True)
    stats = cache.stats()
    
    print(f"\n📊 Cache Statistics:")
    print(f"  Enabled: {stats['enabled']}")
    print(f"  Total entries: {stats['total_entries']}")
    print(f"  Total size: {stats['total_size_mb']:.2f} MB")
    print(f"  Tickers: {', '.join(stats['tickers'][:10])}")
    
    if stats['oldest_entry']:
        print(f"  Oldest: {stats['oldest_entry']}")
    if stats['newest_entry']:
        print(f"  Newest: {stats['newest_entry']}")
    
    print(f"\n🗑️  Testing cache clear:")
    deleted = cache.clear('AAPL')
    print(f"  Cleared {deleted} AAPL cache entries")
    
    print(f"\n✅ Cache management working")


def test_cache_disabled():
    """Test behavior when caching is disabled"""
    print("\n" + "="*60)
    print("TEST 5: CACHE DISABLED MODE")
    print("="*60)
    
    cache = ChainCache(enabled=False)
    
    # Try to write
    test_chain = pd.DataFrame({'strike': [100.0]})
    success = cache.set('TEST', '2025-01-01', test_chain, 100.0, [], 30)
    
    print(f"\n🚫 Cache disabled:")
    print(f"  Write attempted: {not success}")
    print(f"  Stats: {cache.stats()}")
    
    assert not success, "Should not write when disabled"
    assert cache.stats()['total_entries'] == 0, "Should have no entries"
    
    print(f"\n✅ Disabled mode works correctly")


if __name__ == '__main__':
    print("\n" + "="*60)
    print("CHAIN CACHE VALIDATION TEST")
    print("="*60)
    print(f"\nCache location: {Path(os.getenv('CHAIN_CACHE_DIR', '.cache/chains'))}")
    print(f"Cache enabled: {os.getenv('DEBUG_CACHE_CHAINS')}")
    
    try:
        # Run all tests
        test_cache_infrastructure()
        test_cache_round_trip()
        test_cache_performance()
        test_cache_stats()
        test_cache_disabled()
        
        print("\n" + "="*60)
        print("🎉 ALL TESTS PASSED")
        print("="*60)
        print("\nKey Benefits:")
        print("  ✅ Deterministic: Same data on every run")
        print("  ✅ Fast: Milliseconds instead of seconds")
        print("  ✅ Reproducible: Debug with exact historical data")
        print("  ✅ Cost-effective: Reduced API quota usage")
        
        print("\nUsage:")
        print("  export DEBUG_CACHE_CHAINS=1")
        print("  python run_pipeline.py")
        
    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
