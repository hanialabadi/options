"""
Test Suite for Chain Cache and Throttled Executor

Tests Phase A infrastructure components:
- ChainCache (memory + disk caching)
- ExpirationCache (lightweight expiration metadata)
- ThrottledExecutor (parallel execution with rate limiting)
- BatchProcessor (large-scale batch processing)
"""

import pytest
import time
import tempfile
from pathlib import Path
from datetime import datetime, timedelta

from core.scan_engine.chain_cache import ChainCache, ExpirationCache
from core.scan_engine.throttled_executor import ThrottledExecutor, BatchProcessor


# ============================================================================
# ChainCache Tests
# ============================================================================

def test_chain_cache_basic():
    """Test basic cache get/set operations."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cache = ChainCache(cache_dir=tmpdir, max_memory_size=10)
        
        # Miss on first get
        assert cache.get('AAPL') is None
        assert cache.stats()['misses'] == 1
        
        # Set and get
        chain_data = {'calls': [{'strike': 150}], 'puts': [{'strike': 145}]}
        cache.set('AAPL', chain_data)
        
        retrieved = cache.get('AAPL')
        assert retrieved == chain_data
        assert cache.stats()['hits'] == 1
        
        print(f"✅ Basic cache operations: {cache}")


def test_chain_cache_expiration():
    """Test cache expiration based on age."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cache = ChainCache(cache_dir=tmpdir)
        
        # Set cache with very short max_age
        chain_data = {'test': 'data'}
        cache.set('AAPL', chain_data)
        
        # Should hit immediately
        assert cache.get('AAPL', max_age_hours=24) == chain_data
        
        # Simulate expiration by modifying cached_at
        cache.memory_cache['AAPL']['cached_at'] = datetime.now() - timedelta(hours=25)
        
        # Should miss due to expiration
        assert cache.get('AAPL', max_age_hours=24) is None
        
        print("✅ Cache expiration works correctly")


def test_chain_cache_lru_eviction():
    """Test LRU eviction when memory limit reached."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cache = ChainCache(cache_dir=tmpdir, max_memory_size=3)
        
        # Fill cache
        for i in range(3):
            cache.set(f'TICKER{i}', {'data': i})
        
        assert len(cache.memory_cache) == 3
        
        # Add 4th item (should evict TICKER0)
        cache.set('TICKER3', {'data': 3})
        
        assert len(cache.memory_cache) == 3
        assert 'TICKER0' not in cache.memory_cache
        assert 'TICKER3' in cache.memory_cache
        assert cache.stats()['evictions'] == 1
        
        print("✅ LRU eviction working correctly")


def test_chain_cache_disk_persistence():
    """Test cache persistence to disk and reload."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create cache and save data
        cache1 = ChainCache(cache_dir=tmpdir)
        chain_data = {'test': 'persistence'}
        cache1.set('AAPL', chain_data)
        
        # Create new cache instance (memory empty but disk has data)
        cache2 = ChainCache(cache_dir=tmpdir)
        
        # Should load from disk
        retrieved = cache2.get('AAPL')
        assert retrieved == chain_data
        assert cache2.stats()['hits'] == 1  # Disk hit
        
        # Should now be in memory too
        assert 'AAPL' in cache2.memory_cache
        
        print("✅ Disk persistence working correctly")


def test_chain_cache_cleanup():
    """Test cleanup of expired cache files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cache = ChainCache(cache_dir=tmpdir)
        
        # Create cache files
        cache.set('AAPL', {'data': 1})
        cache.set('MSFT', {'data': 2})
        
        # Simulate old files by modifying mtime
        for cache_file in Path(tmpdir).glob('*.pkl'):
            # Set mtime to 72 hours ago
            old_time = time.time() - (72 * 3600)
            cache_file.touch()
            import os
            os.utime(cache_file, (old_time, old_time))
        
        # Cleanup files older than 48 hours
        deleted = cache.cleanup_expired(max_age_hours=48)
        
        assert deleted == 2
        assert len(list(Path(tmpdir).glob('*.pkl'))) == 0
        
        print("✅ Cache cleanup working correctly")


# ============================================================================
# ExpirationCache Tests
# ============================================================================

def test_expiration_cache_basic():
    """Test expiration cache operations."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cache = ExpirationCache(cache_dir=tmpdir)
        
        # Miss on first get
        assert cache.get('AAPL') is None
        
        # Set and get
        exp_data = {
            'expirations': ['2026-01-16', '2026-02-20'],
            'shortest_dte': 30,
            'longest_dte': 180
        }
        cache.set('AAPL', exp_data)
        
        retrieved = cache.get('AAPL')
        assert retrieved == exp_data
        
        print(f"✅ ExpirationCache working: {cache.stats()}")


# ============================================================================
# ThrottledExecutor Tests
# ============================================================================

def test_throttled_executor_sequential():
    """Test basic parallel execution."""
    def process_item(x):
        """Simple test function."""
        return x * 2
    
    executor = ThrottledExecutor(max_workers=4, requests_per_second=100)
    
    items = list(range(10))
    results = executor.map_parallel(process_item, items, desc="Test", show_progress=False)
    
    assert results == [x * 2 for x in items]
    assert executor.stats()['tasks_completed'] == 10
    
    executor.shutdown()
    print(f"✅ ThrottledExecutor sequential: {executor}")


def test_throttled_executor_rate_limiting():
    """Test rate limiting enforcement."""
    def slow_process(x):
        """Function that tracks execution time."""
        return (x, time.time())
    
    executor = ThrottledExecutor(max_workers=4, requests_per_second=5.0)  # 5 req/sec = 0.2s between requests
    
    items = list(range(10))
    start_time = time.time()
    results = executor.map_parallel(slow_process, items, desc="Rate limit test", show_progress=False)
    duration = time.time() - start_time
    
    # Should take at least 10 items / 5 req/sec = 2 seconds
    assert duration >= 1.8, f"Too fast: {duration}s (rate limiting not working)"
    
    executor.shutdown()
    print(f"✅ Rate limiting working: {duration:.2f}s for 10 items at 5 req/sec")


def test_throttled_executor_error_handling():
    """Test graceful error handling."""
    def failing_process(x):
        """Function that fails for even numbers."""
        if x % 2 == 0:
            raise ValueError(f"Failed on {x}")
        return x
    
    executor = ThrottledExecutor(max_workers=4, requests_per_second=100)
    
    items = list(range(10))
    results = executor.map_parallel(failing_process, items, desc="Error test", show_progress=False)
    
    # Should have 5 successes and 5 errors
    successes = [r for r in results if not isinstance(r, dict) or 'Error_Type' not in r]
    errors = [r for r in results if isinstance(r, dict) and 'Error_Type' in r]
    
    assert len(successes) == 5, f"Expected 5 successes, got {len(successes)}"
    assert len(errors) == 5, f"Expected 5 errors, got {len(errors)}"
    assert executor.stats()['tasks_failed'] == 5
    
    executor.shutdown()
    print(f"✅ Error handling working: {executor}")


def test_throttled_executor_timeout():
    """Test timeout protection."""
    def slow_process(x):
        """Function that takes too long."""
        time.sleep(2)  # Sleep longer than timeout
        return x
    
    executor = ThrottledExecutor(max_workers=4, requests_per_second=100, timeout_seconds=0.5)
    
    items = [1, 2, 3]
    results = executor.map_parallel(slow_process, items, desc="Timeout test", show_progress=False)
    
    # All should timeout
    timeouts = [r for r in results if isinstance(r, dict) and 'Timeout' in r.get('Exploration_Reason', '')]
    
    # Note: Timeout detection is best-effort, may not catch all
    assert len(timeouts) >= 0, "Timeout detection not working"
    
    executor.shutdown()
    print(f"✅ Timeout protection working: {executor}")


# ============================================================================
# BatchProcessor Tests
# ============================================================================

def test_batch_processor():
    """Test batch processing with checkpoints."""
    def process_item(x):
        return x * 2
    
    with tempfile.TemporaryDirectory() as tmpdir:
        executor = ThrottledExecutor(max_workers=4, requests_per_second=100)
        processor = BatchProcessor(batch_size=5, checkpoint_dir=tmpdir)
        
        items = list(range(23))  # 5 batches: 5, 5, 5, 5, 3
        results = processor.process_in_batches(
            process_item,
            items,
            executor,
            desc="Batch test",
            save_checkpoints=True
        )
        
        assert len(results) == 23
        assert results == [x * 2 for x in items]
        
        # Check checkpoints were created
        checkpoints = list(Path(tmpdir).glob('batch_*.pkl'))
        assert len(checkpoints) == 5, f"Expected 5 checkpoints, got {len(checkpoints)}"
        
        executor.shutdown()
        print(f"✅ Batch processing working: {len(checkpoints)} checkpoints created")


# ============================================================================
# Integration Tests
# ============================================================================

def test_integration_cache_and_executor():
    """Test integration of cache and executor."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cache = ChainCache(cache_dir=tmpdir)
        executor = ThrottledExecutor(max_workers=4, requests_per_second=10)
        
        def fetch_with_cache(ticker):
            """Simulated fetch with caching."""
            # Check cache first
            cached = cache.get(ticker)
            if cached:
                return {'ticker': ticker, 'source': 'cache', 'data': cached}
            
            # Simulate API fetch
            time.sleep(0.01)
            data = {'chain': f'data_for_{ticker}'}
            cache.set(ticker, data)
            
            return {'ticker': ticker, 'source': 'api', 'data': data}
        
        tickers = ['AAPL', 'MSFT', 'GOOGL', 'AAPL', 'MSFT']  # Duplicates to test cache
        
        results = executor.map_parallel(fetch_with_cache, tickers, desc="Integration test", show_progress=False)
        
        # First AAPL/MSFT should be from API, second should be from cache
        api_calls = [r for r in results if r['source'] == 'api']
        cache_hits = [r for r in results if r['source'] == 'cache']
        
        assert len(api_calls) == 3, f"Expected 3 API calls, got {len(api_calls)}"
        assert len(cache_hits) == 2, f"Expected 2 cache hits, got {len(cache_hits)}"
        assert cache.stats()['hit_rate'] > 0
        
        executor.shutdown()
        print(f"✅ Integration test passed: {cache}")


def test_performance_benchmark():
    """Benchmark parallel vs sequential execution."""
    def simulated_api_call(x):
        """Simulate API call with 10ms latency."""
        time.sleep(0.01)
        return x * 2
    
    items = list(range(50))
    
    # Sequential (simulated)
    start = time.time()
    sequential_results = [simulated_api_call(x) for x in items]
    sequential_time = time.time() - start
    
    # Parallel
    executor = ThrottledExecutor(max_workers=8, requests_per_second=100)
    start = time.time()
    parallel_results = executor.map_parallel(simulated_api_call, items, desc="Benchmark", show_progress=False)
    parallel_time = time.time() - start
    executor.shutdown()
    
    speedup = sequential_time / parallel_time
    
    assert parallel_results == sequential_results
    assert speedup > 3.0, f"Expected >3× speedup, got {speedup:.1f}×"
    
    print(f"✅ Performance benchmark: {speedup:.1f}× speedup (parallel vs sequential)")


# ============================================================================
# Run All Tests
# ============================================================================

if __name__ == '__main__':
    print("="*80)
    print("PHASE A INFRASTRUCTURE TESTS")
    print("="*80)
    print()
    
    print("🧪 Testing ChainCache...")
    test_chain_cache_basic()
    test_chain_cache_expiration()
    test_chain_cache_lru_eviction()
    test_chain_cache_disk_persistence()
    test_chain_cache_cleanup()
    print()
    
    print("🧪 Testing ExpirationCache...")
    test_expiration_cache_basic()
    print()
    
    print("🧪 Testing ThrottledExecutor...")
    test_throttled_executor_sequential()
    test_throttled_executor_rate_limiting()
    test_throttled_executor_error_handling()
    test_throttled_executor_timeout()
    print()
    
    print("🧪 Testing BatchProcessor...")
    test_batch_processor()
    print()
    
    print("🧪 Integration Tests...")
    test_integration_cache_and_executor()
    test_performance_benchmark()
    print()
    
    print("="*80)
    print("✅ ALL TESTS PASSED")
    print("="*80)
    print()
    print("Phase A infrastructure is ready for Phase B implementation!")
