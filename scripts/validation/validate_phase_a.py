"""Quick validation of Phase A infrastructure components."""

import sys
from pathlib import Path
import tempfile

# Add parent to path
parent_dir = Path(__file__).parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

print("="*80)
print("PHASE A INFRASTRUCTURE VALIDATION")
print("="*80)
print()

# Test 1: ChainCache import and basic operations
print("1️⃣ Testing ChainCache...")
try:
    from core.scan_engine.chain_cache import ChainCache
    
    with tempfile.TemporaryDirectory() as tmpdir:
        cache = ChainCache(cache_dir=tmpdir, max_memory_size=5)
        
        # Set and get
        cache.set('AAPL', {'test': 'data'})
        result = cache.get('AAPL')
        
        assert result == {'test': 'data'}, "Cache get/set failed"
        assert cache.stats()['memory_size'] == 1, "Memory cache not working"
        
        print(f"   ✅ ChainCache working: {cache}")
except Exception as e:
    print(f"   ❌ ChainCache failed: {e}")
    sys.exit(1)

# Test 2: ThrottledExecutor import and basic operations
print("2️⃣ Testing ThrottledExecutor...")
try:
    from core.scan_engine.throttled_executor import ThrottledExecutor
    
    def test_fn(x):
        return x * 2
    
    executor = ThrottledExecutor(max_workers=2, requests_per_second=10)
    results = executor.map_parallel(test_fn, [1, 2, 3], show_progress=False)
    executor.shutdown()
    
    assert results == [2, 4, 6], "Parallel execution failed"
    
    print(f"   ✅ ThrottledExecutor working: {executor}")
except Exception as e:
    print(f"   ❌ ThrottledExecutor failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 3: ExpirationCache
print("3️⃣ Testing ExpirationCache...")
try:
    from core.scan_engine.chain_cache import ExpirationCache
    
    with tempfile.TemporaryDirectory() as tmpdir:
        exp_cache = ExpirationCache(cache_dir=tmpdir)
        
        exp_cache.set('AAPL', {'expirations': ['2026-01-16']})
        result = exp_cache.get('AAPL')
        
        assert result == {'expirations': ['2026-01-16']}, "ExpirationCache failed"
        
        print(f"   ✅ ExpirationCache working: {exp_cache.stats()}")
except Exception as e:
    print(f"   ❌ ExpirationCache failed: {e}")
    sys.exit(1)

# Test 4: BatchProcessor
print("4️⃣ Testing BatchProcessor...")
try:
    from core.scan_engine.throttled_executor import BatchProcessor, ThrottledExecutor
    
    with tempfile.TemporaryDirectory() as tmpdir:
        executor = ThrottledExecutor(max_workers=2, requests_per_second=50)
        processor = BatchProcessor(batch_size=3, checkpoint_dir=tmpdir)
        
        results = processor.process_in_batches(
            lambda x: x * 2,
            list(range(10)),
            executor,
            save_checkpoints=False
        )
        executor.shutdown()
        
        assert len(results) == 10, "Batch processing failed"
        assert results == [x * 2 for x in range(10)], "Batch results incorrect"
        
        print(f"   ✅ BatchProcessor working: {len(results)} items processed")
except Exception as e:
    print(f"   ❌ BatchProcessor failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print()
print("="*80)
print("✅ PHASE A INFRASTRUCTURE VALIDATED")
print("="*80)
print()
print("Components ready:")
print("  • ChainCache - Multi-level option chain caching")
print("  • ExpirationCache - Lightweight expiration metadata cache")
print("  • ThrottledExecutor - Parallel execution with rate limiting")
print("  • BatchProcessor - Large-scale batch processing")
print()
print("Next steps:")
print("  1. Review STEP9B_SCALABILITY_REFACTOR_PLAN.md")
print("  2. Proceed to Phase B: Implement Phase 1 Sampled Exploration")
print("  3. Test with 10-100 tickers")
print("  4. Proceed to Phase C: Implement Phase 2 Deep Exploration")
