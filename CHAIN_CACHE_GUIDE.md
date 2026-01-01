"""
CHAIN CACHING GUIDE

This document explains the chain caching system and how to use it for
deterministic, fast iteration during development and debugging.

================================================================================
WHY CHAIN CACHING?
================================================================================

PROBLEM:
  - API calls are slow (1-3 seconds per ticker)
  - API quota limits iteration speed
  - Non-deterministic data (market changes between runs)
  - Debugging requires exact same data each time

SOLUTION:
  - Cache raw option chains to disk
  - First run: Fetch from API + cache (seconds)
  - Subsequent runs: Read from cache (milliseconds)
  - Deterministic: Same data every time

IMPACT:
  - Development: Minutes → Milliseconds
  - Debugging: Reproducible with exact historical data
  - Cost: Reduce API quota usage by 90%+
  - Sanity: Same input → same output

================================================================================
WHAT IS CACHED?
================================================================================

✅ CACHED (Raw Market Data):
  - Option chains (strike, bid, ask, volume, OI)
  - Greeks (delta, gamma, theta, vega, IV)
  - Underlying price snapshots
  - Available expirations

❌ NOT CACHED (Derived Decisions):
  - PCS scores (always computed fresh)
  - Contract selection decisions
  - Status annotations (Explored_*, etc.)
  - Liquidity grades
  - Strategy recommendations

WHY THIS SPLIT?
  - Cache only immutable market data
  - Recompute all logic on every run
  - Allows testing logic changes without refetching chains
  - Separates concerns: Data vs. Decisions

================================================================================
CACHE KEY DESIGN
================================================================================

Format: {Ticker}_{Expiration}_{AsOfDate}.pkl

Examples:
  AAPL_2025-02-14_2025-12-28.pkl
  MSFT_2026-01-16_2025-12-28.pkl  (LEAP)
  GOOGL_2025-01-17_2025-12-28.pkl

Key Properties:
  - Ticker: Identifies the underlying security
  - Expiration: Specific option expiration date
  - AsOfDate: Data snapshot date (defaults to today)
  - Granular: One file per (Ticker, Expiration) pair

Storage Location:
  Default: .cache/chains/
  Custom:  export CHAIN_CACHE_DIR=/path/to/cache

================================================================================
USAGE PATTERNS
================================================================================

PATTERN 1: NORMAL DEVELOPMENT (Cache Enabled)
----------------------------------------------
# Enable caching for session
export DEBUG_CACHE_CHAINS=1

# First run - builds cache from API calls
python run_pipeline.py
# Output: ✅ 127 tickers × 3 expirations = ~381 API calls (slow)

# Subsequent runs - use cached data
python run_pipeline.py  # Instant!
python run_pipeline.py  # Still instant!

# Make logic changes, rerun instantly
vim core/scan_engine/step9b_fetch_contracts.py
python run_pipeline.py  # Tests new logic with cached data


PATTERN 2: PRODUCTION RUN (Cache Disabled)
-------------------------------------------
# Disable caching (default)
unset DEBUG_CACHE_CHAINS

# Always fetch fresh data from API
python run_pipeline.py
# Output: Fresh market data, no cache


PATTERN 3: DEBUG WITH FROZEN DATA
----------------------------------
# Day 1: Reproduce issue
export DEBUG_CACHE_CHAINS=1
python run_pipeline.py  # Caches problematic data

# Day 2-N: Fix issue with exact same data
python run_pipeline.py  # Uses cached data from Day 1
# Data is frozen - perfect for debugging


PATTERN 4: CACHE MANAGEMENT
----------------------------
# View cache statistics
python -c "
from core.scan_engine.step9b_fetch_contracts import ChainCache
cache = ChainCache(enabled=True)
print(cache.stats())
"

# Clear specific ticker
python -c "
from core.scan_engine.step9b_fetch_contracts import ChainCache
cache = ChainCache(enabled=True)
cache.clear('AAPL')  # Clear only AAPL
"

# Clear all cache
rm -rf .cache/chains/*

================================================================================
PERFORMANCE COMPARISON
================================================================================

Scenario: 127 tickers, 3 strategies each, 381 total chains

WITHOUT CACHE (API calls):
  - Time per chain: ~1.5 seconds
  - Total time: 381 × 1.5s = 571 seconds (~9.5 minutes)
  - API quota used: 381 calls
  - Reproducibility: None (data changes)

WITH CACHE (disk reads):
  - First run: 571 seconds (build cache)
  - Subsequent runs: ~2 seconds (read cache)
  - Speedup: 285× faster
  - API quota used: 0 calls
  - Reproducibility: 100% (frozen data)

DEVELOPMENT WORKFLOW:
  - Without cache: Change logic → wait 10 min → test → repeat (hours)
  - With cache: Change logic → wait 2 sec → test → repeat (minutes)

================================================================================
CACHE LIFECYCLE
================================================================================

1. INITIALIZATION
   - Check DEBUG_CACHE_CHAINS=1
   - Create .cache/chains/ directory
   - Load ChainCache instance

2. FETCH CHAIN
   - Build cache key: (Ticker, Expiration, AsOfDate)
   - Check cache: .cache/chains/{key}.pkl exists?
     - YES: Read from disk (milliseconds)
     - NO: Fetch from API + write to cache (seconds)

3. USE CHAIN
   - Apply liquidity filters (computed fresh)
   - Select strikes (computed fresh)
   - Generate status (computed fresh)
   - All logic runs on every iteration

4. CACHE EXPIRY
   - Manual: Delete .pkl files
   - Automatic: AsOfDate in filename (stale detection possible)
   - Best practice: Clear cache daily in production

================================================================================
DEBUGGING WITH CACHE
================================================================================

PROBLEM: "Why did AAPL get rejected on Dec 15?"

SOLUTION: Cache-based reproduction

Step 1: Enable cache on Dec 15
  export DEBUG_CACHE_CHAINS=1
  export CHAIN_CACHE_DIR=.cache/dec15_issue
  python run_pipeline.py  # Issue occurs, data cached

Step 2: Analyze cached data
  ls .cache/dec15_issue/
  # AAPL_2025-02-14_2025-12-15.pkl
  # AAPL_2026-01-16_2025-12-15.pkl

Step 3: Fix logic (days later)
  vim core/scan_engine/step9b_fetch_contracts.py
  # Change LEAP threshold logic

Step 4: Test fix with exact Dec 15 data
  export CHAIN_CACHE_DIR=.cache/dec15_issue
  python run_pipeline.py  # Uses frozen Dec 15 data
  # Verify AAPL no longer rejected

Step 5: Validate with fresh data
  unset DEBUG_CACHE_CHAINS
  python run_pipeline.py  # Fresh API data

================================================================================
CACHE SAFETY
================================================================================

✅ SAFE:
  - Cache enabled during development
  - Cache enabled for debugging
  - Cache enabled for testing logic changes
  - Multiple cache dirs for different scenarios

❌ UNSAFE:
  - Cache enabled in production (data staleness risk)
  - Caching PCS scores (defeats iterative improvement)
  - Caching decisions/statuses (breaks logic updates)
  - Never clearing cache (disk bloat)

BEST PRACTICES:
  1. Use cache for development/debug only
  2. Clear cache daily (or per trading day)
  3. Separate cache dirs per scenario
  4. Document cache usage in commits
  5. Never commit .cache/ to git

================================================================================
INTEGRATION WITH PHASE 1 FIXES
================================================================================

Cache enables rapid iteration on Phase 1 fixes:

WORKFLOW:
  1. Enable cache + run pipeline (build cache)
  2. Modify LEAP thresholds in step9b
  3. Rerun instantly with cached chains
  4. Check status distribution (should see more Explored_*)
  5. Adjust thresholds
  6. Rerun instantly
  7. Repeat until optimal

Without cache: Each iteration takes 10 minutes
With cache: Each iteration takes 2 seconds

This turns Phase 1 fix tuning from hours → minutes.

================================================================================
TROUBLESHOOTING
================================================================================

ISSUE: Cache not enabled
SOLUTION:
  export DEBUG_CACHE_CHAINS=1
  python run_pipeline.py
  # Should see "🗄️  Chain cache ENABLED: .cache/chains"

ISSUE: Cache hit but data looks wrong
SOLUTION:
  # Clear cache and rebuild
  rm -rf .cache/chains/*
  python run_pipeline.py

ISSUE: Disk space growing
SOLUTION:
  # Check cache size
  du -sh .cache/chains/
  # Clear old entries
  find .cache/chains/ -mtime +7 -delete

ISSUE: Want separate caches for different experiments
SOLUTION:
  # Experiment 1
  export CHAIN_CACHE_DIR=.cache/experiment1
  python run_pipeline.py

  # Experiment 2
  export CHAIN_CACHE_DIR=.cache/experiment2
  python run_pipeline.py

================================================================================
NEXT STEPS
================================================================================

Now that caching is enabled:

1. ✅ Run pipeline with cache to validate Phase 1 fixes
   export DEBUG_CACHE_CHAINS=1
   python run_pipeline.py

2. ✅ Audit status distribution
   python audit_status_distribution.py
   # Expect: More Explored_* than hard failures
   # Expect: LEAPs present and annotated
   # Expect: 180-240/266 strategies with data

3. ✅ Iterate on thresholds if needed
   # With cache: Each iteration takes seconds
   # Without cache: Each iteration takes minutes

4. ⏳ Proceed to PCS (Step 10) redesign
   # Now that data is preserved, PCS can be a ranking system
   # Penalize thin liquidity, wide spreads, capital inefficiency
   # Never erase data

================================================================================
"""

if __name__ == '__main__':
    print(__doc__)
