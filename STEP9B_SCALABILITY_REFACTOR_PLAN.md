# Step 9B Scalability Refactor - Implementation Plan
**Date:** December 28, 2025  
**Objective:** 10-20× performance improvement for S&P 500-level scans  
**Principle:** Exploration ≠ Selection (NO strategies dropped in Step 9B)

---

## Executive Summary

Current Step 9B is too slow for S&P 500 scans because it:
- Fetches full option chains unnecessarily
- Scans all expirations/strikes for every strategy
- Repeats work per ticker
- No distinction between cheap discovery vs expensive analysis

**Target:** Minutes instead of hours for 500-ticker scans

**Non-Negotiable:** All strategies preserved, fully auditable, no silent drops

---

## Architectural Changes

### 1. Two-Phase Exploration (MANDATORY)

#### Phase 1: Sampled Exploration (FAST)
**Purpose:** Cheap structural viability check before expensive deep analysis

**Process:**
```python
def _sampled_exploration(ticker, strategy, dte_range, horizon):
    """
    Pull SINGLE expiration near target DTE.
    Check if strategy is structurally viable.
    NO full chain fetch.
    """
    # 1. Fetch expiration list only (lightweight API call)
    expirations = get_expiration_dates(ticker)
    
    # 2. Select closest expiration to target DTE
    target_dte = (dte_range['min'] + dte_range['max']) / 2
    sampled_exp = find_nearest_expiration(expirations, target_dte)
    
    # 3. Fetch ONLY that expiration's chain
    chain = get_chain_for_expiration(ticker, sampled_exp)
    
    # 4. Quick viability checks
    checks = {
        'expiration_exists': sampled_exp is not None,
        'atm_strike_exists': has_atm_strike(chain),
        'bid_ask_nonzero': has_valid_quotes(chain),
        'oi_positive': has_open_interest(chain),
        'spread_acceptable': check_spread(chain, horizon)
    }
    
    # 5. Classify result
    if all(checks.values()):
        return {
            'Exploration_Status': 'Success',
            'Sampled_Expiration': sampled_exp,
            'Sampled_DTE': calculate_dte(sampled_exp),
            'Sampled_ATM_Strike': get_atm_strike(chain),
            'Sampled_Liquidity_Class': assess_liquidity(chain, horizon),
            'Escalated_To_Full_Chain': False,
            'Exploration_Reason': 'Passed sampled viability checks'
        }
    else:
        # Annotate failure but DON'T reject
        failed_checks = [k for k, v in checks.items() if not v]
        return {
            'Exploration_Status': 'Not_Viable',
            'Exploration_Reason': f"Failed: {', '.join(failed_checks)}",
            'Escalated_To_Full_Chain': False
        }
```

**Horizon-Specific Sampling:**
- SHORT (30-45 DTE): Sample 1 expiration near 37 days
- MEDIUM (45-60 DTE): Sample 1 expiration near 52 days  
- LEAP (180+ DTE): Sample 1 far-dated expiration (6-12 months out)

**Output Columns:**
- `Sampled_Expiration`
- `Sampled_DTE`
- `Sampled_ATM_Strike`
- `Sampled_Liquidity_Class`
- `Exploration_Status` (Success / Not_Viable)
- `Exploration_Reason`
- `Escalated_To_Full_Chain` (False in Phase 1)

---

#### Phase 2: Conditional Deep Exploration (SLOW, LIMITED)
**Purpose:** Full analysis ONLY for strategies that passed Phase 1

**Trigger:** `Exploration_Status == 'Success'`

**Process:**
```python
def _deep_exploration(ticker, strategies_list):
    """
    Fetch full chain ONCE per ticker.
    Cache and reuse for all strategies.
    Evaluate only required strikes per strategy.
    """
    # 1. Check cache first
    if ticker in chain_cache:
        chain = chain_cache[ticker]
    else:
        # 2. Fetch full chain (expensive!)
        chain = fetch_full_option_chain(ticker)
        
        # 3. Cache in memory
        chain_cache[ticker] = chain
        
        # 4. Persist to disk
        save_chain_to_cache(ticker, chain, ttl_hours=24)
    
    # 5. Process all strategies for this ticker
    results = []
    for strategy in strategies_list:
        result = _evaluate_strategy_from_cache(
            ticker, 
            strategy, 
            chain,
            strategy_type=strategy['Recommended_Strategy']
        )
        result['Escalated_To_Full_Chain'] = True
        result['Escalation_Reason'] = 'Passed Phase 1 viability checks'
        results.append(result)
    
    return results
```

**Strategy-Aware Strike Selection:**
```python
def _evaluate_strategy_from_cache(ticker, strategy, chain, strategy_type):
    """
    Only evaluate strikes required by strategy.
    """
    if strategy_type == 'Long Call':
        # Only evaluate calls, ATM ± 3 strikes
        strikes = get_atm_range(chain['calls'], n=3)
    
    elif strategy_type == 'Cash-Secured Put':
        # Only evaluate puts, ATM to -15% OTM
        strikes = get_otm_puts(chain['puts'], range_pct=0.15)
    
    elif strategy_type in ['Long Straddle', 'Long Strangle']:
        # Evaluate ATM call + put pairs
        strikes = get_paired_strikes(chain, strategy_type)
    
    elif 'LEAP' in strategy.get('Horizon_Class', ''):
        # Only far-dated expirations (180+ DTE)
        strikes = get_leap_strikes(chain)
    
    # ... more strategy types
    
    return evaluate_strikes(strikes, strategy)
```

**Output Columns (additions):**
- `Selected_Strike` or `Selected_Strikes` (for multi-leg)
- `Greeks_Delta`, `Greeks_Theta`, etc.
- `Capital_Required`
- `Final_Liquidity_Context`
- `Escalated_To_Full_Chain` (True)
- `Escalation_Reason`

---

### 2. Tier-0 Preflight Filter (OPTIONAL BUT RECOMMENDED)

**Purpose:** Skip expensive computation for obviously unviable tickers

**NOT a trade rejection - compute optimization only**

```python
def _tier0_preflight(ticker, underlying_price, market_cap=None):
    """
    Quick skip checks before ANY API calls.
    These are COMPUTE skips, not trade rejections.
    """
    skip_reasons = []
    
    # Check 1: Ultra-high price with no historical success
    if underlying_price > 1000 and not has_historical_liquidity(ticker):
        skip_reasons.append('Price > $1,000 with no liquidity history')
    
    # Check 2: Micro-cap with no options activity
    if market_cap and market_cap < 5_000_000_000:  # < $5B
        if not has_recent_options_volume(ticker):
            skip_reasons.append('Small cap with no options volume')
    
    # Check 3: Known illiquid ticker (from historical cache)
    if is_historically_illiquid(ticker):
        skip_reasons.append('Historical pattern: consistently illiquid')
    
    if skip_reasons:
        return {
            'Exploration_Skipped': True,
            'Skip_Reason': ' | '.join(skip_reasons),
            'Exploration_Status': 'Skipped_Compute',
            'Exploration_Reason': 'Preflight optimization - not a trade rejection'
        }
    
    return {'Exploration_Skipped': False}
```

**Key Properties:**
- Logged explicitly: `Exploration_Skipped = True`
- Reason documented: `Skip_Reason`
- NOT counted as rejected in audit
- Can be overridden by user flag: `force_full_exploration=True`

**Output Columns:**
- `Exploration_Skipped` (True/False)
- `Skip_Reason`

---

### 3. Expiration-Only Fetch Optimization (MANDATORY)

**Purpose:** Lightweight metadata fetch before expensive chain pull

```python
def _fetch_expiration_metadata(ticker):
    """
    Fetch expiration dates only (no strike data).
    Use to select sampled expiration intelligently.
    """
    # Tradier API: /v1/markets/options/expirations?symbol={ticker}
    expirations = tradier_api.get_expirations(ticker)
    
    # Analyze metadata
    metadata = {
        'available_dtes': [calculate_dte(exp) for exp in expirations],
        'shortest_dte': min(dtes),
        'longest_dte': max(dtes),
        'has_leaps': any(dte > 180 for dte in dtes),
        'expiration_density': len(expirations),
        'weekly_options': has_weekly_options(expirations)
    }
    
    return expirations, metadata
```

**Usage:**
```python
# In Phase 1:
expirations, meta = _fetch_expiration_metadata(ticker)

# Select best expiration for sampling
if strategy['Horizon_Class'] == 'LEAP':
    sampled_exp = select_leap_expiration(expirations, meta)
else:
    sampled_exp = select_nearest_expiration(expirations, target_dte)

# Only then fetch chain for that expiration
chain = get_chain_for_expiration(ticker, sampled_exp)
```

**Performance Gain:** ~80% reduction in data transfer for Phase 1

---

### 4. Strict Chain Caching (MANDATORY)

**Purpose:** Never refetch the same chain during a run

```python
class ChainCache:
    """
    Multi-level chain cache:
    1. In-memory (LRU cache, 100 tickers max)
    2. Disk cache (24-48h TTL)
    3. Redis (optional, for distributed systems)
    """
    
    def __init__(self, cache_dir='data/chain_cache'):
        self.memory_cache = {}  # {ticker: chain_data}
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
    
    def get(self, ticker, max_age_hours=24):
        """Get chain from cache (memory → disk → None)"""
        # 1. Check memory
        if ticker in self.memory_cache:
            return self.memory_cache[ticker]
        
        # 2. Check disk
        cache_file = self.cache_dir / f"{ticker}_{datetime.now().date()}.pkl"
        if cache_file.exists():
            age_hours = (datetime.now() - datetime.fromtimestamp(cache_file.stat().st_mtime)).total_seconds() / 3600
            if age_hours < max_age_hours:
                chain = pickle.load(cache_file.open('rb'))
                self.memory_cache[ticker] = chain  # Promote to memory
                return chain
        
        return None
    
    def set(self, ticker, chain):
        """Save chain to all cache levels"""
        # 1. Memory
        self.memory_cache[ticker] = chain
        
        # 2. Disk
        cache_file = self.cache_dir / f"{ticker}_{datetime.now().date()}.pkl"
        pickle.dump(chain, cache_file.open('wb'))
    
    def stats(self):
        """Return cache hit rate"""
        return {
            'memory_size': len(self.memory_cache),
            'disk_files': len(list(self.cache_dir.glob('*.pkl'))),
            'hit_rate': self._calculate_hit_rate()
        }
```

**Cache Key Strategy:**
- `{ticker}_{date}.pkl` - One cache file per ticker per day
- Invalidates automatically next day
- Can force refresh: `force_refresh=True`

**Expected Improvement:** 50-70% runtime reduction on subsequent runs

---

### 5. Strategy-Aware Laziness (MANDATORY)

**Purpose:** Only fetch what each strategy structurally requires

```python
def _get_required_option_types(strategy_type):
    """Return which option types to fetch"""
    strategy_requirements = {
        'Long Call': ['calls'],
        'Long Put': ['puts'],
        'Cash-Secured Put': ['puts'],
        'Covered Call': ['calls'],
        'Buy-Write': ['calls'],
        'Long Straddle': ['calls', 'puts'],
        'Long Strangle': ['calls', 'puts'],
        'LEAP Call': ['calls'],
        'LEAP Put': ['puts']
    }
    return strategy_requirements.get(strategy_type, ['calls', 'puts'])

def _get_required_strike_range(strategy_type, atm_price):
    """Return strike range to evaluate"""
    if 'Call' in strategy_type:
        return {
            'min': atm_price * 0.95,  # 5% OTM
            'max': atm_price * 1.10   # 10% ITM
        }
    elif 'Put' in strategy_type:
        return {
            'min': atm_price * 0.85,  # 15% OTM
            'max': atm_price * 1.05   # 5% ITM
        }
    elif 'Straddle' in strategy_type:
        return {
            'min': atm_price * 0.98,  # ATM ± 2%
            'max': atm_price * 1.02
        }
    # ... more strategy types
```

**Usage:**
```python
# In Phase 2:
option_types = _get_required_option_types(strategy['Recommended_Strategy'])
strike_range = _get_required_strike_range(strategy['Recommended_Strategy'], underlying_price)

# Filter chain to only required data
filtered_chain = filter_chain(
    full_chain,
    option_types=option_types,
    strike_range=strike_range
)

# Evaluate only filtered strikes
result = evaluate_strikes(filtered_chain, strategy)
```

**Performance Gain:** 60-80% reduction in strikes evaluated per strategy

---

### 6. Parallelism With Safety Cap (MANDATORY)

**Purpose:** Process multiple tickers concurrently without API throttling

```python
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

class ThrottledExecutor:
    """
    Thread pool with rate limiting to respect API constraints.
    """
    
    def __init__(self, max_workers=8, requests_per_second=10):
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.rate_limit = 1.0 / requests_per_second
        self.last_request_time = 0
    
    def submit_with_throttle(self, fn, *args, **kwargs):
        """Submit task with rate limiting"""
        # Enforce rate limit
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)
        
        self.last_request_time = time.time()
        return self.executor.submit(fn, *args, **kwargs)
    
    def map_parallel(self, fn, items, desc="Processing"):
        """
        Parallel map with progress tracking.
        Deterministic order (results match input order).
        """
        futures = []
        for item in items:
            future = self.submit_with_throttle(fn, item)
            futures.append((item, future))
        
        # Collect results in original order
        results = []
        for item, future in futures:
            try:
                result = future.result(timeout=30)  # 30s timeout per ticker
                results.append(result)
            except Exception as e:
                # Log error but continue (don't fail entire batch)
                logger.error(f"Failed to process {item}: {e}")
                results.append(create_error_result(item, str(e)))
        
        return results

# Usage in Step 9B:
executor = ThrottledExecutor(max_workers=8, requests_per_second=10)

# Phase 1: Parallel sampled exploration
sampled_results = executor.map_parallel(
    _sampled_exploration,
    strategy_list,
    desc="Phase 1: Sampled Exploration"
)

# Phase 2: Parallel deep exploration (only for successes)
successful_strategies = [s for s in sampled_results if s['Exploration_Status'] == 'Success']
deep_results = executor.map_parallel(
    _deep_exploration,
    successful_strategies,
    desc="Phase 2: Deep Exploration"
)
```

**Safety Features:**
- Hard cap on concurrency (8-12 workers)
- Rate limiting (10 req/sec default)
- Timeouts per ticker (30s)
- Graceful error handling (continue on failure)
- Deterministic order (results match input)

**Performance Gain:** 5-8× speedup on multi-core machines

---

## New Output Schema

### Required Columns (ADD ALL)

```python
# Phase 1 columns (all strategies get these)
'Sampled_Expiration': datetime,
'Sampled_DTE': int,
'Sampled_ATM_Strike': float,
'Sampled_Liquidity_Class': str,  # Excellent/Good/Acceptable/Thin
'Exploration_Status': str,  # Success/Not_Viable/Skipped_Compute
'Exploration_Reason': str,
'Exploration_Skipped': bool,
'Skip_Reason': str,
'Escalated_To_Full_Chain': bool,
'Escalation_Reason': str,

# Phase 2 columns (only if Escalated_To_Full_Chain=True)
'Selected_Strike': float,  # or Selected_Strikes for multi-leg
'Selected_Expiration': datetime,
'Final_DTE': int,
'Greeks_Delta': float,
'Greeks_Theta': float,
'Greeks_Vega': float,
'Greeks_Gamma': float,
'Capital_Required': float,
'Final_Liquidity_Context': str,
'Chain_Cache_Hit': bool,
'Phase1_Duration_Ms': int,
'Phase2_Duration_Ms': int
```

---

## Implementation Steps

### Step 1: Create Phase 1 Sampled Exploration
**File:** `core/scan_engine/step9b_fetch_contracts.py`

```python
def _phase1_sampled_exploration(df_strategies: pd.DataFrame) -> pd.DataFrame:
    """
    Phase 1: Fast sampled exploration.
    Pull single expiration per strategy, check viability.
    NO full chain fetch.
    """
    results = []
    
    for idx, row in df_strategies.iterrows():
        # 1. Tier-0 preflight check
        preflight = _tier0_preflight(row['Ticker'], row['Close'])
        if preflight['Exploration_Skipped']:
            results.append({**row.to_dict(), **preflight})
            continue
        
        # 2. Fetch expiration metadata
        try:
            expirations, meta = _fetch_expiration_metadata(row['Ticker'])
        except Exception as e:
            results.append({
                **row.to_dict(),
                'Exploration_Status': 'API_Error',
                'Exploration_Reason': f'Expiration fetch failed: {e}'
            })
            continue
        
        # 3. Select sampled expiration
        target_dte = (row['DTE_Min'] + row['DTE_Max']) / 2
        sampled_exp = _select_nearest_expiration(expirations, target_dte)
        
        if sampled_exp is None:
            results.append({
                **row.to_dict(),
                'Exploration_Status': 'Not_Viable',
                'Exploration_Reason': f'No expiration found near DTE {target_dte:.0f}'
            })
            continue
        
        # 4. Fetch ONLY sampled expiration's chain
        try:
            chain = _fetch_single_expiration_chain(row['Ticker'], sampled_exp)
        except Exception as e:
            results.append({
                **row.to_dict(),
                'Exploration_Status': 'API_Error',
                'Exploration_Reason': f'Chain fetch failed: {e}'
            })
            continue
        
        # 5. Quick viability checks
        viability = _assess_sampled_viability(
            chain,
            row['Recommended_Strategy'],
            row.get('Horizon_Class', 'MEDIUM')
        )
        
        results.append({**row.to_dict(), **viability})
    
    return pd.DataFrame(results)
```

### Step 2: Create Phase 2 Deep Exploration
**File:** `core/scan_engine/step9b_fetch_contracts.py`

```python
def _phase2_deep_exploration(df_successes: pd.DataFrame, chain_cache: ChainCache) -> pd.DataFrame:
    """
    Phase 2: Deep exploration for strategies that passed Phase 1.
    Fetch full chain ONCE per ticker, cache and reuse.
    """
    # Group by ticker to minimize API calls
    results = []
    
    for ticker, group in df_successes.groupby('Ticker'):
        # 1. Check cache first
        chain = chain_cache.get(ticker)
        cache_hit = chain is not None
        
        if chain is None:
            # 2. Fetch full chain (expensive!)
            try:
                chain = _fetch_full_chain(ticker)
                chain_cache.set(ticker, chain)
            except Exception as e:
                # Mark all strategies for this ticker as failed
                for idx, row in group.iterrows():
                    results.append({
                        **row.to_dict(),
                        'Exploration_Status': 'Chain_Fetch_Failed',
                        'Exploration_Reason': f'Full chain fetch error: {e}',
                        'Escalated_To_Full_Chain': True,
                        'Chain_Cache_Hit': False
                    })
                continue
        
        # 3. Evaluate all strategies for this ticker
        for idx, row in group.iterrows():
            try:
                evaluation = _evaluate_strategy_from_cache(
                    ticker,
                    row,
                    chain,
                    strategy_type=row['Recommended_Strategy']
                )
                results.append({
                    **row.to_dict(),
                    **evaluation,
                    'Escalated_To_Full_Chain': True,
                    'Escalation_Reason': 'Passed Phase 1 viability',
                    'Chain_Cache_Hit': cache_hit
                })
            except Exception as e:
                results.append({
                    **row.to_dict(),
                    'Exploration_Status': 'Evaluation_Error',
                    'Exploration_Reason': f'Strike evaluation failed: {e}',
                    'Escalated_To_Full_Chain': True,
                    'Chain_Cache_Hit': cache_hit
                })
    
    return pd.DataFrame(results)
```

### Step 3: Refactor Main Entry Point
**File:** `core/scan_engine/step9b_fetch_contracts.py`

```python
def fetch_and_select_contracts(
    df_strategies: pd.DataFrame,
    num_contracts: int = 1,
    dollar_allocation: float = 1000.0,
    force_full_exploration: bool = False,
    enable_parallelism: bool = True,
    max_workers: int = 8
) -> pd.DataFrame:
    """
    Two-phase option chain exploration.
    
    Phase 1: Sampled exploration (fast viability check)
    Phase 2: Deep exploration (full chain, only for successes)
    
    Args:
        df_strategies: Strategy recommendations from Step 9A
        num_contracts: Target number of contracts (for position sizing)
        dollar_allocation: Max capital per position
        force_full_exploration: Skip Tier-0 preflight (for testing)
        enable_parallelism: Use parallel execution
        max_workers: Thread pool size
    
    Returns:
        DataFrame with ALL strategies preserved + exploration metadata
    """
    logger.info(f"🔎 Step 9B: Two-phase exploration for {len(df_strategies)} strategies")
    
    # Initialize chain cache
    chain_cache = ChainCache(cache_dir='data/chain_cache')
    
    # ========================================
    # PHASE 1: SAMPLED EXPLORATION (FAST)
    # ========================================
    logger.info("⚡ Phase 1: Sampled exploration (fast viability checks)")
    start_phase1 = time.time()
    
    if enable_parallelism:
        executor = ThrottledExecutor(max_workers=max_workers, requests_per_second=10)
        df_phase1 = executor.map_parallel(
            _sampled_exploration_single,
            df_strategies.to_dict('records'),
            desc="Phase 1"
        )
        df_phase1 = pd.DataFrame(df_phase1)
    else:
        df_phase1 = _phase1_sampled_exploration(df_strategies)
    
    phase1_duration = time.time() - start_phase1
    logger.info(f"✅ Phase 1 complete: {phase1_duration:.1f}s")
    
    # Phase 1 stats
    status_counts = df_phase1['Exploration_Status'].value_counts()
    logger.info(f"📊 Phase 1 Status: {status_counts.to_dict()}")
    
    # ========================================
    # PHASE 2: DEEP EXPLORATION (CONDITIONAL)
    # ========================================
    successes = df_phase1[df_phase1['Exploration_Status'] == 'Success']
    
    if len(successes) == 0:
        logger.warning("⚠️  Phase 2 skipped: No strategies passed Phase 1")
        return df_phase1
    
    logger.info(f"🔬 Phase 2: Deep exploration for {len(successes)} successful strategies")
    start_phase2 = time.time()
    
    if enable_parallelism:
        # Group by ticker for cache efficiency
        ticker_groups = successes.groupby('Ticker')
        df_phase2 = executor.map_parallel(
            lambda ticker: _deep_exploration_ticker(ticker, successes, chain_cache),
            ticker_groups.groups.keys(),
            desc="Phase 2"
        )
        df_phase2 = pd.concat(df_phase2, ignore_index=True)
    else:
        df_phase2 = _phase2_deep_exploration(successes, chain_cache)
    
    phase2_duration = time.time() - start_phase2
    logger.info(f"✅ Phase 2 complete: {phase2_duration:.1f}s")
    
    # Merge Phase 1 failures + Phase 2 results
    failures = df_phase1[df_phase1['Exploration_Status'] != 'Success']
    df_final = pd.concat([failures, df_phase2], ignore_index=True)
    
    # ========================================
    # INTEGRITY CHECKS
    # ========================================
    assert len(df_final) == len(df_strategies), "Row count mismatch!"
    
    logger.info(f"📊 Final Exploration Summary:")
    logger.info(f"   Total strategies: {len(df_final)}")
    logger.info(f"   Phase 1 duration: {phase1_duration:.1f}s")
    logger.info(f"   Phase 2 duration: {phase2_duration:.1f}s")
    logger.info(f"   Total duration: {phase1_duration + phase2_duration:.1f}s")
    logger.info(f"   Cache hit rate: {chain_cache.stats()['hit_rate']:.1%}")
    
    return df_final
```

### Step 4: Add Helper Functions

All helper functions needed:
- `_tier0_preflight()`
- `_fetch_expiration_metadata()`
- `_select_nearest_expiration()`
- `_fetch_single_expiration_chain()`
- `_assess_sampled_viability()`
- `_fetch_full_chain()`
- `_evaluate_strategy_from_cache()`
- `_get_required_option_types()`
- `_get_required_strike_range()`
- `ChainCache` class
- `ThrottledExecutor` class

### Step 5: Update Tests

Create test suite:
```python
# test_step9b_scalability.py

def test_phase1_sampled_exploration():
    """Test Phase 1 fast sampled exploration"""
    # Should complete in <5s for 100 strategies
    pass

def test_phase2_deep_exploration():
    """Test Phase 2 conditional deep exploration"""
    # Should only run for Phase 1 successes
    pass

def test_chain_caching():
    """Test chain cache hit rate"""
    # Should achieve >80% hit rate on 2nd run
    pass

def test_parallelism():
    """Test parallel execution"""
    # Should be 5-8× faster than sequential
    pass

def test_no_strategies_dropped():
    """Test exploration integrity"""
    # Row count IN == row count OUT
    pass

def test_sp500_scale():
    """Test S&P 500 scale (500 tickers)"""
    # Should complete in <10 minutes
    pass
```

---

## Performance Targets

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| 100 tickers | 30 min | 2-3 min | 10-15× |
| 500 tickers (S&P 500) | 2.5 hrs | 10-15 min | 10-15× |
| API calls per ticker | 5-10 | 1-2 | 5× reduction |
| Cache hit rate (2nd run) | 0% | 80%+ | ∞ |
| Strategies preserved | 100% | 100% | No change |

---

## Migration Path

### Phase A: Prepare Infrastructure (Week 1)
- [ ] Create `ChainCache` class
- [ ] Create `ThrottledExecutor` class
- [ ] Add expiration-only fetch functions
- [ ] Set up disk cache directory structure

### Phase B: Implement Phase 1 (Week 2)
- [ ] Build `_phase1_sampled_exploration()`
- [ ] Add Tier-0 preflight filter
- [ ] Test on 10 tickers
- [ ] Validate integrity (no drops)

### Phase C: Implement Phase 2 (Week 3)
- [ ] Build `_phase2_deep_exploration()`
- [ ] Integrate chain caching
- [ ] Add strategy-aware laziness
- [ ] Test on 100 tickers

### Phase D: Add Parallelism (Week 4)
- [ ] Integrate `ThrottledExecutor`
- [ ] Test rate limiting
- [ ] Benchmark performance
- [ ] Tune worker count

### Phase E: Final Integration (Week 5)
- [ ] Refactor main entry point
- [ ] Update output schema
- [ ] Add new columns to dashboard
- [ ] Test on S&P 500 scale
- [ ] Document performance gains

---

## Success Criteria

✅ **Performance:**
- S&P 500 scan completes in <15 minutes
- Cache hit rate >80% on 2nd run
- API call reduction >5×

✅ **Integrity:**
- All strategies preserved (row count IN == OUT)
- No silent drops
- Full audit trail

✅ **Quality:**
- LEAPs remain visible
- High-price stocks annotated, not rejected
- Exploration status always populated

✅ **Scalability:**
- Handles 500+ tickers without timeout
- Graceful degradation on API errors
- Resumable on failure

---

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| API rate limiting | `ThrottledExecutor` with 10 req/sec cap |
| Cache stale data | 24h TTL, force refresh option |
| Parallel race conditions | Thread-safe `ChainCache` |
| Memory exhaustion | LRU cache with 100-ticker limit |
| Network timeouts | 30s timeout per ticker, continue on error |
| Data loss | Persist to disk + audit logs |

---

## Next Steps

1. **Review this plan** - Confirm approach aligns with requirements
2. **Prioritize phases** - Which phase to implement first?
3. **Set timeline** - How much time available per phase?
4. **Start with Phase A** - Infrastructure first (ChainCache, ThrottledExecutor)

**Question:** Should we proceed with Phase A (infrastructure setup)?
