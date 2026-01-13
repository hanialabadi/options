#!/bin/bash
#
# CHAIN CACHE QUICK REFERENCE
# ========================================================================
# Common commands for working with chain caching
# ========================================================================

# ========================================================================
# BASIC USAGE
# ========================================================================

# Enable caching for development
enable_cache() {
    export DEBUG_CACHE_CHAINS=1
    echo "✅ Chain caching ENABLED"
    echo "   Location: ${CHAIN_CACHE_DIR:-.cache/chains}"
}

# Disable caching for production
disable_cache() {
    unset DEBUG_CACHE_CHAINS
    echo "🚫 Chain caching DISABLED (fresh API calls)"
}

# Run pipeline with caching
run_with_cache() {
    export DEBUG_CACHE_CHAINS=1
    echo "🚀 Running pipeline with chain caching..."
    ./venv/bin/python run_pipeline.py "$@"
}

# Run pipeline without caching
run_without_cache() {
    unset DEBUG_CACHE_CHAINS
    echo "🚀 Running pipeline with fresh API data..."
    ./venv/bin/python run_pipeline.py "$@"
}

# ========================================================================
# CACHE MANAGEMENT
# ========================================================================

# View cache statistics
cache_stats() {
    export DEBUG_CACHE_CHAINS=1
    ./venv/bin/python -c "
from core.scan_engine.step9b_fetch_contracts import ChainCache
import json
cache = ChainCache(enabled=True)
stats = cache.stats()
print('📊 Cache Statistics:')
print(json.dumps(stats, indent=2))
"
}

# Clear all cache
cache_clear_all() {
    echo "🗑️  Clearing ALL cache entries..."
    rm -rf .cache/chains/*
    echo "✅ Cache cleared"
}

# Clear specific ticker
cache_clear_ticker() {
    local ticker=$1
    if [ -z "$ticker" ]; then
        echo "❌ Usage: cache_clear_ticker TICKER"
        return 1
    fi
    
    export DEBUG_CACHE_CHAINS=1
    ./venv/bin/python -c "
from core.scan_engine.step9b_fetch_contracts import ChainCache
cache = ChainCache(enabled=True)
deleted = cache.clear('$ticker')
print(f'🗑️  Cleared {deleted} cache entries for $ticker')
"
}

# Check cache size
cache_size() {
    if [ -d .cache/chains ]; then
        echo "📦 Cache size:"
        du -sh .cache/chains/
        echo ""
        echo "📋 Cache entries:"
        ls -lh .cache/chains/ | tail -n +2 | wc -l | xargs echo "  Files:"
    else
        echo "❌ Cache directory not found"
    fi
}

# ========================================================================
# VALIDATION & AUDIT
# ========================================================================

# Test cache functionality
test_cache() {
    export DEBUG_CACHE_CHAINS=1
    echo "🧪 Testing chain cache..."
    ./venv/bin/python test_chain_cache.py
}

# Audit status distribution (after running pipeline)
audit_status() {
    echo "🔍 Auditing status distribution..."
    ./venv/bin/python audit_status_distribution.py
}

# Full validation workflow
validate_phase1() {
    echo "="*70
    echo "PHASE 1 VALIDATION WORKFLOW"
    echo "="*70
    
    # Step 1: Enable cache
    enable_cache
    
    # Step 2: Test cache infrastructure
    echo ""
    echo "Step 1: Testing cache infrastructure..."
    test_cache
    
    # Step 3: Run pipeline with cache
    echo ""
    echo "Step 2: Running pipeline with cache..."
    run_with_cache
    
    # Step 4: Audit results
    echo ""
    echo "Step 3: Auditing status distribution..."
    audit_status
    
    echo ""
    echo "✅ Phase 1 validation complete"
}

# ========================================================================
# DEBUGGING SCENARIOS
# ========================================================================

# Freeze current market data for debugging
freeze_data() {
    local scenario=$1
    if [ -z "$scenario" ]; then
        scenario="debug_$(date +%Y%m%d_%H%M%S)"
    fi
    
    export DEBUG_CACHE_CHAINS=1
    export CHAIN_CACHE_DIR=".cache/${scenario}"
    
    echo "📸 Freezing market data for scenario: $scenario"
    echo "   Cache dir: $CHAIN_CACHE_DIR"
    
    ./venv/bin/python run_pipeline.py
    
    echo ""
    echo "✅ Data frozen in: $CHAIN_CACHE_DIR"
    echo "   To replay: export CHAIN_CACHE_DIR=.cache/${scenario}"
}

# Replay frozen scenario
replay_scenario() {
    local scenario=$1
    if [ -z "$scenario" ]; then
        echo "❌ Usage: replay_scenario SCENARIO_NAME"
        echo "   Available scenarios:"
        ls -d .cache/*/ 2>/dev/null | sed 's|.cache/||' | sed 's|/||'
        return 1
    fi
    
    if [ ! -d ".cache/${scenario}" ]; then
        echo "❌ Scenario not found: $scenario"
        return 1
    fi
    
    export DEBUG_CACHE_CHAINS=1
    export CHAIN_CACHE_DIR=".cache/${scenario}"
    
    echo "🔄 Replaying scenario: $scenario"
    echo "   Using cache: $CHAIN_CACHE_DIR"
    
    ./venv/bin/python run_pipeline.py
}

# ========================================================================
# PERFORMANCE TESTING
# ========================================================================

# Benchmark with vs without cache
benchmark_cache() {
    echo "⚡ Benchmarking cache performance..."
    
    # Clear cache for fair comparison
    cache_clear_all
    
    # First run (build cache)
    echo ""
    echo "Run 1: Building cache (API calls)..."
    time run_with_cache > /dev/null 2>&1
    
    # Second run (use cache)
    echo ""
    echo "Run 2: Using cache (disk reads)..."
    time run_with_cache > /dev/null 2>&1
    
    # Third run (no cache)
    echo ""
    echo "Run 3: Without cache (API calls)..."
    time run_without_cache > /dev/null 2>&1
}

# ========================================================================
# HELP
# ========================================================================

show_help() {
    cat << 'EOF'
CHAIN CACHE QUICK REFERENCE
========================================================================

BASIC USAGE:
  enable_cache              Enable caching for session
  disable_cache             Disable caching
  run_with_cache            Run pipeline with caching
  run_without_cache         Run pipeline without caching

CACHE MANAGEMENT:
  cache_stats               View cache statistics
  cache_clear_all           Clear all cache entries
  cache_clear_ticker AAPL   Clear specific ticker
  cache_size                Check cache disk usage

VALIDATION:
  test_cache                Test cache functionality
  audit_status              Audit status distribution
  validate_phase1           Run full Phase 1 validation

DEBUGGING:
  freeze_data [name]        Freeze current data for debugging
  replay_scenario name      Replay frozen scenario
  benchmark_cache           Performance comparison

EXAMPLES:
  # Development workflow
  enable_cache
  run_with_cache
  audit_status
  
  # Debug specific issue
  freeze_data "leap_issue_dec28"
  # ... fix code ...
  replay_scenario "leap_issue_dec28"
  
  # Production run
  disable_cache
  run_without_cache

ENVIRONMENT VARIABLES:
  DEBUG_CACHE_CHAINS=1      Enable caching
  CHAIN_CACHE_DIR=path      Custom cache location

For more info: cat CHAIN_CACHE_GUIDE.md
EOF
}

# ========================================================================
# MAIN
# ========================================================================

# If sourced, make functions available
# If executed, show help
if [ "${BASH_SOURCE[0]}" == "${0}" ]; then
    show_help
fi
