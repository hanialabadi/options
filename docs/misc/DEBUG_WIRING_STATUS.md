# ✅ Debug CLI Wired to Real Pipeline

## What Was Done

### 1. **Real Functions Wired** (No Placeholders)

Updated `cli/run_pipeline_debug.py` to use actual pipeline functions:

```python
from core.scan_engine.step2_load_snapshot import load_ivhv_snapshot
from core.scan_engine.step3_filter_ivhv import filter_ivhv_gap  
from core.scan_engine.step5_chart_signals import compute_chart_signals
from core.scan_engine.step6_gem_filter import validate_data_quality
from core.scan_engine.step7b_multi_strategy_ranker import generate_multi_strategy_suggestions
from core.scan_engine.step9b_fetch_contracts import fetch_and_select_contracts
from core.strategy_tiers import get_strategy_tier, is_execution_ready, get_execution_blocker
```

### 2. **✅ Import Test Passes**

```bash
$ python test_debug_wiring.py
✅ All imports successful!
```

### 3. **Tier Gating Already in Pipeline**

`step9b_fetch_contracts.py` already has Tier 1 execution gate (lines 97-115):

```python
if 'Execution_Ready' not in df.columns:
    logger.warning("⚠️ 'Execution_Ready' column missing")
    
tier1 = df[df['Execution_Ready'] == True].copy()
tier2_plus = df[df['Execution_Ready'] == False].copy()

logger.info(f"Tier 1 (will scan chains): {len(tier1)}")
logger.info(f"Tier 2+ (strategy-only): {len(tier2_plus)}")
```

**This means:**
- ✅ Tier gating logic exists
- ✅ Step 9B already filters to Tier 1 only
- ✅ Debug script will show this happening

## What's Needed to Run

### 1. **Create or Load a Snapshot**

Option A - Use existing data:
```bash
# Check if you have raw data
ls data/raw/

# If you have raw IV/HV export from Fidelity, process it
```

Option B - Generate test snapshot:
```bash
# Run Step 0 scraper if available
# Or manually create test CSV
```

### 2. **Run Debug for AAPL**

```bash
python cli/run_pipeline_debug.py --ticker AAPL
```

**Expected Output Structure:**

```
🔍 PIPELINE DEBUG TRACE - TICKER: AAPL
================================================================================

📂 STEP 1: Load Snapshot
--------------------------------------------------------------------------------
✅ Step 1: PASS | Loaded snapshot_20251226_120000.csv | Count: 1

📊 STEP 2: Load & Enrich Snapshot  
--------------------------------------------------------------------------------
✅ Step 2: PASS | IV_Rank=75.3, IV_Trend=Rising, HV_Trend=Stable | Count: 1

📉 STEP 3: IVHV Gap Filter
--------------------------------------------------------------------------------
✅ Step 3: PASS | Gap=4.2, Regime=[ElevatedVol, IV_Rich] | Count: 1

📈 STEP 5: Chart Signals & Market Regime
--------------------------------------------------------------------------------
✅ Step 5: PASS | Signal=Bullish, Regime=Trending, Trend=2.50 | Count: 1

💎 STEP 6: Data Quality Validation
--------------------------------------------------------------------------------
✅ Step 6: PASS | Data complete, Crossover=Age_0_5 | Count: 1

🎯 STEP 7B: Strategy Ranking + Tier Assignment
--------------------------------------------------------------------------------

  Strategy Breakdown:
  • Total Strategies: 8
  • Tier 1 (Broker-Approved): 3
  • Tier 2 (Broker-Blocked): 3  
  • Tier 3 (Logic-Blocked): 2

  ✅ Tier 1 Strategies (Executable):
     • Long Call (Score: 85.50)
     • Cash-Secured Put (Score: 78.30)
     • Long Straddle (Score: 72.10)

  ⏭️  Tier 2 Strategies (Broker-Blocked):
     • Call Debit Spread - Requires spreads approval
     • Iron Condor - Requires advanced spreads approval

✅ Step 7B: PASS | T1=3, T2=3, T3=2 | Count: 8

🚪 STEP 9A: Tier Execution Gate
--------------------------------------------------------------------------------

  ✅ Proceeding with 3 Tier-1 strategies:
     • Long Call
     • Cash-Secured Put  
     • Long Straddle

  ⏭️  Skipped 5 Tier-2+ strategies

✅ Step 9A: PASS | 3 Tier-1 strategies approved | Count: 3

📋 STEP 9B: Fetch Option Contracts
--------------------------------------------------------------------------------
[Tier 1 gating happens here - built into fetch_and_select_contracts]

  ✅ Contract Fetch Results:
     • Success: 2 strategies
     • Failed: 1 strategies

✅ Step 9B: PASS | 2 with contracts | Count: 2

🎯 STEP 11: Final Execution Decision
--------------------------------------------------------------------------------

  ✅ EXECUTABLE STRATEGIES (2):
     • Long Call | Strike: 190
     • Cash-Secured Put | Strike: 180

✅ Step 11: PASS | 2 executable strategies | Count: 2

================================================================================
📋 EXECUTION SUMMARY
================================================================================

Ticker: AAPL
Final Result: EXECUTION_READY

✅ No blockers - execution ready!

Pipeline Stats:
   • Passed Steps: 8
   • Failed Steps: 0
   • Total Steps: 8

================================================================================

📄 Structured log saved: output/debug_execution_trace_AAPL_20251226_143522.json
```

## What This Achieves

### ✅ Observability (Zero Logic Changes)
- See exactly where tickers pass/fail
- Explicit reasons at each step
- Tier breakdown visible
- Blocker tracking

### ✅ AAPL Test Case
AAPL should:
- ✅ Pass liquidity (highly liquid)
- ✅ Have tight spreads
- ✅ Have contracts available  
- ✅ Generate Tier 1 strategies

If AAPL fails:
- **NOT market reality** (AAPL is liquid)
- **YES filter logic or assumptions**
- Debug output shows exactly where/why

### ✅ Tier System Validation
When Step 7B generates strategies:
- Shows Tier 1 count (broker-approved)
- Shows Tier 2 count (broker-blocked spreads)
- Shows Tier 3 count (logic-blocked multi-expiry)

When Step 9A gates:
- Only Tier 1 proceeds
- Tier 2+ explicitly skipped with reasons

When Step 9B fetches:
- Built-in tier check (already in pipeline)
- Only scans chains for Tier 1
- Tier 2+ passthrough as "Strategy_Only"

## Next Steps

1. **Get Snapshot Data**
   - Check if Step 0 scraper works
   - Or manually export IV/HV from Fidelity
   - Save to `data/snapshots/snapshot_YYYYMMDD_HHMMSS.csv`

2. **Run Debug**
   ```bash
   python cli/run_pipeline_debug.py --ticker AAPL
   ```

3. **Analyze Results**
   - If NO_EXECUTION → check blockers
   - If EXECUTION_READY → system works!
   - Either way → you have explanation

4. **Test Other Tickers**
   ```bash
   python cli/run_pipeline_debug.py --ticker MSFT
   python cli/run_pipeline_debug.py --ticker NVDA --min-gap 3.5
   ```

## Critical Notes

### ✅ This is NOT:
- ❌ Dashboard work
- ❌ Filter tuning
- ❌ Performance optimization
- ❌ New features
- ❌ Logic changes

### ✅ This IS:
- ✅ Observability
- ✅ Diagnostic tool
- ✅ Tier validation
- ✅ Blocker identification

### Key Insight

> **Zero executable strategies is acceptable.**  
> **Zero explanation is not.**

This debug mode provides the **explanation**.

## Files Modified

- ✅ `cli/run_pipeline_debug.py` - Wired to real functions
- ✅ `test_debug_wiring.py` - Import validation (passes)
- ✅ `DEBUG_MODE_GUIDE.md` - Usage documentation
- ✅ `cli/run_pipeline_debug_simple.py` - Concept demo

## Status

**READY TO RUN** - Just need snapshot data.

The debug CLI is fully wired to the real pipeline. All imports work. Tier gating is already built into Step 9B. The system will show you exactly where and why execution succeeds or fails.
