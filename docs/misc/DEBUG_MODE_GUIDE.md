# Pipeline Debug Mode - Usage Guide

## Purpose
Comprehensive step-by-step execution trace to diagnose why Tier-1 strategies do or do not result in executable option contracts.

## What It Does
- **Traces single ticker** through entire pipeline (Steps 1-11)
- **Shows PASS/FAIL** at each stage with explicit reasons
- **Tier-aware gating** - only Tier-1 strategies proceed to execution
- **NO logic changes** - uses existing pipeline code
- **Structured logging** - both console and JSON output

## Usage

### Basic Usage
```bash
python cli/run_pipeline_debug.py --ticker AAPL
```

### With Custom IVHV Gap
```bash
python cli/run_pipeline_debug.py --ticker MSFT --min-gap 3.5
```

### Short Flags
```bash
python cli/run_pipeline_debug.py -t AAPL -g 2.0
```

## Output

### Console Output Format
```
🔍 PIPELINE DEBUG TRACE - TICKER: AAPL
================================================================================

📂 STEP 1: Load Snapshot
--------------------------------------------------------------------------------
✅ Step 1: PASS | Loaded snapshot_20251226_120000.csv | Count: 1

📊 STEP 2: Parse Snapshot
--------------------------------------------------------------------------------
✅ Step 2: PASS | IV_Rank=75.3, IV_Trend=Rising, HV_Trend=Stable | Count: 1

📉 STEP 3: IVHV Gap Filter
--------------------------------------------------------------------------------
✅ Step 3: PASS | Gap=4.2, Regime=[ElevatedVol, IV_Rich] | Count: 1

📈 STEP 5: Chart Classification
--------------------------------------------------------------------------------
✅ Step 5: PASS | Pattern=Bullish_Breakout, Strength=Strong | Count: 1

💎 STEP 6: GEM Filter
--------------------------------------------------------------------------------
✅ Step 6: PASS | GEM_Score=82.5, Tier=Prime | Count: 1

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
     • Call Debit Spread - Requires spreads approval (upgrade broker account)
     • Iron Condor - Requires advanced spreads approval (upgrade broker account)

✅ Step 7B: PASS | T1=3, T2=3, T3=2 | Count: 8

🚪 STEP 9A: Tier Execution Gate
--------------------------------------------------------------------------------

  ✅ Proceeding with 3 Tier-1 strategies:
     • Long Call
     • Cash-Secured Put
     • Long Straddle

  ⏭️  Skipped 5 Tier-2+ strategies:
     • Call Debit Spread (Tier 2) - Requires spreads approval
     • Iron Condor (Tier 2) - Requires advanced spreads approval
     • Calendar Spread (Tier 3) - Requires multi-expiration logic

✅ Step 9A: PASS | 3 Tier-1 strategies approved | Count: 3

📋 STEP 9B: Fetch Option Contracts
--------------------------------------------------------------------------------

  ✅ Contract Fetch Results:
     • Success: 2 strategies
     • Failed: 1 strategies

  💧 Liquidity Filter:
     • Passed: 2
     • Failed: 0

✅ Step 9B: PASS | 2 with contracts | Count: 2

🎲 STEP 10: PCS Scoring Filter
--------------------------------------------------------------------------------

  PCS Score Results:
     • Passed (≥70): 2
     • Failed (<70): 0

  ✅ Top PCS Scores:
     • Long Call: PCS=85.2
     • Cash-Secured Put: PCS=78.5

✅ Step 10: PASS | 2 passed PCS ≥ 70 | Count: 2

🎯 STEP 11: Final Execution Decision
--------------------------------------------------------------------------------

  ✅ EXECUTABLE STRATEGIES (2):
     • Long Call (PCS=85.2)
     • Cash-Secured Put (PCS=78.5)

✅ Step 11: PASS | 2 executable strategies | Count: 2

================================================================================
📋 EXECUTION SUMMARY
================================================================================

Ticker: AAPL
Final Result: EXECUTION_READY

✅ No blockers - execution ready!

Pipeline Stats:
   • Passed Steps: 9
   • Failed Steps: 0
   • Total Steps: 9

================================================================================

📄 Structured log saved: output/debug_execution_trace_AAPL_20251226_143522.json
```

### JSON Output Format
```json
{
  "ticker": "AAPL",
  "timestamp": "2025-12-26T14:35:22.123456",
  "min_gap": 2.0,
  "final_result": "EXECUTION_READY",
  "blockers": [],
  "steps": [
    {
      "step": "1",
      "result": "PASS",
      "detail": "Loaded snapshot_20251226_120000.csv",
      "reason": "",
      "count": 1,
      "timestamp": "2025-12-26T14:35:22.123456"
    },
    {
      "step": "7B",
      "result": "PASS",
      "detail": "T1=3, T2=3, T3=2",
      "reason": "",
      "count": 8,
      "timestamp": "2025-12-26T14:35:23.456789"
    }
  ],
  "summary": {
    "total_steps": 9,
    "passed_steps": 9,
    "failed_steps": 0
  }
}
```

## Tier System Rules

### Tier 1 (Broker-Approved - EXECUTABLE)
- ✅ Long Call/Put
- ✅ Covered Call
- ✅ Cash-Secured Put
- ✅ Wheel Strategy
- ✅ Buy-Write
- ✅ Rolling Covered Call
- ✅ Long Straddle/Strangle

**These proceed to Step 9B for option chain scanning.**

### Tier 2 (Broker-Blocked)
- ⛔ Call/Put Debit Spreads
- ⛔ Call/Put Credit Spreads
- ⛔ Iron Condor
- ⛔ Iron Butterfly

**Blocker**: Requires Level 2+ broker approval (upgrade account)  
**Status**: Shows as "Strategy_Only" - no contract scanning

### Tier 3 (Logic-Blocked)
- 🔧 Calendar Spreads
- 🔧 Diagonal Spreads
- 🔧 PMCC variants
- 🔧 LEAP variants

**Blocker**: Requires multi-expiration logic or LEAP filtering  
**Status**: Shows as "Strategy_Only" - no contract scanning

## Expected Failure Scenarios

### Scenario 1: Ticker Not in Snapshot
```
❌ Step 1: FAIL | Ticker not in snapshot | Reason: XYZ not found in snapshot_20251226_120000.csv
```

### Scenario 2: IVHV Gap Too Low
```
❌ Step 3: FAIL | IV=25.3, HV=24.8, Gap=0.5 | Reason: Gap 0.5 < threshold 2.0
```

### Scenario 3: No GEM Qualification
```
❌ Step 6: FAIL | Reason: Did not meet GEM criteria
```

### Scenario 4: All Strategies Tier 2+
```
❌ Step 9A: FAIL | 0 Tier-1 strategies | Reason: All strategies are Tier 2+ (non-executable)

  ⛔ Blocked Strategies (Tier 2+):
     • Call Debit Spread (Tier 2) - Requires spreads approval
     • Iron Condor (Tier 2) - Requires advanced spreads approval
```

### Scenario 5: Liquidity Filter Fails
```
❌ Step 9B: FAIL | 0 strategies with contracts | Reason: No_Contracts_Within_Delta=3

  ❌ Contract Fetch Failures:
     • No_Contracts_Within_Delta: 2 strategies
        - Long Call
        - Long Put
```

### Scenario 6: PCS Score Too Low
```
❌ Step 10: FAIL | Avg PCS=65.3 | Reason: All strategies PCS < 70

  ❌ Failed PCS (<70):
     • Long Call: PCS=67.2
     • Cash-Secured Put: PCS=63.5
```

## Pipeline Steps Traced

1. **Step 1**: Load snapshot - find latest snapshot file and ticker
2. **Step 2**: Parse snapshot - enrich with IV/HV trends
3. **Step 3**: IVHV filter - check volatility divergence threshold
4. **Step 5**: Chart classification - identify patterns
5. **Step 6**: GEM filter - qualify for strategy generation
6. **Step 7B**: Strategy ranking - generate + assign tiers
7. **Step 9A**: Tier gate - filter to Tier-1 only
8. **Step 9B**: Fetch contracts - scan option chains + liquidity
9. **Step 10**: PCS scoring - quality filter
10. **Step 11**: Final decision - executable or not

## Exit Codes

- **0**: `EXECUTION_READY` - strategies found and executable
- **1**: `NO_EXECUTION` - legitimate filtering (not an error)
- **1**: `ERROR` - exception occurred

## Use Cases

### Debug Why AAPL Has Zero Strategies
```bash
python cli/run_pipeline_debug.py --ticker AAPL
```
Shows exactly where AAPL gets filtered out.

### Check if MSFT Meets GEM Criteria
```bash
python cli/run_pipeline_debug.py --ticker MSFT
```
Step 6 will show GEM score and tier.

### Validate Tier System for TSLA
```bash
python cli/run_pipeline_debug.py --ticker TSLA
```
Step 7B shows tier breakdown, Step 9A shows gating.

### Find Liquidity Issues for NVDA
```bash
python cli/run_pipeline_debug.py --ticker NVDA
```
Step 9B shows contract fetch and liquidity filter results.

## Success Criteria

✅ **Complete** when:
- Running for AAPL shows where execution fails (if it fails)
- Failure reason is explicit (data, liquidity, PCS, tier)
- Can determine if filters are too strict OR result is correct

## Guiding Principle

> **Zero executable strategies is acceptable.**  
> **Zero explanation is not.**

This tool provides the explanation.
