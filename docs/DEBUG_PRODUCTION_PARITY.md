# Debug/Production Parity - Implementation Summary

**Status:** ✅ COMPLETE
**Date:** 2026-02-07
**Audit Report:** See audit findings in git history

---

## ARCHITECTURAL PRINCIPLE

```
Debug = Production ÷ Scale
```

Debug mode is NOT a separate execution path. It is the production pipeline with a restricted ticker universe.

---

## PROBLEM STATEMENT

### What Was Wrong (Before Fix)

The original debug mode (`cli/run_pipeline_debug.py`) was a **separate orchestrator** (721 lines) that:

❌ Used custom `PipelineDebugTracer` class
❌ Manually orchestrated steps (bypassing production pipeline)
❌ Skipped Step 10 (PCS recalibration)
❌ Used simplified Step 12 (no two-stage gate)
❌ No Fidelity IV escalation (Stage 2 absent)
❌ No maturity integration (Stage 5 absent)
❌ No wait loop re-evaluation (Step -1 absent)

**Result:** Debug produced 0 READY candidates not because of correct gating, but because of **logic divergence** from production.

### Parity Violations Identified

| Violation | Impact |
|-----------|--------|
| **V1:** Separate execution path | Different orchestration logic |
| **V2:** Manual snapshot loading | Missing DuckDB integration |
| **V3:** Enrichment bypass | Incomplete persistence |
| **V4:** Missing Step 10 | No PCS scoring |
| **V5:** Simplified Step 12 | No two-stage gate |
| **V6:** No wait loop | Missing AWAIT_CONFIRMATION state |
| **V7:** No maturity integration | INCOME strategies skip 120-day IV requirement |

---

## SOLUTION IMPLEMENTED

### What Changed (After Fix)

**File:** `cli/run_pipeline_debug.py` (REPLACED - now 257 lines)

✅ Calls production `run_full_scan_pipeline()` directly
✅ Sets `DEBUG_TICKER_MODE=1` environment variable
✅ All steps execute in production order
✅ All execution gates active
✅ Two-stage gate with Fidelity escalation
✅ Maturity tier classification
✅ Wait loop re-evaluation

**Result:** Debug now executes identical logic as production, differing only by ticker count.

---

## USAGE

### Basic Debug Mode

```bash
# Default debug tickers (AAPL, AMZN, NVDA)
python cli/run_pipeline_debug.py
```

### Single Ticker Debug

```bash
python cli/run_pipeline_debug.py --ticker AAPL
```

### Custom Ticker List

```bash
export DEBUG_TICKERS=TSLA,MSFT,COIN
python cli/run_pipeline_debug.py
```

### With Specific Snapshot

```bash
python cli/run_pipeline_debug.py --snapshot data/snapshots/ivhv_snapshot_live_20260207_120000.csv
```

---

## HOW IT WORKS

### Environment Variables

- **`DEBUG_TICKER_MODE`**: Set to `"1"` to activate debug mode
- **`DEBUG_TICKERS`**: Comma-separated list of tickers to restrict universe

### Universe Restriction Logic

**File:** `scan_engine/debug/debug_mode.py:46-82`

```python
def restrict_universe(self, df: pd.DataFrame, top_n: Optional[int] = None):
    """Restricts ticker universe if DEBUG_TICKER_MODE=1"""
    if os.getenv("DEBUG_TICKER_MODE") != "1":
        return df  # No restriction in production

    id_col = 'Symbol' if 'Symbol' in df.columns else 'Ticker'
    return df[df[id_col].isin(self.debug_tickers)].copy()
```

**Called From:** `scan_engine/step2_load_snapshot.py:872`

```python
# Universe restriction happens inside load_ivhv_snapshot()
if os.getenv("DEBUG_TICKER_MODE") == "1":
    df = debug_manager.restrict_universe(df)
```

### Execution Flow

```
cli/run_pipeline_debug.py
  ↓
Sets DEBUG_TICKER_MODE=1
  ↓
Calls scan_engine.pipeline.run_full_scan_pipeline()
  ↓
Step 2: load_ivhv_snapshot() checks DEBUG_TICKER_MODE
  ↓
DebugManager.restrict_universe() filters to debug tickers
  ↓
Pipeline continues with production logic
  ↓
All steps execute (Step -1, 2-12, Stage 2, Stage 5)
  ↓
Same gates, same persistence, same maturity checks
```

---

## VALIDATION

### Parity Tests

**File:** `test/test_debug_production_parity.py`

Run validation tests:

```bash
pytest test/test_debug_production_parity.py -v
```

### Test Coverage

1. **test_same_ticker_same_execution_status**: Verify identical `Execution_Status` for same ticker
2. **test_same_snapshot_same_gate_decisions**: Verify identical gate logic
3. **test_debug_mode_scale_only**: Verify ONLY scale differs (meta-test)

### Manual Validation

```bash
# Run production
python -m scan_engine

# Run debug for AAPL
python cli/run_pipeline_debug.py --ticker AAPL

# Compare outputs
diff output/Step12_Acceptance_<prod_ts>.csv output/Step12_Acceptance_<debug_ts>.csv

# Expected: AAPL rows should be identical
```

---

## INVARIANTS (VERIFIED)

### ✅ Same Data Sources
- Debug: Schwab API, DuckDB, Fidelity scraper
- Production: Schwab API, DuckDB, Fidelity scraper

### ✅ Same Execution Gates
- Debug: Two-stage gate with `apply_execution_gate()` called twice
- Production: Two-stage gate with `apply_execution_gate()` called twice

### ✅ Same Maturity Logic
- Debug: `apply_maturity_and_eligibility()` at Stage 5
- Production: `apply_maturity_and_eligibility()` at Stage 5

### ✅ Same IV Escalation Rules
- Debug: `_step_enrich_with_fidelity_long_term_iv()` at Stage 2
- Production: `_step_enrich_with_fidelity_long_term_iv()` at Stage 2

### ✅ Same READY/WAIT/BLOCKED Classification
- Debug: Uses `Execution_Status` with values `READY`, `AWAIT_CONFIRMATION`, `BLOCKED`, `CONDITIONAL`
- Production: Uses `Execution_Status` with values `READY`, `AWAIT_CONFIRMATION`, `BLOCKED`, `CONDITIONAL`

---

## EXECUTION GATE FLOW (SAME IN BOTH MODES)

```
Step 9B: Contract Selection
  ↓
Step 12 (Initial Pass)
  ├─ R0.1: Critical data missing → BLOCKED
  ├─ R0.2: Illiquid contract → BLOCKED
  ├─ R0.3: INCOME strategy → AWAIT_CONFIRMATION (IV_Fidelity_Required=True)
  ├─ R0.4: DIRECTIONAL + MATURE IV → AWAIT_CONFIRMATION (IV_Fidelity_Required=False)
  └─ R0.5: Default → AWAIT_CONFIRMATION
  ↓
Stage 2: Fidelity IV Enrichment
  ├─ Filter: Execution_Status=='AWAIT_CONFIRMATION' AND IV_Fidelity_Required==True
  ├─ Query DuckDB for long-term IV
  └─ Merge Fidelity IV back to acceptance_all
  ↓
Step 12 (Final Pass)
  ├─ R1.1-R1.6: Hard blocks (missing data, immature IV, API failures)
  ├─ R2.1-R2.5: Conditional (partial data, caution required)
  └─ R3.1-R3.2: READY (mature IV, good liquidity)
  ↓
Stage 5: Maturity & Eligibility
  ├─ Compute Volatility_Maturity_Tier from iv_history_count
  ├─ INCOME requires MATURE (120+ days)
  ├─ DIRECTIONAL requires EARLY+ (7+ days)
  └─ Generate Fidelity demand report
```

---

## TROUBLESHOOTING

### "0 READY strategies in debug mode"

This is CORRECT when:
- IV history accumulating (< 120 days for INCOME strategies)
- Liquidity filters block contracts
- Maturity gates active

**Verify parity:**
```bash
# Check if production also has 0 READY for same tickers
python cli/run_pipeline_debug.py --ticker AAPL
# Then check output/Step12_Acceptance_*.csv for Gate_Reason
```

### "Debug produces different Execution_Status than production"

This is a PARITY VIOLATION. File a bug with:
1. Snapshot used (timestamp)
2. Ticker tested
3. Debug `Execution_Status` and `Gate_Reason`
4. Production `Execution_Status` and `Gate_Reason`

### "Debug skipping steps"

Verify `DEBUG_TICKER_MODE=1` is set:
```bash
python -c "import os; print(os.getenv('DEBUG_TICKER_MODE'))"
```

If not set, universe won't be restricted, but all steps still run.

---

## MAINTENANCE

### Adding New Execution Gates

When adding gates to production:

✅ **DO:** Add to `apply_execution_gate()` in `step12_acceptance.py`
✅ **DO:** Document in rule comments (R#.#)
❌ **DON'T:** Add separate debug-specific logic
❌ **DON'T:** Use environment checks to bypass gates

### Testing New Features

Always test with debug mode enabled:

```bash
export DEBUG_TICKER_MODE=1
export DEBUG_TICKERS=AAPL
pytest test/test_new_feature.py -v
```

If feature works in debug but not production → **PARITY VIOLATION**

---

## FILES MODIFIED

| File | Change | Lines |
|------|--------|-------|
| `cli/run_pipeline_debug.py` | **REPLACED** | 721 → 257 |
| `scan_engine/debug/debug_mode.py` | No change | (already correct) |
| `scan_engine/step2_load_snapshot.py` | No change | (already correct) |
| `scan_engine/pipeline.py` | No change | (already correct) |
| `test/test_debug_production_parity.py` | **NEW** | 257 |
| `docs/DEBUG_PRODUCTION_PARITY.md` | **NEW** | (this file) |

---

## REFERENCES

- **Audit Report:** Principal Engineering Review (2026-02-07)
- **Architecture:** `ARCHITECTURE_ROADMAP.md`
- **Execution Semantics:** `docs/EXECUTION_SEMANTICS.md`
- **Debug Manager:** `scan_engine/debug/debug_mode.py`
- **Pipeline Orchestrator:** `scan_engine/pipeline.py`

---

## ACCEPTANCE CRITERIA (MET)

✅ Debug and production execute identical code paths
✅ Same snapshot → same Step 12 output (except ticker count)
✅ Same missing data → same BLOCKED reason
✅ Same maturity state → same eligibility decision
✅ Debug = Production ÷ `len(DEBUG_TICKERS)`

---

**Status:** ✅ Debug/Production parity established
**Last Updated:** 2026-02-07
**Auditor:** Principal Engineering Review
