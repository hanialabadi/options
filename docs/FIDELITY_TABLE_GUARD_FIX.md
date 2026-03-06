# Fidelity IV Table Guard Fix

**Date**: 2026-02-06
**Status**: ✅ COMPLETE
**Type**: Production-Safe Diagnostic Enhancement

---

## PROBLEM STATEMENT

The system correctly blocked trade execution when `fidelity_iv_long_term_history` table was missing or empty, but did so **silently** without diagnostic signals. This made it difficult to understand why candidates were being blocked.

### Observed Behavior
- IV Rank resolution fails silently
- IV maturity remains IMMATURE
- Execution Gate escalates to R0.5
- All candidates become WAIT/BLOCKED
- **No clear diagnostic message** explaining the root cause

---

## ROOT CAUSE ANALYSIS

### Code Locations
1. **[iv_term_history.py:281-359](../core/shared/data_layer/iv_term_history.py#L281-L359)**: `get_fidelity_iv_rank()`
   - Queries `fidelity_iv_long_term_history` table
   - Silently catches all exceptions and returns `None`
   - No logging when table is missing

2. **[resolver_implementations.py:189-229](../core/enrichment/resolver_implementations.py#L189-L229)**: `resolve_iv_rank_from_cache()`
   - Calls `get_fidelity_iv_rank()` for each ticker
   - No pre-flight check for table existence
   - Generic error logging only

3. **[resolver_implementations.py:104-182](../core/enrichment/resolver_implementations.py#L104-L182)**: `resolve_iv_history_from_fidelity()`
   - Checks Fidelity cache but doesn't validate table exists
   - Silent failure on query errors

### Why This Is Correct Behavior
The system is **architecturally correct**:
- ✅ Missing IV data → IV maturity = IMMATURE
- ✅ IMMATURE IV → Execution blocked per gating rules
- ✅ No execution without validated IV Rank

The issue was **observability**, not correctness.

---

## SOLUTION IMPLEMENTED

### Approach: Guard Queries with `table_exists()` Checks

Added explicit table existence checks in all three functions that query `fidelity_iv_long_term_history`:

#### 1. `get_fidelity_iv_rank()` - [iv_term_history.py:311-321](../core/shared/data_layer/iv_term_history.py#L311-L321)

```python
# GUARD: Check if fidelity_iv_long_term_history table exists
table_exists = con.execute("""
    SELECT COUNT(*) FROM information_schema.tables
    WHERE table_name = 'fidelity_iv_long_term_history'
    AND table_schema = 'main'
""").fetchone()[0] > 0

if not table_exists:
    logger.warning(
        f"⚠️ DIAGNOSTIC: fidelity_iv_long_term_history table does not exist. "
        f"IV Rank unavailable for {ticker}. "
        f"Run Fidelity scraper (scan_engine/iv2_v2.py) to populate IV history."
    )
    return None
```

#### 2. `resolve_iv_rank_from_cache()` - [resolver_implementations.py:207-219](../core/enrichment/resolver_implementations.py#L207-L219)

```python
# GUARD: Check if fidelity_iv_long_term_history table exists
table_exists = con.execute("""
    SELECT COUNT(*) FROM information_schema.tables
    WHERE table_name = 'fidelity_iv_long_term_history'
    AND table_schema = 'main'
""").fetchone()[0] > 0

if not table_exists:
    logger.warning(
        f"⚠️ DIAGNOSTIC: fidelity_iv_long_term_history table does not exist. "
        f"IV Rank unavailable for {len(tickers)} tickers. "
        f"Execution will be blocked per gating rules. "
        f"Run Fidelity scraper (scan_engine/iv2_v2.py) to populate IV history."
    )
    con.close()
    return results
```

#### 3. `resolve_iv_history_from_fidelity()` - [resolver_implementations.py:154-166](../core/enrichment/resolver_implementations.py#L154-L166)

```python
# GUARD: Check if fidelity_iv_long_term_history table exists
table_exists = con.execute("""
    SELECT COUNT(*) FROM information_schema.tables
    WHERE table_name = 'fidelity_iv_long_term_history'
    AND table_schema = 'main'
""").fetchone()[0] > 0

if not table_exists:
    logger.warning(
        f"⚠️ DIAGNOSTIC: fidelity_iv_long_term_history table does not exist. "
        f"Cannot check Fidelity cache for {len(tickers)} tickers. "
        f"Run Fidelity scraper (scan_engine/iv2_v2.py) to populate IV history."
    )
    con.close()
    return results
```

---

## VERIFICATION

### Test 1: Table Exists (Production Case)
```python
from core.shared.data_layer.duckdb_utils import get_duckdb_connection, PIPELINE_DB_PATH
from core.shared.data_layer.iv_term_history import get_fidelity_iv_rank

con = get_duckdb_connection(str(PIPELINE_DB_PATH), read_only=True)
result = get_fidelity_iv_rank(con, 'AAPL')
# Result: None (insufficient history, but table exists - no warning)
```

### Test 2: Table Missing (Failure Case)
```python
import duckdb
con = duckdb.connect(':memory:')
result = get_fidelity_iv_rank(con, 'AAPL')
# Output: ⚠️ DIAGNOSTIC: fidelity_iv_long_term_history table does not exist...
# Result: None
```

---

## IMPACT ANALYSIS

### What Changed
1. Added table existence checks in 3 resolver functions
2. Changed `logger.debug()` to `logger.warning()` for missing table scenarios
3. Added actionable error messages pointing to the scraper script

### What Did NOT Change
- ✅ No new execution paths introduced
- ✅ Gating behavior remains identical (still blocks without IV)
- ✅ No changes to thresholds or execution criteria
- ✅ No changes to data authority (Schwab remains authoritative for execution)
- ✅ Table is still created on-demand by `initialize_fidelity_iv_long_term_history_table()`

---

## WHY THIS PRESERVES BIAS-FREE EXECUTION

### 1. No Manual Judgment
- The fix is purely **observability** - surfaces existing conditions
- Does not introduce discretionary logic or heuristics

### 2. No Threshold Changes
- Execution gates remain unchanged
- IV maturity thresholds unchanged (120 days for MATURE)
- Fidelity staleness threshold unchanged (2 trading days)

### 3. No Missing Data Bypass
- Missing table → explicit `return results` (empty dict)
- Tickers still escalate to WAIT/BLOCKED per existing gating rules
- No silent pass-through or substitution

### 4. No Data Authority Changes
- Schwab remains authoritative for execution eligibility
- Fidelity remains optional enrichment source
- No changes to which source is queried when

### 5. No Pipeline Collapse
- Resolver chain remains intact
- Table initialization remains separate (in `duckdb_utils.py`)
- Scraper remains separate (in `scan_engine/iv2_v2.py`)

---

## OPERATIONAL GUIDANCE

### When Table Is Missing
**Symptom**: All candidates escalate to WAIT/BLOCKED with IMMATURE IV maturity

**Diagnostic Log**:
```
⚠️ DIAGNOSTIC: fidelity_iv_long_term_history table does not exist.
IV Rank unavailable for [N] tickers.
Execution will be blocked per gating rules.
Run Fidelity scraper (scan_engine/iv2_v2.py) to populate IV history.
```

**Resolution**:
```bash
# Initialize table (automatic on first write)
python scan_engine/iv2_v2.py --tickers-from-pipeline output/enrichment_iv_demand.csv

# Table will be created automatically on first insert
```

### When Table Exists But Empty
**Symptom**: Same blocking behavior, but no table warning

**Diagnostic**:
- Check row count: `SELECT COUNT(*) FROM fidelity_iv_long_term_history`
- Check ticker coverage: `SELECT COUNT(DISTINCT Ticker) FROM fidelity_iv_long_term_history`

**Resolution**: Run scraper to populate data

### When Data Is Stale
**Symptom**: Tickers blocked even with IV Rank data

**Diagnostic**:
- Check freshness in `get_fidelity_iv_rank()` (30-day threshold)
- Check staleness in `check_fidelity_staleness()` (2-day threshold for execution)

**Resolution**: Run scraper again to refresh

---

## FILES MODIFIED

1. **[core/shared/data_layer/iv_term_history.py](../core/shared/data_layer/iv_term_history.py)**
   - Modified: `get_fidelity_iv_rank()` (lines 281-390)
   - Added: Table existence check + diagnostic warning

2. **[core/enrichment/resolver_implementations.py](../core/enrichment/resolver_implementations.py)**
   - Modified: `resolve_iv_rank_from_cache()` (lines 189-246)
   - Modified: `resolve_iv_history_from_fidelity()` (lines 104-182)
   - Added: Table existence checks + diagnostic warnings in both

---

## ACCEPTANCE CRITERIA

- [x] Table missing → explicit warning logged
- [x] Table exists → no spurious warnings
- [x] Gating behavior unchanged (still blocks without IV)
- [x] No new execution paths introduced
- [x] Diagnostic messages include actionable resolution steps
- [x] No changes to data authority or thresholds
- [x] No changes to pipeline structure

---

## CONCLUSION

This fix enhances **observability** without changing **behavior**. The system still correctly blocks execution when IV data is unavailable, but now surfaces this condition explicitly via warning logs.

The fix is production-safe because:
1. It adds **read-only checks** (no writes, no mutations)
2. It preserves **existing control flow** (same returns, same gates)
3. It provides **actionable diagnostics** (points to scraper script)
4. It maintains **architectural integrity** (no shortcuts, no bypasses)

**Recommended for immediate deployment.**
