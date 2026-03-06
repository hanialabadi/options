# Fidelity-Primary IV Architecture (Simplified)

**Date:** 2026-02-03
**Status:** AUTHORITATIVE (Post-Independent Audit)
**Supersedes:** HYBRID_IV_ARCHITECTURE.md, FIDELITY_SCHWAB_RECONCILIATION.md

---

## Executive Summary

**Verdict:** APPROVED (Simplified architecture)

After independent systems audit, the complex weighted IV blending architecture has been **rejected** in favor of a simpler, more reliable approach:

- **Fidelity IV Rank is the ONLY source of percentile data** (authoritative)
- **Schwab IV used for validation only** (alignment check, not computation)
- **NO percentile computation from Schwab history** (methodology risk)
- **NO percentile blending** (mathematically unsound)
- **Smaller trade universe, higher confidence** (quality > coverage)

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│  FIDELITY (Authoritative IV Rank)                           │
│  - Event-driven snapshots (monthly + earnings + stress)     │
│  - Stores: ticker, IV30, IV_Rank, snapshot_date             │
│  - Professional-grade percentile calculation                │
└─────────────────────────────────────────────────────────────┘
                          │
                          │ (1) Query IV Rank
                          ▼
┌─────────────────────────────────────────────────────────────┐
│  STEP 2: IV RANK ENRICHMENT                                 │
│  ----------------------------------------                    │
│  FOR each ticker:                                           │
│    1. Get Fidelity IV Rank (authoritative)                  │
│    2. Check staleness (<30 days)                            │
│    3. Check Schwab-Fidelity IV alignment (<2% divergence)   │
│    4. IF fresh AND aligned: USE Fidelity IV Rank            │
│       ELSE: SKIP ticker                                     │
└─────────────────────────────────────────────────────────────┘
                          │
                          │ (2) Validation (optional)
                          ▼
┌─────────────────────────────────────────────────────────────┐
│  SCHWAB MONITORING (Validation Layer)                       │
│  - Fetch current IV30 from snapshot                         │
│  - Check: |Schwab_IV30 - Fidelity_IV30| < 2%                │
│  - Alert if divergence detected                             │
│  - DO NOT compute percentiles from Schwab history           │
└─────────────────────────────────────────────────────────────┘
                          │
                          │ (3) Trading decision
                          ▼
┌─────────────────────────────────────────────────────────────┐
│  OUTPUT: Enriched Snapshot                                  │
│  ----------------------------------------                    │
│  Ticker | IV_Rank_30D | IV_Rank_Source | Snapshot_Age       │
│  AAPL   | 42.5        | Fidelity       | 5 days             │
│  MSFT   | NaN         | SKIPPED (stale)| 35 days            │
│  GOOGL  | NaN         | SKIPPED (div)  | 3 days             │
└─────────────────────────────────────────────────────────────┘
```

---

## Non-Negotiable Invariants

### INV-1: Schwab-Fidelity IV Alignment
```python
ENFORCE: |Schwab_IV30 - Fidelity_IV30| < 2% at snapshot time
ACTION: If violated, mark ticker SKIPPED until resolved
REASON: Percentiles are meaningless if current IV can't be agreed upon
```

**Implementation:**
```python
from core.shared.data_layer.iv_term_history import check_iv_alignment

schwab_iv = 23.5
fidelity_iv = 23.94
is_aligned, divergence = check_iv_alignment(schwab_iv, fidelity_iv)

if not is_aligned:
    logger.warning(f"SKIP: IV divergence {divergence:.2f}pp exceeds 2% threshold")
```

### INV-2: No Percentile Blending
```python
ENFORCE: Never compute weighted average of percentiles from different distributions
ACTION: Use single authoritative source (Fidelity) OR flag as unknown
REASON: Blended percentiles have no statistical interpretation
```

**Rationale:** A percentile from Fidelity's 1-year distribution and a percentile from Schwab's 180-day distribution reference different universes. Averaging them produces a mathematical artifact with no market meaning.

### INV-3: Staleness Thresholds
```python
ENFORCE: Fidelity snapshot must be <30 days old for trading decisions
ACTION: SKIP tickers with stale IV Rank (don't guess, don't interpolate)
REASON: False confidence is worse than missed trades
```

**Implementation:**
```python
from core.shared.data_layer.iv_term_history import check_fidelity_staleness

_, _, snapshot_ts = get_fidelity_iv_rank(con, 'AAPL')
is_fresh, age_days = check_fidelity_staleness(snapshot_ts)

if not is_fresh:
    logger.warning(f"SKIP: Fidelity data is {age_days} days old (threshold: 30d)")
```

### INV-4: Trading Days Only
```python
ENFORCE: All history counts must exclude weekends + holidays
ACTION: Filter via strftime('%w', date) NOT IN (0,6) in all queries
REASON: Calendar day contamination leads to false maturity classification
```

**Status:** ✅ Already implemented and validated (forensic audit 2026-02-03)

### INV-5: Monotonic History (No Backfill)
```python
ENFORCE: created_at timestamps should be approximately chronological with date
ACTION: Reject bulk backfills where created_at is same for all dates
REASON: Backfilled data suggests synthetic/sample data, not real collection
```

**Validation:**
```sql
-- Check for bulk backfill pattern
SELECT COUNT(DISTINCT created_at) as distinct_timestamps
FROM iv_term_history
WHERE ticker = 'AAPL';

-- Expected: Many timestamps (daily collection)
-- Alert if: 1 timestamp (bulk backfill) or <10 timestamps (batch inserts)
```

### INV-6: Source Transparency
```python
ENFORCE: Every IV value must have source={'schwab'|'fidelity'}, never 'sample'
ACTION: Reject any insert with source='sample' in production
REASON: Forensic audit revealed sample data contamination
```

---

## Key Functions

### 1. Get Fidelity IV Rank (Authoritative)
```python
from core.shared.data_layer.iv_term_history import get_fidelity_iv_rank

result = get_fidelity_iv_rank(con, ticker='AAPL')

if result:
    iv_30d, iv_rank, snapshot_ts = result
    print(f"AAPL IV: {iv_30d:.2f}%, Rank: {iv_rank:.1f}%, Snapshot: {snapshot_ts}")
else:
    print("No Fidelity data available - SKIP ticker")
```

**Returns:**
- `(iv_30d, iv_rank, snapshot_timestamp)` if available
- `None` if no Fidelity data

**Data Source:** `fidelity_iv_long_term_history` table in `pipeline.duckdb`

### 2. Check Staleness
```python
from core.shared.data_layer.iv_term_history import check_fidelity_staleness

is_fresh, age_days = check_fidelity_staleness(snapshot_timestamp, staleness_threshold_days=30)

if is_fresh:
    print(f"Fresh data ({age_days} days old)")
else:
    print(f"SKIP: Stale data ({age_days} days old)")
```

**Default threshold:** 30 days
**Configurable:** Yes (adjust `staleness_threshold_days` parameter)

### 3. Check IV Alignment
```python
from core.shared.data_layer.iv_term_history import check_iv_alignment

schwab_iv = 23.5
fidelity_iv = 23.94
is_aligned, divergence = check_iv_alignment(schwab_iv, fidelity_iv, alignment_threshold=2.0)

if is_aligned:
    print(f"Aligned (divergence: {divergence:.2f}pp)")
else:
    print(f"SKIP: Divergent (divergence: {divergence:.2f}pp)")
```

**Default threshold:** 2.0 percentage points
**Configurable:** Yes (adjust `alignment_threshold` parameter)

---

## Step 2 Integration

### Before (Hybrid Architecture - REJECTED)
```python
# Complex weighted blending logic (REMOVED)
if maturity_state == 'IMMATURE':
    weighted_rank = (schwab_weight * schwab_rank) + (fidelity_weight * fidelity_rank)
    # ❌ Mathematical artifact with no market interpretation
```

### After (Fidelity-Primary - APPROVED)
```python
# Simple hierarchical fallback with validation
fidelity_data = get_fidelity_iv_rank(con, ticker)

if fidelity_data is None:
    # No Fidelity data - SKIP ticker
    iv_rank_source = 'SKIPPED (No Fidelity data)'
    continue

fidelity_iv, fidelity_rank, snapshot_ts = fidelity_data

# Check staleness (INV-3)
is_fresh, age_days = check_fidelity_staleness(snapshot_ts)
if not is_fresh:
    iv_rank_source = f'SKIPPED (Stale: {age_days}d old)'
    continue

# Check alignment (INV-1)
is_aligned, divergence = check_iv_alignment(schwab_iv, fidelity_iv)
if not is_aligned:
    iv_rank_source = f'SKIPPED (IV divergence: {divergence:.2f}pp)'
    continue

# All checks passed - USE Fidelity IV Rank
iv_rank = fidelity_rank
iv_rank_source = 'Fidelity (authoritative)'
```

---

## Expected Behavior

### Scenario 1: Fresh, Aligned Fidelity Data
```
Ticker: AAPL
Fidelity IV30: 23.94%
Fidelity IV Rank: 42.5%
Snapshot age: 5 days
Schwab IV30: 23.5%
Divergence: 0.44pp

✅ Result: IV_Rank_30D = 42.5%, Source = 'Fidelity (authoritative)'
```

### Scenario 2: Stale Fidelity Data
```
Ticker: MSFT
Fidelity IV30: 28.5%
Fidelity IV Rank: 65.0%
Snapshot age: 35 days
Schwab IV30: 30.2%

⚠️ Result: IV_Rank_30D = NaN, Source = 'SKIPPED (Stale: 35d old)'
```

### Scenario 3: IV Divergence
```
Ticker: GOOGL
Fidelity IV30: 21.0%
Fidelity IV Rank: 50.0%
Snapshot age: 3 days
Schwab IV30: 25.5%
Divergence: 4.5pp

❌ Result: IV_Rank_30D = NaN, Source = 'SKIPPED (IV divergence: 4.50pp)'
```

### Scenario 4: No Fidelity Data
```
Ticker: TSLA
Fidelity data: None

❌ Result: IV_Rank_30D = NaN, Source = 'SKIPPED (No Fidelity data)'
```

---

## Trade Universe Implications

**Before (Hybrid):** Attempt to compute IV Rank for all tickers with any data
**After (Fidelity-Primary):** Only trade tickers with fresh, aligned Fidelity data

**Expected reduction in universe size:** 30-50% (depends on Fidelity coverage)

**Tradeoff accepted:**
- ✅ Higher execution confidence (professional-grade IV Rank)
- ✅ No methodology risk (validated source)
- ✅ Explicit data quality enforcement
- ❌ Smaller trade universe (quality > coverage)

**Example:**
- Total tickers in snapshot: 100
- Fidelity coverage: 60
- Fresh (<30d): 50
- Aligned (<2% div): 45
- **Final tradeable universe: ~45 tickers** (down from 100)

---

## Fidelity Snapshot Refresh Strategy

**Current approach:** Event-driven (manual)
- Monthly refresh
- Earnings events
- Market stress events

**Recommended cadence:**
- **Minimum:** Monthly (to keep <30d staleness)
- **Optimal:** Bi-weekly (to maximize fresh coverage)
- **Event-driven:** Earnings announcements, VIX spikes, sector rotation

**Automation opportunity:**
- Schedule bi-weekly Fidelity snapshot fetch
- Auto-refresh on market volatility events (VIX >30)

---

## Migration Path

### Phase 1: Immediate (✅ COMPLETE)
1. Remove `calculate_weighted_iv_rank()` function
2. Remove `get_iv_maturity_state()` function
3. Add `get_fidelity_iv_rank()` function
4. Add `check_fidelity_staleness()` function
5. Add `check_iv_alignment()` function
6. Simplify `_enrich_iv_rank_from_duckdb()` in step2_load_snapshot.py

### Phase 2: Validation (Next)
1. Run verification script on real Fidelity snapshot
2. Validate alignment checks with live Schwab data
3. Confirm staleness thresholds are appropriate
4. Test with various scenarios (fresh, stale, divergent, missing)

### Phase 3: Monitoring (Ongoing)
1. Log staleness distribution (track how many tickers are fresh)
2. Log divergence distribution (identify systematic issues)
3. Alert if >50% of universe becomes stale (Fidelity refresh needed)
4. Track trade universe size over time

---

## Removed Concepts (No Longer Valid)

### ❌ IV Maturity States (MATURE/IMMATURE/MISSING)
**Reason:** Conflates data quantity with data quality. Removed.

### ❌ Weighted IV Rank Blending
**Reason:** Blends percentiles from different distributions (mathematically unsound). Removed.

### ❌ Schwab Percentile Computation
**Reason:** Methodology risk (Schwab and Fidelity use different IV calculations). Removed.

### ❌ Linear Weight Decay Formula
**Reason:** Arbitrary threshold (252 days) with no validation. Removed.

### ❌ Fidelity as "Structural Reference"
**Reason:** Category error (Fidelity provides complete percentile, not reference input). Removed.

---

## Documentation Hierarchy

**Authoritative (Use These):**
1. **FIDELITY_PRIMARY_ARCHITECTURE.md** (this document)
2. **FORENSIC_AUDIT_SUMMARY.md** (weekend contamination findings)
3. **CORRECTED_MATURITY_DEFINITION.md** (trading days vs calendar days)

**Historical (Do Not Use):**
1. ~~HYBRID_IV_ARCHITECTURE.md~~ (superseded, complex blending rejected)
2. ~~FIDELITY_SCHWAB_RECONCILIATION.md~~ (superseded, blending method rejected)

---

## Key Takeaways

1. **Simplicity > Complexity:** Single authoritative source (Fidelity) is more reliable than complex blending
2. **Quality > Coverage:** 45 high-confidence trades > 100 questionable trades
3. **Explicit > Implicit:** SKIP tickers with clear reason rather than compute uncertain percentiles
4. **Validation > Computation:** Use Schwab to validate Fidelity, not as percentile source
5. **Fresh Data Required:** Staleness threshold enforces data quality

---

**Status:** ARCHITECTURE APPROVED AND IMPLEMENTED

**Next Step:** Verify implementation with real Fidelity snapshot and live Schwab data

