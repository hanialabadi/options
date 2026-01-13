# Earnings Calendar Integration - Architecture & Implementation Plan

**Date:** 2026-01-04  
**Status:** 🔵 Implementation Phase  
**Objective:** Integrate earnings calendar for Phase 3 enrichment (observation-only, Phase 1-4 compliant)

---

## 🎯 EXECUTIVE SUMMARY

**Current State:**
- ✅ Stub implementation exists (`compute_earnings_proximity.py` - returns 999)
- ✅ Data layer module exists (`earnings_calendar.py` - Yahoo Finance + static fallback)
- ⚠️ **NOT WIRED TOGETHER** - Phase 3 enrichment doesn't call data layer
- ⚠️ Strategy-contaminated flag logic in `tag_earnings_flags.py` (needs isolation)

**Goal:**
- Wire Phase 3 enrichment to use existing data layer
- Ensure Phase 1-4 compliance (observation only, no filtering)
- Similar pattern to IV_Rank implementation

**Effort:** ~1-2 hours (mostly wiring, minimal new code)

---

## 📊 CURRENT ARCHITECTURE AUDIT

### Existing Components

| **Component** | **Location** | **Status** | **Notes** |
|--------------|-------------|-----------|-----------|
| Data Layer | `core/data_layer/earnings_calendar.py` | ✅ Complete | Yahoo Finance + static fallback |
| Phase 3 Stub | `core/phase3_enrich/compute_earnings_proximity.py` | ⚠️ Stub | Returns 999, needs wiring |
| Strategy Flags | `core/phase3_enrich/tag_earnings_flags.py` | ❌ Contaminated | Phase 8+ logic, not Phase 3 |
| Static Data | `data/earnings_calendar.csv` | ❓ Unknown | Need to check existence |

### Data Layer Capabilities (Existing)

✅ **Already Implemented:**
```python
from core.data_layer.earnings_calendar import (
    get_earnings_date_yfinance,      # Yahoo Finance API
    get_earnings_date_static,        # Static CSV fallback
    compute_days_to_earnings,        # Per-ticker calculation
    add_earnings_proximity           # Batch processing
)
```

**Data Sources (Priority Order):**
1. Yahoo Finance (`yfinance` library) - Primary, free, reliable
2. Static calendar CSV (`data/earnings_calendar.csv`) - Fallback
3. None - Return NaN (no false positives)

**Functions Available:**
- `get_earnings_date_yfinance(ticker)` → Returns next earnings date or None
- `compute_days_to_earnings(ticker, snapshot_date)` → Returns days to earnings or None
- `add_earnings_proximity(df, snapshot_date)` → Batch processing for DataFrames

### Phase 3 Stub (Current)

**File:** `core/phase3_enrich/compute_earnings_proximity.py`

**Current Behavior:**
```python
def compute_earnings_proximity(df, snapshot_ts=None):
    # STUB: Returns 999 for all positions
    df["Days_to_Earnings"] = 999
    df["Earnings_Source"] = "stub"
    return df
```

**Issues:**
- ❌ Doesn't call data layer
- ❌ Magic default (999) instead of NaN
- ❌ Unclear that data layer exists

### Strategy Flags (Contaminated)

**File:** `core/phase3_enrich/tag_earnings_flags.py`

**Current Behavior:**
```python
def tag_earnings_flags(df, reference_date=None):
    # Tags earnings-related event setups (straddles/strangles)
    # Uses thresholds: EARNINGS_PROXIMITY_DAYS_MIN, EARNINGS_PROXIMITY_DAYS_MAX
    # Filters by strategy: STRATEGY_LONG_STRADDLE, STRATEGY_LONG_STRANGLE
    # Adds: Is_Event_Setup, Event_Reason
```

**Issues:**
- ❌ **Phase 8+ logic in Phase 3** (strategy filtering, thresholds)
- ❌ Should NOT be in Phase 3 enrichment (violates observation-only rule)
- ⚠️ Name collision with `Days_to_Earnings` column

**Correct Architecture:**
- Phase 3: Calculate `Days_to_Earnings` (observation)
- Phase 8: Use `Days_to_Earnings` for strategy tagging (decision)

---

## 🏗️ ARCHITECTURE DESIGN

### Phase 1-4 Compliant Design

**Principle:** Earnings proximity is **observation**, not **decision**

```
┌─────────────────────────┐
│ Yahoo Finance API       │  ← Primary source (yfinance)
│ (Real-time earnings)    │
└──────────┬──────────────┘
           │
           ▼
┌─────────────────────────┐
│ Static Calendar CSV     │  ← Fallback (testing/offline)
│ data/earnings_calendar  │
└──────────┬──────────────┘
           │
           ▼
┌─────────────────────────┐
│ Data Layer              │  ← Existing module
│ earnings_calendar.py    │
│ (per-ticker lookup)     │
└──────────┬──────────────┘
           │
           ▼
┌─────────────────────────┐
│ Phase 3 Enrichment      │  ← UPDATE THIS
│ compute_earnings_       │
│ proximity.py            │
└──────────┬──────────────┘
           │
     ┌─────┴─────┐
     ▼           ▼
┌─────────┐  ┌──────────┐
│ Phase 6 │  │ Phase 8+ │
│ Freeze  │  │ Strategy │
└─────────┘  └──────────┘
```

### Output Schema

**Phase 3 enrichment should add:**

| **Column** | **Type** | **Values** | **Notes** |
|-----------|---------|-----------|-----------|
| `Days_to_Earnings` | int or NaN | Positive = days until, Negative = days since | Calendar days (not trading days) |
| `Next_Earnings_Date` | datetime or NaT | YYYY-MM-DD | Date of next earnings |
| `Earnings_Source` | str | "yfinance", "static", "unknown" | Data provenance |

**Phase 6 will freeze:**
- `Days_to_Earnings_Entry` (snapshot at trade entry)
- `Next_Earnings_Date_Entry` (reference for drift)

**Phase 7+ will observe:**
- Drift: `Days_to_Earnings` vs `Days_to_Earnings_Entry`
- Crossing: Did earnings occur since entry?

### Phase Boundary Rules

| **Component** | **Allowed Phase** | **Forbidden Before** | **Notes** |
|--------------|------------------|---------------------|-----------|
| Earnings date lookup | Phase 1-4 | N/A (always safe) | Observation only |
| Days_to_Earnings calculation | **Phase 3 only** | Phase 1-2 | Pure enrichment |
| Days_to_Earnings_Entry | **Phase 6** | Phase 1-5 | Freeze at entry |
| Earnings drift analysis | **Phase 7+** | Phase 1-6 | Compare vs entry |
| Strategy thresholds | **Phase 8+** | Phase 1-7 | **Never** in perception |

### NaN Handling

**Return NaN when:**
- ✅ Ticker not found in Yahoo Finance
- ✅ Static calendar missing ticker
- ✅ API call fails (network error, rate limit)
- ✅ Earnings date is None/unknown

**DO NOT:**
- ❌ Return magic defaults (999, 0, 50)
- ❌ Infer earnings dates from sector averages
- ❌ Backfill with historical patterns
- ❌ Block trades when earnings unknown

**Rationale:** Explicit unknowns > false confidence

---

## ✅ PHASE 1-4 COMPLIANCE CHECKLIST

### Observation-Only Requirements

- [x] ✅ Calculate `Days_to_Earnings` (observation)
- [x] ✅ Store in snapshots (no interpretation)
- [x] ✅ Return NaN for missing data (no magic defaults)
- [x] ✅ Track metadata (source, date)
- [x] ❌ **NO filtering** (calculate for ALL tickers)
- [x] ❌ **NO thresholds** (no "near" vs "far" tagging)
- [x] ❌ **NO strategy bias** (no straddle/strangle logic)
- [x] ❌ **NO PCS contamination** (structure-only until Phase 6)

### Do / Do Not Lists

**✅ DO (Approved Actions):**

| **Action** | **Component** | **Reason** |
|-----------|--------------|------------|
| ✅ Wire Phase 3 to data layer | `compute_earnings_proximity.py` | Use existing infrastructure |
| ✅ Use Yahoo Finance | `yfinance` library | Free, reliable, real-time |
| ✅ Keep static fallback | `data/earnings_calendar.csv` | Testing, offline mode |
| ✅ Return NaN when unknown | Calculation logic | Explicit unknowns |
| ✅ Calculate for ALL tickers | Phase 3 pipeline | No filtering |
| ✅ Add provenance column | `Earnings_Source` | Data quality tracking |
| ✅ Use calendar days | Day calculation | Standard convention |

**❌ DO NOT (Forbidden Actions):**

| **Action** | **Component** | **Reason** |
|-----------|--------------|------------|
| ❌ Add strategy thresholds | Phase 3 logic | Phase 8+ only |
| ❌ Filter by proximity | Phase 3 pipeline | Violates observation rule |
| ❌ Tag event setups | Phase 3 enrichment | Phase 8+ only |
| ❌ Use in PCS calculation | Phase 1-4 | Structure-only until Phase 6 |
| ❌ Return magic defaults | Calculation logic | False confidence |
| ❌ Infer missing dates | Data layer | No guessing |
| ❌ Block trades | Phase 3 | Observation, not decision |

---

## 🔧 IMPLEMENTATION PLAN

### Task 1: Verify Data Layer (5 min)

**Check existing module:**
```bash
# Test data layer functions
python -c "
from core.data_layer.earnings_calendar import get_earnings_date_yfinance
import datetime

# Test with known ticker
date = get_earnings_date_yfinance('AAPL')
print(f'AAPL next earnings: {date}')
"
```

**Verify `yfinance` installed:**
```bash
pip show yfinance
# If not: pip install yfinance
```

**Check static calendar:**
```bash
ls -la data/earnings_calendar.csv
# If missing: Create template
```

### Task 2: Update Phase 3 Enrichment (30 min)

**File:** `core/phase3_enrich/compute_earnings_proximity.py`

**Replace stub with data layer integration:**

```python
"""
Phase 3 Observable: Earnings Proximity

Calculates days until next earnings announcement for risk awareness.

Design:
- Days_to_Earnings = calendar days until next earnings date
- Negative = days since last earnings (post-earnings)
- NaN = no earnings data available

This is an OBSERVATION, not a freeze. Phase 6 will create Days_to_Earnings_Entry.

Implementation:
- Uses core.data_layer.earnings_calendar module
- Yahoo Finance (primary source)
- Static calendar fallback
- Returns NaN for missing data (no magic defaults)
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime
from typing import Optional

from core.data_layer.earnings_calendar import (
    get_earnings_date_yfinance,
    get_earnings_date_static,
    load_static_earnings_calendar,
    compute_days_to_earnings
)

logger = logging.getLogger(__name__)


def compute_earnings_proximity(
    df: pd.DataFrame,
    snapshot_ts: Optional[pd.Timestamp] = None
) -> pd.DataFrame:
    """
    Add earnings proximity columns (observation only).
    
    Parameters
    ----------
    df : pd.DataFrame
        Must contain 'Symbol' or 'Underlying' column
    snapshot_ts : pd.Timestamp, optional
        Reference date. If None, uses pd.Timestamp.now()
    
    Returns
    -------
    pd.DataFrame
        Input DataFrame with added columns:
        - 'Days_to_Earnings' (int or NaN): Calendar days to next earnings
        - 'Next_Earnings_Date' (datetime or NaT): Next earnings date
        - 'Earnings_Source' (str): Data provenance
          * "yfinance" = Yahoo Finance API
          * "static" = Static calendar fallback
          * "unknown" = No data available
    
    Notes
    -----
    - Observation only (no filtering, no thresholds)
    - Returns NaN when earnings date unknown
    - Phase 6 will freeze as Days_to_Earnings_Entry
    
    Phase 1-4 Compliance:
    - ✅ Pure observation (no decisions)
    - ✅ Calculate for ALL tickers (no filtering)
    - ✅ Explicit NaN (no magic defaults)
    - ✅ No strategy bias
    """
    # Implementation here
    pass
```

### Task 3: Remove Strategy Contamination (15 min)

**Option A: Move to Phase 8 module**
- Create `core/phase8_strategy/tag_event_setups.py`
- Move `tag_earnings_flags()` logic there
- Keep Phase 3 clean (observation only)

**Option B: Deprecate entirely**
- Tag with comment: "⚠️ DEPRECATED: Phase 8+ logic, not Phase 3"
- Remove from Phase 3 pipeline
- Document as future Phase 8 work

**Recommendation:** Option A (cleaner architecture)

### Task 4: Test & Validate (15 min)

**Test script:**
```python
# test_earnings_integration.py

import pandas as pd
from core.phase3_enrich.compute_earnings_proximity import compute_earnings_proximity

# Test data
test_df = pd.DataFrame({
    'Symbol': ['AAPL', 'MSFT', 'NVDA', 'NONEXISTENT'],
    'Strategy': ['Iron Condor', 'Bull Put', 'Call Debit', 'Straddle']
})

# Run enrichment
result_df = compute_earnings_proximity(test_df)

# Validate outputs
print(result_df[['Symbol', 'Days_to_Earnings', 'Next_Earnings_Date', 'Earnings_Source']])

# Check compliance
assert 'Days_to_Earnings' in result_df.columns
assert result_df['Days_to_Earnings'].dtype in [np.int64, np.float64]  # int or NaN
assert (result_df['Earnings_Source'].isin(['yfinance', 'static', 'unknown'])).all()

# Check no filtering occurred
assert len(result_df) == len(test_df)  # No rows removed

print("✅ All tests passed")
```

**Expected output:**
```
   Symbol  Days_to_Earnings Next_Earnings_Date Earnings_Source
0    AAPL               24.0         2026-01-28        yfinance
1    MSFT               25.0         2026-01-29        yfinance
2    NVDA               46.0         2026-02-19        yfinance
3  NONEXISTENT           NaN                NaT         unknown

✅ All tests passed
```

### Task 5: Update Documentation (10 min)

**Update files:**
- `PHASE_1_4_IMPLEMENTATION_COMPLETE.md` - Remove stub warning
- `PHASE_1_4_SCHEMA_REFERENCE.md` - Update earnings columns
- `README.md` - Note earnings integration complete

---

## 📝 CRITICAL DIFFERENCES FROM IV_RANK

| **Aspect** | **IV_Rank** | **Earnings Proximity** |
|-----------|------------|----------------------|
| **Data Source** | Internal (Fidelity snapshots) | External (Yahoo Finance API) |
| **History Required** | 252 days (or 120 minimum) | None (forward-looking) |
| **Data Type** | Historical time-series | Future event date |
| **Calculation** | Per-ticker percentile | Calendar day subtraction |
| **Missing Data** | Common (insufficient history) | Rare (most tickers have earnings) |
| **API Dependency** | None (local CSV) | Yes (Yahoo Finance, static fallback) |
| **Update Frequency** | Daily snapshots | Quarterly (earnings cycle) |

**Key Insight:** Earnings is **simpler** than IV_Rank:
- No historical window needed
- No percentile calculation
- Single date lookup, simple subtraction
- Data layer already exists (just wire it)

---

## 🚀 IMPLEMENTATION STATUS

### Completed ✅
- [x] Data layer module exists (`earnings_calendar.py`)
- [x] Yahoo Finance integration ready
- [x] Static calendar fallback implemented
- [x] Architecture design approved

### In Progress 🔵
- [ ] Wire Phase 3 enrichment to data layer
- [ ] Remove strategy contamination from Phase 3
- [ ] Test with real tickers
- [ ] Validate Phase 1-4 compliance

### Blocked ❌
- None (all dependencies satisfied)

---

## 🎯 SUCCESS CRITERIA

**Phase 3 enrichment outputs:**
- ✅ `Days_to_Earnings` (int or NaN)
- ✅ `Next_Earnings_Date` (datetime or NaT)
- ✅ `Earnings_Source` (provenance string)

**Phase 1-4 compliance:**
- ✅ No filtering (calculate for ALL)
- ✅ No thresholds (observation only)
- ✅ No strategy bias (pure data)
- ✅ Explicit NaN (no magic defaults)

**Production readiness:**
- ✅ Yahoo Finance integration works
- ✅ Static fallback tested
- ✅ Error handling graceful
- ✅ Logging informative

---

## 📊 COMPARISON: Before vs After

### Before (Current Stub)
```python
df["Days_to_Earnings"] = 999  # Magic default
df["Earnings_Source"] = "stub"
```

**Issues:**
- ❌ No real data
- ❌ Magic default (999)
- ❌ Ignored existing data layer

### After (Wired to Data Layer)
```python
for symbol in df['Symbol'].unique():
    earnings_date = get_earnings_date_yfinance(symbol)
    
    if earnings_date:
        days = (earnings_date - snapshot_date).days
        df.loc[df['Symbol'] == symbol, 'Days_to_Earnings'] = days
        df.loc[df['Symbol'] == symbol, 'Earnings_Source'] = 'yfinance'
    else:
        df.loc[df['Symbol'] == symbol, 'Days_to_Earnings'] = np.nan
        df.loc[df['Symbol'] == symbol, 'Earnings_Source'] = 'unknown'
```

**Benefits:**
- ✅ Real earnings data
- ✅ Explicit NaN
- ✅ Uses existing infrastructure

---

## 🔐 ARCHITECTURAL APPROVAL

**Status:** ✅ **APPROVED** - Implementation-only work

**Why correct:**
1. Data layer already exists (no new dependencies)
2. Yahoo Finance is free, reliable, widely used
3. Static fallback prevents API dependency risk
4. NaN handling matches IV_Rank pattern
5. Phase 1-4 compliant (observation only)
6. No strategy contamination

**Remaining work:** Wire Phase 3 to data layer (~1 hour)

**Effort estimate:** 1-2 hours total

---

## 📚 APPENDIX: Data Layer API Reference

### Function: `get_earnings_date_yfinance(ticker)`

**Returns:** `datetime` or `None`

**Example:**
```python
>>> get_earnings_date_yfinance('AAPL')
datetime(2026, 1, 28)

>>> get_earnings_date_yfinance('NONEXISTENT')
None
```

### Function: `compute_days_to_earnings(ticker, snapshot_date)`

**Returns:** `int` or `None`

**Example:**
```python
>>> compute_days_to_earnings('AAPL', datetime(2026, 1, 4))
24  # AAPL earnings on 2026-01-28

>>> compute_days_to_earnings('NONEXISTENT', datetime(2026, 1, 4))
None
```

### Function: `add_earnings_proximity(df, snapshot_date)`

**Returns:** `pd.DataFrame` with added columns

**Example:**
```python
>>> df = pd.DataFrame({'Ticker': ['AAPL', 'MSFT']})
>>> df = add_earnings_proximity(df, datetime(2026, 1, 4))
>>> df[['Ticker', 'days_to_earnings', 'earnings_proximity_flag']]
  Ticker  days_to_earnings  earnings_proximity_flag
0   AAPL                24                    False
1   MSFT                25                    False
```

---

## ✅ CONCLUSION

**Implementation Status:** Ready to wire Phase 3 enrichment

**Complexity:** Low (data layer exists, just connect it)

**Risk:** Minimal (Yahoo Finance reliable, static fallback available)

**Phase 1-4 Compliance:** Guaranteed (observation-only design)

**Next Step:** Update `compute_earnings_proximity.py` to call data layer

**Timeline:** 1-2 hours to completion
