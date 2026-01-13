# Phase 3: Decision-Layer Integration - Implementation Complete

**Date:** January 2, 2026  
**Status:** ✅ Complete

---

## Overview

Phase 3 integrates IV availability flags from the derived analytics layer (Phase 2) into the scan engine's acceptance logic (Step 12). This enables the system to explicitly distinguish between:

- **"Market conditions are bad"** (strategy failed acceptance rules)
- **"Data is insufficient"** (strategy passed rules but lacks IV history)

## Design Principle

> **"Preserve history > fabricate completeness"**
> 
> If IV history is insufficient: **expose it** — never compensate for it.

---

## Implementation Summary

### 1. IV Availability Loader (`core/data_layer/ivhv_availability_loader.py`)

**Purpose:** Load IV availability flags from derived analytics and merge with strategy dataframes.

**Key Functions:**
- `load_iv_availability(df, snapshot_date)`: Merge IV flags by ticker + date
- `get_iv_diagnostic_reason(iv_history_days)`: Generate human-readable diagnostic
- Returns columns:
  - `iv_rank_available` (bool): True if IV Rank can be computed
  - `iv_percentile_available` (bool): True if IV Percentile can be computed
  - `iv_history_days` (int): Days of IV history available
  - `iv_index_30d` (float): 30-day IV Index

**Data Source:** `data/ivhv_timeseries/ivhv_timeseries_derived.csv`

**Current Status:**
- 5 historical dates (2025-08-03 → 2025-12-29)
- Max history: 4 days per ticker
- IV Rank/Percentile: 0% available (need 120+ days)

---

### 2. Step 12 Acceptance Logic Integration

**File:** `core/scan_engine/step12_acceptance.py`

**Changes:**

#### A. Import IV Loader
```python
from core.data_layer.ivhv_availability_loader import (
    load_iv_availability,
    get_iv_diagnostic_reason
)
```

#### B. Update Function Signature
```python
def apply_acceptance_logic(df: pd.DataFrame, snapshot_date: str = None) -> pd.DataFrame:
    """
    Args:
        df: DataFrame from Step 9B
        snapshot_date: Optional date (YYYY-MM-DD) for IV availability lookup
    """
```

#### C. Load IV Availability Flags
```python
# At start of apply_acceptance_logic()
logger.info(f"\n📊 Loading IV availability flags...")
df_result = load_iv_availability(df_result, snapshot_date=snapshot_date)
```

#### D. IV Availability Downgrade Gate
```python
# After Evaluation Completeness Gate
if 'iv_rank_available' in df_result.columns:
    insufficient_iv_mask = (
        (df_result['acceptance_status'] == 'READY_NOW') &
        (~df_result['iv_rank_available'])
    )
    
    if insufficient_iv_count > 0:
        logger.info(f"\n📊 IV availability gate: {insufficient_iv_count} strategies lack sufficient IV history")
        logger.info(f"   Downgrading to STRUCTURALLY_READY (requires 120+ days)")
        
        # Add diagnostic reasons
        for idx in df_result[insufficient_iv_mask].index:
            iv_history = int(df_result.loc[idx, 'iv_history_days'])
            iv_diagnostic = get_iv_diagnostic_reason(iv_history)
            
            df_result.at[idx, 'acceptance_status'] = 'STRUCTURALLY_READY'
            df_result.at[idx, 'acceptance_reason'] = f"{current_reason} ({iv_diagnostic})"
```

**Acceptance Status Hierarchy (Updated):**
1. **READY_NOW**: Passed acceptance rules AND score ≥ 60 AND **IV Rank available**
2. **STRUCTURALLY_READY**: Passed rules but (score < 60 OR **IV Rank unavailable**)
3. **WAIT**: Good structure but timing not ideal
4. **AVOID**: Failed acceptance rules
5. **INCOMPLETE**: Missing required data

---

### 3. CLI Diagnostics (`scan_live.py`)

**Changes:**

#### A. Regime Analysis Section
```python
# After actual_ready calculation
if 'acceptance_ready' in results and not results['acceptance_ready'].empty:
    df_ready = results['acceptance_ready']
    if 'iv_rank_available' in df_ready.columns:
        iv_unavailable = (~df_ready['iv_rank_available']).sum()
        if iv_unavailable > 0:
            print(f"\n📊 IV Availability Status:")
            print(f"   ⚠️ {iv_unavailable}/{len(df_ready)} strategies lack sufficient IV history")
            print(f"   📅 Average history: {avg_history:.1f} days (need 120+)")
```

#### B. Final Trades Summary
```python
# After capital allocation summary
if 'iv_rank_available' in df_final.columns:
    iv_unavailable_count = (~df_final['iv_rank_available']).sum()
    
    if iv_unavailable_count > 0:
        print("\n" + "="*80)
        print("📊 IV AVAILABILITY SUMMARY")
        print("="*80)
        print(f"⚠️  IV Rank unavailable: {iv_unavailable_count}/{len(df_final)} strategies")
        print(f"📅 History: avg={avg_history:.1f} days, max={max_history} days (need 120+)")
        print(f"⏳ Estimated activation: ~{120 - int(max_history)} more days")
        print("\nℹ️  Strategies without IV Rank would be downgraded to STRUCTURALLY_READY")
```

---

### 4. Dashboard Integration (`streamlit_app/dashboard.py`)

**Changes:**

```python
# After final_trades_count metric
if 'final_trades' in results and not results['final_trades'].empty:
    df_trades = results['final_trades']
    if 'iv_rank_available' in df_trades.columns:
        iv_unavailable = (~df_trades['iv_rank_available']).sum()
        if iv_unavailable > 0:
            st.warning(f"📊 IV Availability: {iv_unavailable}/{len(df_trades)} strategies lack sufficient IV history")
            if 'iv_history_days' in df_trades.columns:
                avg_history = df_trades[~df_trades['iv_rank_available']]['iv_history_days'].mean()
                st.info(f"ℹ️ Average history: {avg_history:.1f} days (need 120+). These would be downgraded to STRUCTURALLY_READY.")
```

---

## Validation Results

### Test Case: 5 Sample Strategies

**Input:**
- 5 tickers: AAPL, MSFT, GOOGL, TSLA, NVDA
- All initially: `acceptance_status = READY_NOW`

**IV Availability Check:**
- IV Rank available: **0/5 (0%)**
- IV history: **4 days** (max across all tickers)
- Required: **120+ days**

**Downgrade Logic Executed:**
```
⬇️ AAPL: READY_NOW → STRUCTURALLY_READY (4 days < 120 required)
⬇️ MSFT: READY_NOW → STRUCTURALLY_READY (4 days < 120 required)
⬇️ GOOGL: READY_NOW → STRUCTURALLY_READY (4 days < 120 required)
⬇️ TSLA: READY_NOW → STRUCTURALLY_READY (4 days < 120 required)
⬇️ NVDA: READY_NOW → STRUCTURALLY_READY (4 days < 120 required)
```

**Final Status:**
- READY_NOW: **0**
- STRUCTURALLY_READY: **5** ✅

**Acceptance Reasons (Updated):**
```
"Passed acceptance rules (IV history insufficient (4 days < 120 required))"
```

---

## Key Insights

### 1. Honest Diagnostics
The system now explicitly says:
> **"This strategy is structurally valid but blocked due to insufficient IV history"**

Not:
> ~~"This strategy failed (no explanation)"~~

### 2. No Threshold Lowering
- Did NOT lower IV Rank requirements
- Did NOT add fallback logic
- Did NOT interpolate or backfill IV data

### 3. Actionable Timeline
System tells user:
> **"Need ~116 more days of data collection"**

Not:
> ~~"IV Rank unavailable (reason unknown)"~~

---

## Impact on Acceptance Flow

### Before Phase 3:
```
READY_NOW → Step 8 (Portfolio Optimization) → Execution
```
**Problem:** Strategies with incomplete IV evaluation claimed READY_NOW status

### After Phase 3:
```
READY_NOW (score ≥ 60 AND IV available) → Step 8 → Execution
STRUCTURALLY_READY (score < 60 OR IV unavailable) → Wait for data/evaluation
```
**Solution:** System blocks execution without full IV context

---

## Data Collection Timeline

**Current State:**
- Dates: 5 (2025-08-03 → 2025-12-29)
- Max history: 4 days per ticker
- IV Rank: 0% available

**Activation Timeline:**
- Need: 120+ days of history
- Remaining: ~116 days
- Frequency: Daily snapshots recommended
- ETA: ~April 2026 (if collected daily)

**Daily Snapshot Command:**
```bash
venv/bin/python core/scan_engine/step0_schwab_snapshot.py
venv/bin/python core/data_layer/ivhv_timeseries_loader.py    # Re-normalize
venv/bin/python core/data_layer/ivhv_derived_analytics.py     # Re-compute IV metrics
```

---

## Files Modified

### Created:
1. **`core/data_layer/ivhv_availability_loader.py`** (240 lines)
   - IV availability loader utility
   - Merge logic, diagnostic reasons
   - Test harness included

### Modified:
1. **`core/scan_engine/step12_acceptance.py`** (+45 lines)
   - Import IV loader
   - Load IV flags at pipeline start
   - Add IV availability downgrade gate

2. **`scan_live.py`** (+30 lines)
   - IV diagnostics in regime analysis
   - IV availability summary after final trades

3. **`streamlit_app/dashboard.py`** (+12 lines)
   - IV availability warning in dashboard metrics

---

## Next Steps (Future Sessions)

### Phase 4: Live Snapshot Integration (P1)
- Merge Schwab live snapshots into canonical time-series
- Add `source='schwab'` records
- Preserve append-only structure

### Phase 5: Execution Confidence Scoring (P2)
- Use IV Rank in confidence calculation (when available)
- Low IV Rank + expansion → higher confidence
- High IV Rank + contraction → lower confidence

### Phase 6: Database Migration (P3 - Optional)
- DuckDB for analytical queries
- Preserve CSV as interchange format
- Faster rolling window calculations

---

## Success Criteria

✅ **Phase 3 Complete:**
- [x] IV availability flags loaded in Step 12
- [x] READY_NOW → STRUCTURALLY_READY downgrade implemented
- [x] Diagnostic reasons added to acceptance_reason
- [x] CLI diagnostics show IV availability status
- [x] Dashboard shows IV availability warnings
- [x] Validation test passed (5/5 strategies downgraded correctly)
- [x] No threshold lowering or data fabrication
- [x] Honest diagnostics: "4 days < 120 required"

**System Integrity:**
- Conservative execution rules preserved
- No strategies execute without full IV context
- Users understand why strategies are blocked
- Actionable timeline provided (~116 more days needed)

---

## Appendix: Diagnostic Messages

### Step 12 Logs:
```
📊 Loading IV availability flags...
✅ Loaded IV availability data: 885 records
   Date range: 2025-08-03 → 2025-12-29
   Using most recent date: 2025-12-29

📊 IV Availability Statistics:
   ❌ IV Rank unavailable: 5/5 (100.0%)
   📅 History for unavailable: avg=4.0 days, max=4 days
   📅 Required history: 120+ days
   ⏳ Estimated activation: ~116 more days needed

📊 IV availability gate: 5 READY_NOW strategies lack sufficient IV history
   Downgrading to STRUCTURALLY_READY (requires 120+ days of IV data)
   📅 IV history: avg=4.0 days, max=4 days (need 120+)
   ⏳ Estimated activation: ~116 more days of data collection
```

### CLI Output:
```
📊 IV AVAILABILITY SUMMARY
================================================
⚠️  IV Rank unavailable: 5/5 strategies
📅 History: avg=4.0 days, max=4 days (need 120+)
⏳ Estimated activation: ~116 more days

ℹ️  Strategies without IV Rank would be downgraded to STRUCTURALLY_READY
   (good structure, awaiting sufficient IV history for full evaluation)
```

---

**Phase 3 Status:** ✅ **COMPLETE**  
**Ready for:** Live snapshot integration (Phase 4)
