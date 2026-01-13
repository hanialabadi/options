# Pipeline Baseline Status (2026-01-02)

## ✅ Current State: KNOWN-GOOD BASELINE

**Git Tag**: `baseline_merge_fix`  
**Commits**: 
- `48b895b` - Add Step 9B merge guardrail
- `66ec0c1` - Fix Step 9B merge collision

---

## 🔒 Verified Working

### Schwab API Integration
- ✅ OAuth token refresh working
- ✅ `/quotes` endpoint: Batch quote fetching (100 symbols)
- ✅ `/pricehistory` endpoint: OHLCV candles for HV calculation
- ✅ `/chains` endpoint: Option contracts with Greeks, pricing, liquidity

### Pipeline Execution
- ✅ **Step 0**: Snapshot generation with live Schwab data
- ✅ **Step 2**: Snapshot loading and enrichment (Sinclair + Murphy)
- ✅ **Step 5**: Chart analysis and pattern detection
- ✅ **Step 7**: Strategy recommendation (multi-strategy)
- ✅ **Step 9A**: DTE window assignment
- ✅ **Step 9B**: Contract fetching from Schwab API ✨ **FIXED**
- ✅ **Step 11**: Independent strategy evaluation
- ✅ **Step 8**: Portfolio capital allocation

### Data Integrity
- ✅ Column-monotonic invariant: All steps preserve input columns
- ✅ Critical fields verified at every boundary:
  - `last_price` (underlying price)
  - `Validation_Status` (evaluation result)
  - All IV/HV metrics
  - All Greek values (delta, gamma, vega, theta)
  - Contract details (strike, expiration, bid, ask, OI)

---

## 🐛 Fixed Issues

### Root Cause: Step 9B Merge Collision
**Problem**: Merge between Step 11 (evaluated strategies) and Step 9A (timeframes) caused column name collisions because both DataFrames contained full snapshot data.

**Impact**: Pandas added suffixes (`_step11`, `_step9a`) to disambiguate, breaking downstream code expecting unsuffixed names like `last_price` and `Validation_Status`.

**Fix**: 
```python
# Before: Merge with full DataFrames → column collision
merged = evaluated_strategies_df.merge(timeframes_df, ...)

# After: Drop duplicates, keep only unique timeframe fields
timeframes_unique_cols = ['Ticker', 'Strategy_Name', 'Min_DTE', 'Max_DTE', ...]
timeframes_df_clean = timeframes_df[timeframes_unique_cols]
merged = evaluated_strategies_df.merge(timeframes_df_clean, ...)
```

### Guardrail Added
```python
# Validates critical columns survive merge
required_columns = ['last_price', 'Validation_Status', 'Ticker', 'Strategy_Name']
missing_columns = [col for col in required_columns if col not in merged.columns]
if missing_columns:
    raise ValueError(f"❌ CRITICAL: Merge dropped required columns: {missing_columns}")
```

---

## 🚫 What NOT to Do

1. **Do NOT modify Step 0** (snapshot generation) - it's stable
2. **Do NOT rebuild DataFrames in Step 11** - use pass-through pattern
3. **Do NOT merge full DataFrames** - only merge unique secondary fields
4. **Do NOT add enhancements without incremental testing** - one at a time

---

## ▶️ Next Steps (When Ready)

### Phase 1: Re-enable Entry Quality Enhancements (Incremental)

**Order of Implementation**:
1. **Step 2 only**: Intraday metrics (compression, gap, position tags)
   - Lowest risk (snapshot-only data)
   - No merge dependencies
   - Easy to validate

2. **Step 9B only**: Execution quality (bid/ask depth, dividend risk)
   - Requires contract data
   - Test after Step 2 is verified

3. **Step 11 only**: Entry readiness scoring (composite 0-100)
   - Depends on all prior enrichments
   - Final integration

**Validation After Each Step**:
```bash
python scan_live.py data/snapshots/ivhv_snapshot_live_*.csv
# Check: No KeyError, no column loss, enrichment columns added
```

### Phase 2: New Enhancement Module Architecture

**If adding new enhancements in the future**:
- Create separate module (e.g., `core/scan_engine/entry_quality_enhancements.py`)
- All functions return dicts (never mutate DataFrames in place)
- Use `df.copy()` pattern for safety
- Wrap all enrichment calls in try/except (non-blocking)
- Add guardrail assertions after critical merges

---

## 📊 Current Output Schema (Verified)

### Snapshot Fields (Step 2 Output)
- `Ticker`, `last_price`, `volume`
- IV metrics: `IV_30_D_Call`, `IV_30_D_Put`, etc.
- HV metrics: `HV_30_D_Cur`, `hv_slope`, etc.
- Murphy indicators: `Signal_Type`, `Regime`, `Chart_Pattern`, etc.
- Sinclair metrics: `IV_Rank_30D`, `Volatility_Regime`, etc.

### Contract Fields (Step 9B Output)
- `strike`, `expiration`, `delta`, `gamma`, `vega`, `theta`
- `bid`, `ask`, `openInterest`, `volume`
- `Liquidity_Grade`, `Contract_Status`
- Spread metrics: `spread_pct`, `bid_ask_spread`

### Evaluation Fields (Step 11 Output)
- `Validation_Status` (Valid/Watch/Reject/Incomplete)
- `Theory_Compliance_Score` (0-100)
- `Execution_State` (DO_EXECUTE/WATCH_LIST/DO_NOT_EXECUTE)
- `Strategy_Family`, `Strategy_Family_Rank`

---

## 🔍 Debug Commands

**Test pipeline end-to-end**:
```bash
source venv/bin/activate
python scan_live.py data/snapshots/ivhv_snapshot_live_*.csv
```

**Verify column preservation**:
```python
# Add at critical boundaries:
print("DEBUG:", list(df.columns))
print(f"Has last_price: {'last_price' in df.columns}")
```

**Check merge results**:
```python
# After any DataFrame merge:
assert 'last_price' in merged.columns, "Merge dropped last_price!"
```

---

## 📌 Architectural Principles (Reinforced)

1. **Column-Monotonic**: Steps add columns, never remove them
2. **Pass-Through Pattern**: Steps process and return (never rebuild from scratch)
3. **Merge Discipline**: Only merge unique secondary fields, not full DataFrames
4. **Fail Loudly**: Use assertions to catch schema drift early
5. **Incremental Validation**: Test each change independently before combining

---

**Last Updated**: 2026-01-02  
**Pipeline Status**: ✅ STABLE - Ready for enhancement work  
**Test Coverage**: End-to-end CLI execution verified
