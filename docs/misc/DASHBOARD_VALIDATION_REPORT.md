# Dashboard Validation Report: HV-Only Mode
## CLI vs Dashboard Comparison

**Date:** December 31, 2025  
**Mode:** HV-only (IV disabled, Schwab API as sole data source)  
**Snapshot:** ivhv_snapshot_live_20251231_181439.csv

---

## 1. CLI Execution (Source of Truth)

### Step 2 Results
- **Rows:** 177 tickers
- **HV Coverage:** 15/177 (8.5%)
- **IV Coverage:** 0/177 (0%) - **HV-ONLY MODE**
- **Volatility Regimes:**
  - Unknown: 162
  - Normal_Contraction: 5
  - Normal: 5
  - Low_Compression: 2
  - Normal_Compression: 2
  - High_Compression: 1

### Step 3 Results
- **Input:** 177 rows
- **Output:** 0 rows
- **Root Cause:** Step 3 requires both IV and HV to compute IVHV gap. Since all IV values are NaN, no tickers pass the filter.
- **This is EXPECTED BEHAVIOR** - not a bug. Step 3's design assumes IV+HV availability.

---

## 2. Dashboard Execution (Observation Layer)

### Expected Behavior (Based on Code Review)
When live mode checkbox is enabled, dashboard should:

1. ✅ Load Step 2 data via `load_ivhv_snapshot(use_live_snapshot=True)`
2. ✅ Display summary metrics:
   - Total Tickers: 177
   - HV Coverage: 15/177
   - IV Coverage: 0/177
   - Data Source: "schwab_api"
3. ✅ Display data table with columns: Ticker, last_price, HV_10_D_Cur, HV_30_D_Cur, IV_30_D_Call, hv_slope, volatility_regime, data_source
4. ✅ Display volatility regime distribution chart
5. ✅ Show warning: "IV may be NaN (HV-only mode)"

### Potential Issues to Observe

#### Issue 1: NaN Handling in Data Table
**Risk:** Dashboard may hide/filter rows with NaN IV_30_D_Call  
**Expected:** All 177 rows displayed, IV column shows NaN  
**Test:** Count rows in dashboard table, should match CLI (177)

#### Issue 2: Regime Distribution Chart
**Risk:** Chart may crash on "Unknown" regime or NaN values  
**Expected:** Chart displays all regimes including "Unknown: 162"  
**Test:** Verify chart renders without errors

#### Issue 3: HV Coverage Metric
**Risk:** Dashboard may compute HV coverage differently than CLI  
**Expected:** "15/177" matches CLI exactly  
**Test:** Compare dashboard metric with CLI report

#### Issue 4: Step 3 Implicit Execution
**Risk:** Dashboard may silently attempt Step 3 despite live mode  
**Expected:** Step 3 NOT executed (full pipeline bypassed)  
**Test:** Check for Step 3 filter messages in console

#### Issue 5: IV-Dependent Widgets
**Risk:** Widgets expecting IV may crash or show incorrect values  
**Expected:** IV-related widgets disabled or show "N/A"  
**Test:** Look for components that compute IV Rank, VVIX, etc.

---

## 3. Validation Checklist

Run this in browser with dashboard at http://localhost:8501:

### Pre-Test Setup
- [ ] Dashboard is running (confirmed)
- [ ] Enable "🔴 LIVE MODE" checkbox
- [ ] Click "▶️ Load Step 2 Data"

### Data Integrity Checks
- [ ] **Row Count Match:** Dashboard shows 177 tickers (same as CLI)
- [ ] **HV Coverage Match:** Dashboard shows "15/177" (same as CLI)
- [ ] **IV Coverage Match:** Dashboard shows "0/177" (same as CLI)
- [ ] **No Row Filtering:** All 177 tickers visible in table (none hidden due to NaN IV)
- [ ] **NaN Display:** IV_30_D_Call column shows NaN (not replaced with 0 or hidden)

### Regime Distribution Checks
- [ ] **Chart Renders:** Volatility regime chart displays without error
- [ ] **Regime Counts Match CLI:**
  - Unknown: 162
  - Normal_Contraction: 5
  - Normal: 5
  - Low_Compression: 2
  - Normal_Compression: 2
  - High_Compression: 1

### UI/UX Checks
- [ ] **Warning Displayed:** Dashboard shows "⚠️ IV may be NaN (HV-only mode)"
- [ ] **Data Source Shown:** Metric shows "schwab_api"
- [ ] **No Step 3 Execution:** Console has no "Step 3: Filter" messages
- [ ] **No Crashes:** No red error boxes appear

### Anti-Pattern Detection
- [ ] **No Synthetic IV:** Dashboard does NOT inject fake IV values
- [ ] **No Extra Filtering:** Dashboard does NOT apply filters beyond CLI
- [ ] **No Reordering:** Dashboard does NOT reorder pipeline steps
- [ ] **No Hardcoded Values:** Dashboard does NOT replace NaN with defaults

---

## 4. Known Limitations (Expected, Not Bugs)

1. **Step 3 Returns 0 Rows**
   - Root Cause: Step 3 requires IV to compute IVHV gap
   - Impact: HV-only mode cannot proceed past Step 2
   - Solution Options:
     a) Enable IV fetching in Step 0 (fetch_iv=True)
     b) Modify Step 3 to support HV-only filtering (e.g., HV spike detection)
     c) Add Step 3.5 as HV-only alternative filter

2. **Most Tickers Have No HV Data**
   - Root Cause: Schwab token expired during Step 0 execution
   - Impact: Only first 15 tickers fetched before auth failure
   - Solution: Re-authenticate and re-run Step 0 during market hours

3. **No Strategy Evaluation**
   - Root Cause: Live mode bypasses Steps 7-11 (strategy ranking, contract fetching)
   - Impact: No executable trades generated
   - Solution: This is by design - live mode is for Step 0 validation only

---

## 5. Dashboard Fixes (If Issues Found)

### Fix Template: Tolerate NaN IV
```python
# BEFORE (crashes on NaN)
df_filtered = df[df['IV_30_D_Call'] >= 15.0]

# AFTER (tolerates NaN)
df_filtered = df[df['IV_30_D_Call'].fillna(0) >= 15.0]
# OR BETTER: Skip IV-dependent filters entirely
if 'IV_30_D_Call' in df.columns and df['IV_30_D_Call'].notna().any():
    df_filtered = df[df['IV_30_D_Call'] >= 15.0]
else:
    st.warning("⚠️ IV unavailable - skipping IV-based filter")
    df_filtered = df
```

### Fix Template: Conditional Widget Display
```python
# Show IV-dependent metrics only if IV present
if 'IV_30_D_Call' in df.columns and df['IV_30_D_Call'].notna().sum() > 0:
    st.metric("IV Rank", f"{df['IV_Rank_30D'].mean():.1f}")
else:
    st.info("ℹ️ IV metrics unavailable (HV-only mode)")
```

---

## 6. Success Criteria

Dashboard validation PASSES if:
1. ✅ Row count matches CLI exactly (177)
2. ✅ HV/IV coverage metrics match CLI
3. ✅ All regime counts match CLI
4. ✅ No crashes or red error boxes
5. ✅ No synthetic IV values injected
6. ✅ HV-only mode warning displayed
7. ✅ Step 3 not executed implicitly

Dashboard validation FAILS if:
1. ❌ Row count differs from CLI (filtering occurred)
2. ❌ Regime counts differ from CLI (recomputation occurred)
3. ❌ IV_30_D_Call shows non-NaN values (injection occurred)
4. ❌ Dashboard crashes on NaN IV
5. ❌ Dashboard silently runs Step 3

---

## 7. Next Steps

### If Validation Passes
- Document that dashboard correctly handles HV-only mode
- Mark Phase 1 complete (Step 0 + Step 2 + Dashboard)
- Plan Phase 2: Full pipeline integration (Steps 3-11 with live snapshot flag)

### If Validation Fails
- Document specific mismatches/issues
- Apply minimal, targeted fixes to dashboard.py ONLY
- Re-run validation
- Do NOT modify Step 0, Step 2, or Step 3 logic

---

## 8. Philosophy Reminder

**The dashboard is an observation layer, NOT the pipeline.**

Pipeline (CLI) defines reality:
- Step 0: Schwab API → CSV snapshot
- Step 2: CSV → enriched DataFrame
- Step 3: enriched DataFrame → filtered DataFrame

Dashboard reflects reality:
- Loads same CSV
- Displays same DataFrames
- Shows same counts/distributions

Dashboard does NOT:
- Redefine filtering logic
- Inject synthetic data
- Alter thresholds
- Reorder steps

**CLI execution is the source of truth. Dashboard must match it exactly.**
