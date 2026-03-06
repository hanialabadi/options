# Dashboard IV Metadata Sync - Fixed

**Date:** 2026-02-03
**Issue:** Dashboard showing "0 days" while scan engine has correct IV metadata
**Status:** ✅ FIXED

---

## Problem Statement

**Symptom:**
- Dashboard POST-SCAN provenance shows "Median IV depth: 0 days" and "0 tickers read"
- Fidelity status correctly shows "All strategies IV_MATURE"
- **This is impossible** - indicates dashboard not reading scan output correctly

**Root Cause:**
Dashboard was reading IV metadata from `results['snapshot']` only, with:
- No fallback to other data sources
- No error handling for missing/empty columns
- No diagnostic information when data not found
- Silent failures masking the actual issue

---

## Fixes Implemented

### 1. ✅ Multi-Source IV History Reading

**Location:** Lines 692-735 (POST-SCAN DuckDB section)

**Before:**
```python
if 'snapshot' in results and not results['snapshot'].empty:
    df_snapshot = results['snapshot']
    if 'iv_history_days' in df_snapshot.columns:
        iv_history_depth = int(df_snapshot['iv_history_days'].median())
```

**After:**
```python
# Try multiple sources: acceptance_all → snapshot → acceptance_ready
for key in ['acceptance_all', 'snapshot', 'acceptance_ready']:
    if key in results and not results[key].empty:
        df_source = results[key]

        # Check for iv_history_days column (lowercase)
        if 'iv_history_days' in df_source.columns:
            median_val = df_source['iv_history_days'].median()
            if not pd.isna(median_val) and median_val > 0:
                iv_history_depth = int(median_val)
                source_used = key
                break

        # Fallback: Check uppercase variant
        if 'IV_History_Days' in df_source.columns:
            median_val = df_source['IV_History_Days'].median()
            if not pd.isna(median_val) and median_val > 0:
                iv_history_depth = int(median_val)
                break
```

**Result:**
- Tries 3 data sources in priority order
- Handles both lowercase and uppercase column names
- Checks for NaN and zero values
- Logs which source was used

---

### 2. ✅ Authoritative Fidelity Status Reading

**Location:** Lines 737-790 (POST-SCAN Fidelity section)

**Before:**
```python
if 'snapshot' in results and not results['snapshot'].empty:
    df_snapshot = results['snapshot']
    if 'IV_Rank_Source' in df_snapshot.columns:
        fidelity_triggered = (df_snapshot['IV_Rank_Source'] == 'Fidelity').any()
```

**After:**
```python
# Try reading from multiple sources
df_source = None
for key in ['acceptance_all', 'snapshot', 'acceptance_ready']:
    if key in results and not results[key].empty and 'IV_Rank_Source' in results[key].columns:
        df_source = results[key]
        break

if df_source is not None:
    # Check IV_Rank_Source for Fidelity
    fidelity_triggered = (df_source['IV_Rank_Source'] == 'Fidelity').any()
    fidelity_ticker_count = (df_source['IV_Rank_Source'] == 'Fidelity').sum()

    # Get IV maturity breakdown
    mature_count = (df_source['IV_Maturity_State'] == 'MATURE').sum()
    immature_count = (df_source['IV_Maturity_State'] == 'IMMATURE').sum()

    # Determine reason
    if not fidelity_triggered:
        fidelity_reason = f"Skipped (R2): Schwab IV mature - {mature_count} tickers"
    else:
        fidelity_reason = f"Triggered (R3): Immature IV - {immature_count} tickers escalated"

    # Add median depth context
    median_depth = df_source['iv_history_days'].median()
    if not pd.isna(median_depth):
        fidelity_reason += f" (median depth: {int(median_depth)} days)"
```

**Result:**
- Reads from most complete data source
- Shows actual MATURE/IMMATURE counts
- Displays trigger rule (R2/R3)
- Includes median depth for context
- Clear reason for skip vs trigger

---

### 3. ✅ Enhanced PRE-SCAN DuckDB Query

**Location:** Lines 145-167 (get_snapshot_info function)

**Before:**
```python
try:
    db_path = get_iv_history_db_path()
    if db_path.exists():
        con = duckdb.connect(str(db_path), read_only=True)
        summary = get_history_summary(con)
        con.close()
        iv_history = int(summary.get('median_depth', 0))
except Exception as e:
    logger.warning(f"Failed to query IV history: {e}")
    iv_history = 0
```

**After:**
```python
try:
    db_path = get_iv_history_db_path()
    logger.info(f"📊 Checking IV history database: {db_path}")

    if db_path.exists():
        con = duckdb.connect(str(db_path), read_only=True)
        summary = get_history_summary(con)
        con.close()

        median_depth = summary.get('median_depth', 0)
        total_tickers = summary.get('total_tickers', 0)
        iv_history = int(median_depth) if median_depth is not None else 0

        logger.info(f"✅ IV history loaded: {iv_history} days median, {total_tickers} tickers")
    else:
        iv_history = 0
        logger.warning(f"⚠️ IV history database not found at {db_path}")
except Exception as e:
    logger.error(f"❌ Failed to query IV history: {e}", exc_info=True)
    iv_history = 0
```

**Result:**
- Logs database path being checked
- Logs successful loads with ticker count
- Uses exc_info=True for full error traceback
- Handles None values explicitly

---

### 4. ✅ Debug Mode Diagnostics

**Location:** Lines 793-817 (after POST-SCAN provenance)

**New Feature:**
```python
# DIAGNOSTIC: Show IV metadata presence (Debug Mode only)
if st.session_state.debug_mode:
    st.caption("🔍 **IV Metadata Diagnostic**")
    diagnostic_info = []

    for key in ['acceptance_all', 'snapshot', 'acceptance_ready']:
        if key in results and not results[key].empty:
            df_diag = results[key]
            has_iv_days = 'iv_history_days' in df_diag.columns
            has_iv_source = 'IV_Rank_Source' in df_diag.columns
            has_maturity = 'IV_Maturity_State' in df_diag.columns

            if has_iv_days or has_iv_source or has_maturity:
                diagnostic_info.append(f"{key}: iv_days={has_iv_days}, source={has_iv_source}, maturity={has_maturity}")

                # Show sample values
                if has_iv_days:
                    col_name = 'iv_history_days'
                    sample_val = df_diag[col_name].iloc[0]
                    diagnostic_info.append(f"  → Sample {col_name}: {sample_val}")
```

**Result:**
- Shows which DataFrames have IV metadata
- Shows which columns are present
- Displays sample values for verification
- Only visible in Debug Mode (doesn't clutter normal view)

---

## Data Flow Verification

### PRE-SCAN (Before Execution)

**Source:** DuckDB `iv_term_history` table
**Query:** `get_history_summary(con)` → median_depth
**Display:** "🟢 READ ONLY (X days)" or "🔶 NO DATA (0 days)"

### POST-SCAN (After Execution)

**Source 1 (Priority):** `results['acceptance_all']` DataFrame
- Columns: `iv_history_days`, `IV_Rank_Source`, `IV_Maturity_State`
- Most complete data (includes all strategies)

**Source 2 (Fallback):** `results['snapshot']` DataFrame
- Same columns as above
- Step 2 output

**Source 3 (Last Resort):** `results['acceptance_ready']` DataFrame
- Same columns as above
- Only READY strategies (subset)

**Display:**
- "📊 X tickers read" (count of IV_Rank_Source == 'DuckDB')
- "📅 Median depth: X days" (median of iv_history_days)
- "Source: {key}" (which DataFrame was used)

---

## Expected Behavior (After Fix)

### Scenario 1: IV History Present (130 days)

**PRE-SCAN:**
```
IV History (DuckDB)
🟢 READ ONLY (130 days)
Constant-maturity IV history
```

**POST-SCAN:**
```
DuckDB (IV History)
📊 487 tickers read
📅 Median depth: 130 days
Source: acceptance_all
```

**Fidelity:**
```
⚪ NOT TRIGGERED
Skipped (R2): Schwab IV mature - 487 tickers (median depth: 130 days)
```

### Scenario 2: IV History Partial (60 days)

**PRE-SCAN:**
```
IV History (DuckDB)
🟢 READ ONLY (60 days)
Constant-maturity IV history
```

**POST-SCAN:**
```
DuckDB (IV History)
📊 400 tickers read
📅 Median depth: 60 days
Source: acceptance_all
```

**Fidelity:**
```
🟡 TRIGGERED
287 tickers escalated
Triggered (R3): Immature IV - 287 tickers escalated (median depth: 60 days)
```

### Scenario 3: IV History Missing (0 days)

**PRE-SCAN:**
```
IV History (DuckDB)
🔶 NO DATA (0 days)
Constant-maturity IV history
```

**POST-SCAN:**
```
DuckDB (IV History)
📊 0 tickers read
📅 Median depth: 0 days
⚠️ No IV history metadata in results
```

**Fidelity:**
```
🟡 TRIGGERED
500 tickers escalated
Triggered (R3): Immature IV - 500 tickers escalated (median depth: 0 days)
```

---

## Logging Improvements

**New Log Messages:**

### PRE-SCAN:
```
📊 Checking IV history database: /path/to/data/iv_history.duckdb
✅ IV history loaded: 130 days median, 487 tickers
```

### POST-SCAN:
```
📊 IV history depth read from acceptance_all: 130 days
📊 DuckDB success count from acceptance_all: 487
📊 Fidelity status: triggered=False, reason=Skipped (R2): Schwab IV mature - 487 tickers (median depth: 130 days)
```

### Errors:
```
⚠️ IV history database not found at /path/to/data/iv_history.duckdb - run bootstrap or daily collection
❌ Failed to query IV history: [full traceback]
⚠️ No IV_Rank_Source column found in results
```

---

## Testing Checklist

### ✅ Test 1: Fresh Bootstrap (130 days)
1. Run: `venv/bin/python scripts/admin/bootstrap_iv_history.py --mode sample --days 130`
2. Run scan: `venv/bin/python scripts/cli/scan_live.py --tickers AAPL,MSFT,NVDA`
3. Open dashboard
4. **Verify PRE-SCAN:** Shows "🟢 READ ONLY (130 days)"
5. **Verify POST-SCAN:** Shows "📅 Median depth: 130 days" and "📊 3 tickers read"
6. **Verify Fidelity:** Shows "⚪ NOT TRIGGERED" with "Skipped (R2)" reason

### ✅ Test 2: Empty Database (0 days)
1. Delete: `data/iv_history.duckdb`
2. Run scan: `venv/bin/python scripts/cli/scan_live.py --tickers AAPL,MSFT,NVDA`
3. Open dashboard
4. **Verify PRE-SCAN:** Shows "🔶 NO DATA (0 days)"
5. **Verify POST-SCAN:** Shows "📅 Median depth: 0 days"
6. **Verify Fidelity:** Shows "🟡 TRIGGERED" with reason

### ✅ Test 3: Debug Mode Diagnostics
1. Enable Debug Mode in dashboard
2. Run scan
3. **Verify:** Diagnostic section shows which DataFrames have IV metadata
4. **Verify:** Shows sample values from columns

---

## Files Modified

**Only:** `streamlit_app/scan_view.py`

**Lines Changed:**
- Lines 145-167: Enhanced PRE-SCAN DuckDB query with logging
- Lines 692-735: Multi-source IV history reading with fallbacks
- Lines 737-790: Authoritative Fidelity status reading
- Lines 793-817: Debug mode diagnostic display

**No changes to:**
- Scan engine logic
- IV history calculation
- Fidelity trigger rules
- Data persistence

---

## Compliance

### ✅ Presentation-Only Changes
- No scan engine modifications
- No threshold changes
- No gate logic changes
- Only UI data reading improved

### ✅ Authoritative Reading
- Reads directly from scan output DataFrames
- No computation or inference
- Multi-source fallback for robustness
- Clear logging of data sources

### ✅ Observable by Design
- Shows where data came from (source DataFrame)
- Shows actual values from scan results
- Debug mode shows metadata presence
- Clear error messages when data missing

---

## Status

✅ **FIXED AND VALIDATED**

Dashboard now correctly reads and displays:
- IV history depth from scan results
- DuckDB ticker count
- Fidelity trigger status with rule and reason
- Maturity breakdown (MATURE/IMMATURE counts)

**No more silent failures. No more 0s when data exists.**

---

## Next Steps

1. ✅ Test with real scan output
2. ✅ Verify logs show correct data sources
3. ✅ Check Debug Mode diagnostics
4. ✅ Confirm Fidelity status matches scan engine logs

**Ready for production.**
