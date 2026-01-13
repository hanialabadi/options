# Dashboard vs CLI Execution Path Audit

**Date:** January 2, 2026  
**Purpose:** Identify execution path divergence between CLI (`scan_live.py`) and Dashboard (`streamlit_app/dashboard.py`)  
**Goal:** Force dashboard to run EXACT same code path as CLI with NO fallback, NO silent degradation

---

## Executive Summary

### 🚨 CRITICAL FINDINGS

**The dashboard has TWO DISTINCT execution modes that report identical success messages:**

1. **"Live Mode"** → Loads Step 2 only, **NEVER runs full pipeline**
2. **"Legacy Mode"** → Runs full pipeline via `run_full_scan_pipeline()`

**Both modes display "✅ Full pipeline completed" or similar success messages, creating epistemic trust violation.**

### Root Cause

The dashboard contains a **dual-mode execution system** where:
- **Button label changes** based on mode: `"▶️ Load Step 2 Data"` vs `"▶️ Run Full Pipeline"`
- **Success messages are conditional** but not sufficiently distinct
- **Live mode bypasses Steps 3-12** but still displays "✅" success indicators
- **User cannot easily distinguish** which execution path ran

---

## Section 1: CLI Execution Path (scan_live.py)

### Entry Point
**File:** `scan_live.py` (122 lines)  
**Execution Model:** Command-line script with explicit arguments

### Snapshot Resolution

```python
# Lines 25-38
if len(sys.argv) > 1:
    snapshot_path = sys.argv[1]
    if not os.path.exists(snapshot_path):
        print(f"❌ Snapshot not found: {snapshot_path}")
        sys.exit(1)
    print(f"✅ Using specified snapshot: {snapshot_path}")
else:
    try:
        snapshot_path = resolve_snapshot_path()
        print(f"✅ Using snapshot: {snapshot_path}")
    except Exception as e:
        print(f"❌ Failed to resolve snapshot: {e}")
        print("Please ensure you have a recent IV/HV snapshot in data/ivhv_snapshots/")
        sys.exit(1)
```

**Resolution Strategy:**
1. **Explicit path** from CLI argument (if provided)
2. **Auto-resolve** using `resolve_snapshot_path()` (finds latest in `data/ivhv_snapshots/`)
3. **Hard fail** if snapshot not found → `sys.exit(1)`

### Pipeline Execution

```python
# Lines 47-52
results = run_full_scan_pipeline(
    snapshot_path=snapshot_path,
    output_dir="data/scan_outputs",
    account_balance=100000.0,
    max_portfolio_risk=0.20
)
```

**Execution Characteristics:**
- **Single code path** → ALWAYS calls `run_full_scan_pipeline()`
- **No fallback** → If pipeline fails, exception propagates to top level
- **Explicit arguments** → All parameters passed as function arguments
- **Hard failure on error** → `sys.exit(1)` on any exception

### Success Reporting

```python
# Lines 54-80
if 'final_trades' in results and results['final_trades'] is not None:
    df_final = results['final_trades']
    
    if df_final.empty:
        print("\n❌ NO CANDIDATES FOUND")
        print("Market conditions may not meet GEM criteria.")
    else:
        print(f"\n✅ {len(df_final)} CANDIDATES FOUND")
        # ... display results
else:
    print("\n⚠️  Pipeline completed but no final trades returned")
    print("Check intermediate steps for filtering reasons")
```

**Success Criteria:**
- Reports success **ONLY if** `final_trades` key exists in results
- Distinguishes **empty results** from **missing results**
- **Never reports "full pipeline completed"** → reports specific outcomes

---

## Section 2: Dashboard Execution Path (streamlit_app/dashboard.py)

### Entry Point
**File:** `streamlit_app/dashboard.py` (1128 lines)  
**Execution Model:** Streamlit app with UI toggles and buttons

### Mode Selection

```python
# Lines 443-453
use_live_snapshot = st.checkbox(
    "🔴 **LIVE MODE**",
    value=False,
    help="Use Live Schwab Snapshot from Step 0"
)

if use_live_snapshot:
    st.success("✅ **STEP 0 ACTIVE** - Will load latest Schwab snapshot (bypasses scraper & full pipeline)")
else:
    st.info("ℹ️ Legacy mode - Uses data source from sidebar + runs full pipeline")
```

**Two Execution Modes:**
1. **Live Mode** (`use_live_snapshot=True`) → Load Step 2 data only
2. **Legacy Mode** (`use_live_snapshot=False`) → Run full pipeline

### Snapshot Resolution (Legacy Mode Only)

```python
# Lines 550-579
if uploaded_file_obj:
    # Save uploaded file to a temporary path
    uploaded_temp_path = Path("./temp_uploaded_snapshot.csv")
    with open(uploaded_temp_path, "wb") as f:
        f.write(uploaded_file_obj.getbuffer())
    snapshot_path = str(uploaded_temp_path)
    data_source_type = 'uploaded'
    st.info(f"Using uploaded file: {uploaded_file_obj.name}")
elif explicit_snapshot_path_input:
    snapshot_path = explicit_snapshot_path_input
    data_source_type = 'step0_disk'
    st.info(f"Using explicit path: {snapshot_path}")
else:
    # Use latest snapshot from data/snapshots/
    snapshot_dir = Path("data/snapshots")
    if snapshot_dir.exists():
        snapshot_files = list(snapshot_dir.glob("ivhv_snapshot_*.csv"))
        if snapshot_files:
            # Get most recent by modification time
            latest_snapshot = max(snapshot_files, key=lambda p: p.stat().st_mtime)
            snapshot_path = str(latest_snapshot)
            data_source_type = 'step0_disk'
            st.info(f"Using latest snapshot: {latest_snapshot.name}")
        else:
            st.error("❌ No snapshot files found in data/snapshots/. Please run Step 0 or upload a snapshot.")
            st.stop()
    else:
        st.error("❌ data/snapshots/ directory not found. Please run Step 0 first.")
        st.stop()
```

**Resolution Strategy (Legacy Mode):**
1. **Uploaded file** (user provides via UI)
2. **Explicit path** (user enters in text input)
3. **Auto-resolve** (finds latest in `data/snapshots/`)
4. **Soft fail** → Shows Streamlit error message, calls `st.stop()` (doesn't exit process)

**Resolution Strategy (Live Mode):**
- **NONE** → Snapshot path never resolved
- **Bypasses** `run_full_scan_pipeline()` entirely

---

## Section 3: Execution Divergence Analysis

### 🔴 DIVERGENCE #1: Dual Execution Paths

#### CLI (scan_live.py)
```python
# ALWAYS runs full pipeline
results = run_full_scan_pipeline(
    snapshot_path=snapshot_path,
    output_dir="data/scan_outputs",
    account_balance=100000.0,
    max_portfolio_risk=0.20
)
```

#### Dashboard (streamlit_app/dashboard.py)
```python
# Lines 492-493
if use_live_snapshot:
    # LIVE MODE: Load Step 2 only
    df_step2 = load_ivhv_snapshot(
        use_live_snapshot=True,
        skip_pattern_detection=True
    )
    # ... store in session state, NEVER call run_full_scan_pipeline
else:
    # LEGACY MODE: Run full pipeline
    results = run_full_scan_pipeline(
        snapshot_path=snapshot_path,
        output_dir=None,
        account_balance=account_balance,
        max_portfolio_risk=max_portfolio_risk,
        sizing_method=sizing_method
    )
```

**Impact:**
- Dashboard has **conditional execution** → CLI has **unconditional execution**
- Live mode **NEVER** calls `run_full_scan_pipeline()`
- Live mode **NEVER** runs Steps 3-12
- User must read button label carefully to understand which mode is active

---

### 🔴 DIVERGENCE #2: Snapshot Resolution Location

#### CLI
```python
# Resolves snapshot BEFORE calling pipeline
snapshot_path = resolve_snapshot_path()  # Or from sys.argv[1]

# Then passes to pipeline
results = run_full_scan_pipeline(snapshot_path=snapshot_path, ...)
```

**Characteristics:**
- Snapshot resolution is **separate concern** from pipeline execution
- **Explicit path required** → No silent fallback
- **Fails fast** → `sys.exit(1)` if snapshot not found

#### Dashboard (Legacy Mode)
```python
# Resolves snapshot INSIDE button click handler
# Lines 550-579
if uploaded_file_obj:
    snapshot_path = str(uploaded_temp_path)
elif explicit_snapshot_path_input:
    snapshot_path = explicit_snapshot_path_input
else:
    # Auto-resolve with fallback chain
    snapshot_files = list(snapshot_dir.glob("ivhv_snapshot_*.csv"))
    if snapshot_files:
        latest_snapshot = max(snapshot_files, key=lambda p: p.stat().st_mtime)
        snapshot_path = str(latest_snapshot)
```

**Characteristics:**
- Snapshot resolution **embedded in UI logic**
- **Three fallback paths** → uploaded, explicit, auto-resolve
- **Soft fail** → `st.stop()` instead of `sys.exit(1)`
- **Auto-resolution uses glob pattern** → May pick stale snapshot

#### Dashboard (Live Mode)
```python
# Lines 495-496
df_step2 = load_ivhv_snapshot(
    use_live_snapshot=True,
    skip_pattern_detection=True
)
```

**Characteristics:**
- **No snapshot path resolution** → `load_ivhv_snapshot()` handles internally
- Calls `load_latest_live_snapshot()` inside `step2_load_snapshot.py`
- **Bypasses CLI snapshot resolution logic entirely**

---

### 🔴 DIVERGENCE #3: Success Message Truthfulness

#### CLI
```python
# Lines 54-60
if 'final_trades' in results and results['final_trades'] is not None:
    df_final = results['final_trades']
    if df_final.empty:
        print("\n❌ NO CANDIDATES FOUND")
    else:
        print(f"\n✅ {len(df_final)} CANDIDATES FOUND")
else:
    print("\n⚠️  Pipeline completed but no final trades returned")
```

**Success Criteria:**
- **Conditional on results** → Checks for `final_trades` key
- **Distinguishes empty vs missing** → Different messages
- **NEVER says "full pipeline completed"** → Reports specific outcomes

#### Dashboard (Legacy Mode)
```python
# Lines 630-634
final_trades_count = len(results.get('final_trades', pd.DataFrame()))
if final_trades_count > 0:
    st.success(f"✅ Full pipeline completed. {final_trades_count} final trades selected.")
else:
    st.warning(f"⚠️ Pipeline completed but 0 trades selected. See diagnostic funnel below.")
```

**Success Criteria:**
- **Says "Full pipeline completed"** → Implies all steps ran
- **Conditional on final_trades count** → But still says "completed"
- **Truthful in Legacy Mode** → Pipeline DID run all steps

#### Dashboard (Live Mode)
```python
# Lines 519-534
st.info(data_ctx.get_banner())
st.success(f"✅ Loaded {len(df_step2)} tickers from live Schwab data")

# Validate IV/HV coverage
if iv_coverage_pct < 0.5:
    st.warning(f"⚠️ Low IV coverage: {iv_coverage_pct:.0%}")
else:
    st.info(f"✅ IV coverage: {iv_coverage_pct:.0%}")

# Show limitations (not contradictions)
st.warning(
    "⚠️ **Live Mode Limitations:**\n"
    "- Step 3+ not executed (analysis bypassed)\n"
    "- IV coverage may vary\n"
    "- Strategy evaluation not available\n"
    "- Data not persisted to disk (ephemeral)"
)
```

**Success Criteria:**
- **Says "✅ Loaded N tickers"** → Truthful (Step 2 loaded)
- **Shows limitations** → "Step 3+ not executed" (truthful)
- **NEVER says "Full pipeline completed"** → Avoids false claim
- **BUT:** Still shows ✅ success icon when NO strategies evaluated

---

### 🔴 DIVERGENCE #4: Fallback Behavior

#### CLI
```python
# Lines 31-38
try:
    snapshot_path = resolve_snapshot_path()
    print(f"✅ Using snapshot: {snapshot_path}")
except Exception as e:
    print(f"❌ Failed to resolve snapshot: {e}")
    print("Please ensure you have a recent IV/HV snapshot in data/ivhv_snapshots/")
    sys.exit(1)
```

**Fallback Strategy:**
- **None** → Hard fail if snapshot not found
- **Explicit error message** → User knows exactly what failed
- **Process exits** → `sys.exit(1)`

#### Dashboard (Legacy Mode)
```python
# Lines 565-579
snapshot_files = list(snapshot_dir.glob("ivhv_snapshot_*.csv"))
if snapshot_files:
    latest_snapshot = max(snapshot_files, key=lambda p: p.stat().st_mtime)
    snapshot_path = str(latest_snapshot)
    data_source_type = 'step0_disk'
    st.info(f"Using latest snapshot: {latest_snapshot.name}")
else:
    st.error("❌ No snapshot files found in data/snapshots/. Please run Step 0 or upload a snapshot.")
    st.stop()
```

**Fallback Strategy:**
- **Auto-resolve fallback** → Finds latest snapshot by modification time
- **Soft fail** → `st.stop()` instead of `sys.exit(1)`
- **UI-friendly error** → Shows Streamlit error message
- **May use stale snapshot** → No age check before auto-resolve

#### Dashboard (Live Mode)
```python
# Lines 495-496
df_step2 = load_ivhv_snapshot(
    use_live_snapshot=True,
    skip_pattern_detection=True
)
```

**Fallback Strategy:**
- **Delegated to step2_load_snapshot.py** → Calls `load_latest_live_snapshot()`
- **Hard fail inside function** → Raises `FileNotFoundError` if no snapshot
- **Error propagates to Streamlit** → Shows exception in UI
- **User sees Python traceback** → Not user-friendly

---

## Section 4: Pipeline Contract Adherence

### CLI Contract
```python
# scan_live.py calls run_full_scan_pipeline() with explicit args
results = run_full_scan_pipeline(
    snapshot_path=snapshot_path,
    output_dir="data/scan_outputs",
    account_balance=100000.0,
    max_portfolio_risk=0.20
)

# Expected return contract (from pipeline.py docstring):
# results = {
#     'snapshot': df_step2,
#     'filtered': df_step3,
#     'charted': df_step5,
#     'validated_data': df_step6,
#     'recommended_strategies': df_step7,
#     'evaluated_strategies': df_step11,
#     'timeframes': df_step9a,
#     'selected_contracts': df_step9b,
#     'acceptance_all': df_step12_all,
#     'acceptance_ready': df_step12_ready,
#     'final_trades': df_step8,
#     'pipeline_health': health_dict
# }
```

**Contract Adherence:**
- ✅ **Always calls canonical entry point** → `run_full_scan_pipeline()`
- ✅ **Receives 12-key results dict** → All intermediate steps included
- ✅ **No conditional execution** → Pipeline runs fully or fails hard

### Dashboard (Legacy Mode) Contract
```python
# Lines 583-591
results = run_full_scan_pipeline(
    snapshot_path=snapshot_path,
    output_dir=None,
    account_balance=account_balance,
    max_portfolio_risk=max_portfolio_risk,
    sizing_method=sizing_method
)

# Store all results in session state
st.session_state['pipeline_results'] = {
    k: sanitize_for_arrow(v) 
    for k, v in results.items() 
    if isinstance(v, pd.DataFrame)
}
```

**Contract Adherence:**
- ✅ **Calls canonical entry point** → `run_full_scan_pipeline()`
- ✅ **Receives 12-key results dict** → Same as CLI
- ⚠️ **Filters results before storage** → Only stores DataFrames (excludes `pipeline_health`)
- ✅ **Later fixed** → `pipeline_health` stored separately (line 622)

### Dashboard (Live Mode) Contract
```python
# Lines 495-510
df_step2 = load_ivhv_snapshot(
    use_live_snapshot=True,
    skip_pattern_detection=True
)

# Create DataContext for provenance tracking
data_ctx = DataContext(
    source='schwab_live',
    capture_timestamp=datetime.now(),
    is_persisted=False,
    snapshot_path=None
)

# Store in session state
st.session_state['pipeline_results'] = {
    'snapshot': sanitize_for_arrow(df_step2)
}
st.session_state['live_snapshot_mode'] = True
```

**Contract Violation:**
- ❌ **NEVER calls canonical entry point** → Bypasses `run_full_scan_pipeline()`
- ❌ **Returns 1-key dict** → Only `'snapshot'` key, missing 11 other keys
- ❌ **No Steps 3-12** → User sees empty results for all downstream steps
- ❌ **Breaks UI assumptions** → Dashboard code expects 12-key dict

---

## Section 5: Execution Equivalence Requirements

### What Does "Execution Equivalence" Mean?

**Definition:**  
The dashboard must execute the **EXACT same code path** as the CLI, with the **EXACT same inputs**, producing the **EXACT same outputs**, with **NO silent fallbacks** or **mode switching**.

**Requirements:**
1. **Single entry point** → Both CLI and dashboard call `run_full_scan_pipeline()`
2. **Explicit snapshot path** → No auto-resolve fallbacks
3. **Same arguments** → Account balance, risk, sizing method
4. **Same outputs** → 12-key results dict with all intermediate steps
5. **Same failure mode** → Hard fail if snapshot missing or pipeline fails
6. **Same success reporting** → Only report "full pipeline completed" if all 12 steps ran

### Current State vs Target State

| Aspect | CLI (Target) | Dashboard (Current) | Equivalence? |
|--------|-------------|---------------------|--------------|
| **Entry point** | `run_full_scan_pipeline()` | `run_full_scan_pipeline()` (Legacy) or `load_ivhv_snapshot()` (Live) | ❌ NO |
| **Snapshot resolution** | Explicit (CLI arg or `resolve_snapshot_path()`) | Auto-resolve with fallback chain | ❌ NO |
| **Execution modes** | Single (always full pipeline) | Dual (Live vs Legacy) | ❌ NO |
| **Results contract** | 12-key dict | 12-key (Legacy) or 1-key (Live) | ❌ NO |
| **Failure mode** | Hard fail (`sys.exit(1)`) | Soft fail (`st.stop()`) | ⚠️ ACCEPTABLE |
| **Success reporting** | "N CANDIDATES FOUND" | "Full pipeline completed" | ⚠️ ACCEPTABLE |

---

## Section 6: Root Cause Analysis

### Why Does This Divergence Exist?

**Historical Context:**
1. **Dashboard predates CLI** → Built before canonical entry point established
2. **Live Mode added later** → Retrofitted onto existing dual-mode architecture
3. **UI convenience prioritized** → Auto-resolve fallback for user experience
4. **Trust not audited** → No systematic verification of execution equivalence

**Architectural Issues:**
1. **Dual entry points** → `load_ivhv_snapshot()` vs `run_full_scan_pipeline()`
2. **Conditional execution in UI** → Business logic mixed with presentation logic
3. **Silent mode switching** → User must read button label to know which mode active
4. **Fallback chains** → Multiple ways to resolve snapshot path
5. **Soft failures** → `st.stop()` instead of hard fail

---

## Section 7: Fix Options (Ranked by Correctness)

### Option 1: Eliminate Live Mode (BEST)

**Change:**
- Remove `use_live_snapshot` checkbox
- Remove all Live Mode code paths (lines 492-546)
- Dashboard ALWAYS calls `run_full_scan_pipeline()`
- Force user to provide explicit snapshot path OR use CLI-style auto-resolve

**Pros:**
- ✅ **Single code path** → Dashboard = CLI
- ✅ **Execution equivalence guaranteed**
- ✅ **No mode confusion**
- ✅ **Simplest to maintain**

**Cons:**
- ⚠️ Removes "quick preview" functionality
- ⚠️ Requires user to run full pipeline every time
- ⚠️ May frustrate users who only want Step 2 data

**Implementation Effort:** 🟢 LOW (delete ~100 lines)

---

### Option 2: Make Live Mode Explicit & Blocking (GOOD)

**Change:**
- Keep Live Mode but add **blocking modal warning**:
  ```
  ⚠️ WARNING: You are about to load Step 2 data ONLY
  
  Steps 3-12 will NOT run:
  - No chart signals
  - No strategy recommendations
  - No option contracts
  - No position sizing
  
  This is for data preview only. To run full pipeline, disable Live Mode.
  
  [Cancel] [I Understand, Load Step 2 Only]
  ```
- Change button label to **"⚠️ PREVIEW STEP 2 ONLY"** (not "Load Step 2 Data")
- **Never show success indicators** (✅) in Live Mode
- **Hide all downstream UI sections** (Pipeline Health, Acceptance, etc.)

**Pros:**
- ✅ **User explicitly acknowledges** what they're NOT getting
- ✅ **Prevents confusion** → Modal forces conscious choice
- ✅ **Preserves quick preview** → But makes limitations crystal clear

**Cons:**
- ⚠️ Still maintains dual code paths
- ⚠️ More complex to maintain
- ⚠️ Modal may annoy power users

**Implementation Effort:** 🟡 MEDIUM (add modal, hide UI sections)

---

### Option 3: Force Dashboard to Use CLI Function (BEST)

**Change:**
- Dashboard calls `resolve_snapshot_path()` BEFORE button click
- Dashboard passes resolved path to `run_full_scan_pipeline()`
- Remove auto-resolve fallback chains
- Remove uploaded file handling (force user to save to disk first)
- **ALWAYS run full pipeline** → No Live Mode

**Pros:**
- ✅ **Execution equivalence with CLI** → Exact same code path
- ✅ **Single entry point** → `run_full_scan_pipeline()`
- ✅ **No fallbacks** → Explicit snapshot required
- ✅ **Same failure mode** → Hard fail if snapshot missing

**Cons:**
- ⚠️ Less convenient for users
- ⚠️ Requires file system access
- ⚠️ Removes uploaded file feature

**Implementation Effort:** 🟢 LOW (20 lines changed)

---

### Option 4: Rename Live Mode to "Debug Mode" (OK)

**Change:**
- Rename `"🔴 LIVE MODE"` → `"🐛 DEBUG MODE: Step 2 Preview"`
- Change success message: `"✅ Loaded Step 2 data (pipeline NOT executed)"`
- Add prominent banner: `"⚠️ THIS IS NOT A FULL PIPELINE RUN"`
- Disable "Pipeline Health" section in debug mode
- Disable "Acceptance Logic" section in debug mode

**Pros:**
- ✅ **Clear naming** → "Debug" implies "not production"
- ✅ **Minimal code changes**
- ✅ **Preserves functionality**

**Cons:**
- ⚠️ Still maintains dual code paths
- ⚠️ Doesn't enforce execution equivalence
- ⚠️ UI changes only (doesn't fix architecture)

**Implementation Effort:** 🟢 LOW (rename + add banners)

---

## Section 8: Recommended Fix (Minimal Changes)

### Fix Plan: Option 3 (Force CLI-Style Execution)

**Goal:** Make dashboard call the EXACT same code path as CLI with NO fallback.

### Changes Required

#### Change 1: Remove Live Mode Entirely

**File:** `streamlit_app/dashboard.py`  
**Lines:** 443-453 (remove checkbox)

```python
# REMOVE THIS:
use_live_snapshot = st.checkbox(
    "🔴 **LIVE MODE**",
    value=False,
    help="Use Live Schwab Snapshot from Step 0"
)

if use_live_snapshot:
    st.success("✅ **STEP 0 ACTIVE** - Will load latest Schwab snapshot (bypasses scraper & full pipeline)")
else:
    st.info("ℹ️ Legacy mode - Uses data source from sidebar + runs full pipeline")
```

**Impact:** Removes dual-mode execution entirely.

---

#### Change 2: Remove Live Mode Execution Branch

**File:** `streamlit_app/dashboard.py`  
**Lines:** 492-546 (remove `if use_live_snapshot:` block)

```python
# REMOVE THIS:
if use_live_snapshot:
    with st.spinner("📥 Loading live snapshot from Step 0..."):
        df_step2 = load_ivhv_snapshot(
            use_live_snapshot=True,
            skip_pattern_detection=True
        )
        # ... ~50 lines of Live Mode code
```

**Impact:** Dashboard ALWAYS runs full pipeline (no Step 2 only mode).

---

#### Change 3: Force Explicit Snapshot Path

**File:** `streamlit_app/dashboard.py`  
**Lines:** 560-579 (replace auto-resolve logic)

**Before:**
```python
else:
    # Use latest snapshot from data/snapshots/
    snapshot_dir = Path("data/snapshots")
    if snapshot_dir.exists():
        snapshot_files = list(snapshot_dir.glob("ivhv_snapshot_*.csv"))
        if snapshot_files:
            latest_snapshot = max(snapshot_files, key=lambda p: p.stat().st_mtime)
            snapshot_path = str(latest_snapshot)
            data_source_type = 'step0_disk'
            st.info(f"Using latest snapshot: {latest_snapshot.name}")
```

**After:**
```python
else:
    # Force explicit snapshot path (no auto-resolve)
    st.error(
        "❌ No snapshot path provided.\n\n"
        "Please either:\n"
        "1. Upload a snapshot file, OR\n"
        "2. Enter an explicit path in the sidebar, OR\n"
        "3. Run scan_live.py from CLI instead"
    )
    st.stop()
```

**Impact:** No auto-resolve fallback → User must explicitly provide path.

---

#### Change 4: Unify Button Label

**File:** `streamlit_app/dashboard.py`  
**Line:** 486

**Before:**
```python
button_label = "▶️ Load Step 2 Data" if use_live_snapshot else "▶️ Run Full Pipeline"
```

**After:**
```python
button_label = "▶️ Run Full Pipeline"
```

**Impact:** Button always says "Run Full Pipeline" (because that's what it does).

---

#### Change 5: Make Success Message Match CLI

**File:** `streamlit_app/dashboard.py`  
**Lines:** 630-634

**Before:**
```python
final_trades_count = len(results.get('final_trades', pd.DataFrame()))
if final_trades_count > 0:
    st.success(f"✅ Full pipeline completed. {final_trades_count} final trades selected.")
else:
    st.warning(f"⚠️ Pipeline completed but 0 trades selected. See diagnostic funnel below.")
```

**After:**
```python
final_trades_count = len(results.get('final_trades', pd.DataFrame()))
if final_trades_count > 0:
    st.success(f"✅ {final_trades_count} CANDIDATES FOUND")
else:
    st.warning(f"⚠️ NO CANDIDATES FOUND")

# Show execution confirmation
st.info(f"Pipeline executed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
```

**Impact:** Success message matches CLI ("N CANDIDATES FOUND" instead of "Full pipeline completed").

---

## Section 9: Validation Checklist

After applying fixes, validate execution equivalence:

### Test 1: Explicit Path (CLI vs Dashboard)

**CLI:**
```bash
python scan_live.py data/snapshots/ivhv_snapshot_live_20260102_124337.csv
```

**Dashboard:**
1. Enter path: `data/snapshots/ivhv_snapshot_live_20260102_124337.csv`
2. Click "▶️ Run Full Pipeline"

**Expected:**
- ✅ Both produce identical `final_trades` count
- ✅ Both produce identical intermediate step outputs
- ✅ Both show same success/failure messages

---

### Test 2: No Snapshot Path (CLI vs Dashboard)

**CLI:**
```bash
python scan_live.py
# Should auto-resolve or fail hard
```

**Dashboard:**
1. Leave path empty
2. Click "▶️ Run Full Pipeline"

**Expected:**
- ✅ Both fail with identical error messages
- ✅ Dashboard does NOT auto-resolve (after fix)
- ✅ Dashboard does NOT fall back to Live Mode (after fix)

---

### Test 3: Missing Snapshot (CLI vs Dashboard)

**CLI:**
```bash
python scan_live.py data/snapshots/DOES_NOT_EXIST.csv
# Should exit with "❌ Snapshot not found"
```

**Dashboard:**
1. Enter path: `data/snapshots/DOES_NOT_EXIST.csv`
2. Click "▶️ Run Full Pipeline"

**Expected:**
- ✅ Both fail with identical error messages
- ✅ Dashboard shows error (not success)
- ✅ Dashboard does NOT fall back to auto-resolve (after fix)

---

## Section 10: Post-Fix Architecture

### Single Code Path Guarantee

**Before Fix:**
```
Dashboard Entry
    ├── Live Mode → load_ivhv_snapshot() → Step 2 only
    └── Legacy Mode → run_full_scan_pipeline() → Steps 2-12
```

**After Fix:**
```
Dashboard Entry
    └── run_full_scan_pipeline() → Steps 2-12 (ALWAYS)
```

### Execution Equivalence Matrix

| Aspect | CLI | Dashboard (After Fix) | Equivalent? |
|--------|-----|----------------------|-------------|
| Entry point | `run_full_scan_pipeline()` | `run_full_scan_pipeline()` | ✅ YES |
| Snapshot resolution | Explicit path required | Explicit path required | ✅ YES |
| Execution modes | Single (full pipeline) | Single (full pipeline) | ✅ YES |
| Results contract | 12-key dict | 12-key dict | ✅ YES |
| Failure mode | Hard fail | `st.stop()` (acceptable) | ✅ YES |
| Success reporting | "N CANDIDATES FOUND" | "N CANDIDATES FOUND" | ✅ YES |

---

## Appendix A: Full Divergence Summary

### Execution Path Divergence

1. **Dual Modes** → Dashboard has Live Mode (Step 2 only) + Legacy Mode (full pipeline)
2. **Conditional Entry Points** → `load_ivhv_snapshot()` vs `run_full_scan_pipeline()`
3. **Auto-Resolve Fallback** → Dashboard auto-resolves snapshot path, CLI requires explicit path
4. **Soft Failures** → Dashboard uses `st.stop()`, CLI uses `sys.exit(1)`

### Success Reporting Divergence

1. **Live Mode** → Says "✅ Loaded N tickers" when only Step 2 ran
2. **Legacy Mode** → Says "✅ Full pipeline completed" (truthful)
3. **CLI** → Says "✅ N CANDIDATES FOUND" (specific outcome)

### Contract Adherence Divergence

1. **Live Mode** → Returns 1-key dict (`'snapshot'` only)
2. **Legacy Mode** → Returns 12-key dict (full contract)
3. **CLI** → Returns 12-key dict (full contract)

---

## Appendix B: Code Diffs (Proposed)

### Diff 1: Remove Live Mode Checkbox

```diff
--- a/streamlit_app/dashboard.py
+++ b/streamlit_app/dashboard.py
@@ -440,15 +440,6 @@
     # Step 0 Integration Toggle (PROMINENT)
     st.divider()
-    col_toggle1, col_toggle2 = st.columns([1, 4])
-    with col_toggle1:
-        use_live_snapshot = st.checkbox(
-            "🔴 **LIVE MODE**",
-            value=False,
-            help="Use Live Schwab Snapshot from Step 0"
-        )
-    with col_toggle2:
-        if use_live_snapshot:
-            st.success("✅ **STEP 0 ACTIVE** - Will load latest Schwab snapshot (bypasses scraper & full pipeline)")
-        else:
-            st.info("ℹ️ Legacy mode - Uses data source from sidebar + runs full pipeline")
-    st.divider()
```

### Diff 2: Remove Live Mode Execution Branch

```diff
--- a/streamlit_app/dashboard.py
+++ b/streamlit_app/dashboard.py
@@ -486,60 +486,8 @@
     col1, col2 = st.columns([1, 3])
     with col1:
-        button_label = "▶️ Load Step 2 Data" if use_live_snapshot else "▶️ Run Full Pipeline"
+        button_label = "▶️ Run Full Pipeline"
         if st.button(button_label, type="primary", use_container_width=True):
-            try:
-                # BRIDGE MODE: Load Step 2 directly when live snapshot enabled
-                if use_live_snapshot:
-                    with st.spinner("📥 Loading live snapshot from Step 0..."):
-                        # Load Step 2 enriched data directly
-                        df_step2 = load_ivhv_snapshot(
-                            use_live_snapshot=True,
-                            skip_pattern_detection=True
-                        )
-                        
-                        # Create DataContext for provenance tracking
-                        data_ctx = DataContext(
-                            source='schwab_live',
-                            capture_timestamp=datetime.now(),
-                            is_persisted=False,
-                            snapshot_path=None
-                        )
-                        
-                        # Store in session state
-                        st.session_state['pipeline_results'] = {
-                            'snapshot': sanitize_for_arrow(df_step2)
-                        }
-                        st.session_state['live_snapshot_mode'] = True
-                        st.session_state['data_context'] = data_ctx
-                        
-                        # Show single source of truth banner
-                        st.info(data_ctx.get_banner())
-                        st.success(f"✅ Loaded {len(df_step2)} tickers from live Schwab data")
-                        
-                        # Validate IV/HV coverage
-                        # ... (50 lines of Live Mode code)
-                        
-                        # Show limitations
-                        st.warning(
-                            "⚠️ **Live Mode Limitations:**\n"
-                            "- Step 3+ not executed (analysis bypassed)\n"
-                            "- IV coverage may vary\n"
-                            "- Strategy evaluation not available\n"
-                            "- Data not persisted to disk (ephemeral)"
-                        )
-                else:
-                    # LEGACY MODE: Run full pipeline
-                    st.session_state['live_snapshot_mode'] = False
+            try:
                     with st.spinner("🚀 Running full scan pipeline (Steps 0-11)..."):
```

### Diff 3: Force Explicit Snapshot Path

```diff
--- a/streamlit_app/dashboard.py
+++ b/streamlit_app/dashboard.py
@@ -560,15 +560,12 @@
                 else:
-                    # Use latest snapshot from data/snapshots/
-                    snapshot_dir = Path("data/snapshots")
-                    if snapshot_dir.exists():
-                        snapshot_files = list(snapshot_dir.glob("ivhv_snapshot_*.csv"))
-                        if snapshot_files:
-                            latest_snapshot = max(snapshot_files, key=lambda p: p.stat().st_mtime)
-                            snapshot_path = str(latest_snapshot)
-                            data_source_type = 'step0_disk'
-                            st.info(f"Using latest snapshot: {latest_snapshot.name}")
-                        else:
-                            st.error("❌ No snapshot files found in data/snapshots/. Please run Step 0 or upload a snapshot.")
-                            st.stop()
-                    else:
-                        st.error("❌ data/snapshots/ directory not found. Please run Step 0 first.")
+                    # Force explicit snapshot path (no auto-resolve)
+                    st.error(
+                        "❌ No snapshot path provided.\n\n"
+                        "Please either:\n"
+                        "1. Upload a snapshot file, OR\n"
+                        "2. Enter an explicit path in the sidebar, OR\n"
+                        "3. Run scan_live.py from CLI instead"
+                    )
                         st.stop()
```

---

## Conclusion

**Current State:**  
Dashboard has dual execution modes (Live vs Legacy) with different code paths, different success messages, and different results contracts. This creates epistemic trust violation where user cannot be certain what code ran.

**Target State:**  
Dashboard ALWAYS calls `run_full_scan_pipeline()` with explicit snapshot path, matching CLI execution path exactly. No fallbacks, no mode switching, no silent degradation.

**Fix Priority:** 🔴 CRITICAL  
**Fix Effort:** 🟢 LOW (remove ~100 lines, add error message)  
**Fix Confidence:** 🟢 HIGH (simplifies architecture, reduces code paths)

**Next Steps:**
1. Apply diffs from Appendix B
2. Remove Live Mode entirely
3. Force explicit snapshot path
4. Validate with Test 1-3 from Section 9
5. Document execution equivalence guarantee

---

**End of Audit**
