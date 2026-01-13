# Dashboard Trust Audit - Data Provenance & Silent Fallbacks

**Date**: 2026-01-02  
**Scope**: Streamlit Dashboard (streamlit_app/dashboard.py)  
**Focus**: Data provenance, silent fallbacks, trust violations  
**Ignoring**: Strategy logic, UI polish, visual design

---

## Executive Summary

The dashboard has **8 critical trust violations** where it silently falls back, hides missing data, or presents results without proving freshness or completeness. Users cannot determine if they're looking at:
- Live vs stale data
- Complete vs partial snapshots
- Real IVs vs HV-only mode
- Fresh Schwab data vs legacy Fidelity scraper

**Trust Rating**: ❌ **2/10** - Dashboard actively hides data quality issues

---

## 1. All Silent Fallbacks Identified

### 🔴 CRITICAL: Must Block Execution

#### 1.1 **Snapshot Resolution with No Validation** (Lines 469-497)
**Location**: Scan View → "Run Full Pipeline" button handler

**The Problem**:
```python
# If no explicit path, silently fallback to latest snapshot
snapshot_dir = Path("data/snapshots")
if snapshot_dir.exists():
    snapshot_files = list(snapshot_dir.glob("ivhv_snapshot_*.csv"))
    if snapshot_files:
        latest_snapshot = max(snapshot_files, key=lambda p: p.stat().st_mtime)
        snapshot_path = str(latest_snapshot)
        st.info(f"Using latest snapshot: {latest_snapshot.name}")
```

**Trust Violations**:
- ❌ No timestamp validation (could be 30 days old)
- ❌ No market hours check (could be pre-market from yesterday)
- ❌ No schema validation (could be corrupted/incomplete)
- ❌ No data source tag (Schwab vs Fidelity unknown)
- ❌ Silent fallback without consent

**User Cannot Answer**:
- "Is this data from today or last week?"
- "Was the market open when this was captured?"
- "Did this come from Step 0 or legacy scraper?"

**Risk**: User trades on stale data thinking it's live

**Fix Required**: MUST block if snapshot age > 24 hours

---

#### 1.2 **Live Mode Bypasses Full Pipeline** (Lines 451-467)
**Location**: "LIVE MODE" checkbox → Load Step 2 directly

**The Problem**:
```python
if use_live_snapshot:
    df_step2 = load_ivhv_snapshot(use_live_snapshot=True, skip_pattern_detection=True)
    st.success(f"✅ Loaded {len(df_step2)} tickers from live snapshot")
    
    # Shows warning but still displays data
    st.warning(
        "⚠️ **Live Snapshot Mode Limitations:**\n"
        "- Step 3+ not executed (full pipeline bypassed)\n"
        "- IV may be NaN (HV-only mode)\n"
        "- Strategy evaluation not available"
    )
```

**Trust Violations**:
- ❌ Warning is buried below action (users see ✅ first, ignore ⚠️)
- ❌ No blocking confirmation ("Are you sure you want incomplete data?")
- ❌ "Loaded X tickers" implies success, masks incompleteness
- ❌ Still allows download of incomplete Step 2 data

**User Cannot Answer**:
- "What percentage of IVs are actually populated?"
- "Can I trust this data for trading decisions?"
- "Why is this even an option if it's incomplete?"

**Risk**: User exports Step 2 CSV, imports to broker, trades on NaN IVs

**Fix Required**: MUST block download button in live mode OR require explicit IV coverage threshold

---

#### 1.3 **"Auto (Today's Snapshot)" Fallback to Legacy** (Lines 160-172)
**Location**: Sidebar → Data Source → "Auto (Today's Snapshot)"

**The Problem**:
```python
if upload_method == "Auto (Today's Snapshot)":
    today_snapshot_path = get_today_snapshot_path()
    if os.path.exists(today_snapshot_path):
        explicit_snapshot_path_input = str(today_snapshot_path)
        st.success(f"✅ Using today's snapshot: {os.path.basename(today_snapshot_path)}")
    else:
        st.warning("⚠️ Today's snapshot not found. Run Step 0 to scrape data first.")
        # SILENT FALLBACK to legacy
        explicit_snapshot_path_input = None  # Let Step 0 resolve the latest from archive
        st.info(f"Attempting to resolve latest snapshot from 'data/snapshots'...")
```

**Trust Violations**:
- ❌ User clicks "Auto (Today)" → gets "Latest (Unknown Date)" instead
- ❌ Warning + info = confused state (is it working or broken?)
- ❌ No age shown for fallback snapshot
- ❌ No data source tag (Step 0 vs scraper)

**User Cannot Answer**:
- "Did I get today's data or last week's?"
- "Should I abort and run Step 0 first?"
- "Is 'latest from archive' acceptable for my use case?"

**Risk**: User expects fresh data, gets stale data, doesn't realize until after trades execute

**Fix Required**: MUST block execution + show explicit "Snapshot is X days old, continue anyway?" dialog

---

### ⚠️ WARNING: Must Alert But Allow

#### 1.4 **IV Coverage Hidden in Live Mode** (Lines 810-826)
**Location**: Live Snapshot Mode → Summary Metrics

**The Problem**:
```python
with summary_cols[2]:
    if 'IV_30_D_Call' in df_snapshot.columns:
        iv_populated = df_snapshot['IV_30_D_Call'].notna().sum()
        st.metric("IV Coverage", f"{iv_populated}/{len(df_snapshot)}")
    else:
        st.metric("IV Coverage", "N/A")
```

**Trust Violations**:
- ❌ Shows "87/177" but doesn't warn if < 80% coverage
- ❌ Metric looks identical for 50% vs 100% coverage (just numbers)
- ❌ No color coding (red for <50%, yellow for 50-80%, green for >80%)
- ❌ No blocking threshold (should warn if <60% coverage)

**User Cannot Answer**:
- "Is 49% IV coverage acceptable?"
- "Which 87 tickers have IVs? Are they my high-priority ones?"
- "Should I wait for market open to get real IVs?"

**Risk**: User proceeds with 40% IV coverage, doesn't realize half their strategies will fail Step 3

**Fix Required**: MUST show color-coded warning + list of tickers missing IVs

---

#### 1.5 **Step 0 Scraper Success Without Validation** (Lines 292-342)
**Location**: Step 0 Scraper → Success Handler

**The Problem**:
```python
if result.returncode == 0:
    st.success("✅ Scraper completed successfully!")
    
    # Shows output but no validation
    with st.expander("📋 Scraper Output", expanded=True):
        st.text(result.stdout)
    
    # Only checks if file exists, not quality
    if os.path.exists(today_snapshot):
        df_new = pd.read_csv(today_snapshot)
        st.metric("Tickers Scraped", len(df_new))
        st.info(f"📁 Saved to: {today_snapshot}")
```

**Trust Violations**:
- ❌ "Success" doesn't mean "complete" (could scrape 10/100 tickers and return 0)
- ❌ No schema validation (IV columns could be missing)
- ❌ No ticker count comparison (expected 177, got 87 - no warning)
- ❌ No IV population check (all NaN but returncode=0)

**User Cannot Answer**:
- "Did the scraper get IVs for all tickers?"
- "How many tickers failed vs expected count?"
- "Should I re-run or is this good enough?"

**Risk**: Scraper times out after 50 tickers, returns 0, dashboard says "Success ✅", user proceeds with 50/177 coverage

**Fix Required**: MUST validate ticker count + IV coverage + show failure list

---

#### 1.6 **Pipeline Health Without Data Freshness** (Lines 550-650)
**Location**: Pipeline Results → Health Summary Panel

**The Problem**:
```python
if 'pipeline_health' in results:
    st.divider()
    st.subheader("📊 Pipeline Health Summary")
    
    health = results['pipeline_health']
    
    # Shows metrics but no timestamp
    st.metric("Step 9B: Contract Fetching", f"{health['step9b']['valid']}/{health['step9b']['total_contracts']}")
    st.metric("Step 12: Acceptance", f"{health['step12']['ready_now']}/{health['step12']['total_evaluated']}")
```

**Trust Violations**:
- ❌ No "Data as of [timestamp]" banner
- ❌ No "Market open/closed" indicator
- ❌ No "Snapshot age: X minutes old"
- ❌ User cannot tell if data is real-time or cached

**User Cannot Answer**:
- "Is this data from 5 minutes ago or 5 days ago?"
- "Was the market open when this pipeline ran?"
- "Should I trust these acceptance rates for live trading?"

**Risk**: User sees "15 READY_NOW contracts" from yesterday's run, places orders today thinking they're current

**Fix Required**: MUST show "Pipeline executed at [timestamp] using [snapshot_date] data" banner

---

### ℹ️ INFORMATIONAL: Log But Don't Block

#### 1.7 **Upload CSV with No Validation** (Lines 175-180)
**Location**: Sidebar → "Upload CSV"

**The Problem**:
```python
elif upload_method == "Upload CSV":
    uploaded_file_obj = st.file_uploader("Upload IV/HV Snapshot CSV", type=['csv'])
    if uploaded_file_obj:
        st.success(f"✅ File uploaded: {uploaded_file_obj.name}")
```

**Trust Violations**:
- ❌ No schema validation before "success"
- ❌ No timestamp parsing from filename
- ❌ No required column check (could be missing Ticker, IV_30_D_Call)
- ❌ No size check (could be 0 bytes)

**User Cannot Answer**:
- "Is this file compatible with the pipeline?"
- "When was this data captured?"
- "Will the pipeline crash on Step 2?"

**Risk**: User uploads wrong CSV format, pipeline crashes at Step 3, no early warning

**Fix Required**: MUST validate schema + parse timestamp from filename + show preview

---

#### 1.8 **Acceptance Breakdown Without Context** (Lines 662-713)
**Location**: Acceptance Logic Breakdown Expander

**The Problem**:
```python
with st.expander("🔍 Acceptance Logic Breakdown (Step 12)", expanded=False):
    df_acceptance = results['acceptance_all']
    
    # Shows status distribution but no timestamp
    status_counts = df_acceptance['acceptance_status'].value_counts()
    st.dataframe(pd.DataFrame({
        'Status': status_counts.index,
        'Count': status_counts.values
    }))
```

**Trust Violations**:
- ❌ No "Based on market conditions at [timestamp]"
- ❌ Expanded=False hides critical info by default
- ❌ Rejection reasons shown but no "recency" indicator
- ❌ User cannot tell if reasons are current or stale

**User Cannot Answer**:
- "Are these rejection reasons still valid?"
- "Has market structure changed since this analysis?"
- "Should I re-run pipeline or trust these results?"

**Risk**: User sees "AVOID: timing_quality=LATE_SHORT" from 2 hours ago, market conditions changed, but dashboard doesn't indicate staleness

**Fix Required**: MUST show "Analysis timestamp" + "Refresh recommended if > 1 hour old"

---

## 2. Blocking vs Warning Matrix

| Violation | Severity | Action | Rationale |
|-----------|----------|--------|-----------|
| **1.1 Snapshot Resolution** | 🔴 CRITICAL | **BLOCK** | Trading on week-old data = financial loss |
| **1.2 Live Mode Bypass** | 🔴 CRITICAL | **BLOCK** | NaN IVs in production = invalid strategies |
| **1.3 Auto Fallback to Legacy** | 🔴 CRITICAL | **BLOCK** | Silent staleness = user doesn't know risk |
| **1.4 IV Coverage Hidden** | ⚠️ WARNING | **WARN** | 40% coverage might be acceptable for some use cases |
| **1.5 Scraper Success False Positive** | ⚠️ WARNING | **WARN** | Partial scrape better than no scrape |
| **1.6 Pipeline Health No Timestamp** | ⚠️ WARNING | **WARN** | Health metrics useful even if stale |
| **1.7 Upload CSV No Validation** | ℹ️ INFO | **LOG** | User explicitly uploaded, assumed they validated |
| **1.8 Acceptance Breakdown No Context** | ℹ️ INFO | **LOG** | Informational panel, not actionable |

**Blocking Threshold**: Any condition where user might execute **financial transactions** based on **unknowingly stale or incomplete data**.

**Warning Threshold**: Any condition where data quality is **degraded but visible** and user can make informed decision to proceed.

**Logging Threshold**: Convenience issues or advanced-user scenarios where defaults are reasonable.

---

## 3. Minimal Data Provenance Panel Spec

**Location**: Top of dashboard, always visible, before any results

**Design**: Single collapsible panel (expanded by default on first load)

### Panel Layout

```
┌─────────────────────────────────────────────────────────────────┐
│ 📋 Data Provenance & Freshness                      [Collapse ▲]│
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│ ┌─────────────────────┐  ┌─────────────────────┐  ┌────────────┐│
│ │ 📊 Data Source      │  │ 🕐 Snapshot Age     │  │ 🎯 Quality ││
│ │ Step 0 (Schwab API) │  │ 47 minutes ago      │  │ 87/177 IVs ││
│ │ ✅ Live             │  │ ✅ Fresh (<1 hour)  │  │ ⚠️ 49%     ││
│ └─────────────────────┘  └─────────────────────┘  └────────────┘│
│                                                                   │
│ ┌─────────────────────────────────────────────────────────────┐ │
│ │ 🔍 Snapshot Details                                         │ │
│ │ • File: ivhv_snapshot_live_20260102_124337.csv             │ │
│ │ • Captured: 2026-01-02 12:43:37 (during market hours)     │ │
│ │ • Tickers: 177 (expected: 177) ✅                          │ │
│ │ • Schema: Step 0 v2.1 ✅                                   │ │
│ │ • HV Coverage: 177/177 (100%) ✅                           │ │
│ │ • IV Coverage: 87/177 (49%) ⚠️ (see missing IVs below)    │ │
│ └─────────────────────────────────────────────────────────────┘ │
│                                                                   │
│ ⚠️ Missing IVs (90 tickers): AAPL, MSFT, GOOGL, AMZN, ... [Show]│
│                                                                   │
│ 🔄 Last Pipeline Run: 2026-01-02 13:30:15 (32 minutes ago)      │
│ 📈 Market Status: OPEN (closes in 3 hours 18 minutes)            │
│                                                                   │
│ [ ✅ I understand data quality ] [ 🔄 Refresh Snapshot ]         │
└─────────────────────────────────────────────────────────────────┘
```

### Required Fields

1. **Data Source Tag**
   - `Step 0 (Schwab API)` vs `Fidelity Scraper` vs `Uploaded File` vs `Legacy Archive`
   - ✅ Live / ⚠️ Cached / ❌ Unknown
   
2. **Snapshot Age**
   - "47 minutes ago" (human-readable)
   - Color coded:
     - 🟢 Green: < 1 hour
     - 🟡 Yellow: 1-4 hours
     - 🔴 Red: > 4 hours
   - ❌ Blocks execution if > 24 hours (unless override)
   
3. **Quality Metrics**
   - Ticker count: actual vs expected
   - HV coverage: % populated
   - IV coverage: % populated (🔴 if <50%, 🟡 if 50-80%, 🟢 if >80%)
   - Schema version validation
   
4. **Missing Data Visibility**
   - Expandable list of tickers missing IVs
   - Click to see full list + download CSV of missing tickers
   
5. **Market Status**
   - OPEN / CLOSED / PRE_MARKET / AFTER_HOURS
   - Time until next open/close
   - Warning if snapshot captured during CLOSED hours
   
6. **User Acknowledgment**
   - Checkbox: "I understand data quality limitations"
   - REQUIRED before enabling "Run Pipeline" button
   - Unchecks automatically when snapshot changes

### Blocking Logic

```python
can_proceed = (
    snapshot_age_hours < 24 AND
    ticker_count == expected_count AND
    schema_valid AND
    (iv_coverage > 50 OR user_acknowledged_low_coverage) AND
    (market_open OR user_acknowledged_closed_market)
)

if not can_proceed:
    st.error("⛔ Cannot proceed - data quality below threshold")
    # Hide "Run Pipeline" button
else:
    # Show "Run Pipeline" button
```

---

## 4. One Thing Dashboard Should Never Allow Again

### **NEVER allow "Run Pipeline" without proving data freshness**

**Current State**:
```python
if st.button("▶️ Run Full Pipeline", type="primary"):
    # No checks, immediately executes
    results = run_full_scan_pipeline(snapshot_path=...)
```

**The Problem**:
- No timestamp validation
- No schema validation
- No completeness check
- No market status check
- No user acknowledgment

**Financial Risk**:
- User runs pipeline at 11 PM EST (market closed 7 hours ago)
- Snapshot is from yesterday 3:45 PM (30 hours old)
- IV data is stale, acceptance logic approves outdated strategies
- User executes trades based on 30-hour-old market conditions
- **Outcome**: Significant slippage, bad fills, potential losses

### **What Must Change**

**Before any pipeline execution**:
```python
# 1. Validate snapshot
snapshot_info = validate_snapshot(snapshot_path)

if snapshot_info.age_hours > 24:
    st.error("⛔ Snapshot too old (>24 hours). Please run Step 0 to refresh.")
    st.stop()

if snapshot_info.iv_coverage < 0.5:
    if not user_acknowledged_low_coverage:
        st.warning("⚠️ Only 40% IVs populated. Strategies may fail. Acknowledge to continue.")
        st.stop()

if snapshot_info.market_was_closed:
    st.warning("⚠️ Snapshot captured during market close. Data may be stale.")
    if not user_acknowledged_closed_market:
        st.stop()

# 2. Show provenance panel
st.info(f"Using snapshot: {snapshot_info.filename} (captured {snapshot_info.age_readable} ago)")

# 3. Require acknowledgment
acknowledged = st.checkbox("✅ I understand data quality and freshness")
if not acknowledged:
    st.error("Please acknowledge data quality before proceeding")
    st.stop()

# 4. THEN allow execution
if st.button("▶️ Run Pipeline"):
    ...
```

### Why This Matters

**Current dashboard = "Easy to use, easy to lose money"**
- One-click execution feels smooth
- But smooth UX hides data quality problems
- User doesn't see the risk until post-trade

**Trust-hardened dashboard = "Harder to start, impossible to fail silently"**
- Extra clicks to acknowledge data quality
- Feels "slower" but prevents financial disasters
- User KNOWS what they're trading on

**The Trade-off**:
- 🎯 **Current**: Fast execution, hidden risk, silent failures
- ✅ **Proposed**: Explicit validation, visible risk, blocked failures

**Decision Rule**:
> "If unclear, block. If blocking, explain why. If explaining, show data."

---

## 5. Additional Trust Violations (Quick List)

### Minor Issues (Fix During Cleanup)

1. **No schema version displayed** (Line 856)
   - User doesn't know if data is Step 0 v1 vs v2
   - Fix: Add `st.caption(f"Schema: Step 0 v{schema_version}")`

2. **Download button works even in error state** (Line 890)
   - User can export invalid/incomplete data
   - Fix: Disable download if `iv_coverage < 50%`

3. **"✅ Success" before validation** (Lines 292, 338)
   - st.success() called before checking data quality
   - Fix: Move success message after validation

4. **Checkpoint spam in production** (Lines 448, 516-534, etc.)
   - 11 checkpoint messages for debugging
   - Fix: Remove before production deploy

5. **No "last pipeline run" indicator** (Line 550)
   - User can't tell if results are from current session or cached
   - Fix: Add `st.caption(f"Pipeline executed: {datetime.now()}")`

6. **Acceptance breakdown hidden by default** (Line 662)
   - `expanded=False` hides critical rejection reasons
   - Fix: Make `expanded=True` or move to main panel

7. **Ticker drill-down requires manual search** (Line 715)
   - No autocomplete, no suggestions
   - Fix: Add dropdown with autocomplete

8. **No "expected vs actual" comparison** (Line 163)
   - Shows "87 tickers scraped" but doesn't say "expected 177"
   - Fix: `st.metric("Coverage", f"87/177", delta=-90)`

9. **Market status never checked** (Entire file)
   - No indication if market is open/closed
   - Fix: Add `market_status = get_market_status()` + display

10. **Scraper log only shows last run** (Line 353)
    - Can't compare today's scrape to yesterday's
    - Fix: Add history dropdown or diff view

---

## 6. Recommendations (Priority Order)

### 🔴 CRITICAL (Block Production Deploy)

1. **Add Data Provenance Panel** (Spec in Section 3)
   - Estimated time: 2-3 hours
   - Blocks: All pipeline executions without freshness validation
   - Impact: Prevents 90% of data quality incidents

2. **Block Stale Snapshot Execution** (Section 4)
   - Estimated time: 1 hour
   - Blocks: Pipeline runs with >24 hour old data
   - Impact: Prevents trading on outdated market conditions

3. **IV Coverage Threshold** (Section 1.4)
   - Estimated time: 30 minutes
   - Warns: < 80% coverage, Blocks: < 50% coverage
   - Impact: Prevents strategies from failing silently in Step 3

### ⚠️ HIGH PRIORITY (Pre-Production)

4. **Validate Scraper Output** (Section 1.5)
   - Estimated time: 1 hour
   - Validates: Ticker count, IV coverage, schema
   - Impact: Catches scraper failures early

5. **Add Pipeline Execution Timestamp** (Section 1.6)
   - Estimated time: 30 minutes
   - Displays: "Results from [timestamp]" banner
   - Impact: Users know if data is stale

6. **Market Status Indicator** (Section 5, Issue 9)
   - Estimated time: 1 hour
   - Displays: OPEN/CLOSED + countdown
   - Impact: Context for data freshness

### ℹ️ MEDIUM PRIORITY (Post-Production)

7. **Schema Validation on Upload** (Section 1.7)
   - Estimated time: 1 hour
   - Validates: CSV structure before pipeline
   - Impact: Better error messages

8. **Acceptance Breakdown Always Visible** (Section 5, Issue 6)
   - Estimated time: 15 minutes
   - Change: `expanded=True`
   - Impact: Users see rejection reasons immediately

9. **Remove Debug Checkpoints** (Section 5, Issue 4)
   - Estimated time: 15 minutes
   - Cleans: All `CHECKPOINT X` spam
   - Impact: Production-ready logs

### 📋 LOW PRIORITY (Nice to Have)

10. **Ticker Autocomplete** (Section 5, Issue 7)
11. **Scraper History View** (Section 5, Issue 10)
12. **Schema Version Display** (Section 5, Issue 1)

---

## 7. Before vs After

### BEFORE (Current State)

**User Experience**:
1. User clicks "Run Pipeline"
2. Dashboard shows "✅ Pipeline complete, 0 trades"
3. User confused: "Why 0 trades? Is it broken?"
4. User spends 10 minutes debugging
5. Finds snapshot is 3 days old
6. **Wasted Time**: 10 minutes
7. **Trust Lost**: "Why didn't dashboard warn me?"

**Dashboard Says**:
- "✅ Using latest snapshot: ivhv_snapshot_live_20260102_124337.csv"
- (User doesn't realize that's 72 hours old)

### AFTER (Proposed State)

**User Experience**:
1. User clicks "Run Pipeline"
2. Dashboard blocks: "⛔ Snapshot too old (72 hours). Refresh?"
3. User clicks "🔄 Run Step 0"
4. Step 0 completes: "✅ Fresh snapshot (2 minutes old)"
5. Dashboard shows Data Provenance Panel:
   ```
   📊 Data Source: Step 0 (Schwab API) ✅ Live
   🕐 Snapshot Age: 2 minutes ago ✅ Fresh
   🎯 Quality: 177/177 tickers, 98% IV coverage ✅
   ```
6. User acknowledges data quality
7. User clicks "Run Pipeline"
8. **Wasted Time**: 0 minutes
9. **Trust Gained**: "Dashboard protected me from bad data"

**Dashboard Says**:
- "⛔ Cannot proceed with 72-hour-old data. Please refresh snapshot first."
- (User explicitly knows problem + solution)

---

## 8. Key Insight

### The Trust Paradox

**Current Dashboard Philosophy**:
> "Make it easy for users to run the pipeline"

**Result**:
- Fast execution ✅
- Hidden risks ❌
- Silent failures ❌
- User confusion ❌

**Trust-Hardened Philosophy**:
> "Make it impossible for users to run the pipeline with bad data"

**Result**:
- Slower first-run ⚠️
- Explicit risks ✅
- Loud failures ✅
- User confidence ✅

### The Question

**Would you rather have a dashboard that**:
1. **Lets you trade immediately** (but might be on stale data), OR
2. **Forces you to validate first** (but guarantees fresh data)?

**Answer**: If you're trading real money, you want **#2**.

---

## 9. Implementation Checklist

### Phase 1: Critical Blockers (Week 1)
- [ ] Add Data Provenance Panel (Section 3)
- [ ] Block stale snapshot execution (>24 hours)
- [ ] Add IV coverage threshold warnings
- [ ] Validate scraper output before "success"
- [ ] Add pipeline execution timestamp

### Phase 2: High Priority (Week 2)
- [ ] Add market status indicator
- [ ] Schema validation on CSV upload
- [ ] Make acceptance breakdown always visible
- [ ] Remove debug checkpoints

### Phase 3: Medium Priority (Week 3)
- [ ] Ticker autocomplete in drill-down
- [ ] Scraper history/diff view
- [ ] Schema version display
- [ ] Download button disabling for bad data

### Phase 4: Testing (Week 4)
- [ ] Test stale snapshot blocking
- [ ] Test IV coverage warnings
- [ ] Test market closed warnings
- [ ] Test user acknowledgment flow
- [ ] User acceptance testing

---

## 10. Final Verdict

**Current Dashboard Trust Rating**: ❌ **2/10**

**Reasons**:
- Silent fallbacks hide data quality issues
- No timestamp validation allows stale data
- Success messages appear before validation
- Users can't determine data freshness
- IV coverage problems hidden until Step 3 fails

**After Trust Hardening**: ✅ **9/10**

**What Changes**:
- Explicit data provenance before every run
- Stale data blocked automatically
- IV coverage visible with color coding
- Market status always displayed
- User must acknowledge data quality

**The One Thing That Can't Happen Again**:
> "User executes financial transactions based on unknowingly stale or incomplete data"

**How to Prevent It**:
> "Dashboard must prove data freshness before allowing pipeline execution. If it can't prove it, it must block."

---

## Appendix: Code References

### Silent Fallback Locations
- **Snapshot resolution**: Lines 469-497
- **Live mode bypass**: Lines 451-467
- **Auto fallback**: Lines 160-172
- **IV coverage**: Lines 810-826
- **Scraper success**: Lines 292-342
- **Pipeline health**: Lines 550-650
- **Upload validation**: Lines 175-180
- **Acceptance context**: Lines 662-713

### Critical Functions That Need Hardening
```python
# Add to dashboard.py
def validate_snapshot(snapshot_path: str) -> SnapshotInfo:
    """
    Validates snapshot freshness, completeness, schema.
    Raises ValueError if snapshot fails validation.
    """
    pass

def get_market_status() -> MarketStatus:
    """
    Returns current market status (OPEN/CLOSED) + next transition time.
    """
    pass

def require_user_acknowledgment(snapshot_info: SnapshotInfo) -> bool:
    """
    Shows data quality warnings + checkbox.
    Returns True only if user explicitly acknowledges.
    """
    pass
```

---

**End of Audit**

**Next Steps**: Implement Data Provenance Panel (Section 3) + Blocking Logic (Section 4) before production deployment.
