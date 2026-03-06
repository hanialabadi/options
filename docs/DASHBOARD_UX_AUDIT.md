# Dashboard UI/UX Audit & Improvement Plan

**Date:** 2026-02-04
**Status:** 🔄 IN PROGRESS
**Priority:** HIGH (User-Facing Usability)

---

## Executive Summary

The dashboard has strong technical foundations but has critical usability gaps that create friction for users. This audit identifies 8 major issues and provides actionable improvements.

**Critical Issues:**
1. ❌ **Cannot fetch data from dashboard** - Users must drop to CLI
2. ⚠️ **Unclear why scan button is disabled** - No error messaging
3. ⚠️ **Stale data not auto-detected** - Silent failures
4. ⚠️ **Complex terminology without context** - Steep learning curve
5. ⚠️ **No data status indicator** - Users don't know if data is fresh

---

## Issue 1: Cannot Fetch Data from Dashboard ❌ CRITICAL

**Current State:**
- Scan button disabled when data is stale
- Only shows passive message: "To fetch fresh data, use CLI: ..."
- Forces user to exit dashboard, run CLI command, return to dashboard

**User Impact:** HIGH
- Breaks workflow continuity
- Requires terminal knowledge
- Adds 3-5 minutes to scan process

**Fix:**
Add a "Fetch Fresh Data" button that runs the data collection directly from the dashboard.

**Implementation:**
```python
# In scan_view.py, add before scan button:

if execution_blocked and block_reason.startswith("Snapshot is older"):
    st.error(f"🛑 **DATA IS STALE:** {block_reason}")

    col_fix1, col_fix2 = st.columns([1, 2])
    with col_fix1:
        if st.button("🔄 Fetch Fresh Data", type="primary", key="fetch_data_btn"):
            with st.status("Fetching fresh market data from Schwab...", expanded=True) as status:
                try:
                    from scan_engine.step0_schwab_snapshot import main as fetch_snapshot
                    fetch_snapshot(["--fetch-iv"])
                    status.update(label="✅ Fresh data fetched!", state="complete")
                    st.success("Data is now fresh. Please click 'Run Full Pipeline' below.")
                    st.rerun()
                except Exception as e:
                    status.update(label="❌ Fetch failed", state="error")
                    st.error(f"Error: {e}")

    with col_fix2:
        st.info("💡 **One-Click Fix:** Click the button to fetch fresh data automatically.")
```

**Priority:** CRITICAL (blocks core functionality)

---

## Issue 2: Unclear Why Scan Button is Disabled ⚠️

**Current State:**
- Button shows `disabled=True` with no tooltip
- Error message is below in a different section
- Users don't immediately understand why they can't scan

**User Impact:** MEDIUM
- Confusion and frustration
- Users may think dashboard is broken

**Fix:**
Add tooltip to disabled button explaining why it's disabled.

**Implementation:**
```python
# In scan_view.py line 522:

button_help_text = None
if execution_blocked:
    button_help_text = f"Cannot scan: {block_reason}"

if st.button(
    "▶️ Run Full Pipeline",
    type="primary",
    width='stretch',
    disabled=execution_blocked or st.session_state.pipeline_running,
    help=button_help_text  # Add this
):
```

**Priority:** HIGH (usability)

---

## Issue 3: Stale Data Not Auto-Detected ⚠️

**Current State:**
- Dashboard loads without checking data freshness first
- User must navigate to Scan view to see data status
- No global indicator of data health

**User Impact:** MEDIUM
- Users may operate on stale data unknowingly
- No proactive warnings

**Fix:**
Add a global data status indicator at the top of every page.

**Implementation:**
```python
# In dashboard.py, after page config:

def check_data_freshness():
    """Check if snapshot data is fresh (<24h)."""
    from scan_engine.step0_resolve_snapshot import resolve_snapshot_path

    try:
        snapshot_path = resolve_snapshot_path()
        mod_time = datetime.fromtimestamp(os.path.getmtime(snapshot_path))
        age_hours = (datetime.now() - mod_time).total_seconds() / 3600

        return {
            'is_fresh': age_hours < 24,
            'age_hours': age_hours,
            'path': snapshot_path,
            'mod_time': mod_time
        }
    except:
        return None

# Add global status banner
data_status = check_data_freshness()
if data_status:
    if not data_status['is_fresh']:
        st.warning(f"⚠️ **Data is {data_status['age_hours']:.1f}h old** - Consider fetching fresh data before scanning")
```

**Priority:** MEDIUM (proactive UX)

---

## Issue 4: Complex Terminology Without Context ⚠️

**Current State:**
- Terms like "IV Maturity State", "PCS Score", "Expression Tier" used without explanation
- No tooltips or help icons
- Steep learning curve for new users

**User Impact:** HIGH
- Users don't understand what they're looking at
- Reduces confidence in system

**Fix:**
Add contextual help throughout the dashboard.

**Implementation:**
```python
# Use st.help or info icons next to terms:

st.metric("PCS Score", 85.2, help="Probability of Capital Success - higher is better (0-100)")
st.metric("Expression Tier", "CORE", help="CORE = High liquidity, full size OK | STANDARD = Normal | NICHE = Limited size")

# Add glossary section
with st.expander("📖 Glossary"):
    st.markdown("""
    **PCS Score:** Probability of Capital Success (0-100). Combines liquidity, Greeks, and timing.
    **IV Rank:** Where current IV sits vs 1-year range (0-100 percentile).
    **Expression Tier:** Position sizing guidance (CORE/STANDARD/NICHE).
    **Execution Status:** READY (trade now) | AWAIT_CONFIRMATION (waiting) | BLOCKED (rejected).
    """)
```

**Priority:** HIGH (comprehension)

---

## Issue 5: No Clear Data Status Indicator ⚠️

**Current State:**
- Data freshness buried in "Data Plan" section
- No at-a-glance status indicator
- Users don't know if they're working with live data

**User Impact:** MEDIUM
- Uncertainty about data quality
- May make decisions on stale data

**Fix:**
Add prominent data status badge at top of scan view.

**Implementation:**
```python
# At top of scan_view, before everything:

def render_data_status_badge(info):
    """Render prominent data status badge."""
    if not info:
        st.error("🔴 **NO DATA** - Please fetch snapshot first")
        return

    age_hours = (datetime.now() - info['timestamp']).total_seconds() / 3600

    if age_hours < 4:
        st.success(f"🟢 **LIVE DATA** - {info['timestamp'].strftime('%I:%M %p')} ({age_hours:.1f}h ago)")
    elif age_hours < 24:
        st.info(f"🟡 **RECENT DATA** - {info['timestamp'].strftime('%Y-%m-%d %I:%M %p')} ({age_hours:.1f}h ago)")
    else:
        st.error(f"🔴 **STALE DATA** - {info['timestamp'].strftime('%Y-%m-%d %I:%M %p')} ({age_hours:.0f}h ago)")

# Call at top of render_scan_view
render_data_status_badge(info)
```

**Priority:** MEDIUM (awareness)

---

## Issue 6: Confusing Multi-Step Data Fetching ⚠️

**Current State:**
- "Data Plan" section shows what WILL happen
- "Data Provenance Summary" shows what DID happen
- Two similar sections create confusion

**User Impact:** LOW-MEDIUM
- Cognitive overhead
- Users don't know which section to trust

**Fix:**
Combine into a single "Data Status" section with before/after tabs.

**Implementation:**
```python
# Replace both sections with:

st.header("📊 Data Status")
tab_before, tab_after = st.tabs(["📋 Pre-Scan Plan", "✅ Post-Scan Results"])

with tab_before:
    # Show what will happen (current "Data Plan" content)
    pass

with tab_after:
    if 'pipeline_results' in st.session_state:
        # Show what did happen (current "Provenance Summary" content)
        pass
    else:
        st.info("Run scan to see actual data sources used")
```

**Priority:** LOW (polish)

---

## Issue 7: No Progress Indicator for Long Operations ⚠️

**Current State:**
- Scan button just shows "disabled" while running
- No progress bar or step indicator
- Users don't know if scan is frozen or progressing

**User Impact:** MEDIUM
- Anxiety during long scans (2-5 minutes)
- May click button multiple times

**Fix:**
Add live progress tracking during scan.

**Implementation:**
```python
# Use st.progress and st.status with live updates:

with st.status("🚀 Executing Full Scan Pipeline...", expanded=True) as status:
    progress_bar = st.progress(0)
    status_text = st.empty()

    # Hook into pipeline progress (would need pipeline callback support)
    # For now, use placeholder steps:
    steps = ["Loading snapshot", "Filtering IVHV", "Computing signals",
             "Recommending strategies", "Fetching contracts", "Recalibrating PCS",
             "Applying acceptance gates", "Computing position sizing"]

    for i, step in enumerate(steps):
        status_text.text(f"Step {i+1}/{len(steps)}: {step}...")
        progress_bar.progress((i + 1) / len(steps))
        # Actual pipeline step execution here

    status.update(label="✅ Scan complete", state="complete")
```

**Priority:** MEDIUM (user comfort)

---

## Issue 8: Poor Mobile Responsiveness 📱

**Current State:**
- Dashboard uses `layout="wide"` with many columns
- Not optimized for tablet/mobile viewing
- Buttons and tables overflow on smaller screens

**User Impact:** LOW
- Cannot use dashboard on mobile effectively
- Limited to desktop use only

**Fix:**
Add responsive layout detection and adjust UI accordingly.

**Implementation:**
```python
# Add to dashboard.py:

# Detect screen size (Streamlit doesn't have native support, use workaround)
# For now, add a toggle in sidebar:

with st.sidebar:
    st.header("Display Options")
    compact_mode = st.checkbox("Compact Mode (Mobile-Friendly)", value=False)

    if compact_mode:
        # Use single columns instead of multi-column layouts
        # Stack metrics vertically
        # Reduce table widths
```

**Priority:** LOW (edge case)

---

## Proposed Improvements Summary

### Immediate Fixes (This PR)
1. ✅ Add "Fetch Fresh Data" button to dashboard
2. ✅ Add tooltip to disabled scan button explaining why
3. ✅ Add global data status badge
4. ✅ Add contextual help/tooltips for complex terms
5. ✅ Combine Data Plan and Provenance into single section

### Short-Term Improvements (Next PR)
6. Add live progress indicator during scans
7. Add glossary/help section
8. Improve error messages with actionable guidance

### Long-Term Enhancements (Future)
9. Add mobile-responsive layouts
10. Add keyboard shortcuts (Enter to scan, Esc to cancel)
11. Add dark mode support
12. Add export capabilities for all tables

---

## UI/UX Best Practices Applied

### ✅ Good Patterns Currently Used
1. **Consistent Navigation** - Back button always in same place
2. **Color Coding** - Green (READY), Yellow (WAIT), Red (BLOCKED)
3. **Clear Hierarchy** - Headers, subheaders, dividers used correctly
4. **Expandable Sections** - Complex info hidden in expanders
5. **Status Indicators** - Icons (✅ ❌ ⚠️) for quick scanning

### ❌ Anti-Patterns to Avoid
1. **Forcing CLI Usage** - Dashboard should be self-sufficient
2. **Silent Failures** - Always show why something failed
3. **Jargon Without Context** - Explain or link to docs
4. **Hidden State** - Always show current system state
5. **Misleading Placeholders** - Don't show disabled buttons without explanation

---

## Accessibility Considerations

### Current Issues
- ❌ No keyboard navigation support
- ❌ Poor screen reader support (no aria labels)
- ❌ Color-only status indicators (need icons too)
- ⚠️ Small text in some tables

### Recommended Fixes
- Add `help` parameter to all buttons
- Use icons + text (not just icons)
- Ensure all tables have headers
- Add alt text to status indicators

---

## Performance Considerations

### Current Issues
- ⚠️ Full page rerun on every button click (Streamlit limitation)
- ⚠️ Large DataFrames cause slow rendering
- ⚠️ No caching on expensive operations

### Recommended Optimizations
```python
@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_snapshot_info(path):
    # ... expensive operations ...
    pass

# Use st.dataframe with virtual scrolling for large tables
st.dataframe(df, height=400)  # Virtualized rendering
```

---

## Testing Checklist

### Functionality Tests
- [ ] Fetch data button works from dashboard
- [ ] Scan button tooltip shows correct reason when disabled
- [ ] Global data status updates after fetch
- [ ] All buttons are clickable when expected
- [ ] Progress indicators show during long operations

### Usability Tests
- [ ] New user can complete first scan without docs
- [ ] Error messages are actionable
- [ ] All jargon has tooltips or help text
- [ ] Data freshness is always visible
- [ ] User never needs to drop to CLI

### Edge Cases
- [ ] No snapshot exists at all
- [ ] Snapshot exists but is corrupted
- [ ] Network error during data fetch
- [ ] Pipeline crashes mid-scan
- [ ] Multiple users accessing simultaneously

---

## Implementation Plan

### Phase 1: Critical Fixes (This Session)
```bash
1. Add fetch data button to scan_view.py
2. Add tooltip to disabled scan button
3. Add global data status indicator
4. Add help text to complex terms
5. Test end-to-end flow
```

### Phase 2: Enhancements (Next Week)
```bash
1. Add progress tracking during scans
2. Add comprehensive glossary section
3. Improve error messaging throughout
4. Add export buttons for all tables
```

### Phase 3: Polish (Future)
```bash
1. Mobile-responsive layouts
2. Keyboard shortcuts
3. Dark mode support
4. Advanced filtering/sorting
```

---

## Success Metrics

### User Experience
- **Time to First Scan:** <2 minutes (currently ~5-10 minutes with CLI)
- **Error Resolution Time:** <30 seconds (currently unknown, requires CLI knowledge)
- **User Satisfaction:** 8+/10 (gather feedback after improvements)

### Technical
- **Page Load Time:** <2 seconds
- **Scan Execution Time:** <3 minutes for 200 tickers
- **Error Rate:** <5% of scan attempts

---

## References

- [Streamlit Best Practices](https://docs.streamlit.io/library/advanced-features/performance)
- [Nielsen Norman Group - Dashboard UX](https://www.nngroup.com/articles/dashboard-design/)
- [Material Design - Data Display](https://material.io/design/communication/data-visualization.html)

---

**Audit Completed by:** Claude (Dashboard UX Specialist)
**Date:** 2026-02-04
**Next Review:** After Phase 1 implementation
**Status:** 🔄 Implementing improvements now
