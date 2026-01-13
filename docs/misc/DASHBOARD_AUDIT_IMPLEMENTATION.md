# Dashboard Audit Implementation Guide

**Date:** 2026-01-02  
**Source:** DASHBOARD_EXECUTION_AUDIT.md  
**Priority:** P0 (Critical acceptance_ready fix required)

---

## Quick Summary

**Critical Finding:** Dashboard never displays `acceptance_ready` (the filtered READY_NOW + MEDIUM+ subset that Step 8 receives).

**Impact:** When "15 READY_NOW contracts" becomes "0 final trades", user cannot see that Step 12 filtered 15 → 3 MEDIUM+, creating blind spot.

**Fix Time:** 30 minutes for critical fix, 80 minutes for all 6 fixes

---

## Implementation Checklist

### P0: Critical (Must Implement Today)

- [ ] **Fix 1: Display acceptance_ready** (30 min, ~40 lines)
  - Location: After line 763 in dashboard.py
  - Add new section "🎯 Ready for Position Sizing (Step 12 → Step 8)"
  - Show metrics: Contracts Ready, HIGH confidence, MEDIUM confidence
  - Show data preview with columns: Ticker, Symbol, Strategy_Type, confidence_band, acceptance_reason
  - Handle empty case: show why 0 contracts ready

### P1: High Priority (Implement This Week)

- [ ] **Fix 2: Improve 0 trades diagnosis** (15 min, ~10 lines)
  - Location: Lines 623-632 in dashboard.py
  - Add acceptance_ready count check
  - Distinguish "Step 12 filtered to 0 MEDIUM+" vs "Step 8 filtered all"

### P2: Medium Priority (Next Week)

- [ ] **Fix 3: Add pipeline execution timestamp** (10 min, ~10 lines)
  - Location: Line 616 in dashboard.py
  - Add timestamp to Pipeline Health Summary header
  - Show data age from DataContext

- [ ] **Fix 4: Conditional success message** (5 min, ~5 lines)
  - Location: Line 611 in dashboard.py
  - Change: `st.success()` when 0 trades → `st.warning()`

- [ ] **Fix 5: Live mode IV/HV validation** (15 min, ~15 lines)
  - Location: After line 517 in dashboard.py
  - Add IV coverage % check (warn if < 50%)
  - Add HV coverage % check (warn if < 50%)

### P3: Low Priority (Nice to Have)

- [ ] **Fix 6: Empty acceptance message** (5 min, ~5 lines)
  - Location: Line 705 in dashboard.py
  - Show explicit message when acceptance_all is empty

---

## Fix 1: Display acceptance_ready (CRITICAL)

### Code Location
- **File:** `streamlit_app/dashboard.py`
- **Insert After:** Line 763 (end of acceptance_all expander)
- **Section:** "Acceptance Logic Breakdown"

### Current Code (Line 763)
```python
                                        st.caption(f"      {status}: {count}")
        
        # ============================================================
        # TICKER DRILL-DOWN (Priority 3)
        # ============================================================
```

### Code to Add
```python
                                        st.caption(f"      {status}: {count}")
        
        # ============================================================
        # READY FOR SIZING (Step 12 → Step 8)
        # ============================================================
        if 'acceptance_ready' in results and not results['acceptance_ready'].empty:
            st.divider()
            st.subheader("🎯 Ready for Position Sizing (Step 12 → Step 8)")
            df_ready = results['acceptance_ready']
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Contracts Ready", len(df_ready))
            with col2:
                if 'confidence_band' in df_ready.columns:
                    high_count = (df_ready['confidence_band'] == 'HIGH').sum()
                    st.metric("HIGH Confidence", high_count)
            with col3:
                if 'confidence_band' in df_ready.columns:
                    medium_count = (df_ready['confidence_band'] == 'MEDIUM').sum()
                    st.metric("MEDIUM Confidence", medium_count)
            
            st.caption("✅ These contracts passed Step 12 acceptance (READY_NOW + MEDIUM/HIGH confidence)")
            st.caption("➡️ Step 8 received these contracts for position sizing")
            
            display_cols = ['Ticker', 'Symbol', 'Strategy_Type', 'confidence_band', 'acceptance_reason']
            display_cols = [c for c in display_cols if c in df_ready.columns]
            st.dataframe(df_ready[display_cols], use_container_width=True, height=200)
        
        elif 'acceptance_ready' in results:
            st.divider()
            st.warning("⚠️ **0 contracts ready for sizing**")
            st.caption("Step 12 filtered all READY_NOW contracts to 0 MEDIUM+ confidence")
            st.caption("This is why Step 8 produced 0 final trades")
        
        # ============================================================
        # TICKER DRILL-DOWN (Priority 3)
        # ============================================================
```

### What This Fixes
**Before:**
- User sees "15 READY_NOW" in Pipeline Health
- User sees "0 final trades"
- User has NO IDEA what happened in between

**After:**
- User sees "15 READY_NOW" in Pipeline Health
- User sees "3 Contracts Ready" (MEDIUM+ confidence)
- User sees "0 final trades"
- User knows: Step 12 filtered 15 → 3, Step 8 filtered 3 → 0

### Testing
```bash
# Run pipeline with dashboard
python -m streamlit run streamlit_app/dashboard.py

# Steps:
1. Disable "🔴 LIVE MODE"
2. Select "Auto (Today's Snapshot)"
3. Click "▶️ Run Full Pipeline"
4. Scroll to "Acceptance Logic Breakdown"
5. Verify new section "🎯 Ready for Position Sizing" appears
6. Check metrics: Contracts Ready, HIGH Confidence, MEDIUM Confidence
7. Verify data table shows MEDIUM/HIGH contracts only
```

---

## Fix 2: Improve 0 Trades Diagnosis

### Code Location
- **File:** `streamlit_app/dashboard.py`
- **Lines:** 623-632
- **Section:** Pipeline Health Summary

### Current Code
```python
if health['step8']['final_trades'] == 0:
    if health['step9b']['valid'] == 0:
        st.error("⚠️ **0 trades: All contracts failed validation (Step 9B)**")
        st.caption("Likely cause: API issue, liquidity filters too strict, or market closed")
    elif health['step12']['ready_now'] == 0:
        st.warning("⚠️ **0 trades: All contracts rejected by acceptance logic (Step 12)**")
        st.caption("Market conditions don't match acceptance criteria (timing, structure, etc.)")
    else:
        st.info("ℹ️ **0 trades: Step 8 filtered all READY_NOW contracts**")
        st.caption("Position sizing or risk limits removed candidates")
```

### Code to Replace
```python
if health['step8']['final_trades'] == 0:
    if health['step9b']['valid'] == 0:
        st.error("⚠️ **0 trades: All contracts failed validation (Step 9B)**")
        st.caption("Likely cause: API issue, liquidity filters too strict, or market closed")
    elif health['step12']['ready_now'] == 0:
        st.warning("⚠️ **0 trades: All contracts rejected by acceptance logic (Step 12)**")
        st.caption("Market conditions don't match acceptance criteria (timing, structure, etc.)")
    else:
        # Check acceptance_ready to distinguish Step 12 vs Step 8 filtering
        acceptance_ready_count = len(results.get('acceptance_ready', pd.DataFrame()))
        if acceptance_ready_count == 0:
            st.warning("⚠️ **0 trades: Step 12 filtered all READY_NOW to 0 MEDIUM+ confidence**")
            st.caption(f"{health['step12']['ready_now']} READY_NOW contracts were LOW confidence (filtered before Step 8)")
        else:
            st.info(f"ℹ️ **0 trades: Step 8 filtered {acceptance_ready_count} MEDIUM+ contracts**")
            st.caption("Position sizing or risk limits removed all candidates")
```

### What This Fixes
**Before:** "Step 8 filtered all READY_NOW contracts" (ambiguous - did Step 8 see all 15 or filtered subset?)

**After:** 
- "Step 12 filtered all READY_NOW to 0 MEDIUM+" (user knows Step 8 received 0)
- OR "Step 8 filtered 3 MEDIUM+ contracts" (user knows Step 8 received 3)

---

## Fix 3: Add Pipeline Execution Timestamp

### Code Location
- **File:** `streamlit_app/dashboard.py`
- **Line:** 616 (before "Pipeline Health Summary")
- **Section:** Pipeline Health Summary header

### Current Code
```python
if 'pipeline_health' in results:
    st.divider()
    st.subheader("📊 Pipeline Health Summary")
    
    health = results['pipeline_health']
```

### Code to Replace
```python
if 'pipeline_health' in results:
    st.divider()
    
    # Add execution context
    col1, col2 = st.columns([3, 1])
    with col1:
        st.subheader("📊 Pipeline Health Summary")
    with col2:
        if 'data_context' in st.session_state:
            data_ctx = st.session_state['data_context']
            if data_ctx.capture_timestamp:
                from datetime import datetime
                age_str = _format_age(data_ctx.capture_timestamp)
                st.caption(f"📅 Data: {age_str}")
    
    st.caption(f"⏰ Executed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    health = results['pipeline_health']
```

### What This Fixes
- User knows when pipeline ran
- User knows age of underlying snapshot
- Prevents confusion from stale session state

---

## Fix 4: Conditional Success Message

### Code Location
- **File:** `streamlit_app/dashboard.py`
- **Line:** 611

### Current Code
```python
final_trades_count = len(results.get('final_trades', pd.DataFrame()))
st.success(f"✅ Full pipeline completed. {final_trades_count} final trades selected.")
```

### Code to Replace
```python
final_trades_count = len(results.get('final_trades', pd.DataFrame()))
if final_trades_count > 0:
    st.success(f"✅ Full pipeline completed. {final_trades_count} final trades selected.")
else:
    st.warning(f"⚠️ Pipeline completed but 0 trades selected. See diagnostic funnel below.")
```

---

## Fix 5: Live Mode IV/HV Validation

### Code Location
- **File:** `streamlit_app/dashboard.py`
- **Line:** 517 (after live snapshot load success)

### Current Code
```python
st.success(f"✅ Loaded {len(df_step2)} tickers from live Schwab data")

# Show limitations (not contradictions)
st.warning(
    "⚠️ **Live Mode Limitations:**\n"
    "- Step 3+ not executed (analysis bypassed)\n"
    "- IV coverage may vary\n"
    "- Strategy evaluation not available\n"
    "- Data not persisted to disk (ephemeral)"
)
```

### Code to Add (After Success Message)
```python
st.success(f"✅ Loaded {len(df_step2)} tickers from live Schwab data")

# Validate IV/HV coverage
if 'IV_30_D_Call' in df_step2.columns:
    iv_coverage_pct = df_step2['IV_30_D_Call'].notna().sum() / len(df_step2)
    iv_count = df_step2['IV_30_D_Call'].notna().sum()
    if iv_coverage_pct < 0.5:
        st.warning(f"⚠️ Low IV coverage: {iv_coverage_pct:.0%} ({iv_count}/{len(df_step2)} tickers)")
    else:
        st.info(f"✅ IV coverage: {iv_coverage_pct:.0%} ({iv_count}/{len(df_step2)} tickers)")

if 'HV_30_D_Cur' in df_step2.columns:
    hv_coverage_pct = df_step2['HV_30_D_Cur'].notna().sum() / len(df_step2)
    hv_count = df_step2['HV_30_D_Cur'].notna().sum()
    if hv_coverage_pct < 0.5:
        st.warning(f"⚠️ Low HV coverage: {hv_coverage_pct:.0%} ({hv_count}/{len(df_step2)} tickers)")
    else:
        st.info(f"✅ HV coverage: {hv_coverage_pct:.0%} ({hv_count}/{len(df_step2)} tickers)")

# Show limitations (not contradictions)
st.warning(
    "⚠️ **Live Mode Limitations:**\n"
    "- Step 3+ not executed (analysis bypassed)\n"
    "- IV coverage may vary\n"
    "- Strategy evaluation not available\n"
    "- Data not persisted to disk (ephemeral)"
)
```

---

## Fix 6: Empty Acceptance Message

### Code Location
- **File:** `streamlit_app/dashboard.py`
- **Line:** 705

### Current Code
```python
if 'acceptance_all' in results and not results['acceptance_all'].empty:
    st.divider()
    with st.expander("🔍 Acceptance Logic Breakdown (Step 12)", expanded=False):
        df_acceptance = results['acceptance_all']
        # ... existing code
```

### Code to Replace
```python
if 'acceptance_all' in results:
    st.divider()
    if not results['acceptance_all'].empty:
        with st.expander("🔍 Acceptance Logic Breakdown (Step 12)", expanded=False):
            df_acceptance = results['acceptance_all']
            # ... existing code
    else:
        st.info("ℹ️ No contracts to evaluate (Step 9B produced no valid contracts)")
```

---

## Validation Plan

### Test Scenario 1: Normal Pipeline (Some MEDIUM+ Contracts)
```bash
# Expected Result:
- Pipeline Health: 50 total → 30 valid → 15 READY_NOW → 0 final trades
- Acceptance Ready: Shows "3 Contracts Ready (2 MEDIUM, 1 HIGH)"
- 0 Trades Diagnosis: "Step 8 filtered 3 MEDIUM+ contracts" (position sizing too strict)
```

### Test Scenario 2: All LOW Confidence Contracts
```bash
# Expected Result:
- Pipeline Health: 50 total → 30 valid → 15 READY_NOW → 0 final trades
- Acceptance Ready: "⚠️ 0 contracts ready for sizing"
- 0 Trades Diagnosis: "Step 12 filtered all READY_NOW to 0 MEDIUM+" (15 were LOW confidence)
```

### Test Scenario 3: No Valid Contracts from Step 9B
```bash
# Expected Result:
- Pipeline Health: 50 total → 0 valid → 0 READY_NOW → 0 final trades
- Acceptance Breakdown: "ℹ️ No contracts to evaluate"
- 0 Trades Diagnosis: "All contracts failed validation (Step 9B)"
```

---

## Before vs After User Experience

### Scenario: 15 READY_NOW → 0 Final Trades

**BEFORE (Missing acceptance_ready):**
```
📊 Pipeline Health Summary
Step 12: Acceptance
  15/50 (30.0% accepted)
  ⏸️ 20 WAIT | ❌ 15 AVOID

Step 8: Position Sizing
  0/15 (0.0% converted)

ℹ️ 0 trades: Step 8 filtered all READY_NOW contracts
Position sizing or risk limits removed candidates

[USER QUESTION: Did Step 8 see all 15 or filtered subset?]
```

**AFTER (With acceptance_ready):**
```
📊 Pipeline Health Summary
Step 12: Acceptance
  15/50 (30.0% accepted)
  ⏸️ 20 WAIT | ❌ 15 AVOID

🎯 Ready for Position Sizing (Step 12 → Step 8)
Contracts Ready: 3
HIGH Confidence: 0
MEDIUM Confidence: 3

✅ These contracts passed Step 12 acceptance (READY_NOW + MEDIUM/HIGH confidence)
➡️ Step 8 received these contracts for position sizing

Step 8: Position Sizing
  0/3 (0.0% converted)

ℹ️ 0 trades: Step 8 filtered 3 MEDIUM+ contracts
Position sizing or risk limits removed all candidates

[USER KNOWS: 15 READY_NOW → 3 MEDIUM+ → 0 final trades]
[USER ACTION: Adjust position sizing parameters]
```

---

## Implementation Priority

1. **Today (P0):** Implement Fix 1 (acceptance_ready display) - 30 minutes
2. **This Week (P1):** Implement Fix 2 (0 trades diagnosis) - 15 minutes
3. **Next Week (P2):** Implement Fixes 3, 4, 5 - 30 minutes total
4. **Optional (P3):** Implement Fix 6 - 5 minutes

**Total Time:** 80 minutes for complete implementation

---

**Implementation Guide Complete** ✅
