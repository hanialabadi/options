# STRUCTURALLY_READY: Decision Semantics & UX Proposal

**Status:** 🎯 **PROPOSED**  
**Date:** 2026-01-02  
**Context:** System at 8.9/10 (Production-Ready with Market Stress Mode)

---

## Executive Summary

**Current State:**
- System correctly downgrades strategies lacking IV history from READY_NOW → STRUCTURALLY_READY
- These strategies have good structure but insufficient context for full evaluation
- Currently: Visible in intermediate outputs, not surfaced prominently to user

**Proposal:**
- Surface STRUCTURALLY_READY strategies explicitly in CLI/dashboard
- Show clear diagnostic: "Why is this blocked?" + "When will it be ready?"
- Enable manual promotion ONLY when IV history matures (120+ days)
- No automatic execution, no threshold lowering, no fallbacks

**Trust Impact:**
- Transparency ↑ (user sees valid setups, understands why blocked)
- Education ↑ (user learns about IV history requirements)
- Control ↑ (user can track maturation, execute when ready)
- Discipline preserved ✅ (no auto-execution without context)

---

## Design Philosophy

### Conservative Principles Preserved

1. **No Automatic Promotion**
   - STRUCTURALLY_READY never auto-executes
   - Requires explicit user decision + maturation criteria met
   - System never says "good enough" - user decides

2. **No Threshold Lowering**
   - Still requires 120 days IV history for IV Rank
   - Still requires Theory_Compliance_Score ≥ 60
   - Still requires passing acceptance rules

3. **No Fallback Execution**
   - Doesn't execute "at reduced size"
   - Doesn't execute "with caution flag"
   - Blocked means blocked

4. **Transparency Maximized**
   - Shows exactly why blocked
   - Shows exactly when unblocked
   - Shows maturation timeline

### What Changes

**Before (Current):**
```
User: "Why did I only get 2 trades?"
System: [silent about 15 STRUCTURALLY_READY setups]
User: "Did I miss opportunities?"
Result: Confusion, lack of trust
```

**After (Proposed):**
```
User: "Why did I only get 2 trades?"
System: "2 READY_NOW + 15 STRUCTURALLY_READY (awaiting IV history)"
User: "What's blocking the 15?"
System: "IV history: 4 days < 120 required. Estimated: 116 more days"
User: "Got it. I'll watch these as IV accumulates."
Result: Understanding, trust in discipline
```

---

## UX Wording Specifications

### 1. CLI Output (scan_live.py)

#### A. Acceptance Summary (After Step 12)

**Current:**
```
📊 Acceptance Summary:
   ✅ READY_NOW: 2 (1.2%)
   ⏸️  WAIT: 50 (30.1%)
   ❌ AVOID: 110 (66.3%)
   ⚠️  INCOMPLETE: 4 (2.4%)
```

**Proposed:**
```
📊 Acceptance Summary:
   ✅ READY_NOW: 2 (1.2%)
   🔶 STRUCTURALLY_READY: 15 (9.0%)  ← NEW
   ⏸️  WAIT: 50 (30.1%)
   ❌ AVOID: 110 (66.3%)
   ⚠️  INCOMPLETE: 4 (2.4%)

📋 STRUCTURALLY_READY Breakdown:
   • 15 strategies: Good structure, awaiting IV history maturation
   • Average IV history: 4.2 days (need 120+)
   • Estimated maturation: ~116 more days
   • These will auto-promote to READY_NOW when IV data sufficient
```

#### B. Detailed STRUCTURALLY_READY Section (New)

**Add after final trades display:**

```
================================================================================
🔶 STRUCTURALLY_READY STRATEGIES (AWAITING IV HISTORY)
================================================================================

These strategies passed acceptance rules but lack sufficient IV context.
They will automatically become READY_NOW when IV history reaches 120+ days.

Current IV Status:
   • Tickers: 15 strategies across 12 tickers
   • IV History: 4 days (need 120+)
   • Estimated activation: ~116 more days (~17 weeks)
   • Next milestone: 30 days (6% progress toward IV Rank)

Top 5 STRUCTURALLY_READY Strategies:
────────────────────────────────────────────────────────────────────────────────
1. AAPL | Long Call Vertical | Score: 75/100
   Structure: ✅ COMPRESSION + NO_GAP + NEAR_LOW (bullish setup)
   Blocked: IV history insufficient (4 days < 120 required)
   Confidence: MEDIUM
   Status: Will auto-promote when IV data matures

2. MSFT | Bull Put Spread | Score: 72/100
   Structure: ✅ COMPRESSION + NO_GAP + NEAR_LOW (income opportunity)
   Blocked: IV history insufficient (4 days < 120 required)
   Confidence: MEDIUM
   Status: Will auto-promote when IV data matures

3. GOOGL | Long Call Vertical | Score: 70/100
   Structure: ✅ COMPRESSION + NO_GAP + NEAR_LOW (directional long)
   Blocked: IV history insufficient (4 days < 120 required)
   Confidence: MEDIUM
   Status: Will auto-promote when IV data matures

[+12 more STRUCTURALLY_READY strategies]

💡 Understanding STRUCTURALLY_READY:
────────────────────────────────────────────────────────────────────────────────
Q: Should I execute these manually?
A: Only if you have external IV context. System can't verify IV Rank yet.

Q: When will these become READY_NOW?
A: Automatically after 120 days of IV data accumulation (~17 weeks from now).

Q: Why 120 days?
A: IV Rank requires full year context (252 trading days). System currently has 4 days.

Q: Are these "good" setups?
A: Structure is valid, but execution quality unknown without IV context.
   Directional traders might execute with external research.
   Income/volatility traders should wait for full IV data.

📊 Track IV Progress:
   Run this daily to see IV history accumulation:
   $ venv/bin/python core/data_layer/ivhv_timeseries_loader.py
   $ grep "IV Rank available" output/Step12_Acceptance_*.csv
```

#### C. Quick Reference Summary

**Add to final summary:**

```
================================================================================
FINAL SUMMARY
================================================================================

Executable Strategies:
   ✅ READY_NOW: 2 strategies (fully evaluated, ready for execution)

Awaiting Maturation:
   🔶 STRUCTURALLY_READY: 15 strategies (good structure, awaiting IV history)
      • Auto-promote when IV history ≥ 120 days
      • Current: 4 days (3% of requirement)
      • Timeline: ~116 more days

Not Recommended:
   ⏸️  WAIT: 50 strategies (good structure, timing not ideal)
   ❌ AVOID: 110 strategies (failed acceptance rules)

================================================================================
```

---

### 2. Dashboard Output (streamlit_app/dashboard.py)

#### A. Acceptance Status Cards (Top of Results)

**Proposed Layout:**

```python
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        "✅ READY NOW", 
        ready_count,
        help="Fully evaluated, executable strategies"
    )

with col2:
    st.metric(
        "🔶 STRUCTURALLY READY", 
        structurally_ready_count,
        delta=f"Awaiting IV history",
        delta_color="off",
        help="Good structure, insufficient IV context for full evaluation"
    )

with col3:
    st.metric(
        "⏸️ WAIT", 
        wait_count,
        help="Good structure, timing not ideal"
    )

with col4:
    st.metric(
        "❌ AVOID", 
        avoid_count,
        help="Failed acceptance rules"
    )
```

#### B. STRUCTURALLY_READY Expandable Section

**Add below acceptance cards:**

```python
if structurally_ready_count > 0:
    with st.expander(f"🔶 View {structurally_ready_count} STRUCTURALLY_READY Strategies", expanded=False):
        st.info("""
        **What is STRUCTURALLY_READY?**
        
        These strategies have valid structure (passed acceptance rules) but lack sufficient 
        IV history for full evaluation. They will automatically promote to READY_NOW when 
        IV data accumulation reaches 120+ days.
        
        **Should you execute these?**
        - ✅ Directional traders: May execute with external IV research
        - ⚠️ Income/volatility traders: Wait for full IV context
        - 🛑 Conservative traders: Wait for automatic promotion
        """)
        
        # Show IV maturation timeline
        if 'iv_history_days' in df_structurally_ready.columns:
            avg_history = df_structurally_ready['iv_history_days'].mean()
            days_needed = 120 - avg_history
            
            st.metric(
                "IV History Progress",
                f"{avg_history:.1f} / 120 days",
                delta=f"{days_needed:.0f} days remaining"
            )
            
            # Progress bar
            progress = min(avg_history / 120.0, 1.0)
            st.progress(progress)
        
        # Show strategies table
        st.subheader("Strategies Awaiting IV History")
        
        display_cols = [
            'Ticker', 'Strategy', 'Theory_Compliance_Score', 
            'acceptance_reason', 'confidence_band', 'iv_history_days',
            'compression_tag', 'gap_tag', 'intraday_position_tag'
        ]
        
        available_cols = [c for c in display_cols if c in df_structurally_ready.columns]
        st.dataframe(
            df_structurally_ready[available_cols],
            use_container_width=True,
            height=400
        )
        
        # Download button
        csv = df_structurally_ready.to_csv(index=False)
        st.download_button(
            label="📥 Download STRUCTURALLY_READY Strategies",
            data=csv,
            file_name=f"structurally_ready_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )
```

#### C. Warning Banner (If STRUCTURALLY_READY present)

**Add at top of results section:**

```python
if structurally_ready_count > 0:
    st.warning(f"""
    📊 **{structurally_ready_count} STRUCTURALLY_READY strategies found**
    
    These have valid structure but insufficient IV history for full evaluation.
    - Current IV history: {avg_iv_history:.1f} days (need 120+)
    - Estimated maturation: ~{days_needed:.0f} more days
    - Auto-promote to READY_NOW when IV data sufficient
    
    ℹ️ Manual execution is your decision, but system cannot verify IV Rank yet.
    """)
```

---

## Maturation Rules: STRUCTURALLY_READY → READY_NOW

### Automatic Promotion Criteria

**A strategy automatically promotes from STRUCTURALLY_READY to READY_NOW when:**

1. **IV History Threshold Met:**
   - `iv_history_days >= 120` (currently 4 days)
   - This enables IV Rank calculation (requires 120+ days for 252-day window)

2. **All Other Gates Still Pass:**
   - Theory_Compliance_Score ≥ 60 (unchanged)
   - Acceptance rules passed (unchanged)
   - Contract validation successful (unchanged)
   - Market Stress Mode not active (unchanged)

3. **No Manual Action Required:**
   - Happens automatically in next pipeline run
   - User just runs scan_live.py again after 116 days
   - System re-evaluates with new IV data

### Timeline

**Current State (Day 4):**
```
IV History: 4 days
Status: STRUCTURALLY_READY (blocked)
Progress: ████░░░░░░░░░░░░░░░░ 3%
```

**Milestone 1 (Day 30):**
```
IV History: 30 days
Status: STRUCTURALLY_READY (blocked)
Progress: ████████░░░░░░░░░░░░ 25%
Note: IV Percentile now available, but IV Rank still requires 120 days
```

**Milestone 2 (Day 60):**
```
IV History: 60 days
Status: STRUCTURALLY_READY (blocked)
Progress: ████████████░░░░░░░░ 50%
Note: Approaching minimum, but not yet sufficient
```

**Milestone 3 (Day 120):**
```
IV History: 120 days
Status: READY_NOW (automatic promotion) ✅
Progress: ████████████████████ 100%
Note: IV Rank now available, full evaluation possible
```

### No Manual Override

**Design Decision:** NO manual "promote anyway" button

**Rationale:**
- Prevents "execute without context" fallback
- Forces discipline (wait for data OR do external research)
- Preserves conservative philosophy
- If user really wants to execute: export CSV, analyze externally, execute via broker

**Alternative for Impatient Traders:**
```
User: "I want to execute AAPL now, I have my own IV research"
System: "Download STRUCTURALLY_READY CSV, verify IV context externally, execute via broker"
User: "Why can't you just execute it?"
System: "I don't have IV Rank yet. You're overriding system discipline. Do it yourself."
Result: User takes full responsibility, system stays conservative
```

---

## Persona-by-Persona Justification

### 1. Conservative Income Trader

**Use Case:** "I ignore STRUCTURALLY_READY until IV history is solid"

**Behavior:**
- Sees 15 STRUCTURALLY_READY strategies
- Reads: "Awaiting IV history (4 days < 120 required)"
- Decision: "Not interested. I'll check back in 17 weeks."
- Executes: READY_NOW only (2 strategies)

**Why This Works:**
- No pressure to execute incomplete setups
- Clear timeline for maturation
- Can track progress daily without noise
- Conservative discipline reinforced

**Trust Impact:** +0.2
- "System won't let me sell premium without IV context. Good."

---

### 2. Directional Swing Trader

**Use Case:** "I see the setup early, I'll watch it or execute with external research"

**Behavior:**
- Sees 15 STRUCTURALLY_READY strategies
- Reads: "Good structure, awaiting IV history"
- Decision: "AAPL looks good structurally. I'll check IV on TradingView."
- Action: Downloads CSV, verifies IV externally, executes 3 via broker
- Leaves 12 for automatic promotion when IV matures

**Why This Works:**
- Sees valid setups early (opportunity visibility)
- Understands why blocked (education)
- Can override with external research (control)
- System doesn't auto-execute (discipline preserved)

**Trust Impact:** +0.3
- "System shows me setups but doesn't force execution. I control it."

---

### 3. Volatility Trader

**Use Case:** "Good structure, but no rank yet — wait"

**Behavior:**
- Sees 15 STRUCTURALLY_READY strategies
- Reads: "IV history insufficient (4 days < 120 required)"
- Decision: "Can't trade vol without IV Rank. Hard pass."
- Executes: READY_NOW only (2 strategies)

**Why This Works:**
- System respects IV Rank as non-negotiable
- No temptation to execute without context
- Clear maturation timeline
- Can revisit when IV data complete

**Trust Impact:** +0.2
- "System won't let me trade vol blind. That's correct."

---

### 4. Risk Manager

**Use Case:** "No execution without context"

**Behavior:**
- Sees 15 STRUCTURALLY_READY strategies
- Reads: "Blocked due to insufficient IV history"
- Decision: "Correct. These should not execute."
- Reviews: Verifies no auto-execution, no fallbacks
- Approves: READY_NOW strategies only

**Why This Works:**
- Explicit blocking reason (audit trail)
- No silent fallbacks
- Clear maturation rule
- Can enforce "READY_NOW only" policy

**Trust Impact:** +0.3
- "System enforces context requirements. I can audit this."

---

## Implementation Requirements

### 1. Code Changes (Minimal)

**No logic changes required.** System already creates STRUCTURALLY_READY status correctly.

**Only UX changes:**

#### A. scan_live.py (+60 lines)

Add STRUCTURALLY_READY section after final trades display:

```python
# Show STRUCTURALLY_READY strategies
if 'acceptance_status' in df_all_strategies.columns:
    df_structurally_ready = df_all_strategies[
        df_all_strategies['acceptance_status'] == 'STRUCTURALLY_READY'
    ]
    
    if not df_structurally_ready.empty:
        print("\n" + "="*80)
        print("🔶 STRUCTURALLY_READY STRATEGIES (AWAITING IV HISTORY)")
        print("="*80)
        print(f"\nThese strategies passed acceptance rules but lack sufficient IV context.")
        print(f"They will automatically become READY_NOW when IV history reaches 120+ days.")
        
        # Show IV status
        if 'iv_history_days' in df_structurally_ready.columns:
            avg_history = df_structurally_ready['iv_history_days'].mean()
            max_history = df_structurally_ready['iv_history_days'].max()
            days_needed = 120 - max_history
            
            print(f"\nCurrent IV Status:")
            print(f"   • Strategies: {len(df_structurally_ready)}")
            print(f"   • IV History: {avg_history:.1f} days (need 120+)")
            print(f"   • Progress: {(avg_history/120*100):.1f}% toward IV Rank")
            print(f"   • Estimated maturation: ~{days_needed:.0f} more days")
        
        # Show top 5
        print(f"\nTop 5 STRUCTURALLY_READY Strategies:")
        print("-"*80)
        
        for idx, row in df_structurally_ready.head(5).iterrows():
            print(f"\n{idx+1}. {row['Ticker']} | {row['Strategy']} | Score: {row.get('Theory_Compliance_Score', 'N/A')}/100")
            print(f"   Structure: {row.get('compression_tag', 'N/A')} + {row.get('gap_tag', 'N/A')} + {row.get('intraday_position_tag', 'N/A')}")
            print(f"   Blocked: {row.get('acceptance_reason', 'N/A')}")
            print(f"   Confidence: {row.get('confidence_band', 'N/A')}")
        
        if len(df_structurally_ready) > 5:
            print(f"\n[+{len(df_structurally_ready) - 5} more STRUCTURALLY_READY strategies]")
        
        # Education section
        print(f"\n💡 Understanding STRUCTURALLY_READY:")
        print("-"*80)
        print(f"Q: Should I execute these manually?")
        print(f"A: Only if you have external IV context. System can't verify IV Rank yet.")
        print(f"\nQ: When will these become READY_NOW?")
        print(f"A: Automatically after 120 days of IV data accumulation.")
        print(f"\nQ: Why 120 days?")
        print(f"A: IV Rank requires full year context (252 trading days).")
```

#### B. streamlit_app/dashboard.py (+100 lines)

Add STRUCTURALLY_READY expandable section and status card (see UX spec above).

#### C. No Changes Required

- Step 12 logic: Already creates STRUCTURALLY_READY correctly ✅
- IV availability loader: Already tracks iv_history_days ✅
- Maturation logic: Already automatic (next run with 120+ days) ✅

---

### 2. Documentation Updates

**Files to update:**
- `PHASE_3_IV_AVAILABILITY_INTEGRATION_COMPLETE.md` - Add maturation rules
- `SYSTEM_ASSESSMENT_FINAL_RAG_EVALUATION.md` - Update transparency score
- `README.md` - Add STRUCTURALLY_READY explanation

---

## Trust Impact Analysis

### Current State (8.9/10)

**Transparency Gap:**
- User sees: "2 final trades"
- User doesn't see: "15 good setups blocked by IV history"
- User thinks: "Did I miss something? Why so few?"
- Result: Confusion, reduced trust

### After STRUCTURALLY_READY UX (Projected 9.1/10)

**Transparency Improved:**
- User sees: "2 READY_NOW + 15 STRUCTURALLY_READY (awaiting IV)"
- User understands: "System found setups but enforcing context requirement"
- User tracks: "116 more days until IV Rank available"
- Result: Education, trust in discipline, control

**Rating Impact:**
- Conservative Income: 8.3 → 8.4 (+0.1, clear timeline)
- Directional Swing: 8.5 → 8.8 (+0.3, opportunity visibility)
- Volatility Trader: 7.8 → 8.0 (+0.2, respects IV requirement)
- Risk Manager: 8.5 → 8.8 (+0.3, audit trail + discipline)

**Overall:** 8.9 → 9.1 (+0.2 from transparency improvement alone)

---

## Conservative Philosophy Validation

### ✅ No Threshold Lowering
- Still requires 120 days for IV Rank
- Still requires Theory_Compliance_Score ≥ 60
- Still requires passing acceptance rules
- Only change: **visibility** of blocked strategies

### ✅ No Fallback Execution
- STRUCTURALLY_READY never auto-executes
- No "execute at reduced size" option
- No "execute with caution flag" option
- Blocked means blocked

### ✅ No Reduced Transparency
- Actually **increases** transparency
- Shows why blocked (IV history insufficient)
- Shows when unblocked (120 days timeline)
- Shows progress (4/120 days = 3%)

### ✅ Improves Trust Without Increasing Trade Count
- System still executes 2 READY_NOW strategies
- Shows 15 STRUCTURALLY_READY (education, not execution)
- User may execute externally (their decision, not system's)
- Trade count unchanged, trust increased

---

## Failure Mode Prevention

### Failure Mode 1: "System Pressures User to Execute"

**Prevention:**
- Wording: "Awaiting IV history" (neutral, not "available now")
- No "Execute Anyway" button
- Education: "Only execute if you have external IV context"
- Default action: Wait for maturation

### Failure Mode 2: "User Ignores IV Requirements"

**Prevention:**
- Explicit warning: "System can't verify IV Rank yet"
- Persona-specific guidance (income trader: wait, directional: external research)
- Download CSV for external analysis (forces explicit override)
- No one-click execution

### Failure Mode 3: "System Dilutes Discipline Over Time"

**Prevention:**
- Maturation rule hardcoded (120 days, non-negotiable)
- No threshold slider, no "adjust sensitivity"
- No "smart execution" fallback
- Conservative philosophy documented

---

## Implementation Priority

**Effort:** Low (2-3 hours, mostly UX wording)  
**Value:** High (transparency, education, control)  
**Risk:** Very Low (no logic changes, only visibility)

**Priority:** P1 (High-Value Enhancement)

**Rationale:**
- Low effort, high trust impact
- Preserves all conservative principles
- Improves user understanding
- Enables tracking without execution pressure

---

## Next Steps

1. **Implement STRUCTURALLY_READY UX** (2-3 hours)
   - Add CLI section (scan_live.py)
   - Add dashboard expandable (dashboard.py)
   - Test with current 4-day IV history

2. **Validate Persona Responses** (1 hour)
   - Review with hypothetical income trader
   - Review with hypothetical directional trader
   - Verify no execution pressure

3. **Document Maturation Timeline** (30 minutes)
   - Update Phase 3 docs with auto-promotion rule
   - Add to README.md
   - Create tracking guide

4. **Production Deployment** (immediate)
   - No breaking changes
   - Backwards compatible
   - Safe to deploy

---

## Conclusion

**Problem:** Valid setups with insufficient IV history are invisible to user

**Solution:** Surface STRUCTURALLY_READY explicitly with clear diagnostics and maturation timeline

**Philosophy:** Transparency > silence, education > execution, discipline > convenience

**Trust Impact:** 8.9 → 9.1 (+0.2 from transparency improvement)

**Conservative Validation:** ✅ All principles preserved
- No threshold lowering
- No fallback execution
- No automatic promotion to execution
- Improved trust through clarity, not trade count

**Recommendation:** **IMPLEMENT** as P1 enhancement (High value, low effort, zero risk)

---

## Appendix: Sample User Flows

### Flow 1: Conservative Income Trader

```
Day 1:
  User runs scan_live.py
  Output: "2 READY_NOW, 15 STRUCTURALLY_READY (4/120 days)"
  User: "Not interested in STRUCTURALLY_READY. I'll execute READY_NOW only."
  Action: Execute 2 strategies via broker

Day 30:
  User runs scan_live.py
  Output: "3 READY_NOW, 18 STRUCTURALLY_READY (30/120 days)"
  User: "Still not enough IV history. Execute READY_NOW only."
  Action: Execute 3 strategies via broker

Day 120:
  User runs scan_live.py
  Output: "8 READY_NOW (5 new from STRUCTURALLY_READY maturation)"
  User: "Great! IV history now sufficient. Execute all 8."
  Action: Execute 8 strategies via broker
  
Result: Disciplined, patient, no regret trades
```

### Flow 2: Directional Swing Trader

```
Day 1:
  User runs scan_live.py
  Output: "2 READY_NOW, 15 STRUCTURALLY_READY (4/120 days)"
  User: "Let me check AAPL - structure looks good."
  Action: 
    1. Download STRUCTURALLY_READY CSV
    2. Check IV on TradingView (IV Rank ~40, not extreme)
    3. Execute AAPL Long Call Vertical via broker (override system)
  User: "I took responsibility for external IV check. 2 other setups I'll wait."
  
Day 120:
  User runs scan_live.py
  Output: "8 READY_NOW (includes AAPL - would have auto-promoted)"
  User: "System would have approved AAPL anyway. My early entry was validated."
  
Result: Opportunity captured early, system discipline respected
```

### Flow 3: Volatility Trader

```
Day 1:
  User runs scan_live.py
  Output: "2 READY_NOW, 15 STRUCTURALLY_READY (4/120 days)"
  User: "Can't trade vol without IV Rank. Hard pass on STRUCTURALLY_READY."
  Action: Execute 2 READY_NOW strategies only
  
Day 120:
  User runs scan_live.py
  Output: "8 READY_NOW (now with full IV Rank)"
  User: "Now I can evaluate vol trades properly."
  Action: Execute 8 strategies via broker
  
Result: No blind vol trading, disciplined approach validated
```

---

**Status:** 🎯 Ready for Implementation  
**Approval Required:** User review of UX wording and persona flows
