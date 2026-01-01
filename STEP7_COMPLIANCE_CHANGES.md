# Step 7 Stricter Canonical Rules - Implementation Changes

**Date:** December 27, 2025  
**Status:** ✅ COMPLETED

## Changes Implemented

### 1. ✅ Removed Execution-Level Metrics from Step 7B Table

**Removed columns:**
- `Capital_Requirement_Est` (exact capital requires strikes/premiums)
- `Percent_Of_Account` (position sizing is Step 9B)
- `Success_Probability` (Win % - real POP only in Step 9+)
- `Risk_Reward_Ratio` (requires real option pricing)

**Result:** Step 7B table now shows only discovery-layer information (structure, timeframe, suitability, goal alignment)

### 2. ✅ Renamed Columns per Canonical Rules

**Column name updates:**
- `Confidence` → `Context Confidence` (emphasizes heuristic nature)
- `Suitability_Score` → `Context Confidence` (Step 7B table)
- `Entry Priority` → `Evaluation Priority` (NOT execution priority)
- `Success Probability` → removed (was Win %)

**Tooltips updated:** All columns now have explicit disclaimers (NOT PCS, NOT GEM, NOT execution signal)

### 3. ✅ Updated Tier Toggle Labels

**New labels:**
- ✅ Tier 1 (Executable) - unchanged
- 📚 Tier 2 (Educational Only) - was "Broker-Blocked"
- 📚 Tier 3 (Educational Only) - was "Logic-Blocked"

**Tooltips updated:** Explicitly state "NOT actionable" for Tier 2/3

### 4. ✅ Added Non-Dismissable Boundary Notices

**Step 7 and Step 7B headers now display:**
```
🚨 STEP 7 BOUNDARY (NON-NEGOTIABLE):

Step 7 identifies which strategy structures are worth evaluating.
This is NOT an execution signal.

Execution quality (PCS / GEM status) is determined in Step 9+ via option chain analysis.

If a value depends on strikes, premiums, or real option chains, it does NOT belong in Step 7.
```

**Display method:** `st.error()` - red banner, cannot be collapsed/dismissed

### 5. ✅ Updated STEP7_CANONICAL_RULES.md

**New structure:**
- Added PREAMBLE defining Step 7 as "prescriptive discovery layer"
- Section 1: Tier Enforcement (Non-Negotiable)
- Section 2: Strategy Filtering (Required)
- Section 3: Column Naming & Meaning
- Section 4: Remove Execution-Level Fields (MANDATORY)
- Section 5: Buy-Write Strategy Rules
- Section 6: Execution Boundary (Hard Line)
- Section 7: Role Separation

**Key addition:** "This document supersedes all prior Step 7 descriptions. Any violation is a bug."

### 6. ✅ Updated Step 7 Semantics Warnings

**Enhanced clarity:**
- "Primary Strategy" explicitly defined as "Best structural fit GIVEN current context"
- "Context Confidence" explicitly defined as "Heuristic alignment score"
- Added explicit "does NOT mean" lists for both
- Added role separation (Step 7 = structure, Step 8 = sizing, Step 9+ = validation)

## Files Modified

1. **streamlit_app/dashboard.py**
   - Lines 1086-1104: Step 7B header (non-dismissable notice)
   - Lines 1236-1239: Tier toggle labels
   - Lines 1329-1347: Column configurations (removed execution metrics)
   - Lines 1441-1489: Step 7 header (non-dismissable notice + enhanced semantics)
   - Line 1517: Renamed "Recommended" → "Worth Evaluating"

2. **STEP7_CANONICAL_RULES.md**
   - Complete restructure per stricter interpretation
   - 10 sections covering all mandatory rules
   - Final Authority Statement

3. **core/scan_engine/step7_strategy_recommendation.py**
   - Added canonical rules reference in file header

4. **core/scan_engine/step7b_multi_strategy_ranker.py**
   - Added canonical rules reference in file header
   - Buy-Write section references Canonical Rule 7

## Validation Checklist

- [x] No execution-level metrics in Step 7B table
- [x] Column names match strict requirements (Context Confidence, Evaluation Priority)
- [x] Tier toggles labeled "Educational Only" for Tier 2/3
- [x] Non-dismissable boundary notice at top of Step 7/7B sections
- [x] All tooltips explicitly disclaim execution signals
- [x] Documentation updated to match implementation
- [x] Code references canonical rules document
- [x] Python syntax validated (no compile errors)

## Testing Recommendations

1. **Visual Verification:**
   - Start Streamlit dashboard
   - Scroll to Step 7B (Strategy Explorer)
   - Verify red "STEP 7 BOUNDARY" banner is visible and non-collapsible
   - Verify Tier 2/3 toggles say "Educational Only"
   - Verify table does NOT show Capital, % Account, Win %, Risk/Reward columns

2. **Functional Verification:**
   - Generate multi-strategies (Step 7B)
   - Verify Tier 1 checkbox is checked by default
   - Verify Tier 2/3 checkboxes are unchecked by default
   - Toggle Tier 2 on - verify additional strategies appear
   - Verify column headers match new names (Context Confidence, Evaluation Priority)

3. **Semantic Verification:**
   - Read warning boxes - confirm they emphasize "NOT execution signal"
   - Check tooltips on all columns - confirm explicit disclaimers present
   - Verify "Worth Evaluating" metric instead of "Recommended"

## Legal Defensibility

**Achieved:**
- Users cannot miss boundary warnings (red, non-collapsable)
- No execution-level metrics that could be misinterpreted
- Every column explicitly disclaims execution quality
- "Educational Only" labeling prevents Tier 2/3 confusion
- Role separation clearly stated (Step 7 ≠ Step 9+)

**Result:** UI explicitly prevents confusion between strategy discovery and execution approval.
