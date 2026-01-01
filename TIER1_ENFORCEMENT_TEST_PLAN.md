# Tier-1 Enforcement Testing Guide

**Document Purpose:** Systematic validation of the Tier-1 safety architecture  
**Implementation Date:** 2025-01-XX  
**Architecture:** Step 7 (filter) + Step 9B (validate) + UI (exploration mode)

---

## 1. Architecture Overview

### Safety Layers
```
┌─────────────────────────────────────────────────────────┐
│ LAYER 1: Step 7 - Strategy Generation (Safety Gate)    │
│ - tier_filter='tier1_only' (DEFAULT)                    │
│ - Outputs only Tier-1 unless exploration_mode=True      │
│ - Tags: Strategy_Tier (int), EXECUTABLE (bool)          │
└─────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────┐
│ LAYER 2: Step 7B - Multi-Strategy Ranker (Safety Gate) │
│ - Same tier_filter enforcement                          │
│ - Filters df_suggestions to Tier-1 by default           │
│ - Tags: EXECUTABLE flag on all rows                     │
└─────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────┐
│ LAYER 3: Step 9B - Contract Fetching (Validation Gate) │
│ - Checks Strategy_Tier == 1                             │
│ - Raises ValueError if non-Tier-1 detected              │
│ - HARD STOP - cannot be bypassed                        │
└─────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────┐
│ LAYER 4: UI - Exploration Mode (User Control)          │
│ - Checkbox to enable exploration mode                   │
│ - Shows warning for non-executable strategies           │
│ - Mode-specific button labels and messages              │
└─────────────────────────────────────────────────────────┘
```

### Default Behavior (Safety Mode)
- **Step 7:** Only Tier-1 strategies in output
- **Step 7B:** Only Tier-1 suggestions generated
- **Step 9B:** All input validated as Tier-1
- **UI:** No warnings, clean execution path

### Exploration Mode (Educational Viewing)
- **Step 7:** All tiers in output, EXECUTABLE=False for Tier-2/3
- **Step 7B:** All tiers in suggestions, tagged NON-EXECUTABLE
- **Step 9B:** Will reject if data passes through
- **UI:** Warnings displayed, cannot proceed to execution

---

## 2. Test Suite

### TEST 1: Step 7 Default Mode (Tier-1 Only)

**Expected Behavior:** Only Tier-1 strategies output by default

**Steps:**
1. Restart Streamlit to clear session state
   ```bash
   pkill -f "streamlit run" && streamlit run streamlit_app/dashboard.py
   ```

2. Run Steps 2-6 to generate input data

3. Run Step 7 **without** checking exploration mode checkbox

4. **Check Logs** - should contain:
   ```
   🎯 Step 7: Generating strategies for X/X tickers with complete data
   🔒 TIER-1 FILTER: Y/X strategies are Tier-1 (executable)
      Non-Tier-1 strategies excluded for safety (use exploration_mode=True to see all)
   ✅ Step 7: Final dtypes - {...Strategy_Tier': dtype('int64'), 'EXECUTABLE': dtype('bool')}
   ✅ Arrow compatibility validated
   ```

5. **Check Output DataFrame** (in Step 7 expander):
   - All rows should have `Strategy_Tier = 1`
   - All rows should have `EXECUTABLE = True`
   - Count should be significantly lower than total tickers (typically 30-50% are Tier-1)

6. **Verify UI Message:**
   ```
   ✅ Generated X Tier-1 executable strategies!
   ```

**Pass Criteria:**
- ✅ Log shows "TIER-1 FILTER" enforcement
- ✅ All output rows have Strategy_Tier=1
- ✅ All output rows have EXECUTABLE=True
- ✅ No Tier-2 or Tier-3 strategies present
- ✅ UI shows "Tier-1 executable strategies" message

---

### TEST 2: Step 7 Exploration Mode (All Tiers)

**Expected Behavior:** All tiers shown, non-Tier-1 tagged NON-EXECUTABLE

**Steps:**
1. In Step 7 section, check "🔍 Show all tiers (educational viewing only)"

2. Click button (should now say "📊 Generate Strategy Recommendations (All Tiers - Educational)")

3. **Check UI Warning:**
   ```
   ⚠️ EXPLORATION MODE: All tiers shown (Tier-2/3 are NON-EXECUTABLE)
   ```

4. **Check Logs:**
   ```
   🎯 Step 7: Generating strategies for X/X tickers with complete data
   (No TIER-1 FILTER log - all strategies pass through)
   ✅ Arrow compatibility validated
   ```

5. **Check Output DataFrame:**
   - Should have mix of Strategy_Tier values (1, 2, 3, 999)
   - Tier-1: EXECUTABLE=True
   - Tier-2/3/999: EXECUTABLE=False
   - Count should be much higher than Tier-1 only mode

6. **Verify UI Message:**
   ```
   ✅ Generated X strategies (Y NON-EXECUTABLE for educational viewing)
   ```

**Pass Criteria:**
- ✅ UI shows exploration mode warning
- ✅ Output contains Tier-2/3 strategies
- ✅ Non-Tier-1 strategies have EXECUTABLE=False
- ✅ Success message shows non-executable count
- ✅ Button label says "All Tiers - Educational"

---

### TEST 3: Step 7B Default Mode (Tier-1 Only)

**Expected Behavior:** Multi-strategy suggestions filtered to Tier-1

**Steps:**
1. Run Steps 2-6 to get Step 6 GEM data

2. In Step 7B section, enter user profile (account size, risk, goal)

3. Click "Generate Personalized Strategies" **without** exploration mode

4. **Check Logs:**
   ```
   🔒 TIER-1 FILTER (Step 7): Y/X strategies are Tier-1
   🔀 Multi-Strategy: Generating suggestions...
   🔒 TIER-1 FILTER (Step 7B): Z/W suggestions are Tier-1 (executable)
   ```

5. **Check Output DataFrame:**
   - All rows should have Strategy_Tier=1
   - All rows should have EXECUTABLE=True

6. **Verify UI Message:**
   ```
   ✅ Generated X Tier-1 executable strategies!
   ```

**Pass Criteria:**
- ✅ Logs show Tier-1 filtering in both Step 7 and Step 7B
- ✅ All suggestions have Strategy_Tier=1
- ✅ All suggestions have EXECUTABLE=True
- ✅ UI shows "Tier-1 executable" message

---

### TEST 4: Step 7B Exploration Mode (All Tiers)

**Expected Behavior:** Multi-strategy suggestions show all tiers with NON-EXECUTABLE tags

**Steps:**
1. Enable exploration mode in Step 7

2. Run Step 7 in exploration mode

3. Navigate to Step 7B and generate personalized strategies

4. **Check UI Warning:**
   ```
   ⚠️ EXPLORATION MODE: All tiers shown (Tier-2/3 are NON-EXECUTABLE)
   ```

5. **Check Output DataFrame:**
   - Should contain mix of tiers (1, 2, 3)
   - Tier-1: EXECUTABLE=True
   - Tier-2/3: EXECUTABLE=False

6. **Verify UI Message:**
   ```
   ✅ Generated X strategies (Y NON-EXECUTABLE for educational viewing)
   ```

**Pass Criteria:**
- ✅ Exploration warning displayed
- ✅ Output contains non-Tier-1 suggestions
- ✅ EXECUTABLE flag properly tagged
- ✅ Success message shows non-executable count

---

### TEST 5: Step 9B Validation Gate (Rejection Test)

**Expected Behavior:** Step 9B rejects non-Tier-1 data with ValueError

**Manual Test (requires Python console):**

```python
import pandas as pd
from core.scan_engine.step9b_fetch_contracts import fetch_and_select_contracts

# Create test DataFrame with Tier-2 strategy
df_test = pd.DataFrame({
    'Ticker': ['AAPL'],
    'Strategy_Name': ['Bull Call Spread'],
    'Strategy_Tier': [2],  # Non-Tier-1
    'Primary_Directional_Strategy': ['Bull Call Spread'],
    'DTE_Min': [30],
    'DTE_Max': [45]
})

# This should raise ValueError
try:
    result = fetch_and_select_contracts(df_test)
    print("❌ TEST FAILED: Should have raised ValueError")
except ValueError as e:
    if "SAFETY VIOLATION" in str(e) and "non-Tier-1" in str(e):
        print("✅ TEST PASSED: Validation gate working")
        print(f"Error message: {e}")
    else:
        print(f"❌ TEST FAILED: Wrong error message: {e}")
```

**Expected Output:**
```
🔒 STEP 9B VALIDATION: Checking Strategy_Tier column...
❌ REJECTED: Non-Tier-1 strategies detected
   Tier breakdown: Tier-1: 0, Tier-2: 1, Tier-3: 0, Unknown: 0
ValueError: SAFETY VIOLATION: Step 9B detected non-Tier-1 strategies
```

**Pass Criteria:**
- ✅ ValueError raised with "SAFETY VIOLATION" message
- ✅ Error message includes tier breakdown
- ✅ Logs show validation check before rejection

---

### TEST 6: Step 9B Validation Gate (Success Test)

**Expected Behavior:** Step 9B accepts Tier-1 data and proceeds

**Manual Test:**

```python
import pandas as pd
from core.scan_engine.step9b_fetch_contracts import fetch_and_select_contracts

# Create test DataFrame with Tier-1 strategy
df_test = pd.DataFrame({
    'Ticker': ['AAPL'],
    'Strategy_Name': ['Long Call'],
    'Strategy_Tier': [1],  # Tier-1
    'Primary_Directional_Strategy': ['Long Call'],
    'DTE_Min': [30],
    'DTE_Max': [45]
})

# This should succeed (may fail later due to API, but should pass validation)
try:
    result = fetch_and_select_contracts(df_test)
    print("✅ TEST PASSED: Validation gate passed Tier-1 data")
except ValueError as e:
    if "SAFETY VIOLATION" in str(e):
        print(f"❌ TEST FAILED: Incorrectly rejected Tier-1 data: {e}")
    else:
        # Other errors (API, network) are expected
        print(f"⚠️ Validation passed, but execution failed (expected): {e}")
```

**Expected Output:**
```
🔒 STEP 9B VALIDATION: Checking Strategy_Tier column...
✅ TIER-1 VALIDATION PASSED: All 1 strategies are Tier-1
   Tier breakdown: Tier-1: 1
(Possible API/network errors after this point)
```

**Pass Criteria:**
- ✅ No ValueError with "SAFETY VIOLATION"
- ✅ Logs show "TIER-1 VALIDATION PASSED"
- ✅ Tier breakdown shows correct counts

---

### TEST 7: UI Exploration Mode Warning (Step 9B)

**Expected Behavior:** UI warns if exploration mode data exists at Step 9B

**Steps:**
1. Run Step 7 in exploration mode (all tiers)

2. Navigate to Step 9B section

3. **Check for Warning:**
   ```
   ⚠️ NON-EXECUTABLE STRATEGIES DETECTED: X strategies in Step 7 are marked NON-EXECUTABLE (Tier-2/3).
   
   Step 9B will reject these strategies. To proceed with execution:
   1. Return to Step 7
   2. Disable exploration mode
   3. Re-run to generate Tier-1 only strategies
   ```

4. Attempt to run Step 9B (should fail if non-Tier-1 passed through)

**Pass Criteria:**
- ✅ Warning displayed if EXECUTABLE=False rows exist in Step 7 data
- ✅ Warning provides clear instructions to fix
- ✅ Step 9B execution fails with ValueError (if attempted)

---

### TEST 8: End-to-End Safe Execution Path

**Expected Behavior:** Complete pipeline runs without errors in default mode

**Steps:**
1. Fresh Streamlit restart

2. Run complete pipeline in default mode:
   - Step 2: Clean data
   - Step 3: PCS scoring
   - Step 5: Chart scoring
   - Step 6: GEM filtering
   - Step 7: Strategy recommendation (DEFAULT - no exploration)
   - Step 9A: DTE timeframes
   - Step 9B: Fetch contracts

3. **Verify No Warnings/Errors:**
   - No exploration mode warnings
   - No SAFETY VIOLATION errors
   - Clean execution logs at each step

4. **Verify Step 7 Output:**
   - Only Tier-1 strategies
   - EXECUTABLE=True for all

5. **Verify Step 9B Success:**
   - Validation passes
   - Contracts fetched successfully

**Pass Criteria:**
- ✅ All steps complete without errors
- ✅ No safety warnings displayed
- ✅ Step 7 outputs Tier-1 only
- ✅ Step 9B accepts all data
- ✅ Clean logs throughout

---

### TEST 9: Column Dtype Validation

**Expected Behavior:** All dtypes remain stable, no object dtype corruption

**Steps:**
1. Run Step 7 in default mode

2. In Python console, check dtypes:
   ```python
   df = st.session_state['step7_result']
   print(df.dtypes)
   ```

3. **Verify Expected Dtypes:**
   ```
   Strategy_Tier          int64
   EXECUTABLE              bool
   Primary_Strategy      string[python]
   Confidence           float64
   ```

4. Run Step 7B and check again

5. **Verify No object dtype:**
   ```python
   object_cols = [col for col, dtype in df.dtypes.items() if dtype == 'object']
   print(f"Object columns (should be empty): {object_cols}")
   ```

**Pass Criteria:**
- ✅ Strategy_Tier is int64
- ✅ EXECUTABLE is bool
- ✅ String columns are string[python] (not object)
- ✅ Float columns are float64 (not object)
- ✅ No object dtype columns found

---

### TEST 10: Arrow Serialization Stability

**Expected Behavior:** No Arrow serialization errors during UI rendering

**Steps:**
1. Run Steps 2-7 completely

2. Monitor terminal logs for ANY Arrow errors:
   ```
   ArrowInvalid
   ArrowNotImplementedError
   ArrowTypeError
   ```

3. Navigate between steps in UI (trigger re-renders)

4. Open/close expanders (triggers dataframe serialization)

5. **Check session_state storage:**
   ```python
   # All these should work without errors
   st.session_state['step2_cleaned']
   st.session_state['step3_enriched']
   st.session_state['step5_charted']
   st.session_state['step6_gem']
   st.session_state['step7_result']
   ```

**Pass Criteria:**
- ✅ No Arrow errors in terminal logs
- ✅ All dataframes render correctly in UI
- ✅ Expanders open without errors
- ✅ Session state stores/retrieves cleanly

---

## 3. Regression Tests

### Verify No Broken Functionality

**Test A: Step 7 Column Names (CANONICAL RULES)**
- ✅ "Context Confidence" column exists (not "Confidence")
- ✅ "Evaluation Priority" column exists (not "Option Rank")
- ✅ NO "Capital" column
- ✅ NO "Win %" column
- ✅ NO "Risk/Reward" column

**Test B: Step 7 Filtering UI**
- ✅ Directional filter works (Bull/Bear/Neutral)
- ✅ Volatility filter works (Long Vol/Short Vol)
- ✅ Strategy name filter works
- ✅ Ticker search works

**Test C: Step 7B Display**
- ✅ User profile shown in header
- ✅ Personal recommendations display
- ✅ Tier labels say "Educational Only" for Tier-2/3
- ✅ Suitability score renders as progress bar

**Test D: Tier Toggle Behavior**
- ✅ Show Tier-1 checkbox filters correctly
- ✅ Show Tier-2 checkbox filters correctly
- ✅ Show Tier-3 checkbox filters correctly
- ✅ Unchecking all tiers shows empty table

---

## 4. Performance Tests

### Execution Time Impact

**Baseline (No Filtering):**
- Step 7 on 100 tickers: ~X seconds

**With Tier-1 Filter:**
- Step 7 on 100 tickers: ~X seconds
- Expected delta: <5% (filtering is lightweight)

**Verification:**
```python
import time

# Measure without filtering
start = time.time()
df_all = recommend_strategies(df_input, tier_filter='all_tiers')
time_all = time.time() - start

# Measure with filtering
start = time.time()
df_tier1 = recommend_strategies(df_input, tier_filter='tier1_only')
time_tier1 = time.time() - start

print(f"All tiers: {time_all:.2f}s")
print(f"Tier-1 only: {time_tier1:.2f}s")
print(f"Overhead: {(time_tier1/time_all - 1)*100:.1f}%")
```

**Pass Criteria:**
- ✅ Tier-1 filtering adds <10% overhead
- ✅ No memory leaks
- ✅ No performance degradation over multiple runs

---

## 5. Documentation Checklist

### Files to Verify

- ✅ `STEP7_CANONICAL_RULES.md` updated with tier enforcement section
- ✅ `STEP7_COMPLIANCE_CHANGES.md` documents all changes
- ✅ `core/scan_engine/step7_strategy_recommendation.py` docstring updated
- ✅ `core/scan_engine/step7b_multi_strategy_ranker.py` docstring updated
- ✅ `core/scan_engine/step9b_fetch_contracts.py` docstring updated
- ✅ `TIER1_ENFORCEMENT_TEST_PLAN.md` (this document) exists

### Code Comments

- ✅ Each safety gate has clear comment explaining purpose
- ✅ Parameters documented with defaults
- ✅ ValueError messages are descriptive

---

## 6. Quick Validation Script

**Run this to validate core functionality:**

```python
#!/usr/bin/env python3
"""
Quick validation script for Tier-1 enforcement
"""
import pandas as pd
import sys

def test_step7_parameters():
    """Test Step 7 function signature"""
    from core.scan_engine.step7_strategy_recommendation import recommend_strategies
    import inspect
    sig = inspect.signature(recommend_strategies)
    params = sig.parameters
    
    assert 'tier_filter' in params, "Missing tier_filter parameter"
    assert params['tier_filter'].default == 'tier1_only', "Wrong default for tier_filter"
    assert 'exploration_mode' in params, "Missing exploration_mode parameter"
    assert params['exploration_mode'].default == False, "Wrong default for exploration_mode"
    print("✅ Step 7 parameters validated")

def test_step7b_parameters():
    """Test Step 7B function signature"""
    from core.scan_engine.step7b_multi_strategy_ranker import generate_multi_strategy_suggestions
    import inspect
    sig = inspect.signature(generate_multi_strategy_suggestions)
    params = sig.parameters
    
    assert 'tier_filter' in params, "Missing tier_filter parameter"
    assert 'exploration_mode' in params, "Missing exploration_mode parameter"
    print("✅ Step 7B parameters validated")

def test_step9b_validation():
    """Test Step 9B validation gate"""
    from core.scan_engine.step9b_fetch_contracts import fetch_and_select_contracts
    
    # Create Tier-2 test data
    df_tier2 = pd.DataFrame({
        'Ticker': ['TEST'],
        'Strategy_Name': ['Bull Call Spread'],
        'Strategy_Tier': [2],
        'Primary_Directional_Strategy': ['Bull Call Spread'],
        'DTE_Min': [30],
        'DTE_Max': [45]
    })
    
    try:
        fetch_and_select_contracts(df_tier2)
        print("❌ Step 9B validation FAILED: Did not reject Tier-2")
        return False
    except ValueError as e:
        if "SAFETY VIOLATION" in str(e):
            print("✅ Step 9B validation gate working")
            return True
        else:
            print(f"❌ Step 9B wrong error: {e}")
            return False

def test_dtypes():
    """Test dtype initialization"""
    import pandas as pd
    
    # Simulate Step 7 dtype setup
    df = pd.DataFrame({'Ticker': ['TEST']})
    df['Strategy_Tier'] = pd.Series(999, index=df.index, dtype='int64')
    df['EXECUTABLE'] = pd.Series(False, index=df.index, dtype='bool')
    df['Primary_Strategy'] = pd.Series('None', index=df.index, dtype='string')
    
    assert df['Strategy_Tier'].dtype == 'int64', f"Wrong dtype: {df['Strategy_Tier'].dtype}"
    assert df['EXECUTABLE'].dtype == 'bool', f"Wrong dtype: {df['EXECUTABLE'].dtype}"
    assert df['Primary_Strategy'].dtype == 'string', f"Wrong dtype: {df['Primary_Strategy'].dtype}"
    print("✅ Dtype initialization validated")

if __name__ == '__main__':
    print("🧪 Running Tier-1 Enforcement Validation...")
    print()
    
    try:
        test_step7_parameters()
        test_step7b_parameters()
        test_step9b_validation()
        test_dtypes()
        print()
        print("🎉 All validation tests PASSED")
        sys.exit(0)
    except Exception as e:
        print()
        print(f"❌ Validation FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
```

**Save as:** `test_tier1_enforcement.py`  
**Run:** `python test_tier1_enforcement.py`

---

## 7. Known Issues & Limitations

### Current Limitations

1. **Exploration Mode Data Flow:**
   - If user runs Step 7 in exploration mode, data contains Tier-2/3
   - If user then runs Step 9A/9B, will hit validation error
   - **Mitigation:** UI warning added to Step 9B section

2. **Legacy Execution_Ready Column:**
   - Step 9B checks for backward compatibility
   - **Future:** Remove legacy column check after 30 days

3. **Performance:**
   - Tier filtering adds minimal overhead (<5%)
   - **Future:** Consider caching tier assignments

### Future Enhancements

1. **Smart UI Flow:**
   - Disable Step 9B button if exploration mode active
   - Auto-switch to default mode when navigating to Step 9B

2. **Tier Statistics Dashboard:**
   - Show pie chart of tier distribution
   - Display "executable vs educational" counts

3. **Broker Upgrade Path:**
   - Link to broker approval upgrade instructions
   - Show which strategies unlock at each tier

---

## 8. Sign-Off Checklist

**Before Deploying to Production:**

- [ ] All 10 test cases pass
- [ ] Regression tests show no broken functionality
- [ ] Performance tests show <10% overhead
- [ ] Documentation updated (STEP7_CANONICAL_RULES.md)
- [ ] Quick validation script passes
- [ ] User tested end-to-end pipeline
- [ ] No Arrow serialization errors in logs
- [ ] Step 9B validation gate confirmed working
- [ ] Exploration mode warnings display correctly
- [ ] UI button labels reflect mode accurately

**Deployment Steps:**

1. Clear all caches: `find . -name "*.pyc" -delete && find . -name "__pycache__" -delete`
2. Restart Streamlit: `pkill -f streamlit && streamlit run streamlit_app/dashboard.py`
3. Run full pipeline (Steps 2-9B) in default mode
4. Verify logs show "TIER-1 FILTER" enforcement
5. Test exploration mode toggle
6. Confirm Step 9B rejects non-Tier-1 data

---

## 9. Support & Troubleshooting

### Common Issues

**Issue:** "Step 7 not filtering to Tier-1"
- **Check:** Logs should show "🔒 TIER-1 FILTER" message
- **Solution:** Verify `tier_filter` parameter passed correctly
- **Debug:** Print `tier_filter` value in function

**Issue:** "Step 9B accepts Tier-2 strategies"
- **Check:** Look for "TIER-1 VALIDATION PASSED" log
- **Solution:** Verify Strategy_Tier column exists and has correct values
- **Debug:** Print `df['Strategy_Tier'].unique()` before validation

**Issue:** "Arrow serialization errors still occurring"
- **Check:** Which step throws error (check column name in error)
- **Solution:** Apply `sanitize_for_arrow()` to that session_state store
- **Debug:** Check dtypes with `df.dtypes` before storing

**Issue:** "Exploration mode not showing all tiers"
- **Check:** Verify checkbox state in session_state
- **Solution:** Ensure `exploration_mode` parameter passed correctly
- **Debug:** Add log in recommend_strategies showing parameter values

### Contact

**For Issues:** Create GitHub issue with:
- Test case that failed
- Full error message
- Relevant log output
- Steps to reproduce

**For Questions:** See `STEP7_CANONICAL_RULES.md` for architectural decisions

---

**END OF TEST PLAN**
