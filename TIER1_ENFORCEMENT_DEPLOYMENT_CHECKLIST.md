# Tier-1 Enforcement Deployment Checklist

**Date:** 2025-12-27  
**Implementer:** _________________  
**Reviewer:** _________________

---

## Pre-Deployment Checks

### Code Validation
- [x] All Python files compile without errors
- [x] Validation script passes (7/7 tests)
- [x] No syntax errors in modified files
- [x] Proper parameter defaults set (tier_filter='tier1_only', exploration_mode=False)

### Documentation
- [x] TIER1_ENFORCEMENT_SUMMARY.md created
- [x] TIER1_ENFORCEMENT_TEST_PLAN.md created
- [x] TIER1_ENFORCEMENT_QUICK_REFERENCE.md created
- [x] test_tier1_enforcement.py created
- [x] STEP7_CANONICAL_RULES.md updated

### Testing
- [x] Step 7 parameters validated
- [x] Step 7B parameters validated
- [x] Step 9B rejection test passed
- [x] Step 9B acceptance test passed
- [x] Dtype initialization validated
- [x] Arrow sanitization verified
- [x] Canonical rules compliance checked

---

## Deployment Steps

### 1. Clear Caches

```bash
# Clear Python bytecode
find /Users/haniabadi/Documents/Github/options -name "*.pyc" -delete
find /Users/haniabadi/Documents/Github/options -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null
```

**Status:** [ ] Complete  
**Date/Time:** _________________  
**Notes:** _________________

---

### 2. Stop Streamlit

```bash
# Kill any running Streamlit processes
pkill -f "streamlit run"
```

**Status:** [ ] Complete  
**Date/Time:** _________________  
**Notes:** _________________

---

### 3. Run Validation Suite

```bash
cd /Users/haniabadi/Documents/Github/options
python test_tier1_enforcement.py
```

**Expected Output:**
```
🎉 ALL VALIDATION TESTS PASSED

Safety architecture verified:
  ✅ Step 7 defaults to Tier-1 only
  ✅ Step 7B enforces same safety rules
  ✅ Step 9B rejects non-Tier-1 data
  ✅ Dtype system prevents Arrow errors
  ✅ Canonical rules compliance validated
```

**Status:** [ ] Complete  
**Date/Time:** _________________  
**Test Results:** [ ] 7/7 passed  [ ] FAILED (see notes)  
**Notes:** _________________

---

### 4. Start Streamlit

```bash
streamlit run streamlit_app/dashboard.py
```

**Status:** [ ] Complete  
**Date/Time:** _________________  
**URL:** http://localhost:8501  
**Notes:** _________________

---

### 5. Verify Step 2-6 (Baseline)

Run Steps 2-6 to generate input data:
- [ ] Step 2: Clean data
- [ ] Step 3: PCS scoring
- [ ] Step 5: Chart scoring
- [ ] Step 6: GEM filtering

**Status:** [ ] Complete  
**Tickers Output:** _______ tickers  
**Notes:** _________________

---

### 6. Test Step 7 (Default Mode)

**Actions:**
1. Navigate to Step 7 section
2. Verify exploration mode checkbox is UNCHECKED
3. Click "📊 Generate Strategy Recommendations (Tier-1 Only)"

**Expected Results:**
- [ ] Button label says "(Tier-1 Only)"
- [ ] No exploration warning displayed
- [ ] Logs show: "🔒 TIER-1 FILTER: X/Y strategies are Tier-1 (executable)"
- [ ] Success message: "✅ Generated X Tier-1 executable strategies!"
- [ ] Output DataFrame has Strategy_Tier and EXECUTABLE columns
- [ ] All Strategy_Tier values are 1
- [ ] All EXECUTABLE values are True
- [ ] Output count is ~30-50% of input tickers

**Status:** [ ] Complete  
**Input Tickers:** _______  
**Output Strategies:** _______  
**Filter Ratio:** _______% (Expected: 30-50%)  
**Notes:** _________________

---

### 7. Test Step 7 (Exploration Mode)

**Actions:**
1. Check "🔍 Show all tiers (educational viewing only)"
2. Click "📊 Generate Strategy Recommendations (All Tiers - Educational)"

**Expected Results:**
- [ ] Checkbox is checked
- [ ] Warning displays: "⚠️ EXPLORATION MODE: All tiers shown (Tier-2/3 are NON-EXECUTABLE)"
- [ ] Button label says "(All Tiers - Educational)"
- [ ] Success message: "✅ Generated X strategies (Y NON-EXECUTABLE for educational viewing)"
- [ ] Output has mix of Strategy_Tier values (1, 2, 3, 999)
- [ ] Tier-1: EXECUTABLE = True
- [ ] Tier-2/3/999: EXECUTABLE = False
- [ ] Output count is higher than default mode

**Status:** [ ] Complete  
**Total Strategies:** _______  
**Non-Executable:** _______  
**Tier-1:** _______  
**Tier-2:** _______  
**Tier-3:** _______  
**Notes:** _________________

---

### 8. Test Step 7B (Default Mode)

**Actions:**
1. Disable exploration mode in Step 7 (uncheck checkbox)
2. Re-run Step 7 in default mode
3. Navigate to Step 7B section
4. Enter user profile (account size, risk, goal)
5. Click "🔀 Generate Personalized Strategies"

**Expected Results:**
- [ ] Logs show: "🔒 TIER-1 FILTER (Step 7): X/Y strategies are Tier-1"
- [ ] Logs show: "🔒 TIER-1 FILTER (Step 7B): X/Y suggestions are Tier-1 (executable)"
- [ ] Success message shows Tier-1 count
- [ ] Output has Strategy_Tier and EXECUTABLE columns
- [ ] All Strategy_Tier values are 1
- [ ] All EXECUTABLE values are True

**Status:** [ ] Complete  
**Input Tickers:** _______  
**Output Suggestions:** _______  
**All Tier-1:** [ ] Yes  [ ] No  
**Notes:** _________________

---

### 9. Test Step 7B (Exploration Mode)

**Actions:**
1. Enable exploration mode in Step 7
2. Re-run Step 7 in exploration mode
3. Navigate to Step 7B
4. Click "🔀 Generate Personalized Strategies"

**Expected Results:**
- [ ] Warning displays: "⚠️ EXPLORATION MODE"
- [ ] Success message shows non-executable count
- [ ] Output has mix of tiers
- [ ] EXECUTABLE flag properly tagged

**Status:** [ ] Complete  
**Total Suggestions:** _______  
**Non-Executable:** _______  
**Notes:** _________________

---

### 10. Test Step 9B (Success Path)

**Actions:**
1. Ensure Step 7 is in DEFAULT mode (Tier-1 only)
2. Re-run Step 7
3. Run Step 9A (DTE timeframes)
4. Run Step 9B (Fetch contracts)

**Expected Results:**
- [ ] No warnings displayed in Step 9B section
- [ ] Logs show: "🔒 STEP 9B VALIDATION: Checking Strategy_Tier column..."
- [ ] Logs show: "✅ TIER-1 VALIDATION PASSED: All X strategies are Tier-1"
- [ ] No ValueError raised
- [ ] Contracts fetched successfully (or fails with API error, not safety error)

**Status:** [ ] Complete  
**Validation Passed:** [ ] Yes  [ ] No  
**Contracts Fetched:** _______  
**Notes:** _________________

---

### 11. Test Step 9B (Rejection Path)

**Actions:**
1. Enable exploration mode in Step 7
2. Re-run Step 7 (includes Tier-2/3 strategies)
3. Navigate to Step 9B section

**Expected Results:**
- [ ] Warning displays: "⚠️ NON-EXECUTABLE STRATEGIES DETECTED"
- [ ] Warning lists count and tiers
- [ ] Warning provides instructions to fix
- [ ] If user clicks "Run Step 9B", ValueError raised
- [ ] Error message: "SAFETY VIOLATION: Step 9B detected non-Tier-1"

**Status:** [ ] Complete  
**Warning Displayed:** [ ] Yes  [ ] No  
**ValueError Raised:** [ ] Yes  [ ] No  
**Notes:** _________________

---

### 12. Verify Arrow Sanitization

**Actions:**
1. Run complete pipeline (Steps 2-7)
2. Monitor terminal logs

**Expected Results:**
- [ ] NO Arrow errors in terminal
- [ ] NO "ArrowInvalid" errors
- [ ] NO "Could not convert string[python]" errors
- [ ] All dataframes render correctly in UI
- [ ] Expanders open without errors

**Status:** [ ] Complete  
**Arrow Errors:** [ ] None  [ ] Found (see notes)  
**Notes:** _________________

---

### 13. Regression Testing

**Test A: Step 7 Column Names**
- [ ] "Context Confidence" column exists (not "Confidence")
- [ ] "Evaluation Priority" column exists (not "Option Rank")
- [ ] NO "Capital" column
- [ ] NO "Win %" column
- [ ] NO "Risk/Reward" column

**Test B: Step 7 Filters**
- [ ] Directional filter works (Bull/Bear/Neutral)
- [ ] Volatility filter works (Long Vol/Short Vol)
- [ ] Strategy name filter works
- [ ] Ticker search works

**Test C: Step 7B Display**
- [ ] User profile shown in header
- [ ] Personal recommendations display
- [ ] Tier labels say "Educational Only" for Tier-2/3
- [ ] Suitability score renders as progress bar

**Test D: Tier Toggles**
- [ ] Show Tier-1 checkbox filters correctly
- [ ] Show Tier-2 checkbox filters correctly
- [ ] Show Tier-3 checkbox filters correctly
- [ ] Unchecking all tiers shows empty table

**Status:** [ ] All tests passed  [ ] Some failed (see notes)  
**Notes:** _________________

---

### 14. Performance Check

**Measure execution times:**

**Step 7 (Default):** _______ seconds  
**Step 7 (Exploration):** _______ seconds  
**Overhead:** _______% (Expected: <5%)

**Step 7B (Default):** _______ seconds  
**Step 7B (Exploration):** _______ seconds  

**Expected Performance:**
- Step 7 (100 tickers): ~2.5 seconds
- Step 7B (400 suggestions): ~0.5 seconds

**Status:** [ ] Performance acceptable  [ ] Too slow (see notes)  
**Notes:** _________________

---

### 15. Documentation Verification

**Check that all documents are accessible:**
- [ ] `TIER1_ENFORCEMENT_SUMMARY.md` opens correctly
- [ ] `TIER1_ENFORCEMENT_TEST_PLAN.md` opens correctly
- [ ] `TIER1_ENFORCEMENT_QUICK_REFERENCE.md` opens correctly
- [ ] `test_tier1_enforcement.py` runs successfully
- [ ] `STEP7_CANONICAL_RULES.md` reflects tier enforcement

**Status:** [ ] Complete  
**Notes:** _________________

---

## Post-Deployment Verification

### 16. Production Smoke Test

Run complete pipeline end-to-end:
1. [ ] Steps 2-6 complete without errors
2. [ ] Step 7 (default) outputs Tier-1 only
3. [ ] Step 7B respects tier enforcement
4. [ ] Step 9A completes successfully
5. [ ] Step 9B validates and fetches contracts
6. [ ] No Arrow errors in logs
7. [ ] No unexpected warnings

**Status:** [ ] Complete  
**Date/Time:** _________________  
**Notes:** _________________

---

### 17. Error Monitoring

Monitor for 24 hours after deployment:
- [ ] No SAFETY VIOLATION errors (except intentional exploration mode tests)
- [ ] No Arrow serialization errors
- [ ] No dtype corruption issues
- [ ] Logs show proper tier filtering

**Monitoring Period:** _______ to _______  
**Issues Found:** [ ] None  [ ] See notes  
**Notes:** _________________

---

### 18. User Feedback

Collect feedback from users:
- [ ] Exploration mode clearly labeled
- [ ] Warnings effectively communicate restrictions
- [ ] Button labels make sense
- [ ] Error messages are helpful

**Feedback Period:** _______ to _______  
**Summary:** _________________

---

## Rollback Plan (If Needed)

### Signs That Rollback is Needed
- [ ] Step 9B incorrectly rejects all strategies
- [ ] Arrow errors prevent pipeline execution
- [ ] Performance degradation >10%
- [ ] Data corruption issues

### Rollback Steps
1. [ ] Git revert to previous commit
2. [ ] Clear caches
3. [ ] Restart Streamlit
4. [ ] Notify stakeholders

**Rollback Executed:** [ ] Yes  [ ] No  
**Date/Time:** _________________  
**Reason:** _________________

---

## Final Sign-Off

### Implementation Complete
- [ ] All deployment steps completed successfully
- [ ] All tests passed
- [ ] No critical issues found
- [ ] Documentation complete
- [ ] User feedback positive (or N/A for initial deployment)

**Deployed By:** _________________  
**Date/Time:** _________________  
**Signature:** _________________

---

### Production Ready
- [ ] System is stable
- [ ] Performance is acceptable
- [ ] No errors in logs
- [ ] Users can access all features

**Approved By:** _________________  
**Date/Time:** _________________  
**Signature:** _________________

---

## Notes & Issues

### Issues Encountered During Deployment
_________________________________________________________________________________
_________________________________________________________________________________
_________________________________________________________________________________

### Resolutions
_________________________________________________________________________________
_________________________________________________________________________________
_________________________________________________________________________________

### Follow-Up Items
_________________________________________________________________________________
_________________________________________________________________________________
_________________________________________________________________________________

---

**END OF DEPLOYMENT CHECKLIST**
