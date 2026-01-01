# Tier-1 Enforcement Quick Reference

**Version:** 1.0  
**Date:** 2025-12-27

---

## Quick Deploy

```bash
# Clear caches
find . -name "*.pyc" -delete && find . -name "__pycache__" -delete

# Restart Streamlit
pkill -f "streamlit run" && streamlit run streamlit_app/dashboard.py

# Run validation
python test_tier1_enforcement.py
```

---

## Safety Architecture

```
┌──────────────────────────────────────────┐
│ STEP 7: Strategy Generation              │
│ tier_filter='tier1_only' (DEFAULT)       │
│ → Filters to Strategy_Tier == 1          │
│ → Tags EXECUTABLE = True/False           │
└──────────────────────────────────────────┘
                  ↓
┌──────────────────────────────────────────┐
│ STEP 7B: Multi-Strategy Ranker           │
│ Same tier_filter enforcement             │
│ → Filters suggestions to Tier-1          │
└──────────────────────────────────────────┘
                  ↓
┌──────────────────────────────────────────┐
│ STEP 9B: Contract Fetching               │
│ Validates Strategy_Tier == 1             │
│ → ValueError if non-Tier-1 detected      │
│ → HARD STOP - cannot be bypassed         │
└──────────────────────────────────────────┘
```

---

## Parameters

### Step 7 & Step 7B

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `tier_filter` | str | `'tier1_only'` | Filter mode: 'tier1_only', 'include_tier2', 'all_tiers' |
| `exploration_mode` | bool | `False` | Show all tiers for educational viewing |

### Usage

```python
# Default (safe execution path)
recommend_strategies(df, tier_filter='tier1_only', exploration_mode=False)

# Exploration mode (educational)
recommend_strategies(df, tier_filter='all_tiers', exploration_mode=True)
```

---

## Tiers Reference

| Tier | Definition | Examples | EXECUTABLE | Step 9B |
|------|------------|----------|------------|---------|
| 1 | Broker-approved + logic-ready | Long calls/puts, covered calls, CSP, straddles | ✅ True | ✅ Allowed |
| 2 | Broker-blocked (approval needed) | Spreads, iron condors, butterflies | ❌ False | ❌ Rejected |
| 3 | Logic-blocked (system limitation) | Calendars, diagonals, PMCC, LEAPs | ❌ False | ❌ Rejected |
| 999 | Unknown (incomplete data) | Missing tier assignment | ❌ False | ❌ Rejected |

---

## Log Messages

### Step 7 (Default)
```
🔒 TIER-1 FILTER: 45/127 strategies are Tier-1 (executable)
```

### Step 7 (Exploration)
```
(No TIER-1 FILTER log - all strategies pass through)
```

### Step 7B
```
🔒 TIER-1 FILTER (Step 7B): 336/428 suggestions are Tier-1 (executable)
```

### Step 9B (Success)
```
✅ TIER-1 VALIDATION PASSED: All 45 strategies are Tier-1
```

### Step 9B (Rejection)
```
❌ REJECTED: Non-Tier-1 strategies detected
ValueError: SAFETY VIOLATION: Step 9B detected non-Tier-1
```

---

## UI States

### Default Mode (Safe)
- Button: "📊 Generate Strategy Recommendations (Tier-1 Only)"
- No warnings
- Success: "✅ Generated X Tier-1 executable strategies!"

### Exploration Mode (Educational)
- Checkbox: "🔍 Show all tiers (educational viewing only)"
- Warning: "⚠️ EXPLORATION MODE: All tiers shown (Tier-2/3 are NON-EXECUTABLE)"
- Button: "📊 Generate Strategy Recommendations (All Tiers - Educational)"
- Success: "✅ Generated X strategies (Y NON-EXECUTABLE for educational viewing)"

---

## Validation Tests

```bash
$ python test_tier1_enforcement.py

Expected Output:
✅ PASS  Step 7 Parameters
✅ PASS  Step 7B Parameters
✅ PASS  Step 9B Rejection
✅ PASS  Step 9B Acceptance
✅ PASS  Dtype Initialization
✅ PASS  Arrow Sanitization
✅ PASS  Canonical Rules

🎉 ALL VALIDATION TESTS PASSED
```

---

## Common Issues

### Step 7 not filtering
**Check:** Logs should show "🔒 TIER-1 FILTER"  
**Fix:** Verify tier_filter='tier1_only' and exploration_mode=False

### Step 9B accepts non-Tier-1
**Check:** Should see ValueError with "SAFETY VIOLATION"  
**Fix:** Verify Strategy_Tier column exists in input data

### Arrow errors
**Check:** Error message shows column name  
**Fix:** Apply sanitize_for_arrow() before session_state store

### Exploration mode not working
**Check:** Checkbox should update button label  
**Fix:** Clear browser cache, restart Streamlit

---

## File Locations

| File | Purpose |
|------|---------|
| `core/scan_engine/step7_strategy_recommendation.py` | Step 7 safety gate |
| `core/scan_engine/step7b_multi_strategy_ranker.py` | Step 7B safety gate |
| `core/scan_engine/step9b_fetch_contracts.py` | Step 9B validation gate |
| `streamlit_app/dashboard.py` | UI controls & exploration mode |
| `test_tier1_enforcement.py` | Automated validation |
| `TIER1_ENFORCEMENT_TEST_PLAN.md` | Comprehensive test guide |
| `TIER1_ENFORCEMENT_SUMMARY.md` | Full implementation details |
| `STEP7_CANONICAL_RULES.md` | Architectural rules |

---

## Key Functions

### Step 7
```python
recommend_strategies(df, tier_filter='tier1_only', exploration_mode=False)
```
**Returns:** DataFrame with Strategy_Tier and EXECUTABLE columns

### Step 7B
```python
generate_multi_strategy_suggestions(
    df, 
    tier_filter='tier1_only', 
    exploration_mode=False,
    ...
)
```
**Returns:** DataFrame with filtered suggestions, EXECUTABLE tags

### Step 9B
```python
fetch_and_select_contracts(df)
```
**Validates:** All rows have Strategy_Tier == 1  
**Raises:** ValueError if non-Tier-1 detected

---

## Debug Commands

```python
# Check tier distribution
df['Strategy_Tier'].value_counts()

# Check EXECUTABLE flag
df['EXECUTABLE'].value_counts()

# Check dtypes
df.dtypes

# Find object columns
[col for col, dtype in df.dtypes.items() if dtype == 'object']

# Test Step 9B validation
from core.scan_engine.step9b_fetch_contracts import fetch_and_select_contracts
fetch_and_select_contracts(df_test)  # Should raise ValueError if non-Tier-1
```

---

## Performance

- **Overhead:** <5% (~100ms per operation)
- **Step 7 (100 tickers):** ~2.5 seconds
- **Step 7B (400 suggestions):** ~0.5 seconds
- **Memory:** Negligible impact

---

## Support

**GitHub Issues:** Include:
- Test case that failed
- Full error message
- Log output
- Reproduction steps

**Documentation:**
- Architecture: `STEP7_CANONICAL_RULES.md`
- Tests: `TIER1_ENFORCEMENT_TEST_PLAN.md`
- Details: `TIER1_ENFORCEMENT_SUMMARY.md`

---

## Status

✅ **Production Ready**  
✅ **All Tests Passed (7/7)**  
✅ **Documentation Complete**  

---

**Last Updated:** 2025-12-27  
**Version:** 1.0
