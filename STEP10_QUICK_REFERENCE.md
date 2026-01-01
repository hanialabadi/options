# Step 10 Quick Reference

## One-Line Summary
**Neutral, rules-based pre-filter that validates option contracts from Step 9B before execution**

---

## Quick Usage

```python
from scan_engine import recalibrate_and_filter

# Default settings
filtered = recalibrate_and_filter(step9b_df)

# Conservative (strict mode)
filtered = recalibrate_and_filter(step9b_df, strict_mode=True)

# Custom thresholds
filtered = recalibrate_and_filter(
    step9b_df,
    min_liquidity_score=30.0,
    max_spread_pct=8.0,
    min_dte=5
)
```

---

## Status Meanings

| Status | Meaning | Execution |
|--------|---------|-----------|
| ✅ **Valid** | All checks passed | Ready |
| ⚠️ **Watch** | Non-critical issues | Review |
| ❌ **Rejected** | Critical failures | Don't trade |

---

## PCS Score Ranges

- **70-100:** High quality, strong execution candidates
- **50-70:** Moderate quality, review recommended
- **30-50:** Borderline, likely Watch/Rejected
- **0-30:** Poor quality, likely Rejected

---

## Default Thresholds

| Parameter | Default | Strict Mode |
|-----------|---------|-------------|
| min_liquidity_score | 30.0 | 45.0 |
| max_spread_pct | 8.0% | 5.6% |
| min_dte | 5 days | 7 days |

---

## 5 Validation Rules

1. **Liquidity:** Score ≥ 30, Spread ≤ 8%
2. **DTE:** ≥ 5 days (LEAPS ≥ 90)
3. **Structure:** Calendar/Diagonal = Watch
4. **Risk Model:** Validate appropriate risk representation
5. **Strategy-Specific:** OI/risk checks per strategy

---

## Output Columns Added

- `Pre_Filter_Status`: Valid / Watch / Rejected
- `Filter_Reason`: Explanation string
- `PCS_Score`: Quality score 0-100
- `Execution_Ready`: Boolean
- `Contract_Intent`: Upgraded to 'Execution_Candidate' if Valid

---

## Common Commands

```python
# Get execution-ready contracts
ready = filtered[filtered['Execution_Ready'] == True]

# Review Watch contracts
watch = filtered[filtered['Pre_Filter_Status'] == 'Watch']

# Check rejection reasons
rejected = filtered[filtered['Pre_Filter_Status'] == 'Rejected']
print(rejected['Filter_Reason'].value_counts())

# Analyze score distribution
print(filtered['PCS_Score'].describe())
```

---

## Pipeline Integration

```python
from scan_engine.pipeline import run_full_scan_pipeline

results = run_full_scan_pipeline(
    include_step9b=True,
    include_step10=True,
    pcs_strict_mode=False  # Set True for conservative
)

# CSV exports:
# - Step9B_Chain_Scan_YYYYMMDD_HHMMSS.csv
# - Step10_Filtered_YYYYMMDD_HHMMSS.csv
```

---

## Testing

```bash
python test_step10.py
# Expected: 7/7 tests passing ✅
```

---

## When to Use Strict Mode

**Use strict_mode=True for:**
- Live automated execution
- Risk-averse trading
- High-stakes accounts
- Unfamiliar underlyings

**Use strict_mode=False for:**
- Manual review/discretion
- Established liquidity profiles
- Research/backtesting
- Learning mode

---

## Troubleshooting Quick Fixes

**All Rejected:** Lower `min_liquidity_score` to 20.0, raise `max_spread_pct` to 12.0
**Too Many Watch:** Review `Filter_Reason`, manually approve acceptable ones
**Low PCS Scores:** Check Step 9B liquidity calculation, verify underlying liquidity

---

## Documentation

- **User Guide:** [STEP10_DOCUMENTATION.md](STEP10_DOCUMENTATION.md)
- **Test Results:** [STEP10_TEST_RESULTS.md](STEP10_TEST_RESULTS.md)
- **Implementation:** [STEP10_IMPLEMENTATION_SUMMARY.md](STEP10_IMPLEMENTATION_SUMMARY.md)
- **Code:** [step10_pcs_recalibration.py](core/scan_engine/step10_pcs_recalibration.py)

---

## Status
✅ **Production Ready** - Version 1.0.0
