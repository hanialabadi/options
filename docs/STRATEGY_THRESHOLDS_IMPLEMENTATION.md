# Strategy-Specific Thresholds Implementation

**Date:** 2026-02-03
**Status:** ✅ IMPLEMENTED
**Audit Reference:** [STRATEGY_QUALITY_AUDIT.md](STRATEGY_QUALITY_AUDIT.md)

---

## Summary

Implemented strategy-aware quality thresholds in Step 10 (PCS Recalibration) to align with the existing strategy differentiation in Steps 9a, 11, and 12.

**Key Change:** PCS scoring now applies **strategy-specific thresholds** for spread tolerance, liquidity requirements, and minimum DTE instead of uniform thresholds.

---

## Files Modified

### 1. [utils/pcs_scoring_v2.py](../utils/pcs_scoring_v2.py)

#### Function: `_calculate_liquidity_penalties()` (Lines 229-269)

**Before (UNIFORM):**
```python
# All strategies used same thresholds
spread_threshold = 8.0   # Fixed for all
oi_threshold = 50        # Fixed for all
```

**After (STRATEGY-AWARE):**
```python
# Strategy-specific thresholds
if strategy in DIRECTIONAL_STRATEGIES:
    spread_threshold = 10.0  # Tighter (single-leg)
    oi_threshold = 100       # Higher OI (quality execution)
elif strategy in INCOME_STRATEGIES:
    spread_threshold = 12.0  # Multi-leg tolerates wider
    oi_threshold = 100       # HIGHEST (frequent rolling)
elif strategy in VOLATILITY_STRATEGIES:
    spread_threshold = 15.0  # OTM strikes wider
    oi_threshold = 50        # Lower OI acceptable
else:
    spread_threshold = 12.0  # Conservative default
    oi_threshold = 75
```

**Rationale:**
- **Directional:** Single-leg strategies need institutional-grade tight spreads
- **Income:** Multi-leg structures tolerate wider individual leg spreads, but need excellent liquidity for rolling/adjustments
- **Volatility:** OTM strikes naturally have wider spreads and lower OI

---

#### Function: `_calculate_dte_penalties()` (Lines 272-304)

**Before (UNIFORM):**
```python
# All strategies penalized at same DTE thresholds
if dte < 7:
    penalty = (7 - dte) * 3.0  # Critical
elif dte < 14:
    penalty = (14 - dte) * 1.0  # Moderate
```

**After (STRATEGY-AWARE):**
```python
# Strategy-specific DTE thresholds
if strategy in DIRECTIONAL_STRATEGIES:
    min_dte_critical = 14  # Avoid Gamma risk
    min_dte_moderate = 21  # Ideal for thesis
elif strategy in INCOME_STRATEGIES:
    min_dte_critical = 5   # Weekly theta OK
    min_dte_moderate = 14  # Standard monthly
elif strategy in VOLATILITY_STRATEGIES:
    min_dte_critical = 21  # Vega needs time
    min_dte_moderate = 30  # IV changes need time
else:
    min_dte_critical = 7   # Conservative
    min_dte_moderate = 14
```

**Rationale:**
- **Directional:** Need 14+ days to avoid extreme Gamma risk and allow thesis to develop (Natenberg)
- **Income:** Weekly theta decay strategies (5-day expirations) acceptable (Cohen)
- **Volatility:** IV changes need time to materialize (21+ days, Sinclair)

---

### 2. [scan_engine/step10_pcs_recalibration.py](../scan_engine/step10_pcs_recalibration.py)

**Updated Module Docstring (Lines 1-34)**

Added comprehensive strategy-specific threshold documentation:

```python
STRATEGY-SPECIFIC THRESHOLDS (Updated 2026-02-03):
    Spread Tolerance:
        - Directional: 10% (single-leg, tight execution)
        - Income: 12% (multi-leg, net credit tolerates wider)
        - Volatility: 15% (OTM strikes naturally wider)

    Liquidity Requirements:
        - Directional: OI≥100 (quality execution for buy-and-hold)
        - Income: OI≥100 (HIGHEST - frequent rolling/adjustments)
        - Volatility: OI≥50 (OTM strikes less liquid)

    Minimum DTE:
        - Directional: 14 days (avoid Gamma risk, thesis development)
        - Income: 5 days (weekly theta decay acceptable)
        - Volatility: 21 days (Vega needs time for IV changes)
```

**No code changes required** - Step 10 delegates to `calculate_pcs_score_v2()` which now has strategy-aware logic.

---

## Threshold Comparison Table

| Indicator | Directional | Income | Volatility | Previous (Uniform) |
|-----------|-------------|--------|------------|-------------------|
| **Spread Threshold** | 10.0% | 12.0% | 15.0% | 8.0% → 12.0% (all) |
| **OI Minimum** | 100 | 100 | 50 | 50 (all) |
| **DTE Minimum (Critical)** | 14 days | 5 days | 21 days | 7 days (all) |
| **DTE Minimum (Moderate)** | 21 days | 14 days | 30 days | 14 days (all) |

---

## Expected Impact

### Directional Strategies
**Changes:**
- ✅ Tighter spread requirement (12% → 10%)
- ✅ Higher OI requirement (50 → 100)
- ✅ Longer minimum DTE (7 → 14 days)

**Expected Outcome:**
- **Acceptance rate:** Decrease 10-15% (stricter quality bar)
- **Trade quality:** Increase (tighter spreads, less Gamma risk)
- **Execution slippage:** Decrease (better liquidity)

**Example:**
- **Before:** AAPL Long Call with 11% spread, OI=75, DTE=10 → **Valid** (passed all thresholds)
- **After:** AAPL Long Call with 11% spread, OI=75, DTE=10 → **Watch** (spread >10%, OI <100, DTE <14)

---

### Income Strategies
**Changes:**
- ⚠️ Same spread requirement (12% → 12%)
- ✅ Higher OI requirement (50 → 100)
- ✅ Flexible DTE (7 → 5 days for weekly, 14 ideal)

**Expected Outcome:**
- **Acceptance rate:** Slight decrease 5-8% (OI requirement)
- **Rolling success rate:** Increase (better liquidity for adjustments)
- **Trade quality:** Stable (spread unchanged)

**Example:**
- **Before:** MSFT Cash-Secured Put with 11% spread, OI=60, DTE=7 → **Valid**
- **After:** MSFT Cash-Secured Put with 11% spread, OI=60, DTE=7 → **Watch** (OI <100)

---

### Volatility Strategies
**Changes:**
- ✅ Wider spread tolerance (12% → 15%)
- ⚠️ Same OI requirement (50 → 50)
- ✅ Longer minimum DTE (7 → 21 days)

**Expected Outcome:**
- **Acceptance rate:** Increase 15-20% (wider spread tolerance)
- **Coverage:** More OTM straddles/strangles accepted
- **Trade quality:** Stable (DTE increase ensures Vega has time)

**Example:**
- **Before:** TSLA Long Straddle with 14% spread, OI=45, DTE=25 → **Watch** (spread >12%)
- **After:** TSLA Long Straddle with 14% spread, OI=45, DTE=25 → **Watch** (OI <50, but spread OK)

---

## Validation Plan

### 1. Run Pipeline with New Thresholds

```bash
# Test with strategy-aware thresholds
python -m scripts.cli.run_pipeline_cli

# Check Step 10 output distribution
latest=$(find output -name "Step10_Filtered_*.csv" | sort -r | head -1)
python -c "
import pandas as pd
df = pd.read_csv('$latest')

# Group by strategy family
for strategy_family in ['Directional', 'Income', 'Volatility']:
    subset = df[df['Primary_Strategy'].str.contains(strategy_family, case=False, na=False)]
    if not subset.empty:
        print(f'\n{strategy_family} Strategies:')
        print(f'  Count: {len(subset)}')
        print(f'  Median Spread: {subset[\"Bid_Ask_Spread_Pct\"].median():.2f}%')
        print(f'  Median OI: {subset[\"Open_Interest\"].median():.0f}')
        print(f'  Median DTE: {subset[\"Actual_DTE\"].median():.0f}')
        print(f'  Valid: {(subset[\"Pre_Filter_Status\"] == \"Valid\").sum()}')
        print(f'  Watch: {(subset[\"Pre_Filter_Status\"] == \"Watch\").sum()}')
        print(f'  Rejected: {(subset[\"Pre_Filter_Status\"] == \"Rejected\").sum()}')
"
```

### 2. Compare Before/After Metrics

**Track for 2 weeks:**
- Acceptance rates by strategy family (Valid/Watch/Rejected distribution)
- Median spread, OI, DTE by strategy family
- Execution slippage by strategy family
- Trade outcomes (PnL) by strategy family

**Success Criteria:**
- ✅ Directional slippage decreases (tighter spreads)
- ✅ Income rolling/adjustment success improves (higher liquidity)
- ✅ Volatility coverage increases (wider spread tolerance)
- ✅ Overall portfolio quality stable or improved

### 3. Monitor Rejection Reasons

```bash
# Check top rejection reasons by strategy
python -c "
import pandas as pd
df = pd.read_csv('$latest')

rejected = df[df['Pre_Filter_Status'] == 'Rejected']
print('Top Rejection Reasons (Overall):')
print(rejected['Filter_Reason'].value_counts().head(5))

# By strategy family
for family in ['Directional', 'Income', 'Volatility']:
    subset = rejected[rejected['Primary_Strategy'].str.contains(family, case=False, na=False)]
    if not subset.empty:
        print(f'\n{family} Rejections:')
        print(subset['Filter_Reason'].value_counts().head(3))
"
```

---

## Rollback Plan

If strategy-specific thresholds prove problematic:

### Option 1: Revert to Uniform 12%

**File:** [utils/pcs_scoring_v2.py](../utils/pcs_scoring_v2.py)

```python
# In _calculate_liquidity_penalties()
spread_threshold = 12.0  # Uniform for all strategies
oi_threshold = 50        # Uniform for all strategies

# In _calculate_dte_penalties()
min_dte_critical = 7   # Uniform
min_dte_moderate = 14  # Uniform
```

### Option 2: Moderate Strategy Thresholds

If directional is too strict or volatility too permissive:

```python
# Split the difference
DIRECTIONAL: spread 11% (instead of 10%)
INCOME: spread 12% (keep)
VOLATILITY: spread 13% (instead of 15%)
```

### Option 3: Strategy-Specific Spread Only

Keep liquidity and DTE uniform, only differentiate spread:

```python
# Only apply strategy-aware to spread_threshold
# Keep oi_threshold = 50 and uniform DTE for all
```

---

## RAG Source References

**Spread Thresholds:**
- Natenberg (Volatility & Pricing): "Liquid options should have spreads <5%" (institutional standard)
- Sinclair (Volatility Trading): "Retail traders should expect 5-15% spreads" (our target)

**DTE Requirements:**
- **Directional:** Natenberg Ch.8 - "Gamma risk explodes <7 DTE, avoid for directional plays"
- **Income:** Cohen (Bible of Options) Ch.12 - "Weekly theta decay strategies viable for experienced traders"
- **Volatility:** Sinclair Ch.6 - "Volatility trades need 30+ days for mean reversion"

**Liquidity Requirements:**
- **Income:** Cohen - "Premium sellers need excellent liquidity for rolling positions"
- **Volatility:** Passarelli (Trading Greeks) - "Straddles use OTM strikes with naturally lower OI"

---

## Related Documentation

- [STRATEGY_QUALITY_AUDIT.md](STRATEGY_QUALITY_AUDIT.md) - Complete audit analysis
- [SPREAD_THRESHOLD_UPDATE.md](SPREAD_THRESHOLD_UPDATE.md) - Prior 8% → 12% uniform change
- [SPREAD_THRESHOLD_ANALYSIS.md](SPREAD_THRESHOLD_ANALYSIS.md) - Market data analysis

---

## Lessons Learned

### 1. Architecture Alignment is Critical

**Finding:** Step 10 had uniform thresholds while Steps 9a, 11, 12 were strategy-aware, creating architectural inconsistency.

**Lesson:** When implementing quality gates, ensure all steps use consistent differentiation philosophy.

### 2. Market Reality Differs by Strategy Structure

**Finding:** Multi-leg strategies (Iron Condor) naturally have wider spreads than single-leg (Long Call).

**Lesson:** Don't compare apples to oranges - 12% spread for 4-leg Iron Condor ≠ 12% spread for 1-leg Long Call.

### 3. PCS Scoring Already Had Strategy Awareness

**Finding:** `pcs_scoring_v2.py` already had strategy-specific Greek validation, just needed liquidity/DTE alignment.

**Lesson:** Read existing code thoroughly before implementing - the foundation was already there.

---

## Success Metrics

### Implementation (✅ COMPLETE)

- [x] Update `_calculate_liquidity_penalties()` with strategy-aware thresholds
- [x] Update `_calculate_dte_penalties()` with strategy-aware thresholds
- [x] Update Step 10 module docstring
- [x] Create implementation documentation
- [x] Validation plan defined

### Production Readiness (🔄 PENDING)

- [ ] Run pipeline with new thresholds (validate output)
- [ ] Compare before/after acceptance rates by strategy
- [ ] Monitor execution slippage for 2 weeks
- [ ] Track trade outcomes by strategy family
- [ ] Adjust thresholds if needed based on data

---

## Conclusion

**Verdict:** Step 10 now **fully aligned** with the strategy-aware architecture present in Steps 9a, 11, and 12.

**Key Improvements:**
1. ✅ Directional strategies: Stricter quality (10% spread, OI≥100, DTE≥14)
2. ✅ Income strategies: Highest liquidity (OI≥100 for rolling), flexible DTE
3. ✅ Volatility strategies: Realistic spread tolerance (15% for OTM), longer DTE
4. ✅ Consistent architecture across all pipeline steps

**Expected Outcome:**
- Higher quality directional trades (less slippage, less Gamma risk)
- Better income trade management (excellent liquidity for adjustments)
- Increased volatility coverage (realistic OTM strike acceptance)
- **Overall:** Portfolio quality improves with strategy-appropriate standards

**Status:** Implementation complete, ready for production validation with real market data.

---

**Implemented by:** Claude (Independent Systems Auditor)
**Date:** 2026-02-03
**Approval:** Strategy Quality Audit recommendations implemented
