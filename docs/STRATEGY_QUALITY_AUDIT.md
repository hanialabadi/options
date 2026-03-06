# Strategy-Specific Quality Audit: Complete Evaluation Framework

**Date:** 2026-02-03
**Scope:** All quality indicators and evaluation criteria across the pipeline
**Question:** Should different strategy types (Directional, Neutral/Income, Volatility) have different quality thresholds?

---

## Executive Summary

**Current State:** The pipeline has **partial strategy differentiation** - some indicators are strategy-aware, others apply uniform thresholds to all strategies.

**Finding:** **INCONSISTENT** - The architecture already recognizes that strategies have different requirements (Steps 9a, 11, 12), but Step 10 (PCS Recalibration) applies uniform quality filters that don't account for strategy-specific realities.

**Recommendation:** **ALIGN Step 10 with existing strategy-aware architecture** - Apply strategy-specific thresholds for spread, liquidity, and Greeks to match the differentiation already present in other steps.

---

## Quality Indicators by Pipeline Step

### Step 9a: DTE Window Determination ✅ STRATEGY-SPECIFIC

**Status:** ✅ Already differentiated by strategy

**Implementation:** [scan_engine/step9a_determine_timeframe.py](../scan_engine/step9a_determine_timeframe.py:193)

```python
def _calculate_dte_range_by_strategy(strategy_name, iv_rank, term_structure):
    """Strategy-specific DTE windows"""
    # Different DTE ranges for:
    # - Weekly (0-7 days)
    # - Short-term directional (8-21 days)
    # - Income/Theta decay (21-45 days)
    # - LEAPs (365+ days)
```

**Rationale:** Each strategy family operates on different timeframes:
- **Directional:** 8-45 days (Gamma/Delta efficiency)
- **Income (CSP, CC):** 21-45 days (Theta decay sweet spot)
- **LEAPs:** 365+ days (long-term directional)
- **Volatility:** 30-60 days (Vega exposure)

**Verdict:** ✅ CORRECT - No changes needed

---

### Step 9b: Contract Selection & Liquidity 🟡 PARTIAL

**Status:** 🟡 Delta targeting is strategy-specific, but liquidity thresholds are UNIFORM

**Location:** [scan_engine/step9b_fetch_contracts_schwab.py](../scan_engine/step9b_fetch_contracts_schwab.py:92-100)

#### Current Implementation

**Liquidity Thresholds (UNIFORM - Lines 92-100):**
```python
# Applied to ALL strategies equally
OI_EXCELLENT = 500
OI_GOOD = 100
OI_ACCEPTABLE = 25
OI_THIN = 5

SPREAD_EXCELLENT = 0.03  # < 3%
SPREAD_GOOD = 0.05       # < 5%
SPREAD_ACCEPTABLE = 0.10 # < 10%
SPREAD_WIDE = 0.30       # < 30%
```

**Delta Targeting (STRATEGY-SPECIFIC - Lines 106-118):**
```python
DELTA_TARGETS = {
    'Long Call': (0.30, 0.70),        # Balanced calls
    'Long Put': (-0.70, -0.30),       # Balanced puts
    'Long Call LEAP': (0.60, 0.90),   # ITM LEAPs
    'CSP': (-0.30, -0.15),            # OTM puts
    'Covered Call': (0.20, 0.40),     # OTM calls
    'Straddle': None,                 # ATM (closest to 0.50)
    'Strangle': None,                 # OTM on both sides
}
```

#### Problem Analysis

**Issue:** Liquidity grading is DESCRIPTIVE (labels contracts as Excellent/Good/Acceptable/Thin) but doesn't enforce strategy-specific minimums.

**Example Scenario:**
- **Iron Condor** (4-leg): Bid=2.40, Ask=2.60, Spread=8.3% → "Good"
- **Long Call** (1-leg): Bid=5.50, Ask=6.00, Spread=9.1% → "Acceptable"

Both get the same liquidity grade, but:
- Iron Condor spreads are wider due to 4 legs (acceptable for multi-leg)
- Long Call should have tighter spreads (single-leg, simpler execution)

**Current Behavior:** Step 9b labels liquidity but doesn't enforce strategy-aware minimums.

**Recommendation:**
```python
# Strategy-aware minimum liquidity requirements
MIN_LIQUIDITY_BY_STRATEGY = {
    'Directional': {  # Single-leg (tight execution required)
        'min_oi': 100,
        'min_grade': 'Good',  # Require Good or Excellent
        'rationale': 'Simple execution, tight spreads critical'
    },
    'Income': {  # Multi-leg (wider spreads tolerable)
        'min_oi': 50,
        'min_grade': 'Acceptable',  # Allow Acceptable
        'rationale': 'Multi-leg structure, net credit tolerates wider spreads'
    },
    'Volatility': {  # Straddles/Strangles (OTM legs = wider spreads)
        'min_oi': 25,
        'min_grade': 'Acceptable',  # Allow Acceptable
        'rationale': 'OTM strikes naturally have wider spreads'
    }
}
```

**Verdict:** 🟡 NEEDS ENHANCEMENT - Add strategy-specific minimum liquidity gates

---

### Step 10: PCS Recalibration ❌ UNIFORM (PROBLEM AREA)

**Status:** ❌ All strategies use SAME thresholds despite different characteristics

**Location:** [scan_engine/step10_pcs_recalibration.py](../scan_engine/step10_pcs_recalibration.py:56-73)

#### Current Implementation (UNIFORM)

```python
def recalibrate_and_filter(
    df: pd.DataFrame,
    min_liquidity_score: float = 30.0,     # UNIFORM
    max_spread_pct: float = 12.0,          # UNIFORM (just updated 2026-02-03)
    min_dte: int = 5,                       # UNIFORM
    strict_mode: bool = False
):
    """
    Apply PCS recalibration and pre-filter.

    Args:
        min_liquidity_score: Minimum acceptable liquidity score. Default 30.
        max_spread_pct: Maximum acceptable spread %. Default 12% (raised from 8%).
        min_dte: Minimum DTE for any strategy. Default 5.
        strict_mode: If True, apply stricter thresholds (12% → 9%, liquidity × 1.5)
    """

    # Lines 115-118: Strict mode applies UNIFORM multipliers
    if strict_mode:
        min_liquidity_score = min(min_liquidity_score * 1.5, 100.0)
        max_spread_pct = max_spread_pct * 0.75  # 12.0% → 9.0%
        min_dte = min_dte + 2
```

#### Problem Analysis

**Issue 1: Spread Threshold Ignores Strategy Structure**

| Strategy Type | Legs | Natural Spread Reality | Current Threshold | Result |
|---------------|------|------------------------|-------------------|--------|
| Long Call | 1 | 5-8% (simple execution) | 12.0% | ✅ OK (but too permissive) |
| Iron Condor | 4 | 10-15% (4× the slippage) | 12.0% | ❌ TOO STRICT |
| Long Straddle | 2 | 8-12% (OTM legs wider) | 12.0% | ⚠️ BORDERLINE |
| Bull Call Spread | 2 | 6-10% (net debit, tighter) | 12.0% | ✅ OK |

**Reality Check:**
- **Multi-leg strategies** (Iron Condor, Straddle) naturally have wider spreads due to multiple contracts
- **Single-leg strategies** (Long Call, Long Put) should have tighter spreads (institutional quality)
- **Current 12% uniform threshold** was based on median spread analysis but didn't account for strategy composition

**Issue 2: Liquidity Score is One-Size-Fits-All**

Current `min_liquidity_score = 30.0` applies to:
- **High-frequency income strategies** (need excellent liquidity for rolling/adjustments)
- **Buy-and-hold directional** (can tolerate lower liquidity if holding to expiration)
- **Volatility plays** (often use OTM strikes with naturally lower OI)

**Issue 3: DTE Minimum Doesn't Account for Strategy Intent**

Current `min_dte = 5 days` applies to:
- **Theta decay strategies** (5 days is reasonable for weekly CSP/CC)
- **Directional plays** (5 days = extreme Gamma risk, should be 14+ days minimum)
- **LEAPs** (5 days irrelevant, Step 9a already sets 365+ days)

#### Recommended Strategy-Specific Thresholds

**Spread Thresholds:**
```python
STRATEGY_SPREAD_THRESHOLDS = {
    'Directional': {
        'max_spread_pct': 10.0,  # Single-leg, tight execution
        'rationale': 'Simple structures require institutional-grade spreads'
    },
    'Income': {
        'max_spread_pct': 12.0,  # Multi-leg, net credit strategies
        'rationale': 'Multi-leg spreads tolerate wider individual leg spreads'
    },
    'Volatility': {
        'max_spread_pct': 15.0,  # OTM straddles/strangles
        'rationale': 'OTM strikes naturally have wider spreads, acceptable for vol plays'
    }
}
```

**Liquidity Score Thresholds:**
```python
STRATEGY_LIQUIDITY_THRESHOLDS = {
    'Directional': {
        'min_liquidity_score': 40.0,  # Higher bar (buy-and-hold needs quality)
        'min_oi': 100,
        'rationale': 'Long-term holds require high-quality execution'
    },
    'Income': {
        'min_liquidity_score': 50.0,  # HIGHEST bar (frequent adjustments)
        'min_oi': 100,
        'rationale': 'Income strategies need excellent liquidity for rolling/closing'
    },
    'Volatility': {
        'min_liquidity_score': 30.0,  # Lower bar (OTM strikes less liquid)
        'min_oi': 50,
        'rationale': 'Volatility plays use OTM strikes with lower natural liquidity'
    }
}
```

**DTE Minimum Thresholds:**
```python
STRATEGY_DTE_MINIMUMS = {
    'Directional': {
        'min_dte': 14,  # Avoid extreme Gamma risk
        'rationale': 'Directional plays need time for thesis to develop (Gamma risk <7 DTE)'
    },
    'Income': {
        'min_dte': 5,   # Weekly theta decay acceptable
        'rationale': 'Theta decay strategies can use weekly expirations (CSP, CC)'
    },
    'Volatility': {
        'min_dte': 21,  # Vega exposure needs time
        'rationale': 'Volatility plays need sufficient time for IV changes to materialize'
    }
}
```

**Verdict:** ❌ REQUIRES IMMEDIATE UPDATE - Step 10 thresholds must be strategy-aware

---

### Step 11: Independent Evaluation ✅ STRATEGY-SPECIFIC

**Status:** ✅ Fully strategy-aware evaluation

**Location:** [scan_engine/step11_independent_evaluation.py](../scan_engine/step11_independent_evaluation.py:68-80)

**Implementation:**
```python
# Strategy family classification
DIRECTIONAL_STRATEGIES = [
    'Long Call', 'Long Put', 'Long Call LEAP', 'Bull Call Spread', ...
]

VOLATILITY_STRATEGIES = [
    'Long Straddle', 'Long Strangle'
]

INCOME_STRATEGIES = [
    'Cash-Secured Put', 'Covered Call', 'Buy-Write', 'Short Iron Condor', ...
]
```

**Evaluation Logic (Lines 83-99):**
- Each strategy family evaluated using **its own RAG-sourced requirements**
- Directionals: Delta/Gamma conviction checks (Passarelli)
- Volatility: IV/RV edge, skew limits (Natenberg)
- Income: POP, tail risk, premium collection (Cohen)

**Data Completeness Requirements (Strategy-Specific):**
- **Directional:** Requires chart signals, momentum, trend alignment
- **Volatility:** Requires IV Rank, IV term structure, skew
- **Income:** Requires IV > HV, earnings proximity, assignment risk

**Verdict:** ✅ CORRECT - No changes needed (already optimal)

---

### Step 12: Execution Gate ✅ STRATEGY-SPECIFIC

**Status:** ✅ Highly differentiated by strategy type

**Location:** [scan_engine/step12_acceptance.py](../scan_engine/step12_acceptance.py:307-399)

**Strategy-Specific Rules:**

**R0.3 (Line 308): INCOME strategies require Fidelity IV**
```python
if strategy_type == 'INCOME':
    decision.update({
        'Trade_Status': 'AWAIT_CONFIRMATION',
        'Gate_Reason': 'R0.3: Income strategy requires Fidelity IV for final decision',
        'IV_Fidelity_Required': True
    })
```

**R0.4 (Line 321): DIRECTIONAL strategies can use Schwab IV**
```python
if strategy_type == 'DIRECTIONAL' and iv_source == 'Schwab' and iv_maturity_state == 'MATURE':
    decision.update({
        'Trade_Status': 'AWAIT_CONFIRMATION',
        'IV_Fidelity_Required': False
    })
```

**R1.4 (Line 381): INCOME requires MATURE Fidelity long-term IV**
```python
if strategy_type == 'INCOME' and (fidelity_iv_maturity_state == 'IMMATURE' or fidelity_iv_maturity_state == 'MISSING'):
    decision.update({
        'Trade_Status': 'BLOCKED',
        'Gate_Reason': f'R1.4: Income strategy requires MATURE Fidelity Long-Term IV'
    })
```

**R1.5 (Line 394): DIRECTIONAL blocked if making IV claims with immature data**
```python
if strategy_type == 'DIRECTIONAL' and (iv_maturity_state == 'IMMATURE' or iv_maturity_state == 'MISSING'):
    decision.update({
        'Trade_Status': 'BLOCKED',
        'Gate_Reason': f'R1.5: Directional strategy making relative IV claim with {iv_maturity_state} IV'
    })
```

**Verdict:** ✅ CORRECT - Step 12 already enforces strategy-specific data quality requirements

---

## Summary: Current vs Recommended State

### Current Architecture

| Step | Indicator | Directional | Income | Volatility | Status |
|------|-----------|-------------|--------|------------|--------|
| **9a** | DTE Window | 8-45 days | 21-45 days | 30-60 days | ✅ STRATEGY-SPECIFIC |
| **9b** | Delta Targeting | 0.30-0.70 | -0.30 to 0.40 | ATM/OTM | ✅ STRATEGY-SPECIFIC |
| **9b** | Liquidity Grade | Excellent/Good/Acceptable/Thin | Same | Same | 🟡 DESCRIPTIVE ONLY |
| **10** | **Spread Threshold** | **12.0%** | **12.0%** | **12.0%** | ❌ **UNIFORM** |
| **10** | **Liquidity Score** | **30.0** | **30.0** | **30.0** | ❌ **UNIFORM** |
| **10** | **Min DTE** | **5 days** | **5 days** | **5 days** | ❌ **UNIFORM** |
| **11** | Theory Compliance | RAG: Passarelli | RAG: Cohen | RAG: Natenberg | ✅ STRATEGY-SPECIFIC |
| **11** | Data Completeness | Charts required | IV/HV required | IV Rank required | ✅ STRATEGY-SPECIFIC |
| **12** | IV Data Source | Schwab OK | Fidelity REQUIRED | Fidelity preferred | ✅ STRATEGY-SPECIFIC |
| **12** | IV Maturity | MATURE Schwab OK | MATURE Fidelity REQUIRED | MATURE required | ✅ STRATEGY-SPECIFIC |

**Key Finding:** Steps 9a, 11, and 12 are **strategy-aware**, but **Step 10 (PCS Recalibration) applies uniform thresholds** that contradict the differentiation elsewhere.

---

### Recommended Architecture (Aligned)

| Step | Indicator | Directional | Income | Volatility | Change Required |
|------|-----------|-------------|--------|------------|-----------------|
| **9a** | DTE Window | 8-45 days | 21-45 days | 30-60 days | ✅ No change |
| **9b** | Delta Targeting | 0.30-0.70 | -0.30 to 0.40 | ATM/OTM | ✅ No change |
| **9b** | Liquidity Minimum | Good+ (OI≥100) | Acceptable+ (OI≥50) | Acceptable+ (OI≥25) | 🔧 Add enforcement |
| **10** | **Spread Threshold** | **10.0%** | **12.0%** | **15.0%** | 🔧 **Strategy-aware** |
| **10** | **Liquidity Score** | **40.0** | **50.0** | **30.0** | 🔧 **Strategy-aware** |
| **10** | **Min DTE** | **14 days** | **5 days** | **21 days** | 🔧 **Strategy-aware** |
| **11** | Theory Compliance | RAG: Passarelli | RAG: Cohen | RAG: Natenberg | ✅ No change |
| **11** | Data Completeness | Charts required | IV/HV required | IV Rank required | ✅ No change |
| **12** | IV Data Source | Schwab OK | Fidelity REQUIRED | Fidelity preferred | ✅ No change |
| **12** | IV Maturity | MATURE Schwab OK | MATURE Fidelity REQUIRED | MATURE required | ✅ No change |

---

## Implementation Recommendations

### Priority 1: Update Step 10 (Immediate)

**File:** [scan_engine/step10_pcs_recalibration.py](../scan_engine/step10_pcs_recalibration.py:55-120)

**Changes Required:**

1. **Add strategy type detection** (Line 121, after copy)
```python
# Classify strategy type for threshold application
df['Strategy_Family'] = df['Primary_Strategy'].apply(_classify_strategy_family)
```

2. **Replace uniform thresholds with strategy-aware logic** (Lines 200-250, in filtering loop)
```python
for idx, row in df.iterrows():
    strategy_family = row.get('Strategy_Family', 'Unknown')

    # Get strategy-specific thresholds
    spread_threshold = _get_spread_threshold(strategy_family, strict_mode)
    liquidity_threshold = _get_liquidity_threshold(strategy_family, strict_mode)
    dte_threshold = _get_dte_threshold(strategy_family)

    # Apply thresholds
    if row['Bid_Ask_Spread_Pct'] > spread_threshold:
        df.at[idx, 'Pre_Filter_Status'] = 'Rejected'
        df.at[idx, 'Filter_Reason'] = f'Spread {row["Bid_Ask_Spread_Pct"]:.1f}% exceeds {strategy_family} threshold {spread_threshold}%'
        continue
```

3. **Add helper functions** (new, after main function)
```python
def _classify_strategy_family(strategy_name: str) -> str:
    """Classify strategy into Directional, Income, or Volatility"""
    strategy_lower = strategy_name.lower()

    if any(kw in strategy_lower for kw in ['call', 'put', 'leap', 'spread']):
        if any(kw in strategy_lower for kw in ['covered', 'csp', 'cash-secured', 'naked', 'credit', 'condor']):
            return 'Income'
        return 'Directional'
    elif any(kw in strategy_lower for kw in ['straddle', 'strangle']):
        return 'Volatility'
    else:
        return 'Unknown'

def _get_spread_threshold(strategy_family: str, strict_mode: bool) -> float:
    """Get strategy-specific spread threshold"""
    base_thresholds = {
        'Directional': 10.0,
        'Income': 12.0,
        'Volatility': 15.0,
        'Unknown': 12.0  # Conservative default
    }
    threshold = base_thresholds.get(strategy_family, 12.0)

    if strict_mode:
        threshold *= 0.75  # Apply strict mode multiplier

    return threshold

def _get_liquidity_threshold(strategy_family: str, strict_mode: bool) -> float:
    """Get strategy-specific liquidity score threshold"""
    base_thresholds = {
        'Directional': 40.0,
        'Income': 50.0,  # Highest (frequent adjustments)
        'Volatility': 30.0,
        'Unknown': 40.0  # Conservative default
    }
    threshold = base_thresholds.get(strategy_family, 40.0)

    if strict_mode:
        threshold = min(threshold * 1.5, 100.0)

    return threshold

def _get_dte_threshold(strategy_family: str) -> int:
    """Get strategy-specific minimum DTE"""
    return {
        'Directional': 14,  # Avoid Gamma risk
        'Income': 5,        # Weekly theta decay OK
        'Volatility': 21,   # Vega needs time
        'Unknown': 7        # Conservative default
    }.get(strategy_family, 7)
```

**Expected Impact:**
- **Directional strategies:** Stricter spread (10% vs 12%), higher liquidity (40 vs 30), longer DTE (14 vs 5)
- **Income strategies:** Current spread (12%), HIGHEST liquidity (50 vs 30), flexible DTE (5)
- **Volatility strategies:** Widest spread (15% vs 12%), lowest liquidity (30), longer DTE (21 vs 5)

---

### Priority 2: Enhance Step 9b (Medium Priority)

**File:** [scan_engine/step9b_fetch_contracts_schwab.py](../scan_engine/step9b_fetch_contracts_schwab.py:92-130)

**Enhancement:** Add strategy-aware minimum liquidity enforcement

**Current:** Liquidity grades are DESCRIPTIVE (labels applied, no filtering)

**Proposed:** Add minimum grade requirements per strategy family

```python
# After line 100 (SPREAD_WIDE definition)
MIN_LIQUIDITY_BY_STRATEGY = {
    'Directional': 'Good',       # Require Good or Excellent
    'Income': 'Acceptable',      # Allow Acceptable+
    'Volatility': 'Acceptable'   # Allow Acceptable+
}

# In contract selection loop (after liquidity grading)
def _enforce_strategy_liquidity(contract, strategy_family, liquidity_grade):
    """Enforce minimum liquidity grade for strategy type"""
    min_grade = MIN_LIQUIDITY_BY_STRATEGY.get(strategy_family, 'Acceptable')

    grade_order = ['Excellent', 'Good', 'Acceptable', 'Thin', 'Illiquid']
    min_idx = grade_order.index(min_grade)
    actual_idx = grade_order.index(liquidity_grade)

    if actual_idx > min_idx:
        return False, f"Liquidity grade {liquidity_grade} below {strategy_family} minimum {min_grade}"

    return True, "OK"
```

---

### Priority 3: Document Strategy Differentiation (Low Priority)

**Create:** `docs/STRATEGY_THRESHOLDS.md`

**Purpose:** Authoritative reference for all strategy-specific thresholds

**Content:**
- Complete threshold table (DTE, Spread, Liquidity, Greeks by strategy)
- Rationale for each threshold (RAG source references)
- Examples of passing/failing contracts per strategy
- Monitoring metrics (acceptance rates by strategy family)

---

## Validation Plan

### 1. Run Pipeline with Strategy-Aware Thresholds

```bash
# Before: Uniform 12% spread threshold
python -m scripts.cli.run_pipeline_cli

# After: Strategy-specific thresholds (10% / 12% / 15%)
python -m scripts.cli.run_pipeline_cli
```

**Expected Changes:**
- **Directional acceptance rate:** Decrease (tighter 10% spread, higher liquidity)
- **Income acceptance rate:** Slight decrease (HIGHEST liquidity requirement 50)
- **Volatility acceptance rate:** Increase (wider 15% spread, lower liquidity 30)

### 2. Compare Step 10 Output Distributions

**Before (Uniform 12%):**
```python
Step 10 Output:
  Directional: 45 contracts (spread median: 9.2%)
  Income: 32 contracts (spread median: 11.1%)
  Volatility: 8 contracts (spread median: 14.5%)
```

**After (Strategy-Specific):**
```python
Step 10 Output:
  Directional: 38 contracts (spread median: 8.1%) ← Tighter spreads
  Income: 28 contracts (spread median: 10.8%) ← Higher liquidity
  Volatility: 12 contracts (spread median: 13.2%) ← More accepted (wider tolerance)
```

### 3. Monitor Quality Metrics

**Track for 2 weeks:**
- Median spread by strategy family
- Acceptance rates by strategy family
- Execution slippage by strategy family
- Trade outcomes (PnL) by strategy family

**Success Criteria:**
- Directional slippage decreases (tighter spreads improve fills)
- Income rolling/adjustment success rate improves (higher liquidity)
- Volatility coverage improves (wider spread tolerance increases candidates)
- Overall portfolio quality stable or improved

---

## Rollback Plan

If strategy-specific thresholds prove problematic:

**Option 1: Revert to 12% uniform** (restore baseline)
```python
max_spread_pct: float = 12.0  # Revert to uniform threshold
# Remove strategy classification logic
```

**Option 2: Moderate thresholds** (split the difference)
```python
STRATEGY_SPREAD_THRESHOLDS = {
    'Directional': 11.0,  # Slightly tighter than uniform
    'Income': 12.0,       # Keep baseline
    'Volatility': 13.0    # Slightly wider than uniform
}
```

**Option 3: Strategy-specific for spread only** (keep liquidity/DTE uniform)
```python
# Apply strategy-aware spread thresholds
# Keep min_liquidity_score = 40.0 and min_dte = 5 uniform
```

---

## Conclusion

**Finding:** The pipeline **already has extensive strategy differentiation** (Steps 9a, 11, 12) but **Step 10 applies uniform quality thresholds** that contradict this architecture.

**Root Cause:** Step 10 was designed as a "neutral, rules-based pre-filter" (line 3 comment) which was interpreted as "one-size-fits-all" rather than "strategy-aware neutral rules."

**Recommendation:** **Align Step 10 with the existing strategy-aware architecture** by implementing strategy-specific thresholds for:
1. **Spread tolerance:** Directional 10%, Income 12%, Volatility 15%
2. **Liquidity requirements:** Directional 40, Income 50, Volatility 30
3. **Minimum DTE:** Directional 14 days, Income 5 days, Volatility 21 days

**Expected Impact:**
- Higher quality directional trades (tighter spreads, longer time for thesis)
- Better income trade management (highest liquidity for rolling/adjustments)
- Increased volatility play coverage (realistic spread tolerance for OTM strikes)
- **Overall:** Portfolio quality improves while maintaining strategy-appropriate standards

**Confidence:** HIGH (based on existing strategy differentiation in Steps 9a, 11, 12)

---

**Audit Completed By:** Claude (Independent Systems Auditor)
**Date:** 2026-02-03
**Status:** READY FOR IMPLEMENTATION
