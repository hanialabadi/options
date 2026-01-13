# PCS RAG COMPLIANCE AUDIT
**Date:** January 4, 2026  
**Purpose:** Audit current PCS implementation against RAG requirements for Entry_PCS and Current_PCS (Phase 2)

---

## 🎯 EXECUTIVE SUMMARY

**✅ CURRENT STATUS: Entry_PCS Complete, Current_PCS Needs Enhancements**

### What's Working
- ✅ **Entry_PCS (Phase D.1)**: Frozen baseline using entry Greeks, profile-based weighting
- ✅ **Basic Current_PCS**: Gamma/Vega/ROI scoring with strategy profiles
- ✅ **Core Weights**: Align with RAG principles (volatility-focused for vol plays, ROI for income)

### What's Missing for Full Current_PCS (Phase 2)
- ⚠️ **IV_Rank** (RAG: 252-day percentile) - Not yet integrated into Current_PCS
- ⚠️ **Liquidity Metrics** (RAG: OI, Volume, Spread) - Not in Current_PCS formula
- ⚠️ **Days_In_Trade / Time-Series** - Not yet in Current_PCS scoring
- ⚠️ **Chart Signal Integration** (RAG: Regime, Signal_Type) - Phase 7+ only
- ⚠️ **Multi-Factor Weighting** (RAG: Volatility 30%, Chart 25%, Liquidity 25%, Greeks 20%)

---

## 📊 RAG REQUIREMENTS VS CURRENT IMPLEMENTATION

### **Entry_PCS (Phase D.1 - Complete ✅)**

| **Component** | **RAG Requirement** | **Current Implementation** | **Status** |
|--------------|-------------------|---------------------------|------------|
| **Purpose** | Frozen baseline at entry | ✅ Frozen at first_seen using Entry Greeks | ✅ **COMPLIANT** |
| **Inputs** | Entry Greeks, Entry IV_Rank, Strategy | ✅ Gamma_Entry, Vega_Entry, Strategy | ✅ **COMPLIANT** |
| **ROI Scoring** | Premium/Basis at entry | ✅ Premium_Entry / Basis | ✅ **COMPLIANT** |
| **Profile Weighting** | Strategy-specific weights | ✅ NEUTRAL_VOL, INCOME, DIRECTIONAL | ✅ **COMPLIANT** |
| **Gamma Weight** | Varies by profile (0.2-0.5) | ✅ 0.2-0.5 depending on profile | ✅ **COMPLIANT** |
| **Vega Weight** | Higher for vol strategies (0.6) | ✅ 0.6 for NEUTRAL_VOL | ✅ **COMPLIANT** |
| **ROI Weight** | Higher for income (0.5) | ✅ 0.5 for INCOME strategies | ✅ **COMPLIANT** |
| **Scoring Range** | Not specified in RAG | ✅ 0-65 (gamma 0-25, vega 0-20, roi 0-20) | ✅ **ACCEPTABLE** |
| **Idempotent** | Never changes after freeze | ✅ Frozen at first_seen | ✅ **COMPLIANT** |

**Verdict:** ✅ **Entry_PCS is RAG-compliant and production-ready**

---

### **Current_PCS (Phase 2 - Partial Implementation ⚠️)**

#### Current Implementation (pcs_score.py)

```python
# Current formula (simplified)
gamma_score = min(Gamma * 1500, 25)
vega_score = min(Vega * 5000, 25)
roi_score = tiered(Premium/Basis, thresholds=[0.30, 0.20])

# Profile-based weighting
if profile == NEUTRAL_VOL:
    PCS = vega * 0.6 + gamma * 0.25 + roi * 0.15
elif profile == INCOME:
    PCS = roi * 0.5 + vega * 0.3 + gamma * 0.2
elif profile == DIRECTIONAL:
    PCS = gamma * 0.5 + vega * 0.3 + roi * 0.2
else:
    PCS = gamma * 0.4 + vega * 0.4 + roi * 0.2
```

#### RAG Requirements (Step 10 - Full PCS)

**From RAG_CONTENT_DRAFT.md (Step 10):**

> **PCS_Final (50-100 scale):**  
> Multi-factor quality score combining volatility + chart + liquidity + greeks
> 
> **Components:**
> - IVHV magnitude (30% weight)
> - Chart regime/signal (25% weight)
> - Liquidity (OI, volume, spread) (25% weight)
> - Greeks alignment (delta, theta efficiency) (20% weight)

**Comparison Table:**

| **Component** | **RAG Weight** | **Current Implementation** | **Status** |
|--------------|---------------|---------------------------|------------|
| **IVHV/Volatility Context** | 30% | ❌ Not in formula | ⚠️ **MISSING** |
| **Chart Regime/Signal** | 25% | ❌ Not in formula (Phase 7+) | ⚠️ **DEFERRED** |
| **Liquidity (OI/Volume/Spread)** | 25% | ❌ Not in formula | ⚠️ **MISSING** |
| **Greeks (Gamma/Vega/Theta)** | 20% | ✅ Gamma/Vega only (100% weight) | ⚠️ **PARTIAL** |
| **ROI/Premium** | Not specified | ✅ Included in profile weights | ✅ **EXTRA** |
| **Profile-Based Weighting** | Not explicit | ✅ Strategy-specific profiles | ✅ **ENHANCEMENT** |

---

## 🔍 GAP ANALYSIS

### Gap 1: IV_Rank / IVHV Context (30% Weight - Missing)

**RAG Requirement:**
- Use 252-day IV_Rank percentile
- Weight: 30% of final PCS
- Purpose: Measure volatility elevation for mean reversion

**Current Status:**
- ✅ IV_Rank calculation exists (see SCAN_ENGINE_IV_LOGIC_AUDIT.md)
- ❌ NOT integrated into Current_PCS formula
- ❌ No IV_Rank weighting in any profile

**Implementation Need:**
```python
# Proposed enhancement to Current_PCS
if 'IV_Rank' in df.columns and df['IV_Rank'].notna():
    iv_rank_score = df['IV_Rank'] / 100 * 25  # 0-25 scale
    # Add to composite with 30% weight
else:
    iv_rank_score = 0  # Graceful degradation
```

**Priority:** ⚠️ **HIGH** (30% of RAG formula)

---

### Gap 2: Liquidity Metrics (25% Weight - Missing)

**RAG Requirement:**
- Open Interest >= 100
- Volume >= 10
- Bid-Ask Spread <= 10% of mid
- Weight: 25% of final PCS

**Current Status:**
- ✅ Liquidity columns exist in Phase 3 enrichment
- ❌ NOT used in Current_PCS scoring
- ❌ No liquidity penalty/bonus in formula

**Implementation Need:**
```python
# Proposed liquidity scoring
liquidity_score = 0
if Open_Interest >= 500: liquidity_score += 8
elif Open_Interest >= 100: liquidity_score += 5

if Volume >= 50: liquidity_score += 8
elif Volume >= 10: liquidity_score += 5

spread_pct = (Ask - Bid) / Mid
if spread_pct <= 0.03: liquidity_score += 9
elif spread_pct <= 0.10: liquidity_score += 6

# Max liquidity_score = 25 (0-25 scale)
# Add to composite with 25% weight
```

**Priority:** ⚠️ **HIGH** (25% of RAG formula)

---

### Gap 3: Chart Context (25% Weight - Deferred to Phase 7+)

**RAG Requirement:**
- Chart Regime, Signal_Type
- Crossover age, trend slope
- Weight: 25% of final PCS

**Current Status:**
- ❌ Phase 1-4 cannot use chart signals (perception loop purity)
- ✅ Chart data available in scan engine
- 📋 Deferred to Phase 7+ (active management phase)

**Implementation Need:**
- None for now (Phase 1-4 compliant)
- Add in Phase 7+: `chart_score` based on Regime/Signal_Type alignment

**Priority:** ✅ **DEFERRED** (Phase boundary protection)

---

### Gap 4: Theta Efficiency (Part of Greeks 20% - Missing)

**RAG Requirement:**
- Greeks alignment includes **theta efficiency**
- Delta/Theta ratio for directional plays
- Theta decay rate for income strategies

**Current Status:**
- ✅ Gamma and Vega scoring exist
- ❌ Theta not in Current_PCS formula
- ❌ No theta/premium efficiency metric

**Implementation Need:**
```python
# Proposed theta scoring (for income strategies)
if profile == INCOME and 'Theta' in df.columns:
    theta_efficiency = abs(Theta) / Premium  # Theta per dollar
    theta_score = min(theta_efficiency * 100, 10)  # 0-10 scale
    # Add to Greeks component
```

**Priority:** ⚠️ **MEDIUM** (part of 20% Greeks weight)

---

### Gap 5: Days_In_Trade / Time-Series Context (Missing)

**RAG Requirement:** (Implicit from "evolving score")
- Current_PCS should reflect position aging
- Days_In_Trade affects theta decay expectations
- P&L performance influences quality perception

**Current Status:**
- ✅ Days_In_Trade exists in Phase 4 snapshots
- ❌ NOT used in Current_PCS formula
- ❌ No time-based adjustments

**Implementation Need:**
```python
# Proposed time-series adjustment
if 'Days_In_Trade' in df.columns:
    # Penalize positions that haven't decayed as expected
    expected_theta_decay = Theta_Entry * Days_In_Trade
    actual_pnl = Unrealized_PnL
    decay_efficiency = actual_pnl / expected_theta_decay
    
    # Adjust PCS based on performance vs expectation
```

**Priority:** ⚠️ **MEDIUM** (Phase 2 enhancement, not Phase D.1)

---

## 📋 CURRENT PCS FORMULA WEIGHTS

### **As Implemented (Entry_PCS & Current_PCS)**

| **Profile** | **Gamma** | **Vega** | **ROI** | **Total** |
|------------|-----------|----------|---------|-----------|
| NEUTRAL_VOL | 25% | 60% | 15% | 100% |
| INCOME | 20% | 30% | 50% | 100% |
| DIRECTIONAL | 50% | 30% | 20% | 100% |
| DEFAULT | 40% | 40% | 20% | 100% |

**Max Scores:**
- Gamma: 0-25 points (Gamma * 1500, capped at 25)
- Vega: 0-20 points (Vega * 5000, capped at 25 in code, but weights scale to 20)
- ROI: 0-20 points (tiered: 5/10/15 for low/mid/high)

**Total Range:** 0-65 (not 0-100 as RAG specifies)

---

### **RAG Target (Step 10 - Full PCS)**

| **Component** | **Weight** | **Current** | **Gap** |
|--------------|-----------|-------------|---------|
| IV/Volatility Context | 30% | 0% | **-30%** |
| Chart Regime/Signal | 25% | 0% (Phase 7+) | **-25%** |
| Liquidity | 25% | 0% | **-25%** |
| Greeks (Gamma/Vega/Theta) | 20% | 100% | **+80%** |

**Observation:** Current PCS is 100% Greeks-based. RAG expects Greeks to be only 20% of composite.

---

## 🎯 IMPLEMENTATION ROADMAP

### **Phase D.1 (Complete ✅)**
- ✅ Entry_PCS: Frozen baseline using Entry Greeks
- ✅ Current_PCS: Basic Greeks + ROI with profile weights
- ✅ Separation: Entry_PCS vs Current_PCS architecture

### **Phase D.2 (Next - Missing Data Aggregation)**

**Objective:** Aggregate all inputs needed for full Current_PCS

#### Step 1: IV_Rank Integration
```python
# File: core/phase3_enrich/compute_iv_rank.py (REPLACE STUB)
from core.volatility.compute_iv_rank_252d import compute_iv_rank_batch

def compute_iv_rank(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate IV_Rank (252-day percentile) for Current_PCS input."""
    return compute_iv_rank_batch(
        df,
        symbol_col='Symbol',
        iv_col='IV Mid',
        date_col='Snapshot_TS',
        lookback_days=252
    )
```

**Status:** ⚠️ Architecture approved (see SCAN_ENGINE_IV_LOGIC_AUDIT.md), needs implementation

#### Step 2: Liquidity Data Availability
```python
# Check if liquidity columns exist in Phase 3
required_liquidity_cols = ['Open_Interest', 'Volume', 'Bid', 'Ask', 'Mid']
```

**Status:** ✅ Already available in snapshots (from Schwab API)

#### Step 3: Days_In_Trade
```python
# File: core/phase4_snapshot.py
# Already computed: Days_In_Trade = (Snapshot_TS - First_Seen_Date).days
```

**Status:** ✅ Already available

#### Step 4: Theta Greeks
```python
# Verify Theta is in Greeks columns
required_greeks = ['Delta', 'Gamma', 'Vega', 'Theta', 'Rho']
```

**Status:** ✅ Already available from Schwab API

---

### **Phase D.3 (Future - Full Current_PCS Formula)**

**Objective:** Implement RAG-compliant multi-factor Current_PCS

```python
def calculate_current_pcs_v2(df: pd.DataFrame) -> pd.DataFrame:
    """
    RAG-compliant Current_PCS with multi-factor scoring.
    
    Components (RAG Step 10):
    - IV Context (30%): IV_Rank, IVHV gap magnitude
    - Liquidity (25%): OI, Volume, Spread quality
    - Greeks (20%): Gamma, Vega, Theta efficiency
    - Chart Context (25%): DEFERRED to Phase 7+
    
    Phase D.3 implements: IV (30%) + Liquidity (25%) + Greeks (20%) = 75%
    Phase 7+ adds: Chart (25%) = 100%
    """
    
    # === 1. IV/Volatility Score (30% weight, 0-30 points) ===
    if 'IV_Rank' in df.columns and df['IV_Rank'].notna():
        # High IV_Rank (80-100) = good for selling premium
        # Low IV_Rank (0-20) = good for buying options
        # Use strategy context to interpret
        iv_score = calculate_iv_context_score(df)  # 0-30 scale
    else:
        iv_score = 0  # Graceful degradation
    
    # === 2. Liquidity Score (25% weight, 0-25 points) ===
    liquidity_score = calculate_liquidity_score(df)  # 0-25 scale
    
    # === 3. Greeks Score (20% weight, 0-20 points) ===
    # Use profile-based weights for Gamma/Vega/Theta
    greeks_score = calculate_greeks_score(df)  # 0-20 scale
    
    # === 4. Chart Score (25% weight, 0-25 points) - PHASE 7+ ONLY ===
    chart_score = 0  # Deferred to Phase 7+
    
    # === Composite Current_PCS (0-100 scale) ===
    df['PCS'] = iv_score + liquidity_score + greeks_score + chart_score
    
    # === Adjust for Phase 1-4: Scale to 75-point max until Phase 7+ ===
    # Since chart_score = 0, max is 75. Scale to 100 for consistency:
    df['PCS'] = (df['PCS'] / 75) * 100  # Normalize to 0-100
    
    return df
```

**Status:** 📋 **DESIGN PHASE** (needs requirements confirmation)

---

## 🚨 CRITICAL DECISIONS NEEDED

### Decision 1: Current_PCS Scope for Phase D.2

**Options:**

**A) Keep Current_PCS as Greeks-only (Status Quo)**
- ✅ Phase 1-4 compliant (structural only)
- ✅ Simple, deterministic, already working
- ❌ Deviates from RAG multi-factor formula
- ❌ Missing 55% of RAG scoring (IV 30% + Liquidity 25%)

**B) Implement Partial Current_PCS (Greeks 20% + IV 30% + Liquidity 25%)**
- ✅ Closer to RAG (75% complete, missing only Chart 25%)
- ✅ Phase 1-4 compliant (all inputs are observables)
- ✅ Enables better quality ranking
- ⚠️ Requires IV_Rank implementation (2-3 hours)
- ⚠️ Requires formula refactor (1-2 hours)

**C) Defer Full Current_PCS to Phase 7+ (Wait for Chart Context)**
- ✅ Implement complete RAG formula once (no rework)
- ✅ Clean phase boundary (Phase 7+ adds chart signals)
- ❌ Current_PCS remains Greeks-only for longer
- ❌ Missing liquidity scoring until Phase 7+

**Recommendation:** **Option B** (Partial Current_PCS with IV + Liquidity)
- Rationale: IV and Liquidity are Phase 3 observables (no phase violation)
- Impact: Better quality ranking NOW, smoother transition to Phase 7+
- Effort: 4-5 hours (IV_Rank + formula refactor)

---

### Decision 2: Entry_PCS Retroactive Recalculation

**Question:** Should we recalculate Entry_PCS with new formula if we change Current_PCS?

**Answer:** ✅ **NO - Entry_PCS is frozen and should NOT change**

**Rationale:**
- Entry_PCS is audit-grade baseline (immutable)
- Changing Entry_PCS invalidates historical comparisons
- Current_PCS can evolve independently

**Action:** Keep Entry_PCS formula separate from Current_PCS formula

---

### Decision 3: Scaling Target (0-65 vs 0-100)

**Current:** Both Entry_PCS and Current_PCS use 0-65 range

**RAG:** Step 10 specifies 0-100 range with tier thresholds:
- 85-100: Tier S
- 75-84: Tier A
- 65-74: Tier B
- 50-64: Tier C

**Options:**

**A) Keep 0-65 scale, adjust tier thresholds**
- Tier 1: >= 52 (80% of 65)
- Tier 2: >= 45 (70% of 65)
- Tier 3: >= 39 (60% of 65)
- Tier 4: < 39

**B) Scale to 0-100 range**
- Multiply all scores by (100/65) = 1.538
- Use RAG thresholds as-is

**Recommendation:** **Option B** (Scale to 0-100)
- Rationale: Aligns with RAG documentation
- Impact: Easier to understand ("65 sounds low, but it's actually Tier B")
- Effort: Trivial (one-line multiplier)

---

## ✅ ACTION ITEMS

### Immediate (Phase D.2)

1. ✅ **IV_Rank Implementation** (2-3 hours)
   - Create `core/volatility/compute_iv_rank_252d.py`
   - Wire into Phase 3 enrichment
   - Validate with 252-day lookback

2. ⚠️ **Aggregate Missing Data** (User Question - This Audit)
   - ✅ IV_Rank: Needs implementation (action item #1)
   - ✅ Liquidity: Already available (Open_Interest, Volume, Bid/Ask)
   - ✅ Days_In_Trade: Already available (Phase 4)
   - ✅ Theta: Already available (Schwab Greeks)

3. ⚠️ **Document Data Gaps** (This Document)
   - ✅ Identified: IV_Rank (30%), Liquidity scoring (25%), Theta efficiency
   - ✅ Prioritized: High priority for IV + Liquidity
   - ✅ Roadmap: Phase D.2 (data agg) → Phase D.3 (formula refactor)

### Next (Phase D.3 - Formula Enhancement)

4. 📋 **Refactor Current_PCS Formula** (3-4 hours)
   - Add IV_Rank scoring (30% weight)
   - Add Liquidity scoring (25% weight)
   - Adjust Greeks scoring (20% weight, add Theta)
   - Scale to 0-100 range
   - Update tier thresholds

5. 📋 **Validation Tests** (2 hours)
   - Compare Current_PCS v1 (Greeks) vs v2 (Multi-factor)
   - Validate score distribution (should be 50-100, not 0-20)
   - Confirm tier assignments match RAG thresholds

6. 📋 **Documentation Updates** (1 hour)
   - Update PHASE_D1_PCS_SEPARATION_COMPLETE.md
   - Document Current_PCS v2 formula
   - Add RAG compliance notes

### Future (Phase 7+)

7. 📋 **Add Chart Context** (Phase 7+ only)
   - Chart Regime/Signal_Type scoring (25% weight)
   - Full 100% RAG-compliant PCS
   - Exit logic integration

---

## 📊 SUMMARY TABLE

| **Component** | **Entry_PCS** | **Current_PCS (v1)** | **Current_PCS (v2 - Target)** | **RAG Requirement** |
|--------------|--------------|---------------------|----------------------------|-------------------|
| **Gamma** | ✅ 0-25 pts | ✅ 0-25 pts | ✅ Part of Greeks (20%) | ✅ Greeks 20% |
| **Vega** | ✅ 0-20 pts | ✅ 0-25 pts | ✅ Part of Greeks (20%) | ✅ Greeks 20% |
| **Theta** | ❌ N/A | ❌ Missing | ⚠️ Add to Greeks (20%) | ✅ Greeks 20% |
| **ROI/Premium** | ✅ 0-20 pts | ✅ 0-15 pts | ✅ Part of Greeks (20%) | ✅ (implicit) |
| **IV_Rank** | ❌ N/A | ❌ Missing | ⚠️ TODO (30%) | ✅ IV Context 30% |
| **Liquidity** | ❌ N/A | ❌ Missing | ⚠️ TODO (25%) | ✅ Liquidity 25% |
| **Chart Context** | ❌ N/A | ❌ N/A (Phase 7+) | ❌ Deferred (25%) | ✅ Chart 25% |
| **Profile Weights** | ✅ Strategy-specific | ✅ Strategy-specific | ✅ Strategy-specific | ✅ (enhancement) |
| **Score Range** | 0-65 | 0-65 | 0-100 (target) | 0-100 |
| **Status** | ✅ COMPLETE | ✅ WORKS (partial) | ⚠️ NEEDS WORK | 🎯 TARGET |

---

## 🎓 CONCLUSION

### What We Have
- ✅ **Entry_PCS**: Complete, frozen, RAG-compliant baseline scoring
- ✅ **Current_PCS v1**: Basic Greeks + ROI scoring with profile weights
- ✅ **Architecture**: Proper separation of Entry vs Current PCS

### What We Need (Phase D.2)
- ⚠️ **IV_Rank**: 252-day percentile calculation (30% of RAG formula)
- ⚠️ **Liquidity Scoring**: OI/Volume/Spread quality (25% of RAG formula)
- ⚠️ **Theta Integration**: Add to Greeks component (part of 20%)
- ⚠️ **Formula Refactor**: Multi-factor composite (not just Greeks)

### What We're Deferring (Phase 7+)
- 📋 **Chart Context**: Regime/Signal_Type (25% of RAG formula)
- 📋 **Exit Logic**: PCS_Active drift-based scoring
- 📋 **Full 100% RAG Compliance**: Requires chart signals (Phase boundary)

### User's Question: "Do we still need to aggregate all the information needed for the 2nd PCS?"

**Answer:** ✅ **YES - We need to aggregate:**
1. **IV_Rank** (252-day percentile) - Architecture approved, needs implementation
2. **Liquidity columns** - Already available, needs formula integration
3. **Theta Greeks** - Already available, needs formula integration
4. **Days_In_Trade** - Already available, needs scoring logic

**Current PCS is 55% incomplete vs RAG target** (missing IV 30% + Liquidity 25%)

**Recommendation:** Proceed with Phase D.2 (data aggregation + formula refactor) to achieve 75% RAG compliance before Phase 7+ adds Chart context (final 25%).

---
**Status:** ⚠️ **PHASE D.2 REQUIRED** - Current PCS works but needs enhancement to match RAG specifications
