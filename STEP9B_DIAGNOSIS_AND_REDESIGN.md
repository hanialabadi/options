# Step 9B Architecture Diagnosis & Redesign

**Date:** December 28, 2025  
**Issue:** Only ~58/266 strategies producing contracts despite chains existing and liquidity present  
**Root Cause:** Step 9B mixing exploration with premature approval/rejection

---

## 1. CURRENT ARCHITECTURE AUDIT

### **Identified Premature Rejection Points**

#### **A. Phase 1 Sampled Exploration (lines 450-600)**
**Purpose:** Fast viability check using single-expiration sampling

**Early Rejection Statuses:**
1. `'No_Viable_Expirations'` - No expirations in DTE range (lines 496, 527)
2. `'Fast_Reject'` - Sample quality poor (no ATM strike or no liquidity)
3. `'No_Chain_Data'` - Empty chain for sampled expiration
4. `'Sampling_Error'` - Exception during sampling

**Problem:** 
- **Phase 1 is TOO AGGRESSIVE** - rejects strategies based on single-expiration sample
- A strategy might have 10 viable expirations but gets rejected because ONE sampled expiration lacks liquidity
- "Fast_Reject" prevents deep exploration even when other expirations might be perfect

**Example Failure:**
```
AAPL Long Straddle:
  - Target DTE: 45 days
  - Phase 1 samples Feb 15 (43 DTE) - finds low OI
  - Status: 'Fast_Reject' - "Sample quality poor: no_liquidity"
  - NEVER checks Feb 22, Mar 1, Mar 8, Mar 15 (which might have excellent liquidity)
```

#### **B. Phase 2 Strategy Laziness (lines 1155-1175)**
**Purpose:** Skip full chain fetch for single-expiration strategies

**Early Status:**
- `'Requires_PCS'` - "Full chain skipped, PCS will select strikes"

**Problem:**
- **Status name is misleading** - sounds like a failure when it's actually SUCCESS
- Marks valid discoveries as "Requires_PCS" which gets grouped with failures
- Dashboard interprets "Requires_PCS" as incomplete, not as "PCS will handle strike selection"

**Example Confusion:**
```
MSFT Cash-Secured Put:
  - Phase 2: Single expiration (Mar 15)
  - Status: 'Requires_PCS'
  - Interpretation: "Failed - needs PCS"
  - Reality: "Success - PCS will select optimal strike"
```

#### **C. Liquidity Filtering (lines 1236-1240)**
**Early Rejection Status:**
- `'Low_Liquidity'` - No contracts pass adjusted OI/spread thresholds

**Problem:**
- **Applied uniformly regardless of strategy** - Long Straddles are rejected with same OI thresholds as CSPs
- **No fallback to candidate contracts** - if best strike has 9% spread but threshold is 8%, entire strategy rejected
- **LEAPs are unfairly filtered** - LEAP spreads are ALWAYS wider (12-20% is normal), but same thresholds applied

**Example Failure:**
```
GOOGL Long Call LEAP (547 DTE):
  - Underlying: $142
  - Best strike: $145 Jan 2027
  - Spread: 9.2%
  - Threshold: 8.0%
  - Status: 'Low_Liquidity'
  - Reality: 9.2% spread is EXCELLENT for a 547-day LEAP on $142 stock
```

#### **D. Strike Selection (lines 1253-1257)**
**Early Rejection Status:**
- `'No_Suitable_Strikes'` - Strike selection function returns empty

**Problem:**
- **Too strict for edge cases** - Buy-Write on $1800 stock rejected because no strike within 5% of ATM
- **No "near-miss" preservation** - if 10 strikes are "almost good", none are returned
- **LEAPs need wider delta bands** - 365+ DTE deep ITM LEAP with Delta=0.55 rejected because not 0.65-0.80

---

## 2. PCS CLARIFICATION

### **What PCS Currently Is:**

Based on code analysis:

**PCS = "Pre-filter Confidence Score"** (NOT Probabilistic Confidence Score)

**Location:** Step 10 (step10_pcs_recalibration.py)

**Purpose:** Neutral, rules-based quality score for structural trade validation

**Metrics Used:**
- Liquidity Score (0-100)
- Bid-Ask Spread %
- Open Interest
- Actual Risk Per Contract
- DTE (minimum thresholds)
- Risk Model alignment
- Greek alignment (Delta/Vega match strategy intent)

**Output:**
- `Pre_Filter_Status`: 'Valid', 'Watch', 'Rejected'
- `PCS_Score`: 0-100 quality score
- `Execution_Ready`: True/False

### **Current PCS Placement:**

```
Step 9B → Contract_Selection_Status
   ↓
Step 10 (PCS) → Pre_Filter_Status
   ↓
Step 11 → Ranking
   ↓
Step 12 → Execution
```

### **Problem with Current Flow:**

**Step 9B is doing Step 10's job!**

- Step 9B applies liquidity thresholds → Sets `'Low_Liquidity'` status
- Step 10 applies liquidity thresholds again → Sets `Pre_Filter_Status = 'Rejected'`
- **Duplicate filtering** causes early collapse

**Correct Flow Should Be:**

```
Step 9B → PURE EXPLORATION (discover all contracts)
   ↓ (ALL strategies preserved)
Step 10 (PCS) → QUALITY SCORING (rank liquidity, not filter)
   ↓ (Low-scoring strategies tagged 'Watch', not dropped)
Step 11 → RANKING (best vs 2nd-best within ticker)
   ↓ (Top N strategies selected)
Step 12 → EXECUTION APPROVAL (final gate)
```

---

## 3. STRATEGY-AWARE FILTERING ISSUES

### **Current Problem: Uniform Thresholds**

Step 9B applies same liquidity rules to ALL strategies:

```python
# lines 1226-1228
adjusted_min_oi, adjusted_max_spread = _get_price_aware_liquidity_thresholds(
    underlying_price, actual_dte
)
```

**Reality Check:**

| Strategy | Typical OI | Typical Spread | Current Threshold | Result |
|----------|-----------|----------------|-------------------|---------|
| Long Call (30 DTE, SPY) | 1000+ | 2-4% | min_oi=50, max_spread=8% | ✅ Pass |
| Long Straddle (45 DTE, AAPL) | 200+ | 6-10% | min_oi=50, max_spread=8% | ❌ Fail (spread) |
| Long Call LEAP (547 DTE, GOOGL) | 20-50 | 12-18% | min_oi=50, max_spread=8% | ❌ Fail (both) |
| Buy-Write (SPY) | N/A (uses stock) | N/A | min_oi=50, max_spread=8% | ❌ Fail (irrelevant metric) |
| CSP (BKNG, $4500 stock) | 5-15 | 10-15% | min_oi=50, max_spread=8% | ❌ Fail (elite stock penalty) |

### **Strategy-Specific Reality:**

#### **Long Straddle / Long Strangle:**
- **Structure:** ATM call + ATM put (straddle) or OTM call + OTM put (strangle)
- **Liquidity Reality:** Combined spread is 8-15% (individual legs 4-8% each)
- **Current Threshold:** 8% max spread
- **Fix Needed:** Accept 12-18% combined spread for volatility strategies

#### **LEAPs (365+ DTE):**
- **Structure:** Long-dated calls/puts (1-2 years out)
- **Liquidity Reality:** 
  - OI: 10-100 (vs 500+ for short-term)
  - Spread: 10-25% (vs 3-8% for short-term)
  - This is NORMAL and ACCEPTABLE
- **Current Threshold:** Same as short-term (min_oi=50, max_spread=8%)
- **Fix Needed:** Relaxed thresholds for DTE >= 365 (min_oi=5, max_spread=20%)

#### **Buy-Write / Covered Call:**
- **Structure:** Long 100 shares + short 1 call
- **Liquidity Reality:** Stock liquidity matters more than call OI
- **Current Threshold:** Call OI >= 50
- **Fix Needed:** Accept OI >= 10 for high-price stocks ($500+)

#### **CSP on Elite Stocks (BKNG, MELI, etc.):**
- **Structure:** High-capital put selling on expensive underlyings
- **Liquidity Reality:** OI is ALWAYS low (5-20) due to capital intensity
- **Current Threshold:** min_oi=50, max_spread=8%
- **Fix Needed:** Price-aware relaxation (if price > $1000, min_oi=5, max_spread=15%)

---

## 4. LEAP INVESTIGATION

### **LEAP Definition in System:**

**DTE Threshold:** `actual_dte >= 365` (line 1269)

**LEAP Tagging Logic (lines 1269-1280):**
```python
if actual_dte >= 365:
    result['is_leap'] = True
    result['horizon_class'] = 'LEAP'
    result['leap_reason'] = f'DTE={actual_dte} >= 365 days'
```

### **Where LEAPs Are Being Excluded:**

#### **Exclusion Point 1: Step 7 (Strategy Nomination)**
- **Status:** ✅ **FIXED** (as of today)
- Added `_validate_long_call_leap` and `_validate_long_put_leap` validators
- LEAPs are now nominated with strategy name "Long Call LEAP" / "Long Put LEAP"

#### **Exclusion Point 2: Step 9A (DTE Assignment)**
- **Status:** ✅ **FIXED** (as of today)
- LEAP strategies get `Min_DTE=365, Max_DTE=730`

#### **Exclusion Point 3: Step 9B (Liquidity Filtering) ⚠️ ACTIVE ISSUE**
- **Status:** ❌ **FAILING**
- LEAPs reach Step 9B with correct DTE ranges
- **But:** Liquidity thresholds are NOT LEAP-aware
- Line 1226: `_get_price_aware_liquidity_thresholds()` only adjusts for price, not DTE
- LEAP spreads (12-20%) exceed threshold (8%)
- **Result:** `'Low_Liquidity'` status even though 15% spread is EXCELLENT for LEAP

**Example LEAP Failure:**
```
AAPL Long Call LEAP:
  - DTE: 547
  - Strike: $180 Jan 2027
  - Spread: 14.5%
  - OI: 35
  - Adjusted threshold: min_oi=50, max_spread=8% (WRONG for LEAP)
  - Status: 'Low_Liquidity'
  - Should be: 'Success' with Liquidity_Class='Acceptable_LEAP'
```

### **LEAP-Specific Threshold Fix Needed:**

```python
def _get_price_aware_liquidity_thresholds(underlying_price, actual_dte):
    """Get liquidity thresholds adjusted for price AND DTE."""
    
    # LEAP adjustments (DTE >= 365)
    if actual_dte >= 365:
        if underlying_price >= 1000:
            return (5, 25.0)  # Elite stock LEAP: ultra-relaxed
        elif underlying_price >= 500:
            return (8, 20.0)  # Large-cap LEAP: very relaxed
        elif underlying_price >= 200:
            return (10, 18.0)  # Mid-cap LEAP: relaxed
        else:
            return (15, 15.0)  # Standard LEAP: moderate relaxation
    
    # Short-term (DTE < 90)
    elif actual_dte < 90:
        if underlying_price >= 1000:
            return (10, 12.0)
        elif underlying_price >= 500:
            return (20, 10.0)
        elif underlying_price >= 200:
            return (30, 8.0)
        else:
            return (50, 6.0)
    
    # Medium-term (90-364)
    else:
        if underlying_price >= 1000:
            return (8, 15.0)
        elif underlying_price >= 500:
            return (15, 12.0)
        elif underlying_price >= 200:
            return (25, 10.0)
        else:
            return (40, 8.0)
```

---

## 5. PROPOSED STEP 9B REDESIGN

### **Core Principle: "Explore Everything, Annotate Everything, Reject Nothing"**

### **New Status Model:**

Replace binary success/failure with **exploration depth tracking**:

```python
Contract_Exploration_Status: (WHAT was explored)
  - 'Fully_Explored': Full chain fetched, strikes selected
  - 'Sampled': Single expiration sampled, viability confirmed
  - 'Expirations_Only': Expirations fetched, no chain data
  - 'No_Expirations': No expirations in DTE range

Contract_Selection_Outcome: (WHAT was found)
  - 'Primary_Selected': Best strike(s) identified
  - 'Candidate_Identified': Near-miss strikes available
  - 'Thin_But_Viable': Low liquidity but structurally sound
  - 'No_Liquid_Strikes': Explored but no liquid strikes
  - 'Structural_Mismatch': Chain exists but strategy incompatible
```

### **Output Columns (Redesigned):**

#### **Exploration Metadata:**
- `Exploration_Status`: Depth of exploration
- `Chain_Fetched`: True/False
- `Expirations_Count`: Number of expirations in DTE range
- `Strikes_Scanned`: Number of strikes evaluated
- `Sampling_Quality`: 'Good' / 'Marginal' / 'Poor' (if sampled)

#### **Selection Outcome:**
- `Selection_Outcome`: What was found
- `Primary_Contract`: Best contract (if selected)
- `Candidate_Contracts`: Near-miss contracts (JSON list)
- `Selection_Confidence`: 0-100 score

#### **Liquidity Annotation (NOT filtering):**
- `Liquidity_Grade`: 'Excellent' / 'Good' / 'Acceptable' / 'Thin' / 'Illiquid'
- `Liquidity_Context`: Strategy-aware explanation
  - "12% spread normal for LEAP"
  - "Low OI expected for $4500 stock"
  - "Volatility strategy, combined spread 14%"
- `Liquidity_Score`: 0-100 (for ranking, not filtering)
- `Spread_Pct`: Actual spread
- `OI_Total`: Actual OI

#### **Capital Annotation (NOT filtering):**
- `Capital_Required`: Dollar amount
- `Capital_Class`: 'Light' / 'Standard' / 'Heavy' / 'VeryHeavy' / 'Institutional'
- `Within_Budget`: True/False (for reference)

#### **Tradability Flags (for downstream):**
- `Structurally_Valid`: True/False (chain exists, strikes exist)
- `Execution_Viable`: True/False (meets minimum structural requirements)
- `PCS_Required`: True/False (needs Step 10 scoring)

### **Key Changes:**

1. **Phase 1 Never Rejects:**
   - Old: `'Fast_Reject'` → dropped
   - New: `'Sampled'` + `Sampling_Quality='Poor'` → preserved with annotation

2. **Phase 2 Laziness Renamed:**
   - Old: `'Requires_PCS'` (sounds like failure)
   - New: `'Sampled'` + `PCS_Required=True` (sounds like success pending PCS)

3. **Liquidity Becomes Descriptive:**
   - Old: `'Low_Liquidity'` → dropped
   - New: `Liquidity_Grade='Thin'` + `Liquidity_Context='Low OI normal for elite stock'` → preserved

4. **Strike Selection Softened:**
   - Old: `'No_Suitable_Strikes'` → dropped
   - New: `Selection_Outcome='No_Liquid_Strikes'` + `Candidate_Contracts=[]` → preserved with context

---

## 6. REVISED FLOW (PSEUDOCODE)

```python
def _process_single_strategy_REDESIGNED(row_data, idx, token, min_oi, max_spread):
    """
    PURE EXPLORATION MODE - NO PREMATURE REJECTION
    """
    result = {
        'exploration_status': 'Pending',
        'selection_outcome': 'Pending',
        'chain_fetched': False,
        'expirations_count': 0,
        'strikes_scanned': 0,
        'primary_contract': None,
        'candidate_contracts': [],
        'liquidity_grade': 'Unknown',
        'liquidity_context': '',
        'capital_required': 0,
        'capital_class': 'Unknown',
        'structurally_valid': False,
        'execution_viable': False,
        'pcs_required': False
    }
    
    try:
        # PHASE 1: SAMPLED EXPLORATION (no rejection)
        phase1 = _tier1_sampled_exploration(...)
        
        result['exploration_status'] = 'Sampled'
        result['sampling_quality'] = phase1['sample_quality']
        result['phase1_metadata'] = phase1
        
        # Decision: Deep exploration needed?
        if phase1['sample_quality'] == 'Poor':
            # OLD: return 'Fast_Reject'
            # NEW: Annotate and preserve
            result['selection_outcome'] = 'Sampling_Suggests_Poor_Liquidity'
            result['liquidity_context'] = f"Sample shows {phase1['reason']}, may not be viable"
            result['structurally_valid'] = phase1['has_atm_strike']
            result['pcs_required'] = False
            return result  # Preserved, not rejected
        
        # PHASE 2: FETCH EXPIRATIONS
        expirations = _fetch_expirations_only(...)
        result['expirations_count'] = len(expirations)
        
        if not expirations:
            # OLD: return 'No_Expirations'
            # NEW: Annotate
            result['exploration_status'] = 'Expirations_Fetched'
            result['selection_outcome'] = 'No_Expirations_In_DTE_Range'
            result['liquidity_context'] = f"No expirations between {min_dte}-{max_dte} days"
            result['structurally_valid'] = False
            return result  # Preserved
        
        # Strategy laziness check
        if _single_expiration_strategy(strategy, expirations):
            # OLD: return 'Requires_PCS'
            # NEW: Success with PCS flag
            result['exploration_status'] = 'Sampled'
            result['selection_outcome'] = 'PCS_Will_Select_Strikes'
            result['structurally_valid'] = True
            result['execution_viable'] = True
            result['pcs_required'] = True
            result['liquidity_context'] = 'Single expiration, PCS will optimize strike'
            return result  # Success path
        
        # PHASE 3: FETCH FULL CHAIN
        chain_df = _fetch_chain_with_greeks(...)
        result['chain_fetched'] = True
        result['exploration_status'] = 'Fully_Explored'
        result['strikes_scanned'] = len(chain_df)
        
        if chain_df.empty:
            # OLD: return 'No_Chain_Data'
            # NEW: Annotate
            result['selection_outcome'] = 'Chain_Empty'
            result['structurally_valid'] = False
            return result  # Preserved
        
        # PHASE 4: STRATEGY-AWARE LIQUIDITY ASSESSMENT
        liquidity_thresholds = _get_strategy_aware_thresholds(
            strategy=strategy,
            underlying_price=underlying_price,
            actual_dte=actual_dte,
            is_leap=(actual_dte >= 365)
        )
        
        # Filter chain
        filtered_chain = _filter_by_liquidity(chain_df, **liquidity_thresholds)
        
        if filtered_chain.empty:
            # OLD: return 'Low_Liquidity'
            # NEW: Grade and preserve
            result['selection_outcome'] = 'No_Liquid_Strikes_Found'
            result['liquidity_grade'] = 'Illiquid'
            result['liquidity_context'] = _generate_liquidity_context(
                strategy, underlying_price, actual_dte, chain_df
            )
            result['structurally_valid'] = True  # Chain exists
            result['execution_viable'] = False  # But no liquid strikes
            
            # Preserve candidate strikes (even if illiquid)
            result['candidate_contracts'] = _extract_near_miss_strikes(chain_df)
            return result  # Preserved with rich annotation
        
        # PHASE 5: SELECT STRIKES
        selected = _select_strikes_for_strategy(filtered_chain, strategy, ...)
        
        if not selected or not selected['strikes']:
            # OLD: return 'No_Suitable_Strikes'
            # NEW: Preserve with candidates
            result['selection_outcome'] = 'No_Optimal_Strikes'
            result['liquidity_grade'] = _grade_liquidity(filtered_chain)
            result['liquidity_context'] = 'Strikes exist but none optimal for strategy'
            result['structurally_valid'] = True
            result['execution_viable'] = False
            result['candidate_contracts'] = _extract_near_miss_strikes(filtered_chain)
            return result  # Preserved
        
        # SUCCESS: Primary contract selected
        result['selection_outcome'] = 'Primary_Contract_Selected'
        result['primary_contract'] = selected
        result['liquidity_grade'] = _grade_liquidity_from_selection(selected)
        result['liquidity_context'] = _generate_context(strategy, selected, actual_dte)
        result['capital_required'] = selected['risk_per_contract']
        result['capital_class'] = _classify_capital(selected['risk_per_contract'])
        result['structurally_valid'] = True
        result['execution_viable'] = True
        result['pcs_required'] = False  # Primary contract ready
        
        # Extract candidate contracts (near-miss strikes)
        result['candidate_contracts'] = _extract_candidates(filtered_chain, selected)
        
        return result  # Success with full data
        
    except Exception as e:
        # OLD: return 'Failed: {e}'
        # NEW: Preserve with error context
        result['exploration_status'] = 'Error'
        result['selection_outcome'] = 'Exploration_Failed'
        result['liquidity_context'] = f'Error: {str(e)[:100]}'
        result['structurally_valid'] = False
        return result  # Preserved even on error
```

---

## 7. STRATEGY-AWARE LIQUIDITY THRESHOLDS

```python
def _get_strategy_aware_thresholds(
    strategy: str,
    underlying_price: float,
    actual_dte: int,
    is_leap: bool
) -> Dict:
    """
    Return liquidity thresholds that respect strategy structure and time horizon.
    """
    
    # LEAP adjustments (universal)
    if is_leap:
        base_oi = 5 if underlying_price >= 1000 else 10
        base_spread = 25.0 if underlying_price >= 1000 else 20.0
        multiplier_oi = 0.5
        multiplier_spread = 2.5
    else:
        base_oi = 50
        base_spread = 8.0
        multiplier_oi = 1.0
        multiplier_spread = 1.0
    
    # Strategy-specific adjustments
    if strategy in ['Long Straddle', 'Long Strangle']:
        # Volatility strategies: tolerate wider spreads (combined legs)
        return {
            'min_oi': base_oi * 0.6 * multiplier_oi,
            'max_spread': base_spread * 1.8 * multiplier_spread,  # 14.4% for short-term, 45% for LEAP
            'context': 'Volatility strategy, combined spread expected'
        }
    
    elif strategy in ['Buy-Write', 'Covered Call']:
        # Stock-based strategies: call OI less critical
        return {
            'min_oi': base_oi * 0.4 * multiplier_oi,
            'max_spread': base_spread * 1.3 * multiplier_spread,
            'context': 'Stock-based strategy, stock liquidity primary'
        }
    
    elif strategy in ['Cash-Secured Put'] and underlying_price >= 1000:
        # Elite stock CSPs: low OI is normal
        return {
            'min_oi': 5 * multiplier_oi,
            'max_spread': 15.0 * multiplier_spread,
            'context': 'Elite stock CSP, low OI normal due to capital intensity'
        }
    
    elif 'LEAP' in strategy:
        # Explicit LEAP strategies
        return {
            'min_oi': 5 * multiplier_oi,
            'max_spread': 20.0 * multiplier_spread,
            'context': 'LEAP strategy, wider spreads and lower OI expected'
        }
    
    else:
        # Standard directional strategies
        return {
            'min_oi': base_oi * multiplier_oi,
            'max_spread': base_spread * multiplier_spread,
            'context': 'Standard directional strategy'
        }
```

---

## 8. DEBUGGING CACHING STRATEGY

### **Proposed Cache Structure:**

```python
CACHE_DIR = '/Users/haniabadi/Documents/Github/options/cache/raw_chains/'

def _fetch_chain_with_cache(ticker: str, expiration: str, token: str, debug_mode: bool = False):
    """
    Fetch chain with optional disk caching for debug mode.
    """
    if not debug_mode:
        return _fetch_chain_with_greeks(ticker, expiration, token)
    
    # Cache path
    cache_file = os.path.join(CACHE_DIR, f"{ticker}_{expiration.replace('-', '')}.parquet")
    
    # Load from cache if exists
    if os.path.exists(cache_file):
        logger.info(f"📦 Loading cached chain: {ticker} {expiration}")
        return pd.read_parquet(cache_file)
    
    # Fetch from API
    logger.info(f"🌐 Fetching fresh chain: {ticker} {expiration}")
    chain_df = _fetch_chain_with_greeks(ticker, expiration, token)
    
    # Save to cache
    os.makedirs(CACHE_DIR, exist_ok=True)
    chain_df.to_parquet(cache_file, index=False)
    logger.info(f"💾 Cached chain: {cache_file}")
    
    return chain_df
```

**Usage:**
```python
# Enable debug caching
export DEBUG_MODE=1

# Run pipeline
./venv/bin/python run_pipeline.py

# Chains are cached to disk
# Subsequent runs load from cache (deterministic)

# Clear cache when done
rm -rf cache/raw_chains/
```

---

## 9. DELIVERABLES SUMMARY

### **1. Diagnosis:**
- Step 9B is mixing exploration with approval
- 4 premature rejection points identified:
  - Phase 1 Fast_Reject
  - Phase 2 Requires_PCS (misleading name)
  - Liquidity Low_Liquidity
  - Strike Selection No_Suitable_Strikes
- PCS is currently in Step 10 but Step 9B duplicates its filtering logic

### **2. Proposed Restructuring:**
- Replace binary success/failure with exploration depth tracking
- Replace rejection statuses with descriptive annotation
- Preserve ALL strategies with rich context
- Move all quality filtering to Step 10 (PCS)

### **3. Revised Flow:**
```
Step 9B (Exploration) → Annotate everything, reject nothing
   ↓ (266 strategies preserved)
Step 10 (PCS) → Score quality, flag Watch, never drop
   ↓ (266 strategies with scores)
Step 11 (Ranking) → Select top N per ticker
   ↓ (Best strategies selected)
Step 12 (Execution) → Final approval gate
```

### **4. PCS Clarification:**
- PCS is Step 10's pre-filter confidence score
- Should rank/flag, NOT filter/drop
- Step 9B should NOT apply PCS-like logic

### **5. Pseudocode:**
- See Section 6 for non-collapsing Step 9B
- See Section 7 for strategy-aware thresholds
- See Section 8 for debug caching

---

## 10. IMPLEMENTATION PRIORITY

**Phase 1 (Critical - Unblock LEAPs):**
1. Fix `_get_price_aware_liquidity_thresholds()` to be DTE-aware (LEAP support)
2. Rename `'Requires_PCS'` → `'PCS_Will_Select_Strikes'` (clarity)
3. Change `'Low_Liquidity'` → `Liquidity_Grade='Thin'` + preserve row

**Phase 2 (High - Preserve Visibility):**
4. Replace Phase 1 `'Fast_Reject'` → `'Sampled'` + `Sampling_Quality='Poor'`
5. Add `Candidate_Contracts` column (near-miss strikes)
6. Add strategy-aware liquidity context generation

**Phase 3 (Medium - Full Redesign):**
7. Implement new status model (`Exploration_Status` + `Selection_Outcome`)
8. Implement strategy-aware threshold function
9. Add debug caching mode

**Phase 4 (Future - Optimization):**
10. Audit Step 10 PCS to ensure no duplication with Step 9B
11. Ensure Step 11 ranking respects preserved data
12. Dashboard updates to display new columns

---

## CONCLUSION

**The core issue is architectural:**

Step 9B is trying to be both an explorer AND a judge. This causes premature collapse of strategy visibility.

**The solution is separation of concerns:**

- **Step 9B:** Pure exploration engine (discover and annotate)
- **Step 10:** Quality scoring engine (rank and flag)
- **Step 11:** Selection engine (choose best per ticker)
- **Step 12:** Execution gate (final approval)

By making Step 9B **reject nothing**, we preserve full visibility into what was explored, why certain strategies are thin, and what near-miss contracts exist. Downstream stages can then make informed decisions with complete context.
