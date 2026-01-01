# Exploration vs Selection: Step 9B Refactor Plan

## Core Principle

**Exploration ≠ Selection**

Current Problem: Step 9B is doing **competitive filtering during discovery**, causing:
- LEAPs disappearing (fail short-term liquidity rules)
- BKNG looking "illiquid" (when actually tradable)
- Capital-heavy trades hidden (instead of labeled)
- Multi-leg strategies rejected because one leg imperfect
- Same ticker fetching chains multiple times for different strategies

## The Mental Model Shift

### How Professionals Think:
1. **Scan broadly** → See everything available
2. **Understand constraints** → Label what makes each trade special/difficult
3. **Compare options** → Once full visibility exists
4. **Select** → Based on portfolio fit, capital, goals

### What System Currently Does:
1. Scan (Steps 1-6) ✅
2. Recommend strategies (Step 7) ✅
3. **❌ REJECT during discovery** (Step 9B) ← **THIS IS THE PROBLEM**
4. Nothing left to compare (Steps 10-11)

---

## What Must Change in Step 9B

### Current Behavior (WRONG):
```python
# In Step 9B loop (lines 260-425):
for idx, row in df.iterrows():
    ticker = row['Ticker']
    
    # ❌ FETCHES CHAIN PER STRATEGY (inefficient)
    chain_df = _fetch_chain_with_greeks(ticker, best_expiry, token)
    
    # ❌ REJECTS if liquidity fails
    if filtered_chain.empty:
        df.at[idx, 'Contract_Selection_Status'] = 'Low_Liquidity'
        continue  # Strategy LOST
    
    # ❌ REJECTS if no suitable strikes
    if not selected_contracts:
        df.at[idx, 'Contract_Selection_Status'] = 'No_Suitable_Strikes'
        continue  # Strategy LOST
```

**Problems:**
1. Same ticker = multiple API calls (AAPL Long Call, AAPL Straddle → 2 calls)
2. Rejection means **strategy disappears** from pipeline
3. No annotation of WHY it failed
4. No visibility into what WAS available

### New Behavior (CORRECT):
```python
# Step 9B should be PURE EXPLORATION

# Phase 1: Fetch chains ONCE per ticker (efficient)
chain_cache = {}
for ticker in df['Ticker'].unique():
    chain_cache[ticker] = fetch_all_expirations_and_chains(ticker)

# Phase 2: Evaluate ALL strategies WITHOUT REJECTION
for idx, row in df.iterrows():
    ticker = row['Ticker']
    strategy = row['Strategy_Name']
    
    # Reuse cached chain
    chain_data = chain_cache[ticker]
    
    # Evaluate strategy - NO REJECTION
    result = evaluate_strategy_for_exploration(
        chain_data, 
        strategy,
        liquidity_mode='descriptive'  # Not restrictive
    )
    
    # ANNOTATE - don't reject
    df.at[idx, 'Status'] = result['status']  # Viable / Marginal / Thin
    df.at[idx, 'Liquidity_Quality'] = result['liquidity']  # Good / Acceptable / Wide
    df.at[idx, 'Capital_Required'] = result['capital']
    df.at[idx, 'Strike_Quality'] = result['strike_quality']
    df.at[idx, 'Spread_Quality'] = result['spread_quality']
    df.at[idx, 'Reason_Flags'] = result['flags']  # "wide_spread, sparse_strikes"
    
    # ALL strategies preserved with context
```

---

## Specific Changes Needed

### 1. Chain Fetching Efficiency

**Current:** Each strategy fetches chains independently
**New:** Fetch once per ticker, reuse for all strategies

```python
def _build_chain_cache(df: pd.DataFrame, token: str) -> Dict[str, Dict]:
    """
    Fetch option chains ONCE per ticker, cache for all strategies.
    
    Returns:
        {
            'AAPL': {
                'expirations': ['2025-02-14', '2025-03-21', ...],
                'chains': {
                    '2025-02-14': pd.DataFrame(...),
                    '2025-03-21': pd.DataFrame(...),
                },
                'underlying_price': 150.25,
                'liquidity_profile': 'Elite'
            },
            'BKNG': {...}
        }
    """
    cache = {}
    tickers = df['Ticker'].unique()
    
    for ticker in tickers:
        # Get ALL expirations (don't filter by DTE yet)
        all_expirations = _get_all_expirations(ticker, token)
        
        # Fetch chains for key expirations (weekly, monthly, quarterly, LEAP)
        chains = {}
        for expiry in _select_key_expirations(all_expirations):
            chains[expiry] = _fetch_chain_with_greeks(ticker, expiry, token)
        
        cache[ticker] = {
            'expirations': all_expirations,
            'chains': chains,
            'underlying_price': _get_stock_price(ticker, chains[list(chains.keys())[0]]),
            'liquidity_profile': _classify_liquidity_profile(ticker, chains)
        }
    
    return cache
```

### 2. Liquidity Should Be Descriptive, Not Restrictive

**Current:** Absolute thresholds reject strategies
**New:** Contextual assessment with labels

```python
def _assess_liquidity_quality(
    chain_df: pd.DataFrame,
    underlying_price: float,
    actual_dte: int,
    is_leap: bool = False
) -> Dict:
    """
    Assess liquidity DESCRIPTIVELY - don't reject.
    
    Returns:
        {
            'quality': 'Excellent' / 'Good' / 'Acceptable' / 'Thin' / 'Illiquid',
            'spread_assessment': 'Tight' / 'Standard' / 'Wide' / 'Very Wide',
            'oi_assessment': 'Deep' / 'Moderate' / 'Light' / 'Sparse',
            'tradable': True / False,
            'reason_flags': ['wide_spread', 'low_oi'],
            'context': 'LEAP on elite stock - wide spreads normal'
        }
    """
    # Contextual thresholds based on price/DTE
    if underlying_price >= 1000:
        spread_acceptable = 20.0 if is_leap else 15.0
        oi_good = 10 if is_leap else 20
    elif underlying_price >= 500:
        spread_acceptable = 15.0 if is_leap else 12.0
        oi_good = 20 if is_leap else 40
    else:
        spread_acceptable = 10.0
        oi_good = 50
    
    median_spread = chain_df['spread_pct'].median()
    median_oi = chain_df['open_interest'].median()
    
    # DESCRIBE, don't reject
    if median_spread <= spread_acceptable * 0.5 and median_oi >= oi_good * 2:
        quality = 'Excellent'
    elif median_spread <= spread_acceptable and median_oi >= oi_good:
        quality = 'Good'
    elif median_spread <= spread_acceptable * 1.5 and median_oi >= oi_good * 0.5:
        quality = 'Acceptable'
    elif median_spread <= spread_acceptable * 2:
        quality = 'Thin'
    else:
        quality = 'Illiquid'
    
    flags = []
    if median_spread > spread_acceptable:
        flags.append('wide_spread')
    if median_oi < oi_good:
        flags.append('low_oi')
    
    return {
        'quality': quality,
        'spread_pct': median_spread,
        'oi': median_oi,
        'tradable': quality in ['Excellent', 'Good', 'Acceptable'],
        'reason_flags': flags,
        'context': _generate_context_message(underlying_price, actual_dte, is_leap)
    }
```

### 3. LEAPs as Separate Exploration Category

**Current:** LEAPs evaluated with short-term rules, fail, disappear
**New:** LEAPs get different evaluation criteria

```python
def _evaluate_leap_strategy(
    chain_data: Dict,
    strategy: str,
    trade_bias: str
) -> Dict:
    """
    Evaluate LEAP strategies separately with appropriate criteria.
    
    LEAPs are:
    - Different capital instrument (higher premium)
    - Different liquidity regime (wider spreads OK)
    - Different decay profile (low theta)
    - Different risk intent (long-term positioning)
    """
    leap_expirations = [e for e in chain_data['expirations'] 
                        if _calc_dte(e) >= 365]
    
    if not leap_expirations:
        return {
            'viable': False,
            'reason': 'No LEAP expirations available'
        }
    
    # Select best LEAP expiration (typically 12-18 months)
    best_leap = _select_best_leap_expiration(leap_expirations)
    chain = chain_data['chains'][best_leap]
    
    # LEAP-appropriate liquidity assessment
    liquidity = _assess_liquidity_quality(
        chain,
        underlying_price=chain_data['underlying_price'],
        actual_dte=_calc_dte(best_leap),
        is_leap=True  # Relaxed criteria
    )
    
    # Select strikes (LEAP prefers ITM for intrinsic value)
    strikes = _select_leap_strikes(chain, strategy, trade_bias)
    
    return {
        'viable': True,
        'category': 'LEAP',
        'expiration': best_leap,
        'dte': _calc_dte(best_leap),
        'strikes': strikes['strikes'],
        'capital_required': strikes['capital'],
        'liquidity_quality': liquidity['quality'],
        'spread_pct': liquidity['spread_pct'],
        'oi': liquidity['oi'],
        'annotation': f"LEAP | Capital: ${strikes['capital']} | Spread: {liquidity['spread_pct']:.1f}%"
    }
```

### 4. Capital as Annotation, Not Filter

```python
def _annotate_capital_feasibility(
    capital_required: float,
    typical_allocation: float = 5000
) -> Dict:
    """
    Annotate capital requirements - NEVER hide expensive trades.
    
    Professionals want to see high-capital trades even if they won't take them.
    """
    if capital_required <= typical_allocation * 0.5:
        category = 'Light'
    elif capital_required <= typical_allocation:
        category = 'Standard'
    elif capital_required <= typical_allocation * 2:
        category = 'Heavy'
    elif capital_required <= typical_allocation * 5:
        category = 'Very Heavy'
    else:
        category = 'Institutional'
    
    within_budget = capital_required <= typical_allocation
    
    return {
        'capital_required': capital_required,
        'capital_category': category,
        'within_typical_allocation': within_budget,
        'visibility_note': f"${capital_required:,.0f} required - {category} capital" + 
                          ("" if within_budget else " (exceeds typical allocation)")
    }
```

### 5. Multi-Leg Evaluated as Structure

**Current:** Straddle rejected if one leg fails liquidity
**New:** Evaluate the pair as a structure

```python
def _evaluate_multi_leg_structure(
    chain_df: pd.DataFrame,
    strategy: str,
    required_legs: List[Dict]
) -> Dict:
    """
    Evaluate multi-leg strategies as STRUCTURES, not individual legs.
    
    Example: Straddle needs ATM call + ATM put
    - Don't reject if call has OI=8 and put has OI=12
    - Instead: "Straddle viable but thin liquidity on call leg"
    """
    leg_results = []
    
    for leg in required_legs:
        leg_chain = chain_df[
            (chain_df['option_type'] == leg['type']) &
            (chain_df['strike'] == leg['strike'])
        ]
        
        if leg_chain.empty:
            leg_results.append({
                'leg': leg['name'],
                'status': 'Not Available',
                'viable': False
            })
        else:
            leg_data = leg_chain.iloc[0]
            leg_results.append({
                'leg': leg['name'],
                'status': 'Available',
                'viable': True,
                'liquidity': _assess_leg_liquidity(leg_data),
                'bid': leg_data['bid'],
                'ask': leg_data['ask'],
                'oi': leg_data['open_interest']
            })
    
    # Structure is viable if ALL legs available
    structure_viable = all(r['viable'] for r in leg_results)
    
    # But annotate quality per leg
    weakest_leg = min(leg_results, key=lambda x: x.get('oi', 0)) if structure_viable else None
    
    return {
        'structure_viable': structure_viable,
        'legs': leg_results,
        'weakest_leg': weakest_leg['leg'] if weakest_leg else None,
        'structure_annotation': _generate_structure_annotation(leg_results),
        'tradable': structure_viable and _all_legs_tradable(leg_results)
    }
```

---

## New Step 9B Output Schema

### Current Columns (reject/succeed binary):
- `Contract_Selection_Status`: Success / Failed / No_Chains
- `Selected_Strikes`: Only if success

### New Columns (exploration with annotation):
```python
# Discovery columns (ALWAYS populated)
'Exploration_Status': 'Discovered' / 'Not_Available'
'Strategy_Viable': True / False
'Expirations_Available': "[2025-02-14, 2025-03-21, ...]"

# Liquidity annotation (descriptive)
'Liquidity_Quality': 'Excellent' / 'Good' / 'Acceptable' / 'Thin' / 'Illiquid'
'Spread_Assessment': 'Tight' / 'Standard' / 'Wide' / 'Very Wide'
'OI_Assessment': 'Deep' / 'Moderate' / 'Light' / 'Sparse'

# Capital annotation (not a filter)
'Capital_Required': 6500.00
'Capital_Category': 'Heavy' / 'Very Heavy' / etc.
'Within_Allocation': True / False

# Strike/contract details (always populated if available)
'Best_Expiration': '2025-03-21'
'Best_Strikes': "[150.0, 155.0]"
'Best_Symbols': "['AAPL250321C150', 'AAPL250321C155']"
'DTE': 85

# Reason flags (comma-separated)
'Reason_Flags': "wide_spread, sparse_strikes, capital_heavy"

# Context explanation
'Exploration_Note': "LEAP on elite stock - wide spreads normal for this structure"

# Tradability assessment (for later filtering)
'Tradable': True / False
'Tradable_Reason': "Meets liquidity thresholds" / "Wide spread but acceptable for LEAP"
```

---

## Where Competition Happens (Later Steps)

Competition ONLY in:
- **Step 10 (PCS Recalibration):** Refine scores based on actual contracts
- **Step 11 (Strategy Pairing):** Compare strategies per ticker
- **Step 8 (Position Sizing):** Apply capital constraints
- **Final Ranking:** User goal alignment

These steps receive FULL VISIBILITY of all explored strategies with rich annotations.

---

## Implementation Plan

### Phase 1: Efficiency (Chain Caching)
1. ✅ Create `_build_chain_cache()` - fetch once per ticker
2. ✅ Modify main loop to use cached chains
3. ✅ Measure API call reduction (expect 50-70% reduction)

### Phase 2: Descriptive Liquidity
1. ✅ Replace `_filter_by_liquidity()` with `_assess_liquidity_quality()`
2. ✅ Add contextual thresholds (price-aware, DTE-aware)
3. ✅ Return descriptions instead of rejections

### Phase 3: LEAP Category
1. ✅ Add `_evaluate_leap_strategy()` separate path
2. ✅ Use relaxed LEAP criteria (wider spreads, lower OI OK)
3. ✅ Annotate as LEAP category

### Phase 4: Capital Annotation
1. ✅ Add `_annotate_capital_feasibility()`
2. ✅ Never hide expensive trades
3. ✅ Label as "exceeds allocation" instead

### Phase 5: Multi-Leg Structures
1. ✅ Add `_evaluate_multi_leg_structure()`
2. ✅ Evaluate pairs/combos as units
3. ✅ Annotate weak legs instead of rejecting

### Phase 6: New Output Schema
1. ✅ Add exploration columns
2. ✅ Preserve ALL strategies
3. ✅ Rich annotations for filtering later

---

## Expected Outcomes

### Before (Current):
- Input: 266 strategies
- Output: ~50-100 strategies (rest rejected/hidden)
- BKNG: "Illiquid" (disappeared)
- LEAPs: Missing (failed short-term rules)
- Step 11: Comparing subset

### After (New):
- Input: 266 strategies
- Output: 266 strategies (ALL preserved)
- BKNG: "Excellent quality, wide spread (20%) typical for elite stock"
- LEAPs: "LEAP | Capital: $6500 | Wide spread acceptable for 18-month horizon"
- Step 11: Comparing FULL catalog with rich context

---

## One-Sentence Summary

"The system must separate exploration from selection: during exploration (Step 9B), all viable strategies per ticker (including LEAPs and capital-heavy trades) must be discovered, labeled, and explained without competing against each other; rejection and optimization only occur after full visibility exists in Steps 10-11-8."

---

## Next Steps

1. Review this plan for alignment
2. Implement Phase 1 (chain caching) for efficiency wins
3. Implement Phase 2 (descriptive liquidity) for visibility
4. Test with BKNG / AAPL / high-capital tickers
5. Verify Step 11 receives full exploration catalog
6. Measure improvement in professional "trust" of outputs
