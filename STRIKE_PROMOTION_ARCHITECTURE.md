# Strike Promotion Architecture

**Date:** 2024-12-28  
**Status:** Implementation Required

---

## Directive

> "Strike selection should be range-based internally (delta bands / ATM proximity), but the engine must promote exactly one strike per strategy to execution. The UI should display only the selected strike and its Greeks — not full option chains or multi-strike tables. Full chains are debug-only, not decision surfaces."

---

## Current State (Before)

**Step 9B Strike Selection:**
- Returns: `'symbols': [contract1, contract2, ...]` (array of 2-4 contracts per strategy)
- Example: Put Credit Spread → `[short_put, long_put]`
- Example: Iron Condor → `[long_put, short_put, short_call, long_call]`

**Step 10/11:**
- Extract Greeks from `symbols` JSON array
- Calculate net Greeks for multi-leg strategies

**UI Display:**
- Shows full `Contract_Symbols` JSON dumps
- Exposes all strikes in raw format

**Problem:**
- UI cluttered with multi-leg details
- No clear "promoted strike" for decision-making
- Full option chains visible in production (should be debug-only)

---

## Target State (After)

**Step 9B Strike Selection:**
1. **Internal Exploration:** Range-based (delta bands, ATM proximity)
2. **Promotion:** Rank candidates and select exactly ONE strike per strategy
3. **Output:** Single promoted strike with complete metadata

**Promotion Criteria (by strategy type):**
- **Credit Spreads:** Highest credit/risk ratio (Cohen POP ≥65%)
- **Debit Spreads:** Best risk/reward with Delta ≥0.40
- **Straddles/Strangles:** ATM liquidity + Vega ≥0.25
- **Iron Condors:** Symmetric credit with best POP
- **Single Legs:** Highest Delta (directional) or Vega (volatility)

**UI Display:**
- One clean row per strategy
- Promoted strike: `Strike: 150.0 | Delta: 0.52 | Greeks: Γ=0.05, ν=0.18, θ=-0.12`
- No JSON dumps, no multi-strike tables
- Full chains hidden behind debug toggle

---

## Implementation Plan

### Phase 1: Add Promotion Function (Step 9B)

**File:** `core/scan_engine/step9b_fetch_contracts.py`

**New Function:**
```python
def _promote_best_strike(symbols: List[Dict], strategy: str, bias: str, underlying_price: float) -> Dict:
    """
    Promote exactly one strike from multi-leg strategy for execution and UI display.
    
    Args:
        symbols: List of contract dicts from _build_contract_with_greeks
        strategy: Strategy type (Credit Spread, Debit Spread, Straddle, etc.)
        bias: Bullish/Bearish/Neutral
        underlying_price: Current stock price
    
    Returns:
        Single promoted strike dict with complete metadata
    
    Promotion Criteria:
    - Credit Spreads: Highest credit/risk ratio (short strike promoted)
    - Debit Spreads: Best Delta/Theta ratio (long strike promoted)
    - Straddles: ATM strike with highest Vega
    - Iron Condors: Credit center (short put promoted)
    - Single Legs: Highest Delta (directional) or Vega (vol)
    """
```

**Modify Each Strategy Helper:**
```python
# BEFORE:
return {
    'symbols': [_build_contract_with_greeks(short_put), _build_contract_with_greeks(long_put)],
    ...
}

# AFTER:
all_contracts = [_build_contract_with_greeks(short_put), _build_contract_with_greeks(long_put)]
promoted_strike = _promote_best_strike(all_contracts, 'Credit Spread', bias, underlying_price)

return {
    'promoted_strike': promoted_strike,  # Single strike for UI/execution
    'all_contracts': all_contracts,      # Full leg details (debug only)
    ...
}
```

### Phase 2: Update Step 10/11 (Greek Extraction)

**File:** `core/scan_engine/step10_pcs_recalibration.py`

**Change:**
```python
# BEFORE:
symbols = json.loads(row['Contract_Symbols'])
for symbol in symbols:
    delta += symbol['Delta']
    gamma += symbol['Gamma']
    ...

# AFTER:
promoted = json.loads(row['Promoted_Strike'])
delta = promoted['Delta']
gamma = promoted['Gamma']
vega = promoted['Vega']
theta = promoted['Theta']
```

**Rationale:** Use promoted strike for PCS scoring (not net Greeks across all legs)

### Phase 3: Update UI (Single Strike Display)

**File:** `streamlit_app/dashboard.py`

**Step 9B Display:**
```python
# BEFORE:
display_cols = ['Ticker', 'Primary_Strategy', 'Contract_Symbols', ...]

# AFTER:
display_cols = ['Ticker', 'Primary_Strategy', 'Promoted_Strike_Summary', 'Delta', 'Gamma', 'Vega', 'Theta', ...]

# Extract promoted strike details into clean columns
df['Promoted_Strike_Summary'] = df['Promoted_Strike'].apply(lambda x: f"{x['Strike']} {x['Option_Type']}")
df['Delta'] = df['Promoted_Strike'].apply(lambda x: x['Delta'])
df['Gamma'] = df['Promoted_Strike'].apply(lambda x: x['Gamma'])
...
```

**Debug Toggle:**
```python
with st.expander("🔧 Debug: Full Contract Details", expanded=False):
    st.markdown("**All Legs (Debug Only):**")
    st.dataframe(df[['Ticker', 'All_Contracts']])
```

---

## Expected Outcomes

### Before:
- Step 9B Output: `symbols: [{"Strike": 150, ...}, {"Strike": 145, ...}]`
- UI: Raw JSON dumps
- Decision Clarity: Low (multiple strikes per row)

### After:
- Step 9B Output: `promoted_strike: {"Strike": 150, "Delta": 0.52, ...}`
- UI: Clean single-strike rows with Greeks
- Decision Clarity: High (one clear choice per strategy)

---

## Validation Criteria

✅ **Step 9B:** Each strategy returns exactly one promoted strike  
✅ **Step 10:** PCS scoring uses promoted strike (not net Greeks)  
✅ **Step 11:** Validation references single promoted strike  
✅ **UI:** No JSON dumps in main view  
✅ **UI:** Full chains behind debug toggle  
✅ **Performance:** No regression (promotion is O(n) where n ≤ 4 legs)

---

## Next Steps

1. Implement `_promote_best_strike()` function
2. Modify all 8 strategy helpers to call promotion logic
3. Update Step 10/11 to consume promoted strike
4. Refactor UI to display single strike + Greeks cleanly
5. Add debug toggle for full contract details
6. Test with real data (Meta, GOOGL)

