# Step 12: Price-Aware Liquidity + LEAP Fallback Implementation

**Status**: ✅ Design Approved | 🔄 Implementation In Progress  
**Date**: December 28, 2025

---

## 1️⃣ Core Problem (Validated)

### Current Behavior (Structurally Incorrect)
- **Fixed liquidity rules**: `min_OI = 50`, `max_spread = 10%`  
- **Applied universally** to all stock prices ($50 → $3,000)  
- **Result**: Elite large-caps (BKNG, AZO, MTD, FICO, MELI) systematically rejected  

### Why This Is Wrong
Liquidity is **relative to underlying price**, not absolute.

A $3,000 stock will **never** have contract-level liquidity comparable to a $150 stock under the same thresholds.

---

## 2️⃣ Solution: Price-Aware Liquidity Scaling

### Proposed Buckets (Market-Realistic)

| Underlying Price | Min OI | Max Spread | Rationale |
|-----------------|--------|------------|-----------|
| **< $200** | 50 | 10% | Strict (prevent small/junk stocks) |
| **$200-500** | 25 | 12% | Moderate (mid-caps) |
| **$500-1000** | 15 | 15% | Relaxed (large-caps) |
| **≥ $1000** | 5 | 20% | Realistic (elite names) |

### DTE Adjustments (Applied AFTER Price Buckets)

| DTE Range | OI Adjustment | Spread Adjustment |
|-----------|--------------|-------------------|
| **LEAPS (≥365)** | ÷2 (half) | ×1.25 (25% wider) |
| **Medium (60-364)** | ×0.75 (75%) | ×1.15 (15% wider) |
| **Short (<60)** | No change | No change |

**Example**: BKNG @ $3,000 with 400 DTE LEAP  
- Base thresholds: OI≥5, spread≤20%  
- LEAP adjustment: OI≥2, spread≤25%

---

## 3️⃣ LEAP Fallback Logic (Explicit Opt-In)

### When to Trigger LEAP Fallback
```
IF short-term contracts fail liquidity filter
AND strategy is LEAP-eligible
AND underlying price ≥ $300
THEN:
    Retry with DTE 365-730 days
    Apply price-aware liquidity (relaxed)
    Tag: Is_LEAP=True, Selection_Mode='LEAP_Fallback'
```

### LEAP-Eligible Strategies
- ✅ Long Call  
- ✅ Long Put  
- ✅ Buy-Write (optional)  
- ✅ Covered Call (with cost-basis checks)

### LEAP-Incompatible Strategies
- ❌ Long Straddle  
- ❌ Long Strangle  
- ❌ Short premium strategies (Put Spreads, Call Spreads)

---

## 4️⃣ Visibility Columns (Auditability)

### New Tracking Columns
Even when contracts **fail**, persist full attempt details:

| Column | Type | Purpose |
|--------|------|---------|
| `Underlying_Price` | float | Stock price used for bucketing |
| `Is_LEAP` | bool | True if LEAP fallback used |
| `Selection_Mode` | string | 'Standard' or 'LEAP_Fallback' |
| `Liquidity_Profile` | string | e.g. 'Elite_$3000_LEAP_Relaxed' |
| `Attempted_DTE` | int | Target DTE before attempt |
| `Failure_Reason` | string | Why contract selection failed |
| `Closest_Expiration_Considered` | datetime | Best expiry found |
| `Best_Strike_Considered` | float | Closest strike evaluated |

**Why**: Without this, system feels "broken" even when behaving correctly.

---

## 5️⃣ Implementation Status

### ✅ Completed
1. **STRATEGY_LEAP_ELIGIBLE** constant added to `step9b_fetch_contracts.py`  
2. **_get_price_aware_liquidity_thresholds()** function created  
3. **Visibility columns** initialized in Step 9B  
4. **Backward compatibility** maintained (_get_dte_adjusted_liquidity_thresholds deprecated but kept)

### 🔄 In Progress (Need to Update Main Loop)
Location: [core/scan_engine/step9b_fetch_contracts.py:237-330](core/scan_engine/step9b_fetch_contracts.py#L237-L330)

**Changes Needed**:
1. Replace `_get_dte_adjusted_liquidity_thresholds()` call with `_get_price_aware_liquidity_thresholds()`  
2. Extract `underlying_price` from chain data  
3. Add LEAP fallback logic when `filtered_chain.empty`  
4. Populate visibility columns: `Failure_Reason`, `Best_Strike_Considered`, `Liquidity_Profile`  
5. Tag LEAPs: `Is_LEAP`, `Selection_Mode`, `Liquidity_Profile` += '_LEAP_Relaxed'

### 📋 Step 10 Updates (PCS Recalibration)
Location: [core/scan_engine/step10_pcs_recalibration.py:140-200](core/scan_engine/step10_pcs_recalibration.py#L140-L200)

**Changes Needed**:
1. Use price-aware thresholds in validation logic  
2. Recognize and score `Is_LEAP` flag appropriately  
3. Adjust spread/OI validation based on `Liquidity_Profile`

---

## 6️⃣ Testing Plan

### Test Cases
1. **Small stock** ($50): Strict thresholds, no LEAP fallback  
2. **Mid-cap** ($300): Moderate thresholds, LEAP fallback available  
3. **Large-cap** ($800): Relaxed thresholds, LEAP fallback if needed  
4. **Elite stock** ($3000): Realistic thresholds, LEAP primary candidate  

### Validation
- No "small stock leakage" (strict rules preserved)  
- Elite names (BKNG, AZO, MELI) now visible  
- LEAPs explicitly tagged, never silent  
- Failure reasons captured for audit

---

## 7️⃣ Why This Prevents Small-Stock Leakage

| Stock Type | Price | OI Req | Spread Cap | Risk Level |
|------------|-------|--------|------------|------------|
| Junk/Small | $50 | **50** | **10%** | ✅ Strict filtering |
| Mid-Cap | $300 | 25 | 12% | 🔒 Moderate filtering |
| Large-Cap | $800 | 15 | 15% | ⚖️ Normalized |
| Elite | $3000 | 5 | 20% | 🎯 Realistic |

**Key**: Small stocks still face **strict thresholds**. Only expensive, high-quality stocks get relaxed rules.

---

## 8️⃣ Next Steps

1. **Complete Step 9B main loop update** (lines 237-330)  
   - Add price extraction  
   - Implement LEAP fallback  
   - Populate visibility columns

2. **Update Step 10 validation** (lines 140-200)  
   - Recognize price buckets  
   - Score LEAPs appropriately  
   - Adjust thresholds dynamically

3. **Test full pipeline** with price spectrum:  
   - Run CLI test with tickers: AAPL ($150), BKNG ($3,000), AZO ($2,500)  
   - Validate liquidity buckets applied correctly  
   - Confirm LEAP fallback triggers when appropriate

4. **Update dashboard** to display:  
   - `Liquidity_Profile` column  
   - `Is_LEAP` badge  
   - `Failure_Reason` for rejected contracts

---

## 9️⃣ Code Snippets for Reference

### Price-Aware Thresholds (Already Implemented)
```python
def _get_price_aware_liquidity_thresholds(
    underlying_price: float,
    actual_dte: int
) -> Tuple[int, float]:
    """Calculate liquidity thresholds based on price AND DTE."""
    
    # Price buckets
    if underlying_price < 200:
        base_min_oi, base_max_spread = 50, 10.0
    elif underlying_price < 500:
        base_min_oi, base_max_spread = 25, 12.0
    elif underlying_price < 1000:
        base_min_oi, base_max_spread = 15, 15.0
    else:
        base_min_oi, base_max_spread = 5, 20.0
    
    # DTE adjustments
    if actual_dte >= 365:  # LEAPS
        return max(2, base_min_oi // 2), base_max_spread * 1.25
    elif actual_dte >= 60:  # Medium-term
        return max(3, int(base_min_oi * 0.75)), base_max_spread * 1.15
    else:
        return base_min_oi, base_max_spread
```

### LEAP Fallback Pattern (Pseudocode)
```python
if filtered_chain.empty:
    if strategy in STRATEGY_LEAP_ELIGIBLE and underlying_price >= 300:
        # Retry with LEAP DTE range (365-730)
        leap_expirations = _get_expirations_in_dte_range(ticker, 365, 730, token)
        if leap_expirations:
            leap_filtered = _filter_by_liquidity(leap_chain, leap_oi, leap_spread)
            if not leap_filtered.empty:
                # Success! Tag as LEAP
                df.at[idx, 'Is_LEAP'] = True
                df.at[idx, 'Selection_Mode'] = 'LEAP_Fallback'
                df.at[idx, 'Liquidity_Profile'] += '_LEAP_Relaxed'
```

---

## 🎯 Expected Outcome

**Before**:  
- BKNG ($3,000) → Rejected (OI<50, spread>10%)  
- AZO ($2,500) → Rejected  
- FICO ($2,000) → Rejected  
- MELI ($1,800) → Rejected

**After**:  
- BKNG ($3,000) → ✅ Pass (OI≥5, spread≤20%) or LEAP fallback  
- AZO ($2,500) → ✅ Pass (OI≥5, spread≤20%) or LEAP fallback  
- FICO ($2,000) → ✅ Pass (OI≥5, spread≤20%) or LEAP fallback  
- MELI ($1,800) → ✅ Pass (OI≥5, spread≤20%) or LEAP fallback

**Small stocks still rejected** (strict rules preserved).

---

**Implementation Date**: December 28, 2025  
**Design Author**: User + GitHub Copilot  
**Status**: Core functions implemented, main loop integration pending
