# PCS Analysis: Available Data vs. Current Usage

## Executive Summary

**Problem:** PCS (Premium Collection Score) system is **underutilizing** rich data from Step 9B option chains. We fetch Greeks, IV, spreads, OI, volume - but PCS doesn't properly consume most of it.

**Impact:** PCS scores are **generic and not strategy-aware**. Missing opportunity to penalize thin liquidity, wide spreads, poor Greeks alignment.

---

## What Data We're Fetching (Step 9B)

### From Tradier API (per contract)
```python
# Raw chain data from _fetch_chain_with_greeks()
{
    # Pricing
    'strike': 180.0,
    'bid': 5.0,
    'ask': 5.5,
    'last': 5.25,
    'mid_price': 5.25,              # ← Calculated
    'spread_pct': 9.5,               # ← Calculated
    
    # Market depth
    'volume': 1000,
    'open_interest': 5000,
    
    # Option basics
    'option_type': 'call',
    'expiration_date': '2025-02-14',
    'underlying': 180.0,
    'underlying_price': 180.0,
    
    # Greeks (from Tradier with greeks=true)
    'delta': 0.52,
    'gamma': 0.03,
    'theta': -0.15,
    'vega': 0.25,
    'rho': 0.10,
    'phi': -0.05,
    
    # IV metrics
    'bid_iv': 0.22,
    'mid_iv': 0.23,
    'ask_iv': 0.24,
    'smv_vol': 0.23                  # ← Smoothed vol
}
```

### Aggregated by Step 9B (per strategy)
```python
# What Step 9B produces
{
    # Basics
    'Ticker': 'AAPL',
    'Primary_Strategy': 'Long Call',
    'Trade_Bias': 'Bullish',
    'Actual_DTE': 42,
    
    # Selected contracts
    'Selected_Strikes': '[180.0]',           # JSON
    'Contract_Symbols': '[...]',             # JSON (includes Greeks!)
    'Option_Type': 'call',
    
    # Risk metrics
    'Actual_Risk_Per_Contract': 550.0,
    'Total_Debit': 550.0,
    'Total_Credit': 0.0,
    'Risk_Model': 'Debit_Max',
    
    # Liquidity metrics
    'Bid_Ask_Spread_Pct': 9.5,               # ✅ Available
    'Open_Interest': 5000,                   # ✅ Available
    'Liquidity_Score': 75.0,                 # ✅ Calculated (OI+spread+volume)
    'Liquidity_Class': 'Normal',             # ✅ Categorized
    'Liquidity_Context': 'LEAP: wider spreads normal',  # ✅ Descriptive
    
    # Chain audit
    'Chain_Liquid': True,
    'Total_Strikes_Scanned': 120,
    'Calls_With_Bid': 60,
    'Puts_With_Bid': 60,
    'Median_Call_Spread_Pct': 8.5,
    'Median_Put_Spread_Pct': 7.8,
    'Max_OI_Seen': 12000,
    'ATM_Strike_Liquid': True,
    'Chain_Audit_Summary': 'Excellent depth...',
    
    # Candidate preservation
    'Candidate_Contracts': '[{\"strike\": 180, \"spread_pct\": 12.5, ...}]',  # ✅ Phase 1 fix
    'Num_Candidates': 3,
    
    # Status
    'Contract_Selection_Status': 'Success',
    'Contract_Executable': True
}
```

**KEY ISSUE:** Greeks are stored in `Contract_Symbols` JSON, **not as separate columns**. PCS can't easily access them.

---

## What PCS Is Currently Using

### Step 10 (step10_pcs_recalibration.py)
```python
# USES:
✅ Liquidity_Score (30% weight)
✅ Bid_Ask_Spread_Pct (checked against threshold)
✅ Open_Interest (checked against threshold)
✅ Actual_DTE (20% weight)
✅ Risk_Model (20% weight)
✅ Contract_Selection_Status (gate)

# NOT USING:
❌ Greeks (Delta, Gamma, Vega, Theta)
❌ IV metrics (bid_iv, mid_iv, ask_iv)
❌ Liquidity_Class (Excellent/Normal/Thin/Poor)
❌ Liquidity_Context (descriptive explanation)
❌ Candidate_Contracts (for marginal cases)
❌ Chain audit metrics (median spread, max OI)
❌ Volume (used in Liquidity_Score, but not separately)
```

**Scoring:**
```python
# Current PCS formula (simplified)
pcs_score = (
    0.30 * liquidity_component +    # Liquidity_Score / 100
    0.20 * dte_component +          # Actual_DTE / 60
    0.20 * risk_component +         # Risk_Model clarity (100/50/0)
    0.30 * strategy_component       # Strategy-specific rules
)
```

**Problems:**
1. **No Greek validation** - Can't detect mismatched strategies (e.g., directional with |Delta| < 0.20)
2. **No IV metrics** - Can't penalize overpriced options (high ask_iv)
3. **No granular liquidity** - Binary pass/fail, doesn't penalize gradations
4. **Strategy-specific validation weak** - Only checks DTE and structure
5. **No candidate evaluation** - Ignores near-miss contracts

### Legacy PCS Engines (pcs_engine_v3_unified.py, phase3_enrich/pcs_score.py)

```python
# pcs_engine_v3_unified.py USES:
✅ Gamma
✅ Vega
✅ Delta
✅ Theta
✅ Days_Held
✅ Chart_Trend
✅ Strategy (for tiering)

# phase3_enrich/pcs_score.py USES:
✅ Gamma
✅ Vega
✅ Premium
✅ Basis
✅ Strategy (for profile)
✅ Expiration (for DTE calc)
```

**KEY ISSUE:** These are **portfolio monitoring** engines (for active trades), **not** contract selection engines (for Step 10).

They assume:
- Trades already entered
- Greeks available as columns
- Chart data available
- Historical basis/premium tracked

**They don't work for Step 9B → Step 10 flow** where:
- Contracts not yet entered
- Greeks in JSON, not columns
- No historical data
- Pure option chain analysis

---

## Gap Analysis: What's Missing

### Critical Gap: Greek Extraction

**Problem:**
```python
# Step 9B stores Greeks in JSON
df['Contract_Symbols'] = '[{"symbol": "AAPL250214C180", "delta": 0.52, "vega": 0.25, ...}]'

# Step 10 expects Greeks as columns
if 'Delta' in row.columns:  # ← Never True!
    delta = row['Delta']
```

**Solution:**
```python
# Need to extract Greeks from Contract_Symbols JSON
def extract_greeks_from_contracts(df):
    """Extract Greeks from Contract_Symbols JSON to separate columns"""
    for idx, row in df.iterrows():
        contracts = json.loads(row['Contract_Symbols'])
        
        # Aggregate Greeks across multi-leg strategies
        if len(contracts) == 1:
            # Single-leg: Direct assignment
            df.at[idx, 'Delta'] = contracts[0].get('delta')
            df.at[idx, 'Vega'] = contracts[0].get('vega')
            df.at[idx, 'Gamma'] = contracts[0].get('gamma')
            df.at[idx, 'Theta'] = contracts[0].get('theta')
        else:
            # Multi-leg: Net Greeks
            df.at[idx, 'Delta'] = sum(c.get('delta', 0) for c in contracts)
            df.at[idx, 'Vega'] = sum(c.get('vega', 0) for c in contracts)
            # etc.
    
    return df
```

### Critical Gap: Strategy-Aware Greek Validation

**Current:** Generic DTE/liquidity checks
**Needed:** Strategy-specific Greek thresholds

```python
# Directional strategies (Long Call/Put, Debit Spreads)
REQUIRES:
  - |Delta| > 0.35 (meaningful directional exposure)
  - Vega > 0.18 (sensitive to IV changes)
  
# Volatility strategies (Straddle/Strangle)
REQUIRES:
  - Vega > 0.25 (high IV sensitivity)
  - |Delta| < 0.15 (neutral bias)
  - Gamma > 0.02 (responsive to movement)
  
# Income strategies (CSP, Covered Call)
REQUIRES:
  - |Theta| > Vega (time decay > IV risk)
  - Delta 0.20-0.40 (moderate exposure)
  
# Credit spreads (Bull Put, Bear Call)
REQUIRES:
  - Delta aligns with bias
  - Theta positive (collecting premium)
```

### Critical Gap: Liquidity Penalty Gradient

**Current:** Binary pass/fail
```python
if spread_pct > max_spread_pct:
    status = 'Rejected'  # Binary
```

**Needed:** Gradient penalty
```python
# Penalize proportionally, don't reject
if spread_pct > 8.0:
    penalty = (spread_pct - 8.0) * 5  # -5 points per % over threshold
    pcs_score -= penalty
    reason = f"Wide spread: {spread_pct:.1f}% (-{penalty:.0f} points)"
    status = 'Watch' if pcs_score >= 60 else 'Rejected'
```

### Critical Gap: IV Premium/Discount

**Current:** Not evaluated
**Available:** bid_iv, mid_iv, ask_iv
**Needed:** Compare mid_iv to historical IV

```python
# Penalize overpriced options (buying high IV)
if strategy in ['Long Call', 'Long Put', 'Long Straddle']:
    iv_percentile = get_iv_percentile(ticker, mid_iv)  # Need historical IV
    if iv_percentile > 75:
        penalty = (iv_percentile - 75) * 0.5
        pcs_score -= penalty
        reason = f"Expensive IV: {iv_percentile}th percentile"
```

### Critical Gap: Candidate Contract Evaluation

**Current:** Ignored
**Available:** `Candidate_Contracts` JSON (Phase 1 fix)
**Needed:** Evaluate near-miss contracts for PCS override

```python
# Example: Strategy has thin liquidity but good candidates
if status == 'Explored_Thin_Liquidity':
    candidates = json.loads(row['Candidate_Contracts'])
    
    # Check if candidates are "good enough"
    best_candidate = candidates[0]
    if best_candidate['spread_pct'] < 15.0 and best_candidate['open_interest'] > 5:
        status = 'Watch'  # Don't outright reject
        pcs_score = 50  # Reduced but not zero
        reason = f"Thin liquidity but viable candidate: {best_candidate['strike']}"
```

---

## Redesigned PCS Architecture

### Phase 1: Greek Extraction (PREREQUISITE)

Add to Step 9B or Step 10 entry point:
```python
def extract_greeks_to_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract Greeks from Contract_Symbols JSON to separate columns.
    Handles single-leg and multi-leg strategies.
    
    Adds columns: Delta, Gamma, Vega, Theta, Rho, IV_Mid
    """
    df['Delta'] = 0.0
    df['Gamma'] = 0.0
    df['Vega'] = 0.0
    df['Theta'] = 0.0
    df['Rho'] = 0.0
    df['IV_Mid'] = 0.0
    
    for idx, row in df.iterrows():
        try:
            contracts = json.loads(row['Contract_Symbols'])
            
            if len(contracts) == 1:
                # Single-leg: Direct Greeks
                c = contracts[0]
                df.at[idx, 'Delta'] = c.get('delta', 0.0)
                df.at[idx, 'Gamma'] = c.get('gamma', 0.0)
                df.at[idx, 'Vega'] = c.get('vega', 0.0)
                df.at[idx, 'Theta'] = c.get('theta', 0.0)
                df.at[idx, 'Rho'] = c.get('rho', 0.0)
                df.at[idx, 'IV_Mid'] = c.get('mid_iv', 0.0)
            else:
                # Multi-leg: Net Greeks
                df.at[idx, 'Delta'] = sum(c.get('delta', 0) for c in contracts)
                df.at[idx, 'Gamma'] = sum(c.get('gamma', 0) for c in contracts)
                df.at[idx, 'Vega'] = sum(c.get('vega', 0) for c in contracts)
                df.at[idx, 'Theta'] = sum(c.get('theta', 0) for c in contracts)
                df.at[idx, 'Rho'] = sum(c.get('rho', 0) for c in contracts)
                # IV: Weighted average by vega
                total_vega = sum(abs(c.get('vega', 0)) for c in contracts)
                if total_vega > 0:
                    df.at[idx, 'IV_Mid'] = sum(
                        c.get('mid_iv', 0) * abs(c.get('vega', 0)) 
                        for c in contracts
                    ) / total_vega
        except (json.JSONDecodeError, KeyError, TypeError):
            pass  # Keep defaults (0.0)
    
    return df
```

### Phase 2: Strategy-Aware Scoring

```python
def calculate_pcs_score_v2(row: pd.Series) -> Tuple[float, List[str]]:
    """
    Calculate PCS score with strategy-aware penalties.
    
    Returns: (score, reasons)
        score: 0-100 (higher = better)
        reasons: List of penalty explanations
    """
    score = 100.0
    reasons = []
    
    strategy = row['Primary_Strategy']
    trade_bias = row['Trade_Bias']
    
    # === LIQUIDITY PENALTIES (30% weight) ===
    spread_pct = row['Bid_Ask_Spread_Pct']
    oi = row['Open_Interest']
    liquidity_score = row['Liquidity_Score']
    
    # Gradient penalty for spread
    if spread_pct > 8.0:
        penalty = (spread_pct - 8.0) * 2.0
        score -= penalty
        reasons.append(f"Wide spread: {spread_pct:.1f}% (-{penalty:.0f})")
    
    # Gradient penalty for OI
    if oi < 50:
        penalty = (50 - oi) / 5.0
        score -= penalty
        reasons.append(f"Low OI: {oi} (-{penalty:.0f})")
    
    # === GREEK PENALTIES (40% weight) ===
    delta = row.get('Delta', 0.0)
    vega = row.get('Vega', 0.0)
    gamma = row.get('Gamma', 0.0)
    theta = row.get('Theta', 0.0)
    
    # Directional strategies
    if strategy in ['Long Call', 'Long Put', 'Bull Call Spread', 'Bear Put Spread']:
        if abs(delta) < 0.35:
            penalty = (0.35 - abs(delta)) * 50
            score -= penalty
            reasons.append(f"Weak directional exposure: |Δ|={abs(delta):.2f} (-{penalty:.0f})")
        
        if vega < 0.18:
            penalty = (0.18 - vega) * 30
            score -= penalty
            reasons.append(f"Low IV sensitivity: ν={vega:.2f} (-{penalty:.0f})")
    
    # Volatility strategies
    elif strategy in ['Long Straddle', 'Long Strangle']:
        if vega < 0.25:
            penalty = (0.25 - vega) * 40
            score -= penalty
            reasons.append(f"Insufficient vega: ν={vega:.2f} (-{penalty:.0f})")
        
        if abs(delta) > 0.15:
            penalty = (abs(delta) - 0.15) * 30
            score -= penalty
            reasons.append(f"Not neutral: |Δ|={abs(delta):.2f} (-{penalty:.0f})")
    
    # Income strategies
    elif strategy in ['Cash-Secured Put', 'Covered Call']:
        if abs(theta) < vega:
            penalty = (vega - abs(theta)) * 20
            score -= penalty
            reasons.append(f"Theta < Vega: θ/ν imbalance (-{penalty:.0f})")
    
    # === DTE PENALTIES (15% weight) ===
    dte = row['Actual_DTE']
    if dte < 7:
        penalty = (7 - dte) * 3
        score -= penalty
        reasons.append(f"Very short DTE: {dte} days (-{penalty:.0f})")
    
    # === RISK PENALTIES (15% weight) ===
    risk = row.get('Actual_Risk_Per_Contract', 0.0)
    if risk > 5000:  # Over $5k risk
        penalty = (risk - 5000) / 200
        score -= min(penalty, 15.0)
        reasons.append(f"High risk: ${risk:.0f} (-{min(penalty, 15):.0f})")
    
    return max(0.0, min(100.0, score)), reasons
```

### Phase 3: Status Classification

```python
def classify_pcs_status(pcs_score: float, reasons: List[str]) -> str:
    """
    Classify PCS status based on score and reasons.
    
    Returns:
        'Valid' - Ready for execution (PCS ≥ 70)
        'Watch' - Marginal but tradeable (PCS 50-69)
        'Rejected' - Should not trade (PCS < 50)
    """
    if pcs_score >= 70:
        return 'Valid'
    elif pcs_score >= 50:
        return 'Watch'
    else:
        return 'Rejected'
```

---

## Implementation Plan

### Step 1: Greek Extraction Function ✅
- Add `extract_greeks_to_columns()` to Step 10 entry point
- Test on sample data with Contract_Symbols JSON
- Validate multi-leg Greek aggregation

### Step 2: Enhanced PCS Scoring ✅
- Implement `calculate_pcs_score_v2()` with gradient penalties
- Add strategy-specific Greek validation
- Test thresholds with real data

### Step 3: Candidate Evaluation (Optional)
- Add `evaluate_candidate_contracts()` for Explored_* statuses
- Provide second chance to marginal strategies
- Document override criteria

### Step 4: Integration Testing
- Run full pipeline: Step 7 → 9A → 9B → 10 (new)
- Compare old PCS vs. new PCS scores
- Validate status distribution (expect more 'Watch', fewer hard rejections)

### Step 5: Dashboard Enhancement
- Display Greek columns in Step 10 output
- Show PCS penalty breakdown
- Add candidate contract viewer

---

## Expected Impact

### Before (Current)
```
Status Distribution:
  Valid: 45%
  Rejected: 55%
  
PCS Score Range: 0, 40, 60, 80, 100 (lumpy)
Greeks: Not validated
Liquidity: Binary pass/fail
Candidate contracts: Ignored
```

### After (Enhanced)
```
Status Distribution:
  Valid: 30% (high quality)
  Watch: 45% (marginal but trackable)
  Rejected: 25% (true failures)
  
PCS Score Range: 0-100 (smooth gradient)
Greeks: Validated per strategy type
Liquidity: Gradient penalties
Candidate contracts: Evaluated for overrides
```

### Key Improvements
1. **Greek-aware:** Detect mismatched strategies (directional with low delta)
2. **Gradient scoring:** Smooth penalties instead of binary rejection
3. **Strategy-specific:** Different rules for directional/volatility/income
4. **Candidate-aware:** Second look at near-miss contracts
5. **Descriptive:** Clear penalty breakdown (not just "Rejected")

---

## Conclusion

**Current State:** PCS is a **placeholder** that checks basic liquidity/DTE but **ignores 60% of available data**.

**Root Cause:** Greeks stored in JSON, not extracted to columns. Step 10 can't use them.

**Fix:** 
1. Extract Greeks to columns (1 function, ~50 lines)
2. Enhance PCS with strategy-aware scoring (1 function, ~100 lines)
3. Add gradient penalties (replace binary pass/fail)

**ROI:** High - unlocks rich Tradier data already being fetched, makes PCS actually useful for ranking/filtering.

**Next:** Implement Phase 1 (Greek extraction) + Phase 2 (enhanced scoring).

---

**Date:** December 28, 2025  
**Status:** Analysis Complete - Ready for implementation  
**Priority:** HIGH - PCS is current bottleneck after chain caching solved
