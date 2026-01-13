# Persona-Based System Audit Results

**Date**: January 4, 2026  
**Command**: `python audit_persona_compliance.py --all --options-only`  
**Positions Analyzed**: 16 options

---

## Executive Summary

### Overall Scores by Persona
| Persona | Score | Status |
|---------|-------|--------|
| **INCOME** | 36.1/100 | ❌ Needs Work |
| **NEUTRAL_VOL** | 36.1/100 | ❌ Needs Work |
| **DIRECTIONAL** | 44.4/100 | ⚠️ Fair |

### Critical Findings
1. **🔴 IV_Rank Historical Data**: 0% coverage - CRITICAL for NEUTRAL_VOL
2. **🔴 Strategy Naming Mismatch**: System uses "Covered Call" but RAG expects "Covered_Call" 
3. **🔴 Missing Metrics**: Theta_Efficiency, ROI, Assignment_Risk not in dataset
4. **🟡 Exit Triggers**: Persona-specific triggers not fully implemented
5. **🟡 PCS Profile Alignment**: Only 12.5% of positions match expected persona profiles

---

## Detailed Audit Results

### INCOME Persona (36.1/100)
**Focus**: Theta decay, premium collection, ROI optimization  
**Primary Strategies**: CSP, Covered_Call, Credit_Spread, Iron_Condor

#### Data Completeness: ❌ 50/100
**Missing**:
- `Theta_Efficiency` - Not computed
- `ROI` - Column missing (have Unrealized_PnL but not ROI)
- `Assignment_Risk` - Computed but named `Assignment_Risk_Level`

**Recommendation**: 
```python
# Add to phase3_enrich
df['ROI'] = df['Unrealized_PnL'] / df['Basis']
df['Theta_Efficiency'] = np.abs(df['Theta']) / np.abs(df['Premium'])
```

#### PCS Weights: ❌ 0/100
**Issue**: Profile case mismatch
- Expected: `INCOME` 
- Actual: `Income` (lowercase 'i')

**Actual Distribution**:
- Income: 10 positions ✅
- Directional_Bull: 4 positions
- Neutral_Vol: 2 positions

**Recommendation**: Fix case sensitivity in Entry_PCS_Profile assignment.

#### Strategy Alignment: ❌ 0/100
**Issue**: Strategy name mismatch
- Expected: `Covered_Call` (underscore)
- Actual: `Covered Call` (space)

**Actual Strategies**:
- Covered Call: 9 (should be `Covered_Call`)
- Buy Call: 4
- Long Straddle: 2
- Cash-Secured Put: 1 (should be `CSP`)

**Recommendation**: Standardize strategy names in Phase 2 to match RAG expectations.

#### Exit Triggers: ❌ 0/100
**Missing Triggers**:
1. `profit_target_50pct` - Need to enhance Exit_Rationale text
2. `assignment_risk` - Need to reference ITM/pin risk in rationale
3. `theta_exhaustion` - Not implemented

**Recommendation**: Update `exit_recommendations.py` to include trigger keywords in rationale.

#### Target Metrics: ✅ 100/100
**Warnings**:
- 9 positions outside optimal DTE range (30-60 days)

**Recommendation**: Add DTE range alerts for INCOME persona.

#### Current_PCS v2: ⚠️ 67/100
**Component Coverage**:
- IV_Rank: 0% ❌
- Liquidity: 100% ✅
- Greeks: 100% ✅

---

### NEUTRAL_VOL Persona (36.1/100)
**Focus**: Volatility premium capture, IV_Rank exploitation, vega management  
**Primary Strategies**: Straddle, Strangle, Iron_Condor, Iron_Butterfly

#### Data Completeness: ❌ 50/100
**Missing**:
- `Moneyness` - Named `Moneyness_Label` instead

**Incomplete**:
- `IV_Rank`: 0% coverage ❌ **CRITICAL**
- `IV_Rank_Drift`: 0% coverage ❌ **CRITICAL**

**Recommendation**: 
```bash
# Populate IV_Rank historical data
# Run: python populate_iv_rank_history.py --lookback 252
```

#### PCS Weights: ❌ 0/100
Same issue as INCOME (case mismatch).

#### Strategy Alignment: ❌ 0/100
**Issue**: No Straddle/Strangle/Iron_Condor positions detected
- Actual: "Long Straddle" (2 positions)
- Expected: "Straddle"

**Recommendation**: Add strategy aliases in Phase 2 parser.

#### Exit Triggers: ❌ 0/100
**Missing Triggers**:
1. `iv_collapse` - IV_Rank_Drift < -30 not in rationale
2. `vega_decay` - Vega deterioration not referenced
3. `profit_target_50pct`

**Recommendation**: Add IV-specific triggers to exit logic.

#### Current_PCS v2: ⚠️ 67/100
**CRITICAL ISSUE**: IV_Rank component at 0% - **essential for NEUTRAL_VOL persona**

---

### DIRECTIONAL Persona (44.4/100)
**Focus**: Directional conviction, delta exposure, gamma acceleration  
**Primary Strategies**: Buy_Call, Buy_Put, Debit_Spread, Vertical_Spread

#### Data Completeness: ✅ 100/100
All required metrics present!

#### PCS Weights: ❌ 0/100
Same case mismatch issue.

#### Strategy Alignment: ❌ 0/100
**Issue**: Strategy name mismatch
- Expected: `Buy_Call`
- Actual: `Buy Call` (space instead of underscore)

#### Exit Triggers: ❌ 0/100
**Missing Triggers**:
1. `profit_target_100pct` - Higher target for directional plays
2. `chart_breakdown` - Chart regime not in rationale
3. `gamma_decay_75pct` - Gamma_Drift_Pct threshold not mentioned

**Recommendation**: Add directional-specific triggers.

#### Target Metrics: ✅ 100/100
**Warnings**:
- 1 position with weak Delta (<0.30)

**Recommendation**: Alert on low delta for DIRECTIONAL persona.

---

## Action Plan

### Phase 1: CRITICAL Fixes (Immediate)

#### 1.1 Fix Strategy Name Standardization
**File**: `core/phase2_parse.py`
**Issue**: Strategy names use spaces instead of underscores

```python
# Current: "Covered Call", "Buy Call", "Cash-Secured Put"
# Expected: "Covered_Call", "Buy_Call", "CSP"

STRATEGY_ALIASES = {
    'Covered Call': 'Covered_Call',
    'Buy Call': 'Buy_Call',
    'Buy Put': 'Buy_Put',
    'Cash-Secured Put': 'CSP',
    'Long Straddle': 'Straddle',
    'Short Straddle': 'Short_Straddle',
    # ... etc
}
```

**Impact**: Fixes 0% Strategy Alignment scores across all personas

---

#### 1.2 Fix Entry_PCS_Profile Case Sensitivity
**File**: `core/phase3_enrich/pcs_score_entry.py`
**Issue**: Profile uses "Income" instead of "INCOME"

```python
# Current: 'Income', 'Directional_Bull', 'Neutral_Vol'
# Expected: 'INCOME', 'DIRECTIONAL', 'NEUTRAL_VOL'

PROFILE_MAP = {
    'Income': 'INCOME',
    'Directional_Bull': 'DIRECTIONAL',
    'Directional_Bear': 'DIRECTIONAL',
    'Neutral_Vol': 'NEUTRAL_VOL',
}
```

**Impact**: Fixes 0% PCS Weights scores across all personas

---

#### 1.3 Add Missing Metrics
**File**: `core/phase3_enrich/compute_pnl_metrics.py`

```python
# Add ROI column
df['ROI'] = np.where(
    df['Basis'] > 0,
    df['Unrealized_PnL'] / df['Basis'],
    np.nan
)

# Add Theta_Efficiency
df['Theta_Efficiency'] = np.where(
    np.abs(df['Premium']) > 0.01,
    np.abs(df['Theta']) / np.abs(df['Premium']),
    np.nan
)
```

**File**: `core/phase3_enrich/compute_assignment_risk.py`

```python
# Rename for consistency
df['Assignment_Risk'] = df['Assignment_Risk_Level']
```

**Impact**: Fixes 50% Data Completeness scores

---

### Phase 2: IMPORTANT Enhancements (This Week)

#### 2.1 Populate IV_Rank Historical Data
**Priority**: 🔴 CRITICAL for NEUTRAL_VOL

**Approach**:
1. Integrate with existing `compute_iv_rank_252d.py`
2. Populate DuckDB with historical IV data
3. Re-run enrichment pipeline

**Command**:
```bash
python scripts/populate_iv_history.py --symbols AAPL,MSFT,GOOGL --days 252
```

**Impact**: Unlocks NEUTRAL_VOL persona scoring (currently 0%)

---

#### 2.2 Enhance Exit Triggers with Persona-Specific Logic
**File**: `core/phase7_recommendations/exit_recommendations.py`

**Add Trigger Keywords**:
```python
def _evaluate_position_persona_aware(row, persona):
    """Generate persona-specific rationale keywords."""
    
    if persona == 'INCOME':
        # Check profit target
        if roi >= 0.50:
            rationale += "Profit target hit (50% ROI target for INCOME)"
        
        # Check assignment risk
        if assignment_risk == 'HIGH':
            rationale += "Assignment risk elevated - ITM exposure"
        
        # Check theta exhaustion
        if theta_efficiency < 0.005:
            rationale += "Theta exhaustion - decay rate below threshold"
    
    elif persona == 'NEUTRAL_VOL':
        # Check IV collapse
        if iv_rank_drift < -30:
            rationale += "IV collapse detected (IV_Rank drift < -30)"
        
        # Check vega decay
        if vega_drift_pct < -50:
            rationale += "Vega decay - volatility premium eroding"
    
    elif persona == 'DIRECTIONAL':
        # Check profit target (100% for directional)
        if roi >= 1.00:
            rationale += "Profit target hit (100% ROI target for DIRECTIONAL)"
        
        # Check chart breakdown
        if chart_regime in ['Bearish', 'Breakdown']:
            rationale += "Chart breakdown - directional thesis invalidated"
        
        # Check gamma decay
        if gamma_drift_pct < -75:
            rationale += "Severe gamma decay (>75%) - position exhausted"
```

**Impact**: Fixes 0% Exit Triggers scores

---

#### 2.3 Add Persona-Specific Alerts
**New File**: `core/monitoring/persona_alerts.py`

```python
def generate_persona_alerts(df, persona):
    """Generate alerts tailored to persona priorities."""
    
    alerts = []
    
    if persona == 'INCOME':
        # DTE range check
        out_of_range = df[(df['DTE'] < 30) | (df['DTE'] > 60)]
        if len(out_of_range) > 0:
            alerts.append({
                'severity': 'MEDIUM',
                'message': f'{len(out_of_range)} positions outside optimal DTE (30-60)',
                'positions': out_of_range['TradeID'].tolist()
            })
        
        # Low theta efficiency
        low_theta = df[df['Theta_Efficiency'] < 0.01]
        if len(low_theta) > 0:
            alerts.append({
                'severity': 'HIGH',
                'message': f'{len(low_theta)} positions with low theta efficiency',
                'positions': low_theta['TradeID'].tolist()
            })
    
    elif persona == 'NEUTRAL_VOL':
        # IV_Rank entry check
        low_iv_entry = df[df['Entry_IV_Rank'] < 50]
        if len(low_iv_entry) > 0:
            alerts.append({
                'severity': 'MEDIUM',
                'message': f'{len(low_iv_entry)} positions entered below IV_Rank 50',
                'positions': low_iv_entry['TradeID'].tolist()
            })
        
        # IV collapse check
        iv_collapsed = df[df['IV_Rank_Drift'] < -30]
        if len(iv_collapsed) > 0:
            alerts.append({
                'severity': 'CRITICAL',
                'message': f'{len(iv_collapsed)} positions with IV collapse',
                'positions': iv_collapsed['TradeID'].tolist()
            })
    
    elif persona == 'DIRECTIONAL':
        # Weak delta check
        weak_delta = df[np.abs(df['Delta']) < 0.30]
        if len(weak_delta) > 0:
            alerts.append({
                'severity': 'MEDIUM',
                'message': f'{len(weak_delta)} positions with weak delta exposure',
                'positions': weak_delta['TradeID'].tolist()
            })
    
    return alerts
```

---

### Phase 3: OPTIMIZATION (Next Sprint)

#### 3.1 Improve Phase 2 Strategy Detection
**Goal**: Reduce "Unknown" strategies from 22 to <5

**Approach**:
- Add regex patterns for complex structures
- Implement ML-based strategy classifier
- Add user override mechanism

#### 3.2 Create Persona-Specific Dashboards
**Goal**: Separate views for each persona with tailored metrics

**Features**:
- INCOME: Theta decay tracker, ROI leaderboard, assignment risk grid
- NEUTRAL_VOL: IV_Rank heatmap, vega exposure tracker, volatility events
- DIRECTIONAL: Delta exposure chart, gamma acceleration tracker, chart regime overlay

#### 3.3 Backtest Persona Strategies
**Goal**: Validate persona-specific PCS weights and exit triggers

**Approach**:
- Use ML training data (completed trades)
- Run simulations with different PCS weight profiles
- Optimize exit trigger thresholds per persona

---

## Implementation Priority

### Week 1 (Critical Path)
1. ✅ Fix strategy name standardization → +50 points per persona
2. ✅ Fix Entry_PCS_Profile case sensitivity → +30 points per persona
3. ✅ Add ROI, Theta_Efficiency, Assignment_Risk → +20 points per persona

**Expected Improvement**: 36/100 → 86/100 (INCOME), 36/100 → 86/100 (NEUTRAL_VOL)

### Week 2 (Data Population)
4. 🔴 Populate IV_Rank historical data → +40 points NEUTRAL_VOL
5. 🟡 Enhance exit triggers with persona keywords → +30 points all personas

**Expected Improvement**: NEUTRAL_VOL: 86/100 → 100/100

### Week 3 (Monitoring)
6. 🟡 Implement persona-specific alerts
7. 🟢 Create persona dashboards

---

## Success Metrics

### Target Scores (Post-Implementation)
| Persona | Current | Target | Delta |
|---------|---------|--------|-------|
| INCOME | 36.1 | 95+ | +59 |
| NEUTRAL_VOL | 36.1 | 95+ | +59 |
| DIRECTIONAL | 44.4 | 95+ | +51 |

### Key Performance Indicators
- **Strategy Alignment**: 0% → 90%+
- **Data Completeness**: 50-100% → 100%
- **Exit Triggers**: 0% → 90%+
- **PCS Weights**: 0% → 100%
- **Current_PCS v2**: 67% → 100%

---

## Conclusion

The audit revealed **systematic naming and data availability issues** rather than architectural problems. The three-cycle system is structurally sound, but needs:

1. **Naming consistency**: Strategy and profile names must match RAG expectations
2. **Data population**: IV_Rank historical data is critical
3. **Metric computation**: Add missing derived metrics (ROI, Theta_Efficiency)
4. **Trigger enhancement**: Add persona-aware rationale keywords

**Estimated effort**: 2-3 days for critical fixes, 1 week for full implementation.

**Next Command**:
```bash
# Re-run audit after fixes
python audit_persona_compliance.py --all --options-only
```
