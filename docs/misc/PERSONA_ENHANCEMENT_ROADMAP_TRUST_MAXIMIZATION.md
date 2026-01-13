# Persona-Driven Enhancement Roadmap: Trust Maximization (8.7 → 9.5+)

**Date:** January 2, 2026  
**Framework:** Conservative Philosophy Preserved  
**Goal:** Increase trust and risk-adjusted performance without diluting edge

---

## Conservative Income Trader: Top 2 Enhancements

### Enhancement 1: Portfolio-Level Aggregate Greek Limits (P1)

**Why It Matters to This Persona:**

Income traders sell premium systematically - they accumulate **short vega** and **short gamma** positions across multiple tickers. The current system sizes positions individually but doesn't prevent:

> **Failure Scenario:** System suggests 5 bull put spreads on tech stocks (AAPL, MSFT, GOOGL, NVDA, TSLA). Each passes acceptance individually. Trader executes all 5. Tech sector drops 5% → all 5 positions hit max loss simultaneously → portfolio drawdown 10% in one day.

**Current Gap:**
- Step 8 enforces `max_trade_risk = 2%` per strategy
- Does NOT enforce `max_portfolio_vega = -X` or `max_sector_exposure = Y%`

**What It Prevents:**

1. **Regret:** "Why did I have 5 short-vol positions on correlated names?"
2. **Drawdown:** Correlated losses exceed `max_portfolio_risk = 20%` because system didn't aggregate
3. **Blind Spot:** Can't see total short gamma until it blows up during earnings week

**Implementation (P1):**

```python
# In Step 8, before allocating capital
portfolio_greeks = {
    'total_vega': df_allocated['Vega'].sum(),
    'total_gamma': df_allocated['Gamma'].sum(),
    'total_delta': df_allocated['Delta'].sum()
}

# Conservative limits (per $100k account)
MAX_SHORT_VEGA = -0.50  # Max -$50 per 1% IV move
MAX_SHORT_GAMMA = -0.10  # Max -$10 per $1 underlying move

if portfolio_greeks['total_vega'] < MAX_SHORT_VEGA:
    logger.warning(f"⚠️ Portfolio vega limit exceeded: {portfolio_greeks['total_vega']:.2f} < {MAX_SHORT_VEGA}")
    # Block new premium-selling strategies OR reduce allocation
```

**Trust Impact:**
- Income trader knows system prevents overconcentration
- Can sleep through earnings season without checking portfolio Greeks manually
- System becomes **position manager**, not just **signal generator**

**Classification:** **P1 (2-3 weeks, high trust ROI)**

---

### Enhancement 2: Earnings Proximity Hard Gate (P1)

**Why It Matters to This Persona:**

Conservative income traders **avoid event risk**. They don't want to:
- Sell premium 3 days before earnings (IV spike risk)
- Hold short gamma through earnings (gap risk)

**Failure Scenario:**
> System suggests cash-secured put on NVDA (passes all acceptance rules). Trader executes. Next day: NVDA earnings announcement. Stock gaps down 8% → assignment at unfavorable price → "Why didn't the system warn me?"

**Current Gap:**
- System has `dividend_risk` flag (Phase 2)
- Does NOT have `earnings_proximity` filter

**What It Prevents:**

1. **Regret:** "I sold premium into an earnings event"
2. **Assignment Risk:** Short puts assigned after gap down
3. **IV Crush:** Sold premium at elevated IV, IV collapses post-earnings

**Implementation (P1):**

```python
# In Step 12, after loading IV availability
if 'earnings_date' in df_result.columns:
    earnings_proximity_mask = (
        (df_result['acceptance_status'] == 'READY_NOW') &
        (df_result['earnings_days'] < 7)  # Within 1 week
    )
    
    if earnings_proximity_mask.sum() > 0:
        logger.info(f"🚫 Earnings proximity gate: {earnings_proximity_mask.sum()} strategies within 7 days of earnings")
        df_result.loc[earnings_proximity_mask, 'acceptance_status'] = 'WAIT'
        df_result.loc[earnings_proximity_mask, 'acceptance_reason'] += ' (earnings in <7 days, await post-event clarity)'
```

**Trust Impact:**
- Conservative income traders can **delegate earnings avoidance** to system
- No need for manual calendar checks
- System aligns with Passarelli's "avoid selling options before earnings" principle

**Classification:** **P1 (1-2 days, immediate trust gain)**

---

## Volatility Trader: Top 2 Enhancements

### Enhancement 1: Volatility Skew Usage (Classification, Not Execution) (P1)

**Why It Matters to This Persona:**

Volatility traders care about **where IV is expensive**. The system currently:
- Tracks IV Index (ATM-equivalent)
- Does NOT track put skew vs call skew

**Failure Scenario:**
> System suggests straddle on SPY (IV Index 30d = 18%). Trader executes. Discovers later: put skew is 5 vol points expensive (90% strike IV = 23%), call skew flat. Trade is implicitly short puts (overpaying for downside protection).

**Current Gap:**
- Step 0 fetches skew data (`IV_7_D_Call` vs `IV_7_D_Put` at multiple strikes)
- System does NOT compute: `put_skew = IV(90% strike) - IV(ATM)`

**What It Prevents:**

1. **Blind Spot:** "Are puts expensive relative to calls?" (Natenberg Ch. 24)
2. **Strategy Mismatch:** Selling put spreads when put skew already elevated (crowded trade)
3. **Edge Erosion:** Buying straddles when skew pricing unfavorable

**Implementation (P1):**

```python
# In derived analytics (Phase 2 extension)
def compute_volatility_skew(df):
    """
    Compute put/call skew from IV term structure.
    
    Put Skew = IV(90% strike) - IV(ATM)
    Call Skew = IV(110% strike) - IV(ATM)
    """
    # Approximate: 7d tenor = ATM-equivalent
    iv_atm = df['iv_7d_call']  # Proxy for ATM
    
    # Longer tenors = OTM strikes (rough approximation)
    iv_otm_put = df['iv_30d_put']   # Downside
    iv_otm_call = df['iv_30d_call'] # Upside
    
    df['put_skew'] = iv_otm_put - iv_atm  # Positive = puts expensive
    df['call_skew'] = iv_otm_call - iv_atm  # Positive = calls expensive
    
    # Classification (not execution gate)
    df['skew_regime'] = 'NEUTRAL'
    df.loc[df['put_skew'] > 5, 'skew_regime'] = 'PUT_RICH'
    df.loc[df['call_skew'] > 5, 'skew_regime'] = 'CALL_RICH'
    
    return df
```

**Usage in Acceptance Logic:**
```python
# In Step 12 - add to acceptance_reason, NOT downgrade
if 'skew_regime' in df_result.columns:
    for idx in df_result.index:
        skew = df_result.loc[idx, 'skew_regime']
        strategy = df_result.loc[idx, 'Strategy']
        
        if 'put spread' in strategy.lower() and skew == 'PUT_RICH':
            # Inform, don't block
            df_result.at[idx, 'acceptance_reason'] += ' [⚠️ Put skew elevated - crowded trade]'
```

**Trust Impact:**
- Volatility trader sees **where edge is**: "Puts expensive? Sell put spreads."
- System provides **context**, not commands
- Aligns with Natenberg: "Volatility skew... changes in skew can significantly impact P&L"

**Classification:** **P1 (2-3 days, high edge clarity)**

---

### Enhancement 2: Term Structure Diagnostic (Not Weighting) (P2)

**Why It Matters to This Persona:**

Volatility traders care about **calendar spread opportunities**. Current system:
- Computes IV Index 7d/30d/60d
- Does NOT flag term structure inversions

**Failure Scenario:**
> Market stress event. Short-term IV spikes to 40%, long-term IV stays at 20% (term structure inversion). System doesn't flag: "Calendar spread opportunity - sell short, buy long". Trader misses edge.

**What It Prevents:**

1. **Missed Opportunities:** Term structure inversions = calendar spread edge
2. **Blind Execution:** Selling straddles during inversion (wrong strategy)
3. **Regime Ignorance:** Not knowing if vol term structure is normal vs stressed

**Implementation (P2):**

```python
# In derived analytics
def compute_term_structure_diagnostic(df):
    """
    Detect term structure regimes:
    - NORMAL: 7d < 30d < 60d (contango)
    - INVERSION: 7d > 30d (backwardation, stress)
    - FLAT: 7d ≈ 30d ≈ 60d (low vol)
    """
    iv_7d = df['iv_index_7d']
    iv_30d = df['iv_index_30d']
    iv_60d = df['iv_index_60d']
    
    # Thresholds
    INVERSION_THRESHOLD = 5  # vol points
    FLAT_THRESHOLD = 2
    
    df['term_structure'] = 'NORMAL'
    df.loc[iv_7d > iv_30d + INVERSION_THRESHOLD, 'term_structure'] = 'INVERSION'
    df.loc[abs(iv_7d - iv_30d) < FLAT_THRESHOLD, 'term_structure'] = 'FLAT'
    
    return df

# In Step 12 - diagnostic message only
if 'term_structure' in df_result.columns:
    inversion_count = (df_result['term_structure'] == 'INVERSION').sum()
    if inversion_count > 0:
        logger.info(f"📊 Term structure: {inversion_count} tickers in INVERSION (calendar spread opportunity)")
```

**Trust Impact:**
- Volatility trader sees **calendar spread flags** automatically
- System provides **opportunity identification**, not forced execution
- Aligns with Natenberg Ch. 20: "Term structure of implied volatility"

**Classification:** **P2 (3-4 days, nice-to-have)**

---

## Directional Swing Trader: Top 2 Enhancements

### Enhancement 1: Momentum × IV Divergence Flag (P1)

**Why It Matters to This Persona:**

Directional traders want **cheap entry on strong momentum**. Ideal scenario:
> Stock has STRONG_UP_DAY (momentum strong), IV contracting (options getting cheaper) → "Buy calls now, cheap entry"

**Failure Scenario:**
> System suggests long call on TSLA (passes acceptance). Trader executes. Discovers later: IV was expanding (+10% in 3 days) despite strong momentum → paid up for expensive options, theta/vega risk increased.

**Current Gap:**
- Phase 1 has `momentum_tag: STRONG_UP_DAY`
- Phase 2 has `iv_index_30d` (current level)
- Does NOT have: **IV trend** (expanding vs contracting)

**What It Prevents:**

1. **Theta Regret:** "I bought options when IV was spiking" (Passarelli: "IV expansion = premium inflation")
2. **Overpaying:** Buying directional options at IV peaks
3. **Blind Entry:** Not knowing if options are getting cheaper or more expensive

**Implementation (P1):**

```python
# In derived analytics - add IV Rate of Change
def compute_iv_momentum(df):
    """
    Compute IV Rate of Change (3-day window).
    Requires time-series IV data.
    """
    df = df.sort_values(['ticker', 'date'])
    
    df['iv_roc_3d'] = df.groupby('ticker')['iv_index_30d'].pct_change(periods=3) * 100
    
    # Classification
    df['iv_trend'] = 'STABLE'
    df.loc[df['iv_roc_3d'] > 10, 'iv_trend'] = 'EXPANDING'  # IV up >10%
    df.loc[df['iv_roc_3d'] < -10, 'iv_trend'] = 'CONTRACTING'  # IV down >10%
    
    return df

# In Step 12 - add timing flag
if 'momentum_tag' in df_result.columns and 'iv_trend' in df_result.columns:
    cheap_entry_mask = (
        (df_result['momentum_tag'] == 'STRONG_UP_DAY') &
        (df_result['iv_trend'] == 'CONTRACTING')
    )
    
    expensive_entry_mask = (
        (df_result['momentum_tag'] == 'STRONG_UP_DAY') &
        (df_result['iv_trend'] == 'EXPANDING')
    )
    
    # Add to acceptance_reason (inform, don't block)
    df_result.loc[cheap_entry_mask, 'acceptance_reason'] += ' [✅ Cheap entry: momentum + IV contracting]'
    df_result.loc[expensive_entry_mask, 'acceptance_reason'] += ' [⚠️ Expensive entry: IV expanding]'
```

**Trust Impact:**
- Directional trader sees **entry quality signal**: "Momentum strong + IV cheap = execute now"
- Avoids regret: "I bought calls at IV peak"
- Aligns with Passarelli: "Direction and implied volatility... traders using directional strategies must consider both"

**Classification:** **P1 (2-3 days, high entry quality boost)**

---

### Enhancement 2: DTE Guidance Based on IV Rank (P1)

**Why It Matters to This Persona:**

Directional traders want **optimal DTE selection**. Current problem:
> IV Rank = 15% (low vol). System suggests 30 DTE long call. Trader executes. Options cheap but theta decay accelerates → loses money even if direction correct (Hull: "Theta decay accelerates in final 30 days").

**Better approach:**
> When IV Rank low: suggest 45-60 DTE (avoid cheap but decaying options)  
> When IV Rank high: suggest 21-30 DTE (capture mean reversion faster)

**Current Gap:**
- Step 9A generates DTE recommendations (21/30/45/60 days)
- Does NOT adjust based on IV Rank

**What It Prevents:**

1. **Theta Regret:** "Options were cheap but decayed fast"
2. **Structural Mismatch:** Short DTE + low IV = theta > vega (bad for directional)
3. **Missed Edge:** Not using IV Rank to inform time horizon

**Implementation (P1):**

```python
# In Step 9A - adjust DTE recommendations
def generate_dte_recommendations(df_strategy, iv_rank_available=False, iv_rank=None):
    """
    Generate DTE recommendations based on IV Rank context.
    
    Low IV Rank (<30): Prefer longer DTE (45-60) - avoid theta trap
    High IV Rank (>70): Prefer shorter DTE (21-30) - capture reversion
    """
    if not iv_rank_available:
        # Default behavior (current)
        return [21, 30, 45, 60]
    
    if iv_rank < 30:
        # Low vol regime - go longer
        logger.info(f"   DTE guidance: IV Rank {iv_rank:.0f}% (low) → prefer 45-60 DTE (avoid theta trap)")
        return [45, 60, 30]  # Prioritize longer
    elif iv_rank > 70:
        # High vol regime - go shorter
        logger.info(f"   DTE guidance: IV Rank {iv_rank:.0f}% (high) → prefer 21-30 DTE (capture reversion)")
        return [21, 30, 45]  # Prioritize shorter
    else:
        # Normal regime - balanced
        return [30, 45, 21, 60]
```

**Trust Impact:**
- Directional trader gets **structural guidance**: "This IV regime → this DTE"
- Avoids theta regret on low-IV entries
- Aligns with Hull's theta acceleration principle

**Classification:** **P1 (1-2 days, immediate structural improvement)**

---

## Risk Manager / System Designer: Top 2 Enhancements

### Enhancement 1: Pre-Execution Scenario Stress Test Banner (P1)

**Why It Matters to This Persona:**

Risk managers want **tail risk visibility before execution**. Current gap:
> System suggests 5 strategies (all READY_NOW). Risk manager approves. Next day: market-wide IV drop 10 points → portfolio P&L -$8,000 → "Why didn't system show stress test?"

**What It Prevents:**

1. **Blind Execution:** Not knowing worst-case P&L impact
2. **Tail Risk Ignorance:** "What if IV drops 10 points overnight?"
3. **Portfolio Blow-Up:** Aggregate loss exceeds `max_portfolio_risk` under stress

**Implementation (P1):**

```python
# In Step 8, before final allocation
def compute_portfolio_stress_scenarios(df_allocated):
    """
    Stress test portfolio under adverse scenarios:
    - Scenario 1: IV drops 10 points (vega risk)
    - Scenario 2: Underlying drops 5% (delta/gamma risk)
    - Scenario 3: Time decay 7 days (theta risk)
    """
    total_vega = df_allocated['Vega'].sum()
    total_delta = df_allocated['Delta'].sum()
    total_theta = df_allocated['Theta'].sum()
    
    # Stress scenarios
    stress_results = {
        'iv_drop_10pts': total_vega * -10,  # IV drops 10 points
        'underlying_drop_5pct': total_delta * -0.05 * df_allocated['Underlying_Price'].mean(),
        'time_decay_7d': total_theta * 7
    }
    
    # Check if any scenario exceeds max_portfolio_risk
    account_balance = 100000  # From config
    max_loss_allowed = account_balance * 0.20  # 20% max portfolio risk
    
    worst_case = min(stress_results.values())
    
    if abs(worst_case) > max_loss_allowed:
        logger.warning(f"🚨 STRESS TEST WARNING:")
        logger.warning(f"   Worst-case scenario: ${worst_case:,.0f} loss")
        logger.warning(f"   Max allowed: ${-max_loss_allowed:,.0f}")
        logger.warning(f"   Scenario breakdown:")
        for scenario, loss in stress_results.items():
            logger.warning(f"     {scenario}: ${loss:,.0f}")
        
        # Recommend reduction
        reduction_factor = abs(worst_case) / max_loss_allowed
        logger.warning(f"   Recommendation: Reduce allocation by {(reduction_factor-1)*100:.0f}%")
    
    return stress_results
```

**Trust Impact:**
- Risk manager sees **worst-case P&L** before execution
- Can reject portfolio if stress test fails
- Aligns with Hull Ch. 15: "Stress testing... what happens if IV drops 10 points?"

**Classification:** **P1 (3-4 days, critical risk visibility)**

---

### Enhancement 2: Market Stress Mode (Yellow/Red Alert) (P1)

**Why It Matters to This Persona:**

Risk managers want **system-wide halt capability during market stress**. Current gap:
> VIX spikes from 15 to 40 (market panic). System continues suggesting trades (all pass acceptance). Risk manager wants: "Halt new trades, manual review required".

**What It Prevents:**

1. **Panic Execution:** Trading into market chaos
2. **Volatility Spike Risk:** Opening positions when IV at extremes
3. **No Kill Switch:** System can't auto-pause during stress

**Implementation (P1):**

```python
# In scan pipeline initialization
def detect_market_stress(df_snapshot):
    """
    Detect market-wide stress conditions.
    
    Yellow Alert: High volatility (caution)
    Red Alert: Extreme volatility (halt new trades)
    """
    # Aggregate IV across all tickers
    median_iv = df_snapshot['IV_30_D_Call'].median()
    
    # VIX proxy: if median IV > 35 = stress
    stress_level = 'GREEN'
    
    if median_iv > 30:
        stress_level = 'YELLOW'
        logger.warning(f"⚠️ MARKET STRESS: YELLOW ALERT")
        logger.warning(f"   Median IV: {median_iv:.1f}% (elevated)")
        logger.warning(f"   Recommendation: Reduce size, increase scrutiny")
    
    if median_iv > 40:
        stress_level = 'RED'
        logger.error(f"🚨 MARKET STRESS: RED ALERT")
        logger.error(f"   Median IV: {median_iv:.1f}% (extreme)")
        logger.error(f"   Recommendation: HALT new trades, manual review required")
    
    return stress_level

# In Step 12, after acceptance logic
market_stress = detect_market_stress(df_snapshot)

if market_stress == 'RED':
    logger.error(f"🚨 RED ALERT: Downgrading all READY_NOW → WAIT (market stress override)")
    
    stress_mask = df_result['acceptance_status'] == 'READY_NOW'
    df_result.loc[stress_mask, 'acceptance_status'] = 'WAIT'
    df_result.loc[stress_mask, 'acceptance_reason'] += ' (market stress: manual review required)'
```

**Trust Impact:**
- Risk manager can **delegate stress detection** to system
- System auto-pauses during market panic (VIX spike, flash crash)
- Prevents regret: "Why did I trade into that chaos?"

**Classification:** **P1 (2-3 days, critical safety feature)**

---

## Summary: Trust Maximization Roadmap

### P1 Enhancements (2-4 weeks total)

| Persona | Enhancement | Trust Impact | Failure Mode Prevented |
|---------|-------------|--------------|----------------------|
| **Income Trader** | Portfolio Greek Limits | +++++ | Correlated drawdown |
| **Income Trader** | Earnings Proximity Gate | ++++ | Event risk assignment |
| **Volatility Trader** | Skew Usage (Classification) | ++++ | Overpaying for expensive wings |
| **Directional Trader** | Momentum × IV Divergence | ++++ | Theta regret, expensive entry |
| **Directional Trader** | DTE Guidance (IV Rank) | +++ | Structural mismatch |
| **Risk Manager** | Scenario Stress Test | +++++ | Tail risk blind spot |
| **Risk Manager** | Market Stress Mode | +++++ | Panic execution |

### P2 Enhancements (4-8 weeks)

| Persona | Enhancement | Trust Impact | Failure Mode Prevented |
|---------|-------------|--------------|----------------------|
| **Volatility Trader** | Term Structure Diagnostic | +++ | Missed calendar opportunities |
| **All** | Correlation-Adjusted Sizing | ++++ | Sector concentration |
| **All** | Sharpe Ratio Tracking | ++ | Historical blind spot |

---

## Conservative Philosophy Preserved ✅

All enhancements:
- ✅ **No threshold lowering** (no IVHV gap reduction, no score threshold reduction)
- ✅ **No fallback execution** (stress mode = HALT, not "execute smaller")
- ✅ **No reduced transparency** (all enhancements ADD diagnostics)
- ✅ **No artificial trade frequency** (some enhancements REDUCE trades, e.g., earnings gate)

**Philosophy Statement:**
> "These enhancements make the system **more conservative**, not less. They add guardrails (Greek limits, stress mode), improve context (skew, momentum-IV), and prevent regret (earnings gate, DTE guidance). Trust increases because the system says NO more intelligently."

---

## Expected Rating Progression

| Milestone | Rating | Confidence |
|-----------|--------|-----------|
| **Current** | 8.7/10 | Production-ready |
| **After P1 Enhancements** | 9.3/10 | Institution-grade |
| **After P2 Enhancements** | 9.6/10 | Industry-leading |

**Key Insight:**
- 8.7 → 9.3 requires **risk controls** (Greek limits, stress mode)
- 9.3 → 9.6 requires **edge refinements** (skew, term structure)
- 9.6 → 10.0 requires **track record** (cannot be engineered, only earned)

---

**Final Verdict:**

The system is already production-ready. These enhancements move it from **"safe and honest"** to **"safe, honest, and sophisticated"**. Each enhancement answers a persona-specific question:

- Income: "Can I trust this won't blow up my portfolio?"
- Volatility: "Does this understand where edge is?"
- Directional: "Will this prevent me from overpaying?"
- Risk Manager: "Can I see tail risk before it happens?"

**All enhancements align with the core principle:** *"Honesty and conservatism are features, not bugs."*
