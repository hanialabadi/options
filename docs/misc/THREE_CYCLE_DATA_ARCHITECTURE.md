# THREE-CYCLE DATA ARCHITECTURE
**Date:** January 4, 2026  
**Purpose:** Map complete data flow across Perception → Freeze → Recommendation → ML cycles

---

## 🎯 EXECUTIVE SUMMARY

**System Architecture: Three Interconnected Cycles**

```
┌─────────────────────────────────────────────────────────────────┐
│  CYCLE 1: PERCEPTION LOOP (Phase 1-4)                          │
│  Multiple snapshots/day - "What IS the position right now?"    │
│  NO chart data - Pure observables only                         │
└────────────┬────────────────────────────────────────────────────┘
             │
             │ Raw snapshots (Greeks, IV, Premium, Moneyness)
             ▼
┌─────────────────────────────────────────────────────────────────┐
│  CYCLE 2: FREEZE/TIME-SERIES (Phase 5-6)                       │
│  First-seen detection - "What WERE the entry conditions?"      │
│  Creates baseline for drift analysis                           │
└────────────┬────────────────────────────────────────────────────┘
             │
             │ Entry baseline + Time-series snapshots
             ▼
┌─────────────────────────────────────────────────────────────────┐
│  CYCLE 3: RECOMMENDATION/ML (Phase 7+)                          │
│  Exit logic + ML training - "What should we DO?"               │
│  Chart data ALLOWED here (not in Cycles 1-2)                   │
└────────────┬────────────────────────────────────────────────────┘
             │
             │ Outcomes + Performance data
             ▼
┌─────────────────────────────────────────────────────────────────┐
│  ML FEEDBACK LOOP                                               │
│  Training: Entry conditions → Outcomes                          │
│  Learning: What worked? What didn't? Why?                       │
└────────────┬────────────────────────────────────────────────────┘
             │
             │ Enhanced scoring, refined thresholds
             └──────────────────────────────────────────► CYCLE 1
```

**Key Insight:** Chart data is **NOT needed until Cycle 3** because:
1. Perception loop (Cycle 1) observes position structure - not market opinion
2. Freeze (Cycle 2) captures entry baseline - not market timing
3. Recommendations (Cycle 3) decide exits - THIS is where chart context matters

**Why This Design Works:**
- Cycles 1-2 are **replay-safe** (deterministic, audit-grade)
- Cycle 3 is **decision-grade** (uses Cycles 1-2 data + market context)
- ML learns from **complete data**: Entry structure + Market context + Outcome

---

## 📊 CYCLE 1: PERCEPTION LOOP (Phase 1-4)

### Purpose
**"What IS the position right now?"**

Capture the **structural reality** of each position at snapshot time. Multiple snapshots per day possible.

### Frequency
- **Intraday**: Every 15-60 minutes during market hours
- **End-of-Day**: Final snapshot at market close
- **On-Demand**: Manual snapshot trigger

### Data Sources

#### Input (Phase 1-2: Raw Data)
```
FROM: Schwab API
├── Position data: Symbol, Quantity, Basis, Premium
├── Greeks: Delta, Gamma, Vega, Theta, Rho
├── Market data: Bid, Ask, Last, Volume, Open Interest
├── IV surface: IV_30D (call/put), IV term structure
└── Contract specs: Strike, Expiration, OptionType
```

#### Processing (Phase 3: Enrichment)
```
COMPUTE (Deterministic):
├── Moneyness: Moneyness_Pct, ITM/ATM/OTM classification
├── Greeks validation: Gamma >= 0, Vega >= 0
├── Capital deployed: Basis * 100 (for options)
├── Breakeven: Strike ± Premium (strategy-aware)
├── ROI: Premium / Basis (strategy-aware sign)
├── DTE: Days to expiration
├── Liquidity flags: OI, Volume, Spread quality
└── Current_PCS (v1): Gamma + Vega + ROI scores
```

#### Output (Phase 4: Snapshot Storage)
```
STORE to DuckDB:
├── Raw position data (47 columns)
├── Enriched metrics (35 columns)
├── Current_PCS + subscores (6 columns)
├── Data quality flags (5 columns)
└── Snapshot metadata (Snapshot_TS, Run_ID)

Total: ~100 columns per snapshot
```

### What's EXCLUDED (Intentionally)

❌ **Chart Signals** (Regime, Signal_Type, EMA crossovers)
- **Why:** External market opinion, not position structure
- **Phase:** Allowed in Phase 7+ (recommendation engine)

❌ **Predictive Models** (ML predictions, probability forecasts)
- **Why:** Not observable facts, introduces bias
- **Phase:** Used in Phase 8+ (decision support)

❌ **Strategy Recommendations** (Buy/Sell/Hold decisions)
- **Why:** This is perception, not prescription
- **Phase:** Computed in Phase 9+ (execution engine)

❌ **Comparative Rankings** ("Best 10 positions")
- **Why:** Snapshot is per-position, not portfolio-wide
- **Phase:** Analyzed in Phase 10+ (portfolio optimization)

### Data Quality Properties

✅ **Deterministic:** Same inputs → Same outputs (always)
✅ **Replay-Safe:** Can reconstruct any historical snapshot exactly
✅ **Audit-Grade:** Every value traceable to source API field
✅ **Multi-Snapshot:** Can run 10x/day without contamination
✅ **Phase-Pure:** No Phase 7+ data leakage (chart, ML, decisions)

### Why Multiple Snapshots Per Day?

**Intraday Greek Drift Detection:**
```
09:30 snapshot: Delta=0.65, Gamma=0.08, Vega=2.5
12:00 snapshot: Delta=0.58, Gamma=0.07, Vega=2.3  ← Greeks decaying
15:30 snapshot: Delta=0.52, Gamma=0.06, Vega=2.1  ← Continued decay

Analysis: Position losing sensitivity faster than expected
Decision (Cycle 3): Consider early exit or roll
```

**Intraday IV Spike Detection:**
```
10:00 snapshot: IV=32%, IV_Rank=45
13:00 snapshot: IV=41%, IV_Rank=68  ← IV jump (news event?)
15:00 snapshot: IV=38%, IV_Rank=61  ← IV settling

Analysis: Temporary volatility expansion
Decision (Cycle 3): Sell into elevated IV (if income strategy)
```

**Real-Time P&L Tracking:**
```
Market open:  Unrealized_PnL = -$1,200
Midday:       Unrealized_PnL = -$800   ← Position recovering
Market close: Unrealized_PnL = -$400   ← Daily recovery trend

Analysis: Position performing better intraday than overnight
Decision (Cycle 3): Monitor overnight risk, consider longer hold
```

### ML Training Inputs from Cycle 1

```python
# Features for ML (extracted from Cycle 1 snapshots)
entry_features = [
    'Gamma_Entry', 'Vega_Entry', 'Delta_Entry', 'Theta_Entry',
    'Entry_PCS', 'Entry_PCS_GammaScore', 'Entry_PCS_VegaScore',
    'Entry_IV_Rank', 'Entry_Moneyness_Pct', 'Entry_DTE',
    'Premium_Entry', 'Basis', 'ROI_Entry',
    'Strategy', 'Symbol', 'Sector'
]

# Time-series features (from multiple snapshots)
drift_features = [
    'Days_In_Trade',
    'Delta_Drift', 'Gamma_Drift', 'Vega_Drift',  # Current - Entry
    'IV_Rank_Drift',  # Current_IV_Rank - Entry_IV_Rank
    'Moneyness_Migration',  # How far from entry moneyness
    'PCS_Drift',  # Current_PCS - Entry_PCS
    'Unrealized_PnL', 'ROI_Current'
]

# Target variable (from Cycle 3 - outcome)
target = 'Exit_Outcome'  # Win/Loss/Breakeven + Exit_PnL
```

**What Cycle 1 Provides to ML:**
- ✅ Entry conditions (frozen baseline)
- ✅ Position evolution (time-series drift)
- ✅ Structural quality (Entry_PCS, Current_PCS)
- ❌ Market context (chart signals) ← Comes from Cycle 3

---

## 🔒 CYCLE 2: FREEZE/TIME-SERIES (Phase 5-6)

### Purpose
**"What WERE the entry conditions when this position was opened?"**

Create **immutable baseline** for drift analysis and performance attribution.

### Frequency
- **First Snapshot Only**: When TradeID first appears in system
- **Idempotent**: Subsequent snapshots do NOT overwrite frozen values

### Data Sources

#### Input (from Cycle 1)
```
FROM: Phase 1-4 snapshots
├── First-seen detection: New TradeIDs not in first_seen table
├── Entry Greeks: Gamma, Vega, Delta, Theta, Rho (at first snapshot)
├── Entry IV: IV Mid, IV_Rank (at first snapshot)
├── Entry Context: Moneyness_Pct, DTE (at first snapshot)
└── Entry Premium: Premium, Basis (at first snapshot)
```

#### Processing (Phase 6: Freeze)
```
FREEZE (One-Time Only):
├── Entry Greeks → Gamma_Entry, Vega_Entry, etc.
├── Entry IV → IV_Entry, Entry_IV_Rank
├── Entry Context → Entry_Moneyness_Pct, Entry_DTE
├── Entry Premium → Premium_Entry
├── Entry_PCS → Calculate using Entry Greeks (frozen baseline)
├── Entry_Timestamp → First_Seen_Date
└── First_Seen_Date → Registered in first_seen table
```

#### Output (Phase 6: Historical Record)
```
ADD to snapshots (append-only):
├── 11 Entry Greek columns (_Entry suffix)
├── 6 Entry_PCS columns (frozen baseline)
├── Entry_Timestamp, First_Seen_Date
└── Total: +17 columns (147 total)

STORE in first_seen table:
├── TradeID → First_Seen_Date mapping
└── Used for idempotent freeze detection
```

### Time-Series Drift Analysis

Once frozen, every subsequent snapshot can compute drift:

```python
# Drift metrics (computed in every snapshot AFTER freeze)
Delta_Drift = Delta - Delta_Entry
Gamma_Drift = Gamma - Gamma_Entry
Vega_Drift = Vega - Vega_Entry

IV_Rank_Drift = IV_Rank - Entry_IV_Rank
Moneyness_Migration = Moneyness_Pct - Entry_Moneyness_Pct

PCS_Drift = Current_PCS - Entry_PCS

# Performance attribution
PnL_From_Delta = Delta_Drift * (Underlying_Price - Entry_Price)
PnL_From_Theta = Theta * Days_In_Trade
PnL_From_Vega = Vega_Drift * (IV - IV_Entry)
```

### Why Freeze is Critical for ML

**Without Freeze (Bad):**
```
ML sees: "Position with Gamma=0.03, ended with loss"
Problem: Was Gamma ALWAYS 0.03? Or did it decay from 0.12?
Result: ML learns "low gamma = loss" (wrong correlation)
```

**With Freeze (Good):**
```
ML sees: "Position with Gamma_Entry=0.12, Gamma_Exit=0.03 (decay), ended with loss"
Problem: Did decay happen too fast? Or was entry gamma too high?
Result: ML learns "rapid gamma decay + long hold = loss" (correct correlation)
```

**Freeze enables causal analysis:**
- Entry_PCS high but ended in loss → Was entry timing wrong? (chart context)
- Entry_PCS low but ended in win → Was exit timing excellent? (chart context)
- Entry_PCS high, PCS_Drift negative → Position deteriorating (structural)
- Entry_PCS low, PCS_Drift positive → Position improving (structural)

### ML Training Inputs from Cycle 2

```python
# Baseline features (frozen)
baseline = {
    'Entry_PCS': 15.2,
    'Gamma_Entry': 0.12,
    'Vega_Entry': 2.8,
    'Entry_IV_Rank': 72,
    'Entry_Moneyness_Pct': 5.2,  # 5.2% OTM at entry
    'Entry_DTE': 42
}

# Evolution features (time-series)
evolution = {
    'Days_In_Trade': 18,
    'Gamma_Drift': -0.09,  # Gamma decayed 75%
    'Vega_Drift': -1.2,    # Vega decayed 43%
    'IV_Rank_Drift': -25,  # IV collapsed from 72 to 47
    'Moneyness_Migration': -3.1,  # Now 2.1% OTM (moved toward money)
    'PCS_Drift': -8.5      # Quality deteriorated
}

# Outcome (from Cycle 3)
outcome = {
    'Exit_Decision': 'Early_Close',  # Closed before expiration
    'Exit_PnL': -$320,
    'Exit_Reason': 'IV_Collapse',
    'Win_Loss': 'Loss'
}
```

**ML Can Learn:**
- High Entry_IV_Rank (72) + IV_Rank_Drift (-25) = Risk of IV crush
- Gamma decay (75%) + Moneyness_Migration (toward money) = Time decay not compensating for directionality
- Entry_PCS (15.2) was good, but PCS_Drift (-8.5) signaled deterioration
- **Lesson:** Exit earlier when IV_Rank drops >20 points AND Greeks decay faster than expected

### What Cycle 2 Provides to ML

✅ **Entry Baseline**: What were the initial conditions?
✅ **Drift Vectors**: How did the position evolve over time?
✅ **Performance Attribution**: Which Greeks contributed to P&L?
✅ **Quality Evolution**: Entry_PCS vs Current_PCS trajectory
❌ **Exit Context**: When/why did we exit? ← Comes from Cycle 3

---

## 🎯 CYCLE 3: RECOMMENDATION/ML (Phase 7+)

### Purpose
**"What should we DO with this position?"**

Combine **structural data (Cycles 1-2)** + **market context (chart signals)** → **actionable decisions**

### Frequency
- **Decision Engine**: End-of-day (primary) + intraday alerts (secondary)
- **ML Training**: Weekly batch (after exits complete)

### Data Sources

#### Input from Cycles 1-2
```
FROM: Perception + Freeze cycles
├── Current snapshot: Greeks, IV, Premium, Moneyness
├── Entry baseline: Entry_PCS, Entry Greeks, Entry_IV_Rank
├── Drift vectors: Delta_Drift, Gamma_Drift, IV_Rank_Drift, PCS_Drift
├── Time-series: Days_In_Trade, P&L history (multiple snapshots)
└── Quality metrics: Current_PCS, Entry_PCS
```

#### Input from External Sources (NEW - Phase 7+)
```
FROM: Chart analysis, market data
├── Chart Regime: Bullish, Bearish, Sideways, Transition
├── Signal Type: Crossover, Reversal, Continuation, Breakdown
├── EMA Signals: Days_Since_Cross, Crossover_Age_Bucket
├── Trend Strength: Slope, ATR%, Momentum
├── Support/Resistance: Distance to key levels
└── Market Context: VIX, sector rotation, correlation
```

#### Processing (Phase 7+: Decision Logic)
```
DECIDE (Action-Oriented):
├── Exit Logic:
│   ├── Profit target hit? (ROI >= target)
│   ├── Stop loss triggered? (Unrealized_PnL < -threshold)
│   ├── Greeks deteriorated? (PCS_Drift < -threshold)
│   ├── IV collapsed? (IV_Rank_Drift < -20)
│   ├── Chart breakdown? (Regime=Bearish + Signal=Breakdown)
│   └── Time decay optimal? (Theta efficiency vs Days_In_Trade)
│
├── Hold Logic:
│   ├── Position performing as designed? (PCS_Drift near 0)
│   ├── Chart aligned with thesis? (Bullish position + Bullish regime)
│   ├── IV stable? (IV_Rank_Drift < 10)
│   └── Greeks within range? (Delta/Gamma/Vega acceptable)
│
├── Roll Logic:
│   ├── Expiration approaching? (DTE < 7)
│   ├── Moneyness risky? (ITM risk for credit strategies)
│   ├── Can extend duration? (Next expiration available)
│   └── Chart supports continuation? (Trend intact)
│
└── Adjust Logic:
    ├── Add hedge? (Delta exposure too high)
    ├── Close one leg? (Spread not working as expected)
    └── Scale position? (High conviction + PCS_Drift positive)
```

#### Output (Phase 7+: Recommendations)
```
RECOMMEND:
├── Exit recommendations:
│   ├── Action: CLOSE, ROLL, HOLD, ADJUST
│   ├── Urgency: HIGH (today), MEDIUM (this week), LOW (monitor)
│   ├── Rationale: Why this action? (drift + chart + risk)
│   └── Expected outcome: Profit/loss if action taken
│
├── Portfolio adjustments:
│   ├── Rebalance: Over-concentrated in sector/strategy?
│   ├── Risk management: Portfolio Greeks within limits?
│   └── Capital allocation: Redeploy capital from exits?
│
└── Learning feedback:
    ├── Decision made: What action was recommended?
    ├── Action taken: What did user actually do?
    ├── Outcome observed: Win/loss/breakeven result
    └── ML training data: Entry + Drift + Chart + Decision + Outcome
```

### Why Chart Data Belongs in Cycle 3 (Not Cycle 1)

**Chart signals are PRESCRIPTIVE (opinions), not DESCRIPTIVE (facts):**

| **Data Type** | **Example** | **Cycle** | **Reason** |
|--------------|-------------|-----------|------------|
| ✅ **Observable Fact** | "Delta = 0.65" | Cycle 1 | Provable from API, deterministic |
| ✅ **Observable Fact** | "Premium = $2.85" | Cycle 1 | Market price, objective |
| ✅ **Computed Fact** | "Moneyness = 5.2% OTM" | Cycle 1 | Derived from Strike vs Spot, deterministic |
| ✅ **Frozen Baseline** | "Entry_PCS = 15.2" | Cycle 2 | Historical record, immutable |
| ❌ **Market Opinion** | "Regime = Bullish" | Cycle 3 | Interpretation of price action (subjective) |
| ❌ **Market Opinion** | "Signal = Crossover" | Cycle 3 | Derived from EMA rules (methodology-dependent) |
| ❌ **Market Opinion** | "Trend = Strong" | Cycle 3 | Slope + ATR interpretation (subjective) |

**If chart data leaked into Cycle 1:**
- ❌ Snapshots become **non-deterministic** (different chart methodologies → different outputs)
- ❌ Cannot replay history exactly (chart signals depend on future data in some methods)
- ❌ Contaminated ML training (model learns chart signal correlations, not structural patterns)
- ❌ Phase boundary violation (perception loop influenced by prescriptive logic)

**By keeping chart data in Cycle 3:**
- ✅ Cycles 1-2 remain **audit-grade** (provable, traceable, deterministic)
- ✅ Cycle 3 can **experiment** with different chart methods without breaking history
- ✅ ML can **separate** structural patterns (Cycles 1-2) from timing patterns (Cycle 3)
- ✅ Can compare: "Same entry structure, different chart timing → which performed better?"

### ML Training Inputs from Cycle 3

```python
# Complete training example
training_record = {
    # From Cycle 1 (entry snapshot)
    'entry': {
        'Entry_PCS': 15.2,
        'Gamma_Entry': 0.12,
        'Entry_IV_Rank': 72,
        'Entry_Moneyness_Pct': 5.2,
        'Strategy': 'CSP',
        'Symbol': 'AAPL',
        'DTE_Entry': 42
    },
    
    # From Cycle 2 (time-series evolution)
    'evolution': {
        'Days_In_Trade': 18,
        'Gamma_Drift': -0.09,
        'IV_Rank_Drift': -25,
        'PCS_Drift': -8.5,
        'Moneyness_Migration': -3.1,
        'Unrealized_PnL_Max': +$180,  # Best P&L seen
        'Unrealized_PnL_Exit': -$320  # P&L at exit
    },
    
    # From Cycle 3 (chart context + decision)
    'context': {
        'Entry_Regime': 'Bullish',           # Chart at entry
        'Entry_Signal': 'EMA_Crossover',
        'Exit_Regime': 'Transition',         # Chart at exit
        'Exit_Signal': 'Breakdown_Alert',
        'Days_Since_Cross_Entry': 3,         # Fresh signal at entry
        'Days_Since_Cross_Exit': 21,         # Aging signal at exit
        'Recommendation': 'Early_Close',     # What system recommended
        'Action_Taken': 'Early_Close',       # What user did
        'Exit_Reason': 'IV_Collapse + Chart_Breakdown'
    },
    
    # Outcome (label for supervised learning)
    'outcome': {
        'Exit_PnL': -$320,
        'Win_Loss': 'Loss',
        'Exit_Type': 'Early_Close',
        'Held_Pct_of_DTE': 43,  # Held 18 of 42 days
        'Max_Favorable_Excursion': +$180,
        'Max_Adverse_Excursion': -$450
    }
}
```

**ML Can Learn (Multi-Factor Analysis):**

1. **Entry Quality Validation:**
   - Entry_PCS=15.2 + Entry_IV_Rank=72 → Entry was structurally good
   - Entry_Regime=Bullish + Days_Since_Cross=3 → Entry timing was good
   - **Lesson:** Entry decisions were correct

2. **Evolution Pattern Recognition:**
   - IV_Rank_Drift=-25 (collapsed) + Gamma_Drift=-0.09 (rapid decay)
   - PCS_Drift=-8.5 (quality deteriorated)
   - Max_Favorable_Excursion=+$180 → Position WAS profitable at day 8
   - **Lesson:** Exit too late (should have closed at +$180)

3. **Chart Context Importance:**
   - Entry_Regime=Bullish, Exit_Regime=Transition (shift)
   - Exit_Signal=Breakdown_Alert (trend reversing)
   - **Lesson:** Chart breakdown + IV collapse = strong exit signal

4. **Optimal Exit Timing:**
   - Model learns: High Entry_IV_Rank (>70) + IV_Rank_Drift (<-20) + Chart_Breakdown → Exit within 5 days
   - Model learns: If Max_Favorable_Excursion > +$150 and PCS_Drift < -5 → Consider profit-taking
   - **Lesson:** Don't let winners turn into losers when chart + structure both deteriorate

### What Cycle 3 Provides to ML

✅ **Market Context**: Chart regime, signal timing, trend strength
✅ **Decision Logic**: What did system recommend? What did user do?
✅ **Outcome Labels**: Win/Loss, Exit P&L, Exit reason
✅ **Complete Causality**: Entry structure + Evolution + Market timing + Decision + Outcome

---

## 🔄 ML FEEDBACK LOOP

### Purpose
**"What can we learn from historical trades to improve future decisions?"**

Train models on **complete data** (Cycles 1-3) to enhance scoring, timing, and recommendations.

### Training Data Pipeline

```
┌─────────────────────────────────────────────────────────────┐
│  1. COLLECT COMPLETED TRADES                                 │
│     - Entry conditions (Cycle 1 first snapshot)              │
│     - Evolution metrics (Cycle 2 time-series)                │
│     - Chart context (Cycle 3 at entry + exit)                │
│     - Decision + Outcome (Cycle 3 final)                     │
└────────────┬────────────────────────────────────────────────┘
             │
             │ Aggregate historical trades
             ▼
┌─────────────────────────────────────────────────────────────┐
│  2. FEATURE ENGINEERING                                      │
│     Entry features:                                          │
│       - Entry_PCS, Entry Greeks, Entry_IV_Rank               │
│       - Entry chart regime, signal freshness                 │
│     Evolution features:                                      │
│       - Drift vectors (Delta, Gamma, Vega, IV_Rank, PCS)     │
│       - Time-series metrics (Days_In_Trade, P&L path)        │
│     Context features:                                        │
│       - Chart regime changes (entry → exit)                  │
│       - Market volatility (VIX at entry/exit)                │
│     Outcome:                                                 │
│       - Win/Loss, Exit P&L, Exit type (early/expiration)     │
└────────────┬────────────────────────────────────────────────┘
             │
             │ Features ready for training
             ▼
┌─────────────────────────────────────────────────────────────┐
│  3. MODEL TRAINING                                           │
│     A. Entry Quality Predictor                               │
│        Input: Entry structure + Chart context at entry       │
│        Output: Probability of profitable exit                │
│        Use: Enhance Entry_PCS calculation                    │
│                                                              │
│     B. Exit Timing Predictor                                 │
│        Input: Entry + Drift + Current chart context          │
│        Output: Optimal exit timing (days, profit target)     │
│        Use: Early exit recommendations                       │
│                                                              │
│     C. Risk Classifier                                       │
│        Input: Entry + Drift + Greeks deterioration           │
│        Output: Risk level (Low/Medium/High/Critical)         │
│        Use: Stop-loss triggers, position sizing              │
│                                                              │
│     D. Strategy Selector                                     │
│        Input: Chart regime + IV_Rank + Sector                │
│        Output: Best strategy for current conditions          │
│        Use: New position recommendations                     │
└────────────┬────────────────────────────────────────────────┘
             │
             │ Trained models
             ▼
┌─────────────────────────────────────────────────────────────┐
│  4. MODEL DEPLOYMENT                                         │
│     Cycle 1 Enhancement:                                     │
│       - ML-adjusted Entry_PCS (not replacing, augmenting)    │
│       - Confidence scores: "This looks like winning trades"  │
│                                                              │
│     Cycle 3 Enhancement:                                     │
│       - Exit timing: "Optimal exit window: 3-5 days"         │
│       - Risk alerts: "Greeks deteriorating faster than avg"  │
│       - Strategy suggestions: "Market favors CSPs now"       │
└────────────┬────────────────────────────────────────────────┘
             │
             │ Enhanced decisions
             ▼
┌─────────────────────────────────────────────────────────────┐
│  5. PERFORMANCE MONITORING                                   │
│     Track:                                                   │
│       - Model prediction accuracy vs actual outcomes         │
│       - Recommendation acceptance rate (did user follow?)    │
│       - Outcome improvement (vs baseline strategy)           │
│     Retrain:                                                 │
│       - Weekly: Incremental updates with new trades          │
│       - Monthly: Full retraining with expanded dataset       │
└────────────┬────────────────────────────────────────────────┘
             │
             │ Improved models
             └──────────────────────────────────────► CYCLE 1-3
```

### ML Model Types & Use Cases

#### Model 1: Entry Quality Predictor
```python
# Predict probability of profitable exit at entry time
model_entry = RandomForestClassifier()

features = [
    'Entry_PCS', 'Gamma_Entry', 'Vega_Entry', 'Entry_IV_Rank',
    'Entry_Moneyness_Pct', 'DTE_Entry', 'Strategy',
    'Entry_Chart_Regime', 'Days_Since_Cross_Entry',
    'VIX_at_Entry', 'Sector'
]

target = 'Win_Loss'  # Binary: Win or Loss

# Train on 1000 historical trades
model_entry.fit(X=historical_trades[features], y=historical_trades[target])

# At new position entry:
entry_data = {...}
win_probability = model_entry.predict_proba(entry_data)[1]

# Use to adjust Entry_PCS:
if win_probability > 0.70:
    Entry_PCS_ML = Entry_PCS * 1.1  # Boost score by 10%
elif win_probability < 0.40:
    Entry_PCS_ML = Entry_PCS * 0.9  # Penalize score by 10%
else:
    Entry_PCS_ML = Entry_PCS  # No adjustment
```

**Use Case:** Filter out positions that "look good structurally" but historically underperformed.

---

#### Model 2: Exit Timing Predictor
```python
# Predict optimal exit day given current conditions
model_exit_timing = GradientBoostingRegressor()

features = [
    'Entry_PCS', 'Entry_IV_Rank', 'DTE_Entry',
    'Days_In_Trade', 'Gamma_Drift', 'IV_Rank_Drift', 'PCS_Drift',
    'Current_Chart_Regime', 'Unrealized_PnL', 'ROI_Current',
    'Max_Favorable_Excursion'  # Best P&L seen so far
]

target = 'Optimal_Exit_Day'  # Regression: Day number that maximized profit

# Train
model_exit_timing.fit(X=historical_trades[features], y=historical_trades[target])

# During position monitoring:
current_data = {...}
predicted_optimal_day = model_exit_timing.predict(current_data)

if Days_In_Trade >= predicted_optimal_day - 2:
    recommendation = "Consider closing within 2 days (optimal exit window approaching)"
```

**Use Case:** Avoid holding too long and letting winners turn into losers.

---

#### Model 3: Risk Classifier
```python
# Classify position risk level based on deterioration patterns
model_risk = LogisticRegression(multi_class='multinomial')

features = [
    'PCS_Drift', 'Gamma_Drift', 'IV_Rank_Drift',
    'Moneyness_Migration', 'Days_In_Trade',
    'Chart_Regime_Change',  # Did regime shift since entry?
    'Theta_Decay_Efficiency'  # Actual vs expected decay
]

target = 'Risk_Level'  # Multi-class: Low, Medium, High, Critical

# Train
model_risk.fit(X=historical_trades[features], y=historical_trades[target])

# During monitoring:
current_data = {...}
risk_level = model_risk.predict(current_data)

if risk_level == 'High' or risk_level == 'Critical':
    recommendation = "🚨 HIGH RISK: Greeks deteriorated + Chart breakdown. Consider stop-loss."
```

**Use Case:** Early warning system for positions going wrong.

---

#### Model 4: Strategy Selector
```python
# Recommend best strategy given current market conditions
model_strategy = XGBoostClassifier()

features = [
    'Chart_Regime', 'IV_Rank', 'VIX', 'Sector',
    'Trend_Strength', 'Days_Since_Cross',
    'IVHV_Gap', 'Market_Correlation'
]

target = 'Best_Strategy'  # Multi-class: CSP, Covered Call, Bull Put Spread, etc.

# Train on historical trades grouped by market conditions
model_strategy.fit(X=market_conditions[features], y=winning_strategies[target])

# For new position entry:
current_market = {...}
recommended_strategy = model_strategy.predict(current_market)
confidence = model_strategy.predict_proba(current_market).max()

if confidence > 0.75:
    recommendation = f"High confidence: {recommended_strategy} performs well in these conditions"
```

**Use Case:** Guide strategy selection for new positions based on what's working now.

---

### Learning Feedback Example

**Trade History:**
```
Trade A: CSP on AAPL
  Entry: Entry_PCS=18.2, Entry_IV_Rank=78, Entry_Chart=Bullish
  Evolution: Days_In_Trade=12, IV_Rank_Drift=-30, PCS_Drift=-12
  Exit: Early close on day 12, PnL=-$280 (loss)
  Chart at Exit: Transition (regime shifted)
  
Trade B: CSP on MSFT  
  Entry: Entry_PCS=17.8, Entry_IV_Rank=75, Entry_Chart=Bullish
  Evolution: Days_In_Trade=8, IV_Rank_Drift=-28, PCS_Drift=-10
  Exit: Early close on day 8, PnL=+$120 (win)
  Chart at Exit: Transition (regime shifting)
```

**ML Learning:**
- Both had high Entry_IV_Rank (75-78) → IV collapse risk
- Both saw IV_Rank_Drift of -28 to -30 (massive IV drop)
- Trade B exited earlier (day 8 vs day 12) → Avoided further loss
- **Lesson:** When Entry_IV_Rank >75 and IV_Rank_Drift <-25, exit within 8 days

**Apply to Future Trades:**
```python
# New CSP on NVDA
entry_conditions = {
    'Entry_PCS': 19.1,
    'Entry_IV_Rank': 79,  # High IV at entry
    'Entry_Chart': 'Bullish'
}

# On day 6:
current_conditions = {
    'Days_In_Trade': 6,
    'IV_Rank_Drift': -26,  # IV collapsing
    'PCS_Drift': -11,
    'Unrealized_PnL': +$85  # Currently profitable
}

# ML recommendation:
# "High IV_Rank at entry + rapid IV collapse detected. Historical pattern shows optimal exit at day 8.
#  Current P&L: +$85. Consider closing within 2 days to lock profit before further decay."
```

**Outcome:** Exit on day 7 at +$90 profit, avoiding what would have been -$150 loss by day 14.

---

## 📋 COMPLETE DATA FLOW SUMMARY

### What Each Cycle Needs

| **Cycle** | **Primary Inputs** | **Processing** | **Outputs** | **ML Role** |
|-----------|-------------------|----------------|-------------|-------------|
| **Cycle 1: Perception** | Schwab API (Greeks, IV, Premium) | Enrichment (Moneyness, ROI, Current_PCS) | Snapshots (100 cols) | Provides entry features |
| **Cycle 2: Freeze** | Cycle 1 first snapshot | Freeze entry baseline | Entry_PCS, Entry Greeks (+17 cols) | Provides drift features |
| **Cycle 3: Recommendations** | Cycles 1-2 + Chart signals | Exit logic, decision engine | Action recommendations | Provides context + outcomes |
| **ML Loop** | All completed trades (Cycles 1-3) | Model training | Enhanced scoring, timing | Improves all cycles |

### Data Flow Between Cycles

```
CYCLE 1 (Perception)
    │
    ├─► Snapshot 1 (first seen) ──────────► CYCLE 2 (Freeze Entry)
    │                                           │
    ├─► Snapshot 2 (intraday) ─────────────────┼─► Drift calculation
    │                                           │
    ├─► Snapshot 3 (intraday) ─────────────────┼─► Drift calculation
    │                                           │
    └─► Snapshot N (end-of-day) ───────────────┴─► CYCLE 3 (Recommendations)
                                                        │
                                                        ├─► Exit decision
                                                        │
                                                        └─► ML LOOP (Training)
                                                                │
                                                                ├─► Entry Quality Model
                                                                ├─► Exit Timing Model
                                                                ├─► Risk Classifier
                                                                └─► Strategy Selector
                                                                        │
                      ┌─────────────────────────────────────────────────┘
                      │
                      └──► Enhanced scoring/recommendations ──► CYCLE 1-3 (Future trades)
```

### Why This Architecture Works

✅ **Separation of Concerns:**
- Cycle 1: Observe (facts only)
- Cycle 2: Remember (baseline)
- Cycle 3: Decide (facts + context + ML)

✅ **Replay Safety:**
- Cycles 1-2 are deterministic → Can reconstruct any historical snapshot
- Cycle 3 can use different chart methods → Compare strategies without breaking history

✅ **ML Training Quality:**
- Complete causality: Structure + Evolution + Timing + Outcome
- Can separate: "Was entry good?" vs "Was exit timing good?"
- Enables counterfactual: "What if we exited 3 days earlier?"

✅ **Incremental Enhancement:**
- Start with basic rules (Cycle 3 hardcoded logic)
- Add ML models as data accumulates
- Models improve over time without changing Cycles 1-2

---

## 🎯 ANSWERING YOUR QUESTION

### "Do we need chart for this base phase?"

**Answer:** ❌ **NO - Chart data does NOT belong in the base phase (Cycles 1-2)**

**Why:**
1. **Phase Purity:** Perception loop (Cycle 1) must be deterministic, audit-grade
2. **Replay Safety:** Cannot reconstruct historical snapshots if chart signals change methodology
3. **ML Training:** Need to separate structural patterns (Cycles 1-2) from timing patterns (Cycle 3)
4. **Causality:** Must know "Was entry structure good?" independent of "Was market timing good?"

**Where Chart Belongs:** Cycle 3 (Recommendation phase) where decisions are made

---

### "What other information do we need?"

**For Cycle 1 (Perception) - ✅ COMPLETE:**
- Greeks (Delta, Gamma, Vega, Theta, Rho)
- IV data (IV Mid, IV_Rank when implemented)
- Premium, Bid, Ask, Last
- Volume, Open Interest
- Contract specs (Strike, Expiration, OptionType)

**For Cycle 2 (Freeze) - ✅ COMPLETE:**
- First-seen detection (First_Seen_Date)
- Entry Greeks frozen
- Entry_PCS calculated

**For Cycle 3 (Recommendations) - ⚠️ NEEDS CHART DATA:**
- Chart Regime (Bullish/Bearish/Sideways)
- Signal Type (Crossover/Reversal/Breakdown)
- EMA signals (Days_Since_Cross, Trend_Slope)
- Support/Resistance levels
- Market context (VIX, sector rotation)

**For ML Loop - ✅ COLLECTING:**
- Entry conditions (from Cycle 1 first snapshot)
- Evolution (from Cycle 2 drift calculations)
- Chart context (from Cycle 3 when added)
- Outcomes (from Cycle 3 exit decisions)

---

### "Three big cycles feeding each other"

**Your Architecture Understanding is Correct:**

```
PRE-FREEZE (Cycle 1): Multiple snapshots/day
    ↓
FROZEN (Cycle 2): Creates time-series baseline for drift
    ↓
REC PHASE (Cycle 3): Execute based on data + chart context
    ↓
ML LOOP: Learn from history, enhance future decisions
    ↓
(Feedback to all cycles)
```

**What's Working:**
- ✅ Cycle 1: Perception loop operational (100 columns/snapshot)
- ✅ Cycle 2: Entry freeze operational (Entry_PCS + 17 columns)
- ⚠️ Cycle 3: Chart data not yet integrated (waiting for Phase 7+)
- ⚠️ ML Loop: Data structure ready, models not yet trained

**Next Steps:**
1. Complete Cycle 3 chart integration (Phase 7+)
2. Begin collecting completed trade outcomes
3. Train initial ML models (start with simple rules)
4. Deploy ML enhancements incrementally

---

## ✅ CONCLUSION

Your three-cycle architecture is **exactly right**:
1. **Perception (Cycles 1-2)**: Pure observables, no opinions → ML training features
2. **Recommendation (Cycle 3)**: Observables + Chart context → ML training labels
3. **ML Loop**: Complete data → Enhanced decisions → Better outcomes

**Chart data belongs in Cycle 3**, not Cycles 1-2, because:
- Cycles 1-2 provide **structural causality** (what IS the position)
- Cycle 3 provides **timing causality** (when to act)
- ML learns from **both**: Structure + Timing → Outcome

**System is ready** for ML training once Cycle 3 (chart integration) completes and we have 50-100 completed trades for initial model training.
