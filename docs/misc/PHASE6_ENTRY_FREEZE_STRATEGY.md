# Phase 6 Entry Freeze Strategy - Recommendation Engine Optimization

**Date:** January 3, 2026  
**Context:** Pre-Phase 6 planning for entry value freezing  
**Purpose:** Identify all entry conditions needed for ML/recommendation engine

---

## Current State (Phase 2)

**Already Freezing:**
- `Strike_Entry` - Immutable leg definition
- `Expiration_Entry` - Immutable leg definition

**Planned for Phase 6 (from design):**
- **Leg-level Greeks**: `Delta_Leg_Entry`, `Gamma_Leg_Entry`, `Vega_Leg_Entry`, `Theta_Leg_Entry`
- **Leg-level Premium**: `Premium_Leg_Entry`
- **Leg-level IV**: `IV_Leg_Entry`
- **Trade-level Greeks**: `Delta_Entry`, `Gamma_Entry`, `Vega_Entry`, `Theta_Entry`
- **Trade-level values**: `Premium_Entry`, `Capital_Deployed_Entry`, `PCS_Entry`, `DTE_Entry`

---

## Critical Question for Rec Engine

**What entry context should we freeze to enable learning?**

The recommendation engine needs to learn:
1. **When** to enter (market conditions)
2. **Why** specific strategies succeed/fail
3. **What** adjustments work (requires comparing entry vs current)

---

## Recommended Additional Entry Freezes

### Tier 1: Essential for Rec Engine (HIGH PRIORITY)

#### 1. **Underlying_Price_Entry** ⭐⭐⭐
- **Why**: Required to calculate % moves, track rolls
- **Use Case**: "Entered AAPL at $150, now at $160 → +6.7% move"
- **Phase**: Phase 6 (from broker snapshot)
- **Source**: Broker data (current underlying price)

#### 2. **IV_Rank_Entry** ⭐⭐⭐
- **Why**: Core signal for when to enter vol strategies
- **Use Case**: "Long Strangles entered at IV Rank 80+ had 75% win rate"
- **Phase**: Phase 6 (from enrichment)
- **Source**: Phase 3 enrichment (if available)

#### 3. **DTE_Entry** ⭐⭐⭐
- **Why**: Time decay management, theta strategy validation
- **Use Case**: "45 DTE entries managed to 21 DTE averaged 15% profit"
- **Phase**: Phase 6 (calculated from Expiration_Entry)
- **Source**: `(Expiration_Entry - Entry_Date).days`

#### 4. **VIX_Entry** ⭐⭐⭐
- **Why**: Market volatility regime at entry
- **Use Case**: "Positions entered when VIX > 25 had higher profit potential"
- **Phase**: Phase 6 (external data fetch)
- **Source**: Yahoo Finance / external API

### Tier 2: Strategic Context (MEDIUM PRIORITY)

#### 5. **Days_to_Earnings_Entry** ⭐⭐
- **Why**: Earnings risk proximity
- **Use Case**: "Avoid entries within 7 days of earnings (historical 60% loss rate)"
- **Phase**: Phase 6 (from enrichment)
- **Source**: Phase 3 earnings proximity module

#### 6. **Volatility_Regime_Entry** ⭐⭐
- **Why**: Regime-specific strategy performance
- **Use Case**: "Credit spreads in 'Low Vol' regime had 80% success"
- **Phase**: Phase 6 (from enrichment)
- **Source**: Phase 3 Sinclair regime classification

#### 7. **IV_Term_Structure_Entry** ⭐⭐
- **Why**: Vol surface shape (contango/backwardation)
- **Use Case**: "Calendars in contango outperformed by 20%"
- **Phase**: Phase 6 (from enrichment)
- **Source**: Phase 3 IV surface analysis

#### 8. **IVHV_Gap_Entry** ⭐⭐
- **Why**: IV vs HV divergence (mean reversion signal)
- **Use Case**: "IV 10pts above HV → short vol strategies favorable"
- **Phase**: Phase 6 (from enrichment)
- **Source**: Phase 3 `IVHV_gap_30D`

### Tier 3: Technical Context (NICE TO HAVE)

#### 9. **Trend_State_Entry** ⭐
- **Why**: Directional bias validation
- **Use Case**: "Bullish strategies in 'Bullish' trend had 70% win rate"
- **Phase**: Phase 6 (from enrichment)
- **Source**: Phase 3 Murphy indicators

#### 10. **RSI_Entry** ⭐
- **Why**: Overbought/oversold conditions
- **Use Case**: "Short puts when RSI < 30 had higher success"
- **Phase**: Phase 6 (from enrichment)
- **Source**: Phase 3 momentum indicators

---

## Implementation Priorities for Phase 6

### Must Have (Phase 6 Initial Implementation):
```python
# Position-level entry freeze
df["Underlying_Price_Entry"] = df["Underlying_Price"]  # From broker
df["DTE_Entry"] = (df["Expiration"] - df["Entry_Date"]).dt.days
df["VIX_Entry"] = fetch_vix_at_date(df["Entry_Date"])  # External fetch
df["Entry_Date"] = pd.Timestamp.now()  # Timestamp of first freeze

# Greeks (already planned)
df["Delta_Leg_Entry"] = df["Delta_Leg"]
df["Premium_Leg_Entry"] = df["Premium"]
# ... other Greeks
```

### Should Have (Phase 6 Enhancement):
```python
# Market context from Phase 3 enrichment
if "IV_Rank_30D" in df.columns:
    df["IV_Rank_Entry"] = df["IV_Rank_30D"]

if "days_to_earnings" in df.columns:
    df["Days_to_Earnings_Entry"] = df["days_to_earnings"]

if "Volatility_Regime" in df.columns:
    df["Regime_Entry"] = df["Volatility_Regime"]

if "IVHV_gap_30D" in df.columns:
    df["IVHV_Gap_Entry"] = df["IVHV_gap_30D"]

if "IV_Term_Structure" in df.columns:
    df["IV_Term_Structure_Entry"] = df["IV_Term_Structure"]
```

### Nice to Have (Phase 6 Future):
```python
# Technical indicators
if "Trend_State" in df.columns:
    df["Trend_State_Entry"] = df["Trend_State"]

if "RSI" in df.columns:
    df["RSI_Entry"] = df["RSI"]

if "Price_vs_SMA20" in df.columns:
    df["Price_vs_SMA20_Entry"] = df["Price_vs_SMA20"]
```

---

## Entry Freeze Schema (Complete)

### Immutable Position Identity (Phase 2 ✅)
- `Strike_Entry` - Never changes
- `Expiration_Entry` - Never changes
- `LegID` - Stable identifier
- `LegRole` - Semantic function

### Immutable Entry Values (Phase 6 - Leg Level)
- `Premium_Leg_Entry` - Leg premium at entry
- `Delta_Leg_Entry` - Leg delta at entry
- `Gamma_Leg_Entry` - Leg gamma at entry
- `Vega_Leg_Entry` - Leg vega at entry
- `Theta_Leg_Entry` - Leg theta at entry
- `IV_Leg_Entry` - Leg IV at entry

### Immutable Entry Values (Phase 6 - Trade Level)
- `Premium_Entry` - Total trade premium
- `Delta_Entry` - Net trade delta
- `Gamma_Entry` - Net trade gamma
- `Vega_Entry` - Net trade vega
- `Theta_Entry` - Net trade theta
- `Capital_Deployed_Entry` - Total capital at risk
- `PCS_Entry` - Position Confidence Score at entry
- `DTE_Entry` - Days to expiration at entry

### Immutable Market Context (Phase 6 - Essential)
- `Entry_Date` - Timestamp of first freeze
- `Underlying_Price_Entry` - Stock price at entry ⭐⭐⭐
- `VIX_Entry` - Market volatility at entry ⭐⭐⭐
- `IV_Rank_Entry` - IV percentile at entry ⭐⭐⭐

### Immutable Strategic Context (Phase 6 - Optional)
- `Days_to_Earnings_Entry` - Earnings proximity ⭐⭐
- `Regime_Entry` - Market regime (Sinclair) ⭐⭐
- `IV_Term_Structure_Entry` - Vol surface shape ⭐⭐
- `IVHV_Gap_Entry` - IV vs HV divergence ⭐⭐
- `Trend_State_Entry` - Technical trend ⭐
- `RSI_Entry` - Momentum indicator ⭐

---

## Recommendation Engine Use Cases

### Learning Patterns
With these frozen entry values, the rec engine can learn:

1. **Entry Timing Patterns**:
   ```
   IF IV_Rank_Entry > 70 AND Regime_Entry == "High Vol" AND Days_to_Earnings_Entry > 7
   THEN Long_Strangle success_rate = 75%
   ```

2. **Exit Timing Patterns**:
   ```
   IF DTE_Entry == 45 AND DTE_Current == 21 AND Premium_Current / Premium_Entry < 0.5
   THEN Close position (50% profit target hit)
   ```

3. **Adjustment Triggers**:
   ```
   IF Underlying_Price_Current / Underlying_Price_Entry > 1.10 AND Delta_Entry was -0.3
   THEN Roll up strikes (underlying moved 10% against position)
   ```

4. **Risk Management**:
   ```
   IF VIX_Current / VIX_Entry > 1.5 AND Position_PnL < -Capital_Deployed_Entry * 0.2
   THEN Consider defensive adjustment (vol spike + 20% loss)
   ```

5. **Strategy Selection**:
   ```
   IF IV_Rank > 70 AND IVHV_Gap > 10 AND Trend_State == "Neutral"
   THEN Recommend: Iron Condor (high IV, neutral market)
   ```

---

## Data Availability Check

**Already Available in Pipeline:**
- ✅ Underlying_Price (broker snapshot)
- ✅ DTE (calculable from Expiration)
- ✅ Greeks (Phase 3 enrichment)
- ✅ IV_Rank_30D (Phase 3 enrichment)
- ✅ Volatility_Regime (Phase 3 enrichment)
- ✅ days_to_earnings (Phase 3 enrichment)
- ✅ IVHV_gap_30D (Phase 3 enrichment)
- ✅ IV_Term_Structure (Phase 3 enrichment)
- ✅ Trend_State (Phase 3 enrichment)
- ✅ RSI (Phase 3 enrichment)

**Needs External Fetch:**
- ⚠️ VIX_Entry (can fetch from Yahoo Finance at Entry_Date)

**Recommendation:** All Tier 1 + Tier 2 fields are achievable with current pipeline + simple VIX fetch.

---

## Phase 6 Implementation Plan

### Phase 6A: Core Entry Freeze (NOW)
1. Freeze leg-level Greeks and Premium
2. Freeze trade-level aggregates
3. Add `Entry_Date` timestamp
4. Add `Underlying_Price_Entry`
5. Calculate and freeze `DTE_Entry`

### Phase 6B: Market Context Freeze (NEXT)
1. Fetch and freeze `VIX_Entry`
2. Freeze `IV_Rank_Entry` (if Phase 3 ran)
3. Freeze `Regime_Entry` (if Phase 3 ran)
4. Freeze `Days_to_Earnings_Entry` (if Phase 3 ran)

### Phase 6C: Strategic Context Freeze (OPTIONAL)
1. Freeze `IVHV_Gap_Entry`
2. Freeze `IV_Term_Structure_Entry`
3. Freeze `Trend_State_Entry`
4. Freeze `RSI_Entry`

---

## Critical Design Principle

**"Entry values are frozen ONCE at position open and NEVER recalculated"**

This means:
- Phase 6 runs only on NEW positions (not in existing freeze records)
- Entry values persist forever in Phase 4 snapshot
- Phase 7 drift analysis compares Current vs Entry (no recalc)
- Recommendation engine trains on historical Entry conditions

---

## Next Steps

1. **Review and approve** entry freeze fields (Tier 1 must-haves)
2. **Implement Phase 6A** with core entry freeze logic
3. **Add VIX fetch** utility for `VIX_Entry`
4. **Test** entry freeze with live broker data
5. **Validate** immutability enforcement
6. **Document** entry freeze provenance

---

**Recommendation:** Implement **Tier 1 (Essential)** + **Tier 2 (Strategic)** fields in Phase 6 initial version. This gives the rec engine maximum learning signal without over-complicating the first implementation.

The trade-off is clear:
- **More entry context** = Better rec engine learning
- **Simpler implementation** = Faster to production

Suggested compromise: **Tier 1 + Tier 2 (8 additional fields)** provides 80% of value with manageable complexity.
