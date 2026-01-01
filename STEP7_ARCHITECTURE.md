# Step 7 Architecture & Implementation Rules

**Date:** 2025-12-27  
**Status:** Production Rules (Non-Negotiable)

---

## 1. STEP 7 INTENT (PRESCRIPTIVE LAYER)

### What Step 7 Does:
```
"Given the current market context, what STRUCTURE makes sense for THIS ticker?"
```

**"Primary Strategy" means:**
- ✅ Best structural fit given current market context (signal + IV regime + volatility structure)
- ✅ Most context-appropriate structure IF user were to act

**"Primary Strategy" does NOT mean:**
- ❌ Lowest risk strategy
- ❌ Highest return strategy
- ❌ GEM-quality (that's Step 9+)
- ❌ Execute immediately signal
- ❌ Safest option

### What Step 7 Does NOT Do:
- ❌ Determine execution readiness
- ❌ Validate GEM status
- ❌ Provide "execute now" signals
- ❌ Calculate final PCS scores
- ❌ Validate option chain quality
- ❌ Check Greeks/liquidity/pricing

**Mental Model:**
- **Step 7:** "What structure makes sense right now?" (Discovery)
- **Step 8:** "How big could this position be?" (Sizing)
- **Step 9+:** "Is this a GEM worth executing now?" (Validation)

---

## 2. TIER VISIBILITY & FILTERING

### UI Default Behavior:
```
✅ Show ONLY Tier 1 (Executable) strategies by default
❌ Hide Tier 2 (Broker-Blocked) behind toggle
❌ Hide Tier 3 (Logic-Blocked) behind toggle
```

### Tier Definitions:

| Tier | Status | Meaning | Action |
|------|--------|---------|--------|
| **Tier 1** | ✅ Executable | Broker-approved, logic-ready, can execute TODAY | Proceed to Step 9B |
| **Tier 2** | 📈 Broker-Blocked | Valid strategy, but account approval needed | Upgrade broker approval |
| **Tier 3** | 🔧 Logic-Blocked | System cannot execute yet (multi-expiry, complex) | Future development |

### Implementation:
```python
# Default filter state (in UI)
show_tier1 = st.checkbox("Tier 1", value=True)   # ✅ DEFAULT ON
show_tier2 = st.checkbox("Tier 2", value=False)  # ❌ DEFAULT OFF
show_tier3 = st.checkbox("Tier 3", value=False)  # ❌ DEFAULT OFF
```

---

## 3. STRATEGY FILTERING (REQUIRED)

Users must be able to filter Tier 1 strategies by:

### Filter Dimensions:
1. **Strategy Type**
   - CSP (Cash-Secured Put)
   - Covered Call
   - Buy-Write
   - Credit Spread (Put/Call)
   - Debit Spread (Put/Call)
   - LEAP Call/Put
   - Straddle/Strangle
   - Iron Condor
   - Wheel
   - PMCC (Poor Man's Covered Call)

2. **Timeframe**
   - Short (30-45 DTE)
   - Medium (60-120 DTE)
   - Long-LEAP (180-365 DTE)
   - Ultra-LEAP (450-900 DTE)

3. **Bias**
   - Bullish
   - Bearish
   - Neutral
   - Bidirectional

4. **Capital Fit**
   - Fits account size (≤ 10% of account)
   - Within risk tolerance
   - Affordable (absolute dollar constraint)

### Implementation:
```python
# UI Filter Controls
timeframe_filter = st.multiselect("Timeframe", ['Short', 'Medium', 'Long-LEAP'])
risk_filter = st.multiselect("Risk Profile", ['Conservative', 'Moderate', 'Aggressive'])
sort_by = st.selectbox("Rank By", ['Suitability_Score', 'Success_Probability'])
ticker_search = st.text_input("Search Ticker")
```

---

## 4. CONFIDENCE ≠ PCS (CRITICAL DISTINCTION)

### Step 7 "Confidence" Is:
- ✅ Contextual alignment score (signal + IV regime + structure fit)
- ✅ Used to rank strategies for the SAME ticker: "best structural fit / second-best / fallback"
- ✅ Based on: Signal strength, IV context alignment, crossover freshness, volatility structure

### Step 7 "Confidence" Is NOT:
- ❌ PCS (Portfolio Confidence Score)
- ❌ GEM signal (that's Step 9+ validation)
- ❌ Execution trigger ("execute now")
- ❌ Risk score
- ❌ Absolute quality metric (no cross-ticker comparison)
- ❌ Return estimate

### Confidence Scale (Contextual Alignment):
```
80-100: High   - Multiple aligned signals, strong IV edge, fresh crossover, clear structure fit
60-79:  Medium - Some alignment, moderate IV edge, aging signal, reasonable structure
40-59:  Low    - Weak alignment, marginal IV edge, stale signal, structural uncertainty
```

**High confidence means:** Strong contextual alignment (NOT "safe trade" or "GEM")

**Low confidence means:** Weak contextual alignment (NOT "bad trade" or "avoid")

Only Step 9+ (option chain quality + Greeks + pricing) determines GEM execution readiness.

### UI Disclosure (MANDATORY):
```
⚠️ STEP 7 SEMANTICS (READ CAREFULLY):

"Primary Strategy" = Best structural fit GIVEN current market context (signal + IV regime + volatility structure).

It does NOT mean:
- ✗ Lowest risk strategy
- ✗ Highest return strategy  
- ✗ GEM-quality (that's Step 9+)
- ✗ Execute immediately signal

"Confidence" = Contextual alignment score (signal + IV + structure fit).

It is NOT:
- ✗ PCS (Portfolio Confidence Score)
- ✗ GEM indicator
- ✗ Risk score

Actionability boundary:
- Step 7: "What structure makes sense right now?"
- Step 8: "How big could this position be?"
- Step 9+: "Is this a GEM worth executing now?"

Step 7 recommendations are NOT execution signals.
```

---

## 5. BUY-WRITE STRATEGY (SCOPE CLARIFICATION)

### Placement:
- ✅ **Step 7/7B:** Prescriptive recommendation
- ❌ **Step 3-6:** Descriptive observation only

### Conditions for Recommendation:
```python
if (bullish_signal 
    and iv_rich_context  # ShortTerm_IV_Edge + gap_30d > 0
    and account_size >= 10000  # Minimum for stock purchase
    and stock_price_approx > 0):  # Data available
    
    recommend "Buy-Write"
```

### Capital Calculation (Approximate):
```python
# Use Step 5 yfinance-derived price (±5% accuracy acceptable)
stock_price = SMA20 + Price_vs_SMA20
shares = 100
stock_cost = stock_price * shares
call_premium_est = stock_price * 0.01 * shares  # ~1% OTM estimate
net_capital = stock_cost - call_premium_est
```

### Execution (Step 9B):
- Fetch real-time stock price
- Fetch live option chains
- Calculate exact strike (1-2% OTM)
- Build order: `BUY 100 shares + SELL 1 CALL`

---

## 6. PRICE DATA ASSUMPTIONS (APPROVED)

### Strategy Layer (Step 7/7B):
```
✅ Use stock price from Step 5 (yfinance)
✅ Accuracy tolerance: ±5%
✅ Purpose: Ranking + capital estimates
✅ Speed: Fast (no real-time API calls)
```

### Execution Layer (Step 9B):
```
✅ Fetch real-time stock price (Tradier API)
✅ Fetch live option chains (Tradier API)
✅ Accuracy tolerance: Exact
✅ Purpose: Order construction + breakeven
```

### Architectural Principle:
> **Strategy discovery should be fast and approximate.**
> **Execution should be precise and slow.**

Mixing real-time pricing into Step 7:
- ❌ Slows scans (API rate limits)
- ❌ Breaks modularity (coupling)
- ❌ Causes hidden bugs (state management)

---

## 7. UI DISCLOSURE (MANDATORY TEXT)

### Display in Step 7 Header:
```
⚠️ CRITICAL: Confidence is a RELATIVE RANKING score, NOT execution readiness.

- Step 7 answers: "What strategies make sense for this ticker?"
- PCS (Step 10+) answers: "Should I execute this trade now?"

High confidence ≠ "Execute now" ≠ GEM status
```

### Display in Strategy Explorer (Step 7B):
```
⚠️ IMPORTANT: Suitability Score ranks strategies FOR THIS TICKER relative to each other.

It does NOT indicate:
- ✗ Execution readiness (PCS scoring happens in Step 10+)
- ✗ GEM status (final validation downstream)
- ✗ "Execute now" signal

Do NOT conflate ranking confidence with execution approval.
```

---

## 8. SUMMARY MENTAL MODEL

### Separation of Concerns:
```
┌─────────────┬────────────────┬──────────────────┐
│   Layer     │    Question    │   Complexity     │
├─────────────┼────────────────┼──────────────────┤
│ Step 2-6    │ What exists?   │ Descriptive      │
│ Step 7      │ What could I   │ Prescriptive     │
│             │   do?          │   (Fast)         │
│ Step 9B     │ How exactly?   │ Execution        │
│             │                │   (Precise)      │
│ Step 10+    │ Should I do it │ Validation       │
│ (PCS)       │   NOW?         │   (Gating)       │
└─────────────┴────────────────┴──────────────────┘
```

### DO NOT Collapse These Layers:
- ❌ Do NOT mix execution pricing into Step 7
- ❌ Do NOT treat Confidence as PCS
- ❌ Do NOT use Step 7 output as "execute now" signal

---

## 9. IMPLEMENTATION CHECKLIST

### Step 7/7B Code:
- [x] Uses yfinance-derived stock price (not real-time)
- [x] Calculates approximate capital requirements
- [x] Ranks strategies by suitability (relative)
- [x] Includes Buy-Write for bullish + IV_Rich setups
- [x] No Tradier API calls (defer to Step 9B)
- [x] No PCS calculation (defer to Step 10+)

### UI (Dashboard):
- [x] Tier 1 shown by default
- [x] Tier 2/3 hidden behind toggles
- [x] Confidence disclaimer displayed prominently
- [x] Filters: Timeframe, Risk Profile, Rank By, Ticker Search
- [x] Strategy type filter (CSP, Covered Call, Buy-Write, etc.)
- [x] Capital fit indicator (% of account)

### Documentation:
- [x] STEP7_ARCHITECTURE.md created
- [x] Confidence vs PCS distinction documented
- [x] Tier definitions documented
- [x] Buy-Write logic documented

---

## 10. PRODUCTION RULES (ENFORCE)

### Forbidden Actions:
1. ❌ Showing all tiers by default
2. ❌ Calling Step 7 confidence "PCS"
3. ❌ Using Confidence as execution signal
4. ❌ Fetching real-time prices in Step 7
5. ❌ Mixing Buy-Write into Step 3-6
6. ❌ Removing tier filter toggles
7. ❌ Omitting confidence disclaimer

### Required Actions:
1. ✅ Default show Tier 1 only
2. ✅ Display confidence disclaimer
3. ✅ Provide strategy type filters
4. ✅ Calculate approximate capital
5. ✅ Defer exact pricing to Step 9B
6. ✅ Maintain separation: Discovery → Validation → Execution

---

## 11. FUTURE ENHANCEMENTS (OPTIONAL)

### RAG Explanations (Per Strategy):
```
"Why is Put Credit Spread #1 for AAPL?"
→ "Bullish signal + Age_0_5 crossover + IV_Rich (30D gap = 3.9) 
   + fits Conservative risk profile + 95/100 income goal alignment"
```

### Confidence Normalization:
```python
# Normalize confidence to 0-100 scale per ticker
# Account for: Signal strength, IV magnitude, crossover age, goal alignment
confidence_normalized = (base_score * signal_multiplier * iv_multiplier) / 100
```

### Strategy Constraint Engine:
```python
# Block strategies that violate user constraints
if capital_required > max_position_size * 3:
    exclude_strategy()
if risk_profile_mismatch:
    downrank_strategy()
```

---

## 12. TESTING & VALIDATION

### Unit Tests:
- [ ] Tier filtering works correctly
- [ ] Buy-Write only recommended when conditions met
- [ ] Confidence scores normalized 0-100
- [ ] Capital calculations within ±10% of actual

### Integration Tests:
- [ ] Step 7 → Step 9B transition works
- [ ] Tier 1 strategies proceed to option chain fetch
- [ ] Tier 2/3 strategies blocked correctly

### UI Tests:
- [ ] Default shows Tier 1 only
- [ ] Tier toggles work
- [ ] Filters apply correctly
- [ ] Disclaimer is visible

---

## 13. CANONICAL BEHAVIOR (REFERENCE)

This is now the **official Step 7 behavior**:

1. **Buy-Write enabled** in Step 7B for bullish + IV_Rich setups
2. **yfinance price** from Step 5 is sufficient for strategy ranking
3. **Confidence** is relative ranking, NOT execution readiness
4. **Tier 1 default**, Tier 2/3 behind toggles
5. **Execution precision** deferred to Step 9B
6. **PCS validation** deferred to Step 10+

**All future work must comply with these rules.**

---

**End of Document**
