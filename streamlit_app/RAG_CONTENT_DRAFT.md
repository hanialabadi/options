# RAG Content Outlines for Steps 6, 9A, 10, 11

This document contains the structured RAG content to be implemented in the dashboard for remaining steps. Use Step 3's implementation as the template.

---

## Step 6: GEM Filter - RAG Content

### 📘 Step Header (Brief Purpose)

```markdown
**Purpose:** Data completeness validation and execution readiness filtering

**What This Step Does:**
- Validates required data fields (chart signals + volatility structure)
- Applies strategy-specific completeness checks
- Assigns Scan_Tier (Tier 1: fresh crossovers, Tier 2: recent signals)
- Calculates PCS_Seed (preliminary quality score 68-75)

**What This Step Does NOT Do:**
- Assign specific strategies (that's Step 7)
- Fetch option contracts (that's Step 9B)
- Calculate position sizing (that's Step 8)

**Filtering Behavior:**
- Typical reduction: 80-100 tickers → 45-50 tickers
- Filters for: Data completeness, not trade quality
- Output: Tickers with enough data to make strategy decisions
```

### 🔍 Explain This Step (RAG) - Full Dropdown

```markdown
### Core Question This Step Answers

**"Does this ticker have complete data to support strategy assignment and execution?"**

---

### Purpose & Scope

**Classification Type:** Validation filter (prescriptive gate)

**Strategy Scope:** Strategy-aware (different rules for directional vs neutral)
- Directional strategies: Require chart signals (Regime, Signal_Type, crossover data)
- Neutral strategies: Require volatility structure (IVHV gaps, term structure)
- All strategies: Require minimum data completeness

**Filtering Behavior:** Moderate  
- Filters OUT: Tickers with incomplete/missing data
- Passes THROUGH: 45-50 tickers with sufficient data for decisions
- This is where the funnel narrows significantly

---

### Inputs Required

From **Step 3** (IVHV Analysis):
- `IVHV_gap_30D`, magnitude regimes, pricing direction
- Multi-timeframe gaps (60D, 90D, 180D, 360D)

From **Step 5** (Chart Signals):
- `Regime`, `Signal_Type`, `EMA_Signal`
- `Days_Since_Cross`, `Crossover_Age_Bucket`
- `Trend_Slope`, `Atr_Pct`

---

### Outputs Produced

**Data Completeness Flags:**
- `Universal_Data_Complete`: Boolean (has all required fields)
- `Directional_Ready`: Chart signals populated
- `Neutral_Ready`: Volatility structure complete

**Execution Metadata:**
- `Scan_Tier`: GEM_Tier_1 (0-14 days since cross) or GEM_Tier_2 (15-30 days)
- `PCS_Seed`: Preliminary quality score (68-75 range)
- `Crossover_Age_Bucket`: Fresh/Recent/Aging classification

---

### Common Misinterpretations

❌ **"Only 45 tickers means I need to loosen filters"**  
✅ Step 6 is working correctly - most tickers lack complete data

❌ **"Tier 1 is better than Tier 2"**  
✅ Tier classification is about crossover freshness, not quality (both are tradeable)

❌ **"PCS_Seed is the final quality score"**  
✅ PCS_Seed is preliminary (68-75); Step 10 recalculates with option contract data (50-100 scale)

❌ **"GEM candidates are the final trade list"**  
✅ GEM candidates still need: strategy assignment (Step 7), position sizing (Step 8), contracts (Step 9B), validation (Step 10), pairing (Step 11)

---

### Metric Interpretation Guide

**Universal_Data_Complete:**
- What: Boolean indicating all required fields present
- Purpose: Ensures ticker has enough data for any strategy type
- Typical: 60-70% of Step 5 output passes this check

**Scan_Tier (GEM_Tier_1 vs GEM_Tier_2):**
- What: Crossover freshness classification
- Tier 1: 0-14 days since cross (fresh momentum)
- Tier 2: 15-30 days since cross (established trend)
- NOT an indicator of: Trade quality, profit potential, or priority
- IS an indicator of: Signal recency and momentum stage

**PCS_Seed:**
- What: Preliminary quality score (68-75 range)
- Purpose: Baseline score before option contract analysis
- Step 10 will recalculate: Final PCS (50-100) using bid-ask, liquidity, greeks
- Do NOT use for: Ranking trades (use final PCS from Step 10 instead)

**Crossover_Age_Bucket:**
- Fresh (0-7 days): Early momentum capture
- Recent (8-14 days): Established trend confirmation
- Aging (15-30 days): Mature signal, lower urgency

---

### Debugging Checklist

If Step 6 output looks wrong:

- [ ] **Check Step 5 completion:** Did chart signals populate correctly?
- [ ] **Verify IVHV data:** Are magnitude regimes and gaps present from Step 3?
- [ ] **Review missing data:** Which fields are causing Universal_Data_Complete = False?
- [ ] **Check Tier assignments:** Are Days_Since_Cross values reasonable?
- [ ] **Validate PCS_Seed:** Should be in 68-75 range (if outside, calculation error)

If Step 6 shows "too few" tickers (< 20):

- ✅ This may be correct (low volatility market conditions)
- ✅ Check if Step 5 had chart signal failures (API rate limits?)
- ❌ Do NOT lower data completeness requirements to "fix" this

If Step 6 shows "too many" tickers (> 80):

- ⚠️ Check if filtering logic is being applied correctly
- ⚠️ Verify IVHV threshold is >= 3.5 (not lowered to 2.0)
- ⚠️ Confirm directional extension filters are active

---

### Which Strategies Use This Data?

**All strategies** require Step 6 validation, but interpretation differs:

- **Directional strategies (CSP, Covered Call, Vertical Spreads):**
  - MUST have: Chart signals (Regime, Signal_Type, crossover data)
  - Use: Tier classification for timing urgency
  
- **Neutral strategies (Iron Condor, Butterfly, Straddle/Strangle):**
  - MUST have: Volatility structure (IVHV gaps, term structure)
  - Use: Regime for strike width and distance from money
  
- **Hybrid strategies (PMCC, Diagonal, Calendar):**
  - MUST have: Both chart signals AND volatility structure
  - Use: Both Tier and magnitude regime for strategy selection

Step 6 ensures every ticker has the MINIMUM data needed for its appropriate strategies.
```

---

## Step 9A: Determine DTE Timeframe - RAG Content

### 📘 Step Header (Brief Purpose)

```markdown
**Purpose:** Map strategies to appropriate Days-To-Expiration (DTE) ranges

**What This Step Does:**
- Maps strategy names to DTE buckets (30-45, 60-120, 180-365, 450-900)
- Sets min/max DTE bounds per strategy
- Adds timeframe classification (ShortTerm, MediumTerm, LEAP, UltraLEAP)

**What This Step Does NOT Do:**
- Filter by broker approval tier (that's a separate tier system)
- Fetch actual option contracts (that's Step 9B)
- Validate contract liquidity (that's Step 10)

**Logic:**
- Short (30-45 DTE): CSP, Covered Call, Credit Spreads
- Medium (60-120 DTE): Directional Spreads, Straddle/Strangle
- LEAP (180-365 DTE): LEAP strategies, PMCC base leg
- Ultra-LEAP (450-900 DTE): Multi-year thesis plays

**Output:** Each strategy row gets `Min_DTE`, `Max_DTE`, `Timeframe` columns
```

### 🔍 Explain This Step (RAG) - Full Dropdown

```markdown
### Core Question This Step Answers

**"What option expiration timeframe does each strategy require?"**

---

### Purpose & Scope

**Classification Type:** Mapping (descriptive metadata, not filtering)

**Strategy Scope:** Strategy-specific (different DTE ranges per strategy)
- Each strategy has optimal DTE range based on risk/reward profile
- Timeframes align with volatility decay patterns (theta)
- Does NOT exclude strategies, just defines search parameters

**Filtering Behavior:** None  
- No tickers filtered out
- Simply adds DTE metadata columns to each strategy row
- Actual expiration filtering happens in Step 9B

---

### Inputs Required

From **Step 7** (Strategy Recommendation):
- `Primary_Strategy`: Strategy name (e.g., "CSP", "Bull Put Spread")
- `Trade_Type`: Directional/Neutral/Hybrid classification

---

### Outputs Produced

**DTE Range Columns:**
- `Min_DTE`: Minimum days to expiration (e.g., 30)
- `Max_DTE`: Maximum days to expiration (e.g., 45)
- `Timeframe`: ShortTerm/MediumTerm/LEAP/UltraLEAP

**DTE Mapping Logic:**
```
CSP → 30-45 DTE (ShortTerm)
Covered Call → 30-45 DTE (ShortTerm)
Bull Put Spread → 30-60 DTE (ShortTerm)
Bear Call Spread → 30-60 DTE (ShortTerm)
Bull Call Spread → 60-120 DTE (MediumTerm)
Bear Put Spread → 60-120 DTE (MediumTerm)
Iron Condor → 30-60 DTE (ShortTerm-Medium)
LEAP Call/Put → 180-365 DTE (LEAP)
PMCC → 180-730 DTE (LEAP-Ultra)
Ultra-LEAP → 450-900 DTE (UltraLEAP)
```

---

### Common Misinterpretations

❌ **"Step 9A filters by broker approval tier"**  
✅ No - this step only maps DTE ranges. Tier filtering would be separate logic.

❌ **"Strategies with longer DTE are better"**  
✅ DTE selection depends on strategy intent (income vs growth vs theta decay)

❌ **"Min_DTE and Max_DTE are hard constraints"**  
✅ These are SEARCH bounds for Step 9B, not pass/fail filters

❌ **"Step 9A should reduce ticker count"**  
✅ This step adds metadata only - no filtering occurs

---

### Metric Interpretation Guide

**Min_DTE / Max_DTE:**
- What: Days-to-expiration range to search in Step 9B
- Purpose: Define expiration window aligned with strategy intent
- Example: CSP with Min_DTE=30, Max_DTE=45 → search 30-45 day expirations only
- NOT a filter: If no contracts exist in range, Step 9B handles fallback

**Timeframe:**
- ShortTerm: Theta-focused, high-probability income strategies
- MediumTerm: Directional plays with moderate theta decay
- LEAP: Stock replacement, multi-month trends, reduced theta decay
- UltraLEAP: Multi-year thesis, minimal theta, high delta exposure

---

### Debugging Checklist

If Step 9A output looks wrong:

- [ ] **Check strategy names:** Are Primary_Strategy values recognized?
- [ ] **Verify DTE ranges:** Do Min_DTE/Max_DTE values match strategy type?
- [ ] **Confirm timeframe tags:** Does Timeframe align with DTE range?
- [ ] **Review unmapped strategies:** Are any strategies getting default/null DTE values?

If Step 9B fails to find contracts:

- ⚠️ Check if DTE range is too narrow (expand by 5-10 days)
- ⚠️ Verify expiration availability (market holidays, weekly vs monthly)
- ✅ This is NOT a Step 9A bug (Step 9B handles expiration search)

---

### Which Strategies Use This Data?

**Step 9B** consumes this data to:
- Filter option expirations within Min_DTE to Max_DTE range
- Select appropriate contracts (calls vs puts vs spreads)
- Match strikes to strategy requirements (ITM/ATM/OTM)

**Step 10** uses timeframe to:
- Apply timeframe-specific PCS scoring adjustments
- Validate liquidity requirements (longer DTE = lower liquidity OK)

**Step 11** uses timeframe to:
- Balance portfolio across Short/Medium/LEAP allocations
- Ensure diversification of theta exposure
```

---

## Step 10: Filter & Validate Contracts - RAG Content

### 📘 Step Header (Brief Purpose)

```markdown
**Purpose:** Quality validation and final PCS scoring with real option contract data

**What This Step Does:**
- Validates contract liquidity (open interest, volume, bid-ask spread)
- Recalculates PCS (Probability-Calibrated Score) from 68-75 seed → 50-100 final
- Filters for: OI >= 100, volume >= 10, spread <= 10% of mid
- Adds contract metadata: bid, ask, delta, gamma, theta, implied probability

**What This Step Does NOT Do:**
- Assign strategies (that's Step 7)
- Select final positions (that's Step 11)
- Calculate position sizing (that's Step 8)

**Logic:**
- Uses ACTUAL option contract data from Step 9B
- Applies multi-factor quality scoring (volatility + chart + liquidity + greeks)
- Filters out: Low liquidity, wide spreads, unsuitable strikes

**Output:** Validated contracts with final PCS scores, ready for pairing
```

### 🔍 Explain This Step (RAG) - Full Dropdown

```markdown
### Core Question This Step Answers

**"Is this specific option contract executable, liquid, and properly priced?"**

---

### Purpose & Scope

**Classification Type:** Validation filter (prescriptive gate + enrichment)

**Strategy Scope:** Strategy-specific (different rules per strategy type)
- Credit strategies: Require high premium, suitable probability
- Debit strategies: Require tight spreads, good delta exposure
- Neutral strategies: Require balanced Greeks, symmetric liquidity

**Filtering Behavior:** High  
- Filters OUT: Low liquidity contracts, wide spreads, poor quality
- Passes THROUGH: 15-25 validated contracts with final PCS scores
- This is the CRITICAL quality gate before execution

---

### Inputs Required

From **Step 9B** (Fetch Contracts):
- Option contract data: strike, bid, ask, last, OI, volume
- Greeks: delta, gamma, theta, vega, implied_vol
- Contract symbols and expiration dates

From **Step 3** (IVHV Analysis):
- IVHV gaps, magnitude regimes (for PCS recalibration)

From **Step 5** (Chart Signals):
- Regime, Signal_Type (for PCS recalibration)

---

### Outputs Produced

**Contract Validation Flags:**
- `Liquidity_OK`: Boolean (passes OI/volume thresholds)
- `Spread_OK`: Boolean (bid-ask spread <= 10% of mid)
- `Strike_OK`: Boolean (strike appropriate for strategy)

**Final Scoring:**
- `PCS_Final`: Recalibrated score (50-100 scale)
  - 85-100: Tier S (exceptional quality)
  - 75-84: Tier A (strong quality)
  - 65-74: Tier B (acceptable)
  - 50-64: Tier C (marginal, consider avoiding)

**Probability Metrics:**
- `Win_Probability`: Estimated success rate
- `Risk_Reward_Ratio`: Expected payoff structure
- `Breakeven_Price`: Price needed to avoid loss

---

### Common Misinterpretations

❌ **"PCS_Final 65 is too low to trade"**  
✅ Tier B (65-74) is acceptable for most strategies. Only avoid < 60.

❌ **"Higher PCS always means better trade"**  
✅ PCS measures quality, not profit potential. Consider full context.

❌ **"Step 10 should pass all contracts from Step 9B"**  
✅ Step 10 is SUPPOSED to filter aggressively (30-50% rejection normal)

❌ **"Wide bid-ask spread is OK if I use limit orders"**  
✅ Wide spreads indicate poor liquidity - execution risk even with limits

❌ **"Open interest doesn't matter for small positions"**  
✅ Low OI increases slippage risk regardless of position size

---

### Metric Interpretation Guide

**PCS_Final (50-100 scale):**
- What: Multi-factor quality score combining volatility + chart + liquidity + greeks
- Components:
  - IVHV magnitude (30% weight)
  - Chart regime/signal (25% weight)
  - Liquidity (OI, volume, spread) (25% weight)
  - Greeks alignment (delta, theta efficiency) (20% weight)
- Tier Cutoffs:
  - 85+: Tier S (exceptional setups, rare)
  - 75-84: Tier A (strong quality, most trades here)
  - 65-74: Tier B (acceptable, monitor closely)
  - 50-64: Tier C (marginal, avoid unless thesis is strong)

**Liquidity_OK:**
- What: Composite check of OI, volume, bid-ask spread
- Criteria:
  - Open Interest >= 100 (ensures exit liquidity)
  - Volume >= 10 (recent trading activity)
  - Spread <= 10% of mid price (execution efficiency)
- Failure = contract filtered out (not tradeable)

**Win_Probability:**
- What: Estimated success rate based on delta (for spreads) or realized prob (for premium)
- NOT a prediction: Market conditions change, use as guideline only
- Typical ranges:
  - CSP/Credit Spreads: 60-80% (high-probability strategies)
  - Debit Spreads: 40-60% (lower probability, higher payoff)
  - LEAPs: N/A (success measured by price movement, not expiration)

---

### Debugging Checklist

If Step 10 output looks wrong:

- [ ] **Check contract data:** Did Step 9B fetch bid/ask/OI correctly?
- [ ] **Verify liquidity thresholds:** Are OI/volume criteria too strict?
- [ ] **Review PCS distribution:** Are scores clustered or spread across 50-100?
- [ ] **Validate Greeks:** Are delta/theta/gamma values reasonable?
- [ ] **Check spread calculations:** Is spread % being calculated correctly?

If Step 10 filters out too many contracts (< 10 remaining):

- ⚠️ Check if market conditions are illiquid (low volatility period?)
- ⚠️ Review if liquidity thresholds are too aggressive
- ✅ Do NOT lower quality standards just to get more contracts

If Step 10 passes too many contracts (> 40 remaining):

- ⚠️ Verify filtering logic is being applied
- ⚠️ Check if quality thresholds are too loose
- ⚠️ Confirm PCS_Final is being calculated (not reusing PCS_Seed)

---

### Which Strategies Use This Data?

**Step 11** (Final Pairing) uses PCS_Final to:
- Rank contracts within each strategy
- Select top 1-2 contracts per strategy type
- Balance portfolio allocation across quality tiers
- Apply capital limits (highest PCS gets priority)

**Manual Review** uses PCS_Final to:
- Prioritize which trades to research first
- Identify exceptional setups (PCS >= 85)
- Flag marginal trades for closer scrutiny (PCS < 65)

Step 10 is the LAST objective quality gate. After this, decisions are strategic allocation, not quality filtering.
```

---

## Step 11: Final Strategy Pairing - RAG Content

### 📘 Step Header (Brief Purpose)

```markdown
**Purpose:** Final execution-ready position selection with capital allocation

**What This Step Does:**
- Pairs strategies for portfolio construction (e.g., CSP + PMCC)
- Validates Option_Type column (required for execution)
- Applies capital limits and max positions per ticker
- Selects top 1-2 contracts per strategy based on PCS_Final

**What This Step Does NOT Do:**
- Calculate position sizing (that's Step 8 - already done)
- Validate contract quality (that's Step 10 - already done)
- Place trades (output is recommendations, not orders)

**Logic:**
- Groups by ticker and strategy
- Ranks by PCS_Final (highest quality first)
- Applies capital limit (default $50k total allocation)
- Outputs: 8-12 final execution-ready positions

**Output:** Final trade list with position size, strikes, expirations, greeks
```

### 🔍 Explain This Step (RAG) - Full Dropdown

```markdown
### Core Question This Step Answers

**"What is my final trade list, and how should I allocate capital?"**

---

### Purpose & Scope

**Classification Type:** Execution planning (prescriptive selection + allocation)

**Strategy Scope:** Portfolio-level (considers strategy diversification)
- Balances directional vs neutral strategies
- Diversifies timeframes (Short/Medium/LEAP)
- Considers correlation (max 2 positions per ticker)

**Filtering Behavior:** Final reduction  
- Filters OUT: Lower PCS contracts when multiple options exist
- Passes THROUGH: 8-12 final execution-ready positions
- Output is TRADE LIST (ready for broker submission)

---

### Inputs Required

From **Step 10** (Validated Contracts):
- Validated contracts with PCS_Final scores
- Option contract metadata (strike, expiration, greeks)
- Liquidity validation flags

From **Step 8** (Position Sizing):
- `Position_Size_Shares`: Calculated position size per strategy
- `Max_Risk_Per_Trade`: Capital at risk calculation
- `Portfolio_Allocation_Pct`: % of total capital for this trade

---

### Outputs Produced

**Final Positions:**
- Top 1-2 contracts per strategy (ranked by PCS_Final)
- Complete execution package:
  - Ticker, Strategy, Strike, Expiration, Option_Type (Call/Put)
  - Bid, Ask, Mid, Position_Size, Total_Capital_Required
  - Greeks: Delta, Gamma, Theta, Vega
  - PCS_Final, Win_Probability, Risk_Reward_Ratio

**Portfolio Summary:**
- Total capital allocated
- Strategy type distribution (% directional vs neutral)
- Timeframe distribution (% Short vs Medium vs LEAP)
- Risk concentration (max exposure per ticker)

---

### Common Misinterpretations

❌ **"All Step 10 contracts should be in final output"**  
✅ Step 11 selects TOP contracts only. Many validated contracts get excluded.

❌ **"Higher capital allocation means better trade"**  
✅ Capital allocation reflects position sizing rules, not quality assessment

❌ **"I should trade all 12 final positions"**  
✅ Final output is prioritized list - trade top 3-5 first, others are backups

❌ **"Step 11 guarantees these trades will profit"**  
✅ Step 11 identifies QUALITY opportunities. Execution, timing, and market still matter.

❌ **"I need to increase capital_limit to get more trades"**  
✅ If < 8 positions, issue is upstream (not enough quality contracts), not capital

---

### Metric Interpretation Guide

**Number of Final Positions:**
- What: Total execution-ready positions after all filtering
- Typical: 8-12 positions (can be lower in low-volatility conditions)
- If < 5: Check upstream steps (likely data quality or market conditions)
- If > 15: Check if capital_limit is too high or max_per_ticker not enforced

**Strategy Type Distribution:**
- What: % of capital allocated to directional vs neutral strategies
- Healthy portfolio: 60-70% directional, 30-40% neutral
- All directional = high correlation risk
- All neutral = missing upside capture

**Timeframe Distribution:**
- What: % of positions in Short/Medium/LEAP timeframes
- Balanced: 50% Short (income), 30% Medium (tactical), 20% LEAP (structural)
- All Short = high theta decay, capital intensive
- All LEAP = low income generation, delayed feedback

**Max Exposure Per Ticker:**
- What: Total capital allocated to single underlying
- Limit: 2 positions per ticker (1 short-term + 1 long-term typical)
- Purpose: Diversification, avoid concentration risk

---

### Debugging Checklist

If Step 11 output looks wrong:

- [ ] **Check Option_Type column:** Is it populated for all rows? (Step 9B issue if missing)
- [ ] **Verify PCS_Final:** Are contracts ranked correctly by quality?
- [ ] **Review capital allocation:** Does total allocated capital match capital_limit?
- [ ] **Validate position counts:** Are max_per_ticker and max_total_positions enforced?
- [ ] **Check strategy pairing:** Are Short/Medium/LEAP balanced?

If Step 11 shows too few positions (< 5):

- ⚠️ Review Step 10 output: Did enough contracts pass quality filters?
- ⚠️ Check capital_limit: Is it too restrictive?
- ⚠️ Verify upstream filtering: Were Steps 3/6 too aggressive?
- ✅ This may be correct (low opportunity environment)

If Step 11 shows no positions:

- ❌ Check for Option_Type column missing error
- ❌ Verify Step 10 output is not empty
- ❌ Confirm capital_limit > 0 and position limits not set to 0

---

### Which Strategies Appear in Final Output?

**Common strategy mixes (typical portfolio):**

**Income-focused (50-60% capital):**
- CSP (30-45 DTE): High-probability premium collection
- Covered Call (30-45 DTE): Enhance stock returns
- Credit Spreads (30-60 DTE): Defined-risk premium selling

**Growth-focused (20-30% capital):**
- LEAP Call/Put (180-365 DTE): Stock replacement, leveraged exposure
- Vertical Spread (60-120 DTE): Directional plays with defined risk

**Neutral/Hedge (10-20% capital):**
- Iron Condor (30-60 DTE): Range-bound income
- PMCC (180-730 DTE): Synthetic covered call, capital efficient

**Step 11 ensures:**
- Each strategy type is represented (if qualified candidates exist)
- No single ticker dominates allocation
- Timeframes are diversified (theta exposure balanced)
- Capital is allocated to highest PCS contracts first

This is your FINAL CHECKLIST before execution.
```

---

## Implementation Instructions

### Priority Order:
1. **Step 3** - Already implemented (proof of concept complete)
2. **Step 6** - Implement next (critical filtering checkpoint)
3. **Step 10** - High confusion potential (PCS recalibration)
4. **Step 11** - Final gate (execution readiness)
5. **Step 9A** - Lower confusion (straightforward DTE mapping)

### Standard Pattern for Each Step:

```python
# === STEP X: [Name] ===
st.header("[Icon] Step X: [Name]")

# Brief purpose (2-3 lines)
st.markdown("""
**Purpose:** [One sentence]

**What This Step Does:** [3-4 bullets]

**What This Step Does NOT Do:** [2-3 bullets]

**Filtering Behavior:** [Typical input → output count]
""")

# RAG Expander 1: Quick explanation
with st.expander("ℹ️ [Quick Context Title]", expanded=False):
    st.markdown("""[Brief explanation of expected behavior]""")

# RAG Expander 2: Full explanation
with st.expander("📘 Explain This Step (RAG)", expanded=False):
    st.markdown("""
    ### Core Question This Step Answers
    [Full RAG content from above]
    """)

# Execution button + metrics (with tooltips)
col1, col2 = st.columns([1, 3])
with col1:
    # Button logic
with col2:
    # Metrics with help="" tooltips

# Inspection expander with inline info boxes
if 'stepX_output' in st.session_state:
    with st.expander("🔍 Inspect Step X Output", expanded=False):
        # Tabs with st.info() explanations per tab
```

### Metric Tooltip Template:

```python
st.metric(
    "Metric Name",
    value,
    help="📘 [What this is] | [What it means] | [When to use it] | [Common misinterpretation]"
)
```

---

## Testing Checklist

After implementing RAG for each step:

- [ ] Header purpose is clear and concise (2-3 lines)
- [ ] Quick expander explains expected behavior (why many/few tickers is normal)
- [ ] Full RAG expander answers: What question? What scope? What inputs/outputs?
- [ ] Common misinterpretations listed with ❌/✅ format
- [ ] Metric tooltips added to all st.metric() calls
- [ ] Tab-level st.info() boxes explain what each view shows
- [ ] Debugging checklist included in RAG dropdown
- [ ] "Which strategies use this?" section explains downstream dependencies

---

## User Feedback Loop

After Step 3 + Step 6 implementation:
1. Deploy to test environment
2. User runs pipeline on real data
3. Collect feedback on:
   - Is RAG content answering their questions?
   - Are tooltips appearing at right moments?
   - Is any content redundant or too verbose?
   - Are there NEW questions RAG doesn't address?
4. Refine template based on feedback
5. Batch-implement Steps 9A, 10, 11 with refined pattern
