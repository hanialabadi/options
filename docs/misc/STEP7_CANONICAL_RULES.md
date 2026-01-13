# STEP 7 – STRATEGY RECOMMENDATION: CANONICAL RULES & INTERPRETATION

**AUTHORITATIVE SYSTEM CONTRACT FOR STEP 7**

Last Updated: December 27, 2025

---

## PREAMBLE

Step 7 is a **prescriptive discovery layer**, not an execution or approval engine. Its purpose is to identify which strategy structures are worth evaluating, not which trades should be executed.

The following rules are mandatory and define the authoritative contract for Step 7.

---

## 1. What Step 7 IS

Step 7 is the first **PRESCRIPTIVE** step in the pipeline.

• Steps 2–6 describe what exists in the market  
• Step 7 recommends what to do in principle, given:
  - market context
  - volatility regime
  - trend/regime
  - user profile (risk, goal, capital)

**Step 7 does NOT mean "execute now".**

---

## 2. Primary vs Secondary Strategy (Strict Semantics)

• Each ticker MUST have exactly one Primary Strategy  
• Secondary Strategy is optional and must be visually de-emphasized  

**Primary Strategy means:**
→ "Best structural expression given the current market + volatility context"

**Primary Strategy does NOT mean:**
→ Safest  
→ Lowest risk  
→ Highest return  
→ GEM  
→ Execute now  

**Secondary_Strategy means:**
→ a valid alternative if assumptions change  
→ NOT equal priority  
→ NOT a hedge recommendation  

---

## 3. Tier Enforcement (Non-Negotiable)

• Step 7 MUST display **Tier 1 strategies only** by default  
• Tier 2 and Tier 3 strategies are not actionable and must be:
  - Hidden entirely, OR
  - Available only behind an explicit "Show non-Tier-1 (educational only)" toggle (OFF by default)

**Rationale:**
Tier ≠ quality score. **Tier = execution eligibility.**  
Step 7 is about evaluation priority, not education breadth.

**Tier Definitions:**

**Tier 1:**
• Broker-approved  
• Logic-ready  
• Executable TODAY  

**Tier 2 / Tier 3:**
• Structurally valid but:
  - broker constraints (Tier 2)
  - system logic not ready (Tier 3)
  - NOT actionable (educational only)

**Tier ≠ timing**  
**Tier ≠ conviction**

---

## 4. Column Naming & Meaning (Disambiguation Required)

Rename and enforce semantics:

• **Confidence → Context Confidence**
  - Heuristic alignment score (signal + IV regime + structure fit)
  - NOT PCS
  - NOT execution approval

• **Success Probability → Estimated POP (Contextual)**
  - Heuristic estimate only
  - NOT real probability

• **Entry Priority → Evaluation Priority**
  - Indicates urgency to review
  - NOT urgency to trade

**Explicit rule:**

**High Context Confidence ≠ GEM ≠ Execute Now**

**PCS and real conviction only happen after option chains exist (Step 9+).**

---

## 5. Remove Execution-Level Fields from Step 7 UI (MANDATORY)

The following **MUST NOT appear** in Step 7 UI:

• Capital allocation  
• % Account  
• Win %  
• Risk/Reward ratio  
• Position sizing  

**Reason:**
These require strikes, premiums, and option chains.

**Execution-level metrics belong exclusively to Step 9B+.**

**Before option chains exist:**
• Win % is a placeholder heuristic  
• Success_Probability is NOT real  
• Risk/Reward ratio is structural only  

Real probabilities and payoffs only exist in Step 9+.

---

## 6. Strategy Filtering (Required)

Step 7 MUST support filtering by:

• **Strategy Type:**
  - Directional
  - Neutral
  - Volatility

• **Strategy Name:**
  - Buy-Write
  - Put Credit Spread
  - LEAP Call
  - Iron Condor
  - etc.

**Rationale:**
Step 7 must function as a **decision-narrowing tool**, not a scrolling list.

---

## 7. Buy-Write Strategy Rules (Explicit)

Buy-Write is a **prescriptive recommendation**, not a descriptive observation.

**Buy-Write may appear in Step 7 ONLY if:**
• Signal is Bullish or Sustained Bullish  
• IV supports call premium selling  
• Stock price data exists (Step 5 data is sufficient)  
• User profile permits stock ownership  

**When displayed:**
• Label explicitly as **"Buy-Write (Stock + Short Call)"**  
• Tooltip must state: "Requires purchasing shares. Not a covered call."

**Buy-Write is:**
• A distinct strategy  
• Stock purchase + call sale at the same time  
• NOT a Covered Call  
• NOT assumed stock ownership  

**Buy-Write belongs ONLY in Step 7 because:**
• It is prescriptive  
• It depends on user capital  
• It competes with CSP, not Covered Calls  

**If these conditions are not met, Buy-Write must NOT appear.**

---

## 8. Execution Boundary (Hard Line)

Step 7 MUST include an **explicit, non-dismissable notice:**

> "Step 7 identifies which strategy structures are worth evaluating.
> **This is not an execution signal.**
> Execution quality (PCS / GEM status) is determined in Step 9+ via option chain analysis."

**Canonical rule:**

**If a value depends on strikes, premiums, or real option chains, it does NOT belong in Step 7.**

**Execution details:**
• Strikes  
• Premiums  
• Real POP  
• Exact capital  
• Real risk/reward  

**ALL belong to:**
→ Step 9B and beyond  

**Anything before that is conceptual only.**  

---

## 9. Personal Recommendation Text (RAG Alignment)

**Personal recommendation text MUST:**

• Reference ONLY strategies explicitly produced by Step 7  
• Match RAG definitions exactly  
• Avoid execution language  
• Avoid urgency  
• Avoid ownership assumptions  

**Allowed tone:**
→ "Given your income goal and current volatility setup, this strategy is structurally aligned…"

**Forbidden tone:**
→ "You should execute this trade now"  
→ "This guarantees income"  
→ "Best trade"  

---

## 10. Role Separation (Final Definition)

• **Steps 2–6:** Descriptive (what exists)  
• **Step 7:** Prescriptive discovery (what to evaluate)  
• **Step 8:** Sizing logic (still non-executional)  
• **Step 9+:** Execution validation (PCS / GEM / real prices)  

**Step 7 answers:**

"What strategy structure makes sense to look at next?"

**It does NOT answer:**

"Should I place this trade?"

---

---

## FINAL AUTHORITY STATEMENT

**This document supersedes all prior Step 7 descriptions and should be treated as the canonical system contract.**

**Any UI, logic, or labeling that violates these rules is considered a bug.**

---

## FINAL SYSTEM CONTRACT (ONE LINE)

**Step 7 answers:**  
*"Given this market context and this user, what strategies are worth evaluating?"*

**It does NOT answer:**  
*"What trade should I place right now?"*

---

## Implementation References

- **Core Logic**: `core/scan_engine/step7_strategy_recommendation.py`
- **Multi-Strategy Ranker**: `core/scan_engine/step7b_multi_strategy_ranker.py`
- **UI Implementation**: `streamlit_app/dashboard.py` (Step 7 section, lines 1424+)
- **Buy-Write Logic**: `step7b_multi_strategy_ranker.py` (lines 307-346)

## Compliance Checklist

Before any Step 7 modification:
- [ ] Does it preserve the "worth evaluating" vs "execute now" boundary?
- [ ] Does it avoid claiming Confidence = PCS?
- [ ] Does it defer execution details to Step 9B?
- [ ] Does Buy-Write only appear when Step 7 produces it?
- [ ] Are all numeric values labeled "Indicative" or "Approximate"?
- [ ] Does UI default to Tier 1 only?
- [ ] Does personal recommendation avoid urgency/execution language?

**Any deviation from these rules requires explicit architectural review.**
