# Step 3: Persona P1 Enhancement Questionnaire

**Date:** 2026-01-03  
**Context:** System at 9.1/10 after Market Stress Mode + STRUCTURALLY_READY UX  
**Purpose:** Identify single smallest P1 enhancement before production deployment  

---

## Questionnaire Protocol

**Single Question (Verbatim):**

> "What is the single smallest P1 enhancement that would most increase your trust in declining trades, without increasing trade frequency, relaxing gates, or introducing override paths?"

**Critical Constraints:**
- Answer must not increase trade count
- Answer must not create execution pressure
- Answer must not relax existing gates
- Focus: trust in NO decisions, not maximizing output

**Required Score (Per Persona):**

> **Trust Impact (0–10):**  
> How much would this single change increase your confidence in the system's NO decisions?

**Format:** Ask each persona independently (no group brainstorm)

---

## Persona 1: Risk Manager

**Background:**
- Current trust level: 9.1/10
- Recent enhancements: Market Stress Mode (hard halt at RED), STRUCTURALLY_READY visibility
- Production scan result: 0 READY_NOW (system said NO clearly, user accepted)

**Question:**

> "What is the single smallest P1 enhancement that would most increase your trust in declining trades, without increasing trade frequency, relaxing gates, or introducing override paths?"

**Response Framework:**
- Must protect against hidden risk (not maximize output)
- Must be measurable and auditable
- Must prevent regret scenarios ("why didn't system warn me?")

**Required Score:**
> **Trust Impact (0–10):** How much would this single change increase your confidence in the system's NO decisions?

**Expected Answer Range:**
- Portfolio Greek limits (prevent correlated drawdown)
- Earnings proximity gate (avoid binary event risk)
- Position concentration caps (sector/ticker)

---

## Persona 2: Conservative Income Trader

**Background:**
- Current trust level: 9.1/10
- Recent enhancements: Market Stress Mode (hard halt), STRUCTURALLY_READY (visible but blocked)
- Core need: Consistent, predictable income without tail risk

**Question:**

> "What is the single smallest P1 enhancement that would most increase your trust in declining trades, without increasing trade frequency, relaxing gates, or introducing override paths?"

**Response Framework:**
- Must reduce surprise/volatility (not increase opportunity)
- Must be binary and simple (not nuanced)
- Must protect capital first, income second

**Required Score:**
> **Trust Impact (0–10):** How much would this single change increase your confidence in the system's NO decisions?

**Expected Answer Range:**
- Earnings proximity gate (avoid unpredictable moves)
- Dividend ex-date awareness (protect calendar income)
- Assignment risk visibility (early warning)

---

## Persona 3: Volatility Trader

**Background:**
- Current trust level: 9.1/10
- Recent enhancements: Market Stress Mode, STRUCTURALLY_READY UX
- Core need: Exploit volatility dislocations without getting crushed

**Question:**

> "What is the single smallest P1 enhancement that would most increase your trust in declining trades, without increasing trade frequency, relaxing gates, or introducing override paths?"

**Response Framework:**
- Must prevent IV crush scenarios (not find more trades)
- Must be volatility-centric (not directional)
- Must protect against regime shifts

**Required Score:**
> **Trust Impact (0–10):** How much would this single change increase your confidence in the system's NO decisions?

**Expected Answer Range:**
- Earnings proximity gate (prevent IV collapse)
- Skew classification (understand risk distribution)
- Term structure diagnostic (calendar risk)

---

## Persona 4: Directional Swing Trader

**Background:**
- Current trust level: 9.1/10
- Recent enhancements: Market Stress Mode, STRUCTURALLY_READY visibility
- Core need: Align option structure with directional conviction

**Question:**

> "What is the single smallest P1 enhancement that would most increase your trust in declining trades, without increasing trade frequency, relaxing gates, or introducing override paths?"

**Response Framework:**
- Must confirm directional alignment (not generate signals)
- Must prevent false setups (high structure quality, wrong timing)
- Must be momentum-aware

**Required Score:**
> **Trust Impact (0–10):** How much would this single change increase your confidence in the system's NO decisions?

**Expected Answer Range:**
- Earnings proximity gate (avoid timing traps)
- Momentum × IV divergence (confirm setup quality)
- 52-week regime confirmation (trend context)

---

## Analysis Framework

**After collecting responses:**

### Step 1: Collect Trust Impact Scores
For each enhancement mentioned, record:
- Enhancement name
- Persona count (how many mentioned it)
- Average Trust Impact score (0-10)
- Score range (min-max across personas)

Example:
```
Earnings Proximity Gate
  Personas: 4/4
  Avg Trust Impact: 8.5
  Range: 8-9

Portfolio Greek Limits
  Personas: 2/4
  Avg Trust Impact: 7.5
  Range: 7-8
```

### Step 2: Evaluate Against Invariants
For each suggested enhancement, verify:
- ✅ Does NOT auto-execute STRUCTURALLY_READY
- ✅ Does NOT lower IV history requirement
- ✅ Does NOT add "execute anyway" override
- ✅ Does NOT increase trade frequency as goal
- ✅ Does NOT silently degrade evaluation quality
- ✅ Does NOT add fallback execution paths
- ✅ Does NOT optimize for user satisfaction via execution

### Step 3: Rank by Trust-to-Effort Ratio
For each validated enhancement:
- **Trust Impact:** Use average score from personas (0-10)
- **Effort:** Implementation complexity (days)
- **Ratio:** Trust Impact / Effort

Example:
```
Earnings Proximity Gate
  Trust Impact: 8.5
  Effort: 1.5 days
  Ratio: 5.67 per day

Portfolio Greek Limits
  Trust Impact: 7.5
  Effort: 17.5 days (2.5 weeks)
  Ratio: 0.43 per day
```

### Step 4: Select P1 Winner(s)
Criteria:
- Highest trust-to-effort ratio
- Mentioned by 2+ personas
- Average Trust Impact ≥ 7.0
- Can be implemented conservatively (hard gate, no fallbacks)
- Completes a protection gap (not an optimization)

**Decision Rule:**
- If one clear winner (highest ratio + 3+ personas): Implement immediately
- If two strong candidates: Implement in sequence (quick win first)
- If no consensus (all <2 personas): System already complete enough, skip P1 additions

---

## Expected Consensus (Pre-Survey Hypothesis)

**Primary Answer:** Earnings Proximity Gate
- **Why:** All personas fear binary events
- **Predicted Trust Impact Score:** 8-9 (across all personas)
- **Implementation:** Hard gate (block <7 days before earnings)
- **System trust gain:** +0.05 (9.1 → 9.15)
- **Effort:** 1-2 days
- **Trust-to-effort ratio:** 4.0 - 9.0 per day

**Secondary Answer:** Portfolio Greek Limits
- **Why:** Risk Manager + Conservative Income need it
- **Predicted Trust Impact Score:** 7-8 (from 2-3 personas)
- **Implementation:** Aggregate vega/gamma caps
- **System trust gain:** +0.10 (9.15 → 9.25)
- **Effort:** 2-3 weeks (14-21 days)
- **Trust-to-effort ratio:** 0.33 - 0.57 per day

**P2 Deferred:**
- Skew classification (useful but not urgent)
- Term structure diagnostic (optimization, not protection)
- Momentum × IV divergence (already have momentum_tag)

---

## Red Flags to Reject

If any persona suggests:
❌ "Lower IV requirement to 90 days" → Violates invariant
❌ "Add 'confident override' button" → Violates invariant
❌ "Show more STRUCTURALLY_READY as 'possible'" → Execution pressure
❌ "Smart sizing for blocked strategies" → Fallback execution
❌ "Relax score threshold during quiet periods" → Degraded quality

**Response:** "That suggestion conflicts with locked invariants. Can you suggest something that increases transparency or adds protection without changing acceptance criteria?"

---

## Post-Survey Action Plan

### If Earnings Gate wins:
1. Implement hard gate in Step 12: `acceptance_status = 'WAIT'` if `days_to_earnings < 7`
2. Add earnings calendar data source (Schwab API or static lookup)
3. Display diagnostic: "Blocked: Earnings in 3 days (high binary risk)"
4. Test with recent earnings events (validate block worked)
5. Document in acceptance semantics

### If Portfolio Greeks wins:
1. Design Greek aggregation logic (sum vega/gamma across READY_NOW)
2. Set conservative limits (e.g., max_portfolio_vega = 50, max_gamma = 5)
3. Add portfolio limit gate in Step 12
4. Display diagnostics: "Portfolio vega: 45/50 (90% capacity)"
5. Test with multi-ticker READY_NOW scenarios

### If Both win:
Implement in sequence:
1. Earnings gate first (quick win, immediate trust gain)
2. Portfolio Greeks second (complex, higher trust ceiling)

---

## Success Criteria

**Step 3 is successful if:**
- ✅ All 4 personas answered independently
- ✅ Consensus emerged naturally (2+ personas same answer)
- ✅ Winning enhancement passes invariant checklist
- ✅ Trust-to-effort ratio is favorable (>0.01 per day)
- ✅ No feature creep pressure ("just add this one more thing")
- ✅ Clear P1/P2 boundary established

**Step 3 fails if:**
- ❌ Personas suggest relaxing gates
- ❌ Consensus is "add more indicators"
- ❌ Winning enhancement increases trade frequency
- ❌ Multiple conflicting "must-haves" (no clear priority)

---

## Timing

**Execute Step 3:** Now (questionnaire ready)  
**Response window:** 1-2 hours (independent responses)  
**Analysis:** 30 minutes (tally + evaluate)  
**Decision:** 15 minutes (select P1 winner)  
**Implementation:** 1 day - 3 weeks (depending on winner)

---

## Final Note

This questionnaire is designed to **confirm existing priorities**, not discover new ones.

We already know the likely answers:
- Earnings gate (quick, universal)
- Portfolio Greeks (complex, essential)

Step 3 validates that personas agree, then locks P1 scope permanently.

**After Step 3:** No more feature discussions until P1 complete and production-validated.

---

**Status:** 📋 Draft ready for review  
**Next:** Sharpen question if needed, then execute survey
