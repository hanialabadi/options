# Acceptance Semantics - Frozen Invariants

**Status:** 🔒 **PERMANENTLY LOCKED**  
**Date:** 2026-01-03  
**Purpose:** Protect conservative discipline from future erosion

---

## Why This Document Exists

As systems evolve, there's pressure to:
- "Just this once, let it execute without full context"
- "Lower the threshold to get more trades"
- "Add a smart fallback for edge cases"

**This document says NO to all of that.**

These invariants are **non-negotiable**. If future requirements conflict with these rules, the answer is "change the requirement" - never "change the rule."

---

## The Four Unbreakable Rules

### Rule 1: STRUCTURALLY_READY Never Auto-Executes

**Statement:**
> The system will never automatically promote STRUCTURALLY_READY to READY_NOW or execute STRUCTURALLY_READY strategies under any circumstances.

**What This Means:**
- STRUCTURALLY_READY is visible to user (transparency)
- STRUCTURALLY_READY never reaches execution engine (safety)
- User can download CSV and execute via broker (control)
- System only promotes when IV history ≥ 120 days (maturation)

**Why This Exists:**
- Prevents "execute without context" feature creep
- Forces discipline: wait for data OR do external research
- Protects system reputation (never executes blind)
- User takes full responsibility for overrides

**Examples of Forbidden Changes:**
❌ "Add 'Execute Anyway' button for STRUCTURALLY_READY"  
❌ "Allow STRUCTURALLY_READY if user checks a checkbox"  
❌ "Execute STRUCTURALLY_READY at reduced size"  
❌ "Promote to READY_NOW if 2/3 conditions met"  

**The Only Exception:**
✅ Automatic promotion when IV history reaches 120 days (maturation rule)

---

### Rule 2: READY_NOW Means Fully Evaluated

**Statement:**
> READY_NOW requires ALL context available: acceptance rules passed + score ≥ 60 + IV Rank available + no market stress.

**What This Means:**
- Can't be "mostly ready" or "ready with caution"
- All four gates must pass:
  1. Acceptance rules (structure + bias)
  2. Theory compliance (score ≥ 60)
  3. IV availability (120+ days history)
  4. Market stress (median IV < 40)
- No partial status, no gradients

**Why This Exists:**
- READY_NOW is a promise: "System confident in this recommendation"
- User trusts READY_NOW means "fully vetted"
- Any degradation erodes trust

**Examples of Forbidden Changes:**
❌ "READY_NOW_WITH_CAUTION status for edge cases"  
❌ "Lower score threshold to 50 during low-trade periods"  
❌ "Skip IV check if user acknowledges risk"  
❌ "Allow READY_NOW during YELLOW market stress"  

**No Exceptions:** This rule has zero exceptions.

---

### Rule 3: Downgrade Gates Are One-Way

**Statement:**
> Acceptance status can only move down the hierarchy, never up. Once downgraded, only maturation (not logic) can promote.

**Hierarchy (High to Low):**
```
READY_NOW             ← Fully evaluated, executable
    ↓ (downgrade only)
STRUCTURALLY_READY    ← Good structure, awaiting context
    ↓
WAIT                  ← Good structure, timing not ideal
    ↓
AVOID                 ← Failed acceptance rules
    ↓
INCOMPLETE            ← Missing data / validation failed
    ↓
HALTED_MARKET_STRESS  ← Market stress active
```

**What This Means:**
- Evaluation completeness gate: READY_NOW → STRUCTURALLY_READY if score < 60
- IV availability gate: READY_NOW → STRUCTURALLY_READY if IV unavailable
- Market stress gate: READY_NOW → HALTED_MARKET_STRESS if median IV ≥ 40
- Earnings proximity gate: READY_NOW → WAIT_EARNINGS if earnings within 7 days
- No upgrade gates exist (no WAIT → READY_NOW shortcuts)

**Why This Exists:**
- Prevents silent fallbacks ("it's close enough, promote it")
- Maintains conservative discipline
- Makes system behavior predictable and auditable
- Protects against binary event risk (earnings IV collapse)

**Examples of Forbidden Changes:**
❌ "Upgrade WAIT to READY_NOW if chart signal strong"  
❌ "Promote STRUCTURALLY_READY if user requests it"  
❌ "Add bypass gate for high-confidence strategies"  
❌ "Smart promotion during favorable market conditions"  
❌ "Relax earnings gate to 3 days instead of 7"  
❌ "Allow earnings trades if IV is 'high enough'"  

**The Only Exception:**
✅ Maturation: STRUCTURALLY_READY → READY_NOW when IV history ≥ 120 days (automatic, next pipeline run)

---

### Rule 4: No Fallback Execution

**Statement:**
> Blocked means blocked. No workarounds, no reduced-size execution, no "smart" alternatives.

**What This Means:**
- STRUCTURALLY_READY: Blocked (no execution)
- WAIT: Blocked (no execution)
- WAIT_EARNINGS: Blocked (no execution)
- AVOID: Blocked (no execution)
- INCOMPLETE: Blocked (no execution)
- HALTED_MARKET_STRESS: Blocked (no execution)
- Only READY_NOW executes

**Why This Exists:**
- One execution path = simple, auditable, trustworthy
- Multiple paths = erosion of discipline over time
- "Smart fallbacks" = complexity without benefit
- Binary events (earnings) create tail risk IV can't measure

**Examples of Forbidden Changes:**
❌ "Execute STRUCTURALLY_READY at 50% size"  
❌ "Execute WAIT strategies if chart signal very strong"  
❌ "Smart sizing: execute with extra caution flags"  
❌ "Emergency execution mode during user request"  
❌ "Execute if 4/5 criteria met (close enough)"  

**No Exceptions:** This rule has zero exceptions.

---

### Rule 5: Earnings Proximity Gate (P1 Guardrail)

**Statement:**
> No trades within 7 days of earnings. Binary events create tail risk that IV measurements cannot capture.

**What This Means:**
- Earnings gate: READY_NOW → WAIT_EARNINGS if 0 ≤ days_to_earnings ≤ 7
- Hard block (not warning, not reduced size)
- Applies to ALL strategies regardless of confidence
- Calendar-based protection (not volatility-based)

**Why This Exists:**
- Earnings create binary outcomes (beat/miss)
- IV collapses immediately post-earnings (realized vol ≠ IV premium)
- Gap risk cannot be hedged with volatility positioning
- Protects against "technically perfect setup, wrong timing" regret

**Examples of Forbidden Changes:**
❌ "Reduce threshold to 3 days for high IV"  
❌ "Allow trades if earnings 'priced in'"  
❌ "Smart sizing based on earnings uncertainty"  
❌ "Execute if user acknowledges risk"  
❌ "Calendar override button"  

**The Only Exception:**
✅ None. 7-day threshold is permanent. If earnings date unknown, trade allowed (conservative: block known risk, allow unknown).

---

## Non-Goals: What This System Will Never Do

Document this to protect future development from well-intentioned erosion:

### Execution Workarounds

❌ **Auto-execute STRUCTURALLY_READY**  
   *Reason:* Violates Rule 1. User controls override via external analysis.

❌ **Smart sizing for blocked strategies**  
   *Reason:* Violates Rule 4. Blocked means blocked at any size.

❌ **Partial execution modes**  
   *Reason:* Violates Rule 4. One path only.

### Data Compromises

❌ **Lower IV history requirement**  
   *Reason:* Violates Rule 2. IV Rank needs 120+ days lookback.

❌ **Use external data to fake IV continuity**  
   *Reason:* Data integrity > convenience. Wait for real accumulation.

❌ **Skip IV check if "close enough"**  
   *Reason:* Violates Rule 2. Close enough = not sufficient.

### Threshold Erosion

❌ **Lower Theory_Compliance_Score threshold**  
   *Reason:* Violates Rule 2. 60 is the validated minimum.

❌ **Adjust market stress threshold upward**  
   *Reason:* Violates Rule 2. Median IV ≥ 40 is evidence-based.

❌ **Reduce earnings proximity threshold (<7 days)**  
   *Reason:* Violates Rule 5. Binary risk requires wide protective buffer.

❌ **Add "relaxed mode" for low-trade periods**  
   *Reason:* Violates all rules. Trade count is not a success metric.

### Feature Creep

❌ **"Execute Anyway" override button**  
   *Reason:* Violates Rule 1. User exports CSV if override needed.

❌ **Upgrade gates (WAIT → READY_NOW)**  
   *Reason:* Violates Rule 3. Only downgrade gates exist.

❌ **Emergency execution mode**  
   *Reason:* Violates Rule 4. No execution workarounds.

### Optimization Pressure

❌ **Optimize for higher trade frequency**  
   *Reason:* Trust > output. Few good trades > many questionable trades.

❌ **Add more indicators to boost scores**  
   *Reason:* Complexity ≠ accuracy. Current signals are sufficient.

❌ **User satisfaction via execution**  
   *Reason:* Satisfaction comes from trust, not trade count.

---

## How to Handle Future Requests

### Request: "Users want more trades"

**Wrong Response:** Lower thresholds, add fallbacks  
**Right Response:** Educate on STRUCTURALLY_READY visibility, track maturation timeline

### Request: "Can we execute STRUCTURALLY_READY at reduced size?"

**Wrong Response:** Add sizing logic for blocked strategies  
**Right Response:** No. User downloads CSV, analyzes externally, executes via broker.

### Request: "IV requirement is too strict, most strategies blocked"

**Wrong Response:** Lower to 90 days, or use external IV data  
**Right Response:** This is expected. Accumulate data over 116 days. Show timeline.

### Request: "Add override button for experienced traders"

**Wrong Response:** Add checkbox to bypass IV requirement  
**Right Response:** Download CSV feature already exists. That's the override.

### Request: "Market stress threshold too low, missing opportunities"

**Wrong Response:** Raise from 40 to 50  
**Right Response:** Median IV 40 = 95th percentile panic. Threshold is evidence-based.

---

## Enforcement Checklist

Before implementing ANY change to Step 12 acceptance logic, verify:

- [ ] Does this change allow STRUCTURALLY_READY to auto-execute?  
      **If YES:** Rejected. Violates Rule 1.

- [ ] Does this change lower requirements for READY_NOW?  
      **If YES:** Rejected. Violates Rule 2.

- [ ] Does this change add upgrade gates?  
      **If YES:** Rejected. Violates Rule 3.

- [ ] Does this change add fallback execution?  
      **If YES:** Rejected. Violates Rule 4.

- [ ] Is this change motivated by "increase trade count"?  
      **If YES:** Rejected. Wrong success metric.

- [ ] Is this change motivated by "user wants this"?  
      **If YES:** Question the user goal. Usually education solves it.

**If all checkboxes pass:** Change may be acceptable. Proceed with caution.

---

## Success Metrics (What We Optimize For)

### ✅ Trust Metrics (Good)
- User understands why trades blocked
- User knows when trades unblock
- User doesn't feel tempted to override blindly
- System reputation: "Says NO clearly, never lies"

### ✅ Quality Metrics (Good)
- READY_NOW strategies have full context
- No regret trades ("why did system let me execute that?")
- Downgrade gates protect user from incomplete evaluation

### ❌ Output Metrics (Ignore)
- Trade count per day
- Execution frequency
- "User satisfaction" via more trades
- Matching competitors' trade volume

---

## Final Statement

**This system is a decision system, not a trade generator.**

Its value comes from saying NO clearly when context is incomplete, not from maximizing output.

If future pressure arises to "just make one small exception," re-read this document. The answer is still NO.

**These rules are frozen. Permanently.**

---

**Status:** 🔒 Locked  
**Last Updated:** 2026-01-03  
**Next Review:** Never (rules are permanent)
