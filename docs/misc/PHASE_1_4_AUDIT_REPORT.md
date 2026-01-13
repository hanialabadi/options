|------------------|-----------|
| **Greeks (Gamma/Vega/Delta/Theta)** | ✅ **YES** | None | N/A | Current Greeks captured every snapshot. Can plot `Gamma[t] - Gamma[t-1]` |
| **Price (Underlying)** | ✅ **YES** | None | N/A | `UL Last` captured. Can compute `UL_Drift = UL_Last - Underlying_Price_Entry` |
| **Price (Moneyness)** | ✅ **YES** | None | N/A | `Moneyness_Pct` recomputed each snapshot. Can track moneyness migration |
| **IV (Implied Vol)** | ✅ **YES** | None | N/A | `IV Mid` captured. Can track `IV_Drift = IV_Mid[t] - IV_Mid[t-1]` |
| **IV Rank** | ❌ **NO** | `IV_Rank` field | Phase 3 | Cannot observe IV percentile rank drift (not captured) |
| **Time (DTE)** | ⚠️ **PARTIAL** | `DTE` field | Phase 3 | Can INFER from `(Expiration - TODAY).days`, but not explicit. Should add `DTE` column for consistency |
| **Capital (Deployed)** | ❌ **NO** | `Capital_Deployed` field | Phase 1 or 3 | Cannot observe capital exposure drift (not captured) |
| **Earnings Proximity** | ❌ **NO** | `Days_to_Earnings` field | Phase 3 | Cannot observe proximity change (not captured) |
| **Trade-Level Greeks** | ❌ **NO** | `*_Trade` fields | Phase 3 | Cannot observe trade-level net exposure drift (leg-level only) |

### B. Recommendations

**Add to Phase 3 (Pre-Freeze):**
1. `DTE = (Expiration - pd.Timestamp.now()).days` — Explicit time to expiration
2. `IV_Rank = compute_iv_rank(IV_Mid, symbol, lookback=252)` — IV percentile rank (0-100)
3. `Days_to_Earnings = fetch_earnings_proximity(symbol, Expiration)` — Days to next earnings
4. `Capital_Deployed = extract_or_estimate_margin(row)` — Broker "Margin Required" or fallback calculation
5. `Delta_Trade`, `Gamma_Trade`, `Theta_Trade`, `Vega_Trade`, `Premium_Trade` — Trade-level aggregates (`groupby('TradeID').sum()`)

**Why Pre-Freeze?**
- These fields are OBSERVABLE NOW (current market state)
- Phase 6 will freeze them as `DTE_Entry`, `IV_Rank_Entry`, etc.
- Without them, cannot assess drift readiness or validate freeze semantics

---

## 4️⃣ CHART ENGINE ROLE VALIDATION

### A. Chart Engine Architecture

**File:** `core/chart_engine.py`

**Components:**
- **Candlestick Patterns:** ShootingStar, Doji, Hammer, Engulfing, Harami
- **Trend Indicators:** EMA9, EMA21, SMA20, SMA50, Overextension detection
- **Momentum:** RSI, MACD, Bollinger Bands, CCI, ADX, MFI
- **Volume:** OBV, ATR

**Outputs:**
- `Chart_Tags` (list of pattern names)
- `Chart_Trend` ("Bullish" / "Bearish" / "Neutral")
- `Chart_CompositeScore` (0-100 directional strength)
- `Chart_Support` / `Chart_Resistance` (price levels)

### B. Separation Assessment

| **Criteria** | **Status** | **Evidence** |
|--------------|------------|--------------|
| **Chart signals computed separately?** | ✅ YES | `chart_engine.py` is standalone module |
| **Chart mutates trade state?** | ✅ NO | Chart functions return new columns only, do not modify identity/Greeks |
| **Chart influences PCS computation?** | ✅ NO (within Phase 3) | PCS formula uses only Gamma/Vega/ROI. No chart inputs in `calculate_pcs()` |
| **Chart used for exit logic (Phase 7+)?** | ⚠️ YES (downstream) | Found in rec engine: `PCS_UnifiedScore = 0.5*PCS + 0.3*SignalScore + 0.2*Chart` (Phase 7 concern) |

### C. Verdict

**Within Phases 1-4: ✅ CLEAN**
- Chart engine does NOT contaminate snapshot state
- Chart outputs are CONTEXTUAL ONLY
- No chart signals affect identity, Greeks, structure, or capital

**Downstream Concern (Phase 7+):**
- Recommendation engines MIX chart signals into PCS scoring
- Found: `Chart_Support` influences `Needs_Revalidation` flags
- This violates PCS semantic purity (PCS should measure POSITION quality, not MARKET context)

**Recommendation:**
- Keep chart_engine in Phase 3 (observation layer)
- DO NOT mix chart signals into PCS (use separately in Phase 9 exit logic)
- PCS_Entry = setup quality (immutable)
- PCS_Active = behavior vs design (Phase 8)
- Chart = market context for exit timing (Phase 9)

---

## 5️⃣ PCS PLACEMENT AUDIT

### A. Current PCS Implementation

**File:** `core/phase3_enrich/pcs_score.py`
**Function:** `calculate_pcs(df: pd.DataFrame) -> pd.DataFrame`

**Inputs:**
- `Gamma` (current absolute)
- `Vega` (current absolute)
- `Premium` (current)
- `Basis` (entry cost)
- Derived: `ROI = (Premium / Basis) * 100`

**Formula:**
```python
gamma_score = min(Gamma * 1000, 25)
vega_score = min((Vega / Basis) * 100, 25)
roi_score = 15 if ROI >= 3 else 10 if ROI >= 2 else 5

# Profile-based weighting
if Profile == "Neutral Vol":
    PCS = 0.40 * vega_score + 0.35 * gamma_score + 0.25 * roi_score
elif Profile == "Income":
    PCS = 0.50 * roi_score + 0.30 * vega_score + 0.20 * gamma_score
# ... etc
```

**Tier Assignment:**
- PCS >= 80 → Tier 1 (High Quality)
- PCS >= 70 → Tier 2 (Good)
- PCS >= 60 → Tier 3 (Acceptable)
- PCS < 60 → Tier 4 (Low Quality)

### B. Semantic Assessment

**Question:** What does current PCS measure?

**Answer:** PCS measures **"Current Snapshot Quality"** using absolute Greeks.

**Semantic Interpretation:**
- High Gamma NOW → High score
- High Vega NOW → High score
- High ROI NOW → High score

**Problem:** Absolute Greeks decay naturally over time due to:
- **Theta burn** (time decay reduces extrinsic value)
- **Vega decay** (vol sensitivity drops as expiration approaches)
- **Gamma peak** (Gamma peaks at ATM near expiration, then collapses)

**Consequence:**
- Same trade with Gamma=0.15 at entry will have Gamma=0.08 after 20 days (natural decay)
- Current PCS would DROP even if trade is behaving EXACTLY as designed
- Fixed thresholds (60/70/80) penalize aging positions unfairly

### C. What PCS Should NOT Be Used For

❌ **Active Trade Management Decisions**
- "Should I trim this position?" → Requires DRIFT from entry, not absolute values
- "Is this trade behaving as expected?" → Requires comparison to entry baseline

❌ **Exit Recommendations**
- PCS measures position quality, NOT exit timing
- Exit decisions need: Chart context + PCS_Active drift + Days_Held + Moneyness migration

❌ **Drift Detection**
- Cannot detect "Gamma exploded beyond design" without entry baseline
- Cannot detect "Vega collapsed unexpectedly" without IV_Rank_Entry

### D. What PCS SHOULD Be Used For (Current Snapshot-Only Context)

✅ **Position Quality Scoring at Snapshot Time**
- "Among positions TODAY, which are highest quality?"
- Relative ranking: Sort by PCS, identify outliers

✅ **ML Training Labels (Once frozen as PCS_Entry)**
- "Which entry characteristics predict success?"
- Features: Gamma_Entry, Vega_Entry, IV_Rank_Entry, DTE_Entry, Moneyness_Entry
- Label: Did position hit profit target?

✅ **Comparative Analysis**
- Compare PCS_Entry across strategies: "Which strategy setups score best?"

### E. Verdict

**PCS Placement: Phase 3 ✅ (Correct phase)**  
**PCS Semantics: SNAPSHOT-ONLY ✅ (Measuring current state)**  
**PCS Limitations: CANNOT support active management without entry baseline ⚠️**

**Critical Insight:**
> Current PCS answers: "How good is this position RIGHT NOW given current Greeks?"  
> It does NOT answer: "How is this position BEHAVING relative to entry design?"

**Forward Path (Phase 6+):**
1. **Phase 3**: Keep PCS as-is (current snapshot quality)
2. **Phase 6**: Freeze entry Greeks → Create `PCS_Entry` (immutable setup quality)
3. **Phase 7**: Calculate drift metrics (`Gamma_Drift`, `Vega_Drift`)
4. **Phase 8**: Create `PCS_Active` (uses drift, NOT absolutes)
5. **Phase 9**: Exit logic uses `PCS_Active` + Chart context (separate from PCS_Entry)

---

## 6️⃣ SNAPSHOT DETERMINISM TEST

### A. Test Scenario

**Simulation:** Same CSV file (`schwab_positions_2025_01_03.csv`) ingested twice on different days.

**Question:** Will Phases 1-4 produce identical output?

### B. Determinism Analysis

| **Component** | **Deterministic?** | **Reason** |
|---------------|-------------------|------------|
| Phase 1 (broker fields) | ✅ YES | Raw CSV parsing (same input → same output) |
| Phase 2 (identity) | ✅ YES | TradeID, LegID, Strategy computed deterministically |
| Phase 2 (`Entry_Date`) | ❌ **NO** | Uses `pd.Timestamp.now()` → changes every run |
| Phase 2 (entry freezes) | ✅ YES | `Strike_Entry`, `Expiration_Entry`, `Underlying_Price_Entry` frozen from CSV |
| Phase 3 (enrichment) | ✅ YES | BreakEven, Moneyness, PCS calculated from deterministic inputs |
| Phase 4 (metadata) | ⚠️ EXPECTED NON-DETERMINISM | `Snapshot_TS`, `run_id` = current timestamp (intentional metadata) |

### C. Determinism Verdict

**FAILS (Due to Entry_Date)**

**Critical Non-Determinism:**
- `Entry_Date = pd.Timestamp.now()` in Phase 2
- Same position ingested on 2025-01-03 and 2025-01-10 will have different `Entry_Date` values

**Impact:**
- Historical replay not possible (Entry_Date changes every reprocess)
- Time-series analysis corrupted (cannot anchor to true entry time)

**Acceptable Non-Determinism:**
- `Snapshot_TS` and `run_id` (Phase 4 metadata) — These are INTENTIONAL observation timestamps
- Market data (`Last`, `UL Last`, `IV Mid`) — Time-sensitive by nature, expected to change

### D. Recommendation

**Fix Entry_Date Semantics (Phase 2):**

**Option 1: Broker-Provided Entry Date**
```python
if "Entry_Date" in df.columns:
    df["Entry_Date"] = pd.to_datetime(df["Entry_Date"])
else:
    df["Entry_Date"] = pd.Timestamp.now()  # Fallback only
```

**Option 2: First Snapshot Timestamp (Phase 4)**
```python
# Phase 4: Track first seen date per TradeID
if TradeID not in history:
    history[TradeID] = {"First_Seen_Date": Snapshot_TS}
df["Entry_Date"] = df["TradeID"].map(lambda tid: history[tid]["First_Seen_Date"])
```

**Recommended:** Option 2 (Phase 4 tracking)
- Makes determinism explicit
- Entry_Date = timestamp of FIRST snapshot containing TradeID
- Preserves historical truth
- Replay-safe

---

## 7️⃣ SAFE RECOMMENDATIONS (Phase # Only, No Code)

### A. Immediate Fixes (Pre-Phase 6)

**Phase 2:**
1. Fix `Entry_Date` determinism (use broker-provided date or first-seen tracking)

**Phase 3:**
2. Add `DTE` column: `(Expiration - pd.Timestamp.now()).days`
3. Add `IV_Rank` column: Compute IV percentile rank (0-100) from historical IV
4. Add `Days_to_Earnings` column: Fetch earnings calendar proximity
5. Add `Capital_Deployed` column: Extract from broker "Margin Required" or calculate from Greeks
6. Add trade-level aggregates: `Delta_Trade`, `Gamma_Trade`, `Theta_Trade`, `Vega_Trade`, `Premium_Trade` (`groupby('TradeID').sum()`)

**Phase 4:**
7. Add `First_Seen_Date` metadata: Track first snapshot timestamp per TradeID

### B. Phase 6 Readiness

**Before implementing Phase 6 freeze logic:**
1. ✅ Complete Phase 3 additions (DTE, IV_Rank, Days_to_Earnings, Capital_Deployed, Trade aggregates)
2. ✅ Validate drift observability (run 2+ snapshots, confirm can plot Gamma drift)
3. ✅ Fix Entry_Date determinism (ensure consistent entry timestamps)
4. ✅ Confirm schema stability (no breaking changes to Phase 1-4 outputs)

**Then Phase 6 can freeze:**
- Identity: TradeID, LegID, Strategy, Structure ✅ (already frozen)
- Capital: Capital_Deployed_Entry, Premium_Entry, Premium_Trade_Entry
- Risk: Delta_Entry, Gamma_Entry, Theta_Entry, Vega_Entry (leg + trade), IV_Entry, IV_Rank_Entry
- Time: DTE_Entry, Earnings_Proximity_Entry
- Price: Underlying_Price_Entry ✅, Moneyness_Entry, BreakEven_Entry

### C. PCS Evolution Path

**Phase 3 (Current):**
- Keep `PCS` as-is (snapshot quality using current Greeks)
- Add missing observables (DTE, IV_Rank, Capital_Deployed)

**Phase 6 (Entry Freeze):**
- Create `PCS_Entry` = `calculate_pcs()` using `*_Entry` fields (frozen)
- Keep `PCS` for current snapshot comparison (optional)

**Phase 7 (Drift Calculation):**
- Compute drift metrics: `Gamma_Drift`, `Vega_Drift`, `Theta_Burn_Ratio`, `Moneyness_Drift`, `Days_Held`

**Phase 8 (Active Scoring):**
- Create `PCS_Active` using drift metrics (NOT absolutes)
- Inputs: Gamma_Drift, Vega_Drift, Theta_Burn_Ratio, DTE_Remaining, Moneyness_Drift
- Purpose: "Is this trade behaving as designed?"

**Phase 9 (Exit Logic):**
- Use `PCS_Active` + `Chart_Context` for trim/hold/revalidate recommendations
- Keep PCS_Entry separate (immutable ML training label)

---

## 8️⃣ AUDIT SUMMARY SCORECARD

| **Audit Dimension** | **Grade** | **Key Finding** |
|---------------------|-----------|-----------------|
| **Schema Completeness** | B+ (70%) | Core fields present. Missing: IV_Rank, DTE, Earnings, Capital, Trade aggregates |
| **Phase Boundary Integrity** | A- | Mostly stateless. One violation: `Entry_Date` non-determinism |
| **Drift Observability** | B | Ready for Greeks/Price drift. NOT ready for Vol/Capital drift |
| **Chart Engine Separation** | A | Clean within Phase 1-4. Downstream contamination noted (Phase 7+ concern) |
| **PCS Semantic Fitness** | B+ | Correctly placed in Phase 3. Semantically measures "current quality" not "setup quality." Needs split for active management |
| **Snapshot Determinism** | C | Fails due to `Entry_Date = pd.Timestamp.now()`. Easy fix |

**Overall Assessment: B+ (Structurally Sound, Minor Gaps)**

---

## 9️⃣ PHASE 1-4 READINESS FOR PHASE 6

### ✅ READY (Can Proceed)
- Identity fields complete (TradeID, LegID, Strategy, Structure)
- Entry freeze foundation established (Strike_Entry, Expiration_Entry, Underlying_Price_Entry)
- Phase separation discipline maintained (Phases 1-4 are stateless)
- Schema versioning implemented (Phase 4 Schema_Hash)
- PCS semantic clarity achieved (snapshot quality, not setup quality)

### ⚠️ BLOCKERS (Must Fix Before Phase 6)
1. **Entry_Date determinism** (Phase 2 fix required)
2. **Missing observables** (DTE, IV_Rank, Days_to_Earnings, Capital_Deployed, Trade aggregates)
3. **Drift validation** (run 2+ snapshots, confirm drift computation works)

### 📋 Pre-Phase 6 Checklist
- [ ] Fix `Entry_Date` to use broker-provided date or first-seen tracking
- [ ] Add `DTE` column (Phase 3)
- [ ] Add `IV_Rank` column (Phase 3)
- [ ] Add `Days_to_Earnings` column (Phase 3)
- [ ] Add `Capital_Deployed` column (Phase 1 or 3)
- [ ] Add trade-level Greek aggregates: `*_Trade` (Phase 3)
- [ ] Run 2 consecutive snapshots and validate drift computation
- [ ] Document PCS split architecture (PCS_Entry vs PCS_Active)

**Estimated Effort:** 2-3 days (assuming IV_Rank and Earnings data sources available)

---

## 🔟 FINAL VERDICT

**Phase 1-4 Perception Snapshot Quality: ACCEPTABLE WITH MINOR FIXES**

**Strengths:**
- ✅ Solid identity foundation (TradeID, LegID, Strategy)
- ✅ Clean phase boundaries (mostly stateless)
- ✅ Chart engine properly separated
- ✅ PCS semantically coherent (snapshot quality scoring)
- ✅ Schema versioning implemented
- ✅ Deterministic enrichment (BreakEven, Moneyness, PCS)

**Weaknesses:**
- ⚠️ Entry_Date non-determinism (easy fix)
- ⚠️ Missing 6 observable fields (DTE, IV_Rank, Earnings, Capital, Trade aggregates)
- ⚠️ PCS cannot support active management without entry baseline (requires Phase 6+ split)

**Confidence in Phase 6 Readiness: 75%**

**Action Required:**
1. Address 3 blockers (Entry_Date fix + 6 missing observables + drift validation)
2. Document PCS split architecture
3. Re-run audit after fixes to confirm 100% readiness

---

**End of Phase 1-4 Auditor Report**
**Submitted:** January 4, 2026  
**Next Step:** Review findings, address blockers, then proceed to Phase 6 implementation.
