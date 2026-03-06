# Execution Semantics & Escalation Eligibility

## RAG-Defined Execution Contract

This document formalizes the distinction between "valid execution" and "escalation eligibility" to prevent conflation during testing and audit.

---

## 1. Valid Execution (Scan Completion)

**Definition**: A scan that completes all pipeline steps with fresh, complete foundational data.

**Requirements** (RAG Sources):
- **Fresh Schwab Data**: Snapshot timestamp <48 hours old
  - Source: User requirement (conversation), ARCHITECTURE_ROADMAP.md
- **Complete Foundational Data**: Price, basic Greeks, chart signals
  - Source: `scan_engine/step2_load_snapshot.py`, `scan_engine/step3_chart_signals.py`
- **Pipeline Completion**: All steps (0-12) execute without critical errors
  - Source: `scan_engine/pipeline.py`

**Outcome**:
- Audit artifacts generated (`audit_steps/*.csv`, `audit_trace/*.csv`)
- Step outputs persisted
- Execution completes with status summary

**Does NOT Guarantee**:
- ❌ Strategies reaching `READY` status
- ❌ Contracts passing liquidity filters
- ❌ Fidelity IV escalation triggering

**Example**: The 2026-02-02 21:30 scan with TSLA/NVDA/COIN/PLTR was a **valid execution** (fresh data, complete pipeline) but produced zero `READY` strategies.

---

## 2. Escalation Eligibility (Fidelity IV Requirements)

**Definition**: A strategy that has passed all prerequisite gates to qualify for Fidelity long-term IV enrichment.

**Prerequisites** (RAG Sources):
1. **Pass Contract Selection** (Step 9B)
   - `Contract_Status IN ['OK', 'LEAP_FALLBACK']`
   - Source: `scan_engine/step12_acceptance.py:522`

2. **Pass Execution Gate Pre-Filter** (Step 12)
   - Successfully selected contracts reach gate evaluation
   - Source: `scan_engine/step12_acceptance.py:556`

3. **Meet Escalation Rules** (Step 12 Gate Logic)
   - **R0.3**: `strategy_type == 'INCOME'` → `IV_Fidelity_Required = True`
   - **R0.4**: `strategy_type == 'DIRECTIONAL' AND iv_maturity_state == 'MATURE'` → `IV_Fidelity_Required = False`
   - **R0.5**: Default → `IV_Fidelity_Required = False`
   - Source: `scan_engine/step12_acceptance.py:300-330`

4. **Reach Escalation Trigger** (Pipeline Stage 2)
   - `Trade_Status == 'AWAIT_CONFIRMATION'` AND `IV_Fidelity_Required == True`
   - Source: `scan_engine/pipeline.py:120-135`

**Outcome**:
- Ticker added to `fidelity_iv_demand_tickers.csv`
- DuckDB queried for existing Fidelity IV history
- If stale/missing: Manual scraper invocation required

**Blocking Factors** (RAG-Verified):
- ❌ Liquidity filter failure (Step 9B) → Contract never reaches gate
- ❌ `strategy_type != 'INCOME'` → Gate sets `IV_Fidelity_Required = False`
- ❌ `iv_maturity_state == 'MATURE'` for DIRECTIONAL → No Fidelity needed

---

## 3. Gating Sequence (RAG-Verified Flow)

```
Step 0: Generate/Load Snapshot
  ↓ (fresh data <48h)
Step 2: Load & Validate Snapshot
  ↓ (IVHV gap filter)
Step 3: Chart Signals
  ↓ (technical analysis)
Step 5: Strategy Generation
  ↓ (multi-strategy ledger)
Step 9B: Contract Selection
  ├─ LIQUIDITY FILTER (OI, spread, depth)
  │  ├─ PASS → Contract_Status = 'OK' or 'LEAP_FALLBACK'
  │  └─ FAIL → Contract_Status = 'FAILED_LIQUIDITY_FILTER'
  ↓
Step 12: Acceptance Gate
  ├─ PRE-FILTER (Contract_Status validation)
  │  ├─ IF Contract_Status NOT IN ['OK', 'LEAP_FALLBACK']:
  │  │    → Trade_Status = 'BLOCKED'
  │  │    → IV_Fidelity_Required = False
  │  │    → SKIP EXECUTION GATE
  │  └─ ELSE: Proceed to Execution Gate
  │
  ├─ EXECUTION GATE (R0.1 - R0.5 rules)
  │  ├─ R0.3: INCOME → IV_Fidelity_Required = True, Trade_Status = 'AWAIT_CONFIRMATION'
  │  ├─ R0.4: DIRECTIONAL + MATURE IV → IV_Fidelity_Required = False, Trade_Status = 'AWAIT_CONFIRMATION'
  │  └─ R0.5: Default → IV_Fidelity_Required = False, Trade_Status = 'AWAIT_CONFIRMATION'
  ↓
Pipeline Stage 2: Fidelity IV Enrichment
  ├─ Filter: Trade_Status == 'AWAIT_CONFIRMATION' AND IV_Fidelity_Required == True
  ├─ IF tickers_to_fetch NOT EMPTY:
  │    → Emit fidelity_iv_demand_tickers.csv
  │    → Query DuckDB for cached Fidelity IV
  └─ ELSE:
       → Skip Stage 2 (no eligible tickers)
```

---

## 4. Common Scenarios & Expected Outcomes

### Scenario 1: Fresh Scan, All Contracts Pass Liquidity, Income Strategy
- **Execution**: Valid ✅
- **Escalation**: Eligible ✅ (R0.3 triggers)
- **Outcome**: Fidelity demand file emitted for INCOME strategies

### Scenario 2: Fresh Scan, All Contracts Fail Liquidity
- **Execution**: Valid ✅ (pipeline completes)
- **Escalation**: NOT Eligible ❌ (blocked at Step 9B pre-filter)
- **Outcome**: No Fidelity demand file (correct behavior)
- **Example**: 2026-02-02 21:30 scan (TSLA/NVDA/COIN/PLTR)

### Scenario 3: Stale Snapshot (>48h)
- **Execution**: NOT Valid ❌ (data freshness requirement violated)
- **Escalation**: NOT Applicable (invalid execution)
- **Outcome**: Scan rejected (not qualifying)

### Scenario 4: Fresh Scan, Directional Strategy, MATURE Schwab IV
- **Execution**: Valid ✅
- **Escalation**: NOT Eligible ❌ (R0.4: Schwab IV sufficient)
- **Outcome**: No Fidelity demand (by design)

---

## 5. Audit Interpretation Guidelines

**When reviewing audit artifacts**:

1. **Check Execution Validity First**:
   - `audit_trace/step2_output.csv`: Verify `Snapshot_Age_Hours < 48`
   - `audit_trace/step12_output.csv`: Verify all steps completed

2. **Then Check Escalation Eligibility**:
   - `audit_trace/step12_output.csv`: Check `Contract_Status` values
   - If all `FAILED_LIQUIDITY_FILTER` → Escalation correctly NOT triggered
   - If any `OK` → Check `IV_Fidelity_Required` and `Trade_Status`

3. **Verify Escalation Outcome**:
   - `output/fidelity_iv_demand_tickers.csv`: Should exist only if eligible tickers found
   - Pipeline logs: "No candidates require Fidelity Long-Term IV" → Correct if no INCOME or immature DIRECTIONAL passed liquidity

**Do NOT expect**:
- ❌ Escalation to trigger simply because IV Rank is missing
- ❌ Liquidity failures to be "overridden" for escalation testing
- ❌ Fresh data to guarantee any specific number of READY strategies

---

## 6. RAG Sources (Authoritative References)

- **Fresh Data Requirement**: User correction (conversation summary), ARCHITECTURE_ROADMAP.md
- **Contract Selection**: `scan_engine/step9b_fetch_contracts_schwab.py`
- **Pre-Filter Logic**: `scan_engine/step12_acceptance.py:522-544`
- **Execution Gate Rules**: `scan_engine/step12_acceptance.py:300-330`
- **Escalation Trigger**: `scan_engine/pipeline.py:120-153`
- **Two-Stage Gate**: ARCHITECTURE_ROADMAP.md (Scan Engine definition)

---

**Last Updated**: 2026-02-02
**Purpose**: Formalize execution vs escalation semantics per RAG-defined contract
