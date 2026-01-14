# Options Management Engine — Architecture Roadmap

Status: Active
Last Locked: 2026-01-14
Authority: RAG-validated (McMillan, Hull, Passarelli, Natenberg)

## 1. System Purpose

The Options Management Engine is designed to manage existing option positions through objective, data-driven analysis, separating observation, measurement, and decision into strictly isolated cycles.

The system explicitly does not:
- Predict markets
- Infer intent
- Optimize entries
- Reconstruct missing historical data

Its core objective is to provide audit-grade, emotion-free explanations and recommendations based on frozen historical truth and measured drift.

## 2. Cycle Architecture Overview

The engine operates as a three-cycle pipeline:
- Cycle 1 — Perception (Ledger of Truth)
- Cycle 2 — Drift (Time-Series Measurement)
- Cycle 3 — Decision (Action & Recommendation)

Each cycle:
- Owns its data
- May not mutate prior cycles
- May not infer missing historical state

## 3. Cycle 1 — Perception (LOCKED)

### 3.1 Purpose
Cycle 1 records the irreducible ground-zero state of broker-observed positions:
- What exists
- What it cost
- How it is mechanically sensitive to the market

It is a descriptive ledger, not an analytical engine.

### 3.2 Allowed Data (Frozen)

**A. Identity Anchors (Frozen Once — “Birth Certificate”)**
- Symbol (OCC string, canonical primary key)
- Account
- Quantity
- Basis
- Strike
- Expiration
- CallPut
- Type (Cash / Margin)

*RAG Authority: McMillan (contract identity)*

**B. Sensitivity Anchors (Frozen Per Snapshot — “Vital Signs”)**
- UL_Last
- Last
- Delta
- Gamma
- Vega
- Theta
- Rho
- AsOf_Timestamp

*RAG Authority: Passarelli / Natenberg (P&L attribution)*

### 3.3 Explicitly Forbidden Data (Never Persisted)
- Strategy labels
- IV / IV Rank / Skew
- DTE
- Earnings proximity
- Broker-calculated P&L ($G/L, %G/L)
- Risk judgments
- Liquidity metrics
- Chart signals
- Recommendations
- Rationale

**Rationale:**
Any field that:
- Compares timestamps
- Requires inference
- Introduces narrative
belongs to a later cycle.

### 3.4 Freezing Semantics
- Identity + Basis: Frozen once per TradeID
- Market Sensitivities: Frozen per snapshot
- No overwrites: Rehydration only
- No inference: Missing data remains NULL

### 3.5 Enforcement Mechanisms
- Ingest whitelist (hard-fail on unknown columns)
- Snapshot schema hash (detects phase creep)
- Immutable DuckDB append-only ledger
- Dashboard is strictly read-only

### 3.6 Status
🚫 **Cycle 1 is permanently LOCKED**
Any modification requires:
- Architecture audit
- Version bump
- Explicit roadmap update

## 4. Cycle 2 — Drift (DESIGN PENDING)

### 4.1 Purpose
Cycle 2 measures how positions change over time relative to their frozen Cycle 1 anchors.

It answers:
- What changed?
- Why did P&L change?

It does not recommend actions.

### 4.2 Inputs (Read-Only)
- All frozen Cycle 1 fields

### 4.3 Allowed Outputs (Mutable, Time-Series)
- Price drift
- Greek drift
- Attribution deltas (Δ, Γ, Θ, V)
- Time-indexed metrics

### 4.4 Explicit Non-Goals
- No freezing of Cycle 2 data
- No strategy interpretation
- No risk scoring
- No recommendations

### 4.5 Status
🟡 **Architecture Definition In Progress**
❌ Implementation forbidden until locked

## 5. Cycle 3 — Decision (FUTURE)

### 5.1 Purpose
Cycle 3 converts measured drift into actionable, rule-based recommendations.

Examples:
- Hold
- Trim
- Exit
- Hedge
- Roll

### 5.2 Constraints
- Must consume Cycle 2 outputs only
- Must never mutate historical data
- Must be explainable and rule-driven

## 6. Global Architecture Laws
- No cycle may mutate data owned by a prior cycle
- No inferred historical data may be persisted
- Observation ≠ Measurement ≠ Decision
- All freezes must be explicit
- All phase creep must hard-fail

## 7. RAG Canon (Non-Negotiable)
- McMillan — Contract identity & mechanics
- Hull — Economics & valuation neutrality
- Passarelli / Natenberg — Greeks & attribution
- Lean Data Mandate — Persist only what cannot be recreated

## 8. Change Control
All changes require:
- Section reference
- Rationale
- Version update

Silent evolution is forbidden

**End of Roadmap**
