# Options Intelligence Platform: Architecture Roadmap

## 🏗️ System Definitions

### 1. Scan Engine (Discovery)
The **Scan Engine** is the discovery layer of the platform. Its purpose is to scan the broad market universe to find **new trade opportunities**.
*   **Input:** Raw market data snapshots (IV/HV).
*   **Process:** Filters for volatility edges, validates technical chart signals, selects specific option contracts, and applies "READY_NOW" acceptance logic.
*   **Output:** A list of new trade candidates with specific sizing and scaling roadmaps.
*   **Execution Semantics:** See [EXECUTION_SEMANTICS.md](docs/EXECUTION_SEMANTICS.md) for formal definitions of "valid execution" vs "escalation eligibility".

### 2. Management Engine (Cycles 1-3)
The **Management Engine** is the monitoring and decision layer for **existing open positions**. It operates in three distinct cycles:
*   **Cycle 1 (Perception):** Ingests current positions from the broker and establishes "Ground Truth." It freezes "Anchors" (entry price, Greeks, technical regime) to provide a baseline for measurement.
*   **Cycle 2 (Measurement):** Measures **Drift**—the migration of the current state away from the entry anchors. It performs P&L attribution to explain *why* a position is winning or losing (Delta, Theta, Vega, etc.).
*   **Cycle 3 (Decision):** Applies doctrinal rules (McMillan, Passarelli, Hull, Natenberg) to the measured drift to generate authoritative actions (e.g., HOLD, EXIT, TRIM).

---

## 🔍 Holistic Audit Summary

### 1. The "Computation Leak" (Critical)
The dashboard previously violated its "Read-Only" mandate by calculating drift signals (Direction, Persistence, Magnitude) on the fly in `manage_view.py`.
*   **Status:** Fixed. Signals moved to `core/management/cycle2/drift/drift_engine.py`.

### 2. Data Storage Inconsistency
Cycle 1 used DuckDB, while Cycle 2/3 and Scan results relied on CSV artifacts. This created a "split-brain" architecture.
*   **Status:** Fixed. DuckDB persistence added to `run_all.py` and `scan_engine/pipeline.py`.

### 3. Orchestration Blind Spots
Running pipelines via the UI used blocking `subprocess.run` calls without log streaming, leading to a "frozen" UI and hidden failures.
*   **Status:** Fixed. Unified `core.runner` implemented with `st.status` log streaming.

---

## 🗺️ Implementation Roadmap

### Phase 1: Hardening the "Truth Layer" (Persistence)
- [x] **Signal Migration:** Move all drift and risk signal calculations from `streamlit_app/` to `core/management/cycle2/`.
- [x] **Unified Persistence (Management):** Persist Cycle 3 recommendations to DuckDB `management_recommendations` table.
- [x] **Unified Persistence (Scan):** Persist Step 12 READY_NOW results to DuckDB `scan_results` table.
- [x] **UI Migration:** Updated `manage_view.py` and `scan_view.py` to prefer DuckDB views (`v_latest_recommendations`, `v_latest_scan_results`) over CSV files.
- [x] **Schema Enforcement:** Implemented strict data contracts in `core/shared/data_contracts/schema.py` where the UI only consumes pre-calculated "Evidence Packets."

### Phase 2: Orchestration & Parity
- [x] **Unified Orchestrator:** Created `core.runner` module that handles environment-agnostic execution for both CLI and UI.
- [x] **Live Feedback:** Upgraded Scan, Management, and Snapshot trigger buttons to stream live logs using `st.status`.
- [ ] **CLI/UI Parity:** Ensure the "Trust Audit" in the UI uses the exact same agent logic as the `cli/forensic_auditor.py`.

### Phase 3: Situational Awareness (The "Cockpit")
- [x] **Cross-Cycle Simulation:** Added a "What-If" simulation cockpit to the Manage View using a new `SimulationEngine`.
- [x] **Regime-Aware UI:** Integrated Market Stress Banners and Regime Stability metrics into the Risk View.
- [ ] **Interactive Drift:** Replace static drift tables with interactive Plotly charts showing Greek decay vs. Price movement.

---

## 🎯 Trust Framework Definition

### What "Trust" Is NOT in This System

Options trading is not a classification problem. It is a **distribution + sizing + path dependency problem**.

Framing trust as a hit rate, scan accuracy %, or % profitable trades is structurally misleading:
- Elite institutional systems with full feedback loops achieve only 60–65% directional accuracy and ~55% win rate
- Stable Sharpe > 2 is rare even with those systems
- Regime shifts break calibration; winners and losers cluster; tail risk dominates

### What "Trust" MEANS in This System

This system is evolving from a **signal detector** → **doctrine enforcement engine**.

Trust = *the probability the engine prevents structural mistakes*, not *the probability a trade wins*.

### The Three Trust Layers

| Layer | Definition | Target |
|-------|-----------|--------|
| **Layer 1 — Structural Validity** | How often does the scan admit only structurally coherent setups? (convexity gate, DTE correction, directional logic, pullback anchor, discrete contract awareness) | **90–95% (current focus)** |
| **Layer 2 — EV Stability** | How stable is expected value across regimes? | Moderate — not a point metric |
| **Layer 3 — Realized Win Rate** | Directional accuracy, regime-dependent | ~50–65% (regime-dependent) |

The architecture is currently **correctly optimizing Layer 1**.

### What 90% Can Defensibly Mean

> "90% probability that the scan does not admit structurally invalid trades"

This is a **quality gate metric**, not a performance metric. With the following completed fixes, the system is approaching this:
- Convexity gate (Gamma_ROC_3D + Gamma threshold)
- Single-contract roll path (trim-via-roll)
- DTE correction (45 → 21 days)
- Directional logic fixes (put intrinsic, Gate 2.5 direction)
- Pullback anchor (EMA9 / SMA20 / Lower BB)
- Forward expectancy gate (EV_Feasibility_Ratio)
- Conviction decay timer (Delta_Deterioration_Streak)
- IV-implied entry target (Price_Target_Entry freeze)

### Why More Filters Are NOT the Answer

> More filters → overfit signal stack
> Feedback loop → calibration + learning

The closed-trade feedback loop is the **next correct build** — not more scan filters.

---

### Phase 4: Closed-Trade Feedback Loop (Next Build)

**Goal:** Enable Layer 2 (EV stability) and Layer 3 (win rate calibration) measurement.

**What existed:** `core/_support/ml_training/collect_trades.py` detects closed trades. `feedback_engine.py` was already writing `closed_trades` (40 rows) and `doctrine_feedback` — but the feedback was not reaching the scan engine.

**What was built (Feb 2026):**

- [x] **Outcome Writer:** `feedback_engine.py` → `closed_trades` table (40 rows) + `doctrine_feedback` (condition buckets by strategy × momentum state). Runs after every management cycle.
- [x] **Calibration Reader:** `scan_engine/feedback_calibration.py` — reads `doctrine_feedback`, returns DQS multiplier for `(strategy, momentum_state)` bucket. Injected into `step12_acceptance.py` R3.2 after timing penalty. Graceful: DB failure → neutral (×1.0). Guarded: only adjusts when N ≥ 15.
- [x] **Calibrated Confidence:** TIGHTEN → cap HIGH→MEDIUM; REINFORCE → promote MEDIUM→HIGH. `Calibrated_Confidence`, `Feedback_Win_Rate`, `Feedback_Sample_N`, `Feedback_Action`, `Feedback_Note` columns in `acceptance_all`.
- [x] **Dashboard Feedback Row:** `scan_view.py` renders TIGHTEN as warning, REINFORCE as success. Silent when neutral.
- [ ] **Calibration Activation:** All buckets currently INSUFFICIENT_SAMPLE (N < 15 — correct, data accumulating). Self-activates as `closed_trades` reaches ~15 outcomes per bucket. No code change needed.

**Why this matters:**
- Enables regime adaptation (if LONG_PUT win rate drops in uptrend regime, score is penalized)
- Enables strategy ranking (LEAPS vs short-DTE vs spreads — which works in which regime?)
- Does NOT produce a "90% win rate" — produces **calibrated structural confidence** aligned with Layer 1

**Design constraint:** The feedback loop MUST be read-only in the scan engine. Scan confidence adjusts based on historical outcomes, but the scan does not retrain or overfit in real-time. Regime-specific lookback window: rolling 90 days.
