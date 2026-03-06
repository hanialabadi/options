# Cycle-3 Implementation Validation Checklist

This checklist is for verifying the correctness of the Cycle-3 Recommendation Engine implementation against the established Strategy Origin Doctrine.

## 1. Authority Verification (Input Failure Modes)
Verify that the system correctly identifies missing authoritative data and defaults to the safe state.

| Strategy | Failure Mode | Expected Outcome |
| :--- | :--- | :--- |
| **BUY_WRITE** | Missing Stock Entry Price (Anchor) | `UNCERTAIN` |
| **BUY_WRITE** | Stock/Option Entry Timestamps Mismatch | `UNCERTAIN` (or `COVERED_CALL`) |
| **COVERED_CALL** | Missing Stock Entry Price (Anchor) | `UNCERTAIN` |
| **CSP** | Missing Original Thesis | `UNCERTAIN` |
| **LONG_OPTION** | Missing Greeks (Delta/Theta) | `UNCERTAIN` |
| **STRADDLE** | Missing IV Rank / IV Percentile | `UNCERTAIN` |

## 2. Decision State Edge Cases
Verify that representative market scenarios produce the doctrinally correct decision state.

| Scenario | Market Condition | Expected Decision |
| :--- | :--- | :--- |
| **Standard Hold** | Delta 0.30, DTE 30, Thesis Valid | `NEUTRAL_CONFIDENT` |
| **Risk Breach** | Short Call Delta = 0.72 | `ACTIONABLE` |
| **Time Decay** | DTE = 5 | `ACTIONABLE` |
| **Profit Target** | 85% of Max Profit Realized | `ACTIONABLE` |
| **Thesis Collapse** | Thesis marked "Collapsed" by user | `ACTIONABLE` |
| **Missing IV (CC)** | Price valid, IV Rank missing | `NEUTRAL_CONFIDENT` (IV supportive) |
| **Missing IV (Straddle)** | Price valid, IV Rank missing | `UNCERTAIN` (IV required) |

## 3. Forced Uncertainty Signals
The following signals must force an `UNCERTAIN` state regardless of how favorable price or PnL may appear.

- [ ] **Ambiguous Origin**: Any position where the `Entry_Structure` cannot be verified as unitary or reconstructed.
- [ ] **Missing Thesis**: Any position lacking a documented entry thesis.
- [ ] **Volatility Blindness**: Any relative-value decision (e.g., "Roll for Volatility") attempted without verified IV Rank/Percentile.
- [ ] **Anchor Drift**: Any position where the frozen entry price (Stock or Option) has been lost or corrupted.

## 4. HOLD Prohibitions
Verify that the system never returns `NEUTRAL_CONFIDENT` (HOLD) under the following conditions:

- [ ] **Boundary Breach**: Delta > 0.70 or DTE < 7.
- [ ] **Authority Gap**: Any "REQUIRED" field in the Strategy Authority Matrix is NULL.
- [ ] **Default Fallback**: The system must never default to `HOLD` simply because it cannot find a reason to act; it must default to `UNCERTAIN`.
- [ ] **Retroactive Promotion**: A `COVERED_CALL` must never be promoted to `BUY_WRITE` status, even if quantities are perfectly aligned.
