# Doctrinal Ground Truth: Strategy Origin and Management

This document serves as the authoritative reference for strategy identity and professional management standards. These rules are locked as ground truth; system behavior must be audited against this reference.

## 1. Strategy Origin Semantics (BUY_WRITE vs. COVERED_CALL)

*   **Immutable Inception Property**: Strategy origin is established at the moment of entry and must never be overwritten by management actions (e.g., rolling).
*   **BUY_WRITE Qualification**: A position qualifies as `BUY_WRITE` if and only if the stock leg and short call leg share the same `Entry_Timestamp` (unitary trade).
*   **Temporal Inversion Rule**: If an option leg precedes the stock leg, the position is disqualified from `BUY_WRITE` status. It must be classified as `COVERED_CALL` (reconstructed) or `OPTION_ONLY`.
*   **Reconciliation Standards**:
    *   **AAPL & SOFI (2025-12-29)**: Confirmed as true unitary `BUY_WRITE` trades.
    *   **UUUU (2025-12-04/30)**: Confirmed as `COVERED_CALL` due to temporal inversion (option preceded stock).

## 2. Cash-Secured Put (CSP) Management Doctrine

*   **Authoritative Inputs**: Strike Price, Days to Expiration (DTE), Current Underlying Price, Original Thesis, and Current Option Premium.
*   **Role of Implied Volatility (IV)**:
    *   IV is **supportive** for timing and **required** for relative-value claims (e.g., rolling for volatility).
    *   Without IV data, volatility-based rolling and "premium is expensive/cheap" justifications are **explicitly forbidden**.
*   **Assignment Governance**: Assignment is acceptable only if the original ownership thesis remains intact. If the fundamental thesis has collapsed, assignment is undesirable.

## 3. Decision Semantics (HOLD vs. UNCERTAIN)

*   **HOLD**: A confident neutral decision. Valid only when all authoritative inputs are present and no structural risk boundaries are breached.
*   **UNCERTAIN**: A doctrinally necessary safety state. Required when authoritative inputs (e.g., IV for relative-value decisions or stock thesis) are missing.
*   **Enforcement Rule**: Forced `HOLD` decisions due to missing authority must never be presented as confident neutrality.

## 4. Structural Risk Boundaries (Enforcement Refinement)

*   **Delta Threshold**: When a short option's Delta approaches **~0.65–0.70** with limited DTE, the position exits the passive `HOLD` envelope.
*   **Required Action**: Such positions require explicit reassessment (roll, close, or assignment intent), even if the underlying thesis remains valid.
