# LEGACY Scan Engine Artifacts - DO NOT REFERENCE

This document explicitly lists files, functions, and concepts that are considered **legacy, deprecated, or architecturally invalid** within the Scan Engine. They are retained for backward compatibility or historical reference only and **must never be referenced by new code, tests, or documentation.**

Referencing these artifacts will lead to architectural violations, silent regressions, and incorrect behavior under the current strategy isolation paradigm.

---

## ❌ Deprecated Files

The following files are entirely deprecated and should not be used:

*   `core/scan_engine/step7_strategy_recommendation_OLD.py`: This is an outdated strategy recommendation engine that does not adhere to the multi-strategy ledger or independent evaluation principles.
*   `core/scan_engine/step11_strategy_pairing.py`: This file contains logic for cross-strategy ranking and pairing, which violates the authoritative strategy isolation architecture. Strategies are now evaluated independently in `step11_independent_evaluation.py`.

## ❌ Deprecated Functions

The following functions are deprecated and must not be called:

*   `compare_and_rank_strategies` (from `step11_strategy_pairing.py`): This function performs cross-strategy ranking, which is no longer authoritative.
*   `pair_and_select_strategies` (from `step11_strategy_pairing.py`): This function performs strategy pairing and selection, which is no longer authoritative.
*   **All deprecated functions within `core/scan_engine/step8_position_sizing.py`**: This file has been refactored to be "execution-only". Any functions within it that perform strategy selection, ranking, or competitive comparison are legacy. These include (but are not limited to):
    *   `finalize_and_size_positions`
    *   `_select_top_ranked_per_ticker`
    *   `_apply_final_filters`
    *   `_apply_portfolio_constraints`
    *   `_calculate_position_sizing_new`
    *   `_generate_selection_audit`
    *   `_explain_strategy_selection`
    *   `_explain_contract_selection`
    *   `_explain_liquidity_acceptance`
    *   `_explain_capital_approval`
    *   `_explain_competitive_rejection`
    *   `_log_audit_summary`
    *   `_log_final_selection_summary`
    *   `calculate_position_sizing` (the legacy version)

## ❌ Deprecated Concepts

The following architectural concepts are invalid under the current Scan Engine design:

*   **Any logic assuming Step 7 = final strategy selection**: Step 7 (`step7_strategy_recommendation.py`) now generates a multi-strategy ledger. Final selection and position sizing occur in Step 8 (`step8_position_sizing.py`) after independent evaluation in Step 11 (`step11_independent_evaluation.py`).
*   **Any logic assuming single strategy per ticker**: The system now supports multiple valid strategies per ticker, evaluated independently.
*   **Any logic assuming ranking before validation**: Strategies are first validated independently against their own theoretical requirements. Ranking (if any) occurs *within* strategy families or at the portfolio allocation layer, not as a pre-filter.
*   **`Strategy_Rank` or `Comparison_Score` as a primary selection metric**: These metrics were part of the old cross-strategy ranking paradigm and are no longer authoritative for strategy selection.

## ❌ Outdated Documentation

The following documentation files describe the old architecture and should be updated or removed:

*   `core/scan_engine/PIPELINE_ARCHITECTURE.md`
*   `core/scan_engine/QUICK_REFERENCE.md`
*   `core/scan_engine/README.md`

---

**Purpose of this document:** To eliminate ambiguity and prevent future regressions by clearly defining what is real vs. historical. Future development, testing, and documentation must strictly adhere to the authoritative code paths and architectural principles.
