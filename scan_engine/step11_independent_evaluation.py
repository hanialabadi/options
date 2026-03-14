"""
Backward-compatibility shim.

Step 8 was originally named ``step11_independent_evaluation`` in several scripts
and tests.  This module re-exports the public API from the new orchestrator so
that all existing imports continue to work unchanged.
"""

# Re-export public API
from .step8_independent_evaluation import (          # noqa: F401
    evaluate_strategies_independently,
    compare_and_rank_strategies,
)

# Re-export per-family evaluators under the old private names
# (used by test_rag_coverage_100.py)
from .evaluators.directional import evaluate_directional as _evaluate_directional_strategy   # noqa: F401
from .evaluators.volatility import evaluate_volatility as _evaluate_volatility_strategy      # noqa: F401
from .evaluators.income import evaluate_income as _evaluate_income_strategy                  # noqa: F401
