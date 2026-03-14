"""
Modular strategy evaluators with doctrine-backed rules.

Public API
----------
evaluate_strategies_independently(df, ...) -> DataFrame
    Main entry point — drop-in replacement for the monolith.

Per-family evaluators (for direct use / testing):
    evaluate_directional(row) -> EvaluationResult
    evaluate_volatility(row)  -> EvaluationResult
    evaluate_income(row)      -> EvaluationResult
"""

from ._types import (
    EvaluationResult,
    DIRECTIONAL_STRATEGIES,
    VOLATILITY_STRATEGIES,
    INCOME_STRATEGIES,
    BULLISH_STRATEGIES,
    BEARISH_STRATEGIES,
)
from .directional import evaluate_directional
from .volatility import evaluate_volatility
from .income import evaluate_income
from ._shared import contract_status_precheck, resolve_strategy_name

__all__ = [
    "EvaluationResult",
    "DIRECTIONAL_STRATEGIES",
    "VOLATILITY_STRATEGIES",
    "INCOME_STRATEGIES",
    "BULLISH_STRATEGIES",
    "BEARISH_STRATEGIES",
    "evaluate_directional",
    "evaluate_volatility",
    "evaluate_income",
    "contract_status_precheck",
    "resolve_strategy_name",
]
