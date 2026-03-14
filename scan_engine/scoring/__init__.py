"""
Scoring — pure helper functions extracted from step12_acceptance.py.

Modules:
    classifiers     — strategy type, capital bucket, operating mode, confidence band
    bias_detectors  — directional bias, structure bias, timing quality
    income_gates    — income eligibility volatility-edge checklist
    filters         — regime matrix lookup, filter_ready_contracts, sort_by_confidence
"""

from .classifiers import (
    classify_strategy_type,
    operating_mode,
    dqs_confidence_band,
    assign_capital_bucket,
)
from .bias_detectors import (
    detect_directional_bias,
    detect_structure_bias,
    evaluate_timing_quality,
)
from .income_gates import check_income_eligibility
from .filters import (
    REGIME_STRATEGY_MATRIX,
    lookup_regime_fit,
    filter_ready_contracts,
    sort_by_confidence,
)

__all__ = [
    'classify_strategy_type', 'operating_mode', 'dqs_confidence_band',
    'assign_capital_bucket',
    'detect_directional_bias', 'detect_structure_bias', 'evaluate_timing_quality',
    'check_income_eligibility',
    'REGIME_STRATEGY_MATRIX', 'lookup_regime_fit',
    'filter_ready_contracts', 'sort_by_confidence',
]
