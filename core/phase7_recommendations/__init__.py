"""
Phase 7: Recommendation Engine

Decision support system combining perception data (Cycles 1-2) with market context
to generate actionable exit/hold/adjust recommendations.

Architecture:
- Input: Entry baseline + Current snapshot + Drift metrics + Chart signals
- Processing: Risk scoring, exit triggers, timing optimization
- Output: Action recommendations with rationale and urgency
"""

__all__ = [
    'load_chart_signals',
    'compute_exit_recommendations',
    'classify_risk_level',
]
