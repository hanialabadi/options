"""
Phase 3: Enrichment Module Exports

Exposes all Phase 3 observable functions and the main enrichment runner.
"""

# Main enrichment orchestrator
from .sus_compose_pcs_snapshot import run_phase3_enrichment

# Individual observable modules (for selective use)
from .compute_breakeven import compute_breakeven
from .compute_moneyness import compute_moneyness
from .compute_dte import compute_dte
from .compute_iv_rank import compute_iv_rank
from .compute_earnings_proximity import compute_earnings_proximity
from .compute_capital_deployed import compute_capital_deployed
from .compute_trade_aggregates import compute_trade_aggregates
from .compute_pnl_metrics import compute_pnl_metrics, aggregate_trade_pnl
from .compute_pnl_attribution import compute_pnl_attribution, aggregate_trade_pnl_attribution
from .compute_assignment_risk import compute_assignment_risk, get_high_assignment_risk_positions
from .sus_score_confidence_tier import score_confidence_tier
from .tag_strategy_metadata import tag_strategy_metadata
from .tag_earnings_flags import tag_earnings_flags
from .pcs_score import calculate_pcs, calculate_current_pcs  # Phase D.1: calculate_pcs is alias for backward compat
from .pcs_score_entry import calculate_entry_pcs, validate_entry_pcs
from .compute_current_pcs_v2 import compute_current_pcs_v2, compute_pcs_drift_v2  # Phase D.2: RAG-compliant multi-factor

__all__ = [
    "run_phase3_enrichment",
    "compute_breakeven",
    "compute_moneyness",
    "compute_dte",
    "compute_iv_rank",
    "compute_earnings_proximity",
    "compute_capital_deployed",
    "compute_trade_aggregates",
    "compute_pnl_metrics",
    "aggregate_trade_pnl",
    "compute_pnl_attribution",
    "aggregate_trade_pnl_attribution",
    "compute_assignment_risk",
    "get_high_assignment_risk_positions",
    "score_confidence_tier",
    "tag_strategy_metadata",
    "tag_earnings_flags",
    "calculate_pcs",  # Backward compat alias
    "calculate_current_pcs",  # New explicit name (Phase D.1)
    "calculate_entry_pcs",  # Entry baseline scoring (Phase D.1)
    "validate_entry_pcs",
    "compute_current_pcs_v2",  # RAG-compliant multi-factor (Phase D.2)
    "compute_pcs_drift_v2",
]

