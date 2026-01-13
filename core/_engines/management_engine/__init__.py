# management_engine/__init__.py
# Phase 2: Active Trade Management Engine
#
# Public API for monitoring, scoring, and managing active option trades.
# All Phase 2 logic lives here — no scanning, no UI.

# Import functions that don't have external dependencies
from .monitor import (
    run_phase7_drift_engine,
    load_drift_timeseries,
    calculate_drift_metrics,
    flag_drift_signals
)

from .pcs_live import (
    pcs_engine_v3_2_strategy_aware,
    score_pcs_batch
)

# Lazy imports to avoid sklearn dependency at package init
# Functions that depend on recommend.py (which requires sklearn):
#   - run_v6_overlay
#   - run_full_revalidation_pipeline (calls recommend)
#
# Use direct imports when needed:
#   from core.management_engine.recommend import run_v6_overlay
#   from core.management_engine.revalidate import run_full_revalidation_pipeline

def __getattr__(name):
    if name == "run_v6_overlay":
        from .recommend import run_v6_overlay
        return run_v6_overlay
    elif name == "run_full_revalidation_pipeline":
        from .revalidate import run_full_revalidation_pipeline
        return run_full_revalidation_pipeline
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")

# Clean public interface
__all__ = [
    # Drift monitoring
    "run_phase7_drift_engine",
    "load_drift_timeseries",
    "calculate_drift_metrics",
    "flag_drift_signals",
    
    # PCS scoring
    "pcs_engine_v3_2_strategy_aware",
    "score_pcs_batch",
    
    # Recommendations (lazy loaded - requires sklearn)
    "run_v6_overlay",
    
    # Revalidation pipeline (lazy loaded - requires sklearn via recommend)
    "run_full_revalidation_pipeline",
]



