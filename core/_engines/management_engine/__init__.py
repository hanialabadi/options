# management_engine/__init__.py
# Public API for the Management Engine.
# Covers: drift monitoring, PCS scoring, doctrine overlay, and revalidation.
# No scan logic here — engines are separate, dashboard is the shared fork.

from .monitor import (
    run_phase7_drift_engine,
    load_drift_timeseries,
    calculate_drift_metrics,
    flag_drift_signals,
)

from .pcs_live import (
    pcs_engine_v3_2_strategy_aware,
    score_pcs_batch,
)

from .recommend import run_v6_overlay

from .revalidate import (
    run_full_revalidation_pipeline,
    market_is_open,
)

__all__ = [
    # Drift monitoring
    "run_phase7_drift_engine",
    "load_drift_timeseries",
    "calculate_drift_metrics",
    "flag_drift_signals",
    # PCS scoring
    "pcs_engine_v3_2_strategy_aware",
    "score_pcs_batch",
    # Doctrine overlay
    "run_v6_overlay",
    # Revalidation pipeline
    "run_full_revalidation_pipeline",
    "market_is_open",
]
