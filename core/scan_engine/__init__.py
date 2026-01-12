"""
Scan Engine - Modular Market Scanning Pipeline

This package contains the complete scan pipeline broken into logical steps:

DESCRIPTIVE STEPS (2-6): Strategy-neutral observation
- Step 2: Load IV/HV snapshot with enrichment
- Step 3: Filter by IVHV gap and classify volatility regimes
- Step 5: Compute chart signals and regime classification
- Step 6: Validate data completeness and quality

PRESCRIPTIVE STEPS (7-11): Strategy-specific recommendations
- Step 7: Strategy recommendation engine (trade selection, scoring, confidence)
- Step 8: Position sizing & risk management (allocation, contracts, limits)
- Step 9A: Determine optimal option timeframe (DTE ranges based on strategy + conviction)
- Step 9B: Fetch option chains & select contracts (Tradier API, liquidity filters, strike selection)
- Step 10: PCS recalibration & pre-filter (validate structural quality, execution readiness, Greek alignment)
- Step 11: Strategy pairing & selection (straddles, strangles, best-per-ticker)

Each step is independently testable and documented.
Design: Clear boundary between observation (Steps 2-6) and action (Steps 7-11).
"""

from .step2_load_snapshot import load_ivhv_snapshot
from .step3_filter_ivhv import filter_ivhv_gap
from .step5_chart_signals import compute_chart_signals, classify_regime
from .step6_gem_filter import validate_data_quality
from .step7_strategy_recommendation import recommend_strategies
from .step0_resolve_snapshot import resolve_snapshot_path
from .step7b_multi_strategy_ranker import generate_multi_strategy_suggestions, rank_strategies_by_criteria
from .step8_position_sizing import compute_thesis_capacity
from .step9a_determine_timeframe import determine_timeframe

# Schwab-native option chain fetcher (Step 9B)
# Note: step9b_fetch_contracts.py (Tradier version) archived to _archived_legacy/
from .step9b_fetch_contracts_schwab import fetch_and_select_contracts_schwab as fetch_and_select_contracts

from .step10_pcs_recalibration import recalibrate_and_filter
from .step11_independent_evaluation import evaluate_strategies_independently # Correct import for Step 11
from .utils import validate_input
from .pipeline import run_full_scan_pipeline

# --- Runtime Guards for Legacy Imports/Calls ---
# These prevent accidental use of deprecated modules/functions.
# Any attempt to import or call these will raise an error.

# Legacy module: step7_strategy_recommendation_OLD
try:
    import sys
    if 'core.scan_engine.step7_strategy_recommendation_OLD' in sys.modules:
        raise ImportError("Legacy module 'step7_strategy_recommendation_OLD' is deprecated and cannot be imported.")
except ImportError:
    pass # Allow initial import to fail if not already imported

# Legacy module: step11_strategy_pairing
try:
    import sys
    if 'core.scan_engine.step11_strategy_pairing' in sys.modules:
        raise ImportError("Legacy module 'step11_strategy_pairing' is deprecated and cannot be imported.")
except ImportError:
    pass # Allow initial import to fail if not already imported

# Legacy functions (from step11_strategy_pairing)
def _raise_legacy_error(func_name):
    raise RuntimeError(f"Legacy function '{func_name}' is deprecated and cannot be called. Refer to LEGACY.md.")

# Placeholder for legacy functions to prevent direct calls
# These will be replaced by actual imports if the legacy module is somehow loaded,
# but the import guard above should prevent that.
compare_and_rank_strategies = lambda *args, **kwargs: _raise_legacy_error("compare_and_rank_strategies")
pair_and_select_strategies = lambda *args, **kwargs: _raise_legacy_error("pair_and_select_strategies")
calculate_position_sizing = lambda *args, **kwargs: _raise_legacy_error("calculate_position_sizing") # Legacy function from step8_position_sizing

# --- End Runtime Guards ---

__all__ = [
    'load_ivhv_snapshot',
    'filter_ivhv_gap',
    'compute_chart_signals',
    'classify_regime',
    'validate_data_quality',
    'recommend_strategies',
    'generate_multi_strategy_suggestions',
    'rank_strategies_by_criteria',
    'compute_thesis_capacity',
    'determine_timeframe',
    'fetch_and_select_contracts', # This is now the public wrapper
    'recalibrate_and_filter',
    'evaluate_strategies_independently',
    'validate_input',
    'run_full_scan_pipeline',
    'resolve_snapshot_path'
]

__version__ = '1.0.0'
