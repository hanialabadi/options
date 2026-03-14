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

from .pipeline import run_full_scan_pipeline
from .step0_resolve_snapshot import resolve_snapshot_path

# Expose loaders for convenience
from core.shared.data_layer.price_history_loader import load_price_history
from .loaders.schwab_api_client import SchwabClient
from .loaders.entry_quality_enhancements import (
    enrich_snapshot_with_entry_quality,
    enrich_contracts_with_execution_quality
)


__all__ = [
    'run_full_scan_pipeline',
    'resolve_snapshot_path',
    'load_price_history',
    'SchwabClient',
    'enrich_snapshot_with_entry_quality',
    'enrich_contracts_with_execution_quality'
]

__version__ = '1.0.0'
