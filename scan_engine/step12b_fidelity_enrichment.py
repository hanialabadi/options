"""
Step 12B: Fidelity Long-Term IV Enrichment (DEPRECATED)

This module has been replaced by the IVEngine (scan_engine/enrichment/iv_engine.py).
All IV metrics are now computed from Schwab IV history in iv_term_history DuckDB table.
"""

import logging

logger = logging.getLogger(__name__)


def enrich_with_fidelity_long_term_iv(ctx, db_con):
    """No-op: Fidelity enrichment removed. IVEngine handles all IV metrics."""
    logger.info("Step 12B: Skipped (Fidelity enrichment deprecated - using IVEngine)")
