#!/usr/bin/env python3
"""
Derived IV Analytics Layer - REMOVED
====================================

This module and its functionality have been removed as part of the refactoring
to eliminate the canonical CSV time-series layer. Derived IV analytics will
now be computed directly from the DuckDB iv_term_history table as needed,
rather than from a separate derived CSV file.

The previous functionality included:
  - IV Index (aggregated term structure)
  - IV Rank (252-day rolling)
  - IV Percentile (252-day rolling)
  - Data availability diagnostics

These computations will be integrated into the data access layer for DuckDB
or computed on-the-fly by downstream consumers.
"""

import logging

logger = logging.getLogger(__name__)

logger.info("core/shared/data_layer/ivhv_derived_analytics.py has been removed as part of the canonical CSV layer elimination.")
logger.info("Derived IV analytics will now be computed directly from the DuckDB iv_term_history table as needed.")
