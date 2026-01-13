"""
Core Volatility Module

Shared neutral volatility calculations for Phase 1-4 compliance.

This module provides IV_Rank and related volatility metrics with:
- Per-ticker historical analysis (not cross-sectional)
- 252-day lookback (industry standard)
- Explicit NaN for insufficient data (no magic defaults)
- No strategy bias (observation only)
- No thresholds or tagging

Used by:
- Phase 3 enrichment (compute_iv_rank.py)
- Scan engine (candidate selection)
"""

from .compute_iv_rank_252d import compute_iv_rank_252d, compute_iv_rank_batch

__all__ = ["compute_iv_rank_252d", "compute_iv_rank_batch"]
