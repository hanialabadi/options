"""
IV Maturity Classification Utility

SINGLE SOURCE OF TRUTH for IV_Maturity_State classification thresholds.

This module consolidates maturity classification logic previously duplicated across:
- scan_engine/step2_load_and_enrich_snapshot.py
- core/enrichment/resolver_implementations.py

THRESHOLDS (DO NOT MODIFY without updating all consumers):
- MATURE: ≥120 days of IV history
- PARTIAL_MATURE: 30-119 days (IVEngine Phase 2 computes IV_Rank_30D + ZScore at 30d)
- IMMATURE: 1-29 days
- MISSING: 0 days (no IV history available)

USAGE:
    from core.shared.volatility.maturity_classifier import classify_iv_maturity

    maturity_state = classify_iv_maturity(history_days=150)  # Returns 'MATURE'
"""


def classify_iv_maturity(history_days: int) -> str:
    """
    Classify IV maturity based on days of historical IV data available.

    This function implements the canonical IV maturity classification thresholds
    used throughout the pipeline. All maturity classification MUST use this function
    to ensure consistency.

    Args:
        history_days: Number of days of IV history available (0 if no data)

    Returns:
        str: Maturity classification:
            - 'MATURE': ≥120 days (sufficient for long-term IV analysis, required for INCOME strategies)
            - 'PARTIAL_MATURE': 30-119 days (IVEngine Phase 2 provides IV_Rank_30D, ZScore_30)
            - 'IMMATURE': 1-29 days (limited history, use with caution)
            - 'MISSING': 0 days (no IV data available)

    Examples:
        >>> classify_iv_maturity(150)
        'MATURE'
        >>> classify_iv_maturity(90)
        'PARTIAL_MATURE'
        >>> classify_iv_maturity(35)
        'PARTIAL_MATURE'
        >>> classify_iv_maturity(20)
        'IMMATURE'
        >>> classify_iv_maturity(0)
        'MISSING'

    Notes:
        - INCOME strategies (CSP, Buy-Write, etc.) require MATURE tier for execution
        - DIRECTIONAL strategies can execute with IMMATURE+ data
        - MISSING state always blocks execution regardless of strategy type
        - 30d threshold: IVEngine Phase 2 computes IV_Rank_30D and ZScore at 30+ days,
          providing enough derived metrics for meaningful strategy evaluation.
    """
    if history_days >= 120:
        return 'MATURE'
    elif history_days >= 30:
        return 'PARTIAL_MATURE'
    elif history_days >= 1:
        return 'IMMATURE'
    else:
        return 'MISSING'
