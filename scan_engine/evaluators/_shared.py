"""
Shared helpers for evaluators.

Extracted from the monolith to prevent the 4 known bug classes:
  1. NaN-truthy: ``np.nan or fallback`` passes NaN through
  2. Strategy name resolution across 3 column aliases
  3. Data completeness % calculation
  4. Contract status pre-check routing
"""

from __future__ import annotations

import math
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from ._types import EvaluationResult


# ── NaN-safe accessors ────────────────────────────────────────


def safe_get(row: pd.Series, *keys, default=None):
    """Return the first non-NaN value among *keys*, or *default*.

    Prevents the ``np.nan or fallback`` truthy bug.
    """
    for k in keys:
        v = row.get(k)
        if v is not None and not (isinstance(v, float) and math.isnan(v)):
            return v
    return default


def safe_float(row: pd.Series, *keys, default=None) -> Optional[float]:
    """Like :func:`safe_get` but coerces to ``float``."""
    v = safe_get(row, *keys)
    if v is None:
        return default
    try:
        f = float(v)
        return default if math.isnan(f) else f
    except (TypeError, ValueError):
        return default


# ── Strategy name resolver ────────────────────────────────────


def resolve_strategy_name(row: pd.Series) -> str:
    """Return the first non-NaN strategy name from known aliases."""
    return safe_get(row, 'Strategy_Name', 'Strategy', 'Primary_Strategy', default='')


# ── Data completeness ─────────────────────────────────────────


def check_required_data(
    row: pd.Series,
    required_fields: Dict[str, str],
) -> Tuple[List[str], float]:
    """Check which *required_fields* are missing.

    Parameters
    ----------
    required_fields : dict
        ``{display_name: column_name}``

    Returns
    -------
    (missing_list, completeness_pct)
    """
    missing: List[str] = []
    for display, col in required_fields.items():
        v = row.get(col)
        if v is None or (isinstance(v, float) and math.isnan(v)):
            missing.append(display)
    total = len(required_fields)
    pct = ((total - len(missing)) / total * 100) if total > 0 else 100.0
    return missing, pct


# ── Contract status pre-check ─────────────────────────────────


def contract_status_precheck(row: pd.Series) -> Optional[EvaluationResult]:
    """Route based on Contract_Status.

    Returns an :class:`EvaluationResult` if the row should be short-circuited
    (rejected / deferred), or ``None`` to proceed to full evaluation.
    """
    contract_status = row.get('Contract_Status')
    failure_reason = row.get('Failure_Reason', '')
    market_open = row.get('is_market_open', True)

    if contract_status == 'OK':
        return None  # proceed
    if contract_status == 'LEAP_FALLBACK':
        return None  # proceed (with reduced confidence handled by evaluator)
    if contract_status == 'OI_FALLBACK':
        return None  # proceed — contract found at alternative expiration via cascade

    if contract_status == 'NO_EXPIRATIONS_IN_WINDOW':
        return EvaluationResult(
            'Deferred_DTE', 75.0, 'No expirations in DTE window', 50.0,
            f"Deferred: {failure_reason}. Not rejected — can retry with different DTE window.",
        )

    if contract_status == 'FAILED_LIQUIDITY_FILTER':
        if not market_open:
            return EvaluationResult(
                'Deferred_Liquidity', 60.0, 'Thin liquidity (off-hours)', 50.0,
                f"Deferred: {failure_reason}. Off-hours thin tape — retry during market hours.",
            )
        return EvaluationResult(
            'Reject', 40.0, 'Structurally illiquid', 0.0,
            f"Rejected: {failure_reason}. Illiquid during market hours — not tradable.",
        )

    if contract_status == 'FAILED_GREEKS_FILTER':
        return EvaluationResult(
            'Reject', 30.0, 'Missing or invalid Greeks', 0.0,
            f"Rejected: {failure_reason}. Cannot be risk-managed without Greeks.",
        )

    if contract_status == 'FAILED_IV_FILTER':
        return EvaluationResult(
            'Reject', 30.0, 'Missing implied volatility', 0.0,
            f"Rejected: {failure_reason}. Volatility strategies require IV data.",
        )

    if contract_status == 'NO_CHAIN_RETURNED':
        return EvaluationResult(
            'Reject', 0.0, 'No option chains available', 0.0,
            f"Rejected: {failure_reason}. Ticker not optionable or API failure.",
        )

    if contract_status in ('NO_CALLS_AVAILABLE', 'NO_PUTS_AVAILABLE'):
        leg = 'calls' if 'CALL' in str(contract_status) else 'puts'
        return EvaluationResult(
            'Reject', 0.0, f'No {leg} available', 0.0,
            f"Rejected: {failure_reason}. Strategy requires {leg} but chain has none.",
        )

    # Legacy path (Step 9B not yet run)
    if contract_status is None or contract_status == '':
        legacy = row.get('Contract_Selection_Status', 'Pending')
        if legacy in ('Pending', 'No_Chains_Available'):
            return EvaluationResult(
                'Pending_Greeks', 50.0, 'Contract selection not yet run', 50.0,
                'Pre-contract evaluation — awaiting Step 9B',
            )
        if legacy == 'No_Expirations_In_DTE_Window':
            return EvaluationResult(
                'Deferred_DTE', 75.0, 'No expirations in DTE window', 50.0,
                'Deferred: No matching expirations. Can retry with adjusted DTE.',
            )
        if legacy != 'Contracts_Available':
            return EvaluationResult(
                'Reject', 0.0, 'No valid contracts selected', 0.0,
                f"Rejected: Contract selection failed ({legacy})",
            )
        return None  # Contracts_Available → proceed

    # Unknown status
    return EvaluationResult(
        'Reject', 0.0, f'Unknown contract status: {contract_status}', 0.0,
        f"Rejected: Unrecognized Contract_Status value: {contract_status}",
    )
