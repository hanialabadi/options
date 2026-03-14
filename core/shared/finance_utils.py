"""
Shared financial utility functions used across management, scan, and UI.

Single source of truth for common calculations that were previously
duplicated across 10+ files. All modules should import from here
instead of reimplementing.

Functions:
    safe_row_float     — NaN-safe numeric extraction with column fallback
    effective_cost_per_share — broker basis / qty hierarchy
    monthly_income     — premium / DTE * 30
    annualized_yield   — (premium / capital) * (365 / dte)
    normalize_iv       — decimal/percent disambiguation
    is_retirement_account — Roth/IRA/401K detection
"""

from __future__ import annotations

from typing import Optional

import pandas as pd


# ── Safe numeric extraction ──────────────────────────────────────────────────


def safe_row_float(row, *cols, default: float = 0.0) -> float:
    """NaN-safe numeric read from a pandas row with column fallback chain.

    Python's ``or`` operator treats NaN as truthy, so
    ``float(row.get('A') or row.get('B') or 0)`` silently returns NaN
    when the first column contains NaN — bypassing all fallbacks.

    This helper uses ``pd.notna()`` to properly detect NaN/None before
    falling through to the next candidate column or the default.

    Usage::

        dte = safe_row_float(row, 'Short_Call_DTE', 'DTE', default=999)
        delta = abs(safe_row_float(row, 'Short_Call_Delta', 'Delta'))
    """
    for col in cols:
        val = row.get(col)
        if pd.notna(val):
            return float(val)
    return float(default)


# ── Cost basis ───────────────────────────────────────────────────────────────


def effective_cost_per_share(
    row,
    spot_fallback: float = 0.0,
) -> tuple[float, float, int]:
    """Compute authoritative per-share cost with tier tracking.

    Hierarchy (same across all strategy modules):
        Tier 1: Net_Cost_Basis_Per_Share (premium-adjusted)
        Tier 2: abs(Basis) / abs(Quantity) (broker raw)
        Tier 3: Underlying_Price_Entry or spot_fallback

    Returns:
        (effective_cost, broker_cost_per_share, cost_tier)
    """
    net_cost_basis = safe_row_float(row, 'Net_Cost_Basis_Per_Share')
    broker_basis_total = abs(safe_row_float(row, 'Basis'))
    qty_abs = abs(safe_row_float(row, 'Quantity', default=1.0))
    broker_cost_per_share = (
        (broker_basis_total / qty_abs)
        if qty_abs > 0 and broker_basis_total > 0
        else 0.0
    )

    entry_price = safe_row_float(row, 'Underlying_Price_Entry')

    if net_cost_basis > 0:
        return net_cost_basis, broker_cost_per_share, 1
    elif broker_cost_per_share > 0:
        return broker_cost_per_share, broker_cost_per_share, 2
    else:
        fallback = entry_price if entry_price > 0 else spot_fallback
        return fallback, broker_cost_per_share, 3


# ── Income calculations ─────────────────────────────────────────────────────


def monthly_income(premium: float, dte: float) -> float:
    """Estimate monthly income from a single option premium.

    Formula: (premium / DTE) * 30 — annualizes a single cycle's
    premium to a 30-day rate. Used across recovery detection,
    roll evaluation, and CC opportunity scoring.
    """
    if premium <= 0 or dte <= 0:
        return 0.0
    return (premium / max(dte, 1)) * 30


def annualized_yield(premium: float, capital: float, dte: float) -> float:
    """Compute annualized yield from a single option premium.

    Formula: (premium / capital) * (365 / dte).
    Used in roll scoring, negative carry gate, CC opportunity, income interpreters.
    """
    if premium <= 0 or capital <= 0 or dte <= 0:
        return 0.0
    return (premium / capital) * (365.0 / max(dte, 1))


# ── IV normalization ─────────────────────────────────────────────────────────


def normalize_iv(val: Optional[float]) -> Optional[float]:
    """Normalize IV to decimal form (0.2227) from percent form (22.27).

    Brokers report IV inconsistently — sometimes as decimal (0.22),
    sometimes as percentage (22.27). This function uses a single
    canonical threshold of 10.0:
        - val > 10.0 → divide by 100 (was in percent form)
        - val <= 10.0 → already decimal, return as-is
        - None/NaN   → return None

    The threshold 10.0 means: IV of 1000% (val=10.0) is the max plausible
    decimal IV. Speculative/penny stocks can legitimately reach 200-500% IV
    (val 2.0-5.0 in decimal). Fidelity percentage-form values (e.g. 22.27
    for 22.27% IV) are always > 10.0 for any meaningful IV, so they still
    get divided correctly.
    """
    if val is None or (isinstance(val, float) and val != val):  # NaN check
        return None
    if pd.notna(val) and val > 10.0:
        return val / 100.0
    return val


def normalize_iv_series(series: pd.Series) -> pd.Series:
    """Vectorized IV normalization for pandas Series."""
    return series.apply(lambda x: x / 100.0 if pd.notna(x) and x > 10.0 else x)


# ── Account type ─────────────────────────────────────────────────────────────

_RETIREMENT_KEYWORDS = ('ROTH', 'IRA', '401K', 'RETIRE', 'SEP', '403B')


def is_retirement_account(account: str) -> bool:
    """Detect retirement/tax-advantaged accounts.

    Matches: Roth IRA, Traditional IRA, 401K, SEP IRA, 403B, etc.
    Used to zero margin costs and apply wider strike buffers.
    """
    if not account:
        return False
    upper = account.upper()
    return any(k in upper for k in _RETIREMENT_KEYWORDS)
