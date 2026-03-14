"""
Pure formatting and scoring helpers extracted from scan_view.py.

All functions are pure: (inputs) -> output, no Streamlit calls, no side effects.
"""

import math


def safe_get(row, *keys, default=None):
    """Safe getter — tries multiple column name variants."""
    for k in keys:
        v = row.get(k)
        if v is not None and str(v) not in ('nan', 'None', ''):
            try:
                return v
            except Exception:
                pass
    return default


def fmt_price(v, decimals=2):
    """Format float as currency string."""
    try:
        return f"${float(v):.{decimals}f}"
    except Exception:
        return "—"


def fmt_pct(v, decimals=1):
    """Format float as percentage string."""
    try:
        return f"{float(v):.{decimals}f}%"
    except Exception:
        return "—"


def fmt_float(v, decimals=3):
    """Format float with arbitrary decimal places."""
    try:
        return f"{float(v):.{decimals}f}"
    except Exception:
        return "—"


def conviction_score(r) -> float:
    """
    Compute conviction score for priority sorting.

    Components (all strategy-agnostic):
      DQS_Score          50% — directional quality
      directional_bias   25% — signal strength
      IV_Maturity_Level  15% — data reliability
      confidence_band    10% — Step 12 gating verdict
    """
    try:
        dqs = float(r.get('DQS_Score') or 75)

        _bias_raw = str(r.get('directional_bias') or '').upper()
        bias = (
            100 if 'STRONG' in _bias_raw else
            60  if 'MODERATE' in _bias_raw else
            20
        )

        mat = {1: 0, 2: 20, 3: 50, 4: 80, 5: 100}.get(
            int(float(r.get('IV_Maturity_Level') or 1)), 0)

        cband = {'HIGH': 100, 'MEDIUM': 60, 'LOW': 20}.get(
            str(r.get('confidence_band') or 'LOW').upper(), 20)

        return dqs * 0.50 + bias * 0.25 + mat * 0.15 + cband * 0.10
    except Exception:
        return 0.0


def expected_move_pct(r) -> float:
    """
    Expected move % (1-sigma): IV * sqrt(DTE / 365).

    Returns float('nan') when data is missing.
    """
    try:
        iv  = float(r.get('Implied_Volatility') or 0)
        dte = float(r.get('Actual_DTE') or 0)
        if iv > 0 and dte > 0:
            return iv * math.sqrt(dte / 365.0)
        return float('nan')
    except Exception:
        return float('nan')


def iv_edge_score(r) -> float:
    """
    IV edge aligned to strategy type.

    Buyers benefit from cheap vol (IV < HV), sellers from rich vol (IV > HV).
    Returns 0-100: 100 = maximum edge for this strategy type.
    """
    try:
        gap = float(r.get('IVHV_gap_30D') or 0)
        strat = str(r.get('Strategy_Name') or '').lower()
        is_seller = any(k in strat for k in ('covered call', 'buy-write', 'cash-secured put', 'csp'))
        edge_gap = gap if is_seller else -gap
        return max(0.0, min(100.0, 50.0 + edge_gap * 2.5))
    except Exception:
        return 50.0
