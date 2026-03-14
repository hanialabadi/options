"""
Input resolution for the unified MC engine.

Resolves spot, HV, IV, strike, premium, DTE from a position row (pd.Series)
using context-specific fallback chains controlled by ResolutionPolicy.

Unifies 7 different fallback chains across existing MC modules into one
resolver with explicit policy parameters — no single blind fallback.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd


# ── Resolution policy ────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ResolutionPolicy:
    """
    Context-specific defaults for input resolution.

    hv_fallback: annualised HV when all columns missing (0.30 scan, 0.25 mgmt)
    apply_iv_floor: blend IV into HV when IV > HV by >20%
    apply_ewma: attempt EWMA vol lookup before static columns
    """
    hv_fallback: float = 0.30
    apply_iv_floor: bool = True
    apply_ewma: bool = False


SCAN_POLICY = ResolutionPolicy(hv_fallback=0.30, apply_iv_floor=True, apply_ewma=True)
MGMT_POLICY = ResolutionPolicy(hv_fallback=0.25, apply_iv_floor=True, apply_ewma=False)


# ── Resolved inputs container ────────────────────────────────────────────────

@dataclass
class ResolvedInputs:
    """All inputs needed by MCEngine, resolved from a position row."""
    spot: float
    hv: float
    iv: float
    strike: float
    premium: float
    dte: int
    is_call: bool = True
    # Optional context for specific P&L models
    cost_basis: Optional[float] = None
    leap_strike: Optional[float] = None
    net_debit: Optional[float] = None
    n_shares: float = 100.0
    # Carry
    daily_carry: float = 0.0
    # Regime-adjusted drift
    drift: float = 0.0
    drift_source: str = ""
    # Audit
    hv_source: str = ""
    iv_source: str = ""
    spot_source: str = ""


# ── Column chains (superset of all existing modules) ─────────────────────────

_SPOT_COLS = ("UL Last", "Underlying_Last", "last_price", "Last", "Close", "Spot", "close")
_HV_COLS = ("hv_30", "HV_30_D_Cur", "HV_30D", "hv_20", "HV_20_D_Cur", "HV_20D",
            "hv_20d", "hv_60", "HV_60_D_Cur")
_HV_IV_PROXY_COLS = ("IV30_Call", "Implied_Volatility", "IV_30D", "iv_30d", "IV_Now")
_IV_COLS = ("IV_Now", "iv_30d", "IV_30D", "Execution_IV", "IV30_Call", "Implied_Volatility")
_STRIKE_COLS = ("Selected_Strike", "Strike", "strike", "Short_Call_Strike")
_PREMIUM_COLS = ("Mid_Price", "Mid", "mid", "Last", "last", "Total_Debit", "Premium_Entry")
_DTE_COLS = ("DTE", "Actual_DTE", "Target_DTE", "Min_DTE", "Short_Call_DTE")


def _normalise_vol(v: float) -> float:
    """Convert percentage-stored vol (e.g. 28.5) to decimal (0.285)."""
    if v > 1.0:
        v /= 100.0
    return v


def _first_valid(row: pd.Series, cols: tuple[str, ...],
                 normalise: bool = False,
                 min_val: float = 0.0,
                 max_val: float = float("inf")) -> tuple[Optional[float], str]:
    """Return (value, column_name) for the first valid column in chain."""
    for col in cols:
        val = row.get(col)
        if val is None or (isinstance(val, float) and np.isnan(val)):
            continue
        try:
            v = float(val)
        except (ValueError, TypeError):
            continue
        if normalise:
            v = _normalise_vol(v)
        if min_val <= v <= max_val:
            return v, col
    return None, ""


# ── Resolve functions ─────────────────────────────────────────────────────────

def _resolve_spot(row: pd.Series) -> tuple[float, str]:
    val, src = _first_valid(row, _SPOT_COLS, min_val=0.01)
    if val is None:
        return 0.0, ""
    return val, src


def _resolve_hv(row: pd.Series, policy: ResolutionPolicy) -> tuple[float, str]:
    """
    Resolve annualised HV (decimal).

    Priority: EWMA (if policy) -> static HV columns -> IV proxy -> fallback.
    IV floor: when IV > HV by >20%, blend 70% HV + 30% IV.
    """
    hv_val = None
    hv_src = ""

    # EWMA lookup (scan context only)
    if policy.apply_ewma:
        ticker = row.get("Ticker") or row.get("ticker") or row.get("Underlying_Ticker")
        if ticker:
            try:
                from scan_engine.ewma_vol import ewma_vol
                ewma = ewma_vol(str(ticker))
                if ewma is not None and 0.01 <= ewma <= 5.0:
                    hv_val, hv_src = ewma, f"EWMA({ticker})"
            except Exception:
                pass

    # Static HV columns
    if hv_val is None:
        hv_val, hv_src = _first_valid(row, _HV_COLS, normalise=True,
                                       min_val=0.01, max_val=5.0)

    # IV proxy fallback
    if hv_val is None:
        hv_val, hv_src = _first_valid(row, _HV_IV_PROXY_COLS, normalise=True,
                                       min_val=0.01, max_val=5.0)

    # Hard fallback
    if hv_val is None:
        hv_val = policy.hv_fallback
        hv_src = "FALLBACK"

    # IV floor blending
    if policy.apply_iv_floor:
        iv_raw, _ = _first_valid(row, _IV_COLS, normalise=True,
                                  min_val=0.01, max_val=5.0)
        if iv_raw is not None and iv_raw > hv_val * 1.20:
            hv_val = 0.70 * hv_val + 0.30 * iv_raw
            hv_src = f"{hv_src}+IV_blend"

    return hv_val, hv_src


def _resolve_iv(row: pd.Series, hv_fallback: float) -> tuple[float, str]:
    """Resolve annualised IV (decimal). Falls back to HV."""
    iv_val, iv_src = _first_valid(row, _IV_COLS, normalise=True,
                                   min_val=0.01, max_val=5.0)
    if iv_val is None:
        return hv_fallback, "HV_proxy"
    return iv_val, iv_src


def _resolve_strike(row: pd.Series) -> float:
    val, _ = _first_valid(row, _STRIKE_COLS, min_val=0.01)
    return val if val is not None else 0.0


def _resolve_premium(row: pd.Series) -> float:
    val, _ = _first_valid(row, _PREMIUM_COLS, min_val=0.0)
    return val if val is not None else 0.0


def _resolve_dte(row: pd.Series) -> int:
    val, _ = _first_valid(row, _DTE_COLS, min_val=0.0)
    return max(int(val), 1) if val is not None else 1


def _resolve_is_call(row: pd.Series) -> bool:
    """Determine if the position is a call or put."""
    for col in ("Option_Type", "option_type", "Type", "type"):
        val = row.get(col)
        if val is not None:
            s = str(val).lower()
            if s.startswith("p"):
                return False
            if s.startswith("c"):
                return True
    # Strategy name heuristic
    strategy = str(row.get("Strategy", row.get("Strategy_Name", "")) or "").upper()
    if "PUT" in strategy and "CALL" not in strategy:
        return False
    return True


def _resolve_daily_carry(row: pd.Series) -> float:
    """Daily margin carry cost ($/day). 0 for retirement / non-margin."""
    is_ret = row.get("Is_Retirement")
    if is_ret is True or str(is_ret).upper() in ("TRUE", "1"):
        return 0.0
    val = row.get("Daily_Margin_Cost")
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return 0.0
    cost = float(val)
    return cost if cost > 0 else 0.0


# ── Regime-adjusted drift ─────────────────────────────────────────────────

# Annualised drift bounds — intentionally conservative.
# These are bias terms that nudge GBM path shape, not directional bets.
# A ±5-8% annualised drift over 30 DTE shifts median terminal price by ~0.4-0.7%.
_MAX_DRIFT = 0.15   # hard cap (matches paths.py MAX_DRIFT)

# Base drift by regime — applied when trend indicators confirm.
_REGIME_DRIFT_BASE = {
    "TRENDING_CHASE":    0.08,   # confirmed trend: gentle positive bias
    "RECOVERY_GRIND":    0.04,   # recovering from drawdown: modest upward bias
    "MEAN_REVERSION":    0.00,   # oscillating: no directional bias
    "SIDEWAYS_INCOME":   0.00,   # range-bound: no directional bias
    "NEUTRAL":           0.00,   # unknown: stay risk-neutral
}

# Trend integrity multipliers — scale base drift by conviction level.
_TREND_INTEGRITY_SCALE = {
    "STRONG_TREND":      1.0,    # full drift
    "WEAK_TREND":        0.5,    # half drift
    "TREND_EXHAUSTED":   0.0,    # trend dying: don't drift
    "NO_TREND":          0.0,    # no trend: don't drift
}

# Price structure direction — determines drift SIGN for short-side strategies.
# For BW/CC/CSP, "STRUCTURAL_DOWN" means the stock is falling, which hurts
# the position, so drift should be negative (paths drift down).
_STRUCTURE_SIGN = {
    "STRUCTURAL_UP":     +1,
    "STRUCTURAL_DOWN":   -1,
    "RANGE_BOUND":        0,
    "STRUCTURE_BROKEN":   0,
    "CHAOTIC":            0,
}


def resolve_regime_drift(row: pd.Series) -> tuple[float, str]:
    """
    Compute annualised drift from position regime and trend indicators.

    Uses Position_Regime as the base classification, then scales by:
    - TrendIntegrity_State: conviction level (STRONG → full, WEAK → half)
    - PriceStructure_State: direction sign for the drift
    - ADX: additional dampening below 20 (no directional strength)
    - Drift_Persistence: bonus when drift has been consistent

    Returns (drift_annualised, source_label).
    Drift is bounded to ±_MAX_DRIFT.
    """
    regime = str(row.get("Position_Regime", "") or "").strip()
    if not regime or regime not in _REGIME_DRIFT_BASE:
        return 0.0, ""

    base = _REGIME_DRIFT_BASE[regime]
    if base == 0.0:
        return 0.0, f"regime={regime}→0"

    # Scale by trend integrity
    trend_integrity = str(row.get("TrendIntegrity_State", "") or "").strip()
    scale = _TREND_INTEGRITY_SCALE.get(trend_integrity, 0.5)  # default conservative
    if scale == 0.0:
        return 0.0, f"regime={regime},trend={trend_integrity}→0"

    # Determine sign from price structure
    price_struct = str(row.get("PriceStructure_State", "") or "").strip()
    sign = _STRUCTURE_SIGN.get(price_struct, 0)
    if sign == 0:
        return 0.0, f"regime={regime},struct={price_struct}→0"

    drift = sign * base * scale

    # ADX dampening: below 20 means no directional strength → halve drift
    adx = row.get("adx_14") or row.get("ADX")
    if adx is not None:
        try:
            adx_val = float(adx)
            if adx_val < 20:
                drift *= 0.5
        except (ValueError, TypeError):
            pass

    # Drift persistence bonus: if drift has been consistent for 5+ snapshots,
    # add 20% more confidence
    persistence = row.get("Drift_Persistence")
    if persistence is not None:
        try:
            p_val = int(persistence)
            if p_val >= 5:
                drift *= 1.2
        except (ValueError, TypeError):
            pass

    # Clamp
    drift = max(-_MAX_DRIFT, min(_MAX_DRIFT, drift))

    source = (
        f"regime={regime},trend={trend_integrity},"
        f"struct={price_struct},drift={drift:+.3f}"
    )
    return round(drift, 4), source


# ── Public API ────────────────────────────────────────────────────────────────

def resolve_inputs(row: pd.Series,
                   policy: ResolutionPolicy = SCAN_POLICY) -> ResolvedInputs:
    """
    Resolve all MC inputs from a position row.

    Uses the policy to control context-specific behaviour
    (EWMA, IV floor, fallback values).
    """
    spot, spot_src = _resolve_spot(row)
    hv, hv_src = _resolve_hv(row, policy)
    iv, iv_src = _resolve_iv(row, hv)
    strike = _resolve_strike(row)
    premium = _resolve_premium(row)
    dte = _resolve_dte(row)
    is_call = _resolve_is_call(row)
    daily_carry = _resolve_daily_carry(row)

    # Optional context
    cost_basis = None
    for col in ("Net_Cost_Basis_Per_Share", "Basis"):
        val = row.get(col)
        if val is not None and pd.notna(val):
            v = abs(float(val))
            if v > 0:
                # Basis is total; per-share needs quantity
                if col == "Basis":
                    qty = abs(float(row.get("Quantity", 1) or 1))
                    v = v / qty if qty > 0 else v
                cost_basis = v
                break

    leap_strike = None
    for col in ("LEAP_Call_Strike", "PMCC_LEAP_Strike"):
        val = row.get(col)
        if val is not None and pd.notna(val) and float(val) > 0:
            leap_strike = float(val)
            break

    net_debit = None
    val = row.get("PMCC_Net_Debit") or row.get("Total_Debit")
    if val is not None and pd.notna(val):
        net_debit = float(val)

    n_shares = abs(float(row.get("Quantity", 100) or 100))

    # Regime-adjusted drift
    drift, drift_src = resolve_regime_drift(row)

    return ResolvedInputs(
        spot=spot,
        hv=hv,
        iv=iv,
        strike=strike,
        premium=premium,
        dte=dte,
        is_call=is_call,
        cost_basis=cost_basis,
        leap_strike=leap_strike,
        net_debit=net_debit,
        n_shares=n_shares,
        daily_carry=daily_carry,
        drift=drift,
        drift_source=drift_src,
        hv_source=hv_src,
        iv_source=iv_src,
        spot_source=spot_src,
    )
