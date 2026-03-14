"""
Near-Term Volatility Blend — Vol Schedule Builder
===================================================

Builds a per-day volatility schedule that captures vol clustering:
recent realised vol decays toward long-run HV over the simulation horizon.

Problem
-------
GBM uses flat HV_30D for all path days.  But vol clusters — high-vol days
follow high-vol days (Mandelbrot, 1963; Cont, 2001).  A stock that just had
a 3% gap move has higher near-term realised vol than its 30-day average.

Using flat HV_30D under-estimates early dispersion and over-estimates late
dispersion.  This matters most for short-DTE positions (7-21 days) where
the first week of paths dominates the P&L distribution.

Solution
--------
Exponential decay blend from EWMA vol (near-term) to HV (long-run):

    σ(d) = σ_ewma × w(d) + σ_hv × (1 - w(d))

where w(d) = exp(-d / τ), τ = decay half-life in trading days.

Default τ = 10 days (half-life ≈ 7 days):
  Day  0:  w = 1.00  →  100% EWMA
  Day  5:  w = 0.61  →   61% EWMA, 39% HV
  Day 10:  w = 0.37  →   37% EWMA, 63% HV
  Day 20:  w = 0.14  →   14% EWMA, 86% HV
  Day 30:  w = 0.05  →    5% EWMA, 95% HV

This plugs directly into MCEngine's iv_schedule parameter — no engine
changes needed.

When EWMA ≈ HV (within 10%), the schedule is flat and equivalent to
the current behaviour.  The blend only matters when near-term vol diverges
significantly from the 30-day average.

Fallback
--------
If EWMA is unavailable (no price history, DB error), returns None and
the caller uses flat HV as before.  Non-blocking by design.

References
----------
  Mandelbrot (1963): "The Variation of Certain Speculative Prices"
    — vol clustering is empirically universal
  Cont (2001): "Empirical properties of asset returns" — autocorrelation
    of |r_t| decays slowly, supporting exponential vol decay models
  Hull Ch.22: EWMA as exponentially weighted estimate of near-term σ²
  RiskMetrics (1994): λ=0.94 effective window ≈ 17 sessions
"""

from __future__ import annotations

import logging
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

DECAY_TAU = 10.0        # exponential decay time constant (trading days)
MIN_DIVERGENCE = 0.10   # |EWMA - HV| / HV threshold to bother building schedule
MAX_EWMA_RATIO = 3.0    # cap EWMA/HV ratio to prevent extreme near-term vol
VOL_FLOOR = 0.05        # minimum annualised vol in schedule (5%)
VOL_CAP = 3.0           # maximum annualised vol in schedule (300%)


def build_vol_schedule(
    hv: float,
    ewma: float,
    n_days: int,
    tau: float = DECAY_TAU,
) -> Optional[np.ndarray]:
    """
    Build a per-day volatility schedule blending EWMA → HV.

    Parameters
    ----------
    hv     : long-run annualised HV (decimal, e.g. 0.28)
    ewma   : near-term EWMA annualised vol (decimal, e.g. 0.38)
    n_days : number of simulation days (= DTE)
    tau    : decay time constant in trading days (default 10)

    Returns
    -------
    np.ndarray of shape (n_days,) with per-day annualised vol, or
    None if the blend is unnecessary (EWMA ≈ HV within MIN_DIVERGENCE).

    The returned schedule is suitable for passing directly to
    gbm_daily_paths(iv_schedule=...) or MCEngine(iv_schedule=...).
    """
    if n_days < 1:
        return None

    # Validate inputs
    if hv <= 0 or ewma <= 0:
        return None

    # Check if blend is worth it
    divergence = abs(ewma - hv) / max(hv, 0.01)
    if divergence < MIN_DIVERGENCE:
        return None  # EWMA ≈ HV — flat vol is fine

    # Cap EWMA ratio to prevent extreme schedules
    if ewma > hv * MAX_EWMA_RATIO:
        ewma = hv * MAX_EWMA_RATIO
    elif ewma < hv / MAX_EWMA_RATIO:
        ewma = hv / MAX_EWMA_RATIO

    # Build decay weights: w(d) = exp(-d / tau)
    days = np.arange(n_days, dtype=float)
    weights = np.exp(-days / tau)

    # Blend: σ(d) = ewma × w(d) + hv × (1 - w(d))
    schedule = ewma * weights + hv * (1.0 - weights)

    # Clamp to floor/cap
    schedule = np.clip(schedule, VOL_FLOOR, VOL_CAP)

    return schedule


def resolve_vol_schedule(
    ticker: Optional[str],
    hv: float,
    n_days: int,
    *,
    ewma_override: Optional[float] = None,
    tau: float = DECAY_TAU,
) -> tuple[Optional[np.ndarray], str]:
    """
    Resolve a vol schedule for a ticker, with DB lookup and fallback.

    Parameters
    ----------
    ticker         : equity ticker for EWMA lookup (None = skip lookup)
    hv             : long-run HV from position row (decimal)
    n_days         : simulation horizon in trading days
    ewma_override  : if provided, skip DB lookup and use this EWMA value
    tau            : decay time constant

    Returns
    -------
    (schedule, source) where:
      schedule : np.ndarray(n_days,) or None (if blend unnecessary)
      source   : str describing what happened:
        'EWMA_BLEND(TICKER)' — built from DB EWMA lookup
        'EWMA_BLEND(override)' — built from provided ewma_override
        'FLAT' — EWMA ≈ HV, no schedule needed
        'UNAVAILABLE' — no EWMA data available
    """
    ewma = ewma_override

    # Try DB lookup if no override
    if ewma is None and ticker:
        try:
            from scan_engine.ewma_vol import ewma_vol
            ewma = ewma_vol(str(ticker))
        except Exception as exc:
            logger.debug(f"vol_blend: EWMA lookup failed for {ticker}: {exc}")

    if ewma is None:
        return None, "UNAVAILABLE"

    # Validate EWMA
    if not (0.01 <= ewma <= 5.0):
        return None, "UNAVAILABLE"

    schedule = build_vol_schedule(hv, ewma, n_days, tau=tau)

    if schedule is None:
        return None, "FLAT"

    source = f"EWMA_BLEND({ticker or 'override'})"
    return schedule, source
