"""
Option valuation primitives for the unified MC engine.

Brenner-Subrahmanyam (1988): ATM approximation for European options.
  Time value ~= 0.4 * S * sigma * sqrt(T)
  Moneyness decay: exp(-2.0 * |S - K| / S)

Used for mid-life option valuation (e.g., optimal exit day-by-day)
where full BS isn't needed but intrinsic-only is too crude.
"""

from __future__ import annotations

import numpy as np

BRENNER_COEFFICIENT = 0.4
MONEYNESS_DECAY_RATE = 2.0


def brenner_option_value(spot: np.ndarray, strike: float,
                         iv: float, dte_remaining: int,
                         is_call: bool = True) -> np.ndarray:
    """
    Approximate option value using Brenner-Subrahmanyam + moneyness decay.

    Parameters
    ----------
    spot : (n,) array of underlying prices
    strike : strike price
    iv : annualised implied volatility (decimal)
    dte_remaining : trading days remaining to expiry
    is_call : True for calls, False for puts

    Returns
    -------
    (n,) array of approximate option values (per share)
    """
    from core.shared.mc.paths import TRADING_DAYS

    # Intrinsic value
    if is_call:
        intrinsic = np.maximum(spot - strike, 0.0)
    else:
        intrinsic = np.maximum(strike - spot, 0.0)

    # Time value: ATM approximation scaled by moneyness
    t = max(dte_remaining, 0) / TRADING_DAYS
    if t <= 0 or iv <= 0:
        return intrinsic

    time_val = BRENNER_COEFFICIENT * spot * iv * np.sqrt(t)
    moneyness = np.abs(spot - strike) / np.maximum(spot, 1e-8)
    time_scale = np.exp(-MONEYNESS_DECAY_RATE * moneyness)

    return intrinsic + time_val * time_scale


def intrinsic_value(spot: np.ndarray, strike: float,
                    is_call: bool = True) -> np.ndarray:
    """
    Vectorized intrinsic value at expiry.

    Returns (n,) array of max(S - K, 0) for calls or max(K - S, 0) for puts.
    """
    if is_call:
        return np.maximum(spot - strike, 0.0)
    return np.maximum(strike - spot, 0.0)
