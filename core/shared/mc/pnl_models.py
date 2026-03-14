"""
Terminal P&L models for the unified MC engine.

Each model computes per-share P&L at expiry given terminal prices.
The caller (MCEngine) handles contract multiplier (x100) and carry.

Unifies 2 independent P&L implementations:
  - scan_engine/mc_position_sizing.py: simulate_pnl_paths()
  - core/management/mc_management.py: mc_roll_ev_comparison()
"""

from __future__ import annotations

import numpy as np


# ── Individual P&L models (per share) ─────────────────────────────────────────

def long_option_pnl(s_terminal: np.ndarray, strike: float,
                    premium: float, is_call: bool) -> np.ndarray:
    """
    Long option P&L at expiry (per share).

    P&L = intrinsic - premium_paid
    Max loss = -premium (debit position)
    """
    if is_call:
        intrinsic = np.maximum(s_terminal - strike, 0.0)
    else:
        intrinsic = np.maximum(strike - s_terminal, 0.0)
    return intrinsic - premium


def short_put_pnl(s_terminal: np.ndarray, strike: float,
                  premium: float) -> np.ndarray:
    """
    Short put / CSP P&L at expiry (per share).

    P&L = premium_received - assignment_cost
    Max gain = premium (all paths OTM)
    """
    loss_on_assign = np.maximum(strike - s_terminal, 0.0)
    return premium - loss_on_assign


def short_call_pnl(s_terminal: np.ndarray, strike: float,
                   premium: float) -> np.ndarray:
    """
    Short call P&L at expiry (per share). Used for naked short calls.

    P&L = premium_received - call_assignment_cost
    """
    loss_on_assign = np.maximum(s_terminal - strike, 0.0)
    return premium - loss_on_assign


def stock_plus_short_call_pnl(s_terminal: np.ndarray, strike: float,
                              premium: float, cost_basis: float,
                              n_shares: float = 100.0) -> np.ndarray:
    """
    Buy-write / covered call P&L at expiry (per contract, NOT per share).

    Combined position: long stock + short call.
    P&L = (S_T - cost_basis + premium - call_intrinsic) * n_shares
    """
    call_intrinsic = np.maximum(s_terminal - strike, 0.0)
    pnl_per_share = (s_terminal - cost_basis) + premium - call_intrinsic
    return pnl_per_share * n_shares


def pmcc_pnl(s_terminal: np.ndarray, short_strike: float,
             leap_strike: float, net_debit: float) -> np.ndarray:
    """
    PMCC (Poor Man's Covered Call) P&L at short-call expiry (per share).

    Long deep-ITM LEAP call + short near-term OTM call.
    Spread value = LEAP_intrinsic - short_call_liability
    P&L = spread_value - net_debit
    Max loss = net_debit
    Max gain = (short_strike - leap_strike) - net_debit
    """
    leap_intrinsic = np.maximum(s_terminal - leap_strike, 0.0)
    short_liability = np.maximum(s_terminal - short_strike, 0.0)
    spread_value = leap_intrinsic - short_liability
    return spread_value - net_debit


# ── Dispatch ──────────────────────────────────────────────────────────────────

def compute_terminal_pnl(model: str, s_terminal: np.ndarray,
                         strike: float, premium: float,
                         is_call: bool = True,
                         cost_basis: float = 0.0,
                         leap_strike: float = 0.0,
                         net_debit: float = 0.0,
                         n_shares: float = 100.0) -> np.ndarray:
    """
    Dispatch to the correct P&L model.

    Parameters
    ----------
    model : P&L model key from StrategyProfile.pnl_model
    s_terminal : (n,) terminal prices
    strike : option strike
    premium : entry premium (per share)
    is_call : True for calls
    cost_basis : stock entry price (BW/CC only)
    leap_strike : LEAP strike (PMCC only)
    net_debit : total spread debit (PMCC only)
    n_shares : shares per contract (BW/CC only)

    Returns
    -------
    (n,) P&L array. Per-share for option-only models,
    per-contract for stock_plus_short_call.
    """
    if model == "long_option":
        return long_option_pnl(s_terminal, strike, premium, is_call)

    if model == "short_put":
        return short_put_pnl(s_terminal, strike, premium)

    if model == "short_call":
        return short_call_pnl(s_terminal, strike, premium)

    if model == "stock_plus_short_call":
        effective_basis = cost_basis if cost_basis > 0 else strike
        return stock_plus_short_call_pnl(
            s_terminal, strike, premium, effective_basis, n_shares
        )

    if model == "pmcc":
        effective_debit = net_debit if net_debit > 0 else premium
        effective_leap = leap_strike if leap_strike > 0 else strike * 0.80
        return pmcc_pnl(s_terminal, strike, effective_leap, effective_debit)

    # Fallback: treat as long option
    return long_option_pnl(s_terminal, strike, premium, is_call)
