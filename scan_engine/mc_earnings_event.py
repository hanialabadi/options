"""
Monte Carlo — Earnings Event Simulation
=========================================

Simulates the expected P&L of holding a directional option through an earnings
announcement vs closing before the event.

Context
-------
Earnings announcements create two opposing forces on long options:
  1. IV crush: implied volatility collapses post-announcement (Augen 0.754)
  2. Price gap: the underlying gaps up/down based on the surprise

For stocks where avg_move_ratio < 0.6 (market overprices earnings moves),
holding through is systematically -EV for option buyers.

Model
-----
GBM with an earnings gap overlay:
  - Pre-earnings: standard GBM diffusion for remaining days
  - Earnings gap: drawn from calibrated distribution using stock's historical
    avg_actual_move_pct and avg_gap_pct from earnings_stats
  - Post-earnings IV: IV × (1 - avg_iv_crush_pct/100)

The simulation compares:
  A. CLOSE_BEFORE: sell at mid-life value (Black-Scholes approx) on day before earnings
  B. HOLD_THROUGH: hold to earnings, apply gap + IV crush, sell post-announcement

Outputs
-------
  MC_Earn_EV_Hold      – expected P&L if holding through ($)
  MC_Earn_EV_Close     – expected P&L if closing before ($)
  MC_Earn_P_Profit     – P(profit if holding through)
  MC_Earn_Verdict      – HOLD_THROUGH | CLOSE_BEFORE | NEUTRAL
  MC_Earn_Edge         – EV difference ($): positive = hold-through is better
  MC_Earn_Note         – human-readable context with track record

References
----------
  Augen Ch.4:      "rising vol before earnings, falling vol immediately after"
  Natenberg Ch.12: "vega risk dominates near-term option positions"
  Sinclair Ch.5:   "variance premium is largest around earnings"
"""

from __future__ import annotations

import logging
import numpy as np
import pandas as pd
from typing import Optional

from core.shared.mc.paths import gbm_terminal, TRADING_DAYS
from core.shared.mc.valuation import brenner_option_value

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────
N_PATHS       = 2_000
SEED          = 42

# Default earnings parameters when stock-specific data unavailable
DEFAULT_AVG_MOVE_PCT     = 0.04    # 4% average earnings move
DEFAULT_AVG_IV_CRUSH_PCT = 30.0    # 30% IV crush
DEFAULT_AVG_GAP_PCT      = 0.03    # 3% average gap


def mc_earnings_event(
    row: pd.Series,
    n_paths: int = N_PATHS,
    rng: Optional[np.random.Generator] = None,
) -> dict:
    """
    Simulate hold-through vs close-before for a directional option near earnings.

    Parameters
    ----------
    row     : pipeline row with spot, strike, DTE, premium, IV, earnings data
    n_paths : number of MC paths
    rng     : numpy random generator (None = seeded default)

    Returns
    -------
    dict with MC_Earn_* keys
    """
    _default = {
        "MC_Earn_EV_Hold":  np.nan,
        "MC_Earn_EV_Close": np.nan,
        "MC_Earn_P_Profit": np.nan,
        "MC_Earn_Verdict":  "SKIP",
        "MC_Earn_Edge":     np.nan,
        "MC_Earn_Note":     "MC_SKIP",
    }

    # ── Resolve inputs ───────────────────────────────────────────────────
    spot = _get_float(row, ("last_price", "Last", "Close", "close", "Spot"))
    if spot is None or spot <= 0:
        _default["MC_Earn_Note"] = "MC_SKIP: no spot price"
        return _default

    strike = _get_float(row, ("Selected_Strike", "Strike", "strike"))
    if strike is None or strike <= 0:
        _default["MC_Earn_Note"] = "MC_SKIP: no strike"
        return _default

    premium = _get_float(row, ("Mid_Price", "Mid", "mid", "Last"))
    if premium is None or premium <= 0:
        _default["MC_Earn_Note"] = "MC_SKIP: no premium"
        return _default

    dte_earn = _get_float(row, ("days_to_earnings",))
    if dte_earn is None or dte_earn < 0 or dte_earn > 30:
        _default["MC_Earn_Note"] = "MC_SKIP: no near-term earnings"
        return _default
    dte_earn = max(1, int(dte_earn))

    # Option type
    strat = str(row.get("Strategy_Name", "") or "").upper().replace("_", " ")
    is_call = "CALL" in strat
    is_put = "PUT" in strat
    if not is_call and not is_put:
        _default["MC_Earn_Note"] = "MC_SKIP: not directional"
        return _default

    # HV for diffusion
    hv = _resolve_hv_simple(row)

    # IV for pre-earnings valuation
    iv = _get_float(row, ("Execution_IV", "iv_30d", "IV30_Call", "IV_Now", "IV_30D"))
    if iv is not None and iv > 1.0:
        iv /= 100.0
    if iv is None or iv <= 0:
        iv = hv  # fallback

    # ── Earnings-specific parameters (from track record) ─────────────────
    avg_move = _get_float(row, ("Earnings_Avg_Actual_Move",)) or DEFAULT_AVG_MOVE_PCT
    if avg_move > 1.0:
        avg_move /= 100.0

    avg_crush = _get_float(row, ("Earnings_Avg_IV_Crush",)) or DEFAULT_AVG_IV_CRUSH_PCT
    # DB stores crush as decimal fraction (0.30 = 30%), but the formula below
    # expects whole-percentage form (30.0) to divide by 100. Normalize.
    if 0 < avg_crush <= 1.0:
        avg_crush *= 100.0

    avg_gap = _get_float(row, ("Earnings_Avg_Gap",)) or DEFAULT_AVG_GAP_PCT
    if avg_gap > 1.0:
        avg_gap /= 100.0

    move_ratio = _get_float(row, ("Earnings_Move_Ratio",))
    beat_rate = _get_float(row, ("Earnings_Beat_Rate",))

    # ── Simulate ─────────────────────────────────────────────────────────
    if rng is None:
        rng = np.random.default_rng(SEED)

    # A. Pre-earnings diffusion (dte_earn - 1 days of normal GBM)
    pre_days = max(0, dte_earn - 1)
    if pre_days > 0:
        spot_pre_earnings = gbm_terminal(spot, hv, pre_days, n_paths, rng)
    else:
        spot_pre_earnings = np.full(n_paths, spot)

    # B. Earnings gap: calibrated from historical moves
    # Direction: beat_rate determines P(up gap) for calls
    p_up = (beat_rate / 100.0) if (beat_rate is not None and beat_rate > 0) else 0.55
    gap_direction = rng.choice([1.0, -1.0], size=n_paths, p=[p_up, 1 - p_up])

    # Magnitude: log-normal around avg_move with some dispersion
    gap_magnitude = np.abs(rng.normal(avg_move, avg_move * 0.5, n_paths))
    earnings_gap = gap_direction * gap_magnitude  # signed gap as fraction

    spot_post_earnings = spot_pre_earnings * (1.0 + earnings_gap)

    # C. IV crush: post-earnings IV = pre-earnings IV × (1 - crush_pct/100)
    iv_post = iv * (1.0 - avg_crush / 100.0)
    iv_post = max(iv_post, 0.05)  # floor

    # ── Compute option P&L for both scenarios ────────────────────────────

    # DTE for option (total DTE, not just to earnings)
    total_dte = _get_float(row, ("DTE", "Actual_DTE", "Target_DTE")) or dte_earn
    total_dte = max(1, total_dte)
    remaining_dte_post = max(1, total_dte - dte_earn)

    # Scenario A: CLOSE_BEFORE (sell on day before earnings at pre-earnings prices)
    # Approximate option value using Brenner-Subrahmanyam from shared valuation
    dte_pre = max(remaining_dte_post + 1, 1)
    option_val_pre = brenner_option_value(spot_pre_earnings, strike, iv, dte_pre, is_call)

    pnl_close = (option_val_pre - premium) * 100  # per contract

    # Scenario B: HOLD_THROUGH (option value post-earnings with IV crush)
    dte_post = max(remaining_dte_post, 1)
    option_val_post = brenner_option_value(spot_post_earnings, strike, iv_post, dte_post, is_call)

    pnl_hold = (option_val_post - premium) * 100  # per contract

    # ── Compute outputs ──────────────────────────────────────────────────
    ev_hold = float(np.mean(pnl_hold))
    ev_close = float(np.mean(pnl_close))
    p_profit = float(np.mean(pnl_hold > 0))
    edge = ev_hold - ev_close

    # Verdict
    if edge > premium * 10:  # hold edge > 10% of premium paid (in $ terms)
        verdict = "HOLD_THROUGH"
    elif edge < -premium * 10:
        verdict = "CLOSE_BEFORE"
    else:
        verdict = "NEUTRAL"

    # Build note with track record context
    note_parts = [f"Earnings in {dte_earn}d"]
    if beat_rate is not None:
        note_parts.append(f"beat_rate={beat_rate:.0f}%")
    if avg_crush > 0:
        note_parts.append(f"avg_crush={avg_crush:.0f}%")
    if move_ratio is not None:
        note_parts.append(f"move_ratio={move_ratio:.2f}")
    note_parts.append(f"EV(hold)=${ev_hold:+.0f} vs EV(close)=${ev_close:+.0f}")
    note_parts.append(f"→ {verdict}")

    return {
        "MC_Earn_EV_Hold":  round(ev_hold, 2),
        "MC_Earn_EV_Close": round(ev_close, 2),
        "MC_Earn_P_Profit": round(p_profit, 4),
        "MC_Earn_Verdict":  verdict,
        "MC_Earn_Edge":     round(edge, 2),
        "MC_Earn_Note":     " | ".join(note_parts),
    }


# ── Helpers ──────────────────────────────────────────────────────────────────

def _get_float(row: pd.Series, cols: tuple) -> Optional[float]:
    """Get first valid float from a list of column names."""
    for col in cols:
        val = row.get(col)
        if val is not None and pd.notna(val):
            try:
                v = float(val)
                if v == v:  # not NaN
                    return v
            except (TypeError, ValueError):
                pass
    return None


def _resolve_hv_simple(row: pd.Series) -> float:
    """Resolve HV from row columns, fallback to 0.30."""
    for col in ("hv_30", "HV_30_D_Cur", "hv_20", "HV_20_D_Cur",
                "IV30_Call", "Execution_IV"):
        val = row.get(col)
        if val is not None and pd.notna(val):
            v = float(val)
            if v > 1.0:
                v /= 100.0
            if 0.01 <= v <= 5.0:
                return v
    return 0.30
