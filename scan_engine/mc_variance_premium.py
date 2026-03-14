"""
Monte Carlo — Variance Premium Scoring
========================================

Estimates whether the option buyer is overpaying relative to the realized
volatility distribution.

Problem
-------
IV_Headwind flags high IV_Rank but doesn't distinguish:
  A. IV is high because HV is high → justified premium
  B. IV is high but HV is normal → variance premium is inflated → overpaying

The variance premium (IV - RV) is the "insurance markup" priced into options.
When it's high, selling options is +EV; when low, buying options has better edge.

Model
-----
For a long directional option:
  1. Simulate N price paths using HV (realized vol, not implied)
  2. Compute option payoff at each path's terminal price
  3. Expected payoff = mean(max(S_T - K, 0)) for calls
  4. Compare expected payoff to premium paid
  5. Ratio = Expected_Payoff / Premium → >1.0 means option is cheap

Outputs
-------
  MC_VP_Score          – Expected_Payoff / Premium_Paid (>1 = cheap, <1 = expensive)
  MC_VP_Edge           – expected excess return per contract ($)
  MC_VP_Premium_Fair   – MC-implied fair premium using HV
  MC_VP_Verdict        – CHEAP | FAIR | EXPENSIVE
  MC_VP_Note           – human-readable context

References
----------
  Sinclair Ch.5:   "Variance premium is the single most important empirical fact"
  Natenberg Ch.20: "IV as predictor — subtract variance premium for forecast"
  Bennett Ch.1:    "Short-dated implied volatility has historically been overpriced"
"""

from __future__ import annotations

import logging
import numpy as np
import pandas as pd
from typing import Optional

from core.shared.mc.paths import gbm_terminal, TRADING_DAYS

logger = logging.getLogger(__name__)

N_PATHS      = 2_000
HV_FALLBACK  = 0.30
SEED         = 42


def mc_variance_premium(
    row: pd.Series,
    n_paths: int = N_PATHS,
    rng: Optional[np.random.Generator] = None,
) -> dict:
    """
    Score the variance premium for a directional option candidate.

    Parameters
    ----------
    row     : pipeline row with spot, strike, DTE, premium, HV, IV
    n_paths : number of MC paths
    rng     : numpy random generator

    Returns
    -------
    dict with MC_VP_* keys
    """
    _default = {
        "MC_VP_Score":        np.nan,
        "MC_VP_Edge":         np.nan,
        "MC_VP_Premium_Fair": np.nan,
        "MC_VP_Verdict":      "SKIP",
        "MC_VP_Note":         "MC_SKIP",
    }

    # ── Resolve inputs ───────────────────────────────────────────────────
    spot = _get_float(row, ("last_price", "Last", "Close", "close"))
    if spot is None or spot <= 0:
        _default["MC_VP_Note"] = "MC_SKIP: no spot"
        return _default

    strike = _get_float(row, ("Selected_Strike", "Strike", "strike"))
    if strike is None or strike <= 0:
        _default["MC_VP_Note"] = "MC_SKIP: no strike"
        return _default

    premium = _get_float(row, ("Mid_Price", "Mid", "mid"))
    if premium is None or premium <= 0:
        _default["MC_VP_Note"] = "MC_SKIP: no premium"
        return _default

    dte = _get_float(row, ("DTE", "Actual_DTE", "Target_DTE", "Min_DTE"))
    if dte is None or dte < 1:
        _default["MC_VP_Note"] = "MC_SKIP: no DTE"
        return _default

    strat = str(row.get("Strategy_Name", "") or "").upper().replace("_", " ")
    is_call = "CALL" in strat
    is_put = "PUT" in strat
    if not is_call and not is_put:
        _default["MC_VP_Note"] = "MC_SKIP: not directional"
        return _default

    # ── Resolve HV (realized vol) — this is the key input ────────────────
    hv = HV_FALLBACK
    hv_source = "FALLBACK"
    for col in ("hv_30", "HV_30_D_Cur", "hv_20", "HV_20_D_Cur"):
        val = row.get(col)
        if val is not None and pd.notna(val):
            v = float(val)
            if v > 1.0:
                v /= 100.0
            if 0.01 <= v <= 5.0:
                hv = v
                hv_source = col
                break

    # Also get IV for comparison
    iv = hv
    iv_source = hv_source
    for col in ("Execution_IV", "iv_30d", "IV30_Call"):
        val = row.get(col)
        if val is not None and pd.notna(val):
            v = float(val)
            if v > 1.0:
                v /= 100.0
            if 0.01 <= v <= 5.0:
                iv = v
                iv_source = col
                break

    # ── Simulate using HV (not IV) ──────────────────────────────────────
    if rng is None:
        rng = np.random.default_rng(SEED)

    dte_int = max(int(dte), 1)
    s_terminal = gbm_terminal(spot, hv, dte_int, n_paths, rng)

    # ── Compute expected payoff ──────────────────────────────────────────
    if is_call:
        payoffs = np.maximum(s_terminal - strike, 0)
    else:
        payoffs = np.maximum(strike - s_terminal, 0)

    expected_payoff = float(np.mean(payoffs))
    fair_premium = expected_payoff  # risk-neutral fair value using HV

    # ── Score: ratio of fair value to market price ───────────────────────
    if premium > 0:
        vp_score = fair_premium / premium
    else:
        vp_score = np.nan

    edge = (fair_premium - premium) * 100  # per contract

    # ── Verdict ──────────────────────────────────────────────────────────
    if vp_score > 1.15:
        verdict = "CHEAP"      # Market underprices relative to realized vol
    elif vp_score < 0.75:
        verdict = "EXPENSIVE"  # Significant variance premium markup
    else:
        verdict = "FAIR"

    # ── Variance premium magnitude ───────────────────────────────────────
    vp_pct = ((iv - hv) / hv * 100) if hv > 0 else 0

    note_parts = [
        f"HV={hv:.1%}({hv_source}) vs IV={iv:.1%}({iv_source})",
        f"VP={vp_pct:+.0f}%",
        f"fair=${fair_premium:.2f} vs market=${premium:.2f}",
        f"→ {verdict}",
    ]

    return {
        "MC_VP_Score":        round(vp_score, 4) if pd.notna(vp_score) else np.nan,
        "MC_VP_Edge":         round(edge, 2),
        "MC_VP_Premium_Fair": round(fair_premium, 4),
        "MC_VP_Verdict":      verdict,
        "MC_VP_Note":         " | ".join(note_parts),
    }


# ── Helpers ──────────────────────────────────────────────────────────────────

def _get_float(row: pd.Series, cols: tuple) -> Optional[float]:
    for col in cols:
        val = row.get(col)
        if val is not None and pd.notna(val):
            try:
                v = float(val)
                if v == v:
                    return v
            except (TypeError, ValueError):
                pass
    return None
