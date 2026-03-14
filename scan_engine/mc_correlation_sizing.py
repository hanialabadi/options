"""
Monte Carlo — Correlation-Aware Position Sizing
================================================

Adjusts MC_Max_Contracts based on portfolio correlation exposure.

Problem
-------
Standard MC sizes each position independently. 5 long calls on AAPL, MSFT,
GOOGL, AMZN, META look individually sized at 2% risk each, but they're all
>0.7 correlated to each other. A broad tech selloff hits all 5 simultaneously.

Solution
--------
After individual MC sizing, check if the new candidate is correlated with
existing portfolio holdings. If avg correlation > threshold, scale down
max contracts proportionally.

Outputs
-------
  MC_Corr_Adjustment    – multiplier applied to MC_Max_Contracts (0.5-1.0)
  MC_Corr_Overlap       – number of existing positions with corr > 0.5
  MC_Corr_Avg           – average correlation with existing holdings
  MC_Corr_Note          – human-readable context

References
----------
  Pedersen Ch.7: "Continue to resize positions according to risk and conviction"
  Hull Ch.22:    "Diversification reduces VaR when correlations are < 1"
  McMillan Ch.3: "Position sizing must consider existing portfolio exposure"
"""

from __future__ import annotations

import logging
import numpy as np
import pandas as pd
from typing import Optional

logger = logging.getLogger(__name__)

# Thresholds
CORR_HIGH      = 0.70    # High correlation — significant overlap
CORR_MODERATE  = 0.50    # Moderate correlation — some overlap
MAX_OVERLAP    = 5        # Beyond this many correlated positions, hard cap


def mc_correlation_adjustment(
    candidate_ticker: str,
    existing_tickers: list[str],
    mc_max_contracts: int,
) -> dict:
    """
    Compute correlation-adjusted position size for a new candidate.

    Parameters
    ----------
    candidate_ticker  : ticker being considered for new position
    existing_tickers  : list of tickers currently in portfolio
    mc_max_contracts  : individual MC-computed max contracts

    Returns
    -------
    dict with MC_Corr_* keys and adjusted MC_Max_Contracts
    """
    _default = {
        "MC_Corr_Adjustment":    1.0,
        "MC_Corr_Overlap":       0,
        "MC_Corr_Avg":           0.0,
        "MC_Corr_Note":          "",
        "MC_Corr_Max_Contracts": mc_max_contracts,
    }

    if not existing_tickers or mc_max_contracts <= 0:
        return _default

    # Remove duplicate of candidate from existing
    existing = [t for t in existing_tickers if t != candidate_ticker]
    if not existing:
        return _default

    # ── Compute pairwise correlations ────────────────────────────────────
    correlations = _compute_correlations(candidate_ticker, existing)

    if not correlations:
        return _default

    # ── Count overlapping positions ──────────────────────────────────────
    high_corr = [c for c in correlations.values() if c >= CORR_HIGH]
    mod_corr = [c for c in correlations.values() if c >= CORR_MODERATE]
    overlap = len(mod_corr)
    avg_corr = float(np.mean(list(correlations.values()))) if correlations else 0.0

    # ── Compute adjustment factor ────────────────────────────────────────
    if overlap == 0:
        adjustment = 1.0
    else:
        # Scale down based on number of correlated positions and avg correlation
        # More correlated positions → smaller size per position
        # Formula: 1 / (1 + n_overlap × avg_corr)
        # Examples: 1 overlap at 0.7 → 0.59, 3 overlaps at 0.8 → 0.29
        adjustment = 1.0 / (1.0 + overlap * avg_corr)
        adjustment = max(adjustment, 0.25)  # Floor: never below 25% of MC size

    adjusted_contracts = max(1, int(mc_max_contracts * adjustment))

    # ── Build note ───────────────────────────────────────────────────────
    note_parts = []
    if len(high_corr) > 0:
        note_parts.append(
            f"{len(high_corr)} positions with corr>{CORR_HIGH:.0%}"
        )
    if overlap > 0 and adjustment < 1.0:
        note_parts.append(
            f"sizing reduced to {adjustment:.0%} ({mc_max_contracts}→{adjusted_contracts} contracts)"
        )
    if avg_corr > CORR_HIGH:
        note_parts.append(f"avg_corr={avg_corr:.2f} — concentrated exposure")

    return {
        "MC_Corr_Adjustment":    round(adjustment, 4),
        "MC_Corr_Overlap":       overlap,
        "MC_Corr_Avg":           round(avg_corr, 4),
        "MC_Corr_Note":          " | ".join(note_parts) if note_parts else "",
        "MC_Corr_Max_Contracts": adjusted_contracts,
    }


def _compute_correlations(
    candidate: str,
    existing: list[str],
) -> dict[str, float]:
    """
    Compute pairwise correlations between candidate and existing tickers.
    Uses 60-day returns from price_history DuckDB.
    """
    try:
        from core.shared.data_layer.price_history_loader import load_price_history

        # Load candidate returns
        df_cand, _ = load_price_history(candidate, days=60, skip_auto_fetch=True)
        if df_cand is None or len(df_cand) < 20:
            return {}

        close_col = next((c for c in ("Close", "close", "Adj Close") if c in df_cand.columns), None)
        if close_col is None:
            return {}
        cand_returns = df_cand[close_col].pct_change().dropna().values

        correlations = {}
        for ticker in existing:
            try:
                df_t, _ = load_price_history(ticker, days=60, skip_auto_fetch=True)
                if df_t is None or len(df_t) < 20:
                    continue
                t_col = next((c for c in ("Close", "close", "Adj Close") if c in df_t.columns), None)
                if t_col is None:
                    continue
                t_returns = df_t[t_col].pct_change().dropna().values

                # Align lengths
                min_len = min(len(cand_returns), len(t_returns))
                if min_len < 10:
                    continue

                corr = np.corrcoef(cand_returns[-min_len:], t_returns[-min_len:])[0, 1]
                if np.isfinite(corr):
                    correlations[ticker] = float(corr)
            except Exception:
                continue

        return correlations

    except Exception as e:
        logger.debug(f"Correlation computation failed: {e}")
        return {}
