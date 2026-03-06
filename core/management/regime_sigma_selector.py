"""
Regime-Aware Sigma Selector
============================

Replaces the flat EWMA(λ=0.94) sigma fed into Monte Carlo with a
regime-conditioned sigma that reflects *which volatility state we are in*,
not just the most recent trailing observation.

Problem with EWMA alone
-----------------------
EWMA(λ=0.94) has an effective window of ≈17 sessions.  After a regime
transition (e.g. low-vol → high-vol spike), it takes 2–3 sessions for
EWMA to react.  During that lag window, MC under-estimates path dispersion
and the arbitration layer fires HOLD on positions that are already in a
deteriorating vol environment.

MSFT case (Feb 2026): EWMA returned 0.28 while realised vol was already
running at 0.38.  Gate 6 fired ROLL based on regime mismatch; MC said
HOLD based on stale sigma.  Both answers were technically correct given
their inputs — the inputs were wrong.

Solution
--------
2-state Hidden Markov Model on the EWMA vol series.

States:
  - State 0: LOW_VOL  — σ_low  (typically 15–25% ann.)
  - State 1: HIGH_VOL — σ_high (typically 35–55% ann.)

The HMM decodes the Viterbi sequence and returns the *most likely current
regime* and the *regime-conditioned sigma* for that state.

Sigma construction:
  σ_regime = σ_state_mean (long-run mean of that HMM state)
  σ_blend  = 0.70 × σ_regime + 0.30 × σ_ewma   (anchored to recency)

This blended sigma is:
  - Faster than EWMA at regime transitions (the 0.70 weight uses the state
    mean, which captures the full regime distribution)
  - More stable than pure σ_state (the 0.30 EWMA anchor prevents the state
    mean from persisting after a regime exit)

Returns
-------
RegimeSigmaResult namedtuple:
  sigma        – blended annualised sigma (decimal) to feed MC
  sigma_ewma   – raw EWMA sigma (for comparison/audit)
  sigma_regime – state mean sigma (for display)
  regime       – 'LOW_VOL' | 'HIGH_VOL' | 'UNKNOWN'
  regime_prob  – P(current state) from HMM posterior
  n_sessions   – number of sessions used
  source       – 'HMM_BLEND' | 'EWMA_FALLBACK' | 'STATIC_FALLBACK'
  note         – human-readable audit string

Fallback chain
--------------
1. HMM fit on ≥30 EWMA vol observations  → source = 'HMM_BLEND'
2. EWMA available but < 30 obs           → source = 'EWMA_FALLBACK'
3. No price history                      → source = 'STATIC_FALLBACK' (0.25)

References
----------
  Ernie Chan – Quantitative Trading (2008): Ch.6 — regime-switching models
    for volatility; 2-state HMM with EM estimation.
    "Regime-switching models are of great value to options traders because
    the regime — not the point estimate of volatility — determines which
    strategy is optimal."

  Jim Gatheral – The Volatility Surface (2006): sticky-strike vs sticky-delta
    regimes; regime determines valid vol surface interpolation for Greeks.

  Hull – Options, Futures, and Other Derivatives (2022): Ch.22 — GARCH/EWMA
    as regime-blind estimators; limitation noted explicitly.

  Natenberg – Option Volatility and Pricing (2014): Ch.12 — vol forecast
    quality directly impacts MC accuracy and position sizing.

Dependencies
------------
  hmmlearn   — pip install hmmlearn   (lightweight, no torch required)
  numpy      — already in requirements
  scan_engine.ewma_vol — already in codebase

hmmlearn is optional: if absent, falls back to EWMA gracefully.
"""

from __future__ import annotations

import logging
import warnings
from typing import Optional, NamedTuple

import numpy as np

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
BLEND_REGIME_WEIGHT = 0.70   # weight on HMM state mean
BLEND_EWMA_WEIGHT   = 0.30   # weight on EWMA point estimate
MIN_HMM_OBS         = 30     # minimum EWMA observations for HMM fit
STATIC_FALLBACK_VOL = 0.25   # 25% — conservative management fallback
TRADING_DAYS        = 252

# Import EWMA series function — same path resolution as mc_management.py
try:
    import sys as _sys
    import os as _os
    _repo = _os.path.dirname(
        _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
    )
    if _repo not in _sys.path:
        _sys.path.insert(0, _repo)
    from scan_engine.ewma_vol import (
        ewma_vol as _ewma_vol_scalar,
        ewma_vol_series as _ewma_vol_series,
        _load_close_prices,
    )
    _EWMA_AVAILABLE = True
except Exception:
    _EWMA_AVAILABLE = False
    _ewma_vol_scalar = None   # type: ignore[assignment]
    _ewma_vol_series = None   # type: ignore[assignment]
    _load_close_prices = None # type: ignore[assignment]

# hmmlearn import — optional
try:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        from hmmlearn.hmm import GaussianHMM as _GaussianHMM
    _HMM_AVAILABLE = True
except ImportError:
    _HMM_AVAILABLE = False
    _GaussianHMM = None  # type: ignore[assignment]


# ── Result type ───────────────────────────────────────────────────────────────

class RegimeSigmaResult(NamedTuple):
    sigma:        float   # blended sigma to use in MC
    sigma_ewma:   float   # raw EWMA sigma
    sigma_regime: float   # HMM state mean sigma (NaN if HMM not used)
    regime:       str     # 'LOW_VOL' | 'HIGH_VOL' | 'UNKNOWN'
    regime_prob:  float   # P(current state), NaN if HMM not used
    n_sessions:   int     # price history sessions used
    source:       str     # 'HMM_BLEND' | 'EWMA_FALLBACK' | 'STATIC_FALLBACK'
    note:         str     # audit string


# ── Internal helpers ───────────────────────────────────────────────────────────

def _fit_hmm_2state(vol_series: np.ndarray) -> Optional[dict]:
    """
    Fit a 2-state Gaussian HMM on the EWMA vol series.

    vol_series: 1-D array of annualised EWMA vol values (oldest → newest).
    Returns dict with: state_means, state_stds, current_state, state_prob
    Returns None on any fit failure.
    """
    if not _HMM_AVAILABLE or _GaussianHMM is None:
        return None

    n = len(vol_series)
    if n < MIN_HMM_OBS:
        return None

    X = vol_series.reshape(-1, 1)

    try:
        import io
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")   # suppress Python UserWarning
            # hmmlearn prints convergence warnings to stderr directly;
            # redirect to /dev/null for the fit call only.
            import os as _os
            _devnull = open(_os.devnull, "w")
            import sys as _sys
            _old_stderr = _sys.stderr
            _sys.stderr = _devnull
            try:
                model = _GaussianHMM(
                    n_components=2,
                    covariance_type="diag",
                    n_iter=200,
                    tol=1e-3,
                    random_state=42,
                )
                model.fit(X)
            finally:
                _sys.stderr = _old_stderr
                _devnull.close()

        # Viterbi decode to get most-likely state sequence
        state_seq = model.predict(X)
        current_state = int(state_seq[-1])

        # Posterior probabilities for the last observation
        log_posteriors = model.predict_proba(X)
        state_prob = float(log_posteriors[-1, current_state])

        # State means (annualised sigma per state)
        means = model.means_.flatten()           # shape (2,)
        stds  = np.sqrt(model.covars_.flatten()) # shape (2,)

        # Label states: state with lower mean = LOW_VOL, higher = HIGH_VOL
        low_state  = int(np.argmin(means))
        high_state = int(np.argmax(means))

        return {
            "state_means":    means,            # [state0_mean, state1_mean]
            "state_stds":     stds,
            "current_state":  current_state,
            "state_prob":     state_prob,
            "low_state":      low_state,
            "high_state":     high_state,
            "n_states":       2,
        }
    except Exception as exc:
        logger.debug(f"HMM fit failed: {exc}")
        return None


# ── Public API ─────────────────────────────────────────────────────────────────

def regime_sigma(
    ticker: str,
    db_path: str = "data/pipeline.duckdb",
) -> RegimeSigmaResult:
    """
    Return regime-aware sigma for `ticker`.

    This is the drop-in replacement for ewma_vol(ticker).  Instead of
    returning a single EWMA scalar, it returns a RegimeSigmaResult with
    a blended sigma that is conditioned on the current HMM volatility regime.

    Parameters
    ----------
    ticker  : equity ticker symbol
    db_path : path to pipeline.duckdb (default matches ewma_vol.py)

    Returns
    -------
    RegimeSigmaResult namedtuple — always returns a valid result (never raises).
    Use result.sigma as the drop-in for the EWMA scalar.
    """
    _unknown = RegimeSigmaResult(
        sigma=STATIC_FALLBACK_VOL,
        sigma_ewma=STATIC_FALLBACK_VOL,
        sigma_regime=float("nan"),
        regime="UNKNOWN",
        regime_prob=float("nan"),
        n_sessions=0,
        source="STATIC_FALLBACK",
        note=f"regime_sigma({ticker}): no price history → static fallback {STATIC_FALLBACK_VOL:.0%}",
    )

    if not _EWMA_AVAILABLE:
        _unknown = _unknown._replace(note=f"regime_sigma({ticker}): EWMA module unavailable → static fallback")
        return _unknown

    # ── Step 1: Load EWMA vol series (use full history for richer HMM fit) ───
    # ewma_vol_series() defaults to 60 closes → 49 observations after warmup.
    # We load all available closes (up to 127) for a richer state separation.
    try:
        closes = _load_close_prices(ticker, n_sessions=127, db_path=db_path)
        if closes is not None and len(closes) >= 11:
            vol_series = _ewma_vol_series(ticker, db_path=db_path)
            # Rebuild series on full closes if ewma_vol_series only uses 60
            from scan_engine.ewma_vol import ewma_vol_from_array, MIN_SESSIONS
            import numpy as _np
            if len(closes) > 60:
                # Compute full EWMA series on all available closes
                log_rets = _np.diff(_np.log(closes.astype(float)))
                n = len(log_rets)
                var = float(_np.var(log_rets[:MIN_SESSIONS], ddof=1))
                _series = []
                for i in range(MIN_SESSIONS, n):
                    var = 0.94 * var + 0.06 * log_rets[i] ** 2
                    _series.append(float(_np.sqrt(var)) * _np.sqrt(252))
                vol_series = _np.array(_series) if _series else vol_series
        else:
            vol_series = _ewma_vol_series(ticker, db_path=db_path)
    except Exception as exc:
        logger.debug(f"regime_sigma: vol series load failed for {ticker}: {exc}")
        try:
            vol_series = _ewma_vol_series(ticker, db_path=db_path)
        except Exception:
            return _unknown

    if vol_series is None or len(vol_series) == 0:
        return _unknown

    # EWMA scalar = last point in series
    sigma_ewma = float(vol_series[-1])
    n_sessions = len(vol_series)

    # ── Step 2: Attempt HMM fit ───────────────────────────────────────────────
    hmm_result = _fit_hmm_2state(vol_series) if _HMM_AVAILABLE else None

    if hmm_result is None:
        # Fallback: return raw EWMA
        return RegimeSigmaResult(
            sigma=sigma_ewma,
            sigma_ewma=sigma_ewma,
            sigma_regime=float("nan"),
            regime="UNKNOWN",
            regime_prob=float("nan"),
            n_sessions=n_sessions,
            source="EWMA_FALLBACK",
            note=(
                f"regime_sigma({ticker}): HMM unavailable (n={n_sessions}) → "
                f"EWMA σ={sigma_ewma*100:.1f}%"
            ),
        )

    # ── Step 3: Extract regime from HMM ──────────────────────────────────────
    current_state = hmm_result["current_state"]
    low_state     = hmm_result["low_state"]
    high_state    = hmm_result["high_state"]
    state_means   = hmm_result["state_means"]
    state_prob    = hmm_result["state_prob"]

    sigma_regime = float(state_means[current_state])
    regime_label = "LOW_VOL" if current_state == low_state else "HIGH_VOL"

    # Sanity: sigma_regime must be a plausible vol value
    if not (0.01 <= sigma_regime <= 5.0):
        logger.debug(f"regime_sigma({ticker}): HMM state mean {sigma_regime:.3f} out of range → EWMA fallback")
        return RegimeSigmaResult(
            sigma=sigma_ewma,
            sigma_ewma=sigma_ewma,
            sigma_regime=sigma_regime,
            regime=regime_label,
            regime_prob=state_prob,
            n_sessions=n_sessions,
            source="EWMA_FALLBACK",
            note=(
                f"regime_sigma({ticker}): HMM state mean {sigma_regime:.3f} implausible → "
                f"EWMA σ={sigma_ewma*100:.1f}%"
            ),
        )

    # ── Step 4: Blend ─────────────────────────────────────────────────────────
    sigma_blend = BLEND_REGIME_WEIGHT * sigma_regime + BLEND_EWMA_WEIGHT * sigma_ewma

    # Clamp to sensible range
    sigma_blend = float(np.clip(sigma_blend, 0.05, 3.0))

    state_means_str = " | ".join(
        f"{'LOW' if i == low_state else 'HIGH'}={state_means[i]*100:.1f}%"
        for i in range(2)
    )

    note = (
        f"regime_sigma({ticker}): {regime_label} (p={state_prob:.0%}) | "
        f"σ_regime={sigma_regime*100:.1f}% | σ_ewma={sigma_ewma*100:.1f}% | "
        f"σ_blend={sigma_blend*100:.1f}% [states: {state_means_str}] "
        f"n={n_sessions}sess → HMM_BLEND"
    )

    return RegimeSigmaResult(
        sigma=sigma_blend,
        sigma_ewma=sigma_ewma,
        sigma_regime=sigma_regime,
        regime=regime_label,
        regime_prob=state_prob,
        n_sessions=n_sessions,
        source="HMM_BLEND",
        note=note,
    )
