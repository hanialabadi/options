"""
EWMA Volatility Forecast
=========================

Exponentially Weighted Moving Average (RiskMetrics λ=0.94) volatility
estimator. Gives a forward-leaning sigma that feeds MC simulation more
accurately than a flat backward-looking HV window.

Why EWMA over flat HV
---------------------
Flat HV_30 = equal weight to all 30 sessions. A vol spike 28 days ago
weighs the same as yesterday's session.

EWMA weights recent sessions exponentially: yesterday contributes λ^0=1,
two days ago λ^1=0.94, three days ago λ^2=0.8836, etc.

Effect on MC:
  - Vol expansion in progress → EWMA > HV_30 → MC uses higher sigma
    → P10/CVaR worsens → fewer contracts sized → protects from over-sizing
    into a vol spike.
  - Post-earnings vol crush → EWMA < HV_30 → MC uses lower sigma
    → P10/CVaR improves → more contracts sized → captures post-crush entry.

λ = 0.94 (RiskMetrics standard, J.P. Morgan 1994).
  - λ=0.94: effective window ≈ 1/(1-0.94) = 16.7 sessions
  - λ=0.97: ≈ 33 sessions (slower — closer to HV_30)
  - λ=0.90: ≈ 10 sessions (faster — very reactive)

Minimum history: 10 sessions to compute a stable estimate.
Below 10 sessions → returns None (caller falls back to flat HV).

Data source: price_history table in pipeline.duckdb (close_price, daily).
If DuckDB unavailable or insufficient history → returns None gracefully.

References
----------
  J.P. Morgan RiskMetrics Technical Document (1994): λ=0.94 standard
  Hull Ch.22: EWMA as special case of GARCH(1,1) with ω=0, α=1-λ, β=λ
  Natenberg Ch.12: vol forecast quality directly impacts MC accuracy
"""

from __future__ import annotations

import logging
import numpy as np
from typing import Optional

from core.shared.data_layer.duckdb_utils import get_domain_connection, DbDomain

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
LAMBDA       = 0.94    # RiskMetrics decay factor
TRADING_DAYS = 252
MIN_SESSIONS = 10      # minimum returns needed for a stable EWMA estimate


def _load_close_prices(ticker: str, n_sessions: int = 60,
                       db_path: str = None) -> Optional[np.ndarray]:
    """
    Load the most recent `n_sessions` daily close prices for `ticker`
    from price_history table (CHART domain).

    Returns np.ndarray of closes (oldest→newest), or None on any failure.
    """
    try:
        con = get_domain_connection(DbDomain.CHART, read_only=True)
        rows = con.execute(
            """
            SELECT close_price
            FROM   price_history
            WHERE  ticker = ?
              AND  close_price IS NOT NULL
              AND  close_price > 0
            ORDER  BY date DESC
            LIMIT  ?
            """,
            [ticker, n_sessions],
        ).fetchall()
        con.close()
        if not rows or len(rows) < MIN_SESSIONS:
            return None
        # rows are newest-first; reverse to oldest-first for return computation
        closes = np.array([r[0] for r in reversed(rows)], dtype=float)
        return closes
    except Exception as exc:
        logger.debug(f"EWMA: price_history load failed for {ticker}: {exc}")
        return None


def ewma_vol(
    ticker: str,
    lam: float = LAMBDA,
    annualise: bool = True,
    db_path: str = None,
) -> Optional[float]:
    """
    Compute EWMA volatility for `ticker` from price_history (CHART domain).

    Parameters
    ----------
    ticker    : equity ticker (e.g. 'AAPL')
    lam       : EWMA decay factor (default 0.94 = RiskMetrics)
    annualise : if True, return annualised vol (× √252); else daily
    db_path   : deprecated — ignored, uses domain connection

    Returns
    -------
    float  — annualised vol as decimal (e.g. 0.285 for 28.5%)
    None   — if < MIN_SESSIONS history available or any error
    """
    closes = _load_close_prices(ticker, n_sessions=60)
    if closes is None or len(closes) < MIN_SESSIONS + 1:
        return None

    # Log returns: r_t = ln(S_t / S_{t-1})
    log_rets = np.diff(np.log(closes))
    n = len(log_rets)

    # EWMA variance initialised at simple variance of first MIN_SESSIONS returns
    # (warm-start avoids the cold-start bias from σ²_0 = r_0² only)
    var = float(np.var(log_rets[:MIN_SESSIONS], ddof=1))

    # Recursive: σ²_t = λ·σ²_{t-1} + (1-λ)·r²_t
    for i in range(MIN_SESSIONS, n):
        var = lam * var + (1.0 - lam) * log_rets[i] ** 2

    daily_vol = float(np.sqrt(var))
    if annualise:
        return daily_vol * np.sqrt(TRADING_DAYS)
    return daily_vol


def ewma_vol_series(
    ticker: str,
    lam: float = LAMBDA,
    db_path: str = None,
) -> Optional[np.ndarray]:
    """
    Return the full EWMA vol series (annualised, oldest→newest).
    Used for diagnostics and Markov state history.

    Returns np.ndarray or None.
    """
    closes = _load_close_prices(ticker, n_sessions=60)
    if closes is None or len(closes) < MIN_SESSIONS + 1:
        return None

    log_rets = np.diff(np.log(closes))
    n = len(log_rets)
    var = float(np.var(log_rets[:MIN_SESSIONS], ddof=1))

    series = []
    for i in range(MIN_SESSIONS, n):
        var = lam * var + (1.0 - lam) * log_rets[i] ** 2
        series.append(float(np.sqrt(var)) * np.sqrt(TRADING_DAYS))

    return np.array(series) if series else None


def ewma_vol_from_array(
    closes: np.ndarray,
    lam: float = LAMBDA,
    annualise: bool = True,
) -> Optional[float]:
    """
    Compute EWMA vol from a pre-loaded closes array (oldest→newest).
    Used when close prices are already in memory (avoids repeated DB reads).

    Returns float or None.
    """
    if closes is None or len(closes) < MIN_SESSIONS + 1:
        return None
    log_rets = np.diff(np.log(closes.astype(float)))
    n = len(log_rets)
    var = float(np.var(log_rets[:MIN_SESSIONS], ddof=1))
    for i in range(MIN_SESSIONS, n):
        var = lam * var + (1.0 - lam) * log_rets[i] ** 2
    daily = float(np.sqrt(var))
    return daily * np.sqrt(TRADING_DAYS) if annualise else daily
