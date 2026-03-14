"""
Monte Carlo — Portfolio VaR (Correlated Multi-Position Stress Test)
===================================================================

Simulates correlated price paths for ALL positions simultaneously to compute
portfolio-level risk metrics that per-position MC cannot capture.

Problem
-------
Per-position MC (mc_management.py) treats each position independently.
5 long calls on correlated tech stocks look "fine" individually but represent
concentrated directional risk. Portfolio VaR captures this.

Model
-----
Correlated GBM using Cholesky decomposition of the historical return
correlation matrix:
  1. Fetch 60-day returns for all unique tickers from price_history DuckDB
  2. Compute pairwise correlation matrix
  3. Cholesky decompose → L such that L @ L.T = correlation matrix
  4. Generate independent Z ~ N(0,1) per ticker per path, transform: Z_corr = L @ Z
  5. GBM paths using correlated Z's
  6. Compute per-position option P&L, sum across portfolio

Outputs (portfolio-level, single row)
--------------------------------------
  Portfolio_VaR_5pct       – 5th percentile portfolio P&L ($)
  Portfolio_CVaR_5pct      – expected loss in worst 5% of scenarios ($)
  Portfolio_P50            – median portfolio P&L ($)
  Portfolio_P95            – 95th percentile portfolio P&L ($)
  Portfolio_Max_Drawdown   – worst single-scenario loss ($)
  Portfolio_Concentration  – Herfindahl index of notional exposure (0-1)
  Portfolio_Corr_Risk      – avg pairwise correlation × concentration score
  Portfolio_Stress_SPY_5   – portfolio P&L if SPY drops 5%
  Portfolio_MC_Note        – human-readable summary

References
----------
  Hull Ch.22:      "Model-building approach: sample from multivariate distribution"
  Bouchaud Ch.12:  "VaR with correlations — Cholesky factorization"
  McNeil Ch.2:     "Coherent risk measures — CVaR preferred over VaR"
"""

from __future__ import annotations

import logging
import numpy as np
import pandas as pd
from typing import Optional

logger = logging.getLogger(__name__)

N_PATHS       = 2_000
TRADING_DAYS  = 252
HORIZON_DAYS  = 5       # 1-week risk horizon
HV_FALLBACK   = 0.25
SEED          = 42


def mc_portfolio_var(
    df: pd.DataFrame,
    horizon_days: int = HORIZON_DAYS,
    n_paths: int = N_PATHS,
    seed: Optional[int] = SEED,
) -> dict:
    """
    Compute portfolio-level VaR via correlated Monte Carlo simulation.

    Parameters
    ----------
    df            : positions DataFrame (from management pipeline)
    horizon_days  : risk horizon in trading days (default 5 = 1 week)
    n_paths       : number of MC scenarios
    seed          : RNG seed

    Returns
    -------
    dict with Portfolio_* keys
    """
    _default = {
        "Portfolio_VaR_5pct":      np.nan,
        "Portfolio_CVaR_5pct":     np.nan,
        "Portfolio_P50":           np.nan,
        "Portfolio_P95":           np.nan,
        "Portfolio_Max_Drawdown":  np.nan,
        "Portfolio_Concentration": np.nan,
        "Portfolio_Corr_Risk":     np.nan,
        "Portfolio_Stress_SPY_5":  np.nan,
        "Portfolio_MC_Note":       "MC_SKIP",
    }

    if df.empty:
        _default["Portfolio_MC_Note"] = "MC_SKIP: empty portfolio"
        return _default

    # ── Extract position data ────────────────────────────────────────────
    positions = []
    for _, row in df.iterrows():
        pos = _extract_position(row)
        if pos is not None:
            positions.append(pos)

    if len(positions) < 1:
        _default["Portfolio_MC_Note"] = "MC_SKIP: no valid positions"
        return _default

    tickers = list(set(p["ticker"] for p in positions))
    n_tickers = len(tickers)
    ticker_idx = {t: i for i, t in enumerate(tickers)}

    # ── Build correlation matrix ─────────────────────────────────────────
    corr_matrix = _build_correlation_matrix(tickers)

    # ── HV per ticker ────────────────────────────────────────────────────
    hv_map = {}
    for p in positions:
        if p["ticker"] not in hv_map:
            hv_map[p["ticker"]] = p["hv"]

    # ── Cholesky decomposition ───────────────────────────────────────────
    try:
        L = np.linalg.cholesky(corr_matrix)
    except np.linalg.LinAlgError:
        # Not positive definite — use nearest PD approximation
        corr_matrix = _nearest_pd(corr_matrix)
        L = np.linalg.cholesky(corr_matrix)

    # ── Simulate correlated GBM paths ────────────────────────────────────
    rng = np.random.default_rng(seed)
    t = horizon_days / TRADING_DAYS

    # Generate independent normals, then correlate
    Z_indep = rng.standard_normal((n_paths, n_tickers))
    Z_corr = Z_indep @ L.T  # (n_paths, n_tickers) correlated

    # Terminal prices per ticker
    terminal_prices = {}
    for ticker in tickers:
        i = ticker_idx[ticker]
        hv = hv_map[ticker]
        log_r = (-0.5 * hv**2) * t + hv * np.sqrt(t) * Z_corr[:, i]
        # We need spot price — get from any position with this ticker
        spot = next(p["spot"] for p in positions if p["ticker"] == ticker)
        terminal_prices[ticker] = spot * np.exp(log_r)

    # ── Compute per-position P&L across all scenarios ────────────────────
    portfolio_pnl = np.zeros(n_paths)

    for p in positions:
        s_terminal = terminal_prices[p["ticker"]]

        if p["asset_type"] == "OPTION":
            if p["option_type"] == "CALL":
                payoff = np.maximum(s_terminal - p["strike"], 0)
            else:
                payoff = np.maximum(p["strike"] - s_terminal, 0)

            # Approximate mid-life value (not expiry) — keep time value
            t_remaining = max(p["dte"] - horizon_days, 1) / TRADING_DAYS
            time_val = 0.4 * s_terminal * p["hv"] * np.sqrt(t_remaining)
            moneyness = np.abs(s_terminal - p["strike"]) / np.maximum(s_terminal, 1)
            time_scale = np.exp(-2.0 * moneyness)
            option_val = payoff + time_val * time_scale

            position_pnl = (option_val - p["basis"]) * p["quantity"] * 100
        else:
            # Stock
            position_pnl = (s_terminal - p["spot"]) * p["quantity"]

        portfolio_pnl += position_pnl

    # ── Compute portfolio metrics ────────────────────────────────────────
    var_5 = float(np.percentile(portfolio_pnl, 5))
    cvar_5 = float(np.mean(portfolio_pnl[portfolio_pnl <= var_5]))
    p50 = float(np.median(portfolio_pnl))
    p95 = float(np.percentile(portfolio_pnl, 95))
    max_dd = float(np.min(portfolio_pnl))

    # ── Concentration (Herfindahl index) ─────────────────────────────────
    notionals = []
    for p in positions:
        mult = 100 if p["asset_type"] == "OPTION" else 1
        notional = abs(p["spot"] * p["quantity"] * mult)
        notionals.append(notional)
    total_notional = sum(notionals) or 1
    weights = [n / total_notional for n in notionals]
    hhi = sum(w**2 for w in weights)

    # ── Correlation risk = avg pairwise corr × concentration ─────────────
    if n_tickers > 1:
        upper_tri = corr_matrix[np.triu_indices(n_tickers, k=1)]
        avg_corr = float(np.mean(upper_tri))
    else:
        avg_corr = 1.0
    corr_risk = round(avg_corr * hhi, 4)

    # ── Stress test: SPY -5% scenario ────────────────────────────────────
    stress_pnl = _stress_test(positions, -0.05)

    # ── Summary note ─────────────────────────────────────────────────────
    note_parts = [
        f"{len(positions)} positions, {n_tickers} tickers",
        f"VaR(5%)=${var_5:+,.0f}",
        f"CVaR(5%)=${cvar_5:+,.0f}",
        f"concentration={hhi:.2f}",
        f"avg_corr={avg_corr:.2f}",
    ]
    if stress_pnl is not None:
        note_parts.append(f"SPY-5%=${stress_pnl:+,.0f}")

    return {
        "Portfolio_VaR_5pct":      round(var_5, 2),
        "Portfolio_CVaR_5pct":     round(cvar_5, 2),
        "Portfolio_P50":           round(p50, 2),
        "Portfolio_P95":           round(p95, 2),
        "Portfolio_Max_Drawdown":  round(max_dd, 2),
        "Portfolio_Concentration": round(hhi, 4),
        "Portfolio_Corr_Risk":     corr_risk,
        "Portfolio_Stress_SPY_5":  round(stress_pnl, 2) if stress_pnl is not None else np.nan,
        "Portfolio_MC_Note":       " | ".join(note_parts),
    }


# ── Helpers ──────────────────────────────────────────────────────────────────

def _extract_position(row: pd.Series) -> Optional[dict]:
    """Extract position parameters from a management row."""
    ticker = row.get("Ticker") or row.get("ticker")
    if not ticker:
        return None

    spot = None
    for col in ("UL Last", "Underlying_Last", "last_price", "Last"):
        val = row.get(col)
        if val is not None and pd.notna(val):
            try:
                spot = float(val)
                if spot > 0:
                    break
            except (TypeError, ValueError):
                pass
    if spot is None or spot <= 0:
        return None

    quantity = float(row.get("Quantity", 0) or 0)
    if quantity == 0:
        return None

    asset_type = str(row.get("AssetType", "OPTION") or "OPTION").upper()

    # HV
    hv = HV_FALLBACK
    for col in ("HV_20D", "hv_20d", "HV_30D", "hv_30", "IV_30D"):
        val = row.get(col)
        if val is not None and pd.notna(val):
            v = float(val)
            if v > 1.0:
                v /= 100.0
            if 0.01 <= v <= 5.0:
                hv = v
                break

    pos = {
        "ticker": str(ticker),
        "spot": spot,
        "quantity": quantity,
        "hv": hv,
        "asset_type": asset_type,
        "strike": float(row.get("Strike", 0) or 0),
        "dte": float(row.get("DTE", 30) or 30),
        "basis": float(row.get("Basis", row.get("Last", 0)) or 0),
        "option_type": "CALL" if str(row.get("Option_Type", "") or "").upper().startswith("C") else "PUT",
    }
    return pos


def _build_correlation_matrix(tickers: list) -> np.ndarray:
    """
    Build correlation matrix from DuckDB price history.
    Falls back to identity (no correlation) if data unavailable.
    """
    n = len(tickers)
    if n <= 1:
        return np.eye(n)

    try:
        from core.shared.data_layer.price_history_loader import load_price_history
        returns_dict = {}
        for ticker in tickers:
            df_hist, _ = load_price_history(ticker, days=60, skip_auto_fetch=True)
            if df_hist is not None and len(df_hist) >= 20:
                close_col = next((c for c in ("Close", "close", "Adj Close") if c in df_hist.columns), None)
                if close_col:
                    returns_dict[ticker] = df_hist[close_col].pct_change().dropna().values[-40:]

        if len(returns_dict) < 2:
            return np.eye(n)

        # Build returns matrix — align lengths
        min_len = min(len(v) for v in returns_dict.values())
        if min_len < 10:
            return np.eye(n)

        returns_matrix = np.zeros((min_len, n))
        for ticker in tickers:
            i = tickers.index(ticker)
            if ticker in returns_dict:
                returns_matrix[:, i] = returns_dict[ticker][-min_len:]
            # else: zeros → correlation with others will be ~0

        corr = np.corrcoef(returns_matrix, rowvar=False)

        # Ensure valid correlation matrix
        np.fill_diagonal(corr, 1.0)
        corr = np.nan_to_num(corr, nan=0.0)
        # Symmetrize
        corr = (corr + corr.T) / 2.0
        np.fill_diagonal(corr, 1.0)

        return corr

    except Exception as e:
        logger.debug(f"Correlation matrix fallback to identity: {e}")
        return np.eye(n)


def _nearest_pd(A: np.ndarray) -> np.ndarray:
    """Find the nearest positive-definite matrix (Higham 2002)."""
    B = (A + A.T) / 2
    _, s, V = np.linalg.svd(B)
    H = V.T @ np.diag(s) @ V
    A2 = (B + H) / 2
    A3 = (A2 + A2.T) / 2
    if _is_pd(A3):
        return A3
    # Add small diagonal perturbation
    I = np.eye(A.shape[0])
    k = 1
    while not _is_pd(A3):
        mineig = np.min(np.real(np.linalg.eigvals(A3)))
        A3 += I * (-mineig * k**2 + 1e-8)
        k += 1
        if k > 10:
            return np.eye(A.shape[0])
    return A3


def _is_pd(A: np.ndarray) -> bool:
    """Check if matrix is positive definite."""
    try:
        np.linalg.cholesky(A)
        return True
    except np.linalg.LinAlgError:
        return False


def _stress_test(positions: list, spy_shock: float) -> Optional[float]:
    """
    Estimate portfolio P&L under a SPY shock using beta approximation.

    Assumes beta ≈ 1.0 for simplicity (most equity options are correlated).
    A -5% SPY move → each stock moves -5% × beta.
    """
    total_pnl = 0.0
    for p in positions:
        beta = 1.0  # simplification; could enhance with actual betas
        stock_move = spy_shock * beta
        new_spot = p["spot"] * (1.0 + stock_move)

        if p["asset_type"] == "OPTION":
            if p["option_type"] == "CALL":
                new_intrinsic = max(new_spot - p["strike"], 0)
                old_intrinsic = max(p["spot"] - p["strike"], 0)
            else:
                new_intrinsic = max(p["strike"] - new_spot, 0)
                old_intrinsic = max(p["strike"] - p["spot"], 0)
            # Approximate delta-based P&L
            pnl = (new_intrinsic - old_intrinsic) * p["quantity"] * 100
        else:
            pnl = (new_spot - p["spot"]) * p["quantity"]

        total_pnl += pnl

    return total_pnl
