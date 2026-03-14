"""
GBM path generators for the unified MC engine.

Generators support an optional annualised drift parameter (mu). When mu = 0
(default), paths are risk-neutral. When a regime-adjusted drift is provided,
the log-return formula becomes:

    log_r = (mu - 0.5 * sigma^2) * t + sigma * sqrt(t) * Z

Drift is bounded at ±15% annualised by convention — it's a bias term, not
a prediction engine.

Terminal generators return (n_paths,) arrays; daily generators return
(n_paths, n_days+1) arrays where column 0 = spot.

References:
  - Gatheral Ch.2 (volatility surface)
  - Merton (1976) jump-diffusion model
  - Lopez de Prado 0.683 (triple-barrier needs full paths)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np

TRADING_DAYS = 252
MAX_DRIFT = 0.15   # ±15% annualised cap — bias term, not prediction


# ── Jump-diffusion configuration ────────────────────────────────────────────

@dataclass(frozen=True)
class JumpConfig:
    """
    Merton jump-diffusion parameters.

    intensity: lambda (jumps per day); ~0.05 = ~12 jumps/year
    mean:      average log-jump size; -0.03 = negative skew
    std:       jump magnitude std dev; 0.05 = 5%
    """
    intensity: float = 0.05
    mean: float = -0.03
    std: float = 0.05

    def scale(self, intensity_mult: float = 1.0,
              std_mult: float = 1.0,
              mean_adj: float = 0.0) -> JumpConfig:
        """Return a new JumpConfig with macro-calibrated parameters."""
        return JumpConfig(
            intensity=self.intensity * intensity_mult,
            mean=self.mean + mean_adj,
            std=self.std * std_mult,
        )


DEFAULT_JUMP = JumpConfig()


# ── Terminal price generators ────────────────────────────────────────────────

def gbm_terminal(spot: float, hv: float, dte: int,
                 n_paths: int, rng: np.random.Generator,
                 drift: float = 0.0) -> np.ndarray:
    """
    GBM terminal prices.

    drift: annualised drift (mu). 0 = risk-neutral.
           Clamped to ±MAX_DRIFT internally.

    Returns (n_paths,) array of prices at expiry.
    """
    t = max(dte, 1) / TRADING_DAYS
    mu = max(-MAX_DRIFT, min(MAX_DRIFT, drift))
    z = rng.standard_normal(n_paths)
    log_r = (mu - 0.5 * hv**2) * t + hv * np.sqrt(t) * z
    return spot * np.exp(log_r)


def gbm_terminal_with_jumps(spot: float, hv: float, dte: int,
                             n_paths: int, rng: np.random.Generator,
                             jump: JumpConfig = DEFAULT_JUMP,
                             drift: float = 0.0) -> np.ndarray:
    """
    GBM + Merton jump-diffusion terminal prices.

    drift: annualised drift added on top of jump compensation.
    Jump component: Poisson(lambda * dte) arrivals, each N(mean, std^2).

    Returns (n_paths,) array.
    """
    dte_safe = max(dte, 1)
    t = dte_safe / TRADING_DAYS
    mu = max(-MAX_DRIFT, min(MAX_DRIFT, drift))

    z = rng.standard_normal(n_paths)

    # Poisson number of jumps in [0, T]
    n_jumps = rng.poisson(jump.intensity * dte_safe, size=n_paths)
    jump_component = np.zeros(n_paths)
    max_jumps = int(n_jumps.max()) if n_jumps.max() > 0 else 0
    if max_jumps > 0:
        all_jumps = rng.normal(jump.mean, jump.std, size=(n_paths, max_jumps))
        for j in range(max_jumps):
            mask = n_jumps > j
            jump_component[mask] += all_jumps[mask, j]

    # Drift compensation for jumps: E[e^J] - 1
    k = np.exp(jump.mean + 0.5 * jump.std**2) - 1
    lambda_annual = jump.intensity * TRADING_DAYS
    drift_adj = -lambda_annual * k

    log_r = (mu + drift_adj - 0.5 * hv**2) * t + hv * np.sqrt(t) * z + jump_component
    return spot * np.exp(log_r)


# ── Daily path generators ────────────────────────────────────────────────────

def gbm_daily_paths(spot: float, hv: float, n_days: int,
                    n_paths: int, rng: np.random.Generator,
                    iv_schedule: Optional[np.ndarray] = None,
                    path_modifier: Optional[callable] = None,
                    drift: float = 0.0) -> np.ndarray:
    """
    GBM daily price paths.

    Returns shape (n_paths, n_days + 1) where column 0 = spot.

    drift: annualised drift (mu). Clamped to ±MAX_DRIFT. Applied per-day
           as mu * dt alongside the diffusion term.

    iv_schedule: optional (n_days,) array of per-day annualised IV.
                 When provided, each day's step uses iv_schedule[d]
                 instead of flat hv. Used for macro IV ramp/crush.

    path_modifier: optional callable(log_returns) -> log_returns.
                   Applied after base diffusion, before cumsum.
                   Used for event-day vol amplification.
    """
    dt = 1.0 / TRADING_DAYS
    mu = max(-MAX_DRIFT, min(MAX_DRIFT, drift))
    mu_dt = mu * dt

    if iv_schedule is not None:
        # Per-day varying volatility
        # iv_schedule shape: (n_days,)
        drift_term = (mu_dt - 0.5 * iv_schedule**2 * dt)  # (n_days,)
        vol = iv_schedule * np.sqrt(dt)                     # (n_days,)
        z = rng.standard_normal((n_paths, n_days))
        log_returns = drift_term[np.newaxis, :] + vol[np.newaxis, :] * z
    else:
        drift_term = mu_dt - 0.5 * hv**2 * dt
        vol = hv * np.sqrt(dt)
        z = rng.standard_normal((n_paths, n_days))
        log_returns = drift_term + vol * z

    if path_modifier is not None:
        log_returns = path_modifier(log_returns)

    cum_log = np.concatenate(
        [np.zeros((n_paths, 1)), np.cumsum(log_returns, axis=1)], axis=1
    )
    return spot * np.exp(cum_log)
