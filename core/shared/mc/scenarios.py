"""
Action scenarios and result containers for the unified MC engine.

Units convention:
  $ (USD per contract): ev, p10, p50, p90, cvar, carry_drag
  0-1 probability:      p_profit, p_assign
  Trading days:         horizon_days, valuation_day, optimal_day
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

import numpy as np


class ActionScenario(Enum):
    """Actions that can be evaluated on a shared set of GBM paths."""
    HOLD_TO_EXPIRY = "hold"
    EXIT_NOW = "exit"
    ROLL = "roll"
    LET_EXPIRE = "let_expire"
    HOLD_N_DAYS = "hold_n_days"


@dataclass
class ScenarioResult:
    """
    Outcome of evaluating one action scenario on simulated paths.

    All monetary values are USD per contract (×100 multiplier applied).
    Probabilities are 0-1. Time fields are in trading days.
    """
    scenario: ActionScenario
    ev: float                           # Expected value ($)
    p10: float                          # 10th percentile P&L ($)
    p50: float                          # Median P&L ($)
    p90: float                          # 90th percentile P&L ($)
    p_profit: float                     # P(P&L > 0), 0-1
    cvar: float                         # Conditional VaR: mean of worst 10% ($)
    p_assign: float                     # P(short strike ITM), NaN for non-income
    carry_drag: float                   # Margin carry cost over horizon ($)
    note: str                           # Human-readable context

    # Horizon metadata
    horizon_days: int                   # How many trading days this scenario spans
    valuation_day: int                  # Day at which P&L is measured (0=now, dte=expiry)
    event_adjusted: bool = False        # True if macro/earnings IV overlay was applied

    # Simulation stability
    n_paths_used: int = 2_000           # Audit: how many paths actually ran
    stderr_ev: Optional[float] = None   # Standard error of EV (ev_std / sqrt(n)); None = not computed
    path_basis: str = "gbm"             # What generated paths: "gbm", "gbm_jump", "hv_only", "event_overlay"

    # Daily path results (only populated for HOLD_N_DAYS)
    daily_ev: Optional[np.ndarray] = field(default=None, repr=False)
    optimal_day: Optional[int] = None   # Day of peak EV (trading days)
