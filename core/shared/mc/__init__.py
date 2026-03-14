"""
Unified Monte Carlo Engine — shared infrastructure.

Public API:
  MCEngine          — per-position simulation engine with lazy path caching
  ActionScenario    — enum of evaluable actions (HOLD/EXIT/ROLL/...)
  ScenarioResult    — standardised result container with EV, CVaR, P(profit), ...
  resolve_profile   — map strategy name to StrategyProfile
  resolve_inputs    — resolve MC inputs from a position row
  JumpConfig        — Merton jump-diffusion parameters

  SCAN_POLICY       — input resolution policy for scan context
  MGMT_POLICY       — input resolution policy for management context

  build_vol_schedule    — per-day vol array blending EWMA → HV (vol clustering)
  resolve_vol_schedule  — resolve vol schedule from ticker + HV
"""

from core.shared.mc.engine import MCEngine  # noqa: F401
from core.shared.mc.scenarios import ActionScenario, ScenarioResult  # noqa: F401
from core.shared.mc.profiles import StrategyProfile, resolve_profile, PROFILES  # noqa: F401
from core.shared.mc.inputs import (  # noqa: F401
    resolve_inputs,
    resolve_regime_drift,
    ResolvedInputs,
    ResolutionPolicy,
    SCAN_POLICY,
    MGMT_POLICY,
)
from core.shared.mc.paths import JumpConfig, TRADING_DAYS  # noqa: F401
from core.shared.mc.valuation import brenner_option_value, intrinsic_value  # noqa: F401
from core.shared.mc.vol_blend import build_vol_schedule, resolve_vol_schedule  # noqa: F401
