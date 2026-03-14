"""
Unified Monte Carlo Engine.

Narrow simulation core that generates and caches GBM paths, then
evaluates action scenarios on those paths.

Does NOT contain earnings logic, macro event logic, or special event
handling. Those are path modifiers composed by the calling wrapper:
  - jump_config: Merton jump-diffusion (scan macro weeks)
  - iv_schedule: per-day IV array (macro IV ramp/crush)
  - path_modifier: callable(log_returns) -> log_returns

Architecture:
  MCEngine (this file)
    -> paths.py (GBM generation)
    -> pnl_models.py (strategy-specific P&L)
    -> valuation.py (mid-life option pricing)
    -> profiles.py (strategy classification)
    -> scenarios.py (result containers)
"""

from __future__ import annotations

from typing import Optional

import numpy as np

from core.shared.mc.paths import (
    TRADING_DAYS,
    JumpConfig,
    gbm_terminal,
    gbm_terminal_with_jumps,
    gbm_daily_paths,
)
from core.shared.mc.pnl_models import compute_terminal_pnl
from core.shared.mc.profiles import StrategyProfile
from core.shared.mc.scenarios import ActionScenario, ScenarioResult
from core.shared.mc.valuation import brenner_option_value

N_PATHS = 2_000
SEED = 42


class MCEngine:
    """
    Per-position Monte Carlo engine with lazy path caching.

    Usage:
        engine = MCEngine(spot=150, hv=0.28, iv=0.32, dte=45)
        result = engine.run_scenario(ActionScenario.HOLD_TO_EXPIRY, profile, ctx)
        comparison = engine.compare(
            [ActionScenario.HOLD_TO_EXPIRY, ActionScenario.EXIT_NOW],
            profile, ctx
        )
    """

    def __init__(
        self,
        spot: float,
        hv: float,
        iv: float,
        dte: int,
        n_paths: int = N_PATHS,
        seed: int = SEED,
        jump_config: Optional[JumpConfig] = None,
        iv_schedule: Optional[np.ndarray] = None,
        path_modifier: Optional[callable] = None,
        drift: float = 0.0,
    ):
        self.spot = spot
        self.hv = hv
        self.iv = iv
        self.dte = max(dte, 1)
        self.n_paths = n_paths
        self.seed = seed
        self.jump_config = jump_config
        self.iv_schedule = iv_schedule
        self.path_modifier = path_modifier
        self.drift = drift

        self._rng = np.random.default_rng(seed)
        self._terminal_cache: Optional[np.ndarray] = None
        self._daily_cache: Optional[np.ndarray] = None

    # ── Path accessors (lazy, cached) ────────────────────────────────────────

    @property
    def path_basis(self) -> str:
        """Describe what generated the paths."""
        base = "gbm"
        if self.jump_config is not None:
            base = "gbm_jump"
        elif self.iv_schedule is not None:
            base = "event_overlay"
        if self.drift != 0.0:
            base += f"_drift({self.drift:+.3f})"
        return base

    def terminal_prices(self) -> np.ndarray:
        """
        Cached (n_paths,) terminal GBM prices at expiry.

        Uses jump-diffusion if jump_config is set.
        """
        if self._terminal_cache is not None:
            return self._terminal_cache

        # Use a dedicated sub-seed for terminal paths so daily paths
        # don't consume the same random stream
        rng = np.random.default_rng(self.seed)

        if self.jump_config is not None:
            prices = gbm_terminal_with_jumps(
                self.spot, self.hv, self.dte, self.n_paths,
                rng, self.jump_config, drift=self.drift
            )
        else:
            prices = gbm_terminal(
                self.spot, self.hv, self.dte, self.n_paths, rng,
                drift=self.drift
            )
        self._terminal_cache = prices
        return prices

    def daily_paths(self) -> np.ndarray:
        """
        Cached (n_paths, dte+1) daily GBM prices.

        Generated on demand (more expensive than terminal-only).
        Supports iv_schedule and path_modifier overlays.
        """
        if self._daily_cache is not None:
            return self._daily_cache

        # Use a different sub-seed so daily and terminal are independent
        rng = np.random.default_rng(self.seed + 1)

        paths = gbm_daily_paths(
            self.spot, self.hv, self.dte, self.n_paths,
            rng, self.iv_schedule, self.path_modifier,
            drift=self.drift
        )
        self._daily_cache = paths
        return paths

    # ── Scenario evaluation ──────────────────────────────────────────────────

    def run_scenario(
        self,
        scenario: ActionScenario,
        profile: StrategyProfile,
        context: dict,
    ) -> ScenarioResult:
        """
        Evaluate one action scenario on cached paths.

        context keys (depend on scenario):
          strike, premium, is_call, cost_basis, leap_strike, net_debit,
          n_shares, daily_carry, new_strike, new_premium, new_dte,
          hold_days (for HOLD_N_DAYS)
        """
        if scenario == ActionScenario.HOLD_TO_EXPIRY:
            return self._hold_to_expiry(profile, context)
        if scenario == ActionScenario.EXIT_NOW:
            return self._exit_now(profile, context)
        if scenario == ActionScenario.HOLD_N_DAYS:
            return self._hold_n_days(profile, context)
        if scenario == ActionScenario.ROLL:
            return self._roll(profile, context)
        if scenario == ActionScenario.LET_EXPIRE:
            return self._let_expire(profile, context)
        raise ValueError(f"Unknown scenario: {scenario}")

    def compare(
        self,
        scenarios: list[ActionScenario],
        profile: StrategyProfile,
        context: dict,
    ) -> dict[ActionScenario, ScenarioResult]:
        """Run multiple scenarios on the same cached paths."""
        return {s: self.run_scenario(s, profile, context) for s in scenarios}

    # ── Scenario implementations ─────────────────────────────────────────────

    def _hold_to_expiry(self, profile: StrategyProfile,
                        ctx: dict) -> ScenarioResult:
        """Hold position to expiry. Terminal P&L from cached paths."""
        s_T = self.terminal_prices()
        strike = ctx.get("strike", 0.0)
        premium = ctx.get("premium", 0.0)
        is_call = ctx.get("is_call", True)

        pnl = compute_terminal_pnl(
            model=profile.pnl_model,
            s_terminal=s_T,
            strike=strike,
            premium=premium,
            is_call=is_call,
            cost_basis=ctx.get("cost_basis", 0.0),
            leap_strike=ctx.get("leap_strike", 0.0),
            net_debit=ctx.get("net_debit", 0.0),
            n_shares=ctx.get("n_shares", 100.0),
        )

        # Per-share models need contract multiplier
        if profile.pnl_model not in ("stock_plus_short_call",):
            pnl = pnl * 100.0

        # Carry drag
        carry = ctx.get("daily_carry", 0.0) * self.dte

        pnl_net = pnl - carry

        return self._build_result(
            scenario=ActionScenario.HOLD_TO_EXPIRY,
            pnl=pnl_net,
            carry=carry,
            horizon_days=self.dte,
            valuation_day=self.dte,
            s_T=s_T,
            strike=strike,
            profile=profile,
        )

    def _exit_now(self, profile: StrategyProfile,
                  ctx: dict) -> ScenarioResult:
        """Exit position immediately. Deterministic P&L (no simulation)."""
        premium = ctx.get("premium", 0.0)
        current_mid = ctx.get("current_mid", 0.0)
        is_call = ctx.get("is_call", True)

        if profile.pnl_model == "stock_plus_short_call":
            # BW/CC: close option, keep stock
            n_shares = ctx.get("n_shares", 100.0)
            option_pnl = (premium - current_mid) * n_shares if current_mid > 0 else 0.0
            # Stock P&L not included in exit — it's the option exit
            pnl = np.full(self.n_paths, option_pnl)
        elif profile.pnl_model in ("short_put", "short_call"):
            # Close short option: buy back at mid
            pnl = np.full(self.n_paths, (premium - current_mid) * 100.0)
        else:
            # Close long option: sell at mid
            pnl = np.full(self.n_paths, (current_mid - premium) * 100.0)

        return self._build_result(
            scenario=ActionScenario.EXIT_NOW,
            pnl=pnl,
            carry=0.0,
            horizon_days=0,
            valuation_day=0,
            s_T=self.terminal_prices(),
            strike=ctx.get("strike", 0.0),
            profile=profile,
        )

    def _hold_n_days(self, profile: StrategyProfile,
                     ctx: dict) -> ScenarioResult:
        """
        Day-by-day hold evaluation to find optimal exit day.

        Uses daily paths + Brenner-Subrahmanyam time-value approximation.
        Returns ScenarioResult with daily_ev and optimal_day populated.
        """
        paths = self.daily_paths()  # (n_paths, dte+1)
        strike = ctx.get("strike", 0.0)
        premium = ctx.get("premium", 0.0)
        is_call = ctx.get("is_call", True)
        daily_carry = ctx.get("daily_carry", 0.0)

        n_days = self.dte

        # Option value at each day using Brenner-Subrahmanyam
        daily_ev_arr = np.zeros(n_days + 1)
        for d in range(n_days + 1):
            spot_d = paths[:, d]
            dte_remaining = n_days - d

            if dte_remaining <= 0:
                # At expiry: intrinsic only
                if is_call:
                    opt_val = np.maximum(spot_d - strike, 0.0)
                else:
                    opt_val = np.maximum(strike - spot_d, 0.0)
            else:
                opt_val = brenner_option_value(
                    spot_d, strike, self.iv, dte_remaining, is_call
                )

            # P&L = option value - entry premium (per share, then x100)
            if profile.is_income:
                pnl_d = (premium - opt_val) * 100.0
            else:
                pnl_d = (opt_val - premium) * 100.0

            # Subtract carry up to day d
            pnl_d = pnl_d - daily_carry * d

            daily_ev_arr[d] = float(np.mean(pnl_d))

        # Find optimal exit day (peak EV)
        optimal_day = int(np.argmax(daily_ev_arr))

        # Build result at the optimal day
        spot_opt = paths[:, optimal_day]
        dte_at_opt = n_days - optimal_day

        if dte_at_opt <= 0:
            if is_call:
                opt_val_opt = np.maximum(spot_opt - strike, 0.0)
            else:
                opt_val_opt = np.maximum(strike - spot_opt, 0.0)
        else:
            opt_val_opt = brenner_option_value(
                spot_opt, strike, self.iv, dte_at_opt, is_call
            )

        if profile.is_income:
            pnl_opt = (premium - opt_val_opt) * 100.0
        else:
            pnl_opt = (opt_val_opt - premium) * 100.0

        carry_to_opt = daily_carry * optimal_day
        pnl_opt = pnl_opt - carry_to_opt

        result = self._build_result(
            scenario=ActionScenario.HOLD_N_DAYS,
            pnl=pnl_opt,
            carry=carry_to_opt,
            horizon_days=n_days,
            valuation_day=optimal_day,
            s_T=spot_opt,
            strike=strike,
            profile=profile,
        )
        result.daily_ev = daily_ev_arr
        result.optimal_day = optimal_day
        return result

    def _roll(self, profile: StrategyProfile,
              ctx: dict) -> ScenarioResult:
        """
        Roll to a new position. Generates new terminal prices
        at the new DTE horizon.
        """
        new_strike = ctx.get("new_strike", ctx.get("strike", 0.0))
        new_premium = ctx.get("new_premium", 0.0)
        new_dte = ctx.get("new_dte", self.dte)
        is_call = ctx.get("is_call", True)
        roll_cost = ctx.get("roll_cost", 0.0)
        daily_carry = ctx.get("daily_carry", 0.0)

        # Generate terminal prices at new DTE horizon
        rng = np.random.default_rng(self.seed + 2)
        if self.jump_config is not None:
            s_T_new = gbm_terminal_with_jumps(
                self.spot, self.hv, new_dte, self.n_paths,
                rng, self.jump_config, drift=self.drift
            )
        else:
            s_T_new = gbm_terminal(
                self.spot, self.hv, new_dte, self.n_paths, rng,
                drift=self.drift
            )

        pnl = compute_terminal_pnl(
            model=profile.pnl_model,
            s_terminal=s_T_new,
            strike=new_strike,
            premium=new_premium,
            is_call=is_call,
            cost_basis=ctx.get("cost_basis", 0.0),
            leap_strike=ctx.get("leap_strike", 0.0),
            net_debit=ctx.get("net_debit", 0.0),
            n_shares=ctx.get("n_shares", 100.0),
        )

        if profile.pnl_model not in ("stock_plus_short_call",):
            pnl = pnl * 100.0

        # Subtract roll cost and carry
        carry = daily_carry * new_dte
        pnl_net = pnl - roll_cost - carry

        return self._build_result(
            scenario=ActionScenario.ROLL,
            pnl=pnl_net,
            carry=carry,
            horizon_days=new_dte,
            valuation_day=new_dte,
            s_T=s_T_new,
            strike=new_strike,
            profile=profile,
        )

    def _let_expire(self, profile: StrategyProfile,
                    ctx: dict) -> ScenarioResult:
        """Let expire worthless. Same as hold_to_expiry but with that intent."""
        return self._hold_to_expiry(profile, ctx)

    # ── Result builder ───────────────────────────────────────────────────────

    def _build_result(
        self,
        scenario: ActionScenario,
        pnl: np.ndarray,
        carry: float,
        horizon_days: int,
        valuation_day: int,
        s_T: np.ndarray,
        strike: float,
        profile: StrategyProfile,
    ) -> ScenarioResult:
        """Compute summary statistics from a P&L array."""
        ev = float(np.mean(pnl))
        ev_std = float(np.std(pnl))
        n = len(pnl)

        # Percentiles
        p10 = float(np.percentile(pnl, 10))
        p50 = float(np.percentile(pnl, 50))
        p90 = float(np.percentile(pnl, 90))

        # P(profit)
        p_profit = float(np.mean(pnl > 0))

        # CVaR: mean of worst 10%
        cutoff = np.percentile(pnl, 10)
        worst = pnl[pnl <= cutoff]
        cvar = float(np.mean(worst)) if len(worst) > 0 else float(cutoff)

        # P(assignment) for income strategies
        if profile.assignment_relevant and strike > 0:
            if profile.pnl_model in ("short_put",):
                p_assign = float(np.mean(s_T < strike))
            else:
                p_assign = float(np.mean(s_T > strike))
        else:
            p_assign = float("nan")

        # Note
        note = f"{scenario.value}: EV=${ev:+.0f}, P(profit)={p_profit:.0%}"

        return ScenarioResult(
            scenario=scenario,
            ev=ev,
            p10=p10,
            p50=p50,
            p90=p90,
            p_profit=p_profit,
            cvar=cvar,
            p_assign=p_assign,
            carry_drag=carry,
            note=note,
            horizon_days=horizon_days,
            valuation_day=valuation_day,
            event_adjusted=self.iv_schedule is not None,
            n_paths_used=n,
            stderr_ev=ev_std / np.sqrt(n) if n > 0 else None,
            path_basis=self.path_basis,
        )
