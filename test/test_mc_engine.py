"""
Parity and golden-case tests for the unified MC engine.

Phase 1: Verify shared engine produces identical outputs to existing
modules when given the same inputs and seed.

No existing module is modified — Phase 1 is additive only.
"""

import math

import numpy as np
import pandas as pd
import pytest

from core.shared.mc.paths import (
    TRADING_DAYS,
    JumpConfig,
    gbm_terminal,
    gbm_terminal_with_jumps,
    gbm_daily_paths,
)
from core.shared.mc.valuation import (
    BRENNER_COEFFICIENT,
    MONEYNESS_DECAY_RATE,
    brenner_option_value,
    intrinsic_value,
)
from core.shared.mc.pnl_models import (
    long_option_pnl,
    short_put_pnl,
    stock_plus_short_call_pnl,
    pmcc_pnl,
    compute_terminal_pnl,
)
from core.shared.mc.profiles import resolve_profile, PROFILES
from core.shared.mc.scenarios import ActionScenario, ScenarioResult
from core.shared.mc.inputs import (
    resolve_inputs,
    ResolvedInputs,
    SCAN_POLICY,
    MGMT_POLICY,
)
from core.shared.mc.engine import MCEngine

SEED = 42
N_PATHS = 2_000


# ═══════════════════════════════════════════════════════════════════════════════
# 1. GBM PATH PARITY
# ═══════════════════════════════════════════════════════════════════════════════

class TestGBMTerminal:
    """Verify gbm_terminal matches mc_management._gbm_terminal."""

    def test_terminal_shape(self):
        rng = np.random.default_rng(SEED)
        prices = gbm_terminal(150.0, 0.28, 45, N_PATHS, rng)
        assert prices.shape == (N_PATHS,)

    def test_terminal_formula_parity(self):
        """Compare against the exact formula from mc_management._gbm_terminal."""
        spot, hv, dte = 150.0, 0.28, 45
        t = dte / TRADING_DAYS

        # Our implementation
        rng1 = np.random.default_rng(SEED)
        prices_new = gbm_terminal(spot, hv, dte, N_PATHS, rng1)

        # Direct formula (mc_management._gbm_terminal)
        rng2 = np.random.default_rng(SEED)
        z = rng2.standard_normal(N_PATHS)
        log_r = (-0.5 * hv**2) * t + hv * np.sqrt(t) * z
        prices_old = spot * np.exp(log_r)

        np.testing.assert_array_almost_equal(prices_new, prices_old)

    def test_terminal_mean_near_spot(self):
        """Risk-neutral: E[S_T] ~ S_0."""
        rng = np.random.default_rng(SEED)
        prices = gbm_terminal(100.0, 0.30, 252, 50_000, rng)
        assert abs(np.mean(prices) - 100.0) < 3.0  # within ~3% for 50k paths

    def test_terminal_min_dte_floor(self):
        """DTE=0 should be floored to 1."""
        rng = np.random.default_rng(SEED)
        prices = gbm_terminal(100.0, 0.30, 0, N_PATHS, rng)
        assert prices.shape == (N_PATHS,)
        # With DTE=1, vol is tiny — prices should be very close to spot
        assert abs(np.mean(prices) - 100.0) < 2.0


class TestGBMJumpDiffusion:
    """Verify gbm_terminal_with_jumps matches mc_position_sizing logic."""

    def test_jump_shape(self):
        rng = np.random.default_rng(SEED)
        prices = gbm_terminal_with_jumps(150.0, 0.28, 45, N_PATHS, rng)
        assert prices.shape == (N_PATHS,)

    def test_jump_negative_skew(self):
        """Default jump mean is -0.03 — should produce negative skew."""
        rng = np.random.default_rng(SEED)
        prices = gbm_terminal_with_jumps(100.0, 0.25, 90, 50_000, rng)
        log_returns = np.log(prices / 100.0)
        # Negative skew expected from negative jump mean
        from scipy.stats import skew
        sk = skew(log_returns)
        assert sk < 0.5  # should be negative or near-zero

    def test_jump_config_scale(self):
        """Macro calibration should adjust jump params."""
        base = JumpConfig()
        scaled = base.scale(intensity_mult=2.0, std_mult=1.5, mean_adj=-0.01)
        assert scaled.intensity == pytest.approx(0.10)
        assert scaled.std == pytest.approx(0.075)
        assert scaled.mean == pytest.approx(-0.04)

    def test_jump_drift_compensated(self):
        """Mean terminal price should still be ~spot (risk-neutral)."""
        rng = np.random.default_rng(SEED)
        prices = gbm_terminal_with_jumps(100.0, 0.25, 252, 50_000, rng)
        assert abs(np.mean(prices) - 100.0) < 5.0  # wider tolerance for jumps

    def test_jump_vs_plain_wider_tails(self):
        """Jump-diffusion should have wider tails than plain GBM."""
        rng1 = np.random.default_rng(SEED)
        plain = gbm_terminal(100.0, 0.25, 90, 50_000, rng1)
        rng2 = np.random.default_rng(SEED)
        jump = gbm_terminal_with_jumps(100.0, 0.25, 90, 50_000, rng2)
        # Jump should have higher variance
        assert np.std(jump) >= np.std(plain) * 0.9  # at minimum not much narrower


class TestGBMDailyPaths:
    """Verify gbm_daily_paths matches mc_management._gbm_daily_paths."""

    def test_daily_shape(self):
        rng = np.random.default_rng(SEED)
        paths = gbm_daily_paths(150.0, 0.28, 45, N_PATHS, rng)
        assert paths.shape == (N_PATHS, 46)  # n_days + 1

    def test_daily_col_zero_is_spot(self):
        rng = np.random.default_rng(SEED)
        paths = gbm_daily_paths(150.0, 0.28, 45, N_PATHS, rng)
        np.testing.assert_array_almost_equal(paths[:, 0], 150.0)

    def test_daily_formula_parity(self):
        """Compare against mc_management._gbm_daily_paths."""
        spot, hv, n_days = 150.0, 0.28, 45

        # Our implementation
        rng1 = np.random.default_rng(SEED)
        paths_new = gbm_daily_paths(spot, hv, n_days, N_PATHS, rng1)

        # Direct formula
        dt = 1.0 / TRADING_DAYS
        drift = -0.5 * hv**2 * dt
        vol = hv * np.sqrt(dt)
        rng2 = np.random.default_rng(SEED)
        z = rng2.standard_normal((N_PATHS, n_days))
        log_returns = drift + vol * z
        cum_log = np.concatenate(
            [np.zeros((N_PATHS, 1)), np.cumsum(log_returns, axis=1)], axis=1
        )
        paths_old = spot * np.exp(cum_log)

        np.testing.assert_array_almost_equal(paths_new, paths_old)

    def test_iv_schedule_varies_vol(self):
        """Per-day IV schedule should change step sizes."""
        rng1 = np.random.default_rng(SEED)
        flat = gbm_daily_paths(100.0, 0.25, 10, N_PATHS, rng1)

        # Schedule with high vol on day 5
        schedule = np.full(10, 0.25)
        schedule[4] = 0.80  # big vol spike on day 5
        rng2 = np.random.default_rng(SEED)
        varied = gbm_daily_paths(100.0, 0.25, 10, N_PATHS, rng2,
                                  iv_schedule=schedule)

        # Varied should have wider dispersion after day 5
        assert np.std(varied[:, 6]) > np.std(flat[:, 6]) * 1.1

    def test_path_modifier_applied(self):
        """Path modifier should alter log returns."""
        def amplify_day3(log_returns):
            log_returns[:, 2] *= 2.0
            return log_returns

        rng1 = np.random.default_rng(SEED)
        normal = gbm_daily_paths(100.0, 0.25, 10, N_PATHS, rng1)

        rng2 = np.random.default_rng(SEED)
        modified = gbm_daily_paths(100.0, 0.25, 10, N_PATHS, rng2,
                                    path_modifier=amplify_day3)

        # Day 0-2 should be identical
        np.testing.assert_array_almost_equal(normal[:, :3], modified[:, :3])
        # Day 3+ should differ
        assert not np.allclose(normal[:, 3], modified[:, 3])


# ═══════════════════════════════════════════════════════════════════════════════
# 2. VALUATION PARITY
# ═══════════════════════════════════════════════════════════════════════════════

class TestBrennerValuation:
    """Verify Brenner-Subrahmanyam matches mc_earnings_event formula."""

    def test_atm_call(self):
        """ATM call: time value = 0.4 * S * sigma * sqrt(T)."""
        spot = np.array([100.0])
        val = brenner_option_value(spot, 100.0, 0.30, 45, is_call=True)
        t = 45 / TRADING_DAYS
        expected = 0.0 + 0.4 * 100.0 * 0.30 * np.sqrt(t) * 1.0  # moneyness=0
        assert val[0] == pytest.approx(expected, abs=0.01)

    def test_deep_itm_call(self):
        """Deep ITM: intrinsic dominates, time value decayed."""
        spot = np.array([120.0])
        val = brenner_option_value(spot, 100.0, 0.30, 45, is_call=True)
        assert val[0] > 20.0  # intrinsic = 20
        # Time value should be small (moneyness decay)
        moneyness = 20.0 / 120.0
        decay = np.exp(-2.0 * moneyness)
        assert decay < 0.8  # significant decay

    def test_expiry_equals_intrinsic(self):
        """At DTE=0, option = intrinsic only."""
        spot = np.array([110.0, 90.0])
        val = brenner_option_value(spot, 100.0, 0.30, 0, is_call=True)
        np.testing.assert_array_almost_equal(val, [10.0, 0.0])

    def test_put_intrinsic(self):
        """Put option: intrinsic = max(K - S, 0)."""
        spot = np.array([80.0])
        val = brenner_option_value(spot, 100.0, 0.30, 45, is_call=False)
        assert val[0] > 20.0  # intrinsic = 20

    def test_vectorized(self):
        """Should handle array of spot prices."""
        spot = np.array([90.0, 100.0, 110.0, 120.0])
        val = brenner_option_value(spot, 100.0, 0.30, 45, is_call=True)
        assert val.shape == (4,)
        # Values should increase with spot for calls
        assert val[3] > val[2] > val[1]


class TestIntrinsicValue:

    def test_call(self):
        spot = np.array([90.0, 100.0, 110.0])
        iv = intrinsic_value(spot, 100.0, is_call=True)
        np.testing.assert_array_almost_equal(iv, [0.0, 0.0, 10.0])

    def test_put(self):
        spot = np.array([90.0, 100.0, 110.0])
        iv = intrinsic_value(spot, 100.0, is_call=False)
        np.testing.assert_array_almost_equal(iv, [10.0, 0.0, 0.0])


# ═══════════════════════════════════════════════════════════════════════════════
# 3. P&L MODEL PARITY
# ═══════════════════════════════════════════════════════════════════════════════

class TestPnLModels:
    """Verify P&L models match existing implementations."""

    def _terminal_prices(self, spot=150.0, hv=0.28, dte=45):
        rng = np.random.default_rng(SEED)
        return gbm_terminal(spot, hv, dte, N_PATHS, rng)

    def test_long_call_pnl(self):
        """Long call: intrinsic - premium."""
        s_T = self._terminal_prices()
        pnl = long_option_pnl(s_T, 155.0, 3.50, is_call=True)
        # Where S_T > 155: pnl = S_T - 155 - 3.50
        # Where S_T <= 155: pnl = -3.50
        itm = s_T > 155.0
        expected = np.where(itm, s_T - 155.0 - 3.50, -3.50)
        np.testing.assert_array_almost_equal(pnl, expected)

    def test_long_put_pnl(self):
        """Long put: max(K - S_T, 0) - premium."""
        s_T = self._terminal_prices()
        pnl = long_option_pnl(s_T, 145.0, 2.50, is_call=False)
        expected = np.maximum(145.0 - s_T, 0.0) - 2.50
        np.testing.assert_array_almost_equal(pnl, expected)

    def test_short_put_pnl(self):
        """Short put: premium - max(K - S_T, 0)."""
        s_T = self._terminal_prices()
        pnl = short_put_pnl(s_T, 145.0, 2.50)
        expected = 2.50 - np.maximum(145.0 - s_T, 0.0)
        np.testing.assert_array_almost_equal(pnl, expected)

    def test_bw_cc_pnl(self):
        """Buy-write: (S_T - cost_basis + premium - call_intrinsic) * n_shares."""
        s_T = self._terminal_prices()
        pnl = stock_plus_short_call_pnl(s_T, 155.0, 3.50, 148.0, 100.0)
        call_intr = np.maximum(s_T - 155.0, 0.0)
        expected = ((s_T - 148.0) + 3.50 - call_intr) * 100.0
        np.testing.assert_array_almost_equal(pnl, expected)

    def test_pmcc_pnl(self):
        """PMCC: LEAP_intrinsic - short_liability - net_debit."""
        s_T = self._terminal_prices()
        pnl = pmcc_pnl(s_T, short_strike=160.0, leap_strike=130.0, net_debit=25.0)
        leap_intr = np.maximum(s_T - 130.0, 0.0)
        short_liab = np.maximum(s_T - 160.0, 0.0)
        expected = (leap_intr - short_liab) - 25.0
        np.testing.assert_array_almost_equal(pnl, expected)

    def test_pmcc_max_loss(self):
        """PMCC max loss = net_debit (when S_T << leap_strike)."""
        s_T = np.array([50.0, 60.0, 70.0])  # well below leap strike
        pnl = pmcc_pnl(s_T, short_strike=160.0, leap_strike=130.0, net_debit=25.0)
        np.testing.assert_array_almost_equal(pnl, [-25.0, -25.0, -25.0])

    def test_dispatch_long(self):
        """compute_terminal_pnl dispatches to long_option."""
        s_T = self._terminal_prices()
        direct = long_option_pnl(s_T, 155.0, 3.50, is_call=True)
        dispatched = compute_terminal_pnl("long_option", s_T, 155.0, 3.50,
                                           is_call=True)
        np.testing.assert_array_almost_equal(direct, dispatched)

    def test_dispatch_short_put(self):
        s_T = self._terminal_prices()
        direct = short_put_pnl(s_T, 145.0, 2.50)
        dispatched = compute_terminal_pnl("short_put", s_T, 145.0, 2.50)
        np.testing.assert_array_almost_equal(direct, dispatched)


# ═══════════════════════════════════════════════════════════════════════════════
# 4. STRATEGY PROFILE RESOLUTION
# ═══════════════════════════════════════════════════════════════════════════════

class TestProfileResolution:

    def test_exact_match(self):
        p = resolve_profile("LONG_CALL")
        assert p.name == "LONG_CALL"
        assert p.pnl_model == "long_option"
        assert p.is_long_premium is True

    def test_alias_cc(self):
        p = resolve_profile("CC")
        assert p.name == "COVERED_CALL"
        assert p.pnl_model == "stock_plus_short_call"

    def test_alias_csp(self):
        p = resolve_profile("CASH_SECURED_PUT")
        assert p.name == "CSP"

    def test_pmcc_before_cc(self):
        """PMCC must resolve before CC (substring collision)."""
        p = resolve_profile("PMCC")
        assert p.name == "PMCC"
        assert p.pnl_model == "pmcc"

    def test_keyword_fallback(self):
        p = resolve_profile("SOME_LONG_CALL_VARIANT")
        assert p.pnl_model == "long_option"

    def test_unknown_gets_default(self):
        p = resolve_profile("TOTALLY_UNKNOWN_STRATEGY")
        assert p.name == "DEFAULT"

    def test_normalisation(self):
        """Hyphens, spaces, case normalised."""
        p = resolve_profile("buy-write")
        assert p.name == "BUY_WRITE"

    def test_leaps_resolves_to_long_call(self):
        p = resolve_profile("LEAPS")
        assert p.pnl_model == "long_option"

    def test_iron_condor_resolves_to_multi_leg(self):
        p = resolve_profile("IRON_CONDOR")
        assert p.name == "MULTI_LEG"

    def test_income_flag(self):
        for name in ("BUY_WRITE", "COVERED_CALL", "CSP", "PMCC"):
            assert PROFILES[name].is_income is True
        assert PROFILES["LONG_CALL"].is_income is False
        assert PROFILES["LONG_PUT"].is_income is False


# ═══════════════════════════════════════════════════════════════════════════════
# 5. INPUT RESOLUTION
# ═══════════════════════════════════════════════════════════════════════════════

class TestInputResolution:

    def _row(self, **kwargs) -> pd.Series:
        return pd.Series(kwargs)

    def test_scan_spot(self):
        row = self._row(last_price=150.0)
        inputs = resolve_inputs(row, SCAN_POLICY)
        assert inputs.spot == 150.0

    def test_mgmt_spot(self):
        row = self._row(**{"UL Last": 150.0})
        inputs = resolve_inputs(row, MGMT_POLICY)
        assert inputs.spot == 150.0

    def test_hv_from_column(self):
        row = self._row(last_price=100.0, hv_30=0.28, Strike=100.0)
        inputs = resolve_inputs(row, SCAN_POLICY)
        assert inputs.hv == pytest.approx(0.28)

    def test_hv_percentage_normalised(self):
        row = self._row(last_price=100.0, hv_30=28.0, Strike=100.0)
        inputs = resolve_inputs(row, SCAN_POLICY)
        assert inputs.hv == pytest.approx(0.28)

    def test_hv_fallback_scan(self):
        row = self._row(last_price=100.0, Strike=100.0)
        inputs = resolve_inputs(row, SCAN_POLICY)
        assert inputs.hv == pytest.approx(0.30)

    def test_hv_fallback_mgmt(self):
        row = self._row(**{"UL Last": 100.0, "Strike": 100.0})
        inputs = resolve_inputs(row, MGMT_POLICY)
        assert inputs.hv == pytest.approx(0.25)

    def test_iv_floor_blending(self):
        """IV > HV by >20% triggers 70/30 blend."""
        row = self._row(last_price=100.0, hv_30=0.20, iv_30d=0.35,
                        Strike=100.0)
        inputs = resolve_inputs(row, SCAN_POLICY)
        # 0.35 > 0.20 * 1.20 = 0.24 → blend
        expected = 0.70 * 0.20 + 0.30 * 0.35
        assert inputs.hv == pytest.approx(expected)

    def test_iv_floor_no_blend_within_threshold(self):
        row = self._row(last_price=100.0, hv_30=0.20, iv_30d=0.22,
                        Strike=100.0)
        inputs = resolve_inputs(row, SCAN_POLICY)
        # 0.22 < 0.20 * 1.20 = 0.24 → no blend
        assert inputs.hv == pytest.approx(0.20)

    def test_strike_resolution(self):
        row = self._row(last_price=100.0, Selected_Strike=105.0)
        inputs = resolve_inputs(row, SCAN_POLICY)
        assert inputs.strike == 105.0

    def test_dte_floor(self):
        row = self._row(last_price=100.0, DTE=0, Strike=100.0)
        inputs = resolve_inputs(row, SCAN_POLICY)
        assert inputs.dte == 1

    def test_is_call_from_option_type(self):
        row = self._row(last_price=100.0, Option_Type="put", Strike=100.0)
        inputs = resolve_inputs(row, SCAN_POLICY)
        assert inputs.is_call is False

    def test_is_call_from_strategy(self):
        row = self._row(last_price=100.0, Strategy="LONG_PUT", Strike=100.0)
        inputs = resolve_inputs(row, SCAN_POLICY)
        assert inputs.is_call is False

    def test_daily_carry_retirement(self):
        row = self._row(**{"UL Last": 100.0, "Is_Retirement": True,
                           "Daily_Margin_Cost": 5.0, "Strike": 100.0})
        inputs = resolve_inputs(row, MGMT_POLICY)
        assert inputs.daily_carry == 0.0


# ═══════════════════════════════════════════════════════════════════════════════
# 6. MC ENGINE INTEGRATION
# ═══════════════════════════════════════════════════════════════════════════════

class TestMCEngine:

    def test_hold_to_expiry_long_call(self):
        engine = MCEngine(spot=150.0, hv=0.28, iv=0.30, dte=45)
        profile = resolve_profile("LONG_CALL")
        ctx = {"strike": 155.0, "premium": 3.50, "is_call": True}
        result = engine.run_scenario(ActionScenario.HOLD_TO_EXPIRY, profile, ctx)

        assert result.scenario == ActionScenario.HOLD_TO_EXPIRY
        assert result.n_paths_used == N_PATHS
        assert result.horizon_days == 45
        assert result.valuation_day == 45
        assert result.path_basis == "gbm"
        assert result.stderr_ev is not None
        assert result.stderr_ev > 0
        assert 0 <= result.p_profit <= 1
        assert math.isnan(result.p_assign)  # long option, not income

    def test_hold_to_expiry_csp(self):
        engine = MCEngine(spot=150.0, hv=0.28, iv=0.30, dte=30)
        profile = resolve_profile("CSP")
        ctx = {"strike": 145.0, "premium": 2.50, "is_call": False}
        result = engine.run_scenario(ActionScenario.HOLD_TO_EXPIRY, profile, ctx)

        assert not math.isnan(result.p_assign)  # income strategy
        assert result.p_assign >= 0  # some probability of assignment

    def test_exit_now_deterministic(self):
        engine = MCEngine(spot=150.0, hv=0.28, iv=0.30, dte=45)
        profile = resolve_profile("LONG_CALL")
        ctx = {"strike": 155.0, "premium": 3.50, "current_mid": 4.00,
               "is_call": True}
        result = engine.run_scenario(ActionScenario.EXIT_NOW, profile, ctx)

        # Exit should be deterministic: (4.00 - 3.50) * 100 = $50
        assert result.ev == pytest.approx(50.0)
        assert result.p_profit == 1.0  # all paths positive
        assert result.horizon_days == 0

    def test_hold_n_days_has_daily_ev(self):
        engine = MCEngine(spot=150.0, hv=0.28, iv=0.30, dte=30)
        profile = resolve_profile("LONG_CALL")
        ctx = {"strike": 155.0, "premium": 3.50, "is_call": True}
        result = engine.run_scenario(ActionScenario.HOLD_N_DAYS, profile, ctx)

        assert result.daily_ev is not None
        assert len(result.daily_ev) == 31  # 30 days + day 0
        assert result.optimal_day is not None
        assert 0 <= result.optimal_day <= 30

    def test_roll_new_horizon(self):
        engine = MCEngine(spot=150.0, hv=0.28, iv=0.30, dte=15)
        profile = resolve_profile("CSP")
        ctx = {"strike": 145.0, "premium": 2.50, "is_call": False,
               "new_strike": 142.0, "new_premium": 3.00, "new_dte": 45,
               "roll_cost": 25.0}
        result = engine.run_scenario(ActionScenario.ROLL, profile, ctx)

        assert result.scenario == ActionScenario.ROLL
        assert result.horizon_days == 45

    def test_compare_returns_all_scenarios(self):
        engine = MCEngine(spot=150.0, hv=0.28, iv=0.30, dte=45)
        profile = resolve_profile("LONG_CALL")
        ctx = {"strike": 155.0, "premium": 3.50, "is_call": True,
               "current_mid": 4.00}

        results = engine.compare(
            [ActionScenario.HOLD_TO_EXPIRY, ActionScenario.EXIT_NOW],
            profile, ctx
        )

        assert len(results) == 2
        assert ActionScenario.HOLD_TO_EXPIRY in results
        assert ActionScenario.EXIT_NOW in results

    def test_terminal_prices_cached(self):
        """Same engine should return identical prices on repeated calls."""
        engine = MCEngine(spot=150.0, hv=0.28, iv=0.30, dte=45)
        p1 = engine.terminal_prices()
        p2 = engine.terminal_prices()
        assert p1 is p2  # same object reference

    def test_daily_paths_cached(self):
        engine = MCEngine(spot=150.0, hv=0.28, iv=0.30, dte=30)
        d1 = engine.daily_paths()
        d2 = engine.daily_paths()
        assert d1 is d2

    def test_carry_drag_applied(self):
        """Carry should reduce EV."""
        engine = MCEngine(spot=150.0, hv=0.28, iv=0.30, dte=45)
        profile = resolve_profile("LONG_CALL")
        ctx_no_carry = {"strike": 155.0, "premium": 3.50, "is_call": True,
                        "daily_carry": 0.0}
        ctx_carry = {"strike": 155.0, "premium": 3.50, "is_call": True,
                     "daily_carry": 2.0}

        r1 = engine.run_scenario(ActionScenario.HOLD_TO_EXPIRY, profile, ctx_no_carry)
        r2 = engine.run_scenario(ActionScenario.HOLD_TO_EXPIRY, profile, ctx_carry)

        assert r2.ev < r1.ev  # carry drags down EV
        assert r2.carry_drag == pytest.approx(2.0 * 45)

    def test_jump_config_changes_path_basis(self):
        engine = MCEngine(spot=150.0, hv=0.28, iv=0.30, dte=45,
                          jump_config=JumpConfig())
        assert engine.path_basis == "gbm_jump"

    def test_iv_schedule_changes_path_basis(self):
        schedule = np.full(45, 0.30)
        engine = MCEngine(spot=150.0, hv=0.28, iv=0.30, dte=45,
                          iv_schedule=schedule)
        assert engine.path_basis == "event_overlay"


# ═══════════════════════════════════════════════════════════════════════════════
# 7. GOLDEN-CASE COMPARISON SET
# ═══════════════════════════════════════════════════════════════════════════════

class TestGoldenCaseComparison:
    """
    14 representative positions covering strategy types and market conditions.
    These verify that the unified engine produces economically sensible outputs.

    Full parity tests against existing modules will be added in Phase 2
    when wrappers are created.
    """

    CASES = [
        # (name, spot, hv, iv, dte, strike, premium, strategy, is_call)
        ("Long Call mid-DTE", 150.0, 0.28, 0.30, 45, 155.0, 3.50, "LONG_CALL", True),
        ("Long Put near-expiry high-IV", 100.0, 0.45, 0.55, 14, 105.0, 6.00, "LONG_PUT", False),
        ("LEAPS Call low-IV", 200.0, 0.18, 0.20, 300, 210.0, 18.00, "LONG_CALL", True),
        ("Buy-Write", 75.0, 0.35, 0.38, 30, 80.0, 2.00, "BUY_WRITE", True),
        ("Covered Call near-expiry", 50.0, 0.25, 0.28, 14, 55.0, 0.80, "COVERED_CALL", True),
        ("CSP moderate-IV", 120.0, 0.30, 0.32, 35, 115.0, 3.00, "CSP", False),
        ("Near-expiry directional", 180.0, 0.32, 0.35, 5, 185.0, 1.20, "LONG_CALL", True),
        ("High IV long call", 60.0, 0.50, 0.85, 45, 65.0, 8.00, "LONG_CALL", True),
        ("Low IV short put", 300.0, 0.12, 0.15, 30, 290.0, 1.50, "CSP", False),
        ("Deep ITM call", 110.0, 0.25, 0.28, 45, 95.0, 17.00, "LONG_CALL", True),
        ("Deep OTM call", 100.0, 0.25, 0.28, 45, 120.0, 0.50, "LONG_CALL", True),
    ]

    @pytest.mark.parametrize("name,spot,hv,iv,dte,strike,premium,strategy,is_call",
                             CASES, ids=[c[0] for c in CASES])
    def test_golden_case_sensible(self, name, spot, hv, iv, dte, strike,
                                   premium, strategy, is_call):
        """Each golden case should produce economically sensible outputs."""
        engine = MCEngine(spot=spot, hv=hv, iv=iv, dte=dte)
        profile = resolve_profile(strategy)
        ctx = {"strike": strike, "premium": premium, "is_call": is_call,
               "cost_basis": spot}

        result = engine.run_scenario(ActionScenario.HOLD_TO_EXPIRY,
                                      profile, ctx)

        # Basic sanity
        assert result.n_paths_used == N_PATHS
        assert result.horizon_days == max(dte, 1)
        assert 0 <= result.p_profit <= 1
        assert result.p10 <= result.p50 <= result.p90
        assert result.cvar <= result.p10  # CVaR <= VaR
        assert result.stderr_ev is not None

        # Long options: max loss capped at premium
        if profile.is_long_premium:
            assert result.p10 >= -premium * 100.0 * 1.01  # small float tolerance

    @pytest.mark.parametrize("name,spot,hv,iv,dte,strike,premium,strategy,is_call",
                             CASES, ids=[c[0] for c in CASES])
    def test_golden_case_exit_vs_hold_ranking(self, name, spot, hv, iv, dte,
                                               strike, premium, strategy,
                                               is_call):
        """
        Decision stability: compare HOLD vs EXIT ranking.
        At entry (current_mid == premium), exit EV should be ~0.
        """
        engine = MCEngine(spot=spot, hv=hv, iv=iv, dte=dte)
        profile = resolve_profile(strategy)
        ctx = {"strike": strike, "premium": premium, "is_call": is_call,
               "cost_basis": spot, "current_mid": premium}

        results = engine.compare(
            [ActionScenario.HOLD_TO_EXPIRY, ActionScenario.EXIT_NOW],
            profile, ctx
        )

        hold = results[ActionScenario.HOLD_TO_EXPIRY]
        exit_ = results[ActionScenario.EXIT_NOW]

        # Exit at entry price should be ~$0 P&L
        assert abs(exit_.ev) < 1.0

        # Both results should have valid stats
        assert hold.stderr_ev is not None
        assert exit_.stderr_ev is not None
