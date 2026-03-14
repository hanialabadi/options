"""
Tests for the Strategy Interpreter package.

Validates:
  1. Registry maps strategies to correct interpreter families
  2. Each interpreter produces valid ScoredResult with transparent components
  3. Strategy-specific scoring behaves correctly (G1-G5 fixes)
  4. Vol interpretation is strategy-aware (buyers vs sellers)
  5. Edge cases: missing data, zero values, NaN
"""

import pytest
import pandas as pd
import numpy as np

from scan_engine.interpreters import (
    get_interpreter, get_all_interpreters,
    DirectionalInterpreter, LeapInterpreter,
    IncomeInterpreter, VolatilityInterpreter,
    ScoredResult, ScoredComponent, VolContext,
)
from scan_engine.interpreters.base import _sf, _ss, _expected_move, _breakeven_distance_pct


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_row(**kwargs) -> pd.Series:
    """Create a minimal row with defaults for common columns."""
    defaults = {
        'Strategy_Name': 'Long Call',
        'Last': 100.0,
        'Delta': 0.55,
        'Gamma': 0.05,
        'Vega': 0.15,
        'Theta': -0.03,
        'IV_30D': 30.0,
        'HV_20D': 25.0,
        'IV_Rank_30D': 50.0,
        'adx_14': 30.0,
        'rsi_14': 60.0,
        'Actual_DTE': 45,
        'Selected_Strike': 100.0,
        'Mid_Price': 3.50,
        'Bid_Ask_Spread_Pct': 5.0,
        'Open_Interest': 500,
        'Weekly_Trend_Bias': 'BULLISH',
        'Market_Structure': 'UPTREND',
        'Surface_Shape': 'CONTANGO',
        'Entry_Timing_Quality': 'MODERATE',
        'Price_vs_SMA20': 2.0,
    }
    defaults.update(kwargs)
    return pd.Series(defaults)


# ── Registry Tests ────────────────────────────────────────────────────────────

class TestRegistry:
    def test_long_call_maps_to_directional(self):
        assert isinstance(get_interpreter('Long Call'), DirectionalInterpreter)

    def test_long_put_maps_to_directional(self):
        assert isinstance(get_interpreter('Long Put'), DirectionalInterpreter)

    def test_leap_call_maps_to_leap(self):
        assert isinstance(get_interpreter('Long Call LEAP'), LeapInterpreter)

    def test_leap_put_maps_to_leap(self):
        assert isinstance(get_interpreter('Long Put LEAP'), LeapInterpreter)

    def test_csp_maps_to_income(self):
        assert isinstance(get_interpreter('Cash-Secured Put'), IncomeInterpreter)

    def test_covered_call_maps_to_income(self):
        assert isinstance(get_interpreter('Covered Call'), IncomeInterpreter)

    def test_buy_write_maps_to_income(self):
        assert isinstance(get_interpreter('Buy-Write'), IncomeInterpreter)

    def test_straddle_maps_to_volatility(self):
        assert isinstance(get_interpreter('Long Straddle'), VolatilityInterpreter)

    def test_strangle_maps_to_volatility(self):
        assert isinstance(get_interpreter('Long Strangle'), VolatilityInterpreter)

    def test_unknown_falls_back_to_directional(self):
        assert isinstance(get_interpreter('Unknown Strategy'), DirectionalInterpreter)

    def test_case_insensitive(self):
        assert isinstance(get_interpreter('LONG CALL'), DirectionalInterpreter)
        assert isinstance(get_interpreter('cash-secured put'), IncomeInterpreter)

    def test_four_interpreters_total(self):
        assert len(get_all_interpreters()) == 4


# ── ScoredResult Tests ────────────────────────────────────────────────────────

class TestScoredResult:
    def test_score_has_components(self):
        row = _make_row()
        result = get_interpreter('Long Call').score(row)
        assert isinstance(result, ScoredResult)
        assert len(result.components) > 0
        assert result.score > 0
        assert result.max_possible > 0

    def test_breakdown_str_not_empty(self):
        row = _make_row()
        result = get_interpreter('Long Call').score(row)
        breakdown = result.to_breakdown_str()
        assert '|' in breakdown
        assert '/' in breakdown

    def test_json_serializable(self):
        import json
        row = _make_row()
        result = get_interpreter('Long Call').score(row)
        parsed = json.loads(result.to_json())
        assert isinstance(parsed, dict)
        assert all('score' in v and 'max' in v for v in parsed.values())

    def test_status_classification(self):
        row = _make_row()
        result = get_interpreter('Long Call').score(row)
        assert result.status in ('Strong', 'Eligible', 'Weak')

    def test_interpretation_not_empty(self):
        row = _make_row()
        result = get_interpreter('Long Call').score(row)
        assert len(result.interpretation) > 0


# ── G1: Gamma Responsiveness (Directional) ────────────────────────────────────

class TestGammaResponsiveness:
    """G1 fix: directional scores should include gamma."""

    def test_high_gamma_scores_well(self):
        row = _make_row(Gamma=0.08, Last=100)
        result = get_interpreter('Long Call').score(row)
        gamma_comp = result.components.get('gamma_response')
        assert gamma_comp is not None
        assert gamma_comp.score >= 8

    def test_zero_gamma_scores_zero(self):
        row = _make_row(Gamma=0.0)
        result = get_interpreter('Long Call').score(row)
        gamma_comp = result.components['gamma_response']
        assert gamma_comp.score == 0

    def test_low_gamma_penalized(self):
        row = _make_row(Gamma=0.005, Last=100)
        result = get_interpreter('Long Call').score(row)
        gamma_comp = result.components['gamma_response']
        assert gamma_comp.score < 5

    def test_leap_has_no_gamma_component(self):
        """LEAPs inherently have near-zero gamma — not scored."""
        row = _make_row(Strategy_Name='Long Call LEAP', Actual_DTE=365, Gamma=0.002)
        result = get_interpreter('Long Call LEAP').score(row)
        assert 'gamma_response' not in result.components


# ── G2: Trend Strength Weighting ──────────────────────────────────────────────

class TestTrendStrength:
    """G2 fix: ADX magnitude should be weighted, not just tiered."""

    def test_adx_48_beats_adx_22(self):
        row_strong = _make_row(adx_14=48)
        row_weak = _make_row(adx_14=22)
        result_strong = get_interpreter('Long Call').score(row_strong)
        result_weak = get_interpreter('Long Call').score(row_weak)
        ts_strong = result_strong.components['trend_strength'].score
        ts_weak = result_weak.components['trend_strength'].score
        assert ts_strong > ts_weak

    def test_ranging_penalized_for_directional(self):
        row = _make_row(adx_14=12)
        result = get_interpreter('Long Call').score(row)
        ts = result.components['trend_strength']
        assert ts.score < 5  # should be negative or very low

    def test_ranging_rewarded_for_income(self):
        row = _make_row(Strategy_Name='Cash-Secured Put', adx_14=12)
        result = get_interpreter('Cash-Secured Put').score(row)
        ts = result.components['trend_safety']
        assert ts.score >= 8


# ── G3: Expected Move vs Breakeven ────────────────────────────────────────────

class TestMoveCoverage:
    """G3 fix: directional should compare expected move to breakeven."""

    def test_good_coverage_scores_well(self):
        # IV=30, DTE=45, Price=100 → em ≈ 10.5
        # Strike=102, Premium=3.50 → breakeven=105.50 → distance=5.5%
        # em_pct ≈ 10.5% → coverage ≈ 1.9×
        row = _make_row(IV_30D=30, Actual_DTE=45, Last=100,
                        Selected_Strike=102, Mid_Price=3.50)
        result = get_interpreter('Long Call').score(row)
        mc = result.components['move_coverage']
        assert mc.score >= 10

    def test_poor_coverage_penalized(self):
        # Far OTM: strike=130, premium=0.50 → breakeven=130.50 → distance=30.5%
        # em_pct ≈ 10.5% → coverage ≈ 0.34×
        row = _make_row(IV_30D=30, Actual_DTE=45, Last=100,
                        Selected_Strike=130, Mid_Price=0.50)
        result = get_interpreter('Long Call').score(row)
        mc = result.components['move_coverage']
        assert mc.score <= 3

    def test_volatility_has_move_coverage(self):
        row = _make_row(Strategy_Name='Long Straddle', Delta=0.02,
                        IV_30D=30, Actual_DTE=45, Last=100, Mid_Price=5.0)
        result = get_interpreter('Long Straddle').score(row)
        assert 'move_coverage' in result.components


# ── G4: LEAP vs Directional Differentiation ───────────────────────────────────

class TestLeapDifferentiation:
    """G4 fix: LEAPs should weight vega and trend durability, not momentum."""

    def test_leap_has_vega_component(self):
        row = _make_row(Strategy_Name='Long Call LEAP', Actual_DTE=365, Vega=0.35)
        result = get_interpreter('Long Call LEAP').score(row)
        assert 'vega_exposure' in result.components
        assert result.components['vega_exposure'].score >= 10

    def test_leap_has_trend_durability(self):
        row = _make_row(Strategy_Name='Long Call LEAP', Actual_DTE=365)
        result = get_interpreter('Long Call LEAP').score(row)
        assert 'trend_durability' in result.components

    def test_leap_long_dte_is_feature(self):
        row = _make_row(Strategy_Name='Long Call LEAP', Actual_DTE=365)
        result = get_interpreter('Long Call LEAP').score(row)
        dte = result.components['dte_quality']
        assert dte.score >= 8  # 180-540d is sweet spot

    def test_directional_long_dte_is_penalty(self):
        row = _make_row(Strategy_Name='Long Call', Actual_DTE=365)
        result = get_interpreter('Long Call').score(row)
        dte = result.components['dte_fit']
        assert dte.score <= 5  # too long for short-dated

    def test_leap_has_term_structure(self):
        row = _make_row(Strategy_Name='Long Call LEAP', Actual_DTE=365)
        result = get_interpreter('Long Call LEAP').score(row)
        assert 'term_structure' in result.components


# ── G5: Income Premium Yield ──────────────────────────────────────────────────

class TestIncomeYield:
    """G5 fix: income strategies should score premium yield."""

    def test_high_yield_scores_well(self):
        # Premium=2.0, Strike=50, DTE=30 → yield = 2*100/(50*100) = 4%
        # Annualized = 4% × 365/30 ≈ 48.7%
        row = _make_row(Strategy_Name='Cash-Secured Put', Mid_Price=2.0,
                        Selected_Strike=50, Actual_DTE=30, Last=52, Delta=-0.20)
        result = get_interpreter('Cash-Secured Put').score(row)
        py = result.components['premium_yield']
        assert py.score >= 15

    def test_thin_yield_penalized(self):
        # Premium=0.10, Strike=200, DTE=30 → yield = 0.01/200 = 0.05%
        row = _make_row(Strategy_Name='Cash-Secured Put', Mid_Price=0.10,
                        Selected_Strike=200, Actual_DTE=30, Last=210, Delta=-0.05)
        result = get_interpreter('Cash-Secured Put').score(row)
        py = result.components['premium_yield']
        assert py.score <= 5


# ── Vol Interpretation Inversion ──────────────────────────────────────────────

class TestVolInterpretation:
    """Verify IV gap is interpreted opposite for buyers vs sellers."""

    def test_directional_cheap_iv_favorable(self):
        row = _make_row(IV_30D=20, HV_20D=30)
        ctx = get_interpreter('Long Call').interpret_volatility(row)
        assert ctx.edge_direction == 'FAVORABLE'
        assert ctx.regime == 'CHEAP_VOL'

    def test_directional_rich_iv_unfavorable(self):
        row = _make_row(IV_30D=40, HV_20D=25)
        ctx = get_interpreter('Long Call').interpret_volatility(row)
        assert ctx.edge_direction == 'UNFAVORABLE'

    def test_income_rich_iv_favorable(self):
        row = _make_row(Strategy_Name='Cash-Secured Put', IV_30D=40, HV_20D=25, IV_Rank_30D=70)
        ctx = get_interpreter('Cash-Secured Put').interpret_volatility(row)
        assert ctx.edge_direction == 'FAVORABLE'

    def test_income_cheap_iv_unfavorable(self):
        row = _make_row(Strategy_Name='Cash-Secured Put', IV_30D=18, HV_20D=30, IV_Rank_30D=20)
        ctx = get_interpreter('Cash-Secured Put').interpret_volatility(row)
        assert ctx.edge_direction == 'UNFAVORABLE'

    def test_leap_low_rank_favorable(self):
        row = _make_row(Strategy_Name='Long Call LEAP', IV_Rank_30D=15, IV_30D=20, HV_20D=22)
        ctx = get_interpreter('Long Call LEAP').interpret_volatility(row)
        assert ctx.edge_direction == 'FAVORABLE'

    def test_vol_reconciliation_provided(self):
        """When IV rank is high but IV < HV, reconciliation should explain."""
        row = _make_row(IV_30D=25, HV_20D=30, IV_Rank_30D=90)
        ctx = get_interpreter('Long Call').interpret_volatility(row)
        assert len(ctx.reconciliation) > 0


# ── Edge Cases ────────────────────────────────────────────────────────────────

class TestEdgeCases:
    def test_missing_data_does_not_crash(self):
        row = pd.Series({'Strategy_Name': 'Long Call'})
        result = get_interpreter('Long Call').score(row)
        assert isinstance(result, ScoredResult)
        assert result.score >= 0

    def test_nan_values_handled(self):
        row = _make_row(Delta=float('nan'), Gamma=float('nan'), IV_30D=float('nan'))
        result = get_interpreter('Long Call').score(row)
        assert isinstance(result, ScoredResult)

    def test_zero_price_handled(self):
        row = _make_row(Last=0, Selected_Strike=0)
        result = get_interpreter('Long Call').score(row)
        assert isinstance(result, ScoredResult)

    def test_all_interpreters_score_without_crash(self):
        """Every interpreter should handle a basic row without error."""
        strategies = [
            'Long Call', 'Long Put', 'Long Call LEAP', 'Long Put LEAP',
            'Cash-Secured Put', 'Covered Call', 'Buy-Write',
            'Long Straddle', 'Long Strangle',
        ]
        for s in strategies:
            row = _make_row(Strategy_Name=s)
            result = get_interpreter(s).score(row)
            assert isinstance(result, ScoredResult), f"Failed for {s}"
            assert result.max_possible > 0, f"No max possible for {s}"


# ── Base Helpers ──────────────────────────────────────────────────────────────

class TestBaseHelpers:
    def test_sf_extracts_float(self):
        row = pd.Series({'a': 42.5, 'b': 'not a number'})
        assert _sf(row, 'a') == 42.5
        assert _sf(row, 'c', default=99) == 99

    def test_sf_handles_nan(self):
        row = pd.Series({'a': float('nan')})
        assert _sf(row, 'a', default=0) == 0

    def test_ss_extracts_string(self):
        row = pd.Series({'a': 'HELLO', 'b': None})
        assert _ss(row, 'a') == 'HELLO'
        assert _ss(row, 'b', default='X') == 'X'

    def test_expected_move_calculation(self):
        row = pd.Series({'Last': 100, 'IV_30D': 30, 'Actual_DTE': 365})
        em = _expected_move(row)
        assert 28 < em < 32  # ~30% of $100

    def test_breakeven_distance_call(self):
        row = pd.Series({
            'Strategy_Name': 'Long Call', 'Last': 100,
            'Selected_Strike': 105, 'Mid_Price': 3.0,
        })
        dist = _breakeven_distance_pct(row)
        assert 7 < dist < 9  # breakeven at 108, distance 8%

    def test_breakeven_distance_put(self):
        row = pd.Series({
            'Strategy_Name': 'Long Put', 'Last': 100,
            'Selected_Strike': 95, 'Mid_Price': 2.0,
        })
        dist = _breakeven_distance_pct(row)
        assert 6 < dist < 8  # breakeven at 93, distance 7%
