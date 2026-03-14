"""
Tests for institutional-grade chart engine signals.

Covers:
1. Market Structure (HH/HL swing point detection)
2. OBV slope + breakout volume
3. RSI/MACD divergence detection
4. Multi-timeframe weekly trend bias
5. Keltner Channel squeeze detection
6. Relative strength vs SPY
7. Evaluator wiring (directional, income, volatility)
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from scan_engine.step4_chart_signals import (
    _classify_market_structure,
    _compute_obv_metrics,
    _detect_divergence,
    _compute_weekly_bias,
    _detect_keltner_squeeze,
)
from scan_engine.evaluators.directional import evaluate_directional
from scan_engine.evaluators.income import evaluate_income
from scan_engine.evaluators.volatility import evaluate_volatility


# ── Fixtures ─────────────────────────────────────────────────

def _make_uptrend(n=100, seed=42):
    """Create realistic uptrend data with oscillations."""
    np.random.seed(seed)
    base = np.cumsum(np.random.randn(n) * 2 + 0.3) + 100
    highs = pd.Series(base + np.random.uniform(0.5, 2, n))
    lows = pd.Series(base - np.random.uniform(0.5, 2, n))
    close = pd.Series(base)
    volume = pd.Series(np.random.uniform(1e6, 5e6, n))
    return highs, lows, close, volume


def _make_downtrend(n=100, seed=99):
    """Create realistic downtrend data."""
    np.random.seed(seed)
    base = np.cumsum(np.random.randn(n) * 2 - 0.4) + 200
    highs = pd.Series(base + np.random.uniform(0.5, 2, n))
    lows = pd.Series(base - np.random.uniform(0.5, 2, n))
    close = pd.Series(base)
    volume = pd.Series(np.random.uniform(1e6, 5e6, n))
    return highs, lows, close, volume


def _make_consolidation(n=100, seed=77):
    """Create ranging/consolidation data."""
    np.random.seed(seed)
    base = np.sin(np.linspace(0, 8 * np.pi, n)) * 5 + 150
    noise = np.random.randn(n) * 0.5
    highs = pd.Series(base + noise + 2)
    lows = pd.Series(base + noise - 2)
    close = pd.Series(base + noise)
    volume = pd.Series(np.random.uniform(1e6, 3e6, n))
    return highs, lows, close, volume


def _base_directional_row(**overrides):
    """Base row for directional evaluator."""
    row = {
        'Strategy': 'Long Call', 'Delta': 0.55, 'Gamma': 0.03, 'Vega': 0.25,
        'Actual_DTE': 45, 'Trend': 'Bullish', 'Signal_Type': 'Bullish',
        'Price_vs_SMA20': 5.0, 'Volume_Trend': 'Rising', 'Stock_Price': 150,
        'RSI': 60, 'MACD_Histogram': 0.5, 'SlowK_5_3': 60, 'BB_Position': 55,
        'Days_Since_Cross': 3, 'Chart_Regime': 'Trending',
        'Market_Structure': 'Unknown', 'OBV_Slope': np.nan,
        'Volume_Ratio': np.nan, 'RSI_Divergence': 'None',
        'MACD_Divergence': 'None', 'Weekly_Trend_Bias': 'Unknown',
        'Keltner_Squeeze_On': False, 'Keltner_Squeeze_Fired': False,
        'RS_vs_SPY_20d': np.nan,
    }
    row.update(overrides)
    return pd.Series(row)


def _base_income_row(**overrides):
    """Base row for income evaluator."""
    row = {
        'Strategy': 'Cash-Secured Put', 'Theta': -0.5, 'Vega': 0.3,
        'Gamma': -0.01, 'IVHV_gap_30D': 5.0, 'Probability_Of_Profit': 72.0,
        'RV_IV_Ratio': 0.85, 'IV_Rank_30D': 55, 'Actual_DTE': 30,
        'Trend': 'Bullish', 'Signal_Type': 'Bullish', 'Price_vs_SMA20': 3.0,
        'Stock_Price': 100, 'RSI': 45, 'SlowK_5_3': 40, 'BB_Position': 35,
        'Chart_Regime': 'Ranging',
        'Market_Structure': 'Unknown', 'OBV_Slope': np.nan,
        'RSI_Divergence': 'None', 'Weekly_Trend_Bias': 'Unknown',
        'Keltner_Squeeze_On': False, 'Keltner_Squeeze_Fired': False,
    }
    row.update(overrides)
    return pd.Series(row)


def _base_vol_row(**overrides):
    """Base row for volatility evaluator."""
    row = {
        'Strategy': 'Long Straddle', 'Delta': 0.05, 'Gamma': 0.02,
        'Vega': 0.45, 'Theta': -0.15, 'Put_Call_Skew': 1.05,
        'RV_IV_Ratio': 1.15, 'IV_Percentile': 25, 'IV_Rank_30D': 25,
        'Actual_DTE': 30, 'Stock_Price': 200,
        'Volatility_Regime': 'Compression',
        'Keltner_Squeeze_On': False, 'Keltner_Squeeze_Fired': False,
        'Market_Structure': 'Unknown',
    }
    row.update(overrides)
    return pd.Series(row)


# ── 1. Market Structure Tests ────────────────────────────────

class TestMarketStructure:
    def test_uptrend_detection(self):
        h, l, _, _ = _make_uptrend()
        result = _classify_market_structure(h, l)
        assert result == 'Uptrend'

    def test_consolidation_detection(self):
        h, l, _, _ = _make_consolidation()
        result = _classify_market_structure(h, l)
        assert result == 'Consolidation'

    def test_insufficient_data(self):
        h = pd.Series([100, 101, 102])
        l = pd.Series([99, 100, 101])
        assert _classify_market_structure(h, l) == 'Unknown'

    def test_unknown_with_no_swings(self):
        """Monotonically increasing — no swing points detected."""
        h = pd.Series(range(100, 200))
        l = pd.Series(range(99, 199))
        assert _classify_market_structure(h, l) == 'Unknown'


# ── 2. OBV Metrics Tests ────────────────────────────────────

class TestOBVMetrics:
    def test_accumulating(self):
        """Rising price + high volume → positive OBV slope."""
        np.random.seed(42)
        close = pd.Series(np.cumsum(np.ones(50) * 0.5) + 100)
        volume = pd.Series(np.ones(50) * 1e6)
        result = _compute_obv_metrics(close, volume)
        assert result['OBV_Slope'] > 0

    def test_volume_ratio(self):
        """High last-bar volume → ratio > 1."""
        np.random.seed(42)
        close = pd.Series(np.random.randn(50).cumsum() + 100)
        volume = pd.Series(np.ones(50) * 1e6)
        volume.iloc[-1] = 3e6  # 3x spike
        result = _compute_obv_metrics(close, volume)
        assert result['Volume_Ratio'] > 2.5

    def test_empty_volume(self):
        close = pd.Series([100, 101])
        volume = pd.Series([0, 0])
        result = _compute_obv_metrics(close, volume)
        assert np.isnan(result['OBV_Slope'])

    def test_insufficient_data(self):
        result = _compute_obv_metrics(pd.Series([100]), pd.Series([1e6]), period=20)
        assert np.isnan(result['OBV_Slope'])


# ── 3. Divergence Tests ─────────────────────────────────────

class TestDivergence:
    def test_bearish_divergence(self):
        """Price HH but RSI LH → bearish divergence."""
        # Create price with higher highs
        price = pd.Series([100, 102, 104, 103, 101, 100, 102, 104, 106, 104,
                           102, 101, 103, 105, 108, 106, 104, 103])
        # RSI with lower highs at same points
        rsi = pd.Series([55, 65, 72, 68, 60, 55, 62, 70, 69, 64,
                         58, 54, 60, 66, 65, 61, 56, 53])
        result = _detect_divergence(price, rsi, lookback=14)
        assert result == 'Bearish_Divergence'

    def test_bullish_divergence(self):
        """Price LL but RSI HL → bullish divergence."""
        price = pd.Series([100, 98, 96, 97, 99, 100, 98, 96, 94, 95,
                           97, 98, 96, 94, 93, 94, 96, 97])
        rsi = pd.Series([50, 40, 28, 32, 42, 48, 38, 30, 32, 38,
                         44, 46, 36, 32, 35, 39, 44, 47])
        result = _detect_divergence(price, rsi, lookback=14)
        assert result == 'Bullish_Divergence'

    def test_no_divergence(self):
        """Price and indicator moving in sync → no divergence."""
        price = pd.Series(np.linspace(100, 120, 20) + np.sin(np.linspace(0, 4*np.pi, 20)) * 2)
        rsi = pd.Series(np.linspace(40, 70, 20) + np.sin(np.linspace(0, 4*np.pi, 20)) * 5)
        result = _detect_divergence(price, rsi, lookback=14)
        assert result == 'None'

    def test_insufficient_data(self):
        assert _detect_divergence(pd.Series([1, 2, 3]), pd.Series([50, 55, 60])) == 'None'


# ── 4. Weekly Bias Tests ─────────────────────────────────────

class TestWeeklyBias:
    def test_aligned_bullish(self):
        """Uptrend on both daily and weekly → ALIGNED."""
        dates = pd.date_range('2025-06-01', periods=180, freq='B')
        np.random.seed(42)
        close = np.cumsum(np.random.randn(180) * 1.5 + 0.1) + 200
        df = pd.DataFrame({
            'Open': close - 1, 'High': close + 2, 'Low': close - 2, 'Close': close
        }, index=dates)
        assert _compute_weekly_bias(df, 'Bullish') == 'ALIGNED'

    def test_conflicting(self):
        """Uptrend weekly but bearish daily → CONFLICTING."""
        dates = pd.date_range('2025-06-01', periods=180, freq='B')
        np.random.seed(42)
        close = np.cumsum(np.random.randn(180) * 1.5 + 0.1) + 200
        df = pd.DataFrame({
            'Open': close - 1, 'High': close + 2, 'Low': close - 2, 'Close': close
        }, index=dates)
        assert _compute_weekly_bias(df, 'Bearish') == 'CONFLICTING'

    def test_insufficient_data(self):
        dates = pd.date_range('2025-01-01', periods=20, freq='B')
        close = np.ones(20) * 100
        df = pd.DataFrame({
            'Open': close, 'High': close + 1, 'Low': close - 1, 'Close': close
        }, index=dates)
        assert _compute_weekly_bias(df, 'Bullish') == 'Unknown'


# ── 5. Keltner Squeeze Tests ────────────────────────────────

class TestKeltnerSqueeze:
    def test_squeeze_on(self):
        """BB inside Keltner → squeeze active."""
        n = 10
        ema21 = pd.Series([100.0] * n)
        atr = pd.Series([5.0] * n)
        # Keltner: 100 ± 7.5 = [92.5, 107.5]
        # BB inside: [95, 105]
        upper_bb = pd.Series([105.0] * n)
        lower_bb = pd.Series([95.0] * n)
        result = _detect_keltner_squeeze(upper_bb, lower_bb, ema21, atr)
        assert result['Squeeze_On'] is True
        assert result['Squeeze_Fired'] is False

    def test_no_squeeze(self):
        """BB outside Keltner → no squeeze."""
        n = 10
        ema21 = pd.Series([100.0] * n)
        atr = pd.Series([3.0] * n)
        # Keltner: 100 ± 4.5 = [95.5, 104.5]
        # BB outside: [94, 106]
        upper_bb = pd.Series([106.0] * n)
        lower_bb = pd.Series([94.0] * n)
        result = _detect_keltner_squeeze(upper_bb, lower_bb, ema21, atr)
        assert result['Squeeze_On'] is False

    def test_squeeze_fired(self):
        """Was in squeeze yesterday, not today → fired."""
        ema21 = pd.Series([100.0, 100.0])
        atr = pd.Series([5.0, 5.0])
        # Yesterday: BB [95, 105] inside Keltner [92.5, 107.5] → squeeze
        # Today: BB [91, 109] outside Keltner → released
        upper_bb = pd.Series([105.0, 109.0])
        lower_bb = pd.Series([95.0, 91.0])
        result = _detect_keltner_squeeze(upper_bb, lower_bb, ema21, atr)
        assert result['Squeeze_On'] is False
        assert result['Squeeze_Fired'] is True

    def test_nan_handling(self):
        result = _detect_keltner_squeeze(
            pd.Series([np.nan]), pd.Series([np.nan]),
            pd.Series([np.nan]), pd.Series([np.nan])
        )
        assert result['Squeeze_On'] is False


# ── 6. Evaluator Wiring Tests ───────────────────────────────

class TestDirectionalWiring:
    def test_market_structure_confirms(self):
        r_confirm = evaluate_directional(_base_directional_row(Market_Structure='Uptrend'))
        r_base = evaluate_directional(_base_directional_row())
        assert r_confirm.theory_compliance_score > r_base.theory_compliance_score

    def test_market_structure_contradicts(self):
        r = evaluate_directional(_base_directional_row(Market_Structure='Downtrend'))
        assert 'contradicts direction' in r.evaluation_notes

    def test_obv_accumulating_bonus(self):
        r = evaluate_directional(_base_directional_row(OBV_Slope=20.0))
        assert 'OBV accumulating' in r.evaluation_notes

    def test_obv_distributing_penalty(self):
        r = evaluate_directional(_base_directional_row(OBV_Slope=-15.0))
        assert 'OBV distributing' in r.evaluation_notes

    def test_rsi_divergence_penalty(self):
        r = evaluate_directional(_base_directional_row(RSI_Divergence='Bearish_Divergence'))
        assert 'Bearish_Divergence' in r.evaluation_notes

    def test_macd_divergence_confirms(self):
        r = evaluate_directional(_base_directional_row(MACD_Divergence='Bullish_Divergence'))
        assert 'Bullish_Divergence' in r.evaluation_notes and 'confirms' in r.evaluation_notes

    def test_weekly_aligned_bonus(self):
        r_aligned = evaluate_directional(_base_directional_row(Weekly_Trend_Bias='ALIGNED'))
        r_base = evaluate_directional(_base_directional_row())
        assert r_aligned.theory_compliance_score > r_base.theory_compliance_score

    def test_weekly_conflicting_penalty(self):
        r = evaluate_directional(_base_directional_row(Weekly_Trend_Bias='CONFLICTING'))
        assert 'CONFLICTING' in r.evaluation_notes

    def test_squeeze_fired_bonus(self):
        r = evaluate_directional(_base_directional_row(
            Keltner_Squeeze_Fired=True, MACD_Histogram=0.5
        ))
        assert 'squeeze FIRED' in r.evaluation_notes

    def test_breakout_volume(self):
        r = evaluate_directional(_base_directional_row(
            Volume_Ratio=2.0, Chart_Regime='Compressed'
        ))
        assert 'Breakout volume' in r.evaluation_notes

    def test_rs_outperforming_bonus(self):
        r = evaluate_directional(_base_directional_row(RS_vs_SPY_20d=6.0))
        assert 'Outperforming SPY' in r.evaluation_notes

    def test_all_confirming_vs_all_contradicting(self):
        """Score spread should be >30 points with all signals."""
        r_good = evaluate_directional(_base_directional_row(
            Market_Structure='Uptrend', OBV_Slope=15, Volume_Ratio=2.0,
            RSI_Divergence='Bullish_Divergence', MACD_Divergence='Bullish_Divergence',
            Weekly_Trend_Bias='ALIGNED', Keltner_Squeeze_Fired=True,
            RS_vs_SPY_20d=6.0,
        ))
        r_bad = evaluate_directional(_base_directional_row(
            Market_Structure='Downtrend', OBV_Slope=-15,
            RSI_Divergence='Bearish_Divergence', MACD_Divergence='Bearish_Divergence',
            Weekly_Trend_Bias='CONFLICTING', RS_vs_SPY_20d=-8.0,
        ))
        diff = r_good.theory_compliance_score - r_bad.theory_compliance_score
        assert diff > 30, f"Expected >30pt spread, got {diff:.0f}"


class TestIncomeWiring:
    def test_consolidation_bonus(self):
        r = evaluate_income(_base_income_row(Market_Structure='Consolidation'))
        assert 'Consolidation' in r.evaluation_notes and 'stable' in r.evaluation_notes

    def test_downtrend_penalty_for_csp(self):
        r = evaluate_income(_base_income_row(Market_Structure='Downtrend'))
        assert 'Downtrend' in r.evaluation_notes

    def test_squeeze_on_bonus(self):
        r = evaluate_income(_base_income_row(Keltner_Squeeze_On=True))
        assert 'Keltner squeeze active' in r.evaluation_notes

    def test_squeeze_fired_penalty(self):
        r = evaluate_income(_base_income_row(Keltner_Squeeze_Fired=True))
        assert 'FIRED' in r.evaluation_notes

    def test_bearish_divergence_csp_warning(self):
        r = evaluate_income(_base_income_row(RSI_Divergence='Bearish_Divergence'))
        assert 'Bearish RSI divergence' in r.evaluation_notes

    def test_weekly_conflicting_penalty(self):
        r = evaluate_income(_base_income_row(Weekly_Trend_Bias='CONFLICTING'))
        assert 'CONFLICTING' in r.evaluation_notes


class TestVolatilityWiring:
    def test_squeeze_on_big_bonus(self):
        r_sq = evaluate_volatility(_base_vol_row(Keltner_Squeeze_On=True))
        r_base = evaluate_volatility(_base_vol_row())
        diff = r_sq.theory_compliance_score - r_base.theory_compliance_score
        assert diff >= 8, f"Expected >=8pt squeeze bonus, got {diff}"
        assert 'Keltner squeeze active' in r_sq.evaluation_notes

    def test_squeeze_fired_bonus(self):
        r = evaluate_volatility(_base_vol_row(Keltner_Squeeze_Fired=True))
        assert 'squeeze FIRED' in r.evaluation_notes

    def test_consolidation_bonus(self):
        r = evaluate_volatility(_base_vol_row(Market_Structure='Consolidation'))
        assert 'Consolidation' in r.evaluation_notes
