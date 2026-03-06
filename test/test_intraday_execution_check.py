"""
Tests for scan_engine/intraday_execution_check.py (Cycle 3)

All tests use synthetic data — no Schwab connection required.
"""

import pytest
import numpy as np
import pandas as pd
from datetime import datetime, time
from unittest.mock import MagicMock, patch

from scan_engine.intraday_execution_check import (
    compute_vwap,
    check_vwap_signal,
    check_momentum,
    check_spread_quality,
    check_iv_spike,
    compute_composite_score,
    classify_readiness,
    evaluate_intraday_readiness,
    _get_strategy_bias,
    _is_income_strategy,
    _is_market_open,
)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _make_bars(closes, volumes=None, base_high_offset=1.0, base_low_offset=1.0):
    """Build synthetic 5-min bars from close prices."""
    if volumes is None:
        volumes = [10000] * len(closes)
    bars = []
    for c, v in zip(closes, volumes):
        bars.append({
            "open": c - 0.1,
            "high": c + base_high_offset,
            "low": c - base_low_offset,
            "close": c,
            "volume": v,
        })
    return bars


def _make_ready_row(**overrides):
    """Build a minimal READY row for testing."""
    defaults = {
        'Ticker': 'MU',
        'Strategy_Name': 'Cash Secured Put',
        'Strategy_Type': 'INCOME',
        'Trade_Bias': 'Bullish',
        'Execution_Status': 'READY',
        'Contract_Symbol': 'MU260320P80',
        'Bid_Ask_Spread_Pct': 2.5,
        'IV_30D': 35.0,
        'Close': 90.0,
    }
    defaults.update(overrides)
    return defaults


def _make_df(rows):
    """Build a DataFrame from a list of row dicts."""
    return pd.DataFrame(rows)


# ── Market hours Eastern time helpers ────────────────────────────────────────

def _market_open_time():
    """Return a datetime during market hours (ET)."""
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo
    return datetime(2026, 3, 5, 10, 30, tzinfo=ZoneInfo("US/Eastern"))


def _market_closed_time():
    """Return a datetime outside market hours (ET)."""
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo
    return datetime(2026, 3, 5, 20, 0, tzinfo=ZoneInfo("US/Eastern"))


def _weekend_time():
    """Return a Saturday datetime (ET)."""
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo
    return datetime(2026, 3, 7, 11, 0, tzinfo=ZoneInfo("US/Eastern"))  # Saturday


# =============================================================================
# TestVWAPComputation
# =============================================================================

class TestVWAPComputation:
    """Tests for VWAP calculation from 5-min bars."""

    def test_vwap_basic(self):
        """Known bars produce expected VWAP."""
        bars = [
            {"high": 102, "low": 98, "close": 100, "volume": 1000},
            {"high": 104, "low": 100, "close": 102, "volume": 2000},
            {"high": 106, "low": 102, "close": 104, "volume": 3000},
        ]
        vwap = compute_vwap(bars)
        assert vwap is not None
        # tp = [(102+98+100)/3, (104+100+102)/3, (106+102+104)/3] = [100, 102, 104]
        # vwap = (100*1000 + 102*2000 + 104*3000) / (1000+2000+3000) = 616000/6000 = 102.667
        assert abs(vwap - 102.667) < 0.01

    def test_vwap_empty_bars(self):
        """No bars → None."""
        assert compute_vwap([]) is None

    def test_vwap_zero_volume(self):
        """All zero volume → None."""
        bars = _make_bars([100, 101, 102], volumes=[0, 0, 0])
        assert compute_vwap(bars) is None


# =============================================================================
# TestVWAPSignal
# =============================================================================

class TestVWAPSignal:
    """Tests for direction-aware VWAP signal classification."""

    def test_bullish_above_vwap_favorable(self):
        """CSP: price above VWAP → FAVORABLE."""
        assert check_vwap_signal(105.0, 100.0, 'BULLISH') == 'FAVORABLE'

    def test_bullish_below_vwap_unfavorable(self):
        """CSP: price below VWAP → UNFAVORABLE."""
        assert check_vwap_signal(95.0, 100.0, 'BULLISH') == 'UNFAVORABLE'

    def test_bearish_below_vwap_favorable(self):
        """Long Put: price below VWAP → FAVORABLE."""
        assert check_vwap_signal(95.0, 100.0, 'BEARISH') == 'FAVORABLE'

    def test_neutral_strategy(self):
        """Straddle → NEUTRAL regardless of price vs VWAP."""
        assert check_vwap_signal(105.0, 100.0, 'NEUTRAL') == 'NEUTRAL'
        assert check_vwap_signal(95.0, 100.0, 'NEUTRAL') == 'NEUTRAL'


# =============================================================================
# TestIntradayMomentum
# =============================================================================

class TestIntradayMomentum:
    """Tests for 30-min slope momentum classification."""

    def test_momentum_aligned_bullish(self):
        """Rising closes + bullish strategy → ALIGNED."""
        bars = _make_bars([100, 101, 102, 103, 104, 105])
        assert check_momentum(bars, 'BULLISH') == 'ALIGNED'

    def test_momentum_opposing_bullish(self):
        """Falling closes + bullish strategy → OPPOSING."""
        bars = _make_bars([105, 104, 103, 102, 101, 100])
        assert check_momentum(bars, 'BULLISH') == 'OPPOSING'

    def test_momentum_flat(self):
        """Minimal slope → FLAT."""
        bars = _make_bars([100.00, 100.01, 100.00, 100.01, 100.00, 100.01])
        assert check_momentum(bars, 'BULLISH') == 'FLAT'


# =============================================================================
# TestSpreadQuality
# =============================================================================

class TestSpreadQuality:
    """Tests for live spread grading."""

    def test_spread_tight(self):
        """< 3% → TIGHT."""
        grade, widened = check_spread_quality(2.0, 2.0)
        assert grade == 'TIGHT'
        assert widened is False

    def test_spread_normal(self):
        """3-8% → NORMAL."""
        grade, _ = check_spread_quality(5.0, 5.0)
        assert grade == 'NORMAL'

    def test_spread_wide(self):
        """8-15% → WIDE."""
        grade, _ = check_spread_quality(12.0, 10.0)
        assert grade == 'WIDE'

    def test_spread_widened_flag(self):
        """Current > Step10 × 1.5 → Spread_Widened=True."""
        grade, widened = check_spread_quality(8.0, 4.0)
        assert widened is True


# =============================================================================
# TestIVSpike
# =============================================================================

class TestIVSpike:
    """Tests for IV spike detection and strategy-aware classification."""

    def test_iv_spike_up_income_favorable(self):
        """Income + spike > 15% → SPIKE_FAVORABLE (selling rich)."""
        signal, pct = check_iv_spike(42.0, 35.0, is_income=True)
        assert signal == 'SPIKE_FAVORABLE'
        assert pct > 15.0

    def test_iv_spike_up_directional_unfavorable(self):
        """Long Call + spike > 15% → SPIKE_UNFAVORABLE (buying expensive)."""
        signal, pct = check_iv_spike(42.0, 35.0, is_income=False)
        assert signal == 'SPIKE_UNFAVORABLE'
        assert pct > 15.0

    def test_iv_spike_down_income_unfavorable(self):
        """Income + spike < -10% → SPIKE_UNFAVORABLE (selling cheap)."""
        signal, pct = check_iv_spike(30.0, 35.0, is_income=True)
        assert signal == 'SPIKE_UNFAVORABLE'
        assert pct < -10.0

    def test_iv_stable(self):
        """Small change → STABLE."""
        signal, pct = check_iv_spike(36.0, 35.0, is_income=True)
        assert signal == 'STABLE'
        assert abs(pct) < 15.0


# =============================================================================
# TestCompositeScore
# =============================================================================

class TestCompositeScore:
    """Tests for composite score calculation and readiness classification."""

    def test_all_favorable_execute_now(self):
        """All checks favorable → score ≥ 70, EXECUTE_NOW."""
        score = compute_composite_score('FAVORABLE', 'ALIGNED', 'TIGHT', 'SPIKE_FAVORABLE')
        assert score == 100
        assert classify_readiness(score) == 'EXECUTE_NOW'

    def test_mixed_stage_and_wait(self):
        """Some favorable, some not → 50-69, STAGE_AND_WAIT."""
        score = compute_composite_score('FAVORABLE', 'FLAT', 'NORMAL', 'STABLE')
        # 25 + 15 + 20 + 20 = 80 → actually EXECUTE_NOW
        # Use a worse mix:
        score2 = compute_composite_score('FAVORABLE', 'OPPOSING', 'NORMAL', 'STABLE')
        # 25 + 5 + 20 + 20 = 70 → EXECUTE_NOW (boundary)
        # Even worse:
        score3 = compute_composite_score('UNFAVORABLE', 'FLAT', 'NORMAL', 'STABLE')
        # 5 + 15 + 20 + 20 = 60
        assert 50 <= score3 < 70
        assert classify_readiness(score3) == 'STAGE_AND_WAIT'

    def test_all_unfavorable_defer(self):
        """All checks unfavorable → score < 50, DEFER."""
        score = compute_composite_score('UNFAVORABLE', 'OPPOSING', 'ILLIQUID', 'SPIKE_UNFAVORABLE')
        # 5 + 5 + 0 + 5 = 15
        assert score == 15
        assert classify_readiness(score) == 'DEFER'


# =============================================================================
# TestMarketHoursGate
# =============================================================================

class TestMarketHoursGate:
    """Tests for market hours detection and off-hours behavior."""

    def test_off_hours_returns_off_hours(self):
        """Off-hours → all READY rows get OFF_HOURS columns."""
        rows = [_make_ready_row()]
        df = _make_df(rows)
        result = evaluate_intraday_readiness(df, schwab_client=None, now_et=_market_closed_time())

        assert result.at[0, 'Intraday_Readiness'] == 'OFF_HOURS'
        assert result.at[0, 'Intraday_VWAP_Signal'] == 'OFF_HOURS'
        assert result.at[0, 'Intraday_Momentum'] == 'OFF_HOURS'
        assert result.at[0, 'Intraday_Spread_Quality'] == 'OFF_HOURS'
        assert result.at[0, 'Intraday_IV_Spike'] == 'OFF_HOURS'

    def test_non_ready_rows_untouched(self):
        """CONDITIONAL/BLOCKED rows get N/A."""
        rows = [
            _make_ready_row(Execution_Status='CONDITIONAL'),
            _make_ready_row(Execution_Status='BLOCKED'),
        ]
        df = _make_df(rows)
        result = evaluate_intraday_readiness(df, schwab_client=None, now_et=_market_open_time())

        assert result.at[0, 'Intraday_Readiness'] == 'N/A'
        assert result.at[1, 'Intraday_Readiness'] == 'N/A'


# =============================================================================
# TestIntegrationWithPipeline
# =============================================================================

class TestIntegrationWithPipeline:
    """Tests for pipeline integration behavior."""

    def test_columns_added_to_dataframe(self):
        """All 7 new columns present after call (even off-hours)."""
        rows = [_make_ready_row()]
        df = _make_df(rows)
        result = evaluate_intraday_readiness(df, schwab_client=None, now_et=_market_closed_time())

        expected_cols = [
            'Intraday_VWAP_Signal', 'Intraday_Momentum',
            'Intraday_Spread_Quality', 'Intraday_IV_Spike',
            'IV_Spike_Pct', 'Intraday_Execution_Score',
            'Intraday_Readiness',
        ]
        for col in expected_cols:
            assert col in result.columns, f"Missing column: {col}"

    def test_no_schwab_client_graceful(self):
        """schwab_client=None during market hours → OFF_HOURS columns."""
        rows = [_make_ready_row()]
        df = _make_df(rows)
        result = evaluate_intraday_readiness(df, schwab_client=None, now_et=_market_open_time())

        assert result.at[0, 'Intraday_Readiness'] == 'OFF_HOURS'
        assert result.at[0, 'Intraday_VWAP_Signal'] == 'OFF_HOURS'


# =============================================================================
# TestStrategyBias
# =============================================================================

class TestStrategyBias:
    """Tests for strategy bias classification helpers."""

    def test_csp_is_bullish(self):
        row = pd.Series({'Strategy_Name': 'Cash Secured Put', 'Trade_Bias': ''})
        assert _get_strategy_bias(row) == 'BULLISH'

    def test_long_put_is_bearish(self):
        row = pd.Series({'Strategy_Name': 'Long Put', 'Trade_Bias': ''})
        assert _get_strategy_bias(row) == 'BEARISH'

    def test_straddle_is_neutral(self):
        row = pd.Series({'Strategy_Name': 'Straddle', 'Trade_Bias': ''})
        assert _get_strategy_bias(row) == 'NEUTRAL'

    def test_income_detection(self):
        assert _is_income_strategy(pd.Series({'Strategy_Type': 'INCOME', 'Strategy_Name': ''})) is True
        assert _is_income_strategy(pd.Series({'Strategy_Type': 'DIRECTIONAL', 'Strategy_Name': 'Long Call'})) is False


# =============================================================================
# TestMarketOpen
# =============================================================================

class TestMarketOpen:
    """Tests for market hours detection."""

    def test_market_open_weekday(self):
        assert _is_market_open(_market_open_time()) is True

    def test_market_closed_evening(self):
        assert _is_market_open(_market_closed_time()) is False

    def test_market_closed_weekend(self):
        assert _is_market_open(_weekend_time()) is False
