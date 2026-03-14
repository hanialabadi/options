"""
Signal Trajectory Tests
========================
Validates Phase 3: Scan memory via signal trajectory classification,
including Phase 3b: chart-signal-aware TREND_FORMING detection.

Tests:
  1.  EARLY_BREAKOUT detected (accel>15, score>40)
  2.  LATE_CONFIRMATION detected (accel<3, score>70)
  3.  IMPROVING trajectory (accel>5, score>30)
  4.  DEGRADING trajectory (accel<-5)
  5.  STABLE (default / insufficient data)
  6.  Insufficient data → STABLE
  7.  Multiplier stacking with calendar
  8.  Score acceleration math correctness
  9.  Graceful DB failure → STABLE
  10. Minimum score threshold prevents noise boost
  11. TREND_FORMING from chart signals (≥2 signals)
  12. TREND_FORMING suppressed when score ≥ 65
  13. TREND_FORMING suppressed when degrading
  14. Individual forming signals detection
  15. TREND_FORMING in compute_signal_trajectory pipeline
  16. Forming signals list populated correctly

Run:
    pytest test/test_signal_trajectory.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import numpy as np
import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scan_engine.signal_trajectory import (
    _classify,
    _detect_forming_signals,
    compute_signal_trajectory,
    _stable_result,
    TRAJECTORY_TREND_FORMING,
    TRAJECTORY_EARLY_BREAKOUT,
    TRAJECTORY_LATE_CONFIRMATION,
    TRAJECTORY_IMPROVING,
    TRAJECTORY_DEGRADING,
    TRAJECTORY_STABLE,
    TRAJECTORY_MULTIPLIERS,
)


# =============================================================================
# Tests: Classification logic (backward-compatible)
# =============================================================================

class TestClassify:
    """Unit tests for the _classify function."""

    def test_early_breakout(self):
        """accel=20, score=50 → EARLY_BREAKOUT."""
        assert _classify(20.0, 50.0) == TRAJECTORY_EARLY_BREAKOUT

    def test_late_confirmation(self):
        """accel=2, score=75 → LATE_CONFIRMATION."""
        assert _classify(2.0, 75.0) == TRAJECTORY_LATE_CONFIRMATION

    def test_improving(self):
        """accel=8, score=45 → IMPROVING."""
        assert _classify(8.0, 45.0) == TRAJECTORY_IMPROVING

    def test_degrading(self):
        """accel=-8 → DEGRADING (no min score needed)."""
        assert _classify(-8.0, 15.0) == TRAJECTORY_DEGRADING

    def test_stable_default(self):
        """accel=1, score=50 → STABLE."""
        assert _classify(1.0, 50.0) == TRAJECTORY_STABLE

    def test_min_score_prevents_early_breakout(self):
        """accel=20, score=30 → NOT EARLY_BREAKOUT (score < 40 threshold)."""
        result = _classify(20.0, 30.0)
        assert result != TRAJECTORY_EARLY_BREAKOUT
        # score=30 is not > 30 for IMPROVING, so falls to STABLE
        assert result == TRAJECTORY_STABLE

    def test_min_score_prevents_improving_on_noise(self):
        """accel=8, score=20 → STABLE (score < 30 threshold for IMPROVING)."""
        assert _classify(8.0, 20.0) == TRAJECTORY_STABLE

    def test_degrading_no_min_score(self):
        """Degrading penalty always applies regardless of current score."""
        assert _classify(-10.0, 90.0) == TRAJECTORY_DEGRADING
        assert _classify(-10.0, 10.0) == TRAJECTORY_DEGRADING

    def test_late_confirmation_boundary(self):
        """accel=3 exactly → NOT late confirmation (need < 3)."""
        assert _classify(3.0, 80.0) == TRAJECTORY_STABLE

    def test_no_chart_signals_backward_compatible(self):
        """Calling without chart_signals works identically to before."""
        assert _classify(20.0, 50.0) == TRAJECTORY_EARLY_BREAKOUT
        assert _classify(2.0, 75.0) == TRAJECTORY_LATE_CONFIRMATION
        assert _classify(8.0, 45.0) == TRAJECTORY_IMPROVING
        assert _classify(-8.0, 15.0) == TRAJECTORY_DEGRADING
        assert _classify(1.0, 50.0) == TRAJECTORY_STABLE


# =============================================================================
# Tests: TREND_FORMING classification
# =============================================================================

class TestTrendForming:
    """Tests for chart-signal-aware early trend detection."""

    def _signals(self, **overrides):
        """Build chart_signals dict with defaults."""
        base = {
            'squeeze_on': False,
            'chart_regime': '',
            'obv_slope': 0.0,
            'atr_rank': 50.0,
            'volume_ratio': 0.8,
            'days_since_cross': None,
            'trend_slope': 0.0,
            'adx_latest': 20.0,
            'adx_slope': 0.0,
        }
        base.update(overrides)
        return base

    def test_two_signals_triggers_trend_forming(self):
        """Two chart signals + score<65 → TREND_FORMING."""
        signals = self._signals(
            squeeze_on=True, atr_rank=15,       # squeeze_compression
            chart_regime='Emerging_Trend',       # emerging_trend
        )
        assert _classify(2.0, 45.0, signals) == TRAJECTORY_TREND_FORMING

    def test_three_signals_triggers(self):
        """Three signals → TREND_FORMING."""
        signals = self._signals(
            chart_regime='Emerging_Trend',       # emerging_trend
            obv_slope=1.5, volume_ratio=1.2,     # obv_accumulation
            days_since_cross=3, adx_slope=2.0,   # fresh_crossover
        )
        assert _classify(2.0, 40.0, signals) == TRAJECTORY_TREND_FORMING

    def test_one_signal_not_enough(self):
        """Only one chart signal → does NOT trigger TREND_FORMING."""
        signals = self._signals(chart_regime='Emerging_Trend')
        result = _classify(2.0, 45.0, signals)
        assert result != TRAJECTORY_TREND_FORMING
        assert result == TRAJECTORY_STABLE

    def test_high_score_suppresses_trend_forming(self):
        """Score ≥ 65 → TREND_FORMING suppressed (already confirmed)."""
        signals = self._signals(
            squeeze_on=True, atr_rank=15,
            chart_regime='Emerging_Trend',
        )
        result = _classify(2.0, 70.0, signals)
        assert result != TRAJECTORY_TREND_FORMING

    def test_degrading_overrides_trend_forming(self):
        """Negative acceleration → DEGRADING wins over chart signals."""
        signals = self._signals(
            squeeze_on=True, atr_rank=15,
            chart_regime='Emerging_Trend',
        )
        assert _classify(-8.0, 45.0, signals) == TRAJECTORY_DEGRADING

    def test_early_breakout_overrides_trend_forming(self):
        """Strong score acceleration → EARLY_BREAKOUT wins (already detected)."""
        signals = self._signals(
            squeeze_on=True, atr_rank=15,
            chart_regime='Emerging_Trend',
        )
        assert _classify(20.0, 50.0, signals) == TRAJECTORY_EARLY_BREAKOUT

    def test_trend_forming_overrides_improving(self):
        """Chart signals fire for TREND_FORMING even when IMPROVING would match."""
        signals = self._signals(
            squeeze_on=True, atr_rank=15,
            chart_regime='Emerging_Trend',
        )
        # accel=8 would normally be IMPROVING, but chart signals → TREND_FORMING
        assert _classify(8.0, 45.0, signals) == TRAJECTORY_TREND_FORMING

    def test_late_confirmation_overrides_trend_forming(self):
        """High score + low accel → LATE_CONFIRMATION checked first."""
        signals = self._signals(
            squeeze_on=True, atr_rank=15,
            chart_regime='Emerging_Trend',
        )
        # accel=2, score=75 → LATE_CONFIRMATION (score ≥ 65 also blocks TREND_FORMING)
        assert _classify(2.0, 75.0, signals) == TRAJECTORY_LATE_CONFIRMATION

    def test_multiplier_value(self):
        """TREND_FORMING multiplier is 1.15."""
        assert TRAJECTORY_MULTIPLIERS[TRAJECTORY_TREND_FORMING] == 1.15


# =============================================================================
# Tests: Individual forming signal detection
# =============================================================================

class TestDetectFormingSignals:
    """Tests for _detect_forming_signals helper."""

    def _signals(self, **overrides):
        base = {
            'squeeze_on': False, 'chart_regime': '', 'obv_slope': 0.0,
            'atr_rank': 50.0, 'volume_ratio': 0.8, 'days_since_cross': None,
            'trend_slope': 0.0, 'adx_latest': 20.0, 'adx_slope': 0.0,
        }
        base.update(overrides)
        return base

    def test_squeeze_compression(self):
        """Keltner Squeeze ON + ATR_Rank < 30 → squeeze_compression."""
        signals = self._signals(squeeze_on=True, atr_rank=15)
        result = _detect_forming_signals(signals)
        assert 'squeeze_compression' in result

    def test_squeeze_high_atr_no_fire(self):
        """Squeeze ON but ATR_Rank ≥ 30 → not squeeze_compression."""
        signals = self._signals(squeeze_on=True, atr_rank=40)
        result = _detect_forming_signals(signals)
        assert 'squeeze_compression' not in result

    def test_emerging_trend(self):
        """Chart_Regime='Emerging_Trend' → emerging_trend."""
        signals = self._signals(chart_regime='Emerging_Trend')
        result = _detect_forming_signals(signals)
        assert 'emerging_trend' in result

    def test_obv_accumulation(self):
        """Positive OBV + volume > 1.0 → obv_accumulation."""
        signals = self._signals(obv_slope=0.8, volume_ratio=1.3)
        result = _detect_forming_signals(signals)
        assert 'obv_accumulation' in result

    def test_obv_low_volume_no_fire(self):
        """Positive OBV but volume ≤ 1.0 → not obv_accumulation."""
        signals = self._signals(obv_slope=0.8, volume_ratio=0.9)
        result = _detect_forming_signals(signals)
        assert 'obv_accumulation' not in result

    def test_fresh_crossover(self):
        """Days_Since_Cross < 5 + rising ADX → fresh_crossover."""
        signals = self._signals(days_since_cross=3, adx_slope=1.5)
        result = _detect_forming_signals(signals)
        assert 'fresh_crossover' in result

    def test_stale_crossover_no_fire(self):
        """Days_Since_Cross ≥ 5 → not fresh_crossover."""
        signals = self._signals(days_since_cross=7, adx_slope=1.5)
        result = _detect_forming_signals(signals)
        assert 'fresh_crossover' not in result

    def test_volume_breakout(self):
        """Volume > 1.5 + ADX < 25 → volume_breakout."""
        signals = self._signals(volume_ratio=2.0, adx_latest=18)
        result = _detect_forming_signals(signals)
        assert 'volume_breakout' in result

    def test_volume_breakout_high_adx_no_fire(self):
        """Volume spike but ADX ≥ 25 → not volume_breakout (trend already established)."""
        signals = self._signals(volume_ratio=2.0, adx_latest=30)
        result = _detect_forming_signals(signals)
        assert 'volume_breakout' not in result

    def test_stealth_trend(self):
        """Positive trend slope + low score → stealth_trend."""
        signals = self._signals(trend_slope=0.5)
        result = _detect_forming_signals(signals, current_score=35.0)
        assert 'stealth_trend' in result

    def test_stealth_trend_high_score_no_fire(self):
        """Positive trend slope but score ≥ 50 → not stealth_trend."""
        signals = self._signals(trend_slope=0.5)
        result = _detect_forming_signals(signals, current_score=55.0)
        assert 'stealth_trend' not in result

    def test_empty_signals_returns_empty(self):
        """No chart signals active → empty list."""
        signals = self._signals()
        assert _detect_forming_signals(signals) == []

    def test_all_signals_fire(self):
        """All 6 signals active simultaneously."""
        signals = self._signals(
            squeeze_on=True, atr_rank=10,
            chart_regime='Emerging_Trend',
            obv_slope=1.0, volume_ratio=2.0,
            days_since_cross=2, adx_slope=3.0,
            trend_slope=0.8,
            adx_latest=18,
        )
        result = _detect_forming_signals(signals, current_score=30.0)
        assert len(result) == 6


# =============================================================================
# Tests: compute_signal_trajectory with mocked DB
# =============================================================================

class TestComputeTrajectory:
    """Integration tests with mocked DB queries."""

    @patch('scan_engine.signal_trajectory._query_technical_history')
    @patch('scan_engine.signal_trajectory._query_score_history')
    def test_early_breakout_detected(self, mock_scores, mock_tech):
        """Sufficient data with high acceleration → EARLY_BREAKOUT."""
        # accel = (82 - 48) / 2 = 17 (>15), score=82 (>40)
        mock_scores.return_value = {
            'NVDA': [20.0, 35.0, 48.0, 65.0, 82.0],
        }
        mock_tech.return_value = {
            'NVDA': {'adx': [14.0, 18.0, 22.0, 26.0, 30.0], 'rsi': [45.0, 50.0, 55.0]},
        }
        result = compute_signal_trajectory(['NVDA'], con=MagicMock())
        assert result['NVDA']['trajectory'] == TRAJECTORY_EARLY_BREAKOUT
        assert result['NVDA']['multiplier'] == 1.10

    @patch('scan_engine.signal_trajectory._query_technical_history')
    @patch('scan_engine.signal_trajectory._query_score_history')
    def test_late_confirmation_detected(self, mock_scores, mock_tech):
        """High stable score with low acceleration → LATE_CONFIRMATION."""
        mock_scores.return_value = {
            'AAPL': [72.0, 73.0, 74.0, 74.5, 75.0],
        }
        mock_tech.return_value = {'AAPL': {'adx': [35.0, 35.0, 35.0], 'rsi': [60.0, 60.0]}}
        result = compute_signal_trajectory(['AAPL'], con=MagicMock())
        assert result['AAPL']['trajectory'] == TRAJECTORY_LATE_CONFIRMATION
        assert result['AAPL']['multiplier'] == 0.90

    @patch('scan_engine.signal_trajectory._query_technical_history')
    @patch('scan_engine.signal_trajectory._query_score_history')
    def test_insufficient_data_stable(self, mock_scores, mock_tech):
        """< 3 data points → STABLE."""
        mock_scores.return_value = {
            'TEST': [50.0, 60.0],  # Only 2 points
        }
        mock_tech.return_value = {}
        result = compute_signal_trajectory(['TEST'], con=MagicMock())
        assert result['TEST']['trajectory'] == TRAJECTORY_STABLE
        assert result['TEST']['multiplier'] == 1.0
        assert result['TEST']['data_points'] == 2

    @patch('scan_engine.signal_trajectory._query_technical_history')
    @patch('scan_engine.signal_trajectory._query_score_history')
    def test_missing_ticker_stable(self, mock_scores, mock_tech):
        """Ticker not in DB → STABLE."""
        mock_scores.return_value = {}
        mock_tech.return_value = {}
        result = compute_signal_trajectory(['MISSING'], con=MagicMock())
        assert result['MISSING']['trajectory'] == TRAJECTORY_STABLE

    @patch('scan_engine.signal_trajectory._query_technical_history')
    @patch('scan_engine.signal_trajectory._query_score_history')
    def test_score_acceleration_math(self, mock_scores, mock_tech):
        """Verify acceleration = (score[-1] - score[-3]) / 2."""
        mock_scores.return_value = {
            'TSLA': [40.0, 45.0, 50.0, 55.0, 60.0],
        }
        mock_tech.return_value = {'TSLA': {'adx': [], 'rsi': []}}
        result = compute_signal_trajectory(['TSLA'], con=MagicMock())
        # acceleration = (60.0 - 50.0) / 2 = 5.0
        assert result['TSLA']['score_acceleration'] == 5.0

    @patch('scan_engine.signal_trajectory._query_technical_history')
    @patch('scan_engine.signal_trajectory._query_score_history')
    def test_degrading_trajectory(self, mock_scores, mock_tech):
        """Negative acceleration → DEGRADING."""
        mock_scores.return_value = {
            'META': [70.0, 60.0, 50.0, 40.0, 30.0],
        }
        mock_tech.return_value = {'META': {'adx': [], 'rsi': []}}
        result = compute_signal_trajectory(['META'], con=MagicMock())
        assert result['META']['trajectory'] == TRAJECTORY_DEGRADING
        assert result['META']['multiplier'] == 0.95

    @patch('scan_engine.signal_trajectory._query_technical_history')
    @patch('scan_engine.signal_trajectory._query_score_history')
    def test_trend_forming_via_chart_signals(self, mock_scores, mock_tech):
        """Chart signals in tech_history → TREND_FORMING when ≥2 fire."""
        # Score acceleration: (50 - 42) / 2 = 4.0 (would be IMPROVING but signals override)
        mock_scores.return_value = {
            'SOFI': [35.0, 38.0, 42.0, 46.0, 50.0],
        }
        mock_tech.return_value = {
            'SOFI': {
                'adx': [18.0, 19.0, 21.0, 23.0, 25.0],
                'rsi': [48.0, 50.0, 52.0],
                'chart_signals': {
                    'squeeze_on': True,
                    'chart_regime': 'Emerging_Trend',
                    'obv_slope': 0.0,
                    'atr_rank': 15.0,
                    'volume_ratio': 0.8,
                    'days_since_cross': None,
                    'trend_slope': 0.0,
                    'adx_latest': 25.0,
                },
            },
        }
        result = compute_signal_trajectory(['SOFI'], con=MagicMock())
        assert result['SOFI']['trajectory'] == TRAJECTORY_TREND_FORMING
        assert result['SOFI']['multiplier'] == 1.15
        assert len(result['SOFI']['forming_signals']) >= 2
        assert 'squeeze_compression' in result['SOFI']['forming_signals']
        assert 'emerging_trend' in result['SOFI']['forming_signals']

    @patch('scan_engine.signal_trajectory._query_technical_history')
    @patch('scan_engine.signal_trajectory._query_score_history')
    def test_no_chart_signals_falls_back_to_score(self, mock_scores, mock_tech):
        """No chart_signals in tech_history → score-only classification."""
        mock_scores.return_value = {
            'AMD': [40.0, 45.0, 48.0, 55.0, 62.0],
        }
        mock_tech.return_value = {
            'AMD': {'adx': [20.0, 22.0, 24.0], 'rsi': [50.0, 52.0]},
            # No 'chart_signals' key
        }
        result = compute_signal_trajectory(['AMD'], con=MagicMock())
        # acceleration = (62-48)/2 = 7.0 (>5), score=62 (>30) → IMPROVING
        assert result['AMD']['trajectory'] == TRAJECTORY_IMPROVING
        assert result['AMD']['forming_signals'] == []

    @patch('scan_engine.signal_trajectory._query_technical_history')
    @patch('scan_engine.signal_trajectory._query_score_history')
    def test_forming_signals_empty_when_not_trend_forming(self, mock_scores, mock_tech):
        """Non-TREND_FORMING trajectory → forming_signals is empty list."""
        mock_scores.return_value = {
            'GOOG': [72.0, 73.0, 74.0, 74.5, 75.0],
        }
        mock_tech.return_value = {'GOOG': {'adx': [], 'rsi': []}}
        result = compute_signal_trajectory(['GOOG'], con=MagicMock())
        assert result['GOOG']['trajectory'] == TRAJECTORY_LATE_CONFIRMATION
        assert result['GOOG']['forming_signals'] == []


class TestGracefulFailure:
    """Verify non-fatal behavior on DB errors."""

    def test_db_connection_failure(self):
        """DB connection failure → all tickers STABLE."""
        # Pass no con and mock the import to fail
        with patch('scan_engine.signal_trajectory.os.path.join', side_effect=Exception('no db')):
            result = compute_signal_trajectory(['AAPL', 'NVDA'], con=None)
        # Should return STABLE for all tickers
        for ticker in ('AAPL', 'NVDA'):
            assert result[ticker]['trajectory'] == TRAJECTORY_STABLE


class TestMultiplierValues:
    """Verify multiplier constants."""

    def test_multiplier_map(self):
        assert TRAJECTORY_MULTIPLIERS[TRAJECTORY_TREND_FORMING] == 1.15
        assert TRAJECTORY_MULTIPLIERS[TRAJECTORY_EARLY_BREAKOUT] == 1.10
        assert TRAJECTORY_MULTIPLIERS[TRAJECTORY_LATE_CONFIRMATION] == 0.90
        assert TRAJECTORY_MULTIPLIERS[TRAJECTORY_IMPROVING] == 1.05
        assert TRAJECTORY_MULTIPLIERS[TRAJECTORY_DEGRADING] == 0.95
        assert TRAJECTORY_MULTIPLIERS[TRAJECTORY_STABLE] == 1.00


class TestStableResult:
    """Verify _stable_result includes forming_signals."""

    def test_stable_result_has_forming_signals(self):
        r = _stable_result()
        assert r['forming_signals'] == []
        assert r['trajectory'] == TRAJECTORY_STABLE


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
