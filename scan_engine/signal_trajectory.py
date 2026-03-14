"""
Signal Trajectory — Scan Memory
================================
Computes how a ticker's scan quality is evolving over time by querying
historical entries from ``scan_candidates`` and ``technical_indicators``
tables in pipeline.duckdb.

Classifications:
  TREND_FORMING      chart signals ≥ 2 AND score < 65               ×1.15
  EARLY_BREAKOUT     accel > 15/day AND score > 40                   ×1.10
  LATE_CONFIRMATION  accel < 3/day  AND score > 70                   ×0.90
  IMPROVING          accel > 5/day  AND score > 30                   ×1.05
  DEGRADING          accel < -5/day                                  ×0.95
  STABLE             default (or < 3 data points)                    ×1.00

TREND_FORMING detects trends as they build — before DQS score catches up —
using chart signals already stored in ``technical_indicators``:
  Keltner Squeeze + low ATR         (Raschke)
  Emerging_Trend regime ADX 20-29   (Murphy Ch.14)
  OBV accumulation + above-avg vol  (Murphy Ch.7)
  Fresh EMA crossover + rising ADX  (Murphy Ch.9)
  Volume spike on low ADX           (Murphy Ch.3)
  Positive trend slope + low score  (stealth trend)

Graceful degradation: < 3 data points → STABLE. DB failure → STABLE.
Chart signal columns missing → falls back to score-only classification.
"""

from __future__ import annotations

import logging
import os
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from core.shared.data_layer.duckdb_utils import get_domain_connection, DbDomain

logger = logging.getLogger(__name__)

# ── Trajectory classifications and multipliers ──────────────────────────────
TRAJECTORY_TREND_FORMING = 'TREND_FORMING'
TRAJECTORY_EARLY_BREAKOUT = 'EARLY_BREAKOUT'
TRAJECTORY_LATE_CONFIRMATION = 'LATE_CONFIRMATION'
TRAJECTORY_IMPROVING = 'IMPROVING'
TRAJECTORY_DEGRADING = 'DEGRADING'
TRAJECTORY_STABLE = 'STABLE'

TRAJECTORY_MULTIPLIERS = {
    TRAJECTORY_TREND_FORMING: 1.15,
    TRAJECTORY_EARLY_BREAKOUT: 1.10,
    TRAJECTORY_LATE_CONFIRMATION: 0.90,
    TRAJECTORY_IMPROVING: 1.05,
    TRAJECTORY_DEGRADING: 0.95,
    TRAJECTORY_STABLE: 1.00,
}

# Minimum data points for a non-STABLE classification
MIN_DATA_POINTS = 3

# Chart signal columns queried from technical_indicators
_CHART_SIGNAL_COLS = [
    'Keltner_Squeeze_On', 'Chart_Regime', 'OBV_Slope',
    'ATR_Rank', 'Volume_Ratio', 'Days_Since_Cross', 'Trend_Slope',
]


# ── Early trend detection ───────────────────────────────────────────────────

def _detect_forming_signals(chart_signals: dict, current_score: float = 50.0) -> List[str]:
    """
    Detect early trend formation signals from chart indicators.

    Each signal represents a pre-confirmation indicator that fires before
    the DQS score catches up.  Requires ≥ 2 signals for TREND_FORMING.
    """
    signals: List[str] = []

    # 1. Keltner Squeeze building: compression before breakout (Raschke)
    #    Squeeze ON + low ATR percentile = volatility contracting, energy coiling
    if chart_signals.get('squeeze_on') and (chart_signals.get('atr_rank') or 100) < 30:
        signals.append('squeeze_compression')

    # 2. Emerging Trend regime: ADX 20-29, new directional movement (Murphy Ch.14)
    #    ADX crossed above 20 but hasn't reached full trend strength
    if str(chart_signals.get('chart_regime') or '').strip() == 'Emerging_Trend':
        signals.append('emerging_trend')

    # 3. OBV accumulation with above-average volume (Murphy Ch.7)
    #    Positive OBV slope + volume > average = stealth accumulation/distribution
    _obv = chart_signals.get('obv_slope') or 0
    _vr = chart_signals.get('volume_ratio') or 0
    if _obv > 0 and _vr > 1.0:
        signals.append('obv_accumulation')

    # 4. Fresh EMA crossover with rising ADX (Murphy Ch.9)
    #    Recent crossover (<5 days) + ADX rising = new trend just starting
    _dsc = chart_signals.get('days_since_cross')
    if _dsc is not None and _dsc < 5 and (chart_signals.get('adx_slope') or 0) > 0:
        signals.append('fresh_crossover')

    # 5. Volume spike on low ADX: breakout volume before confirmation (Murphy Ch.3)
    #    50%+ above-average volume while ADX < 25 = big move brewing
    if _vr > 1.5 and (chart_signals.get('adx_latest') or 100) < 25:
        signals.append('volume_breakout')

    # 6. Positive trend slope with low score recognition
    #    Price is trending but DQS hasn't caught up yet — stealth trend
    if (chart_signals.get('trend_slope') or 0) > 0 and current_score < 50:
        signals.append('stealth_trend')

    return signals


# ── Classification ──────────────────────────────────────────────────────────

def _classify(
    acceleration: float,
    current_score: float,
    chart_signals: Optional[dict] = None,
) -> str:
    """
    Classify trajectory based on score acceleration, current score, and
    optional chart signals for early trend detection.
    """
    # Penalties always apply (no minimum score required)
    if acceleration < -5.0:
        return TRAJECTORY_DEGRADING
    if acceleration < 3.0 and current_score > 70.0:
        return TRAJECTORY_LATE_CONFIRMATION

    # Strong score momentum — already detected by score alone
    if acceleration > 15.0 and current_score > 40.0:
        return TRAJECTORY_EARLY_BREAKOUT

    # Chart-signal-aware early trend detection (fires before score catches up)
    # Only when score < 65 (not already at confirmed levels) and not degrading
    if chart_signals and current_score < 65.0:
        forming = _detect_forming_signals(chart_signals, current_score)
        if len(forming) >= 2:
            return TRAJECTORY_TREND_FORMING

    # Moderate score momentum
    if acceleration > 5.0 and current_score > 30.0:
        return TRAJECTORY_IMPROVING

    return TRAJECTORY_STABLE


# ── Main entry point ────────────────────────────────────────────────────────

def compute_signal_trajectory(
    tickers: list[str],
    con=None,
    lookback_days: int = 5,
) -> Dict[str, dict]:
    """
    Compute signal trajectory for a list of tickers.

    Queries the last ``lookback_days`` entries from ``scan_candidates``
    (DQS_Score per ticker+strategy) and ``technical_indicators`` (ADX, RSI,
    plus chart signals for early trend detection).

    Returns:
        Dict keyed by ticker → trajectory dict with:
          trajectory, multiplier, score_acceleration, adx_slope, rsi_trend,
          data_points, scores, forming_signals
    """
    if not tickers:
        return {}

    close_con = False
    if con is None:
        try:
            con = get_domain_connection(DbDomain.SCAN, read_only=True)
            close_con = True
        except Exception as e:
            logger.debug(f"[Trajectory] DB connection failed: {e}")
            return {t: _stable_result() for t in tickers}

    result = {}
    try:
        # ── Query scan_candidates for DQS_Score history ──────────────────
        score_history = _query_score_history(con, tickers, lookback_days)
        # ── Query technical_indicators for ADX/RSI + chart signals ───────
        tech_history = _query_technical_history(con, tickers, lookback_days)

        for ticker in tickers:
            scores = score_history.get(ticker, [])
            tech = tech_history.get(ticker, {})
            adx_vals = tech.get('adx', [])
            rsi_vals = tech.get('rsi', [])
            chart_signals = tech.get('chart_signals')

            if len(scores) < MIN_DATA_POINTS:
                result[ticker] = _stable_result(data_points=len(scores))
                continue

            # Score acceleration: (latest - 2nd oldest) / gap
            acceleration = (scores[-1] - scores[-3]) / 2.0 if len(scores) >= 3 else 0.0
            current_score = scores[-1]

            # ADX slope
            adx_slope = 0.0
            if len(adx_vals) >= 3:
                adx_slope = (adx_vals[-1] - adx_vals[-3]) / 2.0

            # RSI trend
            rsi_trend = 0.0
            if len(rsi_vals) >= 2:
                rsi_trend = rsi_vals[-1] - rsi_vals[-2]

            # Inject adx_slope into chart_signals for _classify → _detect_forming_signals
            if chart_signals is not None:
                chart_signals['adx_slope'] = adx_slope

            trajectory = _classify(acceleration, current_score, chart_signals)
            multiplier = TRAJECTORY_MULTIPLIERS[trajectory]

            # Compute forming signals for logging/debugging
            forming_signals = []
            if trajectory == TRAJECTORY_TREND_FORMING and chart_signals:
                forming_signals = _detect_forming_signals(chart_signals, current_score)

            result[ticker] = {
                'trajectory': trajectory,
                'multiplier': multiplier,
                'score_acceleration': round(acceleration, 2),
                'adx_slope': round(adx_slope, 2),
                'rsi_trend': round(rsi_trend, 2),
                'data_points': len(scores),
                'scores': scores,
                'forming_signals': forming_signals,
            }

    except Exception as e:
        logger.debug(f"[Trajectory] Computation failed: {e}")
        result = {t: _stable_result() for t in tickers}
    finally:
        if close_con and con is not None:
            try:
                con.close()
            except Exception:
                pass

    # Fill missing tickers with STABLE
    for t in tickers:
        if t not in result:
            result[t] = _stable_result()

    return result


def _stable_result(data_points: int = 0) -> dict:
    """Default STABLE result when data is insufficient."""
    return {
        'trajectory': TRAJECTORY_STABLE,
        'multiplier': 1.0,
        'score_acceleration': 0.0,
        'adx_slope': 0.0,
        'rsi_trend': 0.0,
        'data_points': data_points,
        'scores': [],
        'forming_signals': [],
    }


# ── DB queries ──────────────────────────────────────────────────────────────

def _query_score_history(
    con,
    tickers: list[str],
    lookback: int,
) -> Dict[str, list[float]]:
    """Query scan_candidates for recent DQS_Score history per ticker."""
    try:
        placeholders = ', '.join(['?' for _ in tickers])
        query = f"""
            SELECT Ticker, DQS_Score, Scan_TS
            FROM scan_candidates
            WHERE Ticker IN ({placeholders})
              AND Scan_TS >= CURRENT_TIMESTAMP - INTERVAL '{lookback}' DAY
              AND DQS_Score IS NOT NULL
            ORDER BY Ticker, Scan_TS ASC
        """
        df = con.execute(query, tickers).df()
        if df.empty:
            return {}

        result = {}
        for ticker, group in df.groupby('Ticker'):
            # Take the best score per scan run (one per day typically)
            scores = group['DQS_Score'].tolist()
            result[ticker] = scores[-lookback:]  # Keep last N
        return result
    except Exception as e:
        logger.debug(f"[Trajectory] scan_candidates query failed: {e}")
        return {}


def _query_technical_history(
    con,
    tickers: list[str],
    lookback: int,
) -> Dict[str, dict]:
    """
    Query technical_indicators for ADX/RSI history + chart signals per ticker.

    Tries expanded query with chart signal columns first. Falls back to
    basic ADX/RSI-only query if columns don't exist (older databases).
    """
    try:
        placeholders = ', '.join(['?' for _ in tickers])
        _extra = ', '.join(_CHART_SIGNAL_COLS)
        _has_chart_cols = True

        try:
            query = f"""
                SELECT Ticker, ADX_14, RSI_14, {_extra}, Snapshot_TS
                FROM technical_indicators
                WHERE Ticker IN ({placeholders})
                  AND Snapshot_TS >= CURRENT_TIMESTAMP - INTERVAL '{lookback}' DAY
                ORDER BY Ticker, Snapshot_TS ASC
            """
            df = con.execute(query, tickers).df()
        except Exception:
            # Fallback: chart signal columns may not exist in older databases
            _has_chart_cols = False
            query = f"""
                SELECT Ticker, ADX_14, RSI_14, Snapshot_TS
                FROM technical_indicators
                WHERE Ticker IN ({placeholders})
                  AND Snapshot_TS >= CURRENT_TIMESTAMP - INTERVAL '{lookback}' DAY
                ORDER BY Ticker, Snapshot_TS ASC
            """
            df = con.execute(query, tickers).df()

        if df.empty:
            return {}

        result = {}
        for ticker, group in df.groupby('Ticker'):
            adx_vals = group['ADX_14'].dropna().tolist()
            rsi_vals = group['RSI_14'].dropna().tolist()

            entry = {
                'adx': adx_vals[-lookback:],
                'rsi': rsi_vals[-lookback:],
            }

            # Extract chart signals from latest row if columns available
            if _has_chart_cols and not group.empty:
                entry['chart_signals'] = _extract_chart_signals(
                    group.iloc[-1], adx_vals)

            result[ticker] = entry
        return result
    except Exception as e:
        logger.debug(f"[Trajectory] technical_indicators query failed: {e}")
        return {}


def _extract_chart_signals(latest_row, adx_vals: list) -> dict:
    """Extract latest chart signals for trend forming detection."""
    def _sf(v, default=0.0):
        """Safe float conversion."""
        try:
            f = float(v) if v is not None else default
            return default if (isinstance(f, float) and np.isnan(f)) else f
        except (TypeError, ValueError):
            return default

    _dsc = latest_row.get('Days_Since_Cross')
    try:
        _dsc = float(_dsc) if _dsc is not None and not pd.isna(_dsc) else None
    except (TypeError, ValueError):
        _dsc = None

    return {
        'squeeze_on': bool(latest_row.get('Keltner_Squeeze_On') or False),
        'chart_regime': str(latest_row.get('Chart_Regime') or ''),
        'obv_slope': _sf(latest_row.get('OBV_Slope')),
        'atr_rank': _sf(latest_row.get('ATR_Rank'), 100.0),
        'volume_ratio': _sf(latest_row.get('Volume_Ratio')),
        'days_since_cross': _dsc,
        'trend_slope': _sf(latest_row.get('Trend_Slope')),
        'adx_latest': adx_vals[-1] if adx_vals else None,
        # adx_slope is injected by compute_signal_trajectory after this returns
    }
