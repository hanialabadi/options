"""
Pure bias-detection functions extracted from step12_acceptance.py.

All functions are pure: (inputs) -> output, no side effects.
"""

import pandas as pd


def detect_directional_bias(
    momentum: str, regime_52w: str, gap: str, timing: str,
    ema_signal: str = 'UNKNOWN', trend_state: str = 'UNKNOWN',
    rsi: float = None, macd: float = None,
) -> str:
    """
    Detect bullish/bearish/neutral bias from chart + Phase 1 signals.

    Primary signals (chart): EMA signal, Trend_State, RSI, MACD
    Secondary signals (price structure): momentum_tag, 52w_regime

    Returns:
        'BULLISH_STRONG' | 'BULLISH_MODERATE' | 'BEARISH_STRONG' |
        'BEARISH_MODERATE' | 'NEUTRAL'
    """
    ema_upper   = str(ema_signal).upper()
    trend_upper = str(trend_state).upper()

    bearish_chart = ema_upper in ('BEARISH', 'BEARISH_CROSS') or trend_upper == 'BEARISH'
    bullish_chart = ema_upper in ('BULLISH', 'BULLISH_CROSS') or trend_upper == 'BULLISH'

    rsi_bearish = rsi is not None and not pd.isna(rsi) and float(rsi) < 45
    rsi_bullish = rsi is not None and not pd.isna(rsi) and float(rsi) > 55
    macd_bearish = macd is not None and not pd.isna(macd) and float(macd) < 0
    macd_bullish = macd is not None and not pd.isna(macd) and float(macd) > 0

    bearish_score = sum([bearish_chart, rsi_bearish, macd_bearish,
                         momentum == 'STRONG_DOWN_DAY'])
    bullish_score = sum([bullish_chart, rsi_bullish, macd_bullish,
                         momentum == 'STRONG_UP_DAY'])

    if bearish_chart and bearish_score >= 3:
        return 'BEARISH_STRONG'
    if bullish_chart and bullish_score >= 3:
        return 'BULLISH_STRONG'
    if bearish_chart and bearish_score >= 1:
        return 'BEARISH_MODERATE'
    if bullish_chart and bullish_score >= 1:
        return 'BULLISH_MODERATE'

    if momentum == 'STRONG_DOWN_DAY' and regime_52w in ['NEAR_52W_HIGH', 'MID_RANGE']:
        return 'BEARISH_MODERATE'
    if momentum == 'STRONG_UP_DAY' and regime_52w in ['NEAR_52W_LOW', 'MID_RANGE']:
        return 'BULLISH_MODERATE'

    return 'NEUTRAL'


def detect_structure_bias(
    compression: str, regime_52w: str, momentum: str,
    adx: float = 0.0, chart_regime: str = '',
) -> str:
    """
    Detect range-bound vs trending vs breakout structure.

    Uses compression/momentum as primary signals, with ADX and Chart_Regime
    as trend confirmation (Murphy: ADX > 25 = trending market).

    Returns:
        'RANGE_BOUND' | 'TRENDING' | 'BREAKOUT_SETUP' | 'BREAKOUT_TRIGGERED' | 'UNCLEAR'
    """
    _quiet_momentum = momentum in ['FLAT_DAY', 'NORMAL']
    _strong_momentum = momentum in ['STRONG_UP_DAY', 'STRONG_DOWN_DAY']

    try:
        _adx_f = float(adx or 0)
    except (ValueError, TypeError):
        _adx_f = 0.0
    _chart_trending = str(chart_regime).lower() in ('trending', 'strong_trend', 'emerging_trend')
    _adx_trending = _adx_f > 25

    if compression == 'COMPRESSION' and _strong_momentum:
        return 'BREAKOUT_TRIGGERED'
    if compression == 'COMPRESSION' and _quiet_momentum:
        return 'BREAKOUT_SETUP'
    if _strong_momentum and compression in ['NORMAL', 'EXPANSION']:
        return 'TRENDING'
    if _quiet_momentum and _adx_trending and _chart_trending:
        return 'TRENDING'
    if _quiet_momentum and compression in ['COMPRESSION', 'NORMAL', 'EXPANSION']:
        return 'RANGE_BOUND'

    return 'UNCLEAR'


def evaluate_timing_quality(timing: str, intraday_pos: str, gap: str, momentum: str) -> str:
    """
    Evaluate entry timing quality.

    Returns:
        'EXCELLENT' | 'GOOD' | 'FAIR' | 'POOR' | 'MODERATE'
    """
    if timing in ['EARLY_LONG', 'EARLY_SHORT'] and gap == 'NO_GAP':
        return 'EXCELLENT'
    elif timing == 'MODERATE' and intraday_pos == 'MID_RANGE':
        return 'GOOD'
    elif timing in ['LATE_LONG', 'LATE_SHORT'] and momentum == 'NORMAL':
        return 'FAIR'
    elif timing in ['LATE_LONG', 'LATE_SHORT'] and gap in ['GAP_UP', 'GAP_DOWN']:
        return 'POOR'
    else:
        return 'MODERATE'
