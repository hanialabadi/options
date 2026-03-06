import pandas as pd
from ..base import ChartStateResult
from ..state_definitions import MomentumVelocityState


def compute_momentum_velocity(row: pd.Series) -> ChartStateResult:
    """
    E. Momentum Velocity — deterministic 6-state classifier.

    State hierarchy (evaluated top-down, first match wins):

      LATE_CYCLE   — price still advancing but momentum diverging
      ACCELERATING — rate of change increasing across all confirming signals
      TRENDING     — sustained directional move, slope intact, not yet parabolic
      REVERSING    — short-term ROC sign opposite to medium-term (crossover)
      DECELERATING — momentum falling, slope negative
      STALLING     — near-zero slope, no clear direction
      UNKNOWN      — insufficient primitives

    Required primitives (all must be present):
      roc_5, roc_10, roc_20, momentum_slope, price_acceleration

    Optional primitives (used for richer classification, degrade gracefully):
      bb_width_slope    — Bollinger Band width change over 5 bars
      atr_slope         — 5-bar ATR change (normalised), positive = expanding vol
      rsi_14            — current RSI (from scan-engine enrichment)
      rsi_slope         — 5-bar RSI change (computed from price series)
      sma_distance_pct  — (price - SMA20) / SMA20 — overextension measure
      up_volume_down_volume_ratio — volume balance proxy
    """
    # ── Required primitives ───────────────────────────────────────────────────
    roc5        = row.get("roc_5")
    roc10       = row.get("roc_10")
    roc20       = row.get("roc_20")
    mom_slope   = row.get("momentum_slope")
    acceleration = row.get("price_acceleration")   # = roc5 - roc10

    required = [roc5, roc10, roc20, mom_slope, acceleration]
    if any(v is None or (isinstance(v, float) and pd.isna(v)) for v in required):
        return ChartStateResult(
            state=MomentumVelocityState.UNKNOWN,
            raw_metrics={},
            resolution_reason="MISSING_PRIMITIVES",
            data_complete=False
        )

    roc5        = float(roc5)
    roc10       = float(roc10)
    roc20       = float(roc20)
    mom_slope   = float(mom_slope)
    acceleration = float(acceleration)

    # ── Optional primitives (degrade to neutral if absent) ────────────────────
    def _opt(key, default=0.0):
        v = row.get(key)
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return default
        return float(v)

    bb_width_slope  = _opt("bb_width_slope")       # >0 = bands expanding
    atr_slope       = _opt("atr_slope")             # >0 = ATR growing (vol confirming move)
    rsi_14          = _opt("rsi_14", default=50.0)  # current RSI
    rsi_slope       = _opt("rsi_slope")             # >0 = RSI rising with price
    sma_dist        = _opt("sma_distance_pct")      # >0.15 = >15% above SMA20 = overextended
    vol_ratio       = _opt("up_volume_down_volume_ratio", default=1.0)

    raw_metrics = {
        "roc_5":               roc5,
        "roc_10":              roc10,
        "roc_20":              roc20,
        "momentum_slope":      mom_slope,
        "price_acceleration":  acceleration,
        "bb_width_slope":      bb_width_slope,
        "atr_slope":           atr_slope,
        "rsi_14":              rsi_14,
        "rsi_slope":           rsi_slope,
        "sma_distance_pct":    sma_dist,
        "vol_ratio":           vol_ratio,
    }

    # ── Classification tree ───────────────────────────────────────────────────
    # Evaluated top-down. Each gate requires a minimum quorum of confirming signals
    # so that a single noisy metric cannot trigger a false classification.

    # 1. LATE_CYCLE — price still advancing but momentum internally diverging.
    #    Hallmarks: RSI > 70 AND flattening/falling, ROC slowing, overextended.
    #    Requires at least 2 of the 4 divergence signals to fire.
    _late_signals = 0
    if rsi_14 > 70 and rsi_slope < 0:
        _late_signals += 2   # RSI overbought AND falling = strongest single signal
    elif rsi_14 > 70:
        _late_signals += 1
    if acceleration < 0 and roc20 > 0:
        _late_signals += 1   # ROC decelerating while longer-term trend still positive
    if sma_dist > 0.12:
        _late_signals += 1   # price >12% above SMA20 = overextended (McMillan threshold)
    if bb_width_slope < 0 and roc5 > 0:
        _late_signals += 1   # BB contracting while price rising = squeeze into resistance

    if _late_signals >= 2 and mom_slope > 0:
        # Trend still nominally up (mom_slope > 0) but internals diverging
        state = MomentumVelocityState.LATE_CYCLE
        resolution = (
            f"LATE_CYCLE: {_late_signals} divergence signals — "
            f"RSI={rsi_14:.0f} (slope {rsi_slope:+.1f}), "
            f"accel={acceleration:+.1f}, SMA_dist={sma_dist:+.1%}, "
            f"BB_slope={bb_width_slope:+.4f}"
        )
        return ChartStateResult(state=state, raw_metrics=raw_metrics,
                                resolution_reason=resolution, data_complete=True)

    # 2. REVERSING — short and medium ROC on opposite sides of zero.
    #    Classic crossover: 5-day just turned negative while 20-day still positive
    #    (or vice versa for a reversal upward out of a decline).
    if (roc5 > 0 and roc10 < 0) or (roc5 < 0 and roc10 > 0):
        state = MomentumVelocityState.REVERSING
        resolution = f"REVERSING: ROC5={roc5:+.2f}% vs ROC10={roc10:+.2f}% — crossover detected"
        return ChartStateResult(state=state, raw_metrics=raw_metrics,
                                resolution_reason=resolution, data_complete=True)

    # 3. ACCELERATING — rate of change increasing, confirmed by expansion signals.
    #    Requires: acceleration > 0 (ROC5 > ROC10) AND ROC5 > ROC20 (short > long)
    #    Plus at least one expansion confirmation (BB, ATR, or volume).
    _accel_confirms = 0
    if bb_width_slope > 0:
        _accel_confirms += 1    # bands expanding = volatility supporting the move
    if atr_slope > 0:
        _accel_confirms += 1    # ATR growing = range expansion confirming
    if vol_ratio > 1.3:
        _accel_confirms += 1    # up-volume dominating down-volume

    if acceleration > 0 and roc5 > roc20 and mom_slope > 0 and _accel_confirms >= 1:
        state = MomentumVelocityState.ACCELERATING
        resolution = (
            f"ACCELERATING: accel={acceleration:+.1f}, ROC5={roc5:+.2f}%>ROC20={roc20:+.2f}%, "
            f"BB_slope={bb_width_slope:+.4f}, ATR_slope={atr_slope:+.3f}, vol_ratio={vol_ratio:.2f} "
            f"({_accel_confirms} expansion confirms)"
        )
        return ChartStateResult(state=state, raw_metrics=raw_metrics,
                                resolution_reason=resolution, data_complete=True)

    # 4. TRENDING — sustained directional move, slope intact, not parabolic.
    #    Acceleration may be slightly negative (momentum pulling back) but the
    #    underlying trend is intact: mom_slope positive, ADX implies trending.
    #    This is the "MU" case: up 118%, pulling back slightly, thesis still active.
    _trend_up   = mom_slope > 0 and roc20 > 0
    _trend_down = mom_slope < 0 and roc20 < 0
    _not_stalling = abs(mom_slope) >= 0.05
    if (_trend_up or _trend_down) and _not_stalling:
        direction = "up" if _trend_up else "down"
        state = MomentumVelocityState.TRENDING
        resolution = (
            f"TRENDING ({direction}): mom_slope={mom_slope:+.3f}, "
            f"ROC20={roc20:+.2f}%, accel={acceleration:+.1f} (not parabolic)"
        )
        return ChartStateResult(state=state, raw_metrics=raw_metrics,
                                resolution_reason=resolution, data_complete=True)

    # 5. DECELERATING — momentum falling, slope negative, not yet crossing zero.
    if acceleration < 0 and mom_slope < 0:
        state = MomentumVelocityState.DECELERATING
        resolution = f"DECELERATING: accel={acceleration:+.1f}, mom_slope={mom_slope:+.3f}"
        return ChartStateResult(state=state, raw_metrics=raw_metrics,
                                resolution_reason=resolution, data_complete=True)

    # 6. STALLING — near-zero slope, no directional conviction.
    if abs(mom_slope) < 0.05:
        state = MomentumVelocityState.STALLING
        resolution = f"STALLING: mom_slope={mom_slope:+.3f} ≈ 0, ROC5={roc5:+.2f}%"
        return ChartStateResult(state=state, raw_metrics=raw_metrics,
                                resolution_reason=resolution, data_complete=True)

    # Fallback — should not reach here given the tree covers all cases, but kept
    # as a safety net if future primitives create an edge case.
    return ChartStateResult(
        state=MomentumVelocityState.UNKNOWN,
        raw_metrics=raw_metrics,
        resolution_reason=f"UNCATEGORISED: mom_slope={mom_slope:+.3f}, accel={acceleration:+.1f}",
        data_complete=True
    )
