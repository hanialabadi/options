import pandas as pd
from ..base import ChartStateResult
from ..state_definitions import TrendIntegrityState

def compute_trend_integrity(row: pd.Series) -> ChartStateResult:
    """
    B. Trend Integrity (Quality, not direction)
    Measures EMA slopes, alignment, ADX, and distance to EMA.
    """
    ema20_slope = row.get("ema20_slope")
    ema50_slope = row.get("ema50_slope")
    ema_alignment = row.get("ema_alignment_score")
    adx = row.get("adx_14")
    dist_to_ema = row.get("price_dist_to_ema_atr")

    metrics = [ema20_slope, ema50_slope, ema_alignment, adx, dist_to_ema]
    if any(v is None or pd.isna(v) for v in metrics):
        return ChartStateResult(
            state=TrendIntegrityState.UNKNOWN,
            raw_metrics={},
            resolution_reason="MISSING_PRIMITIVES",
            data_complete=False
        )

    raw_metrics = {
        "ema20_slope": float(ema20_slope),
        "ema50_slope": float(ema50_slope),
        "ema_alignment_score": float(ema_alignment),
        "adx_14": float(adx),
        "price_dist_to_ema_atr": float(dist_to_ema),
        "sma_distance_pct": float(row.get("sma_distance_pct", 0.0))
    }

    if adx > 25 and ema_alignment > 0.8:
        state = TrendIntegrityState.STRONG_TREND
    elif adx > 15 and ema_alignment > 0.4:
        state = TrendIntegrityState.WEAK_TREND
    elif dist_to_ema > 3.0 or (adx > 40 and ema20_slope * ema50_slope < 0):
        state = TrendIntegrityState.TREND_EXHAUSTED
    else:
        state = TrendIntegrityState.NO_TREND

    return ChartStateResult(state=state, raw_metrics=raw_metrics)
