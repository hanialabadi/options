import pandas as pd
from ..base import ChartStateResult
from ..state_definitions import CompressionMaturityState

def compute_compression_maturity(row: pd.Series) -> ChartStateResult:
    """
    D. Compression Maturity (Volatility Cycle Timing)
    Measures the lifecycle of volatility compression and release.
    """
    low_bb_bars = row.get("consecutive_low_bb_width_bars")
    bb_slope = row.get("bb_width_slope")
    range_contraction = row.get("range_contraction_ratio")
    inside_bars = row.get("inside_bar_count")

    metrics = [low_bb_bars, bb_slope, range_contraction, inside_bars]
    if any(v is None or pd.isna(v) for v in metrics):
        return ChartStateResult(
            state=CompressionMaturityState.UNKNOWN,
            raw_metrics={},
            resolution_reason="MISSING_PRIMITIVES",
            data_complete=False
        )

    raw_metrics = {
        "consecutive_low_bb_width_bars": int(low_bb_bars),
        "bb_width_slope": float(bb_slope),
        "range_contraction_ratio": float(range_contraction),
        "inside_bar_count": int(inside_bars)
    }

    if bb_slope > 0.1 and low_bb_bars > 3:
        state = CompressionMaturityState.RELEASING
    elif low_bb_bars > 8 or inside_bars > 2:
        state = CompressionMaturityState.MATURE_COMPRESSION
    elif low_bb_bars > 0 or range_contraction < 0.8:
        state = CompressionMaturityState.EARLY_COMPRESSION
    elif bb_slope < -0.1:
        state = CompressionMaturityState.POST_EXPANSION
    else:
        state = CompressionMaturityState.UNKNOWN

    return ChartStateResult(state=state, raw_metrics=raw_metrics)
