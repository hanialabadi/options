import pandas as pd
from ..base import ChartStateResult
from ..state_definitions import RangeEfficiencyState

def compute_range_efficiency(row: pd.Series) -> ChartStateResult:
    """
    G. Range Efficiency (Movement Quality)
    Measures Kaufman Efficiency, Choppiness, and movement quality.
    """
    kaufman_er = row.get("kaufman_efficiency_ratio")
    net_vs_total = row.get("net_movement_total_movement_ratio")
    choppiness = row.get("choppiness_index")

    metrics = [kaufman_er, net_vs_total, choppiness]
    if any(v is None or pd.isna(v) for v in metrics):
        return ChartStateResult(
            state=RangeEfficiencyState.UNKNOWN,
            raw_metrics={},
            resolution_reason="MISSING_PRIMITIVES",
            data_complete=False
        )

    raw_metrics = {
        "kaufman_efficiency_ratio": float(kaufman_er),
        "net_movement_total_movement_ratio": float(net_vs_total),
        "choppiness_index": float(choppiness)
    }

    if kaufman_er > 0.6 and choppiness < 38:
        state = RangeEfficiencyState.EFFICIENT_TREND
    elif choppiness > 61:
        state = RangeEfficiencyState.NOISY
    elif kaufman_er < 0.3 and choppiness > 50:
        state = RangeEfficiencyState.INEFFICIENT_RANGE
    elif net_vs_total > 0.8 and kaufman_er < 0.4:
        state = RangeEfficiencyState.FAKE_BREAK
    else:
        state = RangeEfficiencyState.UNKNOWN

    return ChartStateResult(state=state, raw_metrics=raw_metrics)
