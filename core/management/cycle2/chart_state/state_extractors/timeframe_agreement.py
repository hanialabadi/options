import pandas as pd
from ..base import ChartStateResult
from ..state_definitions import TimeframeAgreementState

def compute_timeframe_agreement(row: pd.Series) -> ChartStateResult:
    """
    H. Timeframe Agreement (Multi-Scale Consistency)
    Measures alignment across daily, weekly, and intraday trends.
    """
    daily_weekly_align = row.get("daily_weekly_trend_alignment")
    daily_intraday_align = row.get("daily_intraday_trend_alignment")
    compression_align = row.get("compression_alignment_score")

    metrics = [daily_weekly_align, daily_intraday_align, compression_align]
    if any(v is None or pd.isna(v) for v in metrics):
        return ChartStateResult(
            state=TimeframeAgreementState.UNKNOWN,
            raw_metrics={},
            resolution_reason="MISSING_PRIMITIVES",
            data_complete=False
        )

    raw_metrics = {
        "daily_weekly_trend_alignment": float(daily_weekly_align),
        "daily_intraday_trend_alignment": float(daily_intraday_align),
        "compression_alignment_score": float(compression_align)
    }

    if daily_weekly_align > 0.8 and daily_intraday_align > 0.8:
        state = TimeframeAgreementState.ALIGNED
    elif daily_weekly_align > 0.5 or daily_intraday_align > 0.5:
        state = TimeframeAgreementState.PARTIAL
    else:
        state = TimeframeAgreementState.DIVERGENT

    return ChartStateResult(state=state, raw_metrics=raw_metrics)
