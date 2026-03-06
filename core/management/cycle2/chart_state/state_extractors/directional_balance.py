import pandas as pd
from ..base import ChartStateResult
from ..state_definitions import DirectionalBalanceState

def compute_directional_balance(row: pd.Series) -> ChartStateResult:
    """
    F. Directional Balance (Control, not direction)
    Measures volume balance, close position, VWAP deviation, and delta pressure.
    """
    vol_ratio = row.get("up_volume_down_volume_ratio")
    close_pos = row.get("close_position_daily_range")
    vwap_dev = row.get("vwap_deviation")
    delta_pressure = row.get("delta_pressure_proxy")

    metrics = [vol_ratio, close_pos, vwap_dev, delta_pressure]
    if any(v is None or pd.isna(v) for v in metrics):
        return ChartStateResult(
            state=DirectionalBalanceState.UNKNOWN,
            raw_metrics={},
            resolution_reason="MISSING_PRIMITIVES",
            data_complete=False
        )

    raw_metrics = {
        "up_volume_down_volume_ratio": float(vol_ratio),
        "close_position_daily_range": float(close_pos),
        "vwap_deviation": float(vwap_dev),
        "delta_pressure_proxy": float(delta_pressure)
    }

    if vol_ratio > 1.5 and close_pos > 0.7:
        state = DirectionalBalanceState.BUYER_DOMINANT
    elif vol_ratio < 0.66 and close_pos < 0.3:
        state = DirectionalBalanceState.SELLER_DOMINANT
    elif 0.8 < vol_ratio < 1.25 and 0.4 < close_pos < 0.6:
        state = DirectionalBalanceState.BALANCED
    else:
        state = DirectionalBalanceState.CONTESTED

    return ChartStateResult(state=state, raw_metrics=raw_metrics)
