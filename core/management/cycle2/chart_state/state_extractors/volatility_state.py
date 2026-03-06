import pandas as pd
from ..base import ChartStateResult
from ..state_definitions import VolatilityState

def compute_volatility_state(row: pd.Series) -> ChartStateResult:
    """
    C. Volatility State (Level + Context)
    Measures HV, ATR, and Bollinger Band Width.
    """
    # RAG: Case-Insensitive Retrieval. Handle both broker (Uppercase) and system (Lowercase) schemas.
    hv_20d = row.get("hv_20d") if pd.notna(row.get("hv_20d")) else row.get("HV_20D")
    hv_percentile = row.get("hv_20d_percentile")
    atr_14 = row.get("atr_14")
    bb_width_pct = row.get("bb_width_pct")
    bb_width_z = row.get("bb_width_z")
    bb_width_percentile = row.get("bb_width_percentile") # Added for expansion

    metrics = [hv_20d, hv_percentile, atr_14, bb_width_pct, bb_width_z]
    if any(v is None or pd.isna(v) for v in metrics):
        return ChartStateResult(
            state=VolatilityState.UNKNOWN,
            raw_metrics={},
            resolution_reason="MISSING_PRIMITIVES",
            data_complete=False
        )

    raw_metrics = {
        "hv_20d": float(hv_20d),
        "hv_20d_percentile": float(hv_percentile),
        "atr_14": float(atr_14),
        "bb_width_pct": float(bb_width_pct),
        "bb_width_z": float(bb_width_z),
        "bb_width_percentile": float(bb_width_percentile) if bb_width_percentile is not None else None
    }

    if bb_width_z < -1.5 or bb_width_pct < 0.05:
        state = VolatilityState.COMPRESSED
    elif bb_width_z > 2.0 or hv_percentile > 0.9:
        state = VolatilityState.EXTREME
    elif bb_width_z > 0.5:
        state = VolatilityState.EXPANDING
    else:
        state = VolatilityState.NORMAL

    return ChartStateResult(state=state, raw_metrics=raw_metrics)
