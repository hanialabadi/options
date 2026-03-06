import pandas as pd
from ..base import ChartStateResult
from ..state_definitions import PriceStructureState

def compute_price_structure(row: pd.Series) -> ChartStateResult:
    """
    A. Price Structure (Market Geometry)
    Measures swing counts, structure breaks, and range expansion.
    """
    # Required Raw Metrics
    hh_count = row.get("swing_hh_count")
    hl_count = row.get("swing_hl_count")
    lh_count = row.get("swing_lh_count")
    ll_count = row.get("swing_ll_count")
    bos = row.get("break_of_structure")
    range_expansion = row.get("atr_normalized_range_expansion")
    close_loc = row.get("close_location_in_structure")

    # Check for missing data
    metrics = [hh_count, hl_count, lh_count, ll_count, bos, range_expansion, close_loc]
    if any(v is None or pd.isna(v) for v in metrics):
        return ChartStateResult(
            state=PriceStructureState.UNKNOWN,
            raw_metrics={},
            resolution_reason="MISSING_PRIMITIVES",
            data_complete=False
        )

    raw_metrics = {
        "swing_hh_count": int(hh_count),
        "swing_hl_count": int(hl_count),
        "swing_lh_count": int(lh_count),
        "swing_ll_count": int(ll_count),
        "break_of_structure": bool(bos),
        "atr_normalized_range_expansion": float(range_expansion),
        "close_location_in_structure": float(close_loc),
        "range_containment_pct": float(row.get("range_containment_pct", 0.0)),
        "net_displacement_pct": float(row.get("net_displacement_pct", 0.0))
    }

    # Deterministic Mapping Logic
    if bos:
        state = PriceStructureState.STRUCTURE_BROKEN
    elif hh_count > lh_count and hl_count > ll_count:
        state = PriceStructureState.STRUCTURAL_UP
    elif ll_count > hl_count and lh_count > hh_count:
        state = PriceStructureState.STRUCTURAL_DOWN
    elif range_expansion > 2.0:
        state = PriceStructureState.CHAOTIC
    else:
        state = PriceStructureState.RANGE_BOUND

    return ChartStateResult(state=state, raw_metrics=raw_metrics)
