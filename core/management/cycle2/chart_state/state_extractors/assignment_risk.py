import pandas as pd
from ..base import ChartStateResult
from ..state_definitions import AssignmentRiskState

def compute_assignment_risk(row: pd.Series) -> ChartStateResult:
    """
    J. Assignment Risk (Options-Specific)
    Measures distance to strike, normalized by HV and DTE.
    """
    # RAG: Case-Insensitive Retrieval. Handle both broker (Uppercase) and system (Lowercase) schemas.
    dist_to_strike = row.get("dist_to_short_strike_pct")
    hv = row.get("hv_20d") if pd.notna(row.get("hv_20d")) else row.get("HV_20D")
    dte = row.get("dte") if pd.notna(row.get("dte")) else row.get("DTE")

    if any(v is None or pd.isna(v) for v in [dist_to_strike, hv, dte]):
        # Long options have no short strike — no assignment risk by definition.
        # Quantity > 0 means long position; return LOW rather than UNKNOWN.
        qty = row.get("Quantity") or row.get("quantity")
        is_long = pd.notna(qty) and float(qty) > 0
        if is_long:
            return ChartStateResult(
                state=AssignmentRiskState.LOW,
                raw_metrics={},
                resolution_reason="LONG_OPTION_NO_ASSIGNMENT_RISK",
                data_complete=True
            )
        return ChartStateResult(
            state=AssignmentRiskState.UNKNOWN,
            raw_metrics={},
            resolution_reason="MISSING_PRIMITIVES",
            data_complete=False
        )

    dist_to_strike = float(dist_to_strike)
    hv = float(hv)
    dte = float(dte)
    
    # Normalize distance by HV (approximate 1-std dev move for the DTE)
    # HV is annual, so HV * sqrt(DTE/252)
    expected_move = hv * (dte / 252.0)**0.5
    risk_ratio = abs(dist_to_strike) / expected_move if expected_move > 0 else 0.0

    raw_metrics = {
        "dist_to_short_strike_pct": dist_to_strike,
        "hv_20d": hv,
        "dte": dte,
        "risk_ratio": risk_ratio
    }

    if risk_ratio < 0.5 or dte < 2:
        state = AssignmentRiskState.IMMINENT
    elif risk_ratio < 1.5 or dte < 7:
        state = AssignmentRiskState.ELEVATED
    else:
        state = AssignmentRiskState.LOW

    return ChartStateResult(state=state, raw_metrics=raw_metrics)
