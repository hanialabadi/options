import pandas as pd
import numpy as np
from ..base import ChartStateResult
from ..state_definitions import GreekDominanceState

def compute_greek_dominance(row: pd.Series) -> ChartStateResult:
    """
    I. Greek Dominance (Options-Specific)
    Measures the balance between Theta and Gamma.
    """
    # RAG: Case-Insensitive Retrieval. Handle both broker (Uppercase) and system (Lowercase) schemas.
    theta = row.get("theta") if pd.notna(row.get("theta")) else row.get("Theta")
    gamma = row.get("gamma") if pd.notna(row.get("gamma")) else row.get("Gamma")

    if any(v is None or pd.isna(v) for v in [theta, gamma]):
        return ChartStateResult(
            state=GreekDominanceState.UNKNOWN,
            raw_metrics={},
            resolution_reason="MISSING_PRIMITIVES",
            data_complete=False
        )

    abs_theta = abs(float(theta))
    abs_gamma = abs(float(gamma))

    # Avoid division by zero
    ratio = abs_gamma / abs_theta if abs_theta > 1e-9 else 100.0

    # DTE-aware context: for LEAPs (DTE > 180), theta decay is trivial relative
    # to delta exposure. A ratio < 0.5 at 300+ DTE means ~$0.04/day on a
    # multi-thousand dollar position — not a meaningful risk signal.
    dte = float(row.get("DTE", 0) or 0)

    raw_metrics = {
        "abs_theta": abs_theta,
        "abs_gamma": abs_gamma,
        "gamma_theta_ratio": ratio,
        "dte": dte
    }

    if ratio > 2.0:
        state = GreekDominanceState.GAMMA_DOMINANT
    elif ratio < 0.5 and dte <= 180:
        state = GreekDominanceState.THETA_DOMINANT
    else:
        state = GreekDominanceState.BALANCED

    return ChartStateResult(state=state, raw_metrics=raw_metrics)
