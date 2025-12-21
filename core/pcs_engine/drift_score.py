# core/pcs_engine/drift_score.py

import numpy as np
import pandas as pd

def compute_drift_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes the drift penalty (or bonus) for PCS.
    Adds/returns a 'PCS_DriftPenalty' column (negative means penalty).
    """
    df = df.copy()
    drift = df.get("PCS_Drift", 0)
    vega = df.get("Vega", 0)
    gamma = df.get("Gamma", 0)

    # Example logic: Penalty if drift > 10, bonus if vega/gamma strong and drift low
    penalty = np.where(
        drift > 10,
        -10,
        np.where((vega > 0.3) & (gamma > 0.08) & (drift < 5), 5, 0)
    )
    df["PCS_DriftPenalty"] = penalty
    return df
