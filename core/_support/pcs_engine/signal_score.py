# core/pcs_engine/signal_score.py

import numpy as np
import pandas as pd

def compute_signal_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes the PCS Signal Score using Greeks, IV, ROI, and Skew.
    Adds/returns a 'PCS_SignalScore' column.
    """
    # Use get to avoid KeyError if columns are missing; default to 0.
    df = df.copy()
    df["PCS_SignalScore"] = (
        0.3 * df.get("Vega", 0) +
        0.2 * df.get("Gamma", 0) +
        0.15 * df.get("Delta", 0) +
        0.15 * df.get("IV", 0) +
        0.1 * df.get("ROI", 0) +
        0.1 * df.get("Skew", 0)
    )
    # Optional: normalize/clip if needed
    df["PCS_SignalScore"] = df["PCS_SignalScore"].clip(0, 100)
    return df
