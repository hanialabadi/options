# core/pcs_engine/chart_score.py

import numpy as np
import pandas as pd

def compute_chart_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes/normalizes the chart composite score.
    Adds/returns a 'Chart_CompositeScore' column.
    """
    df = df.copy()
    # Assume Chart_CompositeScore is already computed by chart engine.
    # Otherwise, fallback to e.g. Chart_Score, or 50 as neutral.
    if "Chart_CompositeScore" not in df.columns:
        df["Chart_CompositeScore"] = df.get("Chart_Score", 50)
    df["Chart_CompositeScore"] = df["Chart_CompositeScore"].fillna(50).clip(0, 100)
    return df
