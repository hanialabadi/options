# core/pcs_engine/aggregate.py

import pandas as pd
from pcs_engine.signal_score import compute_signal_score
from pcs_engine.chart_score import compute_chart_score
from pcs_engine.drift_score import compute_drift_score
from pcs_engine.strategy_score import compute_strategy_score

def pcs_engine_enrich(df: pd.DataFrame) -> pd.DataFrame:
    """
    PCS enrichment: Adds all PCS sub-scores/feature columns (no tier or rec).
    """
    df = df.copy()
    df = compute_signal_score(df)
    df = compute_chart_score(df)
    df = compute_drift_score(df)
    df = compute_strategy_score(df)
    # DO NOT assign PCS_UnifiedScore, PCS_Tier, Rec_Action here!
    return df
