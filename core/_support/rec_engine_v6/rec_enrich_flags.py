# core/rec_engine_v6/rec_enrich_flags.py

import pandas as pd

def enrich_signal_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Tag common signal-based flags like PCS drift, Vega flatlining, and IV/HV collapse.
    These flags are used downstream in decision and alert logic.
    """

    # PCS Drift flag
    if "PCS_Drift" in df.columns:
        df["Flag_PCS_Drift"] = df["PCS_Drift"].abs() > 15

    # Vega ROC flag
    if "Vega_ROC" in df.columns:
        df["Flag_Vega_Flat"] = df["Vega_ROC"] < 0

    # IV-HV collapse flag
    if "IVHV_Gap_Entry" in df.columns and "IVHV_Gap" in df.columns:
        df["Flag_IVHV_Collapse"] = (df["IVHV_Gap"] - df["IVHV_Gap_Entry"]) < -3

    # Outcome tag (optional but helpful)
    if "OutcomeTag" not in df.columns:
        try:
            from core.rec_engine_v6.rec_outcome_tags import assign_outcome_tags
            df = assign_outcome_tags(df)
        except ImportError:
            pass  # Optional fallback if outcome tagging module is missing

    return df
