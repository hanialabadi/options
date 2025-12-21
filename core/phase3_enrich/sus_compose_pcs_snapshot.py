import pandas as pd
# from .ivhv_gap import calculate_ivhv_gap
from .skew_kurtosis import calculate_skew_and_kurtosis
# from .pcs_score import calculate_pcs

# === Phase 3: Enrichment Runner ===
def run_phase3_enrichment(df: pd.DataFrame) -> pd.DataFrame:
    # df = calculate_ivhv_gap(df)
    df = calculate_skew_and_kurtosis(df)
    # df = calculate_pcs(df)
    return df
