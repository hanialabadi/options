# prefreeze_pipeline.py

from core.phase1_clean import phase1_load_and_clean_raw_v2
from core.phase2_parse import phase2_parse_symbols
from core.phase3_pcs_score import calculate_pcs
from core.phase3_pcs_score import calculate_ivhv_gap, calculate_skew_and_kurtosis
from core.phase3_5_freeze_fields import phase3_5_fill_freeze_fields
from core.phase6_freeze_and_archive import phase6_freeze_and_archive

def run_prefreeze_pipeline(df_input, df_master_current):
    # === Run Core Phases ===
    df_clean = phase1_load_and_clean_raw_v2(df_input)
    df_parsed = phase2_parse_symbols(df_clean)
    df_pcs = calculate_pcs(df_parsed)
    df_ivhv = calculate_ivhv_gap(df_pcs)
    df_skew = calculate_skew_and_kurtosis(df_ivhv)
    df_annotated = phase3_5_fill_freeze_fields(df_skew)

    # === Freeze and Archive ===
    df_frozen = phase6_freeze_and_archive(df_annotated, df_master_current)
    return df_frozen
