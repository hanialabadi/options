from core.rec_engine_v6.rec_cleanup import clean_rec_output
from core.rec_engine_v6.rec_signal_core import apply_rec_signals
from core.rec_engine_v6.rec_enrich_flags import enrich_signal_flags
from core.rec_engine_v6.rec_infer_rules import infer_recommendation_rules
from core.rec_engine_v6.rec_strategy_alignment import align_strategy_signals
from core.rec_engine_v6.rec_tag_persona_confidence import tag_persona_and_confidence
from core.rec_engine_v6.rec_overlay_patch import final_patch_overlay
from core.rec_engine_v6.rec_audit_logger import log_recommendations
from core.probability_model import predict_success_probabilities  # <-- import
from core.management_engine.drift_engine import DriftEngine

def run_v6_overlay(df):
    # 1. Alpha Layer (Rec Engine V6)
    df = apply_rec_signals(df)
    df = enrich_signal_flags(df)
    df = infer_recommendation_rules(df)
    df = align_strategy_signals(df)
    df = tag_persona_and_confidence(df)
    df = final_patch_overlay(df)
    df = clean_rec_output(df)

    # 🛡 Patch to avoid duplicate Success_Prob
    if "Success_Prob" in df.columns:
        df.drop(columns=["Success_Prob"], inplace=True)

    df = predict_success_probabilities(df)  # ⬅️ Run model last

    # 2. Resilience Layer (Authoritative Drift Engine)
    # Note: DriftEngine expects certain columns like PCS_Drift, Snapshot_TS, etc.
    # These should be present in the active_master or enriched during the pipeline.
    drift_engine = DriftEngine(persona='conservative') # Default to conservative
    df = drift_engine.run_drift_analysis(df)
    
    # 3. Synthesis (Authoritative Filter)
    # Intersects Rec_Action (Alpha) with Drift_Action (Resilience)
    df = drift_engine.apply_drift_filter(df, rec_col='Rec_Action')

    log_recommendations(df)
    return df
