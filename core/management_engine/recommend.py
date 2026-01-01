from core.rec_engine_v6.rec_cleanup import clean_rec_output
from core.rec_engine_v6.rec_signal_core import apply_rec_signals
from core.rec_engine_v6.rec_enrich_flags import enrich_signal_flags
from core.rec_engine_v6.rec_infer_rules import infer_recommendation_rules
from core.rec_engine_v6.rec_strategy_alignment import align_strategy_signals
from core.rec_engine_v6.rec_tag_persona_confidence import tag_persona_and_confidence
from core.rec_engine_v6.rec_overlay_patch import final_patch_overlay
from core.rec_engine_v6.rec_audit_logger import log_recommendations
from core.probability_model import predict_success_probabilities  # <-- import

def run_v6_overlay(df):
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

    log_recommendations(df)
    return df
