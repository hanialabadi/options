import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from core.management_engine.recommend import run_v6_overlay
from core.data_contracts import ACTIVE_MASTER_PATH

def run_runtime_audit():
    print("🔍 Starting Runtime Audit of Management Layer Governance...")
    
    # 1. Prepare Audit Scenario Data
    now = datetime.now()
    audit_data = {
        'TradeID': ['AUDIT_INTACT', 'AUDIT_BROKEN', 'AUDIT_STALE', 'AUDIT_VIOLATED', 'AUDIT_OVER_LIMIT'],
        'Symbol': ['AAPL_C', 'MSFT_P', 'GOOGL_C', 'TSLA_P', 'AMZN_C'],
        'Snapshot_TS': [
            now - timedelta(minutes=5),   # INTACT: Fresh
            now - timedelta(minutes=5),   # BROKEN: Fresh
            now - timedelta(minutes=45),  # STALE: 45m old
            now - timedelta(minutes=5),   # VIOLATED: Fresh
            now - timedelta(minutes=5),   # OVER_LIMIT: Fresh
        ],
        'PCS_Drift': [0, 0, 0, 30, 0], 
        'LegStatus': ['Active', 'Broken', 'Active', 'Active', 'Active'],
        'Delta_Trade': [1, 1, 1, 1, 1000], # AMZN_C will trigger portfolio violation
        'Underlying': ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'AMZN'],
        'Strategy': ['Long Call', 'Bull Put', 'Long Call', 'Long Call', 'Long Call'],
        'Rec_Action': ['HOLD', 'HOLD', 'HOLD', 'HOLD', 'HOLD'], # Force base Rec_Action to HOLD
        'Rec_V6': ['HOLD', 'HOLD', 'HOLD', 'HOLD', 'HOLD'], # Initial state
        'Days_Held': [1, 1, 1, 1, 1],
        'Vega': [0.3, 0.3, 0.3, 0.3, 0.3],
        'Gamma': [0.03, 0.03, 0.03, 0.03, 0.03],
        'Theta': [0.1, 0.1, 0.1, 0.1, 0.1],
        'PCS': [85, 85, 85, 85, 85],
        'IVHV_Gap': [5, 5, 5, 5, 5]
    }
    df_audit = pd.DataFrame(audit_data)
    
    # 2. Execute Management Pipeline (Alpha + Resilience)
    print("\n🚀 Executing run_v6_overlay (Alpha + Drift Engine)...")
    
    # Mock market stress to GREEN for general audit
    import core.management_engine.drift_engine as de
    original_check = de.check_market_stress
    de.check_market_stress = lambda: ('GREEN', 20.0)
    
    # Mock evaluate_leg_status to preserve our audit states
    import core.management_engine.drift_engine as de_mod
    original_els = de_mod.evaluate_leg_status
    de_mod.evaluate_leg_status = lambda df, legs_dir=None: df

    # Mock predict_success_probabilities to avoid feature mismatch in audit
    import core.management_engine.recommend as rec_mod
    original_predict = rec_mod.predict_success_probabilities
    rec_mod.predict_success_probabilities = lambda df: df

    # Mock the entire alpha layer to preserve our audit Rec_Action
    import core.rec_engine_v6.rec_signal_core as rsc
    original_apply = rsc.apply_rec_signals
    rsc.apply_rec_signals = lambda df: df

    import core.rec_engine_v6.rec_enrich_flags as ref
    original_enrich = ref.enrich_signal_flags
    ref.enrich_signal_flags = lambda df: df

    import core.rec_engine_v6.rec_infer_rules as rir
    original_infer = rir.infer_recommendation_rules
    rir.infer_recommendation_rules = lambda df: df

    import core.rec_engine_v6.rec_strategy_alignment as rsa
    original_align = rsa.align_strategy_signals
    rsa.align_strategy_signals = lambda df: df

    import core.rec_engine_v6.rec_tag_persona_confidence as rtp
    original_tag = rtp.tag_persona_and_confidence
    rtp.tag_persona_and_confidence = lambda df: df

    import core.rec_engine_v6.rec_overlay_patch as rop
    original_patch = rop.final_patch_overlay
    rop.final_patch_overlay = lambda df: df

    import core.rec_engine_v6.rec_cleanup as rcl
    original_clean = rcl.clean_rec_output
    rcl.clean_rec_output = lambda df: df
    
    try:
        # run_v6_overlay internally calls DriftEngine.run_drift_analysis and apply_drift_filter
        df_result = run_v6_overlay(df_audit)
        
        # 3. Inspect Results
        print("\n📋 AUDIT RESULTS: Rec_Action (Alpha) vs Rec_Action_Final (Governance)")
        print("-" * 110)
        cols = ['TradeID', 'Data_State', 'Structural_State', 'Portfolio_State', 'Drift_Action', 'Rec_Action', 'Rec_Action_Final']
        print(df_result[cols].to_markdown(index=False))
        print("-" * 110)
        
        # 4. Verification Logic
        # Note: Because AUDIT_OVER_LIMIT triggers a portfolio-wide violation, 
        # even INTACT trades are restricted to TRIM. This is CORRECT governance.
        verifications = [
            ('AUDIT_INTACT', 'HOLD', 'TRIM', "Normal trade restricted to TRIM due to portfolio violation"),
            ('AUDIT_BROKEN', 'HOLD', 'EXIT', "Broken trade must be FORCE_EXITed (Priority over Portfolio)"),
            ('AUDIT_STALE', 'HOLD', 'REVALIDATE', "Stale trade must enter Quarantine (Priority over Portfolio)"),
            ('AUDIT_VIOLATED', 'HOLD', 'EXIT', "Violated signal must be EXITed (Priority over Portfolio)"),
            ('AUDIT_OVER_LIMIT', 'HOLD', 'TRIM', "Portfolio violation must trigger TRIM")
        ]
        
        success = True
        for tid, expected_alpha, expected_final, msg in verifications:
            actual_final = df_result.loc[df_result['TradeID'] == tid, 'Rec_Action_Final'].values[0]
            if actual_final == expected_final:
                print(f"✅ {tid}: {msg} (Actual: {actual_final})")
            else:
                print(f"❌ {tid}: {msg} (Expected: {expected_final}, Actual: {actual_final})")
                success = False
                
        if success:
            print("\n🏆 RUNTIME AUDIT PASSED: Drift Engine is acting as the final authoritative gate.")
        else:
            print("\n⚠️ RUNTIME AUDIT FAILED: Discrepancies detected in governance enforcement.")
            
    finally:
        de.check_market_stress = original_check
        de_mod.evaluate_leg_status = original_els
        rec_mod.predict_success_probabilities = original_predict
        rsc.apply_rec_signals = original_apply
        ref.enrich_signal_flags = original_enrich
        rir.infer_recommendation_rules = original_infer
        rsa.align_strategy_signals = original_align
        rtp.tag_persona_and_confidence = original_tag
        rop.final_patch_overlay = original_patch
        rcl.clean_rec_output = original_clean

if __name__ == "__main__":
    run_runtime_audit()
