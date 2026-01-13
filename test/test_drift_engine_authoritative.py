import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from core.management_engine.drift_engine import DriftEngine

def test_drift_authoritative_overrides():
    print("🧪 Testing Drift Engine Authoritative Overrides...")
    
    # 1. Setup Mock Data
    now = datetime.now()
    data = {
        'TradeID': ['T1', 'T2', 'T3', 'T4', 'T5'],
        'Symbol': ['AAPL_C', 'MSFT_P', 'GOOGL_C', 'TSLA_P', 'META_C'],
        'Snapshot_TS': [
            now - timedelta(minutes=5),   # T1: Fresh
            now - timedelta(minutes=20),  # T2: Stale
            now - timedelta(minutes=70),  # T3: Orphaned
            now - timedelta(minutes=5),   # T4: Fresh
            now - timedelta(minutes=5),   # T5: Fresh
        ],
        'PCS_Drift': [0, 0, 0, 30, 0], # T4: Violated Signal
        'LegStatus': ['Active', 'Active', 'Active', 'Active', 'Broken'], # T5: Broken Structure
        'Rec_Action': ['HOLD', 'HOLD', 'HOLD', 'HOLD', 'HOLD'], # Alpha says HOLD for all
        'Delta_Trade': [1, 1, 1, 1, 1], # Low delta to avoid portfolio drift
        'Underlying': ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'META']
    }
    df = pd.DataFrame(data)
    
    # 2. Initialize Drift Engine
    engine = DriftEngine(persona='conservative')
    
    # 3. Run Analysis
    # We mock check_market_stress and evaluate_leg_status for this test
    import core.management_engine.drift_engine as de
    original_check = de.check_market_stress
    de.check_market_stress = lambda: ('GREEN', 20.0)
    
    # Mock evaluate_leg_status to use the LegStatus we provided in the mock data
    # We must mock it in the drift_engine namespace where it was imported
    import core.management_engine.drift_engine as de_mod
    original_els = de_mod.evaluate_leg_status
    de_mod.evaluate_leg_status = lambda df, legs_dir=None: df 
    
    try:
        df_analyzed = engine.run_drift_analysis(df)
        df_final = engine.apply_drift_filter(df_analyzed, rec_col='Rec_Action')
        
        print("\n📊 Drift Analysis Results:")
        cols_to_show = ['TradeID', 'Data_State', 'Signal_State', 'Structural_State', 'Portfolio_State', 'Drift_Action', 'Rec_Action', 'Rec_Action_Final']
        print(df_final[cols_to_show])
        
        # 4. Assertions
        # T1: Fresh, Valid, Intact -> HOLD
        assert df_final.loc[0, 'Rec_Action_Final'] == 'HOLD'
        
        # T2: Stale -> REVALIDATE
        assert df_final.loc[1, 'Rec_Action_Final'] == 'REVALIDATE'
        
        # T3: Orphaned -> REVALIDATE (Quarantine)
        assert df_final.loc[2, 'Rec_Action_Final'] == 'REVALIDATE'
        
        # T4: Violated Signal -> EXIT
        assert df_final.loc[3, 'Rec_Action_Final'] == 'EXIT'
        
        # T5: Broken Structure -> EXIT (Force Exit)
        assert df_final.loc[4, 'Rec_Action_Final'] == 'EXIT'
        
        print("\n✅ All authoritative overrides verified successfully.")
        
    finally:
        de.check_market_stress = original_check
        de_mod.evaluate_leg_status = original_els

def test_regime_adaptive_contraction():
    print("\n🧪 Testing Regime-Adaptive Risk Contraction...")
    
    now = datetime.now()
    data = {
        'TradeID': ['T1'],
        'Symbol': ['AAPL_C'],
        'Snapshot_TS': [now],
        'Delta_Trade': [45], # Conservative limit is 50. 45 is OK in GREEN, but OVER in YELLOW (50 * 0.75 = 37.5)
        'Underlying': ['AAPL'],
        'LegStatus': ['Active'],
        'Rec_Action': ['HOLD']
    }
    df = pd.DataFrame(data)
    
    engine = DriftEngine(persona='conservative')
    import core.management_engine.drift_engine as de
    original_check = de.check_market_stress
    
    try:
        # Test YELLOW Regime
        print("🟡 Setting Regime to STRESSED (YELLOW)...")
        de.check_market_stress = lambda: ('YELLOW', 35.0)
        df_yellow = engine.run_drift_analysis(df.copy())
        df_yellow_final = engine.apply_drift_filter(df_yellow, rec_col='Rec_Action')
        
        print(f"Regime: {df_yellow_final.loc[0, 'Regime_State']}, Portfolio: {df_yellow_final.loc[0, 'Portfolio_State']}, Final Rec: {df_yellow_final.loc[0, 'Rec_Action_Final']}")
        assert df_yellow_final.loc[0, 'Portfolio_State'] == 'OVER_LIMIT'
        assert df_yellow_final.loc[0, 'Rec_Action_Final'] == 'TRIM'
        
        # Test RED Regime
        print("🔴 Setting Regime to HALTED (RED)...")
        de.check_market_stress = lambda: ('RED', 45.0)
        df_red = engine.run_drift_analysis(df.copy())
        df_red_final = engine.apply_drift_filter(df_red, rec_col='Rec_Action')
        
        print(f"Regime: {df_red_final.loc[0, 'Regime_State']}, Final Rec: {df_red_final.loc[0, 'Rec_Action_Final']}")
        assert df_red_final.loc[0, 'Regime_State'] == 'HALTED'
        assert df_red_final.loc[0, 'Rec_Action_Final'] == 'WAIT'
        
        print("\n✅ Regime-adaptive contraction verified successfully.")
        
    finally:
        de.check_market_stress = original_check

if __name__ == "__main__":
    test_drift_authoritative_overrides()
    test_regime_adaptive_contraction()
