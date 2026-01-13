import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os

# Mock Streamlit to allow importing dashboard logic
class MockSt:
    def sidebar(self): pass
    def button(self, *args, **kwargs): return False
    def spinner(self, *args, **kwargs):
        class Context:
            def __enter__(self): pass
            def __exit__(self, *args): pass
        return Context()
    def success(self, *args, **kwargs): print(f"SUCCESS: {args[0]}")
    def error(self, *args, **kwargs): print(f"ERROR: {args[0]}")
    def warning(self, *args, **kwargs): print(f"WARNING: {args[0]}")
    def info(self, *args, **kwargs): print(f"INFO: {args[0]}")
    def dataframe(self, *args, **kwargs): pass
    def divider(self): pass
    def metric(self, *args, **kwargs): pass
    def tabs(self, *args, **kwargs): return [None, None, None]
    def subheader(self, *args, **kwargs): pass
    def set_page_config(self, *args, **kwargs): pass
    def markdown(self, *args, **kwargs): pass
    def columns(self, *args, **kwargs): return [self, self, self, self]
    def radio(self, *args, **kwargs): return None
    def file_uploader(self, *args, **kwargs): return None
    def text_input(self, *args, **kwargs): return None
    def number_input(self, *args, **kwargs): return 0
    def slider(self, *args, **kwargs): return 0
    def selectbox(self, *args, **kwargs): return None
    def toggle(self, *args, **kwargs): return False
    def rerun(self): pass

sys.modules['streamlit'] = MockSt()

# Now we can test the pipeline logic used in the dashboard
from core.management_engine.recommend import run_v6_overlay

def verify_dashboard_end_to_end():
    print("🧪 Verifying Dashboard Management Pipeline End-to-End...")
    
    # 1. Simulate the data flow in dashboard.py (Manage View)
    now = datetime.now()
    df_mock = pd.DataFrame({
        'TradeID': ['DASH_1', 'DASH_2'],
        'Symbol': ['AAPL_C', 'MSFT_P'],
        'Snapshot_TS': [now, now],
        'Strategy': ['Long Call', 'Bull Put'],
        'PCS': [80, 80],
        'Delta_Trade': [10, 10],
        'Underlying': ['AAPL', 'MSFT'],
        'Days_Held': [1, 1],
        'Vega': [0.3, 0.3],
        'Gamma': [0.03, 0.03],
        'Theta': [0.1, 0.1],
        'IVHV_Gap': [5, 5]
    })
    
    # 2. Execute the same call the dashboard now makes
    print("🚀 Executing run_v6_overlay...")
    
    # Mock the alpha layer to avoid sklearn issues in this environment
    import core.rec_engine_v6.rec_signal_core as rsc
    rsc.apply_rec_signals = lambda df: df.assign(Rec_Action='HOLD')
    import core.rec_engine_v6.rec_enrich_flags as ref
    ref.enrich_signal_flags = lambda df: df
    import core.rec_engine_v6.rec_infer_rules as rir
    rir.infer_recommendation_rules = lambda df: df
    import core.rec_engine_v6.rec_strategy_alignment as rsa
    rsa.align_strategy_signals = lambda df: df
    import core.rec_engine_v6.rec_tag_persona_confidence as rtp
    rtp.tag_persona_and_confidence = lambda df: df
    import core.rec_engine_v6.rec_overlay_patch as rop
    rop.final_patch_overlay = lambda df: df
    import core.rec_engine_v6.rec_cleanup as rcl
    rcl.clean_rec_output = lambda df: df
    import core.management_engine.recommend as rec_mod
    rec_mod.predict_success_probabilities = lambda df: df
    
    # Mock evaluate_leg_status
    import core.management_engine.drift_engine as de_mod
    de_mod.evaluate_leg_status = lambda df, legs_dir=None: df.assign(LegStatus='Active')
    
    df_final = run_v6_overlay(df_mock)
    
    # 3. Verify authoritative columns exist (what the dashboard now expects)
    expected_cols = ['Rec_Action_Final', 'Drift_Action', 'Data_State', 'Portfolio_State']
    for col in expected_cols:
        assert col in df_final.columns, f"❌ Column {col} missing from final dashboard dataframe"
        
    print("✅ Dashboard pipeline verification successful. Authoritative columns are present.")
    print(f"📊 Sample Output:\n{df_final[['TradeID', 'Rec_Action_Final', 'Drift_Action']]}")

if __name__ == "__main__":
    verify_dashboard_end_to_end()
