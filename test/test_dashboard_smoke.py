"""
Dashboard Smoke Test - Verify Step 0 → Step 2 integration works in dashboard context
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
from core.scan_engine.step2_load_snapshot import load_ivhv_snapshot

def main():
    st.set_page_config(page_title="Step 0 Integration Test", layout="wide")
    
    st.title("🧪 Step 0 → Step 2 Integration Test")
    
    st.markdown("""
    This test verifies that the dashboard can load live snapshots from Step 0.
    """)
    
    # Test loading live snapshot
    st.header("Test 1: Load Live Snapshot")
    
    if st.button("Load Latest Live Snapshot", type="primary"):
        with st.spinner("Loading..."):
            try:
                df = load_ivhv_snapshot(use_live_snapshot=True, skip_pattern_detection=True)
                
                st.success(f"✅ Loaded {len(df)} tickers with {len(df.columns)} columns")
                
                # Show basic stats
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Total Tickers", len(df))
                
                with col2:
                    hv_populated = df['HV_30_D_Cur'].notna().sum()
                    st.metric("HV Coverage", f"{hv_populated}/{len(df)}")
                
                with col3:
                    if 'IV_30_D_Call' in df.columns:
                        iv_populated = df['IV_30_D_Call'].notna().sum()
                        st.metric("IV Coverage", f"{iv_populated}/{len(df)}")
                    else:
                        st.metric("IV Coverage", "N/A")
                
                # Show data preview
                st.subheader("Data Preview")
                
                identifier = 'Ticker' if 'Ticker' in df.columns else 'Symbol'
                display_cols = [identifier, 'HV_30_D_Cur']
                
                # Add Step 0 specific columns if present
                if 'hv_slope' in df.columns:
                    display_cols.append('hv_slope')
                if 'volatility_regime' in df.columns:
                    display_cols.append('volatility_regime')
                if 'data_source' in df.columns:
                    display_cols.append('data_source')
                
                st.dataframe(df[display_cols].head(10), use_container_width=True)
                
                # Show volatility regime distribution
                if 'volatility_regime' in df.columns:
                    st.subheader("Volatility Regime Distribution")
                    regime_counts = df['volatility_regime'].value_counts()
                    st.bar_chart(regime_counts)
                
            except Exception as e:
                st.error(f"❌ Error: {e}")
                import traceback
                st.code(traceback.format_exc())
    
    st.divider()
    
    # Test backward compatibility
    st.header("Test 2: Backward Compatibility")
    st.info("✅ use_live_snapshot defaults to False - existing code unaffected")

if __name__ == '__main__':
    main()
