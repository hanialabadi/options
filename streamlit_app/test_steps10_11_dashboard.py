"""
Add this section to your streamlit_app/dashboard.py after Step 6 (GEM Filter)

This adds buttons to run Steps 7-11 individually or the full pipeline.
"""

# ========================================
# STEPS 7-11: PRESCRIPTIVE PIPELINE
# ========================================

st.divider()
st.header("🎯 Prescriptive Pipeline (Steps 7-11)")
st.markdown("""
**Purpose:** Generate actionable trade recommendations  
**Input:** GEM candidates from Step 6  
**Output:** Final execution-ready strategies with PCS scores
""")

# Configuration sidebar
with st.sidebar:
    st.divider()
    st.header("🎯 Strategy Config")
    
    # Step 9B/10/11 settings
    enable_step9b = st.checkbox("Enable Step 9B (Fetch Contracts)", value=True)
    enable_step10 = st.checkbox("Enable Step 10 (PCS Filter)", value=True)
    enable_step11 = st.checkbox("Enable Step 11 (Strategy Pairing)", value=True)
    
    if enable_step11:
        enable_straddles = st.checkbox("Enable Straddles", value=True)
        enable_strangles = st.checkbox("Enable Strangles", value=True)
        capital_limit = st.number_input("Capital Limit per Position", 1000, 50000, 10000, 1000)
        max_contracts = st.slider("Max Contracts per Leg", 1, 50, 20, 1)

# === Full Pipeline Button ===
col1, col2 = st.columns([1, 3])
with col1:
    run_full = st.button("▶️ Run Full Pipeline (Steps 7-11)", type="primary", use_container_width=True)

with col2:
    if 'step6_gem' not in st.session_state or st.session_state['step6_gem'] is None:
        st.warning("⚠️ Run Steps 2-6 first to get GEM candidates")
    else:
        st.info(f"Ready: {len(st.session_state['step6_gem'])} GEM candidates from Step 6")

if run_full:
    if 'step6_gem' not in st.session_state or st.session_state['step6_gem'] is None:
        st.error("❌ No GEM candidates. Run Steps 2-6 first.")
    else:
        try:
            with st.spinner("🚀 Running full prescriptive pipeline..."):
                from core.scan_engine import run_full_scan_pipeline
                
                # Get snapshot path from Step 2
                snapshot_path = st.session_state.get('snapshot_path', None)
                
                # Run full pipeline with Steps 7-11
                results = run_full_scan_pipeline(
                    snapshot_path=snapshot_path,
                    include_step7=True,
                    include_step8=True,
                    include_step9a=True,
                    include_step9b=enable_step9b,
                    include_step10=enable_step10,
                    include_step11=enable_step11,
                    enable_straddles=enable_straddles if enable_step11 else True,
                    enable_strangles=enable_strangles if enable_step11 else True,
                    capital_limit=capital_limit if enable_step11 else 10000,
                    account_balance=100000,
                    max_portfolio_risk=0.20,
                    sizing_method='volatility_scaled'
                )
                
                # Store results
                if enable_step11 and 'ranked_strategies' in results:
                    st.session_state['step11_ranked'] = results['ranked_strategies']
                    if 'final_trades' in results:
                        st.session_state['step8_final'] = results['final_trades']
                        st.success(f"✅ Pipeline complete! {len(results['final_trades'])} final trades selected")
                    else:
                        st.success(f"✅ Pipeline complete! {len(results['ranked_strategies'])} strategies ranked")
                elif enable_step10 and 'filtered_contracts' in results:
                    st.session_state['step10_filtered'] = results['filtered_contracts']
                    st.success(f"✅ Pipeline complete! {len(results['filtered_contracts'])} filtered contracts")
                else:
                    st.warning("⚠️ Pipeline completed but no strategies generated")
                
                # Display summary
                with st.expander("📊 Pipeline Summary", expanded=True):
                    summary_cols = st.columns(5)
                    with summary_cols[0]:
                        st.metric("Step 7: Recommendations", 
                                 len(results.get('recommendations', [])) if 'recommendations' in results else 0)
                    with summary_cols[1]:
                        st.metric("Step 9B: Contracts", 
                                 len(results.get('selected_contracts', [])) if 'selected_contracts' in results else 0)
                    with summary_cols[2]:
                        st.metric("Step 10: Filtered", 
                                 len(results.get('filtered_contracts', [])) if 'filtered_contracts' in results else 0)
                    with summary_cols[3]:
                        st.metric("Step 11: Ranked", 
                                 len(results.get('ranked_strategies', [])) if 'ranked_strategies' in results else 0)
                    with summary_cols[4]:
                        st.metric("Step 8: Final Trades", 
                                 len(results.get('final_trades', [])) if 'final_trades' in results else 0)
                
        except Exception as e:
            st.error(f"❌ Pipeline failed: {e}")
            st.exception(e)

# === Display Step 11 Results ===
if 'step11_ranked' in st.session_state and st.session_state['step11_ranked'] is not None:
    st.divider()
    st.header("🎯 Step 11: Ranked Strategies")
    
    df_ranked = st.session_state['step11_ranked']
    
    if not df_ranked.empty:
        # Summary metrics
        metric_cols = st.columns(4)
        with metric_cols[0]:
            st.metric("Total Strategies", len(df_ranked))
        with metric_cols[1]:
            avg_score = df_ranked['Comparison_Score'].mean() if 'Comparison_Score' in df_ranked else 0
            st.metric("Avg Comparison Score", f"{avg_score:.1f}")
        with metric_cols[2]:
            unique_tickers = df_ranked['Ticker'].nunique() if 'Ticker' in df_ranked else 0
            st.metric("Unique Tickers", unique_tickers)
        with metric_cols[3]:
            rank1_count = len(df_ranked[df_ranked['Strategy_Rank'] == 1]) if 'Strategy_Rank' in df_ranked else 0
            st.metric("Rank #1 Strategies", rank1_count)
        
        # Display table
        display_cols = [
            'Ticker', 'Primary_Strategy', 'Strategy_Rank', 'Comparison_Score',
            'Expected_Return_Score', 'Greeks_Quality_Score', 'Cost_Efficiency_Score',
            'Liquidity_Quality_Score', 'Goal_Alignment_Score', 'Risk_Adjusted_Score',
            'Delta', 'Vega', 'Gamma', 'Total_Debit'
        ]
        display_cols = [c for c in display_cols if c in df_ranked.columns]
        
        st.dataframe(
            df_ranked[display_cols].sort_values('Comparison_Score', ascending=False),
            use_container_width=True,
            height=400
        )
        
        # Download button
        csv = df_ranked.to_csv(index=False)
        st.download_button(
            label="📥 Download Ranked Strategies CSV",
            data=csv,
            file_name=f"ranked_strategies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
    else:
        st.info("No ranked strategies generated. Check filter settings.")

# === Display Step 8 Results ===
if 'step8_final' in st.session_state and st.session_state['step8_final'] is not None:
    st.divider()
    st.header("💰 Step 8: Final Trades")
    
    df_final = st.session_state['step8_final']
    
    if not df_final.empty:
        # Summary metrics
        metric_cols = st.columns(4)
        with metric_cols[0]:
            st.metric("Final Trades", len(df_final))
        with metric_cols[1]:
            total_capital = df_final['Dollar_Allocation'].sum() if 'Dollar_Allocation' in df_final else 0
            st.metric("Total Capital", f"${total_capital:,.0f}")
        with metric_cols[2]:
            total_contracts = df_final['Num_Contracts'].sum() if 'Num_Contracts' in df_final else 0
            st.metric("Total Contracts", int(total_contracts))
        with metric_cols[3]:
            avg_score = df_final['Comparison_Score'].mean() if 'Comparison_Score' in df_final else 0
            st.metric("Avg Score", f"{avg_score:.1f}")
        
        # Display table
        display_cols = [
            'Ticker', 'Primary_Strategy', 'Strategy_Rank', 'Comparison_Score',
            'Dollar_Allocation', 'Num_Contracts', 'Position_Size', 'Capital_Required',
            'Delta', 'Theta', 'Total_Debit'
        ]
        display_cols = [c for c in display_cols if c in df_final.columns]
        
        st.dataframe(
            df_final[display_cols].sort_values('Comparison_Score', ascending=False),
            use_container_width=True,
            height=400
        )
        
        # Download button
        csv = df_final.to_csv(index=False)
        st.download_button(
            label="📥 Download Final Trades CSV",
            data=csv,
            file_name=f"final_trades_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
    else:
        st.info("No final trades generated.")

# === Display Step 10 Results (if Step 11 disabled) ===
elif 'step10_filtered' in st.session_state and st.session_state['step10_filtered'] is not None:
    st.divider()
    st.header("✅ Step 10: Filtered Contracts")
    
    df_filtered = st.session_state['step10_filtered']
    
    if not df_filtered.empty:
        st.metric("Execution-Ready Contracts", len(df_filtered[df_filtered['Execution_Ready'] == True]))
        
        display_cols = [
            'Ticker', 'Primary_Strategy', 'PCS_Final', 'Pre_Filter_Status',
            'Execution_Ready', 'Actual_DTE', 'Delta', 'Vega', 'Gamma',
            'Total_Debit', 'Liquidity_Score', 'Bid_Ask_Spread_Pct'
        ]
        display_cols = [c for c in display_cols if c in df_filtered.columns]
        
        st.dataframe(
            df_filtered[display_cols].sort_values('PCS_Final', ascending=False),
            use_container_width=True,
            height=400
        )
