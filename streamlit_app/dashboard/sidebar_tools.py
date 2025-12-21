# sidebar_tools.py

import streamlit as st

def sidebar_tools():
    st.sidebar.header("🔧 Controls")

    if st.sidebar.button("🧹 Clear Cache & Reset"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.session_state.clear()
        st.experimental_rerun()

    trade_id = st.sidebar.text_input("🔍 Filter by TradeID", key="trade_id_input")
    return trade_id
