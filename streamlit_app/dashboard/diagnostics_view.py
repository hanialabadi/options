# diagnostics_view.py

import streamlit as st

def show_diagnostics(df):
    st.subheader("🔍 PCS Drift + Tier Diagnostics")
    st.dataframe(df[[
        "TradeID", "PCS_Entry", "PCS", "PCS_Drift", "PCS_Tier", "Gamma", "Vega", 
        "IVHV_Gap", "Structure_Intact", "Rec_Tier", "Rec_Action", "Rec_V6_Cause"
    ]].sort_values(by="PCS_Drift", ascending=True))

    st.subheader("📊 Confidence Tier Distribution")
    st.bar_chart(df["PCS_Tier"].value_counts())

    st.subheader("🧠 Composite Signal Summary")
    st.dataframe(df[[
        "TradeID", "Gamma_ROC", "Vega_ROC", "Delta_ROC", "Signal_HH", "PCS_Live"
    ]].sort_values(by="PCS_Live", ascending=False))
