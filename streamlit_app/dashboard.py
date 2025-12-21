
import os
import streamlit as st
import pandas as pd
from datetime import datetime

# === Imports ===
from core.phase1_clean import phase1_load_and_clean_raw_v2 as phase1_load_and_clean
from core.phase2_parse import phase2_run_all

INPUT_PATH = "/Users/haniabadi/Documents/Windows/Positions_Account_.csv"

st.set_page_config(layout="wide")
st.title("🧪 Phase 1 + Phase 2 Debug Viewer")

# === Step 1: Load and Clean ===
if st.sidebar.button("🔍 Step 1: Load + Clean"):
    try:
        df_input = phase1_load_and_clean(input_path=INPUT_PATH)
        st.session_state["df_input"] = df_input
        st.success(f"✅ Loaded and cleaned {len(df_input)} rows")

        with st.expander("📥 Cleaned DataFrame (Step 1)", expanded=True):
            st.dataframe(df_input, use_container_width=True)

        st.subheader("📦 Unique Symbols")
        st.write(df_input["Symbol"].dropna().unique())

    except Exception as e:
        st.error(f"❌ Error in Step 1: {e}")

# === Step 2: Parse Symbols + Tag Strategy ===
if st.sidebar.button("🔎 Step 2: Parse + Tag"):
    try:
        df_input = st.session_state.get("df_input")
        if df_input is None:
            st.warning("⚠️ Run Step 1 first.")
        else:
            df_parsed = phase2_run_all(df_input)
            st.session_state["df_parsed"] = df_parsed
            st.success("✅ Symbols parsed and strategies tagged")

            with st.expander("🔎 Parsed DataFrame (Full)", expanded=True):
                st.dataframe(df_parsed, use_container_width=True)

            st.subheader("🧪 Parsed Columns")
            st.write(df_parsed.columns.tolist())

            st.subheader("🔑 Key Fields Snapshot")
            sample_cols = [col for col in ["Symbol", "Underlying", "Expiration", "OptionType", "Strike", "Strategy", "TradeID"] if col in df_parsed.columns]
            st.dataframe(df_parsed[sample_cols].head(20))

    except Exception as e:
        st.error(f"❌ Error in Step 2: {e}")
