# %% ✅ Imports
import os
import sys
import streamlit as st
import pandas as pd

# Ensure path to core modules
sys.path.append(os.path.abspath("."))

# Streamlit config
st.set_page_config(page_title="🧠 Pre-Freeze PCS Engine", layout="wide")
st.title("🚦 Pre-Freeze Pipeline – Raw Data → PCS → IVHV → Skew")

# ✅ Try core imports (Phase 1–3 only)
try:
    from core.phase1_clean import phase1_load_and_clean_raw_v2
    from core.phase2_parse import phase2_parse_symbols
    from core.phase3_pcs_score import (
        calculate_pcs, calculate_ivhv_gap, calculate_skew_and_kurtosis
    )
    st.success("✅ Core modules (Phase 1–3) imported.")
except Exception as e:
    st.error(f"❌ Import error: {e}")
    st.stop()

# === 🧠 Session state check
if "df" not in st.session_state:
    st.session_state["df"] = pd.DataFrame()

# === 📂 Sidebar: Load raw CSV and run Phases 1–3
st.sidebar.header("📁 Load Raw Data")
if st.sidebar.button("📂 Load & Run Pre-Freeze Pipeline"):
    try:
        input_path = "/Users/haniabadi/Documents/Windows/Positions_Account_.csv"
        st.info(f"⏳ Loading: {input_path}")
        df, _ = phase1_load_and_clean_raw_v2(input_path=input_path)
        df = phase2_parse_symbols(df)
        df = calculate_pcs(df)
        df = calculate_ivhv_gap(df)
        df = calculate_skew_and_kurtosis(df)

        st.session_state["df"] = df
        st.success(f"✅ Raw data loaded and pre-freeze phases complete. Rows: {len(df)}")
    except Exception as e:
        st.error(f"❌ Error running pre-freeze pipeline: {e}")

# === 📊 Display Section

# === 📊 Raw Snapshot Display (Unfiltered)
df = st.session_state.get("df", pd.DataFrame())
if not df.empty:
    st.subheader("📊 Pre-Freeze Snapshot Preview")

    st.write(f"🧮 Showing all {df.shape[0]} trades")
    st.dataframe(df, use_container_width=True)

    st.download_button("📥 Download Full Snapshot CSV", df.to_csv(index=False), file_name="pre_freeze_snapshot.csv")

    # 🔎 Filters
    st.sidebar.markdown("---")
    st.sidebar.subheader("🔍 Filters")
    pcs_min = st.sidebar.slider("PCS Score ≥", 60, 100, 75)
    dte_max = st.sidebar.slider("Max DTE", 0, 60, 30)
    symbol_filter = st.sidebar.text_input("Filter Symbol (optional)", "").upper()

    # 🔍 Apply filters
    filtered_df = df[df["PCS"] >= pcs_min]
    filtered_df = filtered_df[filtered_df["DTE"] <= dte_max]
    if symbol_filter:
        filtered_df = filtered_df[filtered_df["Symbol"].str.contains(symbol_filter)]

    st.write(f"🧮 Showing {filtered_df.shape[0]} of {df.shape[0]} trades")
    st.dataframe(filtered_df, use_container_width=True)

    st.download_button("📥 Download Filtered CSV", filtered_df.to_csv(index=False), file_name="pre_freeze_filtered.csv")

else:
    st.info("ℹ️ No data loaded yet. Click the sidebar button to run the pre-freeze pipeline.")
