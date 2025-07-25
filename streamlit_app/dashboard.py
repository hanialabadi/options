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

# ✅ Try core imports (Phases 1–3.5)
try:
    from core.phase1_clean import phase1_load_and_clean_raw_v2
    from core.phase2_parse import phase_parse_symbols, phase21_strategy_tagging
    from core.phase3_pcs_score import (
        calculate_pcs, calculate_ivhv_gap, calculate_skew_and_kurtosis
    )
    from core.phase3_5_freeze_fields import phase35_fill_freeze_fields
    st.success("✅ Core modules (Phases 1–3.5) imported.")
except Exception as e:
    st.error(f"❌ Import error: {e}")
    st.stop()

# === 🧠 Session state check
if "df" not in st.session_state:
    st.session_state["df"] = pd.DataFrame()

# === 📂 Sidebar: Load raw CSV and run pipeline
st.sidebar.header("📁 Load Raw Data")
if st.sidebar.button("📂 Load & Run Pre-Freeze Pipeline"):
    try:
        input_path = "/Users/haniabadi/Documents/Windows/Positions_Account_.csv"
        st.info(f"⏳ Loading: {input_path}")
        df, _ = phase1_load_and_clean_raw_v2(input_path=input_path)

        df = phase_parse_symbols(df)
        df = phase21_strategy_tagging(df)
        df = calculate_pcs(df)
        df = calculate_ivhv_gap(df)
        df = calculate_skew_and_kurtosis(df)
        df = phase35_fill_freeze_fields(df)

        st.session_state["df"] = df
        st.success(f"✅ Pre-freeze pipeline complete. Rows: {len(df)}")
    except Exception as e:
        st.error(f"❌ Error running pipeline: {e}")

# === 📊 Main Display
df = st.session_state.get("df", pd.DataFrame())
if not df.empty:
    st.subheader("📊 Pre-Freeze Snapshot Preview")
    st.write(f"🧮 Showing all {df.shape[0]} trades")
    st.dataframe(df, use_container_width=True)

    st.download_button("📥 Download Full Snapshot CSV", df.to_csv(index=False), file_name="pre_freeze_snapshot.csv")

    # === 📊 Sidebar Filters
    st.sidebar.markdown("---")
    st.sidebar.subheader("🔍 Filters")

    pcs_min = st.sidebar.slider("PCS Score ≥", 60, 100, 75)
    dte_max = st.sidebar.slider("Max DTE", 0, 60, 30)
    symbol_filter = st.sidebar.text_input("Filter Symbol (optional)", "").upper()
    multi_leg_only = st.sidebar.checkbox("🔍 Show only Multi-leg Strategies", value=False)

    # === 📊 Strategy Breakdown Summary
    st.sidebar.markdown("---")
    st.sidebar.subheader("📊 Strategy Breakdown")
    if "Strategy" in df.columns:
        strategy_counts = df["Strategy"].value_counts()
        st.sidebar.dataframe(strategy_counts)

    # === 📄 Apply filters
    filtered_df = df.copy()
    if "PCS" in df.columns:
        filtered_df = filtered_df[filtered_df["PCS"] >= pcs_min]
    if "DTE" in df.columns:
        filtered_df = filtered_df[filtered_df["DTE"] <= dte_max]
    if symbol_filter:
        filtered_df = filtered_df[filtered_df["Symbol"].str.contains(symbol_filter)]
    if multi_leg_only and "Type" in df.columns:
        filtered_df = filtered_df[filtered_df["Type"] == "Multi-leg"]

    st.write(f"🧮 Showing {filtered_df.shape[0]} of {df.shape[0]} trades (Filtered)")
    st.dataframe(filtered_df, use_container_width=True)

    st.download_button("📥 Download Filtered CSV", filtered_df.to_csv(index=False), file_name="pre_freeze_filtered.csv")

else:
    st.info("ℹ️ No data loaded yet. Click the sidebar button to run the pre-freeze pipeline.")

# === 🧊 Frozen Snapshot Viewer
st.sidebar.markdown("---")
st.sidebar.header("🧊 Frozen Snapshot Viewer")

snapshot_dir = "/Users/haniabadi/Documents/Windows/Optionrec/drift"

# List available frozen files
snapshot_files = sorted([
    f for f in os.listdir(snapshot_dir)
    if f.startswith("positions_") and f.endswith(".csv")
], reverse=True)

selected_file = st.sidebar.selectbox("📁 Select snapshot to view", snapshot_files if snapshot_files else ["<None>"])

if selected_file != "<None>":
    snapshot_path = os.path.join(snapshot_dir, selected_file)

    try:
        df_frozen = pd.read_csv(snapshot_path)
        st.subheader(f"🧊 Snapshot: {selected_file}")
        st.write(f"📅 Timestamp: {df_frozen['Snapshot Timestamp'].iloc[0]}" if 'Snapshot Timestamp' in df_frozen.columns else "")
        st.write(f"🧮 Rows: {df_frozen.shape[0]} | Columns: {df_frozen.shape[1]}")
        st.dataframe(df_frozen, use_container_width=True)

        st.download_button("⬇️ Download Snapshot CSV", df_frozen.to_csv(index=False), file_name=selected_file)

        # === Tier Breakdown
        if "PCS_Tier" in df_frozen.columns:
            st.markdown("### 🏷️ PCS Tier Breakdown")
            st.dataframe(df_frozen["PCS_Tier"].value_counts().rename("Count").reset_index().rename(columns={"index": "Tier"}))

        # === Strategy Breakdown
        if "Strategy" in df_frozen.columns:
            st.markdown("### 🧩 Strategy Summary")
            st.dataframe(df_frozen["Strategy"].value_counts().rename("Count").reset_index().rename(columns={"index": "Strategy"}))

        # === PCS Stats
        if "PCS" in df_frozen.columns:
            st.markdown("### 📈 PCS Statistics")
            st.write("**Average PCS:**", round(df_frozen["PCS"].mean(), 2))
            st.write("**Max PCS:**", round(df_frozen["PCS"].max(), 2))
            st.write("**Min PCS:**", round(df_frozen["PCS"].min(), 2))

    except Exception as e:
        st.error(f"❌ Failed to load snapshot: {e}")
else:
    st.info("ℹ️ Select a snapshot file from the sidebar.")
