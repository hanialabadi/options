import os
import sys
import json
from datetime import datetime
import pandas as pd
import streamlit as st

# === ⛳ Setup Project Paths ===
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
sys.path.append(PROJECT_ROOT)

# === 🧠 Core Pipeline Imports ===
from core.phase1_clean import phase1_load_and_clean_raw_v2
from core.phase2_parse import phase2_parse_symbols, phase21_strategy_tagging
from core.phase3_pcs_score import calculate_pcs, calculate_ivhv_gap, calculate_skew_and_kurtosis
from core.phase3_5_freeze_fields import phase3_5_fill_freeze_fields
from core.phase6_freeze_and_archive import phase6_freeze_and_archive
from core.phase6_5 import phase6_5_inject_derived_fields
from core.phase7_drift_engine import run_phase7_drift_engine
from utils.load_master_snapshot import load_master_snapshot
from core.freeze_leg_status import evaluate_leg_status

# === Streamlit Config ===
st.set_page_config(page_title="🔍 Step Debugger", layout="wide")
st.title("🧠 Options Trade Pipeline Debugger")

# === Session Init ===
for key in ["df", "df_master", "pipeline_log"]:
    if key not in st.session_state:
        st.session_state[key] = pd.DataFrame() if "df" in key else []

# === Logging Helper ===
def log_step(step, success, error=None):
    log_entry = {
        "step": step,
        "status": "✅" if success else "❌",
        "timestamp": datetime.now().strftime("%H:%M:%S"),
        "error": error or ""
    }
    st.session_state.pipeline_log.append(log_entry)
    with open("phase_status.json", "w") as f:
        json.dump(log_entry, f)
    if success:
        st.success(f"{step} ✅ Completed at {log_entry['timestamp']}")
    else:
        st.error(f"{step} ❌ Failed: {error}")

# === Config Paths ===
input_path = "/Users/haniabadi/Documents/Windows/Positions_Account_.csv"
snapshot_dir = "/Users/haniabadi/Documents/Windows/Optionrec/drift"
master_path = "/Users/haniabadi/Documents/Windows/Optionrec/active_master.csv"
legs_dir = "/Users/haniabadi/Documents/Windows/Optionrec/legs"

# === 📜 Pipeline Log Viewer ===
with st.sidebar.expander("📜 Pipeline Log"):
    for entry in st.session_state.pipeline_log:
        st.write(f"{entry['timestamp']} | {entry['status']} {entry['step']}")
        if entry['error']:
            st.code(entry['error'], language="text")

# === 🧠 Memory Debug ===
with st.expander("🧪 Memory Debug Console", expanded=False):
    if "df" in st.session_state:
        st.code(f"df: {st.session_state.df.shape}\nCols: {list(st.session_state.df.columns)}")
    if "df_master" in st.session_state:
        st.code(f"df_master: {st.session_state.df_master.shape}\nCols: {list(st.session_state.df_master.columns)}")

# === 📂 Show Raw CSV Preview ===
if st.sidebar.checkbox("📂 Show Raw CSV Preview"):
    try:
        df_raw = pd.read_csv(input_path)
        st.write(f"🗃️ Raw file preview ({len(df_raw)} rows):")
        st.dataframe(df_raw.head(), use_container_width=True)
    except Exception as e:
        st.error(f"Could not read file: {e}")

# === ✅ Step 1: Clean + Load ===
if st.sidebar.button("1️⃣ Load Raw CSV"):
    with st.spinner("🔄 Loading and cleaning raw data..."):
        try:
            df, _ = phase1_load_and_clean_raw_v2(input_path, snapshot_dir)
            st.session_state.df = df
            log_step("Phase 1 Load", True)
            st.write("📥 Loaded Data:", df.shape)
            st.dataframe(df.head(), use_container_width=True)
        except Exception as e:
            log_step("Phase 1 Load", False, str(e))

# === ✅ Step 2: Parse Symbols ===
if st.sidebar.button("2️⃣ Parse Option Symbols"):
    with st.spinner("🔍 Parsing option symbols..."):
        try:
            df = phase2_parse_symbols(st.session_state.df)
            st.session_state.df = df
            log_step("Phase 2 Parse", True)
            st.dataframe(df.head(), use_container_width=True)
        except Exception as e:
            log_step("Phase 2 Parse", False, str(e))

# === ✅ Step 3: Tag + Score PCS ===
if st.sidebar.button("3️⃣ Strategy + PCS Scoring"):
    with st.spinner("📊 Tagging strategies and calculating PCS..."):
        try:
            df = phase21_strategy_tagging(st.session_state.df)
            df = calculate_pcs(df)
            df = calculate_ivhv_gap(df)
            df = calculate_skew_and_kurtosis(df)
            st.session_state.df = df
            log_step("Phase 3 PCS", True)
            st.dataframe(df.head(), use_container_width=True)
        except Exception as e:
            log_step("Phase 3 PCS", False, str(e))

# === ✅ Step 4: Freeze + Archive ===
if st.sidebar.button("4️⃣ Freeze + Archive"):
    with st.spinner("📦 Freezing positions and archiving to master..."):
        try:
            import traceback
            df_input = st.session_state.df
            required_cols = [
                "TradeID", "Underlying", "Strategy", "PCS", "Vega", "Gamma", "Theta",
                "Delta", "Expiration", "Strike", "OptionType", "DTE"
            ]
            if os.path.exists(master_path):
                df_master_current = pd.read_csv(master_path)
                if not all(col in df_master_current.columns for col in required_cols):
                    df_master_current = pd.DataFrame(columns=required_cols)
            else:
                df_master_current = pd.DataFrame(columns=required_cols)

            df_master = phase6_freeze_and_archive(df=df_input, df_master=df_master_current)
            df_master = evaluate_leg_status(df_master, legs_dir)
            st.session_state.df_master = df_master
            df_master.to_csv(master_path, index=False)
            log_step("Phase 6 Freeze", True)
            st.dataframe(df_master.head(), use_container_width=True)

        except Exception as e:
            tb = traceback.format_exc()
            log_step("Phase 6 Freeze", False, f"{str(e)}\nTRACE:\n{tb}")
            st.error(f"❌ Phase 6 Freeze failed: {e}")
            st.code(tb, language="python")

# === ✅ Step 5: Inject Derived Fields ===
if st.sidebar.button("5️⃣ Inject ROI + Days Held"):
    with st.spinner("➕ Injecting derived fields..."):
        try:
            df_master = phase6_5_inject_derived_fields(st.session_state.df_master, save_path=master_path)
            st.session_state.df_master = df_master
            log_step("Phase 6.5 Inject", True)
            st.dataframe(df_master.head(), use_container_width=True)
        except Exception as e:
            log_step("Phase 6.5 Inject", False, str(e))

# === ✅ Step 6: Drift Engine ===
if st.sidebar.button("6️⃣ Run Drift Engine"):
    with st.spinner("📡 Running Drift Engine..."):
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            export_csv = f"/Users/haniabadi/Documents/Windows/Optionrec/drift_audits/drift_debug_{timestamp}.csv"
            df_drift = run_phase7_drift_engine(drift_dir=snapshot_dir, export_csv=export_csv, update_master=True)
            st.session_state.df_master = df_drift
            log_step("Phase 7 Drift Engine", True)
            st.dataframe(df_drift.head(), use_container_width=True)
        except Exception as e:
            log_step("Phase 7 Drift Engine", False, str(e))

# === 🔍 Debug Trade ===
st.sidebar.markdown("---")
st.sidebar.subheader("🔍 Debug Trade")
trade_id = st.sidebar.text_input("Enter TradeID")
if st.sidebar.button("🧪 Show Trade Details"):
    row = st.session_state.df_master[st.session_state.df_master.TradeID == trade_id]
    if not row.empty:
        st.write(f"### Trade Snapshot for {trade_id}")
        st.dataframe(row.T)
        if "PCS_Live" in row and "PCS_Entry" in row:
            st.metric("PCS Drift", float(row.PCS_Live.values[0]) - float(row.PCS_Entry.values[0]))
        tag_cols = [col for col in row.columns if "Rec" in col or "Tag" in col or "Exit" in col or "Trigger" in col]
        if tag_cols:
            st.write("### Tags + Exit")
            st.dataframe(row[tag_cols].T)
    else:
        st.warning(f"⚠️ TradeID {trade_id} not found.")

# === 📊 Display Snapshot Table ===
if not st.session_state.df_master.empty:
    st.subheader("📘 Final Snapshot Table")
    st.dataframe(st.session_state.df_master.sort_values("PCS", ascending=False), use_container_width=True)