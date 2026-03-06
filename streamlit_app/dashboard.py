import sys
import os
from pathlib import Path

# === DEV-MODE HARDENING: Path Priority & Bytecode Prevention ===
# RAG: Determinism. Ensure working directory always has priority over site-packages.
sys.path.insert(0, os.getcwd())

# Programmatic Bytecode Prevention (Dev Mode Only)
if os.getenv("DEV_MODE") == "1":
    sys.dont_write_bytecode = True

# === Bootstrap: Add project root to Python path BEFORE any core imports ===
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import os
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Re-import shared config safely after sys.path is set
from core.shared.data_contracts.config import PROJECT_ROOT as CORE_PROJECT_ROOT
from core.shared.data_contracts.config import SCAN_OUTPUT_DIR

# Also ensure current working directory and app directory are in path
cwd = os.getcwd()
if cwd not in sys.path:
    sys.path.append(cwd)
app_dir = str(Path(__file__).resolve().parent)
if app_dir not in sys.path:
    sys.path.append(app_dir)

# === Import Modular Views ===
import perception_view
import manage_view
import scan_view
import risk_view
import audit_view

# === Management Context Switch ===
MANAGEMENT_SAFE_MODE = True
SCAN_MODE = False

REQUIRED_COLUMNS = [
    "TradeID", "Underlying_Ticker", "Strategy",
    "GreekDominance_State", "VolatilityState_State", "AssignmentRisk_State",
    "RegimeStability_State", "Structural_Data_Complete", "Resolution_Reason",
    "Decision_State", "Rationale", "Doctrine_Source",
    "run_id", "Snapshot_TS", "Schema_Hash"
]

def load_data():
    """
    Load authoritative data from the Management Truth Layer.
    PERFORMS ZERO COMPUTATION.
    """
    latest_file = Path("core/management/outputs/positions_latest.csv")
    
    if not latest_file.exists():
        st.error(f"❌ AUTHORITATIVE DATA MISSING: {latest_file} not found. Run the management pipeline first.")
        st.stop()
        
    df = pd.read_csv(latest_file)
    
    # --- SCHEMA VALIDATION GATE ---
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        st.error("🚨 UI CONTRACT VIOLATION: Authoritative data is missing required columns.")
        st.write(f"**Missing Columns:** {missing}")
        st.info("The dashboard has stopped rendering to prevent displaying unverified or stale state.")
        st.stop()
    
    # Ensure Snapshot_TS is datetime
    df['Snapshot_TS'] = pd.to_datetime(df['Snapshot_TS'])
        
    return df

def sanitize_for_arrow(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sanitize DataFrame for Arrow serialization (fixes Streamlit display errors).
    """
    if df is None or df.empty:
        return df
        
    df = df.copy()
    
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (list, dict, tuple))).any():
            df[col] = df[col].astype(str)
            continue

        dtype = df[col].dtype
        if str(dtype) == 'string':
            df[col] = df[col].astype('object')
        elif dtype == 'object':
            inferred = pd.api.types.infer_dtype(df[col])
            if inferred == 'mixed' or inferred == 'mixed-integer':
                df[col] = df[col].astype(str)
            elif inferred in ['integer', 'floating']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        elif isinstance(dtype, pd.DatetimeTZDtype):
            df[col] = df[col].dt.tz_localize(None)
            
    return df

st.set_page_config(
    page_title="Options Intelligence Platform",
    layout="wide"
)

# === Startup Validation ===
# validate_cycle1_ledger() # Removed: Dashboard is read-only

# === Schwab Auth Status Banner (sidebar, always visible) ===
try:
    from core.shared.auth.schwab_tokens import load_tokens
    import time as _time

    _tokens, _auth_status = load_tokens()

    if _auth_status == "OK":
        _expires_at = _tokens.get("expires_at", 0) if _tokens else 0
        _mins_left = max(0, int((_expires_at - _time.time()) / 60))
        _refresh_at = _tokens.get("refresh_expires_at", 0) if _tokens else 0
        _refresh_days = max(0, int((_refresh_at - _time.time()) / 86400)) if _refresh_at else 0
        if _refresh_days <= 1:
            st.sidebar.warning(
                f"⚠️ **Schwab: token expiring soon**  \n"
                f"Access token valid ~{_mins_left}m · Refresh window: **{_refresh_days}d left**  \n"
                f"Re-auth now before it lapses:  \n"
                f"`python auth_schwab_minimal.py`"
            )
        else:
            st.sidebar.success(
                f"🟢 **Schwab: connected**  \n"
                f"Access ~{_mins_left}m · Refresh {_refresh_days}d left"
            )

    elif _auth_status == "EXPIRED":
        # Access token expired but refresh token may still be valid — auto-refresh is possible
        _refresh_at = _tokens.get("refresh_expires_at", 0) if _tokens else 0
        _refresh_days = max(0, int((_refresh_at - _time.time()) / 86400)) if _refresh_at else 0
        if _refresh_days > 0:
            st.sidebar.warning(
                f"🟡 **Schwab: access token expired**  \n"
                f"Refresh token valid {_refresh_days}d · Next pipeline run will auto-refresh.  \n"
                f"If pipeline fails, run manually:  \n"
                f"`python auth_schwab_minimal.py`"
            )
        else:
            st.sidebar.error(
                "🔴 **Schwab: token expired — manual re-auth required**  \n"
                "Both access and refresh tokens have expired.  \n"
                "Price history and scan data will fail until renewed.  \n"
                "**Run:** `python auth_schwab_minimal.py`"
            )

    elif _auth_status == "REFRESH_EXPIRED":
        st.sidebar.error(
            "🔴 **Schwab: session expired — manual re-auth required**  \n"
            "The 7-day OAuth refresh window has closed. Auto-refresh is not possible.  \n"
            "Price history, scan, and management runs will fail.  \n"
            "**Run now:** `python auth_schwab_minimal.py`"
        )

    elif _auth_status == "MISSING":
        st.sidebar.error(
            "🔴 **Schwab: no token file found**  \n"
            f"Expected: `~/.schwab/tokens.json`  \n"
            "**Run:** `python auth_schwab_minimal.py` to authenticate."
        )

    else:  # ERROR
        st.sidebar.warning(
            "⚠️ **Schwab: token file unreadable**  \n"
            "Could not parse `~/.schwab/tokens.json`.  \n"
            "**Run:** `python auth_schwab_minimal.py` to re-authenticate."
        )

except Exception as _auth_err:
    st.sidebar.warning(f"⚠️ Schwab auth check failed: {_auth_err}")

# === DEV-MODE DIAGNOSTICS ===
if os.getenv("DEV_MODE") == "1":
    import core
    st.sidebar.warning("🛠️ DEV_MODE ACTIVE")
    if st.sidebar.checkbox("Show Module Paths"):
        st.sidebar.code(f"Core: {core.__file__}\nCWD: {os.getcwd()}")

# === Initialize session state ===
if "view" not in st.session_state:
    st.session_state.view = "home"

if "debug_mode" not in st.session_state:
    st.session_state.debug_mode = False

if "audit_mode" not in st.session_state:
    st.session_state.audit_mode = False

if "intraday_refresh" not in st.session_state:
    st.session_state.intraday_refresh = False

if "scan_running" not in st.session_state:
    st.session_state.scan_running = False

if "pipeline_running" not in st.session_state:
    st.session_state.pipeline_running = False

if "pipeline_run_metadata" not in st.session_state:
    st.session_state.pipeline_run_metadata = {
        "last_run": None,
        "status": "idle",
        "error": None,
        "ready_now_count": 0
    }

# === Navigation Helper ===
def set_view(view_name):
    logger.info(f"Attempting to set view to: {view_name}")
    st.session_state.view = view_name
    st.rerun()

# ========================================
# ROUTER
# ========================================

logger.info(f"Current session state view: {st.session_state.view}")
if st.session_state.view == "home":
    st.title("📊 Options Intelligence Platform")
    st.markdown(
        """
        Welcome. Choose a workflow to begin:

        - **Scan** → Discover new high-conviction trade candidates
        - **Upload Positions** → Ingest a Fidelity CSV to update the ledger
        - **Position Monitor** → Live view of active positions, Greeks, DTE
        - **Risk & Structure** → P/L, assignment risk, concentration, cost basis
        """
    )

    st.divider()

    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        if st.button("🔍 Scan Market", width='stretch'):
            logger.info("Scan Market button clicked.")
            set_view("scan")

    with col2:
        if st.button("📥 Upload Positions", width='stretch'):
            logger.info("Upload Positions button clicked.")
            set_view("perception")

    with col3:
        if st.button("📋 Position Monitor", width='stretch'):
            logger.info("Position Monitor button clicked.")
            set_view("manage")

    with col4:
        if st.button("📊 Risk & Structure", width='stretch'):
            logger.info("Risk & Structure button clicked.")
            set_view("risk")

    with col5:
        if st.button("📚 Doctrine Audit", width='stretch'):
            logger.info("Doctrine Audit button clicked.")
            set_view("audit")

elif st.session_state.view == "scan":
    logger.info("Rendering scan_view.")
    scan_view.render_scan_view(CORE_PROJECT_ROOT, SCAN_OUTPUT_DIR, sanitize_for_arrow, set_view)

elif st.session_state.view == "perception":
    logger.info("Rendering perception_view.")
    if st.button("← Back to Home"):
        logger.info("Back to Home button clicked from perception_view.")
        set_view("home")
    perception_view.render_perception_view(CORE_PROJECT_ROOT, sanitize_for_arrow)

elif st.session_state.view == "manage":
    logger.info("Rendering manage_view.")
    manage_view.render_manage_view(CORE_PROJECT_ROOT, sanitize_for_arrow, set_view)

elif st.session_state.view == "risk":
    logger.info("Rendering risk_view.")
    if st.button("← Back to Home"):
        logger.info("Back to Home button clicked from risk_view.")
        set_view("home")
    risk_view.render_risk_view(CORE_PROJECT_ROOT, sanitize_for_arrow)

elif st.session_state.view == "audit":
    logger.info("Rendering audit_view.")
    audit_view.render_audit_view(CORE_PROJECT_ROOT, set_view)
