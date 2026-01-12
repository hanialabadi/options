import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import sys
import os
import logging
import subprocess
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# === Add parent directory to Python path ===
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Also ensure current working directory is in path if it's the root
cwd = os.getcwd()
if cwd not in sys.path and (Path(cwd) / "core").exists():
    sys.path.append(cwd)

# === Management Context Switch ===
# Hard-coded for safety in dashboard
MANAGEMENT_SAFE_MODE = True
SCAN_MODE = False


def render_drift_analysis(df):
    """
    Phase 7C: Render Drift Analysis visualization.
    Provides human-readable facts about metric migration across windows.
    """
    st.header("🔄 Drift Analysis (Facts Only)")
    st.caption("Observational data showing how Greeks and IV have migrated relative to entry and historical windows.")

    if df.empty:
        st.warning("No data available for drift analysis.")
        return

    # 1. Stability & Sufficiency Overview
    col1, col2, col3 = st.columns(3)
    with col1:
        avg_stability = df['delta_drift_stability'].mean() if 'delta_drift_stability' in df.columns else 0
        st.metric("Avg Delta Stability (σ)", f"{avg_stability:.4f}")
    with col2:
        sufficient_count = df['Drift_History_Sufficient'].sum() if 'Drift_History_Sufficient' in df.columns else 0
        st.metric("Sufficient History", f"{sufficient_count}/{len(df)}")
    with col3:
        avg_snapshots = df['snapshot_count'].mean() if 'snapshot_count' in df.columns else 0
        st.metric("Avg Snapshots/Trade", f"{avg_snapshots:.1f}")

    # 2. Windowed Comparison Table
    st.subheader("Windowed Migration")
    
    # Select a trade to inspect
    selected_trade = st.selectbox("Select TradeID to inspect drift:", df['TradeID'].unique())
    trade_df = df[df['TradeID'] == selected_trade]
    
    # Display windowed facts for key metrics
    metrics = ['Delta', 'Gamma', 'IV', 'Price']
    windows = ['1D', '3D', '10D', 'Structural']
    
    drift_data = []
    for m in metrics:
        row = {'Metric': m}
        for w in windows:
            col_name = f"{m}_Drift_{w}" if w != 'Structural' or m != 'Price' else "Price_Drift_Structural"
            if m == 'IV' and w == 'Structural': col_name = "IV_Drift_Structural"
            
            val = trade_df[col_name].iloc[0] if col_name in trade_df.columns else np.nan
            row[w] = f"{val:+.4f}" if pd.notna(val) else "N/A"
        drift_data.append(row)
    
    st.table(pd.DataFrame(drift_data))

    # 3. Smoothing & Acceleration (Facts Only)
    st.subheader("Signal Quality (Smoothed)")
    smooth_cols = ['TradeID', 'delta_drift_sma_3', 'delta_drift_accel', 'delta_drift_stability', 'Drift_History_Sufficient']
    available_smooth = [c for c in smooth_cols if c in df.columns]
    
    if len(available_smooth) > 1:
        st.dataframe(df[available_smooth], width="stretch")
    else:
        st.info("Smoothing metrics require at least 3 snapshots in DuckDB.")


def render_wait_table(df_wait):
    """
    Render the WAIT strategies table with diagnostic info.
    """
    if df_wait.empty:
        st.success("No WAIT strategies — all clear.")
        return

    st.info("These strategies are structurally valid but deferred due to missing data.")

    cols_to_show = [
        "Ticker",
        "Strategy_Name",
        "IV_Maturity_State",
        "acceptance_status",
        "Acceptance_Reason",
        "Missing_Fields",
        "PCS_Score",
        "Expression_Tier",
        "Timeframe"
    ]

    existing_cols = [c for c in cols_to_show if c in df_wait.columns]

    st.subheader("⏸️ WAIT — Not Rejected, Just Not Ready")
    st.caption(
        "WAIT strategies meet structural requirements but are paused due to missing IV Rank / Skew history. "
        "They will auto-promote once data maturity reaches 120 trading days."
    )

    st.dataframe(
        df_wait[existing_cols],
        width="stretch"
    )
    
    if "Missing_Fields" in df_wait.columns:
        st.caption(
            "🧩 WAIT strategies typically unlock when IV Rank, Skew, or Percentile data matures (120+ days)."
        )


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


# === Imports for Scan Engine ===
from core.scan_engine.pipeline import run_full_scan_pipeline
from core.scan_engine import resolve_snapshot_path
from core.data_layer.market_stress_detector import get_market_stress_summary
from core.scraper.ivhv_bootstrap import get_today_snapshot_path
from core.data_layer.ivhv_availability_loader import load_iv_availability

# === Imports for Management Engine ===
from core.phase1_clean import phase1_load_and_clean_positions
from core.phase2_parse import phase2_run_all
from core.phase3_enrich import run_phase3_enrichment
from core.phase3_enrich import (
    tag_strategy_metadata, compute_breakeven, compute_moneyness, tag_earnings_flags
)
from core.phase3_enrich.pcs_score import calculate_pcs
from core.phase3_enrich.liquidity import enrich_liquidity
from core.phase3_enrich.skew_kurtosis import calculate_skew_and_kurtosis
from core.phase4_snapshot import save_clean_snapshot
from core.phase5_portfolio_limits import check_portfolio_limits, analyze_correlation_risk, get_persona_limits
from core.phase3_enrich.compute_drift_metrics import compute_drift_metrics, classify_drift_severity
from core.phase7_recommendations.load_chart_signals import load_chart_signals, merge_chart_signals
from core.phase7_recommendations.exit_recommendations import compute_exit_recommendations, prioritize_recommendations

st.set_page_config(
    page_title="Options Intelligence Platform",
    layout="wide"
)

# === Initialize session state ===
if "view" not in st.session_state:
    st.session_state.view = "home"

if "pipeline_run_metadata" not in st.session_state:
    st.session_state.pipeline_run_metadata = {
        "last_run": None,
        "status": "idle",
        "error": None,
        "ready_now_count": 0
    }

# === Navigation Helper ===
def set_view(view_name):
    st.session_state.view = view_name
    st.rerun()

def get_snapshot_info(path: str):
    """
    Extract metadata and quality metrics from a snapshot file.
    """
    if not path or not os.path.exists(path):
        return None

    try:
        df = pd.read_csv(path)
        mod_time = datetime.fromtimestamp(os.path.getmtime(path))

        # Calculate IV Coverage
        iv_col = 'IV_30_D_Call' if 'IV_30_D_Call' in df.columns else 'iv_30d'
        iv_populated = df[iv_col].notna().sum() if iv_col in df.columns else 0
        coverage = (iv_populated / len(df) * 100) if len(df) > 0 else 0

        # Calculate IV History
        if 'iv_history_days' in df.columns:
            iv_history = df['iv_history_days'].max()
        else:
            # Fallback: Try to load availability from authoritative source
            try:
                # Ensure we have 'Ticker' column for the loader
                if 'Ticker' not in df.columns and 'Symbol' in df.columns:
                    df = df.rename(columns={'Symbol': 'Ticker'})
                
                if 'Ticker' in df.columns:
                    df_with_avail = load_iv_availability(df)
                    iv_history = df_with_avail['iv_history_days'].max()
                else:
                    iv_history = 0
            except Exception as e:
                logger.warning(f"Failed to load IV availability fallback: {e}")
                iv_history = 0

        return {
            'path': path,
            'filename': os.path.basename(path),
            'timestamp': mod_time,
            'tickers': len(df),
            'iv_coverage': coverage,
            'iv_history': iv_history,
            'is_stale': (datetime.now() - mod_time).total_seconds() > 86400  # > 24h
        }
    except Exception as e:
        logger.error(f"Error reading snapshot info: {e}")
        return None

# ========================================
# HOME VIEW
# ========================================
if st.session_state.view == "home":
    st.title("📊 Options Intelligence Platform")
    st.markdown(
        """
        Welcome. Choose a workflow to begin:

        - **Scan** → Discover new trade opportunities  
        - **Manage** → Monitor and manage existing positions
        """
    )

    st.divider()

    col1, col2 = st.columns(2)

    with col1:
        if st.button("🔍 Scan Market", width='stretch'):
            set_view("scan")

    with col2:
        if st.button("🧪 Manage Positions", width='stretch'):
            set_view("manage")

# ========================================
# SCAN VIEW
# ========================================
elif st.session_state.view == "scan":
    if st.button("← Back to Home"):
        set_view("home")
    
    st.title("🔍 Market Scan - Full Pipeline Orchestration")
    st.markdown("Execute the complete pipeline to discover and evaluate trade opportunities.")

    # Global Guardrails Display
    if 'pipeline_results' in st.session_state:
        results = st.session_state['pipeline_results']
        
        # 1. Market Stress Banner
        if 'market_stress' in results:
            stress = results['market_stress']
            level = stress['level']
            median_iv = stress['median_iv']
            
            if level == 'RED':
                st.error(f"🛑 **MARKET STRESS RED:** All trades halted. Median IV: {median_iv:.1f}")
            elif level == 'YELLOW':
                st.warning(f"⚠️ **MARKET STRESS YELLOW:** Elevated volatility. Median IV: {median_iv:.1f}")
            elif level == 'UNKNOWN':
                st.info(f"❓ **MARKET STRESS UNKNOWN:** Insufficient IV data to determine stress level.")
            else:
                st.success(f"✅ **MARKET STRESS GREEN:** Normal conditions. Median IV: {median_iv:.1f}")

        # 2. Market Regime Info
        if 'regime_info' in results:
            regime = results['regime_info']
            with st.expander(f"📊 Market Regime: {regime['regime']} (Confidence: {regime['confidence']})"):
                st.write(regime['explanation'])
                st.info(f"Expected READY_NOW Range: {regime['expected_ready_range'][0]} - {regime['expected_ready_range'][1]}")
    
    st.divider()
    
    # ========================================
    # FILE UPLOAD & CONFIGURATION
    # ========================================
    with st.sidebar:
        st.header("📂 Data Source")
        
        upload_method = st.radio(
            "Choose input method:",
            ["Auto (Authoritative)", "Use File Path", "Upload CSV"]
        )
        
        uploaded_file_obj = None
        explicit_snapshot_path_input = None
        
        if upload_method == "Auto (Authoritative)":
            try:
                explicit_snapshot_path_input = resolve_snapshot_path()
            except:
                explicit_snapshot_path_input = None
        
        elif upload_method == "Upload CSV":
            uploaded_file_obj = st.file_uploader(
                "Upload IV/HV Snapshot CSV",
                type=['csv'],
                help="Upload Fidelity IV/HV snapshot export"
            )
        
        else:  # Use File Path
            explicit_snapshot_path_input = st.text_input(
                "IV/HV Snapshot Path",
                value=os.getenv('FIDELITY_SNAPSHOT_PATH', ''),
                help="Full path to IV/HV CSV file"
            )
        
        st.divider()
        st.header("⚙️ Pipeline Parameters")
        account_balance = st.number_input("Account Balance ($)", min_value=1000.0, value=100000.0, step=1000.0)
        max_portfolio_risk = st.slider("Max Portfolio Risk (%)", min_value=0.05, max_value=0.50, value=0.20, step=0.05)
        sizing_method = st.selectbox("Sizing Method", ['volatility_scaled', 'fixed_fractional', 'kelly', 'equal_weight'])
        
        st.divider()
        st.header("🛠️ Developer Tools")
        debug_mode = st.checkbox("🧪 Enable Debug Mode", value=False, help="Surface silent failures, degradations, and swallowed exceptions. In Scan mode, this also overrides the universe with DEBUG_TICKERS (AAPL, AMZN, NVDA).")

    # ========================================
    # STEP 0: SCHWAB LIVE SNAPSHOT
    # ========================================
    st.header("🚀 Step 0: Schwab Live Snapshot")
    st.markdown("""
    **Purpose:** Fetch real-time IV/HV data directly from Schwab API.  
    **Output:** Fresh snapshot in `data/snapshots/ivhv_snapshot_live_*.csv`
    """)

    col_step0_1, col_step0_2 = st.columns([3, 1])
    with col_step0_2:
        discovery_mode = st.toggle(
            "🔭 Discovery Mode", 
            value=True, 
            help="Lightweight Scan: Only fetch IV for tickers with high HV or momentum. Drastically reduces API noise."
        )

    with col_step0_1:
        if st.button("📡 Generate Schwab Live Snapshot", type="primary", width='stretch'):
            try:
                with st.spinner("Fetching live data from Schwab..."):
                    from core.scan_engine.step0_schwab_snapshot import main as run_step0
                    df_live = run_step0(fetch_iv=True, discovery_mode=discovery_mode)
                    st.success(f"✅ Live snapshot generated with {len(df_live)} tickers!")
                    if discovery_mode:
                        iv_count = df_live['iv_30d'].notna().sum()
                        st.info(f"🔭 Discovery Mode: Harvested IV for {iv_count}/{len(df_live)} high-interest tickers.")
                    st.rerun()
            except Exception as e:
                st.error(f"❌ Schwab Snapshot failed: {e}")
                st.exception(e)

    st.divider()

    # ========================================
    # DATA PROVENANCE PANEL (TRUST BLOCKER)
    # ========================================
    st.header("📊 Data Provenance & Quality")
    
    prov_path = None
    if upload_method == "Upload CSV" and uploaded_file_obj:
        temp_p = Path("./temp_prov_check.csv")
        with open(temp_p, "wb") as f:
            f.write(uploaded_file_obj.getbuffer())
        prov_path = str(temp_p)
    else:
        prov_path = explicit_snapshot_path_input

    info = get_snapshot_info(prov_path)
    
    execution_blocked = False
    block_reason = ""

    if info:
        col_p1, col_p2, col_p3 = st.columns(3)
        
        with col_p1:
            st.metric("Data Freshness", info['timestamp'].strftime('%H:%M %Z'))
            if info['is_stale']:
                st.error("❌ STALE DATA (>24h)")
                execution_blocked = True
                block_reason = "Snapshot is older than 24 hours."
            else:
                st.success("✅ FRESH")
        
        with col_p2:
            st.metric("IV Coverage", f"{info['iv_coverage']:.1f}%")
            if info['iv_coverage'] < 40:
                st.error("❌ CRITICAL FAILURE (<40%)")
                execution_blocked = True
                block_reason = "Insufficient IV coverage for analysis."
            elif info['iv_coverage'] < 60:
                st.error("❌ HARD BLOCK (<60%)")
                execution_blocked = True
                block_reason = "IV coverage below minimum safety threshold."
            elif info['iv_coverage'] < 80:
                st.warning("⚠️ WARNING (<80%)")
            else:
                st.success("✅ ADEQUATE")
        
        with col_p3:
            st.metric("IV History", f"{int(info['iv_history'])} / 120 days")
            if info['iv_history'] < 120:
                st.warning("🔶 ACCUMULATING")
                # Do not block execution for history; Step 12 will handle downgrades
                # execution_blocked = True 
                # block_reason = "Insufficient IV history (need 120 days for IV Rank)."
            else:
                st.success("✅ MATURE")
        
        st.caption(f"📄 Source: `{info['filename']}` | 🎯 Tickers: {info['tickers']}")
        
        if execution_blocked:
            st.error(f"🛑 **EXECUTION BLOCKED:** {block_reason}")
        else:
            st.success("🟢 **EXECUTION ALLOWED:** Data quality meets safety thresholds.")
    else:
        st.warning("⚠️ No valid snapshot selected. Please fetch data or provide a path.")
        execution_blocked = True

    if upload_method == "Upload CSV" and os.path.exists("./temp_prov_check.csv"):
        os.remove("./temp_prov_check.csv")

    st.divider()

    # ========================================
    # FULL PIPELINE EXECUTION
    # ========================================
    st.header("🚀 Run Full Scan Pipeline")
    st.markdown("""
    **Purpose:** Execute the complete scan pipeline (Steps 2-12).
    **Guarantee:** Execution equivalence with CLI (`scan_live.py`).
    """)
    
    col1, col2 = st.columns([1, 3])
    with col1:
        if st.button("▶️ Run Full Pipeline", type="primary", width='stretch', disabled=execution_blocked):
            # Reset metadata for new run
            st.session_state.pipeline_run_metadata["status"] = "running"
            st.session_state.pipeline_run_metadata["error"] = None
            
            try:
                with st.spinner("🚀 Running full scan pipeline (Steps 2-12)..."):
                    uploaded_temp_path = None
                    if uploaded_file_obj:
                        uploaded_temp_path = Path("./temp_uploaded_snapshot.csv")
                        with open(uploaded_temp_path, "wb") as f:
                            f.write(uploaded_file_obj.getbuffer())
                        snapshot_path = str(uploaded_temp_path)
                    else:
                        snapshot_path = resolve_snapshot_path(explicit_path=explicit_snapshot_path_input)
                    
                    # Toggle Debug Mode via environment variable
                    if debug_mode:
                        os.environ["PIPELINE_DEBUG"] = "1"
                    else:
                        os.environ.pop("PIPELINE_DEBUG", None)

                    results = run_full_scan_pipeline(
                        snapshot_path=snapshot_path,
                        output_dir=None,
                        account_balance=account_balance,
                        max_portfolio_risk=max_portfolio_risk,
                        sizing_method=sizing_method,
                        expiry_intent='ANY'
                    )

                    if uploaded_temp_path and uploaded_temp_path.exists():
                        uploaded_temp_path.unlink()

                    # Persist results in session state
                    st.session_state['pipeline_results'] = {
                        k: sanitize_for_arrow(v) 
                        for k, v in results.items() 
                        if isinstance(v, pd.DataFrame)
                    }
                    # Store non-dataframe results too
                    for k, v in results.items():
                        if not isinstance(v, pd.DataFrame):
                            st.session_state['pipeline_results'][k] = v
                    
                    # Update metadata
                    ready_now_count = len(results.get('acceptance_ready', pd.DataFrame()))
                    st.session_state.pipeline_run_metadata.update({
                        "last_run": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "status": "completed" if ready_now_count > 0 else "completed_empty",
                        "ready_now_count": ready_now_count
                    })
                    
                    # Force clear cache for pipeline results
                    st.cache_data.clear()
                    st.rerun() # Force UI update to show results
                    
            except Exception as e:
                st.session_state.pipeline_run_metadata.update({
                    "last_run": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "status": "failed",
                    "error": str(e)
                })
                st.error(f"❌ Full pipeline failed: {e}")
                st.exception(e)
    
    with col2:
        # Execution Proof & Status Banner
        meta = st.session_state.pipeline_run_metadata
        if meta["last_run"]:
            if meta["status"] == "completed":
                st.success(f"✅ **Pipeline Complete:** {meta['ready_now_count']} candidates found at {meta['last_run']}")
            elif meta["status"] == "completed_empty":
                st.warning(f"⚠️ **Pipeline Complete:** No candidates found at {meta['last_run']}")
            elif meta["status"] == "failed":
                st.error(f"❌ **Pipeline Failed** at {meta['last_run']}: {meta['error']}")
        
        if 'pipeline_results' in st.session_state:
            results = st.session_state['pipeline_results']
            
            # 1. Pipeline Health Funnel
            if 'pipeline_health' in results:
                health = results['pipeline_health']
                st.subheader("📈 Pipeline Conversion Funnel")
                
                f1, f2, f3, f4 = st.columns(4)
                with f1:
                    st.metric("Tickers In", len(results.get('snapshot', pd.DataFrame())))
                with f2:
                    st.metric("Valid Contracts", health['step9b']['valid'])
                with f3:
                    st.metric("READY_NOW", health['step12']['ready_now'])
                with f4:
                    # Shadow Mode Count
                    shadow_count = 0
                    if 'acceptance_all' in results:
                        df_all = results['acceptance_all']
                        if 'acceptance_status' in df_all.columns:
                            shadow_count = (df_all['acceptance_status'] == 'STRUCTURALLY_READY').sum()
                    st.metric("Shadow Mode", shadow_count)

            st.divider()
            
            ready_now_count = len(results.get('acceptance_ready', pd.DataFrame()))
            st.metric("READY_NOW Candidates", ready_now_count)
            
            # Display summary metrics
            st.subheader("Pipeline Summary")
            summary_cols = st.columns(4)
            with summary_cols[0]:
                st.metric("Step 2 (Snapshot)", len(results.get('snapshot', pd.DataFrame())))
            with summary_cols[1]:
                st.metric("Step 3 (Filtered)", len(results.get('filtered', pd.DataFrame())))
            with summary_cols[2]:
                st.metric("Step 5 (Charted)", len(results.get('charted', pd.DataFrame())))
            with summary_cols[3]:
                st.metric("Step 6 (Validated)", len(results.get('validated_data', pd.DataFrame())))
            
            st.subheader("Detailed Results")
            
            # Add Forensic Audit Tab (CLI-style transparency)
            tabs = ["✅ READY_NOW", "⏸️ WAIT", "🔭 Shadow Mode", "🕵️ Forensic Audit", "🔬 Row Counts", "📊 All Steps"]
            if debug_mode:
                tabs.append("🧪 Debug Console")
                
            tab_ready, tab_wait, tab_shadow, tab_audit, tab_counts, tab_all, *extra_tabs = st.tabs(tabs)
            tab_debug = extra_tabs[0] if extra_tabs else None
            
            with tab_ready:
                # FIX 2: READY_NOW tab must bind to Step 12
                acceptance_ready_df = results.get('acceptance_ready', pd.DataFrame())
                thesis_envelopes_df = results.get('thesis_envelopes', pd.DataFrame())
                
                if not acceptance_ready_df.empty:
                    # UX Improvement: Discovery Mode Banner
                    iv_history = 0
                    if 'iv_history_days' in acceptance_ready_df.columns:
                        median_val = acceptance_ready_df['iv_history_days'].median()
                        iv_history = int(median_val) if pd.notna(median_val) else 0
                    
                    st.info(f"""
                    🧠 **Discovery Mode**  
                    These strategies passed acceptance but are not yet executable due to insufficient IV history ({iv_history} / 120 days).  
                    Sizing and capital allocation are intentionally disabled.
                    """)

                    # SEMANTIC SHIFT: Visibility is driven by Acceptance (Step 12), not Sizing (Step 8)
                    sizing_cols = ['Ticker', 'Strategy_Name', 'Thesis_Max_Envelope', 'Expression_Tier', 'Scaling_Roadmap', 'Liquidity_Velocity', 'Theoretical_Capital_Req', 'Portfolio_Audit']
                    available_sizing_cols = [c for c in sizing_cols if c in thesis_envelopes_df.columns]
                    
                    # Only merge if we have sizing columns beyond the join keys
                    if len(available_sizing_cols) > 2:
                        df_display = acceptance_ready_df.merge(
                            thesis_envelopes_df[available_sizing_cols],
                            on=['Ticker', 'Strategy_Name'],
                            how='left'
                        )
                    else:
                        df_display = acceptance_ready_df
                        st.info("✅ **Valid trades found; no sizing metadata applied**")
                    
                    st.info("💡 **Sizing Philosophy:** The system defines the **Thesis Ceiling** (Max Expression). The human defines the **Entry Floor** (usually 1 unit).")
                    
                    with st.expander("📖 Understanding Sizing & Scaling"):
                        st.markdown("""
                        ### The 'Ceiling vs. Floor' Model
                        1. **Entry Floor (1 Unit):** Always start with the minimum viable expression.
                        2. **Thesis Ceiling (Max Expression):** The maximum number of contracts the trade structure and market liquidity can support before risk/reward degrades.
                        3. **Scaling Roadmap:** Only move from Floor to Ceiling if price action confirms the thesis.
                        
                        ### Expression Tiers
                        *   🟢 **CORE:** High-capacity, liquid setups suitable for full portfolio expression.
                        *   🟡 **STANDARD:** Balanced setups with normal risk parameters.
                        *   🔵 **NICHE:** Limited-capacity or high-convexity setups where size is naturally constrained.
                        """)

                    # Color code the Expression Tier
                    def color_tier(val):
                        color = 'white'
                        if val == 'CORE': color = '#00c853'
                        elif val == 'STANDARD': color = '#ffa500'
                        elif val == 'NICHE': color = '#2979ff'
                        return f'color: {color}; font-weight: bold'

                    st.dataframe(
                        df_display.style.applymap(color_tier, subset=['Expression_Tier']) if 'Expression_Tier' in df_display.columns else df_display,
                        width="stretch"
                    )
                    
                    # Capacity Meter for selected trade
                    if not df_display.empty:
                        st.divider()
                        st.subheader("🎯 Expression Capacity Analysis")
                        selected_ticker = st.selectbox("Select Ticker to Analyze Capacity:", df_display['Ticker'].unique())
                        row = df_display[df_display['Ticker'] == selected_ticker].iloc[0]
                        
                        max_units = row.get('Thesis_Max_Envelope', 1)
                        velocity = row.get('Liquidity_Velocity', 10)
                        st.write(f"**Scaling Roadmap for {selected_ticker}:** {row.get('Scaling_Roadmap', 'N/A')}")

                        col_v1, col_v2 = st.columns(2)
                        with col_v1:
                            # Visual progress bar showing 1 unit vs Max
                            progress = 1 / max_units if max_units > 0 else 0
                            st.progress(progress, text=f"Current Entry (1 Unit) is {progress:.0%} of Thesis Ceiling ({max_units} Units)")

                        with col_v2:
                            # Exit Velocity Meter
                            v_color = "green" if velocity >= 7 else "orange" if velocity >= 4 else "red"
                            st.markdown(f"**Exit Velocity:** :{v_color}[{velocity}/10]")
                            st.caption("Measures ease of exiting the full position based on Open Interest.")
                else:
                    st.info("No trades currently meet all acceptance criteria.")

            with tab_wait:
                if 'acceptance_all' in results:
                    df_all = results['acceptance_all']
                    df_wait = df_all[df_all['acceptance_status'] == 'WAIT']
                    render_wait_table(df_wait)
                else:
                    st.info("Run the pipeline to see WAIT strategies.")
            
            with tab_shadow:
                if 'acceptance_all' in results:
                    df_all = results['acceptance_all']
                    df_shadow = df_all[df_all['acceptance_status'] == 'STRUCTURALLY_READY']
                    if not df_shadow.empty:
                        st.info("🔭 These strategies are structurally sound but awaiting IV maturity (120 days).")
                        st.dataframe(df_shadow, width='stretch')
                    else:
                        st.write("No strategies currently in Shadow Mode.")

            with tab_counts:
                st.subheader("🔬 Forensic Row Counts")
                if 'pipeline_results' in st.session_state:
                    res = st.session_state['pipeline_results']
                    # FIX 3: Row Counts must use Step-12 counts correctly
                    count_data = {
                        "Step": [
                            "Step 2 (Snapshot)",
                            "Step 3 (Filtered)",
                            "Step 6 (Validated)",
                            "Step 9B (Contracts)",
                            "Step 12 (Acceptance All)",
                            "Step 12 (READY_NOW)",
                            "Step 8 (Thesis Envelopes)"
                        ],
                        "Row Count": [
                            len(res.get('snapshot', pd.DataFrame())),
                            len(res.get('filtered', pd.DataFrame())),
                            len(res.get('validated_data', pd.DataFrame())),
                            len(res.get('selected_contracts', pd.DataFrame())),
                            len(res.get('acceptance_all', pd.DataFrame())),
                            len(res.get('acceptance_ready', pd.DataFrame())),
                            len(res.get('thesis_envelopes', pd.DataFrame()))
                        ]
                    }
                    st.table(pd.DataFrame(count_data))
                else:
                    st.info("Run the pipeline to see row counts.")

            with tab_audit:
                st.subheader("🕵️ Pipeline Forensic Audit")
                
                # 1. Pipeline Trace (Row Counts)
                if 'pipeline_health' in results:
                    health = results['pipeline_health']
                    audit_data = [
                        {"Step": "1. Tickers In", "Count": len(results.get('snapshot', pd.DataFrame())), "Status": "✅"},
                        {"Step": "2. Filtered (Step 3)", "Count": len(results.get('filtered', pd.DataFrame())), "Status": "✅"},
                        {"Step": "3. Validated (Step 6)", "Count": len(results.get('validated_data', pd.DataFrame())), "Status": "✅"},
                        {"Step": "4. Contracts Found (Step 9B)", "Count": health['step9b']['total_contracts'], "Status": "✅"},
                        {"Step": "5. READY_NOW (Step 12)", "Count": health['step12']['ready_now'], "Status": "✅"},
                        {"Step": "6. Thesis Envelopes (Step 8)", "Count": len(results.get('thesis_envelopes', pd.DataFrame())), "Status": "✅" if not results.get('thesis_envelopes', pd.DataFrame()).empty else "⚠️"}
                    ]
                    st.table(pd.DataFrame(audit_data))

                # 2. Rejection Reasons (Step 11/12)
                st.divider()
                st.subheader("❌ Rejected Candidates")
                if 'evaluated_strategies' in results:
                    df_eval = results['evaluated_strategies']
                    if 'Validation_Status' in df_eval.columns:
                        rejected = df_eval[df_eval['Validation_Status'] == 'Reject']
                        if not rejected.empty:
                            st.dataframe(rejected[['Ticker', 'Strategy_Name', 'Evaluation_Notes']], width='stretch')
                        else:
                            st.write("No strategies rejected by theory evaluation.")

                # 3. IV Availability Diagnostics
                st.divider()
                st.subheader("📊 IV Availability Diagnostics")
                df_ready = results.get('acceptance_ready', pd.DataFrame())
                if not df_ready.empty and 'iv_history_days' in df_ready.columns:
                    iv_unavailable = (df_ready['iv_history_days'] < 120).sum()
                    st.write(f"Strategies lacking full IV history (120d): {iv_unavailable} / {len(df_ready)}")
                    if iv_unavailable > 0:
                        st.dataframe(df_ready[df_ready['iv_history_days'] < 120][['Ticker', 'iv_history_days']], width='stretch')
            
            if tab_debug:
                with tab_debug:
                    st.header("🧪 Pipeline Debug Console")
                    if 'debug_summary' in results:
                        summary = results['debug_summary']
                        
                        # 1. Pipeline Step Trace
                        st.subheader("📈 Pipeline Step Trace")
                        if summary.get('step_counts'):
                            trace_cols = st.columns(len(summary['step_counts']))
                            for i, (step, count) in enumerate(summary['step_counts'].items()):
                                with trace_cols[i % len(trace_cols)]:
                                    st.metric(step.replace("step", "Step "), count)
                        
                        # 2. Debug Events Table
                        st.divider()
                        st.subheader("🚨 Silent Failures & Events")
                        if summary.get('events'):
                            events_df = pd.DataFrame(summary['events'])
                            
                            # Color code severity
                            def color_severity(val):
                                color = 'white'
                                if val == 'ERROR': color = '#ff4b4b'
                                elif val == 'WARN': color = '#ffa500'
                                elif val == 'INFO': color = '#00c853'
                                return f'color: {color}'
                            
                            st.dataframe(
                                events_df.style.applymap(color_severity, subset=['severity']),
                                width='stretch'
                            )
                            
                            # 3. Event Detail Viewer
                            selected_event_idx = st.selectbox("Inspect Event Context:", range(len(events_df)), format_func=lambda x: f"{events_df.iloc[x]['step']} - {events_df.iloc[x]['code']}")
                            event = events_df.iloc[selected_event_idx]
                            st.json(event['context'])
                            
                            # 4. Row-Level Impact Viewer
                            st.divider()
                            st.subheader("🔍 Row-Level Impact Viewer")
                            step_name = event['step']
                            artifact_key = next((k for k in summary.get('artifacts', {}).keys() if step_name in k), None)
                            
                            if artifact_key and artifact_key in results:
                                df_artifact = results[artifact_key]
                                ctx = event['context']
                                if 'tickers' in ctx:
                                    highlight_tickers = ctx['tickers']
                                    if isinstance(highlight_tickers, list):
                                        st.write(f"Highlighting affected tickers: {', '.join(highlight_tickers)}")
                                        st.dataframe(df_artifact[df_artifact['Ticker'].isin(highlight_tickers)], width='stretch')
                                else:
                                    st.write("Full artifact data:")
                                    st.dataframe(df_artifact, width='stretch')
                            else:
                                st.info(f"No specific dataframe artifact recorded for step `{step_name}`.")
                        else:
                            st.success("No debug events recorded. Pipeline ran cleanly.")
                    else:
                        st.info("Run the pipeline with Debug Mode enabled to see trace data.")

            with tab_all:
                selected_step = st.selectbox("View Output for Step:", [k for k, v in results.items() if isinstance(v, pd.DataFrame)])
                if selected_step:
                    df_output = results[selected_step]
                    st.dataframe(df_output, width='stretch', height=400)
                    
                    csv = df_output.to_csv(index=False)
                    st.download_button(
                        label=f"📥 Download {selected_step} Output CSV",
                        data=csv,
                        file_name=f"{selected_step}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        width='stretch'
                    )

# ========================================
# MANAGE VIEW
# ========================================
elif st.session_state.view == "manage":
    if st.button("← Back to Home"):
        set_view("home")
    
    st.title("🧪 Manage Positions")
    st.markdown("Monitor and revalidate existing option positions.")
    
    st.divider()
    
    INPUT_PATH = "/Users/haniabadi/Documents/Windows/Positions_Account_.csv"

    if st.sidebar.button("🚀 Run Full Management Pipeline", type="primary", width='stretch'):
        try:
            with st.spinner("Executing full management pipeline (Phases 1-7)..."):
                # Use the explicit INPUT_PATH if it exists, otherwise fallback to canonical
                target_path = INPUT_PATH if os.path.exists(INPUT_PATH) else None
                # FIX: Align with Phase 1 API contract (remove unexpected input_path keyword)
                df_input, snapshot_path = phase1_load_and_clean_positions(
                    input_path=target_path,
                    save_snapshot=True
                )
                
                if df_input.empty:
                    st.error("⚠️ No positions found in the input file. Please check your brokerage export.")
                    st.stop()

                # === Execution Guard: Canonical Identity Law ===
                assert "Underlying_Ticker" in df_input.columns, "FATAL: Canonical identity not active (Underlying_Ticker missing)"
                assert not df_input["Underlying_Ticker"].isna().any(), "FATAL: NULL Underlying_Ticker detected"
                
                df_parsed = phase2_run_all(df_input)
                df_enriched = run_phase3_enrichment(df_parsed)
                df_snapshot, csv_path, run_id, csv_success, db_success = save_clean_snapshot(df_enriched, to_csv=True, to_db=True)
                df_portfolio, diagnostics = check_portfolio_limits(df_snapshot, limits=get_persona_limits('moderate'), account_balance=100000.0)
                df_portfolio = analyze_correlation_risk(df_portfolio)
                df_drift = compute_drift_metrics(df_portfolio)
                df_drift = classify_drift_severity(df_drift)
                symbols = df_drift['Symbol'].unique().tolist()
                df_chart = load_chart_signals(symbols, source='scan_engine')
                final_df = df_drift
                if not df_chart.empty:
                    final_df = merge_chart_signals(df_drift, df_chart)
                final_df = compute_exit_recommendations(final_df)
                final_df = prioritize_recommendations(final_df)
                st.session_state["df_final_manage"] = final_df
                st.success(f"✅ Full management pipeline complete! Run ID: {run_id}")
                st.rerun()
        except Exception as e:
            st.error(f"❌ Pipeline failed: {e}")
            st.exception(e)

    st.sidebar.divider()

    if st.sidebar.button("🔍 Step 1: Load + Clean", width='stretch'):
        try:
            import inspect
            from core.phase1_clean import phase1_load_and_clean_positions
            print("RUNTIME PHASE 1 FILE:", inspect.getfile(phase1_load_and_clean_positions))
            print("RUNTIME PHASE 1 SIGNATURE:", inspect.signature(phase1_load_and_clean_positions))

            target_path = INPUT_PATH if os.path.exists(INPUT_PATH) else None
            # FIX: Align with Phase 1 API contract (remove unexpected input_path keyword)
            df_input, snapshot_path = phase1_load_and_clean_positions(
                input_path=target_path,
                save_snapshot=True
            )
            
            if df_input.empty:
                st.error("⚠️ No positions found in the input file.")
                st.stop()

            # === Execution Guard: Canonical Identity Law ===
            assert "Underlying_Ticker" in df_input.columns, "FATAL: Canonical identity not active (Underlying_Ticker missing)"
            assert not df_input["Underlying_Ticker"].isna().any(), "FATAL: NULL Underlying_Ticker detected"
            
            st.session_state["df_input"] = df_input
            st.success(f"✅ Loaded and cleaned {len(df_input)} rows")
            st.dataframe(sanitize_for_arrow(df_input), width='stretch')
        except Exception as e:
            st.error(f"❌ Error in Step 1: {e}")

    if st.sidebar.button("🔎 Step 2: Parse + Tag", width='stretch'):
        try:
            df_input = st.session_state.get("df_input")
            if df_input is None:
                st.warning("⚠️ Run Step 1 first.")
            else:
                df_parsed = phase2_run_all(df_input)
                st.session_state["df_parsed"] = df_parsed
                st.success("✅ Symbols parsed and strategies tagged")
                st.dataframe(sanitize_for_arrow(df_parsed), width='stretch')
        except Exception as e:
            st.error(f"❌ Error in Step 2: {e}")

    if st.sidebar.button("🔬 Step 3: Enrich (Phase 3)", width='stretch'):
        try:
            df_parsed = st.session_state.get("df_parsed")
            if df_parsed is None:
                st.warning("⚠️ Run Step 2 first.")
            else:
                df_enriched = run_phase3_enrichment(df_parsed)
                st.session_state["df_enriched"] = df_enriched
                st.success(f"✅ Phase 3 enrichment complete: {len(df_enriched)} positions enriched")
                st.dataframe(sanitize_for_arrow(df_enriched), width='stretch')
        except Exception as e:
            st.error(f"❌ Error in Step 3: {e}")

    if st.sidebar.button("💾 Step 4: Save Snapshot (Phase 4)", width='stretch'):
        try:
            df_enriched = st.session_state.get("df_enriched")
            if df_enriched is None:
                st.warning("⚠️ Run Step 3 first.")
            else:
                df_snapshot, csv_path, run_id, csv_success, db_success = save_clean_snapshot(df_enriched, to_csv=True, to_db=True)
                st.session_state["df_snapshot"] = df_snapshot
                st.success(f"✅ Snapshot saved! Run ID: {run_id}")
        except Exception as e:
            st.error(f"❌ Error in Step 4: {e}")

    if st.sidebar.button("⚖️ Step 5: Portfolio & Risk", width='stretch'):
        try:
            df_snapshot = st.session_state.get("df_snapshot")
            if df_snapshot is None:
                st.warning("⚠️ Run Step 4 first.")
            else:
                df_portfolio, diagnostics = check_portfolio_limits(df_snapshot, limits=get_persona_limits('moderate'), account_balance=100000.0)
                df_portfolio = analyze_correlation_risk(df_portfolio)
                st.session_state["df_portfolio"] = df_portfolio
                st.success("✅ Portfolio risk analysis complete")
                st.dataframe(sanitize_for_arrow(df_portfolio), width='stretch')
        except Exception as e:
            st.error(f"❌ Error in Step 5: {e}")

    if st.sidebar.button("🔄 Step 6: Drift Analysis", width='stretch'):
        try:
            df_portfolio = st.session_state.get("df_portfolio")
            if df_portfolio is None:
                st.warning("⚠️ Run Step 5 first.")
            else:
                df_drift = compute_drift_metrics(df_portfolio)
                df_drift = classify_drift_severity(df_drift)
                st.session_state["df_drift"] = df_drift
                st.success("✅ Drift analysis complete")
                st.dataframe(sanitize_for_arrow(df_drift), width='stretch')
        except Exception as e:
            st.error(f"❌ Error in Step 6: {e}")

    if st.sidebar.button("🎯 Step 7: Recommendations", width='stretch'):
        try:
            df_drift = st.session_state.get("df_drift")
            if df_drift is None:
                st.warning("⚠️ Run Step 6 first.")
            else:
                symbols = df_drift['Symbol'].unique().tolist()
                df_chart = load_chart_signals(symbols, source='scan_engine')
                final_df = df_drift
                if not df_chart.empty:
                    final_df = merge_chart_signals(df_drift, df_chart)
                final_df = compute_exit_recommendations(final_df)
                final_df = prioritize_recommendations(final_df)
                st.session_state["df_final_manage"] = final_df
                st.success("✅ Recommendations generated")
        except Exception as e:
            st.error(f"❌ Error in Step 7: {e}")

    # === Display Results ===
    final_df = st.session_state.get("df_final_manage")
    if final_df is not None:
        st.divider()
        
        # Summary Metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Positions", len(final_df))
        with col2:
            if 'Recommendation' in final_df.columns:
                close_count = (final_df['Recommendation'] == 'CLOSE').sum()
                st.metric("Exit Alerts", close_count, delta=int(close_count), delta_color="inverse")
        with col3:
            if 'Drift_Severity' in final_df.columns:
                critical_count = (final_df['Drift_Severity'] == 'CRITICAL').sum()
                st.metric("Critical Drift", critical_count, delta=int(critical_count), delta_color="inverse")
        with col4:
            if 'Current_PCS_v2' in final_df.columns:
                avg_pcs = final_df['Current_PCS_v2'].mean()
                st.metric("Avg Current PCS", f"{avg_pcs:.1f}")

        tab1, tab2, tab3, tab4 = st.tabs(["🎯 Recommendations", "🏥 Position Health", "🔄 Drift Analysis", "📊 Full Data"])
        
        with tab1:
            st.subheader("Authoritative Exit Recommendations")
            
            # Determine which recommendation column to show
            # Rec_Action_Final is the authoritative one from DriftEngine
            display_rec_col = 'Rec_Action_Final' if 'Rec_Action_Final' in final_df.columns else 'Recommendation'
            
            if display_rec_col in final_df.columns:
                cols_to_show = ['Symbol', 'Strategy', display_rec_col]
                
                # Add Alpha recommendation for comparison if available
                if 'Rec_Action' in final_df.columns and display_rec_col == 'Rec_Action_Final':
                    cols_to_show.append('Rec_Action')
                
                # Add Drift Action for transparency
                if 'Drift_Action' in final_df.columns:
                    cols_to_show.append('Drift_Action')
                
                if 'Urgency' in final_df.columns:
                    cols_to_show.append('Urgency')
                
                rationale_col = 'Exit_Rationale' if 'Exit_Rationale' in final_df.columns else 'Rationale'
                if rationale_col in final_df.columns:
                    cols_to_show.append(rationale_col)
                
                rec_df = final_df[cols_to_show].copy()
                
                # Color coding for recommendations
                def color_recommendation(val):
                    color = 'white'
                    val_up = str(val).upper()
                    if val_up in ['CLOSE', 'EXIT', 'FORCE_EXIT']: color = '#ff4b4b'
                    elif val_up in ['ROLL', 'TRIM', 'REVALIDATE']: color = '#ffa500'
                    elif val_up in ['HOLD', 'ENTER']: color = '#00c853'
                    elif val_up == 'WAIT': color = '#ffff00'
                    return f'background-color: {color}; color: black; font-weight: bold'

                st.info("💡 **Sizing Philosophy:** The system defines the **Thesis Ceiling** (Max Expression). The human defines the **Entry Floor** (usually 1 unit).")

                st.dataframe(
                    rec_df.style.applymap(color_recommendation, subset=[display_rec_col]),
                    width='stretch'
                )
            else:
                st.info("Run Step 7 to see recommendations")

        with tab2:
            st.subheader("Position Health & Drift")
            health_cols = ['Symbol', 'Strategy', 'Current_PCS_v2', 'Current_PCS_Tier_v2', 'Drift_Severity', 'ROI_Pct']
            available_cols = [c for c in health_cols if c in final_df.columns]
            
            if available_cols:
                st.dataframe(
                    final_df[available_cols].sort_values('Current_PCS_v2', ascending=True),
                    width='stretch'
                )
            else:
                st.info("Run enrichment and drift analysis to see health metrics")

        with tab3:
            render_drift_analysis(final_df)

        with tab4:
            st.subheader("Complete Enriched Dataset")
            st.dataframe(sanitize_for_arrow(final_df), width='stretch')
            
            csv = final_df.to_csv(index=False)
            st.download_button(
                label="📥 Download Full Management Report (CSV)",
                data=csv,
                file_name=f"management_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
