import streamlit as st
import pandas as pd
from datetime import datetime
import sys
import os
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# === Add parent directory to Python path ===
parent_dir = Path(__file__).parent.parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))


def sanitize_for_arrow(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sanitize DataFrame for Arrow serialization (fixes Streamlit display errors).
    
    Converts problematic dtypes to Arrow-compatible types:
    - StringDtype → object (Arrow can't handle pandas StringDtype)
    - object → keep as object (more compatible than string)
    - datetime64[ns, UTC] → datetime64[ns] (remove timezone)
    - Mixed types in columns → coerce to consistent type
    
    This prevents: "Could not convert string[python] with type StringDtype"
    """
    df = df.copy()
    
    for col in df.columns:
        dtype = df[col].dtype
        
        # Convert StringDtype back to object (Arrow compatibility)
        if dtype == 'string':
            df[col] = df[col].astype('object')
        
        # Keep object dtype as-is (don't convert to string)
        elif dtype == 'object':
            # Just ensure no mixed types cause issues
            try:
                # Try to coerce if it's numeric hiding as object
                if pd.api.types.infer_dtype(df[col]) in ['integer', 'floating']:
                    df[col] = pd.to_numeric(df[col], errors='ignore')
            except:
                pass  # Keep as object
        
        # Remove timezone from datetime columns (Arrow doesn't support it well)
        elif pd.api.types.is_datetime64tz_dtype(dtype):
            df[col] = df[col].dt.tz_localize(None)
    
    return df


# === Imports for Phase 1 & 2 ===
from core.phase1_clean import phase1_load_and_clean_raw_v2 as phase1_load_and_clean
from core.phase2_parse import phase2_run_all

# === Imports for Scan Engine (Modular) ===
from core.scan_engine import (
    load_ivhv_snapshot,
    filter_ivhv_gap,
    compute_chart_signals,
    validate_data_quality,
    run_full_scan_pipeline,
    resolve_snapshot_path # New import for Step 0
)
from core.scan_engine.step3_filter_ivhv import STEP3_VERSION, STEP3_LOGIC_HASH
from core.scan_engine.step11_independent_evaluation import evaluate_strategies_independently

# === Import for IV/HV Scraper ===
from core.scraper.ivhv_bootstrap import get_today_snapshot_path
from core.scraper.config import DEFAULT_TICKER_CSV

st.set_page_config(
    page_title="Options Intelligence Platform",
    layout="wide"
)

# === Cached Step 2 Loader (for debugging performance) ===
@st.cache_data(ttl=3600, show_spinner="Loading IV/HV snapshot with Murphy + Sinclair data...")
def load_step2_cached(snapshot_path: str):
    """
    Cached wrapper for load_ivhv_snapshot to speed up debugging.
    Cache expires after 1 hour or when snapshot file changes.
    """
    return load_ivhv_snapshot(snapshot_path)

# === Initialize session state ===
if "view" not in st.session_state:
    st.session_state.view = "home"

# === Navigation Helper ===
def set_view(view_name):
    st.session_state.view = view_name
    st.rerun()

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
        if st.button("🔍 Scan Market", use_container_width=True):
            set_view("scan")

    with col2:
        if st.button("🧪 Manage Positions", use_container_width=True):
            set_view("manage")

# ========================================
# SCAN VIEW
# ========================================
elif st.session_state.view == "scan":
    # Back button
    if st.button("← Back to Home"):
        set_view("home")
    
    st.title("🔍 Market Scan - Full Pipeline Orchestration")
    st.markdown("Execute the complete pipeline to discover and evaluate trade opportunities.")
    
    st.divider()
    
    # ========================================
    # FILE UPLOAD & CONFIGURATION
    # ========================================
    with st.sidebar:
        st.header("📂 Data Source")
        
        # Add live snapshot mode indicator at top
        if 'live_snapshot_mode' in st.session_state and st.session_state.get('live_snapshot_mode', False):
            st.success("🔴 **LIVE MODE ACTIVE** - Using Step 0 Schwab data")
            st.info("👇 Legacy data source options disabled in live mode")
        
        upload_method = st.radio(
            "Choose input method:",
            ["Auto (Today's Snapshot)", "Use File Path", "Upload CSV"],
            disabled=st.session_state.get('live_snapshot_mode', False)
        )
        
        uploaded_file_obj = None # Renamed to avoid conflict with path
        explicit_snapshot_path_input = None # Renamed for clarity
        
        if upload_method == "Auto (Today's Snapshot)":
            # Automatically use today's snapshot from scraper
            today_snapshot_path = get_today_snapshot_path()
            if os.path.exists(today_snapshot_path):
                explicit_snapshot_path_input = str(today_snapshot_path)
                st.success(f"✅ Using today's snapshot: {os.path.basename(today_snapshot_path)}")
                # Show metadata
                df_check = pd.read_csv(today_snapshot_path)
                st.caption(f"📊 {len(df_check)} tickers | Last modified: {datetime.fromtimestamp(os.path.getmtime(today_snapshot_path)).strftime('%Y-%m-%d %H:%M')}")
            else:
                st.warning("⚠️ Today's snapshot not found. Run Step 0 to scrape data first.")
                # Fallback to legacy path (this will be handled by resolve_snapshot_path's default logic)
                explicit_snapshot_path_input = None # Let Step 0 resolve the latest from archive
                st.info(f"Attempting to resolve latest snapshot from 'data/snapshots'...")
        
        elif upload_method == "Upload CSV":
            uploaded_file_obj = st.file_uploader(
                "Upload IV/HV Snapshot CSV",
                type=['csv'],
                help="Upload Fidelity IV/HV snapshot export"
            )
            if uploaded_file_obj:
                st.success(f"✅ File uploaded: {uploaded_file_obj.name}")
        
        else:  # Use File Path
            explicit_snapshot_path_input = st.text_input(
                "IV/HV Snapshot Path",
                value=os.getenv('FIDELITY_SNAPSHOT_PATH', 
                               '/Users/haniabadi/Documents/Windows/OptionsSnapshots/fidelity_ivhv_snapshot.csv'),
                help="Full path to IV/HV CSV file"
            )
        
        st.divider()
        st.header("⚙️ Pipeline Parameters")
        min_gap = st.slider("Min IVHV Gap (Step 3)", 1.0, 5.0, 2.0, 0.5)
        min_iv = st.number_input("Min IV (liquidity, Step 3)", 10.0, 30.0, 15.0, 5.0)
    
    # ========================================
    # STEP 0: IV/HV SCRAPER (OPTIONAL)
    # ========================================
    st.header("📡 Step 0: IV/HV Data Scraper (Optional)")
    st.markdown("""
    **Purpose:** Scrape fresh IV/HV data from Fidelity when needed  
    **Output:** Daily snapshot CSV in `data/ivhv_archive/`  
    **When to use:** Before running pipeline, or when data is stale
    """)
    
    with st.expander("ℹ️ How to Use the Scraper", expanded=False):
        st.markdown("""
        ### 📋 Automated Workflow:
        
        **First Time Setup:**
        1. Click "🚀 Run IV/HV Scraper"
        2. Chrome will open automatically
        3. **Manually login to Fidelity** in the Chrome window
        4. Chrome profile is saved to `~/.chrome_fidauto`
        5. Scraper runs automatically after 3-second wait
        
        **Subsequent Runs (Fully Automated!):**
        1. Click "🚀 Run IV/HV Scraper"
        2. Dashboard detects saved Chrome profile ✅
        3. Scraper runs completely automatically
        4. No manual steps required!
        
        ### 🔄 Scraping Modes:
        - **Fresh Run**: Scrapes all tickers (5-15 min for ~100 tickers)
        - **Resume Failed**: Only retries failed tickers from previous run
        
        ### 🔧 Troubleshooting:
        - **"Scraper failed" error**: Chrome profile may need refresh
          - Delete `~/.chrome_fidauto` folder and run again for fresh login
        - **Timeout**: Use "Resume Failed" mode to continue from where it stopped
        - **No data**: Check that [inputs/tickers.csv](inputs/tickers.csv) exists and has valid tickers
        
        ### 💡 Pro Tips:
        - First run: ~15 min (includes manual login)
        - Later runs: Fully automated, no interaction needed!
        - Chrome uses persistent profile - stays logged in between runs
        """)
    
    # Check for today's snapshot
    today_snapshot = get_today_snapshot_path()
    snapshot_exists = os.path.exists(today_snapshot)
    
    col1, col2, col3 = st.columns([2, 2, 2])
    with col1:
        if snapshot_exists:
            st.success(f"✅ Today's snapshot exists")
        else:
            st.warning("⚠️ Today's snapshot missing")
    
    with col2:
        if snapshot_exists:
            df_check = pd.read_csv(today_snapshot)
            st.info(f"📊 {len(df_check)} tickers scraped")
    
    with col3:
        if os.path.exists(today_snapshot):
            st.caption(f"📁 {os.path.basename(today_snapshot)}")
    
    # Scraper controls
    scraper_col1, scraper_col2 = st.columns([1, 3])
    
    with scraper_col1:
        scrape_action = st.radio(
            "Scraper Mode:",
            ["Fresh Run", "Resume Failed"],
            help="Fresh Run: Start from scratch | Resume: Continue from failed tickers"
        )
    
    with scraper_col2:
        if st.button("🚀 Run IV/HV Scraper", type="secondary" if snapshot_exists else "primary", use_container_width=True):
            try:
                # Check if Chrome profile exists (indicates previous login)
                chrome_profile = parent_dir / ".chrome_fidauto"
                profile_exists = chrome_profile.exists()
                
                if not profile_exists:
                    st.warning("⚠️ **First-time setup**: Chrome will open. Please login to Fidelity manually, then the scraper will run automatically.")
                    st.info("💡 After first login, Chrome profile is saved and future runs will be fully automated!")
                else:
                    st.info("✅ Using saved Chrome profile - scraper will run automatically")
                
                with st.spinner("🔄 Scraper running... (Chrome will open briefly)"):
                    import subprocess
                    
                    # Build command - use run.py with --no-prompt for automation
                    scraper_path = parent_dir / "core" / "scraper" / "run.py"
                    
                    # Use same Python interpreter that's running Streamlit (ensures venv is used)
                    python_exe = sys.executable
                    cmd = [python_exe, str(scraper_path), "--no-prompt"]
                    
                    if scrape_action == "Resume Failed":
                        cmd.append("--resume")
                        st.info("Resume mode: Will skip tickers already in today's snapshot")
                    
                    st.info(f"🖥️ Running scraper in auto mode...")
                    st.code(f"Command: {Path(python_exe).name} {scraper_path.relative_to(parent_dir)} --no-prompt" + (" --resume" if scrape_action == "Resume Failed" else ""))
                    
                    # Run scraper with output capture (scripts are self-contained, no PYTHONPATH needed)
                    result = subprocess.run(
                        cmd,
                        cwd=str(parent_dir),
                        capture_output=True,
                        text=True,
                        timeout=3600  # 1 hour timeout
                    )
                    
                    st.info(f"🖥️ Running: `{' '.join(cmd)}`")
                    st.info("⚠️ Chrome will open - please login to Fidelity manually, then return here")
                    
                    # Run scraper (this will block until complete)
                    result = subprocess.run(
                        cmd,
                        cwd=str(parent_dir),
                        capture_output=True,
                        text=True,
                        timeout=3600  # 1 hour timeout
                    )
                    
                    if result.returncode == 0:
                        st.success("✅ Scraper completed successfully!")
                        
                        # Show output summary
                        with st.expander("📋 Scraper Output", expanded=True):
                            st.text(result.stdout)
                            if result.stderr:
                                st.warning("Warnings/Errors:")
                                st.text(result.stderr)
                        
                        # Check if snapshot was created
                        if os.path.exists(today_snapshot):
                            df_new = pd.read_csv(today_snapshot)
                            st.metric("Tickers Scraped", len(df_new))
                            st.info(f"📁 Saved to: {today_snapshot}")
                            
                            # Show Chrome profile status
                            if not profile_exists:
                                st.success("🎉 Chrome profile saved! Future scrapes will be fully automated.")
                        
                        st.rerun()  # Refresh to update status
                    else:
                        st.error("❌ Scraper failed!")
                        with st.expander("🔍 Error Details", expanded=True):
                            st.text(result.stderr)
                            st.text(result.stdout)
                        
            except subprocess.TimeoutExpired:
                st.error("⏱️ Scraper timed out after 1 hour")
            except Exception as e:
                st.error(f"❌ Scraper error: {e}")
                st.exception(e)
    
    # --- Scraper Log Panel (Observability Layer) ---
    st.subheader("📋 Scraper Activity Log")
    log_file_path = os.getenv('SCRAPER_LOG_FILE')
    if log_file_path and os.path.exists(log_file_path):
        with open(log_file_path, "r") as f:
            log_content = f.read()
        st.text_area("Scraper Log", log_content, height=300, key="scraper_log_panel")
        st.info(f"Logs from: {os.path.basename(log_file_path)}")
    else:
        st.info("Scraper log not available yet. Run the scraper to generate logs.")

    # Display existing snapshots
    with st.expander("📂 Available IV/HV Snapshots", expanded=False):
        archive_dir = Path("data/ivhv_archive")
        if archive_dir.exists():
            snapshots = sorted(archive_dir.glob("ivhv_snapshot_*.csv"), reverse=True)
            if snapshots:
                snapshot_data = []
                for snap in snapshots[:10]:  # Show last 10
                    df_snap = pd.read_csv(snap)
                    snapshot_data.append({
                        'Date': snap.stem.replace('ivhv_snapshot_', ''),
                        'Tickers': len(df_snap),
                        'Size': f"{snap.stat().st_size / 1024:.1f} KB",
                        'Path': str(snap)
                    })
                st.dataframe(
                    pd.DataFrame(snapshot_data),
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.info("No snapshots found in archive")
        else:
            st.warning("Archive directory not found: data/ivhv_archive/")
    
    st.divider()
    
    # ========================================
    # FULL PIPELINE EXECUTION
    # ========================================
    
    st.header("🚀 Run Full Scan Pipeline")
    st.markdown("""
    **Purpose:** Execute the complete scan pipeline from Step 2 to Step 11.
    This ensures all data enrichment and validation steps are run in sequence,
    maintaining schema integrity and consistency with CLI execution.
    """)
    
    # Step 0 Integration Toggle (PROMINENT)
    st.divider()
    col_toggle1, col_toggle2 = st.columns([1, 4])
    with col_toggle1:
        use_live_snapshot = st.checkbox(
            "🔴 **LIVE MODE**",
            value=False,
            help="Use Live Schwab Snapshot from Step 0"
        )
    with col_toggle2:
        if use_live_snapshot:
            st.success("✅ **STEP 0 ACTIVE** - Will load latest Schwab snapshot (bypasses scraper & full pipeline)")
        else:
            st.info("ℹ️ Legacy mode - Uses data source from sidebar + runs full pipeline")
    st.divider()
    
    # Configuration for the full pipeline run
    with st.expander("⚙️ Pipeline Configuration", expanded=True):
        pcol1, pcol2, pcol3 = st.columns(3)
        with pcol1:
            account_balance = st.number_input("Account Balance ($)", min_value=1000.0, value=100000.0, step=1000.0)
        with pcol2:
            max_portfolio_risk = st.slider("Max Portfolio Risk (%)", min_value=0.05, max_value=0.50, value=0.20, step=0.05)
        with pcol3:
            sizing_method = st.selectbox("Sizing Method", ['volatility_scaled', 'fixed_fractional', 'kelly', 'equal_weight'])
        
        st.subheader("PCS Filtering (Step 10) Parameters")
        pcs_col1, pcs_col2, pcs_col3 = st.columns(3)
        with pcs_col1:
            pcs_min_liquidity = st.slider("Min Liquidity Score", min_value=0.0, max_value=100.0, value=30.0, step=5.0)
        with pcs_col2:
            pcs_max_spread = st.slider("Max Spread %", min_value=1.0, max_value=20.0, value=8.0, step=1.0)
        with pcs_col3:
            pcs_strict_mode = st.checkbox("Strict PCS Mode", value=False, help="Enable stricter PCS filtering criteria.")
        
        st.subheader("Strategy Options")
        strat_col1, strat_col2 = st.columns(2)
        with strat_col1:
            enable_straddles = st.checkbox("Enable Straddles", value=True)
        with strat_col2:
            enable_strangles = st.checkbox("Enable Strangles", value=True)
        
        capital_limit = st.number_input("Capital Limit per Trade ($)", min_value=100.0, value=10000.0, step=100.0)

    col1, col2 = st.columns([1, 3])
    with col1:
        button_label = "▶️ Load Step 2 Data" if use_live_snapshot else "▶️ Run Full Pipeline"
        if st.button(button_label, type="primary", use_container_width=True):
            st.write("CHECKPOINT A: Before execution")
            logger.info("CHECKPOINT A: Before execution")
            try:
                # BRIDGE MODE: Load Step 2 directly when live snapshot enabled
                if use_live_snapshot:
                    st.info("🔴 Live Snapshot Mode: Loading Step 2 data directly (bypassing full pipeline)")
                    with st.spinner("📥 Loading live snapshot from Step 0..."):
                        # Load Step 2 enriched data directly
                        df_step2 = load_ivhv_snapshot(
                            use_live_snapshot=True,
                            skip_pattern_detection=True
                        )
                        
                        st.success(f"✅ Loaded {len(df_step2)} tickers from live snapshot")
                        
                        # Store in session state for display
                        st.session_state['pipeline_results'] = {
                            'snapshot': sanitize_for_arrow(df_step2)
                        }
                        st.session_state['live_snapshot_mode'] = True
                        
                        # Show warnings about expected limitations
                        st.warning(
                            "⚠️ **Live Snapshot Mode Limitations:**\n"
                            "- Step 3+ not executed (full pipeline bypassed)\n"
                            "- IV may be NaN (HV-only mode)\n"
                            "- Strategy evaluation not available\n"
                            "- This is a temporary bridge for Step 0 validation"
                        )
                else:
                    # LEGACY MODE: Run full pipeline
                    st.session_state['live_snapshot_mode'] = False
                    with st.spinner("🚀 Running full scan pipeline (Steps 0-11)..."):
                        # Resolve snapshot path
                        uploaded_temp_path = None
                        if uploaded_file_obj:
                            # Save uploaded file to a temporary path
                            uploaded_temp_path = Path("./temp_uploaded_snapshot.csv")
                            with open(uploaded_temp_path, "wb") as f:
                                f.write(uploaded_file_obj.getbuffer())
                            snapshot_path = str(uploaded_temp_path)
                            st.info(f"Using uploaded file: {uploaded_file_obj.name}")
                        elif explicit_snapshot_path_input:
                            snapshot_path = explicit_snapshot_path_input
                            st.info(f"Using explicit path: {snapshot_path}")
                        else:
                            # Use latest snapshot from data/snapshots/
                            snapshot_dir = Path("data/snapshots")
                            if snapshot_dir.exists():
                                snapshot_files = list(snapshot_dir.glob("ivhv_snapshot_*.csv"))
                                if snapshot_files:
                                    # Get most recent by modification time
                                    latest_snapshot = max(snapshot_files, key=lambda p: p.stat().st_mtime)
                                    snapshot_path = str(latest_snapshot)
                                    st.info(f"Using latest snapshot: {latest_snapshot.name}")
                                else:
                                    st.error("❌ No snapshot files found in data/snapshots/. Please run Step 0 or upload a snapshot.")
                                    st.stop()
                            else:
                                st.error("❌ data/snapshots/ directory not found. Please run Step 0 first.")
                                st.stop()
                        
                        # Call run_full_scan_pipeline with resolved path
                        results = run_full_scan_pipeline(
                            snapshot_path=snapshot_path,
                            output_dir=None,
                            account_balance=account_balance,
                            max_portfolio_risk=max_portfolio_risk,
                            sizing_method=sizing_method
                        )
                        
                        # Clean up temporary uploaded file if it was created
                        if uploaded_temp_path and uploaded_temp_path.exists():
                            uploaded_temp_path.unlink()
                            logger.info(f"Cleaned up temporary uploaded file: {uploaded_temp_path}")

                        if uploaded_temp_path and uploaded_temp_path.exists():
                            uploaded_temp_path.unlink()
                            logger.info(f"Cleaned up temporary uploaded file: {uploaded_temp_path}")

                        st.write("CHECKPOINT B: After pipeline returns")
                        logger.info("CHECKPOINT B: After pipeline returns")
                        
                        # Store all results in session state for inspection
                        st.session_state['pipeline_results'] = {k: sanitize_for_arrow(v) for k, v in results.items() if isinstance(v, pd.DataFrame)}
                        st.write("CHECKPOINT C: After session_state assignment")
                        logger.info("CHECKPOINT C: After session_state assignment")
                        st.success(f"✅ Full pipeline completed. {len(results.get('final_trades', pd.DataFrame()))} final trades selected.")

                        # Diagnostic Instrumentation for Step 9A/9B outputs
                        if 'timeframes' in st.session_state['pipeline_results'] and st.session_state['pipeline_results']['timeframes'].empty:
                            st.warning("⚠️ Step 9A (Timeframes) produced an empty DataFrame.")
                        if 'selected_contracts' in st.session_state['pipeline_results'] and st.session_state['pipeline_results']['selected_contracts'].empty:
                            st.warning("⚠️ Step 9B (Selected Contracts) produced an empty DataFrame.")
                        st.write("CHECKPOINT D: After Step 9A/9B output diagnostics")
                        logger.info("CHECKPOINT D: After Step 9A/9B output diagnostics")

                        # Optional Hardening: Add schema assertion after pipeline returns
                        if 'validated_data' in st.session_state['pipeline_results']:
                            df_validated_output = st.session_state['pipeline_results']['validated_data']
                            required_cols_post_pipeline = ['Signal_Type', 'Regime']
                            missing_post_pipeline = [col for col in required_cols_post_pipeline if col not in df_validated_output.columns]
                            if missing_post_pipeline:
                                error_msg = (
                                    f"❌ Post-pipeline schema assertion failed: Missing columns {missing_post_pipeline} "
                                    "in validated_data. This indicates an unexpected upstream schema alteration."
                                )
                                logger.error(error_msg)
                                st.error(error_msg)
                                raise ValueError(error_msg)
                            else:
                                st.info("✅ Post-pipeline schema assertion passed for [Signal_Type, Regime].")
                    st.write("CHECKPOINT E: After schema assertion")
                    logger.info("CHECKPOINT E: After schema assertion")

            except Exception as e:
                st.error(f"❌ Full pipeline failed: {e}")
                st.exception(e)
                st.write("CHECKPOINT F: In exception handler")
                logger.info("CHECKPOINT F: In exception handler")
    
    with col2:
        if 'pipeline_results' in st.session_state:
            results = st.session_state['pipeline_results']
            is_live_mode = st.session_state.get('live_snapshot_mode', False)
            
            if is_live_mode:
                # LIVE SNAPSHOT MODE: Display Step 2 data only
                st.info("🔴 **Live Snapshot Mode** - Displaying Step 2 enriched data (full pipeline bypassed)")
                
                df_snapshot = results.get('snapshot', pd.DataFrame())
                
                # Summary metrics for live snapshot
                st.subheader("📊 Live Snapshot Summary")
                summary_cols = st.columns(4)
                
                with summary_cols[0]:
                    st.metric("Total Tickers", len(df_snapshot))
                
                with summary_cols[1]:
                    if 'HV_30_D_Cur' in df_snapshot.columns:
                        hv_populated = df_snapshot['HV_30_D_Cur'].notna().sum()
                        st.metric("HV Coverage", f"{hv_populated}/{len(df_snapshot)}")
                    else:
                        st.metric("HV Coverage", "N/A")
                
                with summary_cols[2]:
                    if 'IV_30_D_Call' in df_snapshot.columns:
                        iv_populated = df_snapshot['IV_30_D_Call'].notna().sum()
                        st.metric("IV Coverage", f"{iv_populated}/{len(df_snapshot)}")
                    else:
                        st.metric("IV Coverage", "N/A")
                
                with summary_cols[3]:
                    if 'data_source' in df_snapshot.columns:
                        source = df_snapshot['data_source'].iloc[0] if len(df_snapshot) > 0 else "N/A"
                        st.metric("Data Source", source)
                    else:
                        st.metric("Data Source", "N/A")
                
                # Display Step 2 data with important columns
                st.subheader("📋 Step 2 Enriched Data")
                
                identifier = 'Ticker' if 'Ticker' in df_snapshot.columns else 'Symbol'
                display_cols = [identifier]
                
                # Add core columns if present
                for col in ['last_price', 'HV_10_D_Cur', 'HV_30_D_Cur', 'IV_30_D_Call', 
                           'hv_slope', 'volatility_regime', 'data_source']:
                    if col in df_snapshot.columns:
                        display_cols.append(col)
                
                st.dataframe(df_snapshot[display_cols], use_container_width=True, height=400)
                
                # Show volatility regime distribution if available
                if 'volatility_regime' in df_snapshot.columns:
                    st.subheader("📈 Volatility Regime Distribution")
                    regime_counts = df_snapshot['volatility_regime'].value_counts()
                    st.bar_chart(regime_counts)
                
                # Download button
                csv = df_snapshot.to_csv(index=False)
                st.download_button(
                    label="📥 Download Step 2 Data (CSV)",
                    data=csv,
                    file_name=f"step2_live_snapshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
            else:
                # LEGACY MODE: Display full pipeline results
                final_trades_count = len(results.get('final_trades', pd.DataFrame()))
                st.metric("Final Trades Selected", final_trades_count)
                st.write("CHECKPOINT G: After final trades metric")
                logger.info("CHECKPOINT G: After final trades metric")
                
                # Display summary metrics from the pipeline results
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
                st.write("CHECKPOINT H: After summary metrics")
                logger.info("CHECKPOINT H: After summary metrics")
                
                st.subheader("Detailed Results")
                selected_step = st.selectbox("View Output for Step:", list(results.keys()))
                if selected_step:
                    df_output = results[selected_step]
                    st.write(f"CHECKPOINT I: Before rendering {selected_step}")
                    logger.info(f"CHECKPOINT I: Before rendering {selected_step}")
                    
                    # Defensive checks for rendering large/empty DataFrames
                    if selected_step in ['timeframes', 'selected_contracts']:
                        if df_output.empty:
                            st.info(f"No data available for {selected_step}.")
                        elif df_output.shape[1] > 50:
                            st.warning(f"DataFrame for {selected_step} has {df_output.shape[1]} columns, which might be too wide for display. Displaying head only.")
                            st.dataframe(df_output.head(), use_container_width=True, height=400)
                        else:
                            st.dataframe(df_output, use_container_width=True, height=400)
                    else:
                        st.dataframe(df_output, use_container_width=True, height=400)
                    
                    st.write(f"CHECKPOINT J: After rendering {selected_step}")
                    logger.info(f"CHECKPOINT J: After rendering {selected_step}")
                    
                    st.write(f"Shape: {df_output.shape}")
                    st.write("Columns:", df_output.columns.tolist())
                    
                    csv = df_output.to_csv(index=False)
                    st.download_button(
                        label=f"📥 Download {selected_step} Output CSV",
                        data=csv,
                        file_name=f"{selected_step}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
                    st.write(f"CHECKPOINT K: After download button for {selected_step}")
                logger.info(f"CHECKPOINT K: After download button for {selected_step}")
    
    st.divider()
    
    # ========================================
    # MANAGE VIEW
    # ========================================
elif st.session_state.view == "manage":
    # Back button
    if st.button("← Back to Home"):
        set_view("home")
    
    st.title("🧪 Manage Positions")
    st.markdown("Monitor and revalidate existing option positions.")
    
    st.divider()
    
    INPUT_PATH = "/Users/haniabadi/Documents/Windows/Positions_Account_.csv"
    
    # === Step 1: Load and Clean ===
    if st.sidebar.button("🔍 Step 1: Load + Clean"):
        try:
            with st.spinner("Loading and cleaning data..."):
                df_input, snapshot_path = phase1_load_and_clean(input_path=INPUT_PATH)
                st.session_state["df_input"] = df_input
                st.success(f"✅ Loaded and cleaned {len(df_input)} rows")

                with st.expander("📥 Cleaned DataFrame (Step 1)", expanded=True):
                    st.dataframe(sanitize_for_arrow(df_input), use_container_width=True)

                st.subheader("📦 Unique Symbols")
                st.write(df_input["Symbol"].dropna().unique())

        except Exception as e:
            st.error(f"❌ Error in Step 1: {e}")
            st.exception(e)

    # === Step 2: Parse Symbols + Tag Strategy ===
    if st.sidebar.button("🔎 Step 2: Parse + Tag"):
        try:
            df_input = st.session_state.get("df_input")
            if df_input is None:
                st.warning("⚠️ Run Step 1 first.")
            else:
                with st.spinner("Parsing symbols and tagging strategies..."):
                    df_parsed = phase2_run_all(df_input)
                    st.session_state["df_parsed"] = df_parsed
                    st.success("✅ Symbols parsed and strategies tagged")

                    with st.expander("🔎 Parsed DataFrame (Full)", expanded=True):
                        st.dataframe(sanitize_for_arrow(df_parsed), use_container_width=True)

                    st.subheader("🧪 Parsed Columns")
                    st.write(df_parsed.columns.tolist())

                    st.subheader("🔑 Key Fields Snapshot")
                    sample_cols = [col for col in ["Symbol", "Underlying", "Expiration", "OptionType", "Strike", "Strategy", "TradeID"] if col in df_parsed.columns]
                    st.dataframe(sanitize_for_arrow(df_parsed[sample_cols].head(20)))

        except Exception as e:
            st.error(f"❌ Error in Step 2: {e}")
            st.exception(e)
