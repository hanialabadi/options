import streamlit as st
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)

try:
    from core.shared.data_layer.duckdb_utils import connect_read_only as _connect_ro
except ImportError:
    import duckdb as _duckdb
    def _connect_ro(path): return _duckdb.connect(path, read_only=True)

def get_perception_db_path(core_project_root):
    """Find the active DuckDB path."""
    from core.shared.data_contracts.config import PIPELINE_DB_PATH
    return PIPELINE_DB_PATH


def get_perception_data(core_project_root, run_id=None):
    """
    Load Cycle 1 Perception data from DuckDB (Read-Only).
    """
    db_path = get_perception_db_path(core_project_root)
    if not db_path:
        return pd.DataFrame()

    try:
        with _connect_ro(str(db_path)) as con:
            # Check for tables in order of authority
            tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").df()['table_name'].tolist()
            
            if 'enriched_legs_v1' in tables:
                table_name = 'enriched_legs_v1'
            elif 'clean_legs_v2' in tables:
                table_name = 'clean_legs_v2'
            else:
                table_name = 'clean_legs'
            
            # Check if table exists
            table_check = con.execute(f"""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = '{table_name}' AND table_schema = 'main'
            """).fetchone()[0] > 0
            
            if not table_check:
                return pd.DataFrame()

            if run_id:
                query = f"SELECT * FROM {table_name} WHERE run_id = ? ORDER BY Snapshot_TS DESC"
                return con.execute(query, [run_id]).df()
            else:
                return con.execute(f"SELECT * FROM {table_name} ORDER BY Snapshot_TS DESC").df()
                
    except Exception as e:
        logger.error(f"Error loading perception data: {e}")
        return pd.DataFrame()


def render_perception_view(core_project_root, sanitize_func):
    """
    Cycle 1: Perception View.
    Displays the current state of active positions as seen by the Management Engine.
    
    RAG Authority: McMillan / Passarelli (Ground Truth Projection)
    """
    st.title("📊 Cycle 1: Perception Loop")
    
    # --- Ingestion Status Panel ---
    db_path = get_perception_db_path(core_project_root)
    if db_path:
        try:
            with _connect_ro(str(db_path)) as con:
                log_exists = con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'cycle1_ingest_log'").fetchone()[0] > 0
                if log_exists:
                    last_ingest = con.execute("SELECT * FROM cycle1_ingest_log ORDER BY ingestion_ts DESC LIMIT 1").df()
                    if not last_ingest.empty:
                        row = last_ingest.iloc[0]
                        try:
                            _ingest_ts = pd.to_datetime(row['ingestion_ts'])
                            _ts_str = _ingest_ts.strftime("%b %d %Y  %I:%M %p")
                        except Exception:
                            _ts_str = str(row['ingestion_ts'])
                        st.info(f"🧊 **Current Snapshot:** {_ts_str} | **File:** `{Path(row['source_file_path']).name}` | **Rows:** {row['row_count']}")
        except Exception as e:
            logger.warning(f"Could not load ingest log: {e}")

    # --- Cycle 1 Ingestion Controls ---
    with st.expander("📥 Ingestion Control Center", expanded=True):
        c1, c2 = st.columns(2)
        
        with c1:
            st.markdown("### Manual Upload")
            uploaded_file = st.file_uploader(
                "Upload Fidelity Positions (CSV)",
                type=["csv"],
                help="Upload a fresh Positions_All_Accounts.csv from Fidelity."
            )
            manual_ingest_clicked = False
            if uploaded_file:
                manual_ingest_clicked = st.button("Ingest Uploaded File", width="stretch")
            
        with c2:
            st.markdown("### Local Ingest")
            from core.management.cycle1.ingest.clean import CANONICAL_INPUT_PATH
            canonical_exists = Path(CANONICAL_INPUT_PATH).exists()
            if canonical_exists:
                st.caption(f"Source: `data/brokerage_inputs/fidelity_positions.csv`")
            elif not uploaded_file:
                # Only show the missing-file warning when no manual upload is in progress
                st.caption("No local file found — use Manual Upload above.")
            local_ingest_clicked = st.button("Ingest Latest Local Download", width="stretch", disabled=not canonical_exists)

        allow_system_time = st.checkbox(
            "Allow System Time Fallback (Manual Upload)",
            value=False,
            help="For manually edited or incomplete CSVs only. Production ingest should always use broker 'As of' time."
        )

        # Trigger Logic
        if manual_ingest_clicked or local_ingest_clicked:
            from core.management.cycle1.ingest.clean import phase1_load_and_clean_positions
            from core.management.cycle1.identity.parse import phase2_run_all
            from core.management.cycle1.snapshot.snapshot import save_clean_snapshot

            try:
                with st.spinner("Hardening Ledger..."):
                    target_path = None
                    _original_filename = None
                    if manual_ingest_clicked:
                        _original_filename = uploaded_file.name
                        # Save with original filename into brokerage_inputs/ so run_all.py
                        # can find it as "latest CSV" when the Doctrine Engine runs next.
                        _brokerage_dir = Path("data/brokerage_inputs")
                        _brokerage_dir.mkdir(parents=True, exist_ok=True)
                        target_path = _brokerage_dir / _original_filename
                        target_path.write_bytes(uploaded_file.getvalue())
                    else:
                        from core.management.cycle1.ingest.clean import CANONICAL_INPUT_PATH
                        target_path = Path(CANONICAL_INPUT_PATH)

                    # 1. Phase 1: Ingest (Clean)
                    df_clean, _ = phase1_load_and_clean_positions(
                        input_path=target_path,
                        save_snapshot=False, # We save the structured snapshot instead
                        allow_system_time=allow_system_time
                    )

                    if not df_clean.empty:
                        # 2. Phase 2: Identity Resolution (Required for TradeID/LegID)
                        df_parsed = phase2_run_all(df_clean)

                        # 3. Strip Interpretive Fields (Phase Creep Prevention)
                        # RAG: Neutrality Mandate. No interpretive fields in Cycle 1 Ledger.
                        interpretive_cols = [
                            'Strategy', 'Structure', 'LegRole', 'LegIndex', 'LegCount',
                            'Premium_Estimated', 'Structure_Valid', 'Validation_Errors',
                            'Needs_Structural_Fix', 'Is_Optionable', 'Stock_Used_In_Options',
                            'Stock_Option_Status', 'Option_Eligibility', 'Option_Usage',
                            'Covered_Call_Contracts', 'Covered_Call_Coverage_Ratio', 'Covered_Call_Stock_Shares'
                        ]
                        df_snapshot_input = df_parsed.drop(columns=[c for c in interpretive_cols if c in df_parsed.columns]).copy()

                        # 4. Persist to DuckDB (Perception Ledger)
                        df_snapshot, _, run_id, _, _ = save_clean_snapshot(
                            df_snapshot_input,
                            source_file_path=str(target_path),
                            ingest_context="ui_manual_upload" if manual_ingest_clicked else "ui_local_ingest"
                        )
                        
                        _src_name = _original_filename or (Path(str(target_path)).name if target_path else "uploaded file")
                        st.success(f"✅ **Ingestion Complete** — {len(df_snapshot)} positions hardened from `{_src_name}`")
                        if manual_ingest_clicked:
                            st.caption("File saved to `data/brokerage_inputs/` — go to Position Monitor → Run Engine tab to update recommendations.")
                        # Clear Streamlit cache so manage_view picks up the new file immediately
                        st.cache_data.clear()
                    else:
                        st.error("❌ Ingestion failed: No positions found or file invalid.")

            except Exception as e:
                st.error(f"❌ Ingestion Error: {e}")
                logger.error(f"UI Ingestion failed: {e}", exc_info=True)

    st.divider()
    st.markdown("### Ground Truth Ledger")
    st.caption("Verbatim projection of the frozen broker-reported state. No interpretation, no enrichment.")
    
    # --- Run ID Selection ---
    db_path = get_perception_db_path(core_project_root)
    run_ids = []
    if db_path:
        try:
            with _connect_ro(str(db_path)) as con:
                tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").df()['table_name'].tolist()
                
                # We want to aggregate run_ids from all relevant tables
                all_run_ids = pd.DataFrame()
                for t in ['enriched_legs_v1', 'clean_legs_v2', 'clean_legs']:
                    if t in tables:
                        df = con.execute(f"SELECT DISTINCT run_id, MAX(Snapshot_TS) as ts FROM {t} GROUP BY run_id").df()
                        all_run_ids = pd.concat([all_run_ids, df])
                
                if not all_run_ids.empty:
                    run_ids_df = all_run_ids.groupby('run_id').agg({'ts': 'max'}).sort_values('ts', ascending=False).reset_index()
                    run_ids = run_ids_df['run_id'].tolist()
        except Exception as e:
            logger.warning(f"Could not load run_ids: {e}")

    if not run_ids:
        st.warning("No perception data found in DuckDB. Run Cycle 1 to populate.")
        return

    selected_run_id = st.selectbox("Select Run ID", run_ids, index=0)
    
    df_latest = get_perception_data(core_project_root, run_id=selected_run_id)
    
    if df_latest.empty:
        st.warning(f"No data found for Run ID: {selected_run_id}")
        return

    st.subheader(f"Snapshot: {selected_run_id}")
    
    # --- Data Source Panel (RAG: Truth Verification) ---
    with st.container(border=True):
        st.markdown("### 🛡️ Data Source Verification")
        d1, d2, d3, d4 = st.columns(4)
        with d1:
            st.markdown("**Source:** `DuckDB`")
        with d2:
            # Determine which table this run_id came from
            source_table = "Unknown"
            if db_path:
                with _connect_ro(str(db_path)) as con:
                    tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").df()['table_name'].tolist()
                    for t in ['enriched_legs_v1', 'clean_legs_v2', 'clean_legs']:
                        if t in tables:
                            count = con.execute(f"SELECT COUNT(*) FROM {t} WHERE run_id = ?", [selected_run_id]).fetchone()[0]
                            if count > 0:
                                source_table = t
                                break
            st.markdown(f"**Table:** `{source_table}`")
        with d3:
            st.markdown(f"**Run ID:** `{selected_run_id}`")
        with d4:
            st.markdown(f"**Cycle:** `1 (Perception Only)`")

    # Summary Metrics (Facts Only)
    m1, m2, m3 = st.columns(3)
    with m1:
        st.metric("Active Legs", len(df_latest))
    with m2:
        st.metric("Unique Accounts", df_latest['Account'].nunique())
    with m3:
        st.metric("Schema Hash", df_latest['Schema_Hash'].iloc[0] if 'Schema_Hash' in df_latest.columns else "N/A")

    # --- CLI Parity Panel ---
    st.divider()
    st.subheader("🖥️ CLI Parity Verification")
    
    if db_path:
        try:
            with _connect_ro(str(db_path)) as con:
                # ARCHIVED count (Closed in this run or previously)
                # For parity, we want to show what the CLI shows: ARCHIVED, ANCHORED, PRESERVED
                # These are lifecycle events.
                
                # We need to look at the entry_anchors table for this run_id or around this time
                # Actually, the CLI prints these based on the delta between DB and current snapshot.
                # To show them in the dashboard, we should ideally have logged them.
                # Since we have cycle1_ingest_log, let's see if we can derive them.
                
                # Find the max snapshot TS for this run across all tables
                tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").df()['table_name'].tolist()
                max_ts = None
                for t in ['enriched_legs_v1', 'clean_legs_v2', 'clean_legs']:
                    if t in tables:
                        ts = con.execute(f"SELECT MAX(Snapshot_TS) FROM {t} WHERE run_id = ?", [selected_run_id]).fetchone()[0]
                        if ts:
                            if max_ts is None or ts > max_ts:
                                max_ts = ts
                
                # For now, let's show the counts from entry_anchors that are relevant.
                anchored_count = 0
                if max_ts:
                    anchored_count = con.execute("SELECT COUNT(*) FROM entry_anchors WHERE Entry_Snapshot_TS = ?", [max_ts]).fetchone()[0]
                
                # Closed positions (Is_Active = FALSE)
                # This is harder to attribute to a specific run without a join or better logging.
                # But we can show total closed.
                total_closed = con.execute("SELECT COUNT(*) FROM entry_anchors WHERE Is_Active = FALSE").fetchone()[0]
                
                # Preserved (Active and NOT new in this run)
                total_active = con.execute("SELECT COUNT(*) FROM entry_anchors WHERE Is_Active = TRUE").fetchone()[0]
                preserved_count = total_active - anchored_count
                
                p1, p2, p3 = st.columns(3)
                with p1:
                    st.metric("ANCHORED (New)", anchored_count)
                with p2:
                    st.metric("PRESERVED (Active)", preserved_count)
                with p3:
                    st.metric("ARCHIVED (Total Closed)", total_closed)
                    
        except Exception as e:
            st.error(f"Error loading parity metrics: {e}")

    # Position Table (Verbatim Projection)
    st.subheader("Cycle 1: Ground Truth Ledger")
    st.info(f"💡 **Note: Verbatim projection from DuckDB ({source_table}). Scoped by run_id.**")
    
    # Clean up legacy columns: Drop columns that are entirely NULL in the latest snapshot
    df_render = df_latest.dropna(axis=1, how='all').copy()
    
    # Render columns verbatim
    st.dataframe(sanitize_func(df_render), width='stretch')
    
    with st.expander("📖 Audit Guide: Frozen vs Vital Signs"):
        st.markdown("""
        ### Identity & Economic Anchors (Frozen Once)
        These fields define the contract and cost basis. They are immutable after the first observation.
        - **Symbol (OCC):** Canonical contract identity.
        - **Basis:** Total cost basis reported by broker.
        - **Quantity:** Signed position size.
        
        ### Sensitivity Anchors (Frozen Per Snapshot)
        These fields record the market state at the moment of perception.
        - **UL Last:** Underlying price at snapshot.
        - **Greeks (Δ, Γ, ν, θ):** Raw sensitivities reported by broker.
        """)
