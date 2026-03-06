import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import os
import sys
import logging
from pathlib import Path


# US market holidays (NYSE) — static list, update annually
_NYSE_HOLIDAYS = {
    date(2025, 1, 1), date(2025, 1, 20), date(2025, 2, 17), date(2025, 4, 18),
    date(2025, 5, 26), date(2025, 6, 19), date(2025, 7, 4), date(2025, 9, 1),
    date(2025, 11, 27), date(2025, 12, 25),
    date(2026, 1, 1), date(2026, 1, 19), date(2026, 2, 16), date(2026, 4, 3),
    date(2026, 5, 25), date(2026, 6, 19), date(2026, 7, 3), date(2026, 9, 7),
    date(2026, 11, 26), date(2026, 12, 25),
}


def _is_trading_day(d: date) -> bool:
    """Return True if d is a NYSE trading day (Mon–Fri, not a holiday)."""
    return d.weekday() < 5 and d not in _NYSE_HOLIDAYS


def _snapshot_is_stale(snapshot_ts: datetime) -> tuple[bool, str]:
    """
    Determine if a snapshot is stale in a trading-day-aware manner.

    A snapshot is NOT stale if:
    - No trading session has closed since it was taken.

    Rules:
    - If today is Sat/Sun/holiday AND snapshot is from the most recent prior
      trading day → NOT stale (no new data exists yet).
    - If today is a trading day AND snapshot was taken before today's open (4am ET)
      AND a full session has passed → stale.
    - Simple fallback: stale if >24h AND at least one trading day has elapsed.

    Returns: (is_stale: bool, reason: str)
    """
    now = datetime.now()
    snap_date = snapshot_ts.date()
    today = now.date()

    # Walk back to find the last trading session close date
    check = today
    # If today is not a trading day (weekend/holiday), wind back to last trading day
    days_back = 0
    while not _is_trading_day(check):
        check -= timedelta(days=1)
        days_back += 1
        if days_back > 10:
            break  # Safety — should never happen
    last_trading_day = check

    # Snapshot is from the last trading session (or later) → not stale
    if snap_date >= last_trading_day:
        age_h = (now - snapshot_ts).total_seconds() / 3600
        return False, f"From last trading session ({age_h:.0f}h ago — weekend/holiday, no new data)"

    # Snapshot is older than the last trading session → stale
    age_h = (now - snapshot_ts).total_seconds() / 3600
    return True, f"Missed {(last_trading_day - snap_date).days} trading session(s); {age_h:.0f}h old"

logger = logging.getLogger(__name__)

try:
    from core.shared.data_layer.duckdb_utils import connect_read_only as _connect_ro
except ImportError:
    import duckdb as _duckdb
    def _connect_ro(path): return _duckdb.connect(str(path), read_only=True)

def _resolve_pipeline_db_path(debug_mode: bool) -> Path:
    from core.shared.data_contracts.config import PIPELINE_DB_PATH, DEBUG_PIPELINE_DB_PATH
    return DEBUG_PIPELINE_DB_PATH if debug_mode else PIPELINE_DB_PATH

def get_latest_step8_artifact(scan_output_dir):
    """
    Enforce Single Source of Truth: Load Step8 + Forensic Artifacts.
    Sorted by modification time, latest only.
    
    RAG: Truth Layer. Prefers DuckDB v_latest_scan_results over CSV.
    """
    debug_mode = st.session_state.get("debug_mode", False)
    db_path = _resolve_pipeline_db_path(debug_mode)

    df_primary = pd.DataFrame()
    mod_time = None
    ts = None
    latest_file_path_str = "N/A"

    output_dir = scan_output_dir
    forensic = {}

    # 1. Try DuckDB (Authoritative Truth Layer)
    if db_path.exists():
        try:
            with _connect_ro(str(db_path)) as con:
                tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").df()['table_name'].tolist()
                if 'v_latest_scan_results' in tables:
                    df_latest_ready = con.execute("SELECT * FROM v_latest_scan_results").df()
                    
                    df_history_all = pd.DataFrame()
                    if 'scan_results_history' in tables:
                        df_history_all = con.execute("SELECT * FROM scan_results_history").df()
                        
                        if pd.api.types.is_string_dtype(df_history_all['scan_timestamp']):
                            df_history_all['scan_timestamp'] = pd.to_datetime(df_history_all['scan_timestamp'], errors='coerce')

                    if not df_latest_ready.empty:
                        df_primary = df_latest_ready.copy()
                        mod_time = df_primary['scan_timestamp'].max()
                        ts = mod_time.strftime("%Y%m%d_%H%M%S")
                        latest_file_path_str = str(db_path)
                        logger.info("✅ Loaded scan results from DuckDB (v_latest_scan_results)")
                        
                        latest_run_id = df_primary['run_id'].iloc[0] if 'run_id' in df_primary.columns and not df_primary.empty else None
                        if latest_run_id and not df_history_all.empty:
                            df_history_all = df_history_all[df_history_all['run_id'] == latest_run_id].copy()
                        
                        # Handle legacy/alternate column names — normalize to Execution_Status
                        if 'Trade_Status' in df_primary.columns:
                            if 'Execution_Status' not in df_primary.columns:
                                df_primary = df_primary.rename(columns={'Trade_Status': 'Execution_Status'})
                            else:
                                df_primary = df_primary.drop(columns=['Trade_Status'])
                        if 'acceptance_status' in df_primary.columns and 'Execution_Status' not in df_primary.columns:
                            df_primary = df_primary.rename(columns={'acceptance_status': 'Execution_Status'})
                        if 'Gate_Reason' in df_primary.columns:
                            if 'Block_Reason' not in df_primary.columns:
                                df_primary = df_primary.rename(columns={'Gate_Reason': 'Block_Reason'})
                            else:
                                df_primary = df_primary.drop(columns=['Gate_Reason'])

                        if 'Trade_Status' in df_history_all.columns:
                            if 'Execution_Status' not in df_history_all.columns:
                                df_history_all = df_history_all.rename(columns={'Trade_Status': 'Execution_Status'})
                            else:
                                df_history_all = df_history_all.drop(columns=['Trade_Status'])
                        if 'acceptance_status' in df_history_all.columns and 'Execution_Status' not in df_history_all.columns:
                            df_history_all = df_history_all.rename(columns={'acceptance_status': 'Execution_Status'})
                        if 'Gate_Reason' in df_history_all.columns:
                            if 'Block_Reason' not in df_history_all.columns:
                                df_history_all = df_history_all.rename(columns={'Gate_Reason': 'Block_Reason'})
                            else:
                                df_history_all = df_history_all.drop(columns=['Gate_Reason'])

                        forensic = {
                            'acceptance_all': df_history_all,
                            'acceptance_ready': df_primary,  # will be overridden by Step12_Ready CSV below
                            'thesis_envelopes': df_primary   # will be overridden by Step12_Ready CSV below
                        }

                        # Also load CSV artifacts for funnel metrics (snapshot row count, contract count)
                        # DuckDB is authoritative for scan results; CSVs provide supplemental counts.
                        # Use run_id (e.g. "scan_20260220_134031") to derive the CSV timestamp suffix.
                        _csv_ts = None
                        if 'run_id' in df_primary.columns and not df_primary.empty:
                            _run_id = df_primary['run_id'].iloc[0]
                            if isinstance(_run_id, str) and _run_id.startswith('scan_'):
                                _csv_ts = _run_id[len('scan_'):]  # "20260220_134031"
                        if not _csv_ts:
                            _csv_ts = ts  # fallback to strftime-derived ts
                        if _csv_ts and output_dir.exists():
                            csv_supplemental = {
                                'snapshot': f"Step2_Snapshot_{_csv_ts}.csv",
                                'filtered': f"Step3_Filtered_{_csv_ts}.csv",
                                'charted': f"Step5_Charted_{_csv_ts}.csv",
                                'validated_data': f"Step6_Validated_{_csv_ts}.csv",
                                'selected_contracts': f"Step9B_SelectedContracts_{_csv_ts}.csv",
                                'recalibrated_contracts': f"Step10_Filtered_{_csv_ts}.csv",
                                'evaluated_strategies': f"Step11_Evaluated_{_csv_ts}.csv",
                                # Step12_Acceptance is the full 561-row run output with all statuses.
                                # Overrides the sparse DuckDB scan_results_history for row count accuracy.
                                'acceptance_all': f"Step12_Acceptance_{_csv_ts}.csv",
                                # Step12_Ready is the full 305-column READY subset needed for card rendering
                                # (last_price, RSI, ADX, Bid, Ask, Delta, Contract_Symbol, etc.)
                                # Overrides sparse 9-column DuckDB df_primary as acceptance_ready and thesis_envelopes.
                                'acceptance_ready': f"Step12_Ready_{_csv_ts}.csv",
                                'thesis_envelopes': f"Step12_Ready_{_csv_ts}.csv",
                            }
                            for key, fname in csv_supplemental.items():
                                fpath = output_dir / fname
                                if fpath.exists():
                                    try:
                                        df_csv = pd.read_csv(fpath)
                                        # Normalize acceptance_status → Execution_Status in CSV too
                                        if key in ('acceptance_all', 'acceptance_ready') and 'acceptance_status' in df_csv.columns and 'Execution_Status' not in df_csv.columns:
                                            df_csv = df_csv.rename(columns={'acceptance_status': 'Execution_Status'})
                                        forensic[key] = df_csv
                                    except Exception:
                                        pass

                        print(f"AUTHORITATIVE LOAD: {latest_file_path_str} (Modified: {mod_time})")
                        return df_primary, mod_time, forensic
        except Exception as e:
            logger.warning(f"DuckDB scan load failed: {e}.")
            if debug_mode:
                st.error("🛑 DEBUG MODE: DuckDB load failed. CSV fallback is disabled.")
                return pd.DataFrame(), None, {}
            
    if debug_mode:
        st.error("🛑 DEBUG MODE: DuckDB missing. CSV fallback is disabled.")
        return pd.DataFrame(), None, {}

    # 2. Fallback to CSV (Legacy/Bootstrap)
    marker_path = output_dir / "LATEST_SCAN_COMPLETE"
    
    if not marker_path.exists():
        st.error("🛑 HARD GATE FAILURE: output/LATEST_SCAN_COMPLETE missing. Management views disabled.")
        return pd.DataFrame(), None, {}

    step12_files = sorted(output_dir.glob("Step12_Acceptance_*.csv"), key=os.path.getmtime, reverse=True)
    if not step12_files:
        st.error("🛑 HARD GATE FAILURE: No Step12_Acceptance_*.csv found in output/. Management views disabled.")
        return pd.DataFrame(), None, {}

    latest_file = step12_files[0]
    latest_file_path_str = str(latest_file)
    mod_time = datetime.fromtimestamp(os.path.getmtime(latest_file))
    ts = latest_file.stem.split("Step12_Acceptance_")[-1]
    
    df_all_strategies = pd.read_csv(latest_file)

    # Handle legacy column names - only rename if target doesn't exist
    if 'Trade_Status' in df_all_strategies.columns:
        if 'Execution_Status' not in df_all_strategies.columns:
            df_all_strategies = df_all_strategies.rename(columns={'Trade_Status': 'Execution_Status'})
        else:
            # Both exist - drop the legacy column to avoid duplicates
            df_all_strategies = df_all_strategies.drop(columns=['Trade_Status'])
    if 'Gate_Reason' in df_all_strategies.columns:
        if 'Block_Reason' not in df_all_strategies.columns:
            df_all_strategies = df_all_strategies.rename(columns={'Gate_Reason': 'Block_Reason'})
        else:
            df_all_strategies = df_all_strategies.drop(columns=['Gate_Reason'])

    # Use .loc for safer filtering (handles edge cases better)
    if 'Execution_Status' in df_all_strategies.columns:
        df_primary = df_all_strategies.loc[df_all_strategies['Execution_Status'] == 'READY'].copy()
    else:
        logger.warning("⚠️ Execution_Status column not found - returning empty DataFrame")
        df_primary = pd.DataFrame()
    
    mapping = {
        'snapshot': f"Step2_Snapshot_{ts}.csv",
        'filtered': f"Step3_Filtered_{ts}.csv",
        'charted': f"Step5_Charted_{ts}.csv",
        'validated_data': f"Step6_Validated_{ts}.csv",
        'selected_contracts': f"Step9B_SelectedContracts_{ts}.csv",
        'recalibrated_contracts': f"Step10_Filtered_{ts}.csv",
        'evaluated_strategies': f"Step11_Evaluated_{ts}.csv",
        'acceptance_all': df_all_strategies,
        'acceptance_ready': df_primary,
        'thesis_envelopes': df_primary
    }
    for key, fname_or_df in mapping.items():
        if isinstance(fname_or_df, str):
            fpath = output_dir / fname_or_df
            if fpath.exists():
                forensic[key] = pd.read_csv(fpath)
            else:
                logger.warning(f"⚠️ Forensic artifact missing: {fpath}")
        else:
            forensic[key] = fname_or_df
            
    print(f"AUTHORITATIVE LOAD: {latest_file_path_str} (Modified: {mod_time})")
    return df_primary, mod_time, forensic

@st.cache_data(ttl=300, show_spinner=False)
def get_iv_maturity_distribution():
    """
    Query actual per-ticker IV history depth from DuckDB and bucket into maturity tiers.
    Returns dict with real counts — no hardcoded estimates.
    Tiers (IVEngine contract): MATURE=120+d, PARTIAL=60-119d, IMMATURE=20-59d, EARLY=1-19d, MISSING=0d
    """
    try:
        from core.shared.data_layer.iv_term_history import get_iv_history_db_path
        import duckdb

        db_path = get_iv_history_db_path()
        if not db_path.exists():
            return None

        con = duckdb.connect(str(db_path), read_only=True)
        rows = con.execute("""
            SELECT ticker, COUNT(*) AS days_count
            FROM iv_term_history
            WHERE iv_30d IS NOT NULL
            GROUP BY ticker
        """).fetchall()
        con.close()

        if not rows:
            return None

        total = len(rows)
        mature = sum(1 for _, d in rows if d >= 120)
        partial = sum(1 for _, d in rows if 60 <= d < 120)
        immature = sum(1 for _, d in rows if 20 <= d < 60)
        early = sum(1 for _, d in rows if 1 <= d < 20)
        # "missing" = tickers in snapshot but not in iv_term_history — we can't know from here
        # so we only report what we have

        avg_days = sum(d for _, d in rows) / total if total else 0
        min_days = min(d for _, d in rows) if rows else 0
        max_days = max(d for _, d in rows) if rows else 0

        return {
            'total': total,
            'mature': mature,
            'partial': partial,
            'immature': immature,
            'early': early,
            'avg_days': avg_days,
            'min_days': min_days,
            'max_days': max_days,
        }
    except Exception as e:
        logger.error(f"❌ IV maturity distribution query failed: {e}")
        return None


@st.cache_data(ttl=300, show_spinner=False)
def get_market_stress_cached():
    """
    Check market stress with 5-minute cache to prevent I/O during every render.
    Returns: (stress_level, median_iv, stress_basis)
    """
    try:
        from core.shared.data_layer.market_stress_detector import classify_market_stress
        return classify_market_stress()
    except Exception as e:
        logger.error(f"❌ Market stress check failed: {e}")
        return ('UNKNOWN', 0.0, 'Error')

@st.cache_data(ttl=60, show_spinner=False)
def get_snapshot_info(path, core_project_root):
    """
    Extract metadata and quality metrics from a snapshot file.
    Cached for 60 seconds to avoid redundant file I/O during render.
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

        # Calculate IV History (Authoritative: Query DuckDB iv_term_history)
        # Phase 4: DuckDB is single source of truth for IV history
        try:
            from core.shared.data_layer.iv_term_history import get_iv_history_db_path, get_history_summary
            import duckdb

            db_path = get_iv_history_db_path()
            logger.info(f"📊 Checking IV history database: {db_path}")

            if db_path.exists():
                con = duckdb.connect(str(db_path), read_only=True)
                summary = get_history_summary(con)
                con.close()

                # Use median depth as representative IV history
                median_depth = summary.get('median_depth', 0)
                total_tickers = summary.get('total_tickers', 0)
                iv_history = int(median_depth) if median_depth is not None else 0

                logger.info(f"✅ IV history loaded: {iv_history} days median, {total_tickers} tickers")
            else:
                iv_history = 0
                logger.warning(f"⚠️ IV history database not found at {db_path} - run bootstrap or daily collection")
        except Exception as e:
            logger.error(f"❌ Failed to query IV history: {e}", exc_info=True)
            iv_history = 0

        return {
            'path': path,
            'filename': os.path.basename(path),
            'timestamp': mod_time,
            'tickers': len(df),
            'iv_coverage': coverage,
            'iv_history': iv_history
        }
    except Exception as e:
        logger.error(f"Error reading snapshot info: {e}")
        return None

def render_waitlist_table(df_waitlist):
    """
    Render WAITLIST (AWAIT_CONFIRMATION) strategies with wait conditions and TTL.

    Matches CLI output_formatter.py semantics.
    """
    if df_waitlist.empty:
        st.success("🟢 No trades in WAITLIST — all clear.")
        return

    st.info("""
    🟡 **WAITLIST** — Trades waiting on specific conditions to be satisfied.
    These strategies are valid but require confirmation (e.g., liquidity improvement, IV maturity, data refresh).
    """)

    # Normalize column names (handle both lowercase and uppercase from different sources)
    df_waitlist = df_waitlist.copy()

    # Create column mapping for common variations
    col_mapping = {
        'ticker': 'Ticker',
        'strategy_name': 'Strategy_Name',
        'strategy_type': 'Strategy_Type'
    }

    for old_col, new_col in col_mapping.items():
        if old_col in df_waitlist.columns and new_col not in df_waitlist.columns:
            df_waitlist[new_col] = df_waitlist[old_col]

    # Core columns for display
    display_cols = [
        "Ticker",
        "Strategy_Name",
        "Strategy_Type",
        "wait_progress",
        "conditions_met_count",
        "total_conditions",
        "wait_expires_at",
        "evaluation_count"
    ]

    # Use available columns
    existing_cols = [c for c in display_cols if c in df_waitlist.columns]

    import json as _json

    def _parse_json_field(val):
        """Parse a field that may be a list already or a JSON string."""
        if isinstance(val, list):
            return val
        if isinstance(val, str):
            try:
                return _json.loads(val)
            except Exception:
                return []
        return []

    # Calculate derived fields if not present
    if 'conditions_met_count' not in df_waitlist.columns and 'conditions_met' in df_waitlist.columns:
        df_waitlist['conditions_met_count'] = df_waitlist['conditions_met'].apply(
            lambda x: len(_parse_json_field(x))
        )

    if 'total_conditions' not in df_waitlist.columns and 'wait_conditions' in df_waitlist.columns:
        df_waitlist['total_conditions'] = df_waitlist['wait_conditions'].apply(
            lambda x: len(_parse_json_field(x))
        )

    st.dataframe(df_waitlist[existing_cols], width="stretch")

    # Show wait conditions detail for selected entry
    if not df_waitlist.empty and 'wait_conditions' in df_waitlist.columns:
        st.divider()
        st.subheader("📋 Wait Conditions Detail")

        ticker_col = 'Ticker' if 'Ticker' in df_waitlist.columns else 'ticker'
        strategy_col = 'Strategy_Name' if 'Strategy_Name' in df_waitlist.columns else 'strategy_name'

        selected_idx = st.selectbox(
            "Select trade to view conditions:",
            range(len(df_waitlist)),
            format_func=lambda x: f"{df_waitlist.iloc[x][ticker_col]} - {df_waitlist.iloc[x][strategy_col]}"
        )

        selected_row = df_waitlist.iloc[selected_idx]
        wait_conditions = _parse_json_field(selected_row.get('wait_conditions', []))
        conditions_met = _parse_json_field(selected_row.get('conditions_met', []))

        st.write(f"**Progress:** {selected_row.get('wait_progress', 0):.0%}")
        st.write(f"**Conditions:** {len(conditions_met)}/{len(wait_conditions)} satisfied")

        for condition in wait_conditions:
            if isinstance(condition, dict):
                condition_id = condition.get('condition_id', '')
                description = condition.get('description', 'No description')
                is_met = condition_id in conditions_met
                status_icon = "✅" if is_met else "⏳"
                st.markdown(f"{status_icon} {description}")

def start_fetch_job():
    """
    Start fetch job using threading (non-blocking, in-process).

    Executes run_snapshot() in a background thread instead of subprocess.
    Benefits:
    - No subprocess overhead
    - No detached process
    - Full logging integration
    - Errors propagate normally
    - No hard timeout kills

    Returns: (success, message, job_start_time, thread)
    Note: thread handle is returned for monitoring (can check thread.is_alive())
    """
    try:
        from datetime import datetime
        from scan_engine.step0_schwab_snapshot import run_snapshot
        import threading

        job_start_time = datetime.now()
        logger.info(f"Starting fetch job (threaded run_snapshot) at {job_start_time}")

        # Container to capture results/errors from thread
        result_container = {'df': None, 'error': None, 'completed': False}

        def _run_in_thread():
            """Thread worker function."""
            try:
                df = run_snapshot(
                    test_mode=False,
                    use_cache=True,
                    fetch_iv=True,
                    discovery_mode=False
                )
                result_container['df'] = df
                result_container['completed'] = True
                logger.info(f"✅ Fetch job completed successfully: {len(df)} tickers")
            except Exception as e:
                result_container['error'] = e
                result_container['completed'] = True
                logger.error(f"❌ Fetch job failed: {e}", exc_info=True)

        # Start background thread
        fetch_thread = threading.Thread(target=_run_in_thread, daemon=True)
        fetch_thread.start()

        # Store result container in session state for error checking
        import streamlit as st
        st.session_state.fetch_result_container = result_container

        return True, "Fetch job started (background thread)", job_start_time, fetch_thread

    except Exception as e:
        logger.error(f"Failed to start fetch job: {e}", exc_info=True)
        return False, f"Failed to start fetch: {str(e)}", None, None


def check_fetch_completion(job_start_time, snapshot_path_hint=None):
    """
    Check if fetch job completed by looking for NEW snapshot files created after job start.
    Returns: (is_complete, message)

    Note: snapshot_path_hint is ignored - we look for ANY new snapshot in the snapshots directory.
    """
    try:
        from datetime import datetime
        import os
        from pathlib import Path

        # Look in the snapshots directory for new files
        snapshots_dir = Path("data/snapshots")
        if not snapshots_dir.exists():
            return False, "Snapshots directory not found"

        # Find all snapshot files
        snapshot_files = sorted(
            snapshots_dir.glob("ivhv_snapshot_live_*.csv"),
            key=os.path.getmtime,
            reverse=True
        )

        if not snapshot_files:
            return False, "No snapshot files found (job may still be running)"

        # Check if the LATEST snapshot was created after job started
        latest_snapshot = snapshot_files[0]
        snapshot_mtime = datetime.fromtimestamp(os.path.getmtime(latest_snapshot))

        if snapshot_mtime > job_start_time:
            logger.info(f"Fetch complete: new snapshot created at {snapshot_mtime}: {latest_snapshot.name}")
            return True, f"Fresh data fetched successfully! ({latest_snapshot.name})"
        else:
            age_seconds = (datetime.now() - job_start_time).total_seconds()
            return False, f"Fetching data... ({age_seconds:.0f}s elapsed)"

    except Exception as e:
        logger.error(f"Error checking fetch completion: {e}")
        return False, f"Error: {str(e)}"


def render_iv_collection_badge():
    """
    Render IV collection health badge in the Data Plan section.

    Reads data/iv_collection_status.json written by collect_iv_daily.py.
    Shows:
      - Green:  collected today (automated 15:45 ET run succeeded)
      - Yellow: prior session IV — intraday scan / weekend / holiday (still usable)
      - Red:    last collection failed (needs manual intervention)
    """
    from scripts.cli.collect_iv_daily import read_iv_status, is_trading_day as _is_trading_day
    from datetime import date as _date

    status = read_iv_status()
    today = _date.today()
    today_str = today.isoformat()
    trading_today = _is_trading_day(today)

    if status is None:
        if trading_today:
            st.info("📡 **IV Collection:** No status yet — will collect automatically at 15:45 ET")
        else:
            st.info("📡 **IV Collection:** Market closed today (weekend/holiday) — no collection needed")
        return

    collected_date = status.get("date", "")
    ok = status.get("ok", False)
    message = status.get("message", "")
    ts_raw = status.get("timestamp", "")
    tickers_ok = status.get("tickers_ok", 0)
    tickers_total = status.get("tickers_total", 0)

    try:
        ts = datetime.fromisoformat(ts_raw)
        ts_label = ts.strftime("%b %d, %I:%M %p")
        days_old = (datetime.now() - ts).days
    except Exception:
        ts_label = ts_raw
        days_old = "?"

    coverage = f"{tickers_ok}/{tickers_total}" if tickers_total > 0 else ""
    coverage_str = f" | {coverage} tickers" if coverage else ""

    if not ok:
        st.error(
            f"⚠️ **IV Collection FAILED** — Last attempt: {ts_label}\n\n"
            f"Error: {message}\n\n"
            "Pipeline will use prior session IV for intraday scans (acceptable). "
            "Run manually: `python scripts/cli/collect_iv_daily.py --force`"
        )
    elif collected_date == today_str:
        st.success(f"📡 **IV Collected Today** ({ts_label}{coverage_str}) — {message}")
    elif not trading_today:
        # Weekend or NYSE holiday — prior session IV is correct, no collection expected
        reason = "weekend" if today.weekday() >= 5 else "market holiday"
        st.info(
            f"📡 **IV from {ts_label}** — {reason.capitalize()}, no collection today (normal). "
            f"Resumes next trading day at 15:45 ET.{coverage_str}"
        )
    else:
        # Trading day but not yet collected (intraday — before 15:45 ET)
        st.info(
            f"📡 **IV from {ts_label}** ({days_old}d ago{coverage_str}) — "
            "Using prior session IV for intraday scan. "
            "Automated collection runs at 15:45 ET today."
        )


def render_data_status_badge(info):
    """Render prominent data status badge at top of page."""
    if not info:
        st.error("🔴 **NO DATA** - Please fetch snapshot first")
        return

    age_hours = (datetime.now() - info['timestamp']).total_seconds() / 3600
    is_stale, stale_reason = _snapshot_is_stale(info['timestamp'])

    if age_hours < 4:
        st.success(f"🟢 **LIVE DATA** - Updated {info['timestamp'].strftime('%I:%M %p')} ({age_hours:.1f}h ago) | {info['tickers']} tickers")
    elif not is_stale:
        # Weekend/holiday — data is current for this session even if >24h old
        st.info(f"🟡 **CURRENT DATA** - From {info['timestamp'].strftime('%b %d, %I:%M %p')} ({age_hours:.0f}h ago) | {info['tickers']} tickers")
        st.caption(f"📅 Market closed (weekend/holiday) — no new data available. {stale_reason}")
    elif age_hours < 24:
        st.info(f"🟡 **RECENT DATA** - From {info['timestamp'].strftime('%b %d, %I:%M %p')} ({age_hours:.1f}h ago) | {info['tickers']} tickers")
    else:
        st.error(f"🔴 **STALE DATA** - From {info['timestamp'].strftime('%b %d, %I:%M %p')} ({age_hours:.0f}h old) | {info['tickers']} tickers")
        st.caption(f"⚠️ {stale_reason}. Fetch fresh data before scanning.")


def render_scan_view(core_project_root, scan_output_dir, sanitize_func, set_view_func):
    """
    Market Scan - Full Pipeline Orchestration.
    """
    from scan_engine.step0_resolve_snapshot import resolve_snapshot_path

    def _init_scan_view_session_state():
        if "fetch_data_intent" not in st.session_state:
            st.session_state.fetch_data_intent = False
        if "is_fetching_data" not in st.session_state:
            st.session_state.is_fetching_data = False
        if "fetch_job_start_time" not in st.session_state:
            st.session_state.fetch_job_start_time = None
        if "fetch_job_status_message" not in st.session_state:
            st.session_state.fetch_job_status_message = ""
        if "fetch_job_success" not in st.session_state:
            st.session_state.fetch_job_success = False

        if "run_scan_intent" not in st.session_state:
            st.session_state.run_scan_intent = False
        if "is_running_pipeline" not in st.session_state:
            st.session_state.is_running_pipeline = False
        # pipeline_run_metadata is already initialized in dashboard.py, so we'll reuse it

    _init_scan_view_session_state()

    # Auto-load latest scan artifacts into session state if not already present.
    # This ensures CLI-run scans are visible in the funnel without needing a UI-triggered run.
    if 'pipeline_results' not in st.session_state:
        try:
            _df_auto, _mod_auto, _forensic_auto = get_latest_step8_artifact(scan_output_dir)
            if _df_auto is not None:
                _auto_results = {
                    'acceptance_ready': _forensic_auto.get('acceptance_ready', pd.DataFrame()),
                    'acceptance_all': _forensic_auto.get('acceptance_all', pd.DataFrame()),
                    'thesis_envelopes': _forensic_auto.get('thesis_envelopes', pd.DataFrame()),
                    'pipeline_health': {
                        'step9b': {
                            'valid': len(_forensic_auto.get('selected_contracts', pd.DataFrame())),
                            'total_contracts': len(_forensic_auto.get('selected_contracts', pd.DataFrame())),
                        },
                        'step12': {'ready_now': len(_forensic_auto.get('acceptance_ready', pd.DataFrame()))},
                    },
                    'artifact_timestamp': _mod_auto,
                    **_forensic_auto,
                }
                st.session_state['pipeline_results'] = {
                    k: sanitize_func(v) for k, v in _auto_results.items() if isinstance(v, pd.DataFrame)
                }
                for k, v in _auto_results.items():
                    if not isinstance(v, pd.DataFrame):
                        st.session_state['pipeline_results'][k] = v
                _ready_auto = len(_forensic_auto.get('acceptance_ready', pd.DataFrame()))
                if 'pipeline_run_metadata' not in st.session_state:
                    st.session_state['pipeline_run_metadata'] = {}
                st.session_state['pipeline_run_metadata'].update({
                    'last_run': _mod_auto,
                    'status': 'completed' if _ready_auto > 0 else 'completed_empty',
                    'ready_count': _ready_auto,
                })
        except Exception:
            pass  # No artifacts yet — funnel stays empty

    logger.info(f"=== SCAN VIEW RENDER START ===")
    logger.info(f"Session state snapshot: is_fetching={st.session_state.get('is_fetching_data', False)}, fetch_start_time={st.session_state.get('fetch_job_start_time')}, is_running_pipeline={st.session_state.get('is_running_pipeline', False)}")

    # Helper to get/cache uploaded file path (prevents duplicate writes)
    def _get_snapshot_path_for_upload(uploaded_file):
        """Write uploaded file once and cache path in session state."""
        if uploaded_file is None:
            return None

        # Check if we already processed this file
        file_id = f"{uploaded_file.name}_{uploaded_file.size}"
        cached_key = f"temp_upload_path_{file_id}"

        if cached_key in st.session_state:
            cached_path = st.session_state[cached_key]
            if os.path.exists(cached_path):
                return cached_path

        # Write file once
        temp_path = core_project_root / f"temp_upload_{file_id}.csv"
        try:
            with open(temp_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
            st.session_state[cached_key] = str(temp_path)
            return str(temp_path)
        except Exception as e:
            logger.error(f"Failed to write uploaded file: {e}")
            return None

    if st.button("← Back to Home"):
        set_view_func("home")

    st.title("🔍 Market Scan - Full Pipeline Orchestration")
    st.markdown("Execute the complete pipeline to discover and evaluate trade opportunities.")

    # === HELP & GLOSSARY ===
    with st.expander("📖 Quick Reference Guide", expanded=False):
        col_g1, col_g2 = st.columns(2)

        with col_g1:
            st.markdown("""
            **Key Concepts:**
            - **PCS Score:** Probability of Capital Success (0-100). Higher = better trade quality.
            - **IV Rank:** Current IV vs 1-year range (0-100 percentile). High = expensive options.
            - **Expression Tier:** Position sizing guidance:
              - 🟢 **CORE:** High liquidity, full size OK
              - 🟡 **STANDARD:** Normal liquidity, moderate size
              - 🔵 **NICHE:** Limited liquidity, constrained size
            """)

        with col_g2:
            st.markdown("""
            **Execution Status:**
            - 🟢 **READY:** Passed all gates, execute now
            - 🟡 **WAITLIST:** Valid but waiting for conditions (liquidity, IV maturity, etc.)
            - 🔴 **BLOCKED:** Rejected due to structural issues or risk

            **Data Freshness:**
            - 🟢 **LIVE:** <4 hours old
            - 🟡 **RECENT:** 4-24 hours old
            - 🔴 **STALE:** >24 hours old (blocked)
            """)

    # Global Guardrails Display
    if 'pipeline_results' in st.session_state:
        results = st.session_state['pipeline_results']
        
        # 1. Market Stress Banner
        if 'market_stress' in results:
            stress = results['market_stress']
            level = stress['level']
            
            if level == 'RED':
                st.error(f"🛑 **MARKET STRESS RED:** All trades halted. Basis: {stress['basis']}")
            elif level == 'YELLOW':
                st.warning(f"⚠️ **MARKET STRESS YELLOW:** Elevated volatility. Basis: {stress['basis']}")
            elif level == 'UNKNOWN':
                st.info(f"❓ **MARKET STRESS UNKNOWN:** Insufficient IV data to determine stress level.")
            else:
                st.success(f"✅ **MARKET STRESS GREEN:** Normal conditions. Basis: {stress['basis']}")
    
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
        
        else:  # Use File Path
            explicit_snapshot_path_input = st.text_input(
                "IV/HV Snapshot Path",
                value="", # Default to empty as Fidelity is removed
                help="Full path to IV/HV CSV file"
            )
        
        st.divider()
        st.header("🛠️ Execution Options")
        st.caption("⚠️ These options affect logging and diagnostics only, not execution logic.")

        # Checkbox pattern: Use key ONLY - Streamlit auto-manages the state
        # Do NOT use value= when using key= - creates infinite loop!
        # debug_mode and audit_mode are initialized in dashboard.py
        st.checkbox(
            "🧪 Debug Mode",
            help="Extra instrumentation and logging. Does not change which trades appear.",
            key="debug_mode"  # Streamlit auto-manages st.session_state.debug_mode
        )

        st.checkbox(
            "🔍 Audit Mode",
            help="Per-ticker trace tables and step-by-step CSVs. Does not change which trades appear.",
            key="audit_mode"  # Streamlit auto-manages st.session_state.audit_mode
        )

        st.checkbox(
            "⚡ Intraday Refresh",
            help="Fetch live 5-min bars, VWAP, and spread quality for READY candidates (adds ~12s). "
                 "When off, intraday columns show OFF_HOURS.",
            key="intraday_refresh"  # Streamlit auto-manages st.session_state.intraday_refresh
        )

    # ========================================
    # DATA ACQUISITION GUIDANCE (PRESENTATION-ONLY)
    # ========================================
    # REMOVED: Step 0 partial run button (violates one-button model)
    # Users must fetch data via CLI: scan_engine/step0_schwab_snapshot.py
    # Dashboard is read-only observatory, not execution controller
    # ========================================

    # ========================================
    # PRE-SCAN DATA PLAN PANEL (PRESENTATION-ONLY)
    # ========================================
    # REQUIREMENT: Show what WILL happen before execution
    # - Data sources (Schwab/DuckDB/Fidelity)
    # - Snapshot quality metrics
    # - IV maturity forecast
    # - Market regime proxy
    # No user controls, no parameters, no overrides
    # ========================================
    st.header("📋 Data Plan — This Run")
    st.caption("⚠️ Read-only. Shows what data will be fetched vs reused. No parameters, no overrides.")

    # Get snapshot path (use cached upload helper to avoid duplicate writes)
    prov_path = None
    if upload_method == "Upload CSV" and uploaded_file_obj:
        prov_path = _get_snapshot_path_for_upload(uploaded_file_obj)
    else:
        prov_path = explicit_snapshot_path_input

    info = get_snapshot_info(prov_path, core_project_root)

    # DEBUG: Log what we got
    logger.info(f"DEBUG: info = {info is not None}")
    if info:
        logger.info(f"DEBUG: info keys = {list(info.keys())}")
        logger.info(f"DEBUG: is_stale in info = {'is_stale' in info}")
        logger.info(f"DEBUG: is_stale value = {info.get('is_stale', 'KEY_MISSING')}")

    # === DATA STATUS BADGE (Prominent) ===
    render_data_status_badge(info)

    # === IV COLLECTION HEALTH BADGE ===
    render_iv_collection_badge()

    # Check if data is stale and show fetch button IMMEDIATELY
    # NOTE: Compute staleness from timestamp (read-only), not in cached function
    execution_blocked = False
    block_reason = ""
    is_stale = False

    # ALWAYS check staleness - never gate the fetch button
    if info and info.get('timestamp'):
        # Trading-day-aware staleness: weekend/holiday data is not stale
        age_seconds = (datetime.now() - info['timestamp']).total_seconds()
        is_stale, _stale_reason = _snapshot_is_stale(info['timestamp'])
        logger.info(f"DEBUG: Data age: {age_seconds/3600:.1f}h, is_stale = {is_stale} ({_stale_reason})")
        if is_stale:
            execution_blocked = True
            block_reason = _stale_reason
            logger.info(f"DEBUG: Set execution_blocked = True")

    # UNCONDITIONAL: Show fetch button when stale (NO other gates)
    logger.info(f"DEBUG: Staleness check - is_stale={is_stale}, is_fetching_data={st.session_state.get('is_fetching_data', False)}, fetch_job_start_time={st.session_state.get('fetch_job_start_time')}")

    if is_stale:
        logger.info("DEBUG: Data is stale - will render fetch button")
        execution_blocked = True
        # block_reason already set above

        # === PROMINENT FIX BUTTON - SIMPLIFIED (no complex nesting) ===
        st.error("🛑 **DATA TOO OLD - Cannot scan with stale data**")
        st.warning("⚠️ **Action Required:** Click the button below to fetch fresh data")

        # === FETCH JOB ORCHESTRATION (Non-Blocking) ===

        # Define callback that executes fetch immediately (no intent flag)
        def _execute_fetch_now():
            """Execute fetch job immediately in callback (runs after current render)."""
            if st.session_state.is_fetching_data:
                return  # Already running

            st.session_state.is_fetching_data = True
            st.session_state.fetch_job_status_message = "Starting fetch job..."
            logger.info("DEBUG: Launching fetch job from button callback")

            success, message, job_start_time, process = start_fetch_job()

            if success:
                st.session_state.fetch_job_start_time = job_start_time
                st.session_state.fetch_job_status_message = message
                st.session_state.fetch_job_success = True
                st.session_state.fetch_process = process  # Store for monitoring
            else:
                st.session_state.fetch_job_status_message = f"Failed: {message}"
                st.session_state.fetch_job_success = False
                st.session_state.is_fetching_data = False  # Release lock on failure

        # Display fetch button or cancel button
        if not st.session_state.is_fetching_data:
            st.button(
                "🔄 **Fetch Fresh Data from Schwab**",
                type="primary",
                key="fetch_top_btn",
                width="stretch",
                on_click=_execute_fetch_now
            )
            st.info("💡 This will fetch live IV/HV data from Schwab (~200 tickers, takes 2-3 minutes)")
        else:
            # Show status message
            st.info(f"⏳ {st.session_state.fetch_job_status_message}")
            st.caption("Fetching IV/HV data from Schwab (~200 tickers)...")

            # Cancel button callback
            def _cancel_fetch():
                """Cancel the running fetch job."""
                process = st.session_state.get("fetch_process")
                if process:
                    try:
                        process.kill()
                        logger.info(f"User cancelled fetch job (PID: {process.pid})")
                    except Exception as e:
                        logger.warning(f"Could not kill process: {e}")

                # Reset state
                st.session_state.is_fetching_data = False
                st.session_state.fetch_job_start_time = None
                st.session_state.fetch_process = None

            # Display cancel button
            col1, col2 = st.columns([3, 1])
            with col2:
                st.button(
                    "❌ Cancel",
                    type="secondary",
                    key="cancel_fetch_btn",
                    width="stretch",
                    on_click=_cancel_fetch
                )

        st.markdown("---")

    st.divider()

    # === MANUAL REFRESH BUTTON (always visible, even when data is fresh) ===
    if not is_stale:
        # Only show manual refresh when data isn't already stale (stale case already has primary fetch button above)
        def _execute_manual_refresh():
            """Manually trigger a fresh IV/HV snapshot even when current data is recent."""
            if st.session_state.is_fetching_data:
                return
            st.session_state.is_fetching_data = True
            st.session_state.fetch_job_status_message = "Starting manual refresh..."
            logger.info("DEBUG: Launching manual IV/HV refresh from button callback")
            success, message, job_start_time, process = start_fetch_job()
            if success:
                st.session_state.fetch_job_start_time = job_start_time
                st.session_state.fetch_job_status_message = message
                st.session_state.fetch_job_success = True
                st.session_state.fetch_process = process
            else:
                st.session_state.fetch_job_status_message = f"Failed: {message}"
                st.session_state.fetch_job_success = False
                st.session_state.is_fetching_data = False

        if not st.session_state.get("is_fetching_data", False):
            col_refresh, col_info = st.columns([1, 2])
            with col_refresh:
                st.button(
                    "🔄 Refresh IV/HV Data",
                    type="secondary",
                    key="manual_refresh_btn",
                    width="stretch",
                    on_click=_execute_manual_refresh,
                )
            with col_info:
                st.caption("Fetch a fresh snapshot from Schwab now (~2-3 min). Use this if you want up-to-the-minute IV/HV before running the scan.")
        else:
            st.info(f"⏳ {st.session_state.fetch_job_status_message}")
            st.caption("Fetching IV/HV data from Schwab (~200 tickers)...")

    if info:
        # === DATA SOURCES SECTION ===
        st.subheader("📦 Data Sources")

        col_src1, col_src2, col_src3 = st.columns(3)

        with col_src1:
            st.markdown("**Fast IV (Schwab)**")
            st.info("🔵 WILL FETCH (live)")
            st.caption("Real-time IV from option chains")

        with col_src2:
            st.markdown("**IV History (DuckDB)**")
            if info['iv_history'] > 0:
                st.success(f"🟢 READ ONLY ({info['iv_history']} days)")
            else:
                st.warning("🔶 NO DATA (0 days)")
            st.caption("Constant-maturity IV history")

        st.divider()

        # === SNAPSHOT INFO SECTION ===
        st.subheader("📊 Snapshot Quality")

        col_q1, col_q2, col_q3 = st.columns(3)

        with col_q1:
            st.metric("Data Freshness", info['timestamp'].strftime('%H:%M %Z'))
            # Use locally computed is_stale (computed above from timestamp)
            if is_stale:
                st.error("❌ STALE (>24h)")
                # execution_blocked already set above when is_stale computed
            else:
                st.success("✅ FRESH")

        with col_q2:
            # IV Coverage: IV no longer lives in the snapshot CSV (Layer 1B decoupled IV
            # collection — iv_term_history is the single source of truth).
            # Gate on DuckDB iv_history depth instead:
            #   ≥20d → IMMATURE but usable (IV_Rank computable)
            #   <20d → EARLY, Step 2 will still proceed with available history
            #    0d  → no history yet (fresh install)
            _iv_days = info.get('iv_history', 0)
            st.metric("IV History", f"{_iv_days}d median")
            if _iv_days >= 30:
                st.success("✅ GOOD")
            elif _iv_days >= 20:
                st.warning("⚠️ IMMATURE")
            elif _iv_days > 0:
                st.warning("⏳ EARLY")
            else:
                st.error("❌ NO HISTORY")
                execution_blocked = True
                block_reason = "No IV history in DuckDB. Run collect_iv_daily.py first."

        with col_q3:
            st.metric("Tickers", info['tickers'])
            st.caption(f"Source: {info['filename']}")

        st.divider()

        # === IV MATURITY DISTRIBUTION ===
        st.subheader("🎯 IV Maturity Distribution")
        st.caption("Actual per-ticker history depth from DuckDB (iv_term_history)")

        dist = get_iv_maturity_distribution()
        if dist and dist['total'] > 0:
            total = dist['total']
            mature_pct  = round(dist['mature']  / total * 100)
            partial_pct = round(dist['partial'] / total * 100)
            immature_pct = round(dist['immature'] / total * 100)
            early_pct   = round(dist['early']   / total * 100)

            # Summary line
            if dist['mature'] > 0:
                st.success(f"✅ **MATURE (120+d):** {dist['mature']} tickers ({mature_pct}%)")
            if dist['partial'] > 0:
                st.warning(f"🔶 **PARTIAL (60-119d):** {dist['partial']} tickers ({partial_pct}%)")
            if dist['immature'] > 0:
                st.info(f"🔵 **IMMATURE (20-59d):** {dist['immature']} tickers ({immature_pct}%)")
            if dist['early'] > 0:
                st.info(f"⏳ **EARLY (1-19d):** {dist['early']} tickers ({early_pct}%)")

            st.caption(
                f"Range: {dist['min_days']}–{dist['max_days']} days · "
                f"Avg: {dist['avg_days']:.1f} days · "
                f"{total} tickers tracked"
            )
        else:
            st.warning("⚠️ **ACCUMULATING:** No IV history in DuckDB yet — run the pipeline to collect.")

        # Market regime proxy (cached to prevent I/O on every render)
        st.divider()
        st.subheader("📈 Market Regime Proxy")
        stress_level, median_iv, stress_basis = get_market_stress_cached()

        if stress_level in ('LOW', 'NORMAL'):
            st.success(f"🟢 **{stress_level}:** Normal conditions (SPY ATR: {median_iv:.2f}%)")
        elif stress_level == 'ELEVATED':
            st.warning(f"🟡 **ELEVATED:** Elevated volatility (SPY ATR: {median_iv:.2f}%)")
        elif stress_level == 'CRISIS':
            st.error(f"🔴 **CRISIS:** High stress (SPY ATR: {median_iv:.2f}%)")
        else:
            st.info("❓ **UNKNOWN:** Insufficient data")

        st.caption(f"Basis: {stress_basis}")

        st.divider()

        # === EXECUTION GATE (PRESENTATION-ONLY) ===
        if execution_blocked:
            # Only show error if NOT stale (stale already shown at top with button)
            if not is_stale:
                st.error(f"🛑 **EXECUTION BLOCKED:** {block_reason}")
                st.info("""
                **To resolve this issue:**
                1. Run: `python scripts/cli/collect_iv_daily.py`
                2. Wait for collection to complete (~5-10 min for full universe)
                3. Refresh this page — IV History will update automatically
                """)

        else:
            st.success("🟢 **READY TO SCAN:** Data quality meets safety thresholds.")
    else:
        st.warning("⚠️ No valid snapshot selected. Please fetch data or provide a path.")
        execution_blocked = True

    # ========================================
    # FULL PIPELINE EXECUTION (SINGLE CONTROL)
    # ========================================
    # REQUIREMENT: ONE primary button only - "Run Full Scan"
    # - Executes entire pipeline end-to-end
    # - Fixed parameters (hardcoded, not exposed in UI)
    # - No partial runs, no toggles, no user-controlled execution parameters
    # - Matches CLI semantics exactly
    # ========================================
    st.header("🚀 Run Full Scan Pipeline")
    st.markdown("""
    **Purpose:** Execute the complete scan pipeline (Steps 2-12).
    **Guarantee:** Execution equivalence with CLI (`scan_live.py`).
    """)
    
    col1, col2 = st.columns([1, 3])
    with col1:
        # Generate helpful tooltip for disabled button
        button_help_text = None
        if execution_blocked:
            button_help_text = f"Cannot scan: {block_reason}"
        elif st.session_state.pipeline_running:
            button_help_text = "Scan already in progress..."

        # Define callback that executes scan immediately (no intent flag)
        def _execute_scan_now():
            """Execute scan pipeline immediately in callback (runs after current render)."""
            if st.session_state.is_running_pipeline:
                return  # Already running

            st.session_state.is_running_pipeline = True
            logger.info("DEBUG: Launching scan pipeline from button callback")

            from core.runner import PipelineRunner
            runner = PipelineRunner(core_project_root)

            # Reset metadata for new run
            st.session_state.pipeline_run_metadata["status"] = "running"
            st.session_state.pipeline_run_metadata["error"] = None
            st.session_state.pipeline_run_metadata["ready_now_count"] = 0

            ret_code = -1

            try:
                # Get snapshot path (reuse cached upload to avoid duplicate write)
                if uploaded_file_obj:
                    snapshot_path = _get_snapshot_path_for_upload(uploaded_file_obj)
                    if not snapshot_path:
                        raise ValueError("Failed to process uploaded file")
                else:
                    snapshot_path = explicit_snapshot_path_input

                logger.info(f"[Dashboard] Scan launch snapshot_path={snapshot_path!r}")

                # Fixed parameters (not exposed in UI per design requirements)
                account_balance = 100000.0
                max_portfolio_risk = 0.20
                sizing_method = 'volatility_scaled'

                # Pre-flight: check if another scan is already holding the DB write lock.
                # Use read_only=True — this succeeds even when a writer holds the lock,
                # but raises IOException when DuckDB's file is corrupt/missing entirely.
                # The subprocess pipeline will acquire the write lock itself; we must NOT
                # hold a write-mode connection here or we block the subprocess immediately.
                from core.shared.data_contracts.config import PIPELINE_DB_PATH, DEBUG_PIPELINE_DB_PATH
                import duckdb as _duckdb
                _db_path = DEBUG_PIPELINE_DB_PATH if st.session_state.debug_mode else PIPELINE_DB_PATH
                if _db_path.exists():
                    try:
                        _test = _duckdb.connect(str(_db_path), read_only=True)
                        _test.close()
                    except Exception as _lock_err:
                        if "Conflicting lock" in str(_lock_err):
                            st.session_state.pipeline_run_metadata.update({
                                "last_run": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                "status": "failed",
                                "error": "Another scan is already running. Wait for it to finish, then try again."
                            })
                            st.rerun()

                ret_code = runner.run_scan_pipeline(
                    snapshot_path,
                    account_balance,
                    max_portfolio_risk,
                    sizing_method,
                    debug=st.session_state.debug_mode,
                    intraday=st.session_state.get('intraday_refresh', False),
                )

                if ret_code != 0:
                    st.session_state.pipeline_run_metadata.update({
                        "last_run": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "status": "failed",
                        "error": "Scan process returned non-zero exit code"
                    })
                else:
                    # Load results
                    df_primary, mod_time, forensic = get_latest_step8_artifact(scan_output_dir)

                    if df_primary is not None:
                        results = {
                            'acceptance_ready': forensic.get('acceptance_ready', pd.DataFrame()),
                            'acceptance_all': forensic.get('acceptance_all', pd.DataFrame()),
                            'thesis_envelopes': forensic.get('thesis_envelopes', pd.DataFrame()),
                            'pipeline_health': {
                                'step9b': {'valid': len(forensic.get('selected_contracts', pd.DataFrame())), 'total_contracts': len(forensic.get('selected_contracts', pd.DataFrame()))},
                                'step12': {'ready_now': len(forensic.get('acceptance_ready', pd.DataFrame()))}
                            },
                            'artifact_timestamp': mod_time,
                            **forensic
                        }

                        st.session_state['pipeline_results'] = {
                            k: sanitize_func(v)
                            for k, v in results.items()
                            if isinstance(v, pd.DataFrame)
                        }
                        for k, v in results.items():
                            if not isinstance(v, pd.DataFrame):
                                st.session_state['pipeline_results'][k] = v

                        ready_now_count = len(results.get('acceptance_ready', pd.DataFrame()))
                        st.session_state.pipeline_run_metadata.update({
                            "last_run": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "status": "completed" if ready_now_count > 0 else "completed_empty",
                            "ready_now_count": ready_now_count
                        })

                    st.cache_data.clear()

            except Exception as e:
                st.session_state.pipeline_run_metadata.update({
                    "last_run": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "status": "failed",
                    "error": str(e)
                })
                logger.error(f"Full pipeline failed: {e}", exc_info=True)

            finally:
                st.session_state.is_running_pipeline = False

        # Display run scan button
        st.button(
            "▶️ Run Full Pipeline",
            type="primary",
            disabled=execution_blocked or st.session_state.is_running_pipeline,
            help=button_help_text or "Execute complete scan pipeline (Steps 2-12)",
            on_click=_execute_scan_now,
            key="run_scan_btn"
        )
    
    with col2:
        # Execution Proof & Status Banner
        meta = st.session_state.pipeline_run_metadata
        if st.session_state.is_running_pipeline:
            st.info("🚀 Executing Full Scan Pipeline...")
        elif meta["last_run"]:
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
                    st.metric(
                        "Tickers In",
                        len(results.get('snapshot', pd.DataFrame())),
                        help="Total tickers loaded from snapshot"
                    )
                with f2:
                    st.metric(
                        "Valid Contracts",
                        health['step9b']['valid'],
                        help="Contracts that passed initial validation (liquidity, Greeks, DTE)"
                    )
                with f3:
                    st.metric(
                        "READY",
                        health['step12']['ready_now'],
                        help="High-conviction trades ready for immediate execution"
                    )
                with f4:
                    waitlist_count = 0
                    if 'acceptance_all' in results:
                        df_all = results['acceptance_all']
                        if 'Execution_Status' in df_all.columns:
                            waitlist_count = (df_all['Execution_Status'] == 'AWAIT_CONFIRMATION').sum()

                    # Also check DuckDB wait_list
                    try:
                        debug_mode = st.session_state.get("debug_mode", False)
                        db_path = _resolve_pipeline_db_path(debug_mode)

                        if db_path.exists():
                            with _connect_ro(str(db_path)) as con:
                                tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").df()['table_name'].tolist()
                                if 'wait_list' in tables:
                                    db_count = con.execute("SELECT COUNT(*) FROM wait_list WHERE status = 'ACTIVE'").fetchone()[0]
                                    waitlist_count = max(waitlist_count, db_count)
                    except Exception:
                        pass

                    st.metric("WAITLIST", waitlist_count)

            st.divider()

            # ========================================
            # POST-SCAN DATA PROVENANCE SUMMARY (PRESENTATION-ONLY)
            # ========================================
            # REQUIREMENT: Show what DID happen after execution
            # - Schwab API: Quotes/chains/IV fetched or reused
            # - DuckDB: Tickers read, median depth, rank source
            # - Fidelity: Triggered or not, rule invoked, ticker count
            # Deterministic, factual, never speculative
            # ========================================
            st.subheader("📦 Data Provenance Summary — What Actually Happened")
            st.caption("Deterministic record of data fetched vs reused")

            prov_col1, prov_col2, prov_col3 = st.columns(3)

            with prov_col1:
                st.markdown("**Schwab API**")
                st.success("✅ Quotes fetched")
                st.success("✅ Option chains fetched")
                st.success("✅ Live IV fetched")
                st.caption("Real-time market data")

            with prov_col2:
                st.markdown("**DuckDB (IV History)**")
                # AUTHORITATIVE READ: Get actual DuckDB reads from scan results
                # Try multiple sources: acceptance_all → snapshot → acceptance_ready
                iv_history_depth = 0
                duckdb_success_count = 0
                source_used = None

                # Try reading from acceptance_all (most complete)
                # Column is written as IV_History_Count by IVEngine
                _depth_cols = ['IV_History_Count', 'iv_history_days', 'IV_History_Days']
                for key in ['acceptance_all', 'snapshot', 'acceptance_ready']:
                    if key in results and not results[key].empty:
                        df_source = results[key]
                        for col in _depth_cols:
                            if col in df_source.columns:
                                median_val = df_source[col].median()
                                if not pd.isna(median_val) and median_val > 0:
                                    iv_history_depth = int(median_val)
                                    source_used = key
                                    logger.info(f"📊 IV history depth read from {key}[{col}]: {iv_history_depth} days")
                                    break
                        if iv_history_depth > 0:
                            break

                # Count tickers with IV rank computed (IV_Rank_Source is ROLLING_20D/30D/etc — never 'DuckDB')
                for key in ['acceptance_all', 'snapshot', 'acceptance_ready']:
                    if key in results and not results[key].empty:
                        df_source = results[key]
                        if 'IV_Rank_Source' in df_source.columns:
                            duckdb_success_count = df_source['IV_Rank_Source'].notna().sum()
                            duckdb_success_count -= (df_source['IV_Rank_Source'] == '').sum()
                            if duckdb_success_count > 0:
                                logger.info(f"📊 IV rank computed for {duckdb_success_count} rows from {key}")
                                break

                # Display results
                if iv_history_depth > 0:
                    st.info(f"📊 {duckdb_success_count} tickers with IV rank")
                    st.info(f"📅 Median depth: {iv_history_depth} days")
                else:
                    st.caption("⏳ IV history accumulating — run pipeline to populate")

            with prov_col3:
                st.markdown("**IV Maturity**")
                # Show per-tier breakdown from acceptance_all
                _mat_src = None
                for key in ['acceptance_all', 'snapshot', 'acceptance_ready']:
                    if key in results and not results[key].empty and 'IV_Maturity_State' in results[key].columns:
                        _mat_src = results[key].drop_duplicates(subset='Ticker') if 'Ticker' in results[key].columns else results[key]
                        break

                if _mat_src is not None and 'IV_Maturity_State' in _mat_src.columns:
                    _mat_counts = _mat_src['IV_Maturity_State'].value_counts()
                    _mature   = int(_mat_counts.get('MATURE', 0))
                    _partial  = int(_mat_counts.get('PARTIAL_MATURE', 0))
                    _immature = int(_mat_counts.get('IMMATURE', 0))
                    _missing  = int(_mat_counts.get('MISSING', 0))
                    _total    = _mature + _partial + _immature + _missing
                    if _mature > 0:
                        st.success(f"✅ {_mature} MATURE (120d+)")
                    if _partial > 0:
                        st.info(f"🔵 {_partial} PARTIAL (60-119d)")
                    if _immature > 0:
                        st.warning(f"⏳ {_immature} IMMATURE (<60d)")
                    if _missing > 0:
                        st.caption(f"⚫ {_missing} MISSING")
                    if _total == 0:
                        st.caption("No maturity data yet")
                else:
                    st.caption("⏳ Run pipeline to populate")

            st.divider()

            # DIAGNOSTIC: Show IV metadata presence (Debug Mode only)
            if st.session_state.debug_mode:
                st.caption("🔍 **IV Metadata Diagnostic**")
                diagnostic_info = []

                for key in ['acceptance_all', 'snapshot', 'acceptance_ready']:
                    if key in results and not results[key].empty:
                        df_diag = results[key]
                        has_iv_days = 'iv_history_days' in df_diag.columns or 'IV_History_Days' in df_diag.columns
                        has_iv_source = 'IV_Rank_Source' in df_diag.columns
                        has_maturity = 'IV_Maturity_State' in df_diag.columns

                        if has_iv_days or has_iv_source or has_maturity:
                            diagnostic_info.append(f"{key}: iv_days={has_iv_days}, source={has_iv_source}, maturity={has_maturity}")

                            # Show sample values
                            if has_iv_days:
                                col_name = 'iv_history_days' if 'iv_history_days' in df_diag.columns else 'IV_History_Days'
                                sample_val = df_diag[col_name].iloc[0] if not df_diag[col_name].empty else 'N/A'
                                diagnostic_info.append(f"  → Sample {col_name}: {sample_val}")

                if diagnostic_info:
                    for info in diagnostic_info:
                        st.caption(info)
                else:
                    st.caption("⚠️ No IV metadata columns found in any result DataFrame")

            ready_now_count = len(results.get('acceptance_ready', pd.DataFrame()))
            st.metric(
                "READY Candidates",
                ready_now_count,
                help="Trades that passed all execution gates and are ready for immediate execution"
            )
            
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
            
            st.subheader("Detailed Results — Three-Tier Execution Model")
            st.caption("Matches CLI output semantics: READY_NOW / WAITLIST / REJECTED")

            tabs_list = ["🟢 READY NOW", "📌 CC Opportunities", "🟡 WAITLIST", "🔴 REJECTED", "🕵️ Forensic Audit", "🔬 Row Counts", "📊 All Steps"]
            if st.session_state.debug_mode:
                tabs_list.append("🧪 Debug Console")

            tabs_objects = st.tabs(tabs_list)

            tab_map = {}
            tab_names = ["tab_ready_now", "tab_cc_opps", "tab_waitlist", "tab_rejected", "tab_audit", "tab_counts", "tab_all"]
            if st.session_state.debug_mode:
                tab_names.append("tab_debug")

            for i, name in enumerate(tab_names):
                tab_map[name] = tabs_objects[i]

            tab_ready_now = tab_map['tab_ready_now']
            tab_cc_opps   = tab_map['tab_cc_opps']
            tab_waitlist = tab_map['tab_waitlist']
            tab_rejected = tab_map['tab_rejected']
            tab_audit = tab_map['tab_audit']
            tab_counts = tab_map['tab_counts']
            tab_all = tab_map['tab_all']
            tab_debug = tab_map.get('tab_debug') # Get debug tab if it exists
            
            with tab_ready_now:
                acceptance_ready_df = results.get('acceptance_ready', pd.DataFrame())

                if not acceptance_ready_df.empty:
                    def _g(row, *keys, default=None):
                        """Safe getter — tries multiple column name variants."""
                        for k in keys:
                            v = row.get(k)
                            if v is not None and str(v) not in ('nan', 'None', ''):
                                try:
                                    return v
                                except Exception:
                                    pass
                        return default

                    def _fmt_price(v, decimals=2):
                        try:
                            return f"${float(v):.{decimals}f}"
                        except Exception:
                            return "—"

                    def _fmt_pct(v, decimals=1):
                        try:
                            return f"{float(v):.{decimals}f}%"
                        except Exception:
                            return "—"

                    def _fmt_float(v, decimals=3):
                        try:
                            return f"{float(v):.{decimals}f}"
                        except Exception:
                            return "—"

                    # ── PRIORITY SORT + CAPITAL FILTER ─────────────────────────
                    # Conviction = quality of structural edge only.
                    # Liquidity and capital cost are execution constraints, not edge —
                    # they belong in the filter layer, not the score.
                    #
                    # Components (all strategy-agnostic — no LEAP vs short-dated bias):
                    #   DQS_Score          50% — directional quality: delta fit, trend
                    #                             alignment, IV timing, spread cost, DTE fit
                    #   directional_bias   25% — signal strength: STRONG=100, MODERATE=60,
                    #                             WEAK/UNKNOWN=20
                    #   IV_Maturity_Level  15% — data reliability of the IV rank signal
                    #                             (1→0, 2→20, 3→50, 4→80, 5→100)
                    #   confidence_band    10% — Step 12 final gating verdict
                    #                             (HIGH=100, MEDIUM=60, LOW=20)
                    #
                    # No capital tiebreaker — equal-conviction trades stay in DQS order.
                    # Liquidity is visible in the summary table but does not affect rank.
                    def _conviction_score(r) -> float:
                        try:
                            dqs = float(r.get('DQS_Score') or 75)

                            _bias_raw = str(r.get('directional_bias') or '').upper()
                            bias = (
                                100 if 'STRONG' in _bias_raw else
                                60  if 'MODERATE' in _bias_raw else
                                20
                            )

                            mat = {1: 0, 2: 20, 3: 50, 4: 80, 5: 100}.get(
                                int(float(r.get('IV_Maturity_Level') or 1)), 0)

                            cband = {'HIGH': 100, 'MEDIUM': 60, 'LOW': 20}.get(
                                str(r.get('confidence_band') or 'LOW').upper(), 20)

                            return dqs * 0.50 + bias * 0.25 + mat * 0.15 + cband * 0.10
                        except Exception:
                            return 0.0

                    acceptance_ready_df = acceptance_ready_df.copy()
                    acceptance_ready_df['_conviction'] = acceptance_ready_df.apply(_conviction_score, axis=1)

                    # ── Compute Expected Move % (1σ) inline ─────────────────
                    # Formula: IV × √(DTE / 365) — how far stock is expected
                    # to travel during the option's life (1 standard deviation).
                    import math as _math
                    def _expected_move_pct(r):
                        try:
                            iv  = float(r.get('Implied_Volatility') or 0)
                            dte = float(r.get('Actual_DTE') or 0)
                            if iv > 0 and dte > 0:
                                return iv * _math.sqrt(dte / 365.0)
                            return float('nan')
                        except Exception:
                            return float('nan')
                    acceptance_ready_df['_Expected_Move_Pct'] = acceptance_ready_df.apply(_expected_move_pct, axis=1)

                    # Sort mode selector
                    _sort_col1, _sort_col2 = st.columns([2, 3])
                    with _sort_col1:
                        _sort_mode = st.selectbox(
                            "📊 Sort cards by",
                            options=[
                                "Conviction (structural edge)",
                                "DQS only",
                                "DQS × TQS (structure + timing)",
                                "⚡ Execution Mode  (TQS → DQS → IV edge)",
                            ],
                            index=0,
                            key="scan_sort_mode",
                        )
                    with _sort_col2:
                        _sort_captions = {
                            "Conviction (structural edge)":
                                "DQS×50% + signal strength×25% + IV data reliability×15% + confidence×10%. "
                                "Pure structural edge — best for research and position sizing.",
                            "DQS only":
                                "Highest directional quality first. "
                                "Use when you want the strongest trend/signal setups regardless of timing.",
                            "DQS × TQS (structure + timing)":
                                "Product of structural quality × timing quality. "
                                "Surfaces trades where *both* dimensions are strong simultaneously.",
                            "⚡ Execution Mode  (TQS → DQS → IV edge)":
                                "**Execution priority stack:**  \n"
                                "1️⃣ TQS — is NOW the right moment?  \n"
                                "2️⃣ DQS — is the direction correct?  \n"
                                "3️⃣ IV edge — is vol cheap (buyers) or rich (sellers)?  \n"
                                "Use when you have capital ready to deploy *today* and want the most actionable trade first.",
                        }
                        st.caption(_sort_captions.get(_sort_mode, ""))

                    def _iv_edge_score(r) -> float:
                        """
                        IV edge aligned to strategy type.
                        Buyers (long calls/puts/LEAPs) benefit from cheap vol (IV < HV → negative gap).
                        Sellers (covered calls, CSPs, income) benefit from rich vol (IV > HV → positive gap).
                        Returns 0-100: 100 = maximum edge for this strategy type.
                        """
                        try:
                            gap = float(r.get('IVHV_gap_30D') or 0)
                            strat = str(r.get('Strategy_Name') or '').lower()
                            is_seller = any(k in strat for k in ('covered call', 'buy-write', 'cash-secured put', 'csp'))
                            # Sellers want positive gap (IV > HV), buyers want negative gap (IV < HV)
                            edge_gap = gap if is_seller else -gap
                            # Normalise: 0% gap → 50pts, +20% → 100pts, −20% → 0pts (clamped)
                            return max(0.0, min(100.0, 50.0 + edge_gap * 2.5))
                        except Exception:
                            return 50.0  # neutral when missing

                    if _sort_mode == "DQS only":
                        acceptance_ready_df['_sort_val'] = acceptance_ready_df.apply(
                            lambda r: float(r.get('DQS_Score') or 0), axis=1)

                    elif _sort_mode == "DQS × TQS (structure + timing)":
                        def _dqs_tqs_key(r):
                            try:
                                dqs = float(r.get('DQS_Score') or 0)
                                tqs_raw = r.get('TQS_Score')
                                tqs = float(tqs_raw) if tqs_raw and str(tqs_raw) not in ('nan', 'None', 'N/A') else dqs
                                return (dqs / 100) * (tqs / 100) * 10000
                            except Exception:
                                return 0.0
                        acceptance_ready_df['_sort_val'] = acceptance_ready_df.apply(_dqs_tqs_key, axis=1)

                    elif _sort_mode == "⚡ Execution Mode  (TQS → DQS → IV edge)":
                        def _execution_key(r):
                            try:
                                dqs = float(r.get('DQS_Score') or 0)
                                tqs_raw = r.get('TQS_Score')
                                tqs = float(tqs_raw) if tqs_raw and str(tqs_raw) not in ('nan', 'None', 'N/A') else dqs
                                iv  = _iv_edge_score(r)
                                # Weighted priority stack — TQS dominates, then DQS, then IV edge as tiebreaker
                                # Scaled so each tier has non-overlapping influence:
                                #   TQS 0-100 → contributes 0-10000 (dominant)
                                #   DQS 0-100 → contributes 0-100  (secondary)
                                #   IV  0-100 → contributes 0-1    (tiebreaker only)
                                return tqs * 100 + dqs + iv / 100
                            except Exception:
                                return 0.0
                        acceptance_ready_df['_sort_val'] = acceptance_ready_df.apply(_execution_key, axis=1)

                    else:
                        acceptance_ready_df['_sort_val'] = acceptance_ready_df['_conviction']

                    acceptance_ready_df = acceptance_ready_df.sort_values(
                        '_sort_val', ascending=False,
                    ).reset_index(drop=True)

                    # Capital budget filter — execution constraint, applied after ranking
                    _cap_col1, _cap_col2 = st.columns([2, 3])
                    with _cap_col1:
                        _cap_options = [
                            ("No limit", 9_999_999),
                            ("≤ $500 / contract", 500),
                            ("≤ $1,000 / contract", 1_000),
                            ("≤ $3,000 / contract", 3_000),
                            ("≤ $5,000 / contract", 5_000),
                            ("≤ $10,000 / contract", 10_000),
                        ]
                        _cap_label = st.selectbox(
                            "💰 Capital budget per trade",
                            options=[o[0] for o in _cap_options],
                            index=0,
                            key="scan_cap_budget",
                        )
                        _cap_limit = dict(_cap_options)[_cap_label]
                    with _cap_col2:
                        st.caption(
                            "Filters by capital at risk — does not affect conviction rank. "
                            "A LEAP and a short-dated option with equal structural edge rank equally."
                        )

                    if _cap_limit < 9_999_999 and 'Capital_Requirement' in acceptance_ready_df.columns:
                        acceptance_ready_df = acceptance_ready_df[
                            acceptance_ready_df['Capital_Requirement'].fillna(0).astype(float) <= _cap_limit
                        ]

                    # ── FILTER CARD (user-controlled lens — engine stays neutral) ──
                    _pre_filter_count = len(acceptance_ready_df)

                    with st.expander("🔎 Filter Candidates", expanded=False):
                        _fc1, _fc2, _fc3 = st.columns(3)

                        # ── Column 1: Strategy + Confidence ─────────────────────
                        with _fc1:
                            # Strategy Type filter
                            _strat_types_avail = sorted(
                                acceptance_ready_df['Strategy_Type'].dropna().unique().tolist()
                            ) if 'Strategy_Type' in acceptance_ready_df.columns else []
                            _strat_filter = st.multiselect(
                                "Strategy Type",
                                options=_strat_types_avail if _strat_types_avail else ['INCOME', 'DIRECTIONAL', 'VOLATILITY'],
                                default=_strat_types_avail if _strat_types_avail else [],
                                key="scan_filter_strat_type",
                            )

                            # Confidence filter
                            _conf_options = ["All", "Medium+ only", "High only"]
                            _conf_filter = st.radio(
                                "Min Confidence",
                                options=_conf_options,
                                index=0,
                                key="scan_filter_confidence",
                                horizontal=True,
                            )

                        # ── Column 2: Edge Quality ──────────────────────────────
                        with _fc2:
                            _min_score = st.slider(
                                "Min Score (DQS / PCS)",
                                min_value=0, max_value=100, value=0, step=5,
                                key="scan_filter_min_score",
                            )

                            _iv_rank_range = st.slider(
                                "IV Rank range",
                                min_value=0, max_value=100, value=(0, 100), step=5,
                                key="scan_filter_iv_rank",
                            )

                            _min_gap = st.number_input(
                                "Min IV-HV Gap (pts)",
                                min_value=0.0, max_value=30.0, value=0.0, step=1.0,
                                key="scan_filter_min_gap",
                            )

                        # ── Column 3: Liquidity + Timing ────────────────────────
                        with _fc3:
                            _min_oi = st.number_input(
                                "Min Open Interest",
                                min_value=0, max_value=10000, value=0, step=100,
                                key="scan_filter_min_oi",
                            )

                            _max_spread = st.number_input(
                                "Max Spread %",
                                min_value=0.0, max_value=30.0, value=30.0, step=1.0,
                                key="scan_filter_max_spread",
                            )

                            _max_dte = st.number_input(
                                "Max DTE (days)",
                                min_value=0, max_value=999, value=999, step=30,
                                key="scan_filter_max_dte",
                                help="Set < 200 to hide LEAPs",
                            )

                            _max_em = st.number_input(
                                "Max Expected Move %",
                                min_value=0.0, max_value=100.0, value=100.0, step=5.0,
                                key="scan_filter_max_em",
                                help="IV × √(DTE/365) — 1σ expected stock move during option life",
                            )

                        # ── Quick presets row ───────────────────────────────────
                        _pq1, _pq2, _pq3, _pq4 = st.columns(4)
                        with _pq1:
                            st.caption("**Presets** — click to apply defaults above")
                        # Note: presets are informational — user adjusts filters manually

                    # ── Apply filters ────────────────────────────────────────
                    _fdf = acceptance_ready_df

                    # Strategy Type
                    if _strat_filter and 'Strategy_Type' in _fdf.columns:
                        _fdf = _fdf[_fdf['Strategy_Type'].isin(_strat_filter)]

                    # Confidence
                    if _conf_filter == "Medium+ only" and 'confidence_band' in _fdf.columns:
                        _fdf = _fdf[_fdf['confidence_band'].str.upper().isin(['MEDIUM', 'HIGH'])]
                    elif _conf_filter == "High only" and 'confidence_band' in _fdf.columns:
                        _fdf = _fdf[_fdf['confidence_band'].str.upper() == 'HIGH']

                    # Score (DQS or PCS)
                    if _min_score > 0:
                        _score_col = 'DQS_Score' if 'DQS_Score' in _fdf.columns else 'PCS_Score_V2' if 'PCS_Score_V2' in _fdf.columns else None
                        if _score_col:
                            _fdf = _fdf[_fdf[_score_col].fillna(0).astype(float) >= _min_score]

                    # IV Rank
                    if _iv_rank_range != (0, 100):
                        _ivr_col = 'IV_Rank_30D' if 'IV_Rank_30D' in _fdf.columns else 'IV_Rank_20D' if 'IV_Rank_20D' in _fdf.columns else None
                        if _ivr_col:
                            _ivr_vals = _fdf[_ivr_col].fillna(50).astype(float)
                            _fdf = _fdf[(_ivr_vals >= _iv_rank_range[0]) & (_ivr_vals <= _iv_rank_range[1])]

                    # IV-HV Gap
                    if _min_gap > 0 and 'IVHV_gap_30D' in _fdf.columns:
                        _fdf = _fdf[_fdf['IVHV_gap_30D'].fillna(0).astype(float) >= _min_gap]

                    # Open Interest
                    if _min_oi > 0 and 'Open_Interest' in _fdf.columns:
                        _fdf = _fdf[_fdf['Open_Interest'].fillna(0).astype(float) >= _min_oi]

                    # Spread %
                    if _max_spread < 30.0 and 'Bid_Ask_Spread_Pct' in _fdf.columns:
                        _fdf = _fdf[_fdf['Bid_Ask_Spread_Pct'].fillna(0).astype(float) <= _max_spread]

                    # Max DTE
                    if _max_dte < 999 and 'Actual_DTE' in _fdf.columns:
                        _fdf = _fdf[_fdf['Actual_DTE'].fillna(0).astype(float) <= _max_dte]

                    # Expected Move %
                    if _max_em < 100.0 and '_Expected_Move_Pct' in _fdf.columns:
                        _fdf = _fdf[_fdf['_Expected_Move_Pct'].fillna(0).astype(float) <= _max_em]

                    acceptance_ready_df = _fdf

                    # Show filter badge if any filtering applied
                    _post_filter_count = len(acceptance_ready_df)
                    if _post_filter_count < _pre_filter_count:
                        st.info(
                            f"🔎 Showing **{_post_filter_count}** of {_pre_filter_count} candidates "
                            f"({_pre_filter_count - _post_filter_count} filtered out)"
                        )

                    # ── SUMMARY TABLE (scannable overview before cards) ──────────
                    n = len(acceptance_ready_df)
                    st.markdown(f"### ✅ {n} Trade{'s' if n != 1 else ''} Ready to Execute")
                    st.caption("Sorted by conviction. Expand any card for full execution detail.")

                    summary_rows = []
                    for _rank_i, (_, r) in enumerate(acceptance_ready_df.iterrows(), start=1):
                        try:
                            mid_v    = float(_g(r, 'Mid_Price') or 0)
                            strike_v = float(_g(r, 'Selected_Strike') or 0)
                            otype    = str(_g(r, 'Option_Type', default='') or '').lower()
                            be = strike_v - mid_v if otype == 'put' else strike_v + mid_v if otype == 'call' else None

                            iv30  = _g(r, 'iv_30d', 'IV_30_D_Call')
                            hv30  = _g(r, 'HV30', 'hv_30')
                            gap   = _g(r, 'IVHV_gap_30D')
                            gap_s = f"{float(gap):+.1f}%" if gap else "—"
                            iv_s  = f"{float(iv30):.0f}%" if iv30 else "—"
                            hv_s  = f"{float(hv30):.0f}%" if hv30 else "—"

                            _conv  = float(r.get('_conviction') or 0)
                            _cap_r = float(r.get('Capital_Requirement') or 0)
                            _dqs_v = r.get('DQS_Score')
                            _tqs_v = r.get('TQS_Score')
                            _tqs_b = str(r.get('TQS_Band') or 'N/A')
                            _tqs_emoji = {'Ideal': '✅', 'Acceptable': '🟡', 'Stretched': '🟠', 'Chase': '🔴'}.get(_tqs_b, '')

                            # MC sizing columns (present after pipeline runs with MC enabled)
                            _mc_cvar  = r.get('MC_CVaR')
                            _mc_ratio = r.get('MC_CVaR_P10_Ratio')
                            _mc_p10   = r.get('MC_P10_Loss')
                            _mc_p50   = r.get('MC_P50_Outcome')
                            _mc_win   = r.get('MC_Win_Probability')
                            _mc_maxc  = r.get('MC_Max_Contracts')
                            _mc_sz    = str(r.get('Sizing_Method_Used') or '')
                            # Extract vol source from MC_Sizing_Note (e.g. "[EWMA(λ=0.94,AAPL)]" or "[hv_30]")
                            _mc_note_r = str(r.get('MC_Sizing_Note') or '')
                            _mc_vsrc  = (_mc_note_r.split('[')[1].split(']')[0] if '[' in _mc_note_r and ']' in _mc_note_r else '')
                            _mc_cvar_s  = f"${float(_mc_cvar):+,.0f}" if _mc_cvar and str(_mc_cvar) not in ('nan','None','') else "—"
                            _mc_ratio_s = f"{float(_mc_ratio):.2f}×" if _mc_ratio and str(_mc_ratio) not in ('nan','None','') else "—"
                            _mc_p10_s = f"${float(_mc_p10):+,.0f}" if _mc_p10 and str(_mc_p10) not in ('nan','None','') else "—"
                            _mc_p50_s = f"${float(_mc_p50):+,.0f}" if _mc_p50 and str(_mc_p50) not in ('nan','None','') else "—"
                            _mc_win_s = f"{float(_mc_win):.0%}" if _mc_win and str(_mc_win) not in ('nan','None','') else "—"
                            _mc_c_s   = str(int(float(_mc_maxc))) if _mc_maxc and str(_mc_maxc) not in ('nan','None','') else "—"
                            _sz_icon  = "🎲" if "MC" in _mc_sz else "📐"
                            # Mark EWMA vol source with indicator
                            _ewma_tag = " ⚡" if "EWMA" in _mc_vsrc else ""

                            summary_rows.append({
                                "#": _rank_i,
                                "Ticker": _g(r, 'Ticker', default='?'),
                                "Strategy": _g(r, 'Strategy_Name', default='—'),
                                "Bias": str(_g(r, 'Trade_Bias', default='—') or '').title(),
                                "Conf": str(_g(r, 'confidence_band', default='—') or '').title(),
                                "DQS": f"{int(_dqs_v)}" if _dqs_v and str(_dqs_v) not in ('nan','None','') else "—",
                                "TQS": f"{_tqs_emoji}{int(float(_tqs_v))}" if _tqs_v and str(_tqs_v) not in ('nan','None','N/A') else "—",
                                "Score": f"{_conv:.0f}",
                                "Cap Req": f"${_cap_r:,.0f}" if _cap_r else "—",
                                "Strike": f"${strike_v:.0f} {otype.upper()}",
                                "DTE": f"{int(float(_g(r, 'Actual_DTE') or 0))}d",
                                "Mid": f"${mid_v:.2f}",
                                "Spread": _fmt_pct(_g(r, 'Bid_Ask_Spread_Pct')),
                                "Gap": gap_s,
                                "Liquidity": str(_g(r, 'Liquidity_Grade', default='—')),
                                f"{_sz_icon} CVaR{_ewma_tag}": _mc_cvar_s,
                                "Tail": _mc_ratio_s,
                                "P50": _mc_p50_s,
                                "Win%": _mc_win_s,
                                "MaxC": _mc_c_s,
                            })
                        except Exception:
                            pass

                    if summary_rows:
                        st.dataframe(
                            pd.DataFrame(summary_rows),
                            width="stretch",
                            hide_index=True,
                        )
                        st.caption(
                            "**Score** = structural edge quality (DQS×50% + Signal strength×25% + IV data reliability×15% + Confidence×10%). "
                            "No capital or liquidity bias — a LEAP and a short-dated option with equal edge rank equally.  \n"
                            "**DQS** = directional quality (is the direction correct?).  "
                            "**TQS** = timing quality (is this the right *moment*?). ✅ Ideal ≥75 · 🟡 Acceptable 50-74 · 🟠 Stretched 25-49 · 🔴 Chase <25.  "
                            "**Cap Req** = capital at risk per contract.  \n"
                            "🎲 **MC columns** (Monte Carlo, 2,000 GBM paths, CVaR-based sizing): "
                            "**CVaR** = Conditional Value at Risk — mean P&L of worst 10% tail (coherent risk measure; Artzner 1999). "
                            "⚡ = EWMA(λ=0.94) vol used (forward-leaning; reacts faster to vol expansion/crush than flat HV_30). "
                            "**Tail** = CVaR/P10 ratio — tail fatness (1.0 = normal GBM; >1.5 = fat tail, size smaller). "
                            "**P50** = median expected P&L per contract; "
                            "**Win%** = fraction of paths that expire profitable; "
                            "**MaxC** = max contracts where CVaR ≤ 2% of account (McMillan Ch.3). "
                            "📐 = ATR/FIXED sizing used when MC skipped (missing spot/strike/DTE)."
                        )

                    st.caption("👇 Expand each card below for full execution detail, Greeks, exit rules, and volatility context.")

                    # ── Regime × Strategy Family Banner (above all cards) ──────────
                    if not acceptance_ready_df.empty:
                        _banner_regime = str(_g(acceptance_ready_df.iloc[0], 'Regime') or 'Unknown')
                        _banner_gate   = str(_g(acceptance_ready_df.iloc[0], 'Regime_Gate') or 'OPEN')
                        _banner_fits   = acceptance_ready_df['Regime_Strategy_Fit'].value_counts().to_dict() \
                            if 'Regime_Strategy_Fit' in acceptance_ready_df.columns else {}
                        _banner_note   = ''
                        if 'Regime_Strategy_Fit' in acceptance_ready_df.columns and 'Regime_Strategy_Note' in acceptance_ready_df.columns:
                            _note_rows = acceptance_ready_df[acceptance_ready_df['Regime_Strategy_Fit'].isin(['CAUTION', 'MISMATCH'])]
                            if not _note_rows.empty:
                                _banner_note = str(_note_rows['Regime_Strategy_Note'].iloc[0] or '')

                        _gate_icons  = {'OPEN': '🟢', 'RESTRICTED': '🟡', 'LOCKED': '🔴'}
                        _gate_icon   = _gate_icons.get(_banner_gate, '⚪')
                        _n_fit       = _banner_fits.get('FIT', 0)
                        _n_caution   = _banner_fits.get('CAUTION', 0)
                        _n_mismatch  = _banner_fits.get('MISMATCH', 0)
                        _fit_counts  = f"FIT: {_n_fit}  ·  CAUTION: {_n_caution}  ·  MISMATCH: {_n_mismatch}"
                        _has_mismatch = _n_mismatch > 0
                        _has_caution  = _n_caution > 0

                        with st.expander(
                            f"{_gate_icon} Regime Context: **{_banner_regime}** | Gate: {_banner_gate} | {_fit_counts}",
                            expanded=(_has_mismatch or _has_caution)
                        ):
                            if _banner_note:
                                st.info(f"📖 **Regime doctrine:** {_banner_note}")
                            # Surface shape distribution across READY candidates
                            if 'Surface_Shape' in acceptance_ready_df.columns:
                                _shapes = acceptance_ready_df['Surface_Shape'].value_counts().to_dict()
                                _shape_parts = [f"{s}: {n}" for s, n in _shapes.items()
                                                if s and str(s) not in ('—', 'nan', 'None', '')]
                                if _shape_parts:
                                    st.caption(f"Term structure across candidates — {' · '.join(_shape_parts)}")
                            if _has_mismatch:
                                st.error(
                                    f"🚫 **{_n_mismatch} candidate(s) are MISMATCH for the current regime.** "
                                    f"These strategy buckets are structurally inappropriate given "
                                    f"**{_banner_regime}** + **{_banner_gate}** conditions.  \n"
                                    f"The setup quality (DQS) may be fine — but regime persistence is against you. "
                                    f"Size down significantly or skip until regime confirms."
                                )
                            elif _has_caution:
                                st.warning(
                                    f"⚠️ **{_n_caution} candidate(s) are CAUTION-rated for the current regime.** "
                                    f"Proceed with reduced size or wait for regime confirmation before entering."
                                )
                            else:
                                if _banner_regime in ('Unknown', 'N/A', '', 'UNKNOWN'):
                                    st.info("ℹ️ Regime unknown — no fit/mismatch judgment possible. Accumulating IV history (30+ days required).")
                                else:
                                    st.success("✅ All candidates are regime-appropriate for current market conditions.")

                    st.divider()

                    for card_idx, (_, row) in enumerate(acceptance_ready_df.iterrows()):
                        ticker        = _g(row, 'Ticker', default='?')
                        strat_name    = _g(row, 'Strategy_Name', default='Unknown Strategy')
                        trade_bias    = _g(row, 'Trade_Bias', default='Unknown')
                        conf_band     = _g(row, 'confidence_band', default='LOW')
                        conf_color    = {'HIGH': '🟢', 'MEDIUM': '🟡', 'LOW': '🔴'}.get(str(conf_band).upper(), '⚪')
                        strat_type     = str(_g(row, 'Strategy_Type', 'strategy_type', default='') or '').upper()
                        is_directional = strat_type == 'DIRECTIONAL'
                        is_income      = strat_type == 'INCOME'
                        is_volatility  = strat_type == 'VOLATILITY'
                        is_buy_write   = str(strat_name).strip().lower() == 'buy-write'

                        # Plain-English gate reason (strip internal codes like "R3.2:")
                        raw_gate = str(_g(row, 'Gate_Reason', default='') or '')
                        import re as _re
                        gate_plain = _re.sub(r'^R\d+\.\d+:\s*', '', raw_gate).strip() or '—'

                        # ── Card wrapped in expander so list of 20 doesn't scroll forever ──
                        _mid_prev = _g(row, 'Mid_Price')
                        try:
                            _mid_str = f"  ·  Mid ${float(_mid_prev):.2f}"
                        except Exception:
                            _mid_str = ''
                        expander_label = f"{conf_color} **{ticker}** — {strat_name} — {str(trade_bias).title()}{_mid_str}  ·  {conf_band} confidence"
                        with st.expander(expander_label, expanded=(card_idx == 0)):
                          # Make the gate line meaningful: for LEAPs add maturity context
                          _dte_gate = None
                          try:
                              _dte_gate = float(_g(row, 'Actual_DTE') or 0)
                          except Exception:
                              pass
                          _is_leap = 'LEAP' in str(strat_name).upper() or (_dte_gate and _dte_gate >= 180)
                          _mat_level_gate = int(float(_g(row, 'IV_Maturity_Level', default=1) or 1))
                          _mat_state_gate = _g(row, 'IV_Maturity_State', default='IMMATURE')
                          _hist_cnt_gate  = int(float(_g(row, 'IV_History_Count', 'iv_history_count', default=0) or 0))
                          if _is_leap and gate_plain and gate_plain != '—':
                              _iv_note = f"IV history: {_hist_cnt_gate}d ({_mat_state_gate}) — IV rank unreliable until 60d+"
                              st.caption(f"Gate: {gate_plain}  ·  {_iv_note}")
                          else:
                              st.caption(f"Gate: {gate_plain}")

                          # ── Feedback Calibration row ──────────────────────────────
                          _fb_action  = str(_g(row, 'Feedback_Action') or '').upper()
                          _fb_note    = str(_g(row, 'Feedback_Note') or '').strip()
                          _fb_wr      = _g(row, 'Feedback_Win_Rate')
                          _fb_n       = int(float(_g(row, 'Feedback_Sample_N') or 0))
                          if _fb_action == 'TIGHTEN' and _fb_n > 0:
                              st.warning(
                                  f"📉 **Feedback: underperforming bucket** — "
                                  f"{_fb_note or f'{_fb_wr:.0%} win rate across {_fb_n} closed trades. DQS penalised ×0.80.'}"
                              )
                          elif _fb_action == 'REINFORCE' and _fb_n > 0:
                              st.success(
                                  f"📈 **Feedback: outperforming bucket** — "
                                  f"{_fb_note or f'{_fb_wr:.0%} win rate across {_fb_n} closed trades. DQS boosted ×1.10.'}"
                              )
                          elif _fb_n > 0 and _fb_action not in ('', 'INSUFFICIENT_SAMPLE', 'NAN', 'NONE'):
                              st.caption(f"📊 Feedback ({_fb_n} trades): {_fb_note or _fb_action}")

                          # ── Calendar risk row ─────────────────────────────────────
                          _cal_flag = str(_g(row, 'Calendar_Risk_Flag') or '').upper()
                          _cal_note = str(_g(row, 'Calendar_Risk_Note') or '').strip()
                          if _cal_flag == 'HIGH_BLEED':
                              st.warning(f"📅 **Calendar risk — pre-holiday long premium:** {_cal_note}")
                          elif _cal_flag == 'ELEVATED_BLEED':
                              st.caption(f"📅 Calendar note: {_cal_note}")
                          elif _cal_flag == 'PRE_HOLIDAY_EDGE':
                              st.success(f"📅 **Calendar edge — pre-holiday income entry:** {_cal_note}")
                          elif _cal_flag == 'ADVANTAGEOUS':
                              st.caption(f"📅 {_cal_note}")

                          # ── Open-position conflict warning ────────────────────────
                          _pos_conflict = str(_g(row, 'Position_Conflict') or '').strip()
                          if _pos_conflict:
                              _has_thesis_broken   = '|THESIS_BROKEN'   in _pos_conflict
                              _has_thesis_degraded = '|THESIS_DEGRADED' in _pos_conflict
                              if _pos_conflict.startswith('CONFLICT'):
                                  st.warning(
                                      f"⚠️ **Portfolio conflict:** {_pos_conflict}  \n"
                                      f"This scan recommendation runs opposite to an existing open position. "
                                      f"Confirm this is intentional (hedge vs. new directional bet) before executing."
                                  )
                              elif _has_thesis_broken:
                                  st.error(
                                      f"🚫 **SIZE_UP blocked by thesis — existing position BROKEN**  \n"
                                      f"The open {ticker} position's thesis is **BROKEN**. "
                                      f"Adding a second leg doubles exposure on a position whose investment thesis "
                                      f"has already failed. Check the Manage tab — evaluate closing the existing "
                                      f"position before entering a new one."
                                  )
                              elif _has_thesis_degraded:
                                  st.warning(
                                      f"⚠️ **SIZE_UP — existing {ticker} position thesis is DEGRADED**  \n"
                                      f"The open position's thesis is deteriorating. Adding here doubles a "
                                      f"weakening position. Review the Manage tab first — confirm the thesis "
                                      f"is still valid before scaling in."
                                  )
                              elif _pos_conflict.startswith('SIZE_UP|DIFF_STRUCTURE'):
                                  st.warning(
                                      f"📌 **Different-structure add:** {_pos_conflict.replace('SIZE_UP|DIFF_STRUCTURE: ', '').replace('SIZE_UP|DIFF_STRUCTURE|THESIS_DEGRADED: ', '').replace('SIZE_UP|DIFF_STRUCTURE|THESIS_BROKEN: ', '')}  \n"
                                      f"Same direction but different timeframe — this creates a multi-leg structure "
                                      f"with different theta/vega profiles on the same underlying. "
                                      f"Confirm this is intentional (layered thesis) and not an accidental duplicate."
                                  )
                              elif _pos_conflict.startswith('SIZE_UP'):
                                  st.info(
                                      f"📌 **Existing position:** {_pos_conflict}  \n"
                                      f"This adds to an existing directional position — confirm size is intentional."
                                  )

                          # ── Regime Gate + Capital Bucket ──────────────────────────
                          _regime_gate = str(_g(row, 'Regime_Gate') or 'OPEN').strip().upper()
                          _cap_bucket  = str(_g(row, 'Capital_Bucket') or '').strip()

                          _gate_icon, _gate_text, _gate_level = {
                              'OPEN':       ('🟢', 'Regime: OPEN',       None),
                              'RESTRICTED': ('🟡', 'Regime: RESTRICTED — reduce tactical size', 'warning'),
                              'LOCKED':     ('🔴', 'Regime: LOCKED — no new Bucket 1 entries', 'error'),
                          }.get(_regime_gate, ('⚪', f'Regime: {_regime_gate}', None))

                          if _gate_level == 'error':
                              st.error(f"{_gate_icon} {_gate_text}")
                          elif _gate_level == 'warning':
                              st.warning(f"{_gate_icon} {_gate_text}")
                          # OPEN: no banner — don't clutter the normal-regime case

                          if _cap_bucket:
                              _bucket_icon = {'TACTICAL': '🔵', 'STRATEGIC': '🟢', 'DEFENSIVE': '🟡'}.get(_cap_bucket, '⚪')
                              st.caption(f"{_bucket_icon} Bucket: **{_cap_bucket}**")

                          # ── Regime × Strategy Fit badge ───────────────────────────
                          _rsf = str(_g(row, 'Regime_Strategy_Fit') or 'FIT').strip().upper()
                          _rsn = str(_g(row, 'Regime_Strategy_Note') or '').strip()
                          _iv_regime_card = str(_g(row, 'Regime') or 'Unknown')
                          if _rsf == 'MISMATCH':
                              st.error(
                                  f"🚫 **Regime mismatch — {_cap_bucket} bucket not recommended in "
                                  f"{_iv_regime_card} + {_regime_gate} conditions.**  \n{_rsn}"
                              )
                          elif _rsf == 'CAUTION':
                              st.warning(
                                  f"⚠️ **Regime caution — {_cap_bucket} bucket tolerated but not preferred "
                                  f"in {_iv_regime_card} + {_regime_gate} conditions.**  \n{_rsn}"
                              )
                          # FIT: no badge — clean happy path

                          # ── IV Surface Shape Warning ──────────────────────────────
                          _ssw  = str(_g(row, 'Surface_Shape_Warning') or 'OK').strip().upper()
                          _sswn = str(_g(row, 'Surface_Shape_Warning_Note') or '').strip()
                          if _ssw == 'ELEVATED_COST':
                              st.warning(f"📈 **Inverted surface — near-dated vol cost elevated:**  \n{_sswn}")
                          elif _ssw == 'ASSIGNMENT_RISK':
                              st.warning(f"📈 **Inverted surface — assignment risk elevated:**  \n{_sswn}")

                          # ── Row 1: Stock context ──────────────────────────────────
                          stock_price   = _g(row, 'last_price', 'Stock_Price')
                          net_chg_pct   = _g(row, 'netPercentChange', 'net_percent_change')
                          rsi_val       = _g(row, 'RSI')
                          adx_val       = _g(row, 'ADX')
                          ema_sig       = _g(row, 'Chart_EMA_Signal', default='—')
                          trend_st      = _g(row, 'Trend_State', default='—')
                          hi52          = _g(row, '52WeekHigh')
                          lo52          = _g(row, '52WeekLow')
                          pos52         = _g(row, '52w_range_position')

                          c1, c2, c3, c4 = st.columns(4)
                          with c1:
                              arrow = "▲" if (net_chg_pct or 0) >= 0 else "▼"
                              st.metric("Stock Price", _fmt_price(stock_price), f"{arrow} {_fmt_pct(net_chg_pct)}" if net_chg_pct else None)
                          with c2:
                              st.metric("RSI", _fmt_float(rsi_val, 1))
                              st.caption(f"ADX {_fmt_float(adx_val, 1)}")
                          with c3:
                              st.metric("Trend / EMA", f"{trend_st} / {ema_sig}")
                          with c4:
                              pos_str = _fmt_pct(pos52, 0) if pos52 else "—"
                              st.metric("52W Position", pos_str)
                              st.caption(f"H {_fmt_price(hi52)} / L {_fmt_price(lo52)}")

                          st.divider()

                          # ── Row 2: Contract ───────────────────────────────────────
                          contract_sym  = _g(row, 'Contract_Symbol', default='—')
                          expiry        = _g(row, 'Selected_Expiration', default='—')
                          strike        = _g(row, 'Selected_Strike')
                          opt_type      = str(_g(row, 'Option_Type', default='—')).upper()
                          dte           = _g(row, 'Actual_DTE')

                          st.markdown("#### 📋 Contract")
                          c1, c2, c3, c4 = st.columns(4)
                          with c1:
                              st.metric("Symbol", str(contract_sym).strip())
                          with c2:
                              st.metric("Expiration", str(expiry))
                          with c3:
                              st.metric("Strike / Type", f"{_fmt_price(strike)} {opt_type}")
                          with c4:
                              st.metric("DTE", f"{int(float(dte))} days" if dte else "—")

                          st.divider()

                          # ── Row 3: Entry pricing ──────────────────────────────────
                          bid           = _g(row, 'Bid')
                          ask           = _g(row, 'Ask')
                          mid           = _g(row, 'Mid_Price')
                          last_opt      = _g(row, 'Last')
                          spread_pct    = _g(row, 'Bid_Ask_Spread_Pct')
                          oi            = _g(row, 'Open_Interest')
                          liq_grade     = _g(row, 'Liquidity_Grade', default='—')
                          liq_reason    = _g(row, 'Liquidity_Reason', default='')
                          theo_price    = _g(row, 'Theoretical_Price')
                          prem_vs_fv    = _g(row, 'Premium_vs_FairValue_Pct')
                          entry_lo      = _g(row, 'Entry_Band_Lower')
                          entry_hi      = _g(row, 'Entry_Band_Upper')

                          try:
                              # Chase limit only applies to directional buyers (you're paying a debit)
                              # Income sellers receive premium — no chase limit concept
                              if is_income or is_buy_write:
                                  chase_limit = None
                                  chase_limit_label = None
                              else:
                                  _liq_g = str(liq_grade or '').lower()
                                  _mid_f_cl = float(mid)
                                  _hi_f_cl  = float(entry_hi) if entry_hi else None

                                  # Deep ITM guard: BS band is meaningless when intrinsic > BS upper.
                                  # Black-Scholes undervalues deep ITM options (rho dominates, log-normal
                                  # distribution underweights the probability of staying deep ITM).
                                  # In this case the correct entry anchor is intrinsic + small time premium,
                                  # NOT the BS band. Suppress the chase limit entirely — it would mislead.
                                  try:
                                      _sp_cl   = float(stock_price or 0)
                                      _str_cl  = float(strike or 0)
                                      _intr_cl = (
                                          max(0.0, _str_cl - _sp_cl) if str(opt_type or '').upper() == 'PUT'
                                          else max(0.0, _sp_cl - _str_cl)
                                      )
                                      _is_deep_itm_cl = (
                                          _intr_cl > 0
                                          and _hi_f_cl is not None
                                          and _intr_cl >= _hi_f_cl * 0.85
                                      )
                                  except Exception:
                                      _is_deep_itm_cl = False

                                  if _is_deep_itm_cl:
                                      # Deep ITM: anchor limit to mid (don't pay ask), not BS upper
                                      # Time value is cheap; the "overpay" signal is a BS artifact.
                                      chase_limit = _mid_f_cl
                                      chase_limit_label = "💡 Limit entry (mid)"
                                  elif _hi_f_cl and _mid_f_cl > _hi_f_cl * 1.05:
                                      # Mid is >5% above BS upper on a non-deep-ITM option: real overpay
                                      chase_limit = _hi_f_cl
                                      chase_limit_label = "🚫 Chase limit (BS upper)"
                                  elif _liq_g in ('thin', 'poor'):
                                      # Thin liquidity: chase limit = mid (no tolerance above mid)
                                      chase_limit = _mid_f_cl
                                      chase_limit_label = "🚫 Chase limit (mid only)"
                                  else:
                                      chase_limit = _mid_f_cl * 1.03
                                      chase_limit_label = "🚫 Chase limit (mid +3%)"
                          except Exception:
                              chase_limit = None
                              chase_limit_label = None

                          # ── Now Score: microstructure readiness badge ─────────────────────
                          # Combines 5 intraday signals already present in Step12 output.
                          # Scores ≥50 → EXECUTE NOW, 25-49 → WAIT FOR FILL, <25 → NOT NOW
                          try:
                              _intraday_tag = str(_g(row, 'intraday_position_tag', default='') or '').upper()
                              _momentum_tag = str(_g(row, 'momentum_tag', default='') or '').upper()
                              _gap_tag      = str(_g(row, 'gap_tag', default='') or '').upper()
                              _gap_pct_val  = _g(row, 'gap_pct')
                              _vol_trend    = str(_g(row, 'Volume_Trend', default='') or '').upper()
                              _spread_pct_f = float(spread_pct) if spread_pct else None
                              _trade_bias   = str(_g(row, 'Trade_Bias', default='') or '').upper()

                              _now_score   = 0
                              _now_reasons = []

                              # 1. Intraday position tag (+25 / 0 / -15)
                              # Bull strategies want price near recent low (buy the dip)
                              # Bear strategies want price near recent high (sell the rip)
                              _is_bull_bias = 'BULL' in _trade_bias or str(_g(row, 'Strategy_Name', default='')).upper().find('PUT') == -1
                              _near_low  = 'NEAR_LOW'  in _intraday_tag
                              _near_high = 'NEAR_HIGH' in _intraday_tag
                              if (_is_bull_bias and _near_low) or (not _is_bull_bias and _near_high):
                                  _now_score += 25
                                  _now_reasons.append('price at entry zone')
                              elif (_is_bull_bias and _near_high) or (not _is_bull_bias and _near_low):
                                  _now_score -= 15
                                  _now_reasons.append('price extended from entry zone')

                              # 2. Momentum tag (+20 / 0 / -15)
                              if 'FLAT' in _momentum_tag:
                                  _now_score += 20
                                  _now_reasons.append('flat day — clean fill likely')
                              elif ('STRONG_UP' in _momentum_tag and not _is_bull_bias) or \
                                   ('STRONG_DOWN' in _momentum_tag and _is_bull_bias):
                                  _now_score -= 15
                                  _now_reasons.append('strong move against thesis')

                              # 3. Bid-ask spread (+20 / +10 / -15)
                              if _spread_pct_f is not None:
                                  if _spread_pct_f <= 1.0:
                                      _now_score += 20
                                      _now_reasons.append('tight spread')
                                  elif _spread_pct_f <= 3.0:
                                      _now_score += 10
                                      _now_reasons.append('acceptable spread')
                                  else:
                                      _now_score -= 15
                                      _now_reasons.append(f'wide spread {_spread_pct_f:.1f}%')

                              # 4. Gap (+15 / -20)
                              _gap_pct_f = float(_gap_pct_val) if _gap_pct_val else 0.0
                              if 'NO_GAP' in _gap_tag or abs(_gap_pct_f) < 0.5:
                                  _now_score += 15
                                  _now_reasons.append('no gap — price settled')
                              elif abs(_gap_pct_f) >= 1.5:
                                  _now_score -= 20
                                  _now_reasons.append(f'gap {_gap_pct_f:+.1f}% — move in price')

                              # 5. Volume trend (+10 / -5)
                              if 'RISING' in _vol_trend:
                                  _now_score += 10
                                  _now_reasons.append('volume rising')
                              elif 'FALLING' in _vol_trend:
                                  _now_score -= 5
                                  _now_reasons.append('volume thin')

                              # Clamp and render badge
                              _now_score = max(-30, min(90, _now_score))
                              if _now_score >= 50:
                                  _badge_color = '#1a7f37'
                                  _badge_label = '🟢 EXECUTE NOW'
                              elif _now_score >= 25:
                                  _badge_color = '#9a6700'
                                  _badge_label = '🟡 WAIT FOR FILL'
                              else:
                                  _badge_color = '#cf222e'
                                  _badge_label = '🔴 NOT NOW'

                              _reason_str = ' · '.join(_now_reasons) if _now_reasons else 'no microstructure data'
                              st.markdown(
                                  f"<div style='padding:8px 12px; border-radius:6px; background:rgba(0,0,0,0.08); "
                                  f"border-left:4px solid {_badge_color}; margin-bottom:8px;'>"
                                  f"<span style='font-weight:700; color:{_badge_color}; font-size:1.05em;'>{_badge_label}</span>"
                                  f"&nbsp;&nbsp;<span style='color:#888; font-size:0.88em;'>score {_now_score} · {_reason_str}</span>"
                                  f"</div>",
                                  unsafe_allow_html=True
                              )
                          except Exception:
                              pass  # Badge is informational — never break card rendering

                          st.markdown("#### 💵 Entry Pricing")
                          c1, c2, c3, c4 = st.columns(4)
                          with c1:
                              st.metric("Bid / Ask", f"{_fmt_price(bid)} / {_fmt_price(ask)}")
                              st.caption(f"Spread: {_fmt_pct(spread_pct)}")
                          with c2:
                              st.metric("Mid (target entry)", _fmt_price(mid))
                              try:
                                  _last_f = float(last_opt) if last_opt else None
                                  _mid_f  = float(mid) if mid else None
                                  if _last_f and _mid_f and abs(_last_f - _mid_f) / _mid_f > 0.20:
                                      st.caption(f"Last trade: {_fmt_price(last_opt)} ⚠️ stale print — use mid")
                                  else:
                                      st.caption(f"Last trade: {_fmt_price(last_opt)}")
                              except Exception:
                                  st.caption(f"Last trade: {_fmt_price(last_opt)}")
                          with c3:
                              if entry_lo and entry_hi:
                                  st.metric("BS Fair-value band", f"{_fmt_price(entry_lo)} – {_fmt_price(entry_hi)}")
                                  try:
                                      mid_f2 = float(mid)
                                      lo_f   = float(entry_lo)
                                      hi_f   = float(entry_hi)
                                      if is_income or is_buy_write:
                                          # Income/Buy-Write sellers: above FV band = GOOD (selling expensive)
                                          if mid_f2 > hi_f:
                                              over_pct = (mid_f2 - hi_f) / hi_f * 100
                                              st.caption(f"✅ Selling {over_pct:.1f}% above BS fair value")
                                          elif mid_f2 < lo_f:
                                              under_pct = (lo_f - mid_f2) / lo_f * 100
                                              st.caption(f"🔴 Selling {under_pct:.1f}% below BS fair value — consider limit order at {_fmt_price(lo_f)}+")
                                          else:
                                              st.caption(f"✅ Within BS fair-value band")
                                      else:
                                          # Directional buyers: below FV = good (getting discount)
                                          if mid_f2 < lo_f:
                                              edge_pct = (lo_f - mid_f2) / lo_f * 100
                                              st.caption(f"✅ Entry {edge_pct:.1f}% below BS fair value")
                                          elif mid_f2 > hi_f:
                                              over_pct = (mid_f2 - hi_f) / hi_f * 100
                                              # Deep ITM check: if intrinsic value alone exceeds the BS band
                                              # the "overpay" is a vol-input artifact, not a real edge erosion.
                                              # Intrinsic for put = max(0, strike - spot); for call = max(0, spot - strike)
                                              try:
                                                  _sp_f   = float(stock_price or 0)
                                                  _str_f  = float(strike or 0)
                                                  _intrinsic = (
                                                      max(0.0, _str_f - _sp_f) if opt_type == 'PUT'
                                                      else max(0.0, _sp_f - _str_f)
                                                  )
                                                  _is_deep_itm = _intrinsic > 0 and _intrinsic >= hi_f * 0.85
                                              except Exception:
                                                  _is_deep_itm = False

                                              if _is_deep_itm:
                                                  # BS band is below intrinsic — artifact of vol/rate inputs,
                                                  # not a real overpay signal. Surface time value instead.
                                                  _time_val = max(0.0, mid_f2 - _intrinsic)
                                                  st.caption(
                                                      f"ℹ️ Deep ITM: intrinsic = {_fmt_price(_intrinsic)}, "
                                                      f"time value = {_fmt_price(_time_val)} — "
                                                      f"BS band below intrinsic (vol-input artifact, not overpay)"
                                                  )
                                              elif over_pct > 5:
                                                  st.caption(f"🔴 Paying {over_pct:.1f}% above BS fair value — enter limit at {_fmt_price(hi_f)} or skip")
                                              else:
                                                  st.caption(f"⚠️ Paying {over_pct:.1f}% above BS fair value")
                                          else:
                                              st.caption(f"✅ Within BS fair-value band")
                                  except Exception:
                                      st.caption(f"BS fair value: {_fmt_price(theo_price)}")
                              else:
                                  st.metric("BS Fair-value band", "—")
                                  st.caption("IV history too short for BS pricing")
                          with c4:
                              if chase_limit and chase_limit_label:
                                  st.metric(chase_limit_label, _fmt_price(chase_limit))
                                  _liq_g2 = str(liq_grade or '').lower()
                                  if chase_limit_label == "💡 Limit entry (mid)":
                                      st.caption(
                                          "Deep ITM — BS band is a vol-input artifact here. "
                                          "Enter at mid or below; time value is already thin."
                                      )
                                  elif chase_limit_label == "🚫 Chase limit (BS upper)":
                                      st.caption("Paying above BS fair value — limit at BS upper or wait")
                                  elif _liq_g2 in ('thin', 'poor'):
                                      st.caption("Thin liquidity — use limit at mid, do NOT pay ask")
                                  else:
                                      st.caption("Do NOT pay above this")
                              elif is_buy_write:
                                  st.metric("💰 Call premium received", _fmt_price(mid))
                                  st.caption("Sell call at mid or better — reduces cost basis of stock purchase")
                              elif is_income:
                                  st.metric("💰 You receive", _fmt_price(mid))
                                  st.caption("Sell at mid or better (higher = more premium)")
                              st.caption(f"Liquidity: **{liq_grade}** — {liq_reason}")
                              st.caption(f"OI: {int(float(oi)) if oi else '—'}")

                          st.divider()

                          # ── Row 4: GTC Exit Rules ─────────────────────────────────
                          try:
                              mid_f = float(mid)
                              if is_buy_write:
                                  # Buy-Write: collected premium on call + own stock.
                                  # Call profit target: buy back call at 50% of premium (keep 50%)
                                  # Stock stop: sell stock if it drops 8% below cost basis
                                  _bw_stock = float(stock_price) if stock_price else 0
                                  profit_tgt = mid_f * 0.50        # buy back call at half price
                                  stop_loss  = _bw_stock * 0.92    # stock stop at −8% of purchase price
                                  profit_usd = (mid_f - profit_tgt) * 100   # call premium kept
                                  loss_usd   = (_bw_stock - stop_loss - mid_f) * 100  # net stock loss after premium
                              elif is_income:
                                  # Income: you collected the premium. Profit = buy back cheap.
                                  # Profit target: buy back at 50% of premium collected (keep 50%)
                                  # Stop loss: buy back at 200% of premium collected (loss = 1× premium)
                                  profit_tgt = mid_f * 0.50   # buy back at half price
                                  stop_loss  = mid_f * 2.00   # buy back at double price
                                  profit_usd = (mid_f - profit_tgt) * 100   # credit kept
                                  loss_usd   = (stop_loss - mid_f) * 100    # extra debit paid
                              else:
                                  # Long options: you paid the premium. Profit = sell higher.
                                  profit_tgt = mid_f * 1.50
                                  stop_loss  = mid_f * 0.50
                                  profit_usd = (profit_tgt - mid_f) * 100
                                  loss_usd   = (mid_f - stop_loss) * 100
                          except Exception:
                              profit_tgt = stop_loss = profit_usd = loss_usd = None

                          if is_buy_write:
                              profit_verb         = "Buy back call at"
                              stop_verb           = "Sell stock at"
                              exit_caption_profit = "Buy to close call when 50% of premium captured — keep stock for further upside (Cohen Ch.7)"
                              exit_caption_stop   = "Sell stock if it drops 8% below purchase price — premium reduces net loss"
                          elif is_income:
                              profit_verb         = "Buy back at"
                              stop_verb           = "Buy back at"
                              exit_caption_profit = "Buy to close when 50% of max premium is captured (Cohen: Ch.7)"
                              exit_caption_stop   = "Buy to close at 2× premium to cap loss (Natenberg Ch.14)"
                          else:
                              profit_verb         = "Sell at"
                              stop_verb           = "Sell at"
                              exit_caption_profit = "Take half the max gain on long options (Natenberg Ch.12)"
                              if _is_leap:
                                  _stop_side = "stock has rallied against put" if opt_type == 'PUT' else "stock has fallen against call"
                                  exit_caption_stop = f"Delta loss: {_stop_side} — cut and redeploy (theta is not the risk here)"
                              elif opt_type == 'PUT':
                                  exit_caption_stop = "Stock rallying against position — cut delta loss early"
                              else:
                                  exit_caption_stop = "Cut losers early — theta erodes value as DTE shrinks"

                          st.markdown("#### 🎯 GTC Exit Rules (Good-Till-Cancelled)")
                          c1, c2, c3 = st.columns(3)
                          with c1:
                              _profit_pct = "+50% (call)" if is_buy_write else "+50%"
                              profit_label = f"**Profit target: {_profit_pct}**\n{profit_verb} {_fmt_price(profit_tgt)}"
                              if profit_usd:
                                  profit_label += f"  (+${profit_usd:,.0f}/contract)"
                              st.success(profit_label)
                              st.caption(exit_caption_profit)
                          with c2:
                              if is_buy_write:
                                  _stop_pct = "–8% (stock)"
                              elif is_income:
                                  _stop_pct = "–200%"
                              else:
                                  _stop_pct = "–50%"
                              stop_label = f"**Stop loss: {_stop_pct}**\n{stop_verb} {_fmt_price(stop_loss)}"
                              if loss_usd:
                                  stop_label += f"  (−${loss_usd:,.0f}/contract)"
                              st.error(stop_label)
                              st.caption(exit_caption_stop)
                          with c3:
                              if is_buy_write:
                                  time_stop_dte = 14
                                  st.warning(f"**Time stop: DTE ≤ {time_stop_dte}**\nClose or roll the call")
                                  st.caption("Let call expire worthless OR roll to next month — do not sell stock unless hitting stock stop")
                              elif _is_leap:
                                  # LEAPs: roll/exit at 90 DTE remaining — avoid vega collapse in final stretch
                                  time_stop_dte = 90
                                  st.warning(f"**Time stop: DTE ≤ {time_stop_dte}**\nRoll or exit 3 months before expiry")
                                  st.caption("LEAP vega decays sharply inside 90 DTE — roll to next year or exit to capture remaining value (Natenberg Ch.12)")
                              else:
                                  time_stop_dte = 14
                                  st.warning(f"**Time stop: DTE ≤ {time_stop_dte}**\nExit regardless of P&L")
                                  st.caption("Gamma risk accelerates inside 2 weeks — avoid holding through expiry")

                          st.divider()

                          # ── Row 5: Greeks ─────────────────────────────────────────
                          delta_v   = _g(row, 'Delta')
                          gamma_v   = _g(row, 'Gamma')
                          vega_v    = _g(row, 'Vega')
                          theta_v   = _g(row, 'Theta')
                          impl_vol  = _g(row, 'Implied_Volatility')
                          try:
                              impl_vol_disp = _fmt_pct(impl_vol) if float(impl_vol) <= 200 else "—"
                          except Exception:
                              impl_vol_disp = "—"

                          st.markdown("#### 🔢 Greeks")
                          c1, c2, c3, c4, c5 = st.columns(5)
                          with c1:
                              if is_buy_write:
                                  try:
                                      _call_delta = float(delta_v)
                                      _combined_delta = 1.0 - _call_delta  # long stock (Δ=1) − short call
                                      st.metric("Delta (Δ)", f"{_fmt_float(delta_v)} call")
                                      st.caption(f"Combined position Δ ≈ {_combined_delta:.3f} (stock 1.0 − call {_call_delta:.3f})")
                                  except Exception:
                                      st.metric("Delta (Δ)", _fmt_float(delta_v))
                                      st.caption("Call delta (combined ≈ 1 − call delta)")
                              else:
                                  st.metric("Delta (Δ)", _fmt_float(delta_v))
                                  try:
                                      _dv = float(delta_v)
                                      if opt_type == 'PUT' and _dv < 0:
                                          st.caption(f"{_dv:+.3f} per $1 stock rise — put profits as stock falls")
                                      elif opt_type == 'CALL' and _dv > 0:
                                          st.caption(f"+{_dv:.3f} per $1 stock rise — call profits as stock rises")
                                      else:
                                          st.caption("Directional exposure per $1 move")
                                  except Exception:
                                      st.caption("Directional exposure per $1 move")
                          with c2:
                              st.metric("Gamma (Γ)", _fmt_float(gamma_v))
                              try:
                                  _gamma_f = float(gamma_v)
                                  if _is_leap and _gamma_f < 0.02:
                                      # Quantify the practical impact: Δ shift per $10 move
                                      _delta_shift_10 = _gamma_f * 10
                                      st.caption(
                                          f"Structural LEAP feature — $10 move shifts delta by only {_delta_shift_10:.2f} "
                                          f"(vega dominates at {int(_dte_gate or _dte_f)}d DTE; Passarelli Ch.2)"
                                      )
                                  elif _is_leap:
                                      _delta_shift_10 = _gamma_f * 10
                                      st.caption(
                                          f"Delta change per $1 move — $10 move shifts delta by {_delta_shift_10:.2f} "
                                          f"(lower than short-dated; normal for LEAPs)"
                                      )
                                  else:
                                      _delta_shift_5 = _gamma_f * 5
                                      st.caption(
                                          f"Delta change per $1 move — $5 move shifts delta by {_delta_shift_5:.2f}"
                                      )
                              except Exception:
                                  st.caption("Delta change per $1 move")
                          with c3:
                              st.metric("Vega (V)", _fmt_float(vega_v))
                              if is_income or is_buy_write:
                                  st.caption("Rising IV hurts (short vega position)")
                              else:
                                  st.caption("P&L per 1% IV move (rising IV helps)")
                          with c4:
                              st.metric("Theta (Θ)", _fmt_float(theta_v))
                              try:
                                  _theta_f = float(theta_v)
                                  _mid_f2  = float(mid) if mid else None
                                  if is_income or is_buy_write:
                                      # Short position: theta shown as negative on contract, but positive for seller
                                      _theta_pct = abs(_theta_f) / _mid_f2 * 100 if (_mid_f2 and _mid_f2 > 0) else 0
                                      st.caption(f"Time decay works FOR you — earn {_theta_pct:.1f}%/day of premium")
                                  elif _mid_f2 and _mid_f2 > 0 and _theta_f < 0:
                                      _theta_pct = abs(_theta_f) / _mid_f2 * 100
                                      if _is_leap and _theta_pct < 0.1:
                                          st.caption(f"Slow decay {_theta_pct:.2f}%/day — negligible for LEAP; monitor delta, not theta")
                                      else:
                                          st.caption(f"Daily time decay — {_theta_pct:.1f}%/day of premium")
                                  else:
                                      st.caption("Daily time decay ($/day)")
                              except Exception:
                                  st.caption("Daily time decay ($/day)")
                          with c5:
                              st.metric("Contract IV", impl_vol_disp)
                              st.caption("Implied vol at this strike")

                          st.divider()

                          # ── Row 6: Risk Profile ───────────────────────────────────
                          risk_profile  = _g(row, 'Risk_Profile', default='—')
                          greeks_exp    = _g(row, 'Greeks_Exposure', default='—')
                          try:
                              mid_f_r      = float(mid)
                              strike_f     = float(strike)
                              if is_buy_write:
                                  # Buy-Write breakeven: stock must recover full cost minus premium received
                                  _bw_stock = float(stock_price) if stock_price else strike_f
                                  breakeven = _bw_stock - mid_f_r
                              elif opt_type == 'PUT':
                                  breakeven = strike_f - mid_f_r
                              else:
                                  breakeven = strike_f + mid_f_r

                              if is_buy_write:
                                  # Buy-Write: buy stock + sell call simultaneously.
                                  # Max loss = (stock cost − premium received) × 100 (stock goes to zero)
                                  # Capital = full stock cost, partially offset by premium received
                                  stock_cost  = float(stock_price) if stock_price else strike_f
                                  max_loss    = (stock_cost - mid_f_r) * 100
                                  cap_display = stock_cost * 100
                                  cap_note    = f"Stock purchase ({_fmt_price(stock_price)} × 100) − premium received"
                              elif is_income and opt_type == 'PUT':
                                  # CSP: max loss = (strike − premium) × 100 (stock assigned at strike, net of premium)
                                  max_loss    = (strike_f - mid_f_r) * 100
                                  cap_display = strike_f * 100
                                  cap_note    = f"Cash-secured: {_fmt_price(strike)} × 100 shares"
                              elif is_income and opt_type == 'CALL':
                                  # CC: max loss is opportunity cost — stock could be called away
                                  max_loss    = None
                                  cap_display = None
                                  cap_note    = "Covered call — stock already in portfolio"
                              else:
                                  # Long option: max loss = full premium paid
                                  max_loss    = mid_f_r * 100
                                  cap_display = mid_f_r * 100
                                  cap_note    = f"Max loss = premium paid ({_fmt_price(mid)} × 100)"
                          except Exception:
                              max_loss = breakeven = cap_display = None
                              cap_note = risk_profile

                          # ── Compute Expected Move for this row ──────────────
                          _em_pct = row.get('_Expected_Move_Pct')
                          try:
                              _em_pct_f = float(_em_pct) if _em_pct and str(_em_pct) not in ('nan', 'None', '') else None
                          except Exception:
                              _em_pct_f = None
                          _em_dollar = None
                          if _em_pct_f is not None and stock_price:
                              try:
                                  _em_dollar = float(stock_price) * _em_pct_f / 100.0
                              except Exception:
                                  pass

                          st.markdown("#### ⚠️ Risk Profile")
                          c1, c2, c3, c4 = st.columns(4)
                          with c1:
                              if is_buy_write and max_loss:
                                  st.metric("Max Loss (1 contract)", f"${max_loss:,.0f}")
                                  st.caption(f"Stock to zero minus premium collected (worst case: stock → $0)")
                              elif max_loss and not is_income:
                                  st.metric("Max Loss (1 contract)", f"${max_loss:,.0f}")
                                  st.caption(f"Defined (max loss = premium paid)")
                              else:
                                  st.metric("Max Loss (1 contract)", "Opportunity cost")
                                  st.caption(risk_profile)
                          with c2:
                              st.metric("Breakeven at expiry", _fmt_price(breakeven))
                              if is_buy_write:
                                  st.caption(f"Stock cost {_fmt_price(stock_price)} − premium {_fmt_price(mid)}")
                              elif opt_type == 'PUT' and breakeven:
                                  try:
                                      _sp_be  = float(stock_price or 0)
                                      _be_val = float(breakeven)
                                      if _sp_be > 0 and _sp_be < _be_val:
                                          _be_cushion = _be_val - _sp_be
                                          _be_pct     = _be_cushion / _sp_be * 100
                                          st.caption(
                                              f"Strike {_fmt_price(strike)} − premium {_fmt_price(mid)}  \n"
                                              f"✅ **Already profitable at entry** — stock (${_sp_be:.2f}) is "
                                              f"${_be_cushion:.2f} ({_be_pct:.1f}%) below breakeven. "
                                              f"Position is in-the-money at current price."
                                          )
                                      else:
                                          _gap = _sp_be - _be_val if _sp_be > 0 else 0
                                          _gap_pct = _gap / _sp_be * 100 if _sp_be > 0 else 0
                                          st.caption(
                                              f"Strike {_fmt_price(strike)} − premium {_fmt_price(mid)} — "
                                              f"stock must fall below {_fmt_price(breakeven)} to profit  \n"
                                              f"Gap to breakeven: ${_gap:.2f} ({_gap_pct:.1f}% further decline needed)"
                                          )
                                  except Exception:
                                      st.caption(f"Strike {_fmt_price(strike)} − premium {_fmt_price(mid)} — stock must fall below {_fmt_price(breakeven)} to profit")
                              elif opt_type == 'CALL' and breakeven:
                                  try:
                                      _sp_be  = float(stock_price or 0)
                                      _be_val = float(breakeven)
                                      if _sp_be > 0 and _sp_be > _be_val:
                                          _be_cushion = _sp_be - _be_val
                                          _be_pct     = _be_cushion / _sp_be * 100
                                          st.caption(
                                              f"Strike {_fmt_price(strike)} + premium {_fmt_price(mid)}  \n"
                                              f"✅ **Already profitable at entry** — stock (${_sp_be:.2f}) is "
                                              f"${_be_cushion:.2f} ({_be_pct:.1f}%) above breakeven. "
                                              f"Position is in-the-money at current price."
                                          )
                                      else:
                                          _gap = _be_val - _sp_be if _sp_be > 0 else 0
                                          _gap_pct = _gap / _sp_be * 100 if _sp_be > 0 else 0
                                          st.caption(
                                              f"Strike {_fmt_price(strike)} + premium {_fmt_price(mid)} — "
                                              f"stock must rise above {_fmt_price(breakeven)} to profit  \n"
                                              f"Gap to breakeven: ${_gap:.2f} ({_gap_pct:.1f}% further rise needed)"
                                          )
                                  except Exception:
                                      st.caption(f"Strike {_fmt_price(strike)} + premium {_fmt_price(mid)} — stock must rise above {_fmt_price(breakeven)} to profit")
                              else:
                                  st.caption(f"Strike {_fmt_price(strike)} {'−' if opt_type == 'PUT' else '+'} premium {_fmt_price(mid)}")
                          with c3:
                              if is_buy_write and cap_display is not None:
                                  try:
                                      _net_outlay = cap_display - float(mid) * 100
                                      st.metric("Net Capital (1 lot)", f"${_net_outlay:,.0f}")
                                      st.caption(f"Stock ${cap_display:,.0f} − premium ${float(mid)*100:,.0f} received")
                                  except Exception:
                                      st.metric("Capital Required", f"${cap_display:,.0f}")
                                      st.caption(cap_note)
                              elif cap_display is not None:
                                  # Format as whole dollars — no cents on large capital figures
                                  try:
                                      _cap_f = float(cap_display)
                                      _cap_str = f"${_cap_f:,.0f}" if _cap_f >= 100 else _fmt_price(cap_display)
                                  except Exception:
                                      _cap_str = _fmt_price(cap_display)
                                  st.metric("Capital Required", _cap_str)
                                  st.caption(cap_note)
                              else:
                                  st.metric("Capital Required", "Stock owned")
                                  st.caption(cap_note)
                          with c4:
                              if _em_pct_f is not None:
                                  _em_label = f"{_em_pct_f:.1f}%"
                                  if _em_dollar is not None:
                                      _em_label += f" (${_em_dollar:,.0f})"
                                  st.metric("Expected Move (1σ)", _em_label)
                                  # Compare to breakeven distance
                                  try:
                                      _sp_f = float(stock_price)
                                      _be_f = float(breakeven)
                                      _be_gap = abs(_sp_f - _be_f)
                                      _be_gap_pct = _be_gap / _sp_f * 100 if _sp_f > 0 else 0
                                      if _be_gap_pct > 0:
                                          _coverage = _em_pct_f / _be_gap_pct
                                          if _coverage >= 1.5:
                                              st.caption(f"BE gap {_be_gap_pct:.1f}% — EM covers **{_coverage:.1f}x** breakeven")
                                          elif _coverage >= 1.0:
                                              st.caption(f"BE gap {_be_gap_pct:.1f}% — EM covers {_coverage:.1f}x breakeven")
                                          else:
                                              st.caption(f"BE gap {_be_gap_pct:.1f}% — EM covers only {_coverage:.1f}x of breakeven")
                                      else:
                                          st.caption(f"IV × √(DTE/365) — 1σ implied move")
                                  except Exception:
                                      st.caption(f"IV × √(DTE/365) — 1σ implied move")
                              else:
                                  st.metric("Expected Move (1σ)", "—")
                                  st.caption("IV or DTE unavailable")

                          st.divider()

                          # ── Row 6.5: Monte Carlo P&L Distribution ────────────────
                          _mc_cvar_v  = row.get('MC_CVaR')
                          _mc_ratio_v = row.get('MC_CVaR_P10_Ratio')
                          _mc_p10_v   = row.get('MC_P10_Loss')
                          _mc_p50_v   = row.get('MC_P50_Outcome')
                          _mc_p90_v   = row.get('MC_P90_Gain')
                          _mc_win_v   = row.get('MC_Win_Probability')
                          _mc_asgn_v  = row.get('MC_Assign_Prob')
                          _mc_maxc_v  = row.get('MC_Max_Contracts')
                          _mc_note_v  = str(row.get('MC_Sizing_Note') or '')
                          _mc_sz_v    = str(row.get('Sizing_Method_Used') or 'FIXED')
                          _mc_paths_v = row.get('MC_Paths_Used')
                          # Extract vol source from note (e.g. "[EWMA(λ=0.94,AAPL)]" or "[hv_30]")
                          _mc_vsrc_c  = (_mc_note_v.split('[')[1].split(']')[0] if '[' in _mc_note_v and ']' in _mc_note_v else 'HV')
                          _mc_ewma_used = 'EWMA' in _mc_vsrc_c

                          _mc_ran = (
                              _mc_p10_v is not None
                              and str(_mc_p10_v) not in ('nan', 'None', '')
                              and int(float(_mc_paths_v or 0)) > 0
                          )

                          if _mc_ran:
                              _ewma_badge = " ⚡EWMA" if _mc_ewma_used else ""
                              st.markdown(f"#### 🎲 Monte Carlo P&L Distribution{_ewma_badge}")
                              try:
                                  _cvar_f = float(_mc_cvar_v) if _mc_cvar_v and str(_mc_cvar_v) not in ('nan','None','') else None
                                  _ratio_f = float(_mc_ratio_v) if _mc_ratio_v and str(_mc_ratio_v) not in ('nan','None','') else None
                                  _p10_f  = float(_mc_p10_v)
                                  _p50_f  = float(_mc_p50_v)
                                  _p90_f  = float(_mc_p90_v)
                                  _win_f  = float(_mc_win_v)
                                  _maxc_i = int(float(_mc_maxc_v))

                                  # Row A: CVaR + tail ratio + P50 + Win%
                                  _mc_c1, _mc_c2, _mc_c3, _mc_c4 = st.columns(4)
                                  with _mc_c1:
                                      if _cvar_f is not None:
                                          _cvar_color = "🔴" if _cvar_f < -500 else ("🟡" if _cvar_f < -100 else "🟢")
                                          _fat_note = f" (fat tail {_ratio_f:.1f}×)" if _ratio_f and _ratio_f > 1.5 else ""
                                          _ratio_desc = f"{_ratio_f:.2f}× — {'fat tail, size conservatively' if _ratio_f > 1.5 else 'normal tail'}" if _ratio_f else ""
                                          st.metric(
                                              f"{_cvar_color} CVaR (tail mean)",
                                              f"${_cvar_f:+,.0f}",
                                              help=(
                                                  "Conditional Value at Risk — mean P&L of worst 10% tail paths. "
                                                  "Coherent risk measure (Artzner 1999). "
                                                  f"CVaR/P10 ratio = {_ratio_desc}. "
                                                  "Sizing denominator: MaxC = (account × 2%) / |CVaR|."
                                              ) if _ratio_f else "Mean P&L of worst 10% paths — CVaR sizing denominator."
                                          )
                                      else:
                                          st.metric("CVaR", "—")
                                  with _mc_c2:
                                      _p10_color = "🔴" if _p10_f < -200 else ("🟡" if _p10_f < 0 else "🟢")
                                      st.metric(
                                          f"{_p10_color} P10 (bad day)",
                                          f"${_p10_f:+,.0f}",
                                          help="10th-percentile P&L per contract — boundary of worst 10% of paths"
                                      )
                                  with _mc_c3:
                                      _p50_color = "🟢" if _p50_f >= 0 else "🔴"
                                      st.metric(
                                          f"{_p50_color} P50 (median)",
                                          f"${_p50_f:+,.0f}",
                                          help="Median expected P&L per contract at expiry"
                                      )
                                  with _mc_c4:
                                      _win_color = "🟢" if _win_f >= 0.55 else ("🟡" if _win_f >= 0.45 else "🔴")
                                      st.metric(
                                          f"{_win_color} Win Prob",
                                          f"{_win_f:.0%}",
                                          help="Fraction of simulated paths that expire profitable"
                                      )

                                  # Row B: P90 + tail ratio badge + vol source
                                  _mc_d1, _mc_d2 = st.columns([1, 3])
                                  with _mc_d1:
                                      st.metric(
                                          "P90 (good day)",
                                          f"${_p90_f:+,.0f}",
                                          help="90th-percentile P&L — best realistic outcome"
                                      )
                                  with _mc_d2:
                                      if _ratio_f is not None:
                                          _tail_color = "🔴" if _ratio_f > 2.0 else ("🟡" if _ratio_f > 1.5 else "🟢")
                                          _tail_label = "fat tail — size conservatively" if _ratio_f > 1.5 else "normal tail"
                                          st.metric(
                                              f"{_tail_color} Tail Fatness (CVaR/P10)",
                                              f"{_ratio_f:.2f}×",
                                              help=(
                                                  "CVaR÷P10 ratio. 1.0 = perfectly normal GBM tail. "
                                                  ">1.5 = fat tail (short DTE deep-ITM, high vol). "
                                                  "MC sizes more conservatively when ratio is high."
                                              )
                                          )

                                  # Assignment probability (income strategies only)
                                  if _mc_asgn_v is not None and str(_mc_asgn_v) not in ('nan', 'None', ''):
                                      _asgn_f = float(_mc_asgn_v)
                                      if _asgn_f > 0:
                                          _asgn_color = "🔴" if _asgn_f > 0.30 else ("🟡" if _asgn_f > 0.15 else "🟢")
                                          st.caption(
                                              f"{_asgn_color} **Assignment probability at expiry:** {_asgn_f:.0%}  "
                                              f"({'elevated — widen strike or reduce DTE' if _asgn_f > 0.30 else 'acceptable' if _asgn_f > 0.15 else 'low — well cushioned'})"
                                          )

                                  # Sizing recommendation
                                  _vol_src_display = f"`{_mc_vsrc_c}`" + (" ⚡ EWMA(λ=0.94) — forward-leaning, reacts faster to vol expansion" if _mc_ewma_used else " — flat backward-looking window")
                                  st.caption(
                                      f"🎲 **MC sizing** (2,000 GBM paths): "
                                      f"max **{_maxc_i} contract{'s' if _maxc_i != 1 else ''}** where **CVaR** ≤ 2% of account (McMillan Ch.3). "
                                      f"Vol source: {_vol_src_display}.  \n"
                                      f"_{_mc_note_v}_"
                                  )
                              except Exception as _mc_render_err:
                                  st.caption(f"🎲 MC data available but render failed: {_mc_render_err}")
                          else:
                              # MC skipped — show why and fall back gracefully
                              _skip_reason = _mc_note_v if _mc_note_v.startswith("MC_SKIP") else "MC not yet run — re-run pipeline"
                              st.caption(f"📐 Position sizing: `{_mc_sz_v}` (ATR/FIXED)  ·  {_skip_reason}")

                          st.divider()

                          # ── Row 7: Volatility Context ─────────────────────────────
                          iv_30        = _g(row, 'iv_30d', 'IV_30_D_Call')
                          hv_30        = _g(row, 'HV30', 'hv_30')
                          iv_rank      = _g(row, 'IV_Rank_20D', 'IV_Rank_30D', 'IV_Rank_252D')
                          iv_rank_src  = _g(row, 'IV_Rank_Source', default='—')
                          ivhv_gap     = _g(row, 'IVHV_gap_30D')
                          surf_shape   = str(_g(row, 'Surface_Shape', default='—') or '').upper()
                          iv_maturity  = _g(row, 'IV_Maturity_Level', default=1)
                          iv_hist_cnt  = _g(row, 'IV_History_Count', 'iv_history_count', default=0)
                          iv_regime    = _g(row, 'Regime', default='Unknown')
                          iv_chase     = _g(row, 'IV_Chase_Risk', default='—')

                          # Surface shape notes — context depends on whether you're buying or selling vol
                          try:
                              _dte_f = float(dte) if dte is not None else 0
                          except (TypeError, ValueError):
                              _dte_f = 0
                          # _is_leap already defined at top of card (gate section) — no redefinition needed
                          if is_income:
                              _shape_notes = {
                                  'CONTANGO':  "Normal term structure — favours income sellers. Shorter-dated premium decays faster.",
                                  'INVERTED':  "Short-term IV elevated — near-term event risk present. Income sellers face elevated assignment risk.",
                                  'FLAT':      "IV flat across maturities. No strong term structure edge for income.",
                              }
                          elif _is_leap:
                              _shape_notes = {
                                  'CONTANGO':  "Normal term structure — LEAP buyers pay fair long-dated vol. No structural disadvantage.",
                                  'INVERTED':  "Short-term IV elevated relative to long-term — LEAP buyers benefit: you're buying the cheaper long-dated vol.",
                                  'FLAT':      "IV flat across maturities. No term structure edge; LEAP priced at same vol as short-dated.",
                              }
                          else:
                              # Short-dated directional (DTE < 180, not a LEAP)
                              _shape_notes = {
                                  'CONTANGO':  "Normal term structure — near-term IV cheaper than long-term. No structural disadvantage for this trade.",
                                  'INVERTED':  "Near-term IV elevated vs long-dated (event/momentum premium). Pays off if the move is fast; theta cost is higher.",
                                  'FLAT':      "IV flat across maturities. No term structure edge for directional buyers.",
                              }
                          shape_note = _shape_notes.get(surf_shape, '')

                          # Regime note when unknown
                          regime_note = ''
                          if str(iv_regime).lower() in ('unknown', 'nan', 'none', '—', ''):
                              regime_note = f"Unknown — need 60+ days IV history (have {int(iv_hist_cnt or 0)}d)"

                          st.markdown("#### 📈 Volatility Context")
                          c1, c2, c3, c4 = st.columns(4)
                          with c1:
                              st.metric("IV 30D / HV 30D", f"{_fmt_pct(iv_30)} / {_fmt_pct(hv_30)}")
                              try:
                                  gap_f = float(ivhv_gap)
                                  if is_income or is_buy_write:
                                      # Sellers want IV > HV (rich premium)
                                      if gap_f > 0:
                                          gap_label = f"✅ Rich (IV > HV) — elevated premium, good to sell"
                                      else:
                                          gap_label = f"⚠️ Cheap (IV < HV) — premium thin for income"
                                  else:
                                      # Buyers want IV < HV (cheap vol)
                                      if gap_f < 0:
                                          gap_label = f"✅ Cheap (IV < HV) — buying vol below realized"
                                      else:
                                          gap_label = f"⚠️ Rich (IV > HV) — paying above realized vol"
                                  st.caption(f"Gap: {gap_f:+.1f}%  → {gap_label}")
                              except Exception:
                                  st.caption("Gap: —")
                          with c2:
                              st.metric("IV Rank", _fmt_float(iv_rank, 1) if iv_rank else "—")
                              if iv_rank:
                                  _rank_src_str = str(iv_rank_src or '—')
                                  # Flag short-window rolling rank as unreliable
                                  _is_short_window = 'ROLLING_20D' in _rank_src_str.upper() or (iv_hist_cnt and int(float(iv_hist_cnt or 0)) < 60)
                                  if _is_short_window:
                                      st.caption(f"Source: {_rank_src_str}  ⚠️ short window — unreliable until 60d+")
                                  else:
                                      st.caption(f"Source: {_rank_src_str}")
                              else:
                                  st.caption(f"Need 30d+ history (have {int(iv_hist_cnt or 0)}d)")
                          with c3:
                              st.metric("Surface Shape", surf_shape if surf_shape else "—")
                              if shape_note:
                                  st.caption(shape_note)
                              else:
                                  st.caption(f"Regime: {iv_regime}" + (f"  · {regime_note}" if regime_note else ''))
                          with c4:
                              maturity_labels = {
                                  1: 'Level 1 (<20d)',   2: 'Level 2 (20-60d)',
                                  3: 'Level 3 (60-120d)', 4: 'Level 4 (120-180d)',
                                  5: 'Level 5 (180d+)'
                              }
                              try:
                                  mat_int = int(float(iv_maturity or 1))
                              except Exception:
                                  mat_int = 1
                              st.metric("IV Maturity", maturity_labels.get(mat_int, f'Level {iv_maturity}'))
                              st.caption(f"{int(iv_hist_cnt or 0)} days collected  ·  Chase risk: {iv_chase}")
                          if regime_note and surf_shape:
                              st.caption(f"ℹ️ Regime: {regime_note}")
                          # Reconcile apparent contradiction: cheap IV/HV gap + INVERTED surface
                          # These measure different axes and are not contradictory.
                          if (not is_income and not is_buy_write and not _is_leap
                                  and surf_shape == 'INVERTED'):
                              try:
                                  _gap_f2 = float(ivhv_gap)
                                  if _gap_f2 < 0:
                                      st.caption(
                                          "ℹ️ **Vol context reconciled:** IV/HV gap (cheap) and INVERTED surface measure different things — "
                                          "they are not contradictory. "
                                          "IV 30D < HV 30D means you're buying below realized vol (level edge). "
                                          "INVERTED surface means near-term IV is elevated vs long-dated IV (term structure shape). "
                                          "Both can be true: the 30D vol is cheap vs what has been realized, "
                                          "but near-term is pricing a faster move than long-dated. "
                                          "Net: you have a level edge but pay a term-structure premium for speed."
                                      )
                              except Exception:
                                  pass

                          st.divider()

                          # ── Row 8a: DQS Score (directional only) ─────────────────
                          if not is_income:
                              dqs_score  = _g(row, 'DQS_Score')
                              dqs_status = _g(row, 'DQS_Status', default='—')
                              dqs_reason = _g(row, 'DQS_Reason', default='—')
                              dqs_break  = _g(row, 'DQS_Breakdown', default='')

                              st.markdown("#### 🎯 DQS Score (Directional Quality Score)")
                              c1, c2 = st.columns([1, 2])
                              with c1:
                                  if dqs_score and str(dqs_score) not in ('nan', 'None'):
                                      score_f = float(dqs_score)
                                      color = "green" if score_f >= 75 else "orange" if score_f >= 50 else "red"
                                      st.markdown(f"<h2 style='color:{color}'>{score_f:.0f}/100</h2>", unsafe_allow_html=True)
                                  else:
                                      st.markdown("<h2 style='color:gray'>N/A</h2>", unsafe_allow_html=True)
                                  st.caption(f"Status: **{dqs_status}**")
                              with c2:
                                  st.caption(f"**{dqs_reason}**")
                                  if dqs_break and str(dqs_break) not in ('nan', 'None', ''):
                                      with st.expander("Score breakdown"):
                                          for comp in str(dqs_break).split(' | '):
                                              if comp.strip():
                                                  st.markdown(f"- {comp.strip()}")
                                  st.caption("DQS scores delta fit, IV entry timing, spread cost, DTE fit, and trend confirmation.")
                              st.divider()

                          # ── Row 8a-ii: TQS Score (directional only) ───────────────
                          if not is_income:
                              tqs_score = _g(row, 'TQS_Score')
                              tqs_band  = _g(row, 'TQS_Band', default='N/A')
                              tqs_break = _g(row, 'TQS_Breakdown', default='')

                              _tqs_has_score = (tqs_score and str(tqs_score) not in ('nan', 'None', 'N/A'))

                              if not _tqs_has_score:
                                  st.markdown("#### ⏱️ TQS Score (Timing Quality Score)")
                                  st.caption(
                                      "⏳ TQS not computed — rerun the pipeline to get timing quality score. "
                                      "TQS measures: extension from SMA20/50, entry context (EARLY/LATE), "
                                      "RSI exhaustion, and EMA cross age."
                                  )
                                  st.divider()
                              else:
                                  _tqs_f = float(tqs_score)
                                  _tqs_band_str = str(tqs_band or 'N/A')

                                  # Band color — timing has a distinct palette from DQS
                                  _tqs_color = (
                                      "green"  if _tqs_band_str == 'Ideal'      else
                                      "#c8a000" if _tqs_band_str == 'Acceptable' else
                                      "darkorange" if _tqs_band_str == 'Stretched' else
                                      "red"
                                  )
                                  _tqs_band_emoji = {
                                      'Ideal': '✅', 'Acceptable': '🟡',
                                      'Stretched': '🟠', 'Chase': '🔴',
                                  }.get(_tqs_band_str, '')

                                  st.markdown("#### ⏱️ TQS Score (Timing Quality Score)")
                                  c1, c2 = st.columns([1, 2])
                                  with c1:
                                      st.markdown(
                                          f"<h2 style='color:{_tqs_color}'>{_tqs_f:.0f}/100</h2>",
                                          unsafe_allow_html=True
                                      )
                                      st.caption(
                                          f"{_tqs_band_emoji} **{_tqs_band_str}** — "
                                          + {
                                              'Ideal':      "well-timed entry",
                                              'Acceptable': "tradeable, at least one caution",
                                              'Stretched':  "entry is late or extended — reduce size or wait",
                                              'Chase':      "multiple extension flags — high mean-reversion risk",
                                          }.get(_tqs_band_str, '')
                                      )
                                  with c2:
                                      if tqs_break and str(tqs_break) not in ('nan', 'None', ''):
                                          with st.expander("Timing breakdown"):
                                              for comp in str(tqs_break).split(' | '):
                                                  if comp.strip():
                                                      st.markdown(f"- {comp.strip()}")
                                      # LEAP-aware interpretation: TQS penalties matter less over 180d+ horizon
                                      _tqs_leap_note = ""
                                      if _is_leap and _tqs_band_str in ('Stretched', 'Chase'):
                                          try:
                                              _dte_for_tqs = float(dte or 0)
                                          except (TypeError, ValueError):
                                              _dte_for_tqs = 0
                                          if _dte_for_tqs >= 180:
                                              _tqs_leap_note = (
                                                  f"  \n⚠️ **LEAP context ({int(_dte_for_tqs)}d DTE):** TQS penalties "
                                                  f"(extension, timing) are near-term signals. A {int(_dte_for_tqs)}d hold "
                                                  f"allows thesis to reset through any mean reversion — but entry price "
                                                  f"still affects your breakeven. Consider entering in 2–3 tranches "
                                                  f"or waiting for RSI to recover above 35 before adding size."
                                              )
                                      st.caption(
                                          "TQS scores extension from SMA20/50, entry context (EARLY/LATE), "
                                          "RSI momentum exhaustion, and EMA cross age. "
                                          "Orthogonal to DQS — measures *when* to enter, not *which direction*."
                                          + _tqs_leap_note
                                      )
                                  st.divider()

                          # ── Row 8b: PCS Score (income only) ──────────────────────
                          _pcs_reason_map = {
                              'IV_NOT_MATURE':      f"IV history too short ({int(iv_hist_cnt or 0)}d collected, need 120d+ for MATURE)",
                              'IV_HISTORY_SHORT':   f"IV history too short ({int(iv_hist_cnt or 0)}d collected, need 120d+ for MATURE)",
                              'INSUFFICIENT_IV':    "Insufficient IV data to score",
                              'LOW_IV_RANK':        "IV rank too low — not enough premium inflation",
                              'POOR_LIQUIDITY':     "Liquidity below threshold",
                              'DTE_TOO_SHORT':      "DTE too short for income trade",
                              'DTE_TOO_LONG':       "DTE too long — excess vega exposure",
                          }
                          if is_income:
                              pcs_score    = _g(row, 'PCS_Score_V2', 'PCS_Score')
                              pcs_status   = _g(row, 'PCS_Status', default='INACTIVE')
                              _raw_reason  = str(_g(row, 'Filter_Reason', default='') or '')
                              pcs_reason   = _pcs_reason_map.get(_raw_reason, _raw_reason or '—')
                              pcs_pens     = _g(row, 'PCS_Penalties', default='')

                              st.markdown("#### 🏆 PCS Score (Premium Collection Standard)")
                              c1, c2 = st.columns([1, 2])
                              with c1:
                                  if pcs_score and str(pcs_score) not in ('nan', 'None'):
                                      score_f = float(pcs_score)
                                      color = "green" if score_f >= 80 else "orange" if score_f >= 50 else "red"
                                      st.markdown(f"<h2 style='color:{color}'>{score_f:.0f}/100</h2>", unsafe_allow_html=True)
                                  else:
                                      st.markdown("<h2 style='color:gray'>N/A</h2>", unsafe_allow_html=True)
                                  st.caption(f"Status: **{pcs_status}**")
                              with c2:
                                  st.caption(f"**Reason:** {pcs_reason}")
                                  if pcs_pens and str(pcs_pens) not in ('nan', 'None', ''):
                                      with st.expander("Penalty breakdown"):
                                          for pen in str(pcs_pens).split(' | '):
                                              if pen.strip():
                                                  st.markdown(f"- {pen.strip()}")
                                  st.caption("PCS scores Greeks quality, liquidity, DTE fit, IV maturity, and premium pricing.")
                              st.divider()

                          # ── Row 8c: Vol Strategy Theory Score (straddle/strangle only) ──
                          if is_volatility:
                              _vol_score  = _g(row, 'Theory_Compliance_Score')
                              _vol_status = str(_g(row, 'Validation_Status', default='—') or '—')
                              _vol_notes  = str(_g(row, 'Evaluation_Notes', default='') or '')

                              st.markdown("#### 🌀 Vol Strategy Score (Theory Compliance)")
                              c1, c2 = st.columns([1, 2])
                              with c1:
                                  if _vol_score and str(_vol_score) not in ('nan', 'None'):
                                      _vsf = float(_vol_score)
                                      _vc = "green" if _vsf >= 80 else "orange" if _vsf >= 60 else "red"
                                      st.markdown(f"<h2 style='color:{_vc}'>{_vsf:.0f}/100</h2>", unsafe_allow_html=True)
                                  else:
                                      st.markdown("<h2 style='color:gray'>N/A</h2>", unsafe_allow_html=True)
                                  _vstatus_emoji = {'Valid': '✅', 'Watch': '🟡', 'Reject': '🔴', 'Incomplete_Data': '⚠️'}.get(_vol_status, '')
                                  st.caption(f"{_vstatus_emoji} **{_vol_status}**")
                              with c2:
                                  if _vol_notes and _vol_notes not in ('nan', 'None', '—'):
                                      with st.expander("Theory breakdown"):
                                          for note in _vol_notes.split(' | '):
                                              if note.strip():
                                                  st.markdown(f"- {note.strip()}")
                                  st.caption(
                                      "Scores: vega ≥ 0.40, gamma/theta ≥ 0.5, delta-neutral (|Δ|<0.15), "
                                      "skew < 1.20 (hard gate), RV/IV < 1.15, regime (Compression/Low-Vol), "
                                      "no recent vol spike, VVIX < 130 (Sinclair + Natenberg + Passarelli)."
                                  )
                              st.divider()

                          # ── Row 9: Thesis & Signal Reference ─────────────────────
                          thesis        = _g(row, 'thesis', 'Valid_Reason', default='—')
                          theory_src    = _g(row, 'Theory_Source', default='—')
                          regime_ctx    = _g(row, 'Regime_Context', default='—')
                          iv_ctx        = _g(row, 'IV_Context', default='—')
                          signal_type   = _g(row, 'Signal_Type', 'Chart_Signal_Type', default='—')
                          chart_regime  = _g(row, 'Chart_Regime', default='—')
                          compression   = _g(row, 'compression_tag', default='—')
                          momentum      = _g(row, 'momentum_tag', default='—')
                          timing_ctx    = _g(row, 'entry_timing_context', default='—')
                          dir_bias      = _g(row, 'directional_bias', default='—')
                          struct_bias   = _g(row, 'structure_bias', default='—')
                          timing_q      = _g(row, 'timing_quality', default='—')
                          sma20         = _g(row, 'SMA20')
                          sma50         = _g(row, 'SMA50')
                          macd_v        = _g(row, 'MACD')
                          atr_pct       = _g(row, 'Atr_Pct')
                          days_cross    = _g(row, 'Days_Since_Cross')
                          conf_score    = _g(row, 'Confidence')

                          st.markdown("#### 🧠 Thesis & Signal Reference")
                          # Strip step6 default IV_Rank=50 fill value (real rank only available after 30d+ history)
                          thesis_display = _re.sub(r',?\s*IV_Rank=50(\.0)?', '', str(thesis)).strip().strip(',').strip()
                          st.markdown(f"> **{thesis_display}**")
                          st.caption(f"Theory source: *{theory_src}*")

                          c1, c2, c3 = st.columns(3)
                          with c1:
                              st.markdown("**Price Structure**")
                              st.caption(f"Chart regime: {chart_regime}")
                              st.caption(f"EMA signal: {signal_type}  ({days_cross} days since cross)")
                              st.caption(f"SMA20: {_fmt_price(sma20)}  |  SMA50: {_fmt_price(sma50)}")
                              st.caption(f"MACD: {_fmt_float(macd_v, 2)}  |  ATR: {_fmt_pct(atr_pct)}")
                          with c2:
                              st.markdown("**Execution Context**")
                              st.caption(f"Directional bias: {dir_bias}")
                              st.caption(f"Structure: {struct_bias}")
                              st.caption(f"Timing quality: {timing_q}")
                              st.caption(f"Momentum: {momentum}  |  Compression: {compression}")
                              st.caption(f"Entry context: {timing_ctx}")
                          with c3:
                              st.markdown("**IV & Regime**")
                              st.caption(f"Regime context: {regime_ctx}")
                              # Strip synthetic IV_Rank=50 fill value from display (not real rank data)
                              _iv_ctx_clean = _re.sub(r',?\s*IV_Rank=50(\.0)?', '', str(iv_ctx) or '').strip().strip(',').strip()
                              _iv_ctx_clean = _iv_ctx_clean or str(iv_ctx)
                              st.caption(f"IV context: {_iv_ctx_clean}")
                              st.caption(f"System confidence: {conf_score}/100" if conf_score else "Confidence: —")

                          # ── Copy Card — plain-text snapshot for clipboard ──────────
                          with st.expander("📋 Copy Card", expanded=False):
                              _cc_lines = []

                              # Header
                              _cc_lines.append(f"{conf_color} {ticker} — {strat_name} — {str(trade_bias).title()} · Mid ${_fmt_price(mid)} · {conf_band} confidence")
                              _cc_lines.append(f"Gate: {gate_plain}")
                              _cc_lines.append("")

                              # Stock context
                              _cc_lines.append("Stock Price")
                              _arrow_cc = "▲" if (net_chg_pct or 0) >= 0 else "▼"
                              _cc_lines.append(f"${_fmt_price(stock_price)}")
                              _cc_lines.append(f"{_arrow_cc} {_fmt_pct(net_chg_pct)}")
                              _cc_lines.append(f"RSI")
                              _cc_lines.append(f"{_fmt_float(rsi_val, 1)}")
                              _cc_lines.append(f"ADX {_fmt_float(adx_val, 1)}")
                              _cc_lines.append("")
                              _cc_lines.append(f"Trend / EMA")
                              _cc_lines.append(f"{trend_st} / {ema_sig}")
                              _pos_str_cc = _fmt_pct(pos52, 0) if pos52 else "—"
                              _cc_lines.append(f"52W Position")
                              _cc_lines.append(f"{_pos_str_cc}")
                              _cc_lines.append(f"H {_fmt_price(hi52)} / L {_fmt_price(lo52)}")
                              _cc_lines.append("")

                              # Contract
                              _cc_lines.append("📋 Contract")
                              _cc_lines.append(f"Symbol")
                              _cc_lines.append(f"{contract_sym}")
                              _cc_lines.append(f"Expiration")
                              _cc_lines.append(f"{expiry}")
                              _cc_lines.append(f"Strike / Type")
                              _cc_lines.append(f"{_fmt_price(strike)} {opt_type}")
                              _cc_lines.append(f"DTE")
                              _cc_lines.append(f"{int(float(dte))} days" if dte else "—")

                              # Now Score
                              try:
                                  _cc_lines.append(f"{_badge_label}  score {_now_score} · {_reason_str}")
                              except Exception:
                                  pass

                              # Entry Pricing
                              _cc_lines.append("💵 Entry Pricing")
                              _cc_lines.append(f"Bid / Ask")
                              _cc_lines.append(f"{_fmt_price(bid)} / {_fmt_price(ask)}")
                              _cc_lines.append(f"Spread: {_fmt_pct(spread_pct)}")
                              _cc_lines.append("")
                              _cc_lines.append(f"Mid (target entry)")
                              _cc_lines.append(f"${_fmt_price(mid)}")
                              try:
                                  _cc_lines.append(f"Last trade: ${_fmt_price(last_opt)}")
                              except Exception:
                                  pass
                              _cc_lines.append("")
                              try:
                                  _cc_lines.append(f"BS Fair-value band")
                                  _cc_lines.append(f"{_fmt_price(entry_lo)} – {_fmt_price(entry_hi)}")
                                  if prem_vs_fv:
                                      _pvf = float(prem_vs_fv)
                                      if is_income and _pvf > 0:
                                          _cc_lines.append(f"✅ Selling {_pvf:.1f}% above BS fair value")
                                      elif _pvf < 0:
                                          _cc_lines.append(f"✅ Buying {abs(_pvf):.1f}% below BS fair value")
                              except Exception:
                                  pass
                              _cc_lines.append("")

                              # Income: you receive / Directional: you pay
                              if is_income or is_buy_write:
                                  _cc_lines.append(f"💰 You receive")
                                  _cc_lines.append(f"${_fmt_price(mid)}")
                                  _cc_lines.append(f"Sell at mid or better (higher = more premium)")
                              else:
                                  _cc_lines.append(f"💰 You pay")
                                  _cc_lines.append(f"${_fmt_price(mid)}")
                                  _cc_lines.append(f"Buy at mid or better (lower = cheaper entry)")
                              _cc_lines.append("")
                              _cc_lines.append(f"Liquidity: {liq_grade}{' — ' + str(liq_reason) if liq_reason else ''}")
                              _cc_lines.append("")
                              _cc_lines.append(f"OI: {oi}")
                              _cc_lines.append("")

                              # GTC Exit Rules
                              try:
                                  _mid_f_gc = float(mid)
                                  if is_income or is_buy_write:
                                      _profit_tgt = _mid_f_gc * 0.50
                                      _stop_loss  = _mid_f_gc * 2.0
                                      _cc_lines.append("🎯 GTC Exit Rules (Good-Till-Cancelled)")
                                      _cc_lines.append(f"Profit target: +50% Buy back at")
                                      _cc_lines.append(f"{_profit_tgt:.2f}")
                                      _cc_lines.append(f"(+{_profit_tgt * 100:.0f}/contract)")
                                      _cc_lines.append("")
                                      _cc_lines.append(f"Stop loss: –200% Buy back at ")
                                      _cc_lines.append(f"{_stop_loss:.2f}")
                                      _cc_lines.append(f"(−{(_stop_loss - _mid_f_gc) * 100:.0f}/contract)")
                                  else:
                                      _profit_tgt = _mid_f_gc * 2.0
                                      _stop_loss  = _mid_f_gc * 0.50
                                      _cc_lines.append("🎯 GTC Exit Rules (Good-Till-Cancelled)")
                                      _cc_lines.append(f"Profit target: +100% Sell at")
                                      _cc_lines.append(f"{_profit_tgt:.2f}")
                                      _cc_lines.append(f"(+{(_profit_tgt - _mid_f_gc) * 100:.0f}/contract)")
                                      _cc_lines.append("")
                                      _cc_lines.append(f"Stop loss: –50% Sell at")
                                      _cc_lines.append(f"{_stop_loss:.2f}")
                                      _cc_lines.append(f"(−{(_mid_f_gc - _stop_loss) * 100:.0f}/contract)")
                                  _cc_lines.append("")
                                  _cc_lines.append(f"Time stop: DTE ≤ 14 Exit regardless of P&L")
                                  _cc_lines.append("")
                              except Exception:
                                  pass

                              # Greeks
                              _cc_lines.append("🔢 Greeks")
                              try:
                                  _d_v = _g(row, 'Delta')
                                  _g_v = _g(row, 'Gamma')
                                  _v_v = _g(row, 'Vega')
                                  _t_v = _g(row, 'Theta')
                                  _iv_c = _g(row, 'Implied_Volatility')
                                  _cc_lines.append(f"Delta (Δ)")
                                  _cc_lines.append(f"{float(_d_v):.3f}" if _d_v else "—")
                                  _cc_lines.append(f"Gamma (Γ)")
                                  _cc_lines.append(f"{float(_g_v):.3f}" if _g_v else "—")
                                  _cc_lines.append(f"Vega (V)")
                                  _cc_lines.append(f"{float(_v_v):.3f}" if _v_v else "—")
                                  _cc_lines.append(f"Theta (Θ)")
                                  _cc_lines.append(f"{float(_t_v):.3f}" if _t_v else "—")
                                  _cc_lines.append(f"Contract IV")
                                  _cc_lines.append(f"{float(_iv_c):.1f}%" if _iv_c else "—")
                              except Exception:
                                  pass
                              _cc_lines.append("")

                              # Risk Profile
                              _cc_lines.append("⚠️ Risk Profile")
                              try:
                                  if max_loss:
                                      _cc_lines.append(f"Max Loss (1 contract)")
                                      if is_income:
                                          _cc_lines.append("Opportunity cost")
                                      else:
                                          _cc_lines.append(f"${max_loss:,.0f}")
                                  if breakeven:
                                      _cc_lines.append(f"Breakeven at expiry")
                                      _cc_lines.append(f"${float(breakeven):.2f}")
                                  if cap_display:
                                      _cc_lines.append(f"Capital Required")
                                      _cc_lines.append(f"${float(cap_display):,.0f}")
                                  if _em_pct_f is not None:
                                      _cc_lines.append(f"Expected Move (1σ)")
                                      _em_cc = f"{_em_pct_f:.1f}%"
                                      if _em_dollar is not None:
                                          _em_cc += f" (${_em_dollar:,.0f})"
                                      _cc_lines.append(_em_cc)
                              except Exception:
                                  pass
                              _cc_lines.append("")

                              # Volatility Context
                              _cc_lines.append("📈 Volatility Context")
                              try:
                                  _iv30_c = _g(row, 'iv_30d')
                                  _hv30_c = _g(row, 'HV30')
                                  _gap_c  = _g(row, 'IVHV_gap_30D')
                                  _ivr_c  = _g(row, 'IV_Rank_20D')
                                  _ss_c   = _g(row, 'Surface_Shape')
                                  _ivm_c  = _g(row, 'IV_Maturity_State')
                                  _ivml_c = _g(row, 'IV_Maturity_Level')
                                  _ihc_c  = _g(row, 'IV_History_Count')
                                  _cc_lines.append(f"IV 30D / HV 30D")
                                  _cc_lines.append(f"{float(_iv30_c):.1f}% / {float(_hv30_c):.1f}%" if _iv30_c and _hv30_c else "—")
                                  if _gap_c:
                                      _cc_lines.append(f"Gap: {'+' if float(_gap_c) > 0 else ''}{float(_gap_c):.1f}%")
                                  _cc_lines.append("")
                                  _cc_lines.append(f"IV Rank")
                                  _cc_lines.append(f"{float(_ivr_c):.1f}" if _ivr_c else "—")
                                  _cc_lines.append("")
                                  _cc_lines.append(f"Surface Shape")
                                  _cc_lines.append(f"{_ss_c}" if _ss_c else "—")
                                  _cc_lines.append("")
                                  _cc_lines.append(f"IV Maturity")
                                  _cc_lines.append(f"Level {_ivml_c} ({_ihc_c}d collected)" if _ivml_c else "—")
                              except Exception:
                                  pass
                              _cc_lines.append("")

                              # Thesis
                              _cc_lines.append("🧠 Thesis & Signal Reference")
                              try:
                                  _thesis_c = _g(row, 'thesis')
                                  _theory_c = _g(row, 'Theory_Source')
                                  _regime_c = _g(row, 'Regime_Context')
                                  _iv_ctx_c = _g(row, 'IV_Context')
                                  _chart_r  = _g(row, 'Chart_Regime')
                                  _ema_s    = _g(row, 'Chart_EMA_Signal')
                                  _sma20_c  = _g(row, 'SMA20')
                                  _sma50_c  = _g(row, 'SMA50')
                                  _macd_c   = _g(row, 'MACD')
                                  _atr_c    = _g(row, 'Atr_Pct')
                                  _dir_b    = _g(row, 'directional_bias')
                                  _str_b    = _g(row, 'structure_bias')
                                  _tim_q    = _g(row, 'timing_quality')
                                  _mom_t    = _g(row, 'momentum_tag')
                                  _comp_t   = _g(row, 'compression_tag')
                                  _ent_t    = _g(row, 'entry_timing_context')
                                  _conf_sc  = _g(row, 'Confidence')

                                  if _thesis_c: _cc_lines.append(f"{_thesis_c}")
                                  if _theory_c: _cc_lines.append(f"Theory source: {_theory_c}")
                                  _cc_lines.append("")
                                  _cc_lines.append("Price Structure")
                                  _cc_lines.append(f"Chart regime: {_chart_r}")
                                  _cc_lines.append(f"EMA signal: {_ema_s}")
                                  _cc_lines.append(f"SMA20: {_fmt_price(_sma20_c)}  |  SMA50: {_fmt_price(_sma50_c)}")
                                  _cc_lines.append(f"MACD: {_fmt_float(_macd_c, 2)} | ATR: {_fmt_pct(_atr_c)}")
                                  _cc_lines.append("")
                                  _cc_lines.append("Execution Context")
                                  _cc_lines.append(f"Directional bias: {_dir_b}")
                                  _cc_lines.append(f"Structure: {_str_b}")
                                  _cc_lines.append(f"Timing quality: {_tim_q}")
                                  _cc_lines.append(f"Momentum: {_mom_t} | Compression: {_comp_t}")
                                  _cc_lines.append(f"Entry context: {_ent_t}")
                                  _cc_lines.append("")
                                  _cc_lines.append("IV & Regime")
                                  _cc_lines.append(f"Regime context: {_regime_c}")
                                  _cc_lines.append(f"IV context: {_iv_ctx_c}")
                                  _cc_lines.append(f"System confidence: {_conf_sc}/100" if _conf_sc else "Confidence: —")
                              except Exception:
                                  pass

                              _copy_text = "\n".join(_cc_lines)
                              st.code(_copy_text, language=None)

                          with st.expander("🔍 All signals used (audit reference)"):
                              used_cols = [
                                  'Ticker','Strategy_Name','Strategy_Type','Trade_Bias','Confidence',
                                  'Contract_Symbol','Selected_Expiration','Actual_DTE','Selected_Strike','Option_Type',
                                  'Bid','Ask','Mid_Price','Bid_Ask_Spread_Pct','Open_Interest','Liquidity_Grade','Liquidity_Reason',
                                  'Implied_Volatility','Theoretical_Price','Premium_vs_FairValue_Pct','Entry_Band_Lower','Entry_Band_Upper',
                                  'Delta','Gamma','Vega','Theta',
                                  'iv_30d','HV30','IVHV_gap_30D','IV_Rank_20D','IV_Rank_Source','Surface_Shape','Regime',
                                  'IV_Maturity_Level','IV_Maturity_State','IV_History_Count',
                                  'RSI','ADX','MACD','SMA20','SMA50','Atr_Pct','Chart_Regime','Chart_EMA_Signal',
                                  'compression_tag','momentum_tag','entry_timing_context','52w_regime_tag',
                                  'directional_bias','structure_bias','timing_quality',
                                  'PCS_Score_V2','PCS_Status','PCS_Penalties','Filter_Reason',
                                  'Execution_Status','Gate_Reason','confidence_band',
                                  'Theory_Source','thesis','Regime_Context','IV_Context',
                              ]
                              available = [c for c in used_cols if c in row.index]
                              ref_df = pd.DataFrame({'Field': available, 'Value': [str(row[c]) for c in available]})
                              st.dataframe(ref_df, width="stretch", hide_index=True)

                else:
                    st.info("No trades currently meet all execution gates (READY).")
                    if not results.get('acceptance_all', pd.DataFrame()).empty:
                        st.dataframe(results.get('acceptance_all', pd.DataFrame()), width="stretch")
                    else:
                        st.info("No strategies were evaluated in the last run.")

            # ── CC Opportunities Tab ─────────────────────────────────────────────
            with tab_cc_opps:
                st.subheader("📌 Covered Call Opportunities")
                st.markdown("""
**These are not execution-ready trades.** A Covered Call requires you to already own 100 shares of
the underlying stock. The system has no portfolio tracker, so it can't confirm ownership automatically.

If you hold any of the tickers below, check whether selling the listed call makes sense for your position.
""")

                # Pull CC rows from acceptance_all
                df_all_cc = results.get('acceptance_all', pd.DataFrame())
                if not df_all_cc.empty and 'Strategy_Name' in df_all_cc.columns:
                    df_cc = df_all_cc[df_all_cc['Strategy_Name'] == 'Covered Call'].copy()
                else:
                    df_cc = pd.DataFrame()

                if df_cc.empty:
                    st.info("No Covered Call opportunities surfaced in the last scan.")
                    st.caption("CC requires: Bearish signal + IV > HV (gap_30d > 0) + Good/Excellent liquidity.")
                else:
                    # Build display table
                    cc_rows = []
                    for _, r in df_cc.iterrows():
                        try:
                            stock_p = float(r.get('last_price') or 0)
                            strike  = r.get('Selected_Strike')
                            mid     = r.get('Mid_Price')
                            iv30    = r.get('iv_30d')
                            hv30    = r.get('HV30') or r.get('hv_30')
                            gap     = r.get('IVHV_gap_30D')
                            dte_v   = r.get('Actual_DTE')
                            oi_v    = r.get('Open_Interest')
                            spread  = r.get('Bid_Ask_Spread_Pct')

                            # Annualised yield: premium / stock price
                            try:
                                ann_yield = (float(mid) * 100) / (stock_p * 100) * (365 / max(float(dte_v), 1)) * 100
                                yield_str = f"{ann_yield:.1f}%/yr"
                            except Exception:
                                yield_str = "—"

                            cc_rows.append({
                                "Ticker":      r.get('Ticker', '?'),
                                "Stock Price": f"${stock_p:.2f}" if stock_p else "—",
                                "Call Strike": f"${float(strike):.0f}" if strike else "—",
                                "Expiration":  str(r.get('Selected_Expiration', '—')),
                                "DTE":         f"{int(float(dte_v))}d" if dte_v else "—",
                                "Call Mid":    f"${float(mid):.2f}" if mid else "—",
                                "Ann. Yield":  yield_str,
                                "Spread":      f"{float(spread):.1f}%" if spread else "—",
                                "OI":          f"{int(float(oi_v))}" if oi_v else "—",
                                "IV/HV":       f"{float(iv30):.0f}%/{float(hv30):.0f}%" if iv30 and hv30 else "—",
                                "Gap":         f"{float(gap):+.1f}%" if gap else "—",
                                "Liquidity":   str(r.get('Liquidity_Grade', '—')),
                                "Signal":      str(r.get('Signal_Type') or r.get('Chart_EMA_Signal') or '—'),
                            })
                        except Exception:
                            pass

                    if cc_rows:
                        st.dataframe(pd.DataFrame(cc_rows), width="stretch", hide_index=True)
                        st.caption(
                            f"**{len(cc_rows)} ticker(s)** with valid CC conditions found. "
                            "Call Mid = premium you collect per share (×100 per contract). "
                            "Ann. Yield = annualised return on stock cost if assigned."
                        )

                        st.info(
                            "**How to use this:** If you already own 100 shares of any ticker above, "
                            "you can sell the listed call to collect premium. The call will expire worthless "
                            "if the stock stays below the strike — that premium is pure income. "
                            "If assigned, you sell your shares at the strike price."
                        )

                        st.warning(
                            "**Risk:** If the stock rallies hard above the strike, you forgo the upside "
                            "(your shares get called away at the strike). Only sell calls on positions "
                            "you're willing to part with at the strike price."
                        )
                    else:
                        st.info("No valid CC setups after data parsing.")

            with tab_waitlist:
                st.subheader("🟡 WAITLIST — Awaiting Confirmation")
                st.markdown("""
                These trades are valid but waiting on specific conditions to be satisfied before execution.
                Each entry has explicit wait conditions and a TTL (Time-To-Live) before expiry.
                """)

                # Try to load from wait_list table (DuckDB)
                df_waitlist = pd.DataFrame()
                try:
                    debug_mode = st.session_state.get("debug_mode", False)
                    db_path = _resolve_pipeline_db_path(debug_mode)

                    if db_path.exists():
                        with _connect_ro(str(db_path)) as con:
                            tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").df()['table_name'].tolist()

                            if 'wait_list' in tables:
                                # Query active wait entries
                                df_waitlist = con.execute("""
                                    SELECT *
                                    FROM wait_list
                                    WHERE status = 'ACTIVE'
                                    ORDER BY wait_started_at DESC
                                """).df()

                                logger.info(f"Loaded {len(df_waitlist)} active wait list entries from DuckDB")
                except Exception as e:
                    logger.warning(f"Could not load wait_list from DuckDB: {e}")
                    if st.session_state.get("debug_mode", False):
                        st.error("🛑 DEBUG MODE: wait_list read failed. CSV fallback is disabled.")

                # Fallback: check acceptance_all for AWAIT_CONFIRMATION status
                if df_waitlist.empty and 'acceptance_all' in results:
                    df_all = results['acceptance_all']
                    if 'Execution_Status' in df_all.columns:
                        df_waitlist = df_all[df_all['Execution_Status'] == 'AWAIT_CONFIRMATION'].copy()

                render_waitlist_table(df_waitlist)
            
            with tab_rejected:
                st.subheader("🔴 REJECTED — Not Suitable for Execution")
                st.markdown("""
                These trades failed one or more execution gates and have been permanently rejected or expired.
                Includes: BLOCKED (structural issues), EXPIRED (TTL exhausted), INVALIDATED (conditions changed).
                """)

                if 'acceptance_all' in results:
                    df_all = results['acceptance_all']

                    # Combine all rejection categories
                    rejected_statuses = ['BLOCKED', 'REJECTED', 'HALTED_MARKET_STRESS']
                    if 'Execution_Status' in df_all.columns:
                        df_rejected = df_all[df_all['Execution_Status'].isin(rejected_statuses)]
                    else:
                        st.error("🛑 DEBUG MODE: Execution_Status column missing in acceptance_all.")
                        df_rejected = pd.DataFrame()

                    # Also load expired/invalidated from wait_list
                    df_expired = pd.DataFrame()
                    try:
                        debug_mode = st.session_state.get("debug_mode", False)
                        db_path = _resolve_pipeline_db_path(debug_mode)

                        if db_path.exists():
                            with _connect_ro(str(db_path)) as con:
                                tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").df()['table_name'].tolist()

                                if 'wait_list' in tables:
                                    # Only show entries updated in the last 24 hours to avoid
                                    # historical accumulation (wait_list has 10K+ stale rows).
                                    df_expired = con.execute("""
                                        SELECT ticker, strategy_name, strategy_type, rejection_reason, updated_at
                                        FROM wait_list
                                        WHERE status IN ('EXPIRED', 'INVALIDATED', 'REJECTED')
                                          AND updated_at >= NOW() - INTERVAL '24 hours'
                                        ORDER BY updated_at DESC
                                        LIMIT 50
                                    """).df()

                                    if not df_expired.empty:
                                        st.write(f"**Also showing {len(df_expired)} expired/invalidated wait list entries (last 24h)**")
                    except Exception as e:
                        logger.warning(f"Could not load expired entries: {e}")
                        if st.session_state.get("debug_mode", False):
                            st.error("🛑 DEBUG MODE: expired wait_list read failed. CSV fallback is disabled.")

                    if not df_rejected.empty:
                        st.write(f"**{len(df_rejected)} strategies rejected in current scan**")

                        display_cols = ['Ticker', 'Strategy_Name', 'Strategy_Type', 'Block_Reason']
                        existing_cols = [c for c in display_cols if c in df_rejected.columns]

                        st.dataframe(df_rejected[existing_cols], width='stretch')

                        # Show rejection reason breakdown
                        if 'Block_Reason' in df_rejected.columns:
                            st.divider()
                            st.subheader("📊 Rejection Reason Breakdown")

                            # Extract gate codes (e.g., "R0.1", "R2.3")
                            df_rejected = df_rejected.copy()
                            df_rejected['Gate_Code'] = df_rejected['Block_Reason'].str.extract(
                                r'(R[0-9]+\.[0-9]+)',
                                expand=False
                            )
                            gate_counts = df_rejected['Gate_Code'].value_counts()

                            col1, col2 = st.columns(2)
                            with col1:
                                st.bar_chart(gate_counts)
                            with col2:
                                st.dataframe(gate_counts.reset_index().rename(columns={'index': 'Gate', 'Gate_Code': 'Count'}))

                    if not df_expired.empty:
                        st.divider()
                        st.write("**Recent Wait List Expirations**")
                        st.dataframe(df_expired, width='stretch')

                    if df_rejected.empty and df_expired.empty:
                        st.success("🎉 No rejected strategies in current scan!")
                else:
                    st.info("Run the pipeline to see rejected strategies.")

            with tab_counts:
                st.subheader("🔬 Forensic Row Counts")
                if 'pipeline_results' in st.session_state:
                    res = st.session_state['pipeline_results']
                    # Calculate counts safely
                    df_acceptance = res.get('acceptance_all', pd.DataFrame())
                    ready_count = 0
                    waitlist_count = 0
                    rejected_count = 0

                    if not df_acceptance.empty and 'Execution_Status' in df_acceptance.columns:
                        ready_count = (df_acceptance['Execution_Status'] == 'READY').sum()
                        waitlist_count = (df_acceptance['Execution_Status'] == 'AWAIT_CONFIRMATION').sum()
                        rejected_count = df_acceptance['Execution_Status'].isin(['BLOCKED', 'REJECTED', 'HALTED_MARKET_STRESS']).sum()

                    count_data = {
                        "Step": [
                            "Step 2 (Snapshot)",
                            "Step 3 (Filtered)",
                            "Step 6 (Validated)",
                            "Step 9B (Contracts)",
                            "Step 12 (Acceptance All)",
                            "🟢 READY NOW",
                            "🟡 WAITLIST",
                            "🔴 REJECTED",
                            "Step 8 (Thesis Envelopes)"
                        ],
                        "Row Count": [
                            len(res.get('snapshot', pd.DataFrame())),
                            len(res.get('filtered', pd.DataFrame())),
                            len(res.get('validated_data', pd.DataFrame())),
                            len(res.get('selected_contracts', pd.DataFrame())),
                            len(df_acceptance),
                            ready_count,
                            waitlist_count,
                            rejected_count,
                            len(res.get('thesis_envelopes', pd.DataFrame()))
                        ]
                    }
                    st.table(pd.DataFrame(count_data))
                else:
                    st.info("Run the pipeline to see row counts.")

            with tab_audit:
                if 'pipeline_health' in results:
                    health = results['pipeline_health']
                    audit_data = [
                        {"Step": "1. Tickers In", "Count": len(results.get('snapshot', pd.DataFrame())), "Status": "✅"},
                        {"Step": "2. Filtered (Step 3)", "Count": len(results.get('filtered', pd.DataFrame())), "Status": "✅"},
                        {"Step": "3. Validated (Step 6)", "Count": len(results.get('validated_data', pd.DataFrame())), "Status": "✅"},
                        {"Step": "4. Contracts Found (Step 9B)", "Count": health['step9b']['total_contracts'], "Status": "✅"},
                        {"Step": "5. READY (Step 12)", "Count": health['step12']['ready_now'], "Status": "✅"},
                        {"Step": "6. Thesis Envelopes (Step 8)", "Count": len(results.get('thesis_envelopes', pd.DataFrame())), "Status": "✅" if not results.get('thesis_envelopes', pd.DataFrame()).empty else "⚠️"}
                    ]
                    st.table(pd.DataFrame(audit_data))

                # ── Rejected Candidates (Theory Evaluation) ──────────────────
                if 'evaluated_strategies' in results:
                    df_eval = results['evaluated_strategies']
                    if 'Validation_Status' in df_eval.columns:
                        # Step11 uses DATA_NOT_MATURE / Watch / Valid — 'Reject' is legacy
                        rejected = df_eval[df_eval['Validation_Status'].isin(['Reject', 'REJECT'])]
                        not_mature = df_eval[df_eval['Validation_Status'] == 'DATA_NOT_MATURE']
                        if not rejected.empty:
                            st.divider()
                            st.subheader("❌ Rejected Candidates (Theory Evaluation)")
                            disp_cols = [c for c in ['Ticker', 'Strategy_Name', 'Evaluation_Notes'] if c in rejected.columns]
                            st.dataframe(rejected[disp_cols], width='stretch')
                        if not not_mature.empty:
                            st.divider()
                            st.subheader("⏳ Data Not Yet Mature")
                            st.caption(f"{len(not_mature)} strategies have insufficient IV history for full theory evaluation.")
                            disp_cols = [c for c in ['Ticker', 'Strategy_Name', 'Evaluation_Notes', 'Data_Completeness_Pct'] if c in not_mature.columns]
                            st.dataframe(not_mature[disp_cols].head(30), width='stretch')

                # ── IV Availability Diagnostics ───────────────────────────────
                # acceptance_ready (Step12_Ready) has IV_History_Count, IV_Maturity_Level, IV_Maturity_State
                df_iv_src = results.get('acceptance_ready', pd.DataFrame())
                hist_col = next((c for c in ['IV_History_Count', 'iv_history_days', 'iv_history_count'] if c in df_iv_src.columns), None)
                if not df_iv_src.empty and hist_col:
                    st.divider()
                    st.subheader("📊 IV Availability Diagnostics")
                    iv_unavailable = (df_iv_src[hist_col] < 120).sum()
                    st.write(f"READY strategies lacking full IV history (<120d): {iv_unavailable} / {len(df_iv_src)}")
                    display_cols = [c for c in ['Ticker', 'Strategy_Name', hist_col, 'IV_Maturity_State', 'IV_Maturity_Level', 'IV_Rank_Source'] if c in df_iv_src.columns]
                    st.dataframe(df_iv_src[display_cols].sort_values(hist_col), width='stretch')
            
            if tab_debug:
                with tab_debug:
                    st.header("🧪 Pipeline Debug Console")
                    if 'debug_summary' in results:
                        summary = results['debug_summary']
                        
                        st.subheader("📈 Pipeline Step Trace")
                        if summary.get('step_counts'):
                            trace_cols = st.columns(len(summary['step_counts']))
                            for i, (step, count) in enumerate(summary['step_counts'].items()):
                                with trace_cols[i % len(trace_cols)]:
                                    st.metric(step.replace("step", "Step "), count)
                        
                        st.divider()
                        st.subheader("🚨 Silent Failures & Events")
                        if summary.get('events'):
                            events_df = pd.DataFrame(summary['events'])
                            
                            def color_severity(val):
                                color = 'white'
                                if val == 'ERROR': color = '#ff4b4b'
                                elif val == 'WARN': color = '#ffa500'
                                elif val == 'INFO': color = '#00c853'
                                return f'color: {color}'
                            
                            st.dataframe(
                                events_df.style.map(color_severity, subset=['severity']),
                                width='stretch'
                            )
                            
                            selected_event_idx = st.selectbox("Inspect Event Context:", range(len(events_df)), format_func=lambda x: f"{events_df.iloc[x]['step']} - {events_df.iloc[x]['code']}")
                            event = events_df.iloc[selected_event_idx]
                            st.json(event['context'])
                            
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

    # --- Polling Status Display (Fragment with Auto-Rerun) ---
    # This uses st.fragment to isolate the polling logic from main render
    # Only the fragment reruns, not the entire page

    @st.fragment(run_every="1s")
    def _poll_fetch_status():
        """
        Fragment that polls fetch job status every 1 second.
        Monitors thread completion and displays errors from result container.
        Runs independently from main render - no blocking sleep/rerun needed.
        """
        if not st.session_state.get("is_fetching_data", False):
            return  # Not fetching, nothing to do

        if st.session_state.get("fetch_job_start_time") is None:
            return  # No job started

        # Get thread handle (no longer subprocess)
        thread = st.session_state.get("fetch_process")  # Note: renamed from 'process' for backwards compat

        # Check if thread has completed (check result container instead of process.poll())
        result_container = st.session_state.get("fetch_result_container")
        if result_container and result_container.get('completed'):
            # Thread completed - check for errors
            if result_container.get('error'):
                error = result_container['error']
                error_msg = str(error)

                # Check for common errors
                if "SCHWAB_APP_KEY" in error_msg or "refresh_token" in error_msg:
                    st.error("❌ **Authentication Failed**")
                    st.error("🔑 Your Schwab token has expired or is invalid.")
                    st.info("**To fix:** Run `python auth_schwab_minimal.py` to re-authenticate")
                else:
                    st.error("❌ **Fetch Job Failed**")
                    with st.expander("🔍 View Error Details"):
                        st.code(error_msg, language="text")

                logger.error(f"Fetch job failed: {error_msg}")

                # Reset state
                st.session_state.is_fetching_data = False
                st.session_state.fetch_job_start_time = None
                st.session_state.fetch_process = None
                st.session_state.fetch_result_container = None
                return

        # Legacy subprocess check (if somehow still a subprocess)
        if thread and hasattr(thread, 'poll') and thread.poll() is not None:
            exit_code = process.returncode

            if exit_code != 0:
                # Process failed - read stderr
                try:
                    stderr = process.stderr.read().decode('utf-8', errors='replace')
                    # Get last 1000 chars to avoid overwhelming UI
                    stderr_excerpt = stderr[-1000:] if len(stderr) > 1000 else stderr

                    # Check for common errors
                    if "AUTH FAILURE" in stderr or "refresh_token" in stderr:
                        st.error("❌ **Authentication Failed**")
                        st.error("🔑 Your Schwab token has expired or is invalid.")
                        st.info("**To fix:** Run `python auth_schwab_minimal.py` to re-authenticate")
                    elif "ModuleNotFoundError" in stderr:
                        st.error("❌ **Import Error**")
                        st.error("Missing required module. Check your Python environment.")
                    else:
                        st.error(f"❌ **Fetch Job Failed** (exit code: {exit_code})")

                    # Show error details in expander
                    with st.expander("🔍 View Error Details"):
                        st.code(stderr_excerpt, language="text")

                    logger.error(f"Fetch job failed with exit code {exit_code}: {stderr_excerpt}")
                except Exception as e:
                    st.error(f"❌ Fetch job failed (exit code: {exit_code})")
                    logger.error(f"Could not read stderr: {e}")

                # Reset state
                st.session_state.is_fetching_data = False
                st.session_state.fetch_job_start_time = None
                st.session_state.fetch_process = None
                return

        # Check for new snapshot file (existing logic)
        is_complete, status_msg = check_fetch_completion(
            st.session_state.fetch_job_start_time
        )

        if is_complete:
            # Mark fetch as complete
            st.session_state.is_fetching_data = False
            st.session_state.fetch_job_start_time = None
            st.session_state.fetch_process = None
            st.session_state.fetch_job_success = True
            st.success(f"✅ {status_msg}")
            st.balloons()
        else:
            # Still running - show status with countdown
            from datetime import timedelta
            elapsed = datetime.now() - st.session_state.fetch_job_start_time
            timeout = timedelta(minutes=5)  # Reduced from 10 to 5 minutes
            remaining = timeout - elapsed

            if remaining.total_seconds() > 0:
                st.info(f"⏳ {status_msg} (timeout in {remaining.seconds}s)")
            else:
                # Timeout reached
                logger.error("Fetch job timed out after 5 minutes")
                st.error("⏱️ **Fetch job timed out** (5 minutes elapsed)")
                st.warning("The background thread may still be running. Thread will complete on its own.")

                # Note: Cannot kill a thread - it will complete naturally
                # Threads are daemon=True so they won't block app shutdown

                # Reset state
                st.session_state.is_fetching_data = False
                st.session_state.fetch_job_start_time = None
                st.session_state.fetch_process = None
                st.session_state.fetch_result_container = None

    # Only render the polling fragment if a job is running
    if st.session_state.get("is_fetching_data", False):
        _poll_fetch_status()

    # Cleanup old temp files from previous sessions (only remove legacy temp files)
    # New cached uploads are managed by session state and cleaned up when session expires
    legacy_temp_path = core_project_root / "temp_prov_check.csv"
    if legacy_temp_path.exists():
        try:
            legacy_temp_path.unlink()
            logger.debug("Removed legacy temp file: temp_prov_check.csv")
        except Exception as e:
            logger.warning(f"Could not remove legacy temp file: {e}")

    logger.info(f"=== SCAN VIEW RENDER END ===\n")
