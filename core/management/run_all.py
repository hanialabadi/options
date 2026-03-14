import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

print(f"DEBUG: sys.path at start of run_all.py: {sys.path}")

import pandas as pd
import numpy as np
import logging
import os
import json
import re
import argparse
from datetime import datetime
from utils.trace_utils import print_provenance, print_preview, TRACE_TERMINAL

from core.management.cycle1.ingest.clean import phase1_load_and_clean_positions
from core.management.cycle1.identity.parse import phase2_run_all
from core.management._future_cycles.enrich.sus_compose_pcs_snapshot import run_phase3_enrichment
from core.management.cycle2.drift.compute_basic_drift import compute_drift_metrics, classify_drift_severity
from core.management.cycle2.drift.compute_windowed_drift import compute_windowed_drift
from core.management.cycle2.drift.drift_engine import DriftEngine
from core.management.cycle1.snapshot.snapshot import save_clean_snapshot, validate_cycle1_ledger
from core.management.cycle3.decision.engine import generate_recommendations, _apply_execution_readiness
from core.management.mc_management import run_management_mc
from core.management.mc_portfolio_var import mc_portfolio_var
from core.management.cycle2.chart_state.chart_state_engine import compute_chart_state
from core.management.cycle2.chart_state.state_drift_engine import compute_state_drift
from core.shared.data_contracts.schema import enforce_management_schema
from core.management.portfolio_circuit_breaker import check_circuit_breaker, persist_equity_curve, load_peak_equity
from core.phase5_portfolio_limits import analyze_correlation_risk, get_persona_limits
from config.sector_benchmarks import get_sector_bucket
from core.management.exit_coordinator import sequence_exits
from scan_engine.loaders.schwab_api_client import SchwabClient
from loguru import logger as loguru_logger # Import loguru's logger
from core.shared.data_layer.duckdb_utils import initialize_price_history_metadata_table # Import for explicit initialization

# Setup standard logging
logging.basicConfig(
    level=logging.DEBUG, # Set to DEBUG
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configure loguru to catch standard logging messages and set its level
loguru_logger.remove() # Remove default handler
loguru_logger.add(sys.stderr, level="DEBUG") # Add a new handler with DEBUG level
loguru_logger.enable("scan_engine") # Enable loguru for scan_engine module
loguru_logger.enable("core.shared.auth") # Enable loguru for auth module
loguru_logger.enable("core.management.cycle2.providers") # Enable loguru for IV providers

REQUIRED_COLUMNS = [
    "TradeID", "Underlying_Ticker", "Symbol", "Strategy", "AssetType", # Added Symbol and AssetType for completeness
    "GreekDominance_State", "VolatilityState_State", "AssignmentRisk_State",
    "RegimeStability_State", "Structural_Data_Complete", "Resolution_Reason",
    "Decision_State", "Rationale", "Doctrine_Source",
    "UL Last", "Basis", "Total_GL_Decimal", # Added UL Last, Basis, Total_GL_Decimal
    "run_id", "Snapshot_TS", "Schema_Hash"
]

def compute_direction_reversals(df: pd.DataFrame) -> pd.DataFrame:
    """Cross-leg direction reversal gate (Natenberg Ch.11 / Passarelli Ch.6).

    For each underlying with multiple legs, computes what happens to the
    combined delta after executing all EXIT actions.  When the direction
    flips (e.g. Neutral → Bullish), annotates the EXIT rows with a warning.

    Adds columns: Direction_Reversal_Warning, Post_Exit_Net_Delta, Direction_Shift.
    """
    df['Direction_Reversal_Warning'] = ''
    df['Post_Exit_Net_Delta'] = np.nan
    df['Direction_Shift'] = ''

    if 'Underlying_Ticker' not in df.columns or 'Action' not in df.columns:
        return df

    def _dir_label(d: float) -> str:
        if d < -10:
            return "Bearish"
        elif d > 10:
            return "Bullish"
        return "Neutral"

    for _dr_ticker, _dr_grp in df.groupby('Underlying_Ticker'):
        if len(_dr_grp) < 2:
            continue

        _dr_deltas = []
        _dr_exit_delta = 0.0
        for _dr_idx, _dr_row in _dr_grp.iterrows():
            _dr_d = float(_dr_row.get('Delta', 0) or 0)
            _dr_at = str(_dr_row.get('AssetType', '') or '').upper()
            _dr_qty = float(_dr_row.get('Quantity', 0) or 0)
            if _dr_at == 'STOCK' and _dr_d == 0 and _dr_qty != 0:
                _dr_d = 1.0
            _dr_net_d = _dr_d * abs(_dr_qty) if _dr_at == 'STOCK' else _dr_d * 100
            _dr_deltas.append((_dr_idx, _dr_net_d, str(_dr_row.get('Action', '') or '')))
            if str(_dr_row.get('Action', '') or '').upper() == 'EXIT':
                _dr_exit_delta += _dr_net_d

        if abs(_dr_exit_delta) < 0.1:
            continue

        _dr_net_now = sum(d for _, d, _ in _dr_deltas)
        _dr_net_post = _dr_net_now - _dr_exit_delta
        _dr_dir_now = _dir_label(_dr_net_now)
        _dr_dir_post = _dir_label(_dr_net_post)
        _dr_shift = f"{_dr_dir_now} → {_dr_dir_post}"

        _dr_idxs = [i for i, _, _ in _dr_deltas]
        df.loc[_dr_idxs, 'Post_Exit_Net_Delta'] = _dr_net_post
        df.loc[_dr_idxs, 'Direction_Shift'] = _dr_shift

        if _dr_dir_now != _dr_dir_post:
            _dr_warning = (
                f"Direction reversal: executing EXIT flips {_dr_ticker} from "
                f"{_dr_dir_now} ({_dr_net_now:+.0f}Δ) to {_dr_dir_post} ({_dr_net_post:+.0f}Δ). "
                f"Confirm remaining exposure is intentional. "
                f"(Natenberg Ch.11: net Greek analysis after all actions)"
            )
            df.loc[_dr_idxs, 'Direction_Reversal_Warning'] = _dr_warning

            _dr_exit_idxs = [i for i, _, a in _dr_deltas if a.upper() == 'EXIT']
            for _eidx in _dr_exit_idxs:
                _existing_rat = str(df.at[_eidx, 'Rationale'] or '')
                df.at[_eidx, 'Rationale'] = (
                    _existing_rat +
                    f" | ⚠️ DIRECTION REVERSAL: EXIT flips {_dr_ticker} net Δ from "
                    f"{_dr_dir_now} ({_dr_net_now:+.0f}) to {_dr_dir_post} ({_dr_net_post:+.0f}). "
                    f"Review remaining legs before executing."
                )

    return df


def run_all(input_csv: str, emit_path: str, archive_dir: str, audit_dir: str,
            allow_system_time: bool = False,
            account_balance: float = 100_000.0, persona: str = 'balanced'):
    start_time = datetime.now()
    run_timestamp = start_time.strftime("%Y%m%d_%H%M%S")
    
    logger.info(f"🚀 Starting Management Pipeline. allow_system_time={allow_system_time}")

    # Missing-data diagnosis tracker — tags NaN fields with causal reasons
    from core.shared.governance.missing_data_tracker import (
        MissingDataTracker, MANAGEMENT_TRACKED_FIELDS,
    )
    _missing_tracker = MissingDataTracker(
        run_id=run_timestamp, registry=MANAGEMENT_TRACKED_FIELDS,
    )

    # Explicitly initialize DuckDB metadata table to ensure it's ready
    initialize_price_history_metadata_table()
    logger.info("✅ DuckDB price history metadata table initialized.")

    # Initialize chart state history bank + prune stale entries
    try:
        from core.shared.data_layer.chart_state_repository import (
            initialize_chart_state_table, prune_stale_chart_states
        )
        initialize_chart_state_table()
        prune_stale_chart_states(max_age_days=30)
    except Exception as _csh_init_err:
        logger.debug(f"Chart state history init skipped: {_csh_init_err}")

    # 1. Cycle 1: Perception
    logger.info("--- CYCLE 1: PERCEPTION ---")
    result = phase1_load_and_clean_positions(
        input_path=Path(input_csv),
        allow_system_time=allow_system_time
    )
    df_clean = result['df'] if isinstance(result, dict) else result[0]
    
    print_provenance("IMPORT", os.path.basename(input_csv), input_csv, df_clean)
    try:
        print_preview("positions_df (clean)", df_clean, ['Symbol', 'Quantity', 'Last', 'Basis', '$ Total G/L'])
    except Exception as e:
        logger.warning(f"Preview failed for positions_df (clean): {e}")

    df_parsed = phase2_run_all(df_clean)
    df_enriched = run_phase3_enrichment(df_parsed)
    
    print_provenance("ENRICH", "In-memory Enrichment", "", df_enriched)
    try:
        print_preview("positions_df (enriched)", df_enriched, ['TradeID', 'Symbol', 'Strategy', 'Delta', 'IV', 'PCS'])
    except Exception as e:
        logger.warning(f"Preview failed for positions_df (enriched): {e}")

    # 2. Cycle 2: Measurement
    logger.info("--- CYCLE 2: MEASUREMENT ---")

    # 2.1 Live Enrichment BEFORE Freezing
    # RAG: Architectural Fix. We must enrich with live IV/Greeks BEFORE freezing anchors
    # to ensure new positions have valid entry conditions even if broker truth is missing them.
    
    # Initialize Schwab Client for Cycle 2/3 enrichment
    schwab_client = None
    schwab_live = False
    try:
        # Ensure environment variables are loaded just before SchwabClient init
        from dotenv import load_dotenv
        load_dotenv()
        
        schwab_client = SchwabClient()
        schwab_client.ensure_valid_token()
        schwab_live = True
        logger.info("✅ Schwab API Client initialized for pre-freeze enrichment.")
    except Exception as e:
        logger.warning(f"⚠️ Schwab API Client initialization failed: {e}. Falling back to yfinance/cache.", exc_info=True)
        schwab_client = None  # Discard broken client so downstream uses DuckDB/file cache
        schwab_live = False

    # Feature B: Smart Schwab Price Refresh
    # Only calls Schwab during market hours AND when price moved >0.5% or Greeks are stale.
    # Falls back to scan_results_latest DuckDB cache, then broker CSV — no wasted API calls.
    try:
        from core.management.cycle2.providers.live_price_provider import LivePriceProvider
        _price_provider = LivePriceProvider()
        if _price_provider.should_refresh(df_enriched):
            _ticker_col = 'Underlying_Ticker' if 'Underlying_Ticker' in df_enriched.columns else 'Ticker'
            _tickers = df_enriched[_ticker_col].dropna().unique().tolist() if _ticker_col in df_enriched.columns else []
            if _tickers:
                _live_prices = _price_provider.fetch_live_prices(_tickers, schwab_client)
                df_enriched = _price_provider.apply_to_df(df_enriched, _live_prices)
                logger.info(f"[LivePriceProvider] Applied live prices for {len(_live_prices)} tickers.")
        else:
            if 'Price_Source' not in df_enriched.columns:
                df_enriched['Price_Source'] = 'broker_csv'
    except Exception as _price_err:
        logger.warning(f"⚠️ Live price refresh failed (non-fatal): {_price_err}")
        if 'Price_Source' not in df_enriched.columns:
            df_enriched['Price_Source'] = 'broker_csv'

    # HV Enrichment
    from core.management.cycle2.drift.auto_enrich_hv import auto_enrich_hv_from_schwab
    # Pass schwab_live if the function supports it
    df_enriched = auto_enrich_hv_from_schwab(df_enriched, schwab_live=schwab_live)

    # IV & Greek Enrichment
    from core.management.cycle2.providers.governed_iv_provider import fetch_governed_sensor_readings
    
    # RAG: IV provider expects option symbols for option IV. Underlying IV is not required.
    option_symbols_for_iv_fetch = df_enriched.loc[df_enriched['AssetType'] == 'OPTION', 'Symbol'].unique().tolist()
    
    iv_sources = []
    readings = []
    if option_symbols_for_iv_fetch:
        loguru_logger.debug(f"[DEBUG_SCHWAB_LIVE_FLAG] schwab_live before fetch_governed_sensor_readings: {schwab_live}")
        try:
            readings = fetch_governed_sensor_readings(option_symbols_for_iv_fetch, schwab_live=schwab_live)
        except Exception as e:
            logger.error(f"❌ IV provider failed for symbols {option_symbols_for_iv_fetch}: {e}", exc_info=True)
            readings = []
        
        if readings:
            unique_sources = sorted(list(set(r.get('Source', 'Unknown') for r in readings)))
            iv_sources = unique_sources

            iv_map = {r['Symbol']: r['IV'] for r in readings if r.get('IV') is not None}
            
            # Map IV back to option legs using their OCC Symbol
            # Preserve existing IV_Now when Schwab returns no data (fillna pattern)
            option_mask = df_enriched['AssetType'] == 'OPTION'
            if 'IV_Now' not in df_enriched.columns:
                df_enriched['IV_Now'] = np.nan
            _mapped_iv = df_enriched.loc[option_mask, 'Symbol'].map(iv_map)
            _existing_iv = df_enriched.loc[option_mask, 'IV_Now'].copy()
            df_enriched.loc[option_mask, 'IV_Now'] = _mapped_iv.fillna(_existing_iv)
            _preserved = int((_mapped_iv.isna() & _existing_iv.notna()).sum())
            if _preserved > 0:
                logger.info(f"[IV] Preserved {_preserved} existing IV_Now values (Schwab returned no data)")
            
            # Stock IV_Now should remain NaN as per user's clarification.
            stock_mask = df_enriched['AssetType'].isin(['STOCK', 'EQUITY'])
            df_enriched.loc[stock_mask, 'IV_Now'] = np.nan # Explicitly set to NaN for stocks

            loguru_logger.debug(f"[DEBUG_IV_POP] IV_Now after initial map (Options): {df_enriched.loc[option_mask, ['Symbol', 'IV_Now']].head()}")

            # RAG: Cache Fallback. If IV_Now is missing for options, try the latest cached IV for the specific contract.
            from core.management.cycle2.providers.iv_history_provider import get_latest_iv_batch
            missing_iv_mask = df_enriched['IV_Now'].isna() & (df_enriched['AssetType'] == 'OPTION')
            if missing_iv_mask.any():
                occ_symbols_to_cache_fallback = df_enriched.loc[missing_iv_mask, 'Symbol'].unique().tolist()
                cached_ivs = get_latest_iv_batch(occ_symbols_to_cache_fallback)
                df_enriched.loc[missing_iv_mask, 'IV_Now'] = df_enriched.loc[missing_iv_mask, 'Symbol'].map(cached_ivs)
                logger.info(f"Filled {missing_iv_mask.sum()} option IVs from history cache.")
            
            loguru_logger.debug(f"[DEBUG_IV_POP] IV_Now after history cache fallback (Options only):\n{df_enriched.loc[df_enriched['AssetType'] == 'OPTION', ['Symbol', 'IV_Now']].head()}")

            # RAG: Anchor Fallback. As a last resort, use IV_Entry to avoid pipeline uncertainty.
            if 'IV_Entry' in df_enriched.columns:
                # Normalize IV_Entry (handle percent vs decimal)
                from core.shared.finance_utils import normalize_iv_series
                iv_entry_decimal = normalize_iv_series(pd.to_numeric(df_enriched['IV_Entry'], errors='coerce'))
                df_enriched.loc[option_mask, 'IV_Now'] = df_enriched.loc[option_mask, 'IV_Now'].fillna(iv_entry_decimal)
            
            loguru_logger.debug(f"[DEBUG_IV_POP] IV_Now after IV_Entry fallback (Options only):\n{df_enriched.loc[df_enriched['AssetType'] == 'OPTION', ['Symbol', 'IV_Now']].head()}")
                
            # ── True underlying IV from iv_term_history (replaces IV_Now alias) ──
            # IV_30D = underlying ATM 30-day IV from the Schwab option chain,
            # collected daily by collect_iv_daily.py → iv_history.duckdb.
            # IV_Now remains per-contract IV for per-option comparisons.
            # IV_Percentile = 252-day IV_Rank from iv_term_history (not management run history).
            try:
                from core.shared.data_contracts.config import IV_HISTORY_DB_PATH
                from core.shared.data_layer.iv_term_history import get_latest_iv_data
                import duckdb as _duckdb_iv

                _ul_tickers = df_enriched['Underlying_Ticker'].dropna().unique().tolist()
                if _ul_tickers:
                    _iv_con = _duckdb_iv.connect(str(IV_HISTORY_DB_PATH), read_only=True)
                    try:
                        _iv_latest = get_latest_iv_data(_iv_con, _ul_tickers)
                        if not _iv_latest.empty:
                            # Map underlying iv_30d to all rows by Underlying_Ticker
                            _iv30_map = dict(zip(_iv_latest['ticker'], _iv_latest['iv_30d']))
                            df_enriched['IV_30D'] = df_enriched['Underlying_Ticker'].map(_iv30_map)
                            _filled = df_enriched['IV_30D'].notna().sum()
                            logger.info(f"IV_30D: {_filled}/{len(df_enriched)} rows filled from iv_term_history (underlying ATM 30d IV)")

                            # Compute IV_Percentile: what % of historical iv_30d values
                            # are below today's iv_30d.  Uses all available history per ticker
                            # (minimum 20 trading days).  More robust than min/max IV_Rank
                            # for tickers with < 120 days of history.
                            _ticker_list_sql = "'" + "','".join(_ul_tickers) + "'"
                            _pctile_df = _iv_con.execute(f"""
                                WITH latest AS (
                                    SELECT ticker, iv_30d as current_iv
                                    FROM iv_term_history
                                    WHERE ticker IN ({_ticker_list_sql})
                                      AND iv_30d IS NOT NULL
                                    QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) = 1
                                ),
                                history AS (
                                    SELECT ticker, iv_30d, COUNT(*) OVER (PARTITION BY ticker) as depth
                                    FROM iv_term_history
                                    WHERE ticker IN ({_ticker_list_sql})
                                      AND iv_30d IS NOT NULL
                                      AND CAST(strftime('%w', date) AS INTEGER) NOT IN (0, 6)
                                )
                                SELECT
                                    l.ticker,
                                    l.current_iv,
                                    CAST(SUM(CASE WHEN h.iv_30d < l.current_iv THEN 1 ELSE 0 END) AS DOUBLE)
                                        / COUNT(*) * 100.0 as iv_pctile,
                                    COUNT(*) as depth
                                FROM latest l
                                JOIN history h ON l.ticker = h.ticker
                                GROUP BY l.ticker, l.current_iv
                                HAVING COUNT(*) >= 20
                            """).df()
                            if not _pctile_df.empty:
                                _rank_map = dict(zip(_pctile_df['ticker'], _pctile_df['iv_pctile']))
                                _depth_map = dict(zip(_pctile_df['ticker'], _pctile_df['depth']))
                                df_enriched['IV_Percentile'] = df_enriched['Underlying_Ticker'].map(_rank_map)
                                df_enriched['IV_Percentile_Depth'] = df_enriched['Underlying_Ticker'].map(_depth_map)
                                _min_depth = int(_pctile_df['depth'].min())
                                _max_depth = int(_pctile_df['depth'].max())
                                logger.info(f"IV_Percentile: {len(_rank_map)} tickers from iv_term_history (depth: {_min_depth}-{_max_depth}d)")
                        else:
                            logger.warning("iv_term_history returned empty — falling back IV_30D = IV_Now")
                            df_enriched['IV_30D'] = df_enriched['IV_Now']
                    finally:
                        _iv_con.close()
                else:
                    df_enriched['IV_30D'] = df_enriched['IV_Now']
            except Exception as _iv_hist_err:
                logger.warning(f"iv_term_history query failed ({_iv_hist_err}) — falling back IV_30D = IV_Now")
                df_enriched['IV_30D'] = df_enriched['IV_Now']

            # Fallback: OPTION rows still missing IV_30D get IV_Now (per-contract).
            # STOCK rows: IV_Now is intentionally NaN (stocks have no contract IV),
            # so only iv_term_history populates IV_30D for stocks. Do NOT fill
            # stock IV_30D with IV_Now — that just propagates NaN.
            _opt_mask_fb = df_enriched['AssetType'] == 'OPTION'
            df_enriched.loc[_opt_mask_fb, 'IV_30D'] = (
                df_enriched.loc[_opt_mask_fb, 'IV_30D'].fillna(df_enriched.loc[_opt_mask_fb, 'IV_Now'])
            )
            loguru_logger.debug(f"[DEBUG_IV_POP] IV_30D from iv_term_history. Sample:\n{df_enriched[['Underlying_Ticker', 'IV_Now', 'IV_30D']].head()}")

            # ── Canonical volatility aliases (Phase 1 migration) ──────────────
            # New names coexist with old names. See schema.py VOLATILITY DATA MODEL.
            # IV_Contract = per-contract IV (OPTION only; NaN for STOCK — correct).
            # IV_Underlying_30D = underlying ATM IV from iv_term_history (all rows).
            # IV_Rank = IV percentile from iv_term_history (all rows).
            df_enriched['IV_Contract']       = df_enriched['IV_Now']        # per-contract (NaN for stocks)
            df_enriched['IV_Underlying_30D'] = df_enriched['IV_30D']        # underlying ATM
            df_enriched['IV_Rank']           = df_enriched.get('IV_Percentile')  # underlying percentile
            
            # RAG: Populate Current_IV based on priority:
            # 1. Live option IV if present (from iv_map for options)
            # 2. IV_30D fallback (already populated above)
            # 3. Live underlying IV (for stocks only, from iv_map for non-options)
            
            df_enriched['Current_IV'] = df_enriched['IV_30D'] # Start with IV_30D as base (priority 2)

            # Apply live option IV (priority 1)
            option_mask = df_enriched['AssetType'] == 'OPTION'
            df_enriched.loc[option_mask, 'Current_IV'] = df_enriched.loc[option_mask, 'Symbol'].map(iv_map).fillna(df_enriched.loc[option_mask, 'Current_IV'])

            # Apply underlying IV for stocks (priority 3) - this should remain NaN as per user's clarification
            stock_mask = df_enriched['AssetType'].isin(['STOCK', 'EQUITY'])
            df_enriched.loc[stock_mask, 'Current_IV'] = np.nan # Explicitly set to NaN for stocks

            loguru_logger.debug(f"[DEBUG_IV_POP] Current_IV after priority assignment. Sample:\n{df_enriched[['Symbol', 'AssetType', 'IV_Now', 'IV_30D', 'Current_IV']].head()}")

            # Update Greeks if available from fresh readings
            for r in readings:
                if not r.get('Is_Fallback') and r.get('Source') == 'schwab':
                    mask = df_enriched['Symbol'] == r['Symbol']
                    for greek in ['Delta', 'Gamma', 'Vega', 'Theta', 'Rho']:
                        if r.get(greek) is not None:
                            df_enriched.loc[mask, greek] = r[greek]

    # 2.2 Compute Chart States BEFORE Freezing
    # RAG: Architectural Fix. Chart states must be resolved before snapshot persistence
    # to ensure entry anchors capture the technical regime.
    df_enriched = compute_chart_state(df_enriched, client=schwab_client)

    # 2.21 Persist chart states to history bank (non-blocking)
    try:
        from core.shared.data_layer.chart_state_repository import persist_chart_states
        persist_chart_states(df_enriched)
    except Exception as _csh_err:
        logger.debug(f"Chart state history persistence skipped: {_csh_err}")

    # 2.25 Thesis State Engine — Layer 0 above chart/vol/Greeks
    # Answers: "Is the underlying company still aligned with my capital thesis?"
    # Runs after chart states (needs primitives) but before snapshot/doctrine.
    # Non-blocking: all yfinance calls are wrapped; pipeline never halts on failure.
    try:
        from core.management.cycle2.thesis.thesis_engine import compute_thesis_state
        df_enriched = compute_thesis_state(df_enriched)
        thesis_broken = (df_enriched.get("Thesis_State", pd.Series()) == "BROKEN").sum() \
            if "Thesis_State" in df_enriched.columns else 0
        logger.info(f"[ThesisEngine] ✅ Thesis states computed. BROKEN={thesis_broken}.")
    except Exception as _thesis_err:
        logger.warning(f"⚠️ ThesisEngine failed (non-fatal): {_thesis_err}")

    # 2.3 Persist Cycle 1 snapshot and handle entry anchors
    # This ensures the Cycle 1 ledger remains pure (Broker Truth only)
    df_snapshot, _, run_id, _, _ = save_clean_snapshot(
        df_enriched, 
        source_file_path=input_csv,
        ingest_context="run_all"
    )

    try:
        print_preview("legs_df (snapshot)", df_snapshot, ['TradeID', 'Symbol', 'LegID', 'Quantity', 'Last', 'Snapshot_TS'])
    except Exception as e:
        logger.warning(f"Preview failed for legs_df (snapshot): {e}")
    
    # RAG: Ensure current run metadata is preserved in the working dataframe
    df_enriched['run_id'] = run_id
    if 'Schema_Hash' in df_snapshot.columns and not df_snapshot.empty:
        df_enriched['Schema_Hash'] = df_snapshot['Schema_Hash'].iloc[0]
    else:
        logger.warning("⚠️ Schema_Hash missing from snapshot or snapshot empty. Setting to None.")
        df_enriched['Schema_Hash'] = None

    # 2.35 Retroactive entry backfill: patch anchors missing chart state / IV
    # Uses price_history (pipeline.duckdb) and iv_term_history (iv_history.duckdb) to
    # reconstruct entry conditions for positions whose broker CSV was exported late.
    # Greeks are NOT backfilled (cannot reconstruct without full pricing model inputs).
    try:
        from core.management.cycle1.snapshot.entry_backfill import backfill_entry_anchors
        _patched = backfill_entry_anchors()
        if _patched:
            logger.info(f"[EntryBackfill] ✅ {_patched} anchor rows retroactively enriched.")
    except Exception as _bf_err:
        logger.warning(f"⚠️ Entry backfill failed (non-fatal): {_bf_err}")

    # 2.4 RAG: Re-join with entry anchors for Cycle 2 measurement
    from core.shared.data_layer.duckdb_utils import get_duckdb_connection # Use the utility function
    from core.shared.data_contracts.config import PIPELINE_DB_PATH
    with get_duckdb_connection(read_only=True) as con: # Use read_only connection
        df_anchors = con.execute("SELECT * FROM entry_anchors WHERE Is_Active = TRUE").df()
    
    if not df_anchors.empty:
        # Defensive fix: Guard against row duplication
        df_anchors = df_anchors.drop_duplicates(subset=['LegID'])
        
        df_enriched = df_enriched.loc[:, ~df_enriched.columns.duplicated()].copy()
        cols_from_anchors = [c for c in df_anchors.columns
                              if c.endswith('_Entry') or 'Entry' in c
                              or c in ('First_Seen_Date', 'Entry_Snapshot_TS')]
        
        if 'LegID' in df_enriched.columns:
            df_enriched = df_enriched.merge(
                df_anchors[['LegID'] + cols_from_anchors],
                on='LegID',
                how='left',
                suffixes=('', '_Anchor')
            )
        else:
            logger.warning("⚠️ LegID missing from df_enriched — skipping anchor merge")
        
        for col in cols_from_anchors:
            anchor_col = f"{col}_Anchor"
            if anchor_col in df_enriched.columns:
                df_enriched[col] = df_enriched[anchor_col].fillna(df_enriched[col])
                df_enriched = df_enriched.drop(columns=[anchor_col])

    # 2.5 RAG: Anchor Repair (Self-Healing)
    # If any active position is missing critical anchors (IV or Chart States), 
    # we repair them using the live data we just computed.
    if 'AssetType' in df_enriched.columns:
        missing_anchor_mask = df_enriched['AssetType'] == 'OPTION'
    else:
        logger.warning("⚠️ AssetType missing — skipping anchor repair")
        missing_anchor_mask = pd.Series(False, index=df_enriched.index)

    critical_anchors = [
        'IV_Entry', 'IV_Entry_Source', 'Delta_Entry', 'Underlying_Price_Entry',
        'Entry_Chart_State_PriceStructure', 'Entry_Chart_State_TrendIntegrity',
        'Entry_Chart_State_VolatilityState', 'Entry_Chart_State_CompressionMaturity'
    ]
    
    repair_needed = False
    for col in critical_anchors:
        if col in df_enriched.columns:
            repair_needed |= df_enriched[missing_anchor_mask][col].isna().any()
            
    if repair_needed:
        logger.info("🛡️ ANCHOR_REPAIR: Detecting missing anchors for active positions...")
        from core.management.cycle1.snapshot.freeze import freeze_entry_data
        
        # Identify specific LegIDs that need repair
        legs_to_repair = []
        if 'IV_Entry' in df_enriched.columns and 'LegID' in df_enriched.columns:
            legs_to_repair = df_enriched[missing_anchor_mask & df_enriched['IV_Entry'].isna()]['LegID'].tolist()
            
        if legs_to_repair:
            logger.info(f"🛡️ ANCHOR_REPAIR: Repairing anchors for {len(legs_to_repair)} legs: {legs_to_repair}")
            
            # Tighten scope: Only repair trades that actually have legs needing repair
            trades_to_repair = df_enriched[df_enriched['LegID'].isin(legs_to_repair)]['TradeID'].unique().tolist()
            df_repaired = freeze_entry_data(df_enriched, new_trade_ids=trades_to_repair)
            
            # Update the database with repaired anchors
            with get_duckdb_connection(read_only=False) as con: # Use non-read-only connection for updates
                for _, row in df_repaired[df_repaired['LegID'].isin(legs_to_repair)].iterrows():
                    con.execute("""
                        UPDATE entry_anchors 
                        SET IV_Entry = ?, IV_Entry_Source = ?, Delta_Entry = ?, Underlying_Price_Entry = ?,
                            Entry_Chart_State_PriceStructure = ?, Entry_Chart_State_TrendIntegrity = ?,
                            Entry_Chart_State_VolatilityState = ?, Entry_Chart_State_CompressionMaturity = ?
                        WHERE LegID = ? AND Is_Active = TRUE
                    """, [
                        row['IV_Entry'], row['IV_Entry_Source'], row['Delta_Entry'], row['Underlying_Price_Entry'],
                        row['Entry_Chart_State_PriceStructure'], row['Entry_Chart_State_TrendIntegrity'],
                        row['Entry_Chart_State_VolatilityState'], row['Entry_Chart_State_CompressionMaturity'],
                        row['LegID']
                    ])
            
            # Sync back to in-memory dataframe
            for col in critical_anchors:
                if col in df_repaired.columns:
                    df_enriched[col] = df_repaired[col].fillna(df_enriched[col])

    # 2.55 Buy-Write Premium Ledger — compute net cost basis before drift
    # McMillan Ch.3: each successive premium collected reduces effective stock cost.
    # Must run AFTER anchor merge so Premium_Entry and Entry_Snapshot_TS are available.
    try:
        from core.management.cycle1.snapshot.premium_ledger import BuyWriteLedger
        _bw_ledger = BuyWriteLedger()
        with get_duckdb_connection(read_only=False) as _ledger_con:
            df_enriched = _bw_ledger.enrich(df_enriched, con=_ledger_con)
        logger.info("[BuyWriteLedger] ✅ Net cost basis computed for BUY_WRITE positions.")
    except Exception as _ledger_err:
        logger.warning(f"⚠️ BuyWriteLedger failed (non-fatal): {_ledger_err}")

    # 2.6 Compute Drift (Temporal, Price, Greek, and IV migration)
    df_with_drift = compute_drift_metrics(df_enriched)
    
    # RAG: Windowed Greek ROC (1D, 3D, 10D) — must run before DriftEngine
    # Reads management_recommendations from pipeline.duckdb for historical comparison.
    # Produces: Delta/Gamma/Vega/Theta/IV_ROC_3D, Delta/Vega/IV_ROC_10D, ROC_Persist_3D.
    df_with_drift = compute_windowed_drift(df_with_drift)

    # 2.6.5 Forward Expectancy: Expected Move vs Required Move (Cycle 2.6.5)
    # Computes EV_Feasibility_Ratio, Required_Move_Breakeven, Required_Move_50pct,
    # Theta_Bleed_Daily_Pct for OPTION legs. Uses IV (forward-looking), 10D window.
    # Guardrails: IV not HV, 10D not full DTE, breakeven + 50% recovery targets.
    from core.management.cycle2.drift.compute_expected_move import compute_expected_move
    df_with_drift = compute_expected_move(df_with_drift)

    # 2.95 Conviction Decay Timer (Cycle 2.95)
    # Queries management_recommendations DuckDB for Delta_ROC_3D history per leg.
    # Produces: Delta_Deterioration_Streak, Conviction_Status, Conviction_Fade_Days.
    # Degrades gracefully: STABLE when no history available.
    from core.management.cycle2.conviction_decay_timer import compute_conviction_decay
    df_with_drift = compute_conviction_decay(df_with_drift)

    # 2.96 Pyramid Tier Tracker (Cycle 2.96)
    # Queries management_recommendations DuckDB for prior SCALE_UP executions per TradeID.
    # Produces: Pyramid_Tier, Winner_Lifecycle.
    # Degrades gracefully: tier 0, THESIS_UNPROVEN when no history available.
    # Must run AFTER conviction_decay_timer (needs Conviction_Status as input).
    from core.management.cycle2.pyramid_tier_tracker import compute_pyramid_tier
    df_with_drift = compute_pyramid_tier(df_with_drift)

    # RAG: Authoritative Drift Engine (Cycle 2)
    # assess_signal_drift() uses ROC columns produced above for signed slope thresholds.
    drift_engine = DriftEngine(persona=persona, account_balance=account_balance)
    df_with_drift = drift_engine.run_drift_analysis(df_with_drift)
    
    # RAG: Explicitly tag output as Legacy_Drift_Severity to avoid Doctrine over-trust
    df_with_drift = classify_drift_severity(df_with_drift)
    if 'Drift_Severity' in df_with_drift.columns:
        df_with_drift = df_with_drift.rename(columns={'Drift_Severity': 'Legacy_Drift_Severity'})

    print_provenance("DRIFT", "Drift Engine", "", df_with_drift)
    try:
        print_preview("drift_df", df_with_drift, ['TradeID', 'Symbol', 'Price_Drift_Pct', 'Delta_Drift', 'IV_Drift', 'Legacy_Drift_Severity'])
    except Exception as e:
        logger.warning(f"Preview failed for drift_df: {e}")

    # 2.65 Margin Carry Enrichment
    # Cumulative carry cost, carry-adjusted P&L, portfolio burn rate.
    # Builds on Daily_Margin_Cost from compute_basic_drift.
    # McMillan Ch.3 / Passarelli Ch.6: carry cost is a silent P&L drain.
    try:
        from core.management.cycle2.carry.margin_carry import MarginCarryCalculator
        _carry_calc = MarginCarryCalculator()
        df_with_drift = _carry_calc.enrich(df_with_drift)
    except Exception as _carry_err:
        logger.warning(f"Margin carry enrichment failed (non-fatal): {_carry_err}")

    # 2.66 BW/CC Efficiency Scorecard
    # Carry-adjusted yield, premium-vs-carry ratio, efficiency grades.
    try:
        from core.management.cycle2.carry.bw_efficiency import BWEfficiencyCalculator
        _eff_calc = BWEfficiencyCalculator()
        df_with_drift = _eff_calc.enrich(df_with_drift)
    except Exception as _eff_err:
        logger.warning(f"BW efficiency enrichment failed (non-fatal): {_eff_err}")

    # 2.7 P&L Attribution
    from core.management.cycle2.drift.compute_pnl_attribution import compute_pnl_attribution, aggregate_trade_pnl_attribution
    
    logger.debug(f"DEBUG: Columns before P&L attribution: {df_with_drift.columns.tolist()}")
    if 'Unrealized_PnL' in df_with_drift.columns:
        logger.debug(f"DEBUG: Sample Unrealized_PnL before P&L attribution:\n{df_with_drift[['LegID', 'Unrealized_PnL']].head()}")
    else:
        logger.debug("DEBUG: Unrealized_PnL column NOT found before P&L attribution.")

    df_with_drift = compute_pnl_attribution(df_with_drift)
    df_with_drift = aggregate_trade_pnl_attribution(df_with_drift)
    
    # 2.75 Equity Integrity State — lightweight structural deterioration monitor.
    # Scores 7 signals from chart primitives already on df (MA slopes, ROC20, HV
    # percentile, ATR slope, drawdown from entry). Produces Equity_Integrity_State:
    # HEALTHY / WEAKENING / BROKEN. Only fires on STOCK legs; options pass through.
    # Non-blocking: any failure leaves state = HEALTHY.
    try:
        from core.management.cycle2.drift.compute_equity_integrity import compute_equity_integrity
        df_with_drift = compute_equity_integrity(df_with_drift)
        _ei_broken = (df_with_drift.get("Equity_Integrity_State", pd.Series()) == "BROKEN").sum()
        if _ei_broken:
            logger.warning(f"[EquityIntegrity] {_ei_broken} stock position(s) in BROKEN state.")
    except Exception as _ei_err:
        logger.warning(f"⚠️ Equity integrity computation failed (non-fatal): {_ei_err}")

    # 2.8 Chart State Temporal Memory (Cycle 2.5)
    # Computes _Days / _Prev / _Change for 6 key state columns from DuckDB history.
    # Enables doctrine to detect: THETA_DOMINANT for N days, sustained NO_TREND, etc.
    try:
        from core.shared.data_contracts.config import PIPELINE_DB_PATH
        df_with_drift = compute_state_drift(df_with_drift, db_path=str(PIPELINE_DB_PATH))
    except Exception as e:
        logger.warning(f"⚠️ State drift computation failed (non-fatal): {e}")

    # 2.85 Position Trajectory — lifecycle regime classification.
    # Distinguishes "sideways income" from "chasing strikes" by analysing
    # stock trajectory + roll history since position entry.
    # Requires: entry_anchors (merged), premium_ledger (enriched), price_history (DuckDB).
    try:
        from core.management.cycle2.drift.compute_position_trajectory import compute_position_trajectory
        df_with_drift = compute_position_trajectory(df_with_drift, db_path=str(PIPELINE_DB_PATH))
        _chase = (df_with_drift.get("Position_Regime", pd.Series(dtype="object")) == "TRENDING_CHASE").sum()
        if _chase:
            logger.warning(f"[PositionTrajectory] {_chase} position(s) classified as TRENDING_CHASE.")
    except Exception as _traj_err:
        logger.warning(f"⚠️ Position trajectory computation failed (non-fatal): {_traj_err}")

    # Live Greeks Refresh (Schwab chain fetch — market hours only)
    # Fetches current Delta/Gamma/Vega/Theta/IV for held option contracts.
    # Results stored as IV_Now / Delta_Live / etc. (transient — not frozen in anchors).
    # Doctrine uses IV_Now preferentially over daily-harvested IV_30D for edge calculations.
    # Session chain cache is shared with roll candidate engine to avoid duplicate calls.
    _greeks_provider = None
    _session_chain_cache: dict = {}
    try:
        from core.management.cycle2.providers.live_greeks_provider import LiveGreeksProvider
        _greeks_provider = LiveGreeksProvider()
        if _greeks_provider.should_refresh() and schwab_client is not None:
            df_with_drift = _greeks_provider.enrich(df_with_drift, schwab_client)
            _session_chain_cache = _greeks_provider._session_cache  # share with roll engine
            logger.info("[LiveGreeks] Option Greeks refreshed from live Schwab chains.")
        else:
            logger.debug("[LiveGreeks] Market closed or no Schwab client — using broker CSV Greeks.")
    except Exception as _greeks_err:
        logger.warning(f"⚠️ Live Greeks refresh failed (non-fatal): {_greeks_err}")

    # Feature A: Condition Monitor + State Persistence
    # ─────────────────────────────────────────────────────────────────────────
    # State store loads PRIOR run's condition onset/resolution timestamps so the
    # engine knows HOW LONG a condition has been active, not just WHETHER it is.
    # Oscillation guard: a condition that resolves and re-fires within 24h is
    # treated as the same condition — prevents HOLD→ROLL→HOLD intraday flips.
    # Design contract: daily granularity, deterministic, idempotent, non-blocking.
    # DB failure → log warning, never halt pipeline.
    _prior_state: dict = {}
    _state_store = None
    try:
        from core.management.conditions.state_store import ManagementStateStore
        _state_store = ManagementStateStore()
        _prior_state = _state_store.load()
        logger.info(f"[StateStore] Loaded {len(_prior_state)} prior state rows.")
    except Exception as _ss_err:
        logger.warning(f"⚠️ StateStore load failed (non-fatal): {_ss_err}")

    # Thesis fallback: if yfinance failed this run and thesis is UNKNOWN,
    # reuse last persisted thesis_state so the gate doesn't silently pass.
    if _prior_state and "Thesis_State" in df_with_drift.columns:
        try:
            for _tid in df_with_drift["TradeID"].dropna().unique():
                _ts_now = str(df_with_drift.loc[df_with_drift["TradeID"] == _tid, "Thesis_State"].iloc[0] or "")
                if _ts_now.upper() in ("", "UNKNOWN"):
                    _prior_thesis = _state_store.get_thesis_state(str(_tid), prior=_prior_state)
                    if _prior_thesis and _prior_thesis.upper() != "UNKNOWN":
                        _tid_mask = df_with_drift["TradeID"] == _tid
                        df_with_drift.loc[_tid_mask, "Thesis_State"] = _prior_thesis
                        # Rebuild summary to match restored state — prevents DEGRADED state + UNKNOWN summary mismatch
                        _summary_map = {
                            "INTACT":   "Thesis INTACT — no structural concerns detected.",
                            "DEGRADED": "Thesis DEGRADED — monitoring signals from prior run; roll with caution.",
                            "BROKEN":   "Thesis BROKEN — exit consideration active from prior run.",
                        }
                        _restored_summary = _summary_map.get(_prior_thesis.upper(), f"Thesis {_prior_thesis} (prior run).")
                        df_with_drift.loc[_tid_mask, "Thesis_Summary"] = _restored_summary
                        logger.debug(f"[StateStore] Restored Thesis_State={_prior_thesis} for trade {_tid} from prior run.")
        except Exception as _tf_err:
            logger.warning(f"⚠️ Thesis state fallback failed (non-fatal): {_tf_err}")

    try:
        from core.management.conditions.monitor import ConditionMonitor
        _monitor = ConditionMonitor()
        _monitor.persist_conditions(df_with_drift, con=None, prior_state=_prior_state)
        _resolved = _monitor.check_conditions(df_with_drift, con=None)
        df_with_drift = _monitor.apply_resolutions(df_with_drift, _resolved, prior_state=_prior_state)
        if _resolved:
            logger.info(f"[ConditionMonitor] {len(_resolved)} conditions resolved — pre-populated in df.")
    except Exception as _cond_err:
        logger.warning(f"⚠️ Condition monitor failed (non-fatal): {_cond_err}")

    # 2.9 Trade-Leg Enrichment: broadcast Short_Call_* columns onto all leg rows.
    # Solves multi-leg blindness: doctrine runs via df.apply() which sees one row
    # at a time. The STOCK leg has Strike=NaN, Delta=0, DTE=NaN — all option gates
    # are silently blind. This groupby pass extracts the short-call leg and
    # broadcasts its fields onto EVERY row of each trade before doctrine runs.
    # Non-blocking: per-trade failures log a warning and leave columns as NaN.
    try:
        from core.management.cycle2.trade_leg_enrichment import enrich_trade_leg_summary
        df_with_drift = enrich_trade_leg_summary(df_with_drift)
        logger.info("[TradeLegEnrichment] Short_Call_* columns broadcast onto all leg rows.")
    except Exception as _tle_err:
        logger.warning(f"⚠️ Trade leg enrichment failed (non-fatal): {_tle_err}")

    # 2.95 Trade Journey Context
    # Fetch the most recent prior recommendation for each TradeID and inject it as
    # Prior_* columns so doctrine can surface continuous journey context in rationale.
    # Non-blocking: DB failure leaves columns as None and doctrine degrades gracefully.
    try:
        from core.shared.data_layer.duckdb_utils import get_duckdb_connection
        with get_duckdb_connection(read_only=True) as _jcon:
            # Day N-1: most recent distinct daily close per TradeID.
            # Technical indicators (momentum_slope, bb_width_z, etc.) are sourced from
            # daily OHLC bars — they are identical across all intraday runs on the same date.
            # We use CAST(Snapshot_TS AS DATE) to deduplicate intraday runs and treat each
            # calendar day as one observation, avoiding single-bar noise from intraday triggers.
            # Check which optional columns exist in the table (may be absent on first run)
            _mr_cols = set()
            try:
                _mr_cols = {r[1] for r in _jcon.execute("PRAGMA table_info('management_recommendations')").fetchall()}
            except Exception:
                pass
            _opt_scale_cols = ""
            if "Scale_Trigger_Price" in _mr_cols:
                _opt_scale_cols += ",\n                    Scale_Trigger_Price   AS Prior_Scale_Trigger_Price"
            if "Scale_Add_Contracts" in _mr_cols:
                _opt_scale_cols += ",\n                    Scale_Add_Contracts   AS Prior_Scale_Add_Contracts"
            if "Pyramid_Tier" in _mr_cols:
                _opt_scale_cols += ",\n                    Pyramid_Tier          AS Prior_Pyramid_Tier"
            if "Doctrine_Source" in _mr_cols:
                _opt_scale_cols += ",\n                    Doctrine_Source       AS Prior_Doctrine_Source"

            # Fix: DuckDB only supports one QUALIFY per query. Use CTE for the two-pass dedup.
            # Pass 1: pick latest snapshot per (TradeID, day)
            # Pass 2: pick latest day per TradeID
            # Exclude today: journey note should compare against the prior
            # *day's* price, not an earlier same-day run (which has the same
            # UL Last and makes the price delta 0%).  Same-day flips are
            # already handled by the intraday stability annotation (3.0h).
            _today_str_j = pd.Timestamp.now().strftime('%Y-%m-%d')
            _prior_d1 = _jcon.execute(f"""
                WITH daily_latest AS (
                    SELECT *
                    FROM management_recommendations
                    WHERE CAST(Snapshot_TS AS DATE) < '{_today_str_j}'
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY TradeID, CAST(Snapshot_TS AS DATE)
                        ORDER BY Snapshot_TS DESC
                    ) = 1
                )
                SELECT
                    TradeID,
                    Action                AS Prior_Action,
                    Urgency               AS Prior_Urgency,
                    Snapshot_TS           AS Prior_Snapshot_TS,
                    "UL Last"             AS Prior_UL_Last,
                    LEFT(Rationale, 160)  AS Prior_Rationale_Digest,
                    bb_width_z            AS Prior_bb_width_z,
                    momentum_slope        AS Prior_momentum_slope,
                    adx_14                AS Prior_adx,
                    rsi_14                AS Prior_rsi,
                    Delta                 AS Prior_Delta,
                    IV_Now                AS Prior_IV_Now,
                    Theta                 AS Prior_Theta,
                    Gamma                 AS Prior_Gamma,
                    Total_GL_Decimal      AS Prior_PnL_Pct{_opt_scale_cols},
                    CAST(Snapshot_TS AS DATE) AS Prior_Date
                FROM daily_latest
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY TradeID
                    ORDER BY CAST(Snapshot_TS AS DATE) DESC
                ) = 1
            """).fetchdf()

            # Day N-2: the distinct daily close immediately before day N-1.
            # Used for the 2-consecutive-daily-close confirmation rule on slope breach —
            # prevents reacting to a single day's negative slope (could be intraday noise
            # even with daily bars, e.g. data staleness or late-close data pull).
            _prior_d2 = _jcon.execute(f"""
                WITH daily_latest AS (
                    SELECT *
                    FROM management_recommendations
                    WHERE CAST(Snapshot_TS AS DATE) < '{_today_str_j}'
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY TradeID, CAST(Snapshot_TS AS DATE)
                        ORDER BY Snapshot_TS DESC
                    ) = 1
                )
                SELECT
                    TradeID,
                    momentum_slope        AS Prior2_momentum_slope,
                    adx_14                AS Prior2_adx,
                    rsi_14                AS Prior2_rsi,
                    bb_width_z            AS Prior2_bb_width_z
                FROM daily_latest
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY TradeID
                    ORDER BY CAST(Snapshot_TS AS DATE) DESC
                ) = 2
            """).fetchdf()

        _prior_recs = _prior_d1
        if not _prior_d2.empty:
            _prior_recs = _prior_recs.merge(_prior_d2, on="TradeID", how="left")
        else:
            for _c in ["Prior2_momentum_slope", "Prior2_adx", "Prior2_rsi", "Prior2_bb_width_z"]:
                _prior_recs[_c] = None

        if not _prior_recs.empty:
            _prior_recs = _prior_recs.drop_duplicates(subset=['TradeID'])
            _prior_recs["Prior_Snapshot_TS"] = pd.to_datetime(
                _prior_recs["Prior_Snapshot_TS"], utc=True, errors="coerce"
            )
            _now_utc = pd.Timestamp.utcnow()
            # Calendar-day difference (not elapsed hours) — since the prior
            # query excludes today, the result is always >= 1 calendar day.
            _prior_recs["Prior_Days_Ago"] = (
                _now_utc.normalize() - _prior_recs["Prior_Snapshot_TS"].dt.normalize()
            ).dt.days
            _journey_cols = [
                "TradeID", "Prior_Action", "Prior_Urgency",
                "Prior_Snapshot_TS", "Prior_UL_Last",
                "Prior_Days_Ago", "Prior_Rationale_Digest",
                "Prior_bb_width_z", "Prior_momentum_slope", "Prior_adx", "Prior_rsi",
                "Prior_Delta", "Prior_IV_Now", "Prior_Theta", "Prior_Gamma",
                "Prior_PnL_Pct",
                "Prior_Scale_Trigger_Price", "Prior_Scale_Add_Contracts", "Prior_Pyramid_Tier",
                "Prior_Doctrine_Source",
                "Prior2_momentum_slope", "Prior2_adx", "Prior2_rsi", "Prior2_bb_width_z",
            ]
            # Only merge columns that actually exist (Prior2_* may be absent on first run)
            _merge_cols = [c for c in _journey_cols if c in _prior_recs.columns]
            df_with_drift = df_with_drift.merge(
                _prior_recs[_merge_cols],
                on="TradeID", how="left",
            )
            # Carry forward Scale_Trigger_Price from prior run for trigger re-check
            # Bug fix: these columns were written to DuckDB but never read back.
            for _carry_col, _prior_col in [
                ("Scale_Trigger_Price", "Prior_Scale_Trigger_Price"),
                ("Scale_Add_Contracts", "Prior_Scale_Add_Contracts"),
            ]:
                if _prior_col in df_with_drift.columns:
                    if _carry_col not in df_with_drift.columns:
                        df_with_drift[_carry_col] = None
                    _carry_mask = df_with_drift[_carry_col].isna() | (df_with_drift[_carry_col] == 0)
                    df_with_drift.loc[_carry_mask, _carry_col] = df_with_drift.loc[_carry_mask, _prior_col]
            logger.info(f"[Journey] Injected prior D1+D2 watch metrics for {_prior_recs['TradeID'].nunique()} trades.")
        else:
            for _col in ["Prior_Action", "Prior_Urgency", "Prior_Snapshot_TS",
                         "Prior_UL_Last", "Prior_Days_Ago", "Prior_Rationale_Digest",
                         "Prior_bb_width_z", "Prior_momentum_slope", "Prior_adx", "Prior_rsi",
                         "Prior_Delta", "Prior_IV_Now", "Prior_Theta", "Prior_Gamma",
                         "Prior_PnL_Pct",
                         "Prior_Scale_Trigger_Price", "Prior_Scale_Add_Contracts", "Prior_Pyramid_Tier",
                         "Prior_Doctrine_Source",
                         "Prior2_momentum_slope", "Prior2_adx", "Prior2_rsi", "Prior2_bb_width_z"]:
                df_with_drift[_col] = None
    except Exception as _je:
        logger.warning(f"⚠️ Journey context fetch failed (non-fatal): {_je}")
        for _col in ["Prior_Action", "Prior_Urgency", "Prior_Snapshot_TS",
                     "Prior_UL_Last", "Prior_Days_Ago", "Prior_Rationale_Digest",
                     "Prior_bb_width_z", "Prior_momentum_slope", "Prior_adx", "Prior_rsi",
                     "Prior_Delta", "Prior_IV_Now", "Prior_Theta", "Prior_Gamma",
                     "Prior_PnL_Pct",
                     "Prior_Scale_Trigger_Price", "Prior_Scale_Add_Contracts", "Prior_Pyramid_Tier",
                     "Prior_Doctrine_Source",
                     "Prior2_momentum_slope", "Prior2_adx", "Prior2_rsi", "Prior2_bb_width_z"]:
            if _col not in df_with_drift.columns:
                df_with_drift[_col] = None

    # 2.955 Action Streak Counter — count consecutive days with same Action per TradeID.
    # Used by the escalation gate (3.0a) to auto-resolve persistent REVIEW
    # and promote stale EXIT signals.  Non-blocking: failure defaults streak to 0.
    try:
        from core.shared.data_layer.duckdb_utils import get_duckdb_connection
        with get_duckdb_connection(read_only=True) as _scon:
            _streak_df = _scon.execute("""
                WITH daily_latest AS (
                    SELECT
                        TradeID,
                        Action,
                        CAST(Snapshot_TS AS DATE) AS run_date
                    FROM management_recommendations
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY TradeID, CAST(Snapshot_TS AS DATE)
                        ORDER BY Snapshot_TS DESC
                    ) = 1
                ),
                ranked AS (
                    SELECT
                        TradeID,
                        Action,
                        run_date,
                        ROW_NUMBER() OVER (PARTITION BY TradeID ORDER BY run_date DESC) AS day_rank
                    FROM daily_latest
                ),
                recent AS (
                    SELECT TradeID, LIST(Action ORDER BY day_rank ASC) AS action_history
                    FROM ranked
                    WHERE day_rank <= 10
                    GROUP BY TradeID
                )
                SELECT TradeID, action_history
                FROM recent
            """).fetchdf()

            if not _streak_df.empty:
                def _count_streak(action_list):
                    """Count consecutive identical actions from most recent (index 0)."""
                    if not action_list or len(action_list) == 0:
                        return 0
                    latest = action_list[0]
                    streak = 0
                    for a in action_list:
                        if a == latest:
                            streak += 1
                        else:
                            break
                    return streak

                _streak_df["Prior_Action_Streak"] = _streak_df["action_history"].apply(_count_streak)

                def _count_exit_appearances(action_list, window=5):
                    """Count EXIT appearances in last N days (not necessarily consecutive)."""
                    if not action_list:
                        return 0
                    return sum(1 for a in action_list[:window] if a == "EXIT")

                _streak_df["EXIT_Count_Last_5D"] = _streak_df["action_history"].apply(_count_exit_appearances)
                _streak_map = dict(zip(_streak_df["TradeID"], _streak_df["Prior_Action_Streak"]))
                _exit_count_map = dict(zip(_streak_df["TradeID"], _streak_df["EXIT_Count_Last_5D"]))
                df_with_drift["Prior_Action_Streak"] = df_with_drift["TradeID"].map(_streak_map).fillna(0).astype(int)
                df_with_drift["EXIT_Count_Last_5D"] = df_with_drift["TradeID"].map(_exit_count_map).fillna(0).astype(int)
                _max_streak = df_with_drift["Prior_Action_Streak"].max()
                logger.info(f"[ActionStreak] Injected streak counts for {len(_streak_map)} trades (max streak: {_max_streak}).")

                # 2.955b Ticker-level streak carry-forward — captures EXIT signals across
                # TradeID changes (BUY_WRITE rolls create new TradeIDs, resetting the per-trade
                # streak counter).  Groups all trades on the same Underlying_Ticker and takes
                # the most-severe action per day.
                # (Audit: DKNG Feb-2026 — 5 EXIT CRITICALs across 3 TradeIDs, streak never fired)
                _ticker_streak_df = _scon.execute("""
                    WITH daily_latest AS (
                        SELECT
                            Underlying_Ticker,
                            Action,
                            CAST(Snapshot_TS AS DATE) AS run_date
                        FROM management_recommendations
                        WHERE Underlying_Ticker IS NOT NULL
                        QUALIFY ROW_NUMBER() OVER (
                            PARTITION BY Underlying_Ticker, CAST(Snapshot_TS AS DATE)
                            ORDER BY
                                CASE Action
                                    WHEN 'EXIT' THEN 1
                                    WHEN 'REVIEW' THEN 2
                                    WHEN 'ROLL' THEN 3
                                    ELSE 4
                                END,
                                Snapshot_TS DESC
                        ) = 1
                    ),
                    ranked AS (
                        SELECT
                            Underlying_Ticker,
                            Action,
                            run_date,
                            ROW_NUMBER() OVER (
                                PARTITION BY Underlying_Ticker ORDER BY run_date DESC
                            ) AS day_rank
                        FROM daily_latest
                    ),
                    recent AS (
                        SELECT Underlying_Ticker,
                               LIST(Action ORDER BY day_rank ASC) AS action_history
                        FROM ranked
                        WHERE day_rank <= 10
                        GROUP BY Underlying_Ticker
                    )
                    SELECT Underlying_Ticker, action_history
                    FROM recent
                """).fetchdf()

                if not _ticker_streak_df.empty:
                    _ticker_streak_df["_t_streak"] = _ticker_streak_df["action_history"].apply(_count_streak)
                    _ticker_streak_df["_t_exit_5d"] = _ticker_streak_df["action_history"].apply(_count_exit_appearances)
                    _ticker_streak_map = dict(zip(_ticker_streak_df["Underlying_Ticker"], _ticker_streak_df["_t_streak"]))
                    _ticker_exit_map = dict(zip(_ticker_streak_df["Underlying_Ticker"], _ticker_streak_df["_t_exit_5d"]))

                    # Merge: use max(trade-level, ticker-level) — never reduce a streak
                    _t_streaks = df_with_drift["Underlying_Ticker"].map(_ticker_streak_map).fillna(0).astype(int)
                    _t_exit_counts = df_with_drift["Underlying_Ticker"].map(_ticker_exit_map).fillna(0).astype(int)
                    import numpy as _np_streak
                    df_with_drift["Prior_Action_Streak"] = _np_streak.maximum(
                        df_with_drift["Prior_Action_Streak"].values, _t_streaks.values
                    )
                    df_with_drift["EXIT_Count_Last_5D"] = _np_streak.maximum(
                        df_with_drift["EXIT_Count_Last_5D"].values, _t_exit_counts.values
                    )
                    _new_max = df_with_drift["Prior_Action_Streak"].max()
                    if _new_max > _max_streak:
                        logger.info(f"[ActionStreak] Ticker-level carry-forward raised max streak: {_max_streak} → {_new_max}.")
            else:
                df_with_drift["Prior_Action_Streak"] = 0
                df_with_drift["EXIT_Count_Last_5D"] = 0
    except Exception as _streak_err:
        logger.warning(f"⚠️ Action streak fetch failed (non-fatal): {_streak_err}")
        if "Prior_Action_Streak" not in df_with_drift.columns:
            df_with_drift["Prior_Action_Streak"] = 0
        if "EXIT_Count_Last_5D" not in df_with_drift.columns:
            df_with_drift["EXIT_Count_Last_5D"] = 0

    # 2.956 Days Since Last Roll — cooldown data for signal coherence (Gate 1).
    # Query premium_ledger for OPEN legs → compute calendar days since leg was opened.
    # Used by buy_write/short_put/long_option doctrine to suppress discretionary ROLLs
    # within a strategy-dependent cooldown window (Natenberg Ch.7, Jabbour Ch.8).
    df_with_drift["Days_Since_Last_Roll"] = float('nan')
    try:
        from core.shared.data_layer.duckdb_utils import get_duckdb_connection
        with get_duckdb_connection(read_only=True) as _roll_con:
            _roll_days_df = _roll_con.execute("""
                SELECT
                    trade_id AS TradeID,
                    DATEDIFF('day', opened_at, CURRENT_DATE) AS Days_Since_Last_Roll
                FROM premium_ledger
                WHERE status = 'OPEN'
                  AND opened_at IS NOT NULL
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY trade_id
                    ORDER BY opened_at DESC
                ) = 1
            """).fetchdf()
            if not _roll_days_df.empty:
                _roll_map = dict(zip(_roll_days_df["TradeID"], _roll_days_df["Days_Since_Last_Roll"]))
                df_with_drift["Days_Since_Last_Roll"] = (
                    df_with_drift["TradeID"].map(_roll_map).astype(float)
                )
                logger.info(f"[SignalCoherence] Days_Since_Last_Roll injected for {len(_roll_map)} trades.")
    except Exception as _roll_days_err:
        logger.warning(f"⚠️ Days_Since_Last_Roll fetch failed (non-fatal): {_roll_days_err}")

    # 2.958 Decision Ledger — execution suppression + flip detection
    # If a ROLL/EXIT was recently executed (marked in executed_actions table),
    # suppress stale re-recommendations until broker data refreshes.
    # Also inject Decision_Flip_Count_5D for decision instability detection.
    df_with_drift["Execution_Pending"] = False
    df_with_drift["Last_Execution_Action"] = ""
    df_with_drift["Last_Execution_TS"] = ""
    df_with_drift["Decision_Flip_Count_5D"] = 0
    try:
        from core.shared.data_contracts.config import PIPELINE_DB_PATH
        from core.shared.data_layer.duckdb_utils import get_duckdb_connection
        with get_duckdb_connection(read_only=True) as _dl_con:
            from core.shared.data_layer.decision_ledger import (
                get_recent_executions,
                count_action_changes,
            )
            # --- Execution suppression ---
            _trade_ids = df_with_drift["TradeID"].dropna().unique().tolist()
            if _trade_ids:
                _recent_exec = get_recent_executions(_dl_con, _trade_ids, within_days=2)
                if not _recent_exec.empty:
                    for _, _ex_row in _recent_exec.iterrows():
                        _ex_tid = _ex_row["trade_id"]
                        _ex_action = _ex_row["action"]
                        _ex_ts = str(_ex_row.get("executed_at", ""))
                        _mask = df_with_drift["TradeID"] == _ex_tid
                        if _mask.any():
                            df_with_drift.loc[_mask, "Execution_Pending"] = True
                            df_with_drift.loc[_mask, "Last_Execution_Action"] = _ex_action
                            df_with_drift.loc[_mask, "Last_Execution_TS"] = _ex_ts
                    logger.info(f"[DecisionLedger] {len(_recent_exec)} recent executions found — suppression applied.")

            # --- Flip detection ---
            for _tid in _trade_ids:
                _flip_count = count_action_changes(_dl_con, _tid, window_days=5)
                if _flip_count > 0:
                    _mask = df_with_drift["TradeID"] == _tid
                    df_with_drift.loc[_mask, "Decision_Flip_Count_5D"] = _flip_count
                    if _flip_count >= 3:
                        logger.warning(f"[DecisionLedger] {_tid}: {_flip_count} action changes in 5 days — decision instability.")
    except Exception as _dl_sup_err:
        logger.debug(f"[DecisionLedger] Execution suppression/flip detection skipped: {_dl_sup_err}")

    # 2.96 Scan Signal Feedback — inject latest scan bias per ticker
    # Loads the most recent Step12_Acceptance_*.csv and extracts directional bias
    # (BULLISH/BEARISH/NEUTRAL) for each ticker.  Injected as Scan_Current_Bias so
    # the Cycle-3 engine can detect when an open position runs against the current
    # scan signal (e.g. long call on AMZN while scan is BEARISH on AMZN).
    # Non-blocking: failure leaves column as None and doctrine degrades gracefully.
    try:
        import glob as _glob
        _step12_files = sorted(
            _glob.glob(
                str(Path(__file__).parents[2] / "output" / "Step12_Acceptance_*.csv")
            ),
            reverse=True,
        )
        if _step12_files:
            _df_scan = pd.read_csv(_step12_files[0])
            # Build a direction map: BULLISH for call-based strategies, BEARISH for put-based
            def _scan_dir(strat: str) -> str:
                s = str(strat).upper()
                if any(k in s for k in ("LONG_PUT", "SHORT_CALL", "BEAR_SPREAD")):
                    return "BEARISH"
                if any(k in s for k in ("LONG_CALL", "BUY_WRITE", "COVERED_CALL", "LEAP", "BULL_SPREAD", "CASH_SECURED_PUT")):
                    return "BULLISH"
                return "NEUTRAL"

            _ticker_col = "Ticker" if "Ticker" in _df_scan.columns else None
            _strat_col  = (
                "Strategy_Name"    if "Strategy_Name"    in _df_scan.columns else
                "Primary_Strategy" if "Primary_Strategy" in _df_scan.columns else
                "Strategy"         if "Strategy"         in _df_scan.columns else None
            )
            if _ticker_col and _strat_col:
                _df_scan["_scan_dir"] = _df_scan[_strat_col].apply(_scan_dir)
                # If a ticker has both BULLISH and BEARISH candidates → MIXED
                _bias_map: dict[str, str] = {}
                for _tkr, _grp in _df_scan.groupby(_ticker_col)["_scan_dir"]:
                    _dirs = set(_grp.unique()) - {"NEUTRAL"}
                    if len(_dirs) == 0:
                        _bias_map[_tkr] = "NEUTRAL"
                    elif len(_dirs) == 1:
                        _bias_map[_tkr] = _dirs.pop()
                    else:
                        _bias_map[_tkr] = "MIXED"

                if "Underlying_Ticker" in df_with_drift.columns:
                    df_with_drift["Scan_Current_Bias"] = df_with_drift["Underlying_Ticker"].map(_bias_map)
                    _injected = df_with_drift["Scan_Current_Bias"].notna().sum()
                    logger.info(
                        f"[ScanFeedback] Injected latest scan bias ({_step12_files[0].split('/')[-1]}) "
                        f"for {_injected}/{len(df_with_drift)} rows — "
                        f"BULLISH:{sum(v=='BULLISH' for v in _bias_map.values())} "
                        f"BEARISH:{sum(v=='BEARISH' for v in _bias_map.values())} "
                        f"tickers."
                    )
                else:
                    df_with_drift["Scan_Current_Bias"] = None
            else:
                df_with_drift["Scan_Current_Bias"] = None
        else:
            df_with_drift["Scan_Current_Bias"] = None
            logger.info("[ScanFeedback] No Step12 CSV found — Scan_Current_Bias set to None.")

        # 2.96b Per-contract scan provenance — links each open position back to the
        # scan thesis that surfaced it.  Join key: Ticker + Strike + Expiration + OptionType.
        # Injects: Scan_DQS_Score, Scan_Thesis, Scan_Theory_Source, Scan_Trade_Bias,
        #          Scan_Gate_Reason, Scan_Entry_Timing, Scan_Confidence.
        # Non-blocking: unmatched positions leave these columns as None.
        _SCAN_PROVENANCE_COLS = [
            'Scan_DQS_Score', 'Scan_Thesis', 'Scan_Theory_Source', 'Scan_Trade_Bias',
            'Scan_Gate_Reason', 'Scan_Entry_Timing', 'Scan_Confidence',
            'Scan_Signal_Trajectory', 'Scan_Trajectory_Multiplier', 'Scan_Score_Acceleration',
        ]
        for _col in _SCAN_PROVENANCE_COLS:
            df_with_drift[_col] = None

        if _step12_files and '_df_scan' in dir() and not _df_scan.empty:
            _contract_map: dict = {}
            for _, _srow in _df_scan.iterrows():
                try:
                    _key = (
                        str(_srow.get('Ticker', '') or '').upper(),
                        float(_srow.get('Selected_Strike') or 0),
                        pd.to_datetime(_srow.get('Selected_Expiration')).strftime('%Y-%m-%d'),
                        str(_srow.get('Option_Type', '') or '').upper(),
                    )
                    _contract_map[_key] = {
                        'Scan_DQS_Score':      _srow.get('DQS_Score'),
                        'Scan_Thesis':         _srow.get('thesis') or _srow.get('Valid_Reason'),
                        'Scan_Theory_Source':  _srow.get('Theory_Source'),
                        'Scan_Trade_Bias':    _srow.get('Trade_Bias'),
                        'Scan_Gate_Reason':    _srow.get('Gate_Reason'),
                        'Scan_Entry_Timing':   _srow.get('entry_timing_context'),
                        'Scan_Confidence':     _srow.get('confidence_band'),
                        'Scan_Signal_Trajectory':      _srow.get('Signal_Trajectory'),
                        'Scan_Trajectory_Multiplier': _srow.get('Trajectory_Multiplier'),
                        'Scan_Score_Acceleration':    _srow.get('Score_Acceleration'),
                    }
                except Exception:
                    continue

            for _idx, _mrow in df_with_drift.iterrows():
                try:
                    _exp_raw = _mrow.get('Expiration') or _mrow.get('expiration')
                    _cp_raw  = str(_mrow.get('Call/Put', '') or '').strip().upper()
                    _opt_type = (
                        'PUT'  if _cp_raw in ('P', 'PUT')  else
                        'CALL' if _cp_raw in ('C', 'CALL') else ''
                    )
                    if not _opt_type or not _exp_raw:
                        continue
                    _mkey = (
                        str(_mrow.get('Underlying_Ticker', '') or '').upper(),
                        float(_mrow.get('Strike') or 0),
                        pd.to_datetime(_exp_raw).strftime('%Y-%m-%d'),
                        _opt_type,
                    )
                    if _mkey in _contract_map:
                        for _fc, _fv in _contract_map[_mkey].items():
                            df_with_drift.at[_idx, _fc] = _fv
                except Exception:
                    continue

            _matched = df_with_drift['Scan_DQS_Score'].notna().sum()
            logger.info(
                f"[ScanFeedback] Per-contract: {_matched}/{len(df_with_drift)} positions "
                f"linked to scan origin ({_step12_files[0].split('/')[-1]})"
            )

    except Exception as _sfe:
        logger.warning(f"⚠️ Scan signal feedback injection failed (non-fatal): {_sfe}")
        if "Scan_Current_Bias" not in df_with_drift.columns:
            df_with_drift["Scan_Current_Bias"] = None
        for _col in ['Scan_DQS_Score', 'Scan_Thesis', 'Scan_Theory_Source',
                     'Scan_Trade_Bias', 'Scan_Gate_Reason',
                     'Scan_Entry_Timing', 'Scan_Confidence']:
            if _col not in df_with_drift.columns:
                df_with_drift[_col] = None

    # 2.97 Portfolio Circuit Breaker — halt entries during portfolio distress
    # Checks drawdown, simultaneous distress, Greek exposure, and market stress.
    # If TRIPPED, sets _circuit_breaker_override=True so doctrine forces EXIT CRITICAL.
    # Non-blocking: failure leaves breaker as OPEN and logs warning.
    _cb_state = "OPEN"
    _cb_reason = ""
    try:
        import duckdb as _cb_duckdb
        _cb_db_path = str(Path(__file__).parents[2] / "data" / "pipeline.duckdb")
        _cb_conn = _cb_duckdb.connect(_cb_db_path, read_only=False)
        _peak_equity, _last_tripped_at = load_peak_equity(_cb_conn)

        # Account balance: sum of position market values + cash approximation
        # Use column '$ Total G/L' as proxy for unrealized P&L
        _account_balance = account_balance  # From CLI --balance arg (default $100k)

        # Market stress from composite regime classifier (fallback: NORMAL)
        _market_stress = "NORMAL"
        try:
            from core.shared.data_layer.market_context import get_latest_market_context
            from core.shared.data_layer.market_regime_classifier import classify_market_regime
            _mkt_ctx = get_latest_market_context()
            if _mkt_ctx is not None:
                _mkt_regime = classify_market_regime(_mkt_ctx)
                if _mkt_regime.confidence >= 0.5:
                    _market_stress = _mkt_regime.stress_level
                    df_with_drift["Market_Regime"] = _mkt_regime.regime
                    df_with_drift["Market_Regime_Score"] = _mkt_regime.score
                    df_with_drift["Market_Term_Structure"] = _mkt_regime.term_structure
                    logger.info(
                        f"[MarketCtx] COMPOSITE: regime={_mkt_regime.regime} "
                        f"score={_mkt_regime.score:.1f} stress={_market_stress} "
                        f"conf={_mkt_regime.confidence:.2f}"
                    )
                else:
                    logger.info(
                        f"[MarketCtx] Low confidence ({_mkt_regime.confidence:.2f}) "
                        f"— using SPY_ATR_FALLBACK"
                    )
        except Exception as _mkt_err:
            logger.debug(f"[MarketCtx] Composite unavailable: {_mkt_err}")

        _cb_state, _cb_reason = check_circuit_breaker(
            df_with_drift,
            account_balance=_account_balance,
            peak_equity=_peak_equity,
            market_stress_level=_market_stress,
            prior_breaker_state="TRIPPED" if _last_tripped_at else "OPEN",
            prior_breaker_tripped_at=_last_tripped_at,
        )

        df_with_drift['Circuit_Breaker_State'] = _cb_state
        df_with_drift['Circuit_Breaker_Reason'] = _cb_reason if _cb_state != "OPEN" else ""

        if _cb_state == "TRIPPED":
            df_with_drift['_circuit_breaker_override'] = True
            logger.critical(f"🚨 CIRCUIT BREAKER TRIPPED — all positions will be forced EXIT CRITICAL")
        elif _cb_state == "COOLDOWN":
            # Cooldown: original trigger cleared. Block new entries (annotation only),
            # but do NOT force EXIT on existing positions — doctrine decides individually.
            df_with_drift['_circuit_breaker_override'] = False
            logger.warning(f"⏳ CIRCUIT BREAKER COOLDOWN — new entries blocked, exits per doctrine")
        else:
            df_with_drift['_circuit_breaker_override'] = False

        # Persist equity curve
        persist_equity_curve(
            _cb_conn,
            account_balance=_account_balance,
            peak_equity=_peak_equity or _account_balance,
            circuit_breaker_state=_cb_state,
            positions_count=len(df_with_drift),
        )
        _cb_conn.close()
        logger.info(f"[CircuitBreaker] State: {_cb_state} — {_cb_reason}")

    except Exception as _cb_err:
        logger.warning(f"⚠️ Circuit breaker check failed (non-fatal): {_cb_err}")
        df_with_drift['Circuit_Breaker_State'] = 'OPEN'
        df_with_drift['Circuit_Breaker_Reason'] = ''
        df_with_drift['_circuit_breaker_override'] = False

    # 2.975 Portfolio Concentration Analysis — sector + underlying + strategy
    # analyze_correlation_risk() adds underlying/strategy concentration columns.
    # Sector concentration uses Sector_Benchmark (from thesis_engine) + SECTOR_BUCKET_MAP.
    # WARNING-only: informational for dashboard, does not block trades.
    try:
        # analyze_correlation_risk expects 'Underlying'; management engine uses 'Underlying_Ticker'
        if 'Underlying_Ticker' in df_with_drift.columns and 'Underlying' not in df_with_drift.columns:
            df_with_drift['Underlying'] = df_with_drift['Underlying_Ticker']
        df_with_drift = analyze_correlation_risk(df_with_drift)

        # Compute Sector_Bucket for each trade
        if 'Underlying_Ticker' in df_with_drift.columns:
            df_with_drift['Sector_Bucket'] = df_with_drift['Underlying_Ticker'].apply(
                lambda t: get_sector_bucket(str(t)) if pd.notna(t) else "Broad Market"
            )

        # Is_ETF flag — used for ETF-aware CC context (no earnings, macro-vol, faster HV mean-reversion)
        if 'Underlying_Ticker' in df_with_drift.columns:
            from config.sector_benchmarks import is_etf as _is_etf_flag
            df_with_drift['Is_ETF'] = df_with_drift['Underlying_Ticker'].apply(
                lambda t: _is_etf_flag(str(t)) if pd.notna(t) else False
            )

        # Sector concentration: % of capital per sector bucket
        if 'Sector_Bucket' in df_with_drift.columns:
            _trade_df = df_with_drift.drop_duplicates('TradeID') if 'TradeID' in df_with_drift.columns else df_with_drift
            _basis_col = 'Basis' if 'Basis' in _trade_df.columns else None
            if _basis_col:
                _abs_basis = pd.to_numeric(_trade_df[_basis_col], errors='coerce').abs()
                _total_basis = _abs_basis.sum()
                if _total_basis > 0:
                    _sector_basis = _abs_basis.groupby(_trade_df['Sector_Bucket']).sum()
                    _sector_pct = (_sector_basis / _total_basis * 100)
                    _max_sector_pct = float(_sector_pct.max())
                    _max_sector_name = _sector_pct.idxmax()

                    _limits = get_persona_limits(persona)
                    _max_conc = _limits.get('max_sector_concentration', 40.0)
                    if _max_sector_pct > _max_conc:
                        _flag = f"SECTOR_CONCENTRATION:{_max_sector_name}={_max_sector_pct:.0f}%"
                        _existing = df_with_drift.get('Portfolio_Risk_Flags', pd.Series([''] * len(df_with_drift))).fillna('')
                        df_with_drift['Portfolio_Risk_Flags'] = _existing + _flag + ';'
                        logger.warning(
                            f"⚠️ Sector concentration: {_max_sector_name} at {_max_sector_pct:.0f}% "
                            f"(limit: {_max_conc:.0f}%)"
                        )

                    # Log sector breakdown
                    _top3 = _sector_pct.sort_values(ascending=False).head(3)
                    _summary = ", ".join(f"{s}={p:.0f}%" for s, p in _top3.items())
                    logger.info(f"[SectorConcentration] Top sectors: {_summary}")

        logger.info("[PortfolioConcentration] Correlation and sector analysis complete.")
    except Exception as _conc_err:
        logger.warning(f"⚠️ Portfolio concentration analysis failed (non-fatal): {_conc_err}")

    # 2.976 Macro Event Proximity — static calendar enrichment.
    # Injects Days_To_Macro, Macro_Next_Type, Macro_Next_Event, Macro_Next_Date,
    # Is_Macro_Week, Macro_Density, Macro_Strip into df_with_drift.
    # Consumed by Section 3.0f (macro decision modifier) after doctrine runs.
    # No API calls — dates are hardcoded from Fed/BLS/BEA schedule.
    try:
        from config.macro_calendar import get_macro_proximity, format_macro_strip

        _snap_date = pd.to_datetime(
            df_with_drift['Snapshot_TS'].iloc[0]
        ).date() if 'Snapshot_TS' in df_with_drift.columns and not df_with_drift.empty else date.today()

        _macro = get_macro_proximity(_snap_date)

        df_with_drift['Days_To_Macro'] = _macro.days_to_next if _macro.days_to_next is not None else np.nan
        df_with_drift['Macro_Next_Type'] = _macro.next_event.event_type if _macro.next_event else ""
        df_with_drift['Macro_Next_Event'] = _macro.next_event.label if _macro.next_event else ""
        df_with_drift['Macro_Next_Date'] = _macro.next_event.event_date.isoformat() if _macro.next_event else ""
        df_with_drift['Is_Macro_Week'] = _macro.is_macro_week
        df_with_drift['Macro_Density'] = _macro.macro_density
        df_with_drift['Macro_Strip'] = format_macro_strip(_macro)

        if _macro.is_macro_week:
            logger.info(
                f"[MacroCalendar] MACRO WEEK: {_macro.next_event.event_type} in "
                f"{_macro.days_to_next}d ({_macro.next_event.event_date}). "
                f"Density: {_macro.macro_density} events within 14d."
            )
        else:
            logger.info(
                f"[MacroCalendar] Next: "
                f"{_macro.next_event.event_type if _macro.next_event else 'none'} in "
                f"{_macro.days_to_next}d. Density: {_macro.macro_density}."
            )
    except Exception as _macro_err:
        logger.warning(f"Macro calendar enrichment failed (non-fatal): {_macro_err}")
        for _mc in ('Days_To_Macro', 'Macro_Density'):
            df_with_drift[_mc] = np.nan
        for _mc in ('Macro_Next_Event', 'Macro_Next_Type', 'Macro_Next_Date', 'Macro_Strip'):
            df_with_drift[_mc] = ""
        df_with_drift['Is_Macro_Week'] = False

    # 2.977 Earnings History Enrichment — DuckDB read from earnings_stats table.
    # Injects 9 columns: Earnings_Beat_Rate, Earnings_Avg_IV_Crush_Pct, etc.
    # Single row per ticker from pre-computed summary table (no API calls).
    # Safe with MANAGEMENT_SAFE_MODE=True — pure DuckDB read.
    try:
        from core.shared.data_layer.earnings_history import get_all_earnings_stats
        from core.shared.data_contracts.config import PIPELINE_DB_PATH as _EH_DB_PATH
        import duckdb as _eh_duckdb
        _eh_con = _eh_duckdb.connect(str(_EH_DB_PATH), read_only=True)
        try:
            _unique_tickers = df_with_drift['Underlying_Ticker'].dropna().unique().tolist()
            _eh_stats = get_all_earnings_stats(_eh_con, _unique_tickers)

            if not _eh_stats.empty:
                _eh_map = _eh_stats.set_index('ticker')
                _col_mapping = {
                    'beat_rate': 'Earnings_Beat_Rate',
                    'avg_iv_crush_pct': 'Earnings_Avg_IV_Crush_Pct',
                    'avg_iv_buildup_pct': 'Earnings_Avg_IV_Ramp_Pct',
                    'avg_expected_move_pct': 'Earnings_Avg_Expected_Move_Pct',
                    'avg_actual_move_pct': 'Earnings_Avg_Actual_Move_Pct',
                    'avg_move_ratio': 'Earnings_Avg_Move_Ratio',
                    'avg_gap_pct': 'Earnings_Avg_Gap_Pct',
                    'last_surprise_pct': 'Earnings_Last_Surprise_Pct',
                    'quarters_available': 'Earnings_Track_Quarters',
                }
                for src_col, dst_col in _col_mapping.items():
                    df_with_drift[dst_col] = df_with_drift['Underlying_Ticker'].map(
                        _eh_map[src_col].to_dict() if src_col in _eh_map.columns else {}
                    )

                _filled = df_with_drift['Earnings_Track_Quarters'].notna().sum()
                logger.info(
                    f"[EarningsHistory] Enriched {_filled}/{len(df_with_drift)} rows "
                    f"from earnings_stats ({len(_eh_stats)} tickers available)"
                )
            else:
                logger.info("[EarningsHistory] No earnings_stats data — run collect_earnings_history.py first")
        finally:
            _eh_con.close()
    except Exception as _eh_err:
        logger.warning(f"Earnings history enrichment failed (non-fatal): {_eh_err}")

    # 2.978 Earnings Formation Enrichment — Phase 1→2→3 positioning detection.
    # Reads from earnings_formation_summary (per-event) and aggregates per ticker.
    # Also runs forward detection: is any position currently in a positioning phase?
    # 4 columns: Earnings_Phase2_Start_Day, Earnings_Drift_Predicted_Gap_Rate,
    #            Earnings_Formation_Quality, Earnings_Current_Phase
    try:
        from core.shared.data_layer.earnings_formation import (
            get_avg_formation_stats, detect_current_phase
        )
        from core.shared.data_contracts.config import (
            PIPELINE_DB_PATH as _EF_DB_PATH,
            IV_HISTORY_DB_PATH as _EF_IV_PATH,
        )
        import duckdb as _ef_duckdb
        from datetime import date as _ef_date

        _ef_con = _ef_duckdb.connect(str(_EF_DB_PATH), read_only=True)
        _ef_iv_con = _ef_duckdb.connect(str(_EF_IV_PATH), read_only=True)
        try:
            _today = _ef_date.today()
            _ef_filled = 0
            _ef_phase_filled = 0

            for _idx, _row in df_with_drift.iterrows():
                _tk = _row.get('Underlying_Ticker')
                if not _tk or pd.isna(_tk):
                    continue

                # Historical formation stats (averaged across events)
                _fstats = get_avg_formation_stats(_ef_con, _tk)
                if _fstats:
                    df_with_drift.at[_idx, 'Earnings_Phase2_Start_Day'] = _fstats.get('avg_phase2_start_day')
                    df_with_drift.at[_idx, 'Earnings_Drift_Predicted_Gap_Rate'] = _fstats.get('drift_predicted_gap_rate')
                    # Quality: best quality across events for this ticker
                    df_with_drift.at[_idx, 'Earnings_Formation_Quality'] = (
                        'COMPLETE' if _fstats.get('event_count', 0) >= 3
                        else 'PARTIAL' if _fstats.get('event_count', 0) >= 1
                        else 'INSUFFICIENT'
                    )
                    _ef_filled += 1

                # Forward detection: current phase (for positions with earnings within 45d)
                try:
                    _phase = detect_current_phase(_ef_con, _ef_iv_con, _tk, _today)
                    if _phase and _phase.get('phase') != 'NO_UPCOMING':
                        df_with_drift.at[_idx, 'Earnings_Current_Phase'] = _phase['phase']
                        _ef_phase_filled += 1
                except Exception:
                    pass  # Non-blocking: missing forward data is OK

            if _ef_filled > 0:
                logger.info(
                    f"[EarningsFormation] Enriched {_ef_filled}/{len(df_with_drift)} rows "
                    f"with formation stats, {_ef_phase_filled} with current phase"
                )
            else:
                logger.info("[EarningsFormation] No formation data — run collect_earnings_history.py --compute-formation first")
        finally:
            _ef_con.close()
            _ef_iv_con.close()
    except Exception as _ef_err:
        logger.warning(f"Earnings formation enrichment failed (non-fatal): {_ef_err}")

    # 2.99 Pre-Doctrine MC: Assignment Risk
    # mc_assignment_risk() only needs spot, strike, DTE, HV — all available now.
    # Running it before doctrine lets the EV comparator inside buy_write/cc/csp
    # use MC_Assign_P_Expiry for probability-weighted ASSIGN EV instead of
    # falling back to delta proxy.
    # (Other MC functions depend on doctrine's Action and run post-doctrine.)
    try:
        from core.management.mc_management import mc_assignment_risk, _is_income_strategy, N_PATHS, SEED
        import numpy as _np_mc
        _mc_pre_rng = _np_mc.random.default_rng(SEED)
        _mc_assign_cols = ["MC_Assign_P_Expiry", "MC_Assign_P_Touch", "MC_Assign_Urgency", "MC_Assign_Note"]
        for _col in _mc_assign_cols:
            if _col not in df_with_drift.columns:
                if _col in ("MC_Assign_P_Expiry", "MC_Assign_P_Touch"):
                    df_with_drift[_col] = _np_mc.nan
                else:
                    df_with_drift[_col] = ""
        _mc_pre_count = 0
        for _idx, _row in df_with_drift.iterrows():
            _strat = str(_row.get("Strategy", _row.get("Strategy_Name", "")) or "")
            if _is_income_strategy(_strat):
                try:
                    _mc_a = mc_assignment_risk(row=_row, n_paths=N_PATHS, rng=_mc_pre_rng)
                    for _col, _val in _mc_a.items():
                        if _col in df_with_drift.columns:
                            df_with_drift.at[_idx, _col] = _val
                    _mc_pre_count += 1
                except Exception:
                    pass
        if _mc_pre_count > 0:
            logger.info(f"🎲 Pre-doctrine MC assignment risk: {_mc_pre_count} income rows enriched")
    except Exception as _mc_pre_err:
        logger.warning(f"Pre-doctrine MC assignment risk failed (non-fatal): {_mc_pre_err}")

    # 2.999 Pre-Doctrine Validation Gate
    # Structured checks: UL Last, DTE, Greeks, strategy-specific inputs.
    # Blocked positions get Pre_Doctrine_Flag = DATA_BLOCKED; doctrine will
    # short-circuit them to STATE_BLOCKED_GOVERNANCE (HOLD/LOW).
    try:
        from core.management.data_integrity_monitor import (
            validate_pre_doctrine, capture_decision_inputs,
        )
        _pre_doc_report = validate_pre_doctrine(df_with_drift, run_id)
        if _pre_doc_report.blocked_count > 0:
            logger.warning(
                f"⚠️ Pre-doctrine: {_pre_doc_report.blocked_count} positions DATA_BLOCKED"
            )
        for _idx, _flag, _detail in _pre_doc_report.blocked_positions:
            df_with_drift.at[_idx, 'Pre_Doctrine_Flag'] = _flag
            df_with_drift.at[_idx, 'Pre_Doctrine_Detail'] = _detail
        # Phase 1: capture input snapshot before doctrine runs
        capture_decision_inputs(df_with_drift, run_id)
    except Exception as _pre_doc_err:
        logger.warning(f"⚠️ Pre-doctrine gate failed (non-fatal): {_pre_doc_err}")

    # Missing-data diagnosis: tag NaN fields with causal reasons (pre-doctrine)
    _missing_tracker.diagnose(df_with_drift, step_num=2)
    _missing_tracker.audit_stage("pre_doctrine", None, df_with_drift)

    # 3. Cycle 3: Doctrine
    logger.info("--- CYCLE 3: DOCTRINE ---")
    df_final = generate_recommendations(df_with_drift)

    # 3.0 Drift Filter — authoritative risk-reduction override.
    # DriftEngine._determine_action() computed Drift_Action during Cycle 2.
    # apply_drift_filter() intersects it with doctrine Action: risk may only be
    # reduced, never increased (FORCE_EXIT overrides HOLD; HOLD never overrides EXIT).
    # Only fires when Drift_Action column exists and is non-null.
    if 'Drift_Action' in df_final.columns and df_final['Drift_Action'].notna().any():
        try:
            df_final = drift_engine.apply_drift_filter(df_final, rec_col='Action')
            # apply_drift_filter writes Action_Final; promote it back to Action
            if 'Action_Final' in df_final.columns:
                _overrides = df_final['Action_Final'] != df_final['Action']
                if _overrides.any():
                    logger.info(
                        f"[DriftFilter] {_overrides.sum()} doctrine action(s) overridden "
                        f"by Drift_Action (risk-reduction only)."
                    )
                df_final['Action'] = df_final['Action_Final']
                df_final = df_final.drop(columns=['Action_Final'])
        except Exception as _drift_filter_err:
            logger.warning(f"⚠️ Drift filter failed (non-fatal): {_drift_filter_err}")

    # Shared income structure exemption — used by Rules 1-3, circuit breaker,
    # and MC Hold Verdict. BW/CC with far-OTM near-expiry short call: doctrine
    # HOLD/REVIEW is income-optimal. Overriding to EXIT costs transaction fees
    # for no risk reduction (short call near-worthless, about to expire).
    # Computed once here; referenced by all downstream guards.
    _strat_col_r = (df_final.get("Strategy", df_final.get("Strategy_Name", pd.Series(dtype=str)))).fillna("").str.upper()
    _is_income_r = _strat_col_r.str.contains("BUY_WRITE|COVERED_CALL|^CC$|^BW$", regex=True, na=False)
    _sc_delta_r = pd.to_numeric(df_final.get("Short_Call_Delta", pd.Series(dtype=float)), errors="coerce")
    _sc_dte_r = pd.to_numeric(df_final.get("DTE", pd.Series(dtype=float)), errors="coerce")
    _income_far_otm_expiring = _is_income_r & (_sc_delta_r < 0.30) & (_sc_dte_r <= 14)

    # 3.0-TR Thesis Review Scorer — resolve REVIEW → concrete executable action.
    # Runs AFTER drift filter (REVIEW actions established) and BEFORE streak
    # escalation (so resolved actions don't accumulate false REVIEW streaks).
    # Income far-OTM expiring positions are auto-REAFFIRMED (drift noise).
    _review_mask = df_final["Action"] == "REVIEW"
    if _review_mask.any():
        try:
            from core.management.cycle3.doctrine.thesis_review_scorer import score_thesis_review
            _tr_count = int(_review_mask.sum())
            _tr_resolved = {"REAFFIRMED": 0, "MONITORING": 0, "WEAKENED": 0, "DEGRADED": 0}
            for _tr_idx in df_final[_review_mask].index:
                _tr_row = df_final.loc[_tr_idx]

                # Income far-OTM expiring → auto-REAFFIRM (drift noise by design)
                if _income_far_otm_expiring.get(_tr_idx, False):
                    df_final.at[_tr_idx, "Action"] = "HOLD"
                    df_final.at[_tr_idx, "Urgency"] = "LOW"
                    df_final.at[_tr_idx, "Thesis_Review_Verdict"] = "REAFFIRMED"
                    df_final.at[_tr_idx, "Thesis_Review_Score"] = 99.0
                    df_final.at[_tr_idx, "Thesis_Review_Evidence"] = (
                        "Income far-OTM near expiry — drift noise, thesis intact"
                    )
                    df_final.at[_tr_idx, "Rationale"] = (
                        str(df_final.at[_tr_idx, "Rationale"] or "")
                        + " | Thesis REAFFIRMED (income far-OTM expiry, drift noise)"
                    )
                    df_final.at[_tr_idx, "Doctrine_Source"] = (
                        str(df_final.at[_tr_idx, "Doctrine_Source"] or "")
                        + " + ThesisReview: REAFFIRMED"
                    )
                    _tr_resolved["REAFFIRMED"] += 1
                    continue

                _verdict = score_thesis_review(_tr_row)
                df_final.at[_tr_idx, "Thesis_Review_Verdict"] = _verdict.verdict
                df_final.at[_tr_idx, "Thesis_Review_Score"] = _verdict.score
                df_final.at[_tr_idx, "Thesis_Review_Evidence"] = " | ".join(_verdict.evidence)
                df_final.at[_tr_idx, "Action"] = _verdict.action
                df_final.at[_tr_idx, "Urgency"] = _verdict.urgency
                df_final.at[_tr_idx, "Rationale"] = (
                    str(df_final.at[_tr_idx, "Rationale"] or "")
                    + f" | Thesis {_verdict.verdict} (score={_verdict.score:+.0f}): "
                    + "; ".join(_verdict.evidence[:3])
                )
                df_final.at[_tr_idx, "Doctrine_Source"] = (
                    str(df_final.at[_tr_idx, "Doctrine_Source"] or "")
                    + f" + ThesisReview: {_verdict.verdict}"
                )
                _tr_resolved[_verdict.verdict] += 1

            _tr_summary = ", ".join(f"{k}={v}" for k, v in _tr_resolved.items() if v > 0)
            logger.info(
                f"[ThesisReview] Resolved {_tr_count} REVIEW positions: {_tr_summary}."
            )
        except Exception as _tr_err:
            logger.warning(
                f"⚠️ Thesis review scorer failed (non-fatal, REVIEW preserved): {_tr_err}"
            )

    # 3.0a Action Streak Escalation — auto-resolve persistent REVIEW / stale EXIT.
    # Runs AFTER drift filter so REVIEW actions from drift override are captured.
    # Rule 1: REVIEW for ≥3 consecutive days → escalate to EXIT MEDIUM.
    #         Rationale: signal degradation is persistent, not transient noise.
    # Rule 2: EXIT for ≥5 consecutive days → promote urgency to CRITICAL.
    #         Rationale: user has not acted on EXIT; urgency must increase.
    if "Prior_Action_Streak" in df_final.columns:
        _streak = pd.to_numeric(df_final["Prior_Action_Streak"], errors="coerce").fillna(0).astype(int)

        _urgency_order = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}

        # Rule 1: REVIEW × 3+ → EXIT MEDIUM
        # EXEMPT: Income structures with far-OTM expiring short calls — persistent
        # REVIEW is likely drift noise from decaying Greeks, not real deterioration.
        _reval_mask = (df_final["Action"] == "REVIEW") & (_streak >= 3) & ~_income_far_otm_expiring
        if _reval_mask.any():
            _reval_count = int(_reval_mask.sum())
            df_final.loc[_reval_mask, "Action"] = "EXIT"
            df_final.loc[_reval_mask, "Urgency"] = "MEDIUM"
            df_final.loc[_reval_mask, "Rationale"] = (
                df_final.loc[_reval_mask, "Rationale"].fillna("")
                + " | Unresolved REVIEW x"
                + _streak[_reval_mask].astype(str)
                + " -- signal degradation persistent, escalating to EXIT."
            )
            df_final.loc[_reval_mask, "Doctrine_Source"] = (
                df_final.loc[_reval_mask, "Doctrine_Source"].fillna("")
                + " + ActionStreak: REVIEW->EXIT"
            )
            logger.info(f"[ActionStreak] Escalated {_reval_count} REVIEW->EXIT MEDIUM (streak >= 3).")

        # Rule 2: EXIT × 5+ → urgency CRITICAL
        # EXEMPT: Income structures with far-OTM near-expiry short calls.
        # Persistent EXIT on these is likely from historical drift false positives
        # (now corrected). Promoting to CRITICAL would force action on a position
        # where doctrine's EV comparator chose HOLD (let call expire worthless).
        _exit_mask = (df_final["Action"] == "EXIT") & (_streak >= 5) & ~_income_far_otm_expiring
        if _exit_mask.any():
            _exit_count = int(_exit_mask.sum())
            # Only promote if not already CRITICAL
            _current_urgency = df_final.loc[_exit_mask, "Urgency"].fillna("LOW").str.upper()
            _not_critical = _current_urgency != "CRITICAL"
            _promote_mask = _exit_mask.copy()
            _promote_mask.loc[_exit_mask] = _not_critical.values
            if _promote_mask.any():
                df_final.loc[_promote_mask, "Urgency"] = "CRITICAL"
                df_final.loc[_promote_mask, "Rationale"] = (
                    df_final.loc[_promote_mask, "Rationale"].fillna("")
                    + " | EXIT signal persisted x"
                    + _streak[_promote_mask].astype(str)
                    + " without action -- urgency critical."
                )
                df_final.loc[_promote_mask, "Doctrine_Source"] = (
                    df_final.loc[_promote_mask, "Doctrine_Source"].fillna("")
                    + " + ActionStreak: EXIT->CRITICAL"
                )
                logger.info(f"[ActionStreak] Promoted {int(_promote_mask.sum())} EXIT urgency->CRITICAL (streak >= 5).")

        # Rule 3: EXIT appeared ≥2 times in last 5 days but today's action is HOLD/ROLL
        # → override to EXIT MEDIUM. An exit signal that persists across days is not noise —
        # the user ignored it and the position continued to deteriorate.
        # (Audit: AMZN/MSFT/META Feb-2026 — EXIT signals ignored, losses doubled)
        #
        # EXEMPT: Income structures (BW/CC) with far-OTM near-expiry short calls.
        # Doctrine HOLD = "let call expire worthless" — the EV comparator already
        # evaluated EXIT vs HOLD. Prior EXIT signals may have been drift-filter
        # false positives that are now corrected. Overriding back to EXIT here
        # would negate the income-optimal doctrine decision.
        if "EXIT_Count_Last_5D" in df_final.columns:
            _exit_5d = pd.to_numeric(df_final["EXIT_Count_Last_5D"], errors="coerce").fillna(0).astype(int)

            # Uses shared _income_far_otm_expiring computed above Rules 1-3
            # Recovery doctrine guard: RECOVERY_PREMIUM (BW) and RECOVERY_LADDER
            # (CSP/BW/CC) positions already evaluated EXIT and chose recovery.
            # Prior EXIT signals from drift overrides should not cancel the recovery
            # strategy. (Mar 2026: EOSE BW + CSP fix)
            _doctrine_state_col = (
                df_final.get("Doctrine_State", pd.Series("", index=df_final.index))
                .fillna("").str.upper()
            )
            _is_recovery_doctrine = _doctrine_state_col.isin(
                ["RECOVERY_PREMIUM", "RECOVERY_LADDER"]
            )
            _ignored_exit_mask = (
                df_final["Action"].isin(["HOLD", "ROLL", "REVIEW"])
                & (_exit_5d >= 2)
                & ~_income_far_otm_expiring
                & ~_is_recovery_doctrine
            )
            if _ignored_exit_mask.any():
                _ignored_count = int(_ignored_exit_mask.sum())
                df_final.loc[_ignored_exit_mask, "Action"] = "EXIT"
                # Promote to at least MEDIUM; don't downgrade if already higher
                _cur_urg = df_final.loc[_ignored_exit_mask, "Urgency"].fillna("LOW").str.upper()
                _needs_promote = _cur_urg.isin(["LOW", ""])
                if _needs_promote.any():
                    _promote_idx = _ignored_exit_mask.copy()
                    _promote_idx.loc[_ignored_exit_mask] = _needs_promote.values
                    df_final.loc[_promote_idx, "Urgency"] = "MEDIUM"
                df_final.loc[_ignored_exit_mask, "Rationale"] = (
                    df_final.loc[_ignored_exit_mask, "Rationale"].fillna("")
                    + " | EXIT appeared "
                    + _exit_5d[_ignored_exit_mask].astype(str)
                    + "x in last 5 days but not acted on — overriding to EXIT. "
                    "(Passarelli Ch.2: an exit signal that persists is not noise.)"
                )
                df_final.loc[_ignored_exit_mask, "Doctrine_Source"] = (
                    df_final.loc[_ignored_exit_mask, "Doctrine_Source"].fillna("")
                    + " + ActionStreak: Ignored EXIT Override"
                )
                logger.info(f"[ActionStreak] Overrode {_ignored_count} HOLD/ROLL->EXIT (EXIT appeared ≥2x in last 5 days).")

    # 3.0c Decision Flip Warning — flag positions with ≥3 action changes in 5 days.
    # Appends instability warning to Rationale WITHOUT overriding the action.
    # Advisory only — the user must verify thesis before acting.
    if "Decision_Flip_Count_5D" in df_final.columns:
        _flip_ct = pd.to_numeric(df_final["Decision_Flip_Count_5D"], errors="coerce").fillna(0).astype(int)
        _flip_mask = _flip_ct >= 3
        if _flip_mask.any():
            df_final.loc[_flip_mask, "Rationale"] = (
                df_final.loc[_flip_mask, "Rationale"].fillna("")
                + " | ⚠️ Decision instability: "
                + _flip_ct[_flip_mask].astype(str)
                + " action changes in 5 days — verify thesis before acting."
            )
            logger.info(f"[DecisionLedger] {int(_flip_mask.sum())} positions flagged for decision instability.")

    # 3.0d Execution Pending — lock action to user's executed decision.
    # When the user has already acted on a ROLL/EXIT but broker data hasn't
    # refreshed yet, the engine must NOT flip the action (e.g. EXIT→HOLD).
    # Lock Action to the executed action and prepend an informational banner.
    if "Execution_Pending" in df_final.columns:
        _ep_mask = df_final["Execution_Pending"] == True  # noqa: E712
        if _ep_mask.any():
            _ep_action = df_final.loc[_ep_mask, "Last_Execution_Action"].fillna("action")
            _ep_ts = df_final.loc[_ep_mask, "Last_Execution_TS"].fillna("")
            # Lock action + urgency to what the user already executed
            df_final.loc[_ep_mask, "Action"] = _ep_action
            df_final.loc[_ep_mask, "Urgency"] = "MEDIUM"
            df_final.loc[_ep_mask, "Doctrine_Source"] = "System: Execution Pending"
            df_final.loc[_ep_mask, "Rationale"] = (
                "⏳ " + _ep_action + " executed " + _ep_ts
                + " — awaiting position data refresh. | "
                + df_final.loc[_ep_mask, "Rationale"].fillna("")
            )
            logger.info(f"[DecisionLedger] {int(_ep_mask.sum())} positions locked to executed action (awaiting broker refresh).")

    # 3.0b Circuit Breaker Override — force EXIT CRITICAL on all positions when tripped.
    # Runs AFTER doctrine + drift filter so it has the final Action column to override.
    # Only fires when _circuit_breaker_override was set by the pre-doctrine check.
    #
    # EXEMPT: Income structures (BW/CC) with far-OTM near-expiry short calls.
    # These positions have negligible risk (short call is near-worthless, about to
    # expire). Forcing EXIT costs transaction fees for no risk reduction. The stock
    # leg is managed separately by doctrine (EXIT the stock if thesis is broken).
    # Exemption annotates the position with a warning instead of overriding.
    if '_circuit_breaker_override' in df_final.columns and df_final['_circuit_breaker_override'].any():
        _cb_override_mask = df_final['_circuit_breaker_override'] == True
        # Income far-OTM near-expiry: annotate with warning, don't force EXIT
        _cb_income_exempt = _cb_override_mask & _income_far_otm_expiring
        _cb_force_mask = _cb_override_mask & ~_income_far_otm_expiring
        _cb_force_count = int(_cb_force_mask.sum())
        _cb_exempt_count = int(_cb_income_exempt.sum())
        df_final.loc[_cb_force_mask, 'Action'] = 'EXIT'
        df_final.loc[_cb_force_mask, 'Urgency'] = 'CRITICAL'
        df_final.loc[_cb_force_mask, 'Exit_Trigger_Type'] = 'CAPITAL'
        _cb_reason_val = df_final.loc[_cb_override_mask, 'Circuit_Breaker_Reason'].iloc[0] if 'Circuit_Breaker_Reason' in df_final.columns else 'Portfolio circuit breaker tripped'
        df_final.loc[_cb_force_mask, 'Rationale'] = (
            df_final.loc[_cb_force_mask, 'Rationale'].fillna('') + ' | '
            + f'CIRCUIT BREAKER: {_cb_reason_val}'
        )
        df_final.loc[_cb_force_mask, 'Doctrine_Source'] = 'McMillan Ch.3: Portfolio Circuit Breaker'
        if _cb_exempt_count > 0:
            df_final.loc[_cb_income_exempt, 'Rationale'] = (
                df_final.loc[_cb_income_exempt, 'Rationale'].fillna('') + ' | '
                + f'⚠️ CIRCUIT BREAKER TRIPPED but income far-OTM near-expiry exempt '
                + f'(short call near-worthless, let expire). Reason: {_cb_reason_val}'
            )
            logger.warning(
                f"[CircuitBreaker] {_cb_exempt_count} income far-OTM near-expiry "
                f"positions annotated (not forced EXIT)"
            )
        if _cb_force_count > 0:
            logger.critical(
                f"🚨 [CircuitBreaker] Forced EXIT CRITICAL on {_cb_force_count} positions"
            )

    # 3.0c Exit Coordinator — sequence simultaneous exits for market impact minimization.
    # Only fires when >3 exits are pending. Adds Exit_Sequence and Exit_Priority_Reason.
    # Non-blocking: failure leaves columns empty.
    _exit_count = int((df_final.get('Action', pd.Series(dtype=str)) == 'EXIT').sum())
    if _exit_count > 3:
        try:
            df_final = sequence_exits(df_final)
            logger.info(f"[ExitCoordinator] Sequenced {_exit_count} simultaneous exits.")
        except Exception as _ec_err:
            logger.warning(f"⚠️ Exit coordinator failed (non-fatal): {_ec_err}")
    else:
        df_final['Exit_Sequence'] = np.nan
        df_final['Exit_Priority_Reason'] = ''

    # 3.0c2 Cross-Leg Direction Reversal Gate (Natenberg Ch.11 / Passarelli Ch.6)
    # When executing EXIT on one leg flips the combined underlying delta direction,
    # annotate the EXIT with a warning so the user knows the remaining exposure changes.
    # Non-blocking: failure leaves columns empty.
    try:
        df_final = compute_direction_reversals(df_final)
        _dr_count = int((df_final.get('Direction_Reversal_Warning', pd.Series(dtype=str)).fillna('') != '').sum())
        if _dr_count > 0:
            logger.info(f"[DirectionReversal] Flagged {_dr_count} legs with direction reversal warnings.")
    except Exception as _drg_err:
        logger.warning(f"Direction reversal gate failed (non-fatal): {_drg_err}")

    # 3.0d Exit Limit Pricer — suggest limit prices for EXIT actions using daily
    # technical levels (EMA9, SMA20, BB) + delta approximation.
    # Non-blocking: failure leaves Exit_Limit_* columns empty/NaN.
    try:
        from core.management.exit_limit_pricer import compute_exit_limit_prices
        df_final = compute_exit_limit_prices(df_final)
        _elp_count = int(pd.notna(df_final.get("Exit_Limit_Price", pd.Series(dtype=float))).sum())
        if _elp_count > 0:
            logger.info(f"[ExitLimitPricer] Computed limit prices for {_elp_count} EXIT rows.")
    except Exception as _elp_err:
        logger.warning(f"Exit limit pricer failed (non-fatal): {_elp_err}")

    # 3.0e Exit Optimal Window — classify intraday execution timing for EXIT actions.
    # Parallel to _classify_roll_timing() but adapted for exit context.
    # Only fires for EXIT HIGH/CRITICAL (LOW/MEDIUM use patience from Phase 1).
    try:
        from core.management.exit_window_classifier import classify_exit_windows
        df_final = classify_exit_windows(df_final)
        _ewc_count = int((df_final.get("Exit_Window_State", pd.Series(dtype=str)).fillna("") != "").sum())
        if _ewc_count > 0:
            logger.info(f"[ExitWindow] Classified exit windows for {_ewc_count} EXIT rows.")
    except Exception as _ewc_err:
        logger.warning(f"Exit window classifier failed (non-fatal): {_ewc_err}")

    # 3.0f Macro Event Decision Modifier — cross-cutting urgency/rationale adjustment.
    # Runs AFTER doctrine (3.0) so it modifies the final Action/Urgency without being
    # duplicated in every strategy file. Analogous to 3.0a (Action Streak Escalation).
    #
    # Rules (strategy-aware, event-type-aware):
    #   DIRECTIONAL + HIGH macro ≤3d + REVIEW → EXIT MEDIUM (binary event + degraded signal)
    #   DIRECTIONAL + HIGH macro ≤3d + HOLD LOW   → HOLD HIGH (review before event)
    #   INCOME + HIGH macro ≤3d                   → keep (IV inflation benefits sellers)
    #   ETF + HIGH macro ≤3d (non-income)         → urgency ≥ HIGH (macro = earnings equivalent)
    #   GDP/PCE (MEDIUM impact)                    → informational only, no urgency change
    #
    # Bennett: "Macro events compress vol pre-release and expand post-release."
    # Natenberg Ch.12: "Binary event risk cannot be hedged by delta alone."
    # Passarelli Ch.6: "Pre-event IV inflation benefits income sellers short-term."
    if 'Days_To_Macro' in df_final.columns and 'Macro_Next_Type' in df_final.columns:
        try:
            _dtm = pd.to_numeric(df_final['Days_To_Macro'], errors='coerce')
            _macro_type = df_final['Macro_Next_Type'].fillna('')
            _macro_event = df_final.get('Macro_Next_Event', pd.Series([''] * len(df_final))).fillna('')
            _macro_date = df_final.get('Macro_Next_Date', pd.Series([''] * len(df_final))).fillna('')
            _is_etf = df_final.get('Is_ETF', pd.Series([False] * len(df_final))).fillna(False)
            _strategy = df_final['Strategy'].fillna('').str.upper()
            _action = df_final['Action'].fillna('')
            _urgency = df_final['Urgency'].fillna('LOW')

            _DIRECTIONAL = {'LONG_CALL', 'LONG_PUT', 'BUY_CALL', 'BUY_PUT', 'LEAPS_CALL', 'LEAPS_PUT'}
            _INCOME = {'BUY_WRITE', 'COVERED_CALL', 'CSP', 'CASH_SECURED_PUT'}
            _HIGH_IMPACT = {'FOMC', 'CPI', 'NFP'}
            _macro_modified = 0

            for idx in df_final.index:
                # Skip rows where the user already executed — never override their action
                if df_final.at[idx, "Execution_Pending"] == True:  # noqa: E712
                    continue
                dtm_val = _dtm.get(idx)
                if pd.isna(dtm_val) or dtm_val > 5:
                    continue  # No macro event within 5 days — skip

                dtm_int = int(dtm_val)
                evt_type = str(_macro_type.get(idx, ''))
                evt_label = str(_macro_event.get(idx, ''))
                evt_date_str = str(_macro_date.get(idx, ''))
                strat = str(_strategy.get(idx, ''))
                act = str(_action.get(idx, ''))
                urg = str(_urgency.get(idx, 'LOW'))
                etf_flag = bool(_is_etf.get(idx, False))
                is_high = evt_type in _HIGH_IMPACT

                is_directional = strat in _DIRECTIONAL or (etf_flag and strat not in _INCOME)
                is_income = strat in _INCOME
                _macro_note = ""

                if is_directional and is_high and dtm_int <= 5:
                    # D0: Long-premium EXIT from theta/non-structural gate + macro catalyst
                    # → downgrade to HOLD HIGH. The macro event IS the expected vol catalyst
                    # that justifies holding through theta bleed.
                    # Exempt: hard stops, direction-adverse, structure-broken (real problems)
                    _is_long_opt_d0 = strat in ('LONG_CALL', 'LONG_PUT', 'BUY_CALL',
                                                 'BUY_PUT', 'LEAPS_CALL', 'LEAPS_PUT')
                    _d0_dte = float(df_final.at[idx, 'DTE'] or 0) if 'DTE' in df_final.columns else 0
                    _d0_src = str(df_final.at[idx, 'Doctrine_Source'] or '') if 'Doctrine_Source' in df_final.columns else ''
                    _d0_structural_exit = any(kw in _d0_src.upper() for kw in (
                        'STRUCTURE BROKEN', 'HARD STOP', 'PIN RISK', 'GAMMA',
                        'DIRECTION ADVERSE', 'DELTA COLLAPSE', 'RECOVERY IMPOSSIBLE',
                    ))
                    if (_is_long_opt_d0
                            and act == 'EXIT'
                            and not _d0_structural_exit
                            and _d0_dte >= 14):
                        df_final.at[idx, 'Action'] = 'HOLD'
                        df_final.at[idx, 'Urgency'] = 'HIGH'
                        _macro_note = (
                            f" | 📅 MACRO CATALYST OVERRIDE: {evt_type} in {dtm_int}d ({evt_date_str}). "
                            f"Prior EXIT from non-structural gate ({_d0_src}) downgraded to HOLD — "
                            f"{evt_type} is expected vol catalyst for long premium at DTE={_d0_dte:.0f}. "
                            f"Hold through event if conviction intact; exit after if vol doesn't expand. "
                            f"(Bennett: macro compress/expand vol; Natenberg Ch.12: binary event catalyst.)"
                        )
                        df_final.at[idx, 'Rationale'] = (
                            str(df_final.at[idx, 'Rationale'] or '') + _macro_note
                        )
                        _macro_modified += 1
                        continue  # D0 fully handled this position — skip other macro rules

                if is_directional and is_high and dtm_int <= 3:
                    # D1: REVIEW → EXIT on directional + HIGH macro within 3d
                    if act == 'REVIEW':
                        df_final.at[idx, 'Action'] = 'EXIT'
                        df_final.at[idx, 'Urgency'] = 'MEDIUM'
                        _macro_note = (
                            f" | MACRO ESCALATION: {evt_type} in {dtm_int}d ({evt_date_str}). "
                            f"Directional + unresolved REVIEW + binary macro event = EXIT. "
                            f"(Natenberg Ch.12: binary event risk.)"
                        )
                    # D2: HOLD LOW/MEDIUM → HOLD HIGH on directional + HIGH macro within 3d
                    # ETF positions: MEDIUM also escalates (macro = earnings equivalent)
                    elif act == 'HOLD' and (urg == 'LOW' or (etf_flag and urg == 'MEDIUM')):
                        # Long options: macro event is a potential catalyst (vol expansion).
                        # Frame as opportunity-aware, not just risk.
                        _is_long_opt = strat in ('LONG_CALL', 'LONG_PUT', 'BUY_CALL',
                                                  'BUY_PUT', 'LEAPS_CALL', 'LEAPS_PUT')

                        # Deep ITM guard: delta > 0.75 AND DTE > 90 = stock replacement
                        # with minimal extrinsic value.  Macro events don't meaningfully
                        # impact these positions — skip urgency escalation, add info note only.
                        _d2_delta = abs(float(df_final.at[idx, 'Delta'] or 0)) if 'Delta' in df_final.columns else 0
                        _d2_dte = float(df_final.at[idx, 'DTE'] or 0) if 'DTE' in df_final.columns else 0
                        _deep_itm_leaps = _is_long_opt and _d2_delta > 0.75 and _d2_dte > 90

                        if _deep_itm_leaps:
                            # Don't escalate urgency — informational note only
                            _macro_note = (
                                f" | Macro: {evt_type} in {dtm_int}d ({evt_date_str}). "
                                f"Deep ITM (Δ {_d2_delta:.2f}, DTE {_d2_dte:.0f}) — minimal extrinsic at risk. "
                                f"Position behaves like stock; macro event is low impact."
                            )
                        elif _is_long_opt:
                            df_final.at[idx, 'Urgency'] = 'HIGH'
                            _macro_note = (
                                f" | MACRO CATALYST: {evt_type} in {dtm_int}d ({evt_date_str}). "
                                f"Binary macro event — potential vol expansion benefits long premium. "
                                f"Review thesis: hold through if conviction strong, trim if vol already priced in. "
                                f"(Bennett: macro events compress/expand vol discontinuously.)"
                            )
                        else:
                            df_final.at[idx, 'Urgency'] = 'HIGH'
                            _macro_note = (
                                f" | MACRO WARNING: {evt_type} in {dtm_int}d ({evt_date_str}). "
                                f"Binary macro risk — review thesis before event, exit if conviction weak. "
                                f"(Bennett: macro events compress/expand vol discontinuously.)"
                            )
                    else:
                        _macro_note = (
                            f" | Macro: {evt_type} in {dtm_int}d ({evt_date_str}). "
                            f"Directional position — monitor for binary risk."
                        )

                elif is_directional and dtm_int <= 5:
                    # D3: Directional within 5d — informational
                    _macro_note = (
                        f" | Macro: {evt_type} in {dtm_int}d ({evt_date_str}). "
                        f"{'Verify thesis conviction — binary macro risk.' if is_high else 'Monitor for vol impact.'}"
                    )

                elif is_income and dtm_int <= 5:
                    # I1: Income within 5d — premium context (DON'T exit)
                    if dtm_int <= 3 and is_high:
                        _macro_note = (
                            f" | Macro IV context: {evt_type} in {dtm_int}d ({evt_date_str}). "
                            f"Pre-event IV inflation benefits short premium — "
                            f"timing quality priced into ROLL EV. "
                            f"(Passarelli Ch.6: pre-event theta is structural edge.)"
                        )
                    else:
                        _macro_note = (
                            f" | Macro: {evt_type} in {dtm_int}d ({evt_date_str}). "
                            f"IV may inflate pre-event — premium favorable."
                        )
                    # Income + ROLL: note roll target should clear event
                    if act == 'ROLL':
                        _macro_note += (
                            f" Verify roll target expiry clears {evt_date_str}."
                        )

                elif etf_flag and dtm_int <= 3 and is_high:
                    # E1: ETF + HIGH macro within 3d = earnings equivalent
                    if act == 'HOLD' and urg in ('LOW', 'MEDIUM'):
                        df_final.at[idx, 'Urgency'] = 'HIGH'
                    _macro_note = (
                        f" | ETF MACRO: {evt_type} in {dtm_int}d ({evt_date_str}). "
                        f"For ETFs, macro events ARE the earnings equivalent — binary catalyst. "
                        f"Review sizing and directional exposure. "
                        f"(Natenberg Ch.12: event gap risk on broad market instruments.)"
                    )

                if _macro_note:
                    df_final.at[idx, 'Rationale'] = str(df_final.at[idx, 'Rationale'] or '') + _macro_note
                    _macro_modified += 1

            if _macro_modified > 0:
                logger.info(f"[MacroModifier] Modified {_macro_modified} positions with macro context.")
        except Exception as _macro_mod_err:
            logger.warning(f"Macro decision modifier failed (non-fatal): {_macro_mod_err}")

    # 3.0g & 3.0g2 — Flip-Flop Dampener & Risk-State Stability Filter
    # REMOVED: v2 proposal-based doctrine resolves oscillation at the source by
    # evaluating all gates and picking the best action via EV comparison.
    # Post-hoc dampening is no longer needed.

    # 3.0h Intraday Stability Annotation (Signal Coherence Gate 4)
    # If prior same-day run produced a different Action → annotate with warning.
    # Does NOT change action — information only for dashboard user.
    df_final["Signal_Stability_Warning"] = ""
    try:
        from core.shared.data_layer.duckdb_utils import get_duckdb_connection
        with get_duckdb_connection(read_only=True) as _intra_con:
            _today_str = pd.Timestamp.now().strftime('%Y-%m-%d')
            _intra_df = _intra_con.execute(f"""
                SELECT TradeID, Action AS Intra_Prior_Action
                FROM management_recommendations
                WHERE CAST(Snapshot_TS AS DATE) = '{_today_str}'
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY TradeID
                    ORDER BY Snapshot_TS DESC
                ) = 1
            """).fetchdf()
            if not _intra_df.empty:
                _intra_map = dict(zip(_intra_df["TradeID"], _intra_df["Intra_Prior_Action"]))
                _intra_prior = df_final["TradeID"].map(_intra_map).fillna("")
                # Translate legacy labels stored before the rename
                _intra_prior = _intra_prior.replace({"REVALIDATE": "REVIEW", "ASSIGN": "LET_EXPIRE"})
                _intra_flip = (
                    (_intra_prior != "")
                    & (_intra_prior.str.upper() != df_final["Action"].fillna("").str.upper())
                )
                _intra_count = int(_intra_flip.sum())
                if _intra_count > 0:
                    df_final.loc[_intra_flip, "Signal_Stability_Warning"] = (
                        "Intraday flip: prior run = "
                        + _intra_prior[_intra_flip]
                        + " → now = " + df_final.loc[_intra_flip, "Action"]
                    )
                    df_final.loc[_intra_flip, "Rationale"] = (
                        df_final.loc[_intra_flip, "Rationale"].fillna("")
                        + " | ⚠️ Intraday signal changed: prior run today was "
                        + _intra_prior[_intra_flip]
                        + ". Verify thesis before acting."
                    )
                    logger.info(f"[SignalCoherence] Intraday stability warning on {_intra_count} position(s).")
    except Exception as _intra_err:
        logger.warning(f"⚠️ Intraday stability annotation failed (non-fatal): {_intra_err}")

    # 3.1 Roll Candidate Evaluation
    # For every ROLL decision, fetch the chain and rank top-3 specific contracts
    # to roll into — matched to the thesis (strategy type, DTE window, delta target).
    # Only calls Schwab if schwab_client is available; reuses session chain cache
    # from LiveGreeks to avoid duplicate API calls.
    # Build mask: include ROLL rows + HOLD rows that have active blocking conditions.
    # Pre-staging candidates on HOLD-with-condition means they are ready the instant
    # the condition resolves — no second Schwab call needed.
    # Include ROLL_WAIT: chain was absent when doctrine ran → that's why it was gated.
    # Fetching candidates here resolves the circular dependency: ROLL_WAIT fires because
    # no chain data → roll engine skips it → chain never fetches → always ROLL_WAIT.
    # Include BUY_WRITE EXIT rows: even on EXIT, the user must decide what to do with
    # the short call leg. Pre-staging candidates (now in WEEKLY mode for BROKEN equity)
    # gives the call leg disposition options without a second pipeline run.
    # Passarelli Ch.6: decouple stock exit from call decision — call still needs context.
    _roll_mask = df_final["Action"].isin(["ROLL", "ROLL_WAIT", "ROLL_UP_OUT"])
    _hold_with_condition_mask = (
        (df_final["Action"] == "HOLD")
        & (
            df_final["_Active_Conditions"].fillna("").str.contains(
                "dead_cat_bounce|iv_depressed", case=False, na=False
            )
        )
    ) if "_Active_Conditions" in df_final.columns else pd.Series(False, index=df_final.index)
    # BUY_WRITE/COVERED_CALL EXIT: pre-stage call leg roll candidates for disposition
    _bw_strategies = {"BUY_WRITE", "COVERED_CALL"}
    _bw_exit_mask = (
        (df_final["Action"] == "EXIT")
        & df_final["Strategy"].isin(_bw_strategies)
    ) if "Strategy" in df_final.columns else pd.Series(False, index=df_final.index)
    # BUY_WRITE/CC HOLD+HIGH urgency: gamma-dominance buyback or other structural
    # issues where user needs re-sell candidates after buying back the call.
    # Passarelli Ch.6: decouple call decision from stock — need chain for both.
    _bw_hold_high_mask = (
        (df_final["Action"] == "HOLD")
        & (df_final["Urgency"].isin(["HIGH", "CRITICAL"]))
        & df_final["Strategy"].isin(_bw_strategies)
    ) if "Strategy" in df_final.columns else pd.Series(False, index=df_final.index)
    _needs_candidates = _roll_mask | _hold_with_condition_mask | _bw_exit_mask | _bw_hold_high_mask

    if schwab_client is not None and _needs_candidates.any():
        try:
            from core.management.cycle3.roll.roll_candidate_engine import find_roll_candidates
            df_final = find_roll_candidates(
                df_final,
                schwab_client,
                session_chain_cache=_session_chain_cache,
                action_mask=_needs_candidates,
            )
            roll_filled = df_final["Roll_Candidate_1"].notna().sum()
            hold_prestaged = _hold_with_condition_mask.sum()
            bw_exit_prestaged = _bw_exit_mask.sum()
            logger.info(
                f"[RollEngine] ✅ Roll candidates attached for {roll_filled} rows "
                f"({_roll_mask.sum()} ROLL + {hold_prestaged} HOLD pre-staged"
                f" + {bw_exit_prestaged} BW/CC EXIT call-leg disposition)."
            )
        except Exception as _roll_err:
            logger.warning(f"⚠️ Roll candidate engine failed (non-fatal): {_roll_err}")

    # 3.2 CC Opportunity Engine — idle stock positions
    # Reads latest Step12 scan output to evaluate whether selling covered calls
    # is currently favorable for each naked stock position. Non-blocking.
    #
    # "Idle" = STOCK row with no short call written against it.
    # Two cases:
    #   STOCK_ONLY_IDLE — orphan stock (tagged by ingest/clean.py: no option legs at all)
    #   STOCK_ONLY      — stock with some option legs (CSP, long puts, etc.) but no
    #                     BUY_WRITE/COVERED_CALL short call covering the shares.
    # Both need CC evaluation; the original STOCK_ONLY_IDLE-only filter missed the latter.
    # Build per-ticker coverage: shares covered by short calls vs total stock
    _covered_shares_cc: dict = {}   # ticker → shares covered by short calls
    _total_shares_cc: dict = {}     # ticker → total stock shares
    if "AssetType" in df_final.columns and "Strategy" in df_final.columns:
        for _, _scrow in df_final[df_final["AssetType"] == "OPTION"].iterrows():
            _sc_strat = str(_scrow.get("Strategy") or "").upper()
            _sc_cp    = str(_scrow.get("Call/Put") or "").upper()
            _sc_qty   = float(_scrow.get("Quantity") or 0)
            _sc_ul    = str(_scrow.get("Underlying_Ticker") or "")
            if _sc_strat in ("BUY_WRITE", "COVERED_CALL") or (_sc_cp in ("C", "CALL") and _sc_qty < 0):
                _covered_shares_cc[_sc_ul] = _covered_shares_cc.get(_sc_ul, 0) + abs(_sc_qty) * 100
        for _, _strow in df_final[df_final["AssetType"] == "STOCK"].iterrows():
            _st_ul = str(_strow.get("Underlying_Ticker") or "")
            _st_qty = abs(float(_strow.get("Quantity") or 0))
            _total_shares_cc[_st_ul] = _total_shares_cc.get(_st_ul, 0) + _st_qty

    # Fully-covered tickers: every share already has a short call
    _fully_covered_tickers = {
        tk for tk, total in _total_shares_cc.items()
        if _covered_shares_cc.get(tk, 0) >= total
    }

    _idle_stock_mask = (
        (df_final.get("AssetType", pd.Series("", index=df_final.index)) == "STOCK")
        & (df_final.get("Strategy", pd.Series("", index=df_final.index)).isin(
            ["STOCK_ONLY_IDLE", "STOCK_ONLY"]
        ))
        & ~df_final.get("Underlying_Ticker", pd.Series("", index=df_final.index)).isin(
            _fully_covered_tickers
        )
    )
    if _idle_stock_mask.any():
        try:
            from core.management.cycle3.cc_opportunity_engine import evaluate_cc_opportunities
            df_final = evaluate_cc_opportunities(df_final, schwab_client=schwab_client)
            _cc_favorable = (df_final.get("CC_Proposal_Status", "") == "FAVORABLE").sum()
            _cc_unfav     = (df_final.get("CC_Proposal_Status", "") == "UNFAVORABLE").sum()
            logger.info(
                f"[CCOpportunity] {_idle_stock_mask.sum()} idle stock(s) evaluated: "
                f"{_cc_favorable} FAVORABLE, {_cc_unfav} UNFAVORABLE"
            )
        except Exception as _cc_err:
            logger.warning(f"⚠️ CC opportunity engine failed (non-fatal): {_cc_err}")

    # RAG: Ensure only trade-level P&L columns are present before renaming for final schema
    if 'PnL_Total' in df_final.columns and 'PnL_Total_Trade' in df_final.columns:
        df_final = df_final.drop(columns=['PnL_Total'])
    if 'PnL_Unexplained' in df_final.columns and 'PnL_Unexplained_Trade' in df_final.columns:
        df_final = df_final.drop(columns=['PnL_Unexplained'])

    # RAG: Rename P&L columns for final schema
    if 'PnL_Total_Trade' in df_final.columns:
        df_final = df_final.rename(columns={'PnL_Total_Trade': 'PnL_Total'})
    if 'PnL_Unexplained_Trade' in df_final.columns:
        df_final = df_final.rename(columns={'PnL_Unexplained_Trade': 'PnL_Unexplained'})

    # 3.5a Recompute PnL for live-refreshed option rows.
    # LiveGreeksProvider.enrich() updated Last/Bid/Ask and recomputed PnL_Total earlier,
    # but compute_pnl_attribution aggregated at trade level (PnL_Total_Trade) using the
    # STALE compute_basic_drift values, then the rename above overwrote our live PnL.
    # Fix: recompute PnL_Total from the live Last for schwab_live rows AFTER the rename.
    try:
        from core.management.cycle2.providers.live_greeks_provider import _recompute_option_pnl
        _live_mask = (df_final.get("AssetType", pd.Series(dtype=str)) == "OPTION")
        _recompute_option_pnl(df_final, _live_mask)
    except Exception as _rp_err:
        logger.debug(f"Live P&L recompute after rename skipped: {_rp_err}")

    # Add ingest_context to df_final
    df_final['ingest_context'] = "local_ingest" # Set context for local runs

    # --- Capital Bucket Classification (management pipeline) ---
    # Must run BEFORE enforce_management_schema — otherwise schema fills Capital_Bucket
    # with "N/A" and the NaN check below never fires.
    # Mirror of step12_acceptance._assign_capital_bucket(). Applied here at trade level
    # so the portfolio snapshot and doctrine cards show correct bucket exposure.
    # DTE is NaN on stock legs — bucket is determined from strategy name alone for income.
    _INCOME_STRATEGIES = {
        "BUY_WRITE", "COVERED_CALL", "CASH_SECURED_PUT",
        "PUT_CREDIT_SPREAD", "CALL_CREDIT_SPREAD",
        # Fidelity/broker short-form aliases
        "CSP", "BW", "CC",
        # Stock legs of income structures — always backing an income trade
        "STOCK_ONLY",
    }
    _LEAP_KEYWORDS = ("leap", "LEAP")
    def _mgmt_capital_bucket(row):
        _sname = str(row.get("Strategy", "") or "").strip()
        _sname_norm = _sname.upper().replace(" ", "_").replace("-", "_")
        if any(k in _sname for k in _LEAP_KEYWORDS):
            return "STRATEGIC"
        if _sname_norm in _INCOME_STRATEGIES:
            return "DEFENSIVE"
        # DIRECTIONAL / VOLATILITY: use DTE to split TACTICAL vs STRATEGIC
        _dte = row.get("DTE")
        try:
            import math
            _dte_f = float(_dte)
            if math.isnan(_dte_f):
                _dte_f = 45.0
        except (TypeError, ValueError):
            _dte_f = 45.0
        if _sname_norm in ("LONG_CALL", "LONG_PUT", "STRADDLE", "STRANGLE", "VOLATILITY"):
            return "STRATEGIC" if _dte_f > 60 else "TACTICAL"
        return "TACTICAL"

    # Always recompute — management data never carries Capital_Bucket from scan engine
    df_final["Capital_Bucket"] = df_final.apply(_mgmt_capital_bucket, axis=1)
    logger.info(f"💼 Capital Buckets — {df_final['Capital_Bucket'].value_counts().to_dict()}")

    # RAG: Schema Enforcement. Hardening the UI contract.
    df_final = enforce_management_schema(df_final)

    print_provenance("SNAPSHOT", "Final Recommendations", "", df_final)
    try:
        print_preview("final_df", df_final, ['TradeID', 'Symbol', 'Strategy', 'Action', 'Urgency', 'Decision_State'])
    except Exception as e:
        logger.warning(f"Preview failed for final_df: {e}")

    # --- Epistemic Enforcement ---
    # Structural_Data_Complete reflects PriceStructure swing metrics (swing_hh_count,
    # break_of_structure, etc.) — supplementary context NOT required for core doctrine.
    # The doctrine engine uses equity integrity, Greeks, IV/HV, DTE, and cost basis,
    # all of which are independent of PriceStructure swing metrics.
    # Previous behavior: override ALL rationale and force EXIT/ROLL→HOLD when swing
    # metrics were missing, which caused every position to get "Structure unresolved"
    # and made the entire doctrine engine inert.
    # New behavior: log a warning but respect the doctrine engine's decision.
    if 'Structural_Data_Complete' in df_final.columns:
        _incomplete_count = (~df_final['Structural_Data_Complete']).sum()
        if _incomplete_count > 0:
            logger.info(
                f"ℹ️ PriceStructure incomplete for {_incomplete_count}/{len(df_final)} positions "
                f"— doctrine decisions preserved (swing metrics are supplementary)"
            )
    
    # --- Data Contract Validation ---
    missing = [c for c in REQUIRED_COLUMNS if c not in df_final.columns]
    if missing:
        logger.error(f"❌ DATA CONTRACT VIOLATION: Missing columns {missing}")
        raise RuntimeError(f"Abort: Missing required authoritative columns {missing}")

    # --- Downstream Protection: re-force governance on DATA_BLOCKED rows ---
    # Drift filter, macro modifier, or post-doctrine scoring may have overwritten
    # the governance state.  Re-force it now.
    if 'Pre_Doctrine_Flag' in df_final.columns:
        _blocked_mask = df_final['Pre_Doctrine_Flag'].isin(['DATA_BLOCKED', 'PRICE_STALE', 'GREEKS_MISSING'])
        _overwritten = _blocked_mask & (df_final.get('Decision_State', '') != 'STATE_BLOCKED_GOVERNANCE')
        if _overwritten.any():
            logger.error(
                f"[DownstreamProtection] {_overwritten.sum()} DATA_BLOCKED rows had governance "
                f"state overwritten — re-forcing STATE_BLOCKED_GOVERNANCE"
            )
            df_final.loc[_overwritten, 'Decision_State'] = 'STATE_BLOCKED_GOVERNANCE'
            df_final.loc[_overwritten, 'Action'] = 'HOLD'
            df_final.loc[_overwritten, 'Urgency'] = 'LOW'
            df_final.loc[_overwritten, 'Doctrine_Source'] = 'System: Data Integrity Gate'
            df_final.loc[_overwritten, 'Resolution_Method'] = 'GOVERNANCE_BLOCK'

    # --- Decision Input Audit Phase 2: persist doctrine outputs ---
    try:
        from core.management.data_integrity_monitor import update_decision_outputs
        update_decision_outputs(df_final, run_id)
    except Exception as _dia_p2_err:
        logger.warning(f"⚠️ Decision input audit Phase 2 failed (non-fatal): {_dia_p2_err}")

    # Missing-data diagnosis: tag NaN fields with causal reasons (post-doctrine)
    _missing_tracker.diagnose(df_final, step_num=3)
    _missing_tracker.audit_stage("post_doctrine", df_with_drift, df_final)
    _missing_tracker.check_impossible(df_final, step_num=3)

    # --- Data Integrity Audit (non-blocking) ---
    # Structured health checks for NaN contamination, stale data, missing Greeks,
    # anomalous value ranges, and resolution reason distribution shifts.
    _integrity_summary = ""
    try:
        from core.management.data_integrity_monitor import (
            run_integrity_checks, log_report,
        )
        _integrity_report = run_integrity_checks(df_final, run_id)
        _integrity_summary = log_report(_integrity_report)
    except Exception as _dim_err:
        logger.warning(f"⚠️ Data integrity monitor failed (non-fatal): {_dim_err}")

    # --- Persist daily market state (non-blocking) ---
    # Saves condition onset/resolution timestamps + thesis state for next run.
    # Oscillation guard, days_active, and thesis fallback all depend on this.
    try:
        if _state_store is not None:
            _state_store.save(df_final)
            logger.info("[StateStore] Market state persisted for next run.")
    except Exception as _ss_save_err:
        logger.warning(f"⚠️ StateStore save failed (non-fatal): {_ss_save_err}")

    # --- Recovery cross-position enrichment ---
    # Recovery mode evaluates positions in isolation but recovery speed depends on
    # total idle shares that COULD have CCs sold against them.  Annotate recovery
    # positions with the full underlying exposure so the rationale reflects reality.
    try:
        _recovery_mask_xp = (
            df_final.get("Doctrine_State", pd.Series("", index=df_final.index)).fillna("")
            == "RECOVERY_LADDER"
        )
        if _recovery_mask_xp.any():
            # Build per-ticker share counts: total stock shares and covered shares
            _stock_mask = df_final.get("AssetType", pd.Series("", index=df_final.index)) == "STOCK"
            _ticker_shares = {}
            for _, _r in df_final[_stock_mask].iterrows():
                _tk = str(_r.get("Underlying_Ticker") or "")
                _qty = abs(float(_r.get("Quantity") or 0))
                _ticker_shares[_tk] = _ticker_shares.get(_tk, 0) + _qty

            _covered_shares = {}
            _opt_mask = df_final.get("AssetType", pd.Series("", index=df_final.index)) == "OPTION"
            for _, _r in df_final[_opt_mask].iterrows():
                _strat = str(_r.get("Strategy") or "").upper()
                _cp = str(_r.get("Call/Put") or "").upper()
                _qty = float(_r.get("Quantity") or 0)
                _tk = str(_r.get("Underlying_Ticker") or "")
                if (_strat in ("BUY_WRITE", "COVERED_CALL") or (_cp in ("C", "CALL") and _qty < 0)):
                    _covered_shares[_tk] = _covered_shares.get(_tk, 0) + abs(_qty) * 100

            for idx in df_final.index[_recovery_mask_xp]:
                _tk = str(df_final.at[idx, "Underlying_Ticker"])
                _total = _ticker_shares.get(_tk, 0)
                _covered = _covered_shares.get(_tk, 0)
                _idle = max(0, _total - _covered)
                _idle_lots = int(_idle / 100)
                if _idle_lots > 0:
                    from core.shared.finance_utils import normalize_iv as _niv
                    _iv_raw = float(df_final.at[idx, "IV_30D"] if pd.notna(df_final.at[idx, "IV_30D"]) else 0)
                    _iv_norm = _niv(_iv_raw) or 0.0
                    _spot = float(df_final.at[idx, "UL Last"] if pd.notna(df_final.at[idx, "UL Last"]) else 0)
                    import math
                    # Weekly ATM premium × 4.3 weeks/mo — matches dashboard Recovery Path formula
                    _weekly_est = 0.4 * _iv_norm * _spot / math.sqrt(52) if _iv_norm > 0 and _spot > 0 else 0
                    _cc_mo = _weekly_est * 4.3
                    _addl_total = _cc_mo * _idle  # total $/mo from all idle shares
                    _annotation = (
                        f" | 📊 Cross-position: {_idle_lots} idle lot(s) "
                        f"({int(_idle)} shares) of {_tk} have NO covered calls. "
                        f"Selling CCs on idle shares at IV {_iv_norm:.0%} adds "
                        f"~${_addl_total:,.0f}/mo total (~${_cc_mo:.2f}/sh/mo) "
                        f"— accelerates recovery. Total {_tk}: {int(_total)} shares, "
                        f"{int(_covered)} covered, {int(_idle)} idle."
                    )
                    df_final.at[idx, "Rationale"] = (
                        str(df_final.at[idx, "Rationale"] or "") + _annotation
                    )
            logger.info(
                f"📊 Recovery cross-position enrichment: "
                f"{_recovery_mask_xp.sum()} positions annotated"
            )
    except Exception as _xp_err:
        logger.debug(f"Recovery cross-position enrichment skipped: {_xp_err}")

    # --- Ticker-Level Recovery Reconciler ---
    # Computes two scenarios for recovery-ladder positions:
    #   Baseline: current covered shares only → months_baseline
    #   Accelerated: full coverage (covered + idle) → months_accelerated
    # Ticker-centric: finds recovery tickers, looks up STOCK row for cost basis,
    # then writes columns to ALL rows for that ticker (OPTION + STOCK).
    # Guardrail: only recommends covering idle if IV > HV * 0.9 and liquidity OK.
    try:
        import math as _math_rec
        _rec_state_col = df_final.get("Doctrine_State", pd.Series("", index=df_final.index)).fillna("")
        _rec_tickers = set(
            df_final.loc[_rec_state_col == "RECOVERY_LADDER", "Underlying_Ticker"]
            .dropna().unique()
        )
        _rec_count = 0
        if _rec_tickers:
            _stock_mask_rec = df_final.get("AssetType", pd.Series("", index=df_final.index)) == "STOCK"
            for _tk in _rec_tickers:
                # Find STOCK row for cost basis (OPTION rows don't carry it)
                _tk_stock = df_final[_stock_mask_rec & (df_final["Underlying_Ticker"] == _tk)]
                if _tk_stock.empty:
                    continue
                _srow = _tk_stock.iloc[0]
                _spot_r = float(_srow.get("UL Last") or 0) if pd.notna(_srow.get("UL Last")) else 0
                _net_cost_r = float(_srow.get("Net_Cost_Basis_Per_Share") or 0) if pd.notna(_srow.get("Net_Cost_Basis_Per_Share")) else 0
                if _spot_r <= 0 or _net_cost_r <= 0:
                    continue

                _gap_ps = max(0.0, _net_cost_r - _spot_r)
                if _gap_ps <= 0:
                    continue

                _total_sh = _ticker_shares.get(_tk, 0)
                _covered_sh = _covered_shares.get(_tk, 0)
                _idle_sh = max(0, _total_sh - _covered_sh)

                # IV / HV: prefer from OPTION row (has IV_30D), fall back to STOCK row
                _tk_opt = df_final[
                    (~_stock_mask_rec) & (df_final["Underlying_Ticker"] == _tk)
                    & (_rec_state_col == "RECOVERY_LADDER")
                ]
                _iv_src = _tk_opt.iloc[0] if not _tk_opt.empty else _srow
                from core.shared.finance_utils import normalize_iv as _niv
                _iv_r = float(_iv_src.get("IV_30D") or 0) if pd.notna(_iv_src.get("IV_30D")) else 0
                _iv_dec = _niv(_iv_r) or 0.0
                _hv_r = float(_srow.get("HV_20D") or 0) if pd.notna(_srow.get("HV_20D")) else 0
                _hv_dec = _niv(_hv_r) or 0.0

                # Per-share monthly CC income: weekly ATM × 4.3 (matches dashboard Recovery Path)
                _weekly_est_r = 0.4 * _iv_dec * _spot_r / _math_rec.sqrt(52) if _iv_dec > 0 else 0
                _cc_ps_mo = _weekly_est_r * 4.3

                # Margin bleed per share per month (10.375% annual)
                _margin_ps_mo = _net_cost_r * (0.10375 / 12.0)
                _net_ps_mo = max(0, _cc_ps_mo - _margin_ps_mo)

                # Scenario A: baseline (covered shares only)
                _income_baseline = _net_ps_mo * _covered_sh
                _total_gap = _gap_ps * _total_sh
                _months_baseline = (_total_gap / _income_baseline) if _income_baseline > 0 else 999

                # Scenario B: full coverage (covered + idle)
                _income_full = _net_ps_mo * _total_sh
                _months_full = (_total_gap / _income_full) if _income_full > 0 else 999

                # Guardrails for recommending idle coverage
                _iv_hv_ok = _iv_dec >= _hv_dec * 0.9 if _hv_dec > 0 else _iv_dec > 0
                _cc_status_col = df_final.get("CC_Proposal_Status", pd.Series("", index=df_final.index))
                _has_favorable_cc = False
                _ticker_mask_cc = df_final["Underlying_Ticker"] == _tk
                if _ticker_mask_cc.any():
                    _statuses = _cc_status_col[_ticker_mask_cc].fillna("").str.upper()
                    _has_favorable_cc = (_statuses == "FAVORABLE").any()

                _recommend_cover = (
                    _idle_sh >= 100
                    and _iv_hv_ok
                    and _months_full < _months_baseline * 0.85
                )

                _accel_pct = (1 - _months_full / _months_baseline) * 100 if _months_baseline > 0 and _months_baseline < 999 else 0

                # Write to ALL rows for this ticker (OPTION + STOCK) so dashboard
                # can find the columns regardless of which row it reads from.
                for _ridx in df_final.index[_ticker_mask_cc]:
                    df_final.at[_ridx, "Recovery_Total_Shares"] = int(_total_sh)
                    df_final.at[_ridx, "Recovery_Covered_Shares"] = int(_covered_sh)
                    df_final.at[_ridx, "Recovery_Idle_Shares"] = int(_idle_sh)
                    df_final.at[_ridx, "Recovery_Gap_Per_Share"] = round(_gap_ps, 2)
                    df_final.at[_ridx, "Recovery_Income_Baseline_Mo"] = round(_income_baseline, 2)
                    df_final.at[_ridx, "Recovery_Income_Full_Mo"] = round(_income_full, 2)
                    df_final.at[_ridx, "Recovery_Months_Baseline"] = round(min(_months_baseline, 999), 1)
                    df_final.at[_ridx, "Recovery_Months_Full"] = round(min(_months_full, 999), 1)
                    df_final.at[_ridx, "Recovery_Acceleration_Pct"] = round(_accel_pct, 1)
                    df_final.at[_ridx, "Recovery_Cover_Idle_Recommended"] = _recommend_cover
                    df_final.at[_ridx, "Recovery_IV_HV_OK"] = _iv_hv_ok
                    df_final.at[_ridx, "Recovery_CC_Favorable"] = _has_favorable_cc
                _rec_count += 1

            logger.info(
                f"📊 Ticker Recovery Reconciler: {_rec_count} tickers evaluated, "
                f"two-scenario timelines computed"
            )
    except Exception as _rec_err:
        logger.debug(f"Ticker Recovery Reconciler skipped: {_rec_err}")

    # --- MC Stock Recovery Comparison ---
    # For recovery tickers with idle shares, run MC to compare EV of current
    # coverage vs full coverage. Ticker-centric: reads reconciler columns
    # (already propagated to all rows), gets strike from OPTION row, cost from
    # STOCK row. Writes MC_Recovery_* to all rows for the ticker.
    try:
        from core.management.mc_management import mc_stock_recovery_comparison
        _mc_rec_count = 0
        # Iterate over tickers that have idle shares (reconciler already set these)
        _idle_col = df_final.get("Recovery_Idle_Shares", pd.Series(0, index=df_final.index)).fillna(0).astype(float)
        _mc_tickers = set(
            df_final.loc[_idle_col >= 100, "Underlying_Ticker"].dropna().unique()
        )
        if _mc_tickers:
            _mc_rng = __import__("numpy").random.default_rng(42)
            _stock_mask_mc = df_final.get("AssetType", pd.Series("", index=df_final.index)) == "STOCK"
            for _tk_mc in _mc_tickers:
                _tk_mask_mc = df_final["Underlying_Ticker"] == _tk_mc
                # Cost basis from STOCK row
                _tk_stock_mc = df_final[_stock_mask_mc & _tk_mask_mc]
                if _tk_stock_mc.empty:
                    continue
                _srow_mc = _tk_stock_mc.iloc[0]
                _spot_mc = float(_srow_mc.get("UL Last") or 0) if pd.notna(_srow_mc.get("UL Last")) else 0
                _cost_mc = float(_srow_mc.get("Net_Cost_Basis_Per_Share") or 0) if pd.notna(_srow_mc.get("Net_Cost_Basis_Per_Share")) else 0
                # Strike from OPTION row
                _tk_opt_mc = df_final[(~_stock_mask_mc) & _tk_mask_mc]
                _strike_mc = 0.0
                if not _tk_opt_mc.empty:
                    for _scol in ("Short_Call_Strike", "Strike"):
                        if _scol in df_final.columns:
                            _sv = _tk_opt_mc.iloc[0].get(_scol)
                            if pd.notna(_sv) and float(_sv or 0) > 0:
                                _strike_mc = float(_sv)
                                break
                # Reconciler columns (same on all rows for this ticker)
                _any_idx = df_final.index[_tk_mask_mc][0]
                _cov_mc = int(df_final.at[_any_idx, "Recovery_Covered_Shares"])
                _tot_mc = int(df_final.at[_any_idx, "Recovery_Total_Shares"])
                _prem_mc = float(df_final.at[_any_idx, "Recovery_Income_Full_Mo"]) / max(_tot_mc, 1)
                # HV: prefer STOCK row HV_20D, fall back to OPTION row IV_30D
                from core.shared.finance_utils import normalize_iv as _niv
                _hv_mc = float(_srow_mc.get("HV_20D") or 0) if pd.notna(_srow_mc.get("HV_20D")) else 0
                _hv_mc = _niv(_hv_mc) or 0.0
                if _hv_mc <= 0 and not _tk_opt_mc.empty:
                    _iv_mc = float(_tk_opt_mc.iloc[0].get("IV_30D") or 0) if pd.notna(_tk_opt_mc.iloc[0].get("IV_30D")) else 0
                    _hv_mc = _niv(_iv_mc) or 0.0

                if _spot_mc > 0 and _cost_mc > 0 and _strike_mc > 0:
                    _mc_result = mc_stock_recovery_comparison(
                        spot=_spot_mc, cost_basis=_cost_mc,
                        cc_strike=_strike_mc, cc_premium_ps_mo=_prem_mc,
                        covered_shares=_cov_mc, total_shares=_tot_mc,
                        hv=_hv_mc, rng=_mc_rng,
                    )
                    # Write to ALL rows for this ticker
                    _mc_recommend = _mc_result.get("MC_Recovery_Recommend_Cover", False)
                    _mc_ev_delta = float(_mc_result.get("MC_Recovery_EV_Delta", 0) or 0)
                    for _ridx_mc in df_final.index[_tk_mask_mc]:
                        for col, val in _mc_result.items():
                            df_final.at[_ridx_mc, col] = val
                        _det_rec = df_final.at[_ridx_mc, "Recovery_Cover_Idle_Recommended"]
                        if _det_rec and not _mc_recommend:
                            # MC downgrades: deterministic said yes, MC says no
                            df_final.at[_ridx_mc, "Recovery_Cover_Idle_Recommended"] = False
                        elif not _det_rec and _mc_recommend and _mc_ev_delta > 0:
                            # MC upgrades: deterministic guardrail blocked, but MC
                            # (which models volatility via GBM) finds positive EV.
                            # Override the simple IV-HV heuristic.
                            df_final.at[_ridx_mc, "Recovery_Cover_Idle_Recommended"] = True
                    _mc_rec_count += 1
            logger.info(f"🎲 MC Stock Recovery: {_mc_rec_count} tickers evaluated")
    except Exception as _mc_rec_err:
        logger.debug(f"MC Stock Recovery skipped: {_mc_rec_err}")

    # --- Recovery propagation: STOCK_ONLY inherits recovery from same-ticker positions ---
    # When a ticker has a BW/CC/CSP in RECOVERY_LADDER, idle STOCK_ONLY shares are
    # part of that recovery.  The stock_only doctrine evaluates in isolation and fires
    # EXIT on BROKEN equity / deep loss, but the CC opportunity engine (Section 3.2)
    # has already evaluated these shares and set CC_Proposal_* / CC_Recovery_Mode.
    # Use that verdict — FAVORABLE → sell CCs, UNFAVORABLE → hold idle with watch signal,
    # STRUCTURAL_DAMAGE (non-ladder) → honour the system's EXIT (CC engine says stop).
    # Jabbour Ch.4: "recovery is a portfolio-level commitment — but the ladder decides
    # whether idle shares should be utilized or held idle."
    try:
        _recov_state_col = df_final.get("Doctrine_State", pd.Series("", index=df_final.index))
        _recovery_tickers = set(
            df_final.loc[_recov_state_col.fillna("") == "RECOVERY_LADDER", "Underlying_Ticker"]
            .dropna().unique()
        )
        if _recovery_tickers:
            _strat_col = df_final.get("Strategy", pd.Series("", index=df_final.index)).fillna("").str.upper()
            _stock_only_mask = _strat_col == "STOCK_ONLY"
            _exit_mask = df_final.get("Action", pd.Series("", index=df_final.index)).fillna("").str.upper() == "EXIT"
            _ticker_match = df_final["Underlying_Ticker"].isin(_recovery_tickers)
            _propagate_mask = _stock_only_mask & _exit_mask & _ticker_match

            if _propagate_mask.any():
                _rp_propagated = 0
                _rp_honoured = 0
                for idx in df_final.index[_propagate_mask]:
                    _tk = str(df_final.at[idx, "Underlying_Ticker"])
                    _qty = abs(float(df_final.at[idx, "Quantity"] or 0))
                    _lots = int(_qty / 100)

                    # Read CC opportunity engine verdict (set in Section 3.2)
                    _cc_status = str(df_final.at[idx, "CC_Proposal_Status"] if "CC_Proposal_Status" in df_final.columns and pd.notna(df_final.at[idx, "CC_Proposal_Status"]) else "")
                    _cc_verdict = str(df_final.at[idx, "CC_Proposal_Verdict"] if "CC_Proposal_Verdict" in df_final.columns and pd.notna(df_final.at[idx, "CC_Proposal_Verdict"]) else "")
                    _cc_rec_mode = str(df_final.at[idx, "CC_Recovery_Mode"] if "CC_Recovery_Mode" in df_final.columns and pd.notna(df_final.at[idx, "CC_Recovery_Mode"]) else "")
                    _cc_watch = str(df_final.at[idx, "CC_Watch_Signal"] if "CC_Watch_Signal" in df_final.columns and pd.notna(df_final.at[idx, "CC_Watch_Signal"]) else "")
                    _cc_unfav_reason = str(df_final.at[idx, "CC_Unfavorable_Reason"] if "CC_Unfavorable_Reason" in df_final.columns and pd.notna(df_final.at[idx, "CC_Unfavorable_Reason"]) else "")
                    _cc_monthly = float(df_final.at[idx, "CC_Ladder_Monthly_Est"] if "CC_Ladder_Monthly_Est" in df_final.columns and pd.notna(df_final.at[idx, "CC_Ladder_Monthly_Est"]) else 0)
                    _cc_months = float(df_final.at[idx, "CC_Ladder_Recovery_Months"] if "CC_Ladder_Recovery_Months" in df_final.columns and pd.notna(df_final.at[idx, "CC_Ladder_Recovery_Months"]) else 0)

                    # STRUCTURAL_DAMAGE + not ladder-eligible: the system says EXIT
                    if _cc_rec_mode == "STRUCTURAL_DAMAGE" and _cc_status == "UNFAVORABLE":
                        _rp_honoured += 1
                        logger.info(
                            f"📊 Recovery propagation: {_tk} STOCK_ONLY — STRUCTURAL_DAMAGE, "
                            f"CC engine says UNFAVORABLE. Honouring EXIT (system says stop)."
                        )
                        continue  # leave EXIT intact

                    # All other cases: propagate into recovery
                    df_final.at[idx, "Action"] = "HOLD"
                    df_final.at[idx, "Urgency"] = "MEDIUM"
                    df_final.at[idx, "Doctrine_State"] = "RECOVERY_LADDER"
                    df_final.at[idx, "Resolution_Method"] = "RECOVERY_PROPAGATION"
                    _rp_propagated += 1

                    if _cc_status == "FAVORABLE":
                        # CC engine says sell CCs — surface full ladder allocation
                        _ldr = {}  # ladder details
                        for _lk in ("CC_Ladder_Total_Lots", "CC_Ladder_Covered_Lots",
                                    "CC_Ladder_Tier_A_Lots", "CC_Ladder_Tier_B_Lots",
                                    "CC_Ladder_Tier_C_Lots", "CC_Ladder_Income_Gap_Ratio",
                                    "CC_Recovery_Gap", "CC_Recovery_Monthly_Est",
                                    "CC_Best_Ann_Yield", "CC_IV_Rank"):
                            if _lk in df_final.columns and pd.notna(df_final.at[idx, _lk]):
                                _ldr[_lk] = float(df_final.at[idx, _lk])
                        _cc_dte_bucket = str(df_final.at[idx, "CC_Best_DTE_Bucket"] if "CC_Best_DTE_Bucket" in df_final.columns and pd.notna(df_final.at[idx, "CC_Best_DTE_Bucket"]) else "")
                        _cc_partial = str(df_final.at[idx, "CC_Partial_Coverage_Note"] if "CC_Partial_Coverage_Note" in df_final.columns and pd.notna(df_final.at[idx, "CC_Partial_Coverage_Note"]) else "")

                        # Build ladder allocation summary
                        _total_l = int(_ldr.get("CC_Ladder_Total_Lots", _lots))
                        _cover_l = int(_ldr.get("CC_Ladder_Covered_Lots", 0))
                        _idle_l = max(0, _total_l - _cover_l)
                        _tier_a = int(_ldr.get("CC_Ladder_Tier_A_Lots", 0))
                        _tier_b = int(_ldr.get("CC_Ladder_Tier_B_Lots", 0))
                        _gap = _ldr.get("CC_Recovery_Gap", 0)
                        _ann_yield = _ldr.get("CC_Best_Ann_Yield", 0)
                        _iv_rank = _ldr.get("CC_IV_Rank", 0)

                        _alloc_parts = []
                        if _cover_l > 0 and _total_l > 0:
                            _alloc_parts.append(
                                f"Cover {_cover_l} of {_total_l} lots "
                                f"({_cover_l * 100 / _total_l:.0f}%), keep {_idle_l} idle (rally protection)"
                            )
                        if _tier_a > 0 or _tier_b > 0:
                            _tier_parts = []
                            if _tier_a > 0:
                                _tier_parts.append(f"Tier A: {_tier_a} lots (weekly/biweekly, δ0.25-0.30)")
                            if _tier_b > 0:
                                _tier_parts.append(f"Tier B: {_tier_b} lots (monthly, δ0.15-0.25)")
                            _alloc_parts.append(" | ".join(_tier_parts))
                        _alloc_str = ". ".join(_alloc_parts) + "." if _alloc_parts else ""

                        _metrics_parts = []
                        if _cc_monthly > 0:
                            _metrics_parts.append(f"~${_cc_monthly:,.0f}/mo")
                        if _cc_months > 0:
                            _metrics_parts.append(f"~{_cc_months:.0f}mo to breakeven")
                        if _gap > 0:
                            _metrics_parts.append(f"gap ${_gap:,.2f}/sh")
                        if _ann_yield > 0:
                            _metrics_parts.append(f"ann. yield {_ann_yield:.0%}")
                        _metrics_str = " | ".join(_metrics_parts)

                        _mode_str = f" Mode: {_cc_rec_mode}." if _cc_rec_mode else ""

                        df_final.at[idx, "Rationale"] = (
                            f"📊 Recovery propagation — SELL CCs: {_tk} RECOVERY_LADDER active.{_mode_str} "
                            f"{_alloc_str} "
                            f"{_metrics_str}. "
                            f"CC engine: {_cc_verdict}. "
                            f"(Jabbour Ch.4: utilize idle shares for basis reduction; "
                            f"McMillan Ch.3: CC converts holding cost into income)"
                        )
                        df_final.at[idx, "Doctrine_Source"] = (
                            "Jabbour Ch.4 + McMillan Ch.3: Recovery Propagation — SELL CCs"
                        )
                    elif _cc_status == "UNFAVORABLE":
                        # CC engine says hold idle — surface exactly why and what to watch
                        _watch_str = f" 👁️ Watch for: {_cc_watch}." if _cc_watch else ""
                        _mode_str = f" Mode: {_cc_rec_mode}." if _cc_rec_mode else ""
                        _iv_rank_uf = float(df_final.at[idx, "CC_IV_Rank"] if "CC_IV_Rank" in df_final.columns and pd.notna(df_final.at[idx, "CC_IV_Rank"]) else 0)
                        _regime_uf = str(df_final.at[idx, "CC_Regime"] if "CC_Regime" in df_final.columns and pd.notna(df_final.at[idx, "CC_Regime"]) else "")
                        _context_parts = []
                        if _iv_rank_uf > 0:
                            _context_parts.append(f"IV_Rank {_iv_rank_uf:.0f}%")
                        if _regime_uf:
                            _context_parts.append(f"regime {_regime_uf}")
                        _context_str = f" ({', '.join(_context_parts)})." if _context_parts else ""
                        df_final.at[idx, "Rationale"] = (
                            f"📊 Recovery propagation — HOLD IDLE: {_tk} RECOVERY_LADDER active.{_mode_str} "
                            f"{int(_qty):,} shares ({_lots} lot(s)) held as inventory — CC not viable now. "
                            f"Reason: {_cc_unfav_reason or _cc_verdict}.{_context_str}{_watch_str} "
                            f"(Jabbour Ch.4: idle shares are the next premium cycle's inventory — "
                            f"wait for conditions to improve before writing)"
                        )
                        df_final.at[idx, "Doctrine_Source"] = (
                            "Jabbour Ch.4: Recovery Propagation — HOLD IDLE (CC conditions unfavorable)"
                        )
                    else:
                        # No CC engine data (SCAN_MISS, ERROR, or columns absent) — basic fallback
                        import math as _math_rp
                        _spot = float(df_final.at[idx, "UL Last"]) if pd.notna(df_final.at[idx, "UL Last"]) else 0
                        from core.shared.finance_utils import normalize_iv as _niv
                        _iv_raw = float(df_final.at[idx, "IV_30D"]) if pd.notna(df_final.at[idx, "IV_30D"]) else 0
                        _iv_norm = _niv(_iv_raw) or 0.0
                        _cc_mo = _spot * _iv_norm * _math_rp.sqrt(1.0 / 12.0) * 0.4 if _iv_norm > 0 and _spot > 0 else 0
                        _monthly_income = _cc_mo * _lots
                        df_final.at[idx, "Rationale"] = (
                            f"📊 Recovery propagation: {_tk} has active RECOVERY_LADDER. "
                            f"{int(_qty):,} shares ({_lots} lot(s)) are CC inventory. "
                            f"CC engine data unavailable — est. ~${_monthly_income:,.0f}/mo at IV {_iv_norm:.0%}. "
                            f"(Jabbour Ch.4: idle shares are the next premium cycle's inventory)"
                        )
                        df_final.at[idx, "Doctrine_Source"] = (
                            "Jabbour Ch.4 + McMillan Ch.3: Recovery Propagation — HOLD for CC"
                        )

                logger.info(
                    f"📊 Recovery propagation: {_rp_propagated} STOCK_ONLY EXIT→HOLD, "
                    f"{_rp_honoured} EXIT honoured (system says stop) — "
                    f"tickers: {_recovery_tickers}"
                )
    except Exception as _rp_err:
        logger.debug(f"Recovery propagation skipped: {_rp_err}")

    # --- Execution Readiness (Layer 2 — backend component) ---
    # Called here (not inside generate_recommendations) so it sees the fully-resolved
    # Action column after drift overrides, schema enforcement, and epistemic gates.
    df_final = _apply_execution_readiness(df_final)
    er_counts = df_final['Execution_Readiness'].value_counts().to_dict() if 'Execution_Readiness' in df_final.columns else {}
    logger.info(f"⚡ Execution Readiness — {er_counts}")

    # --- Lifecycle Phase: PENDING_ROLL annotation ---
    # Post-doctrine: positions with ROLL action transition to PENDING_ROLL phase.
    # Tracked in management_recommendations time-series for phase transition auditing.
    if 'Action' in df_final.columns and 'Lifecycle_Phase' in df_final.columns:
        _roll_mask = df_final['Action'].isin(['ROLL', 'ROLL_WAIT'])
        if _roll_mask.any():
            df_final.loc[_roll_mask, 'Lifecycle_Phase'] = 'PENDING_ROLL'
            logger.info(f"[Lifecycle] {_roll_mask.sum()} positions marked PENDING_ROLL.")

    # --- Management Monte Carlo ---
    # Runs after Execution_Readiness so MC can see the final readiness state.
    # Three functions: roll wait-cost (STAGE_AND_RECHECK rows), exit-vs-hold
    # (HOLD rows), assignment risk (all income positions).
    # Non-blocking — any failure leaves MC_* columns as NaN/empty.
    try:
        df_final = run_management_mc(df_final)

        # MC verdict reconciliation: if MC says ACT_NOW on a STAGE_AND_RECHECK row
        # (high breach risk in wait window), escalate Execution_Readiness to EXECUTE_NOW.
        if "MC_Wait_Verdict" in df_final.columns and "Execution_Readiness" in df_final.columns:
            _stage_mask = df_final["Execution_Readiness"] == "STAGE_AND_RECHECK"
            _act_mask   = df_final["MC_Wait_Verdict"].fillna("") == "ACT_NOW"
            _escalate   = _stage_mask & _act_mask
            if _escalate.any():
                df_final.loc[_escalate, "Execution_Readiness"] = "EXECUTE_NOW"
                df_final.loc[_escalate, "Execution_Readiness_Reason"] = (
                    df_final.loc[_escalate, "Execution_Readiness_Reason"].fillna("") + " | "
                    + df_final.loc[_escalate, "MC_Wait_Note"].fillna("")
                )
                logger.info(f"⚡ MC escalated {_escalate.sum()} STAGE_AND_RECHECK → EXECUTE_NOW")

        # Assignment urgency escalation: if MC_Assign_Urgency is CRITICAL/HIGH
        # and doctrine Action is HOLD, upgrade to EXECUTE_NOW.
        #
        # Wheel override: if Wheel_Ready=True, assignment HIGH/CRITICAL means assignment
        # is IMMINENT and INTENTIONAL — the action is NOT to panic-exit but to prepare
        # to receive stock. Escalating these to EXECUTE_NOW would create false urgency.
        # Only escalate when Wheel_Ready is False (or absent).
        if "MC_Assign_Urgency" in df_final.columns:
            _hold_mask    = df_final["Action"].isin(["HOLD", "HOLD_FOR_REVERSION"])
            _crit_assign  = df_final["MC_Assign_Urgency"].isin(["CRITICAL", "HIGH"])
            # Suppress escalation for wheel-ready rows
            _wheel_col    = "Wheel_Ready" if "Wheel_Ready" in df_final.columns else None
            if _wheel_col:
                _not_wheel = ~df_final[_wheel_col].fillna(False).astype(bool)
            else:
                _not_wheel = pd.Series(True, index=df_final.index)
            _assign_esc   = _hold_mask & _crit_assign & _not_wheel
            if _assign_esc.any():
                df_final.loc[_assign_esc, "Execution_Readiness"] = "EXECUTE_NOW"
                df_final.loc[_assign_esc, "Execution_Readiness_Reason"] = (
                    "MC assignment risk: " + df_final.loc[_assign_esc, "MC_Assign_Note"].fillna("")
                )
                logger.info(f"⚡ MC escalated {_assign_esc.sum()} HOLD rows → EXECUTE_NOW (assignment risk)")
            # Wheel-ready rows with CRITICAL assignment: surface as STAGE_AND_RECHECK (prepare, don't panic)
            if _wheel_col:
                _wheel_imminent = _hold_mask & _crit_assign & df_final[_wheel_col].fillna(False).astype(bool)
                if _wheel_imminent.any():
                    df_final.loc[_wheel_imminent, "Execution_Readiness"] = "STAGE_AND_RECHECK"
                    df_final.loc[_wheel_imminent, "Execution_Readiness_Reason"] = (
                        "Wheel assignment imminent — prepare to accept stock and sell covered call. "
                        + df_final.loc[_wheel_imminent, "Wheel_Note"].fillna("")
                    )
                    logger.info(f"🎡 Wheel: {_wheel_imminent.sum()} rows marked STAGE_AND_RECHECK (assignment imminent, wheel ready)")

        # E2 audit fix: MC verdict → doctrine Urgency escalation (one-way only — never demote).
        # The MC engine produces MC_Wait_Verdict and MC_Hold_Verdict from the forward simulation.
        # When MC confirms urgency, we escalate the doctrine Urgency field so the dashboard
        # surface prioritizes these rows correctly. This is ONE-WAY: MC can promote urgency
        # (LOW→MEDIUM→HIGH) but never demote it (HIGH stays HIGH even if MC says HOLD).
        #
        # Rules (applied in order, one-way escalation):
        #   MC_Wait_Verdict == 'ACT_NOW' AND Action in ('ROLL_WAIT', 'ROLL')
        #     → Urgency escalated to HIGH (MC confirms timing urgency)
        #   MC_Hold_Verdict == 'EXIT_NOW' AND Action == 'HOLD'
        #     → Urgency escalated to HIGH (MC confirms holding is no longer viable)
        #   MC_Wait_Verdict == 'CAUTION' AND Urgency == 'LOW'
        #     → Urgency promoted to MEDIUM (early warning)
        #
        # Natenberg Ch.19: "Model-confirmed urgency should override discretionary hold decisions
        # when the forward expectancy is statistically negative." McMillan Ch.7: "Act on
        # quantitative signals — intuition doesn't override a 75%+ breach probability."
        _urgency_order = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}

        def _promote_urgency(current: str, target: str) -> str:
            """One-way promotion: never demote."""
            cur_rank = _urgency_order.get(str(current).upper(), 0)
            tgt_rank = _urgency_order.get(str(target).upper(), 0)
            return target.upper() if tgt_rank > cur_rank else current

        if "MC_Wait_Verdict" in df_final.columns and "Action" in df_final.columns:
            # Rule 1: MC_Wait_Verdict == ACT_NOW on ROLL/ROLL_WAIT → HIGH urgency
            # GUARD: Only apply to SHORT options (income/short-vol strategies).
            # For LONG_CALL / LONG_PUT / LEAPS, MC ACT_NOW means "option at risk of going OTM"
            # (a different and less urgent signal) — never escalate on that basis.
            _long_strats = {"LONG_CALL", "LONG_PUT", "LEAPS_CALL", "LEAPS_PUT",
                            "BUY_CALL", "BUY_PUT", "LEAP_CALL", "LEAP_PUT"}
            _e2_is_short = ~df_final.get("Entry_Structure", pd.Series(dtype=str)).fillna("").str.upper().isin(_long_strats)
            _e2_roll_mask  = df_final["Action"].isin(["ROLL", "ROLL_WAIT"])
            _e2_act_now    = df_final["MC_Wait_Verdict"].fillna("") == "ACT_NOW"
            _e2_rule1      = _e2_roll_mask & _e2_act_now & _e2_is_short
            if _e2_rule1.any():
                df_final.loc[_e2_rule1, "Urgency"] = df_final.loc[_e2_rule1, "Urgency"].apply(
                    lambda u: _promote_urgency(u, "HIGH")
                )
                # Use MC's own reason instead of hardcoded "breach probability high"
                _e2_mc_reason = df_final.loc[_e2_rule1, "MC_Wait_Reason"].fillna(
                    "forward breach probability elevated"
                )
                df_final.loc[_e2_rule1, "Rationale"] = (
                    df_final.loc[_e2_rule1, "Rationale"].fillna("") +
                    " | ⚡ MC confirms: ACT_NOW — " + _e2_mc_reason + "."
                )
                df_final.loc[_e2_rule1, "Doctrine_Source"] = (
                    df_final.loc[_e2_rule1, "Doctrine_Source"].fillna("") +
                    " + MC_Wait: ACT_NOW (Natenberg Ch.19)"
                )
                logger.info(f"⚡ E2: MC ACT_NOW escalated urgency on {_e2_rule1.sum()} ROLL rows → HIGH")

            # Rule 2: MC_Wait_Verdict == CAUTION on LOW urgency → MEDIUM
            _e2_caution    = df_final["MC_Wait_Verdict"].fillna("") == "CAUTION"
            _e2_low        = df_final["Urgency"].fillna("").str.upper() == "LOW"
            _e2_rule2      = _e2_caution & _e2_low
            if _e2_rule2.any():
                df_final.loc[_e2_rule2, "Urgency"] = "MEDIUM"
                df_final.loc[_e2_rule2, "Doctrine_Source"] = (
                    df_final.loc[_e2_rule2, "Doctrine_Source"].fillna("") +
                    " + MC_Wait: CAUTION (McMillan Ch.7)"
                )
                logger.info(f"⚡ E2: MC CAUTION promoted {_e2_rule2.sum()} LOW→MEDIUM")

        if "MC_Hold_Verdict" in df_final.columns:
            # Rule 3: MC_Hold_Verdict == EXIT_NOW on HOLD → override to EXIT
            # Natenberg Ch.19: when probabilistic analysis says holding is no
            # longer viable, the engine must exit — not merely raise urgency.
            #
            # Bug 39 guard: LEAPS (DTE > 180) with INTACT/RECOVERING thesis
            # are exempt from the hard override.  MC's risk-neutral GBM uses
            # zero drift, which penalises long calls on equities over long
            # horizons — a 318-day LEAPS with INTACT thesis, cheap vol, and
            # minimal theta bleed should NOT be forced to EXIT by a borderline
            # p_recovery (e.g., 32% vs 35% threshold).  For these positions
            # the MC result is downgraded to a WARNING (rationale note only).
            _e2_hold_mask  = df_final["Action"] == "HOLD"
            _e2_exit_now   = df_final["MC_Hold_Verdict"].fillna("") == "EXIT_NOW"

            # LEAPS with intact thesis: suppress hard override
            if "DTE" in df_final.columns:
                _leaps_mask = pd.to_numeric(df_final["DTE"], errors="coerce").fillna(0) > 180
            else:
                _leaps_mask = pd.Series(False, index=df_final.index)
            if "Thesis_State" in df_final.columns:
                _thesis_ok = df_final["Thesis_State"].fillna("").isin(["INTACT", "RECOVERING"])
            else:
                _thesis_ok = pd.Series(False, index=df_final.index)
            _leaps_intact = _leaps_mask & _thesis_ok

            # New-position grace period guard: positions opened today
            # (Days_In_Trade < 2) were just approved by the scan engine.
            # MC's single-day snapshot is unreliable for day-0 positions —
            # suppress to warning.  McMillan Ch.4: give new positions time.
            if "Days_In_Trade" in df_final.columns:
                _grace_mask = pd.to_numeric(
                    df_final["Days_In_Trade"], errors="coerce"
                ).fillna(0) < 2
            else:
                _grace_mask = pd.Series(False, index=df_final.index)

            # Recovery ladder guard: doctrine has committed to recovery via
            # premium collection — MC's GBM (zero-drift, no income modelling)
            # cannot value the basis-reduction path.  Suppress to warning.
            # Jabbour Ch.4: once recovery is active, exit only on thesis BROKEN.
            _recovery_mask = pd.Series(False, index=df_final.index)
            for _rc_col, _rc_vals in [
                ("Doctrine_State", ["RECOVERY_LADDER"]),
                ("Resolution_Method", ["RECOVERY_LADDER", "RECOVERY_PROPAGATION"]),
                ("Winning_Gate", ["hard_stop_recovery_ladder"]),
            ]:
                if _rc_col in df_final.columns:
                    _recovery_mask = _recovery_mask | df_final[_rc_col].fillna("").isin(_rc_vals)

            # Macro catalyst guard: doctrine has cleared EXIT via extended macro
            # window (long premium + imminent FOMC/CPI/NFP vol catalyst + thesis
            # intact). MC's risk-neutral GBM cannot price the asymmetric vol
            # expansion that macro events create for long premium. Suppress to
            # warning. Bennett: macro events justify holding event-driven premium.
            if "Macro_Catalyst_Protected" in df_final.columns:
                _macro_catalyst_mask = df_final["Macro_Catalyst_Protected"].fillna(False).astype(bool)
            else:
                _macro_catalyst_mask = pd.Series(False, index=df_final.index)

            # DATA_BLOCKED guard: positions blocked by data integrity gate
            # had stale/missing prices — MC inputs are equally unreliable.
            # Never override governance HOLD with EXIT based on stale data.
            if "Pre_Doctrine_Flag" in df_final.columns:
                _data_blocked_mask = df_final["Pre_Doctrine_Flag"].fillna("").isin(
                    ["DATA_BLOCKED", "PRICE_STALE", "GREEKS_MISSING"]
                )
            else:
                _data_blocked_mask = pd.Series(False, index=df_final.index)

            # Income structure guard: BW/CC with far-OTM near-expiry short call.
            # MC's zero-drift GBM doesn't model premium income — it can't value
            # the basis-reduction from theta collection. Doctrine's EV comparator
            # already evaluated EXIT vs HOLD with income context and chose HOLD.
            # Suppress to warning. (McMillan Ch.3: near-expiry far-OTM = pure income)
            # Uses shared _income_far_otm_expiring computed at section 3.0a.

            # Full override: not LEAPS-intact AND not in recovery mode AND not grace period AND not macro catalyst AND not data-blocked AND not income-far-OTM
            _e2_exempt = _leaps_intact | _recovery_mask | _grace_mask | _macro_catalyst_mask | _data_blocked_mask | _income_far_otm_expiring
            _e2_rule3      = _e2_hold_mask & _e2_exit_now & ~_e2_exempt
            # LEAPS warning only (no action change)
            _e2_leaps_warn = _e2_hold_mask & _e2_exit_now & _leaps_intact & ~_recovery_mask & ~_grace_mask
            # Recovery warning only (no action change)
            _e2_recovery_warn = _e2_hold_mask & _e2_exit_now & _recovery_mask & ~_grace_mask
            # Grace period warning only (no action change)
            _e2_grace_warn = _e2_hold_mask & _e2_exit_now & _grace_mask
            # Macro catalyst warning only (no action change)
            _e2_macro_warn = _e2_hold_mask & _e2_exit_now & _macro_catalyst_mask & ~_leaps_intact & ~_recovery_mask & ~_grace_mask
            # Income far-OTM expiring warning only (no action change)
            _e2_income_warn = _e2_hold_mask & _e2_exit_now & _income_far_otm_expiring & ~_recovery_mask & ~_grace_mask
            # Data-blocked warning only (no action change)
            _e2_data_blocked_warn = _e2_hold_mask & _e2_exit_now & _data_blocked_mask

            if _e2_rule3.any():
                df_final.loc[_e2_rule3, "Action"]  = "EXIT"
                df_final.loc[_e2_rule3, "Urgency"] = df_final.loc[_e2_rule3, "Urgency"].apply(
                    lambda u: _promote_urgency(u, "HIGH")
                )
                df_final.loc[_e2_rule3, "Rationale"] = (
                    df_final.loc[_e2_rule3, "Rationale"].fillna("") +
                    " | ⚡ MC EXIT_NOW override: p_recovery < 0.35 AND EV < 0 — exit, do not hold."
                )
                df_final.loc[_e2_rule3, "Doctrine_Source"] = (
                    df_final.loc[_e2_rule3, "Doctrine_Source"].fillna("") +
                    " + MC_Hold: EXIT_NOW (Natenberg Ch.19)"
                )
                logger.info(f"⚡ E2: MC EXIT_NOW overrode {_e2_rule3.sum()} HOLD rows → EXIT HIGH")

            if _e2_leaps_warn.any():
                df_final.loc[_e2_leaps_warn, "Rationale"] = (
                    df_final.loc[_e2_leaps_warn, "Rationale"].fillna("") +
                    " | ⚠️ MC EXIT_NOW suppressed (LEAPS DTE>180 + thesis intact) — monitor closely."
                )
                df_final.loc[_e2_leaps_warn, "Doctrine_Source"] = (
                    df_final.loc[_e2_leaps_warn, "Doctrine_Source"].fillna("") +
                    " + MC_Hold: EXIT_NOW↓WARNING (LEAPS guard)"
                )
                logger.info(
                    f"⚠️ E2: MC EXIT_NOW suppressed for {_e2_leaps_warn.sum()} LEAPS "
                    f"(thesis intact, DTE>180) — downgraded to warning"
                )

            if _e2_recovery_warn.any():
                df_final.loc[_e2_recovery_warn, "Rationale"] = (
                    df_final.loc[_e2_recovery_warn, "Rationale"].fillna("") +
                    " | ⚠️ MC EXIT_NOW suppressed (recovery ladder active) — "
                    "MC GBM cannot model basis-reduction income path. "
                    "Monitor thesis; exit only on BROKEN."
                )
                df_final.loc[_e2_recovery_warn, "Doctrine_Source"] = (
                    df_final.loc[_e2_recovery_warn, "Doctrine_Source"].fillna("") +
                    " + MC_Hold: EXIT_NOW↓WARNING (recovery guard)"
                )
                logger.info(
                    f"⚠️ E2: MC EXIT_NOW suppressed for {_e2_recovery_warn.sum()} "
                    f"recovery ladder positions — downgraded to warning"
                )

            if _e2_grace_warn.any():
                df_final.loc[_e2_grace_warn, "Rationale"] = (
                    df_final.loc[_e2_grace_warn, "Rationale"].fillna("") +
                    " | ⚠️ MC EXIT_NOW suppressed (new position grace period, Days_In_Trade < 2) — "
                    "scan engine approved this trade today; MC single-day snapshot unreliable. "
                    "Re-evaluate after 1 full trading day."
                )
                df_final.loc[_e2_grace_warn, "Doctrine_Source"] = (
                    df_final.loc[_e2_grace_warn, "Doctrine_Source"].fillna("") +
                    " + MC_Hold: EXIT_NOW↓WARNING (grace period guard)"
                )
                logger.info(
                    f"⚠️ E2: MC EXIT_NOW suppressed for {_e2_grace_warn.sum()} "
                    f"new positions (Days_In_Trade < 2) — downgraded to warning"
                )

            if _e2_macro_warn.any():
                df_final.loc[_e2_macro_warn, "Rationale"] = (
                    df_final.loc[_e2_macro_warn, "Rationale"].fillna("") +
                    " | ⚠️ MC EXIT_NOW suppressed (macro catalyst protection) — "
                    "imminent HIGH-impact event is vol catalyst for long premium. "
                    "MC GBM cannot price asymmetric event-driven vol expansion. "
                    "Re-evaluate after macro event passes."
                )
                df_final.loc[_e2_macro_warn, "Doctrine_Source"] = (
                    df_final.loc[_e2_macro_warn, "Doctrine_Source"].fillna("") +
                    " + MC_Hold: EXIT_NOW↓WARNING (macro catalyst guard)"
                )
                logger.info(
                    f"⚠️ E2: MC EXIT_NOW suppressed for {_e2_macro_warn.sum()} "
                    f"macro-catalyst-protected long premium positions — downgraded to warning"
                )

            if _e2_income_warn.any():
                df_final.loc[_e2_income_warn, "Rationale"] = (
                    df_final.loc[_e2_income_warn, "Rationale"].fillna("") +
                    " | ⚠️ MC EXIT_NOW suppressed (income far-OTM expiring) — "
                    "MC GBM cannot model theta income on near-expiry short call. "
                    "Doctrine EV comparator is authoritative for income positions."
                )
                df_final.loc[_e2_income_warn, "Doctrine_Source"] = (
                    df_final.loc[_e2_income_warn, "Doctrine_Source"].fillna("") +
                    " + MC_Hold: EXIT_NOW↓WARNING (income guard)"
                )
                logger.info(
                    f"⚠️ E2: MC EXIT_NOW suppressed for {_e2_income_warn.sum()} "
                    f"income positions (far-OTM, DTE≤14) — downgraded to warning"
                )

            if _e2_data_blocked_warn.any():
                df_final.loc[_e2_data_blocked_warn, "Rationale"] = (
                    df_final.loc[_e2_data_blocked_warn, "Rationale"].fillna("") +
                    " | ⚠️ MC EXIT_NOW suppressed (DATA_BLOCKED) — "
                    "price data stale/missing; MC inputs equally unreliable. "
                    "HOLD pending data refresh."
                )
                df_final.loc[_e2_data_blocked_warn, "Doctrine_Source"] = (
                    df_final.loc[_e2_data_blocked_warn, "Doctrine_Source"].fillna("") +
                    " + MC_Hold: EXIT_NOW↓WARNING (data integrity guard)"
                )
                logger.info(
                    f"⚠️ E2: MC EXIT_NOW suppressed for {_e2_data_blocked_warn.sum()} "
                    f"DATA_BLOCKED positions — stale data, downgraded to warning"
                )

    except Exception as _mc_mgmt_err:
        logger.warning(f"⚠️ Management MC failed (non-fatal): {_mc_mgmt_err}")

    # --- Portfolio VaR: correlated multi-position stress test (Hull Ch.22) ----
    # Simulates all positions simultaneously using correlated GBM to compute
    # portfolio-level VaR, CVaR, and SPY stress scenarios.
    try:
        _pvar = mc_portfolio_var(df_final)
        if _pvar.get("Portfolio_MC_Note", "").startswith("MC_SKIP"):
            logger.debug(f"Portfolio VaR skipped: {_pvar.get('Portfolio_MC_Note')}")
        else:
            # Store as metadata (single-row result, not per-position)
            for k, v in _pvar.items():
                df_final.attrs[k] = v
            logger.info(
                f"📊 Portfolio VaR(5%)=${_pvar.get('Portfolio_VaR_5pct', 0):+,.0f} | "
                f"CVaR=${_pvar.get('Portfolio_CVaR_5pct', 0):+,.0f} | "
                f"concentration={_pvar.get('Portfolio_Concentration', 0):.2f} | "
                f"SPY-5%=${_pvar.get('Portfolio_Stress_SPY_5', 0):+,.0f}"
            )
            # Persist to JSON sidecar for dashboard consumption (attrs lost in CSV)
            import json as _json_pvar
            _pvar_path = Path(emit_path).parent / "portfolio_var_latest.json"
            _pvar_serializable = {}
            for _pk, _pv in _pvar.items():
                if isinstance(_pv, float) and (np.isnan(_pv) or np.isinf(_pv)):
                    _pvar_serializable[_pk] = None
                else:
                    _pvar_serializable[_pk] = _pv
            _pvar_path.write_text(_json_pvar.dumps(_pvar_serializable, indent=2))
            logger.debug(f"Portfolio VaR sidecar written: {_pvar_path}")
    except Exception as _pvar_err:
        logger.warning(f"⚠️ Portfolio VaR failed (non-fatal): {_pvar_err}")

    # --- MC Optimal Exit Timing: rationale annotation -------------------------
    # Phase 2: mc_optimal_exit now runs inside run_management_mc (shared paths
    # with mc_exit_vs_hold). This block only handles rationale annotation.
    try:
        _oe_cols = ["MC_Optimal_Exit_DTE", "MC_Exit_Peak_EV", "MC_Exit_Terminal_EV",
                    "MC_Exit_Theta_Crossover", "MC_Exit_Note"]
        for col in _oe_cols:
            if col not in df_final.columns:
                df_final[col] = np.nan if col != "MC_Exit_Note" else ""
            else:
                if col != "MC_Exit_Note":
                    df_final[col] = pd.to_numeric(df_final[col], errors="coerce")

        # Annotate rationale with MC peak EV timing (Given Ch.9: time-stops)
        _oe_annotated = 0
        for idx, row in df_final.iterrows():
            _oe_dte = row.get("MC_Optimal_Exit_DTE")
            _oe_peak = row.get("MC_Exit_Peak_EV")
            if pd.notna(_oe_dte) and pd.notna(_oe_peak) and _oe_dte > 0:
                _oe_current_dte = float(row.get("DTE", 0) or 0)
                _oe_days_to_peak = _oe_current_dte - _oe_dte
                if _oe_days_to_peak > 0:
                    _oe_annotation = (
                        f" | MC timing: peak EV (${_oe_peak:,.0f}) at DTE {_oe_dte:.0f} "
                        f"(~{_oe_days_to_peak:.0f}d from now). "
                    )
                    _existing = str(df_final.at[idx, 'Rationale'] or '')
                    df_final.at[idx, 'Rationale'] = _existing + _oe_annotation
                    _oe_annotated += 1
        if _oe_annotated > 0:
            logger.info(f"MC Optimal Exit annotated {_oe_annotated} HOLD positions")
    except Exception as _oe_err:
        logger.warning(f"MC Optimal Exit annotation failed (non-fatal): {_oe_err}")

    # --- MFE-Based Winner Expansion: wave-phase-gated pyramid sizing -----------
    # When a HOLD income position meets MFE/P&L thresholds AND wave phase is
    # BUILDING, compute pyramid add quantity based on frozen base size and tier.
    # Management owns sizing — scan only tags Scale_Up_Candidate=True.
    try:
        from core.management.cycle3.doctrine.thresholds import (
            WINNER_SCALE_UP_MFE, WINNER_SCALE_UP_PNL_MIN,
        )
        from core.management.cycle2.chart_state.state_extractors.wave_phase import (
            compute_pyramid_add_contracts, is_scale_up_eligible,
        )
        _income_structs = {'COVERED_CALL', 'BUY_WRITE', 'SHORT_PUT', 'CSP'}
        if 'Scale_Trigger_Price' not in df_final.columns:
            df_final['Scale_Trigger_Price'] = np.nan
        if 'Scale_Add_Contracts' not in df_final.columns:
            df_final['Scale_Add_Contracts'] = np.nan
        _we_count = 0
        for idx, row in df_final.iterrows():
            _we_action = str(row.get('Action', '') or '').upper()
            if _we_action not in ('HOLD', 'HOLD_FOR_REVERSION'):
                continue
            _we_struct = str(row.get('Entry_Structure', '') or '').upper()
            if _we_struct not in _income_structs:
                continue
            _we_mfe = float(row.get('Trajectory_MFE', 0) or 0)
            _we_pnl = float(row.get('PnL_Pct', 0) or 0)
            if _we_mfe < WINNER_SCALE_UP_MFE or _we_pnl < WINNER_SCALE_UP_PNL_MIN:
                continue
            # Skip if thesis is degraded
            _we_thesis = str(row.get('Thesis_State', '') or '').upper()
            if 'DEGRAD' in _we_thesis or 'BROKEN' in _we_thesis:
                continue
            # Already has a Scale_Trigger_Price (doctrine-driven) — skip to avoid dups
            if pd.notna(row.get('Scale_Trigger_Price')):
                continue

            # Wave phase gate — only BUILDING phase qualifies
            _we_wave = str(row.get('WavePhase_State', '') or '').upper()
            if not is_scale_up_eligible(_we_wave, _we_pnl):
                continue

            _we_spot = float(row.get('UL Last', 0) or row.get('Spot', 0) or 0)
            if _we_spot <= 0:
                continue

            # Regime gate on pyramid depth — Murphy 0.773:
            # "The third or fourth entry should only be attempted in very bullish markets."
            # Tier 0 = first add (always allowed), Tier 1+ = regime-restricted.
            _we_tier = int(float(row.get('Pyramid_Tier', 0) or 0))
            if _we_tier >= 1:
                _we_regime = str(row.get('Market_Regime', '') or '').upper()
                if _we_regime in ('BEARISH', 'CRISIS'):
                    logger.debug(
                        f"[WinnerExpansion] Skipping tier-{_we_tier + 1} scale-up for "
                        f"{row.get('Underlying_Ticker', '?')} — regime {_we_regime} "
                        f"(Murphy: additional entries only in bullish markets)"
                    )
                    continue

            # Pyramid sizing — use frozen base quantity, not current Quantity
            _we_conv = str(row.get('Conviction_Status', '') or '').upper()
            _we_mom = str(row.get('MomentumVelocity_State', '') or '').upper()
            _we_base = abs(int(float(
                row.get('Base_Quantity') or row.get('Entry_Quantity')
                or row.get('Quantity', 1)
            )))

            _add = compute_pyramid_add_contracts(
                base_quantity=_we_base, pyramid_tier=_we_tier,
                wave_phase=_we_wave, pnl_pct=_we_pnl,
                conviction_status=_we_conv, momentum_state=_we_mom,
            )
            if _add > 0:
                df_final.at[idx, 'Scale_Trigger_Price'] = _we_spot
                df_final.at[idx, 'Scale_Add_Contracts'] = _add
                _we_count += 1
        if _we_count > 0:
            logger.info(f"🏄 Winner Expansion: {_we_count} income positions eligible for scale-up (wave=BUILDING, MFE≥{WINNER_SCALE_UP_MFE:.0%}, P&L≥{WINNER_SCALE_UP_PNL_MIN:.0%})")
    except Exception as _we_err:
        logger.debug(f"[WinnerExpansion] Failed (non-fatal): {_we_err}")

    # --- Directional Winner Expansion: Murphy 0.773 pyramiding ----------------
    # Winning directional positions (LONG_CALL, LONG_PUT, LEAPs) can add one
    # layer if MFE ≥ 25%, P&L ≥ 15%, thesis intact, wave BUILDING.
    # Stricter than income: no premium cushion, full downside exposure.
    # Murphy: "Add only to winning positions. Each layer smaller."
    try:
        from core.management.cycle3.doctrine.thresholds import (
            DIRECTIONAL_SCALE_UP_MFE, DIRECTIONAL_SCALE_UP_PNL_MIN,
            DIRECTIONAL_PYRAMID_MAX_TIER, PYRAMID_TIER_0_RATIO,
        )
        _dir_structs = {
            'LONG_CALL', 'LONG_PUT', 'LEAPS_CALL', 'LEAPS_PUT',
            'BUY_CALL', 'BUY_PUT', 'LEAP_CALL', 'LEAP_PUT',
        }
        _dir_count = 0
        for idx, row in df_final.iterrows():
            _dir_action = str(row.get('Action', '') or '').upper()
            if _dir_action not in ('HOLD', 'HOLD_FOR_REVERSION'):
                continue
            _dir_struct = str(row.get('Entry_Structure', '') or '').upper()
            if _dir_struct not in _dir_structs:
                continue
            _dir_mfe = float(row.get('Trajectory_MFE', 0) or 0)
            _dir_pnl = float(row.get('PnL_Pct', 0) or 0)
            if _dir_mfe < DIRECTIONAL_SCALE_UP_MFE or _dir_pnl < DIRECTIONAL_SCALE_UP_PNL_MIN:
                continue
            _dir_thesis = str(row.get('Thesis_State', '') or '').upper()
            if 'DEGRAD' in _dir_thesis or 'BROKEN' in _dir_thesis:
                continue
            if pd.notna(row.get('Scale_Trigger_Price')):
                continue
            # Wave phase gate
            _dir_wave = str(row.get('WavePhase_State', '') or '').upper()
            if _dir_wave != 'BUILDING':
                continue
            # Tier gate — directional only gets 1 add
            _dir_tier = int(float(row.get('Pyramid_Tier', 0) or 0))
            if _dir_tier > DIRECTIONAL_PYRAMID_MAX_TIER:
                continue
            # Regime gate — no directional adds in bearish/crisis
            _dir_regime = str(row.get('Market_Regime', '') or '').upper()
            if _dir_regime in ('BEARISH', 'CRISIS'):
                continue

            _dir_spot = float(row.get('UL Last', 0) or row.get('Spot', 0) or 0)
            if _dir_spot <= 0:
                continue

            _dir_base = abs(int(float(
                row.get('Base_Quantity') or row.get('Entry_Quantity')
                or row.get('Quantity', 1)
            )))
            _dir_add = max(1, round(_dir_base * PYRAMID_TIER_0_RATIO))
            df_final.at[idx, 'Scale_Trigger_Price'] = _dir_spot
            df_final.at[idx, 'Scale_Add_Contracts'] = _dir_add
            _dir_count += 1

        if _dir_count > 0:
            logger.info(
                f"🎯 Directional Winner Expansion: {_dir_count} positions eligible "
                f"(wave=BUILDING, MFE≥{DIRECTIONAL_SCALE_UP_MFE:.0%}, "
                f"P&L≥{DIRECTIONAL_SCALE_UP_PNL_MIN:.0%})"
            )
    except Exception as _dir_err:
        logger.debug(f"[DirectionalExpansion] Failed (non-fatal): {_dir_err}")

    # --- Scale-Up Bridge: write pending requests to DuckDB for scan engine ----
    # When doctrine identifies Action contains scale-up signals (Scale_Trigger_Price set),
    # persist them so the scan engine can match candidates on the next run.
    try:
        from core.shared.data_layer.scale_up_requests import (
            write_scale_up_request, initialize_scale_up_requests_table
        )
        from core.shared.data_layer.duckdb_utils import get_duckdb_connection as _get_su_con
        _su_mask = (
            df_final['Scale_Trigger_Price'].notna()
            if 'Scale_Trigger_Price' in df_final.columns
            else pd.Series(False, index=df_final.index)
        )
        if _su_mask.any():
            with _get_su_con(read_only=False) as _su_con:
                initialize_scale_up_requests_table(_su_con)
                _su_count = 0
                for _, _su_row in df_final[_su_mask].iterrows():
                    _su_urgency = str(_su_row.get('Urgency', '') or '').upper()
                    _su_lifecycle = str(_su_row.get('Winner_Lifecycle', '') or '').upper()
                    # Priority: CRITICAL=1 for HIGH urgency or CONVICTION_BUILDING, HIGH=2 for MEDIUM, else 3
                    if _su_urgency in ('HIGH', 'CRITICAL') or _su_lifecycle == 'CONVICTION_BUILDING':
                        _su_priority = 1
                    elif _su_urgency == 'MEDIUM':
                        _su_priority = 2
                    else:
                        _su_priority = 3
                    try:
                        write_scale_up_request(
                            con=_su_con,
                            ticker=str(_su_row.get('Underlying_Ticker', '') or _su_row.get('Ticker', '')),
                            strategy=str(_su_row.get('Entry_Structure', '') or _su_row.get('Strategy', '')),
                            trigger_price=float(_su_row['Scale_Trigger_Price']),
                            add_contracts=int(_su_row.get('Scale_Add_Contracts', 1) or 1),
                            priority=_su_priority,
                            source_run_id=run_id if 'run_id' in dir() else None,
                        )
                        _su_count += 1
                    except Exception:
                        pass
                logger.info(f"🔗 Scale-Up Bridge: wrote {_su_count} requests to pipeline.duckdb")
    except Exception as _su_err:
        logger.debug(f"[ScaleUp] Bridge write failed (non-fatal): {_su_err}")

    # --- Outputs ---
    emit_path = Path(emit_path)
    emit_path.parent.mkdir(parents=True, exist_ok=True)
    df_final.to_csv(emit_path, index=False)
    logger.info(f"💾 Authoritative output emitted: {emit_path}")

    # RAG: Unify Truth Layer. Persist final recommendations to DuckDB.
    try:
        from core.shared.data_contracts.config import PIPELINE_DB_PATH
        from core.shared.data_layer.duckdb_utils import get_duckdb_connection # Use the utility function
        with get_duckdb_connection(read_only=False) as con: # Use non-read-only connection for writes
            # RAG: Schema Evolution. Ensure table is altered, not dropped, to preserve history.
            table_name = 'management_recommendations'
            
            # Check if table exists
            table_exists = con.execute(f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table_name}'").fetchone()[0] > 0

            if not table_exists:
                # Create table if it doesn't exist
                con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df_final WHERE 1=0")
                logger.info(f"Created {table_name} table.")
            else:
                # Add any missing columns to the existing table
                db_cols_info = con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
                existing_db_cols = {row[1] for row in db_cols_info}
                for col in df_final.columns:
                    if col not in existing_db_cols:
                        # Determine DuckDB type (simplified, can be made more robust)
                        duckdb_type = 'VARCHAR' # Default to VARCHAR for safety
                        if pd.api.types.is_numeric_dtype(df_final[col]):
                            duckdb_type = 'DOUBLE'
                        elif pd.api.types.is_bool_dtype(df_final[col]):
                            duckdb_type = 'BOOLEAN'
                        elif pd.api.types.is_datetime64_any_dtype(df_final[col]):
                            duckdb_type = 'TIMESTAMP'
                        con.execute(f'ALTER TABLE {table_name} ADD COLUMN "{col}" {duckdb_type}')
                        logger.info(f"Added column '{col}' to {table_name}.")
            
            # Append the latest run
            # Ensure df_final has all columns present in the DB table, filling missing with NaN/None
            db_cols_info = con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
            target_cols = [row[1] for row in db_cols_info]
            
            df_to_insert = df_final.reindex(columns=target_cols)

            con.execute(f"INSERT INTO {table_name} SELECT * FROM df_to_insert")
            
            # Create a view for the latest state (based on ingestion_ts and run_id)
            con.execute("""
                CREATE OR REPLACE VIEW v_latest_recommendations AS 
                SELECT * FROM management_recommendations 
                QUALIFY ROW_NUMBER() OVER (PARTITION BY TradeID ORDER BY Snapshot_TS DESC, run_id DESC) = 1
            """)
            logger.info("✅ Final recommendations persisted to DuckDB (v_latest_recommendations)")

            # ── Doctrine Feedback Loop ────────────────────────────────────────
            # Detect closed trades, track MFE/MAE, classify outcomes, refresh
            # aggregation table. Non-blocking: failure here never halts the run.
            try:
                from core.management.cycle3.feedback.feedback_engine import run_feedback_cycle
                run_feedback_cycle(df_final, con)
                logger.info("[FeedbackEngine] Doctrine feedback cycle complete.")
            except Exception as _fb_err:
                logger.warning(f"⚠️ Feedback engine failed (non-fatal): {_fb_err}")

            # ── Decision Ledger — continuous trade memory ────────────────────
            # Creates the trade_decision_timeline VIEW (synthesises timeline from
            # management_recommendations) and the executed_actions TABLE.
            # Auto-detects executed ROLLs/EXITs by comparing prior strike to current.
            # Non-blocking: failure here never halts the run.
            try:
                from core.shared.data_layer.decision_ledger import (
                    ensure_decision_ledger_view,
                    ensure_executed_actions_table,
                    auto_detect_executions,
                )
                ensure_decision_ledger_view(con)
                ensure_executed_actions_table(con)
                _auto_exec_count = auto_detect_executions(con, df_final)
                if _auto_exec_count > 0:
                    logger.info(f"[DecisionLedger] Auto-detected {_auto_exec_count} executed actions.")
                logger.info("[DecisionLedger] View + table ensured.")
            except Exception as _dl_err:
                logger.warning(f"⚠️ Decision ledger setup failed (non-fatal): {_dl_err}")

            # ── Data Integrity Audit — persist to DuckDB ──────────────────
            try:
                if '_integrity_report' in dir():
                    from core.management.data_integrity_monitor import persist_audit
                    persist_audit(_integrity_report, con)
                    logger.info("[IntegrityAudit] Audit row persisted to data_integrity_audit.")
            except Exception as _ia_err:
                logger.warning(f"⚠️ Integrity audit persist failed (non-fatal): {_ia_err}")

            # ── Missing-Data Health — persist to DuckDB ──────────────────
            try:
                _missing_tracker.persist(con)
            except Exception as _md_err:
                logger.warning(f"⚠️ Missing-data health persist failed (non-fatal): {_md_err}")

    except Exception as e:
        logger.error(f"❌ Failed to persist recommendations to DuckDB: {e}")
    
    # Archive
    archive_dir = Path(archive_dir)
    archive_dir.mkdir(parents=True, exist_ok=True)
    archive_path = archive_dir / f"positions_{run_timestamp}.csv"
    df_final.to_csv(archive_path, index=False)
    
    # Audit Trace
    audit_dir = Path(audit_dir)
    audit_dir.mkdir(parents=True, exist_ok=True)
    trace_path = audit_dir / f"audit_trace_{run_timestamp}.csv"
    df_final.to_csv(trace_path, index=False)
    
    # Manifest
    manifest = {
        "run_id": run_id,
        "run_timestamp": run_timestamp,
        "schema_hash": df_final['Schema_Hash'].iloc[0] if ('Schema_Hash' in df_final.columns and not df_final.empty) else "N/A",
        "cycles_executed": ["cycle1", "cycle2", "cycle3"],
        "rows_emitted": len(df_final),
        "structural_completeness": {
            "resolved": int(df_final['Structural_Data_Complete'].sum()) if 'Structural_Data_Complete' in df_final.columns else 0,
            "unknown": int((~df_final['Structural_Data_Complete']).sum()) if 'Structural_Data_Complete' in df_final.columns else 0
        },
        "data_sources": {
            "prices": "Broker",
            "iv_sources": iv_sources if iv_sources else ["Schwab/Fallback"],
            "schwab_live": schwab_live,
            "allow_system_time": allow_system_time
        },
        "ingest_context": "local_ingest"
    }
    manifest_path = audit_dir / f"run_manifest_{run_timestamp}.json"
    with open(manifest_path, 'w') as f:
        json.dump(manifest, f, indent=2)
    
    logger.info(f"✅ Run complete. Manifest: {manifest_path}")

    # --- Data Integrity Summary (CLI output) ---
    if _integrity_summary:
        print(_integrity_summary)

    return df_final

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run end-to-end management pipeline")
    parser.add_argument("--input", default="auto", help="Input CSV path (default: auto-detect newest)")
    parser.add_argument("--emit", default="core/management/outputs/positions_latest.csv", help="Canonical output path")
    parser.add_argument("--archive", default="core/management/outputs/history/", help="Archive directory")
    parser.add_argument("--audit", default="core/management/outputs/audit/", help="Audit directory")
    parser.add_argument("--allow-system-time", action="store_true", help="Allow fallback to system time if broker timestamp is missing")
    parser.add_argument("--balance", type=float, default=100_000.0, help="Account balance in dollars (default: $100,000)")
    parser.add_argument("--persona", default="balanced", choices=["conservative", "balanced", "aggressive"], help="Risk persona for Greek limits (default: balanced)")

    args = parser.parse_args()

    # Resolve auto-detect for input CSV
    _input_path = args.input
    if _input_path == "auto":
        from core.management.cycle1.ingest.clean import auto_detect_latest_positions
        _input_path = str(auto_detect_latest_positions())
        logger.info(f"[auto-detect] Using: {_input_path}")

    try:
        run_all(_input_path, args.emit, args.archive, args.audit,
                allow_system_time=args.allow_system_time,
                account_balance=args.balance, persona=args.persona)
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}", exc_info=True)
        sys.exit(1)
