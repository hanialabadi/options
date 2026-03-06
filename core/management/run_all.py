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
from core.management.cycle2.drift.compute_basic_drift import compute_drift_metrics
from core.management.cycle2.drift.compute_windowed_drift import compute_windowed_drift
from core.management.cycle2.drift.drift_engine import DriftEngine
from core.management._quarantine.legacy.compute_drift_metrics import classify_drift_severity
from core.management.cycle1.snapshot.snapshot import save_clean_snapshot, validate_cycle1_ledger
from core.management.cycle3.decision.engine import generate_recommendations, _apply_execution_readiness
from core.management.mc_management import run_management_mc
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

def run_all(input_csv: str, emit_path: str, archive_dir: str, audit_dir: str,
            allow_system_time: bool = False,
            account_balance: float = 100_000.0, persona: str = 'balanced'):
    start_time = datetime.now()
    run_timestamp = start_time.strftime("%Y%m%d_%H%M%S")
    
    logger.info(f"🚀 Starting Management Pipeline. allow_system_time={allow_system_time}")

    # Explicitly initialize DuckDB metadata table to ensure it's ready
    initialize_price_history_metadata_table()
    logger.info("✅ DuckDB price history metadata table initialized.")

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
            option_mask = df_enriched['AssetType'] == 'OPTION'
            df_enriched.loc[option_mask, 'IV_Now'] = df_enriched.loc[option_mask, 'Symbol'].map(iv_map)
            
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
                iv_entry_decimal = df_enriched['IV_Entry'].apply(lambda x: x / 100.0 if pd.notna(x) and x > 2.0 else x)
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

            # Fallback: any rows still missing IV_30D get IV_Now (per-contract)
            df_enriched['IV_30D'] = df_enriched['IV_30D'].fillna(df_enriched['IV_Now'])
            loguru_logger.debug(f"[DEBUG_IV_POP] IV_30D from iv_term_history. Sample:\n{df_enriched[['Underlying_Ticker', 'IV_Now', 'IV_30D']].head()}")

            # ── Canonical volatility aliases (Phase 1 migration) ──────────────
            # New names coexist with old names. See schema.py VOLATILITY DATA MODEL.
            df_enriched['IV_Contract']       = df_enriched['IV_Now']        # per-contract
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

            # Fix: DuckDB only supports one QUALIFY per query. Use CTE for the two-pass dedup.
            # Pass 1: pick latest snapshot per (TradeID, day)
            # Pass 2: pick latest day per TradeID
            _prior_d1 = _jcon.execute(f"""
                WITH daily_latest AS (
                    SELECT *
                    FROM management_recommendations
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
                    rsi_14                AS Prior_rsi{_opt_scale_cols},
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
            _prior_d2 = _jcon.execute("""
                WITH daily_latest AS (
                    SELECT *
                    FROM management_recommendations
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
            _prior_recs["Prior_Snapshot_TS"] = pd.to_datetime(
                _prior_recs["Prior_Snapshot_TS"], utc=True, errors="coerce"
            )
            _now_utc = pd.Timestamp.utcnow()
            _prior_recs["Prior_Days_Ago"] = (
                (_now_utc - _prior_recs["Prior_Snapshot_TS"])
                .dt.total_seconds() / 86400
            ).round(1)
            _journey_cols = [
                "TradeID", "Prior_Action", "Prior_Urgency",
                "Prior_Snapshot_TS", "Prior_UL_Last",
                "Prior_Days_Ago", "Prior_Rationale_Digest",
                "Prior_bb_width_z", "Prior_momentum_slope", "Prior_adx", "Prior_rsi",
                "Prior_Scale_Trigger_Price", "Prior_Scale_Add_Contracts", "Prior_Pyramid_Tier",
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
                         "Prior_Scale_Trigger_Price", "Prior_Scale_Add_Contracts", "Prior_Pyramid_Tier",
                         "Prior2_momentum_slope", "Prior2_adx", "Prior2_rsi", "Prior2_bb_width_z"]:
                df_with_drift[_col] = None
    except Exception as _je:
        logger.warning(f"⚠️ Journey context fetch failed (non-fatal): {_je}")
        for _col in ["Prior_Action", "Prior_Urgency", "Prior_Snapshot_TS",
                     "Prior_UL_Last", "Prior_Days_Ago", "Prior_Rationale_Digest",
                     "Prior_bb_width_z", "Prior_momentum_slope", "Prior_adx", "Prior_rsi",
                     "Prior_Scale_Trigger_Price", "Prior_Scale_Add_Contracts", "Prior_Pyramid_Tier",
                     "Prior2_momentum_slope", "Prior2_adx", "Prior2_rsi", "Prior2_bb_width_z"]:
            if _col not in df_with_drift.columns:
                df_with_drift[_col] = None

    # 2.955 Action Streak Counter — count consecutive days with same Action per TradeID.
    # Used by the escalation gate (3.0a) to auto-resolve persistent REVALIDATE
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
                                    WHEN 'REVALIDATE' THEN 2
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

        # Market stress from earlier Cycle 2 or defaults
        _market_stress = "NORMAL"

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

    # 3.0a Action Streak Escalation — auto-resolve persistent REVALIDATE / stale EXIT.
    # Runs AFTER drift filter so REVALIDATE actions from drift override are captured.
    # Rule 1: REVALIDATE for ≥3 consecutive days → escalate to EXIT MEDIUM.
    #         Rationale: signal degradation is persistent, not transient noise.
    # Rule 2: EXIT for ≥5 consecutive days → promote urgency to CRITICAL.
    #         Rationale: user has not acted on EXIT; urgency must increase.
    if "Prior_Action_Streak" in df_final.columns:
        _streak = pd.to_numeric(df_final["Prior_Action_Streak"], errors="coerce").fillna(0).astype(int)

        _urgency_order = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}

        # Rule 1: REVALIDATE × 3+ → EXIT MEDIUM
        _reval_mask = (df_final["Action"] == "REVALIDATE") & (_streak >= 3)
        if _reval_mask.any():
            _reval_count = int(_reval_mask.sum())
            df_final.loc[_reval_mask, "Action"] = "EXIT"
            df_final.loc[_reval_mask, "Urgency"] = "MEDIUM"
            df_final.loc[_reval_mask, "Rationale"] = (
                df_final.loc[_reval_mask, "Rationale"].fillna("")
                + " | Unresolved REVALIDATE x"
                + _streak[_reval_mask].astype(str)
                + " -- signal degradation persistent, escalating to EXIT."
            )
            df_final.loc[_reval_mask, "Doctrine_Source"] = (
                df_final.loc[_reval_mask, "Doctrine_Source"].fillna("")
                + " + ActionStreak: REVALIDATE->EXIT"
            )
            logger.info(f"[ActionStreak] Escalated {_reval_count} REVALIDATE->EXIT MEDIUM (streak >= 3).")

        # Rule 2: EXIT × 5+ → urgency CRITICAL
        _exit_mask = (df_final["Action"] == "EXIT") & (_streak >= 5)
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
        if "EXIT_Count_Last_5D" in df_final.columns:
            _exit_5d = pd.to_numeric(df_final["EXIT_Count_Last_5D"], errors="coerce").fillna(0).astype(int)
            _ignored_exit_mask = (
                df_final["Action"].isin(["HOLD", "ROLL", "REVALIDATE"])
                & (_exit_5d >= 2)
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

    # 3.0b Circuit Breaker Override — force EXIT CRITICAL on all positions when tripped.
    # Runs AFTER doctrine + drift filter so it has the final Action column to override.
    # Only fires when _circuit_breaker_override was set by the pre-doctrine check.
    if '_circuit_breaker_override' in df_final.columns and df_final['_circuit_breaker_override'].any():
        _cb_override_mask = df_final['_circuit_breaker_override'] == True
        _cb_override_count = int(_cb_override_mask.sum())
        df_final.loc[_cb_override_mask, 'Action'] = 'EXIT'
        df_final.loc[_cb_override_mask, 'Urgency'] = 'CRITICAL'
        _cb_reason_val = df_final.loc[_cb_override_mask, 'Circuit_Breaker_Reason'].iloc[0] if 'Circuit_Breaker_Reason' in df_final.columns else 'Portfolio circuit breaker tripped'
        df_final.loc[_cb_override_mask, 'Rationale'] = (
            df_final.loc[_cb_override_mask, 'Rationale'].fillna('') + ' | '
            + f'CIRCUIT BREAKER: {_cb_reason_val}'
        )
        df_final.loc[_cb_override_mask, 'Doctrine_Source'] = 'McMillan Ch.3: Portfolio Circuit Breaker'
        logger.critical(
            f"🚨 [CircuitBreaker] Forced EXIT CRITICAL on {_cb_override_count} positions"
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
    _roll_mask = df_final["Action"].isin(["ROLL", "ROLL_WAIT"])
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
    _short_call_tickers: set = set()
    if "AssetType" in df_final.columns and "Strategy" in df_final.columns:
        for _, _scrow in df_final[df_final["AssetType"] == "OPTION"].iterrows():
            _sc_strat = str(_scrow.get("Strategy") or "").upper()
            _sc_cp    = str(_scrow.get("Call/Put") or "").upper()
            _sc_qty   = float(_scrow.get("Quantity") or 0)
            _sc_ul    = str(_scrow.get("Underlying_Ticker") or "")
            if _sc_strat in ("BUY_WRITE", "COVERED_CALL") or (_sc_cp in ("C", "CALL") and _sc_qty < 0):
                _short_call_tickers.add(_sc_ul)

    _idle_stock_mask = (
        (df_final.get("AssetType", pd.Series("", index=df_final.index)) == "STOCK")
        & (df_final.get("Strategy", pd.Series("", index=df_final.index)).isin(
            ["STOCK_ONLY_IDLE", "STOCK_ONLY"]
        ))
        & ~df_final.get("Underlying_Ticker", pd.Series("", index=df_final.index)).isin(
            _short_call_tickers
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
    
    # --- Persist daily market state (non-blocking) ---
    # Saves condition onset/resolution timestamps + thesis state for next run.
    # Oscillation guard, days_active, and thesis fallback all depend on this.
    try:
        if _state_store is not None:
            _state_store.save(df_final)
            logger.info("[StateStore] Market state persisted for next run.")
    except Exception as _ss_save_err:
        logger.warning(f"⚠️ StateStore save failed (non-fatal): {_ss_save_err}")

    # --- Execution Readiness (Layer 2 — backend component) ---
    # Called here (not inside generate_recommendations) so it sees the fully-resolved
    # Action column after drift overrides, schema enforcement, and epistemic gates.
    df_final = _apply_execution_readiness(df_final)
    er_counts = df_final['Execution_Readiness'].value_counts().to_dict() if 'Execution_Readiness' in df_final.columns else {}
    logger.info(f"⚡ Execution Readiness — {er_counts}")

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
                df_final.loc[_e2_rule1, "Rationale"] = (
                    df_final.loc[_e2_rule1, "Rationale"].fillna("") +
                    " | ⚡ MC confirms: ACT_NOW — forward breach probability high."
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

            # Full override: non-LEAPS, or LEAPS with degraded/broken thesis
            _e2_rule3      = _e2_hold_mask & _e2_exit_now & ~_leaps_intact
            # LEAPS warning only (no action change)
            _e2_leaps_warn = _e2_hold_mask & _e2_exit_now & _leaps_intact

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

    except Exception as _mc_mgmt_err:
        logger.warning(f"⚠️ Management MC failed (non-fatal): {_mc_mgmt_err}")

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
    return df_final

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run end-to-end management pipeline")
    parser.add_argument("--input", default="data/brokerage_inputs/Positions_All_Accounts_25_end.csv", help="Input CSV path")
    parser.add_argument("--emit", default="core/management/outputs/positions_latest.csv", help="Canonical output path")
    parser.add_argument("--archive", default="core/management/outputs/history/", help="Archive directory")
    parser.add_argument("--audit", default="core/management/outputs/audit/", help="Audit directory")
    parser.add_argument("--allow-system-time", action="store_true", help="Allow fallback to system time if broker timestamp is missing")
    parser.add_argument("--balance", type=float, default=100_000.0, help="Account balance in dollars (default: $100,000)")
    parser.add_argument("--persona", default="balanced", choices=["conservative", "balanced", "aggressive"], help="Risk persona for Greek limits (default: balanced)")

    args = parser.parse_args()

    try:
        run_all(args.input, args.emit, args.archive, args.audit,
                allow_system_time=args.allow_system_time,
                account_balance=args.balance, persona=args.persona)
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}", exc_info=True)
        sys.exit(1)
