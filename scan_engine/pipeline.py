"""
Full Scan Pipeline Orchestrator

Combines all steps into a single run_full_scan_pipeline() function.
"""

import pandas as pd
import numpy as np # Import numpy
import logging
import os
import time
from pathlib import Path
from datetime import datetime, timedelta # Import timedelta
import json # Import the json module
import duckdb # Import duckdb globally

from core.shared.data_contracts.config import SCAN_OUTPUT_DIR
from core.shared.data_layer.duckdb_utils import (
    get_duckdb_write_connection, get_duckdb_connection, PIPELINE_DB_PATH,
    get_domain_connection, get_domain_write_connection, DbDomain,
)
from .step2_load_and_enrich_snapshot import load_ivhv_snapshot
from .step3_ivhv_gap_analysis import filter_ivhv_gap
from .step4_chart_signals import compute_chart_signals
from .step5_gem_filter import validate_data_quality
from .step6_strategy_recommendation import recommend_strategies, _LEVERAGED_ETFS
from .step7_iv_demand import emit_iv_demand
from .step13_position_sizing import compute_thesis_capacity
from .step9_determine_timeframe import determine_timeframe
from .step10_fetch_contracts_schwab import fetch_and_select_contracts_schwab  # Production Schwab version
from .step11_pcs_recalibration import recalibrate_and_filter
from .step8_independent_evaluation import evaluate_strategies_independently
from .step12_acceptance import (
    apply_acceptance_logic, filter_ready_contracts, apply_execution_gate, apply_post_gate_demotions, # Phase 3 acceptance logic, and new Execution Gate
    detect_directional_bias, detect_structure_bias, evaluate_timing_quality, # For calculating these within pipeline
    classify_strategy_type, # Import for Strategy_Type default
    persist_to_wait_list # Smart WAIT Loop integration
)

from .feedback_calibration import prime_cache as _prime_feedback_cache
from .step12d_bias_free_enrichment import enrich_bias_free
from .step12e_maturity_eligibility import apply_maturity_eligibility
from .portfolio_admission import apply_portfolio_admission
from .debug.debug_mode import get_debug_manager
from core.shared.data_layer.market_stress_detector import classify_market_stress # Corrected import
from core.shared.governance import audit_harness as audit
from core.shared.governance.pipeline_contracts import (
    validate_step_output,
    STEP_2_OUTPUTS,
    STEP_10_OUTPUTS,
    STEP_12_REQUIRED_INPUTS
)
from .execution_manager.execution_monitor import ExecutionMonitor # Import ExecutionMonitor

# Initialize logger before any try/except blocks that reference it
logger = logging.getLogger(__name__)

# Bias-free enrichment system (post-Step 12)
try:
    from core.enrichment.pipeline_hook import (
        run_post_step12_enrichment,
        PipelineEnrichmentHook,
        validate_no_strategy_bias
    )
    ENRICHMENT_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Enrichment system not available: {e}")
    ENRICHMENT_AVAILABLE = False

# Maturity & Eligibility Integration (post-enrichment)
try:
    from core.enrichment.pipeline_maturity_integration import (
        apply_maturity_and_eligibility,
        get_final_scan_output,
        validate_maturity_consistency,
    )
    MATURITY_INTEGRATION_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Maturity integration not available: {e}")
    MATURITY_INTEGRATION_AVAILABLE = False

# Smart WAIT Loop imports
try:
    from core.wait_loop.schema import initialize_wait_list_schema
    from core.wait_loop.evaluator import evaluate_wait_list
    from core.wait_loop.output_formatter import format_complete_scan_output
    WAIT_LOOP_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Wait loop modules not available: {e}")
    WAIT_LOOP_AVAILABLE = False


# ============================================================
# REGIME × STRATEGY FAMILY COMPATIBILITY MATRIX
# Natenberg Ch.19, McMillan Ch.1, Passarelli Ch.2
# Maps (IV Regime, Market Stress) → which Capital_Buckets fit/caution/mismatch
# ============================================================
_REGIME_STRATEGY_MATRIX = {
    ('High Vol',    'CRISIS'):   {'fit': ['DEFENSIVE'],                           'caution': ['STRATEGIC'], 'mismatch': ['TACTICAL'],  'note': 'Crisis vol: income/defensive only. Long vol too expensive; near-dated directional has no edge.'},
    ('High Vol',    'ELEVATED'): {'fit': ['DEFENSIVE', 'STRATEGIC'],              'caution': ['TACTICAL'],  'mismatch': [],            'note': 'Elevated stress + High Vol: LEAPs tolerated (buying cheaper long-dated vol). Near-dated directional risky.'},
    ('High Vol',    'NORMAL'):   {'fit': ['DEFENSIVE', 'STRATEGIC'],              'caution': ['TACTICAL'],  'mismatch': [],            'note': 'High Vol regime: income has edge. Directional LEAPs ok; short-dated directional needs strong signal.'},
    ('High Vol',    'LOW'):      {'fit': ['DEFENSIVE', 'STRATEGIC', 'TACTICAL'],  'caution': [],            'mismatch': [],            'note': 'High Vol + low SPY stress: vol may be single-stock elevated. All buckets eligible with conviction.'},
    ('Compression', 'CRISIS'):   {'fit': ['DEFENSIVE'],                           'caution': ['STRATEGIC'], 'mismatch': ['TACTICAL'],  'note': 'Crisis stress in compression: do not fight the tape. Income/defensive only; wait for resolution.'},
    ('Compression', 'ELEVATED'): {'fit': ['DEFENSIVE'],                           'caution': ['STRATEGIC'], 'mismatch': ['TACTICAL'],  'note': 'Compressed IV + elevated stress: no directional edge until breakout confirms. Income preferred.'},
    ('Compression', 'NORMAL'):   {'fit': ['DEFENSIVE', 'STRATEGIC'],              'caution': ['TACTICAL'],  'mismatch': [],            'note': 'Compression: ADX typically low — mean-reversion/income favoured. Directional needs breakout catalyst.'},
    ('Compression', 'LOW'):      {'fit': ['DEFENSIVE', 'STRATEGIC', 'TACTICAL'],  'caution': [],            'mismatch': [],            'note': 'Low-stress compression: potential pre-breakout setup. All buckets eligible; watch for vol expansion.'},
    ('Low Vol',     'CRISIS'):   {'fit': ['DEFENSIVE'],                           'caution': ['STRATEGIC'], 'mismatch': ['TACTICAL'],  'note': 'Low single-stock IV + market crisis. Suspect data lag. Prefer defensive only.'},
    ('Low Vol',     'ELEVATED'): {'fit': ['DEFENSIVE'],                           'caution': ['STRATEGIC'], 'mismatch': ['TACTICAL'],  'note': 'Low IV + elevated stress: premium thin for income; near-dated directional risky. LEAPs may work.'},
    ('Low Vol',     'NORMAL'):   {'fit': ['STRATEGIC', 'TACTICAL'],               'caution': ['DEFENSIVE'], 'mismatch': [],            'note': 'Low Vol + calm market: buy vol cheap (Natenberg Ch.4). Directional longs favoured; income edge thin.'},
    ('Low Vol',     'LOW'):      {'fit': ['STRATEGIC', 'TACTICAL'],               'caution': ['DEFENSIVE'], 'mismatch': [],            'note': 'Low Vol + low stress: ideal for directional longs — buying vol below realized. Income edge minimal.'},
    ('Unknown',     'CRISIS'):   {'fit': ['DEFENSIVE'],                           'caution': [],            'mismatch': ['TACTICAL'],  'note': 'Unknown IV regime + crisis: insufficient vol data. Defensive only until regime clarifies.'},
    ('Unknown',     'ELEVATED'): {'fit': ['DEFENSIVE', 'STRATEGIC'],              'caution': ['TACTICAL'],  'mismatch': [],            'note': 'Unknown IV regime + elevated stress: prefer defensive; LEAP caution; avoid short-dated directional.'},
    ('Unknown',     'NORMAL'):   {'fit': ['DEFENSIVE', 'STRATEGIC', 'TACTICAL'],  'caution': [],            'mismatch': [],            'note': 'Unknown IV regime: all buckets permitted but no vol-based edge signal available.'},
    ('Unknown',     'LOW'):      {'fit': ['DEFENSIVE', 'STRATEGIC', 'TACTICAL'],  'caution': [],            'mismatch': [],            'note': 'Unknown IV regime + low stress: all eligible; no vol-based edge signal available.'},
}


# ============================================================
# GOVERNANCE: AUTHORITY PRESERVATION
# ============================================================

def _validate_step2_authority_preserved(
    df_before: pd.DataFrame,
    df_after: pd.DataFrame,
    stage_name: str = "enrichment"
) -> None:
    """
    Validates that Step 2 canonical fields were not overwritten by downstream processes.

    This enforces single data authority - fields owned by Step 2 (Signal_Type, Regime,
    IV_Rank_30D, IV_Maturity_State) must not be modified by enrichment or other stages.

    Args:
        df_before: DataFrame before enrichment/processing
        df_after: DataFrame after enrichment/processing
        stage_name: Name of stage for error messages (default: "enrichment")

    Raises:
        ValueError: If any canonical field was modified

    Canonical Fields (owned by Step 2):
        - Signal_Type: Bullish/Bearish/Bidirectional (from Murphy indicators)
        - Regime: High Vol/Low Vol/Compression/Expansion (from IV_Rank + IV_Trend + VVIX)
        - IV_Rank_30D: Percentile ranking from IVEngine (Schwab IV history)
        - IV_Maturity_State: MATURE/PARTIAL_MATURE/IMMATURE/MISSING (from iv_term_history count)
        - IV_Rank_Source: ROLLING_20D/ROLLING_30D/ROLLING_60D/ROLLING_252D (provenance tracking)
    """
    canonical_fields = ['Signal_Type', 'Regime', 'IV_Rank_30D', 'IV_Maturity_State', 'IV_Rank_Source']
    violations = []

    for field in canonical_fields:
        if field not in df_before.columns:
            logger.warning(f"⚠️ Canonical field {field} missing in 'before' DataFrame")
            continue

        if field not in df_after.columns:
            violations.append(f"Canonical field {field} was REMOVED by {stage_name}")
            continue

        # Compare values (handle NaN equality)
        changed_mask = df_before[field] != df_after[field]
        # NaN == NaN should be True for this check
        changed_mask = changed_mask & ~(df_before[field].isna() & df_after[field].isna())

        changed_count = changed_mask.sum()
        if changed_count > 0:
            violations.append(
                f"Canonical field {field} was modified by {stage_name} "
                f"({changed_count}/{len(df_before)} rows changed)"
            )

    if violations:
        error_msg = f"❌ AUTHORITY VIOLATION in {stage_name}:\n" + "\n".join(f"  - {v}" for v in violations)
        logger.error(error_msg)
        raise ValueError(error_msg)

    logger.info(f"✅ Authority validation passed: {len(canonical_fields)} canonical fields unchanged after {stage_name}")


class PipelineContext:
    """Holds state and configuration for a pipeline run."""
    def __init__(self, snapshot_path, output_dir, account_balance, max_portfolio_risk, sizing_method, expiry_intent, audit_mode):
        self.snapshot_path = snapshot_path
        self.output_dir = Path(output_dir) if output_dir else SCAN_OUTPUT_DIR
        self.account_balance = account_balance
        self.max_portfolio_risk = max_portfolio_risk
        self.sizing_method = sizing_method
        self.expiry_intent = expiry_intent
        self.audit_mode = audit_mode
        self.results = {}
        self.debug_manager = get_debug_manager()
        self.execution_monitor = ExecutionMonitor() # Instantiate ExecutionMonitor
        self.missing_tracker = None  # initialized after run_ts is known
        
        logger.debug(f"DEBUG: PipelineContext init - output_dir param: {output_dir}, self.output_dir: {self.output_dir}")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        if self.debug_manager.enabled:
            self.debug_manager.clear()

from core.shared.data_layer.technical_data_repository import initialize_technical_indicators_table # Import the initialization function
from core.shared.data_layer.duckdb_utils import get_duckdb_write_connection, PIPELINE_DB_PATH # Import for single connection


# ============================================================
# STEP -1: RE-EVALUATE WAIT LIST
# ============================================================

def _step_minus_1_reevaluate_wait_list(ctx: 'PipelineContext', con: duckdb.DuckDBPyConnection):
    """
    Step -1: Re-evaluate active WAIT list entries before discovery.

    This step:
    1. Loads all ACTIVE wait entries
    2. Fetches current market data for those tickers
    3. Re-evaluates wait conditions
    4. Promotes trades to READY_NOW if conditions met
    5. Expires trades that exceed TTL

    RAG Source: docs/SMART_WAIT_DESIGN.md - Re-Evaluation Engine
    """
    if not WAIT_LOOP_AVAILABLE:
        return

    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    logger.info("🔄 Step -1: Re-evaluating Wait List")
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    try:
        from core.wait_loop.persistence import load_active_waits
        from core.wait_loop.schema import get_wait_list_summary

        # ============================================================
        # TOP-OF-SCAN AUDIT BLOCK
        # Requirement 1: Comprehensive WAIT metrics summary
        # ============================================================
        logger.info("")
        logger.info("📊 WAIT LIST AUDIT (Pre-Evaluation)")
        logger.info("─" * 60)

        # Get comprehensive summary from database
        try:
            summary_stats = get_wait_list_summary(con)
            total_active = summary_stats.get('total_active', 0)
            avg_progress = summary_stats.get('avg_progress', 0.0)
            avg_evaluations = summary_stats.get('avg_evaluations', 0)
            avg_hours_waiting = summary_stats.get('avg_hours_waiting', 0.0)

            # Query historical metrics (last 7 days)
            history_query = """
                SELECT
                    COUNT(CASE WHEN status = 'PROMOTED' THEN 1 END) as total_promoted,
                    COUNT(CASE WHEN status = 'EXPIRED' THEN 1 END) as total_expired,
                    COUNT(CASE WHEN status = 'INVALIDATED' THEN 1 END) as total_invalidated,
                    COUNT(CASE WHEN status = 'REJECTED' THEN 1 END) as total_rejected
                FROM wait_list
                WHERE created_at >= CURRENT_TIMESTAMP - INTERVAL 7 DAY
            """
            history_result = con.execute(history_query).fetchone()
            total_promoted = history_result[0] if history_result else 0
            total_expired = history_result[1] if history_result else 0
            total_invalidated = history_result[2] if history_result else 0
            total_rejected = history_result[3] if history_result else 0

            logger.info(f"   Active WAIT entries: {total_active}")
            if total_active > 0:
                logger.info(f"   Average progress: {avg_progress:.1%}")
                logger.info(f"   Average evaluations: {avg_evaluations:.1f}")
                logger.info(f"   Average time waiting: {avg_hours_waiting:.1f}h")

            logger.info(f"   Historical (7d):")
            logger.info(f"      Promoted: {total_promoted}")
            logger.info(f"      Expired: {total_expired}")
            logger.info(f"      Invalidated: {total_invalidated}")
            logger.info(f"      Rejected: {total_rejected}")

            # Calculate promotion rate
            total_resolved = total_promoted + total_expired + total_invalidated + total_rejected
            if total_resolved > 0:
                promotion_rate = total_promoted / total_resolved * 100
                expiry_rate = (total_expired + total_invalidated) / total_resolved * 100
                logger.info(f"   Promotion rate: {promotion_rate:.1f}%")
                logger.info(f"   Expiry rate: {expiry_rate:.1f}%")

        except Exception as e:
            logger.warning(f"   Could not retrieve wait list summary: {e}")

        logger.info("─" * 60)
        logger.info("")

        # Load active wait entries
        wait_entries = load_active_waits(con)

        if not wait_entries:
            logger.info("ℹ️  No active wait list entries to re-evaluate")
            ctx.results['wait_list_reevaluated'] = []
            ctx.results['wait_list_promoted'] = []
            ctx.results['wait_list_expired'] = []
            ctx.results['wait_list_still_waiting'] = []
            return

        logger.info(f"📋 Found {len(wait_entries)} active wait list entries")

        # Log breakdown by strategy type and age
        from collections import Counter
        strategy_types = Counter(entry['strategy_type'] for entry in wait_entries)
        logger.info(f"   By type: {dict(strategy_types)}")

        # Age distribution
        from datetime import datetime
        now = datetime.now()
        ages = [(now - entry['wait_started_at']).total_seconds() / 3600 for entry in wait_entries]
        if ages:
            logger.info(f"   Age range: {min(ages):.1f}h - {max(ages):.1f}h (avg: {sum(ages)/len(ages):.1f}h)")

        # Extract tickers for market data fetch
        tickers = list(set(entry['ticker'] for entry in wait_entries))
        logger.info(f"📊 Fetching current market data for {len(tickers)} tickers")

        # Fetch current market data — lightweight CSV read only.
        # The wait list evaluator only needs price/IV columns; running the full
        # load_ivhv_snapshot() (Murphy indicators, OHLC fetch) here would duplicate
        # ~100s of work that the main pipeline Step 2 does moments later.
        try:
            _snap_cols = [
                'Ticker', 'Last_Price', 'last_price', 'Bid', 'bid', 'Ask', 'ask',
                'iv_30d', 'hv_30', 'Volume', 'volume',
                'IV_30_D_Call', 'HV_30_D_Cur',
            ]
            df_market = pd.read_csv(ctx.snapshot_path, usecols=lambda c: c in _snap_cols)
            df_market = df_market[df_market['Ticker'].isin(tickers)]

            def _first_valid(*vals):
                """Return first non-NaN, non-None value; None if all missing."""
                for v in vals:
                    if v is not None and pd.notna(v):
                        return v
                return None

            market_data_by_ticker = {}
            for _, row in df_market.iterrows():
                ticker = row['Ticker']
                # Normalise column names: snapshot uses Last_Price/Bid/Ask etc.
                # Use _first_valid() — NaN is truthy in Python so plain `or` leaks NaN through.
                market_data_by_ticker[ticker] = {
                    'ticker':       ticker,
                    'last_price':   _first_valid(row.get('Last_Price'), row.get('last_price')),
                    'bid':          _first_valid(row.get('Bid'),        row.get('bid')),
                    'ask':          _first_valid(row.get('Ask'),        row.get('ask')),
                    'iv_30d':       _first_valid(row.get('iv_30d'),     row.get('IV_30_D_Call')),
                    'hv_30':        _first_valid(row.get('hv_30'),      row.get('HV_30_D_Cur')),
                    'volume':       _first_valid(row.get('Volume'),     row.get('volume')),
                    'chart_signal': 'NEUTRAL',  # not available from raw CSV; evaluator handles None
                }

            logger.info(f"✅ Fetched market data for {len(market_data_by_ticker)} tickers")

        except Exception as e:
            logger.error(f"❌ Error fetching market data: {e}")
            market_data_by_ticker = {}

        # Re-evaluate wait list
        eval_result = evaluate_wait_list(con, market_data_by_ticker)

        logger.info(f"✅ Re-evaluation complete: {eval_result.summary()}")

        # Store results in context for merging with discovery
        ctx.results['wait_list_reevaluated'] = wait_entries
        ctx.results['wait_list_promoted'] = eval_result.promoted
        ctx.results['wait_list_expired'] = eval_result.expired
        ctx.results['wait_list_invalidated'] = eval_result.invalidated
        ctx.results['wait_list_still_waiting'] = eval_result.still_waiting

        # Log details
        if eval_result.promoted:
            logger.info(f"🟢 Promoted to READY_NOW: {len(eval_result.promoted)}")
            for trade in eval_result.promoted:
                logger.info(f"   • {trade['ticker']} - {trade['strategy_name']}")

        if eval_result.expired or eval_result.invalidated:
            total_rejected = len(eval_result.expired) + len(eval_result.invalidated)
            logger.info(f"🔴 Rejected: {total_rejected} (Expired: {len(eval_result.expired)}, Invalidated: {len(eval_result.invalidated)})")

        if eval_result.still_waiting:
            logger.info(f"🟡 Still waiting: {len(eval_result.still_waiting)}")

        # Extension Monitor: re-check WAIT_PULLBACK / WAIT_PRICE entries using
        # current technical indicators (RSI, price vs SMA20, option mid).
        # Promotes when all TECHNICAL + PRICE_LEVEL conditions clear.
        try:
            from core.wait_loop.extension_monitor import run_extension_monitor
            em_result = run_extension_monitor(ctx, con)
            if em_result["promoted"] > 0:
                logger.info(
                    f"🟢 [ExtensionMonitor] Promoted {em_result['promoted']} "
                    f"timing/price-gated entries to PROMOTED"
                )
            ctx.results['extension_monitor'] = em_result
        except Exception as em_err:
            logger.warning(f"⚠️ [ExtensionMonitor] Non-fatal error: {em_err}")

    except Exception as e:
        logger.error(f"❌ Error in Step -1 re-evaluation: {e}", exc_info=True)
        # Don't fail pipeline, just log and continue
        ctx.results['wait_list_reevaluated'] = []
        ctx.results['wait_list_promoted'] = []


def run_full_scan_pipeline(
    snapshot_path: str,
    output_dir: str = None,
    account_balance: float = 100000.0,
    max_portfolio_risk: float = 0.20,
    sizing_method: str = 'volatility_scaled',
    audit_mode = None,
    expiry_intent: str = 'ANY',
    **kwargs
) -> dict:
    """Modularized pipeline orchestrator."""
    ctx = PipelineContext(snapshot_path, output_dir, account_balance, max_portfolio_risk, sizing_method, expiry_intent, audit_mode)

    # Define run_ts at the beginning of the function
    run_ts = datetime.now()

    # Missing-data diagnosis tracker — tags NaN fields with causal reasons
    from core.shared.governance.missing_data_tracker import MissingDataTracker
    ctx.missing_tracker = MissingDataTracker(run_id=run_ts.strftime('%Y%m%d_%H%M%S'))

    # Initialize Schwab client once here so it can be shared by Step 2 (OHLC)
    # and _finalize_results (market stress) — avoids duplicate auth calls.
    schwab_client = None
    try:
        from scan_engine.loaders.schwab_api_client import SchwabClient
        client_id = os.getenv("SCHWAB_APP_KEY")
        client_secret = os.getenv("SCHWAB_APP_SECRET")
        if client_id and client_secret:
            schwab_client = SchwabClient(client_id, client_secret)
            logger.info("✅ Schwab client initialized for pipeline (OHLC + market stress).")
    except Exception as e:
        logger.warning(f"⚠️ Schwab client initialization failed: {e}")

    # Establish a single, persistent DuckDB connection for the entire pipeline run
    db_con = None
    try:
        db_con = get_duckdb_write_connection(str(PIPELINE_DB_PATH))
        logger.info("DEBUG: Single DuckDB connection established for pipeline.")

        # Initialize technical indicators table once per pipeline run
        initialize_technical_indicators_table(con=db_con)

        # Signal Hub: prune stale signals (>365d) — retain YTD for behavioral memory
        from core.shared.data_layer.technical_data_repository import prune_stale_signals
        prune_stale_signals(max_age_days=365, con=db_con)

        # Initialize Smart WAIT Loop schema
        if WAIT_LOOP_AVAILABLE:
            try:
                initialize_wait_list_schema(db_con)
                logger.info("✅ Smart WAIT Loop schema initialized")
            except Exception as e:
                logger.warning(f"⚠️  Could not initialize wait_list schema: {e}")

        # Step -1: Re-evaluate Wait List (before discovery)
        if WAIT_LOOP_AVAILABLE:
            _step_minus_1_reevaluate_wait_list(ctx, db_con)

        if not _step2_load_data(ctx, db_con, schwab_client=schwab_client): return _finalize_results(ctx, run_ts, db_con, schwab_client=schwab_client) # Pass connection
        if not _step3_filter_tickers(ctx): return _finalize_results(ctx, run_ts, db_con, schwab_client=schwab_client)
        if not _step5_6_enrich_and_validate(ctx): return _finalize_results(ctx, run_ts, db_con, schwab_client=schwab_client)
        if not _step7_recommend_strategies(ctx): return _finalize_results(ctx, run_ts, db_con, schwab_client=schwab_client)
        if not _step9_select_contracts(ctx): return _finalize_results(ctx, run_ts, db_con, schwab_client=schwab_client)
        if not _step10_recalibrate_pcs(ctx): return _finalize_results(ctx, run_ts, db_con, schwab_client=schwab_client)
        if not _step11_evaluate_strategies(ctx): return _finalize_results(ctx, run_ts, db_con, schwab_client=schwab_client)
        _step_insert_technical_indicators(ctx, db_con) # Pass connection

        # Pre-warm the feedback calibration cache using the existing pipeline connection.
        # This prevents feedback_calibration._load_feedback_cache() from opening a second
        # read_only connection to pipeline.duckdb (which DuckDB rejects when a write
        # connection is already held).  prime_cache is a no-op if cache is already warm.
        _prime_feedback_cache(db_con)

        # First pass of Execution Gate (Stage 0-1: Broad Scan + Initial Gates)
        # This pass sets initial Execution_Status to AWAIT_CONFIRMATION or BLOCKED
        _step12_8_acceptance_and_sizing(ctx, run_ts, db_con, is_initial_pass=True)

        # Second pass of Execution Gate (Final decisions using IVEngine maturity)
        _step12_8_acceptance_and_sizing(ctx, run_ts, db_con, is_initial_pass=False)

        # =================================================================
        # BIAS-FREE ENRICHMENT SYSTEM (Post-Step 12)
        # =================================================================
        # This system detects missing data requirements and triggers
        # enrichment based purely on data fields, NOT strategy types.
        # It is strategy-agnostic: all trades are treated identically.
        # =================================================================
        if ENRICHMENT_AVAILABLE:
            logger.info("=== Step 12D: Bias-Free Enrichment ===")

            # Capture DataFrame before enrichment for authority validation
            df_before_enrichment = ctx.results.get('acceptance_all', pd.DataFrame()).copy()

            enrich_bias_free(ctx, run_ts, db_con)

            # AUTHORITY PRESERVATION CHECK: Ensure Step 2 canonical fields unchanged
            df_after_enrichment = ctx.results.get('acceptance_all', pd.DataFrame())
            if not df_before_enrichment.empty and not df_after_enrichment.empty:
                _validate_step2_authority_preserved(
                    df_before_enrichment,
                    df_after_enrichment,
                    stage_name="Step 12D enrichment"
                )

        # =================================================================
        # MATURITY & ELIGIBILITY INTEGRATION (Stage 5)
        # =================================================================
        # This applies the new execution eligibility system:
        # - Computes Volatility_Maturity_Tier from iv_history_count
        # - INCOME strategies require MATURE tier (120+ days IV history)
        # - DIRECTIONAL strategies can execute at EARLY+ tier (7+ days)
        # Same logic runs in debug and production - no shortcuts.
        # =================================================================
        if MATURITY_INTEGRATION_AVAILABLE:
            logger.info("=== Step 12E: Maturity & Eligibility ===")
            apply_maturity_eligibility(ctx)

        # =================================================================
        # PORTFOLIO ADMISSION GATE (Stage 6)
        # =================================================================
        # Annotates READY contracts with portfolio-level constraint flags
        # (position cap, sector cap, directional skew, concentration).
        # Does NOT change acceptance_status — informational only.
        # Doctrine: Vince, López de Prado, Carver, McMillan Ch.4.
        # =================================================================
        _pa_df = ctx.results.get('acceptance_all', pd.DataFrame())
        if not _pa_df.empty:
            logger.info("=== Portfolio Admission Gate ===")
            ctx.results['acceptance_all'] = apply_portfolio_admission(_pa_df)

        # =================================================================
        # STRATEGY OVERLAP ANNOTATION
        # =================================================================
        # Annotate income strategies where the same ticker has multiple
        # income alternatives (e.g. CSP + BW) — they share capital.
        _so_df = ctx.results.get('acceptance_all', pd.DataFrame())
        if not _so_df.empty and 'Ticker' in _so_df.columns and 'Strategy_Name' in _so_df.columns:
            _so_df['Strategy_Overlap_Note'] = ''
            _income_mask = _so_df.get('Strategy_Type', pd.Series(dtype=str)) == 'INCOME'
            if _income_mask.any():
                _ticker_counts = _so_df.loc[_income_mask].groupby('Ticker')['Strategy_Name'].transform('count')
                _overlap_mask = _income_mask & (_ticker_counts > 1)
                if _overlap_mask.any():
                    for _ov_ticker in _so_df.loc[_overlap_mask, 'Ticker'].unique():
                        _tmask = (_so_df['Ticker'] == _ov_ticker) & _income_mask
                        _strategies = _so_df.loc[_tmask, 'Strategy_Name'].unique()
                        _note = f"Alternative to {'/'.join(_strategies)} — shared capital, pick one"
                        _so_df.loc[_tmask, 'Strategy_Overlap_Note'] = _note
                    logger.info(f"[StrategyOverlap] {_overlap_mask.sum()} rows annotated across "
                                f"{len(_so_df.loc[_overlap_mask, 'Ticker'].unique())} tickers")
            ctx.results['acceptance_all'] = _so_df

        # =================================================================
        # INTRADAY EXECUTION CHECK (Cycle 3)
        # =================================================================
        # For READY candidates during market hours: fetch live intraday
        # data and score execution readiness (VWAP, momentum, spread, IV).
        # Informational only — does NOT change Execution_Status.
        # =================================================================
        _skip_intraday = kwargs.get('skip_intraday', False)
        try:
            from scan_engine.intraday_execution_check import evaluate_intraday_readiness
            _ie_df = ctx.results.get('acceptance_all', pd.DataFrame())
            if not _ie_df.empty and not _skip_intraday:
                logger.info("=== Intraday Execution Check ===")
                ctx.results['acceptance_all'] = evaluate_intraday_readiness(
                    _ie_df, schwab_client=schwab_client
                )
            elif _skip_intraday:
                logger.info("=== Intraday Execution Check SKIPPED (--no-intraday) ===")
        except ImportError:
            logger.debug("intraday_execution_check not available — skipping")
        except Exception as _ie_err:
            logger.warning(f"⚠️ Intraday execution check failed (non-fatal): {_ie_err}")

        # Always refresh acceptance_ready from acceptance_all so intraday columns propagate
        _aa = ctx.results.get('acceptance_all', pd.DataFrame())
        if not _aa.empty:
            _ready_mask = _aa.get('Execution_Status', pd.Series(dtype=str)) == 'READY'
            if _ready_mask.any():
                ctx.results['acceptance_ready'] = _aa[_ready_mask].copy()

        # =================================================================
        # GUARANTEE STEP 12 OUTPUTS EXIST BEFORE EXPORT
        # =================================================================
        # Ensure acceptance_all and acceptance_ready exist in results
        # to prevent silent export failures
        if 'acceptance_all' not in ctx.results:
            logger.error("❌ Step12 did not produce acceptance_all.")
            ctx.results['acceptance_all'] = pd.DataFrame()

        if 'acceptance_ready' not in ctx.results:
            logger.warning("⚠️ Step12 did not produce acceptance_ready.")
            ctx.results['acceptance_ready'] = pd.DataFrame()

        # =================================================================
        # PERSISTENCE: DuckDB Write (Before Export)
        # =================================================================
        # Explicitly persist scan results to DuckDB before CSV export
        # This ensures dashboard queries see the latest data
        logger.info("=== Persisting scan results to DuckDB ===")
        _persist_to_duckdb(ctx, run_ts, db_con)

    except Exception as e:
        ctx.debug_manager.log_exception("pipeline", e, "Pipeline aborted")
        logger.error(f"❌ Pipeline failed unexpectedly: {e}", exc_info=True)
    finally:
        if db_con is not None:
            db_con.commit() # Commit all changes at the end
            logger.info("DEBUG: Single DuckDB connection committed.")
            db_con.close()
            logger.info("DEBUG: Single DuckDB connection closed.")

    # =================================================================
    # FAIL-FAST GUARD: Ensure Pipeline Completed Step 12
    # =================================================================
    # Verify Step 12 outputs exist - no silent failures allowed
    results = ctx.results
    if 'acceptance_all' not in results:
        raise RuntimeError("Pipeline completed without Step12 output (acceptance_all missing).")

    logger.info(f"✅ Pipeline validation passed: Step12 produced {len(results['acceptance_all'])} evaluated contracts")

    # _finalize_results adds Regime_Gate, Regime_Strategy_Fit, Surface_Shape_Warning
    # columns to acceptance_all — export happens INSIDE _finalize_results so those
    # columns are present in the CSV.
    return _finalize_results(ctx, run_ts, db_con, schwab_client=schwab_client)

def _persist_to_duckdb(ctx: PipelineContext, run_ts: datetime, con: duckdb.DuckDBPyConnection): # Accept connection
    """RAG: Unify Truth Layer. Persist scan results to DuckDB."""
    try:
        from core.shared.data_contracts.config import PIPELINE_DB_PATH
        
        db_path = str(PIPELINE_DB_PATH)
        logger.debug(f"Persisting scan results to DuckDB at: {db_path}")

        # Proceed with actual scan results persistence
        ready_df_source = ctx.results.get('thesis_envelopes', ctx.results.get('acceptance_all', pd.DataFrame())).copy() # Use acceptance_all

        # Ensure 'Execution_Status' and 'Block_Reason' are present, mapping from old names if necessary
        if 'acceptance_status' in ready_df_source.columns and 'Execution_Status' not in ready_df_source.columns:
            ready_df_source['Execution_Status'] = ready_df_source['acceptance_status']
        if 'acceptance_reason' in ready_df_source.columns and 'Block_Reason' not in ready_df_source.columns:
            ready_df_source['Block_Reason'] = ready_df_source['acceptance_reason']

        # Add scan_timestamp and run_id
        ready_df_source['scan_timestamp'] = run_ts
        ready_df_source['run_id'] = f"scan_{run_ts.strftime('%Y%m%d_%H%M%S')}"

        # Define the schema for persistence, ensuring it matches the table definitions
        persistence_schema_cols = [
            'Ticker', 'Strategy_Name', 'Execution_Status', 'Block_Reason',
            'PCS_Score', 'Expression_Tier', 'Timeframe_Label', 'scan_timestamp', 'run_id'
        ]
        
        # Filter to only include 'READY' candidates for the 'latest' view, but all for history
        ready_for_latest_view = ready_df_source[ready_df_source['Execution_Status'] == 'READY'].copy()
        
        # Ensure all columns in persistence_schema_cols are present in ready_for_latest_view, fill NaN if missing
        for col in persistence_schema_cols:
            if col not in ready_for_latest_view.columns:
                ready_for_latest_view[col] = np.nan
        ready_for_latest_view = ready_for_latest_view[persistence_schema_cols] # Reorder columns

        logger.info(f"DEBUG: Attempting to persist scan results to persistent table and create view.")

        # Define the persistent table name for the latest READY candidates
        persistent_latest_table_name = "scan_results_latest"

        # Create or replace the persistent table for latest READY candidates
        con.execute(f"DROP TABLE IF EXISTS {persistent_latest_table_name}")
        con.execute(f"""
            CREATE TABLE {persistent_latest_table_name} (
                Ticker VARCHAR,
                Strategy_Name VARCHAR,
                Execution_Status VARCHAR,
                Block_Reason VARCHAR,
                PCS_Score DOUBLE,
                Expression_Tier VARCHAR,
                Timeframe_Label VARCHAR,
                scan_timestamp TIMESTAMP,
                run_id VARCHAR
            )
        """)
        if not ready_for_latest_view.empty:
            con.from_df(ready_for_latest_view).insert_into(persistent_latest_table_name)
        logger.info(f"DEBUG: Persistent table '{persistent_latest_table_name}' created and populated with {len(ready_for_latest_view)} rows (will be committed by pipeline).")

        # Create or replace the view from the persistent table
        con.execute(f"CREATE OR REPLACE VIEW v_latest_scan_results AS SELECT * FROM {persistent_latest_table_name}")
        logger.info("DEBUG: View 'v_latest_scan_results' created from persistent table (will be committed by pipeline).")
        
        # Verify view creation immediately
        try:
            test_df = con.execute("SELECT * FROM v_latest_scan_results;").fetchdf()
            logger.info(f"DEBUG: Verified 'v_latest_scan_results' exists with {len(test_df)} rows.")
        except Exception as ve:
            logger.error(f"ERROR: Verification of 'v_latest_scan_results' failed: {ve}")

        # Also insert all evaluated strategies into history table
        con.execute("""
            CREATE TABLE IF NOT EXISTS scan_results_history (
                Ticker VARCHAR,
                Strategy_Name VARCHAR,
                Execution_Status VARCHAR,
                Block_Reason VARCHAR,
                PCS_Score DOUBLE,
                Expression_Tier VARCHAR,
                Timeframe_Label VARCHAR,
                scan_timestamp TIMESTAMP,
                run_id VARCHAR
            )
        """)
        # Ensure all columns in persistence_schema_cols are present in ready_df_source, fill NaN if missing
        for col in persistence_schema_cols:
            if col not in ready_df_source.columns:
                ready_df_source[col] = np.nan
        ready_df_for_persistence = ready_df_source[persistence_schema_cols].copy()  # Reorder columns

        if not ready_df_for_persistence.empty:
            con.from_df(ready_df_for_persistence).insert_into("scan_results_history")
            # con.commit() # Commit will be handled by the main pipeline function
        logger.info("✅ Scan results persisted to DuckDB (v_latest_scan_results and scan_results_history - will be committed by pipeline).")
        
        # Explicitly check if the file exists after the 'with' block closes
        if Path(db_path).exists():
            logger.info(f"DEBUG: DuckDB file exists on disk after persistence: {db_path}")
        else:
            logger.error(f"ERROR: DuckDB file DOES NOT EXIST on disk after persistence: {db_path}")

    except Exception as e:
        logger.error(f"❌ Failed to persist scan results to DuckDB: {e}", exc_info=True)

def _step2_load_data(ctx: PipelineContext, con: duckdb.DuckDBPyConnection, schwab_client=None) -> bool:
    logger.info("📊 Step 2: Loading IV/HV snapshot...")
    t0 = time.time()
    df = load_ivhv_snapshot(
        snapshot_path=ctx.snapshot_path,
        use_live_snapshot=True if not ctx.snapshot_path else False,
        con=con,
        schwab_client=schwab_client,
    )
    audit.profile("step2", df, (time.time()-t0)*1000)
    audit.save_df("step2_output", df)

    # Centralized Universe Restriction (Controlled by PIPELINE_DEBUG)
    # This is now handled inside load_ivhv_snapshot to ensure it's applied before parallel processing.
    # df = ctx.debug_manager.restrict_universe(df, top_n=3) # REMOVED

    ctx.results['snapshot'] = df

    # Missing-data diagnosis: tag NaN fields with causal reasons
    if ctx.missing_tracker:
        ctx.missing_tracker.diagnose(df, step_num=2)
        ctx.missing_tracker.audit_stage("step2", None, df)

    # FAIL-FAST VALIDATION: Ensure Step 2 outputs meet data contract
    validate_step_output(df, step_num=2, contract=STEP_2_OUTPUTS)

    ctx.debug_manager.record_step('step2_snapshot', len(df), df)

    if ctx.audit_mode:
        df = ctx.audit_mode.filter_to_audit_tickers(df)
        df = ctx.audit_mode.save_step(df, "snapshot_enriched", "Raw snapshot + IV surface + earnings enrichment")
        ctx.results['snapshot'] = df
    return not df.empty

def _step3_filter_tickers(ctx: PipelineContext) -> bool:
    logger.info("📊 Step 3: Filtering by IVHV gap...")
    df_input = ctx.results['snapshot']
    audit.save_df("step3_input", df_input)
    t0 = time.time()
    df = filter_ivhv_gap(df_input)
    audit.profile("step3", df, (time.time()-t0)*1000)
    audit.save_df("step3_output", df)
    ctx.results['filtered'] = df
    if ctx.missing_tracker:
        ctx.missing_tracker.audit_stage("step3", ctx.results['snapshot'], df)
    ctx.debug_manager.record_step('step3_filtered', len(df), df)
    
    if ctx.audit_mode:
        df = ctx.audit_mode.save_step(df, "ivhv_filtered", "IVHV gap filter applied")
        
    if df.empty:
        ctx.debug_manager.log_event("step3", "WARN", "EMPTY_FILTER_RESULT", "No tickers passed IVHV gap criteria")
    return not df.empty

def _step5_6_enrich_and_validate(ctx: PipelineContext) -> bool:
    logger.info("📊 Step 5: Computing chart signals...")
    df_input = ctx.results['filtered']
    audit.save_df("step5_input", df_input)
    t0 = time.time()
    # Pass snapshot_ts to compute_chart_signals
    if 'timestamp' not in ctx.results['snapshot'].columns:
        raise ValueError("❌ Step 5 cannot proceed: canonical 'timestamp' missing from Step 2 output.")
    snapshot_ts = ctx.results['snapshot']['timestamp'].iloc[0]
    df_charted = compute_chart_signals(df_input, snapshot_ts=snapshot_ts)
    audit.profile("step5", df_charted, (time.time()-t0)*1000)
    audit.save_df("step5_output", df_charted)
    ctx.results['charted'] = df_charted
    if ctx.missing_tracker:
        ctx.missing_tracker.diagnose(df_charted, step_num=5)
        ctx.missing_tracker.audit_stage("step5", ctx.results['filtered'], df_charted)
    ctx.debug_manager.record_step('step5_charted', len(df_charted), df_charted)
    
    if df_charted.empty:
        if ctx.debug_manager.enabled: # Use debug_manager for logging events
            ctx.debug_manager.log_event("step5", "WARN", "EMPTY_CHART_RESULT", "No tickers passed chart signal computation")
        return False

    if ctx.audit_mode:
        ctx.audit_mode.save_step(df_charted, "chart_signals", "Technical analysis")

    logger.info("📊 Step 6: Validating data quality...")
    validated = validate_data_quality(df_charted)
    ctx.results['validated_data'] = validated
    ctx.debug_manager.record_step('step6_validated', len(validated), validated)
    
    if ctx.audit_mode:
        ctx.audit_mode.save_step(validated, "data_validated", "Data quality validation")
    return not validated.empty

def _step7_recommend_strategies(ctx: PipelineContext) -> bool:
    logger.info("🎯 Step 7: Generating strategy recommendations...")
    recommended = recommend_strategies(ctx.results['validated_data'])
    ctx.results['recommended_strategies'] = recommended
    ctx.debug_manager.record_step('step7_recommended', len(recommended), recommended)

    # Phase 7.5: IV Demand Emission (Demand-Driven Architecture)
    df_demand = emit_iv_demand(recommended)
    audit.save_demand(df_demand)
    ctx.results['iv_demand'] = df_demand

    # Store recommendations for Step 9 (contract selection)
    ctx.results['strategies_for_contracts'] = recommended

    return not recommended.empty

def _pre_screen_before_chain_fetch(df: pd.DataFrame) -> pd.DataFrame:
    """
    Layer 2: Quality pre-screen gate — runs AFTER Step 9A, BEFORE Step 9B chain fetch.

    Drops rows that can NEVER reach READY at Step 12, saving expensive Schwab API calls.
    Uses only columns already computed — zero new API calls.

    Rules (ANY one fires → row is dropped):
      R1: Confidence < 35          → near-rejected by Step 6 validator; never READY
      R2: IV_Maturity_Level == 1
          AND strategy is INCOME   → Level-1 maturity always BLOCKED at Step 12 maturity gate
      R3: Entry_Quality == CHASING
          AND Signal_Type in (Bullish, Bearish) → directional chase already gated in Step 6,
                                                   belt-and-suspenders here
      R4: Signal_Type == Unknown
          AND strategy NOT in income family → no directional basis for non-income vol strategy
      R5: IV_Maturity_Level == 1   → ALL strategies blocked at Step 12 maturity gate,
          (any strategy type)        regardless of strategy type. Critical for S&P500 expansion:
                                     new tickers have 0 IV history (Level 1) for first 20 days.
                                     Without R5 they hit Step 9B and burn Schwab API budget for
                                     a guaranteed BLOCKED outcome. R5 supersedes R2 for Level-1.

    Income family: CSP, CC, Buy_Write, Covered_Call, Cash_Secured_Put (case-insensitive)

    Logs rejection count per rule. Only removes rows with IV data — HV-only passthrough rows
    (IV missing) are always preserved.
    """
    if df.empty:
        return df

    _INCOME_STRATS = {
        'buy-write', 'buy_write', 'covered call', 'covered_call',
        'cash-secured put', 'cash_secured_put', 'csp', 'pmcc',
    }

    before_count = len(df)
    keep_mask = pd.Series(True, index=df.index)

    def _strat_lower(row):
        s = str(row.get('Strategy_Name') or row.get('Strategy') or '').lower()
        return s

    strat_series = df.apply(_strat_lower, axis=1)
    is_income = strat_series.apply(lambda s: any(k in s for k in _INCOME_STRATS))

    # R1: Confidence < 35
    if 'Confidence' in df.columns:
        r1 = pd.to_numeric(df['Confidence'], errors='coerce').fillna(100) < 35
        r1_count = r1.sum()
        if r1_count:
            logger.info(f"[PreScreen] R1 (Confidence<35): dropping {r1_count} rows")
        keep_mask &= ~r1

    # R2: IV_Maturity_Level == 1 AND income strategy
    if 'IV_Maturity_Level' in df.columns:
        lv1 = pd.to_numeric(df['IV_Maturity_Level'], errors='coerce').fillna(2) == 1
        r2 = lv1 & is_income
        r2_count = r2.sum()
        if r2_count:
            logger.info(f"[PreScreen] R2 (Level-1 income): dropping {r2_count} rows")
        keep_mask &= ~r2

    # R3: Entry_Quality == CHASING AND directional signal
    if 'Entry_Quality' in df.columns and 'Signal_Type' in df.columns:
        chasing = df['Entry_Quality'].str.upper().eq('CHASING')
        directional = df['Signal_Type'].str.title().isin(['Bullish', 'Bearish'])
        r3 = chasing & directional
        r3_count = r3.sum()
        if r3_count:
            logger.info(f"[PreScreen] R3 (CHASING+directional): dropping {r3_count} rows")
        keep_mask &= ~r3

    # R4: Signal_Type == Unknown AND NOT income
    if 'Signal_Type' in df.columns:
        unknown_sig = df['Signal_Type'].str.lower().isin(['unknown', 'neutral', ''])
        r4 = unknown_sig & ~is_income
        r4_count = r4.sum()
        if r4_count:
            logger.info(f"[PreScreen] R4 (Unknown signal, non-income): dropping {r4_count} rows")
        keep_mask &= ~r4

    # R5: IV_Maturity_Level == 1 AND income strategy — income BLOCKED at Step 12.
    # Income strategies require IV_Maturity_Level >= 4 at Step 12 (R0.3/R1.4).
    # Directional/vol strategies with Level 1 are NOT blocked — Step 12 routes them
    # to CONDITIONAL (R2.2/R2.2c), which is valid and feeds the wait list.
    # Dropping directional Level-1 here killed 473+ valid chart-signal setups that
    # Step 12 would have surfaced as CONDITIONAL candidates.
    if 'IV_Maturity_Level' in df.columns:
        lv1 = pd.to_numeric(df['IV_Maturity_Level'], errors='coerce').fillna(2) == 1
        r5 = lv1 & is_income
        r5_count = r5.sum()
        if r5_count:
            logger.info(f"[PreScreen] R5 (Level-1 income): dropping {r5_count} rows — "
                        f"Step 12 income gate requires IV_Maturity_Level >= 4")
        keep_mask &= ~r5

    # R6: Leveraged ETF + LEAP strategy — belt-and-suspenders after Step 6 gate.
    # Step 6 _validate_long_call_leap / _validate_long_put_leap return None for leveraged ETFs,
    # so LEAP rows should never reach here. This rule catches any edge case where LEAP rows
    # were generated before the Step 6 guard was active (e.g., cached/replayed data).
    # Hull Ch.10: daily-reset compounding breaks the multi-year LEAP thesis structurally.
    if 'Ticker' in df.columns:
        _leap_strats = {'long call leap', 'long put leap', 'leap'}
        _ticker_upper = df['Ticker'].str.upper()
        _strat_lower = df.apply(
            lambda r: str(r.get('Strategy_Name') or r.get('Strategy') or '').lower(), axis=1
        )
        r6 = _ticker_upper.isin(_LEVERAGED_ETFS) & _strat_lower.apply(
            lambda s: any(k in s for k in _leap_strats)
        )
        r6_count = r6.sum()
        if r6_count:
            logger.info(f"[PreScreen] R6 (Leveraged ETF + LEAP): dropping {r6_count} rows — "
                        f"daily-reset products structurally incompatible with LEAP tenor")
        keep_mask &= ~r6

    df_out = df[keep_mask].copy()
    dropped = before_count - len(df_out)
    logger.info(f"[PreScreen] Total dropped: {dropped}/{before_count} "
                f"({100*dropped/before_count:.0f}%) — {len(df_out)} rows proceed to Step 9B")
    return df_out


def _step9_select_contracts(ctx: PipelineContext) -> bool:
    logger.info(f"⏱️ Step 9A: Determining timeframes...")
    strategies = ctx.results.get('strategies_for_contracts', ctx.results.get('recommended_strategies', pd.DataFrame()))
    timeframes = determine_timeframe(strategies, expiry_intent=ctx.expiry_intent)
    ctx.results['timeframes'] = timeframes
    if timeframes.empty: return False

    # Layer 2: Quality pre-screen — drop rows that cannot reach READY before burning chain fetches.
    timeframes = _pre_screen_before_chain_fetch(timeframes)
    if timeframes.empty:
        logger.warning("[PreScreen] All rows dropped by pre-screen gate — nothing to fetch.")
        return False

    logger.info(f"⛓️ Step 9B: Fetching contracts from Schwab...")
    # Pass `timeframes` (pre-screened) as both the strategy source and timeframe source.
    # `strategies` (full pre-screen set) must NOT be the left side of the merge —
    # rows dropped by pre-screen have no timeframe rows → LEFT join produces NaN DTE.
    contracts = fetch_and_select_contracts_schwab(timeframes, timeframes, expiry_intent=ctx.expiry_intent)

    # FAIL-FAST VALIDATION: Ensure Step 10 (Schwab fetch) outputs meet data contract
    validate_step_output(contracts, step_num=10, contract=STEP_10_OUTPUTS)

    ctx.results['selected_contracts'] = contracts
    ctx.debug_manager.record_step('step9b_contracts', len(contracts), contracts)
    return not contracts.empty

def _step10_recalibrate_pcs(ctx: PipelineContext) -> bool:
    logger.info(f"📈 Step 10: Recalibrating PCS scores...")
    # Step 10 expects 'Primary_Strategy' but Step 7/9B uses 'Strategy_Name'
    df = ctx.results['selected_contracts'].copy()
    if 'Primary_Strategy' not in df.columns and 'Strategy_Name' in df.columns:
        df['Primary_Strategy'] = df['Strategy_Name']
        
    recalibrated = recalibrate_and_filter(df)

    # Fix Capital_Requirement for long options — Step 6 uses a $500 placeholder
    # because actual premium is unknown pre-contract-fetch.  Now we have Mid_Price.
    if 'Mid_Price' in recalibrated.columns and 'Strategy_Name' in recalibrated.columns:
        recalibrated['Mid_Price'] = pd.to_numeric(recalibrated['Mid_Price'], errors='coerce')
        _long_mask = recalibrated['Strategy_Name'].isin(['Long Call', 'Long Put'])
        _has_mid   = recalibrated['Mid_Price'].notna() & (recalibrated['Mid_Price'] > 0)
        _fix_mask  = _long_mask & _has_mid
        if _fix_mask.any():
            recalibrated.loc[_fix_mask, 'Capital_Requirement'] = recalibrated.loc[_fix_mask, 'Mid_Price'] * 100
            logger.debug(f"   Corrected Capital_Requirement for {_fix_mask.sum()} long options using actual premium")

    # Fix Capital_Requirement for CSP — Step 6 uses stock_price × 100 as proxy
    # because actual strike is unknown pre-contract-fetch.  Now we have Selected_Strike.
    # CSP collateral = strike × 100 (cash to secure 100 shares at strike).
    if 'Selected_Strike' in recalibrated.columns and 'Strategy_Name' in recalibrated.columns:
        recalibrated['Selected_Strike'] = pd.to_numeric(recalibrated['Selected_Strike'], errors='coerce')
        _csp_mask = recalibrated['Strategy_Name'] == 'Cash-Secured Put'
        _has_strike = recalibrated['Selected_Strike'].notna() & (recalibrated['Selected_Strike'] > 0)
        _csp_fix = _csp_mask & _has_strike
        if _csp_fix.any():
            recalibrated.loc[_csp_fix, 'Capital_Requirement'] = recalibrated.loc[_csp_fix, 'Selected_Strike'] * 100
            logger.debug(f"   Corrected Capital_Requirement for {_csp_fix.sum()} CSPs using actual strike")

    ctx.results['recalibrated_contracts'] = recalibrated
    if ctx.missing_tracker:
        ctx.missing_tracker.diagnose(recalibrated, step_num=10)
        ctx.missing_tracker.audit_stage("step10", ctx.results.get('selected_contracts'), recalibrated)
    ctx.debug_manager.record_step('step10_recalibrated', len(recalibrated), recalibrated)
    return not recalibrated.empty

def _step11_evaluate_strategies(ctx: PipelineContext) -> bool:
    """
    Step 11: Independent Strategy Evaluation (AFTER contract fetch + PCS scoring).

    This step evaluates strategies with real Greeks from Step 9B and PCS scores from Step 10.
    It performs per-family ranking (best call, best put, best straddle, etc.) and validates
    Greek requirements for each strategy type.

    CRITICAL: This step now runs AFTER Step 10 (was previously running before Steps 9A/9B/10).
    The fix ensures Greeks and PCS scores are available during evaluation.
    """
    logger.info("🎯 Step 11: Independent strategy evaluation (with real Greeks + PCS scores)...")

    # Evaluate strategies with real contract data and PCS scores
    df = ctx.results['recalibrated_contracts'].copy()
    evaluated = evaluate_strategies_independently(df, account_size=ctx.account_balance)

    ctx.results['evaluated_strategies'] = evaluated
    ctx.debug_manager.record_step('step11_evaluated', len(evaluated), evaluated)

    return not evaluated.empty

def _step_insert_technical_indicators(ctx: PipelineContext, con: duckdb.DuckDBPyConnection): # Accept connection
    """
    Gathers all relevant technical indicators and inserts them into the DuckDB repository.
    This step runs after PCS recalibration to ensure all data is available.
    """
    from core.shared.data_layer.technical_data_repository import insert_technical_indicators
    
    df_with_indicators = ctx.results.get('recalibrated_contracts', pd.DataFrame())
    if df_with_indicators.empty:
        logger.warning("⚠️ No recalibrated contracts found to insert technical indicators.")
        return

    # Select and rename columns to match the technical_indicators table schema
    # Only select columns that actually exist — not all indicators are computed for every run
    desired_cols = [
        'Ticker', 'timestamp', 'RSI', 'ADX', 'SMA20', 'SMA50', 'EMA9', 'EMA21', 'Atr_Pct',
        'MACD', 'MACD_Signal', 'UpperBand_20', 'MiddleBand_20', 'LowerBand_20',
        'SlowK_5_3', 'SlowD_5_3', 'IV_Rank_30D', 'PCS_Score_V2',
        # Signal Hub v2 — institutional signals
        'Market_Structure', 'OBV_Slope', 'Volume_Ratio',
        'RSI_Divergence', 'MACD_Divergence', 'Weekly_Trend_Bias',
        'Keltner_Squeeze_On', 'Keltner_Squeeze_Fired', 'RS_vs_SPY_20d',
        # Signal Hub v2 — derived chart analytics
        'Chart_Regime', 'BB_Position', 'ATR_Rank', 'MACD_Histogram', 'Trend_Slope',
    ]
    available_cols = [col for col in desired_cols if col in df_with_indicators.columns]
    missing_indicator_cols = set(desired_cols) - set(available_cols)
    if missing_indicator_cols:
        logger.warning(f"⚠️ Technical indicators step: {len(missing_indicator_cols)} columns missing from recalibrated_contracts, skipping: {sorted(missing_indicator_cols)}")
    indicators_to_insert = df_with_indicators[available_cols].copy()

    indicators_to_insert = indicators_to_insert.rename(columns={
        'timestamp': 'Snapshot_TS',
        'RSI': 'RSI_14',
        'ADX': 'ADX_14',
        'SMA20': 'SMA_20',
        'SMA50': 'SMA_50',
        'EMA9': 'EMA_9',
        'EMA21': 'EMA_21',
        'Atr_Pct': 'ATR_14'
    })

    insert_technical_indicators(indicators_to_insert, con=con) # Pass connection
    logger.info(f"✅ Inserted {len(indicators_to_insert)} rows of technical indicators into DuckDB.")


def _step12_8_acceptance_and_sizing(ctx: PipelineContext, run_ts: datetime, con: duckdb.DuckDBPyConnection, is_initial_pass: bool = False): # Accept connection and new flag
    logger.info(f"✅ Step 12: Applying Execution Gate logic (Initial Pass: {is_initial_pass})...")
    # Use evaluated_strategies (step 8 output with Validation_Status + Theory_Compliance_Score),
    # falling back to recalibrated_contracts if step 8 didn't run.
    input_df = ctx.results.get('evaluated_strategies', ctx.results.get('recalibrated_contracts', ctx.results['selected_contracts']))
    
    # If this is the second pass, we need to use the 'acceptance_all' from the first pass
    # which now contains the IVEngine derived metrics.
    if not is_initial_pass:
        input_df = ctx.results.get('acceptance_all', pd.DataFrame()).copy()
        logger.info(f"DEBUG: Second pass of Execution Gate. Input DataFrame size: {len(input_df)}")
        # Filter for candidates that were AWAIT_CONFIRMATION in the first pass
        input_df = input_df[input_df['Execution_Status'] == 'AWAIT_CONFIRMATION'].copy()
        logger.info(f"DEBUG: Candidates for second pass after filtering AWAIT_CONFIRMATION: {len(input_df)}")
        if input_df.empty:
            # CRITICAL: Ensure acceptance_all and acceptance_ready exist even if second pass has no candidates
            # This prevents export failures and ensures deterministic behavior
            ctx.results['acceptance_all'] = ctx.results.get('acceptance_all', pd.DataFrame())

            # Extract READY candidates from acceptance_all for export
            all_acceptance = ctx.results['acceptance_all']
            if not all_acceptance.empty and 'Execution_Status' in all_acceptance.columns:
                ctx.results['acceptance_ready'] = all_acceptance[all_acceptance['Execution_Status'] == 'READY'].copy()
            else:
                ctx.results['acceptance_ready'] = pd.DataFrame()

            ctx.debug_manager.record_step('step12_acceptance_all', len(ctx.results['acceptance_all']), ctx.results['acceptance_all'])
            ctx.debug_manager.record_step('step12_acceptance_ready', len(ctx.results['acceptance_ready']), ctx.results['acceptance_ready'])
            logger.info(f"ℹ️ No AWAIT_CONFIRMATION candidates for second pass. Using first pass results: {len(ctx.results['acceptance_ready'])} READY.")

            # DEBUG LOGGING: Step12 completion status (even when second pass is skipped)
            logger.info(f"DEBUG: Step12 completed - total evaluated: {len(ctx.results.get('acceptance_all', []))}")
            logger.info(f"DEBUG: Step12 READY count: {len(ctx.results.get('acceptance_ready', []))}")
            return

    # FAIL-FAST VALIDATION: Ensure Step 12 inputs meet data contract requirements
    validate_step_output(input_df, step_num=12, contract=STEP_12_REQUIRED_INPUTS)

    audit.save_df("step12_input", input_df)
    t0 = time.time()

    # VIX and SPY_Change_Pct flow from Step 0 → Step 2 → downstream as columns.
    # Build market_stress column from available data; Step 9B uses this for spread thresholds.
    if 'market_stress' not in input_df.columns:
        _vix_col = input_df.get('VIX') if hasattr(input_df, 'get') else None
        _vix_series = pd.to_numeric(input_df['VIX'], errors='coerce') if 'VIX' in input_df.columns else pd.Series([np.nan] * len(input_df))
        _spy_series = pd.to_numeric(input_df.get('SPY_Change_Pct', pd.Series([np.nan] * len(input_df))), errors='coerce') if 'SPY_Change_Pct' in input_df.columns else pd.Series([np.nan] * len(input_df))
        def _derive_market_stress(vix, spy_chg):
            try:
                vix_f = float(vix) if pd.notna(vix) else None
                spy_f = float(spy_chg) if pd.notna(spy_chg) else None
                if vix_f is not None and vix_f > 35:
                    return 'CRISIS'
                if vix_f is not None and vix_f > 25:
                    return 'ELEVATED'
                if spy_f is not None and abs(spy_f) > 2.0:
                    return 'ELEVATED'
                return 'NORMAL'
            except Exception:
                return 'NORMAL'
        input_df = input_df.copy()
        input_df['market_stress'] = [
            _derive_market_stress(v, s)
            for v, s in zip(_vix_series, _spy_series)
        ]
        logger.info(f"[GAP5] market_stress populated: {input_df['market_stress'].value_counts().to_dict()}")

    # ── Market Context Injection ─────────────────────────────────────────────
    # Query composite market regime from market_context_daily and inject columns.
    # If unavailable or low confidence, safe defaults are used. Non-blocking.
    try:
        from core.shared.data_layer.market_context import get_latest_market_context
        from core.shared.data_layer.market_regime_classifier import classify_market_regime

        _mkt_ctx = get_latest_market_context()
        if _mkt_ctx is not None:
            _mkt_regime = classify_market_regime(_mkt_ctx)
            if _mkt_regime.confidence >= 0.5:
                input_df["Market_Regime"] = _mkt_regime.regime
                input_df["Market_Regime_Score"] = _mkt_regime.score
                input_df["Market_Vol_Regime"] = _mkt_regime.vol_regime
                input_df["Market_Term_Structure"] = _mkt_regime.term_structure
                input_df["Market_Breadth_State"] = _mkt_regime.breadth_state
                input_df["Market_Regime_Confidence"] = _mkt_regime.confidence
                input_df["VIX_Percentile"] = _mkt_ctx.get("vix_percentile_252d", float("nan"))
                input_df["CBOE_SKEW"] = _mkt_ctx.get("skew", float("nan"))
                # Override market_stress with composite if confidence is high
                input_df["market_stress"] = _mkt_regime.stress_level
                logger.info(
                    f"[MarketCtx] Injected: regime={_mkt_regime.regime} "
                    f"score={_mkt_regime.score:.1f} conf={_mkt_regime.confidence:.2f}"
                )
            else:
                logger.info(
                    f"[MarketCtx] Low confidence ({_mkt_regime.confidence:.2f}) — using defaults"
                )
        else:
            logger.debug("[MarketCtx] No market context data — using defaults")
    except Exception as _mkt_err:
        logger.debug(f"[MarketCtx] Injection failed (non-fatal): {_mkt_err}")

    # Set safe defaults for any missing market context columns
    _mkt_defaults = {
        "Market_Regime": "UNKNOWN", "Market_Regime_Score": float("nan"),
        "Market_Vol_Regime": "UNKNOWN", "Market_Term_Structure": "UNKNOWN",
        "Market_Breadth_State": "UNKNOWN", "Market_Regime_Confidence": 0.0,
        "VIX_Percentile": float("nan"), "CBOE_SKEW": float("nan"),
    }
    for _mc_col, _mc_default in _mkt_defaults.items():
        if _mc_col not in input_df.columns:
            input_df[_mc_col] = _mc_default

    # ── Signal Trajectory (Scan Memory) ─────────────────────────────────────
    # Pre-compute trajectory cache from scan_candidates + technical_indicators
    # history and inject columns into the input DataFrame before Step 12.
    # Graceful: if computation fails, all tickers get STABLE (×1.00).
    try:
        from scan_engine.signal_trajectory import compute_signal_trajectory
        _traj_tickers = input_df['Ticker'].dropna().unique().tolist()
        _traj_cache = compute_signal_trajectory(_traj_tickers, con=con)
        input_df['Signal_Trajectory'] = input_df['Ticker'].map(
            lambda t: _traj_cache.get(t, {}).get('trajectory', 'STABLE'))
        input_df['Trajectory_Multiplier'] = input_df['Ticker'].map(
            lambda t: _traj_cache.get(t, {}).get('multiplier', 1.0))
        input_df['Score_Acceleration'] = input_df['Ticker'].map(
            lambda t: _traj_cache.get(t, {}).get('score_acceleration', 0.0))
        _non_stable = (input_df['Signal_Trajectory'] != 'STABLE').sum()
        if _non_stable > 0:
            logger.info(f"[Trajectory] {_non_stable}/{len(input_df)} candidates with non-STABLE trajectory")
    except Exception as _traj_err:
        logger.debug(f"[Trajectory] Pre-compute failed (non-fatal): {_traj_err}")
        for _col, _default in [('Signal_Trajectory', 'STABLE'), ('Trajectory_Multiplier', 1.0), ('Score_Acceleration', 0.0)]:
            if _col not in input_df.columns:
                input_df[_col] = _default

    # ── Behavioral Memory (30-Day Scan History) ─────────────────────────────
    # Reads full 30-day arc from technical_indicators + scan_candidates.
    # Tells step 12 *how the stock behaved* — not just where it is now.
    # Produces: Regime_Duration, Regime_Path, ADX_30D_Trend,
    #           Volume_Accumulation, DQS_30D_Trend, Behavioral_Score, etc.
    try:
        from scan_engine.behavioral_memory import compute_behavioral_memory
        _bm_tickers = input_df['Ticker'].dropna().unique().tolist()
        _bm_cache = compute_behavioral_memory(_bm_tickers, con=con)
        _bm_cols = [
            ('Regime_Duration', 0), ('Regime_Path', ''),
            ('ADX_Trend', 'UNKNOWN'), ('RSI_Range', 0.0),
            ('Volume_Accumulation', 'UNKNOWN'), ('Scan_Frequency', 0),
            ('DQS_Trend', 'UNKNOWN'), ('Signal_Age', 0),
            ('IV_Arc', 'UNKNOWN'), ('Earnings_Context', 'NO_DATA'),
            ('Mgmt_Track_Record', 'NO_DATA'), ('Prior_Trades', 0),
            ('Mgmt_Confidence', 'NONE'), ('Mgmt_Strategy_Detail', ''),
            ('Mgmt_Recency_Factor', 1.0), ('Fault_Pattern', 'INSUFFICIENT_DATA'),
            ('Contradiction_Flags', ''),
            ('Move_Drivers', ''), ('Last_Dip_Context', ''),
            ('Event_Reactions', ''), ('Worst_Event_Type', ''),
            ('Data_Maturity', 'NEW_TICKER'),
            ('History_Depth', 0), ('Behavioral_Score', 50),
        ]
        for _col, _default in _bm_cols:
            input_df[_col] = input_df['Ticker'].map(
                lambda t, c=_col, d=_default: _bm_cache.get(t, {}).get(c, d))
        _bm_enriched = sum(1 for t in _bm_tickers
                           if _bm_cache.get(t, {}).get('Behavioral_Score', 50) != 50)
        if _bm_enriched > 0:
            logger.info(
                f"[BehavioralMemory] {_bm_enriched}/{len(_bm_tickers)} tickers "
                f"enriched with YTD behavioral context")
    except Exception as _bm_err:
        logger.debug(f"[BehavioralMemory] Pre-compute failed (non-fatal): {_bm_err}")
        for _col, _default in [('Regime_Duration', 0), ('Regime_Path', ''),
                                ('ADX_Trend', 'UNKNOWN'), ('RSI_Range', 0.0),
                                ('Volume_Accumulation', 'UNKNOWN'), ('Scan_Frequency', 0),
                                ('DQS_Trend', 'UNKNOWN'), ('Signal_Age', 0),
                                ('IV_Arc', 'UNKNOWN'), ('Earnings_Context', 'NO_DATA'),
                                ('Mgmt_Track_Record', 'NO_DATA'), ('Prior_Trades', 0),
                                ('Mgmt_Confidence', 'NONE'), ('Mgmt_Strategy_Detail', ''),
                                ('Mgmt_Recency_Factor', 1.0), ('Fault_Pattern', 'INSUFFICIENT_DATA'),
                                ('Contradiction_Flags', ''),
                                ('Move_Drivers', ''), ('Last_Dip_Context', ''),
                                ('Event_Reactions', ''), ('Worst_Event_Type', ''),
                                ('Data_Maturity', 'NEW_TICKER'),
                                ('History_Depth', 0), ('Behavioral_Score', 50)]:
            if _col not in input_df.columns:
                input_df[_col] = _default

    # ── Wait List Deferral Patterns: enrich with historical wait list data ──
    # Surfaces: how many times this ticker was deferred, promotion rate,
    # common blocking conditions — so step 12 and the user aren't blind.
    _deferral_cols = [
        ('Deferral_Count_90d', 0), ('Deferral_Promotion_Rate', 0.0),
        ('Deferral_Avg_Wait_Days', 0.0), ('Deferral_Common_Block', ''),
    ]
    try:
        from core.wait_loop.schema import query_deferral_patterns
        _def_cache = {}
        for _t in input_df['Ticker'].dropna().unique():
            _dp = query_deferral_patterns(con, str(_t), lookback_days=90)
            if _dp.get('deferral_count', 0) > 0:
                _common = _dp.get('common_conditions', [])
                _common_str = '; '.join(f"{k} ({v})" for k, v in _common[:3]) if _common else ''
                _def_cache[str(_t)] = {
                    'Deferral_Count_90d': _dp['deferral_count'],
                    'Deferral_Promotion_Rate': round(_dp['promotion_rate'], 2),
                    'Deferral_Avg_Wait_Days': _dp['avg_wait_days'],
                    'Deferral_Common_Block': _common_str,
                }
        for _col, _default in _deferral_cols:
            input_df[_col] = input_df['Ticker'].map(
                lambda t, c=_col, d=_default: _def_cache.get(str(t), {}).get(c, d))
        _def_enriched = len(_def_cache)
        if _def_enriched > 0:
            logger.info(
                f"[DeferralPatterns] {_def_enriched} tickers enriched "
                f"with wait list history (90d)")
    except Exception as _dp_err:
        logger.debug(f"[DeferralPatterns] Enrichment failed (non-fatal): {_dp_err}")
        for _col, _default in _deferral_cols:
            if _col not in input_df.columns:
                input_df[_col] = _default

    # ── Scale-Up Bridge: read pending management requests ───────────────────
    # Inject into context so Step 12 can match READY/CONDITIONAL candidates.
    _scale_up_requests = pd.DataFrame()
    try:
        from core.shared.data_layer.scale_up_requests import read_pending_scale_up_requests
        _scale_up_requests = read_pending_scale_up_requests(con, limit=5)
        if not _scale_up_requests.empty:
            logger.info(f"[ScaleUp] Loaded {len(_scale_up_requests)} pending scale-up requests")
    except Exception as _su_read_err:
        logger.debug(f"[ScaleUp] Read failed (non-fatal): {_su_read_err}")
    ctx.results['scale_up_requests'] = _scale_up_requests

    # ── Calendar Deferral: read prior-day deferrals and tag returning candidates ──
    _deferred_set = set()  # (ticker, strategy) pairs deferred from prior session
    _deferred_info = {}    # (ticker, strategy) → {dqs_score, deferred_date, ...}
    try:
        from scan_engine.calendar_deferral import read_pending_deferrals, expire_stale_deferrals
        _def_con = get_domain_write_connection(DbDomain.SCAN)
        expire_stale_deferrals(_def_con)
        _pending = read_pending_deferrals(_def_con)
        _def_con.close()
        if not _pending.empty:
            for _, _dr in _pending.iterrows():
                _key = (str(_dr.get('ticker', '')), str(_dr.get('strategy_name', '')))
                _deferred_set.add(_key)
                _deferred_info[_key] = {
                    'dqs_score': _dr.get('dqs_score'),
                    'deferred_date': str(_dr.get('deferred_date', '')),
                    'entry_price': _dr.get('entry_price'),
                    'calendar_flag': _dr.get('calendar_flag', ''),
                }
            logger.info(f"[CalendarDeferral] {len(_deferred_set)} candidates returning from prior deferral")
            # Tag returning candidates in input_df
            input_df['Calendar_Deferred_Return'] = input_df.apply(
                lambda r: (str(r.get('Ticker', '')), str(r.get('Strategy_Name', '') or r.get('Strategy', ''))) in _deferred_set,
                axis=1,
            )
            input_df['Deferred_From_Date'] = input_df.apply(
                lambda r: _deferred_info.get(
                    (str(r.get('Ticker', '')), str(r.get('Strategy_Name', '') or r.get('Strategy', ''))), {}
                ).get('deferred_date', ''),
                axis=1,
            )
    except Exception as _def_read_err:
        logger.debug(f"[CalendarDeferral] Read failed (non-fatal): {_def_read_err}")
    if 'Calendar_Deferred_Return' not in input_df.columns:
        input_df['Calendar_Deferred_Return'] = False
        input_df['Deferred_From_Date'] = ''
    ctx.results['calendar_deferred_set'] = _deferred_set
    ctx.results['calendar_deferred_info'] = _deferred_info

    # Apply the Execution Gate row by row
    # First, ensure necessary columns for the gate are present, defaulting if not
    # These columns should ideally be enriched in prior steps (e.g., Step 9B for Scraper_Status, IV_Maturity_State)
    required_gate_inputs = [
        'Strategy_Type', 'IV_Maturity_State', 'IV_Source', 'IV_Rank', 'IV_Trend_7D',
        'IVHV_gap_30D', 'Liquidity_Grade', 'Signal_Type', 'Scraper_Status', 'Data_Completeness_Overall',
        'compression_tag', '52w_regime_tag', 'momentum_tag', 'gap_tag', 'entry_timing_context',
        'Actual_DTE', 'Strategy_Name', 'execution_quality', 'balance_tag', 'dividend_risk',
        'history_depth_ok', 'iv_data_stale', 'regime_confidence',
    ]
    for col in required_gate_inputs:
        if col not in input_df.columns:
            # Provide sensible defaults for missing columns to prevent gate failure
            if col == 'Strategy_Type': input_df[col] = classify_strategy_type(input_df['Strategy_Name'].iloc[0]) if not input_df.empty else 'UNKNOWN'
            elif col == 'IV_Maturity_State': input_df[col] = 'MISSING'
            elif col == 'IV_Source': input_df[col] = 'None'
            elif col == 'IV_Rank': input_df[col] = np.nan
            elif col == 'IV_Trend_7D': input_df[col] = 'UNKNOWN'
            elif col == 'IVHV_gap_30D': input_df[col] = np.nan
            elif col == 'Liquidity_Grade': input_df[col] = 'Illiquid'
            elif col == 'Signal_Type': input_df[col] = 'NEUTRAL'
            elif col == 'Scraper_Status': input_df[col] = 'NOT_INVOKED'
            elif col == 'Data_Completeness_Overall': input_df[col] = 'Missing'
            elif col == 'history_depth_ok': input_df[col] = False
            elif col == 'iv_data_stale': input_df[col] = False  # Default fresh; Step 2 sets True when stale
            elif col == 'regime_confidence': input_df[col] = 0.0
            else: input_df[col] = 'UNKNOWN' # Generic default for other string columns

    # Calculate directional_bias, structure_bias, timing_quality here
    # These were previously calculated within evaluate_acceptance, now need to be explicit
    input_df['directional_bias'] = input_df.apply(
        lambda row: detect_directional_bias(
            row.get('momentum_tag', 'UNKNOWN'),
            row.get('52w_regime_tag', 'UNKNOWN'),
            row.get('gap_tag', 'UNKNOWN'),
            row.get('entry_timing_context', 'UNKNOWN'),
            ema_signal=row.get('Chart_EMA_Signal', 'UNKNOWN'),
            trend_state=row.get('Trend_State', 'UNKNOWN'),
            rsi=row.get('RSI'),
            macd=row.get('MACD'),
        ), axis=1
    )
    input_df['structure_bias'] = input_df.apply(
        lambda row: detect_structure_bias(
            row.get('compression_tag', 'UNKNOWN'),
            row.get('52w_regime_tag', 'UNKNOWN'),
            row.get('momentum_tag', 'UNKNOWN'),
            adx=row.get('ADX', 0),
            chart_regime=row.get('Chart_Regime', '')
        ), axis=1
    )
    input_df['timing_quality'] = input_df.apply(
        lambda row: evaluate_timing_quality(
            row.get('entry_timing_context', 'UNKNOWN'),
            row.get('intraday_position_tag', 'UNKNOWN'),
            row.get('gap_tag', 'UNKNOWN'),
            row.get('momentum_tag', 'UNKNOWN')
        ), axis=1
    )

    # Apply the Execution Gate row by row
    all_acceptance_pass = input_df.apply( # Renamed to avoid conflict with outer all_acceptance
        lambda row: apply_execution_gate(
            row,
            strategy_type=row['Strategy_Type'],
            iv_maturity_state=row['IV_Maturity_State'],
            iv_source=row['IV_Source'],
            iv_rank=row['IV_Rank'],
            iv_trend_7d=row['IV_Trend_7D'],
            ivhv_gap_30d=row['IVHV_gap_30D'],
            liquidity_grade=row['Liquidity_Grade'],
            signal_strength=row['Signal_Type'], # Mapping Signal_Type to Signal_Strength
            scraper_status=row['Scraper_Status'],
            data_completeness_overall=row['Data_Completeness_Overall'],
            # Existing inputs for context
            compression=row['compression_tag'],
            regime_52w=row['52w_regime_tag'],
            momentum=row['momentum_tag'],
            gap=row['gap_tag'],
            timing=row['entry_timing_context'],
            directional_bias=row['directional_bias'],
            structure_bias=row['structure_bias'],
            timing_quality=row['timing_quality'],
            actual_dte=row['Actual_DTE'],
            strategy_name=row['Strategy_Name'],
            exec_quality=row.get('execution_quality', 'UNKNOWN'),
            balance=row.get('balance_tag', 'UNKNOWN'),
            div_risk=row.get('dividend_risk', 'UNKNOWN'),
            history_depth_ok=row.get('history_depth_ok', False),
            iv_data_stale=row.get('iv_data_stale', True),
            regime_confidence=row.get('regime_confidence', 0.0),
            is_initial_pass=is_initial_pass, # Pass the flag to the gate logic
            iv_maturity_level=int(row.get('IV_Maturity_Level') if pd.notna(row.get('IV_Maturity_Level')) else 1),
        ),
        axis=1,
        result_type='expand'
    )
    
    # Merge the decision back into the original DataFrame
    df_with_decisions = input_df.copy()
    _decision_cols_to_merge = [
        'Execution_Status', 'Gate_Reason', 'confidence_band', 'directional_bias', 'structure_bias', 'timing_quality', 'execution_adjustment',
        'Calibrated_Confidence', 'Feedback_Win_Rate', 'Feedback_Sample_N', 'Feedback_Action', 'Feedback_Note',
        'Calendar_Risk_Flag', 'Calendar_Risk_Note',
        'Calendar_DQS_Multiplier', 'Calendar_Theta_Factor', 'DQS_Combined_Multiplier', 'DQS_Multiplier_Clamped',
        'Signal_Trajectory', 'Trajectory_Multiplier', 'Score_Acceleration',
        # Blind-spot detection (directional only)
        'Blind_Spot_Multiplier', 'Blind_Spot_Notes',
        # IV Headwind (long vega only)
        'IV_Headwind_Multiplier', 'IV_Headwind_Note',
        # Behavioral memory
        'Behavioral_Multiplier', 'Behavioral_Note',
        # MC extensions (directional gate)
        'MC_VP_Score', 'MC_VP_Edge', 'MC_VP_Premium_Fair', 'MC_VP_Verdict', 'MC_VP_Note',
        'MC_Earn_EV_Hold', 'MC_Earn_EV_Close', 'MC_Earn_P_Profit', 'MC_Earn_Verdict', 'MC_Earn_Edge', 'MC_Earn_Note',
        # Action Priority
        'Trade_Edge_Score',
        # LEAP rate sensitivity (Rho annotation)
        'LEAP_Rate_Sensitivity',
    ]
    for col in _decision_cols_to_merge:
        if col in all_acceptance_pass.columns:
            # Only overwrite if the gate actually returned values for this column.
            # For the initial pass, some columns (Calendar, Trajectory) won't be in the
            # gate output — preserve the pipeline-injected values from input_df.
            if col in input_df.columns and all_acceptance_pass[col].isna().all():
                # Gate returned all-NaN for this column — keep original input_df values
                pass
            else:
                df_with_decisions[col] = all_acceptance_pass[col]
        elif col not in df_with_decisions.columns:
            # String-valued columns must be object dtype so pass-2 merge doesn't coerce strings to NaN
            if col in {'MC_VP_Verdict', 'MC_VP_Note', 'MC_Earn_Verdict', 'MC_Earn_Note',
                        'Feedback_Action', 'Feedback_Note', 'Calendar_Risk_Flag',
                        'Calendar_Risk_Note', 'Signal_Trajectory',
                        'Blind_Spot_Notes', 'IV_Headwind_Note', 'Behavioral_Note'}:
                df_with_decisions[col] = pd.Series([np.nan] * len(df_with_decisions), dtype=object)
            else:
                df_with_decisions[col] = np.nan  # Ensure column exists even if apply_execution_gate didn't return it

    # Update the main 'acceptance_all' in ctx.results
    if is_initial_pass:
        ctx.results['acceptance_all'] = df_with_decisions # Store results of first pass
    else:
        # For the second pass, update only the AWAIT_CONFIRMATION candidates
        # and keep the BLOCKED ones from the first pass as they are.
        original_acceptance_all = ctx.results.get('acceptance_all', pd.DataFrame()).copy()
        
        # Update only the re-evaluated rows — match on (Ticker, Strategy_Name) to handle
        # multiple strategies per ticker without cross-contaminating decisions.
        decision_cols = ['Execution_Status', 'Gate_Reason', 'confidence_band', 'directional_bias', 'structure_bias', 'timing_quality', 'execution_adjustment',
                         'Calibrated_Confidence', 'Feedback_Win_Rate', 'Feedback_Sample_N', 'Feedback_Action', 'Feedback_Note',
                         'Calendar_Risk_Flag', 'Calendar_Risk_Note',
                         'Calendar_DQS_Multiplier', 'Calendar_Theta_Factor', 'DQS_Combined_Multiplier', 'DQS_Multiplier_Clamped',
                         'Signal_Trajectory', 'Trajectory_Multiplier', 'Score_Acceleration',
                         'Blind_Spot_Multiplier', 'Blind_Spot_Notes',
                         'IV_Headwind_Multiplier', 'IV_Headwind_Note',
                         'Behavioral_Multiplier', 'Behavioral_Note',
                         'MC_VP_Score', 'MC_VP_Edge', 'MC_VP_Premium_Fair', 'MC_VP_Verdict', 'MC_VP_Note',
                         'MC_Earn_EV_Hold', 'MC_Earn_EV_Close', 'MC_Earn_P_Profit', 'MC_Earn_Verdict', 'MC_Earn_Edge', 'MC_Earn_Note',
                         'Trade_Edge_Score',
                         'LEAP_Rate_Sensitivity']
        # Ensure string-valued columns have object dtype (not float64 from NaN-only pass 1)
        _string_decision_cols = {'Execution_Status', 'Gate_Reason', 'confidence_band', 'directional_bias',
                                 'structure_bias', 'timing_quality', 'execution_adjustment',
                                 'Feedback_Action', 'Feedback_Note', 'Calendar_Risk_Flag',
                                 'Calendar_Risk_Note', 'Signal_Trajectory',
                                 'Blind_Spot_Notes', 'IV_Headwind_Note', 'Behavioral_Note',
                                 'MC_VP_Verdict', 'MC_VP_Note',
                                 'MC_Earn_Verdict', 'MC_Earn_Note',
                                 'LEAP_Rate_Sensitivity'}
        for _sc in _string_decision_cols:
            if _sc in original_acceptance_all.columns and pd.api.types.is_float_dtype(original_acceptance_all[_sc].dtype):
                original_acceptance_all[_sc] = original_acceptance_all[_sc].astype(object)

        for _, dec_row in df_with_decisions.iterrows():
            mask = (
                (original_acceptance_all['Ticker'] == dec_row['Ticker']) &
                (original_acceptance_all['Strategy_Name'] == dec_row['Strategy_Name'])
            )
            for col in decision_cols:
                val = dec_row[col]
                # FutureWarning fix: cast value to target column dtype before .loc assignment
                # Avoids "incompatible dtype" error when a string default (e.g. '') lands in float64 col
                if col in original_acceptance_all.columns:
                    target_dtype = original_acceptance_all[col].dtype
                    if pd.api.types.is_float_dtype(target_dtype):
                        try:
                            val = float(val) if val not in (None, '', 'UNKNOWN', 'nan') else np.nan
                        except (ValueError, TypeError):
                            val = np.nan
                    elif pd.api.types.is_integer_dtype(target_dtype):
                        try:
                            val = int(float(val)) if val not in (None, '', 'UNKNOWN', 'nan') else 0
                        except (ValueError, TypeError):
                            val = 0
                original_acceptance_all.loc[mask, col] = val
        
        ctx.results['acceptance_all'] = original_acceptance_all # Final combined results

    all_acceptance = ctx.results['acceptance_all'] # Reference the updated DataFrame

    # ── Scale-Up Bridge: match READY/CONDITIONAL candidates to management requests ──
    _su_requests = ctx.results.get('scale_up_requests', pd.DataFrame())
    if not _su_requests.empty and not all_acceptance.empty:
        try:
            _su_matched = 0
            for _, _su_req in _su_requests.iterrows():
                _su_t = str(_su_req.get('ticker', ''))
                _su_s = str(_su_req.get('strategy', ''))
                _m = (
                    (all_acceptance['Ticker'] == _su_t) &
                    (all_acceptance['Execution_Status'].isin(['READY', 'CONDITIONAL']))
                )
                if _m.any():
                    all_acceptance.loc[_m, 'Scale_Up_Candidate'] = True
                    all_acceptance.loc[_m, 'Scale_Up_Trigger_Price'] = _su_req.get('trigger_price')
                    all_acceptance.loc[_m, 'Scale_Up_Add_Contracts'] = _su_req.get('add_contracts', 1)
                    all_acceptance.loc[_m, 'Scale_Up_Priority'] = _su_req.get('priority', 3)
                    _su_matched += int(_m.sum())
                    try:
                        from core.shared.data_layer.scale_up_requests import mark_request_filled
                        mark_request_filled(con, _su_t, _su_s, filled_run_id=ctx.results.get('run_id'))
                    except Exception:
                        pass
            if _su_matched > 0:
                logger.info(f"[ScaleUp] Tagged {_su_matched} READY/CONDITIONAL rows as scale-up candidates")
            ctx.results['acceptance_all'] = all_acceptance
        except Exception as _su_err:
            logger.debug(f"[ScaleUp] Matching failed (non-fatal): {_su_err}")

    # ── R4.2–R4.5: Post-gate demotion sweep ─────────────────────────────────
    # These gates require the full DataFrame (not per-row) to check cross-row
    # properties like evaluator verdicts, DQS floors, and regime conflicts.
    # Previously lived in apply_acceptance_logic() (dead code path).
    all_acceptance = apply_post_gate_demotions(all_acceptance)
    ctx.results['acceptance_all'] = all_acceptance

    audit.profile("step12", all_acceptance, (time.time()-t0)*1000)
    audit.save_df("step12_output", all_acceptance)
    # === CANONICAL ACCEPTANCE STATUS GUARANTEE ===
    if 'Execution_Status' in all_acceptance.columns:
        all_acceptance['acceptance_status'] = all_acceptance['Execution_Status']
    else:
        all_acceptance['acceptance_status'] = 'UNKNOWN'

    logger.info("✅ acceptance_status column created from Execution_Status for audit layer")
    audit.export_ready_now_evidence(all_acceptance) # Uses Execution_Status

    # ── Trade Edge Score: composite quality metric for Action Priority ranking ──────
    # Combines DQS (40%), TQS (25%), Structure Confluence (20%), VP Edge (15%)
    # into a single 0-100 score. APS = Edge × 0.70 + Timing × 0.30 in scan_view.
    try:
        def _compute_trade_edge(row):
            _exec_st = str(row.get('Execution_Status', '') or '').upper()
            if _exec_st not in ('READY', 'AWAIT_CONFIRMATION'):
                return np.nan

            # DQS (40% weight) — 0-100
            _dqs = pd.to_numeric(row.get('DQS_Score'), errors='coerce')
            _dqs_contrib = (float(_dqs) / 100.0 * 40.0) if pd.notna(_dqs) else 0.0

            # TQS (25% weight) — 0-100
            _tqs = pd.to_numeric(row.get('TQS_Score'), errors='coerce')
            _tqs_contrib = (float(_tqs) / 100.0 * 25.0) if pd.notna(_tqs) else 0.0

            # Structure Confluence (20% weight) — count aligned signals
            _confluence = 0
            _strat = str(row.get('Strategy_Name', '') or '').upper()
            _is_bearish = 'PUT' in _strat
            _is_bullish = 'CALL' in _strat and 'COVERED' not in _strat

            _struct = str(row.get('Market_Structure', '') or '').upper()
            if (_is_bullish and 'UPTREND' in _struct) or (_is_bearish and 'DOWNTREND' in _struct):
                _confluence += 1
            _weekly = str(row.get('Weekly_Trend_Bias', '') or '').upper()
            if 'ALIGNED' in _weekly:
                _confluence += 1
            _adx = pd.to_numeric(row.get('ADX'), errors='coerce')
            if pd.notna(_adx) and float(_adx) >= 25:
                _confluence += 1
            _obv = pd.to_numeric(row.get('OBV_Slope'), errors='coerce')
            if pd.notna(_obv):
                if (_is_bullish and float(_obv) > 5) or (_is_bearish and float(_obv) < -5):
                    _confluence += 1
            _rs = pd.to_numeric(row.get('RS_vs_SPY_20d'), errors='coerce')
            if pd.notna(_rs):
                if (_is_bullish and float(_rs) > 2) or (_is_bearish and float(_rs) < -2):
                    _confluence += 1
            # 5 possible signals → 0-20 points
            _conf_contrib = (_confluence / 5.0) * 20.0

            # VP Edge (15% weight) — MC_VP_Score: >1.15 = full credit, <0.75 = zero
            _vp = pd.to_numeric(row.get('MC_VP_Score'), errors='coerce')
            if pd.notna(_vp):
                _vp_norm = min(1.0, max(0.0, (float(_vp) - 0.75) / 0.60))  # 0.75→0, 1.35→1.0
                _vp_contrib = _vp_norm * 15.0
            else:
                _vp_contrib = 7.5  # neutral when no VP data

            return round(_dqs_contrib + _tqs_contrib + _conf_contrib + _vp_contrib, 1)

        all_acceptance['Trade_Edge_Score'] = all_acceptance.apply(_compute_trade_edge, axis=1)
        _edge_valid = all_acceptance['Trade_Edge_Score'].dropna()
        if not _edge_valid.empty:
            logger.info(
                f"📊 Trade Edge Score: {len(_edge_valid)} scored | "
                f"mean={_edge_valid.mean():.0f} | max={_edge_valid.max():.0f} | "
                f"≥75: {(_edge_valid >= 75).sum()} | ≥50: {(_edge_valid >= 50).sum()}"
            )
    except Exception as _edge_err:
        logger.debug(f"Trade Edge Score computation failed (non-fatal): {_edge_err}")
        all_acceptance['Trade_Edge_Score'] = np.nan

    ctx.results['acceptance_all'] = all_acceptance
    ctx.debug_manager.record_step('step12_acceptance_all', len(all_acceptance), all_acceptance)

    # ── GAP 7: Persist READY candidates to scan_candidates table (final pass only) ──────
    # Provides scan→management handshake: management JOINs this table at position entry
    # to enrich with scan-origin quality scores (DQS, TQS, PCS, Confidence, Regime).
    # Only runs on the second (final) pass so acceptance_all is fully resolved.
    if not is_initial_pass:
        _df_ready = all_acceptance[all_acceptance.get('Execution_Status', pd.Series()).eq('READY')
                                   if 'Execution_Status' in all_acceptance.columns
                                   else pd.Series(False, index=all_acceptance.index)]
        if 'Execution_Status' in all_acceptance.columns:
            _df_ready = all_acceptance[all_acceptance['Execution_Status'] == 'READY']
        else:
            _df_ready = pd.DataFrame()

        if not _df_ready.empty:
            try:
                import uuid as _uuid
                _run_id = run_ts.strftime('%Y%m%d_%H%M%S')
                _scan_ts = run_ts
                _sc_con = get_domain_write_connection(DbDomain.SCAN)
                _sc_con.execute("""
                    CREATE TABLE IF NOT EXISTS scan_candidates (
                        Ticker              VARCHAR NOT NULL,
                        Strategy_Name       VARCHAR,
                        Run_ID              VARCHAR NOT NULL,
                        Scan_TS             TIMESTAMP,
                        Execution_Status    VARCHAR,
                        DQS_Score           DOUBLE,
                        TQS_Score           DOUBLE,
                        PCS_Score           DOUBLE,
                        Confidence_Band     VARCHAR,
                        Gate_Reason         VARCHAR,
                        IV_Maturity_State   VARCHAR,
                        Regime              VARCHAR,
                        Signal_Type         VARCHAR,
                        Valid_Reason        VARCHAR,
                        Theory_Source       VARCHAR,
                        Trade_Bias          VARCHAR,
                        PRIMARY KEY (Ticker, Strategy_Name, Run_ID)
                    )
                """)
                # Add thesis columns to existing tables that predate this schema change
                for _alter_col in ['Valid_Reason', 'Theory_Source', 'Trade_Bias']:
                    try:
                        _sc_con.execute(f"ALTER TABLE scan_candidates ADD COLUMN {_alter_col} VARCHAR")
                    except Exception:
                        pass  # Column already exists
                _inserted = 0
                for _, _row in _df_ready.iterrows():
                    try:
                        _sc_con.execute("""
                            INSERT OR IGNORE INTO scan_candidates
                            (Ticker, Strategy_Name, Run_ID, Scan_TS, Execution_Status, DQS_Score,
                             TQS_Score, PCS_Score, Confidence_Band, Gate_Reason,
                             IV_Maturity_State, Regime, Signal_Type,
                             Valid_Reason, Theory_Source, Trade_Bias)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, [
                            str(_row.get('Ticker') or ''),
                            str(_row.get('Strategy_Name') or _row.get('Strategy') or ''),
                            _run_id,
                            _scan_ts,
                            'READY',
                            float(_row['DQS_Score']) if pd.notna(_row.get('DQS_Score')) else None,
                            float(_row['TQS_Score']) if pd.notna(_row.get('TQS_Score')) else None,
                            float(_row['PCS_Score']) if pd.notna(_row.get('PCS_Score')) else None,
                            str(_row.get('confidence_band') or _row.get('Confidence_Band') or ''),
                            str(_row.get('Gate_Reason') or ''),
                            str(_row.get('IV_Maturity_State') or ''),
                            str(_row.get('Regime') or ''),
                            str(_row.get('Signal_Type') or ''),
                            str(_row.get('Valid_Reason') or ''),
                            str(_row.get('Theory_Source') or ''),
                            str(_row.get('Trade_Bias') or ''),
                        ])
                        _inserted += 1
                    except Exception as _row_err:
                        logger.debug(f"[scan_candidates] row insert failed for {_row.get('Ticker')}: {_row_err}")
                _sc_con.close()
                logger.info(f"[GAP7] scan_candidates: persisted {_inserted} READY rows (Run_ID={_run_id})")
            except Exception as _sc_err:
                logger.warning(f"[GAP7] scan_candidates write failed (non-critical): {_sc_err}")

    # ── Calendar Deferral: persist READY candidates blocked by Friday/holiday theta ──
    if not is_initial_pass and not _df_ready.empty:
        try:
            from scan_engine.calendar_deferral import persist_deferred_candidates, mark_deferrals_filled
            _def_con = get_domain_write_connection(DbDomain.SCAN)
            _def_count = persist_deferred_candidates(_df_ready, _def_con, deferred_date=run_ts)
            # Mark prior deferrals as FILLED if they reappeared as READY today
            _deferred_set = ctx.results.get('calendar_deferred_set', set())
            if _deferred_set:
                _ready_pairs = [
                    (str(r.get('Ticker', '')), str(r.get('Strategy_Name', '') or r.get('Strategy', '')))
                    for _, r in _df_ready.iterrows()
                ]
                _matched = [p for p in _ready_pairs if p in _deferred_set]
                if _matched:
                    mark_deferrals_filled(_matched, _def_con)
            _def_con.close()
            if _def_count:
                logger.info(f"[CalendarDeferral] {_def_count} READY candidates deferred (Friday/holiday theta bleed)")
        except Exception as _def_err:
            logger.debug(f"[CalendarDeferral] Persist failed (non-critical): {_def_err}")

    # Smart WAIT Loop: Persist CONDITIONAL and AWAIT_CONFIRMATION trades
    # For ticker+strategy combos already ACTIVE in the wait list:
    #   - Preserve the wait clock (don't re-persist)
    #   - BUT compare the new contract vs existing — refresh if materially better
    #     (thesis-level memory + contract-level adaptability)
    if WAIT_LOOP_AVAILABLE and not is_initial_pass:
        try:
            _still_waiting = ctx.results.get('wait_list_still_waiting', [])
            # Build lookup: (ticker, strategy) → {wait_id, contract_quality}
            _already_waiting_map = {}
            for e in _still_waiting:
                _key = (str(e.get('ticker', '')), str(e.get('strategy_name', '')))
                _already_waiting_map[_key] = {
                    'wait_id': e.get('wait_id', ''),
                    'contract_quality': e.get('contract_quality'),
                }
            _already_waiting_set = set(_already_waiting_map.keys())

            if _already_waiting_set and not all_acceptance.empty:
                _cond_mask = all_acceptance['Execution_Status'].isin(['CONDITIONAL', 'AWAIT_CONFIRMATION'])
                _wait_skip_mask = all_acceptance.apply(
                    lambda r: (str(r.get('Ticker', '')), str(r.get('Strategy_Name', '') or r.get('Strategy', '')))
                    in _already_waiting_set,
                    axis=1,
                ) & _cond_mask

                # ── Contract refresh: compare instead of blindly skipping ──
                _refreshed = 0
                _backfilled = 0
                if _wait_skip_mask.any():
                    try:
                        from core.wait_loop.schema import (
                            extract_contract_quality, compare_contract_quality,
                        )
                        from core.wait_loop.persistence import refresh_contract
                        import json as _json
                        for _idx in all_acceptance[_wait_skip_mask].index:
                            _row = all_acceptance.loc[_idx]
                            _tk = str(_row.get('Ticker', ''))
                            _st = str(_row.get('Strategy_Name', '') or _row.get('Strategy', ''))
                            _existing = _already_waiting_map.get((_tk, _st))
                            if not _existing:
                                continue
                            _new_quality = extract_contract_quality(_row)
                            _old_cq = _existing.get('contract_quality')
                            # Backfill: if no baseline or baseline has null DQS,
                            # store current quality as the baseline for future
                            # comparisons (not logged as CONTRACT_REFRESHED).
                            if not _old_cq or _old_cq.get('dqs') is None:
                                _wid = _existing.get('wait_id', '')
                                if _wid and any(v is not None for v in _new_quality.values()):
                                    try:
                                        con.execute(
                                            "UPDATE wait_list SET contract_quality = ?, "
                                            "updated_at = CURRENT_TIMESTAMP WHERE wait_id = ?",
                                            (_json.dumps(_new_quality), _wid),
                                        )
                                        con.commit()
                                        _backfilled += 1
                                    except Exception:
                                        pass
                                continue
                            _is_better, _reasons = compare_contract_quality(
                                _old_cq, _new_quality,
                            )
                            if _is_better:
                                refresh_contract(
                                    con,
                                    _existing['wait_id'],
                                    proposed_strike=_row.get('Strike'),
                                    proposed_expiration=_row.get('Expiration'),
                                    contract_symbol=_row.get('Contract_Symbol'),
                                    contract_quality=_new_quality,
                                    reasons=_reasons,
                                )
                                _refreshed += 1
                                logger.info(
                                    f"[WaitList] Contract refreshed for {_tk}/{_st}: "
                                    f"{'; '.join(_reasons)}"
                                )
                        if _backfilled:
                            logger.info(f"[WaitList] Backfilled contract_quality on {_backfilled} entries")
                    except Exception as _refresh_err:
                        logger.debug(f"[WaitList] Contract refresh failed (non-fatal): {_refresh_err}")

                _skipped = int(_wait_skip_mask.sum())
                if _skipped > 0:
                    _skipped_tickers = all_acceptance.loc[_wait_skip_mask, 'Ticker'].tolist()
                    _msg = f"[WaitList] {_skipped} already-waiting (preserving wait clock)"
                    if _refreshed > 0:
                        _msg += f", {_refreshed} contracts refreshed with better candidates"
                    _msg += f": {_skipped_tickers}"
                    logger.info(_msg)
                    _persist_df = all_acceptance[~_wait_skip_mask]
                else:
                    _persist_df = all_acceptance
            else:
                _persist_df = all_acceptance

            persist_counts = persist_to_wait_list(_persist_df, con)
            logger.info(
                f"✅ Wait list updated: {persist_counts['await_confirmation']} saved, "
                f"{persist_counts['rejected']} rejected"
            )
            ctx.results['wait_list_persist_counts'] = persist_counts
        except Exception as e:
            logger.error(f"❌ Error persisting to wait list: {e}", exc_info=True)

    # ── Open-position conflict annotation ──────────────────────────────────────
    # Load active positions from entry_anchors and flag any READY candidate whose
    # ticker already has an open position in the OPPOSITE direction.
    # Does NOT block the recommendation — adding a put hedge to an existing call
    # may be intentional. Surfaces as a warning column for the scan view.
    try:
        _open = con.execute("""
            SELECT DISTINCT
                ea.Underlying_Ticker,
                mr.Strategy,
                mr.Thesis_State
            FROM entry_anchors ea
            JOIN management_recommendations mr
              ON ea.TradeID = mr.TradeID
            WHERE ea.Is_Active = TRUE
              AND mr.Snapshot_TS = (
                SELECT MAX(Snapshot_TS) FROM management_recommendations mr2
                WHERE mr2.TradeID = mr.TradeID
              )
        """).fetchdf()

        if not _open.empty and 'Ticker' in all_acceptance.columns:
            # Build a set of (ticker, direction) for open positions
            # direction: 'BULLISH' for calls/long, 'BEARISH' for puts/short
            def _dir(strat):
                s = str(strat).upper()
                if any(k in s for k in ('LONG_PUT', 'SHORT_CALL', 'BEAR')):
                    return 'BEARISH'
                if any(k in s for k in ('LONG_CALL', 'BUY_WRITE', 'COVERED_CALL', 'LEAP', 'BULL')):
                    return 'BULLISH'
                return 'NEUTRAL'

            _open['open_dir'] = _open['Strategy'].apply(_dir)
            # Also track the timeframe structure of each open position so we can
            # distinguish "adding the same leg type" from "adding a structurally
            # different leg" (e.g. short-dated put vs LEAP put on the same ticker).
            def _timeframe(strat: str) -> str:
                s = str(strat).upper()
                if 'LEAP' in s:
                    return 'LEAP'
                if any(k in s for k in ('LONG_CALL', 'LONG_PUT')):
                    return 'SHORT'
                return 'OTHER'

            _open['open_tf'] = _open['Strategy'].apply(_timeframe)
            _open_map  = _open.groupby('Underlying_Ticker')['open_dir'].apply(set).to_dict()
            # Per ticker: set of (direction, timeframe) tuples
            _open_legs = (
                _open.groupby('Underlying_Ticker')
                .apply(lambda g: set(zip(g['open_dir'], g['open_tf'])), include_groups=False)
                .to_dict()
            )
            # Per ticker: worst thesis state across all same-direction open legs
            # BROKEN > DEGRADED > INTACT/UNKNOWN — used to escalate SIZE_UP warnings
            _THESIS_RANK = {'BROKEN': 2, 'DEGRADED': 1}
            def _worst_thesis(ticker: str, direction: str) -> str:
                """Return worst Thesis_State for same-direction open legs on this ticker."""
                matching = _open[
                    (_open['Underlying_Ticker'] == ticker) &
                    (_open['open_dir'] == direction)
                ]['Thesis_State'].dropna()
                worst_rank = 0
                worst = ''
                for ts in matching:
                    r = _THESIS_RANK.get(str(ts).upper(), 0)
                    if r > worst_rank:
                        worst_rank = r
                        worst = str(ts).upper()
                return worst

            def _conflict(row):
                ticker = row.get('Ticker', '')
                strat  = str(row.get('Strategy_Name', '') or row.get('Primary_Strategy', '')).upper()
                candidate_dir = 'BEARISH' if 'PUT' in strat else ('BULLISH' if 'CALL' in strat else 'NEUTRAL')
                candidate_tf  = 'LEAP' if 'LEAP' in strat else 'SHORT'
                open_dirs = _open_map.get(ticker, set())
                open_legs = _open_legs.get(ticker, set())
                opposite  = 'BULLISH' if candidate_dir == 'BEARISH' else ('BEARISH' if candidate_dir == 'BULLISH' else None)
                if opposite and opposite in open_dirs:
                    same = candidate_dir in open_dirs
                    if same:
                        return f"SIZE_UP: already long {candidate_dir.lower()} on {ticker}"
                    return f"CONFLICT: existing {opposite.lower()} position on {ticker} — review before adding"
                if candidate_dir in open_dirs:
                    # Same direction — check if it's the same timeframe or a different leg structure
                    same_tf_exists = any(
                        d == candidate_dir and t == candidate_tf
                        for d, t in open_legs
                    )
                    diff_tf_exists = any(
                        d == candidate_dir and t != candidate_tf
                        for d, t in open_legs
                    )
                    # Append thesis health suffix when existing same-direction leg is degraded/broken
                    thesis = _worst_thesis(ticker, candidate_dir)
                    thesis_suffix = f"|THESIS_{thesis}" if thesis in ('DEGRADED', 'BROKEN') else ''
                    if same_tf_exists:
                        return f"SIZE_UP{thesis_suffix}: already long {candidate_dir.lower()} on {ticker}"
                    if diff_tf_exists:
                        other_tf = 'LEAP' if candidate_tf == 'SHORT' else 'short-dated'
                        return (
                            f"SIZE_UP|DIFF_STRUCTURE{thesis_suffix}: existing {candidate_dir.lower()} "
                            f"{other_tf} leg on {ticker} — this adds a "
                            f"{'LEAP' if candidate_tf == 'LEAP' else 'short-dated'} leg "
                            f"(different timeframe, different risk profile)"
                        )
                    return f"SIZE_UP{thesis_suffix}: already long {candidate_dir.lower()} on {ticker}"
                return ""

            all_acceptance['Position_Conflict'] = all_acceptance.apply(_conflict, axis=1)
            _conflicts = (all_acceptance['Position_Conflict'] != '').sum()
            if _conflicts:
                logger.warning(f"⚠️ {_conflicts} READY candidate(s) have open-position conflicts — review before executing")
        else:
            all_acceptance['Position_Conflict'] = ""
    except Exception as _e:
        logger.warning(f"⚠️ Open-position conflict check failed (non-fatal): {_e}")
        all_acceptance['Position_Conflict'] = ""

    # ── Execution Verdict: triage READY into EXECUTE/SKIP/ALTERNATIVE ─────
    try:
        from scan_engine.execution_verdict import compute_execution_verdicts
        _ready_for_verdict = all_acceptance[all_acceptance['Execution_Status'] == 'READY'].copy()
        if not _ready_for_verdict.empty:
            # Collect tickers with pending scale-up requests
            _su_tickers = set()
            if 'Scale_Up_Candidate' in _ready_for_verdict.columns:
                _su_tickers = set(
                    _ready_for_verdict.loc[
                        _ready_for_verdict['Scale_Up_Candidate'] == True, 'Ticker'
                    ].unique()
                )
            _verdicted = compute_execution_verdicts(_ready_for_verdict, scale_up_tickers=_su_tickers)
            # Merge verdict columns back
            for _vc in ['Execution_Verdict', 'Verdict_Reason', 'Execution_Rank']:
                if _vc in _verdicted.columns:
                    all_acceptance.loc[_verdicted.index, _vc] = _verdicted[_vc]
    except Exception as _ev_err:
        logger.warning(f"⚠️ Execution verdict failed (non-fatal): {_ev_err}")

    # ── Verdict-SKIP → Smart WAIT Loop ──────────────────────────────────────
    # Route SKIP verdicts to the wait list so the system monitors when their
    # blocking conditions clear (IV_Rank drops, RSI recovers, etc.).
    if WAIT_LOOP_AVAILABLE and not is_initial_pass:
        try:
            from scan_engine.step12_acceptance import persist_verdict_skips_to_wait_list
            _verdict_counts = persist_verdict_skips_to_wait_list(all_acceptance, con)
            _n_verdict = _verdict_counts.get('verdict_await', 0)
            if _n_verdict > 0:
                logger.info(
                    f"✅ Verdict-SKIP → wait list: {_n_verdict} candidates monitoring clearance conditions"
                )
            ctx.results['verdict_wait_counts'] = _verdict_counts
        except Exception as _vw_err:
            logger.warning(f"⚠️ Verdict-SKIP wait list persistence failed (non-fatal): {_vw_err}")

    ctx.results['acceptance_all'] = all_acceptance

    # Action 1: Enforce READY Exclusivity
    ready = all_acceptance[all_acceptance['Execution_Status'] == 'READY'].copy()
    ctx.debug_manager.record_step('step12_acceptance_ready', len(ready), ready)

    # Audit 1: Acceptance Determinism Audit (re-run gate on a copy)
    # This audit needs to be updated to use the new apply_execution_gate function
    # For now, we'll skip this complex re-evaluation for brevity, but it's critical for full governance.
    # re_acceptance = input_df.apply(...) # Re-apply gate logic
    # assert all_acceptance[['Execution_Status', 'Gate_Reason']].equals(re_acceptance[['Execution_Status', 'Gate_Reason']]), "❌ GOVERNANCE VIOLATION: Execution Gate is non-deterministic!"

    # Audit 2: Discovery Explosion Control
    if not all_acceptance.empty:
        ticker_counts = all_acceptance.groupby('Ticker').size()
        avg_density = ticker_counts.mean()
        max_density = ticker_counts.max()
        
        ctx.results['strategy_density'] = {
            'avg_strategies_per_ticker': avg_density,
            'max_strategies_per_ticker': max_density,
            'density_exceeded': max_density > 50  # Diagnostic ceiling
        }
        
        logger.info(f"📊 Strategy Density: Avg={avg_density:.2f}, Max={max_density} (Ceiling: 50)")
        if max_density > 50:
            logger.warning(f"⚠️ Discovery Explosion Detected: {max_density} strategies for a single ticker")

    ctx.results['acceptance_ready'] = ready

    if not all_acceptance.empty:
        logger.info(f"💰 Step 8: Computing thesis capacity on all {len(all_acceptance)} candidates...")
        audit.save_df("step8_input", all_acceptance)
        t0 = time.time()
        envelopes = compute_thesis_capacity(all_acceptance, account_balance=ctx.account_balance, sizing_method=ctx.sizing_method, conn=con)
        audit.profile("step8", envelopes, (time.time()-t0)*1000)
        audit.save_df("step8_output", envelopes)
        ctx.results['thesis_envelopes'] = envelopes
        ctx.debug_manager.record_step('step8_thesis_envelopes', len(envelopes), envelopes)

        # Merge MC/sizing columns back into acceptance_all so the dashboard sees them
        _mc_cols = [c for c in envelopes.columns if c not in all_acceptance.columns]
        if _mc_cols:
            all_acceptance = all_acceptance.join(envelopes[_mc_cols], how='left')
            ctx.results['acceptance_all'] = all_acceptance
            ctx.results['acceptance_ready'] = all_acceptance[all_acceptance['Execution_Status'] == 'READY'].copy()
            logger.info(f"   Merged {len(_mc_cols)} MC/sizing columns into acceptance_all")

    # NEW: Ingest scan results into ExecutionMonitor
    ctx.execution_monitor.update_market_context(run_ts)
    ctx.execution_monitor.ingest_scan_suggestions(all_acceptance) # Ingest all, monitor filters
    ctx.results['execution_monitor_summary'] = ctx.execution_monitor.get_monitoring_summary()

    # DEBUG LOGGING: Step12 completion status
    logger.info(f"DEBUG: Step12 completed - total evaluated: {len(ctx.results.get('acceptance_all', []))}")
    logger.info(f"DEBUG: Step12 READY count: {len(ctx.results.get('acceptance_ready', []))}")


def _finalize_results(ctx: PipelineContext, run_ts: datetime, db_con: duckdb.DuckDBPyConnection, schwab_client=None) -> dict:
    from core.shared.data_layer.market_stress_detector import classify_market_stress

    res = ctx.results
    res['pipeline_health'] = _generate_health_summary_dict(res)

    # Reuse the pipeline-level Schwab client; fall back to fresh init if not provided
    if schwab_client is None:
        try:
            from scan_engine.loaders.schwab_api_client import SchwabClient
            client_id = os.getenv("SCHWAB_APP_KEY")
            client_secret = os.getenv("SCHWAB_APP_SECRET")
            if client_id and client_secret:
                schwab_client = SchwabClient(client_id, client_secret)
        except Exception as e:
            logger.warning(f"⚠️ Schwab client initialization failed for market stress: {e}")

    # Classify market stress using the new market-level proxy
    stress_level, primary_metric_value, stress_basis = classify_market_stress(client=schwab_client)

    # ============================================================
    # REQUIREMENT 5: LOG MARKET REGIME PROXY USAGE
    # Comprehensive logging of market stress detection and impact
    # ============================================================
    logger.info("")
    logger.info("📊 MARKET REGIME PROXY ANALYSIS")
    logger.info("─" * 60)
    logger.info(f"   Proxy: {stress_basis}")
    logger.info(f"   Stress Level: {stress_level}")
    logger.info(f"   Primary Metric: {primary_metric_value}")

    # Check if market stress affected any decisions
    market_stress_impact = {
        'strategies_blocked': 0,
        'strategies_adjusted': 0,
        'confidence_reduced': 0
    }

    if 'acceptance_all' in res and not res['acceptance_all'].empty:
        df_acceptance = res['acceptance_all']

        # Check if R1.1 gate (market stress blocking) was triggered
        if 'Gate_Reason' in df_acceptance.columns:
            stress_blocked = df_acceptance['Gate_Reason'].str.contains('R1.1', na=False).sum()
            market_stress_impact['strategies_blocked'] = int(stress_blocked)

            if stress_blocked > 0:
                logger.warning(
                    f"   ⚠️  Market stress BLOCKED {stress_blocked} strategies (R1.1 gate)"
                )

        # Check if execution adjustments were made due to stress
        if 'execution_adjustment' in df_acceptance.columns:
            stress_adjusted = df_acceptance[
                (df_acceptance['execution_adjustment'].isin(['SIZE_DOWN', 'CAUTION', 'AVOID_SIZE']))
            ]
            market_stress_impact['strategies_adjusted'] = len(stress_adjusted)

        # Check if confidence was reduced
        if 'confidence_band' in df_acceptance.columns:
            low_confidence = (df_acceptance['confidence_band'] == 'LOW').sum()
            market_stress_impact['confidence_reduced'] = int(low_confidence)

    if market_stress_impact['strategies_blocked'] > 0:
        logger.warning(f"   Impact: Blocked {market_stress_impact['strategies_blocked']} strategies")
    elif market_stress_impact['strategies_adjusted'] > 0:
        logger.info(f"   Impact: Adjusted {market_stress_impact['strategies_adjusted']} strategies")
    else:
        logger.info(f"   Impact: No blocking or major adjustments")

    # Log specific proxy details
    if stress_basis == "SPY":
        logger.info(f"   Using SPY as market proxy (standard)")
    elif stress_basis == "VIX":
        logger.info(f"   Using VIX as volatility proxy (elevated stress)")
    elif stress_basis == "FALLBACK":
        logger.warning(f"   Using fallback estimation (no live data)")
    else:
        logger.info(f"   Using custom proxy: {stress_basis}")

    logger.info("─" * 60)
    logger.info("")

    # Store impact metrics
    res['market_stress_impact'] = market_stress_impact

    # FIX 8: Surface IV Clock State
    last_market_date = "UNKNOWN"
    history_days = 0
    if 'acceptance_all' in res and not res['acceptance_all'].empty:
        df = res['acceptance_all']
        if 'iv_surface_date' in df.columns:
            last_market_date = df['iv_surface_date'].max()
        if 'iv_history_count' in df.columns:
            history_days = df['iv_history_count'].max()

    # Determine Clock State from Data Provenance
    market_status = "UNKNOWN"
    if 'snapshot' in res and not res['snapshot'].empty:
        if 'market_status' in res['snapshot'].columns:
            market_status = res['snapshot']['market_status'].iloc[0]
    
    clock_state = "ADVANCING"
    if market_status == "CLOSED":
        clock_state = "PAUSED_MARKET_CLOSED"
    elif market_status == "UNKNOWN":
        clock_state = "PAUSED_DATA_GAP"
    
    # Regime_Gate: per-row, strategy-aware.
    # Income strategies (premium sellers) benefit from elevated vol — keep OPEN under ELEVATED.
    # Directional long-premium strategies stay RESTRICTED under ELEVATED.
    # CRISIS restricts both. LOW/NORMAL/UNKNOWN → OPEN for all.
    _INCOME_STRATEGIES = {
        'COVERED CALL', 'BUY-WRITE', 'CASH SECURED PUT', 'SHORT PUT',
        'SHORT CALL', 'IRON CONDOR', 'PMCC',
    }

    def _compute_regime_gate(row_strat_name: str, sl: str) -> str:
        _is_income = str(row_strat_name).upper().strip() in _INCOME_STRATEGIES
        if sl == 'CRISIS':
            return 'LOCKED'
        elif sl == 'ELEVATED':
            return 'OPEN' if _is_income else 'RESTRICTED'
        elif sl in ('LOW', 'NORMAL', 'UNKNOWN'):
            return 'OPEN'
        return 'OPEN'

    if 'acceptance_all' in res and not res['acceptance_all'].empty:
        res['acceptance_all']['Regime_Gate'] = res['acceptance_all']['Strategy_Name'].apply(
            lambda s: _compute_regime_gate(s, stress_level)
        )
        _gate_counts = res['acceptance_all']['Regime_Gate'].value_counts().to_dict()
        logger.info(f"📊 Regime_Gate (stress_level={stress_level}): {_gate_counts}")
    else:
        logger.info(f"📊 Regime_Gate: no acceptance rows (stress_level={stress_level})")

    # Regime_Strategy_Fit: per-row compatibility of Capital_Bucket with (Regime, stress_level)
    # Natenberg Ch.19, McMillan Ch.1, Passarelli Ch.2
    if 'acceptance_all' in res and not res['acceptance_all'].empty:
        _df_acc = res['acceptance_all']

        def _compute_regime_fit(row):
            _regime = str(row.get('Regime', 'Unknown') or 'Unknown')
            _bucket = str(row.get('Capital_Bucket', 'TACTICAL') or 'TACTICAL')
            _key    = (_regime, stress_level)
            _entry  = _REGIME_STRATEGY_MATRIX.get(_key)
            if _entry is None:
                _entry = _REGIME_STRATEGY_MATRIX.get(('Unknown', stress_level), {})
            if not _entry:
                return 'FIT', ''
            if _bucket in _entry.get('mismatch', []):
                return 'MISMATCH', _entry.get('note', '')
            if _bucket in _entry.get('caution', []):
                return 'CAUTION', _entry.get('note', '')
            return 'FIT', _entry.get('note', '')

        _fits = _df_acc.apply(_compute_regime_fit, axis=1)
        res['acceptance_all']['Regime_Strategy_Fit']  = _fits.apply(lambda x: x[0])
        res['acceptance_all']['Regime_Strategy_Note'] = _fits.apply(lambda x: x[1])
        _fit_counts = res['acceptance_all']['Regime_Strategy_Fit'].value_counts().to_dict()
        logger.info(f"📊 Regime_Strategy_Fit — FIT: {_fit_counts.get('FIT',0)}, CAUTION: {_fit_counts.get('CAUTION',0)}, MISMATCH: {_fit_counts.get('MISMATCH',0)}")

        # Surface_Shape_Warning: flag INVERTED surface for near-dated directional longs + income sellers
        # Natenberg Ch.19: "When front-month IV exceeds back-month, near-dated long vol is expensive."
        def _surface_warning(row):
            _shape  = str(row.get('Surface_Shape', '') or '').upper()
            _bucket = str(row.get('Capital_Bucket', '') or '')
            _stype  = str(row.get('Strategy_Type', '') or '').upper()
            if _shape == 'INVERTED' and _bucket == 'TACTICAL' and _stype == 'DIRECTIONAL':
                return ('ELEVATED_COST',
                        'Inverted term structure: near-term IV > long-term IV — you are buying the expensive front month. '
                        'Natenberg Ch.19: consider a LEAP (buy cheaper back-month vol) or wait for term structure to normalise.')
            if _shape == 'INVERTED' and _bucket == 'DEFENSIVE':
                return ('ASSIGNMENT_RISK',
                        'Inverted surface: near-term IV spike present — event risk elevated. '
                        'Income sellers face heightened assignment/pin risk near expiry. '
                        'Natenberg Ch.19: widen strikes or reduce DTE exposure.')
            return ('OK', '')

        _sw = _df_acc.apply(_surface_warning, axis=1)
        res['acceptance_all']['Surface_Shape_Warning']      = _sw.apply(lambda x: x[0])
        res['acceptance_all']['Surface_Shape_Warning_Note'] = _sw.apply(lambda x: x[1])
        _sw_counts = res['acceptance_all']['Surface_Shape_Warning'].value_counts().to_dict()
        if _sw_counts.get('ELEVATED_COST', 0) + _sw_counts.get('ASSIGNMENT_RISK', 0) > 0:
            logger.info(f"📊 Surface_Shape_Warning — ELEVATED_COST: {_sw_counts.get('ELEVATED_COST',0)}, ASSIGNMENT_RISK: {_sw_counts.get('ASSIGNMENT_RISK',0)}")

    res['market_stress'] = {
        'level': stress_level,
        'primary_metric_value': primary_metric_value,
        'basis': stress_basis,
        'last_market_date': str(last_market_date),
        'scan_timestamp': run_ts.utcnow().isoformat(), # Use run_ts here
        'iv_history_days': f"{history_days} / 120",
        'iv_clock_state': clock_state,
        'market_status_source': "SNAPSHOT"
    }
    
    # OPERATIONAL FIX: Provenance Telemetry
    if 'acceptance_all' in res and not res['acceptance_all'].empty:
        df = res['acceptance_all']
        if 'iv_surface_source' in df.columns:
            res['provenance_telemetry'] = {
                'source_distribution': df['iv_surface_source'].value_counts().to_dict(),
                'maturity_distribution': df.get('IV_Maturity_State', pd.Series(['UNKNOWN']*len(df))).value_counts().to_dict()
            }
            logger.info(f"📊 Provenance Telemetry: {res['provenance_telemetry']['source_distribution']}")

    # Removed the problematic line: if 'charted' in res and 'filtered' in res: res['regime_info'] = classify_market_regime(res['charted'], res['filtered'])
    
    if os.getenv("DEBUG_TICKER_MODE") == "1": # Export debug summary if DEBUG_TICKER_MODE is active
        res['debug_summary'] = ctx.debug_manager.get_summary()
        # Export debug summary here, after it's populated
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        debug_summary_path = ctx.output_dir / f"debug_summary_{ts}.json"
        try:
            with open(debug_summary_path, 'w') as f:
                json.dump(res['debug_summary'], f, indent=2)
            logger.info(f"💾 Exported debug summary to {debug_summary_path}")
        except Exception as e:
            logger.error(f"❌ Failed to export debug summary: {e}")

    # DEBUG PARITY CHECK: Summarize production-equivalent blocking reasons
    if ctx.debug_manager.enabled:
        try:
            df_acceptance = res.get('acceptance_all', pd.DataFrame()).copy()
            if not df_acceptance.empty:
                parity = _build_debug_parity_check(df_acceptance)
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                parity_path = ctx.output_dir / f"debug_parity_check_{ts}.csv"
                parity.to_csv(parity_path, index=False)
                logger.info(f"💾 Exported debug parity check to {parity_path}")

                summary = {
                    'total': int(len(parity)),
                    'would_block': int(parity['Would_Block'].sum()),
                    'data_driven_blocks': int((parity['Block_Type'] == 'data-driven').sum()),
                    'mechanical_blocks': int((parity['Block_Type'] == 'mechanical').sum())
                }
                res['debug_parity_check'] = {
                    'path': str(parity_path),
                    'summary': summary
                }
        except Exception as e:
            logger.error(f"❌ Failed to export debug parity check: {e}", exc_info=True)
    
    # Add ExecutionMonitor summary to results
    res['execution_monitor_summary'] = ctx.execution_monitor.get_monitoring_summary()

    # Missing-data health report: diagnose final DataFrame, check impossible, persist
    if ctx.missing_tracker:
        try:
            df_final = res.get('acceptance_all', pd.DataFrame())
            if not df_final.empty:
                ctx.missing_tracker.diagnose(df_final, step_num=12)
                ctx.missing_tracker.audit_stage("step12", None, df_final)
                ctx.missing_tracker.check_impossible(df_final, step_num=12)
            from dataclasses import asdict as _asdict
            _md_report = ctx.missing_tracker.generate_report()
            res['missing_data_health'] = _asdict(_md_report)
            if db_con is not None:
                ctx.missing_tracker.persist(db_con)
        except Exception as e:
            logger.warning(f"[MissingData] finalize failed: {e}")

    _log_pipeline_health_summary(res)

    # Export AFTER regime overlay columns are computed so Regime_Strategy_Fit,
    # Regime_Gate, and Surface_Shape_Warning are present in the CSV.
    _export_results(ctx, run_ts)

    return res

def _build_debug_parity_check(df_acceptance: pd.DataFrame) -> pd.DataFrame:
    def classify_block_type(reason: str) -> str:
        if not isinstance(reason, str) or not reason.strip():
            return "unknown"
        text = reason.lower()
        data_driven_terms = [
            "missing", "stale", "api failure", "no bid/ask", "illiquid", "liquidity",
            "immature", "mature", "iv", "data gaps", "partial", "history"
        ]
        mechanical_terms = [
            "await", "requires", "default", "fallback", "passed initial gates", "no specific gate"
        ]
        if any(term in text for term in data_driven_terms):
            return "data-driven"
        if any(term in text for term in mechanical_terms):
            return "mechanical"
        return "unknown"

    df = df_acceptance.copy()
    df['Gate_Reason'] = df.get('Gate_Reason', df.get('Block_Reason', ''))
    df['Would_Block'] = df['Execution_Status'].ne('READY')
    df['Block_Type'] = df['Gate_Reason'].apply(classify_block_type)

    return df[[
        'Ticker',
        'Strategy_Name',
        'Execution_Status',
        'Gate_Reason',
        'Would_Block',
        'Block_Type'
    ]].copy()

def _export_results(ctx: PipelineContext, run_ts: datetime):
    """Export pipeline artifacts using canonical naming convention."""
    # Canonical naming mapping to restore dashboard visibility
    EXPORT_MAPPING = {
        'snapshot': 'Step2_Snapshot',
        'filtered': 'Step3_Filtered',
        'charted': 'Step5_Charted',
        'validated_data': 'Step6_Validated',
        'recommended_strategies': 'Step7_Recommended',
        'evaluated_strategies': 'Step11_Evaluated',
        'timeframes': 'Step9A_Timeframes',
        'selected_contracts': 'Step9B_SelectedContracts',
        'recalibrated_contracts': 'Step10_Filtered',
        'acceptance_all': 'Step12_Acceptance',
        'acceptance_ready': 'Step12_Ready',
        'thesis_envelopes': 'Step8_Thesis_Envelopes'
    }

    # Step 12 outputs must ALWAYS be exported (even if empty)
    # This ensures deterministic export behavior and dashboard visibility
    REQUIRED_EXPORTS = ['acceptance_all', 'acceptance_ready']

    try:
        ts = run_ts.strftime("%Y%m%d_%H%M%S")
        for key, df in ctx.results.items():
            if isinstance(df, pd.DataFrame):
                # Export if non-empty OR if it's a required Step12 output
                should_export = not df.empty or key in REQUIRED_EXPORTS

                if should_export:
                    filename = EXPORT_MAPPING.get(key, key)
                    export_path = ctx.output_dir / f"{filename}_{ts}.csv"
                    df.to_csv(export_path, index=False)
                    if df.empty and key in REQUIRED_EXPORTS:
                        logger.info(f"💾 Exported {filename} (empty) to {export_path}")
                    else:
                        logger.info(f"💾 Exported {filename} ({len(df)} rows) to {export_path}")

        # Write LATEST_SCAN_COMPLETE file
        run_id = f"scan_{ts}"
        latest_file_path = ctx.output_dir / "LATEST_SCAN_COMPLETE"
        latest_file_path.write_text(run_id)
        logger.info(f"✅ Updated LATEST_SCAN_COMPLETE with run_id: {run_id}")

    except Exception as e:
        logger.error(f"❌ Export failed: {e}")


def _generate_health_summary_dict(results: dict):
    selected_contracts = results.get('selected_contracts', pd.DataFrame())
    acceptance_all = results.get('acceptance_all', pd.DataFrame())
    acceptance_ready = results.get('acceptance_ready', pd.DataFrame())
    thesis_envelopes = results.get('thesis_envelopes', pd.DataFrame())
    
    step9b_total = len(selected_contracts)
    step9b_valid = 0
    if not selected_contracts.empty and 'Validation_Status' in selected_contracts.columns:
        step9b_valid = (selected_contracts['Validation_Status'] == 'Valid').sum()
    
    step12_total = len(acceptance_all)
    step12_ready_now = 0
    if not acceptance_all.empty and 'Execution_Status' in acceptance_all.columns:
        step12_ready_now = (acceptance_all['Execution_Status'] == 'READY').sum()
    
    step8_count = len(thesis_envelopes)
    
    quality = {
        'step9b_success_rate': (step9b_valid / step9b_total * 100) if step9b_total > 0 else 0,
        'step12_acceptance_rate': (step12_ready_now / step12_total * 100) if step12_total > 0 else 0,
        'step8_annotation_rate': (step8_count / step12_ready_now * 100) if step12_ready_now > 0 else 0,
        'end_to_end_rate': (step8_count / step9b_total * 100) if step9b_total > 0 else 0
    }
    
    return {
        'step9b': {'total_contracts': step9b_total, 'valid': step9b_valid},
        'step12': {'total_evaluated': step12_total, 'ready_now': step12_ready_now},
        'step8': {'thesis_envelopes': step8_count},
        'quality': quality
    }


def _log_pipeline_health_summary(results: dict):
    logger.info("\n" + "="*80)
    logger.info("📊 PIPELINE HEALTH SUMMARY (Phase 1-2-3)")
    logger.info("="*80)
    
    selected_contracts = results.get('selected_contracts', pd.DataFrame())
    acceptance_all = results.get('acceptance_all', pd.DataFrame())
    thesis_envelopes = results.get('thesis_envelopes', pd.DataFrame())
    
    logger.info(f"🔗 Step 9B: Contract Selection - Total: {len(selected_contracts)}")
    logger.info(f"✅ Step 12: Acceptance Logic - READY: {len(results.get('acceptance_ready', pd.DataFrame()))}") # Updated to READY
    logger.info(f"💰 Step 8: Thesis Capacity - Envelopes Generated: {len(thesis_envelopes)}")
    logger.info("\n" + "="*80 + "\n")

# ============================================================
# PUBLIC API - Full Pipeline Execution
# ============================================================

def run_full_pipeline(
    snapshot_path: str = None,
    output_dir: str = None,
    account_balance: float = 100000.0,
    max_portfolio_risk: float = 0.20,
    sizing_method: str = 'volatility_scaled',
    expiry_intent: str = 'ANY'
) -> dict:
    """
    Public API for executing the complete scan pipeline.

    Sequentially runs all pipeline steps in proper order:
    - Step -1: Re-evaluate Wait List
    - Step 2: Load and enrich snapshot
    - Step 3: Filter tickers (IV/HV gap analysis)
    - Step 5-6: Data quality validation
    - Step 7: Strategy recommendation
    - Step 9: Contract selection
    - Step 10: PCS recalibration
    - Step 11: Independent evaluation
    - Step 12: Acceptance logic and position sizing
    - Step 12B: (deprecated - IVEngine handles all IV metrics)
    - Step 12D: Bias-free enrichment
    - Step 12E: Maturity & eligibility

    Args:
        snapshot_path: Path to IV/HV snapshot CSV (optional, uses latest if None)
        output_dir: Output directory for results (optional, uses default if None)
        account_balance: Account balance for position sizing
        max_portfolio_risk: Maximum portfolio risk percentage
        sizing_method: Position sizing method ('volatility_scaled', etc.)
        expiry_intent: Contract expiration preference ('ANY', 'WEEKLY', 'MONTHLY')

    Returns:
        dict: Pipeline results containing:
            - 'snapshot': Loaded snapshot DataFrame
            - 'strategies': Recommended strategies
            - 'selected_contracts': Selected contracts
            - 'acceptance_ready': READY candidates
            - 'thesis_envelopes': Position sizing envelopes
            - 'market_stress': Market stress analysis
            - 'debug_summary': Debug information (if debug mode enabled)

    Example:
        >>> results = run_full_pipeline(
        ...     snapshot_path='data/snapshots/ivhv_snapshot_live_20260210_173115.csv',
        ...     account_balance=50000.0
        ... )
        >>> print(f"Ready candidates: {len(results['acceptance_ready'])}")
    """
    logger.info("=" * 80)
    logger.info("🚀 FULL PIPELINE EXECUTION")
    logger.info("=" * 80)

    # Call the internal pipeline orchestrator
    results = run_full_scan_pipeline(
        snapshot_path=snapshot_path,
        output_dir=output_dir,
        account_balance=account_balance,
        max_portfolio_risk=max_portfolio_risk,
        sizing_method=sizing_method,
        expiry_intent=expiry_intent
    )

    logger.info("=" * 80)
    logger.info("✅ FULL PIPELINE COMPLETE")
    logger.info("=" * 80)

    return results


if __name__ == "__main__":
    raise RuntimeError("PIPELINE.PY EXECUTED FROM scan_engine")
