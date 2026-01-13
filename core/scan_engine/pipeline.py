"""
Full Scan Pipeline Orchestrator

Combines all steps into a single run_full_scan_pipeline() function.
"""

import pandas as pd
import logging
import os
import time
from pathlib import Path
from datetime import datetime

from .step2_load_snapshot import load_ivhv_snapshot
from .step3_filter_ivhv import filter_ivhv_gap
from .step5_chart_signals import compute_chart_signals
from .step6_gem_filter import validate_data_quality
from .step7_strategy_recommendation import recommend_strategies
from .step7_5_iv_demand import emit_iv_demand
from .step8_position_sizing import compute_thesis_capacity
from .step9a_determine_timeframe import determine_timeframe
from .step9b_fetch_contracts_schwab import fetch_and_select_contracts_schwab  # Production Schwab version
from .step10_pcs_recalibration import recalibrate_and_filter
from .step11_independent_evaluation import evaluate_strategies_independently
from .step12_acceptance import apply_acceptance_logic, filter_ready_contracts  # Phase 3 acceptance logic
from .debug.debug_mode import get_debug_manager
from .market_regime_classifier import classify_market_regime
from core.data_layer.market_stress_detector import check_market_stress
from core.governance import audit_harness as audit

logger = logging.getLogger(__name__)


class PipelineContext:
    """Holds state and configuration for a pipeline run."""
    def __init__(self, snapshot_path, output_dir, account_balance, max_portfolio_risk, sizing_method, expiry_intent, audit_mode):
        self.snapshot_path = snapshot_path
        self.output_dir = Path(output_dir) if output_dir else Path(os.getenv('OUTPUT_DIR', './output'))
        self.account_balance = account_balance
        self.max_portfolio_risk = max_portfolio_risk
        self.sizing_method = sizing_method
        self.expiry_intent = expiry_intent
        self.audit_mode = audit_mode
        self.results = {}
        self.debug_manager = get_debug_manager()
        
        self.output_dir.mkdir(parents=True, exist_ok=True)
        if self.debug_manager.enabled:
            self.debug_manager.clear()

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
    
    try:
        if not _step2_load_data(ctx): return _finalize_results(ctx)
        if not _step3_filter_tickers(ctx): return _finalize_results(ctx)
        if not _step5_6_enrich_and_validate(ctx): return _finalize_results(ctx)
        if not _step7_11_recommend_and_evaluate(ctx): return _finalize_results(ctx)
        if not _step9_select_contracts(ctx): return _finalize_results(ctx)
        if not _step10_recalibrate_pcs(ctx): return _finalize_results(ctx)
        _step12_8_acceptance_and_sizing(ctx)
        
    except Exception as e:
        ctx.debug_manager.log_exception("pipeline", e, "Pipeline aborted")
        logger.error(f"❌ Pipeline failed unexpectedly: {e}", exc_info=True)
    
    _export_results(ctx)
    return _finalize_results(ctx)

def _step2_load_data(ctx: PipelineContext) -> bool:
    logger.info("📊 Step 2: Loading IV/HV snapshot...")
    t0 = time.time()
    # Use modularized Step 2
    df = load_ivhv_snapshot(
        snapshot_path=ctx.snapshot_path,
        use_live_snapshot=True if not ctx.snapshot_path else False
    )
    audit.profile("step2", df, (time.time()-t0)*1000)
    audit.save_df("step2_output", df)

    # Centralized Universe Restriction (Controlled by PIPELINE_DEBUG)
    df = ctx.debug_manager.restrict_universe(df)

    ctx.results['snapshot'] = df
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
    df_charted = compute_chart_signals(df_input)
    audit.profile("step5", df_charted, (time.time()-t0)*1000)
    audit.save_df("step5_output", df_charted)
    ctx.results['charted'] = df_charted
    ctx.debug_manager.record_step('step5_charted', len(df_charted), df_charted)
    
    if ctx.audit_mode:
        ctx.audit_mode.save_step(df_charted, "chart_signals", "Technical analysis")

    if df_charted.empty: return False

    logger.info("📊 Step 6: Validating data quality...")
    validated = validate_data_quality(df_charted)
    ctx.results['validated_data'] = validated
    ctx.debug_manager.record_step('step6_validated', len(validated), validated)
    
    if ctx.audit_mode:
        ctx.audit_mode.save_step(validated, "data_validated", "Data quality validation")
    return not validated.empty

def _step7_11_recommend_and_evaluate(ctx: PipelineContext) -> bool:
    logger.info("🎯 Step 7: Generating strategy recommendations...")
    recommended = recommend_strategies(ctx.results['validated_data'])
    ctx.results['recommended_strategies'] = recommended
    ctx.debug_manager.record_step('step7_recommended', len(recommended), recommended)
    
    # Phase 7.5: IV Demand Emission (Demand-Driven Architecture)
    df_demand = emit_iv_demand(recommended)
    audit.save_demand(df_demand)
    ctx.results['iv_demand'] = df_demand

    if recommended.empty: return False

    logger.info("🎯 Step 11: Independent strategy evaluation...")
    evaluated = evaluate_strategies_independently(recommended, account_size=ctx.account_balance)
    ctx.results['evaluated_strategies'] = evaluated
    ctx.debug_manager.record_step('step11_evaluated', len(evaluated), evaluated)
    return not evaluated.empty

def _step9_select_contracts(ctx: PipelineContext) -> bool:
    logger.info(f"⏱️ Step 9A: Determining timeframes...")
    timeframes = determine_timeframe(ctx.results['evaluated_strategies'], expiry_intent=ctx.expiry_intent)
    ctx.results['timeframes'] = timeframes
    if timeframes.empty: return False

    logger.info(f"⛓️ Step 9B: Fetching contracts from Schwab...")
    contracts = fetch_and_select_contracts_schwab(ctx.results['evaluated_strategies'], timeframes, expiry_intent=ctx.expiry_intent)
    
    # Re-evaluate with real Greeks
    contracts = evaluate_strategies_independently(contracts, account_size=ctx.account_balance)
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
    ctx.results['recalibrated_contracts'] = recalibrated
    ctx.debug_manager.record_step('step10_recalibrated', len(recalibrated), recalibrated)
    return not recalibrated.empty

def _step12_8_acceptance_and_sizing(ctx: PipelineContext):
    logger.info(f"✅ Step 12: Applying acceptance logic...")
    # Use recalibrated contracts if available
    input_df = ctx.results.get('recalibrated_contracts', ctx.results['selected_contracts'])
    audit.save_df("step12_input", input_df)
    t0 = time.time()
    all_acceptance = apply_acceptance_logic(input_df, expiry_intent=ctx.expiry_intent)
    audit.profile("step12", all_acceptance, (time.time()-t0)*1000)
    audit.save_df("step12_output", all_acceptance)
    audit.export_ready_now_evidence(all_acceptance)
    
    ctx.results['acceptance_all'] = all_acceptance
    
    # Action 1: Enforce READY_NOW Exclusivity
    # Step 12 is a semantic firewall. Only READY_NOW is permitted to proceed to sizing.
    ready = all_acceptance[all_acceptance['acceptance_status'] == 'READY_NOW'].copy()
    
    # Audit 1: Acceptance Determinism Audit
    # Prove Step 12 is purely functional: Same input -> same output.
    # We run it again on a copy and assert equality.
    re_acceptance = apply_acceptance_logic(input_df.copy(), expiry_intent=ctx.expiry_intent)
    assert all_acceptance.equals(re_acceptance), "❌ GOVERNANCE VIOLATION: Step 12 is non-deterministic!"
    
    # Audit 2: Discovery Explosion Control
    # Track strategy density to prevent combinatorial explosion.
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
    
    if not ready.empty:
        logger.info(f"💰 Step 8: Computing thesis capacity...")
        audit.save_df("step8_input", ready)
        t0 = time.time()
        envelopes = compute_thesis_capacity(ready, account_balance=ctx.account_balance, sizing_method=ctx.sizing_method)
        audit.profile("step8", envelopes, (time.time()-t0)*1000)
        audit.save_df("step8_output", envelopes)
        ctx.results['thesis_envelopes'] = envelopes
        ctx.debug_manager.record_step('step8_thesis_envelopes', len(envelopes), envelopes)

def _finalize_results(ctx: PipelineContext) -> dict:
    res = ctx.results
    res['pipeline_health'] = _generate_health_summary_dict(res)
    stress_level, median_iv, stress_basis = check_market_stress()
    
    # FIX 8: Surface IV Clock State
    # market_date = Schwab/Fidelity truth (validity date)
    # scan_timestamp = System truth (capture time)
    last_market_date = "UNKNOWN"
    history_days = 0
    if 'acceptance_all' in res and not res['acceptance_all'].empty:
        df = res['acceptance_all']
        if 'iv_surface_date' in df.columns:
            last_market_date = df['iv_surface_date'].max()
        if 'iv_history_count' in df.columns:
            history_days = df['iv_history_count'].max()

    # Determine Clock State from Data Provenance
    # ADVANCING: Market is open and data is fresh
    # PAUSED_MARKET_CLOSED: Weekend or holiday
    # PAUSED_DATA_GAP: Scraper failure or ingestion lag
    
    # FIX 9: Derive market state from snapshot, not runtime clock.
    # We use the 'market_status' field present in the raw snapshot (Step 0).
    market_status = "UNKNOWN"
    if 'snapshot' in res and not res['snapshot'].empty:
        if 'market_status' in res['snapshot'].columns:
            market_status = res['snapshot']['market_status'].iloc[0]
    
    clock_state = "ADVANCING"
    if market_status == "CLOSED":
        clock_state = "PAUSED_MARKET_CLOSED"
    elif market_status == "UNKNOWN":
        clock_state = "PAUSED_DATA_GAP"
    
    res['market_stress'] = {
        'level': stress_level, 
        'median_iv': median_iv, 
        'basis': stress_basis,
        'last_market_date': str(last_market_date),
        'scan_timestamp': datetime.utcnow().isoformat(),
        'iv_history_days': f"{history_days} / 120",
        'iv_clock_state': clock_state,
        'market_status_source': "SNAPSHOT"
    }
    
    # OPERATIONAL FIX: Provenance Telemetry
    # Track distribution of data sources and maturity states for scaling visibility.
    if 'acceptance_all' in res and not res['acceptance_all'].empty:
        df = res['acceptance_all']
        if 'iv_surface_source' in df.columns:
            res['provenance_telemetry'] = {
                'source_distribution': df['iv_surface_source'].value_counts().to_dict(),
                'maturity_distribution': df.get('IV_Maturity_State', pd.Series(['UNKNOWN']*len(df))).value_counts().to_dict()
            }
            logger.info(f"📊 Provenance Telemetry: {res['provenance_telemetry']['source_distribution']}")

    if 'charted' in res and 'filtered' in res:
        res['regime_info'] = classify_market_regime(res['charted'], res['filtered'])
    
    if ctx.debug_manager.enabled:
        res['debug_summary'] = ctx.debug_manager.get_summary()
    
    _log_pipeline_health_summary(res)
    return res

def _export_results(ctx: PipelineContext):
    try:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        for key, df in ctx.results.items():
            if isinstance(df, pd.DataFrame) and not df.empty:
                df.to_csv(ctx.output_dir / f"{key}_{ts}.csv", index=False)
    except Exception as e:
        logger.error(f"❌ Export failed: {e}")


def _generate_health_summary_dict(results: dict) -> dict:
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
    if not acceptance_all.empty and 'acceptance_status' in acceptance_all.columns:
        step12_ready_now = (acceptance_all['acceptance_status'] == 'READY_NOW').sum()
    
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
    logger.info(f"✅ Step 12: Acceptance Logic - READY_NOW: {len(results.get('acceptance_ready', pd.DataFrame()))}")
    logger.info(f"💰 Step 8: Thesis Capacity - Envelopes Generated: {len(thesis_envelopes)}")
    logger.info("\n" + "="*80 + "\n")
