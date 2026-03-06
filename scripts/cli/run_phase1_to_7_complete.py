"""
Phase 1-7 Complete Pipeline Orchestrator

Runs all three cycles end-to-end:
1. Cycle 1: Perception loop (Phases 1-4)
2. Cycle 2: Freeze/Time-series (Phase 5-6)
3. Cycle 3: Recommendations (Phase 7)

Usage:
    python run_phase1_to_7_complete.py
"""

import pandas as pd
import numpy as np
import logging
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.management.cycle1.ingest.clean import phase1_load_and_clean_positions
from core.management.cycle1.identity.parse import phase2_run_all
from core.management._future_cycles.enrich.sus_compose_pcs_snapshot import run_phase3_enrichment
from core.management.cycle2.drift.compute_pnl_metrics import compute_pnl_metrics
from core.management.cycle2.drift.compute_basic_drift import compute_drift_metrics
from core.management._quarantine.legacy.compute_drift_metrics import classify_drift_severity
from core.management.cycle1.snapshot.snapshot import save_clean_snapshot, validate_cycle1_ledger
from core.management.cycle3.bootstrap import bootstrap_doctrines
from core.management.cycle3.decision.engine import generate_recommendations
from core.phase7_recommendations.load_chart_signals import load_chart_signals, merge_chart_signals

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_complete_pipeline(
    schwab_csv_path: str,
    output_db_path: str | None = None,
    enable_chart_signals: bool = True,
    enable_recommendations: bool = True
) -> pd.DataFrame:
    """
    Run complete Phase 1-7 pipeline.
    
    Args:
        schwab_csv_path: Path to Schwab position export CSV
        output_db_path: DuckDB database path
        enable_chart_signals: Load chart data (Phase 7)
        enable_recommendations: Generate exit recommendations (Phase 7)
        
    Returns:
        Final enriched DataFrame with recommendations
    """
    # Startup Validation: Ensure ledger matches contract
    from core.shared.data_contracts.config import PIPELINE_DB_PATH
    output_db_path = output_db_path or str(PIPELINE_DB_PATH)
    
    validate_cycle1_ledger(db_path=output_db_path)

    logger.info("="*80)
    logger.info("STARTING COMPLETE PIPELINE: PHASE 1-7")
    logger.info("="*80)
    
    start_time = datetime.now()
    
    # ========== CYCLE 1: PERCEPTION LOOP (PHASES 1-4) ==========
    logger.info("\n📊 CYCLE 1: PERCEPTION LOOP")
    logger.info("-" * 40)
    
    # Phase 1: Clean raw data
    logger.info("Phase 1: Cleaning raw data...")
    result = phase1_load_and_clean_positions(input_path=Path(schwab_csv_path))
    # phase1_load_and_clean_positions returns a dict with 'df' key
    if isinstance(result, dict):
        df_clean = result['df']
    else:
        # Old signature returned (df, snapshot_path) tuple
        df_clean, _ = result
    logger.info(f"✅ Phase 1 complete: {len(df_clean)} positions cleaned")
    
    # Phase 2: Parse structures
    logger.info("Phase 2: Parsing structures...")
    df_parsed = phase2_run_all(df_clean)
    logger.info(f"✅ Phase 2 complete: {len(df_parsed)} positions structured")
    
    # Phase 3: Enrichment (includes IV_Rank, PCS, all metrics)
    logger.info("Phase 3: Enriching with Greeks, PCS, IV_Rank...")
    df_enriched = run_phase3_enrichment(df_parsed)
    logger.info(f"✅ Phase 3 complete: {len(df_enriched.columns)} total columns")
    
    # Phase 4: Persist snapshot (includes Entry freeze)
    logger.info("Phase 4: Persisting snapshot with Entry freeze...")
    df_snapshot, csv_path, db_path, _, _ = save_clean_snapshot(
        df_enriched, 
        db_path=output_db_path,
        source_file_path=schwab_csv_path,
        ingest_context="cli_complete"
    )
    logger.info(f"✅ Phase 4 complete: Snapshot persisted to {db_path}")

    # --- HARD GATE: Cycle-1 Completeness ---
    import duckdb
    with duckdb.connect(output_db_path) as con:
        # 1. Check for any snapshot (lenient for audit/dev)
        has_data = con.execute(f"SELECT COUNT(*) FROM clean_legs_v2").fetchone()[0] > 0
        if not has_data:
            logger.error(f"❌ HARD GATE FAILURE: No Cycle-1 snapshot found in {CLEAN_LEGS_TABLE}.")
            raise RuntimeError(f"Cycle-2 cannot run: Missing Cycle-1 snapshot")

        # 2. Check for entry_anchors
        has_anchors = con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'entry_anchors'").fetchone()[0] > 0
        if not has_anchors or con.execute("SELECT COUNT(*) FROM entry_anchors").fetchone()[0] == 0:
            logger.error("❌ HARD GATE FAILURE: entry_anchors table is missing or empty.")
            raise RuntimeError("Cycle-2 cannot run: entry_anchors missing or empty")

        # 3. Check for ingest log
        has_log = con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'cycle1_ingest_log'").fetchone()[0] > 0
        if not has_log or con.execute("SELECT COUNT(*) FROM cycle1_ingest_log").fetchone()[0] == 0:
            logger.error("❌ HARD GATE FAILURE: cycle1_ingest_log is missing or empty.")
            raise RuntimeError("Cycle-2 cannot run: cycle1_ingest_log missing or empty")

        # 4. Anchor Integrity Check
        integrity_fail = con.execute("""
            SELECT TradeID, LegID, COUNT(*) AS cnt
            FROM entry_anchors
            GROUP BY TradeID, LegID
            HAVING cnt != 1
        """).fetchone()
        if integrity_fail:
            logger.error("❌ HARD GATE FAILURE: Anchor integrity violation detected (multiple anchors per leg).")
            raise RuntimeError("Cycle-2 cannot run: Anchor integrity violation")
    
    # ========== CYCLE 2: DRIFT ANALYSIS ==========
    logger.info("\n🔄 CYCLE 2: TIME-SERIES DRIFT ANALYSIS")
    logger.info("-" * 40)
    
    # Re-join with entry anchors to enable drift calculation
    logger.info("Re-joining snapshot with entry anchors...")
    with duckdb.connect(output_db_path) as con:
        df_anchors = con.execute("SELECT * FROM entry_anchors WHERE Is_Active = TRUE").df()
        
    if not df_anchors.empty:
        # Join on LegID
        df_snapshot_with_anchors = df_snapshot.merge(
            df_anchors[[c for c in df_anchors.columns if c.endswith('_Entry') or c in ['LegID', 'Entry_Timestamp', 'Entry_Snapshot_TS', 'Entry_Structure']]], 
            on='LegID', 
            how='left'
        )
    else:
        df_snapshot_with_anchors = df_snapshot

    # Compute drift metrics (Entry vs Current)
    logger.info("Computing drift metrics...")
    df_with_drift = compute_drift_metrics(df_snapshot_with_anchors)
    logger.info(f"✅ Drift metrics computed")

    # --- CYCLE 2 AUDIT EXPORT (MANDATORY) ---
    from core.management.cycle3.decision.resolver import StrategyResolver
    df_audit = StrategyResolver.resolve(df_with_drift)
    
    audit_cols = {
        'TradeID': 'TradeID', 'LegID': 'LegID', 'Strategy': 'Strategy',
        'AssetType': 'AssetType', 'Quantity': 'Quantity',
        'Entry_Snapshot_TS': 'Entry_Snapshot_TS', 'Underlying_Price_Entry': 'Entry_UL_Price',
        'Delta_Entry': 'Entry_Delta', 'Gamma_Entry': 'Entry_Gamma',
        'Vega_Entry': 'Entry_Vega', 'Theta_Entry': 'Entry_Theta', 'IV_Entry': 'Entry_IV',
        'Snapshot_TS': 'Observation_TS', 'UL Last': 'UL_Price_Now',
        'Last': 'Option_Price_Now', 'Delta': 'Delta_Now', 'Gamma': 'Gamma_Now',
        'Vega': 'Vega_Now', 'Theta': 'Theta_Now', 'IV_Now': 'IV_Now',
        'Price_Drift_Abs': 'Price_Drift', 'Delta_Drift': 'Delta_Drift',
        'Gamma_Drift': 'Gamma_Drift', 'Vega_Drift': 'Vega_Drift',
        'Theta_Drift': 'Theta_Drift',
        'Days_In_Trade': 'Days_In_Trade', 'DTE': 'DTE'
    }
    available_audit_cols = [c for c in audit_cols.keys() if c in df_audit.columns]
    df_export = df_audit[available_audit_cols].rename(columns=audit_cols)
    
    audit_dir = Path("output/cycle2_audit")
    audit_dir.mkdir(parents=True, exist_ok=True)
    today_str = datetime.now().strftime("%Y-%m-%d")
    audit_path = audit_dir / f"cycle2_drift_audit_{today_str}.csv"
    df_export.to_csv(audit_path, index=False)
    logger.info(f"✅ Cycle 2 Audit Export saved to: {audit_path}")
    
    # Classify drift severity
    logger.info("Classifying drift severity...")
    df_with_drift = classify_drift_severity(df_with_drift)
    logger.info(f"✅ Drift severity classified")
    
    # ========== CYCLE 3: RECOMMENDATIONS ==========
    if enable_chart_signals or enable_recommendations:
        logger.info("\n🎯 CYCLE 3: RECOMMENDATIONS (PHASE 7)")
        logger.info("-" * 40)
    
    final_df = df_with_drift
    
    if enable_chart_signals:
        # Load chart signals
        logger.info("Loading chart signals...")
        symbols = df_with_drift['Symbol'].unique().tolist()
        df_chart = load_chart_signals(symbols, source='scan_engine')
        
        if not df_chart.empty:
            final_df = merge_chart_signals(df_with_drift, df_chart)
            logger.info(f"✅ Chart signals merged for {len(df_chart)} symbols")
        else:
            logger.warning("⚠️  No chart signals available, proceeding without")
    
    if enable_recommendations:
        # Generate exit recommendations
        logger.info("Generating authoritative recommendations (Cycle 3)...")
        # RAG: Parity Enforcement. Use the same engine as the dashboard.
        final_df = generate_recommendations(final_df)
        logger.info(f"✅ Authoritative recommendations generated")
    
    # ========== SUMMARY ==========
    logger.info("\n" + "="*80)
    logger.info("PIPELINE COMPLETE")
    logger.info("="*80)
    
    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(f"⏱️  Total execution time: {elapsed:.2f}s")
    logger.info(f"📊 Final dataset: {len(final_df)} rows × {len(final_df.columns)} columns")
    
    # Summary statistics
    if 'Action' in final_df.columns:
        rec_summary = final_df['Action'].value_counts()
        logger.info(f"\n📋 Authoritative Recommendations (Action):")
        for rec, count in rec_summary.items():
            logger.info(f"   {rec}: {count}")
    
    if 'Drift_Severity' in final_df.columns:
        severity_summary = final_df['Drift_Severity'].value_counts()
        logger.info(f"\n⚠️  Drift Severity:")
        for sev, count in severity_summary.items():
            logger.info(f"   {sev}: {count}")
    
    # --- EXPORT TRUTH CONTRACT (Phase 3) ---
    required_columns = [
        "PriceStructure_State", "TrendIntegrity_State", "VolatilityState_State",
        "CompressionMaturity_State", "MomentumVelocity_State", "DirectionalBalance_State",
        "RangeEfficiency_State", "TimeframeAgreement_State", "GreekDominance_State",
        "AssignmentRisk_State", "RegimeStability_State", "Structural_Data_Complete",
        "Resolution_Reason", "HV_20D", "HV_20D_Source", "HV_20D_Computed_TS"
    ]
    
    missing = [c for c in required_columns if c not in final_df.columns]
    if missing:
        logger.error(f"❌ EXPORT TRUTH CONTRACT VIOLATION: Missing columns {missing}")
        raise RuntimeError(f"Abort export: Missing required authoritative columns {missing}")

    # Export final results
    from core.shared.data_contracts.config import SCAN_OUTPUT_DIR
    output_dir = SCAN_OUTPUT_DIR
    output_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = output_dir / f"positions_with_recommendations_{timestamp}.csv"
    final_df.to_csv(csv_path, index=False)
    logger.info(f"\n💾 Results exported to: {csv_path}")
    
    return final_df


if __name__ == "__main__":
    # Configuration
    SCHWAB_CSV = "data/brokerage_inputs/Positions_All_Accounts.csv"
    
    # Run pipeline
    try:
        df_final = run_complete_pipeline(
            schwab_csv_path=SCHWAB_CSV,
            enable_chart_signals=True,
            enable_recommendations=True
        )
        
        logger.info("\n✅ Pipeline executed successfully!")
        logger.info(f"Final DataFrame shape: {df_final.shape}")
        
    except Exception as e:
        logger.error(f"\n❌ Pipeline failed: {e}", exc_info=True)
        sys.exit(1)
