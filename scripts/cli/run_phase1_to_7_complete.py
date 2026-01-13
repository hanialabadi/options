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
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from core.phase1_clean import phase1_load_and_clean_positions
from core.phase2_parse import phase2_run_all
from core.phase3_enrich import run_phase3_enrichment
from core.phase3_enrich.compute_drift_metrics import compute_drift_metrics, classify_drift_severity
from core.phase4_snapshot import save_clean_snapshot
from core.phase7_recommendations.load_chart_signals import load_chart_signals, merge_chart_signals
from core.phase7_recommendations.exit_recommendations import compute_exit_recommendations, prioritize_recommendations

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_complete_pipeline(
    schwab_csv_path: str,
    output_db_path: str = "output/positions_history.duckdb",
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
    df_snapshot, csv_path, db_path, _, _ = save_clean_snapshot(df_enriched, db_path=output_db_path)
    logger.info(f"✅ Phase 4 complete: Snapshot persisted to {db_path}")
    
    # ========== CYCLE 2: DRIFT ANALYSIS ==========
    logger.info("\n🔄 CYCLE 2: TIME-SERIES DRIFT ANALYSIS")
    logger.info("-" * 40)
    
    # Compute drift metrics (Entry vs Current)
    logger.info("Computing drift metrics...")
    df_with_drift = compute_drift_metrics(df_snapshot)
    logger.info(f"✅ Drift metrics computed")
    
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
        logger.info("Generating exit recommendations...")
        final_df = compute_exit_recommendations(final_df)
        final_df = prioritize_recommendations(final_df)
        logger.info(f"✅ Exit recommendations generated")
    
    # ========== SUMMARY ==========
    logger.info("\n" + "="*80)
    logger.info("PIPELINE COMPLETE")
    logger.info("="*80)
    
    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(f"⏱️  Total execution time: {elapsed:.2f}s")
    logger.info(f"📊 Final dataset: {len(final_df)} rows × {len(final_df.columns)} columns")
    
    # Summary statistics
    display_rec_col = 'Rec_Action_Final' if 'Rec_Action_Final' in final_df.columns else 'Recommendation'
    if display_rec_col in final_df.columns:
        rec_summary = final_df[display_rec_col].value_counts()
        logger.info(f"\n📋 Authoritative Recommendations ({display_rec_col}):")
        for rec, count in rec_summary.items():
            logger.info(f"   {rec}: {count}")
    
    if 'Drift_Severity' in final_df.columns:
        severity_summary = final_df['Drift_Severity'].value_counts()
        logger.info(f"\n⚠️  Drift Severity:")
        for sev, count in severity_summary.items():
            logger.info(f"   {sev}: {count}")
    
    # Export final results
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = output_dir / f"positions_with_recommendations_{timestamp}.csv"
    final_df.to_csv(csv_path, index=False)
    logger.info(f"\n💾 Results exported to: {csv_path}")
    
    return final_df


if __name__ == "__main__":
    # Configuration
    SCHWAB_CSV = "data/brokerage_inputs/schwab_positions.csv"
    OUTPUT_DB = "output/positions_history.duckdb"
    
    # Run pipeline
    try:
        df_final = run_complete_pipeline(
            schwab_csv_path=SCHWAB_CSV,
            output_db_path=OUTPUT_DB,
            enable_chart_signals=True,
            enable_recommendations=True
        )
        
        logger.info("\n✅ Pipeline executed successfully!")
        logger.info(f"Final DataFrame shape: {df_final.shape}")
        
    except Exception as e:
        logger.error(f"\n❌ Pipeline failed: {e}", exc_info=True)
        sys.exit(1)
