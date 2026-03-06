"""
Cycle 1 (Perception) Only Runner

Runs Phases 1-4 of the Management Engine:
1. Ingest (Clean)
2. Identity Resolution (Parse & Validate)
3. Entry Freeze Validation
4. Snapshot Assembly

Usage:
    python scripts/cli/run_cycle1_only.py
"""

import pandas as pd
import logging
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.management.cycle1.ingest.clean import phase1_load_and_clean_positions
from core.management.cycle1.identity.parse import phase2_run_all
from core.management.cycle1.snapshot.snapshot import save_clean_snapshot, validate_cycle1_ledger

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_cycle1(
    schwab_csv_path: str,
    output_db_path: str | None = None
) -> pd.DataFrame:
    """
    Run Cycle 1: Perception Loop.
    """
    from core.shared.data_contracts.config import PIPELINE_DB_PATH
    output_db_path = output_db_path or str(PIPELINE_DB_PATH)

    # Startup Validation: Ensure ledger matches contract
    validate_cycle1_ledger(db_path=output_db_path)

    logger.info("="*80)
    logger.info("STARTING CYCLE 1: PERCEPTION")
    logger.info("="*80)
    
    start_time = datetime.now()
    
    # Phase 1: Clean raw data
    logger.info("Phase 1: Cleaning raw data...")
    df_clean, _ = phase1_load_and_clean_positions(input_path=Path(schwab_csv_path))
    if df_clean.empty:
        logger.error("❌ Phase 1 returned empty DataFrame")
        return df_clean
    logger.info(f"✅ Phase 1 complete: {len(df_clean)} positions cleaned")
    
    # Phase 2: Parse structures & Identity Resolution
    logger.info("Phase 2: Parsing structures & Identity Resolution...")
    df_parsed = phase2_run_all(df_clean)
    logger.info(f"✅ Phase 2 complete: {len(df_parsed)} positions structured")
    
    # === CYCLE 1 BOUNDARY ENFORCEMENT ===
    # Phase 3 (Enrichment) is explicitly forbidden in Cycle 1.
    # All interpretive and relative metrics belong in Cycle 2/3.
    df_base = df_parsed.copy()

    # === HARD INTEGRITY GATE: Identity Invariants ===
    logger.info("Enforcing Identity Invariants...")
    
    # 1. No TradeID mixes expirations
    exp_counts = df_base.groupby("TradeID")["Expiration"].nunique()
    mixed_exp = exp_counts[exp_counts > 1]
    if not mixed_exp.empty:
        logger.error(f"❌ DATA INTEGRITY VIOLATION: TradeIDs with mixed expirations: {mixed_exp.index.tolist()}")
        raise ValueError(f"FATAL: {len(mixed_exp)} TradeIDs violate the 'one lifecycle' invariant (mixed expirations).")

    # 2. No SINGLE_LEG option TradeID has multiple strikes
    single_leg_options = df_base[
        (df_base["Structure"] == "Single-leg") & 
        (df_base["AssetType"] == "OPTION")
    ]
    strike_counts = single_leg_options.groupby("TradeID")["Strike"].nunique()
    mixed_strikes = strike_counts[strike_counts > 1]
    if not mixed_strikes.empty:
        logger.error(f"❌ DATA INTEGRITY VIOLATION: Single-leg TradeIDs with mixed strikes: {mixed_strikes.index.tolist()}")
        raise ValueError(f"FATAL: {len(mixed_strikes)} Single-leg TradeIDs violate the 'atomic object' invariant (mixed strikes).")
    
    logger.info("✅ Identity Invariants verified: No collisions detected.")

    # === STRIP INTERPRETIVE FIELDS (PHASE CREEP PREVENTION) ===
    # RAG: Neutrality Mandate. No interpretive or judgmental fields may cross the Snapshot boundary.
    interpretive_cols = [
        'Strategy', 'Structure', 'LegRole', 'LegIndex', 'LegCount',
        'Premium_Estimated', 'Structure_Valid', 'Validation_Errors',
        'Needs_Structural_Fix', 'Is_Optionable', 'Stock_Used_In_Options',
        'Stock_Option_Status', 'Option_Eligibility', 'Option_Usage',
        'Covered_Call_Contracts', 'Covered_Call_Coverage_Ratio', 'Covered_Call_Stock_Shares'
    ]
    df_snapshot_input = df_base.drop(columns=[c for c in interpretive_cols if c in df_base.columns]).copy()

    # Phase 4: Persist snapshot (includes Entry freeze)
    logger.info("Phase 4: Persisting snapshot with Entry freeze...")
    df_snapshot, csv_path, run_id, _, _ = save_clean_snapshot(
        df_snapshot_input, 
        db_path=output_db_path,
        source_file_path=schwab_csv_path,
        ingest_context="cli_cycle1"
    )
    logger.info(f"✅ Phase 4 complete: Snapshot persisted with run_id {run_id}")

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(f"⏱️  Cycle 1 execution time: {elapsed:.2f}s")
    logger.info(f"📊 Final dataset: {len(df_snapshot)} rows × {len(df_snapshot.columns)} columns")

    return df_snapshot


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run Cycle 1 (Perception) Only")
    parser.add_argument("--file", required=True, help="Path to brokerage positions CSV")
    parser.add_argument("--db", default=None, help="Path to DuckDB database")
    args = parser.parse_args()

    # Run Cycle 1
    try:
        df_final = run_cycle1(
            schwab_csv_path=args.file,
            output_db_path=args.db
        )
        logger.info("\n✅ Cycle 1 executed successfully!")
    except Exception as e:
        logger.error(f"\n❌ Cycle 1 failed: {e}", exc_info=True)
        sys.exit(1)
