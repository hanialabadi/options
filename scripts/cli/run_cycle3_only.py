"""
Cycle 3 (Decision) Only Runner

Loads Cycle 2 drift metrics and generates actionable recommendations.
Usage:
    python scripts/cli/run_cycle3_only.py
"""

import pandas as pd
import logging
import pandas as pd
import logging
import sys
from pathlib import Path
import duckdb

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.management.cycle2.drift.compute_basic_drift import compute_drift_metrics
from core.management.cycle3.decision.engine import generate_recommendations
from core.shared.data_contracts.config import PIPELINE_DB_PATH
from core.shared.data_layer.data_selectors import get_latest_ledger_snapshot

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_cycle3(db_path: str | None = None):
    """
    Run Cycle 3: Decision Loop.
    """
    db_path = db_path or str(PIPELINE_DB_PATH)
    
    logger.info("="*80)
    logger.info("STARTING CYCLE 3: DECISION (JUDGMENT)")
    logger.info("="*80)
    
    if not Path(db_path).exists():
        logger.error(f"❌ DuckDB not found at {db_path}. Run Cycle 1 first.")
        return

    try:
        with duckdb.connect(db_path, read_only=True) as con:
            # Load latest snapshot using the harmonized selector (prioritizing enriched_legs_v1)
            df_snapshot, latest_run_id = get_latest_ledger_snapshot(con, "enriched_legs_v1")
            if df_snapshot.empty:
                df_snapshot, latest_run_id = get_latest_ledger_snapshot(con, "clean_legs_v2")
            
            if df_snapshot.empty:
                logger.warning("⚠️ No data found in latest snapshot from any ledger table.")
                return

            # --- MANDATORY ANCHOR JOIN ---
            # RAG: Auditability. Measurement must join against frozen anchors.
            df_anchors = con.execute("SELECT * FROM entry_anchors").df()
            
            print(f"DEBUG: df_snapshot head:\n{df_snapshot.head()}")
            print(f"DEBUG: df_anchors head:\n{df_anchors.head()}")
            print(f"DEBUG: df_snapshot columns: {df_snapshot.columns.tolist()}")
            print(f"DEBUG: df_anchors columns: {df_anchors.columns.tolist()}")

            # Identify common _Entry columns that should be sourced from anchors
            entry_cols_to_merge = [
                'Delta_Entry', 'Gamma_Entry', 'Vega_Entry', 'Theta_Entry', 
                'Rho_Entry', 'IV_Entry', 'IV_Entry_Source', 'Underlying_Price_Entry', 
                'Entry_Snapshot_TS', 'Quantity_Entry', 'Basis_Entry'
            ]
            
            # Filter df_anchors to only relevant columns for merging
            # Ensure LegID is always present for the merge key
            cols_from_anchors = ['LegID'] + [col for col in entry_cols_to_merge if col in df_anchors.columns]
            
            df_base = df_snapshot.merge(
                df_anchors[cols_from_anchors],
                on='LegID',
                how='left',
                suffixes=('', '_anchor') # Use suffix for anchor columns to avoid collision
            )
            
            # Coalesce _Entry columns: prefer values from df_anchors
            for col in entry_cols_to_merge:
                if col in df_base.columns and f"{col}_anchor" in df_base.columns:
                    df_base[col] = df_base[col].fillna(df_base[f"{col}_anchor"])
                    df_base = df_base.drop(columns=[f"{col}_anchor"])
                elif f"{col}_anchor" in df_base.columns: # If original snapshot didn't have it, but anchor does
                    df_base[col] = df_base[f"{col}_anchor"]
                    df_base = df_base.drop(columns=[f"{col}_anchor"])

            print(f"DEBUG: df_base head after merge and coalesce:\n{df_base.head()}")
            print(f"DEBUG: df_base columns after merge and coalesce: {df_base.columns.tolist()}")

            # 1. Execute Cycle 2 (Measurement)
            logger.info("Executing Cycle 2 Measurement...")
            df_drift = compute_drift_metrics(df_base)

            # 2. Execute Cycle 3 (Decision)
            logger.info("Executing Cycle 3 Decision...")
            df_rec = generate_recommendations(df_drift)

            # Summary Output (Judgment Only)
            logger.info("="*40)
            logger.info("DECISION SUMMARY")
            logger.info("="*40)
            
            summary_cols = [
                'Symbol', 'Action', 'Urgency', 'Rationale', 'RAG_Citation'
            ]
            available_summary = [c for c in summary_cols if c in df_rec.columns]
            
            print(df_rec[available_summary].to_string(index=False))
            
            logger.info("="*40)
            logger.info(f"✅ Cycle 3 complete: {len(df_rec)} recommendations generated.")

    except Exception as e:
        logger.error(f"❌ Cycle 3 failed: {e}", exc_info=True)

if __name__ == "__main__":
    run_cycle3()
