"""
Cycle 2 (Drift) Only Runner

Runs Phase 5-6 of the Management Engine:
1. Load latest Cycle 1 Snapshot
2. Compute Drift Metrics (Entry vs Current)
3. Classify Drift Severity
4. Persist Drift Ledger

Usage:
    python scripts/cli/run_cycle2_only.py
"""

import pandas as pd
import logging
import pandas as pd
import logging
import sys
import os
from pathlib import Path
from core.shared.data_contracts.config import SENSORS_DB_PATH
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.management.cycle2.drift.compute_basic_drift import compute_drift_metrics
from core.management.cycle3.decision.resolver import StrategyResolver
from core.shared.data_contracts.config import PIPELINE_DB_PATH
from core.shared.data_layer.data_selectors import get_latest_ledger_snapshot

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_cycle2(db_path: str = None) -> pd.DataFrame:
    """
    Run Cycle 2: Drift Analysis.
    """
    db_path = db_path or str(PIPELINE_DB_PATH)
    
    logger.info("="*80)
    logger.info("STARTING CYCLE 2: DRIFT ANALYSIS")
    logger.info("="*80)
    
    import duckdb
    with duckdb.connect(db_path) as con:
        # --- PREFLIGHT CHECKS ---
        logger.info("Running Cycle-2 Preflight Checks...")
        
        # 1. Today's snapshot
        # 1. Today's snapshot
        today = datetime.now().strftime('%Y-%m-%d')
        
        # Prioritize enriched_legs_v1, then clean_legs_v2
        df_latest_snapshot, latest_run_id = get_latest_ledger_snapshot(con, "enriched_legs_v1")
        if df_latest_snapshot.empty:
            df_latest_snapshot, latest_run_id = get_latest_ledger_snapshot(con, "clean_legs_v2")
        
        print(f"DEBUG: latest_run_id from selector: {latest_run_id}")
        
        has_today = False
        if latest_run_id:
            # Extract date from run_id (format: YYYY-MM-DD_HH-MM-SS-ms)
            run_date_str = latest_run_id.split('_')[0]
            print(f"DEBUG: run_date_str: {run_date_str}, today: {today}")
            if run_date_str == today:
                has_today = True

        if not has_today:
            raise RuntimeError(f"Preflight Failed: No Cycle-1 snapshot found for today ({today})")
            
        # 2. Anchor Integrity
        integrity_fail = con.execute("""
            SELECT TradeID, LegID, COUNT(*) AS cnt
            FROM entry_anchors
            GROUP BY TradeID, LegID
            HAVING cnt != 1
        """).fetchone()
        if integrity_fail:
            raise RuntimeError("Preflight Failed: Anchor integrity violation (multiple anchors per leg)")

        logger.info("✅ Preflight checks passed.")

        # --- EXECUTION ---
        # Load latest snapshot joined with anchors and sensors
        logger.info("Loading latest snapshot, entry anchors, and market sensors...")
        
        # Use the new selector for the latest snapshot (prioritizing enriched_legs_v1)
        df_latest_snapshot, latest_run_id = get_latest_ledger_snapshot(con, "enriched_legs_v1")
        if df_latest_snapshot.empty:
            df_latest_snapshot, latest_run_id = get_latest_ledger_snapshot(con, "clean_legs_v2")

        if df_latest_snapshot.empty:
            logger.warning("No data found for drift analysis from any ledger table.")
            return pd.DataFrame() # Return empty DataFrame if no snapshot

        # Attach sensor DB
        sensor_db_path = str(SENSORS_DB_PATH)
        sensor_join_query = ""
        if os.path.exists(sensor_db_path):
            con.execute(f"ATTACH '{sensor_db_path}' AS sensors")
            sensor_join_query = """
                LEFT JOIN (
                    SELECT LegID, IV as IV_Now, Sensor_TS
                    FROM sensors.sensor_readings
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY LegID ORDER BY Sensor_TS DESC) = 1
                ) s ON c.LegID = s.LegID
            """
        else:
            logger.warning(f"Sensor DB not found at {sensor_db_path}. IV_Now will be NaN.")
            sensor_join_query = "LEFT JOIN (SELECT NULL as LegID, NULL as IV_Now) s ON 1=0"

        # Register df_latest_snapshot as a temporary view for joining
        temp_view_name = "latest_ledger_view"
        con.register(temp_view_name, df_latest_snapshot)

        query = f"""
            SELECT 
                c.*,
                e.Strike AS Strike_Entry,
                e.Expiration AS Expiration_Entry,
                e."UL Last" AS Underlying_Price_Entry,
                e.Delta AS Delta_Entry,
                e.Gamma AS Gamma_Entry,
                e.Vega AS Vega_Entry,
                e.Theta AS Theta_Entry,
                e.IV_Entry,
                e.IV_Entry_Source,
                e.Entry_Snapshot_TS,
                e.Quantity AS Quantity_Entry,
                e.Basis AS Basis_Entry,
                s.IV_Now
            FROM {temp_view_name} c
            JOIN entry_anchors e ON c.TradeID = e.TradeID AND c.LegID = e.LegID
            {sensor_join_query}
        """
        df_latest = con.execute(query).df()
        
        # Unregister the temporary view
        con.unregister(temp_view_name)
        
    if df_latest.empty:
        logger.warning("No data found for drift analysis after joining.")
        return df_latest

    # Compute Drift
    logger.info(f"Computing drift for run_id: {latest_run_id}")
    df_drift = compute_drift_metrics(df_latest)

    # Resolve Strategy for Audit (Cycle 2 remains agnostic, but Audit needs it)
    df_audit = StrategyResolver.resolve(df_drift)
    
    # --- CYCLE 2 AUDIT EXPORT (MANDATORY) ---
    # RAG: Auditability Mandate. Numeric proof of drift.
    audit_cols = {
        # Identity
        'TradeID': 'TradeID',
        'LegID': 'LegID',
        'Strategy': 'Strategy',
        'AssetType': 'AssetType',
        'Quantity': 'Quantity',
        # Entry Anchors
        'Entry_Snapshot_TS': 'Entry_Snapshot_TS',
        'Underlying_Price_Entry': 'Entry_UL_Price',
        'Basis_Entry': 'Entry_Basis', # Basis is total, not price, but useful
        'Delta_Entry': 'Entry_Delta',
        'Gamma_Entry': 'Entry_Gamma',
        'Vega_Entry': 'Entry_Vega',
        'Theta_Entry': 'Entry_Theta',
        'IV_Entry': 'Entry_IV',
        # Current Observation
        'Snapshot_TS': 'Observation_TS',
        'UL Last': 'UL_Price_Now',
        'Last': 'Option_Price_Now',
        'Delta': 'Delta_Now',
        'Gamma': 'Gamma_Now',
        'Vega': 'Vega_Now',
        'Theta': 'Theta_Now',
        'IV_Now': 'IV_Now', # May be NaN if not in Schwab CSV
        # Pure Math
        'Price_Drift_Abs': 'Price_Drift',
        'Delta_Drift': 'Delta_Drift',
        'Gamma_Drift': 'Gamma_Drift',
        'Vega_Drift': 'Vega_Drift',
        'Theta_Drift': 'Theta_Drift',
        'Days_In_Trade': 'Days_In_Trade',
        'DTE': 'DTE'
    }
    
    # Ensure all columns exist before selecting
    available_audit_cols = [c for c in audit_cols.keys() if c in df_audit.columns]
    df_export = df_audit[available_audit_cols].rename(columns=audit_cols)
    
    output_dir = Path("output/cycle2_audit")
    output_dir.mkdir(parents=True, exist_ok=True)
    today_str = datetime.now().strftime("%Y-%m-%d")
    audit_path = output_dir / f"cycle2_drift_audit_{today_str}.csv"
    df_export.to_csv(audit_path, index=False)
    
    logger.info(f"✅ Cycle 2 Audit Export saved to: {audit_path}")
    
    # Also save the full drift report
    report_dir = Path("output/drift")
    report_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = report_dir / f"drift_report_{timestamp}.csv"
    df_drift.to_csv(report_path, index=False)
    
    logger.info(f"✅ Cycle 2 complete. Drift report saved to: {report_path}")
    
    # Summary of Drift
    if 'Price_Drift_Pct' in df_drift.columns:
        avg_price_drift = df_drift['Price_Drift_Pct'].mean()
        logger.info(f"📊 Average Price Drift: {avg_price_drift:.2%}")
        
    return df_drift

if __name__ == "__main__":
    try:
        run_cycle2()
    except Exception as e:
        logger.error(f"❌ Cycle 2 failed: {e}")
        sys.exit(1)
