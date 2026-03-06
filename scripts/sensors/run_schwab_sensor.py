import os
import argparse
import logging
import duckdb
import pandas as pd
from datetime import datetime
from pathlib import Path

from core.management.cycle2.providers.schwab_iv_provider import fetch_sensor_readings
from core.shared.data_contracts.config import SENSORS_DB_PATH

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent.parent
DEFAULT_PIPELINE_DB = PROJECT_ROOT / "data" / "pipeline.duckdb"
DEFAULT_SENSOR_DB = SENSORS_DB_PATH

def setup_sensor_db(db_path: str):
    """Initialize the sensor_readings table if it doesn't exist."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    with duckdb.connect(db_path) as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS sensor_readings (
                TradeID TEXT,
                LegID TEXT,
                Sensor_TS TIMESTAMP,
                Source TEXT,
                UL_Last DOUBLE,
                Opt_Last DOUBLE,
                IV DOUBLE,
                Delta DOUBLE,
                Gamma DOUBLE,
                Vega DOUBLE,
                Theta DOUBLE,
                Rho DOUBLE
            )
        """)
        logger.info(f"✅ Sensor database initialized at {db_path}")

def get_active_legs(pipeline_db: str) -> pd.DataFrame:
    """Fetch active TradeID and LegID from Cycle 1 canonical anchors."""
    if not os.path.exists(pipeline_db):
        logger.error(f"❌ Pipeline database not found at {pipeline_db}")
        return pd.DataFrame()

    with duckdb.connect(pipeline_db) as con:
        # Use canonical_anchors view to ensure identity hygiene
        # We filter for legs that are present in the LATEST snapshot to define 'active'
        query = """
            WITH latest_snapshot AS (
                SELECT TradeID, LegID 
                FROM clean_legs 
                WHERE Snapshot_TS = (SELECT MAX(Snapshot_TS) FROM clean_legs)
            )
            SELECT a.TradeID, a.LegID, a.Symbol
            FROM canonical_anchors a
            JOIN latest_snapshot l ON a.TradeID = l.TradeID AND a.LegID = l.LegID
        """
        df = con.execute(query).df()
        return df

def run_sensor(active_only: bool, sensor_db: str, pipeline_db: str):
    """Main sensor loop: Fetch from Schwab and append to DuckDB."""
    setup_sensor_db(sensor_db)
    
    df_active = get_active_legs(pipeline_db)
    if df_active.empty:
        logger.warning("No active legs found to sense.")
        return

    symbols = df_active['Symbol'].unique().tolist()
    logger.info(f"Sensing {len(symbols)} symbols for {len(df_active)} legs...")

    readings = fetch_sensor_readings(symbols)
    if not readings:
        logger.error("Failed to fetch any readings from Schwab.")
        return

    # Map readings back to LegID/TradeID
    # Note: One symbol might belong to multiple LegIDs (unlikely but possible in some setups)
    # Here we join on Symbol
    df_readings = pd.DataFrame(readings)
    df_final = df_active.merge(df_readings, on='Symbol', how='inner')

    if df_final.empty:
        logger.warning("No readings matched active legs.")
        return

    # Prepare for insertion
    # sensor_readings (TradeID, LegID, Sensor_TS, Source, UL_Last, Opt_Last, IV, Delta, Gamma, Vega, Theta, Rho)
    insert_cols = [
        'TradeID', 'LegID', 'Sensor_TS', 'Source', 
        'UL_Last', 'Opt_Last', 'IV', 
        'Delta', 'Gamma', 'Vega', 'Theta', 'Rho'
    ]
    df_to_insert = df_final[insert_cols]

    with duckdb.connect(sensor_db) as con:
        con.execute("INSERT INTO sensor_readings SELECT * FROM df_to_insert")
        logger.info(f"✅ Appended {len(df_to_insert)} sensor readings to {sensor_db}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Schwab Market Sensor Ingestion")
    parser.add_argument("--active-only", action="store_true", help="Only sense active legs")
    parser.add_argument("--db", default=str(DEFAULT_SENSOR_DB), help="Path to sensor DuckDB")
    parser.add_argument("--pipeline-db", default=str(DEFAULT_PIPELINE_DB), help="Path to pipeline DuckDB")
    
    args = parser.parse_args()
    run_sensor(args.active_only, args.db, args.pipeline_db)
