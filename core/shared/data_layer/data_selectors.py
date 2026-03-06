import duckdb
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def get_latest_ledger_snapshot(con: duckdb.DuckDBPyConnection, table_name: str) -> pd.DataFrame:
    """
    Retrieves the latest snapshot (by run_id) from a given ledger table.
    
    Args:
        con: An active DuckDB connection.
        table_name: The name of the ledger table ('clean_legs_v2' or 'enriched_legs_v1').
        
    Returns:
        A pandas DataFrame containing the latest snapshot, or an empty DataFrame if no data.
    """
    try:
        # Check if table exists
        table_exists = con.execute(f"""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_name = '{table_name}' AND table_schema = 'main'
        """).fetchone()[0] > 0
        
        if not table_exists:
            logger.warning(f"Table '{table_name}' does not exist in DuckDB.")
            return pd.DataFrame(), None

        # Get the latest run_id by ordering by Snapshot_TS, then run_id for tie-breaking
        # This ensures we get the chronologically latest snapshot.
        latest_run_id_query = f"""
            SELECT run_id FROM {table_name}
            ORDER BY Snapshot_TS DESC, run_id DESC
            LIMIT 1;
        """
        latest_run_id = con.execute(latest_run_id_query).fetchone()[0]
        
        if not latest_run_id:
            logger.info(f"No data found in '{table_name}'.")
            return pd.DataFrame(), None

        # Retrieve the latest snapshot for that run_id
        query = f"SELECT * FROM {table_name} WHERE run_id = '{latest_run_id}' AND LegID IS NOT NULL;"
        df_snapshot = con.execute(query).fetchdf()
        
        if df_snapshot.empty:
            logger.warning(f"No data found for latest run_id '{latest_run_id}' in '{table_name}'.")

        return df_snapshot, latest_run_id

    except Exception as e:
        logger.error(f"Error retrieving latest snapshot from '{table_name}': {e}")
        return pd.DataFrame(), None
