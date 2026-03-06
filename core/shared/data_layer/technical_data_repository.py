import duckdb
import pandas as pd
import logging # Added missing import
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Dict, Any

from core.shared.data_contracts.config import PIPELINE_DB_PATH
from core.shared.data_layer.duckdb_utils import get_duckdb_connection, get_duckdb_write_connection, _table_exists

logger = logging.getLogger(__name__)

TECHNICAL_INDICATORS_TABLE = "technical_indicators"

def initialize_technical_indicators_table(con: Optional[duckdb.DuckDBPyConnection] = None):
    """
    Initializes the technical_indicators table in DuckDB.
    Uses an existing connection if provided, otherwise opens a new write connection.
    """
    _con = con if con is not None else get_duckdb_write_connection()
    try:
        _con.execute(f"""
            CREATE TABLE IF NOT EXISTS {TECHNICAL_INDICATORS_TABLE} (
                Ticker VARCHAR,
                Snapshot_TS TIMESTAMP,
                RSI_14 DOUBLE,
                ADX_14 DOUBLE,
                SMA_20 DOUBLE,
                SMA_50 DOUBLE,
                EMA_9 DOUBLE,
                EMA_21 DOUBLE,
                ATR_14 DOUBLE,
                MACD DOUBLE,
                MACD_Signal DOUBLE,
                UpperBand_20 DOUBLE,
                MiddleBand_20 DOUBLE,
                LowerBand_20 DOUBLE,
                SlowK_5_3 DOUBLE,
                SlowD_5_3 DOUBLE,
                IV_Rank_30D DOUBLE,
                PCS_Score_V2 DOUBLE,
                PRIMARY KEY (Ticker, Snapshot_TS)
            )
        """)
        logger.info(f"✅ {TECHNICAL_INDICATORS_TABLE} table initialized in DuckDB.")
    except Exception as e:
        logger.error(f"❌ Failed to initialize {TECHNICAL_INDICATORS_TABLE} table: {e}")
    finally:
        if con is None: # Only close if this function opened the connection
            _con.close()

def insert_technical_indicators(df_indicators: pd.DataFrame, con: Optional[duckdb.DuckDBPyConnection] = None):
    """
    Inserts a DataFrame of technical indicators into the DuckDB repository.
    Assumes df_indicators has 'Ticker', 'Snapshot_TS', and indicator columns.
    Uses an existing connection if provided, otherwise opens a new write connection.
    """
    if df_indicators.empty:
        return

    _con = con if con is not None else get_duckdb_write_connection()
    try:
        _con.execute(f"INSERT OR REPLACE INTO {TECHNICAL_INDICATORS_TABLE} SELECT * FROM df_indicators")
        logger.debug(f"✅ Inserted/Updated {len(df_indicators)} rows into {TECHNICAL_INDICATORS_TABLE}.")
    except Exception as e:
        logger.error(f"❌ Failed to insert technical indicators into DuckDB: {e}")
    finally:
        if con is None: # Only close if this function opened the connection
            _con.close()

def get_latest_technical_indicators(
    tickers: List[str], 
    db_path: Optional[str] = None,
    con: Optional[duckdb.DuckDBPyConnection] = None
) -> Optional[pd.DataFrame]:
    """
    Retrieves the latest technical indicators for a list of tickers.
    Uses an existing connection if provided, otherwise opens a new read-only connection.
    """
    _con = con if con is not None else get_duckdb_connection(db_path, read_only=True)
    try:
        if not _table_exists(_con, TECHNICAL_INDICATORS_TABLE):
            logger.warning(f"Table {TECHNICAL_INDICATORS_TABLE} not found in DuckDB.")
            return None

        placeholders = ', '.join(['?' for _ in tickers])
        query = f"""
            WITH ranked_indicators AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (PARTITION BY Ticker ORDER BY Snapshot_TS DESC) as rn
                FROM {TECHNICAL_INDICATORS_TABLE}
                WHERE Ticker IN ({placeholders})
            )
            SELECT * EXCLUDE (rn)
            FROM ranked_indicators
            WHERE rn = 1
        """
        return _con.execute(query, tickers).df()
    except FileNotFoundError as e:
        logger.error(f"DuckDB not found: {e}")
        return None
    except Exception as e:
        logger.error(f"Error fetching latest technical indicators from DuckDB: {e}")
        return None
    finally:
        if con is None: # Only close if this function opened the connection
            _con.close()

# Removed automatic initialization: initialize_technical_indicators_table()
