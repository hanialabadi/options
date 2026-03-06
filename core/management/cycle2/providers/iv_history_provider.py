import logging
import pandas as pd
from datetime import datetime
from core.shared.data_contracts.config import PIPELINE_DB_PATH

logger = logging.getLogger(__name__)

class IVHistoryProvider:
    """
    Manages IV timeseries persistence in DuckDB.
    """
    def __init__(self, db_path: str = None):
        self.db_path = db_path or str(PIPELINE_DB_PATH)
        self._init_db()

    def _init_db(self):
        """Initialize the IV history table in DuckDB."""
        import duckdb
        try:
            with duckdb.connect(self.db_path) as con:
                con.execute("""
                    CREATE TABLE IF NOT EXISTS iv_history (
                        symbol VARCHAR,
                        iv DOUBLE,
                        sensor_ts TIMESTAMP,
                        source VARCHAR
                    )
                """)
                # Create index for faster lookups
                con.execute("CREATE INDEX IF NOT EXISTS idx_iv_history_symbol_ts ON iv_history (symbol, sensor_ts)")
        except Exception as e:
            logger.error(f"Failed to initialize IV history DB: {e}")

    def log_iv_readings(self, readings: list[dict]):
        """
        Persist a batch of IV readings.
        Expected reading format: {'Symbol': str, 'IV': float, 'Source': str, 'Sensor_TS': datetime}
        """
        if not readings:
            return

        import duckdb
        try:
            df = pd.DataFrame(readings)
            # Ensure column names match DB schema
            df = df.rename(columns={
                'Symbol': 'symbol',
                'IV': 'iv',
                'Sensor_TS': 'sensor_ts',
                'Source': 'source'
            })
            
            # RAG: Quality. Filter out NaN IVs before persisting.
            # We only want to track valid volatility readings.
            df = df[df['iv'].notna()]
            
            if df.empty:
                return

            # Select only relevant columns
            df = df[['symbol', 'iv', 'sensor_ts', 'source']]
            
            with duckdb.connect(self.db_path) as con:
                con.execute("INSERT INTO iv_history SELECT * FROM df")
            logger.info(f"Persisted {len(df)} IV readings to history")
        except Exception as e:
            logger.error(f"Failed to persist IV readings: {e}")

    def get_iv_history(self, symbol: str, days: int = 30) -> pd.DataFrame:
        """Retrieve IV history for a symbol."""
        import duckdb
        try:
            with duckdb.connect(self.db_path, read_only=True) as con:
                query = """
                    SELECT iv, sensor_ts, source
                    FROM iv_history
                    WHERE symbol = ? AND sensor_ts >= current_timestamp - interval '?' day
                    ORDER BY sensor_ts ASC
                """
                return con.execute(query, [symbol, days]).df()
        except Exception as e:
            logger.error(f"Error reading IV history for {symbol}: {e}")
            return pd.DataFrame()

    def get_earliest_iv_batch(self, symbols: list[str]) -> dict[str, float]:
        """
        Retrieve the earliest recorded IV for a list of symbols.
        Used to establish anchors for positions discovered before IV tracking was active.
        """
        if not symbols:
            return {}
        import duckdb
        try:
            placeholders = ', '.join(['?' for _ in symbols])
            with duckdb.connect(self.db_path, read_only=True) as con:
                query = f"""
                    SELECT symbol, iv
                    FROM iv_history
                    WHERE symbol IN ({placeholders})
                    QUALIFY row_number() OVER (PARTITION BY symbol ORDER BY sensor_ts ASC) = 1
                """
                res = con.execute(query, symbols).fetchall()
                return {row[0]: row[1] for row in res if row[1] is not None}
        except Exception as e:
            logger.error(f"Failed to fetch earliest IV batch: {e}")
            return {}

    def get_latest_iv_batch(self, symbols: list[str]) -> dict[str, float]:
        """
        Retrieve the most recent recorded IV for a list of symbols.
        Used as a fallback when transient IV fetch returns NaN.
        """
        if not symbols:
            return {}
        import duckdb
        try:
            placeholders = ', '.join(['?' for _ in symbols])
            with duckdb.connect(self.db_path, read_only=True) as con:
                query = f"""
                    SELECT symbol, iv
                    FROM iv_history
                    WHERE symbol IN ({placeholders})
                    QUALIFY row_number() OVER (PARTITION BY symbol ORDER BY sensor_ts DESC) = 1
                """
                res = con.execute(query, symbols).fetchall()
                return {row[0]: row[1] for row in res if row[1] is not None}
        except Exception as e:
            logger.error(f"Failed to fetch latest IV batch: {e}")
            return {}

_iv_history_provider = IVHistoryProvider()

def log_iv_readings(readings: list[dict]):
    _iv_history_provider.log_iv_readings(readings)

def get_earliest_iv_batch(symbols: list[str]) -> dict[str, float]:
    return _iv_history_provider.get_earliest_iv_batch(symbols)

def get_latest_iv_batch(symbols: list[str]) -> dict[str, float]:
    return _iv_history_provider.get_latest_iv_batch(symbols)
