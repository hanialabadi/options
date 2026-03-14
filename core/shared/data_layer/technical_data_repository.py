import duckdb
import pandas as pd
import logging
from datetime import datetime
from typing import List, Optional

from core.shared.data_contracts.config import PIPELINE_DB_PATH
from core.shared.data_layer.duckdb_utils import (
    get_duckdb_connection, get_duckdb_write_connection, _table_exists,
    get_domain_connection, get_domain_write_connection, DbDomain,
)

logger = logging.getLogger(__name__)

TECHNICAL_INDICATORS_TABLE = "technical_indicators"

# Signal Hub schema version — bump when adding columns or changing semantics.
SIGNAL_VERSION = 2


def initialize_technical_indicators_table(con: Optional[duckdb.DuckDBPyConnection] = None):
    """
    Initializes the technical_indicators table in DuckDB.
    Creates with full v2 schema (34 columns) for new databases.
    Migrates existing tables to add missing columns.
    """
    _con = con if con is not None else get_domain_write_connection(DbDomain.CHART)
    try:
        _con.execute(f"""
            CREATE TABLE IF NOT EXISTS {TECHNICAL_INDICATORS_TABLE} (
                Ticker VARCHAR,
                Snapshot_TS TIMESTAMP,
                -- Core indicators (v1)
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
                -- Institutional signals (v2 — Signal Hub)
                Market_Structure VARCHAR,
                OBV_Slope DOUBLE,
                Volume_Ratio DOUBLE,
                RSI_Divergence VARCHAR,
                MACD_Divergence VARCHAR,
                Weekly_Trend_Bias VARCHAR,
                Keltner_Squeeze_On BOOLEAN,
                Keltner_Squeeze_Fired BOOLEAN,
                RS_vs_SPY_20d DOUBLE,
                -- Derived chart analytics (v2)
                Chart_Regime VARCHAR,
                BB_Position DOUBLE,
                ATR_Rank DOUBLE,
                MACD_Histogram DOUBLE,
                Trend_Slope DOUBLE,
                -- Infrastructure (v2)
                Computed_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                Signal_Version INTEGER NOT NULL DEFAULT 2,
                PRIMARY KEY (Ticker, Snapshot_TS)
            )
        """)
        # Migrate existing tables that may lack v2 columns
        _migrate_technical_indicators_v2(_con)
        # Create indexes for query performance
        _ensure_indexes(_con)
        logger.info(f"✅ {TECHNICAL_INDICATORS_TABLE} table initialized (v{SIGNAL_VERSION}).")
    except Exception as e:
        logger.error(f"❌ Failed to initialize {TECHNICAL_INDICATORS_TABLE} table: {e}")
    finally:
        if con is None:
            _con.close()


def _migrate_technical_indicators_v2(con: duckdb.DuckDBPyConnection):
    """
    Signal Hub migration: add v2 columns to existing technical_indicators table.
    Idempotent — safe to call on every startup.
    """
    new_columns = [
        # Institutional signals
        ("Market_Structure",      "VARCHAR"),
        ("OBV_Slope",             "DOUBLE"),
        ("Volume_Ratio",          "DOUBLE"),
        ("RSI_Divergence",        "VARCHAR"),
        ("MACD_Divergence",       "VARCHAR"),
        ("Weekly_Trend_Bias",     "VARCHAR"),
        ("Keltner_Squeeze_On",    "BOOLEAN"),
        ("Keltner_Squeeze_Fired", "BOOLEAN"),
        ("RS_vs_SPY_20d",         "DOUBLE"),
        # Derived chart analytics
        ("Chart_Regime",          "VARCHAR"),
        ("BB_Position",           "DOUBLE"),
        ("ATR_Rank",              "DOUBLE"),
        ("MACD_Histogram",        "DOUBLE"),
        ("Trend_Slope",           "DOUBLE"),
        # Infrastructure — DuckDB ALTER TABLE doesn't support constraints,
        # so these are added as bare types; defaults are handled post-ALTER.
        ("Computed_TS",           "TIMESTAMP"),
        ("Signal_Version",        "INTEGER"),
    ]
    for col_name, col_type in new_columns:
        try:
            con.execute(
                f"ALTER TABLE {TECHNICAL_INDICATORS_TABLE} ADD COLUMN {col_name} {col_type}"
            )
            logger.debug(f"Added column {col_name} to {TECHNICAL_INDICATORS_TABLE}")
        except Exception as e:
            if "already exists" in str(e).lower() or "duplicate" in str(e).lower():
                pass  # Column already exists — idempotent
            else:
                logger.warning(f"⚠️ Failed to add column {col_name}: {e}")

    # Backfill defaults for migrated infrastructure columns
    try:
        con.execute(f"""
            UPDATE {TECHNICAL_INDICATORS_TABLE}
            SET Signal_Version = 2
            WHERE Signal_Version IS NULL
        """)
        con.execute(f"""
            UPDATE {TECHNICAL_INDICATORS_TABLE}
            SET Computed_TS = CURRENT_TIMESTAMP
            WHERE Computed_TS IS NULL
        """)
    except Exception:
        pass  # Non-critical

    # Mark legacy rows (those without institutional signals) as v1
    try:
        con.execute(f"""
            UPDATE {TECHNICAL_INDICATORS_TABLE}
            SET Signal_Version = 1
            WHERE Market_Structure IS NULL AND Signal_Version = 2
        """)
    except Exception:
        pass  # Non-critical — legacy marking is best-effort


def _ensure_indexes(con: duckdb.DuckDBPyConnection):
    """Create indexes for query performance."""
    for stmt in [
        f"CREATE INDEX IF NOT EXISTS idx_tech_ticker_ts ON {TECHNICAL_INDICATORS_TABLE}(Ticker, Snapshot_TS DESC)",
        f"CREATE INDEX IF NOT EXISTS idx_tech_computed_ts ON {TECHNICAL_INDICATORS_TABLE}(Computed_TS)",
    ]:
        try:
            con.execute(stmt)
        except Exception:
            pass  # Index may already exist or not be supported in this DuckDB version


def insert_technical_indicators(df_indicators: pd.DataFrame, con: Optional[duckdb.DuckDBPyConnection] = None):
    """
    Inserts a DataFrame of technical indicators into the DuckDB repository.
    Uses column-name-based insertion for schema safety.
    Signal_Version and Computed_TS are auto-populated by DB defaults.
    """
    if df_indicators.empty:
        return

    _con = con if con is not None else get_domain_write_connection(DbDomain.CHART)
    try:
        # Get table columns to match against DataFrame
        table_info = _con.execute(
            f"PRAGMA table_info('{TECHNICAL_INDICATORS_TABLE}')"
        ).fetchall()
        table_cols = {row[1] for row in table_info}

        # Only insert columns that exist in both DataFrame and table
        insert_cols = [c for c in df_indicators.columns if c in table_cols]
        if not insert_cols:
            logger.warning("⚠️ No matching columns between DataFrame and table schema.")
            return

        df_to_insert = df_indicators[insert_cols]
        col_list = ', '.join(insert_cols)
        _con.execute(
            f"INSERT OR REPLACE INTO {TECHNICAL_INDICATORS_TABLE} ({col_list}) "
            f"SELECT {col_list} FROM df_to_insert"
        )
        logger.debug(f"✅ Inserted/Updated {len(df_to_insert)} rows ({len(insert_cols)} cols) into {TECHNICAL_INDICATORS_TABLE}.")
    except Exception as e:
        logger.error(f"❌ Failed to insert technical indicators into DuckDB: {e}")
    finally:
        if con is None:
            _con.close()


def get_latest_technical_indicators(
    tickers: List[str],
    db_path: Optional[str] = None,
    con: Optional[duckdb.DuckDBPyConnection] = None,
    max_age_hours: Optional[int] = None,
    min_signal_version: Optional[int] = None,
) -> Optional[pd.DataFrame]:
    """
    Retrieves the latest technical indicators for a list of tickers.

    Args:
        tickers: List of ticker symbols.
        max_age_hours: If set, only return signals computed within this many hours.
        min_signal_version: If set, only return signals with Signal_Version >= this.
    """
    _con = con if con is not None else get_domain_connection(DbDomain.CHART, read_only=True)
    try:
        if not _table_exists(_con, TECHNICAL_INDICATORS_TABLE):
            logger.warning(f"Table {TECHNICAL_INDICATORS_TABLE} not found in DuckDB.")
            return None

        placeholders = ', '.join(['?' for _ in tickers])

        # Build optional WHERE filters
        extra_filters = ""
        if max_age_hours is not None:
            extra_filters += f" AND Computed_TS >= CURRENT_TIMESTAMP - INTERVAL '{int(max_age_hours)}' HOUR"
        if min_signal_version is not None:
            extra_filters += f" AND Signal_Version >= {int(min_signal_version)}"

        query = f"""
            WITH ranked_indicators AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (PARTITION BY Ticker ORDER BY Snapshot_TS DESC) as rn
                FROM {TECHNICAL_INDICATORS_TABLE}
                WHERE Ticker IN ({placeholders})
                {extra_filters}
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
        if con is None:
            _con.close()


def get_historical_technical_indicators(
    tickers: List[str],
    lookback_days: int = 5,
    db_path: Optional[str] = None,
    con: Optional[duckdb.DuckDBPyConnection] = None,
) -> Optional[pd.DataFrame]:
    """
    Retrieve historical technical indicators (multiple rows per ticker)
    for the last ``lookback_days`` calendar days.

    Unlike ``get_latest_technical_indicators`` which returns only the most
    recent row per ticker, this returns ALL rows within the lookback window,
    ordered by ``Snapshot_TS ASC``, to support signal trajectory analysis.

    Returns None on failure; empty DataFrame when no data found.
    """
    _con = con if con is not None else get_domain_connection(DbDomain.CHART, read_only=True)
    try:
        if not _table_exists(_con, TECHNICAL_INDICATORS_TABLE):
            logger.warning(f"Table {TECHNICAL_INDICATORS_TABLE} not found in DuckDB.")
            return None

        placeholders = ', '.join(['?' for _ in tickers])
        query = f"""
            SELECT *
            FROM {TECHNICAL_INDICATORS_TABLE}
            WHERE Ticker IN ({placeholders})
              AND Snapshot_TS >= CURRENT_TIMESTAMP - INTERVAL '{int(lookback_days)}' DAY
            ORDER BY Ticker, Snapshot_TS ASC
        """
        return _con.execute(query, tickers).df()
    except Exception as e:
        logger.error(f"Error fetching historical technical indicators: {e}")
        return None
    finally:
        if con is None:
            _con.close()


def prune_stale_signals(max_age_days: int = 365, con: Optional[duckdb.DuckDBPyConnection] = None):
    """
    Remove signals older than max_age_days to prevent unbounded table growth.
    Called once at pipeline startup, not after every insert.

    With 571 tickers × 6 scans/day × 365 days ≈ 1.25M rows — well within DuckDB capacity.
    Retention set to 365d to support YTD behavioral memory lookback.
    """
    _con = con if con is not None else get_domain_write_connection(DbDomain.CHART)
    try:
        result = _con.execute(f"""
            DELETE FROM {TECHNICAL_INDICATORS_TABLE}
            WHERE Snapshot_TS < CURRENT_TIMESTAMP - INTERVAL '{int(max_age_days)}' DAY
        """)
        deleted = result.fetchone()[0] if result else 0
        if deleted and deleted > 0:
            logger.info(f"🧹 Pruned {deleted} stale signal rows (>{max_age_days}d old).")
    except Exception as e:
        logger.warning(f"⚠️ Signal pruning failed (non-fatal): {e}")
    finally:
        if con is None:
            _con.close()
