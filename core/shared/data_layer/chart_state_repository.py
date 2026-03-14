"""
Chart State History Repository — Persistent DuckDB Bank.

Persists chart state classifications from every management run to enable:
  - State transition tracking (STRONG_TREND → TREND_EXHAUSTED)
  - Trajectory analysis (how long in COMPRESSED before RELEASING?)
  - Pattern detection (recurring DEAD_CAT_BOUNCE → structural weakness)

Architecture: management engine writes after compute_chart_state();
             doctrine reads via get_chart_state_history() for richer decisions.
"""

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

CHART_STATE_TABLE = "chart_state_history"

# 12 classified state columns (one per chart state extractor)
STATE_COLUMNS = [
    "PriceStructure",
    "TrendIntegrity",
    "VolatilityState",
    "CompressionMaturity",
    "MomentumVelocity",
    "DirectionalBalance",
    "RangeEfficiency",
    "TimeframeAgreement",
    "GreekDominance",
    "AssignmentRisk",
    "RegimeStability",
    "RecoveryQuality",
]

# Key numeric primitives persisted alongside states for trajectory analysis
NUMERIC_COLUMNS = [
    ("adx_14", "DOUBLE"),
    ("rsi_14", "DOUBLE"),
    ("roc_5", "DOUBLE"),
    ("bb_width_pct", "DOUBLE"),
    ("hv_20d_percentile", "DOUBLE"),
]


def initialize_chart_state_table(con: Optional[duckdb.DuckDBPyConnection] = None):
    """
    Creates the chart_state_history table in DuckDB.
    Idempotent — safe to call on every startup.
    """
    _con = con if con is not None else get_domain_write_connection(DbDomain.CHART)
    try:
        state_cols = "\n".join(f"                {col} VARCHAR," for col in STATE_COLUMNS)
        numeric_cols = "\n".join(f"                {name} {dtype}," for name, dtype in NUMERIC_COLUMNS)

        _con.execute(f"""
            CREATE TABLE IF NOT EXISTS {CHART_STATE_TABLE} (
                Ticker VARCHAR,
                Snapshot_TS TIMESTAMP,
{state_cols}
{numeric_cols}
                Computed_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (Ticker, Snapshot_TS)
            )
        """)

        # Index for efficient per-ticker history lookups
        try:
            _con.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_csh_ticker_ts
                ON {CHART_STATE_TABLE}(Ticker, Snapshot_TS DESC)
            """)
        except Exception:
            pass  # Index may already exist

        logger.info(f"✅ {CHART_STATE_TABLE} table initialized.")
    except Exception as e:
        logger.error(f"❌ Failed to initialize {CHART_STATE_TABLE}: {e}")
    finally:
        if con is None:
            _con.close()


def persist_chart_states(df: pd.DataFrame, con: Optional[duckdb.DuckDBPyConnection] = None):
    """
    Persist chart state classifications from the current management run.

    Reads state columns from df (output of compute_chart_state()) and inserts
    one row per unique ticker. Handles missing columns gracefully.

    Args:
        df: DataFrame with chart state columns (e.g., PriceStructure_State, etc.)
    """
    if df.empty:
        return

    _con = con if con is not None else get_domain_write_connection(DbDomain.CHART)
    try:
        if not _table_exists(_con, CHART_STATE_TABLE):
            initialize_chart_state_table(_con)

        ticker_col = "Underlying_Ticker" if "Underlying_Ticker" in df.columns else "Ticker"
        if ticker_col not in df.columns:
            return

        now = datetime.utcnow()

        rows = []
        for ticker in df[ticker_col].dropna().unique():
            ticker_str = str(ticker)
            if not ticker_str or ticker_str in ("nan", "None"):
                continue

            ticker_rows = df[df[ticker_col] == ticker_str]
            rep = ticker_rows.iloc[0]

            row = {
                "Ticker": ticker_str,
                "Snapshot_TS": now,
            }

            # Extract classified states
            for col in STATE_COLUMNS:
                src_col = f"{col}_State"
                val = rep.get(src_col)
                row[col] = str(val) if pd.notna(val) else None

            # Extract numeric primitives
            for col_name, _ in NUMERIC_COLUMNS:
                val = rep.get(col_name)
                row[col_name] = float(val) if pd.notna(val) else None

            row["Computed_TS"] = now
            rows.append(row)

        if not rows:
            return

        insert_df = pd.DataFrame(rows)

        # Column-name-based insertion for schema safety
        table_info = _con.execute(f"PRAGMA table_info('{CHART_STATE_TABLE}')").fetchall()
        table_cols = {r[1] for r in table_info}
        insert_cols = [c for c in insert_df.columns if c in table_cols]

        if not insert_cols:
            return

        df_to_insert = insert_df[insert_cols]
        col_list = ", ".join(insert_cols)
        _con.execute(
            f"INSERT OR REPLACE INTO {CHART_STATE_TABLE} ({col_list}) "
            f"SELECT {col_list} FROM df_to_insert"
        )
        logger.debug(f"✅ Persisted {len(rows)} chart state snapshots to {CHART_STATE_TABLE}.")
    except Exception as e:
        logger.error(f"❌ Failed to persist chart states: {e}")
    finally:
        if con is None:
            _con.close()


def get_chart_state_history(
    tickers: List[str],
    lookback_days: int = 14,
    db_path: Optional[str] = None,
    con: Optional[duckdb.DuckDBPyConnection] = None,
) -> Optional[pd.DataFrame]:
    """
    Retrieve historical chart states for given tickers.

    Returns all snapshots within lookback window, ordered by Snapshot_TS ASC.
    Used by doctrine for transition detection (e.g., STRONG_TREND → TREND_EXHAUSTED).

    Returns None on failure; empty DataFrame when no data found.
    """
    _con = con if con is not None else get_domain_connection(DbDomain.CHART, read_only=True)
    try:
        if not _table_exists(_con, CHART_STATE_TABLE):
            return None

        placeholders = ", ".join(["?" for _ in tickers])
        query = f"""
            SELECT *
            FROM {CHART_STATE_TABLE}
            WHERE Ticker IN ({placeholders})
              AND Snapshot_TS >= CURRENT_TIMESTAMP - INTERVAL '{int(lookback_days)}' DAY
            ORDER BY Ticker, Snapshot_TS ASC
        """
        return _con.execute(query, tickers).df()
    except Exception as e:
        logger.error(f"Error fetching chart state history: {e}")
        return None
    finally:
        if con is None:
            _con.close()


def get_state_transitions(
    ticker: str,
    state_column: str = "TrendIntegrity",
    lookback_days: int = 14,
    con: Optional[duckdb.DuckDBPyConnection] = None,
) -> Optional[pd.DataFrame]:
    """
    Retrieve state transitions for a specific ticker and state dimension.

    Returns rows where the state changed from the previous snapshot,
    with prev_state and transition_ts columns.
    """
    _con = con if con is not None else get_domain_connection(DbDomain.CHART, read_only=True)
    try:
        if not _table_exists(_con, CHART_STATE_TABLE):
            return None

        if state_column not in STATE_COLUMNS:
            logger.warning(f"Unknown state column: {state_column}")
            return None

        query = f"""
            WITH ordered AS (
                SELECT
                    Ticker, Snapshot_TS, {state_column},
                    LAG({state_column}) OVER (ORDER BY Snapshot_TS) AS prev_state
                FROM {CHART_STATE_TABLE}
                WHERE Ticker = ?
                  AND Snapshot_TS >= CURRENT_TIMESTAMP - INTERVAL '{int(lookback_days)}' DAY
                ORDER BY Snapshot_TS ASC
            )
            SELECT
                Ticker, Snapshot_TS AS transition_ts,
                prev_state, {state_column} AS new_state
            FROM ordered
            WHERE {state_column} != prev_state
              AND prev_state IS NOT NULL
            ORDER BY Snapshot_TS ASC
        """
        return _con.execute(query, [ticker]).df()
    except Exception as e:
        logger.error(f"Error fetching state transitions for {ticker}: {e}")
        return None
    finally:
        if con is None:
            _con.close()


def prune_stale_chart_states(max_age_days: int = 30, con: Optional[duckdb.DuckDBPyConnection] = None):
    """
    Remove chart state snapshots older than max_age_days.
    Called at pipeline/management startup.
    """
    _con = con if con is not None else get_domain_write_connection(DbDomain.CHART)
    try:
        result = _con.execute(f"""
            DELETE FROM {CHART_STATE_TABLE}
            WHERE Snapshot_TS < CURRENT_TIMESTAMP - INTERVAL '{int(max_age_days)}' DAY
        """)
        deleted = result.fetchone()[0] if result else 0
        if deleted and deleted > 0:
            logger.info(f"🧹 Pruned {deleted} stale chart state rows (>{max_age_days}d old).")
    except Exception as e:
        logger.warning(f"⚠️ Chart state pruning failed (non-fatal): {e}")
    finally:
        if con is None:
            _con.close()
