"""
Scale-Up Requests — Management→Scan Bridge
============================================
DuckDB CRUD for ``scale_up_requests`` table.

Management writes rows when doctrine identifies a scale-up candidate.
Scan reads pending requests and matches them to READY/CONDITIONAL
candidates, tagging matched rows with management's sizing parameters.

Priority ordering:
  1 = CRITICAL  (Urgency=HIGH or CONVICTION_BUILDING)
  2 = HIGH      (Urgency=MEDIUM)
  3 = MEDIUM    (default)

Pending requests expire after 5 trading days (TTL cleanup).
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Optional, List

import pandas as pd

logger = logging.getLogger(__name__)

_TABLE = 'scale_up_requests'


def _get_pipeline_db_path() -> str:
    return os.path.abspath(
        os.path.join(os.path.dirname(__file__), '..', '..', '..', 'data', 'pipeline.duckdb'))


def initialize_scale_up_requests_table(con) -> None:
    """Create the scale_up_requests table if it does not exist."""
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {_TABLE} (
            ticker              VARCHAR NOT NULL,
            strategy            VARCHAR NOT NULL,
            trigger_price       DOUBLE,
            add_contracts       INTEGER DEFAULT 1,
            target_dte_min      INTEGER,
            target_dte_max      INTEGER,
            target_delta_min    DOUBLE,
            target_delta_max    DOUBLE,
            priority            INTEGER DEFAULT 3,
            signal_trajectory   VARCHAR,
            trajectory_multiplier DOUBLE,
            score_acceleration  DOUBLE,
            request_ts          TIMESTAMP NOT NULL,
            status              VARCHAR DEFAULT 'PENDING',
            source_run_id       VARCHAR,
            filled_run_id       VARCHAR,
            PRIMARY KEY (ticker, strategy, request_ts)
        )
    """)


def write_scale_up_request(
    con,
    ticker: str,
    strategy: str,
    trigger_price: Optional[float] = None,
    add_contracts: int = 1,
    target_dte_min: Optional[int] = None,
    target_dte_max: Optional[int] = None,
    target_delta_min: Optional[float] = None,
    target_delta_max: Optional[float] = None,
    priority: int = 3,
    source_run_id: Optional[str] = None,
    signal_trajectory: Optional[str] = None,
    trajectory_multiplier: Optional[float] = None,
    score_acceleration: Optional[float] = None,
) -> None:
    """Insert a new PENDING scale-up request from management."""
    initialize_scale_up_requests_table(con)
    con.execute(f"""
        INSERT INTO {_TABLE}
        (ticker, strategy, trigger_price, add_contracts,
         target_dte_min, target_dte_max, target_delta_min, target_delta_max,
         priority, signal_trajectory, trajectory_multiplier, score_acceleration,
         request_ts, status, source_run_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?)
    """, [
        ticker, strategy, trigger_price, add_contracts,
        target_dte_min, target_dte_max, target_delta_min, target_delta_max,
        priority, signal_trajectory, trajectory_multiplier, score_acceleration,
        datetime.utcnow(), source_run_id,
    ])


def read_pending_scale_up_requests(
    con,
    limit: int = 5,
) -> pd.DataFrame:
    """
    Read pending scale-up requests ordered by priority then age.

    Returns at most ``limit`` rows (default 5) to prevent overwhelming
    the scan pipeline.  Rows older than 5 trading days are auto-expired
    before reading.
    """
    initialize_scale_up_requests_table(con)
    expire_stale_requests(con)
    try:
        df = con.execute(f"""
            SELECT *
            FROM {_TABLE}
            WHERE status = 'PENDING'
            ORDER BY priority ASC, request_ts ASC
            LIMIT ?
        """, [limit]).df()
        return df
    except Exception as e:
        logger.debug(f"[ScaleUp] Read failed: {e}")
        return pd.DataFrame()


def mark_request_filled(
    con,
    ticker: str,
    strategy: str,
    filled_run_id: Optional[str] = None,
) -> None:
    """Mark a pending request as FILLED by the scan engine."""
    con.execute(f"""
        UPDATE {_TABLE}
        SET status = 'FILLED', filled_run_id = ?
        WHERE ticker = ? AND strategy = ? AND status = 'PENDING'
    """, [filled_run_id, ticker, strategy])


def expire_stale_requests(con) -> int:
    """Expire pending requests older than 5 trading days (~7 calendar days).

    Returns the number of rows expired.
    """
    try:
        result = con.execute(f"""
            UPDATE {_TABLE}
            SET status = 'EXPIRED'
            WHERE status = 'PENDING'
              AND request_ts < CURRENT_TIMESTAMP - INTERVAL '7' DAY
        """)
        count = result.fetchone()
        return count[0] if count else 0
    except Exception:
        return 0
