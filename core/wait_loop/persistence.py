"""
Wait Loop Persistence: Save/Load/Update Wait List Entries

Handles all database operations for wait_list table.
"""

import duckdb
import json
import uuid
from datetime import datetime
from typing import List, Optional, Dict, Any
from .schema import WaitListEntry, ConfirmationCondition, TradeStatus
import logging

logger = logging.getLogger(__name__)


class WaitListPersistence:
    """Persistence layer for wait list operations"""

    def __init__(self, con: duckdb.DuckDBPyConnection):
        self.con = con

    def save_wait_entry(self, entry: WaitListEntry) -> str:
        """
        Save a new wait list entry.

        Args:
            entry: WaitListEntry to persist

        Returns:
            wait_id of saved entry
        """
        query = """
            INSERT INTO wait_list (
                wait_id, ticker, strategy_name, strategy_type,
                proposed_strike, proposed_expiration, contract_symbol,
                wait_started_at, wait_expires_at, last_evaluated_at, evaluation_count,
                wait_conditions, conditions_met, wait_progress,
                entry_price, entry_iv_30d, entry_hv_30, entry_chart_signal, entry_pcs_score,
                current_price, current_iv_30d, current_chart_signal, price_change_pct,
                invalidation_price, max_sessions_wait, max_days_wait,
                status, rejection_reason
            ) VALUES (
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?
            )
        """

        params = (
            entry.wait_id, entry.ticker, entry.strategy_name, entry.strategy_type,
            entry.proposed_strike, entry.proposed_expiration, entry.contract_symbol,
            entry.wait_started_at, entry.wait_expires_at, entry.last_evaluated_at, entry.evaluation_count,
            json.dumps([c.to_dict() for c in entry.wait_conditions]),
            json.dumps(entry.conditions_met),
            entry.wait_progress,
            entry.entry_price, entry.entry_iv_30d, entry.entry_hv_30, entry.entry_chart_signal, entry.entry_pcs_score,
            entry.current_price, entry.current_iv_30d, entry.current_chart_signal, entry.price_change_pct,
            entry.invalidation_price, entry.max_sessions_wait, entry.max_days_wait,
            entry.status.value, entry.rejection_reason
        )

        # Deduplicate: remove any existing ACTIVE entry for same ticker+strategy before inserting.
        # This prevents unbounded accumulation when the pipeline runs repeatedly.
        self.con.execute(
            "DELETE FROM wait_list WHERE ticker = ? AND strategy_name = ? AND status = 'ACTIVE'",
            (entry.ticker, entry.strategy_name)
        )

        self.con.execute(query, params)
        self.con.commit()

        # Log to history
        self._log_history(entry.wait_id, "CREATED", entry.conditions_met, entry.wait_progress, entry.status.value)

        logger.info(f"[WAIT_PERSIST] Saved wait entry: {entry.wait_id} ({entry.ticker} - {entry.strategy_name})")
        return entry.wait_id

    def load_active_waits(self) -> List[Dict[str, Any]]:
        """
        Load all ACTIVE wait list entries.

        Returns:
            List of wait entries as dictionaries
        """
        query = """
            SELECT
                wait_id, ticker, strategy_name, strategy_type,
                proposed_strike, proposed_expiration, contract_symbol,
                wait_started_at, wait_expires_at, last_evaluated_at, evaluation_count,
                wait_conditions, conditions_met, wait_progress,
                entry_price, entry_iv_30d, entry_hv_30, entry_chart_signal, entry_pcs_score,
                current_price, current_iv_30d, current_chart_signal, price_change_pct,
                invalidation_price, max_sessions_wait, max_days_wait,
                status, rejection_reason
            FROM wait_list
            WHERE status = 'ACTIVE'
            ORDER BY wait_started_at ASC
        """

        result = self.con.execute(query).fetchall()
        entries = []

        for row in result:
            entry = {
                "wait_id": row[0],
                "ticker": row[1],
                "strategy_name": row[2],
                "strategy_type": row[3],
                "proposed_strike": row[4],
                "proposed_expiration": row[5],
                "contract_symbol": row[6],
                "wait_started_at": row[7],
                "wait_expires_at": row[8],
                "last_evaluated_at": row[9],
                "evaluation_count": row[10],
                "wait_conditions": json.loads(row[11]) if row[11] else [],
                "conditions_met": json.loads(row[12]) if row[12] else [],
                "wait_progress": row[13],
                "entry_price": row[14],
                "entry_iv_30d": row[15],
                "entry_hv_30": row[16],
                "entry_chart_signal": row[17],
                "entry_pcs_score": row[18],
                "current_price": row[19],
                "current_iv_30d": row[20],
                "current_chart_signal": row[21],
                "price_change_pct": row[22],
                "invalidation_price": row[23],
                "max_sessions_wait": row[24],
                "max_days_wait": row[25],
                "status": row[26],
                "rejection_reason": row[27]
            }
            entries.append(entry)

        logger.info(f"[WAIT_PERSIST] Loaded {len(entries)} active wait entries")
        return entries

    def update_wait_progress(
        self,
        wait_id: str,
        conditions_met: List[str],
        wait_progress: float,
        current_price: Optional[float] = None,
        current_iv_30d: Optional[float] = None,
        current_chart_signal: Optional[str] = None
    ):
        """
        Update wait entry progress after re-evaluation.

        Args:
            wait_id: ID of wait entry
            conditions_met: List of condition IDs that are now satisfied
            wait_progress: Updated progress (0.0 - 1.0)
            current_price: Current market price
            current_iv_30d: Current IV
            current_chart_signal: Current chart signal
        """
        query = """
            UPDATE wait_list
            SET
                conditions_met = ?,
                wait_progress = ?,
                current_price = ?,
                current_iv_30d = ?,
                current_chart_signal = ?,
                price_change_pct = (? - entry_price) / entry_price * 100,
                last_evaluated_at = CURRENT_TIMESTAMP,
                evaluation_count = evaluation_count + 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE wait_id = ?
        """

        params = (
            json.dumps(conditions_met),
            wait_progress,
            current_price,
            current_iv_30d,
            current_chart_signal,
            current_price,
            wait_id
        )

        self.con.execute(query, params)
        self.con.commit()

        # Log to history
        self._log_history(wait_id, "EVALUATED", conditions_met, wait_progress, "ACTIVE")

        logger.info(f"[WAIT_PERSIST] Updated wait progress: {wait_id} ({wait_progress:.1%})")

    def mark_promoted(self, wait_id: str, contract_symbol: str, confidence_score: float):
        """
        Mark wait entry as PROMOTED to READY_NOW.

        Args:
            wait_id: ID of wait entry
            contract_symbol: Finalized contract symbol
            confidence_score: Confidence score for execution
        """
        query = """
            UPDATE wait_list
            SET
                status = 'PROMOTED',
                contract_symbol = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE wait_id = ?
        """

        self.con.execute(query, (contract_symbol, wait_id))
        self.con.commit()

        # Log to history
        self._log_history(
            wait_id,
            "PROMOTED",
            None,
            1.0,
            "PROMOTED",
            notes=f"Promoted to READY_NOW with contract {contract_symbol} (confidence: {confidence_score:.1%})"
        )

        logger.info(f"[WAIT_PERSIST] Promoted to READY_NOW: {wait_id} → {contract_symbol}")

    def mark_rejected(self, wait_id: str, reason: str, status: TradeStatus = TradeStatus.EXPIRED):
        """
        Mark wait entry as REJECTED/EXPIRED/INVALIDATED.

        Args:
            wait_id: ID of wait entry
            reason: Rejection reason
            status: Final status (EXPIRED, INVALIDATED, REJECTED)
        """
        query = """
            UPDATE wait_list
            SET
                status = ?,
                rejection_reason = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE wait_id = ?
        """

        self.con.execute(query, (status.value, reason, wait_id))
        self.con.commit()

        # Log to history
        self._log_history(wait_id, status.value, None, None, status.value, notes=reason)

        logger.info(f"[WAIT_PERSIST] Rejected wait entry: {wait_id} ({status.value}: {reason})")

    def get_wait_entry(self, wait_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a single wait entry by ID.

        Args:
            wait_id: ID of wait entry

        Returns:
            Wait entry as dictionary or None if not found
        """
        query = """
            SELECT
                wait_id, ticker, strategy_name, strategy_type,
                proposed_strike, proposed_expiration, contract_symbol,
                wait_started_at, wait_expires_at, last_evaluated_at, evaluation_count,
                wait_conditions, conditions_met, wait_progress,
                entry_price, entry_iv_30d, entry_hv_30, entry_chart_signal, entry_pcs_score,
                current_price, current_iv_30d, current_chart_signal, price_change_pct,
                invalidation_price, max_sessions_wait, max_days_wait,
                status, rejection_reason
            FROM wait_list
            WHERE wait_id = ?
        """

        result = self.con.execute(query, (wait_id,)).fetchone()
        if not result:
            return None

        return {
            "wait_id": result[0],
            "ticker": result[1],
            "strategy_name": result[2],
            "strategy_type": result[3],
            "proposed_strike": result[4],
            "proposed_expiration": result[5],
            "contract_symbol": result[6],
            "wait_started_at": result[7],
            "wait_expires_at": result[8],
            "last_evaluated_at": result[9],
            "evaluation_count": result[10],
            "wait_conditions": json.loads(result[11]) if result[11] else [],
            "conditions_met": json.loads(result[12]) if result[12] else [],
            "wait_progress": result[13],
            "entry_price": result[14],
            "entry_iv_30d": result[15],
            "entry_hv_30": result[16],
            "entry_chart_signal": result[17],
            "entry_pcs_score": result[18],
            "current_price": result[19],
            "current_iv_30d": result[20],
            "current_chart_signal": result[21],
            "price_change_pct": result[22],
            "invalidation_price": result[23],
            "max_sessions_wait": result[24],
            "max_days_wait": result[25],
            "status": result[26],
            "rejection_reason": result[27]
        }

    def _log_history(
        self,
        wait_id: str,
        event_type: str,
        conditions_met: Optional[List[str]],
        wait_progress: Optional[float],
        status: str,
        notes: Optional[str] = None
    ):
        """
        Log event to wait_list_history for audit trail.

        Args:
            wait_id: ID of wait entry
            event_type: Event type (CREATED, EVALUATED, PROMOTED, etc.)
            conditions_met: List of satisfied condition IDs
            wait_progress: Progress at time of event
            status: Status at time of event
            notes: Additional notes
        """
        history_id = str(uuid.uuid4())
        query = """
            INSERT INTO wait_list_history (
                history_id, wait_id, event_type,
                conditions_met, wait_progress, status, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """

        params = (
            history_id,
            wait_id,
            event_type,
            json.dumps(conditions_met) if conditions_met else None,
            wait_progress,
            status,
            notes
        )

        self.con.execute(query, params)
        self.con.commit()


# Convenience functions for direct use
def save_wait_entry(con: duckdb.DuckDBPyConnection, entry: WaitListEntry) -> str:
    """Save wait entry (convenience function)"""
    persistence = WaitListPersistence(con)
    return persistence.save_wait_entry(entry)


def load_active_waits(con: duckdb.DuckDBPyConnection) -> List[Dict[str, Any]]:
    """Load active waits (convenience function)"""
    persistence = WaitListPersistence(con)
    return persistence.load_active_waits()


def update_wait_progress(
    con: duckdb.DuckDBPyConnection,
    wait_id: str,
    conditions_met: List[str],
    wait_progress: float,
    current_price: Optional[float] = None,
    current_iv_30d: Optional[float] = None,
    current_chart_signal: Optional[str] = None
):
    """Update wait progress (convenience function)"""
    persistence = WaitListPersistence(con)
    persistence.update_wait_progress(
        wait_id, conditions_met, wait_progress,
        current_price, current_iv_30d, current_chart_signal
    )


def mark_promoted(con: duckdb.DuckDBPyConnection, wait_id: str, contract_symbol: str, confidence_score: float):
    """Mark as promoted (convenience function)"""
    persistence = WaitListPersistence(con)
    persistence.mark_promoted(wait_id, contract_symbol, confidence_score)


def mark_rejected(
    con: duckdb.DuckDBPyConnection,
    wait_id: str,
    reason: str,
    status: TradeStatus = TradeStatus.EXPIRED
):
    """Mark as rejected (convenience function)"""
    persistence = WaitListPersistence(con)
    persistence.mark_rejected(wait_id, reason, status)
