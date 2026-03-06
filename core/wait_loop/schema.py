"""
Wait Loop Schema: DuckDB Table Definitions & Data Contracts

Defines the persistence schema for stateful trade tracking across scans.
"""

import duckdb
from dataclasses import dataclass
from typing import List, Dict, Optional, Any
from datetime import datetime
from enum import Enum


class TradeStatus(Enum):
    """Trade lifecycle states"""
    ACTIVE = "ACTIVE"                   # Currently in wait loop
    PROMOTED = "PROMOTED"               # Promoted to READY_NOW
    EXPIRED = "EXPIRED"                 # TTL exceeded
    INVALIDATED = "INVALIDATED"         # Setup broken (price, regime)
    REJECTED = "REJECTED"               # Failed re-validation


class ConditionType(Enum):
    """Types of testable confirmation conditions"""
    PRICE_LEVEL = "price_level"
    CANDLE_PATTERN = "candle_pattern"
    LIQUIDITY = "liquidity"
    TIME_DELAY = "time_delay"
    VOLATILITY = "volatility"
    TECHNICAL = "technical"   # RSI, price vs SMA, momentum recovery (Extension_Monitor)


@dataclass
class ConfirmationCondition:
    """
    A single testable confirmation condition.

    All conditions must be binary (pass/fail) with no discretion.
    """
    condition_id: str
    condition_type: ConditionType
    description: str
    config: Dict[str, Any]              # Type-specific configuration
    is_met: bool = False

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON storage"""
        return {
            "condition_id": self.condition_id,
            "type": self.condition_type.value,
            "description": self.description,
            "config": self.config,
            "is_met": self.is_met
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'ConfirmationCondition':
        """Reconstruct from dictionary"""
        return cls(
            condition_id=data["condition_id"],
            condition_type=ConditionType(data["type"]),
            description=data["description"],
            config=data["config"],
            is_met=data.get("is_met", False)
        )


@dataclass
class WaitListEntry:
    """
    A trade idea in the AWAIT_CONFIRMATION state.

    Persisted across scans until promotion, expiry, or invalidation.
    """
    # Identity
    wait_id: str
    ticker: str
    strategy_name: str
    strategy_type: str

    # Contract Details (proposed, not finalized)
    proposed_strike: Optional[float]
    proposed_expiration: Optional[str]
    contract_symbol: Optional[str]

    # Wait Metadata
    wait_started_at: datetime
    wait_expires_at: datetime
    last_evaluated_at: datetime
    evaluation_count: int

    # Confirmation Conditions
    wait_conditions: List[ConfirmationCondition]
    conditions_met: List[str]           # IDs of satisfied conditions
    wait_progress: float                # 0.0 to 1.0

    # Snapshot State (frozen at entry)
    entry_price: float
    entry_iv_30d: Optional[float]
    entry_hv_30: Optional[float]
    entry_chart_signal: Optional[str]
    entry_pcs_score: Optional[float]

    # Current State (updated each evaluation)
    current_price: Optional[float]
    current_iv_30d: Optional[float]
    current_chart_signal: Optional[str]
    price_change_pct: Optional[float]

    # Exit Conditions
    invalidation_price: Optional[float]
    max_sessions_wait: int
    max_days_wait: int

    # Status
    status: TradeStatus
    rejection_reason: Optional[str]

    def to_dict(self) -> dict:
        """Convert to dictionary for persistence"""
        return {
            "wait_id": self.wait_id,
            "ticker": self.ticker,
            "strategy_name": self.strategy_name,
            "strategy_type": self.strategy_type,
            "proposed_strike": self.proposed_strike,
            "proposed_expiration": self.proposed_expiration,
            "contract_symbol": self.contract_symbol,
            "wait_started_at": self.wait_started_at.isoformat(),
            "wait_expires_at": self.wait_expires_at.isoformat(),
            "last_evaluated_at": self.last_evaluated_at.isoformat(),
            "evaluation_count": self.evaluation_count,
            "wait_conditions": [c.to_dict() for c in self.wait_conditions],
            "conditions_met": self.conditions_met,
            "wait_progress": self.wait_progress,
            "entry_price": self.entry_price,
            "entry_iv_30d": self.entry_iv_30d,
            "entry_hv_30": self.entry_hv_30,
            "entry_chart_signal": self.entry_chart_signal,
            "entry_pcs_score": self.entry_pcs_score,
            "current_price": self.current_price,
            "current_iv_30d": self.current_iv_30d,
            "current_chart_signal": self.current_chart_signal,
            "price_change_pct": self.price_change_pct,
            "invalidation_price": self.invalidation_price,
            "max_sessions_wait": self.max_sessions_wait,
            "max_days_wait": self.max_days_wait,
            "status": self.status.value,
            "rejection_reason": self.rejection_reason
        }


@dataclass
class PromotionResult:
    """Result of attempting to promote a WAIT entry to READY_NOW"""
    wait_id: str
    outcome: str                        # PROMOTED, STILL_WAITING, REJECTED
    reason: Optional[str] = None
    contract_symbol: Optional[str] = None
    confidence_score: Optional[float] = None


def initialize_wait_list_schema(con: duckdb.DuckDBPyConnection):
    """
    Create wait_list table and indexes in DuckDB.

    Args:
        con: DuckDB connection

    RAG Source: docs/SMART_WAIT_DESIGN.md
    """
    # Create wait_list table
    con.execute("""
        CREATE TABLE IF NOT EXISTS wait_list (
            -- Identity
            wait_id VARCHAR PRIMARY KEY,
            ticker VARCHAR NOT NULL,
            strategy_name VARCHAR NOT NULL,
            strategy_type VARCHAR NOT NULL,

            -- Contract Details (proposed)
            proposed_strike DOUBLE,
            proposed_expiration DATE,
            contract_symbol VARCHAR,

            -- Wait Metadata
            wait_started_at TIMESTAMP NOT NULL,
            wait_expires_at TIMESTAMP NOT NULL,
            last_evaluated_at TIMESTAMP NOT NULL,
            evaluation_count INTEGER DEFAULT 1,

            -- Confirmation Conditions (JSON array)
            wait_conditions JSON NOT NULL,
            conditions_met JSON DEFAULT '[]',
            wait_progress DOUBLE DEFAULT 0.0,

            -- Snapshot State (frozen at wait entry)
            entry_price DOUBLE NOT NULL,
            entry_iv_30d DOUBLE,
            entry_hv_30 DOUBLE,
            entry_chart_signal VARCHAR,
            entry_pcs_score DOUBLE,

            -- Current State (updated each evaluation)
            current_price DOUBLE,
            current_iv_30d DOUBLE,
            current_chart_signal VARCHAR,
            price_change_pct DOUBLE,

            -- Exit Conditions
            invalidation_price DOUBLE,
            max_sessions_wait INTEGER DEFAULT 3,
            max_days_wait INTEGER DEFAULT 5,

            -- Status
            status VARCHAR DEFAULT 'ACTIVE',
            rejection_reason VARCHAR,

            -- Audit
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create indexes for efficient queries
    con.execute("CREATE INDEX IF NOT EXISTS idx_wait_list_status ON wait_list(status)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_wait_list_ticker ON wait_list(ticker)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_wait_list_expires ON wait_list(wait_expires_at)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_wait_list_updated ON wait_list(updated_at)")

    # Create wait_list_history for audit trail
    con.execute("""
        CREATE TABLE IF NOT EXISTS wait_list_history (
            history_id VARCHAR PRIMARY KEY,
            wait_id VARCHAR NOT NULL,
            event_type VARCHAR NOT NULL,        -- CREATED, EVALUATED, PROMOTED, EXPIRED, INVALIDATED
            event_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            conditions_met JSON,
            wait_progress DOUBLE,
            status VARCHAR,
            notes VARCHAR
        )
    """)

    con.execute("CREATE INDEX IF NOT EXISTS idx_wait_history_wait_id ON wait_list_history(wait_id)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_wait_history_event ON wait_list_history(event_type)")

    con.commit()


def get_wait_list_summary(con: duckdb.DuckDBPyConnection) -> dict:
    """
    Get summary statistics of wait list.

    Returns:
        Dict with counts by status, avg progress, avg time waiting, etc.
    """
    query = """
        SELECT
            status,
            COUNT(*) as count,
            AVG(wait_progress) as avg_progress,
            AVG(evaluation_count) as avg_evaluations,
            AVG(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - wait_started_at)) / 3600) as avg_hours_waiting
        FROM wait_list
        WHERE status = 'ACTIVE'
        GROUP BY status
        UNION ALL
        SELECT
            'TOTAL' as status,
            COUNT(*) as count,
            AVG(wait_progress) as avg_progress,
            AVG(evaluation_count) as avg_evaluations,
            AVG(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - wait_started_at)) / 3600) as avg_hours_waiting
        FROM wait_list
        WHERE status = 'ACTIVE'
    """

    result = con.execute(query).fetchall()
    return {
        "summary": result,
        "total_active": sum(r[1] for r in result if r[0] == 'ACTIVE'),
        "avg_progress": result[-1][2] if result else 0.0,
        "avg_evaluations": result[-1][3] if result else 0,
        "avg_hours_waiting": result[-1][4] if result else 0.0
    }
