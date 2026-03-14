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

    # Contract Quality (for refresh comparison)
    contract_quality: Optional[Dict[str, Any]] = None

    # Status
    status: TradeStatus = TradeStatus.ACTIVE
    rejection_reason: Optional[str] = None

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

            -- Contract Quality (for refresh comparison)
            contract_quality JSON,              -- {oi, spread_pct, delta, dte, mid_price, dqs, liquidity_grade}

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

    # Migration: add contract_quality to wait_list if missing
    _migrate_wait_list_contract_quality(con)

    # Create wait_list_history for audit trail
    con.execute("""
        CREATE TABLE IF NOT EXISTS wait_list_history (
            history_id VARCHAR PRIMARY KEY,
            wait_id VARCHAR NOT NULL,
            ticker VARCHAR,
            strategy_name VARCHAR,
            event_type VARCHAR NOT NULL,        -- CREATED, EVALUATED, PROMOTED, EXPIRED, INVALIDATED
            event_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            conditions_met JSON,
            wait_conditions_json JSON,          -- original blocking conditions (CREATED events)
            wait_progress DOUBLE,
            status VARCHAR,
            notes VARCHAR
        )
    """)

    con.execute("CREATE INDEX IF NOT EXISTS idx_wait_history_wait_id ON wait_list_history(wait_id)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_wait_history_event ON wait_list_history(event_type)")

    # Migration: add ticker/strategy_name/wait_conditions_json columns if table
    # already exists without them (pre-migration schema).  Must run BEFORE the
    # ticker index so the column exists when the index is created.
    _migrate_wait_list_history(con)

    con.execute("CREATE INDEX IF NOT EXISTS idx_wait_history_ticker ON wait_list_history(ticker)")

    con.commit()


def _migrate_wait_list_contract_quality(con: duckdb.DuckDBPyConnection):
    """Add contract_quality JSON column to wait_list if missing."""
    try:
        existing = {
            row[1] for row in
            con.execute("PRAGMA table_info('wait_list')").fetchall()
        }
    except Exception:
        return
    if 'contract_quality' not in existing:
        con.execute("ALTER TABLE wait_list ADD COLUMN contract_quality JSON")
        con.commit()


# ---------------------------------------------------------------------------
# Contract quality snapshot — what we store and compare
# ---------------------------------------------------------------------------

def extract_contract_quality(row) -> dict:
    """
    Extract contract-quality metrics from a Step 12 acceptance row.

    These metrics are stored on the wait entry and compared when the same
    thesis reappears with a potentially better chain candidate.
    """
    import math

    def _safe_float(val, default=None):
        if val is None:
            return default
        try:
            v = float(val)
            return default if (isinstance(v, float) and math.isnan(v)) else v
        except (TypeError, ValueError):
            return default

    return {
        'oi': _safe_float(row.get('Open_Interest')),
        'spread_pct': _safe_float(row.get('Bid_Ask_Spread_Pct')),
        'delta': _safe_float(row.get('Delta')),
        'dte': _safe_float(row.get('Actual_DTE')),
        'mid_price': _safe_float(row.get('Mid_Price')),
        'dqs': _safe_float(row.get('DQS_Score') if row.get('DQS_Score') is not None else row.get('DQS')),
        'pcs': _safe_float(row.get('PCS_Score')),
        'liquidity_grade': str(row.get('Liquidity_Grade') or ''),
    }


# Minimum improvement thresholds — need 2+ to qualify as "materially better"
_SPREAD_IMPROVEMENT = 0.20   # 20% tighter spread
_OI_IMPROVEMENT = 1.50       # 50% more OI
_PCS_IMPROVEMENT = 5.0       # 5 points higher PCS
_DQS_IMPROVEMENT = 5.0       # 5 points higher DQS
_LIQUIDITY_RANK = {
    'Excellent': 5, 'Good': 4, 'Acceptable': 3, 'Thin': 2, 'Illiquid': 1, '': 0,
}


def compare_contract_quality(
    existing_quality: dict | None,
    new_quality: dict,
) -> tuple[bool, list[str]]:
    """
    Compare existing wait entry's contract quality against a new candidate.

    Returns (is_materially_better, list_of_improvement_reasons).
    Requires 2+ improvements to qualify — prevents noise from single-metric jitter.
    """
    if not existing_quality:
        return False, []  # no baseline to compare against

    improvements = []

    # 1. Tighter spread
    old_spread = existing_quality.get('spread_pct')
    new_spread = new_quality.get('spread_pct')
    if old_spread and new_spread and old_spread > 0:
        if new_spread < old_spread * (1.0 - _SPREAD_IMPROVEMENT):
            improvements.append(
                f"spread {old_spread:.1f}%→{new_spread:.1f}% "
                f"({(1 - new_spread / old_spread) * 100:.0f}% tighter)")

    # 2. Better OI
    old_oi = existing_quality.get('oi')
    new_oi = new_quality.get('oi')
    if old_oi is not None and new_oi is not None and old_oi > 0:
        if new_oi > old_oi * _OI_IMPROVEMENT:
            improvements.append(f"OI {int(old_oi)}→{int(new_oi)}")

    # 3. Higher PCS
    old_pcs = existing_quality.get('pcs')
    new_pcs = new_quality.get('pcs')
    if old_pcs is not None and new_pcs is not None:
        if new_pcs > old_pcs + _PCS_IMPROVEMENT:
            improvements.append(f"PCS {old_pcs:.0f}→{new_pcs:.0f}")

    # 4. Higher DQS
    old_dqs = existing_quality.get('dqs')
    new_dqs = new_quality.get('dqs')
    if old_dqs is not None and new_dqs is not None:
        if new_dqs > old_dqs + _DQS_IMPROVEMENT:
            improvements.append(f"DQS {old_dqs:.0f}→{new_dqs:.0f}")

    # 5. Liquidity grade improvement
    old_liq = _LIQUIDITY_RANK.get(existing_quality.get('liquidity_grade', ''), 0)
    new_liq = _LIQUIDITY_RANK.get(new_quality.get('liquidity_grade', ''), 0)
    if new_liq > old_liq:
        improvements.append(
            f"liquidity {existing_quality.get('liquidity_grade', '?')}"
            f"→{new_quality.get('liquidity_grade', '?')}")

    # Need 2+ improvements to qualify as materially better
    return len(improvements) >= 2, improvements


def _migrate_wait_list_history(con: duckdb.DuckDBPyConnection):
    """Add denormalized columns to wait_list_history if missing (schema evolution)."""
    try:
        existing = {
            row[1] for row in
            con.execute("PRAGMA table_info('wait_list_history')").fetchall()
        }
    except Exception:
        return  # table doesn't exist yet — CREATE TABLE will handle it

    migrations = {
        'ticker': 'VARCHAR',
        'strategy_name': 'VARCHAR',
        'wait_conditions_json': 'JSON',
    }
    for col, dtype in migrations.items():
        if col not in existing:
            con.execute(f"ALTER TABLE wait_list_history ADD COLUMN {col} {dtype}")

    # Backfill ticker/strategy from wait_list for existing history rows
    try:
        con.execute("""
            UPDATE wait_list_history h
            SET ticker = w.ticker,
                strategy_name = w.strategy_name
            FROM wait_list w
            WHERE h.wait_id = w.wait_id
              AND h.ticker IS NULL
        """)
        con.commit()
    except Exception:
        pass  # best-effort backfill


def query_deferral_patterns(
    con: duckdb.DuckDBPyConnection,
    ticker: str,
    lookback_days: int = 90,
) -> dict:
    """
    Query wait list history for deferral patterns on a ticker.

    Returns dict with:
      - deferral_count: number of times deferred in lookback window
      - outcome_distribution: {PROMOTED: n, EXPIRED: n, INVALIDATED: n, REJECTED: n}
      - promotion_rate: PROMOTED / total terminal events
      - avg_wait_days: average days from CREATED to terminal state
      - strategies_deferred: {strategy: count} — which strategies keep getting deferred
      - common_conditions: list of most frequent blocking condition types
      - last_deferral_ts: timestamp of most recent deferral
    """
    result = {
        'deferral_count': 0,
        'outcome_distribution': {},
        'promotion_rate': 0.0,
        'avg_wait_days': 0.0,
        'strategies_deferred': {},
        'common_conditions': [],
        'last_deferral_ts': None,
    }

    try:
        # DuckDB doesn't support parameterized INTERVAL — safe int interpolation
        _days = int(lookback_days)
        _interval = f"INTERVAL '{_days}' DAY"

        # Count CREATED events (= number of times deferred)
        created = con.execute(f"""
            SELECT COUNT(*), MAX(event_timestamp)
            FROM wait_list_history
            WHERE ticker = ?
              AND event_type = 'CREATED'
              AND event_timestamp >= CURRENT_TIMESTAMP - {_interval}
        """, (ticker,)).fetchone()

        if not created or created[0] == 0:
            return result

        result['deferral_count'] = created[0]
        result['last_deferral_ts'] = created[1]

        # Outcome distribution (terminal events only)
        outcomes = con.execute(f"""
            SELECT event_type, COUNT(*)
            FROM wait_list_history
            WHERE ticker = ?
              AND event_type IN ('PROMOTED', 'EXPIRED', 'INVALIDATED', 'REJECTED')
              AND event_timestamp >= CURRENT_TIMESTAMP - {_interval}
            GROUP BY event_type
        """, (ticker,)).fetchall()

        total_terminal = 0
        promoted_count = 0
        for evt, cnt in outcomes:
            result['outcome_distribution'][evt] = cnt
            total_terminal += cnt
            if evt == 'PROMOTED':
                promoted_count = cnt

        if total_terminal > 0:
            result['promotion_rate'] = promoted_count / total_terminal

        # Average wait duration (CREATED → terminal event, per wait_id)
        avg_wait = con.execute(f"""
            SELECT AVG(duration_days) FROM (
                SELECT
                    c.wait_id,
                    EXTRACT(EPOCH FROM (t.event_timestamp - c.event_timestamp)) / 86400.0 AS duration_days
                FROM wait_list_history c
                JOIN wait_list_history t ON c.wait_id = t.wait_id
                WHERE c.ticker = ?
                  AND c.event_type = 'CREATED'
                  AND t.event_type IN ('PROMOTED', 'EXPIRED', 'INVALIDATED', 'REJECTED')
                  AND c.event_timestamp >= CURRENT_TIMESTAMP - {_interval}
            )
        """, (ticker,)).fetchone()
        if avg_wait and avg_wait[0] is not None:
            result['avg_wait_days'] = round(avg_wait[0], 1)

        # Strategies deferred
        strats = con.execute(f"""
            SELECT strategy_name, COUNT(*)
            FROM wait_list_history
            WHERE ticker = ?
              AND event_type = 'CREATED'
              AND strategy_name IS NOT NULL
              AND event_timestamp >= CURRENT_TIMESTAMP - {_interval}
            GROUP BY strategy_name
            ORDER BY COUNT(*) DESC
        """, (ticker,)).fetchall()
        result['strategies_deferred'] = {s: c for s, c in strats}

        # Common blocking conditions (from wait_conditions_json on CREATED events)
        conditions_rows = con.execute(f"""
            SELECT wait_conditions_json
            FROM wait_list_history
            WHERE ticker = ?
              AND event_type = 'CREATED'
              AND wait_conditions_json IS NOT NULL
              AND event_timestamp >= CURRENT_TIMESTAMP - {_interval}
        """, (ticker,)).fetchall()

        import json
        condition_type_counts: dict = {}
        for (cond_json,) in conditions_rows:
            try:
                conditions = json.loads(cond_json) if isinstance(cond_json, str) else cond_json
                if isinstance(conditions, list):
                    for c in conditions:
                        ctype = c.get('type', c.get('condition_type', 'unknown'))
                        desc = c.get('description', '')
                        # Use description prefix for more specificity
                        key = f"{ctype}:{desc[:60]}" if desc else ctype
                        condition_type_counts[key] = condition_type_counts.get(key, 0) + 1
            except (json.JSONDecodeError, TypeError):
                pass

        # Sort by frequency, return top 5
        result['common_conditions'] = sorted(
            condition_type_counts.items(), key=lambda x: -x[1]
        )[:5]

    except Exception as e:
        import logging
        logging.getLogger(__name__).debug(f"query_deferral_patterns({ticker}): {e}")

    return result


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
