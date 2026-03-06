"""
Smart WAIT Loop: Stateful Trade Tracking & Re-Evaluation Engine

The wait_loop module implements a closed-loop execution funnel that tracks trade
ideas from discovery through confirmation to execution or rejection.

Core Components:
- schema: DuckDB table definitions and data contracts
- persistence: Save/load/update wait list entries
- conditions: Testable confirmation condition types
- evaluator: Re-evaluation engine and state transitions
- ttl: Time-to-live and expiry logic
- output_formatter: Three-tier output formatting (READY/WAIT/REJECTED)

Design: docs/SMART_WAIT_DESIGN.md
RAG Source: docs/EXECUTION_SEMANTICS.md
"""

from .schema import (
    initialize_wait_list_schema,
    WaitListEntry,
    ConfirmationCondition,
    PromotionResult
)

from .persistence import (
    WaitListPersistence,
    save_wait_entry,
    load_active_waits,
    update_wait_progress,
    mark_promoted,
    mark_rejected
)

from .conditions import (
    ConditionFactory,
    ConditionType,
    PriceLevelCondition,
    CandlePatternCondition,
    LiquidityCondition,
    TimeDelayCondition,
    VolatilityCondition
)

from .evaluator import (
    WaitConditionEvaluator,
    WaitEvaluationResult,
    evaluate_wait_list
)

from .ttl import (
    TTL_CONFIG,
    should_expire,
    count_trading_sessions
)

from .output_formatter import (
    format_ready_now,
    format_waitlist,
    format_rejected,
    format_scan_summary
)

__all__ = [
    # Schema
    'initialize_wait_list_schema',
    'WaitListEntry',
    'ConfirmationCondition',
    'PromotionResult',

    # Persistence
    'WaitListPersistence',
    'save_wait_entry',
    'load_active_waits',
    'update_wait_progress',
    'mark_promoted',
    'mark_rejected',

    # Conditions
    'ConditionFactory',
    'ConditionType',
    'PriceLevelCondition',
    'CandlePatternCondition',
    'LiquidityCondition',
    'TimeDelayCondition',
    'VolatilityCondition',

    # Evaluator
    'WaitConditionEvaluator',
    'WaitEvaluationResult',
    'evaluate_wait_list',

    # TTL
    'TTL_CONFIG',
    'should_expire',
    'count_trading_sessions',

    # Output
    'format_ready_now',
    'format_waitlist',
    'format_rejected',
    'format_scan_summary',
]

__version__ = '1.0.0'
__author__ = 'Options Intelligence Platform'
