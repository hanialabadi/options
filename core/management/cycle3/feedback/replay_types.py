"""
Trade Replay Types — Pure data containers for the replay engine.

No DB dependencies. Used by replay_engine.py, cli/replay_trades.py, replay_view.py.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional


@dataclass
class DecisionPoint:
    """A single daily management decision with hindsight annotations."""
    run_date: date
    action: str               # EXIT / HOLD / ROLL / REVIEW / TRIM
    urgency: str              # LOW / MEDIUM / HIGH / CRITICAL
    ul_price: float
    strike: Optional[float]
    dte: Optional[float]
    pnl_at_signal: Optional[float]   # Total_GL_Decimal at this snapshot
    doctrine_source: str
    rationale_digest: str            # First 200 chars

    # Hindsight annotations (filled by replay engine)
    price_5d_after: Optional[float] = None
    move_5d_pct: Optional[float] = None
    signal_correct: Optional[bool] = None
    hindsight_note: str = ""


@dataclass
class TradeReplay:
    """Full replay of a single trade's lifecycle."""
    trade_id: str
    ticker: str
    strategy: str
    is_closed: bool

    # Entry state
    entry_date: Optional[date] = None
    entry_ul_price: Optional[float] = None
    entry_strike: Optional[float] = None
    entry_premium: Optional[float] = None
    entry_iv: Optional[float] = None

    # Exit state
    exit_date: Optional[date] = None
    exit_ul_price: Optional[float] = None
    exit_pnl_pct: Optional[float] = None
    exit_pnl_dollar: Optional[float] = None
    outcome_type: Optional[str] = None

    # Decision timeline with hindsight
    decisions: List[DecisionPoint] = field(default_factory=list)

    # Price series over hold period: [{date, close, high, low}]
    price_series: List[Dict] = field(default_factory=list)

    # Per-trade signal accuracy
    exit_signals_total: int = 0
    exit_signals_correct: int = 0
    hold_signals_total: int = 0
    hold_signals_correct: int = 0

    # Counterfactuals
    first_exit_date: Optional[date] = None
    first_exit_pnl: Optional[float] = None
    delay_cost: Optional[float] = None       # $ lost by not following first EXIT
    best_exit_date: Optional[date] = None
    best_exit_pnl: Optional[float] = None
    worst_point_date: Optional[date] = None
    worst_point_pnl: Optional[float] = None


@dataclass
class SignalAccuracyMetrics:
    """Aggregate signal accuracy across all replayed trades."""
    # EXIT accuracy
    exit_total: int = 0
    exit_correct: int = 0
    exit_accuracy_pct: float = 0.0

    # HOLD accuracy
    hold_total: int = 0
    hold_correct: int = 0
    hold_accuracy_pct: float = 0.0

    # Urgency calibration: {urgency: {count, avg_abs_move_5d}}
    urgency_buckets: Dict[str, Dict] = field(default_factory=dict)

    # Strategy-specific: {strategy: {exit_acc, hold_acc, avg_delay, n}}
    strategy_buckets: Dict[str, Dict] = field(default_factory=dict)

    # Aggregate financials
    avg_delay_cost_dollars: float = 0.0
    total_delay_cost_dollars: float = 0.0
    trades_with_exit_signal: int = 0
    trades_exit_followed: int = 0

    # Overall trust (0-100)
    signal_trust_score: float = 0.0
