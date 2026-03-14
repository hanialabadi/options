"""
Tests for the Trade Replay Engine.

Covers:
  - Direction-aware signal correctness (EXIT/HOLD for different strategy types)
  - Counterfactual computation (first EXIT, delay cost, best/worst)
  - Aggregate metrics and trust score
  - Edge cases (no EXIT signals, single decision)
"""
import sys
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.management.cycle3.feedback.replay_types import (
    DecisionPoint,
    SignalAccuracyMetrics,
    TradeReplay,
)
from core.management.cycle3.feedback.replay_engine import (
    _annotate_hindsight,
    _compute_signal_accuracy,
    _aggregate_metrics,
    _exit_was_correct,
    _hold_was_correct,
)


# ═══════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════

def _make_price_series(base_date: date, prices: list[float]) -> list[dict]:
    """Create a price_series from a list of close prices (one per trading day)."""
    series = []
    d = base_date
    for p in prices:
        series.append({"date": d, "close": p, "high": p * 1.01, "low": p * 0.99})
        # Skip weekends
        d += timedelta(days=1)
        while d.weekday() >= 5:
            d += timedelta(days=1)
    return series


def _make_decision(
    run_date: date,
    action: str = "HOLD",
    urgency: str = "LOW",
    ul_price: float = 100.0,
    pnl: float | None = None,
) -> DecisionPoint:
    return DecisionPoint(
        run_date=run_date,
        action=action,
        urgency=urgency,
        ul_price=ul_price,
        strike=100.0,
        dte=30.0,
        pnl_at_signal=pnl,
        doctrine_source="test",
        rationale_digest="test rationale",
    )


# ═══════════════════════════════════════════════════════════════════════
# Test Direction-Aware Signal Correctness
# ═══════════════════════════════════════════════════════════════════════

class TestExitWasCorrect:
    """EXIT correct when stock moves against the position."""

    def test_long_call_exit_correct_stock_dropped(self):
        """LONG_CALL: EXIT correct when stock drops >1%."""
        assert _exit_was_correct("LONG_CALL", -5.0) is True

    def test_long_call_exit_wrong_stock_rose(self):
        """LONG_CALL: EXIT wrong when stock rose."""
        assert _exit_was_correct("LONG_CALL", 3.0) is False

    def test_long_put_exit_correct_stock_rose(self):
        """LONG_PUT: EXIT correct when stock rose >1%."""
        assert _exit_was_correct("LONG_PUT", 5.0) is True

    def test_long_put_exit_wrong_stock_dropped(self):
        """LONG_PUT: EXIT wrong when stock dropped."""
        assert _exit_was_correct("LONG_PUT", -3.0) is False

    def test_csp_exit_correct_stock_dropped(self):
        """CSP: EXIT correct when stock drops >1% toward strike."""
        assert _exit_was_correct("CSP", -5.0) is True

    def test_csp_exit_wrong_stock_rose(self):
        """CSP: EXIT wrong when stock rose (position improving)."""
        assert _exit_was_correct("CSP", 3.0) is False

    def test_buy_write_exit_correct_stock_dropped(self):
        """BUY_WRITE: EXIT correct when stock drops >1%."""
        assert _exit_was_correct("BUY_WRITE", -5.0) is True

    def test_buy_write_exit_wrong_stock_rose(self):
        """BUY_WRITE: EXIT wrong when stock rose."""
        assert _exit_was_correct("BUY_WRITE", 3.0) is False


class TestHoldWasCorrect:
    """HOLD correct when stock moves favorably for the position."""

    def test_long_call_hold_correct_stock_stable(self):
        """LONG_CALL: HOLD correct when stock didn't drop."""
        assert _hold_was_correct("LONG_CALL", 2.0) is True

    def test_long_call_hold_wrong_stock_crashed(self):
        """LONG_CALL: HOLD wrong when stock dropped >1%."""
        assert _hold_was_correct("LONG_CALL", -5.0) is False

    def test_csp_hold_correct_stock_stable(self):
        """CSP: HOLD correct when stock didn't drop."""
        assert _hold_was_correct("CSP", 1.0) is True

    def test_csp_hold_wrong_stock_dropped(self):
        """CSP: HOLD wrong when stock dropped."""
        assert _hold_was_correct("CSP", -5.0) is False


# ═══════════════════════════════════════════════════════════════════════
# Test Hindsight Annotation
# ═══════════════════════════════════════════════════════════════════════

class TestAnnotateHindsight:
    """Integration: annotate_hindsight fills in 5D move and correctness."""

    def test_exit_annotated_with_5d_move(self):
        """EXIT decision gets price_5d_after and move_5d_pct filled."""
        d0 = date(2026, 2, 20)
        decisions = [_make_decision(d0, action="EXIT", ul_price=100.0)]
        # 6 prices: day0=100, then drops to 90 by day5
        prices = _make_price_series(d0, [100, 98, 96, 94, 92, 90])

        _annotate_hindsight(decisions, prices, "LONG_CALL")

        dp = decisions[0]
        assert dp.price_5d_after is not None
        assert dp.move_5d_pct is not None
        assert dp.move_5d_pct < 0  # stock dropped
        assert dp.signal_correct is True  # LONG_CALL EXIT correct when stock drops
        assert "CORRECT" in dp.hindsight_note

    def test_hold_annotated_via_pnl_improvement(self):
        """HOLD correctness uses P&L comparison when next decision available."""
        d0 = date(2026, 2, 20)
        d1 = date(2026, 2, 21)
        decisions = [
            _make_decision(d0, action="HOLD", ul_price=100.0, pnl=-50.0),
            _make_decision(d1, action="HOLD", ul_price=102.0, pnl=20.0),
        ]
        prices = _make_price_series(d0, [100, 102, 104, 106, 108, 110, 112])

        _annotate_hindsight(decisions, prices, "LONG_CALL")

        dp = decisions[0]
        assert dp.signal_correct is True  # P&L improved from -50 to +20
        assert "improved" in dp.hindsight_note

    def test_hold_wrong_when_pnl_worsened(self):
        """HOLD wrong when P&L worsened by next decision."""
        d0 = date(2026, 2, 20)
        d1 = date(2026, 2, 21)
        decisions = [
            _make_decision(d0, action="HOLD", ul_price=100.0, pnl=50.0),
            _make_decision(d1, action="EXIT", ul_price=90.0, pnl=-100.0),
        ]
        prices = _make_price_series(d0, [100, 95, 90, 85, 80, 75, 70])

        _annotate_hindsight(decisions, prices, "LONG_CALL")

        dp = decisions[0]
        assert dp.signal_correct is False  # P&L went from +50 to -100
        assert "worsened" in dp.hindsight_note

    def test_empty_price_series_no_crash(self):
        """Empty price series: no annotation, no crash."""
        d0 = date(2026, 2, 20)
        decisions = [_make_decision(d0, action="EXIT", ul_price=100.0)]
        _annotate_hindsight(decisions, [], "LONG_CALL")
        assert decisions[0].signal_correct is None

    def test_roll_correct_when_stock_stabilized(self):
        """ROLL correct when stock didn't continue against position."""
        d0 = date(2026, 2, 20)
        decisions = [_make_decision(d0, action="ROLL", ul_price=100.0)]
        # Stock stays flat
        prices = _make_price_series(d0, [100, 100.5, 101, 100, 99.5, 100.2])

        _annotate_hindsight(decisions, prices, "CSP")

        dp = decisions[0]
        assert dp.signal_correct is True  # stock didn't crash → roll was correct
        assert "stabilized" in dp.hindsight_note


# ═══════════════════════════════════════════════════════════════════════
# Test Counterfactuals
# ═══════════════════════════════════════════════════════════════════════

class TestCounterfactuals:
    """_compute_signal_accuracy correctly finds first EXIT and computes delay cost."""

    def test_first_exit_found(self):
        """First EXIT date/P&L identified from decision timeline."""
        replay = TradeReplay(
            trade_id="TEST_001", ticker="AAPL", strategy="LONG_CALL", is_closed=True,
            exit_pnl_dollar=-200.0,
        )
        replay.decisions = [
            _make_decision(date(2026, 2, 20), action="HOLD", pnl=50.0),
            _make_decision(date(2026, 2, 21), action="EXIT", pnl=-100.0),
            _make_decision(date(2026, 2, 22), action="HOLD", pnl=-50.0),
            _make_decision(date(2026, 2, 23), action="EXIT", pnl=-200.0),
        ]
        # Set signal_correct so accuracy counts work
        for d in replay.decisions:
            d.signal_correct = True

        _compute_signal_accuracy(replay)

        assert replay.first_exit_date == date(2026, 2, 21)
        assert replay.first_exit_pnl == -100.0

    def test_delay_cost_computed(self):
        """Delay cost = first_exit_pnl - actual_pnl."""
        replay = TradeReplay(
            trade_id="TEST_002", ticker="META", strategy="LONG_PUT", is_closed=True,
            exit_pnl_dollar=-500.0,
        )
        replay.decisions = [
            _make_decision(date(2026, 2, 20), action="EXIT", pnl=-100.0),
            _make_decision(date(2026, 2, 25), action="EXIT", pnl=-500.0),
        ]
        for d in replay.decisions:
            d.signal_correct = True

        _compute_signal_accuracy(replay)

        # first EXIT P&L was -100, actual was -500 → delay cost = -100 - (-500) = +400
        assert replay.delay_cost == 400.0  # saved $400 by waiting? No — lost $400 by NOT following first EXIT
        # Actually: delay_cost = first_exit_pnl - actual_pnl = -100 - (-500) = +400
        # Positive means first EXIT was better (delay hurt)

    def test_best_worst_points_identified(self):
        """Best and worst P&L points found across timeline."""
        replay = TradeReplay(
            trade_id="TEST_003", ticker="TSLA", strategy="BUY_WRITE", is_closed=False,
        )
        replay.decisions = [
            _make_decision(date(2026, 2, 20), pnl=100.0),
            _make_decision(date(2026, 2, 21), pnl=500.0),   # best
            _make_decision(date(2026, 2, 22), pnl=-300.0),   # worst
            _make_decision(date(2026, 2, 23), pnl=50.0),
        ]

        _compute_signal_accuracy(replay)

        assert replay.best_exit_pnl == 500.0
        assert replay.best_exit_date == date(2026, 2, 21)
        assert replay.worst_point_pnl == -300.0
        assert replay.worst_point_date == date(2026, 2, 22)


# ═══════════════════════════════════════════════════════════════════════
# Test Aggregate Metrics
# ═══════════════════════════════════════════════════════════════════════

class TestAggregateMetrics:
    """_aggregate_metrics computes trust score and buckets correctly."""

    def _make_replay(
        self, strategy: str, exit_total: int, exit_correct: int,
        hold_total: int, hold_correct: int, delay_cost: float | None = None,
    ) -> TradeReplay:
        r = TradeReplay(
            trade_id=f"T_{strategy}", ticker="TEST", strategy=strategy, is_closed=True,
        )
        r.exit_signals_total = exit_total
        r.exit_signals_correct = exit_correct
        r.hold_signals_total = hold_total
        r.hold_signals_correct = hold_correct
        r.delay_cost = delay_cost
        if delay_cost is not None:
            r.first_exit_date = date(2026, 2, 20)
            r.first_exit_pnl = -100.0
        # Add decisions for urgency bucket testing
        r.decisions = [
            _make_decision(date(2026, 2, 20), urgency="LOW"),
            _make_decision(date(2026, 2, 21), urgency="HIGH"),
        ]
        r.decisions[0].move_5d_pct = 2.0
        r.decisions[1].move_5d_pct = 8.0
        return r

    def test_exit_hold_accuracy_aggregated(self):
        """Aggregate exit/hold accuracy from multiple trades."""
        replays = [
            self._make_replay("LONG_CALL", 5, 4, 10, 7),
            self._make_replay("CSP", 3, 2, 8, 6),
        ]
        m = _aggregate_metrics(replays)

        assert m.exit_total == 8
        assert m.exit_correct == 6
        assert m.exit_accuracy_pct == pytest.approx(75.0)
        assert m.hold_total == 18
        assert m.hold_correct == 13
        assert m.hold_accuracy_pct == pytest.approx(13 / 18 * 100, rel=0.01)

    def test_trust_score_range(self):
        """Trust score is 0-100."""
        replays = [
            self._make_replay("LONG_CALL", 5, 4, 10, 7, delay_cost=-50.0),
        ]
        m = _aggregate_metrics(replays)
        assert 0 <= m.signal_trust_score <= 100

    def test_strategy_buckets_populated(self):
        """Strategy buckets contain per-strategy accuracy."""
        replays = [
            self._make_replay("LONG_CALL", 5, 4, 10, 7),
            self._make_replay("CSP", 3, 2, 8, 6),
        ]
        m = _aggregate_metrics(replays)

        assert "LONG_CALL" in m.strategy_buckets
        assert "CSP" in m.strategy_buckets
        assert m.strategy_buckets["LONG_CALL"]["n"] == 1
        assert m.strategy_buckets["LONG_CALL"]["exit_acc"] == pytest.approx(80.0)

    def test_urgency_buckets_populated(self):
        """Urgency buckets track avg |move| per urgency level."""
        replays = [
            self._make_replay("LONG_CALL", 5, 4, 10, 7),
        ]
        m = _aggregate_metrics(replays)

        assert "LOW" in m.urgency_buckets
        assert "HIGH" in m.urgency_buckets
        # HIGH should have bigger move than LOW
        assert m.urgency_buckets["HIGH"]["avg_abs_move_5d"] > m.urgency_buckets["LOW"]["avg_abs_move_5d"]


# ═══════════════════════════════════════════════════════════════════════
# Edge Cases
# ═══════════════════════════════════════════════════════════════════════

class TestEdgeCases:
    """Edge cases that shouldn't crash."""

    def test_no_exit_signals(self):
        """Trade with zero EXIT signals: no first_exit_date, no delay_cost."""
        replay = TradeReplay(
            trade_id="TEST_NOEX", ticker="SPY", strategy="BUY_WRITE", is_closed=False,
        )
        replay.decisions = [
            _make_decision(date(2026, 2, 20), action="HOLD", pnl=100.0),
            _make_decision(date(2026, 2, 21), action="HOLD", pnl=150.0),
        ]

        _compute_signal_accuracy(replay)

        assert replay.first_exit_date is None
        assert replay.delay_cost is None
        assert replay.exit_signals_total == 0

    def test_single_decision(self):
        """Trade with only 1 day of history."""
        replay = TradeReplay(
            trade_id="TEST_1D", ticker="NVDA", strategy="LONG_CALL", is_closed=False,
        )
        replay.decisions = [
            _make_decision(date(2026, 2, 20), action="EXIT", pnl=-50.0),
        ]
        replay.decisions[0].signal_correct = True

        _compute_signal_accuracy(replay)

        assert replay.exit_signals_total == 1
        assert replay.exit_signals_correct == 1
        assert replay.first_exit_date == date(2026, 2, 20)

    def test_empty_replays_aggregate(self):
        """Aggregate of empty list doesn't crash."""
        m = _aggregate_metrics([])
        assert m.exit_total == 0
        assert m.signal_trust_score >= 0

    def test_all_none_pnl(self):
        """Decisions with all-None P&L don't crash counterfactual."""
        replay = TradeReplay(
            trade_id="TEST_NONE", ticker="AMD", strategy="CSP", is_closed=False,
        )
        replay.decisions = [
            _make_decision(date(2026, 2, 20), action="EXIT", pnl=None),
            _make_decision(date(2026, 2, 21), action="HOLD", pnl=None),
        ]

        _compute_signal_accuracy(replay)

        assert replay.first_exit_date == date(2026, 2, 20)
        assert replay.delay_cost is None  # can't compute without P&L
        assert replay.best_exit_pnl is None
