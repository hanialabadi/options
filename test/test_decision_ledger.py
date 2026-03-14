"""
Tests for the Trade Decision Ledger.

Uses in-memory DuckDB to avoid filesystem dependencies.
All tests are self-contained — each creates its own tables and data.
"""

import duckdb
import pandas as pd
import pytest
from datetime import date, datetime, timedelta

from core.shared.data_layer.decision_ledger import (
    ensure_decision_ledger_view,
    ensure_executed_actions_table,
    get_trade_timeline,
    get_ticker_timeline,
    detect_action_flips,
    count_action_changes,
    get_roll_chain_summary,
    mark_action_executed,
    get_recent_executions,
    auto_detect_executions,
)


def _seed_management_recommendations(con, rows: list[dict]):
    """Create management_recommendations table and insert rows."""
    df = pd.DataFrame(rows)
    # Ensure required columns exist
    for col in ["TradeID", "Underlying_Ticker", "Strategy", "Action", "Urgency",
                 "Strike", "UL Last", "DTE", "Rationale", "Doctrine_Source", "Snapshot_TS"]:
        if col not in df.columns:
            df[col] = None
    con.execute("CREATE TABLE IF NOT EXISTS management_recommendations AS SELECT * FROM df WHERE 1=0")
    con.execute("INSERT INTO management_recommendations SELECT * FROM df")


def _seed_premium_ledger(con, rows: list[dict]):
    """Create premium_ledger table and insert rows."""
    df = pd.DataFrame(rows)
    con.execute("""
        CREATE TABLE IF NOT EXISTS premium_ledger (
            trade_id VARCHAR, leg_id VARCHAR PRIMARY KEY,
            cycle_number INTEGER, credit_received DOUBLE,
            close_cost DOUBLE DEFAULT 0.0, contracts INTEGER,
            strike DOUBLE, expiry VARCHAR,
            opened_at TIMESTAMP, closed_at TIMESTAMP,
            status VARCHAR DEFAULT 'OPEN', notes VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    for _, r in df.iterrows():
        con.execute(
            "INSERT INTO premium_ledger (trade_id, leg_id, cycle_number, credit_received, close_cost, contracts, strike, expiry, opened_at, status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [r.get("trade_id"), r.get("leg_id"), r.get("cycle_number", 1),
             r.get("credit_received", 0), r.get("close_cost", 0),
             r.get("contracts", 1), r.get("strike"),
             r.get("expiry"), r.get("opened_at"), r.get("status", "ROLLED")],
        )


def _make_rec(trade_id, ticker, action, urgency, strike, spot, dte, ts, strategy="BUY_WRITE"):
    """Helper to build a management_recommendations row dict."""
    return {
        "TradeID": trade_id,
        "Underlying_Ticker": ticker,
        "Strategy": strategy,
        "Action": action,
        "Urgency": urgency,
        "Strike": strike,
        "UL Last": spot,
        "DTE": dte,
        "Rationale": f"{action} {urgency} on {ts}",
        "Doctrine_Source": "test",
        "Snapshot_TS": ts,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# TestDecisionTimelineView
# ═══════════════════════════════════════════════════════════════════════════════

class TestDecisionTimelineView:

    def test_timeline_returns_correct_action_transitions(self):
        """5 daily rows: HOLD, HOLD, ROLL, HOLD, EXIT → correct prev_action and action_changed."""
        con = duckdb.connect(":memory:")
        rows = [
            _make_rec("T1", "AAPL", "HOLD", "LOW", 275, 256, 42, "2026-03-01"),
            _make_rec("T1", "AAPL", "HOLD", "LOW", 275, 257, 41, "2026-03-02"),
            _make_rec("T1", "AAPL", "ROLL", "MEDIUM", 275, 255, 40, "2026-03-03"),
            _make_rec("T1", "AAPL", "HOLD", "LOW", 280, 258, 39, "2026-03-04"),
            _make_rec("T1", "AAPL", "EXIT", "HIGH", 280, 252, 38, "2026-03-05"),
        ]
        _seed_management_recommendations(con, rows)
        ensure_decision_ledger_view(con)

        tl = get_trade_timeline(con, "T1")
        assert len(tl) == 5
        # First row has no prev_action
        assert pd.isna(tl.iloc[0]["prev_action"])
        assert tl.iloc[0]["action_changed"] == False  # noqa
        # HOLD→HOLD: no change
        assert tl.iloc[1]["prev_action"] == "HOLD"
        assert tl.iloc[1]["action_changed"] == False  # noqa
        # HOLD→ROLL: changed
        assert tl.iloc[2]["prev_action"] == "HOLD"
        assert tl.iloc[2]["action_changed"] == True  # noqa
        # ROLL→HOLD: changed
        assert tl.iloc[3]["action_changed"] == True  # noqa
        # HOLD→EXIT: changed
        assert tl.iloc[4]["prev_action"] == "HOLD"
        assert tl.iloc[4]["action_changed"] == True  # noqa
        con.close()

    def test_timeline_deduplicates_intraday_runs(self):
        """3 rows on same date with different timestamps → only latest kept."""
        con = duckdb.connect(":memory:")
        rows = [
            _make_rec("T1", "AAPL", "HOLD", "LOW", 275, 256, 42, "2026-03-01 09:00:00"),
            _make_rec("T1", "AAPL", "ROLL", "MEDIUM", 275, 255, 42, "2026-03-01 12:00:00"),
            _make_rec("T1", "AAPL", "EXIT", "HIGH", 275, 254, 42, "2026-03-01 15:00:00"),
        ]
        _seed_management_recommendations(con, rows)
        ensure_decision_ledger_view(con)

        tl = get_trade_timeline(con, "T1")
        assert len(tl) == 1
        assert tl.iloc[0]["Action"] == "EXIT"  # Latest by timestamp
        con.close()

    def test_strike_event_detection(self):
        """Two days with different strikes → strike_changed flagged."""
        con = duckdb.connect(":memory:")
        rows = [
            _make_rec("T1", "AAPL", "HOLD", "LOW", 275, 256, 42, "2026-03-01"),
            _make_rec("T1", "AAPL", "HOLD", "LOW", 280, 258, 41, "2026-03-02"),
        ]
        _seed_management_recommendations(con, rows)
        ensure_decision_ledger_view(con)

        tl = get_trade_timeline(con, "T1")
        assert len(tl) == 2
        assert tl.iloc[1]["strike_changed"] == True  # noqa
        assert tl.iloc[1]["prev_strike"] == 275.0
        con.close()

    def test_ticker_timeline_spans_trade_ids(self):
        """Two TradeIDs for same ticker → get_ticker_timeline returns both."""
        con = duckdb.connect(":memory:")
        rows = [
            _make_rec("T1-OLD", "PLTR", "ROLL", "HIGH", 250, 160, 14, "2026-02-20"),
            _make_rec("T1-OLD", "PLTR", "HOLD", "LOW", 250, 162, 13, "2026-02-21"),
            _make_rec("T2-NEW", "PLTR", "HOLD", "LOW", 200, 160, 70, "2026-03-01"),
            _make_rec("T2-NEW", "PLTR", "ROLL", "HIGH", 200, 158, 69, "2026-03-02"),
        ]
        _seed_management_recommendations(con, rows)
        ensure_decision_ledger_view(con)

        tl = get_ticker_timeline(con, "PLTR")
        assert len(tl) == 4
        assert set(tl["TradeID"].unique()) == {"T1-OLD", "T2-NEW"}
        con.close()


# ═══════════════════════════════════════════════════════════════════════════════
# TestFlipDetection
# ═══════════════════════════════════════════════════════════════════════════════

class TestFlipDetection:

    def test_flip_detected_multiple_changes(self):
        """HOLD→EXIT→HOLD→EXIT→HOLD over 5 days → flip detected."""
        con = duckdb.connect(":memory:")
        today = date.today()
        rows = [
            _make_rec("T1", "AAPL", "HOLD", "LOW", 275, 256, 42,
                       str(today - timedelta(days=4))),
            _make_rec("T1", "AAPL", "EXIT", "HIGH", 275, 252, 41,
                       str(today - timedelta(days=3))),
            _make_rec("T1", "AAPL", "HOLD", "LOW", 275, 255, 40,
                       str(today - timedelta(days=2))),
            _make_rec("T1", "AAPL", "EXIT", "HIGH", 275, 251, 39,
                       str(today - timedelta(days=1))),
            _make_rec("T1", "AAPL", "HOLD", "LOW", 275, 254, 38, str(today)),
        ]
        _seed_management_recommendations(con, rows)
        ensure_decision_ledger_view(con)

        flips = detect_action_flips(con, "T1", window_days=5)
        assert len(flips) == 1
        assert flips[0]["flip_count"] >= 2
        assert len(flips[0]["action_sequence"]) == 5

        flip_count = count_action_changes(con, "T1", window_days=5)
        assert flip_count >= 2
        con.close()

    def test_no_flip_on_stable_action(self):
        """5 consecutive HOLD days → no flips."""
        con = duckdb.connect(":memory:")
        today = date.today()
        rows = [
            _make_rec("T1", "AAPL", "HOLD", "LOW", 275, 256 + i, 42 - i,
                       str(today - timedelta(days=4 - i)))
            for i in range(5)
        ]
        _seed_management_recommendations(con, rows)
        ensure_decision_ledger_view(con)

        flips = detect_action_flips(con, "T1", window_days=5)
        assert len(flips) == 0

        flip_count = count_action_changes(con, "T1", window_days=5)
        assert flip_count == 0
        con.close()


# ═══════════════════════════════════════════════════════════════════════════════
# TestExecutionTracking
# ═══════════════════════════════════════════════════════════════════════════════

class TestExecutionTracking:

    def test_mark_and_retrieve_execution(self):
        """mark_action_executed → get_recent_executions returns it."""
        con = duckdb.connect(":memory:")
        ensure_executed_actions_table(con)

        ok = mark_action_executed(con, "T1", "ROLL", notes="Rolled to $200")
        assert ok is True

        recent = get_recent_executions(con, ["T1"], within_days=3)
        assert len(recent) == 1
        assert recent.iloc[0]["trade_id"] == "T1"
        assert recent.iloc[0]["action"] == "ROLL"
        con.close()

    def test_auto_detect_roll_execution(self):
        """Prior ROLL at $150, current data shows $175 → auto-detected."""
        con = duckdb.connect(":memory:")
        ensure_executed_actions_table(con)

        # Seed prior recommendation: ROLL at strike 150 for PLTR
        prior_df = pd.DataFrame([{
            "TradeID": "T1", "Underlying_Ticker": "PLTR", "Strategy": "BUY_WRITE",
            "Action": "ROLL", "Urgency": "HIGH", "Strike": 150.0,
            "UL Last": 160.0, "DTE": 14, "Rationale": "Roll", "Doctrine_Source": "test",
            "Snapshot_TS": "2026-03-05", "run_id": "r1",
        }])
        con.execute("CREATE TABLE management_recommendations AS SELECT * FROM prior_df")
        con.execute("""
            CREATE OR REPLACE VIEW v_latest_recommendations AS
            SELECT * FROM management_recommendations
            QUALIFY ROW_NUMBER() OVER (PARTITION BY TradeID ORDER BY Snapshot_TS DESC) = 1
        """)

        # Current data: same ticker, different strike
        df_current = pd.DataFrame([{
            "TradeID": "T2-NEW", "Underlying_Ticker": "PLTR",
            "Strike": 175.0, "Action": "HOLD",
        }])

        count = auto_detect_executions(con, df_current)
        assert count == 1

        # Verify it was recorded
        recent = get_recent_executions(con, ["T1"], within_days=5)
        assert len(recent) == 1
        assert recent.iloc[0]["confirmed_by"] == "auto_detected"
        assert recent.iloc[0]["strike_old"] == 150.0
        assert recent.iloc[0]["strike_new"] == 175.0
        con.close()

    def test_no_double_detection(self):
        """Auto-detection should not re-mark an already-marked execution."""
        con = duckdb.connect(":memory:")
        ensure_executed_actions_table(con)

        prior_df = pd.DataFrame([{
            "TradeID": "T1", "Underlying_Ticker": "PLTR", "Strategy": "BUY_WRITE",
            "Action": "ROLL", "Urgency": "HIGH", "Strike": 150.0,
            "UL Last": 160.0, "DTE": 14, "Rationale": "Roll", "Doctrine_Source": "test",
            "Snapshot_TS": "2026-03-05", "run_id": "r1",
        }])
        con.execute("CREATE TABLE management_recommendations AS SELECT * FROM prior_df")
        con.execute("""
            CREATE OR REPLACE VIEW v_latest_recommendations AS
            SELECT * FROM management_recommendations
            QUALIFY ROW_NUMBER() OVER (PARTITION BY TradeID ORDER BY Snapshot_TS DESC) = 1
        """)

        df_current = pd.DataFrame([{
            "TradeID": "T2-NEW", "Underlying_Ticker": "PLTR",
            "Strike": 175.0, "Action": "HOLD",
        }])

        # First detection
        count1 = auto_detect_executions(con, df_current)
        assert count1 == 1

        # Second detection — should find existing mark and skip
        count2 = auto_detect_executions(con, df_current)
        assert count2 == 0
        con.close()


# ═══════════════════════════════════════════════════════════════════════════════
# TestRollChainSummary
# ═══════════════════════════════════════════════════════════════════════════════

class TestRollChainSummary:

    def test_roll_chain_reconstruction(self):
        """4 cycles for PLTR → correct strike chain, credits, totals."""
        con = duckdb.connect(":memory:")
        _seed_premium_ledger(con, [
            {"trade_id": "PLTR260101_250_BW_1", "leg_id": "L1", "cycle_number": 1,
             "credit_received": 8.50, "close_cost": 0.0, "contracts": 1,
             "strike": 250.0, "expiry": "2026-02-20", "opened_at": "2026-01-10", "status": "ROLLED"},
            {"trade_id": "PLTR260220_225_BW_1", "leg_id": "L2", "cycle_number": 2,
             "credit_received": 6.20, "close_cost": 2.50, "contracts": 1,
             "strike": 225.0, "expiry": "2026-03-20", "opened_at": "2026-02-20", "status": "ROLLED"},
            {"trade_id": "PLTR260320_200_BW_1", "leg_id": "L3", "cycle_number": 3,
             "credit_received": 5.00, "close_cost": 1.80, "contracts": 1,
             "strike": 200.0, "expiry": "2026-04-17", "opened_at": "2026-03-20", "status": "ROLLED"},
            {"trade_id": "PLTR260417_150_BW_1", "leg_id": "L4", "cycle_number": 4,
             "credit_received": 3.50, "close_cost": 0.0, "contracts": 1,
             "strike": 150.0, "expiry": "2026-05-15", "opened_at": "2026-04-17", "status": "OPEN"},
        ])

        chain = get_roll_chain_summary(con, "PLTR")
        assert chain["ticker"] == "PLTR"
        assert chain["strike_chain"] == [250.0, 225.0, 200.0, 150.0]
        assert chain["cycle_count"] == 4
        assert chain["total_gross"] == 23.20  # 8.5 + 6.2 + 5.0 + 3.5
        assert chain["total_close_cost"] == 4.30  # 0 + 2.5 + 1.8 + 0
        assert chain["total_net"] == 18.90  # 23.20 - 4.30
        con.close()

    def test_roll_chain_empty_for_non_bw(self):
        """Ticker with no premium_ledger entries → empty dict."""
        con = duckdb.connect(":memory:")
        # Create empty premium_ledger table
        con.execute("""
            CREATE TABLE premium_ledger (
                trade_id VARCHAR, leg_id VARCHAR PRIMARY KEY,
                cycle_number INTEGER, credit_received DOUBLE,
                close_cost DOUBLE, contracts INTEGER,
                strike DOUBLE, expiry VARCHAR,
                opened_at TIMESTAMP, closed_at TIMESTAMP,
                status VARCHAR, notes VARCHAR,
                created_at TIMESTAMP, updated_at TIMESTAMP
            )
        """)

        chain = get_roll_chain_summary(con, "MSFT")
        assert chain == {}
        con.close()


# ═══════════════════════════════════════════════════════════════════════════════
# TestGracefulDegradation
# ═══════════════════════════════════════════════════════════════════════════════

class TestGracefulDegradation:

    def test_all_functions_return_empty_on_missing_tables(self):
        """All query functions return empty results on a fresh DB with no tables."""
        con = duckdb.connect(":memory:")

        # Timeline queries — no management_recommendations table
        tl = get_trade_timeline(con, "T1")
        assert tl.empty

        tl2 = get_ticker_timeline(con, "AAPL")
        assert tl2.empty

        flips = detect_action_flips(con, "T1")
        assert flips == []

        fc = count_action_changes(con, "T1")
        assert fc == 0

        # Roll chain — no premium_ledger table
        chain = get_roll_chain_summary(con, "PLTR")
        assert chain == {}

        # Execution — no executed_actions table
        recent = get_recent_executions(con, ["T1"])
        assert recent.empty

        ok = mark_action_executed(con, "T1", "ROLL")
        assert ok is False  # Table doesn't exist

        # Auto-detect — no v_latest_recommendations
        count = auto_detect_executions(con, pd.DataFrame([{"TradeID": "T1"}]))
        assert count == 0

        con.close()
