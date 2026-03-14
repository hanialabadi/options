"""
Tests for Historical Earnings Data + IV Crush Analytics.

Tests the DuckDB tables, data layer helpers, IV crush computation,
expected/actual move metrics, IV ramp detection, and summary stats.
"""

import math
from datetime import date, timedelta

import duckdb
import numpy as np
import pandas as pd
import pytest

from core.shared.data_layer.earnings_history import (
    initialize_tables,
    upsert_earnings_batch,
    get_earnings_history,
    get_ticker_earnings_stats,
    get_all_earnings_stats,
    classify_beat_miss,
    compute_iv_crush_for_event,
    refresh_earnings_stats,
    refresh_all_earnings_stats,
    _nearest_iv_reading,
    _nearest_price,
    _compute_iv_ramp_start,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def pipeline_con():
    """In-memory DuckDB for pipeline tables (earnings + price_history)."""
    con = duckdb.connect(":memory:")
    initialize_tables(con)
    # Create price_history table for price lookups
    con.execute("""
        CREATE TABLE IF NOT EXISTS price_history (
            ticker VARCHAR, date DATE, open_price DOUBLE,
            high_price DOUBLE, low_price DOUBLE, close_price DOUBLE,
            volume BIGINT
        )
    """)
    return con


@pytest.fixture
def iv_con():
    """In-memory DuckDB for iv_term_history."""
    con = duckdb.connect(":memory:")
    con.execute("""
        CREATE TABLE IF NOT EXISTS iv_term_history (
            ticker VARCHAR, date DATE, iv_7d DOUBLE, iv_14d DOUBLE,
            iv_21d DOUBLE, iv_30d DOUBLE, iv_60d DOUBLE, iv_90d DOUBLE,
            iv_120d DOUBLE, iv_180d DOUBLE, iv_360d DOUBLE,
            source VARCHAR, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ticker, date)
        )
    """)
    return con


def _sample_earnings_df(ticker="AAPL"):
    """4 quarters of AAPL-like earnings data."""
    return pd.DataFrame([
        {"ticker": ticker, "earnings_date": date(2025, 7, 31), "fiscal_quarter": "Q3 2025",
         "eps_estimate": 1.35, "eps_actual": 1.40, "eps_surprise_pct": 3.7,
         "beat_miss": "BEAT", "source": "yfinance"},
        {"ticker": ticker, "earnings_date": date(2025, 10, 30), "fiscal_quarter": "Q4 2025",
         "eps_estimate": 1.60, "eps_actual": 1.65, "eps_surprise_pct": 3.1,
         "beat_miss": "BEAT", "source": "yfinance"},
        {"ticker": ticker, "earnings_date": date(2026, 1, 29), "fiscal_quarter": "Q1 2026",
         "eps_estimate": 2.35, "eps_actual": 2.40, "eps_surprise_pct": 2.1,
         "beat_miss": "BEAT", "source": "yfinance"},
        {"ticker": ticker, "earnings_date": date(2026, 4, 30), "fiscal_quarter": "Q2 2026",
         "eps_estimate": 1.95, "eps_actual": 1.80, "eps_surprise_pct": -7.7,
         "beat_miss": "MISS", "source": "yfinance"},
    ])


def _insert_price_data(con, ticker, dates_prices):
    """Insert price data: [(date, close), ...]"""
    for d, close in dates_prices:
        con.execute(
            "INSERT INTO price_history VALUES (?, ?, ?, ?, ?, ?, ?)",
            [ticker, d, close, close + 1, close - 1, close, 1000000],
        )


def _insert_iv_data(con, ticker, dates_iv30d):
    """Insert IV data: [(date, iv_30d), ...]"""
    for d, iv30d in dates_iv30d:
        con.execute(
            "INSERT INTO iv_term_history (ticker, date, iv_30d, source) VALUES (?, ?, ?, 'schwab')",
            [ticker, d, iv30d],
        )


# ═══════════════════════════════════════════════════════════════════════════════
# 1. Table Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestTableInitialization:
    def test_creates_all_three_tables(self, pipeline_con):
        tables = pipeline_con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        ).fetchall()
        names = {t[0] for t in tables}
        assert "earnings_history" in names
        assert "earnings_iv_crush" in names
        assert "earnings_stats" in names

    def test_upsert_inserts_new_rows(self, pipeline_con):
        df = _sample_earnings_df()
        count = upsert_earnings_batch(pipeline_con, df)
        assert count == 4
        rows = pipeline_con.execute("SELECT COUNT(*) FROM earnings_history").fetchone()[0]
        assert rows == 4

    def test_upsert_updates_existing_rows(self, pipeline_con):
        df = _sample_earnings_df()
        upsert_earnings_batch(pipeline_con, df)
        # Update one row
        df2 = df.iloc[:1].copy()
        df2["eps_actual"] = 1.50
        df2["eps_surprise_pct"] = 11.1
        df2["beat_miss"] = "BEAT"
        upsert_earnings_batch(pipeline_con, df2)
        # Should still be 4 rows
        rows = pipeline_con.execute("SELECT COUNT(*) FROM earnings_history").fetchone()[0]
        assert rows == 4
        # Updated value
        actual = pipeline_con.execute(
            "SELECT eps_actual FROM earnings_history WHERE ticker='AAPL' AND earnings_date='2025-07-31'"
        ).fetchone()[0]
        assert actual == 1.50

    def test_etf_excluded_from_known_set(self):
        from core.shared.data_contracts.config import KNOWN_ETFS
        assert "SPY" in KNOWN_ETFS
        assert "QQQ" in KNOWN_ETFS
        assert "GLD" in KNOWN_ETFS
        assert "AAPL" not in KNOWN_ETFS


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Data Layer Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestDataLayer:
    def test_get_earnings_history_ordered_desc(self, pipeline_con):
        upsert_earnings_batch(pipeline_con, _sample_earnings_df())
        hist = get_earnings_history(pipeline_con, "AAPL", limit=4)
        assert len(hist) == 4
        # First row should be most recent (DuckDB returns Timestamp)
        assert pd.Timestamp(hist.iloc[0]["earnings_date"]).date() == date(2026, 4, 30)
        assert pd.Timestamp(hist.iloc[-1]["earnings_date"]).date() == date(2025, 7, 31)

    def test_get_earnings_history_empty_ticker(self, pipeline_con):
        hist = get_earnings_history(pipeline_con, "ZZZZ")
        assert hist.empty

    def test_get_ticker_earnings_stats_returns_dict(self, pipeline_con):
        upsert_earnings_batch(pipeline_con, _sample_earnings_df())
        refresh_earnings_stats(pipeline_con, "AAPL")
        stats = get_ticker_earnings_stats(pipeline_con, "AAPL")
        assert stats is not None
        assert stats["ticker"] == "AAPL"
        assert stats["quarters_available"] == 4

    def test_beat_miss_classification(self):
        assert classify_beat_miss(5.0) == "BEAT"
        assert classify_beat_miss(1.1) == "BEAT"
        assert classify_beat_miss(0.5) == "INLINE"
        assert classify_beat_miss(-0.5) == "INLINE"
        assert classify_beat_miss(-1.1) == "MISS"
        assert classify_beat_miss(-10.0) == "MISS"
        assert classify_beat_miss(None) == "UNKNOWN"
        assert classify_beat_miss(float("nan")) == "UNKNOWN"

    def test_batch_upsert_large(self, pipeline_con):
        rows = []
        for i in range(100):
            rows.append({
                "ticker": f"T{i:03d}",
                "earnings_date": date(2025, 1, 1) + timedelta(days=i),
                "eps_estimate": 1.0,
                "eps_actual": 1.1,
                "eps_surprise_pct": 10.0,
                "beat_miss": "BEAT",
                "source": "yfinance",
            })
        df = pd.DataFrame(rows)
        count = upsert_earnings_batch(pipeline_con, df)
        assert count == 100
        total = pipeline_con.execute("SELECT COUNT(*) FROM earnings_history").fetchone()[0]
        assert total == 100


# ═══════════════════════════════════════════════════════════════════════════════
# 3. IV Crush + Expected Move Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestIVCrush:
    def test_complete_data(self, pipeline_con, iv_con):
        """All IV and price readings available → quality = COMPLETE."""
        t = "AAPL"
        ed = date(2026, 1, 29)
        # IV: 5d before, 1d before, 1d after, 5d after
        _insert_iv_data(iv_con, t, [
            (date(2026, 1, 24), 0.35),  # ~5d before
            (date(2026, 1, 28), 0.42),  # 1d before
            (date(2026, 1, 30), 0.28),  # 1d after
            (date(2026, 2, 3), 0.30),   # ~5d after
        ])
        # Prices
        _insert_price_data(pipeline_con, t, [
            (date(2026, 1, 28), 230.0),  # 1d before
            (date(2026, 1, 29), 235.0),  # day of
            (date(2026, 1, 30), 234.0),  # 1d after
            (date(2026, 2, 3), 237.0),   # 5d after
        ])

        result = compute_iv_crush_for_event(pipeline_con, iv_con, t, ed)
        assert result["iv_data_quality"] == "COMPLETE"
        assert result["price_data_quality"] == "COMPLETE"
        # IV crush: (0.42 - 0.28) / 0.42 ≈ 0.333
        assert abs(result["iv_crush_pct"] - 0.333) < 0.01
        # Day move: (235 - 230) / 230 ≈ 0.0217
        assert abs(result["day_move_pct"] - 0.0217) < 0.001

    def test_partial_iv_data(self, pipeline_con, iv_con):
        """Some IV readings missing → quality = PARTIAL."""
        t = "MSFT"
        ed = date(2026, 1, 29)
        _insert_iv_data(iv_con, t, [
            (date(2026, 1, 28), 0.30),  # only 1d before
        ])
        _insert_price_data(pipeline_con, t, [
            (date(2026, 1, 28), 400.0),
            (date(2026, 1, 29), 410.0),
            (date(2026, 1, 30), 408.0),
            (date(2026, 2, 3), 412.0),
        ])
        result = compute_iv_crush_for_event(pipeline_con, iv_con, t, ed)
        assert result["iv_data_quality"] == "PARTIAL"
        assert result["iv_30d_1d_before"] == 0.30
        assert result["iv_30d_5d_before"] is None

    def test_missing_iv_data(self, pipeline_con, iv_con):
        """No IV data at all → quality = MISSING."""
        t = "GOOG"
        ed = date(2026, 2, 4)
        _insert_price_data(pipeline_con, t, [
            (date(2026, 2, 3), 180.0),
            (date(2026, 2, 4), 185.0),
        ])
        result = compute_iv_crush_for_event(pipeline_con, iv_con, t, ed)
        assert result["iv_data_quality"] == "MISSING"
        assert result["iv_crush_pct"] is None

    def test_gap_and_day_move(self, pipeline_con, iv_con):
        """gap_pct and day_move_pct computed correctly."""
        t = "TSLA"
        ed = date(2026, 1, 22)
        _insert_price_data(pipeline_con, t, [
            (date(2026, 1, 21), 200.0),  # 1d before
            (date(2026, 1, 22), 220.0),  # day of (10% up)
            (date(2026, 1, 23), 215.0),  # 1d after
            (date(2026, 1, 27), 225.0),  # 5d after
        ])
        result = compute_iv_crush_for_event(pipeline_con, iv_con, t, ed)
        # day_move: (220-200)/200 = 0.10
        assert abs(result["day_move_pct"] - 0.10) < 0.001
        # move_5d: (225-200)/200 = 0.125
        assert abs(result["move_5d_pct"] - 0.125) < 0.001

    def test_expected_move_straddle_approximation(self, pipeline_con, iv_con):
        """expected_move_pct = iv_30d × √(1/252)."""
        t = "NVDA"
        ed = date(2026, 2, 19)
        # IV 1d before = 0.60 (60%)
        _insert_iv_data(iv_con, t, [(date(2026, 2, 18), 0.60)])
        _insert_price_data(pipeline_con, t, [
            (date(2026, 2, 18), 800.0),
            (date(2026, 2, 19), 830.0),  # 3.75% move
        ])
        result = compute_iv_crush_for_event(pipeline_con, iv_con, t, ed)
        # Expected = 0.60 × √(1/252) ≈ 0.0378
        expected = 0.60 * math.sqrt(1.0 / 252.0)
        assert abs(result["expected_move_pct"] - expected) < 0.001
        # Actual = |830-800|/800 = 0.0375
        assert abs(result["actual_move_pct"] - 0.0375) < 0.001

    def test_move_ratio_computed(self, pipeline_con, iv_con):
        """move_ratio = actual / expected, handles edge cases."""
        t = "META"
        ed = date(2026, 1, 30)
        _insert_iv_data(iv_con, t, [(date(2026, 1, 29), 0.50)])
        _insert_price_data(pipeline_con, t, [
            (date(2026, 1, 29), 500.0),
            (date(2026, 1, 30), 540.0),  # 8% move
        ])
        result = compute_iv_crush_for_event(pipeline_con, iv_con, t, ed)
        assert result["move_ratio"] is not None
        # Expected ≈ 0.50 × √(1/252) ≈ 0.0315
        # Actual = 40/500 = 0.08
        # Ratio ≈ 0.08 / 0.0315 ≈ 2.54 (market underpriced the move)
        assert result["move_ratio"] > 2.0


# ═══════════════════════════════════════════════════════════════════════════════
# 4. IV Ramp Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestIVRamp:
    def test_ramp_detection(self, iv_con):
        """Detects IV ramp starting before earnings."""
        t = "AAPL"
        ed = date(2026, 1, 29)
        # IV climbs from 0.25 → 0.42 over 2 weeks before earnings
        _insert_iv_data(iv_con, t, [
            (date(2026, 1, 15), 0.25),  # baseline
            (date(2026, 1, 17), 0.26),
            (date(2026, 1, 20), 0.28),
            (date(2026, 1, 22), 0.32),  # ramp starts
            (date(2026, 1, 24), 0.36),
            (date(2026, 1, 27), 0.40),
            (date(2026, 1, 28), 0.42),  # peak before earnings
        ])
        days = _compute_iv_ramp_start(iv_con, t, ed)
        assert days is not None
        assert days >= 10  # ramp started at least 10 days before

    def test_buildup_pct(self, pipeline_con, iv_con):
        """iv_buildup_pct = (1d_before - 5d_before) / 5d_before."""
        t = "CRM"
        ed = date(2026, 3, 5)
        _insert_iv_data(iv_con, t, [
            (date(2026, 2, 28), 0.30),  # ~5d before
            (date(2026, 3, 4), 0.39),   # 1d before
            (date(2026, 3, 6), 0.25),   # 1d after
        ])
        _insert_price_data(pipeline_con, t, [
            (date(2026, 3, 4), 300.0),
            (date(2026, 3, 5), 310.0),
        ])
        result = compute_iv_crush_for_event(pipeline_con, iv_con, t, ed)
        # Buildup: (0.39 - 0.30) / 0.30 = 0.30 (30%)
        assert result["iv_buildup_pct"] is not None
        assert abs(result["iv_buildup_pct"] - 0.30) < 0.01

    def test_sparse_data_ramp(self, iv_con):
        """Sparse IV data still finds approximate ramp start."""
        t = "AMZN"
        ed = date(2026, 2, 5)
        # Only 3 readings with gaps
        _insert_iv_data(iv_con, t, [
            (date(2026, 1, 20), 0.28),  # baseline (16d before)
            (date(2026, 1, 28), 0.35),  # ramp in progress
            (date(2026, 2, 3), 0.45),   # near peak
        ])
        days = _compute_iv_ramp_start(iv_con, t, ed)
        # Should find the ramp (0.28 is the trough)
        assert days is not None
        assert days >= 10


# ═══════════════════════════════════════════════════════════════════════════════
# 5. Summary Stats Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestSummaryStats:
    def test_refresh_computes_all_fields(self, pipeline_con):
        """refresh_earnings_stats computes beat_rate, streaks, etc."""
        upsert_earnings_batch(pipeline_con, _sample_earnings_df())
        stats = refresh_earnings_stats(pipeline_con, "AAPL")
        assert stats is not None
        assert stats["quarters_available"] == 4
        assert stats["beat_rate"] == 0.75  # 3/4
        assert stats["miss_rate"] == 0.25  # 1/4
        assert stats["last_beat_miss"] == "MISS"  # most recent is Q2 2026 MISS
        assert pd.Timestamp(stats["last_earnings_date"]).date() == date(2026, 4, 30)

    def test_consecutive_streak(self, pipeline_con):
        """Streak counts correctly — 3 beats then 1 miss = 0 consecutive beats."""
        upsert_earnings_batch(pipeline_con, _sample_earnings_df())
        stats = refresh_earnings_stats(pipeline_con, "AAPL")
        # Most recent is MISS, so consecutive_beats=0, consecutive_misses=1
        assert stats["consecutive_beats"] == 0
        assert stats["consecutive_misses"] == 1

        # Now test all-beat scenario
        df_all_beat = pd.DataFrame([
            {"ticker": "MSFT", "earnings_date": date(2025, 10, 1),
             "eps_estimate": 2.0, "eps_actual": 2.2, "eps_surprise_pct": 10.0,
             "beat_miss": "BEAT", "source": "yfinance"},
            {"ticker": "MSFT", "earnings_date": date(2026, 1, 1),
             "eps_estimate": 2.1, "eps_actual": 2.3, "eps_surprise_pct": 9.5,
             "beat_miss": "BEAT", "source": "yfinance"},
            {"ticker": "MSFT", "earnings_date": date(2026, 4, 1),
             "eps_estimate": 2.2, "eps_actual": 2.4, "eps_surprise_pct": 9.1,
             "beat_miss": "BEAT", "source": "yfinance"},
        ])
        upsert_earnings_batch(pipeline_con, df_all_beat)
        stats2 = refresh_earnings_stats(pipeline_con, "MSFT")
        assert stats2["consecutive_beats"] == 3
        assert stats2["consecutive_misses"] == 0


# ═══════════════════════════════════════════════════════════════════════════════
# 6. Integration Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestIntegration:
    def test_refresh_all_stats(self, pipeline_con):
        """refresh_all_earnings_stats processes all tickers."""
        upsert_earnings_batch(pipeline_con, _sample_earnings_df("AAPL"))
        upsert_earnings_batch(pipeline_con, _sample_earnings_df("MSFT"))
        count = refresh_all_earnings_stats(pipeline_con)
        assert count == 2
        # Both should be in earnings_stats
        all_stats = get_all_earnings_stats(pipeline_con)
        assert len(all_stats) == 2

    def test_get_all_earnings_stats_filtered(self, pipeline_con):
        """get_all_earnings_stats with ticker filter."""
        upsert_earnings_batch(pipeline_con, _sample_earnings_df("AAPL"))
        upsert_earnings_batch(pipeline_con, _sample_earnings_df("MSFT"))
        refresh_all_earnings_stats(pipeline_con)
        filtered = get_all_earnings_stats(pipeline_con, tickers=["AAPL"])
        assert len(filtered) == 1
        assert filtered.iloc[0]["ticker"] == "AAPL"

    def test_schema_has_earnings_columns(self):
        """MANAGEMENT_UI_COLUMNS includes 14 earnings columns (10 history + 4 formation)."""
        from core.shared.data_contracts.schema import MANAGEMENT_UI_COLUMNS
        earnings_cols = [c for c in MANAGEMENT_UI_COLUMNS if c.startswith("Earnings_")]
        assert len(earnings_cols) == 14
        assert "Earnings_Beat_Rate" in earnings_cols
        assert "Earnings_Avg_Move_Ratio" in earnings_cols
        assert "Earnings_Track_Quarters" in earnings_cols
        assert "Earnings_Current_Phase" in earnings_cols

    def test_known_etfs_in_config(self):
        """KNOWN_ETFS is accessible from config."""
        from core.shared.data_contracts.config import KNOWN_ETFS
        assert isinstance(KNOWN_ETFS, frozenset)
        assert len(KNOWN_ETFS) >= 20
        assert "SPY" in KNOWN_ETFS
        assert "AAPL" not in KNOWN_ETFS
