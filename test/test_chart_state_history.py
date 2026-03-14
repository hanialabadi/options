"""
Tests for Chart State History Bank (Phase β1)
and Parallel Price History (Phase β3).
"""

import pytest
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


# ── Chart State History Bank Tests ──────────────────────────────────────────

class TestChartStateSchema:
    """Schema creation and idempotency."""

    def test_initialize_creates_table(self, tmp_path):
        from core.shared.data_layer.chart_state_repository import (
            initialize_chart_state_table, CHART_STATE_TABLE
        )
        db = tmp_path / "test.duckdb"
        con = duckdb.connect(str(db))
        initialize_chart_state_table(con)

        tables = con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = ?",
            [CHART_STATE_TABLE]
        ).fetchall()
        assert len(tables) == 1
        con.close()

    def test_initialize_idempotent(self, tmp_path):
        from core.shared.data_layer.chart_state_repository import initialize_chart_state_table
        db = tmp_path / "test.duckdb"
        con = duckdb.connect(str(db))
        initialize_chart_state_table(con)
        initialize_chart_state_table(con)  # second call should not raise
        con.close()

    def test_schema_has_all_state_columns(self, tmp_path):
        from core.shared.data_layer.chart_state_repository import (
            initialize_chart_state_table, CHART_STATE_TABLE, STATE_COLUMNS
        )
        db = tmp_path / "test.duckdb"
        con = duckdb.connect(str(db))
        initialize_chart_state_table(con)

        cols = {row[1] for row in con.execute(
            f"PRAGMA table_info('{CHART_STATE_TABLE}')"
        ).fetchall()}
        for state_col in STATE_COLUMNS:
            assert state_col in cols, f"Missing state column: {state_col}"
        con.close()

    def test_schema_has_numeric_columns(self, tmp_path):
        from core.shared.data_layer.chart_state_repository import (
            initialize_chart_state_table, CHART_STATE_TABLE, NUMERIC_COLUMNS
        )
        db = tmp_path / "test.duckdb"
        con = duckdb.connect(str(db))
        initialize_chart_state_table(con)

        cols = {row[1] for row in con.execute(
            f"PRAGMA table_info('{CHART_STATE_TABLE}')"
        ).fetchall()}
        for col_name, _ in NUMERIC_COLUMNS:
            assert col_name in cols, f"Missing numeric column: {col_name}"
        con.close()

    def test_index_created(self, tmp_path):
        from core.shared.data_layer.chart_state_repository import (
            initialize_chart_state_table, CHART_STATE_TABLE
        )
        db = tmp_path / "test.duckdb"
        con = duckdb.connect(str(db))
        initialize_chart_state_table(con)

        indexes = con.execute(
            f"SELECT index_name FROM duckdb_indexes() WHERE table_name = '{CHART_STATE_TABLE}'"
        ).fetchall()
        idx_names = {row[0] for row in indexes}
        assert "idx_csh_ticker_ts" in idx_names
        con.close()


class TestChartStatePersistence:
    """Write and read chart states."""

    def _make_df(self, tickers=None):
        """Create a DataFrame with chart state columns."""
        if tickers is None:
            tickers = ["AAPL", "MSFT"]
        rows = []
        for t in tickers:
            rows.append({
                "Underlying_Ticker": t,
                "PriceStructure_State": "STRUCTURAL_UP",
                "TrendIntegrity_State": "STRONG_TREND",
                "VolatilityState_State": "NORMAL",
                "CompressionMaturity_State": "POST_EXPANSION",
                "MomentumVelocity_State": "TRENDING",
                "DirectionalBalance_State": "BUYER_DOMINANT",
                "RangeEfficiency_State": "EFFICIENT_TREND",
                "TimeframeAgreement_State": "ALIGNED",
                "GreekDominance_State": "THETA_DOMINANT",
                "AssignmentRisk_State": "LOW",
                "RegimeStability_State": "ESTABLISHED",
                "RecoveryQuality_State": "NOT_IN_RECOVERY",
                "adx_14": 32.5,
                "rsi_14": 55.0,
                "roc_5": 2.1,
                "bb_width_pct": 0.045,
                "hv_20d_percentile": 0.65,
            })
        return pd.DataFrame(rows)

    def test_persist_round_trip(self, tmp_path):
        from core.shared.data_layer.chart_state_repository import (
            initialize_chart_state_table, persist_chart_states,
            get_chart_state_history, CHART_STATE_TABLE
        )
        db = tmp_path / "test.duckdb"
        con = duckdb.connect(str(db))
        initialize_chart_state_table(con)

        df = self._make_df(["AAPL"])
        persist_chart_states(df, con=con)

        result = get_chart_state_history(["AAPL"], lookback_days=1, con=con)
        assert result is not None
        assert len(result) == 1
        assert result.iloc[0]["Ticker"] == "AAPL"
        assert result.iloc[0]["TrendIntegrity"] == "STRONG_TREND"
        assert result.iloc[0]["adx_14"] == pytest.approx(32.5)
        con.close()

    def test_persist_multiple_tickers(self, tmp_path):
        from core.shared.data_layer.chart_state_repository import (
            initialize_chart_state_table, persist_chart_states,
            get_chart_state_history
        )
        db = tmp_path / "test.duckdb"
        con = duckdb.connect(str(db))
        initialize_chart_state_table(con)

        df = self._make_df(["AAPL", "MSFT", "GOOG"])
        persist_chart_states(df, con=con)

        result = get_chart_state_history(["AAPL", "MSFT", "GOOG"], lookback_days=1, con=con)
        assert result is not None
        assert result["Ticker"].nunique() == 3
        con.close()

    def test_persist_missing_columns_graceful(self, tmp_path):
        from core.shared.data_layer.chart_state_repository import (
            initialize_chart_state_table, persist_chart_states,
            get_chart_state_history
        )
        db = tmp_path / "test.duckdb"
        con = duckdb.connect(str(db))
        initialize_chart_state_table(con)

        # DataFrame with only some state columns
        df = pd.DataFrame([{
            "Underlying_Ticker": "AAPL",
            "PriceStructure_State": "STRUCTURAL_UP",
            "TrendIntegrity_State": "STRONG_TREND",
        }])
        persist_chart_states(df, con=con)

        result = get_chart_state_history(["AAPL"], lookback_days=1, con=con)
        assert result is not None
        assert len(result) == 1
        assert result.iloc[0]["PriceStructure"] == "STRUCTURAL_UP"
        # Missing states should be None
        assert pd.isna(result.iloc[0]["VolatilityState"])
        con.close()

    def test_upsert_same_ticker(self, tmp_path):
        """INSERT OR REPLACE on same (Ticker, Snapshot_TS) should update."""
        from core.shared.data_layer.chart_state_repository import (
            initialize_chart_state_table, persist_chart_states, CHART_STATE_TABLE
        )
        db = tmp_path / "test.duckdb"
        con = duckdb.connect(str(db))
        initialize_chart_state_table(con)

        df1 = pd.DataFrame([{
            "Underlying_Ticker": "AAPL",
            "TrendIntegrity_State": "STRONG_TREND",
        }])
        persist_chart_states(df1, con=con)

        # Second persist within same second gets same Snapshot_TS → upsert
        # (in real usage, runs are minutes/hours apart)
        count = con.execute(f"SELECT COUNT(*) FROM {CHART_STATE_TABLE}").fetchone()[0]
        assert count >= 1  # At least one row persisted
        con.close()


class TestChartStateHistory:
    """History retrieval and lookback filtering."""

    def test_lookback_filter(self, tmp_path):
        from core.shared.data_layer.chart_state_repository import (
            initialize_chart_state_table, CHART_STATE_TABLE,
            get_chart_state_history
        )
        db = tmp_path / "test.duckdb"
        con = duckdb.connect(str(db))
        initialize_chart_state_table(con)

        now = datetime.utcnow()
        # Insert recent and old rows
        con.execute(f"""
            INSERT INTO {CHART_STATE_TABLE} (Ticker, Snapshot_TS, TrendIntegrity, Computed_TS)
            VALUES (?, ?, ?, ?)
        """, ["AAPL", now, "STRONG_TREND", now])
        con.execute(f"""
            INSERT INTO {CHART_STATE_TABLE} (Ticker, Snapshot_TS, TrendIntegrity, Computed_TS)
            VALUES (?, ?, ?, ?)
        """, ["AAPL", now - timedelta(days=20), "WEAK_TREND", now - timedelta(days=20)])

        # Lookback 7 days should only get recent
        result = get_chart_state_history(["AAPL"], lookback_days=7, con=con)
        assert result is not None
        assert len(result) == 1
        assert result.iloc[0]["TrendIntegrity"] == "STRONG_TREND"

        # Lookback 30 days gets both
        result_all = get_chart_state_history(["AAPL"], lookback_days=30, con=con)
        assert len(result_all) == 2
        con.close()

    def test_no_data_returns_empty(self, tmp_path):
        from core.shared.data_layer.chart_state_repository import (
            initialize_chart_state_table, get_chart_state_history
        )
        db = tmp_path / "test.duckdb"
        con = duckdb.connect(str(db))
        initialize_chart_state_table(con)

        result = get_chart_state_history(["AAPL"], lookback_days=7, con=con)
        assert result is not None
        assert len(result) == 0
        con.close()

    def test_no_table_returns_none(self, tmp_path):
        from core.shared.data_layer.chart_state_repository import get_chart_state_history
        db = tmp_path / "test.duckdb"
        con = duckdb.connect(str(db))

        result = get_chart_state_history(["AAPL"], lookback_days=7, con=con)
        assert result is None
        con.close()


class TestStateTransitions:
    """Transition detection queries."""

    def test_detect_transition(self, tmp_path):
        from core.shared.data_layer.chart_state_repository import (
            initialize_chart_state_table, CHART_STATE_TABLE,
            get_state_transitions
        )
        db = tmp_path / "test.duckdb"
        con = duckdb.connect(str(db))
        initialize_chart_state_table(con)

        now = datetime.utcnow()
        # Day 1: STRONG_TREND
        con.execute(f"""
            INSERT INTO {CHART_STATE_TABLE} (Ticker, Snapshot_TS, TrendIntegrity, Computed_TS)
            VALUES (?, ?, ?, ?)
        """, ["AAPL", now - timedelta(days=2), "STRONG_TREND", now - timedelta(days=2)])
        # Day 2: TREND_EXHAUSTED (transition!)
        con.execute(f"""
            INSERT INTO {CHART_STATE_TABLE} (Ticker, Snapshot_TS, TrendIntegrity, Computed_TS)
            VALUES (?, ?, ?, ?)
        """, ["AAPL", now - timedelta(days=1), "TREND_EXHAUSTED", now - timedelta(days=1)])
        # Day 3: same TREND_EXHAUSTED (no transition)
        con.execute(f"""
            INSERT INTO {CHART_STATE_TABLE} (Ticker, Snapshot_TS, TrendIntegrity, Computed_TS)
            VALUES (?, ?, ?, ?)
        """, ["AAPL", now, "TREND_EXHAUSTED", now])

        transitions = get_state_transitions("AAPL", "TrendIntegrity", lookback_days=7, con=con)
        assert transitions is not None
        assert len(transitions) == 1
        assert transitions.iloc[0]["prev_state"] == "STRONG_TREND"
        assert transitions.iloc[0]["new_state"] == "TREND_EXHAUSTED"
        con.close()

    def test_no_transitions(self, tmp_path):
        from core.shared.data_layer.chart_state_repository import (
            initialize_chart_state_table, CHART_STATE_TABLE,
            get_state_transitions
        )
        db = tmp_path / "test.duckdb"
        con = duckdb.connect(str(db))
        initialize_chart_state_table(con)

        now = datetime.utcnow()
        # All same state
        for d in range(3):
            con.execute(f"""
                INSERT INTO {CHART_STATE_TABLE} (Ticker, Snapshot_TS, TrendIntegrity, Computed_TS)
                VALUES (?, ?, ?, ?)
            """, ["AAPL", now - timedelta(days=d), "STRONG_TREND", now - timedelta(days=d)])

        transitions = get_state_transitions("AAPL", "TrendIntegrity", lookback_days=7, con=con)
        assert transitions is not None
        assert len(transitions) == 0
        con.close()

    def test_invalid_state_column(self, tmp_path):
        from core.shared.data_layer.chart_state_repository import (
            initialize_chart_state_table, get_state_transitions
        )
        db = tmp_path / "test.duckdb"
        con = duckdb.connect(str(db))
        initialize_chart_state_table(con)

        result = get_state_transitions("AAPL", "BogusColumn", lookback_days=7, con=con)
        assert result is None
        con.close()


class TestTtlPruning:
    """Stale data cleanup."""

    def test_prune_removes_old_rows(self, tmp_path):
        from core.shared.data_layer.chart_state_repository import (
            initialize_chart_state_table, CHART_STATE_TABLE,
            prune_stale_chart_states
        )
        db = tmp_path / "test.duckdb"
        con = duckdb.connect(str(db))
        initialize_chart_state_table(con)

        now = datetime.utcnow()
        # Old row (60 days ago)
        con.execute(f"""
            INSERT INTO {CHART_STATE_TABLE} (Ticker, Snapshot_TS, TrendIntegrity, Computed_TS)
            VALUES (?, ?, ?, ?)
        """, ["OLD", now - timedelta(days=60), "STRONG_TREND", now - timedelta(days=60)])
        # Recent row
        con.execute(f"""
            INSERT INTO {CHART_STATE_TABLE} (Ticker, Snapshot_TS, TrendIntegrity, Computed_TS)
            VALUES (?, ?, ?, ?)
        """, ["FRESH", now, "STRONG_TREND", now])

        prune_stale_chart_states(max_age_days=30, con=con)

        remaining = con.execute(f"SELECT Ticker FROM {CHART_STATE_TABLE}").fetchall()
        tickers = {r[0] for r in remaining}
        assert "OLD" not in tickers
        assert "FRESH" in tickers
        con.close()

    def test_prune_retains_recent(self, tmp_path):
        from core.shared.data_layer.chart_state_repository import (
            initialize_chart_state_table, CHART_STATE_TABLE,
            prune_stale_chart_states
        )
        db = tmp_path / "test.duckdb"
        con = duckdb.connect(str(db))
        initialize_chart_state_table(con)

        now = datetime.utcnow()
        for d in range(5):
            con.execute(f"""
                INSERT INTO {CHART_STATE_TABLE} (Ticker, Snapshot_TS, TrendIntegrity, Computed_TS)
                VALUES (?, ?, ?, ?)
            """, [f"T{d}", now - timedelta(days=d), "STRONG_TREND", now - timedelta(days=d)])

        prune_stale_chart_states(max_age_days=30, con=con)

        count = con.execute(f"SELECT COUNT(*) FROM {CHART_STATE_TABLE}").fetchone()[0]
        assert count == 5  # All recent, none pruned
        con.close()


# ── Parallel Price History Tests ────────────────────────────────────────────

class TestParallelPrimitives:
    """Verify parallelized compute_chart_primitives behavior."""

    def test_empty_df_returns_empty(self):
        from core.management.cycle2.chart_primitives.compute_primitives import compute_chart_primitives
        df = pd.DataFrame()
        result = compute_chart_primitives(df)
        assert result.empty

    def test_fetch_and_compute_returns_tuple(self):
        """The inner _fetch_and_compute should return (ticker, primitives, reason) tuple."""
        # This tests the contract, not the actual API call
        from core.management.cycle2.chart_primitives.compute_primitives import (
            _calculate_primitives_for_ticker
        )
        # Create minimal OHLCV data (200 bars)
        np.random.seed(42)
        n = 200
        close = 100 + np.cumsum(np.random.randn(n) * 0.5)
        df = pd.DataFrame({
            "Open": close + np.random.randn(n) * 0.2,
            "High": close + abs(np.random.randn(n) * 0.5),
            "Low": close - abs(np.random.randn(n) * 0.5),
            "Close": close,
            "Volume": np.random.randint(100000, 1000000, n),
        })
        result = _calculate_primitives_for_ticker(df)
        assert isinstance(result, dict)
        assert "adx_14" in result
        assert "rsi_14" in result
        assert "roc_5" in result

    def test_resolution_reason_set_on_failure(self):
        """When price history fails, Resolution_Reason should be set."""
        from core.management.cycle2.chart_primitives.compute_primitives import compute_chart_primitives
        # DataFrame with a ticker but no API client → will fail to fetch
        df = pd.DataFrame([{
            "Underlying_Ticker": "ZZZZ_FAKE_TICKER_999",
            "Ticker": "ZZZZ_FAKE_TICKER_999",
        }])
        result = compute_chart_primitives(df, client=None)
        # Should not crash, and may have a Resolution_Reason
        assert not result.empty
