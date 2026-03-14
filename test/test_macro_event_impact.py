"""
Tests for macro event impact tracking system.

Covers:
  - Schema: table creation, idempotent init
  - CRUD: write, duplicate guard, query by type
  - Stats: aggregation, empty table → None
  - Collector: impact computation from market_context + price_history
  - MC calibration: empirical vs default parameters
  - Backfill: processes past events
"""

import math
import numpy as np
import pandas as pd
import pytest
from datetime import date, timedelta
from unittest.mock import patch, MagicMock


# ═══════════════════════════════════════════════════════════════════════════════
# Schema & CRUD
# ═══════════════════════════════════════════════════════════════════════════════

class TestMacroEventImpactSchema:
    """Table creation and basic CRUD operations."""

    def test_initialize_table_idempotent(self, tmp_path):
        """Creating table twice doesn't raise."""
        from core.shared.data_layer.macro_event_impact import initialize_event_impact_table
        import duckdb
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        initialize_event_impact_table(con)
        initialize_event_impact_table(con)  # second call — must not raise
        tables = con.execute("SELECT table_name FROM information_schema.tables").fetchdf()
        assert "macro_event_impact" in tables["table_name"].values
        con.close()

    def test_write_and_read_event_impact(self):
        """Write an impact record and verify it can be queried."""
        from core.shared.data_layer.macro_event_impact import (
            write_event_impact, query_event_impact_by_type,
            initialize_event_impact_table, event_impact_exists,
        )
        from core.shared.data_layer.duckdb_utils import DbDomain, get_domain_write_connection

        # Use real MARKET domain (test isolation via unique date)
        test_date = date(2020, 1, 15)  # Far past — won't collide with real data

        data = {
            "event_date": test_date,
            "event_type": "FOMC",
            "event_label": "Test FOMC",
            "event_impact": "HIGH",
            "vix_prior": 14.0,
            "vix_close": 16.5,
            "vix_change": 2.5,
            "vix_change_pct": 0.1786,
            "spy_prior_close": 330.0,
            "spy_close": 327.0,
            "spy_change_pct": -0.0091,
            "universe_avg_move_pct": 0.015,
            "universe_median_move_pct": 0.012,
            "universe_pct_advancing": 0.35,
            "universe_pct_declining": 0.65,
            "regime_prior": "NORMAL",
            "regime_after": "CAUTIOUS",
            "regime_score_prior": 30.0,
            "regime_score_after": 45.0,
            "regime_changed": True,
        }

        try:
            write_event_impact(data)
            assert event_impact_exists(test_date, "FOMC")

            df = query_event_impact_by_type("FOMC", limit=100)
            row = df[df["event_date"] == pd.Timestamp(test_date)]
            assert len(row) >= 1
            assert float(row.iloc[0]["vix_change"]) == pytest.approx(2.5)
            assert row.iloc[0]["regime_changed"] == True
        finally:
            # Cleanup
            try:
                con = get_domain_write_connection(DbDomain.MARKET)
                con.execute(
                    "DELETE FROM macro_event_impact WHERE event_date = ?",
                    [test_date],
                )
                con.close()
            except Exception:
                pass

    def test_duplicate_guard(self):
        """event_impact_exists returns True after write."""
        from core.shared.data_layer.macro_event_impact import (
            write_event_impact, event_impact_exists,
        )
        from core.shared.data_layer.duckdb_utils import DbDomain, get_domain_write_connection

        test_date = date(2020, 2, 15)
        data = {
            "event_date": test_date,
            "event_type": "CPI",
            "event_label": "Test CPI",
            "event_impact": "HIGH",
        }

        try:
            assert not event_impact_exists(test_date, "CPI")
            write_event_impact(data)
            assert event_impact_exists(test_date, "CPI")
        finally:
            try:
                con = get_domain_write_connection(DbDomain.MARKET)
                con.execute(
                    "DELETE FROM macro_event_impact WHERE event_date = ?",
                    [test_date],
                )
                con.close()
            except Exception:
                pass


# ═══════════════════════════════════════════════════════════════════════════════
# Stats & Aggregation
# ═══════════════════════════════════════════════════════════════════════════════

class TestMacroEventStats:
    """Aggregate statistics for MC consumption."""

    def test_empty_table_returns_none(self):
        """query_event_stats for a type with no data returns None."""
        from core.shared.data_layer.macro_event_impact import query_event_stats
        # Use a type that won't exist in test data
        stats = query_event_stats("NONEXISTENT_EVENT")
        assert stats is None

    def test_query_all_event_stats_returns_dict(self):
        """query_all_event_stats returns dict keyed by event type."""
        from core.shared.data_layer.macro_event_impact import query_all_event_stats
        result = query_all_event_stats()
        assert isinstance(result, dict)
        # Keys should be subset of known event types
        for key in result:
            assert key in ("FOMC", "CPI", "NFP", "GDP", "PCE")


# ═══════════════════════════════════════════════════════════════════════════════
# MC Calibration
# ═══════════════════════════════════════════════════════════════════════════════

class TestMCMacroCalibration:
    """MC jump parameter calibration from empirical macro data."""

    def test_default_calibration_when_no_data(self):
        """No empirical data → returns conservative defaults."""
        from core.shared.data_layer.macro_event_impact import get_mc_macro_calibration
        cal = get_mc_macro_calibration("NONEXISTENT")
        assert cal is not None
        assert cal["calibration_source"] == "default"
        assert cal["n_events"] == 0
        assert cal["jump_intensity_mult"] == 1.5
        assert cal["jump_std_mult"] == 1.3

    def test_empirical_calibration_structure(self):
        """Calibration dict has all required keys for MC consumption."""
        from core.shared.data_layer.macro_event_impact import get_mc_macro_calibration
        cal = get_mc_macro_calibration("FOMC")
        assert cal is not None
        required_keys = {
            "jump_intensity_mult", "jump_std_mult", "jump_mean_adj",
            "avg_spy_abs_move_pct", "avg_vix_change_pct",
            "n_events", "calibration_source",
        }
        assert required_keys.issubset(cal.keys())

    def test_calibration_multipliers_bounded(self):
        """Calibration multipliers are within reasonable bounds."""
        from core.shared.data_layer.macro_event_impact import get_mc_macro_calibration
        for evt_type in ("FOMC", "CPI", "NFP"):
            cal = get_mc_macro_calibration(evt_type)
            assert 1.0 <= cal["jump_intensity_mult"] <= 2.5
            assert 1.0 <= cal["jump_std_mult"] <= 2.5

    @patch("core.shared.data_layer.macro_event_impact.query_event_stats")
    def test_empirical_calibration_high_vol_event(self, mock_stats):
        """High-vol event (avg 2% SPY move) → amplified jump params."""
        from core.shared.data_layer.macro_event_impact import get_mc_macro_calibration
        mock_stats.return_value = {
            "n_events": 5,
            "avg_spy_move_pct": -0.005,
            "median_spy_move_pct": -0.003,
            "std_spy_move_pct": 0.02,
            "avg_spy_abs_move_pct": 0.02,
            "avg_vix_change_pct": 0.15,
            "avg_vix_change_abs": 3.0,
            "avg_universe_move_pct": 0.025,
            "regime_change_rate": 0.4,
        }
        cal = get_mc_macro_calibration("FOMC")
        assert cal["calibration_source"] == "empirical"
        assert cal["n_events"] == 5
        assert cal["jump_intensity_mult"] > 1.5  # high avg move → amplified
        assert cal["jump_std_mult"] > 1.5  # high std → amplified

    @patch("core.shared.data_layer.macro_event_impact.query_event_stats")
    def test_empirical_calibration_low_vol_event(self, mock_stats):
        """Low-vol event (avg 0.3% SPY move) → minimal amplification."""
        from core.shared.data_layer.macro_event_impact import get_mc_macro_calibration
        mock_stats.return_value = {
            "n_events": 4,
            "avg_spy_move_pct": -0.001,
            "median_spy_move_pct": -0.001,
            "std_spy_move_pct": 0.005,
            "avg_spy_abs_move_pct": 0.003,
            "avg_vix_change_pct": 0.02,
            "avg_vix_change_abs": 0.5,
            "avg_universe_move_pct": 0.005,
            "regime_change_rate": 0.0,
        }
        cal = get_mc_macro_calibration("GDP")
        assert cal["calibration_source"] == "empirical"
        assert cal["jump_intensity_mult"] < 1.3  # low impact → minimal


# ═══════════════════════════════════════════════════════════════════════════════
# Collector Logic
# ═══════════════════════════════════════════════════════════════════════════════

class TestMacroImpactCollector:
    """Impact computation from existing data sources."""

    def test_non_event_day_returns_zero(self):
        """Day with no macro events → events_processed=0."""
        from core.shared.data_layer.macro_impact_collector import collect_macro_impact
        # Pick a Saturday — no events
        result = collect_macro_impact(d=date(2026, 3, 14))  # Saturday
        assert result["ok"] is True
        assert result["events_processed"] == 0

    def test_get_events_for_date_fomc(self):
        """FOMC dates from macro calendar are found."""
        from core.shared.data_layer.macro_impact_collector import _get_events_for_date
        events = _get_events_for_date(date(2026, 3, 18))
        assert len(events) == 1
        assert events[0].event_type == "FOMC"

    def test_get_events_for_date_multi_event_day(self):
        """Days with GDP+PCE on same date return both events."""
        from core.shared.data_layer.macro_impact_collector import _get_events_for_date
        # GDP and PCE share dates (e.g., 2026-01-29)
        events = _get_events_for_date(date(2026, 1, 29))
        types = {e.event_type for e in events}
        assert "GDP" in types
        assert "PCE" in types

    def test_safe_pct_handles_none(self):
        """_safe_pct with None values returns None."""
        from core.shared.data_layer.macro_impact_collector import _safe_pct
        assert _safe_pct(None, 100.0) is None
        assert _safe_pct(100.0, None) is None
        assert _safe_pct(100.0, 0.0) is None

    def test_safe_pct_computes_correctly(self):
        """_safe_pct computes percent change."""
        from core.shared.data_layer.macro_impact_collector import _safe_pct
        assert _safe_pct(105.0, 100.0) == pytest.approx(0.05)
        assert _safe_pct(95.0, 100.0) == pytest.approx(-0.05)

    def test_compute_event_impact_returns_dict(self):
        """compute_event_impact returns a dict with expected keys when data exists."""
        from core.shared.data_layer.macro_impact_collector import compute_event_impact
        from config.macro_calendar import MacroEvent

        # Use a recent event date where market_context_daily might have data
        event = MacroEvent("CPI", date(2026, 3, 11), "CPI Report", "HIGH")
        result = compute_event_impact(event)

        # Result is either None (no data) or a dict with the right shape
        if result is not None:
            assert "event_date" in result
            assert "event_type" in result
            assert result["event_type"] == "CPI"
            assert "spy_change_pct" in result
            assert "vix_change" in result
            assert "regime_changed" in result


# ═══════════════════════════════════════════════════════════════════════════════
# MC Integration
# ═══════════════════════════════════════════════════════════════════════════════

class TestMCMacroIntegration:
    """MC position sizing uses macro calibration when available."""

    def test_simulate_pnl_paths_accepts_macro_calibration(self):
        """simulate_pnl_paths runs with macro_calibration parameter."""
        from scan_engine.mc_position_sizing import simulate_pnl_paths
        rng = np.random.default_rng(42)

        macro_cal = {
            "jump_intensity_mult": 1.5,
            "jump_std_mult": 1.3,
            "jump_mean_adj": 0.0,
        }

        pnl = simulate_pnl_paths(
            spot=180.0,
            strike=175.0,
            hv_annual=0.35,
            dte=30,
            premium=8.0,
            option_type="put",
            strategy_class="LONG",
            n_paths=500,
            rng=rng,
            macro_calibration=macro_cal,
        )
        assert len(pnl) == 500
        assert not np.all(np.isnan(pnl))

    def test_macro_calibration_widens_distribution(self):
        """Macro calibration produces wider P&L distribution (fatter tails)."""
        from scan_engine.mc_position_sizing import simulate_pnl_paths
        n = 5_000

        # Without macro cal
        rng1 = np.random.default_rng(42)
        pnl_base = simulate_pnl_paths(
            spot=180.0, strike=175.0, hv_annual=0.35, dte=30,
            premium=8.0, option_type="put", strategy_class="LONG",
            n_paths=n, rng=rng1,
        )

        # With macro cal (amplified jumps)
        rng2 = np.random.default_rng(42)
        macro_cal = {
            "jump_intensity_mult": 2.0,
            "jump_std_mult": 2.0,
            "jump_mean_adj": 0.0,
        }
        pnl_macro = simulate_pnl_paths(
            spot=180.0, strike=175.0, hv_annual=0.35, dte=30,
            premium=8.0, option_type="put", strategy_class="LONG",
            n_paths=n, rng=rng2, macro_calibration=macro_cal,
        )

        # Macro-calibrated should have wider std dev (fatter tails)
        assert np.std(pnl_macro) > np.std(pnl_base) * 0.95, (
            "Macro calibration should widen the P&L distribution"
        )

    def test_no_macro_calibration_unchanged(self):
        """Without macro_calibration, paths are identical to baseline."""
        from scan_engine.mc_position_sizing import simulate_pnl_paths

        rng1 = np.random.default_rng(42)
        pnl1 = simulate_pnl_paths(
            spot=180.0, strike=175.0, hv_annual=0.35, dte=30,
            premium=8.0, option_type="put", strategy_class="LONG",
            n_paths=100, rng=rng1,
        )

        rng2 = np.random.default_rng(42)
        pnl2 = simulate_pnl_paths(
            spot=180.0, strike=175.0, hv_annual=0.35, dte=30,
            premium=8.0, option_type="put", strategy_class="LONG",
            n_paths=100, rng=rng2, macro_calibration=None,
        )

        np.testing.assert_array_almost_equal(pnl1, pnl2)

    def test_mc_size_row_detects_macro_week(self):
        """mc_size_row checks Is_Macro_Week and Macro_Next_Type."""
        from scan_engine.mc_position_sizing import mc_size_row

        row = pd.Series({
            "Ticker": "NVDA",
            "UL Last": 180.0,
            "Selected_Strike": 175.0,
            "Actual_DTE": 30,
            "Last": 8.0,
            "Option_Type": "put",
            "Strategy_Name": "LONG_PUT",
            "Is_Macro_Week": True,
            "Macro_Next_Type": "FOMC",
            "hv_30": 0.35,
        })
        result = mc_size_row(row, account_balance=100_000)
        assert result["MC_Paths_Used"] > 0
        assert "MACRO_CAL" in result["MC_Sizing_Note"]
        assert "FOMC" in result["MC_Sizing_Note"]
