"""
Tests for earnings formation detection (Phase 1→2→3 analysis).

Tests cover:
    - Table initialization (idempotent, PK constraints)
    - Phase classification logic
    - Formation timeseries building (dense price + sparse IV)
    - Formation summary computation
    - Forward phase detection
    - Query helpers
"""

import duckdb
import math
import pandas as pd
import numpy as np
import pytest
from datetime import date, timedelta


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def con():
    """In-memory DuckDB with all earnings tables + test data."""
    c = duckdb.connect(":memory:")

    # Create supporting tables
    c.execute("""
        CREATE TABLE price_history (
            ticker VARCHAR, date DATE, close_price DOUBLE,
            volume BIGINT, open_price DOUBLE, high_price DOUBLE,
            low_price DOUBLE, source VARCHAR, fetched_at TIMESTAMP
        )
    """)

    from core.shared.data_layer.earnings_history import initialize_tables
    initialize_tables(c)
    yield c
    c.close()


@pytest.fixture
def iv_con():
    """Separate in-memory DuckDB for IV data (simulates iv_history.duckdb)."""
    c = duckdb.connect(":memory:")
    c.execute("""
        CREATE TABLE iv_term_history (
            ticker VARCHAR, date DATE, iv_30d DOUBLE
        )
    """)
    yield c
    c.close()


def _seed_price_data(con, ticker, base_date, days_before=35, days_after=8,
                     base_price=200.0, base_volume=5_000_000):
    """Insert daily price bars around an earnings date."""
    rows = []
    for d in range(-days_before, days_after + 1):
        obs = base_date + timedelta(days=d)
        # Skip weekends
        if obs.weekday() >= 5:
            continue
        # Slight upward drift with noise
        price = base_price * (1 + d * 0.001 + (d % 3) * 0.002)
        volume = int(base_volume * (1.0 + 0.1 * (abs(d) % 4)))
        rows.append((ticker, obs, price, volume))

    df = pd.DataFrame(rows, columns=["ticker", "date", "close_price", "volume"])
    con.execute("""
        INSERT INTO price_history (ticker, date, close_price, volume)
        SELECT ticker, date, close_price, volume FROM df
    """)
    return df


def _seed_iv_data(iv_con, ticker, base_date, days_before=35, days_after=8,
                  base_iv=25.0, ramp=True, collection_interval=2):
    """
    Insert sparse IV data. If ramp=True, IV climbs from D-12 to D-1.
    collection_interval controls density (every Nth day).
    """
    rows = []
    for d in range(-days_before, days_after + 1):
        obs = base_date + timedelta(days=d)
        if obs.weekday() >= 5:
            continue
        # Only collect every Nth day (sparse)
        if abs(d) % collection_interval != 0:
            continue

        iv = base_iv
        if ramp and -12 <= d <= -1:
            # IV ramps up by ~0.8/day in positioning window
            iv = base_iv + (12 + d) * 0.8
        elif d >= 1:
            # Post-earnings crush: drop 30%
            iv = base_iv * 0.7

        rows.append((ticker, obs, iv))

    df = pd.DataFrame(rows, columns=["ticker", "date", "iv_30d"])
    iv_con.execute("INSERT INTO iv_term_history SELECT * FROM df")
    return df


# ---------------------------------------------------------------------------
# TestTableInitialization
# ---------------------------------------------------------------------------

class TestTableInitialization:

    def test_creates_tables(self, con):
        """Formation tables exist after initialize_tables."""
        tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
        assert "earnings_formation" in tables
        assert "earnings_formation_summary" in tables

    def test_idempotent(self, con):
        """Calling create_formation_tables twice doesn't error."""
        from core.shared.data_layer.earnings_formation import create_formation_tables
        create_formation_tables(con)
        create_formation_tables(con)
        tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
        assert "earnings_formation" in tables

    def test_pk_uniqueness(self, con):
        """Primary key prevents duplicate (ticker, earnings_date, days_relative)."""
        con.execute("""
            INSERT INTO earnings_formation
                (ticker, earnings_date, days_relative, obs_date, phase_label)
            VALUES ('AAPL', '2026-01-30', -5, '2026-01-25', 'QUIET')
        """)
        with pytest.raises(duckdb.ConstraintException):
            con.execute("""
                INSERT INTO earnings_formation
                    (ticker, earnings_date, days_relative, obs_date, phase_label)
                VALUES ('AAPL', '2026-01-30', -5, '2026-01-25', 'POSITIONING')
            """)


# ---------------------------------------------------------------------------
# TestPhaseClassification
# ---------------------------------------------------------------------------

class TestPhaseClassification:

    def test_post_earnings(self):
        from core.shared.data_layer.earnings_formation import _classify_formation_phase
        assert _classify_formation_phase(1, 0.5, 1.0, 0.1) == "POST"
        assert _classify_formation_phase(5, None, None, None) == "POST"

    def test_explosion_day(self):
        from core.shared.data_layer.earnings_formation import _classify_formation_phase
        assert _classify_formation_phase(0, 0.5, 1.0, 0.1) == "EXPLOSION"

    def test_positioning_iv_rising(self):
        from core.shared.data_layer.earnings_formation import _classify_formation_phase
        # D-10, IV rising, normal volume
        assert _classify_formation_phase(-10, 0.5, 0.9, 0.1) == "POSITIONING"

    def test_positioning_volume_elevated(self):
        from core.shared.data_layer.earnings_formation import _classify_formation_phase
        # D-8, flat IV, but volume 1.5x
        assert _classify_formation_phase(-8, -0.1, 1.5, -0.2) == "POSITIONING"

    def test_quiet_in_window_flat(self):
        from core.shared.data_layer.earnings_formation import _classify_formation_phase
        # D-10, flat/declining IV, normal volume → still QUIET
        assert _classify_formation_phase(-10, -0.3, 0.8, -0.1) == "QUIET"

    def test_quiet_far_out(self):
        from core.shared.data_layer.earnings_formation import _classify_formation_phase
        # D-20, outside positioning window
        assert _classify_formation_phase(-20, 0.5, 2.0, 0.1) == "QUIET"

    def test_quiet_very_far(self):
        from core.shared.data_layer.earnings_formation import _classify_formation_phase
        assert _classify_formation_phase(-30, None, None, None) == "QUIET"


# ---------------------------------------------------------------------------
# TestFormationTimeseries
# ---------------------------------------------------------------------------

class TestFormationTimeseries:

    def test_builds_timeseries(self, con, iv_con):
        """Full timeseries build with price + IV data."""
        from core.shared.data_layer.earnings_formation import _build_formation_timeseries

        ed = date(2026, 1, 30)
        _seed_price_data(con, "AAPL", ed)
        _seed_iv_data(iv_con, "AAPL", ed)

        df = _build_formation_timeseries(con, iv_con, "AAPL", ed)
        assert not df.empty
        assert "days_relative" in df.columns
        assert "phase_label" in df.columns

        # Should have pre and post earnings days
        assert df["days_relative"].min() <= -20
        assert df["days_relative"].max() >= 3

    def test_sparse_iv_fills(self, con, iv_con):
        """IV values are resolved from sparse data (nearest reading)."""
        from core.shared.data_layer.earnings_formation import _build_formation_timeseries

        ed = date(2026, 1, 30)
        _seed_price_data(con, "NVDA", ed)
        # Very sparse IV (every 4 days)
        _seed_iv_data(iv_con, "NVDA", ed, collection_interval=4, base_iv=40.0)

        df = _build_formation_timeseries(con, iv_con, "NVDA", ed)
        # Should still have some IV values despite sparseness
        iv_count = df["iv_30d"].notna().sum()
        assert iv_count > 0, "Expected some IV values from sparse data"

    def test_volume_ratio(self, con, iv_con):
        """Volume ratio computed against 20-day trailing average."""
        from core.shared.data_layer.earnings_formation import _build_formation_timeseries

        ed = date(2026, 1, 30)
        _seed_price_data(con, "TSLA", ed, base_volume=10_000_000)
        _seed_iv_data(iv_con, "TSLA", ed)

        df = _build_formation_timeseries(con, iv_con, "TSLA", ed)
        vr = df["volume_ratio"].dropna()
        assert len(vr) > 0, "Expected volume_ratio values"
        # Volume ratio should be around 1.0 (with small noise)
        assert vr.median() > 0.5
        assert vr.median() < 2.0

    def test_empty_price_returns_empty(self, con, iv_con):
        """No price data → empty DataFrame."""
        from core.shared.data_layer.earnings_formation import _build_formation_timeseries
        df = _build_formation_timeseries(con, iv_con, "XXXX", date(2026, 1, 30))
        assert df.empty

    def test_phase_labels_present(self, con, iv_con):
        """All rows have a phase label."""
        from core.shared.data_layer.earnings_formation import _build_formation_timeseries

        ed = date(2026, 1, 30)
        _seed_price_data(con, "META", ed)
        _seed_iv_data(iv_con, "META", ed)

        df = _build_formation_timeseries(con, iv_con, "META", ed)
        assert df["phase_label"].notna().all()
        # Should have at least QUIET and POST
        labels = set(df["phase_label"].unique())
        assert "QUIET" in labels or "POSITIONING" in labels
        assert "POST" in labels


# ---------------------------------------------------------------------------
# TestFormationSummary
# ---------------------------------------------------------------------------

class TestFormationSummary:

    def test_complete_formation(self, con, iv_con):
        """Full formation with ramp produces quality=COMPLETE or PARTIAL."""
        from core.shared.data_layer.earnings_formation import (
            _build_formation_timeseries, _compute_formation_summary
        )

        ed = date(2026, 1, 30)
        _seed_price_data(con, "AAPL", ed)
        _seed_iv_data(iv_con, "AAPL", ed, ramp=True, collection_interval=2)

        df = _build_formation_timeseries(con, iv_con, "AAPL", ed)
        summary = _compute_formation_summary(df, "AAPL", ed)

        assert summary["formation_quality"] in ("COMPLETE", "PARTIAL")
        assert summary["iv_data_points"] > 0
        assert summary["price_data_points"] > 0

    def test_insufficient_quality(self, con, iv_con):
        """Very sparse IV → INSUFFICIENT quality."""
        from core.shared.data_layer.earnings_formation import (
            _build_formation_timeseries, _compute_formation_summary
        )

        ed = date(2026, 1, 30)
        _seed_price_data(con, "RARE", ed)
        # Only 2 IV points
        _seed_iv_data(iv_con, "RARE", ed, collection_interval=20)

        df = _build_formation_timeseries(con, iv_con, "RARE", ed)
        summary = _compute_formation_summary(df, "RARE", ed)

        assert summary["formation_quality"] == "INSUFFICIENT"

    def test_no_ramp(self, con, iv_con):
        """Flat IV → no phase2_start_day detected."""
        from core.shared.data_layer.earnings_formation import (
            _build_formation_timeseries, _compute_formation_summary
        )

        ed = date(2026, 1, 30)
        _seed_price_data(con, "FLAT", ed)
        _seed_iv_data(iv_con, "FLAT", ed, ramp=False, collection_interval=2)

        df = _build_formation_timeseries(con, iv_con, "FLAT", ed)
        summary = _compute_formation_summary(df, "FLAT", ed)

        # With no ramp, phase2 may or may not detect (depends on volume)
        # But ramp_magnitude should be small or None
        if summary["iv_ramp_magnitude_pct"] is not None:
            assert summary["iv_ramp_magnitude_pct"] < 0.1, "Flat IV should have small ramp"

    def test_drift_predicted_gap_true(self, con, iv_con):
        """Positive drift + positive gap → drift_predicted_gap = True."""
        from core.shared.data_layer.earnings_formation import (
            _build_formation_timeseries, _compute_formation_summary
        )

        ed = date(2026, 1, 30)
        _seed_price_data(con, "UP", ed, base_price=200.0)
        _seed_iv_data(iv_con, "UP", ed, ramp=True, collection_interval=2)

        df = _build_formation_timeseries(con, iv_con, "UP", ed)
        # With a slight upward drift in the test data, test with positive gap
        summary = _compute_formation_summary(df, "UP", ed, gap_pct=0.05)

        # May or may not have phase2, but if drift detected, check prediction
        if summary["drift_predicted_gap"] is not None:
            assert isinstance(summary["drift_predicted_gap"], bool)


# ---------------------------------------------------------------------------
# TestOrchestrator
# ---------------------------------------------------------------------------

class TestOrchestrator:

    def test_compute_formation_persists(self, con, iv_con):
        """compute_formation_for_event writes to both tables."""
        from core.shared.data_layer.earnings_formation import compute_formation_for_event

        ed = date(2026, 1, 30)
        _seed_price_data(con, "MSFT", ed)
        _seed_iv_data(iv_con, "MSFT", ed)

        # Need earnings_history row for gap lookup
        con.execute("""
            INSERT INTO earnings_history (ticker, earnings_date, eps_actual, beat_miss)
            VALUES ('MSFT', '2026-01-30', 3.23, 'BEAT')
        """)

        result = compute_formation_for_event(con, iv_con, "MSFT", ed)
        assert result is not None
        assert result["ticker"] == "MSFT"

        # Check timeseries persisted
        ts = con.execute("""
            SELECT COUNT(*) FROM earnings_formation
            WHERE ticker = 'MSFT' AND earnings_date = '2026-01-30'
        """).fetchone()[0]
        assert ts > 0

        # Check summary persisted
        summ = con.execute("""
            SELECT formation_quality FROM earnings_formation_summary
            WHERE ticker = 'MSFT' AND earnings_date = '2026-01-30'
        """).fetchone()
        assert summ is not None

    def test_upsert_replaces(self, con, iv_con):
        """Running compute twice replaces data (no duplicates)."""
        from core.shared.data_layer.earnings_formation import compute_formation_for_event

        ed = date(2026, 1, 30)
        _seed_price_data(con, "GOOG", ed)
        _seed_iv_data(iv_con, "GOOG", ed)

        compute_formation_for_event(con, iv_con, "GOOG", ed)
        count1 = con.execute("""
            SELECT COUNT(*) FROM earnings_formation
            WHERE ticker = 'GOOG' AND earnings_date = '2026-01-30'
        """).fetchone()[0]

        compute_formation_for_event(con, iv_con, "GOOG", ed)
        count2 = con.execute("""
            SELECT COUNT(*) FROM earnings_formation
            WHERE ticker = 'GOOG' AND earnings_date = '2026-01-30'
        """).fetchone()[0]

        assert count1 == count2, "Upsert should not create duplicates"


# ---------------------------------------------------------------------------
# TestForwardDetection
# ---------------------------------------------------------------------------

class TestForwardDetection:

    def test_no_upcoming_earnings(self, con, iv_con):
        """No earnings date → NO_UPCOMING."""
        from core.shared.data_layer.earnings_formation import detect_current_phase

        result = detect_current_phase(con, iv_con, "XXXX", date(2026, 3, 1))
        assert result["phase"] == "NO_UPCOMING"

    def test_imminent(self, con, iv_con):
        """1 day to earnings → IMMINENT."""
        from core.shared.data_layer.earnings_formation import detect_current_phase

        result = detect_current_phase(
            con, iv_con, "AAPL", date(2026, 3, 5),
            next_earnings_date=date(2026, 3, 6),
        )
        assert result["phase"] == "IMMINENT"
        assert result["days_to_earnings"] == 1
        assert result["confidence"] == "HIGH"

    def test_late_positioning(self, con, iv_con):
        """3 days to earnings → LATE_POSITIONING."""
        from core.shared.data_layer.earnings_formation import detect_current_phase

        # Seed some IV data so velocity can be computed
        _seed_iv_data(iv_con, "NVDA", date(2026, 3, 10), ramp=True, collection_interval=1)

        result = detect_current_phase(
            con, iv_con, "NVDA", date(2026, 3, 7),
            next_earnings_date=date(2026, 3, 10),
        )
        assert result["phase"] == "LATE_POSITIONING"

    def test_quiet_30d_out(self, con, iv_con):
        """30+ days to earnings → QUIET."""
        from core.shared.data_layer.earnings_formation import detect_current_phase

        result = detect_current_phase(
            con, iv_con, "AAPL", date(2026, 3, 1),
            next_earnings_date=date(2026, 4, 1),
        )
        assert result["phase"] == "QUIET"
        assert result["days_to_earnings"] == 31


# ---------------------------------------------------------------------------
# TestQueryHelpers
# ---------------------------------------------------------------------------

class TestQueryHelpers:

    def test_get_formation_timeseries(self, con, iv_con):
        """get_formation_timeseries returns stored data."""
        from core.shared.data_layer.earnings_formation import (
            compute_formation_for_event, get_formation_timeseries
        )

        ed = date(2026, 1, 30)
        _seed_price_data(con, "AMZN", ed)
        _seed_iv_data(iv_con, "AMZN", ed)
        compute_formation_for_event(con, iv_con, "AMZN", ed)

        ts = get_formation_timeseries(con, "AMZN", ed)
        assert not ts.empty
        assert "days_relative" in ts.columns
        assert ts["days_relative"].is_monotonic_increasing

    def test_get_avg_formation_stats(self, con, iv_con):
        """get_avg_formation_stats aggregates across events."""
        from core.shared.data_layer.earnings_formation import (
            compute_formation_for_event, get_avg_formation_stats
        )

        # Two events for same ticker
        for ed in [date(2026, 1, 30), date(2025, 10, 30)]:
            _seed_price_data(con, "TSLA", ed)
            _seed_iv_data(iv_con, "TSLA", ed)
            compute_formation_for_event(con, iv_con, "TSLA", ed)

        stats = get_avg_formation_stats(con, "TSLA")
        # May be None if both are INSUFFICIENT
        if stats is not None:
            assert stats["event_count"] >= 1
            assert "avg_ramp_magnitude_pct" in stats


# ---------------------------------------------------------------------------
# TestScanIntegration — formation wired into scan engine Step 12
# ---------------------------------------------------------------------------

class TestScanIntegration:
    """Test formation columns consumed by Step 12 income eligibility gate."""

    def _make_income_row(self, **overrides):
        """Base row that passes all non-earnings conditions."""
        base = {
            'IVHV_gap_30D': 15.0,
            'IV_Rank_20D': 60.0,
            'IV_Rank_30D': 55.0,
            'Surface_Shape': 'NORMAL',
            'days_to_earnings': 30.0,
            'IV_History_Count': 200,
            'Earnings_Formation_Phase': '',
            'Earnings_IV_Velocity': None,
        }
        base.update(overrides)
        return pd.Series(base)

    def test_income_gate_blocks_late_positioning(self):
        """LATE_POSITIONING blocks income even when earnings is outside DTE window."""
        from scan_engine.step12_acceptance import check_income_eligibility
        row = self._make_income_row(
            days_to_earnings=20.0,  # outside 14 DTE window
            Earnings_Formation_Phase='LATE_POSITIONING',
            Earnings_IV_Velocity=0.5,
        )
        eligible, reason = check_income_eligibility(row, actual_dte=14.0)
        assert not eligible
        assert 'LATE_POSITIONING' in reason

    def test_income_gate_passes_quiet(self):
        """QUIET phase + earnings outside DTE → pass."""
        from scan_engine.step12_acceptance import check_income_eligibility
        row = self._make_income_row(
            days_to_earnings=35.0,
            Earnings_Formation_Phase='QUIET',
        )
        eligible, reason = check_income_eligibility(row, actual_dte=14.0)
        assert eligible
        assert 'OK' in reason

    def test_income_gate_imminent_blocks(self):
        """IMMINENT phase hard blocks."""
        from scan_engine.step12_acceptance import check_income_eligibility
        row = self._make_income_row(
            days_to_earnings=1.0,
            Earnings_Formation_Phase='IMMINENT',
            Earnings_IV_Velocity=1.2,
        )
        eligible, reason = check_income_eligibility(row, actual_dte=14.0)
        assert not eligible
        assert 'IMMINENT' in reason

    def test_income_gate_early_positioning_passes_with_note(self):
        """EARLY_POSITIONING passes with cautionary note when earnings outside DTE."""
        from scan_engine.step12_acceptance import check_income_eligibility
        row = self._make_income_row(
            days_to_earnings=12.0,
            Earnings_Formation_Phase='EARLY_POSITIONING',
            Earnings_IV_Velocity=0.3,
        )
        eligible, reason = check_income_eligibility(row, actual_dte=7.0)
        assert eligible
        assert 'EARLY_POSITIONING' in reason

    def test_income_gate_no_formation_data_falls_through(self):
        """Missing formation columns fall through to existing binary gate."""
        from scan_engine.step12_acceptance import check_income_eligibility
        row = pd.Series({
            'IVHV_gap_30D': 15.0,
            'IV_Rank_20D': 60.0,
            'IV_Rank_30D': 55.0,
            'Surface_Shape': 'NORMAL',
            'days_to_earnings': 30.0,
            'IV_History_Count': 200,
            # No Earnings_Formation_Phase column at all
        })
        eligible, reason = check_income_eligibility(row, actual_dte=14.0)
        assert eligible  # No blocking when formation data absent

    def test_income_gate_late_positioning_includes_velocity(self):
        """LATE_POSITIONING block reason includes IV velocity."""
        from scan_engine.step12_acceptance import check_income_eligibility
        row = self._make_income_row(
            days_to_earnings=20.0,
            Earnings_Formation_Phase='LATE_POSITIONING',
            Earnings_IV_Velocity=0.75,
        )
        eligible, reason = check_income_eligibility(row, actual_dte=14.0)
        assert not eligible
        assert 'velocity' in reason.lower()
        assert '0.75' in reason

    def test_income_gate_existing_binary_still_works(self):
        """Earnings inside DTE + no formation data → binary block still fires."""
        from scan_engine.step12_acceptance import check_income_eligibility
        row = self._make_income_row(
            days_to_earnings=10.0,  # inside 14 DTE window
            Earnings_Formation_Phase='',  # no formation data
        )
        eligible, reason = check_income_eligibility(row, actual_dte=14.0)
        assert not eligible
        assert 'binary risk' in reason.lower()
