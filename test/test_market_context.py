"""
Market Context Data Layer Tests
================================
Validates Phase 1: market_context.py CRUD, trading_calendar.py, and
market_context_collector.py (with mocked yfinance).

Tests:
  1.  Table creation (idempotent)
  2.  Write single day
  3.  Duplicate guard (same day returns True)
  4.  Overwrite via INSERT OR REPLACE
  5.  get_latest_market_context returns dict
  6.  staleness_bdays computation
  7.  query_vix_history returns Series
  8.  query_market_context lookback
  9.  Empty table → None / empty
  10. regime_detail_json round-trip (dict → JSON → parsed)
  11. is_trading_day: weekday, weekend, holiday
  12. business_days_between: same day, weekend span, holiday span
  13. VIX percentile: extremes (0th, 50th, 100th)
  14. Collector end-to-end with mocked yfinance
  15. Source robustness: ^VXV fails → vix_3m=None, collector succeeds
  16. Collector holiday guard
  17. Collector duplicate guard
  18. Universe breadth with synthetic data
  19. _compute_vix_percentile edge cases
  20. write_market_context with dict regime_detail_json

Run:
    pytest test/test_market_context.py -v
"""

from __future__ import annotations

import json
import math
import sys
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def market_db(tmp_path, monkeypatch):
    """Create an isolated DuckDB for MARKET domain tests."""
    import duckdb
    db_path = tmp_path / "market.duckdb"

    # Patch the domain path so all MARKET connections use our temp DB
    monkeypatch.setattr(
        "core.shared.data_layer.duckdb_utils._DOMAIN_PATHS",
        {
            **_get_original_domain_paths(),
            _get_market_domain(): db_path,
        },
    )
    return db_path


def _get_original_domain_paths():
    from core.shared.data_layer.duckdb_utils import _DOMAIN_PATHS
    return dict(_DOMAIN_PATHS)


def _get_market_domain():
    from core.shared.data_layer.duckdb_utils import DbDomain
    return DbDomain.MARKET


def _sample_context(d: date | None = None, vix: float = 22.5) -> dict:
    """Build a minimal valid market context dict."""
    return {
        "date": d or date.today(),
        "vix": vix,
        "vix_3m": 20.0,
        "vvix": 115.0,
        "skew": 130.0,
        "vix_term_spread": -2.5,
        "vix_term_ratio": 1.125,
        "credit_spread_proxy": 0.95,
        "hyg_price": 78.5,
        "lqd_price": 82.6,
        "universe_breadth_pct_sma50": 55.0,
        "universe_breadth_advancing_5d": 52.0,
        "avg_correlation": 0.35,
        "vix_percentile_252d": 65.0,
        "vix_sma_20": 21.0,
        "market_regime": "NORMAL",
        "regime_score": 28.5,
        "regime_confidence": 0.85,
        "regime_basis": "COMPOSITE",
        "source": "yfinance",
    }


# =============================================================================
# 1. Trading Calendar Tests
# =============================================================================

class TestTradingCalendar:
    """Validate shared trading calendar module."""

    def test_weekday_is_trading_day(self):
        from core.shared.calendar.trading_calendar import is_trading_day
        # 2026-03-09 is Monday
        assert is_trading_day(date(2026, 3, 9)) is True

    def test_weekend_not_trading_day(self):
        from core.shared.calendar.trading_calendar import is_trading_day
        # Saturday
        assert is_trading_day(date(2026, 3, 7)) is False
        # Sunday
        assert is_trading_day(date(2026, 3, 8)) is False

    def test_holiday_not_trading_day(self):
        from core.shared.calendar.trading_calendar import is_trading_day
        # 2026-01-19 MLK Day
        assert is_trading_day(date(2026, 1, 19)) is False

    def test_business_days_same_day(self):
        from core.shared.calendar.trading_calendar import business_days_between
        d = date(2026, 3, 9)
        assert business_days_between(d, d) == 0

    def test_business_days_one_weekday(self):
        from core.shared.calendar.trading_calendar import business_days_between
        # Mon to Tue
        assert business_days_between(date(2026, 3, 9), date(2026, 3, 10)) == 1

    def test_business_days_over_weekend(self):
        from core.shared.calendar.trading_calendar import business_days_between
        # Friday to Monday = 1 business day
        assert business_days_between(date(2026, 3, 6), date(2026, 3, 9)) == 1

    def test_business_days_over_holiday(self):
        from core.shared.calendar.trading_calendar import business_days_between
        # Thu Jan 15 to Tue Jan 20 (Jan 19 = MLK holiday)
        # Jan 16 (Fri) = 1 bday, Jan 19 (Mon, holiday) = skip, Jan 20 (Tue) = 2 bdays
        assert business_days_between(date(2026, 1, 15), date(2026, 1, 20)) == 2

    def test_business_days_reversed(self):
        from core.shared.calendar.trading_calendar import business_days_between
        # d1 > d2 → 0
        assert business_days_between(date(2026, 3, 10), date(2026, 3, 9)) == 0


# =============================================================================
# 2. Schema & Table Tests
# =============================================================================

class TestMarketContextSchema:
    """Validate table creation and schema."""

    def test_table_creation_idempotent(self, market_db):
        from core.shared.data_layer.market_context import initialize_market_context_table
        # Call twice — should not raise
        initialize_market_context_table()
        initialize_market_context_table()

    def test_write_creates_table_on_demand(self, market_db):
        """write_market_context auto-initializes the table."""
        from core.shared.data_layer.market_context import write_market_context
        data = _sample_context(date(2026, 3, 1))
        write_market_context(data, d=date(2026, 3, 1))


# =============================================================================
# 3. Write & Read Tests
# =============================================================================

class TestMarketContextCRUD:
    """Validate write, read, duplicate guard."""

    def test_write_and_read_latest(self, market_db):
        from core.shared.data_layer.market_context import (
            write_market_context, get_latest_market_context,
        )
        data = _sample_context(date(2026, 3, 2), vix=25.0)
        write_market_context(data, d=date(2026, 3, 2))

        latest = get_latest_market_context()
        assert latest is not None
        assert latest["vix"] == pytest.approx(25.0)
        assert latest["market_regime"] == "NORMAL"

    def test_duplicate_guard(self, market_db):
        from core.shared.data_layer.market_context import (
            write_market_context, market_context_collected_today,
        )
        d = date(2026, 3, 3)
        assert market_context_collected_today(d) is False
        write_market_context(_sample_context(d), d=d)
        assert market_context_collected_today(d) is True

    def test_overwrite_same_day(self, market_db):
        from core.shared.data_layer.market_context import (
            write_market_context, get_latest_market_context,
        )
        d = date(2026, 3, 4)
        write_market_context(_sample_context(d, vix=20.0), d=d)
        write_market_context(_sample_context(d, vix=30.0), d=d)

        latest = get_latest_market_context()
        assert latest["vix"] == pytest.approx(30.0)

    def test_latest_returns_most_recent(self, market_db):
        from core.shared.data_layer.market_context import (
            write_market_context, get_latest_market_context,
        )
        write_market_context(_sample_context(date(2026, 3, 1), vix=18.0), d=date(2026, 3, 1))
        write_market_context(_sample_context(date(2026, 3, 5), vix=28.0), d=date(2026, 3, 5))

        latest = get_latest_market_context()
        assert latest["vix"] == pytest.approx(28.0)

    def test_empty_table_returns_none(self, market_db):
        from core.shared.data_layer.market_context import (
            initialize_market_context_table, get_latest_market_context,
        )
        initialize_market_context_table()
        assert get_latest_market_context() is None

    def test_regime_detail_json_round_trip(self, market_db):
        """Dict regime_detail_json is serialized to JSON and parsed back."""
        from core.shared.data_layer.market_context import (
            write_market_context, get_latest_market_context,
        )
        d = date(2026, 3, 6)
        data = _sample_context(d)
        data["regime_detail_json"] = {
            "components": {"vix": {"value": 22.5, "subscore": 35}},
            "missing": [],
            "regime": "NORMAL",
        }
        write_market_context(data, d=d)

        latest = get_latest_market_context()
        assert latest is not None
        detail = latest.get("regime_detail")
        assert detail is not None
        assert detail["regime"] == "NORMAL"
        assert "vix" in detail["components"]


# =============================================================================
# 4. Staleness Tests
# =============================================================================

class TestStaleness:
    """Validate staleness_bdays in get_latest_market_context."""

    def test_staleness_today_is_zero(self, market_db):
        from core.shared.data_layer.market_context import (
            write_market_context, get_latest_market_context,
        )
        today = date.today()
        write_market_context(_sample_context(today), d=today)
        latest = get_latest_market_context()
        assert latest["staleness_bdays"] == 0

    def test_staleness_yesterday_weekday(self, market_db):
        from core.shared.data_layer.market_context import (
            write_market_context, get_latest_market_context,
        )
        from core.shared.calendar.trading_calendar import business_days_between
        # Write for a date 3 calendar days ago
        d = date.today() - timedelta(days=3)
        write_market_context(_sample_context(d), d=d)
        latest = get_latest_market_context()
        expected = business_days_between(d, date.today())
        assert latest["staleness_bdays"] == expected


# =============================================================================
# 5. VIX History & Percentile Tests
# =============================================================================

class TestVIXHistory:
    """Validate query_vix_history and percentile computation."""

    def test_vix_history_returns_series(self, market_db):
        from core.shared.data_layer.market_context import (
            write_market_context, query_vix_history,
        )
        # Write 15 days of VIX data
        for i in range(15):
            d = date(2026, 2, 1) + timedelta(days=i)
            write_market_context({"date": d, "vix": 20.0 + i * 0.5}, d=d)

        history = query_vix_history(days=15)
        assert len(history) == 15
        assert isinstance(history, pd.Series)

    def test_vix_history_empty_table(self, market_db):
        from core.shared.data_layer.market_context import (
            initialize_market_context_table, query_vix_history,
        )
        initialize_market_context_table()
        history = query_vix_history(days=10)
        assert len(history) == 0

    def test_vix_percentile_extremes(self, market_db):
        """VIX at lowest → 0th percentile, at highest → near 100th."""
        from core.shared.data_layer.market_context import write_market_context
        from core.shared.data_layer.market_context_collector import _compute_vix_percentile

        # Write 50 days: VIX from 15 to 64
        for i in range(50):
            d = date(2026, 1, 1) + timedelta(days=i)
            write_market_context({"date": d, "vix": 15.0 + i}, d=d)

        # Lowest → 0%
        pctl_low = _compute_vix_percentile(14.0)
        assert pctl_low == pytest.approx(0.0)

        # Highest → near 100%
        pctl_high = _compute_vix_percentile(100.0)
        assert pctl_high == pytest.approx(100.0)

        # Mid-range
        pctl_mid = _compute_vix_percentile(40.0)
        assert 40 < pctl_mid < 60

    def test_vix_percentile_insufficient_history(self, market_db, monkeypatch):
        """Fewer than 10 stored rows + no yfinance bootstrap → None."""
        from core.shared.data_layer.market_context import (
            initialize_market_context_table, write_market_context,
        )
        import core.shared.data_layer.market_context_collector as _mc_mod
        from core.shared.data_layer.market_context_collector import _compute_vix_percentile

        initialize_market_context_table()
        for i in range(5):
            d = date(2026, 1, 1) + timedelta(days=i)
            write_market_context({"date": d, "vix": 20.0 + i}, d=d)

        # Block yfinance bootstrap so we test the "truly insufficient" path
        import importlib
        _orig_import = __builtins__.__import__ if hasattr(__builtins__, '__import__') else __import__
        def _block_yfinance(name, *args, **kwargs):
            if name == "yfinance":
                raise ImportError("blocked for test")
            return _orig_import(name, *args, **kwargs)
        monkeypatch.setattr("builtins.__import__", _block_yfinance)

        result = _compute_vix_percentile(22.0)
        assert result is None


# =============================================================================
# 6. Query Tests
# =============================================================================

class TestQueryMarketContext:
    """Validate query_market_context lookback."""

    def test_lookback(self, market_db):
        from core.shared.data_layer.market_context import (
            write_market_context, query_market_context,
        )
        for i in range(5):
            d = date(2026, 3, 1) + timedelta(days=i)
            write_market_context(_sample_context(d, vix=20.0 + i), d=d)

        df = query_market_context(d=date(2026, 3, 5), lookback_days=3)
        assert len(df) == 3

    def test_lookback_empty(self, market_db):
        from core.shared.data_layer.market_context import (
            initialize_market_context_table, query_market_context,
        )
        initialize_market_context_table()
        df = query_market_context(d=date(2026, 3, 1), lookback_days=5)
        assert len(df) == 0


# =============================================================================
# 7. Collector Tests (mocked yfinance)
# =============================================================================

def _mock_yf_download(*args, **kwargs):
    """Return synthetic yfinance batch download result."""
    dates = pd.date_range("2026-03-06", periods=5, freq="B")
    symbols = ["^VIX", "^VXV", "^VVIX", "^SKEW", "HYG", "LQD"]
    data = {}
    values = {
        "^VIX": [22.0, 22.5, 23.0, 22.8, 23.5],
        "^VXV": [21.0, 21.2, 21.5, 21.3, 21.8],
        "^VVIX": [110, 112, 115, 113, 118],
        "^SKEW": [128, 130, 132, 131, 133],
        "HYG": [78.0, 78.2, 78.5, 78.3, 78.6],
        "LQD": [82.0, 82.1, 82.3, 82.2, 82.4],
    }
    tuples = []
    for sym in symbols:
        for col in ["Open", "High", "Low", "Close", "Volume"]:
            tuples.append((sym, col))

    idx = pd.MultiIndex.from_tuples(tuples)
    data_arr = []
    for sym in symbols:
        for col in ["Open", "High", "Low", "Close", "Volume"]:
            if col == "Close":
                data_arr.append(values[sym])
            elif col == "Volume":
                data_arr.append([1000000] * 5)
            else:
                data_arr.append(values[sym])

    import numpy as np
    df = pd.DataFrame(
        np.array(data_arr).T,
        index=dates,
        columns=idx,
    )
    return df


class TestCollector:
    """Validate collector end-to-end with mocked yfinance."""

    def test_collector_end_to_end(self, market_db):
        """Full collection with mocked yfinance produces valid output."""
        from core.shared.data_layer.market_context_collector import collect_market_context

        with patch("core.shared.data_layer.market_context_collector._fetch_index_data") as mock_fetch, \
             patch("core.shared.data_layer.market_context_collector._compute_universe_breadth") as mock_breadth, \
             patch("core.shared.data_layer.market_context_collector._compute_avg_correlation") as mock_corr, \
             patch("core.shared.data_layer.market_context_collector.is_trading_day", return_value=True), \
             patch("core.shared.data_layer.market_context_collector.market_context_collected_today", return_value=False):

            mock_fetch.return_value = {
                "vix": 23.5, "vix_3m": 21.8, "vvix": 118.0,
                "skew": 133.0, "hyg": 78.6, "lqd": 82.4,
            }
            mock_breadth.return_value = {
                "universe_breadth_pct_sma50": 55.0,
                "universe_breadth_advancing_5d": 52.0,
            }
            mock_corr.return_value = 0.32

            result = collect_market_context(force=False)
            assert result["ok"] is True
            assert result["data"] is not None
            assert result["data"]["vix"] == 23.5
            assert result["data"]["market_regime"] in (
                "RISK_ON", "NORMAL", "CAUTIOUS", "RISK_OFF", "CRISIS"
            )
            assert result["data"]["regime_basis"] == "COMPOSITE"

    def test_collector_holiday_guard(self, market_db):
        """Collector skips on non-trading day (no force)."""
        from core.shared.data_layer.market_context_collector import collect_market_context

        with patch("core.shared.data_layer.market_context_collector.is_trading_day", return_value=False):
            result = collect_market_context(force=False)
            assert result["ok"] is True
            assert result["data"] is None
            assert "skipping" in result["message"].lower()

    def test_collector_duplicate_guard(self, market_db):
        """Collector skips when already collected today (no force)."""
        from core.shared.data_layer.market_context_collector import collect_market_context

        with patch("core.shared.data_layer.market_context_collector.is_trading_day", return_value=True), \
             patch("core.shared.data_layer.market_context_collector.market_context_collected_today", return_value=True):
            result = collect_market_context(force=False)
            assert result["ok"] is True
            assert result["data"] is None
            assert "already" in result["message"].lower()

    def test_collector_force_bypasses_guards(self, market_db):
        """force=True bypasses both holiday and duplicate guards."""
        from core.shared.data_layer.market_context_collector import collect_market_context

        with patch("core.shared.data_layer.market_context_collector._fetch_index_data") as mock_fetch, \
             patch("core.shared.data_layer.market_context_collector._compute_universe_breadth") as mock_breadth, \
             patch("core.shared.data_layer.market_context_collector._compute_avg_correlation") as mock_corr:

            mock_fetch.return_value = {"vix": 20.0, "vix_3m": 19.0, "vvix": 100.0,
                                        "skew": 125.0, "hyg": 80.0, "lqd": 83.0}
            mock_breadth.return_value = {"universe_breadth_pct_sma50": 60.0,
                                         "universe_breadth_advancing_5d": 55.0}
            mock_corr.return_value = 0.25

            result = collect_market_context(force=True)
            assert result["ok"] is True
            assert result["data"] is not None

    def test_source_robustness_vxv_fails(self, market_db):
        """When ^VXV fails, vix_3m=None but collector still succeeds."""
        from core.shared.data_layer.market_context_collector import collect_market_context

        with patch("core.shared.data_layer.market_context_collector._fetch_index_data") as mock_fetch, \
             patch("core.shared.data_layer.market_context_collector._compute_universe_breadth") as mock_breadth, \
             patch("core.shared.data_layer.market_context_collector._compute_avg_correlation") as mock_corr, \
             patch("core.shared.data_layer.market_context_collector.is_trading_day", return_value=True), \
             patch("core.shared.data_layer.market_context_collector.market_context_collected_today", return_value=False):

            mock_fetch.return_value = {
                "vix": 25.0, "vix_3m": None, "vvix": 120.0,
                "skew": 135.0, "hyg": 78.0, "lqd": 82.0,
            }
            mock_breadth.return_value = {"universe_breadth_pct_sma50": 45.0,
                                         "universe_breadth_advancing_5d": 40.0}
            mock_corr.return_value = 0.38

            result = collect_market_context(force=False)
            assert result["ok"] is True
            assert result["data"]["vix_3m"] is None
            assert result["data"]["vix_term_ratio"] is None
            # Confidence should be reduced (missing term_structure component)
            assert result["data"]["regime_confidence"] < 1.0


# =============================================================================
# 8. Duplicate Guard Edge Cases
# =============================================================================

class TestDuplicateGuardEdgeCases:

    def test_duplicate_guard_no_table(self, market_db):
        """market_context_collected_today returns False when table doesn't exist yet."""
        from core.shared.data_layer.market_context import market_context_collected_today
        # Table not initialized — should return False (not raise)
        assert market_context_collected_today(date(2026, 3, 1)) is False

    def test_duplicate_guard_different_day(self, market_db):
        from core.shared.data_layer.market_context import (
            write_market_context, market_context_collected_today,
        )
        write_market_context(_sample_context(date(2026, 3, 1)), d=date(2026, 3, 1))
        assert market_context_collected_today(date(2026, 3, 2)) is False
