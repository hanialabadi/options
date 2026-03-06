"""
Tests for ETF-awareness in CC Opportunity Engine.

Covers:
  1. ETF detection (is_etf, is_commodity_etf, KNOWN_ETFS)
  2. ETF-aware arbitration messages (_cc_arbitration Gate 2 and Gate 4)
  3. ETF-aware favorability watch signals (_favorability_check)
  4. Is_ETF schema column
"""

from __future__ import annotations

import pandas as pd
import pytest


# ── ETF Detection ────────────────────────────────────────────────────────────


class TestETFDetection:
    """is_etf() function and KNOWN_ETFS frozenset."""

    def test_commodity_etfs_detected(self):
        from config.sector_benchmarks import is_etf
        assert is_etf("SLV") is True
        assert is_etf("GLD") is True
        assert is_etf("GDX") is True

    def test_sector_etfs_detected(self):
        from config.sector_benchmarks import is_etf
        assert is_etf("SPY") is True
        assert is_etf("QQQ") is True
        assert is_etf("XLE") is True
        assert is_etf("XLF") is True

    def test_single_stocks_not_detected(self):
        from config.sector_benchmarks import is_etf
        assert is_etf("AAPL") is False
        assert is_etf("TSLA") is False
        assert is_etf("NVDA") is False
        assert is_etf("SLB") is False

    def test_case_insensitive(self):
        from config.sector_benchmarks import is_etf
        assert is_etf("slv") is True
        assert is_etf("Spy") is True
        assert is_etf("qqq") is True

    def test_commodity_etf_subset(self):
        from config.sector_benchmarks import is_commodity_etf
        assert is_commodity_etf("SLV") is True
        assert is_commodity_etf("GLD") is True
        assert is_commodity_etf("GDX") is True
        assert is_commodity_etf("SPY") is False
        assert is_commodity_etf("QQQ") is False

    def test_known_etfs_frozenset_immutable(self):
        from config.sector_benchmarks import KNOWN_ETFS
        assert isinstance(KNOWN_ETFS, frozenset)
        assert len(KNOWN_ETFS) == 13


# ── ETF-Aware Arbitration ────────────────────────────────────────────────────


class TestETFAwareArbitration:
    """_cc_arbitration adds ETF-specific context without changing verdicts."""

    def _stock_row(self, **overrides):
        base = {
            "Strategy": "STOCK_ONLY",
            "AssetType": "STOCK",
            "Underlying_Ticker": "TEST",
            "Symbol": "TEST",
            "UL Last": 10.0,
            "Last": 10.0,
            "Net_Cost_Basis_Per_Share": 10.0,
            "Quantity": 100,
            "HV_20D": 1.08,
            "IV_30D": 0.83,
            "Thesis_State": "INTACT",
            "Price_Drift_Pct": -0.10,
            "TrendIntegrity_State": "",
            "MomentumVelocity_State": "",
            "roc_5": 0.0,
            "adx_14": 0.0,
            "HV_Daily_Move_1Sigma": 0.0,
            "iv_surface_shape": "",
            "IV_Entry": 0.0,
        }
        base.update(overrides)
        return pd.Series(base)

    def test_gate2_etf_context_in_reason(self):
        """Gate 2 HOLD_STOCK for ETF should contain ETF context."""
        from core.management.cycle3.cc_opportunity_engine import _cc_arbitration
        row = self._stock_row(Underlying_Ticker="SLV", Symbol="SLV")
        verdict, reason = _cc_arbitration(row, False, "")
        assert verdict == "HOLD_STOCK"
        assert "ETF context" in reason
        assert "mean-revert" in reason.lower()
        assert "no earnings" in reason.lower()

    def test_gate2_commodity_etf_extra_context(self):
        """Commodity ETFs get extra macro-flow vol note."""
        from core.management.cycle3.cc_opportunity_engine import _cc_arbitration
        row = self._stock_row(Underlying_Ticker="SLV", Symbol="SLV")
        verdict, reason = _cc_arbitration(row, False, "")
        assert "Commodity ETFs" in reason

    def test_gate2_stock_no_etf_context(self):
        """Gate 2 HOLD_STOCK for regular stock should NOT contain ETF context."""
        from core.management.cycle3.cc_opportunity_engine import _cc_arbitration
        row = self._stock_row(Underlying_Ticker="AAPL", Symbol="AAPL")
        verdict, reason = _cc_arbitration(row, False, "")
        assert verdict == "HOLD_STOCK"
        assert "ETF context" not in reason

    def test_gate2_verdict_unchanged_for_etf(self):
        """ETF context does NOT change the verdict — still HOLD_STOCK."""
        from core.management.cycle3.cc_opportunity_engine import _cc_arbitration
        row = self._stock_row(Underlying_Ticker="GLD", Symbol="GLD")
        verdict, _ = _cc_arbitration(row, False, "")
        assert verdict == "HOLD_STOCK"


# ── ETF-Aware Favorability ───────────────────────────────────────────────────


class TestETFAwareFavorability:
    """_favorability_check ETF-specific watch signal."""

    def test_etf_unfavorable_adds_etf_watch(self):
        from core.management.cycle3.cc_opportunity_engine import _favorability_check
        is_fav, reason, watch = _favorability_check(
            iv_rank=10.0, regime="Low Vol", signal="Neutral", ivhv_gap=0.5,
            recovery_mode="INCOME", ticker="SLV",
        )
        assert not is_fav
        assert "ETF" in watch
        assert "no earnings" in watch.lower()

    def test_stock_unfavorable_no_etf_watch(self):
        from core.management.cycle3.cc_opportunity_engine import _favorability_check
        is_fav, reason, watch = _favorability_check(
            iv_rank=10.0, regime="Low Vol", signal="Neutral", ivhv_gap=0.5,
            recovery_mode="INCOME", ticker="AAPL",
        )
        assert not is_fav
        assert "ETF" not in watch

    def test_etf_favorable_no_extra_watch(self):
        """When favorable, no ETF watch is added (nothing to watch for)."""
        from core.management.cycle3.cc_opportunity_engine import _favorability_check
        is_fav, reason, watch = _favorability_check(
            iv_rank=40.0, regime="High Vol", signal="Neutral", ivhv_gap=5.0,
            recovery_mode="INCOME", ticker="SLV",
        )
        assert is_fav
        assert watch == ""


# ── Schema Column ────────────────────────────────────────────────────────────


class TestIsETFSchema:
    """Is_ETF column in schema."""

    def test_is_etf_in_management_columns(self):
        from core.shared.data_contracts.schema import MANAGEMENT_UI_COLUMNS
        assert "Is_ETF" in MANAGEMENT_UI_COLUMNS

    def test_enforce_schema_defaults_is_etf_false(self):
        from core.shared.data_contracts.schema import enforce_management_schema
        df = pd.DataFrame([{"TradeID": "T1", "Symbol": "AAPL"}])
        result = enforce_management_schema(df)
        assert "Is_ETF" in result.columns
        assert result.iloc[0]["Is_ETF"] == False
