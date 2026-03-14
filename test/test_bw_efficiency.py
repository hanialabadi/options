"""
Tests for BW/CC Efficiency Scorecard.

Validates per-position efficiency metrics (net yield, premium/carry ratio,
days until carry eats GL, efficiency grade) and portfolio-level aggregation.
"""

import pytest
import numpy as np
import pandas as pd

from core.management.cycle2.carry.bw_efficiency import (
    BWEfficiencyCalculator,
    GRADE_A_THRESHOLD,
    GRADE_B_THRESHOLD,
    GRADE_C_THRESHOLD,
    GRADE_D_THRESHOLD,
)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _bw_stock_row(**overrides) -> dict:
    """Minimal BUY_WRITE stock leg row."""
    defaults = {
        "Strategy": "BUY_WRITE",
        "AssetType": "EQUITY",
        "Ticker": "AAPL",
        "Underlying_Ticker": "AAPL",
        "TradeID": "T001",
        "Quantity": 100,
        "UL Last": 150.0,
        "Total_GL_Decimal": 500.0,
        "Days_In_Trade": 60,
        "Cumulative_Premium_Collected": 3.50,  # per-share
        "Daily_Margin_Cost": 8.50,  # per-contract ($/day)
        "Cumulative_Margin_Carry": 510.0,  # per-contract (60 days × $8.50)
    }
    defaults.update(overrides)
    return defaults


def _bw_call_row(**overrides) -> dict:
    """Minimal BUY_WRITE call leg row."""
    defaults = {
        "Strategy": "BUY_WRITE",
        "AssetType": "OPTION",
        "Ticker": "AAPL",
        "Underlying_Ticker": "AAPL",
        "TradeID": "T001",
        "Quantity": -1,
        "Theta": -0.05,
        "Delta": -0.30,
    }
    defaults.update(overrides)
    return defaults


def _make_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


# ── TestPerPositionMetrics ───────────────────────────────────────────────────

class TestPerPositionMetrics:
    """Tests for per-position efficiency calculations."""

    def test_net_yield_computed(self):
        """Net yield = (premium - carry_per_share) / spot, annualized."""
        df = _make_df([_bw_stock_row()])
        calc = BWEfficiencyCalculator()
        result = calc.enrich(df)
        yield_val = result["Net_Yield_Annual_Pct"].iloc[0]
        assert pd.notna(yield_val)
        # Premium $3.50/share over 60 days - carry $510/(100*100)=$0.051/share
        # Net = ~$3.449/share over 60 days → daily = $0.0575 → annual = $0.0575/150 * 365 * 100
        assert yield_val > 0  # should be positive — premium >> carry

    def test_premium_vs_carry_ratio(self):
        """Premium/carry ratio = premium_per_contract / carry_per_contract."""
        df = _make_df([_bw_stock_row(
            Cumulative_Premium_Collected=5.0,
            Cumulative_Margin_Carry=100.0,
        )])
        calc = BWEfficiencyCalculator()
        result = calc.enrich(df)
        ratio = result["Premium_vs_Carry_Ratio"].iloc[0]
        # Premium per contract = $5 × 100 = $500
        # Carry per contract = $100
        # Ratio = 5.0
        assert ratio == pytest.approx(5.0, abs=0.1)

    def test_days_until_carry_eats_gl(self):
        """Days = GL / daily_cost."""
        df = _make_df([_bw_stock_row(
            Total_GL_Decimal=850.0,
            Daily_Margin_Cost=8.50,
        )])
        calc = BWEfficiencyCalculator()
        result = calc.enrich(df)
        days = result["Days_Until_Carry_Eats_GL"].iloc[0]
        assert days == pytest.approx(100, abs=1)

    def test_days_nan_when_losing(self):
        """No days countdown when position is underwater."""
        df = _make_df([_bw_stock_row(Total_GL_Decimal=-200.0)])
        calc = BWEfficiencyCalculator()
        result = calc.enrich(df)
        assert pd.isna(result["Days_Until_Carry_Eats_GL"].iloc[0])

    def test_days_nan_when_no_carry(self):
        """No days countdown when daily cost is zero (retirement)."""
        df = _make_df([_bw_stock_row(Daily_Margin_Cost=0.0)])
        calc = BWEfficiencyCalculator()
        result = calc.enrich(df)
        assert pd.isna(result["Days_Until_Carry_Eats_GL"].iloc[0])

    def test_option_legs_not_enriched(self):
        """Call legs should not get efficiency columns."""
        df = _make_df([_bw_stock_row(), _bw_call_row()])
        calc = BWEfficiencyCalculator()
        result = calc.enrich(df)
        assert pd.isna(result.loc[1, "Net_Yield_Annual_Pct"])
        assert result.loc[1, "Carry_Efficiency_Grade"] == ""

    def test_non_bw_strategies_not_enriched(self):
        """LONG_CALL positions should not get efficiency columns."""
        df = _make_df([_bw_stock_row(Strategy="LONG_CALL", AssetType="OPTION")])
        calc = BWEfficiencyCalculator()
        result = calc.enrich(df)
        assert pd.isna(result["Net_Yield_Annual_Pct"].iloc[0])


# ── TestGrading ──────────────────────────────────────────────────────────────

class TestGrading:
    """Tests for carry efficiency grading."""

    def test_grade_a(self):
        """Premium/carry >= 5× → grade A."""
        df = _make_df([_bw_stock_row(
            Cumulative_Premium_Collected=10.0,
            Cumulative_Margin_Carry=100.0,
        )])  # ratio = 10×
        calc = BWEfficiencyCalculator()
        result = calc.enrich(df)
        assert result["Carry_Efficiency_Grade"].iloc[0] == "A"

    def test_grade_b(self):
        """Premium/carry 3-5× → grade B."""
        df = _make_df([_bw_stock_row(
            Cumulative_Premium_Collected=4.0,
            Cumulative_Margin_Carry=100.0,
        )])  # ratio = 4×
        calc = BWEfficiencyCalculator()
        result = calc.enrich(df)
        assert result["Carry_Efficiency_Grade"].iloc[0] == "B"

    def test_grade_c(self):
        """Premium/carry 1.5-3× → grade C."""
        df = _make_df([_bw_stock_row(
            Cumulative_Premium_Collected=2.0,
            Cumulative_Margin_Carry=100.0,
        )])  # ratio = 2×
        calc = BWEfficiencyCalculator()
        result = calc.enrich(df)
        assert result["Carry_Efficiency_Grade"].iloc[0] == "C"

    def test_grade_d(self):
        """Premium/carry 1.0-1.5× → grade D."""
        df = _make_df([_bw_stock_row(
            Cumulative_Premium_Collected=1.2,
            Cumulative_Margin_Carry=100.0,
        )])  # ratio = 1.2×
        calc = BWEfficiencyCalculator()
        result = calc.enrich(df)
        assert result["Carry_Efficiency_Grade"].iloc[0] == "D"

    def test_grade_f(self):
        """Premium/carry < 1× → grade F (losing money to Fidelity)."""
        df = _make_df([_bw_stock_row(
            Cumulative_Premium_Collected=0.5,
            Cumulative_Margin_Carry=100.0,
        )])  # ratio = 0.5×
        calc = BWEfficiencyCalculator()
        result = calc.enrich(df)
        assert result["Carry_Efficiency_Grade"].iloc[0] == "F"

    def test_grade_a_retirement(self):
        """Retirement positions with premium but no carry → grade A."""
        df = _make_df([_bw_stock_row(
            Cumulative_Premium_Collected=3.0,
            Cumulative_Margin_Carry=0.0,
            Daily_Margin_Cost=0.0,
        )])
        calc = BWEfficiencyCalculator()
        result = calc.enrich(df)
        assert result["Carry_Efficiency_Grade"].iloc[0] == "A"

    def test_grade_dash_new_position(self):
        """New position with no premium and no carry → grade —."""
        df = _make_df([_bw_stock_row(
            Cumulative_Premium_Collected=0.0,
            Cumulative_Margin_Carry=0.0,
        )])
        calc = BWEfficiencyCalculator()
        result = calc.enrich(df)
        assert result["Carry_Efficiency_Grade"].iloc[0] == "—"

    def test_boundary_a_b(self):
        """Ratio exactly at GRADE_A_THRESHOLD → A."""
        df = _make_df([_bw_stock_row(
            Cumulative_Premium_Collected=5.0,
            Cumulative_Margin_Carry=100.0,
        )])  # ratio = 5.0
        calc = BWEfficiencyCalculator()
        result = calc.enrich(df)
        assert result["Carry_Efficiency_Grade"].iloc[0] == "A"


# ── TestPortfolioMetrics ─────────────────────────────────────────────────────

class TestPortfolioMetrics:
    """Tests for portfolio-level BW/CC efficiency."""

    def test_total_premium_and_carry(self):
        """Portfolio totals aggregate correctly."""
        df = _make_df([
            _bw_stock_row(Ticker="AAPL", Cumulative_Premium_Collected=3.0, Cumulative_Margin_Carry=200.0, Quantity=100),
            _bw_stock_row(Ticker="MSFT", Cumulative_Premium_Collected=2.0, Cumulative_Margin_Carry=150.0, Quantity=100),
        ])
        calc = BWEfficiencyCalculator()
        calc.enrich(df)
        m = calc.portfolio_metrics
        # Premium: $3/share × 100 × 100 + $2/share × 100 × 100 = $30,000 + $20,000
        assert m["total_premium_collected"] == pytest.approx(50000, rel=0.01)
        assert m["total_carry_paid"] == pytest.approx(350, abs=1)

    def test_portfolio_ratio(self):
        """Portfolio premium/carry ratio computed correctly."""
        df = _make_df([
            _bw_stock_row(Cumulative_Premium_Collected=5.0, Cumulative_Margin_Carry=100.0, Quantity=100),
        ])
        calc = BWEfficiencyCalculator()
        calc.enrich(df)
        m = calc.portfolio_metrics
        # Premium = $5 × 100 × 100 = $50,000. Carry = $100. Ratio = 500
        assert m["portfolio_premium_carry_ratio"] > 100

    def test_grade_distribution(self):
        """Grade distribution counts correctly."""
        df = _make_df([
            _bw_stock_row(Ticker="A", Cumulative_Premium_Collected=10.0, Cumulative_Margin_Carry=100.0),  # A
            _bw_stock_row(Ticker="B", Cumulative_Premium_Collected=0.5, Cumulative_Margin_Carry=100.0),   # F
        ])
        calc = BWEfficiencyCalculator()
        calc.enrich(df)
        m = calc.portfolio_metrics
        assert "A" in m["grade_distribution"]
        assert "F" in m["grade_distribution"]

    def test_attrs_stored(self):
        """Portfolio metrics stored in df.attrs."""
        df = _make_df([_bw_stock_row()])
        calc = BWEfficiencyCalculator()
        calc.enrich(df)
        assert "bw_efficiency" in df.attrs
        assert df.attrs["bw_efficiency"]["total_bw_cc_positions"] == 1

    def test_empty_df(self):
        """Empty DataFrame → no metrics."""
        df = pd.DataFrame()
        calc = BWEfficiencyCalculator()
        result = calc.enrich(df)
        assert calc.portfolio_metrics == {}

    def test_no_bw_cc(self):
        """No BW/CC positions → no metrics."""
        df = _make_df([{
            "Strategy": "LONG_CALL",
            "AssetType": "OPTION",
            "Ticker": "AAPL",
        }])
        calc = BWEfficiencyCalculator()
        calc.enrich(df)
        assert calc.portfolio_metrics == {}

    def test_theta_from_call_legs(self):
        """Daily theta aggregated from call legs of BW/CC trades."""
        df = _make_df([
            _bw_stock_row(TradeID="T001"),
            _bw_call_row(TradeID="T001", Theta=-0.05, Quantity=-1),
        ])
        calc = BWEfficiencyCalculator()
        calc.enrich(df)
        m = calc.portfolio_metrics
        # Theta = -0.05 × -1 × 100 = $5/day
        assert m["bw_cc_daily_theta"] == pytest.approx(5.0, abs=0.5)


# ── TestEdgeCases ────────────────────────────────────────────────────────────

class TestEdgeCases:
    """Edge case handling."""

    def test_missing_columns(self):
        """Missing premium/carry columns → no crash."""
        df = _make_df([{
            "Strategy": "BUY_WRITE",
            "AssetType": "EQUITY",
            "Ticker": "TEST",
        }])
        calc = BWEfficiencyCalculator()
        result = calc.enrich(df)
        assert "Carry_Efficiency_Grade" in result.columns

    def test_zero_spot(self):
        """Spot price = 0 → net yield is NaN, not crash."""
        df = _make_df([_bw_stock_row(**{"UL Last": 0.0})])
        calc = BWEfficiencyCalculator()
        result = calc.enrich(df)
        assert pd.isna(result["Net_Yield_Annual_Pct"].iloc[0])

    def test_covered_call_included(self):
        """COVERED_CALL strategy is treated same as BUY_WRITE."""
        df = _make_df([_bw_stock_row(Strategy="COVERED_CALL")])
        calc = BWEfficiencyCalculator()
        result = calc.enrich(df)
        assert result["Carry_Efficiency_Grade"].iloc[0] != ""
