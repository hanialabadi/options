"""
Tests for MarginCarryCalculator.

Validates cumulative carry cost, carry-adjusted P&L, theta coverage ratio,
carry classification, and portfolio-level burn rate aggregation.

McMillan Ch.3: "The covered writer must earn at least the cost of carrying the stock."
Passarelli Ch.6: "Negative carry — yield below financing rate — is a ROLL signal."
"""

import pytest
import numpy as np
import pandas as pd

from core.management.cycle2.carry.margin_carry import (
    MarginCarryCalculator,
    MARGIN_RATE,
    MARGIN_BURN_WARNING_DAILY,
    MARGIN_BURN_CRITICAL_DAILY,
)
from core.management.cycle3.doctrine.thresholds import (
    CARRY_INVERSION_SEVERE,
    CARRY_INVERSION_MILD,
)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _stock_row(**overrides) -> dict:
    """Minimal BUY_WRITE stock leg."""
    base = {
        "TradeID": "T001",
        "LegID": "L001",
        "Symbol": "AAPL",
        "Strategy": "BUY_WRITE",
        "AssetType": "STOCK",
        "Quantity": 100,
        "Last": 200.0,
        "Daily_Margin_Cost": 200.0 * 100 * MARGIN_RATE / 365,  # ~$5.68/day
        "Days_In_Trade": 30,
        "Total_GL_Decimal": 500.0,
        "Basis": 20000.0,
        "Theta": np.nan,
    }
    base.update(overrides)
    return base


def _option_row(**overrides) -> dict:
    """Short call leg for income strategy."""
    base = {
        "TradeID": "T001",
        "LegID": "L002",
        "Symbol": "AAPL260417C210",
        "Strategy": "BUY_WRITE",
        "AssetType": "OPTION",
        "Quantity": -1,
        "Last": 3.50,
        "Daily_Margin_Cost": 3.50 * 100 * MARGIN_RATE / 365,
        "Days_In_Trade": 30,
        "Total_GL_Decimal": 150.0,
        "Basis": -350.0,
        "Theta": -0.05,  # $5/day theta income per contract
    }
    base.update(overrides)
    return base


def _build_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


# ═════════════════════════════════════════════════════════════════════════════
# Cumulative Carry
# ═════════════════════════════════════════════════════════════════════════════

class TestCumulativeCarry:

    def test_basic_computation(self):
        """Cumulative = Daily × Days."""
        df = _build_df([_stock_row(Daily_Margin_Cost=10.0, Days_In_Trade=30)])
        calc = MarginCarryCalculator()
        df = calc.enrich(df)
        assert df["Cumulative_Margin_Carry"].iloc[0] == 300.0

    def test_zero_days(self):
        """Day 0 → no cumulative carry."""
        df = _build_df([_stock_row(Daily_Margin_Cost=10.0, Days_In_Trade=0)])
        calc = MarginCarryCalculator()
        df = calc.enrich(df)
        assert df["Cumulative_Margin_Carry"].iloc[0] == 0.0

    def test_missing_daily_cost(self):
        """Missing Daily_Margin_Cost → 0 cumulative."""
        df = _build_df([_stock_row(Daily_Margin_Cost=np.nan, Days_In_Trade=30)])
        calc = MarginCarryCalculator()
        df = calc.enrich(df)
        assert df["Cumulative_Margin_Carry"].iloc[0] == 0.0

    def test_missing_days_in_trade(self):
        """Missing Days_In_Trade → 0 cumulative."""
        df = _build_df([_stock_row(Daily_Margin_Cost=10.0, Days_In_Trade=np.nan)])
        calc = MarginCarryCalculator()
        df = calc.enrich(df)
        assert df["Cumulative_Margin_Carry"].iloc[0] == 0.0

    def test_negative_days_clipped(self):
        """Negative days → clipped to 0."""
        df = _build_df([_stock_row(Daily_Margin_Cost=10.0, Days_In_Trade=-5)])
        calc = MarginCarryCalculator()
        df = calc.enrich(df)
        assert df["Cumulative_Margin_Carry"].iloc[0] == 0.0


# ═════════════════════════════════════════════════════════════════════════════
# Carry-Adjusted P&L
# ═════════════════════════════════════════════════════════════════════════════

class TestCarryAdjustedPnL:

    def test_winner_stays_winner(self):
        """$500 gain - $150 carry = $350 still positive."""
        df = _build_df([_stock_row(
            Total_GL_Decimal=500.0, Daily_Margin_Cost=5.0, Days_In_Trade=30,
        )])
        calc = MarginCarryCalculator()
        df = calc.enrich(df)
        assert df["Carry_Adjusted_GL"].iloc[0] == 350.0

    def test_winner_flips_to_loser(self):
        """$100 gain - $300 carry = -$200 actual loss."""
        df = _build_df([_stock_row(
            Total_GL_Decimal=100.0, Daily_Margin_Cost=10.0, Days_In_Trade=30,
        )])
        calc = MarginCarryCalculator()
        df = calc.enrich(df)
        assert df["Carry_Adjusted_GL"].iloc[0] == -200.0

    def test_loser_deeper(self):
        """-$500 loss - $150 carry = -$650."""
        df = _build_df([_stock_row(
            Total_GL_Decimal=-500.0, Daily_Margin_Cost=5.0, Days_In_Trade=30,
        )])
        calc = MarginCarryCalculator()
        df = calc.enrich(df)
        assert df["Carry_Adjusted_GL"].iloc[0] == -650.0

    def test_percentage_computed(self):
        """Carry-adjusted GL % = carry_adjusted_gl / |basis| × 100."""
        df = _build_df([_stock_row(
            Total_GL_Decimal=500.0, Daily_Margin_Cost=5.0,
            Days_In_Trade=30, Basis=20000.0,
        )])
        calc = MarginCarryCalculator()
        df = calc.enrich(df)
        # (500 - 150) / 20000 * 100 = 1.75%
        assert abs(df["Carry_Adjusted_GL_Pct"].iloc[0] - 1.75) < 0.01

    def test_no_carry_same_pnl(self):
        """Zero carry → carry-adjusted GL equals raw GL."""
        df = _build_df([_stock_row(
            Total_GL_Decimal=500.0, Daily_Margin_Cost=0.0, Days_In_Trade=30,
        )])
        calc = MarginCarryCalculator()
        df = calc.enrich(df)
        assert df["Carry_Adjusted_GL"].iloc[0] == 500.0


# ═════════════════════════════════════════════════════════════════════════════
# Carry/Theta Ratio
# ═════════════════════════════════════════════════════════════════════════════

class TestCarryThetaRatio:

    def test_theta_covers_carry(self):
        """Theta income > margin cost → ratio < 1.0."""
        df = _build_df([_option_row(
            Daily_Margin_Cost=3.0, Theta=-0.05, Quantity=-1,
        )])
        calc = MarginCarryCalculator()
        df = calc.enrich(df)
        # ratio = 3.0 / (0.05 * 100 * 1) = 3.0 / 5.0 = 0.6
        assert df["Carry_Theta_Ratio"].iloc[0] == pytest.approx(0.6, abs=0.01)

    def test_carry_exceeds_theta(self):
        """Margin cost > theta income → ratio > 1.0."""
        df = _build_df([_option_row(
            Daily_Margin_Cost=8.0, Theta=-0.05, Quantity=-1,
        )])
        calc = MarginCarryCalculator()
        df = calc.enrich(df)
        # ratio = 8.0 / 5.0 = 1.6
        assert df["Carry_Theta_Ratio"].iloc[0] == pytest.approx(1.6, abs=0.01)

    def test_stock_leg_no_ratio(self):
        """Stock legs don't get theta ratio (not an option)."""
        df = _build_df([_stock_row()])
        calc = MarginCarryCalculator()
        df = calc.enrich(df)
        assert pd.isna(df["Carry_Theta_Ratio"].iloc[0])

    def test_long_option_no_ratio(self):
        """Long options (Quantity > 0) don't get theta ratio."""
        df = _build_df([_option_row(Quantity=1, Strategy="LONG_CALL")])
        calc = MarginCarryCalculator()
        df = calc.enrich(df)
        assert pd.isna(df["Carry_Theta_Ratio"].iloc[0])

    def test_multi_contract(self):
        """5 short contracts: theta income = |theta| × 100 × 5."""
        df = _build_df([_option_row(
            Daily_Margin_Cost=15.0, Theta=-0.04, Quantity=-5,
        )])
        calc = MarginCarryCalculator()
        df = calc.enrich(df)
        # ratio = 15.0 / (0.04 * 100 * 5) = 15.0 / 20.0 = 0.75
        assert df["Carry_Theta_Ratio"].iloc[0] == pytest.approx(0.75, abs=0.01)


# ═════════════════════════════════════════════════════════════════════════════
# Carry Classification
# ═════════════════════════════════════════════════════════════════════════════

class TestCarryClassification:

    def test_none_when_no_carry(self):
        """Zero margin cost → NONE."""
        df = _build_df([_stock_row(Daily_Margin_Cost=0.0)])
        calc = MarginCarryCalculator()
        df = calc.enrich(df)
        assert df["Carry_Classification"].iloc[0] == "NONE"

    def test_covered(self):
        """Theta income > margin cost → COVERED."""
        df = _build_df([_option_row(
            Daily_Margin_Cost=3.0, Theta=-0.05, Quantity=-1,
        )])
        calc = MarginCarryCalculator()
        df = calc.enrich(df)
        assert df["Carry_Classification"].iloc[0] == "COVERED"

    def test_mild_inversion(self):
        """Ratio between 1.0 and 1.5 → MILD_INVERSION."""
        # ratio = 6.0 / 5.0 = 1.2
        df = _build_df([_option_row(
            Daily_Margin_Cost=6.0, Theta=-0.05, Quantity=-1,
        )])
        calc = MarginCarryCalculator()
        df = calc.enrich(df)
        assert df["Carry_Classification"].iloc[0] == "MILD_INVERSION"

    def test_severe_inversion(self):
        """Ratio ≥ 1.5 → SEVERE_INVERSION."""
        # ratio = 10.0 / 5.0 = 2.0
        df = _build_df([_option_row(
            Daily_Margin_Cost=10.0, Theta=-0.05, Quantity=-1,
        )])
        calc = MarginCarryCalculator()
        df = calc.enrich(df)
        assert df["Carry_Classification"].iloc[0] == "SEVERE_INVERSION"

    def test_uncovered_for_stock(self):
        """Stock with carry cost but no theta → UNCOVERED."""
        df = _build_df([_stock_row(Daily_Margin_Cost=5.0)])
        calc = MarginCarryCalculator()
        df = calc.enrich(df)
        assert df["Carry_Classification"].iloc[0] == "UNCOVERED"

    def test_threshold_boundary_mild(self):
        """Ratio exactly 1.0 → MILD_INVERSION (>=)."""
        # ratio = 5.0 / 5.0 = 1.0
        df = _build_df([_option_row(
            Daily_Margin_Cost=5.0, Theta=-0.05, Quantity=-1,
        )])
        calc = MarginCarryCalculator()
        df = calc.enrich(df)
        assert df["Carry_Classification"].iloc[0] == "MILD_INVERSION"

    def test_threshold_boundary_severe(self):
        """Ratio exactly 1.5 → SEVERE_INVERSION (>=)."""
        # ratio = 7.5 / 5.0 = 1.5
        df = _build_df([_option_row(
            Daily_Margin_Cost=7.5, Theta=-0.05, Quantity=-1,
        )])
        calc = MarginCarryCalculator()
        df = calc.enrich(df)
        assert df["Carry_Classification"].iloc[0] == "SEVERE_INVERSION"


# ═════════════════════════════════════════════════════════════════════════════
# Portfolio Metrics
# ═════════════════════════════════════════════════════════════════════════════

class TestPortfolioMetrics:

    def test_burn_rate_sum(self):
        """Portfolio burn = sum of all daily costs."""
        df = _build_df([
            _stock_row(Daily_Margin_Cost=10.0),
            _option_row(Daily_Margin_Cost=3.0),
        ])
        calc = MarginCarryCalculator()
        calc.enrich(df)
        m = calc.portfolio_metrics
        assert m["portfolio_daily_margin_burn"] == 13.0

    def test_monthly_annual(self):
        """Monthly = daily × 30, annual = daily × 365."""
        df = _build_df([_stock_row(Daily_Margin_Cost=10.0)])
        calc = MarginCarryCalculator()
        calc.enrich(df)
        m = calc.portfolio_metrics
        assert m["portfolio_monthly_margin_burn"] == 300.0
        assert m["portfolio_annual_margin_burn"] == 3650.0

    def test_theta_income_from_short_options(self):
        """Portfolio theta income counts only short option legs."""
        df = _build_df([
            _stock_row(),                                    # stock: no theta income
            _option_row(Theta=-0.05, Quantity=-2),           # short: 0.05 × 100 × 2 = $10/day
            _option_row(Theta=-0.03, Quantity=1,             # long: excluded
                        Strategy="LONG_CALL", LegID="L003"),
        ])
        calc = MarginCarryCalculator()
        calc.enrich(df)
        m = calc.portfolio_metrics
        assert m["portfolio_theta_income_daily"] == 10.0

    def test_net_carry(self):
        """Net carry = theta income - burn."""
        df = _build_df([
            _stock_row(Daily_Margin_Cost=8.0),
            _option_row(Theta=-0.05, Quantity=-2, Daily_Margin_Cost=2.0),
        ])
        calc = MarginCarryCalculator()
        calc.enrich(df)
        m = calc.portfolio_metrics
        # theta: 0.05 × 100 × 2 = 10.0, burn: 8 + 2 = 10.0, net = 0.0
        assert m["portfolio_net_carry"] == 0.0

    def test_health_green(self):
        """Below warning threshold → GREEN."""
        df = _build_df([_stock_row(Daily_Margin_Cost=10.0)])
        calc = MarginCarryCalculator()
        calc.enrich(df)
        assert calc.portfolio_metrics["portfolio_carry_health"] == "GREEN"

    def test_health_yellow(self):
        """Between warning and critical → YELLOW."""
        df = _build_df([_stock_row(Daily_Margin_Cost=75.0)])
        calc = MarginCarryCalculator()
        calc.enrich(df)
        assert calc.portfolio_metrics["portfolio_carry_health"] == "YELLOW"

    def test_health_red(self):
        """Above critical → RED."""
        df = _build_df([_stock_row(Daily_Margin_Cost=150.0)])
        calc = MarginCarryCalculator()
        calc.enrich(df)
        assert calc.portfolio_metrics["portfolio_carry_health"] == "RED"

    def test_stored_in_attrs(self):
        """Metrics stored in df.attrs['margin_carry']."""
        df = _build_df([_stock_row()])
        calc = MarginCarryCalculator()
        df = calc.enrich(df)
        assert "margin_carry" in df.attrs
        assert df.attrs["margin_carry"]["portfolio_daily_margin_burn"] > 0

    def test_cumulative_carry_sum(self):
        """Portfolio cumulative carry = sum of per-position cumulative."""
        df = _build_df([
            _stock_row(Daily_Margin_Cost=10.0, Days_In_Trade=20),
            _stock_row(Daily_Margin_Cost=5.0, Days_In_Trade=40, LegID="L003"),
        ])
        calc = MarginCarryCalculator()
        calc.enrich(df)
        m = calc.portfolio_metrics
        assert m["portfolio_cumulative_carry"] == 400.0  # 200 + 200


# ═════════════════════════════════════════════════════════════════════════════
# Edge Cases
# ═════════════════════════════════════════════════════════════════════════════

class TestEdgeCases:

    def test_empty_dataframe(self):
        """Empty df → no crash, zero metrics."""
        df = pd.DataFrame()
        calc = MarginCarryCalculator()
        df = calc.enrich(df)
        m = calc.portfolio_metrics
        assert m["portfolio_daily_margin_burn"] == 0.0

    def test_missing_columns_graceful(self):
        """DataFrame with minimal columns → no crash."""
        df = pd.DataFrame({"Symbol": ["AAPL"], "Strategy": ["BUY_WRITE"]})
        calc = MarginCarryCalculator()
        df = calc.enrich(df)
        assert "Cumulative_Margin_Carry" in df.columns

    def test_custom_margin_rate(self):
        """Custom margin rate via constructor."""
        calc = MarginCarryCalculator(margin_rate=0.08)  # 8%
        assert calc.rate == 0.08
        assert abs(calc.rate_daily - 0.08 / 365) < 1e-10

    def test_all_cash_portfolio(self):
        """All positions with zero carry → NONE classification, GREEN health."""
        df = _build_df([
            _stock_row(Daily_Margin_Cost=0.0),
            _option_row(Daily_Margin_Cost=0.0),
        ])
        calc = MarginCarryCalculator()
        df = calc.enrich(df)
        assert (df["Carry_Classification"] == "NONE").all()
        assert calc.portfolio_metrics["portfolio_carry_health"] == "GREEN"


# ═════════════════════════════════════════════════════════════════════════════
# Retirement vs Taxable Account
# ═════════════════════════════════════════════════════════════════════════════

class TestRetirementAccounts:

    def test_roth_tagged_as_retirement(self):
        """ROTH IRA positions get Is_Retirement=True."""
        df = _build_df([_stock_row(Account="ROTH IRA *4854")])
        calc = MarginCarryCalculator()
        df = calc.enrich(df)
        assert bool(df["Is_Retirement"].iloc[0]) is True

    def test_taxable_tagged_as_not_retirement(self):
        """Individual account gets Is_Retirement=False."""
        df = _build_df([_stock_row(Account="Individual - TOD *5376")])
        calc = MarginCarryCalculator()
        df = calc.enrich(df)
        assert bool(df["Is_Retirement"].iloc[0]) is False

    def test_roth_zero_cumulative_carry(self):
        """Roth positions with Daily_Margin_Cost=0 → zero cumulative carry."""
        df = _build_df([_stock_row(
            Account="ROTH IRA *4854",
            Daily_Margin_Cost=0.0,  # upstream zeroed by compute_basic_drift
            Days_In_Trade=60,
        )])
        calc = MarginCarryCalculator()
        df = calc.enrich(df)
        assert df["Cumulative_Margin_Carry"].iloc[0] == 0.0
        assert df["Carry_Classification"].iloc[0] == "NONE"

    def test_mixed_portfolio_only_taxable_burns(self):
        """Mixed portfolio: only taxable account positions contribute to burn."""
        df = _build_df([
            _stock_row(Account="Individual - TOD *5376", Daily_Margin_Cost=10.0),
            _stock_row(Account="ROTH IRA *4854", Daily_Margin_Cost=0.0, LegID="L003"),
        ])
        calc = MarginCarryCalculator()
        calc.enrich(df)
        m = calc.portfolio_metrics
        assert m["portfolio_daily_margin_burn"] == 10.0
        assert m["taxable_positions"] == 1
        assert m["retirement_positions"] == 1
        assert m["taxable_daily_burn"] == 10.0
        assert m["retirement_daily_burn"] == 0.0

    def test_per_account_counts(self):
        """Portfolio metrics include per-account position counts."""
        df = _build_df([
            _stock_row(Account="Individual - TOD *5376", Daily_Margin_Cost=5.0),
            _option_row(Account="Individual - TOD *5376", Daily_Margin_Cost=2.0),
            _stock_row(Account="ROTH IRA *4854", Daily_Margin_Cost=0.0, LegID="L010"),
            _option_row(Account="ROTH IRA *4854", Daily_Margin_Cost=0.0, LegID="L011"),
        ])
        calc = MarginCarryCalculator()
        calc.enrich(df)
        m = calc.portfolio_metrics
        assert m["taxable_positions"] == 2
        assert m["retirement_positions"] == 2
        assert m["taxable_daily_burn"] == 7.0

    def test_no_account_column_defaults_false(self):
        """Missing Account column → Is_Retirement=False for all."""
        df = _build_df([_stock_row()])
        if "Account" in df.columns:
            df = df.drop(columns=["Account"])
        calc = MarginCarryCalculator()
        df = calc.enrich(df)
        assert bool(df["Is_Retirement"].iloc[0]) is False

    def test_carry_adjusted_gl_reflects_account_type(self):
        """Roth position: carry-adjusted GL = raw GL (no carry to subtract)."""
        df = _build_df([
            _stock_row(
                Account="ROTH IRA *4854",
                Daily_Margin_Cost=0.0, Days_In_Trade=60,
                Total_GL_Decimal=500.0,
            ),
            _stock_row(
                Account="Individual - TOD *5376",
                Daily_Margin_Cost=5.0, Days_In_Trade=60,
                Total_GL_Decimal=500.0,
                LegID="L003",
            ),
        ])
        calc = MarginCarryCalculator()
        df = calc.enrich(df)
        # Roth: 500 - 0 = 500
        assert df.iloc[0]["Carry_Adjusted_GL"] == 500.0
        # Taxable: 500 - (5 * 60) = 500 - 300 = 200
        assert df.iloc[1]["Carry_Adjusted_GL"] == 200.0


# ═════════════════════════════════════════════════════════════════════════════
# Actual Margin Debit Override
# ═════════════════════════════════════════════════════════════════════════════

class TestMarginDebitOverride:

    def test_debit_overrides_estimated_burn(self):
        """When margin_debit is set, portfolio burn uses debit × rate / 365."""
        df = _build_df([_stock_row(Daily_Margin_Cost=10.0)])
        calc = MarginCarryCalculator(margin_debit=57605.0)
        calc.enrich(df)
        m = calc.portfolio_metrics
        expected = round(57605.0 * MARGIN_RATE / 365, 2)
        assert m["portfolio_daily_margin_burn"] == expected
        assert m["burn_source"] == "ACTUAL_DEBIT"
        assert m["margin_debit"] == 57605.0

    def test_no_debit_uses_estimated(self):
        """Without margin_debit, falls back to sum of per-position costs."""
        df = _build_df([_stock_row(Daily_Margin_Cost=10.0)])
        calc = MarginCarryCalculator()  # no debit
        calc.enrich(df)
        m = calc.portfolio_metrics
        assert m["portfolio_daily_margin_burn"] == 10.0
        assert m["burn_source"] == "ESTIMATED"
        assert m["margin_debit"] is None

    def test_estimated_still_tracked(self):
        """Even with debit override, estimated burn is separately tracked."""
        df = _build_df([
            _stock_row(Daily_Margin_Cost=8.0),
            _stock_row(Daily_Margin_Cost=4.0, LegID="L003"),
        ])
        calc = MarginCarryCalculator(margin_debit=57605.0)
        calc.enrich(df)
        m = calc.portfolio_metrics
        assert m["estimated_daily_burn"] == 12.0
        assert m["portfolio_daily_margin_burn"] != 12.0  # debit overrides


# ═════════════════════════════════════════════════════════════════════════════
# Threshold Constants
# ═════════════════════════════════════════════════════════════════════════════

class TestThresholdConstants:

    def test_inversion_ordering(self):
        """Mild < severe inversion thresholds."""
        assert CARRY_INVERSION_MILD < CARRY_INVERSION_SEVERE

    def test_warning_thresholds_ordered(self):
        """Warning < critical daily burn thresholds."""
        assert MARGIN_BURN_WARNING_DAILY < MARGIN_BURN_CRITICAL_DAILY

    def test_margin_rate_reasonable(self):
        """Margin rate between 3% and 15%."""
        assert 0.03 <= MARGIN_RATE <= 0.15


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
