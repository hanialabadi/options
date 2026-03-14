"""
Monte Carlo Extensions Tests
==============================
Tests for 5 new MC modules:
  1. Earnings Event Simulation (mc_earnings_event)
  2. Portfolio VaR (mc_portfolio_var)
  3. Optimal Exit Timing (mc_optimal_exit)
  4. Correlation-Aware Sizing (mc_correlation_sizing)
  5. Variance Premium Scoring (mc_variance_premium)

Run:
    pytest test/test_mc_extensions.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# =============================================================================
# 1. Earnings Event Simulation
# =============================================================================

class TestMCEarningsEvent:
    """Test mc_earnings_event simulation."""

    def _make_row(self, **overrides):
        defaults = {
            "Ticker": "AAPL",
            "Strategy_Name": "LONG_CALL",
            "last_price": 180.0,
            "Strike": 185.0,
            "Mid_Price": 5.50,
            "DTE": 30,
            "days_to_earnings": 5,
            "hv_30": 28.0,
            "Execution_IV": 35.0,
            "Earnings_Move_Ratio": 0.43,
            "Earnings_Beat_Rate": 91.0,
            "Earnings_Avg_IV_Crush": 35.0,
            "Earnings_Avg_Actual_Move": 4.0,
            "Earnings_Avg_Gap": 3.0,
        }
        defaults.update(overrides)
        return pd.Series(defaults)

    def test_basic_simulation_runs(self):
        from scan_engine.mc_earnings_event import mc_earnings_event
        row = self._make_row()
        result = mc_earnings_event(row, n_paths=500)
        assert result["MC_Earn_Verdict"] in ("HOLD_THROUGH", "CLOSE_BEFORE", "NEUTRAL")
        assert pd.notna(result["MC_Earn_EV_Hold"])
        assert pd.notna(result["MC_Earn_EV_Close"])
        assert 0 <= result["MC_Earn_P_Profit"] <= 1

    def test_no_earnings_skips(self):
        from scan_engine.mc_earnings_event import mc_earnings_event
        row = self._make_row(days_to_earnings=np.nan)
        result = mc_earnings_event(row)
        assert result["MC_Earn_Verdict"] == "SKIP"

    def test_far_earnings_skips(self):
        from scan_engine.mc_earnings_event import mc_earnings_event
        row = self._make_row(days_to_earnings=35)
        result = mc_earnings_event(row)
        assert result["MC_Earn_Verdict"] == "SKIP"

    def test_note_includes_track_record(self):
        from scan_engine.mc_earnings_event import mc_earnings_event
        row = self._make_row()
        result = mc_earnings_event(row, n_paths=500)
        note = result["MC_Earn_Note"]
        assert "beat_rate=91%" in note
        assert "avg_crush=35%" in note
        assert "move_ratio=0.43" in note

    def test_put_strategy_works(self):
        from scan_engine.mc_earnings_event import mc_earnings_event
        row = self._make_row(Strategy_Name="LONG_PUT", Strike=175.0)
        result = mc_earnings_event(row, n_paths=500)
        assert result["MC_Earn_Verdict"] in ("HOLD_THROUGH", "CLOSE_BEFORE", "NEUTRAL")

    def test_no_spot_skips(self):
        from scan_engine.mc_earnings_event import mc_earnings_event
        row = self._make_row(last_price=np.nan)
        result = mc_earnings_event(row)
        assert result["MC_Earn_Verdict"] == "SKIP"

    def test_edge_is_difference(self):
        from scan_engine.mc_earnings_event import mc_earnings_event
        row = self._make_row()
        result = mc_earnings_event(row, n_paths=500)
        expected_edge = result["MC_Earn_EV_Hold"] - result["MC_Earn_EV_Close"]
        assert abs(result["MC_Earn_Edge"] - expected_edge) < 0.1

    def test_reproducible_with_seed(self):
        from scan_engine.mc_earnings_event import mc_earnings_event
        row = self._make_row()
        r1 = mc_earnings_event(row, n_paths=500, rng=np.random.default_rng(42))
        r2 = mc_earnings_event(row, n_paths=500, rng=np.random.default_rng(42))
        assert r1["MC_Earn_EV_Hold"] == r2["MC_Earn_EV_Hold"]


# =============================================================================
# 2. Portfolio VaR
# =============================================================================

class TestMCPortfolioVaR:
    """Test mc_portfolio_var correlated stress test."""

    def _make_portfolio(self, n=3):
        rows = []
        for i, (ticker, spot, strike, qty) in enumerate([
            ("AAPL", 180, 185, 2),
            ("MSFT", 420, 430, 1),
            ("GOOGL", 170, 175, 3),
        ][:n]):
            rows.append({
                "Ticker": ticker,
                "UL Last": spot,
                "Strike": strike,
                "Quantity": qty,
                "DTE": 30,
                "AssetType": "OPTION",
                "Option_Type": "CALL",
                "Basis": 5.0,
                "Last": 5.0,
                "HV_20D": 0.28,
            })
        return pd.DataFrame(rows)

    def test_basic_var_computation(self):
        from core.management.mc_portfolio_var import mc_portfolio_var
        df = self._make_portfolio()
        result = mc_portfolio_var(df, n_paths=500)
        assert pd.notna(result["Portfolio_VaR_5pct"])
        # VaR is the 5th percentile — it should be less than the median
        assert result["Portfolio_VaR_5pct"] < result["Portfolio_P50"]
        assert pd.notna(result["Portfolio_CVaR_5pct"])
        assert result["Portfolio_CVaR_5pct"] <= result["Portfolio_VaR_5pct"]

    def test_empty_portfolio_skips(self):
        from core.management.mc_portfolio_var import mc_portfolio_var
        result = mc_portfolio_var(pd.DataFrame())
        assert "MC_SKIP" in result["Portfolio_MC_Note"]

    def test_concentration_index(self):
        from core.management.mc_portfolio_var import mc_portfolio_var
        df = self._make_portfolio(n=1)
        result = mc_portfolio_var(df, n_paths=500)
        assert result["Portfolio_Concentration"] == pytest.approx(1.0, abs=0.01)

    def test_stress_test_computed(self):
        from core.management.mc_portfolio_var import mc_portfolio_var
        df = self._make_portfolio()
        result = mc_portfolio_var(df, n_paths=500)
        # Stress test should produce a finite number
        assert pd.notna(result["Portfolio_Stress_SPY_5"])

    def test_note_contains_metrics(self):
        from core.management.mc_portfolio_var import mc_portfolio_var
        df = self._make_portfolio()
        result = mc_portfolio_var(df, n_paths=500)
        note = result["Portfolio_MC_Note"]
        assert "VaR(5%)" in note
        assert "CVaR(5%)" in note
        assert "concentration" in note


# =============================================================================
# 3. Optimal Exit Timing
# =============================================================================

class TestMCOptimalExit:
    """Test mc_optimal_exit day-by-day peak detection."""

    def _make_row(self, **overrides):
        defaults = {
            "Ticker": "AAPL",
            "UL Last": 180.0,
            "Strike": 185.0,
            "DTE": 45,
            "Quantity": 2,
            "Option_Type": "CALL",
            "AssetType": "OPTION",
            "Basis": 5.0,
            "Last": 5.0,
            "HV_20D": 0.28,
            "IV_Now": 0.32,
        }
        defaults.update(overrides)
        return pd.Series(defaults)

    def test_basic_exit_timing(self):
        from core.management.mc_optimal_exit import mc_optimal_exit
        row = self._make_row()
        result = mc_optimal_exit(row, n_paths=500)
        assert pd.notna(result["MC_Optimal_Exit_DTE"])
        assert 0 <= result["MC_Optimal_Exit_DTE"] <= 45

    def test_short_dte_skips(self):
        from core.management.mc_optimal_exit import mc_optimal_exit
        row = self._make_row(DTE=2)
        result = mc_optimal_exit(row)
        assert "MC_SKIP" in result["MC_Exit_Note"]

    def test_no_spot_skips(self):
        from core.management.mc_optimal_exit import mc_optimal_exit
        row = self._make_row(**{"UL Last": np.nan, "Last": np.nan,
                                "Underlying_Last": np.nan, "last_price": np.nan})
        result = mc_optimal_exit(row)
        assert "MC_SKIP" in result["MC_Exit_Note"]

    def test_peak_ev_exists(self):
        from core.management.mc_optimal_exit import mc_optimal_exit
        row = self._make_row()
        result = mc_optimal_exit(row, n_paths=500)
        assert pd.notna(result["MC_Exit_Peak_EV"])
        assert pd.notna(result["MC_Exit_Terminal_EV"])

    def test_note_contains_context(self):
        from core.management.mc_optimal_exit import mc_optimal_exit
        row = self._make_row()
        result = mc_optimal_exit(row, n_paths=500)
        assert "Optimal exit" in result["MC_Exit_Note"]
        assert "Peak EV" in result["MC_Exit_Note"]

    def test_put_option_works(self):
        from core.management.mc_optimal_exit import mc_optimal_exit
        row = self._make_row(Option_Type="PUT", Strike=175.0)
        result = mc_optimal_exit(row, n_paths=500)
        assert pd.notna(result["MC_Optimal_Exit_DTE"])


# =============================================================================
# 4. Correlation-Aware Sizing
# =============================================================================

class TestMCCorrelationSizing:
    """Test mc_correlation_adjustment sizing reduction."""

    def test_no_existing_no_adjustment(self):
        from scan_engine.mc_correlation_sizing import mc_correlation_adjustment
        result = mc_correlation_adjustment("AAPL", [], 10)
        assert result["MC_Corr_Adjustment"] == 1.0
        assert result["MC_Corr_Max_Contracts"] == 10

    def test_adjustment_preserves_minimum(self):
        from scan_engine.mc_correlation_sizing import mc_correlation_adjustment
        # Even with extreme correlation, floor is 1 contract
        result = mc_correlation_adjustment("AAPL", ["MSFT"], 1)
        assert result["MC_Corr_Max_Contracts"] >= 1

    def test_same_ticker_excluded(self):
        from scan_engine.mc_correlation_sizing import mc_correlation_adjustment
        # Candidate is already in existing — should exclude self
        result = mc_correlation_adjustment("AAPL", ["AAPL"], 10)
        assert result["MC_Corr_Adjustment"] == 1.0

    def test_output_keys(self):
        from scan_engine.mc_correlation_sizing import mc_correlation_adjustment
        result = mc_correlation_adjustment("AAPL", ["MSFT", "GOOGL"], 10)
        assert "MC_Corr_Adjustment" in result
        assert "MC_Corr_Overlap" in result
        assert "MC_Corr_Avg" in result
        assert "MC_Corr_Note" in result
        assert "MC_Corr_Max_Contracts" in result


# =============================================================================
# 5. Variance Premium Scoring
# =============================================================================

class TestMCVariancePremium:
    """Test mc_variance_premium scoring."""

    def _make_row(self, **overrides):
        defaults = {
            "Ticker": "AAPL",
            "Strategy_Name": "LONG_CALL",
            "last_price": 180.0,
            "Selected_Strike": 185.0,
            "Mid_Price": 5.50,
            "DTE": 30,
            "hv_30": 25.0,
            "Execution_IV": 35.0,
        }
        defaults.update(overrides)
        return pd.Series(defaults)

    def test_basic_scoring(self):
        from scan_engine.mc_variance_premium import mc_variance_premium
        row = self._make_row()
        result = mc_variance_premium(row, n_paths=500)
        assert result["MC_VP_Verdict"] in ("CHEAP", "FAIR", "EXPENSIVE")
        assert pd.notna(result["MC_VP_Score"])

    def test_high_iv_low_hv_expensive(self):
        """IV much higher than HV → option should be EXPENSIVE."""
        from scan_engine.mc_variance_premium import mc_variance_premium
        row = self._make_row(hv_30=15.0, Execution_IV=45.0)
        result = mc_variance_premium(row, n_paths=1000)
        # High IV, low HV → large variance premium → likely expensive
        assert result["MC_VP_Score"] < 1.0

    def test_note_contains_iv_hv(self):
        from scan_engine.mc_variance_premium import mc_variance_premium
        row = self._make_row()
        result = mc_variance_premium(row, n_paths=500)
        assert "HV=" in result["MC_VP_Note"]
        assert "IV=" in result["MC_VP_Note"]

    def test_no_premium_skips(self):
        from scan_engine.mc_variance_premium import mc_variance_premium
        row = self._make_row(Mid_Price=np.nan)
        result = mc_variance_premium(row)
        assert result["MC_VP_Verdict"] == "SKIP"

    def test_put_strategy(self):
        from scan_engine.mc_variance_premium import mc_variance_premium
        row = self._make_row(Strategy_Name="LONG_PUT", Selected_Strike=175.0)
        result = mc_variance_premium(row, n_paths=500)
        assert result["MC_VP_Verdict"] in ("CHEAP", "FAIR", "EXPENSIVE")

    def test_edge_is_per_contract(self):
        from scan_engine.mc_variance_premium import mc_variance_premium
        row = self._make_row()
        result = mc_variance_premium(row, n_paths=500)
        # Edge = (fair - market) × 100
        expected_edge = (result["MC_VP_Premium_Fair"] - 5.50) * 100
        assert abs(result["MC_VP_Edge"] - expected_edge) < 1.0

    def test_reproducible(self):
        from scan_engine.mc_variance_premium import mc_variance_premium
        row = self._make_row()
        r1 = mc_variance_premium(row, n_paths=500, rng=np.random.default_rng(42))
        r2 = mc_variance_premium(row, n_paths=500, rng=np.random.default_rng(42))
        assert r1["MC_VP_Score"] == r2["MC_VP_Score"]
