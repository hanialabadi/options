"""
MC Split Path Evaluation Tests
===============================
Tests for mc_split_evaluation() — Monte Carlo multi-path roll split comparison.

Run:
    pytest test/test_mc_split_evaluation.py -v
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.management.mc_management import mc_split_evaluation


class TestMCSplitBasic:
    """Core split evaluation logic."""

    def _make_row(self, **overrides):
        defaults = {
            "Ticker": "DKNG",
            "Strategy": "BUY_WRITE",
            "Entry_Structure": "BUY_WRITE",
            "last_price": 42.0,
            "UL Last": 42.0,
            "Strike": 44.0,
            "Short_Call_Strike": 44.0,
            "DTE": 15,
            "Short_Call_DTE": 15,
            "Premium_Entry": 1.80,
            "Last": 0.45,
            "Quantity": -8,
            "Net_Cost_Basis_Per_Share": 38.50,
            "hv_current": 42.0,
            "hv_30": 42.0,
            "Option_Type": "CALL",
        }
        defaults.update(overrides)
        return pd.Series(defaults)

    def _make_candidates(self, n=2):
        cands = [
            {
                "strike": 45.0, "dte": 30, "mid": 1.50, "mid_price": 1.50,
                "primary_edge": "INCOME_EXTENSION", "expiry": "2026-04-10",
                "composite_score": 0.78,
            },
            {
                "strike": 43.0, "dte": 45, "mid": 2.80, "mid_price": 2.80,
                "primary_edge": "STRIKE_IMPROVEMENT", "expiry": "2026-04-25",
                "composite_score": 0.72,
            },
            {
                "strike": 46.0, "dte": 30, "mid": 0.90, "mid_price": 0.90,
                "primary_edge": "DEFENSIVE_ROLL", "expiry": "2026-04-10",
                "composite_score": 0.65,
            },
        ]
        return cands[:n]

    def test_basic_evaluation_runs(self):
        row = self._make_row()
        result = mc_split_evaluation(row, self._make_candidates(), 8, n_paths=500)
        assert result["MC_Split_Verdict"] in ("SPLIT_BETTER", "ALL_IN_BETTER", "MARGINAL")
        assert result["MC_Split_Best"] != ""
        assert len(result["MC_Split_Paths"]) >= 3  # at least ALL_TO_BEST, 50/50, PARTIAL_CLOSE

    def test_returns_sorted_by_ev(self):
        row = self._make_row()
        result = mc_split_evaluation(row, self._make_candidates(), 8, n_paths=1000)
        paths = result["MC_Split_Paths"]
        evs = [p["ev"] for p in paths]
        assert evs == sorted(evs, reverse=True), "Paths must be sorted EV descending"

    def test_best_matches_first_path(self):
        row = self._make_row()
        result = mc_split_evaluation(row, self._make_candidates(), 8, n_paths=500)
        assert result["MC_Split_Best"] == result["MC_Split_Paths"][0]["type"]

    def test_all_paths_have_required_fields(self):
        row = self._make_row()
        result = mc_split_evaluation(row, self._make_candidates(), 8, n_paths=500)
        for p in result["MC_Split_Paths"]:
            assert "type" in p
            assert "ev" in p
            assert "p_profit" in p
            assert "cvar_5" in p
            assert "label" in p
            assert 0 <= p["p_profit"] <= 1

    def test_note_includes_stats(self):
        row = self._make_row()
        result = mc_split_evaluation(row, self._make_candidates(), 8, n_paths=500)
        note = result["MC_Split_Note"]
        assert "MC split" in note
        assert "qty=8" in note
        assert "Best=" in note


class TestMCSplitSkipConditions:
    """Tests for conditions that should skip MC split evaluation."""

    def _make_row(self, **overrides):
        defaults = {
            "Ticker": "AAPL",
            "Strategy": "BUY_WRITE",
            "last_price": 180.0,
            "Strike": 185.0,
            "DTE": 20,
            "Premium_Entry": 3.0,
            "Last": 1.0,
            "Quantity": -2,
            "Net_Cost_Basis_Per_Share": 170.0,
            "hv_current": 30.0,
            "Option_Type": "CALL",
        }
        defaults.update(overrides)
        return pd.Series(defaults)

    def test_skip_fewer_than_4_contracts(self):
        row = self._make_row()
        result = mc_split_evaluation(row, [{"strike": 190, "dte": 30, "mid": 2.0}], 3)
        assert result["MC_Split_Verdict"] == "SKIP"
        assert "<4 contracts" in result["MC_Split_Note"]

    def test_skip_no_candidates(self):
        row = self._make_row()
        result = mc_split_evaluation(row, [], 8)
        assert result["MC_Split_Verdict"] == "SKIP"

    def test_skip_no_spot(self):
        row = self._make_row(last_price=0, Last=0, **{"UL Last": 0, "Underlying_Last": 0})
        cands = [{"strike": 190, "dte": 30, "mid": 2.0}]
        result = mc_split_evaluation(row, cands, 8)
        assert result["MC_Split_Verdict"] == "SKIP"

    def test_skip_no_strike(self):
        row = self._make_row(Strike=0, Short_Call_Strike=0)
        cands = [{"strike": 190, "dte": 30, "mid": 2.0}]
        result = mc_split_evaluation(row, cands, 8)
        assert result["MC_Split_Verdict"] == "SKIP"

    def test_skip_invalid_candidates(self):
        row = self._make_row()
        # candidates with strike=0 should be filtered out
        cands = [{"strike": 0, "dte": 30, "mid": 2.0}]
        result = mc_split_evaluation(row, cands, 8)
        assert result["MC_Split_Verdict"] == "SKIP"


class TestMCSplitPathEnumeration:
    """Tests for path generation logic."""

    def _make_row(self, **overrides):
        defaults = {
            "Ticker": "MSFT",
            "Strategy": "BUY_WRITE",
            "last_price": 400.0,
            "Strike": 410.0,
            "DTE": 12,
            "Premium_Entry": 5.0,
            "Last": 1.5,
            "Quantity": -10,
            "Net_Cost_Basis_Per_Share": 380.0,
            "hv_current": 25.0,
            "Option_Type": "CALL",
        }
        defaults.update(overrides)
        return pd.Series(defaults)

    def test_single_candidate_no_stagger(self):
        """With only 1 candidate, STAGGER path should not appear."""
        cands = [{"strike": 415, "dte": 30, "mid": 4.0, "primary_edge": "INCOME_EXTENSION", "expiry": "2026-04-10"}]
        result = mc_split_evaluation(self._make_row(), cands, 10, n_paths=500)
        types = [p["type"] for p in result["MC_Split_Paths"]]
        assert "STAGGER" not in types
        assert "ALL_TO_BEST" in types

    def test_two_candidates_may_stagger(self):
        """With 2 candidates with different edges, STAGGER should appear."""
        cands = [
            {"strike": 415, "dte": 30, "mid": 4.0, "primary_edge": "INCOME_EXTENSION", "expiry": "2026-04-10"},
            {"strike": 405, "dte": 45, "mid": 6.0, "primary_edge": "STRIKE_IMPROVEMENT", "expiry": "2026-04-25"},
        ]
        result = mc_split_evaluation(self._make_row(), cands, 10, n_paths=500)
        types = [p["type"] for p in result["MC_Split_Paths"]]
        assert "STAGGER" in types

    def test_stagger_excluded_weak_liquidity(self):
        """STAGGER excluded when 2nd candidate is WEAK_LIQUIDITY."""
        cands = [
            {"strike": 415, "dte": 30, "mid": 4.0, "primary_edge": "INCOME_EXTENSION", "expiry": "2026-04-10"},
            {"strike": 405, "dte": 45, "mid": 6.0, "primary_edge": "WEAK_LIQUIDITY", "expiry": "2026-04-25"},
        ]
        result = mc_split_evaluation(self._make_row(), cands, 10, n_paths=500)
        types = [p["type"] for p in result["MC_Split_Paths"]]
        assert "STAGGER" not in types

    def test_partial_close_needs_min_contracts(self):
        """PARTIAL_CLOSE requires one_third >= 1 and two_thirds >= 2."""
        # 4 contracts: one_third=1, two_thirds=3 → should appear
        result = mc_split_evaluation(self._make_row(), [{"strike": 415, "dte": 30, "mid": 4.0}], 4, n_paths=500)
        types = [p["type"] for p in result["MC_Split_Paths"]]
        assert "PARTIAL_CLOSE" in types

    def test_split_50_50_vs_70_30_dedup(self):
        """When 50/50 == 70/30 (e.g. qty=4), only one should appear."""
        # qty=4: half=2, seventy=2 → duplicated → 70/30 skipped
        result = mc_split_evaluation(self._make_row(), [{"strike": 415, "dte": 30, "mid": 4.0}], 4, n_paths=500)
        types = [p["type"] for p in result["MC_Split_Paths"]]
        assert types.count("SPLIT_50_50") <= 1
        assert types.count("SPLIT_70_30") <= 1

    def test_tranche_contracts_sum_to_total(self):
        """Every path's tranche contracts should sum to total_contracts."""
        cands = [
            {"strike": 415, "dte": 30, "mid": 4.0, "primary_edge": "INCOME_EXTENSION", "expiry": "2026-04-10"},
            {"strike": 405, "dte": 45, "mid": 6.0, "primary_edge": "STRIKE_IMPROVEMENT", "expiry": "2026-04-25"},
        ]
        result = mc_split_evaluation(self._make_row(), cands, 10, n_paths=500)
        # Tranches are stored as strings like "5×roll_best", parse for validation
        for p in result["MC_Split_Paths"]:
            total = sum(int(t.split("×")[0]) for t in p["tranches"])
            assert total == 10, f"Path {p['type']}: tranche sum {total} != 10"


class TestMCSplitVerdicts:
    """Tests for verdict logic."""

    def _make_row(self, **overrides):
        defaults = {
            "Ticker": "NVDA",
            "Strategy": "BUY_WRITE",
            "last_price": 120.0,
            "Strike": 125.0,
            "DTE": 10,
            "Premium_Entry": 4.0,
            "Last": 1.0,
            "Quantity": -12,
            "Net_Cost_Basis_Per_Share": 110.0,
            "hv_current": 55.0,
            "Option_Type": "CALL",
        }
        defaults.update(overrides)
        return pd.Series(defaults)

    def test_verdict_is_valid(self):
        cands = [{"strike": 130, "dte": 30, "mid": 3.5}]
        result = mc_split_evaluation(self._make_row(), cands, 12, n_paths=500)
        assert result["MC_Split_Verdict"] in ("SPLIT_BETTER", "ALL_IN_BETTER", "MARGINAL")

    def test_deterministic_with_seed(self):
        """Same seed should produce same results."""
        cands = [{"strike": 130, "dte": 30, "mid": 3.5}]
        rng1 = np.random.default_rng(42)
        rng2 = np.random.default_rng(42)
        r1 = mc_split_evaluation(self._make_row(), cands, 12, n_paths=500, rng=rng1)
        r2 = mc_split_evaluation(self._make_row(), cands, 12, n_paths=500, rng=rng2)
        assert r1["MC_Split_Best"] == r2["MC_Split_Best"]
        assert r1["MC_Split_Verdict"] == r2["MC_Split_Verdict"]
        for p1, p2 in zip(r1["MC_Split_Paths"], r2["MC_Split_Paths"]):
            assert p1["ev"] == p2["ev"]


class TestMCSplitStrategyTypes:
    """Tests for different strategy models."""

    def test_short_put_strategy(self):
        row = pd.Series({
            "Ticker": "AMZN",
            "Strategy": "SHORT_PUT",
            "last_price": 180.0,
            "Strike": 170.0,
            "DTE": 20,
            "Premium_Entry": 3.50,
            "Last": 1.20,
            "Quantity": -6,
            "hv_current": 35.0,
            "Option_Type": "PUT",
        })
        cands = [{"strike": 165, "dte": 35, "mid": 2.80}]
        result = mc_split_evaluation(row, cands, 6, n_paths=500)
        assert result["MC_Split_Verdict"] in ("SPLIT_BETTER", "ALL_IN_BETTER", "MARGINAL")

    def test_long_call_strategy(self):
        row = pd.Series({
            "Ticker": "GOOGL",
            "Strategy": "LONG_CALL",
            "last_price": 170.0,
            "Strike": 175.0,
            "DTE": 45,
            "Premium_Entry": 6.0,
            "Last": 4.5,
            "Quantity": 5,
            "hv_current": 28.0,
            "Option_Type": "CALL",
        })
        cands = [{"strike": 180, "dte": 60, "mid": 5.0}]
        result = mc_split_evaluation(row, cands, 5, n_paths=500)
        assert result["MC_Split_Verdict"] in ("SPLIT_BETTER", "ALL_IN_BETTER", "MARGINAL")

    def test_long_put_strategy(self):
        row = pd.Series({
            "Ticker": "META",
            "Strategy": "LONG_PUT",
            "last_price": 500.0,
            "Strike": 490.0,
            "DTE": 30,
            "Premium_Entry": 12.0,
            "Last": 8.0,
            "Quantity": 4,
            "hv_current": 32.0,
            "Option_Type": "PUT",
        })
        cands = [{"strike": 480, "dte": 45, "mid": 10.0}]
        result = mc_split_evaluation(row, cands, 4, n_paths=500)
        assert result["MC_Split_Verdict"] in ("SPLIT_BETTER", "ALL_IN_BETTER", "MARGINAL")

    def test_pmcc_strategy(self):
        row = pd.Series({
            "Ticker": "AAPL",
            "Strategy": "PMCC",
            "Entry_Structure": "PMCC",
            "last_price": 220.0,
            "Strike": 225.0,
            "Short_Call_Strike": 225.0,
            "DTE": 18,
            "Short_Call_DTE": 18,
            "Premium_Entry": 2.50,
            "Last": 0.80,
            "Quantity": -4,
            "Net_Cost_Basis_Per_Share": 200.0,
            "hv_current": 22.0,
            "Option_Type": "CALL",
        })
        cands = [{"strike": 230, "dte": 35, "mid": 2.00}]
        result = mc_split_evaluation(row, cands, 4, n_paths=500)
        assert result["MC_Split_Verdict"] in ("SPLIT_BETTER", "ALL_IN_BETTER", "MARGINAL")


class TestMCSplitRunManagementMC:
    """Tests for integration with run_management_mc."""

    def test_split_columns_preallocated(self):
        from core.management.mc_management import run_management_mc
        df = pd.DataFrame([{
            "Ticker": "TEST",
            "Strategy": "BUY_WRITE",
            "Action": "HOLD",
            "last_price": 50.0,
            "Strike": 52.0,
            "DTE": 30,
            "Premium_Entry": 2.0,
            "Last": 1.0,
            "Quantity": -2,
            "hv_current": 30.0,
        }])
        result = run_management_mc(df, n_paths=100)
        assert "MC_Split_Best" in result.columns
        assert "MC_Split_Note" in result.columns
        assert "MC_Split_Verdict" in result.columns
        assert "MC_Split_Paths" in result.columns

    def test_split_runs_on_roll_with_qty_ge_4(self):
        from core.management.mc_management import run_management_mc
        cand_json = json.dumps({
            "strike": 55.0, "dte": 30, "mid": 2.5, "mid_price": 2.5,
            "primary_edge": "INCOME_EXTENSION", "expiry": "2026-04-10",
        })
        df = pd.DataFrame([{
            "Ticker": "TEST",
            "Strategy": "BUY_WRITE",
            "Action": "ROLL",
            "Execution_Readiness": "EXECUTE_NOW",
            "last_price": 50.0,
            "UL Last": 50.0,
            "Strike": 52.0,
            "DTE": 5,
            "Premium_Entry": 2.0,
            "Last": 0.30,
            "Quantity": -8,
            "Net_Cost_Basis_Per_Share": 45.0,
            "hv_current": 35.0,
            "hv_30": 35.0,
            "Option_Type": "CALL",
            "Roll_Candidate_1": cand_json,
        }])
        result = run_management_mc(df, n_paths=200)
        note = str(result.iloc[0].get("MC_Split_Note", ""))
        assert "MC split" in note or "MC_SKIP" in note
        verdict = str(result.iloc[0].get("MC_Split_Verdict", ""))
        assert verdict in ("SPLIT_BETTER", "ALL_IN_BETTER", "MARGINAL", "SKIP", "")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
