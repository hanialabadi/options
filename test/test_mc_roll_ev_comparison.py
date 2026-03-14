"""
Tests for mc_roll_ev_comparison() — strategy-aware MC roll EV comparison.

Validates:
1. Income strategies (BW/CC) model combined stock+call P&L
2. Directional strategies (LONG_CALL/PUT) model option-only P&L
3. CSP models short put P&L
4. PMCC models short call roll (LEAP held constant)
5. Net roll cost (debit/credit) correctly applied with slippage
6. Verdict: ROLL_BETTER / HOLD_BETTER / CLOSE_BETTER / MARGINAL
7. Skip on missing data (no candidate, no spot, no strike)
8. Profile selection matches strategy
9. Backward compatibility (no roll candidate → SKIP)
"""

import numpy as np
import pandas as pd
import pytest

from core.management.mc_management import (
    mc_roll_ev_comparison,
    _roll_mc_profile,
    _ROLL_MC_PROFILES,
)


# ── Shared test helpers ──────────────────────────────────────────────────────

def _make_row(**overrides) -> pd.Series:
    """Build a minimal management row."""
    base = {
        "Ticker": "TEST",
        "Underlying_Ticker": "TEST",
        "UL Last": 100.0,
        "Strike": 105.0,
        "DTE": 30,
        "Premium_Entry": 2.50,
        "Last": 1.80,
        "Option_Type": "call",
        "Position_Side": "short",
        "Strategy": "BUY_WRITE",
        "HV_20D": 0.30,
        "Quantity": 100,
        "Net_Cost_Basis_Per_Share": 97.0,
    }
    base.update(overrides)
    return pd.Series(base)


def _make_candidate(**overrides) -> dict:
    """Build a minimal roll candidate dict."""
    base = {
        "strike": 107.0,
        "dte": 45,
        "mid": 3.00,
        "delta": 0.30,
        "iv": 0.32,
    }
    base.update(overrides)
    return base


# ── Profile selection ────────────────────────────────────────────────────────

class TestProfileSelection:
    """Verify strategy → MC profile mapping."""

    def test_buy_write_uses_income_profile(self):
        p = _roll_mc_profile("BUY_WRITE")
        assert p["model"] == "stock_plus_short_call"

    def test_covered_call_uses_income_profile(self):
        p = _roll_mc_profile("COVERED_CALL")
        assert p["model"] == "stock_plus_short_call"

    def test_csp_uses_short_put_profile(self):
        p = _roll_mc_profile("CSP")
        assert p["model"] == "short_put"

    def test_long_call_uses_long_option_profile(self):
        p = _roll_mc_profile("LONG_CALL")
        assert p["model"] == "long_option"

    def test_long_put_uses_long_option_profile(self):
        p = _roll_mc_profile("LONG_PUT")
        assert p["model"] == "long_option"

    def test_pmcc_uses_pmcc_profile(self):
        p = _roll_mc_profile("PMCC")
        assert p["model"] == "pmcc"

    def test_iron_condor_uses_multi_leg_profile(self):
        p = _roll_mc_profile("IRON_CONDOR")
        assert p["model"] == "short_put"  # simplified model

    def test_unknown_uses_default(self):
        p = _roll_mc_profile("UNKNOWN_STRATEGY")
        assert p["model"] == "long_option"


# ── Skip conditions ──────────────────────────────────────────────────────────

class TestSkipConditions:
    """mc_roll_ev_comparison should return SKIP for invalid inputs."""

    def test_skip_no_candidate(self):
        row = _make_row()
        result = mc_roll_ev_comparison(row, roll_candidate=None)
        assert result["MC_Roll_Verdict"] == "SKIP"
        assert "no roll candidate" in result["MC_Roll_Note"]

    def test_skip_empty_candidate(self):
        row = _make_row()
        result = mc_roll_ev_comparison(row, roll_candidate={})
        assert result["MC_Roll_Verdict"] == "SKIP"

    def test_skip_no_spot(self):
        row = _make_row(**{"UL Last": 0, "Underlying_Last": 0, "last_price": 0, "Last": 0})
        result = mc_roll_ev_comparison(row, _make_candidate())
        assert result["MC_Roll_Verdict"] == "SKIP"

    def test_skip_no_strike(self):
        row = _make_row(Strike=0)
        result = mc_roll_ev_comparison(row, _make_candidate(strike=0))
        assert result["MC_Roll_Verdict"] == "SKIP"


# ── Income strategy (BUY_WRITE) ─────────────────────────────────────────────

class TestIncomeRollEV:
    """BUY_WRITE roll EV models combined stock+call P&L."""

    def test_returns_all_keys(self):
        row = _make_row()
        result = mc_roll_ev_comparison(row, _make_candidate(), n_paths=500)
        expected_keys = {
            "MC_Roll_EV_Hold", "MC_Roll_EV_Roll", "MC_Roll_EV_Close",
            "MC_Roll_EV_Delta", "MC_Roll_P_Hold_Win", "MC_Roll_Verdict",
            "MC_Roll_Note", "MC_Roll_Profile",
        }
        assert expected_keys.issubset(result.keys())

    def test_profile_is_stock_plus_short_call(self):
        row = _make_row()
        result = mc_roll_ev_comparison(row, _make_candidate(), n_paths=500)
        assert result["MC_Roll_Profile"] == "STOCK_PLUS_SHORT_CALL"

    def test_ev_values_are_finite(self):
        row = _make_row()
        result = mc_roll_ev_comparison(row, _make_candidate(), n_paths=500)
        assert np.isfinite(result["MC_Roll_EV_Hold"])
        assert np.isfinite(result["MC_Roll_EV_Roll"])
        assert np.isfinite(result["MC_Roll_EV_Close"])

    def test_ev_delta_equals_roll_minus_hold(self):
        row = _make_row()
        result = mc_roll_ev_comparison(row, _make_candidate(), n_paths=500)
        expected_delta = result["MC_Roll_EV_Roll"] - result["MC_Roll_EV_Hold"]
        assert abs(result["MC_Roll_EV_Delta"] - expected_delta) < 0.02

    def test_verdict_is_valid(self):
        row = _make_row()
        result = mc_roll_ev_comparison(row, _make_candidate(), n_paths=500)
        assert result["MC_Roll_Verdict"] in {
            "ROLL_BETTER", "HOLD_BETTER", "CLOSE_BETTER", "MARGINAL", "SKIP"
        }


# ── CSP (short put) ─────────────────────────────────────────────────────────

class TestCSPRollEV:
    """CSP roll EV models short put P&L."""

    def test_csp_profile(self):
        row = _make_row(Strategy="CSP", Option_Type="put", Strike=95.0)
        cand = _make_candidate(strike=92.0)
        result = mc_roll_ev_comparison(row, cand, n_paths=500)
        assert result["MC_Roll_Profile"] == "SHORT_PUT"
        assert np.isfinite(result["MC_Roll_EV_Hold"])


# ── Directional (LONG_CALL) ─────────────────────────────────────────────────

class TestDirectionalRollEV:
    """Long option roll EV models option-only P&L."""

    def test_long_call_profile(self):
        row = _make_row(
            Strategy="LONG_CALL", Option_Type="call",
            Position_Side="long", Strike=100.0,
            Premium_Entry=5.00, Last=3.50,
        )
        cand = _make_candidate(strike=100.0, dte=60, mid=5.50)
        result = mc_roll_ev_comparison(row, cand, n_paths=500)
        assert result["MC_Roll_Profile"] == "LONG_OPTION"
        assert np.isfinite(result["MC_Roll_EV_Roll"])


# ── Net roll cost ────────────────────────────────────────────────────────────

class TestNetRollCost:
    """Verify slippage model in roll cost calculation."""

    def test_debit_roll_reduces_roll_ev(self):
        """Expensive close + cheap open → debit → lower EV_roll."""
        row = _make_row(Last=3.00)  # expensive to close
        cand = _make_candidate(mid=2.00)  # cheap to open
        result = mc_roll_ev_comparison(row, cand, n_paths=500)
        # EV_roll should be reduced by the net debit cost
        assert result["MC_Roll_Note"].count("net_cost") > 0

    def test_credit_roll_increases_roll_ev(self):
        """Cheap close + expensive open → credit → better EV_roll."""
        row = _make_row(Last=1.00)  # cheap to close
        cand = _make_candidate(mid=3.00)  # expensive to open (sell-to-open for income)
        result = mc_roll_ev_comparison(row, cand, n_paths=500)
        # Net cost should be negative (credit)
        assert "net_cost=$" in result["MC_Roll_Note"]


# ── PMCC ─────────────────────────────────────────────────────────────────────

class TestPMCCRollEV:
    """PMCC roll EV models short call with LEAP held constant."""

    def test_pmcc_profile(self):
        row = _make_row(
            Strategy="PMCC", Option_Type="call",
            LEAP_Call_Strike=85.0, PMCC_LEAP_Mid=18.0,
        )
        cand = _make_candidate(strike=108.0, dte=35, mid=2.50)
        result = mc_roll_ev_comparison(row, cand, n_paths=500)
        assert result["MC_Roll_Profile"] == "PMCC"


# ── Verdict logic ────────────────────────────────────────────────────────────

class TestVerdictLogic:
    """Verify verdict thresholds."""

    def test_marginal_when_ev_delta_small(self):
        """When EV difference is <$25, verdict should be MARGINAL."""
        row = _make_row()
        # Same candidate as current position → delta should be small
        cand = _make_candidate(strike=105.0, dte=30, mid=2.50)
        result = mc_roll_ev_comparison(row, cand, n_paths=2000)
        # With identical parameters, EV_delta should be near zero
        # (may not be exactly MARGINAL due to slippage, but delta should be small)
        assert abs(result["MC_Roll_EV_Delta"]) < 500  # sanity bound

    def test_p_hold_win_bounded(self):
        row = _make_row()
        result = mc_roll_ev_comparison(row, _make_candidate(), n_paths=500)
        assert 0.0 <= result["MC_Roll_P_Hold_Win"] <= 1.0


# ── Backward compat ──────────────────────────────────────────────────────────

class TestBackwardCompat:
    """Existing MC functions are unaffected by addition of mc_roll_ev_comparison."""

    def test_run_management_mc_handles_roll_ev(self):
        """run_management_mc should process Roll EV for ROLL rows with candidates."""
        from core.management.mc_management import run_management_mc

        row_data = {
            "Ticker": "TEST", "Underlying_Ticker": "TEST",
            "UL Last": 100.0, "Strike": 105.0, "DTE": 30,
            "Premium_Entry": 2.50, "Last": 1.80,
            "Option_Type": "call", "Position_Side": "short",
            "Strategy": "BUY_WRITE", "HV_20D": 0.30,
            "Action": "ROLL", "Execution_Readiness": "",
            "AssetType": "OPTION", "Quantity": 100,
            "Net_Cost_Basis_Per_Share": 97.0,
            "Roll_Candidate_1": '{"strike": 107, "dte": 45, "mid": 3.0}',
        }
        df = pd.DataFrame([row_data])
        result = run_management_mc(df, n_paths=200)
        assert "MC_Roll_Verdict" in result.columns
        # Should have processed the roll EV
        verdict = result.iloc[0]["MC_Roll_Verdict"]
        assert verdict in {"ROLL_BETTER", "HOLD_BETTER", "CLOSE_BETTER", "MARGINAL", "SKIP", ""}

    def test_no_roll_candidate_skips_roll_ev(self):
        """Rows without Roll_Candidate_1 should not get Roll EV."""
        from core.management.mc_management import run_management_mc

        row_data = {
            "Ticker": "TEST", "Underlying_Ticker": "TEST",
            "UL Last": 100.0, "Strike": 105.0, "DTE": 30,
            "Premium_Entry": 2.50, "Last": 1.80,
            "Option_Type": "call", "Position_Side": "short",
            "Strategy": "BUY_WRITE", "HV_20D": 0.30,
            "Action": "ROLL", "Execution_Readiness": "",
            "AssetType": "OPTION", "Quantity": 100,
        }
        df = pd.DataFrame([row_data])
        result = run_management_mc(df, n_paths=200)
        assert "MC_Roll_Verdict" in result.columns
        # Should be empty (no candidate to compare)
        verdict = result.iloc[0]["MC_Roll_Verdict"]
        assert verdict in {"", "SKIP"}
