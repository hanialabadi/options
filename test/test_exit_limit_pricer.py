"""
Tests for Exit Limit Pricer — Phase 1: Daily Technical Levels
=============================================================
24 tests across 5 classes:
  - TestExitDirectionClassification (6)
  - TestFavorableLevelSelection (6)
  - TestDeltaApproximation (4)
  - TestPatienceDays (5)
  - TestIntegration (3)
"""

import numpy as np
import pandas as pd
import pytest

from core.management.exit_limit_pricer import (
    _classify_exit_direction,
    _compute_patience_days,
    _compute_theta_to_move_ratio,
    _select_favorable_level,
    compute_exit_limit_prices,
)


# ─── Helpers ────────────────────────────────────────────────────────────────

def _make_exit_row(**overrides) -> dict:
    """Minimal EXIT row with sensible defaults."""
    base = {
        "Action": "EXIT",
        "Strategy": "LONG_CALL",
        "Call/Put": "CALL",
        "UL Last": 150.0,
        "Last": 5.00,
        "Bid": 4.80,
        "Ask": 5.20,
        "Delta": 0.50,
        "DTE": 30,
        "Urgency": "MEDIUM",
        "EMA9": 152.0,
        "SMA20": 155.0,
        "SMA50": 148.0,
        "LowerBand_20": 140.0,
        "UpperBand_20": 160.0,
        "MomentumVelocity_State": "ACCELERATING",
        "Short_Call_Last": 0,
        "Short_Call_Delta": 0,
    }
    base.update(overrides)
    return base


def _run(rows: list[dict]) -> pd.DataFrame:
    """Build a DataFrame from row dicts and run the pricer."""
    df = pd.DataFrame(rows)
    return compute_exit_limit_prices(df)


# ─── TestExitDirectionClassification ────────────────────────────────────────

class TestExitDirectionClassification:
    def test_long_call_is_rally(self):
        assert _classify_exit_direction("LONG_CALL", "CALL") == "RALLY"

    def test_long_put_is_dip(self):
        assert _classify_exit_direction("LONG_PUT", "PUT") == "DIP"

    def test_buy_write_is_dip(self):
        assert _classify_exit_direction("BUY_WRITE", "CALL") == "DIP"

    def test_covered_call_is_dip(self):
        assert _classify_exit_direction("COVERED_CALL", "CALL") == "DIP"

    def test_csp_is_rally(self):
        assert _classify_exit_direction("CSP", "PUT") == "RALLY"

    def test_stock_only_is_skip(self):
        assert _classify_exit_direction("STOCK_ONLY", "") == "SKIP"


# ─── TestFavorableLevelSelection ────────────────────────────────────────────

class TestFavorableLevelSelection:
    def test_rally_ema9_above_selected(self):
        """RALLY: EMA9 above current → pick EMA9 as nearest."""
        target, label = _select_favorable_level(
            ul_last=150, ema9=152, sma20=155, sma50=148,
            lower_band=140, upper_band=160, direction="RALLY",
        )
        assert label == "EMA9"
        assert target == 152

    def test_rally_fallthrough_to_sma20(self):
        """RALLY: EMA9 below current → fall through to SMA20."""
        target, label = _select_favorable_level(
            ul_last=150, ema9=148, sma20=155, sma50=148,
            lower_band=140, upper_band=160, direction="RALLY",
        )
        assert label == "SMA20"
        assert target == 155

    def test_rally_fallback_to_current(self):
        """RALLY: all levels below → fallback to current price."""
        target, label = _select_favorable_level(
            ul_last=170, ema9=165, sma20=160, sma50=155,
            lower_band=150, upper_band=165, direction="RALLY",
        )
        assert label == "Current"
        assert target == 170

    def test_dip_ema9_below_selected(self):
        """DIP: EMA9 below current → pick EMA9 as nearest."""
        target, label = _select_favorable_level(
            ul_last=150, ema9=148, sma20=145, sma50=155,
            lower_band=140, upper_band=160, direction="DIP",
        )
        assert label == "EMA9"
        assert target == 148

    def test_dip_fallthrough_to_sma20(self):
        """DIP: EMA9 above current → fall through to SMA20."""
        target, label = _select_favorable_level(
            ul_last=150, ema9=152, sma20=145, sma50=155,
            lower_band=140, upper_band=160, direction="DIP",
        )
        assert label == "SMA20"
        assert target == 145

    def test_dip_fallback_to_current(self):
        """DIP: all levels above → fallback to current price."""
        target, label = _select_favorable_level(
            ul_last=130, ema9=135, sma20=140, sma50=145,
            lower_band=135, upper_band=150, direction="DIP",
        )
        assert label == "Current"
        assert target == 130


# ─── TestDeltaApproximation ─────────────────────────────────────────────────

class TestDeltaApproximation:
    def test_long_call_rally_to_ema9(self):
        """LONG_CALL: stock at $150, EMA9 at $152 → call gains delta×$2."""
        row = _make_exit_row(
            Strategy="LONG_CALL", Last=5.00, Delta=0.50,
            UL_Last=150.0, EMA9=152.0,
        )
        # Override UL Last (dict key with space)
        row["UL Last"] = 150.0
        df = _run([row])
        price = df.iloc[0]["Exit_Limit_Price"]
        # 5.00 + 0.50 * 2 = 6.00
        assert abs(price - 6.00) < 0.01

    def test_long_put_dip_to_sma20(self):
        """LONG_PUT: stock at $150, SMA20 at $145 → put gains delta×$5."""
        row = _make_exit_row(
            Strategy="LONG_PUT", **{"Call/Put": "PUT"},
            Last=3.00, Delta=0.40,
            EMA9=152.0,   # above current → not selected for DIP
            SMA20=145.0,  # below current → selected
        )
        row["UL Last"] = 150.0
        df = _run([row])
        price = df.iloc[0]["Exit_Limit_Price"]
        # 3.00 + 0.40 * 5 = 5.00
        assert abs(price - 5.00) < 0.01

    def test_buy_write_buyback_cheaper_on_dip(self):
        """BUY_WRITE: buying back short call → cheaper when stock dips."""
        row = _make_exit_row(
            Strategy="BUY_WRITE", **{"Call/Put": "CALL"},
            Short_Call_Last=2.50, Short_Call_Delta=0.40,
            EMA9=148.0,  # below current → selected for DIP
        )
        row["UL Last"] = 150.0
        df = _run([row])
        price = df.iloc[0]["Exit_Limit_Price"]
        # 2.50 - 0.40 * 2 = 1.70
        assert abs(price - 1.70) < 0.01

    def test_floor_guard_at_bid(self):
        """Long option: limit price never below Bid."""
        row = _make_exit_row(
            Strategy="LONG_CALL", Last=0.10, Bid=0.05, Delta=0.01,
            EMA9=150.5,  # tiny move → limit might be below bid
        )
        row["UL Last"] = 150.0
        df = _run([row])
        price = df.iloc[0]["Exit_Limit_Price"]
        # Even with delta approx, floor is Bid
        assert price >= 0.05


# ─── TestPatienceDays ───────────────────────────────────────────────────────

class TestPatienceDays:
    def test_critical_urgency(self):
        assert _compute_patience_days("CRITICAL", 30, "", "RALLY") == 0

    def test_high_urgency(self):
        assert _compute_patience_days("HIGH", 30, "", "RALLY") == 1

    def test_medium_urgency(self):
        assert _compute_patience_days("MEDIUM", 30, "", "RALLY") == 2

    def test_dte_override(self):
        """DTE ≤ 3 → 0 regardless of urgency."""
        assert _compute_patience_days("LOW", 2, "", "RALLY") == 0

    def test_reversing_override(self):
        """REVERSING momentum → 0 regardless of urgency."""
        assert _compute_patience_days("LOW", 30, "REVERSING", "RALLY") == 0

    def test_theta_to_move_override_long_option(self):
        """Long option: theta_to_move_ratio > 0.8 → 0 (theta eats position)."""
        assert _compute_patience_days("LOW", 30, "", "RALLY", theta_to_move_ratio=0.9) == 0

    def test_theta_to_move_below_threshold(self):
        """theta_to_move_ratio < 0.8 → normal patience."""
        assert _compute_patience_days("LOW", 30, "", "RALLY", theta_to_move_ratio=0.3) == 3

    def test_theta_to_move_short_option_increases_patience(self):
        """Short option: high theta/move → +1 patience (theta works for seller)."""
        # BASE patience for MEDIUM = 2, +1 = 3
        result = _compute_patience_days(
            "MEDIUM", 30, "", "DIP", theta_to_move_ratio=0.9, is_short_option=True,
        )
        assert result == 3  # 2 + 1

    def test_theta_to_move_short_option_capped_at_3(self):
        """Short option patience boost capped at 3 days max."""
        # BASE patience for LOW = 3, +1 would be 4, but capped at 3
        result = _compute_patience_days(
            "LOW", 30, "", "DIP", theta_to_move_ratio=1.5, is_short_option=True,
        )
        assert result == 3  # min(3+1, 3) = 3

    def test_theta_to_move_short_option_not_override_to_zero(self):
        """Short option: high theta/move must NOT override patience to 0."""
        result = _compute_patience_days(
            "LOW", 30, "", "DIP", theta_to_move_ratio=0.9, is_short_option=True,
        )
        assert result > 0  # NOT 0 like long options


class TestThetaToMoveRatio:
    def test_basic_computation(self):
        """Standard case: theta=0.42, delta=0.50, ATR=10 → 0.084."""
        ratio = _compute_theta_to_move_ratio(0.42, 0.50, 10.0)
        assert abs(ratio - 0.084) < 0.001

    def test_high_theta_ratio(self):
        """Near expiration: theta=1.50, delta=0.30, ATR=8 → 0.625."""
        ratio = _compute_theta_to_move_ratio(1.50, 0.30, 8.0)
        assert abs(ratio - 0.625) < 0.001

    def test_zero_atr_returns_nan(self):
        """ATR=0 → NaN (can't divide by zero)."""
        ratio = _compute_theta_to_move_ratio(0.42, 0.50, 0.0)
        assert np.isnan(ratio)

    def test_zero_delta_returns_nan(self):
        """Delta=0 → NaN (no directional exposure)."""
        ratio = _compute_theta_to_move_ratio(0.42, 0.0, 10.0)
        assert np.isnan(ratio)


# ─── TestIntegration ────────────────────────────────────────────────────────

class TestIntegration:
    def test_non_exit_rows_untouched(self):
        """HOLD rows should not get Exit_Limit_Price populated."""
        rows = [
            _make_exit_row(Action="HOLD", Strategy="LONG_CALL"),
            _make_exit_row(Action="EXIT", Strategy="LONG_CALL"),
        ]
        df = _run(rows)
        assert pd.isna(df.iloc[0]["Exit_Limit_Price"])
        assert pd.notna(df.iloc[1]["Exit_Limit_Price"])

    def test_stock_only_skip(self):
        """STOCK_ONLY EXIT → SKIP direction, no limit price."""
        row = _make_exit_row(Strategy="STOCK_ONLY", **{"Call/Put": ""})
        df = _run([row])
        assert pd.isna(df.iloc[0]["Exit_Limit_Price"])
        assert df.iloc[0]["Exit_Limit_Level"] == "SKIP"

    def test_mixed_portfolio(self):
        """Mix of EXIT + HOLD + ROLL → only EXIT rows priced."""
        rows = [
            _make_exit_row(Action="EXIT", Strategy="LONG_CALL"),
            _make_exit_row(Action="HOLD", Strategy="BUY_WRITE"),
            _make_exit_row(Action="ROLL", Strategy="LONG_PUT"),
            _make_exit_row(Action="EXIT", Strategy="LONG_PUT", **{"Call/Put": "PUT"},
                           EMA9=148.0, SMA20=145.0),
        ]
        df = _run(rows)
        # Row 0: EXIT LONG_CALL → priced
        assert pd.notna(df.iloc[0]["Exit_Limit_Price"])
        # Row 1: HOLD → not priced
        assert pd.isna(df.iloc[1]["Exit_Limit_Price"])
        # Row 2: ROLL → not priced
        assert pd.isna(df.iloc[2]["Exit_Limit_Price"])
        # Row 3: EXIT LONG_PUT → priced
        assert pd.notna(df.iloc[3]["Exit_Limit_Price"])

    def test_buy_write_high_theta_patience_not_zero(self):
        """BUY_WRITE with high theta/move → patience should NOT be 0."""
        row = _make_exit_row(
            Strategy="BUY_WRITE",
            **{"Call/Put": "CALL"},
            Short_Call_Last=1.50,
            Short_Call_Delta=0.20,
            Theta=-2.0,           # high theta
            ATR_14=5.0,           # ratio = |2.0*100|/(5*0.20*100) = 2.0 > 0.8
            Urgency="MEDIUM",     # base = 2
            DTE=30,
            EMA9=148.0,           # below UL Last → DIP direction
            SMA20=145.0,
        )
        df = _run([row])
        patience = df.iloc[0]["Exit_Limit_Patience_Days"]
        # Should be 3 (base 2 + 1 for short option theta benefit), NOT 0
        assert patience == 3

    def test_roll_buy_write_gets_buyback_pricing(self):
        """ROLL BUY_WRITE gets buyback limit pricing (close-leg)."""
        row = _make_exit_row(
            Action="ROLL",
            Strategy="BUY_WRITE",
            **{"Call/Put": "CALL"},
            Short_Call_Last=1.30,
            Short_Call_Delta=0.16,
            Urgency="HIGH",
            DTE=43,
            EMA9=148.0,           # below UL Last → DIP direction
            SMA20=145.0,
        )
        df = _run([row])
        assert pd.notna(df.iloc[0]["Exit_Limit_Price"])
        assert df.iloc[0]["Exit_Limit_Level"] != ""

    def test_roll_covered_call_gets_buyback_pricing(self):
        """ROLL COVERED_CALL also gets buyback pricing."""
        row = _make_exit_row(
            Action="ROLL",
            Strategy="COVERED_CALL",
            **{"Call/Put": "CALL"},
            Short_Call_Last=2.50,
            Short_Call_Delta=0.35,
            Urgency="MEDIUM",
            DTE=30,
            EMA9=148.0,
            SMA20=145.0,
        )
        df = _run([row])
        assert pd.notna(df.iloc[0]["Exit_Limit_Price"])

    def test_roll_long_call_does_not_get_pricing(self):
        """ROLL LONG_CALL should NOT get exit limit pricing."""
        row = _make_exit_row(
            Action="ROLL",
            Strategy="LONG_CALL",
            **{"Call/Put": "CALL"},
        )
        df = _run([row])
        assert pd.isna(df.iloc[0]["Exit_Limit_Price"])

    def test_roll_wait_buy_write_gets_pricing(self):
        """ROLL_WAIT BUY_WRITE also gets buyback pricing."""
        row = _make_exit_row(
            Action="ROLL_WAIT",
            Strategy="BUY_WRITE",
            **{"Call/Put": "CALL"},
            Short_Call_Last=1.00,
            Short_Call_Delta=0.10,
            Urgency="LOW",
            DTE=60,
            EMA9=148.0,
            SMA20=145.0,
        )
        df = _run([row])
        assert pd.notna(df.iloc[0]["Exit_Limit_Price"])
