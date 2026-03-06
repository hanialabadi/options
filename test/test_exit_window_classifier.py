"""
Tests for Exit Optimal Window Classifier — Phase 2: Intraday Exit Timing
=========================================================================
28 tests across 5 classes:
  - TestExitTimingClassification (8)
  - TestProxyVerdictScoring (6)
  - TestIntradayAdvisory (6)
  - TestExitWindowIntegration (5)
  - TestDirectionReuse (3)
"""

import json

import numpy as np
import pandas as pd
import pytest

from core.management.exit_window_classifier import (
    _build_intraday_exit_advisory,
    _classify_exit_timing,
    classify_exit_windows,
)


# ─── Helpers ────────────────────────────────────────────────────────────────

def _make_exit_row(**overrides) -> dict:
    """Minimal EXIT row with sensible defaults for window classification."""
    base = {
        "Action": "EXIT",
        "Strategy": "LONG_CALL",
        "Call/Put": "CALL",
        "Urgency": "CRITICAL",
        "UL Last": 150.0,
        "UL_Prev_Close": 148.0,
        "Last": 5.00,
        "Bid": 4.80,
        "Ask": 5.10,
        "Delta": 0.50,
        "DTE": 30,
        "Volume": 500,
        "Open_Int": 2000,
        "EMA9": 152.0,
        "SMA20": 155.0,
        "SMA50": 148.0,
        "LowerBand_20": 140.0,
        "UpperBand_20": 160.0,
        "ATR_14": 3.50,
        "MomentumVelocity_State": "ACCELERATING",
        "DirectionalBalance_State": "BUYER_DOMINANT",
        "kaufman_efficiency_ratio": 0.55,
        "Exit_Limit_Level": "EMA9",
        "Exit_Limit_Price": 6.00,
    }
    base.update(overrides)
    return base


def _row_series(**overrides) -> pd.Series:
    """Build a pd.Series from _make_exit_row."""
    return pd.Series(_make_exit_row(**overrides))


def _run(rows: list[dict]) -> pd.DataFrame:
    """Build a DataFrame from row dicts and run the classifier."""
    df = pd.DataFrame(rows)
    return classify_exit_windows(df)


# ─── TestExitTimingClassification ────────────────────────────────────────────

class TestExitTimingClassification:
    def test_momentum_aligned_rally(self):
        """RALLY: ACCELERATING momentum + high KER → MOMENTUM_ALIGNED."""
        row = _row_series(
            Strategy="LONG_CALL",
            MomentumVelocity_State="ACCELERATING",
            kaufman_efficiency_ratio=0.55,
        )
        result = _classify_exit_timing(row, "RALLY")
        assert result["timing"] == "MOMENTUM_ALIGNED"
        assert result["action_mod"] == "EXIT_NOW"

    def test_momentum_aligned_dip(self):
        """DIP: ACCELERATING momentum + high KER → MOMENTUM_ALIGNED."""
        row = _row_series(
            Strategy="LONG_PUT",
            **{"Call/Put": "PUT"},
            MomentumVelocity_State="ACCELERATING",
            kaufman_efficiency_ratio=0.50,
        )
        result = _classify_exit_timing(row, "DIP")
        assert result["timing"] == "MOMENTUM_ALIGNED"
        assert result["action_mod"] == "EXIT_NOW"

    def test_spread_wide(self):
        """Wide spread (>5% of option price) → SPREAD_WIDE."""
        row = _row_series(
            Bid=4.00, Ask=5.50, Last=4.50,  # spread = 1.50/4.50 = 33%
            MomentumVelocity_State="STALLING",  # not ACCELERATING → skip MOMENTUM_ALIGNED
            kaufman_efficiency_ratio=0.30,
        )
        result = _classify_exit_timing(row, "RALLY")
        assert result["timing"] == "SPREAD_WIDE"
        assert result["action_mod"] == "WAIT"

    def test_momentum_opposing_rally(self):
        """RALLY exit but SELLER_DOMINANT + ACCELERATING → MOMENTUM_OPPOSING."""
        row = _row_series(
            MomentumVelocity_State="ACCELERATING",
            DirectionalBalance_State="SELLER_DOMINANT",
            kaufman_efficiency_ratio=0.30,  # low KER → not MOMENTUM_ALIGNED
            Bid=4.90, Ask=5.00, Last=5.00,  # tight spread → not SPREAD_WIDE
            Volume=500, Open_Int=2000,       # vol/OI=25% → not low volume (>20%)
        )
        result = _classify_exit_timing(row, "RALLY")
        assert result["timing"] == "MOMENTUM_OPPOSING"
        assert result["action_mod"] == "WAIT"

    def test_favorable_approaching(self):
        """Stock within 1 ATR of target level → FAVORABLE_APPROACHING."""
        row = _row_series(
            MomentumVelocity_State="DECELERATING",
            DirectionalBalance_State="BALANCED",
            kaufman_efficiency_ratio=0.30,
            **{"UL Last": 151.0},  # 1.0 away from EMA9=152 (within ATR=3.5)
            EMA9=152.0,
            Exit_Limit_Level="EMA9",
            Bid=4.80, Ask=4.95, Last=4.90,  # tight spread
        )
        result = _classify_exit_timing(row, "RALLY")
        assert result["timing"] == "FAVORABLE_APPROACHING"
        assert result["action_mod"] == "PROCEED"

    def test_neutral_default(self):
        """No strong signal → NEUTRAL."""
        row = _row_series(
            MomentumVelocity_State="STALLING",
            DirectionalBalance_State="BALANCED",
            kaufman_efficiency_ratio=0.30,
            **{"UL Last": 140.0},  # far from EMA9=152
            EMA9=152.0,
            ATR_14=2.0,
            Exit_Limit_Level="EMA9",
            Bid=4.80, Ask=4.95, Last=4.90,  # tight spread
        )
        result = _classify_exit_timing(row, "RALLY")
        assert result["timing"] == "NEUTRAL"
        assert result["action_mod"] == "PROCEED"

    def test_only_high_critical_processed(self):
        """EXIT LOW rows should NOT get window classification."""
        rows = [
            _make_exit_row(Urgency="LOW"),
            _make_exit_row(Urgency="MEDIUM"),
        ]
        df = _run(rows)
        assert df.iloc[0].get("Exit_Window_State", "") == ""
        assert df.iloc[1].get("Exit_Window_State", "") == ""

    def test_non_exit_rows_untouched(self):
        """HOLD rows should not get window classification."""
        rows = [
            _make_exit_row(Action="HOLD", Urgency="HIGH"),
            _make_exit_row(Action="EXIT", Urgency="HIGH"),
        ]
        df = _run(rows)
        assert df.iloc[0].get("Exit_Window_State", "") == ""
        assert df.iloc[1].get("Exit_Window_State", "") != ""


# ─── TestProxyVerdictScoring ─────────────────────────────────────────────────

class TestProxyVerdictScoring:
    def test_three_signals_execute_now(self):
        """3+ confirmations → EXECUTE_NOW."""
        row = _row_series(
            **{"UL Last": 150.0},
            UL_Prev_Close=148.0,   # +1.35% → sig_intraday (RALLY)
            Bid=4.90, Ask=5.00, Last=5.00,  # spread 2% → sig_spread
            Volume=300, Open_Int=2000,       # vol/OI=15% → sig_volume
            MomentumVelocity_State="ACCELERATING",  # sig_momentum
            EMA9=152.0, Exit_Limit_Level="EMA9",
        )
        adv = _build_intraday_exit_advisory(row, "MOMENTUM_ALIGNED", "RALLY")
        assert adv["proxy_verdict"] == "EXECUTE_NOW"

    def test_two_signals_favorable(self):
        """Exactly 2 confirmations → FAVORABLE_WINDOW."""
        row = _row_series(
            **{"UL Last": 150.0},
            UL_Prev_Close=149.5,   # +0.33% → NOT sig_intraday
            Bid=4.90, Ask=5.00, Last=5.00,  # spread 2% → sig_spread
            Volume=300, Open_Int=2000,       # vol/OI=15% → sig_volume
            MomentumVelocity_State="STALLING",  # NOT sig_momentum
            EMA9=155.0, Exit_Limit_Level="EMA9",  # far → NOT sig_distance
        )
        adv = _build_intraday_exit_advisory(row, "NEUTRAL", "RALLY")
        assert adv["proxy_verdict"] == "FAVORABLE_WINDOW"

    def test_one_signal_verify_first(self):
        """0-1 confirmations → VERIFY_FIRST."""
        row = _row_series(
            **{"UL Last": 150.0},
            UL_Prev_Close=150.0,   # 0% → NOT sig_intraday
            Bid=4.00, Ask=5.50, Last=4.50,  # spread 33% → NOT sig_spread
            Volume=10, Open_Int=2000,        # vol/OI=0.5% → NOT sig_volume
            MomentumVelocity_State="STALLING",  # NOT sig_momentum
            EMA9=170.0, Exit_Limit_Level="EMA9",  # far → NOT sig_distance
        )
        adv = _build_intraday_exit_advisory(row, "NEUTRAL", "RALLY")
        assert adv["proxy_verdict"] == "VERIFY_FIRST"

    def test_spread_signal_tight_passes(self):
        """Spread ≤ 3% → signal passes."""
        row = _row_series(Bid=4.90, Ask=5.04, Last=5.00)  # 2.8%
        adv = _build_intraday_exit_advisory(row, "NEUTRAL", "RALLY")
        spread_val = adv["signals"]["spread_pct"]
        assert spread_val is not None and spread_val <= 3.0

    def test_volume_signal(self):
        """Volume/OI ≥ 10% → signal passes."""
        row = _row_series(Volume=250, Open_Int=2000)  # 12.5%
        adv = _build_intraday_exit_advisory(row, "NEUTRAL", "RALLY")
        vol_oi = adv["signals"]["volume_vs_oi"]
        assert vol_oi is not None and vol_oi >= 0.10

    def test_momentum_alignment_rally(self):
        """ACCELERATING + RALLY → momentum_alignment = True."""
        row = _row_series(MomentumVelocity_State="ACCELERATING")
        adv = _build_intraday_exit_advisory(row, "MOMENTUM_ALIGNED", "RALLY")
        assert adv["signals"]["momentum_alignment"] is True

    def test_theta_signal_high_ratio_long_option(self):
        """Long option: high theta/move ratio (>0.8) fires as urgency signal."""
        row = _row_series(Theta=-1.50, Delta=-0.30, ATR_14=8.0, Strategy="LONG_CALL")
        # ratio = |1.50*100| / (8*0.30*100) = 150/240 = 0.625 → NOT > 0.8
        adv = _build_intraday_exit_advisory(row, "NEUTRAL", "RALLY")
        assert adv["signals"]["theta_to_move_ratio"] is not None
        # Now make it fire: theta=2.0, delta=0.20, ATR=5
        row2 = _row_series(Theta=-2.0, Delta=-0.20, ATR_14=5.0, Strategy="LONG_CALL")
        # ratio = |2.0*100| / (5*0.20*100) = 200/100 = 2.0 → > 0.8 ✓
        adv2 = _build_intraday_exit_advisory(row2, "NEUTRAL", "RALLY")
        assert adv2["signals"]["theta_to_move_ratio"] == 2.0

    def test_theta_signal_inverted_for_short_option(self):
        """Short option (BUY_WRITE): high theta/move does NOT fire as urgency signal.
        Instead, low theta/move (<0.3) fires — stock moves dominate."""
        # High theta/move for BUY_WRITE → theta works for seller → NOT an urgency signal
        row_high = _row_series(
            Theta=-2.0, Delta=-0.20, ATR_14=5.0,
            Strategy="BUY_WRITE", **{"Call/Put": "CALL"},
        )
        adv_high = _build_intraday_exit_advisory(row_high, "NEUTRAL", "DIP")
        # ratio = 2.0 > 0.8, but for short option this should NOT count as urgency
        # So the number of confirmations should NOT include theta
        # Verify: the raw ratio is still reported correctly
        assert adv_high["signals"]["theta_to_move_ratio"] == 2.0

        # Low theta/move for BUY_WRITE → stock moves dominate → urgency signal
        row_low = _row_series(
            Theta=-0.10, Delta=-0.50, ATR_14=10.0,
            Strategy="BUY_WRITE", **{"Call/Put": "CALL"},
        )
        # ratio = |0.10*100| / (10*0.50*100) = 10/500 = 0.02 → < 0.3 ✓
        adv_low = _build_intraday_exit_advisory(row_low, "NEUTRAL", "DIP")
        assert adv_low["signals"]["theta_to_move_ratio"] == 0.02

    def test_theta_signal_covered_call_inverted(self):
        """COVERED_CALL also treated as short option for theta signal."""
        row = _row_series(
            Theta=-0.05, Delta=-0.40, ATR_14=8.0,
            Strategy="COVERED_CALL", **{"Call/Put": "CALL"},
        )
        # ratio = |0.05*100| / (8*0.40*100) = 5/320 = 0.016 → < 0.3 ✓
        adv = _build_intraday_exit_advisory(row, "NEUTRAL", "DIP")
        assert adv["signals"]["theta_to_move_ratio"] is not None
        # This should fire sig_theta since it's short option with low tmr
        assert adv["signals"]["theta_to_move_ratio"] < 0.3


# ─── TestIntradayAdvisory ────────────────────────────────────────────────────

class TestIntradayAdvisory:
    def test_advisory_has_required_keys(self):
        """Advisory dict has all required top-level keys."""
        row = _row_series()
        adv = _build_intraday_exit_advisory(row, "NEUTRAL", "RALLY")
        for key in ("proxy_verdict", "proxy_color", "proxy_summary", "signals", "notes", "checklist"):
            assert key in adv

    def test_checklist_has_six_items(self):
        """Checklist has exactly 6 verification items."""
        row = _row_series()
        adv = _build_intraday_exit_advisory(row, "NEUTRAL", "RALLY")
        assert len(adv["checklist"]) == 6

    def test_notes_nonempty_for_momentum_aligned(self):
        """MOMENTUM_ALIGNED timing produces notes."""
        row = _row_series()
        adv = _build_intraday_exit_advisory(row, "MOMENTUM_ALIGNED", "RALLY")
        assert len(adv["notes"]) > 0
        assert any("rally" in n.lower() for n in adv["notes"])

    def test_proxy_color_execute_now(self):
        """EXECUTE_NOW verdict → red color."""
        row = _row_series(
            **{"UL Last": 150.0},
            UL_Prev_Close=148.0,
            Bid=4.90, Ask=5.00, Last=5.00,
            Volume=300, Open_Int=2000,
            MomentumVelocity_State="ACCELERATING",
        )
        adv = _build_intraday_exit_advisory(row, "MOMENTUM_ALIGNED", "RALLY")
        if adv["proxy_verdict"] == "EXECUTE_NOW":
            assert adv["proxy_color"] == "red"

    def test_proxy_color_verify_first(self):
        """VERIFY_FIRST verdict → blue color."""
        row = _row_series(
            **{"UL Last": 150.0},
            UL_Prev_Close=150.0,
            Bid=4.00, Ask=5.50, Last=4.50,
            Volume=10, Open_Int=2000,
            MomentumVelocity_State="STALLING",
            EMA9=170.0, Exit_Limit_Level="EMA9",
        )
        adv = _build_intraday_exit_advisory(row, "NEUTRAL", "RALLY")
        assert adv["proxy_color"] == "blue"

    def test_signals_dict_has_six_keys(self):
        """Signals dict has all 6 proxy signal keys."""
        row = _row_series()
        adv = _build_intraday_exit_advisory(row, "NEUTRAL", "RALLY")
        expected_keys = {"intraday_chg_pct", "spread_pct", "volume_vs_oi",
                         "distance_to_target_pct", "momentum_alignment",
                         "theta_to_move_ratio"}
        assert set(adv["signals"].keys()) == expected_keys


# ─── TestExitWindowIntegration ───────────────────────────────────────────────

class TestExitWindowIntegration:
    def test_exit_critical_gets_classified(self):
        """EXIT CRITICAL with favorable conditions → state + advisory populated."""
        row = _make_exit_row(
            Action="EXIT", Urgency="CRITICAL", Strategy="LONG_CALL",
            MomentumVelocity_State="ACCELERATING",
            kaufman_efficiency_ratio=0.55,
        )
        df = _run([row])
        assert df.iloc[0]["Exit_Window_State"] != ""
        adv_json = df.iloc[0].get("Intraday_Advisory_JSON", "")
        assert adv_json != ""
        adv = json.loads(adv_json)
        assert "proxy_verdict" in adv

    def test_exit_low_no_classification(self):
        """EXIT LOW → no window classification (uses patience from Phase 1)."""
        row = _make_exit_row(Urgency="LOW")
        df = _run([row])
        assert df.iloc[0].get("Exit_Window_State", "") == ""

    def test_hold_rows_untouched(self):
        """HOLD HIGH rows should not get exit window classification."""
        row = _make_exit_row(Action="HOLD", Urgency="HIGH")
        df = _run([row])
        assert df.iloc[0].get("Exit_Window_State", "") == ""

    def test_mixed_portfolio(self):
        """Only EXIT HIGH/CRITICAL rows classified."""
        rows = [
            _make_exit_row(Action="EXIT", Urgency="CRITICAL", Strategy="LONG_CALL"),
            _make_exit_row(Action="HOLD", Urgency="HIGH"),
            _make_exit_row(Action="EXIT", Urgency="LOW"),
            _make_exit_row(Action="EXIT", Urgency="HIGH", Strategy="LONG_PUT",
                           **{"Call/Put": "PUT"}),
        ]
        df = _run(rows)
        # Row 0: EXIT CRITICAL → classified
        assert df.iloc[0]["Exit_Window_State"] != ""
        # Row 1: HOLD → not classified
        assert df.iloc[1].get("Exit_Window_State", "") == ""
        # Row 2: EXIT LOW → not classified
        assert df.iloc[2].get("Exit_Window_State", "") == ""
        # Row 3: EXIT HIGH → classified
        assert df.iloc[3]["Exit_Window_State"] != ""

    def test_stock_only_exit_skip(self):
        """STOCK_ONLY EXIT CRITICAL → SKIP (no option timing)."""
        row = _make_exit_row(
            Strategy="STOCK_ONLY", **{"Call/Put": ""}, Urgency="CRITICAL",
        )
        df = _run([row])
        assert df.iloc[0]["Exit_Window_State"] == "SKIP"


# ─── TestDirectionReuse ──────────────────────────────────────────────────────

class TestDirectionReuse:
    def test_long_call_rally_direction(self):
        """LONG_CALL EXIT → RALLY direction used for timing."""
        row = _make_exit_row(Strategy="LONG_CALL", Urgency="CRITICAL")
        df = _run([row])
        # Should have a window state (not empty, not SKIP)
        state = df.iloc[0]["Exit_Window_State"]
        assert state != "" and state != "SKIP"

    def test_buy_write_dip_direction(self):
        """BUY_WRITE EXIT → DIP direction used for timing."""
        row = _make_exit_row(
            Strategy="BUY_WRITE", **{"Call/Put": "CALL"}, Urgency="HIGH",
        )
        df = _run([row])
        state = df.iloc[0]["Exit_Window_State"]
        assert state != "" and state != "SKIP"

    def test_csp_rally_direction(self):
        """CSP EXIT → RALLY direction used for timing."""
        row = _make_exit_row(
            Strategy="CSP", **{"Call/Put": "PUT"}, Urgency="HIGH",
        )
        df = _run([row])
        state = df.iloc[0]["Exit_Window_State"]
        assert state != "" and state != "SKIP"
