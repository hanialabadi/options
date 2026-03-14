"""
Tests for High-HV Recovery CC Override.

Gate 4 in _cc_arbitration() normally blocks CC when HV > 80% and daily 1σ > 3× premium.
For RECOVERY/DEEP_RECOVERY positions, it should return WRITE_CALL_CONSTRAINED instead,
allowing chart-aware constrained strike selection with momentum filter.
"""

import numpy as np
import pandas as pd
import pytest

from core.management.cycle3.cc_opportunity_engine import (
    _cc_arbitration,
    _chart_resistance_levels,
    _refilter_constrained,
    _constrained_watch_signal,
)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _base_row(**overrides) -> pd.Series:
    """UUUU-like RECOVERY position with 98% HV that triggers Gate 4."""
    base = {
        "Strategy": "STOCK_ONLY",
        "AssetType": "STOCK",
        "Underlying_Ticker": "UUUU",
        "Symbol": "UUUU",
        "UL Last": 19.03,
        "Last": 19.03,
        "Net_Cost_Basis_Per_Share": 24.19,
        "Quantity": 600,
        "HV_20D": 0.98,
        "HV_10D": 0.95,
        "IV_30D": 1.05,           # IV > HV → passes Gate 2
        "IV_Entry": 0.0,
        "Thesis_State": "INTACT",
        "Price_Drift_Pct": -0.21,  # -21% → RECOVERY
        "TrendIntegrity_State": "",
        "MomentumVelocity_State": "",
        "roc_5": -0.01,            # flat → passes Gate 3 + momentum filter
        "adx_14": 15.0,            # low → passes Gate 3
        "rsi_14": 42.0,            # not overbought
        "HV_Daily_Move_1Sigma": 1.17,
        "iv_surface_shape": "",
        "EMA9": 18.5,
        "SMA20": 19.8,
        "SMA50": 22.0,
        "UpperBand_20": 21.5,
        "LowerBand_20": 17.5,
    }
    base.update(overrides)
    return pd.Series(base)


def _candidate(strike=22.0, delta=0.15, dte=10, mid=0.25, ann_yield=0.12,
               bucket="WEEKLY", **kw):
    """Minimal CC candidate dict."""
    c = {
        "strike": strike, "delta": delta, "dte": dte, "mid": mid,
        "ann_yield": ann_yield, "bucket": bucket, "oi": 200,
    }
    c.update(kw)
    return c


# ── TestHighHVRecoveryCCArbitration ──────────────────────────────────────────

class TestHighHVRecoveryCCArbitration:
    """Gate 4 returns WRITE_CALL_CONSTRAINED for RECOVERY mode with high HV."""

    def test_recovery_hv98_returns_constrained(self):
        row = _base_row()
        verdict, reason = _cc_arbitration(row, True, "", recovery_mode="RECOVERY")
        assert verdict == "WRITE_CALL_CONSTRAINED"
        assert "HIGH_HV_RECOVERY" in reason

    def test_income_hv98_returns_write_call(self):
        """INCOME + high HV → normal WRITE_CALL (rich premiums, sell into vol)."""
        row = _base_row(Price_Drift_Pct=0.0)  # INCOME mode
        verdict, reason = _cc_arbitration(row, True, "", recovery_mode="INCOME")
        assert verdict == "WRITE_CALL", f"Expected WRITE_CALL, got {verdict}"
        assert "HIGH_HV_INCOME" in reason
        assert "CONSTRAINED" not in verdict

    def test_deep_recovery_hv85_returns_constrained(self):
        row = _base_row(HV_20D=0.85, Price_Drift_Pct=-0.30)
        # daily_move = 0.85/√252 * 19.03 ≈ 1.02; premium ≈ 0.4*0.85*19.03/√52/5 ≈ 0.18
        # 1.02 > 3 * 0.18 = 0.54 → Gate 4 fires
        verdict, reason = _cc_arbitration(row, True, "", recovery_mode="DEEP_RECOVERY")
        assert verdict == "WRITE_CALL_CONSTRAINED"

    def test_structural_damage_returns_hold_stock(self):
        row = _base_row(Price_Drift_Pct=-0.40, Thesis_State="INTACT")
        # Gate 1 fires at drift < -35%
        verdict, _ = _cc_arbitration(row, True, "", recovery_mode="STRUCTURAL_DAMAGE")
        assert verdict == "HOLD_STOCK"

    def test_recovery_low_hv_passes_normally(self):
        row = _base_row(HV_20D=0.50)
        # Gate 4 only fires when HV > 80%; at 50% it should pass
        verdict, _ = _cc_arbitration(row, True, "", recovery_mode="RECOVERY")
        assert verdict == "WRITE_CALL"

    def test_recovery_broken_thesis_no_override(self):
        # BROKEN thesis forces INCOME mode in _classify_recovery_mode,
        # but callers may pass INCOME directly; verify INCOME blocks
        row = _base_row(Thesis_State="BROKEN")
        verdict, _ = _cc_arbitration(row, True, "", recovery_mode="INCOME")
        assert verdict != "WRITE_CALL_CONSTRAINED"


# ── TestChartResistanceLevels ────────────────────────────────────────────────

class TestChartResistanceLevels:
    """_chart_resistance_levels extracts valid resistance above spot."""

    def test_returns_levels_above_spot(self):
        row = _base_row()  # spot=19.03, SMA50=22.0, UpperBand=21.5
        levels = _chart_resistance_levels(row, 19.03)
        assert 22.0 in levels
        assert 21.5 in levels

    def test_ignores_levels_below_spot(self):
        row = _base_row(SMA50=18.0, UpperBand_20=19.0)  # below spot×1.05
        levels = _chart_resistance_levels(row, 19.03)
        # SMA50=18 is below 19.03×1.05=19.98; UpperBand=19 also below
        assert 18.0 not in levels
        assert 19.0 not in levels

    def test_handles_nan_and_zero(self):
        row = _base_row(SMA50=np.nan, UpperBand_20=0.0, SMA20=np.nan, EMA9=0.0)
        levels = _chart_resistance_levels(row, 19.03)
        assert isinstance(levels, list)

    def test_sorts_ascending(self):
        row = _base_row(SMA50=25.0, UpperBand_20=23.0, SMA20=22.0, EMA9=21.0)
        levels = _chart_resistance_levels(row, 19.03)
        assert levels == sorted(levels)


# ── TestMomentumFilter ───────────────────────────────────────────────────────

class TestMomentumFilter:
    """Momentum filter in _refilter_constrained blocks during active rallies."""

    def test_roc5_rally_blocks(self):
        row = _base_row(roc_5=0.04)  # 4% rally
        cands = [_candidate()]
        result, reason = _refilter_constrained(cands, 19.03, 24.19, row)
        assert result == []
        assert "rally" in reason.lower() or "Recovery" in reason

    def test_rsi_overbought_with_momentum_blocks(self):
        row = _base_row(rsi_14=65.0, roc_5=0.01)
        cands = [_candidate()]
        result, reason = _refilter_constrained(cands, 19.03, 24.19, row)
        assert result == []
        assert "overbought" in reason.lower() or "RSI" in reason

    def test_flat_roc_passes(self):
        row = _base_row(roc_5=-0.01, rsi_14=42.0)
        cands = [_candidate(strike=22.5, delta=0.15, dte=10, mid=0.25)]
        result, _ = _refilter_constrained(cands, 19.03, 24.19, row)
        # May or may not have candidates (depends on strike floor),
        # but should NOT be blocked by momentum
        assert _ == ""  # no block reason


# ── TestRefilterConstrained ──────────────────────────────────────────────────

class TestRefilterConstrained:
    """_refilter_constrained applies constrained gates and ranks by resistance."""

    def test_filters_high_delta(self):
        row = _base_row()
        cands = [_candidate(delta=0.35)]  # above 0.20 cap
        result, _ = _refilter_constrained(cands, 19.03, 24.19, row)
        assert len(result) == 0

    def test_filters_long_dte(self):
        row = _base_row()
        cands = [_candidate(dte=30)]  # above 14d max
        result, _ = _refilter_constrained(cands, 19.03, 24.19, row)
        assert len(result) == 0

    def test_strike_floor_uses_resistance_buffer(self):
        # Only SMA50=25.0 above spot×1.05. UpperBand/SMA20/EMA9 below threshold.
        # Floor = max(19.03*1.15, 25.0*1.03) = max(21.88, 25.75) = 25.75
        row = _base_row(SMA50=25.0, UpperBand_20=19.5, SMA20=19.0, EMA9=18.5)
        cands = [
            _candidate(strike=25.0, delta=0.15, dte=10, mid=0.15),  # below 25.75 → filtered
            _candidate(strike=26.0, delta=0.12, dte=10, mid=0.12),  # above → passes
        ]
        result, _ = _refilter_constrained(cands, 19.03, 24.19, row)
        if result:
            assert all(c["strike"] >= 25.75 for c in result)

    def test_ranks_by_resistance_proximity(self):
        row = _base_row(SMA50=23.0, UpperBand_20=24.0)
        # floor = max(19.03*1.15, 23.0*1.03) = max(21.88, 23.69) = 23.69
        cands = [
            _candidate(strike=24.0, delta=0.12, dte=10, mid=0.15),  # near UpperBand
            _candidate(strike=26.0, delta=0.08, dte=10, mid=0.10),  # far from any level
        ]
        result, _ = _refilter_constrained(cands, 19.03, 24.19, row)
        if len(result) >= 2:
            # First should be closer to resistance
            assert result[0]["resistance_proximity"] >= result[1]["resistance_proximity"]

    def test_returns_empty_when_nothing_passes(self):
        row = _base_row()
        cands = [_candidate(mid=0.05)]  # below $0.10 minimum
        result, reason = _refilter_constrained(cands, 19.03, 24.19, row)
        assert result == []
        assert reason == ""  # not blocked by momentum, just no viable candidates

    def test_adds_safety_tag(self):
        row = _base_row(SMA50=22.0, UpperBand_20=21.5)
        # floor = max(19.03*1.15, 22.0*1.03) = max(21.88, 22.66) = 22.66
        cands = [_candidate(strike=23.0, delta=0.12, dte=10, mid=0.15)]
        result, _ = _refilter_constrained(cands, 19.03, 24.19, row)
        if result:
            assert result[0]["constrained"] is True
            assert result[0]["safety_tag"] == "HIGH_HV_RECOVERY"


# ── TestConstrainedWatchSignal ───────────────────────────────────────────────

class TestConstrainedWatchSignal:
    """_constrained_watch_signal tracks vol contraction + price stabilization."""

    def test_converging_signals(self):
        row = _base_row(HV_10D=0.85, HV_20D=0.98, EMA9=18.5, roc_5=0.0)
        # HV_10D < HV_20D AND spot (19.03) > EMA9 (18.5) AND roc_5 > -1%
        signal = _constrained_watch_signal(row)
        assert "normalizing" in signal.lower() or "approaching" in signal.lower()

    def test_not_converging(self):
        row = _base_row(HV_10D=1.05, HV_20D=0.98)  # vol EXPANDING
        signal = _constrained_watch_signal(row)
        assert "Watch for" in signal
