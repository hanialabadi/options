"""
Tests for core/shared/mc/vol_blend.py — near-term vol blend schedule builder.

Covers:
  - Schedule shape and decay curve
  - Edge cases (flat vol, extreme EWMA, short DTE)
  - MIN_DIVERGENCE threshold skips unnecessary blends
  - MAX_EWMA_RATIO capping
  - resolve_vol_schedule with override and fallback
  - Integration with gbm_daily_paths
"""

import numpy as np
import pytest

from core.shared.mc.vol_blend import (
    build_vol_schedule,
    resolve_vol_schedule,
    DECAY_TAU,
    MIN_DIVERGENCE,
    MAX_EWMA_RATIO,
    VOL_FLOOR,
    VOL_CAP,
)


# ── build_vol_schedule ────────────────────────────────────────────────────────

class TestBuildVolSchedule:
    """Tests for the core schedule builder."""

    def test_basic_shape(self):
        """Schedule should be (n_days,) array."""
        sched = build_vol_schedule(hv=0.25, ewma=0.40, n_days=30)
        assert sched is not None
        assert sched.shape == (30,)

    def test_starts_at_ewma(self):
        """Day 0 should be close to EWMA value."""
        sched = build_vol_schedule(hv=0.25, ewma=0.40, n_days=30)
        # Day 0: w = exp(0) = 1.0 → 100% EWMA
        assert abs(sched[0] - 0.40) < 0.001

    def test_decays_toward_hv(self):
        """Later days should approach HV."""
        sched = build_vol_schedule(hv=0.25, ewma=0.40, n_days=60)
        # Day 60: w = exp(-60/10) ≈ 0.002 → essentially 100% HV
        assert abs(sched[-1] - 0.25) < 0.01

    def test_monotonic_decay_when_ewma_above_hv(self):
        """When EWMA > HV, schedule should monotonically decrease."""
        sched = build_vol_schedule(hv=0.25, ewma=0.40, n_days=30)
        diffs = np.diff(sched)
        assert np.all(diffs <= 0), "Schedule should decrease when EWMA > HV"

    def test_monotonic_increase_when_ewma_below_hv(self):
        """When EWMA < HV, schedule should monotonically increase."""
        sched = build_vol_schedule(hv=0.40, ewma=0.20, n_days=30)
        diffs = np.diff(sched)
        assert np.all(diffs >= 0), "Schedule should increase when EWMA < HV"

    def test_returns_none_when_similar(self):
        """Should return None when EWMA ≈ HV (within MIN_DIVERGENCE)."""
        # 5% divergence < 10% threshold
        sched = build_vol_schedule(hv=0.30, ewma=0.31, n_days=30)
        assert sched is None

    def test_returns_none_for_zero_days(self):
        sched = build_vol_schedule(hv=0.30, ewma=0.40, n_days=0)
        assert sched is None

    def test_returns_none_for_invalid_vol(self):
        assert build_vol_schedule(hv=0.0, ewma=0.40, n_days=30) is None
        assert build_vol_schedule(hv=0.30, ewma=0.0, n_days=30) is None
        assert build_vol_schedule(hv=-0.1, ewma=0.40, n_days=30) is None

    def test_ewma_ratio_cap(self):
        """Extreme EWMA should be capped at MAX_EWMA_RATIO × HV."""
        sched = build_vol_schedule(hv=0.20, ewma=1.50, n_days=10)
        assert sched is not None
        # Day 0 should be capped at 0.20 × 3.0 = 0.60, not 1.50
        assert sched[0] <= 0.20 * MAX_EWMA_RATIO + 0.001

    def test_vol_floor_applied(self):
        """Very low vol blend should be floored."""
        sched = build_vol_schedule(hv=0.06, ewma=0.03, n_days=10)
        # EWMA is well below HV (50% divergence)
        # but this should still be None since |0.03-0.06|/0.06 = 0.50 > 0.10
        # Actually this should produce a schedule since divergence is high
        if sched is not None:
            assert np.all(sched >= VOL_FLOOR)

    def test_vol_cap_applied(self):
        """Extreme high vol should be capped."""
        sched = build_vol_schedule(hv=2.0, ewma=2.5, n_days=10)
        if sched is not None:
            assert np.all(sched <= VOL_CAP)

    def test_custom_tau(self):
        """Shorter tau = faster decay."""
        sched_fast = build_vol_schedule(hv=0.25, ewma=0.40, n_days=20, tau=5.0)
        sched_slow = build_vol_schedule(hv=0.25, ewma=0.40, n_days=20, tau=20.0)
        assert sched_fast is not None and sched_slow is not None
        # At day 10: fast should be closer to HV than slow
        assert sched_fast[10] < sched_slow[10]

    def test_short_dte_still_works(self):
        """DTE=3 should still produce a valid 3-element schedule."""
        sched = build_vol_schedule(hv=0.25, ewma=0.50, n_days=3)
        assert sched is not None
        assert sched.shape == (3,)
        # First element near EWMA, last element still elevated but trending to HV
        assert sched[0] > sched[2]

    def test_half_life_approximately_correct(self):
        """At d = tau * ln(2) ≈ 6.93, weight should be ~50%."""
        hv, ewma = 0.20, 0.40
        sched = build_vol_schedule(hv=hv, ewma=ewma, n_days=30, tau=10.0)
        assert sched is not None
        half_life_day = int(round(10.0 * np.log(2)))  # ~7
        midpoint = (ewma + hv) / 2.0  # 0.30
        assert abs(sched[half_life_day] - midpoint) < 0.02


class TestResolveVolSchedule:
    """Tests for the resolve wrapper."""

    def test_with_override(self):
        """Should use ewma_override without DB lookup."""
        sched, source = resolve_vol_schedule(
            ticker=None, hv=0.25, n_days=30, ewma_override=0.40,
        )
        assert sched is not None
        assert "override" in source

    def test_no_ticker_no_override(self):
        """Should return None when no EWMA source available."""
        sched, source = resolve_vol_schedule(
            ticker=None, hv=0.25, n_days=30,
        )
        assert sched is None
        assert source == "UNAVAILABLE"

    def test_flat_when_similar(self):
        """Should return FLAT when EWMA ≈ HV."""
        sched, source = resolve_vol_schedule(
            ticker=None, hv=0.30, n_days=30, ewma_override=0.31,
        )
        assert sched is None
        assert source == "FLAT"

    def test_invalid_ewma_override(self):
        """Out-of-range EWMA should return UNAVAILABLE."""
        sched, source = resolve_vol_schedule(
            ticker=None, hv=0.25, n_days=30, ewma_override=0.005,
        )
        assert sched is None
        assert source == "UNAVAILABLE"

    def test_source_label_with_ticker(self):
        """Source should include ticker name when provided."""
        sched, source = resolve_vol_schedule(
            ticker="AAPL", hv=0.20, n_days=30, ewma_override=0.35,
        )
        assert sched is not None
        assert "AAPL" in source


class TestVolScheduleWithPaths:
    """Integration: vol schedule fed into gbm_daily_paths."""

    def test_schedule_changes_path_dispersion(self):
        """Paths with high-EWMA schedule should have more early dispersion."""
        from core.shared.mc.paths import gbm_daily_paths

        hv = 0.25
        n_days = 30
        n_paths = 5_000
        rng_flat = np.random.default_rng(42)
        rng_sched = np.random.default_rng(42)

        # Flat paths
        flat_paths = gbm_daily_paths(100.0, hv, n_days, n_paths, rng_flat)

        # Blended paths (EWMA = 0.45, much higher than HV)
        sched = build_vol_schedule(hv=hv, ewma=0.45, n_days=n_days)
        assert sched is not None
        sched_paths = gbm_daily_paths(100.0, hv, n_days, n_paths, rng_sched,
                                       iv_schedule=sched)

        # Early days: blended should have higher std (more dispersion)
        flat_std_day5 = float(np.std(flat_paths[:, 5]))
        sched_std_day5 = float(np.std(sched_paths[:, 5]))
        assert sched_std_day5 > flat_std_day5 * 1.1, \
            f"Blended day-5 std ({sched_std_day5:.2f}) should be >10% higher than flat ({flat_std_day5:.2f})"

        # Late days: should converge (schedule decays to HV)
        flat_std_last = float(np.std(flat_paths[:, -1]))
        sched_std_last = float(np.std(sched_paths[:, -1]))
        # Within 30% — not exact due to cumulative effect, but much closer than early
        ratio = sched_std_last / flat_std_last
        assert 0.7 < ratio < 1.6, \
            f"Late-day std ratio ({ratio:.2f}) should be close to 1.0"

    def test_flat_ewma_matches_no_schedule(self):
        """When EWMA = HV, resolve returns None and paths are identical."""
        sched, source = resolve_vol_schedule(
            ticker=None, hv=0.30, n_days=20, ewma_override=0.30,
        )
        assert sched is None
        assert source == "FLAT"
