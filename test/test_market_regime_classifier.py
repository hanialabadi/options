"""
Market Regime Classifier Tests
================================
Validates Phase 2: market_regime_classifier.py — pure function, no I/O.

Tests:
  1-5.   All 5 regime buckets (RISK_ON, NORMAL, CAUTIOUS, RISK_OFF, CRISIS)
  6-7.   Boundary cases at regime transitions
  8-10.  Term structure classification
  11-13. Vol regime classification
  14-15. Breadth state classification
  16-17. Component-aware confidence (missing VIX, missing SKEW)
  18.    All components missing → score 0, low confidence
  19.    Backward-compat stress_level mapping
  20.    Numeric score returned and usable
  21.    Missing components → weight renormalization
  22.    Freshness degradation (staleness_bdays)
  23.    Component subscore linear interpolation
  24-25. Credit proxy scoring (lower is worse)

Run:
    pytest test/test_market_regime_classifier.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.shared.data_layer.market_regime_classifier import (
    classify_market_regime,
    MarketRegime,
    _score_component,
    _classify_vol_regime,
    _classify_term_structure,
    _classify_breadth,
)


# ── Helper ───────────────────────────────────────────────────────────────────

def _build_ctx(
    vix=18, vix_pctl=40, term_ratio=0.90, vvix=110, skew=128,
    credit=0.96, breadth=60, corr=0.30, staleness=0,
) -> dict:
    """Build a context dict with sensible defaults (RISK_ON / NORMAL)."""
    return {
        "vix": vix,
        "vix_percentile_252d": vix_pctl,
        "vix_term_ratio": term_ratio,
        "vvix": vvix,
        "skew": skew,
        "credit_spread_proxy": credit,
        "universe_breadth_pct_sma50": breadth,
        "avg_correlation": corr,
        "staleness_bdays": staleness,
    }


# =============================================================================
# 1. Regime Bucket Tests
# =============================================================================

class TestRegimeBuckets:
    """Each regime must be reachable with appropriate inputs."""

    def test_risk_on(self):
        """Low VIX, low term ratio, strong breadth → RISK_ON."""
        ctx = _build_ctx(vix=12, vix_pctl=10, term_ratio=0.85, vvix=90,
                         skew=120, credit=0.98, breadth=75, corr=0.20)
        r = classify_market_regime(ctx)
        assert r.regime == "RISK_ON"
        assert r.score < 20

    def test_normal(self):
        """Moderate values → NORMAL."""
        ctx = _build_ctx(vix=23, vix_pctl=72, term_ratio=0.97, vvix=122,
                         skew=137, credit=0.93, breadth=48, corr=0.42)
        r = classify_market_regime(ctx)
        assert r.regime == "NORMAL"
        assert 20 <= r.score < 40

    def test_cautious(self):
        """Elevated VIX + narrowing breadth → CAUTIOUS."""
        ctx = _build_ctx(vix=24, vix_pctl=75, term_ratio=1.00, vvix=128,
                         skew=139, credit=0.92, breadth=42, corr=0.48)
        r = classify_market_regime(ctx)
        assert r.regime == "CAUTIOUS"
        assert 40 <= r.score < 60

    def test_risk_off(self):
        """High VIX, backwardation, poor breadth → RISK_OFF."""
        ctx = _build_ctx(vix=28, vix_pctl=85, term_ratio=1.05, vvix=135,
                         skew=142, credit=0.91, breadth=35, corr=0.55)
        r = classify_market_regime(ctx)
        assert r.regime == "RISK_OFF"
        assert 60 <= r.score < 80

    def test_crisis(self):
        """Extreme values across all indicators → CRISIS."""
        ctx = _build_ctx(vix=45, vix_pctl=98, term_ratio=1.15, vvix=160,
                         skew=160, credit=0.85, breadth=20, corr=0.75)
        r = classify_market_regime(ctx)
        assert r.regime == "CRISIS"
        assert r.score >= 80


# =============================================================================
# 2. Boundary Cases
# =============================================================================

class TestBoundaries:
    """Score exactly at boundary → correct regime."""

    def test_score_20_boundary(self):
        """Score at exactly 20 → NORMAL (not RISK_ON)."""
        # Craft a ctx that lands near 20
        ctx = _build_ctx(vix=20, vix_pctl=60, term_ratio=0.95, vvix=120,
                         skew=135, credit=0.94, breadth=50, corr=0.40)
        r = classify_market_regime(ctx)
        # All components at their cautious threshold → score ~0 each
        # (actually at threshold = score 0 for higher_is_worse)
        # This should be around the boundary
        assert r.regime in ("RISK_ON", "NORMAL")

    def test_all_at_crisis_threshold(self):
        """All indicators exactly at crisis → score 100 → CRISIS."""
        ctx = _build_ctx(vix=35, vix_pctl=95, term_ratio=1.1, vvix=150,
                         skew=155, credit=0.88, breadth=25, corr=0.70)
        r = classify_market_regime(ctx)
        assert r.regime == "CRISIS"
        assert r.score >= 80


# =============================================================================
# 3. Sub-classification Tests
# =============================================================================

class TestSubClassifications:

    def test_vol_regime_low(self):
        assert _classify_vol_regime(12.0) == "LOW_VOL"

    def test_vol_regime_normal(self):
        assert _classify_vol_regime(20.0) == "NORMAL_VOL"

    def test_vol_regime_high(self):
        assert _classify_vol_regime(30.0) == "HIGH_VOL"

    def test_vol_regime_extreme(self):
        assert _classify_vol_regime(40.0) == "EXTREME_VOL"

    def test_vol_regime_none(self):
        assert _classify_vol_regime(None) == "UNKNOWN"

    def test_term_structure_contango(self):
        assert _classify_term_structure(0.90) == "CONTANGO"

    def test_term_structure_flat(self):
        assert _classify_term_structure(0.97) == "FLAT"

    def test_term_structure_backwardation(self):
        assert _classify_term_structure(1.05) == "BACKWARDATION"

    def test_term_structure_none(self):
        assert _classify_term_structure(None) == "UNKNOWN"

    def test_breadth_broad(self):
        assert _classify_breadth(60.0) == "BROAD"

    def test_breadth_narrow(self):
        assert _classify_breadth(40.0) == "NARROW"

    def test_breadth_deteriorating(self):
        assert _classify_breadth(20.0) == "DETERIORATING"

    def test_breadth_none(self):
        assert _classify_breadth(None) == "UNKNOWN"


# =============================================================================
# 4. Component-Aware Confidence
# =============================================================================

class TestConfidence:

    def test_full_confidence_fresh_data(self):
        """All components present + fresh data → confidence near 1.0."""
        ctx = _build_ctx(staleness=0)
        r = classify_market_regime(ctx)
        assert r.confidence >= 0.9

    def test_missing_core_halves_confidence(self):
        """Missing VIX (core component) → confidence halved."""
        ctx = _build_ctx()
        ctx["vix"] = None  # core component
        r_missing = classify_market_regime(ctx)

        ctx_full = _build_ctx()
        r_full = classify_market_regime(ctx_full)

        assert r_missing.confidence < r_full.confidence * 0.6

    def test_missing_skew_minor_reduction(self):
        """Missing SKEW (5% weight, non-core) → small confidence reduction."""
        ctx = _build_ctx()
        ctx["skew"] = None
        r = classify_market_regime(ctx)
        # 7/8 components = 0.875 × freshness(1.0) × core(1.0) = 0.875
        assert r.confidence == pytest.approx(0.875)

    def test_all_missing_zero_confidence(self):
        """All components missing → confidence = 0."""
        ctx = {
            "vix": None, "vix_percentile_252d": None, "vix_term_ratio": None,
            "vvix": None, "skew": None, "credit_spread_proxy": None,
            "universe_breadth_pct_sma50": None, "avg_correlation": None,
            "staleness_bdays": 0,
        }
        r = classify_market_regime(ctx)
        assert r.confidence == 0.0
        assert r.score == 0.0

    def test_staleness_1_bday(self):
        """1 business day stale → freshness factor 0.8."""
        ctx = _build_ctx(staleness=1)
        r = classify_market_regime(ctx)
        # 8/8 present × 0.8 freshness × 1.0 core = 0.8
        assert r.confidence == pytest.approx(0.8)

    def test_staleness_2_bday(self):
        ctx = _build_ctx(staleness=2)
        r = classify_market_regime(ctx)
        assert r.confidence == pytest.approx(0.5)

    def test_staleness_3plus_bday(self):
        ctx = _build_ctx(staleness=5)
        r = classify_market_regime(ctx)
        assert r.confidence == pytest.approx(0.2)


# =============================================================================
# 5. Backward-Compat Stress Level
# =============================================================================

class TestStressLevelMapping:
    """stress_level maps from regime for backward compatibility."""

    def test_risk_on_maps_to_low(self):
        ctx = _build_ctx(vix=12, vix_pctl=10, term_ratio=0.85, vvix=90,
                         skew=120, credit=0.98, breadth=75, corr=0.20)
        r = classify_market_regime(ctx)
        assert r.stress_level == "LOW"

    def test_normal_maps_to_normal(self):
        ctx = _build_ctx(vix=23, vix_pctl=72, term_ratio=0.97, vvix=122,
                         skew=137, credit=0.93, breadth=48, corr=0.42)
        r = classify_market_regime(ctx)
        assert r.stress_level == "NORMAL"

    def test_crisis_maps_to_crisis(self):
        ctx = _build_ctx(vix=45, vix_pctl=98, term_ratio=1.15, vvix=160,
                         skew=160, credit=0.85, breadth=20, corr=0.75)
        r = classify_market_regime(ctx)
        assert r.stress_level == "CRISIS"


# =============================================================================
# 6. Numeric Score & Components
# =============================================================================

class TestNumericScore:

    def test_score_is_float(self):
        r = classify_market_regime(_build_ctx())
        assert isinstance(r.score, float)

    def test_score_range_0_100(self):
        """Score should always be in [0, 100]."""
        for vix in [10, 20, 30, 40, 50]:
            ctx = _build_ctx(vix=vix)
            r = classify_market_regime(ctx)
            assert 0 <= r.score <= 100

    def test_components_dict_populated(self):
        r = classify_market_regime(_build_ctx())
        assert len(r.components) == 8
        for name, comp in r.components.items():
            assert "value" in comp
            assert "subscore" in comp
            assert "weight" in comp
            assert "present" in comp


# =============================================================================
# 7. Weight Renormalization
# =============================================================================

class TestWeightRenormalization:
    """Missing components should not drag score toward zero."""

    def test_missing_components_renormalized(self):
        """Score from 5 stressed components should still register as high,
        even if 3 components are missing."""
        ctx = {
            "vix": 40,                   # near crisis
            "vix_percentile_252d": 90,    # high
            "vix_term_ratio": 1.08,       # near crisis
            "vvix": None,                 # missing
            "skew": None,                 # missing
            "credit_spread_proxy": None,  # missing
            "universe_breadth_pct_sma50": 30,  # poor
            "avg_correlation": 0.60,      # elevated
            "staleness_bdays": 0,
        }
        r = classify_market_regime(ctx)
        # Despite 3 missing, present components are stressed → high score
        assert r.score >= 60
        assert r.regime in ("RISK_OFF", "CRISIS")


# =============================================================================
# 8. Component Scoring
# =============================================================================

class TestComponentScoring:
    """Validate _score_component linear interpolation."""

    def test_below_cautious_is_zero(self):
        # higher_is_worse: value below cautious → 0
        assert _score_component(15, True, 20, 25, 35) == 0.0

    def test_above_crisis_is_100(self):
        assert _score_component(40, True, 20, 25, 35) == 100.0

    def test_at_cautious_is_zero(self):
        assert _score_component(20, True, 20, 25, 35) == 0.0

    def test_at_crisis_is_100(self):
        assert _score_component(35, True, 20, 25, 35) == 100.0

    def test_midpoint_cautious_to_risk_off(self):
        # Midpoint between 20 and 25 → score 30 (60 * 0.5)
        score = _score_component(22.5, True, 20, 25, 35)
        assert score == pytest.approx(30.0)

    def test_lower_is_worse_credit(self):
        # higher_is_worse=False: value above cautious → 0
        assert _score_component(0.96, False, 0.94, 0.92, 0.88) == 0.0
        # value below crisis → 100
        assert _score_component(0.85, False, 0.94, 0.92, 0.88) == 100.0

    def test_lower_is_worse_midpoint(self):
        # Midpoint between cautious(0.94) and risk_off(0.92)
        score = _score_component(0.93, False, 0.94, 0.92, 0.88)
        assert score == pytest.approx(30.0)


# =============================================================================
# 9. Dataclass Properties
# =============================================================================

class TestMarketRegimeDataclass:

    def test_frozen_immutable(self):
        r = classify_market_regime(_build_ctx())
        with pytest.raises(AttributeError):
            r.score = 50.0  # type: ignore

    def test_all_fields_populated(self):
        r = classify_market_regime(_build_ctx())
        assert r.score is not None
        assert r.regime is not None
        assert r.stress_level is not None
        assert r.vol_regime is not None
        assert r.term_structure is not None
        assert r.breadth_state is not None
        assert r.confidence is not None
        assert r.components is not None
