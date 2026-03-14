"""
Market Regime Classifier — pure function, no I/O.

Classifies market-wide conditions into a composite regime using 8 weighted
indicators. Returns a MarketRegime dataclass with numeric score, regime label,
backward-compatible stress_level, and per-component audit trail.

All thresholds imported from config/indicator_settings.py.
"""

import math
from dataclasses import dataclass, field
from typing import Any

from config.indicator_settings import MARKET_REGIME_THRESHOLDS as _T


# ── Output Dataclass ──────────────────────────────────────────────────────────

@dataclass(frozen=True)
class MarketRegime:
    score: float            # 0-100 composite stress score
    regime: str             # RISK_ON | NORMAL | CAUTIOUS | RISK_OFF | CRISIS
    stress_level: str       # LOW | NORMAL | ELEVATED | CRISIS (backward-compat)
    vol_regime: str         # LOW_VOL | NORMAL_VOL | HIGH_VOL | EXTREME_VOL
    term_structure: str     # CONTANGO | FLAT | BACKWARDATION
    breadth_state: str      # BROAD | NARROW | DETERIORATING
    confidence: float       # 0-1, component-aware
    components: dict = field(default_factory=dict)


# ── Component Definitions ────────────────────────────────────────────────────

_COMPONENTS = [
    # (name, key_in_ctx, weight, higher_is_worse, thresholds: cautious/risk_off/crisis)
    ("vix",            "vix",                       0.20, True,  ("VIX_CAUTIOUS", "VIX_RISK_OFF", "VIX_CRISIS")),
    ("vix_percentile", "vix_percentile_252d",       0.15, True,  ("VIX_PCTL_CAUTIOUS", "VIX_PCTL_RISK_OFF", "VIX_PCTL_CRISIS")),
    ("term_structure", "vix_term_ratio",            0.20, True,  ("TERM_CAUTIOUS", "TERM_RISK_OFF", "TERM_CRISIS")),
    ("vvix",           "vvix",                      0.10, True,  ("VVIX_CAUTIOUS", "VVIX_RISK_OFF", "VVIX_CRISIS")),
    ("skew",           "skew",                      0.05, True,  ("SKEW_CAUTIOUS", "SKEW_RISK_OFF", "SKEW_CRISIS")),
    ("credit_proxy",   "credit_spread_proxy",       0.10, False, ("CREDIT_CAUTIOUS", "CREDIT_RISK_OFF", "CREDIT_CRISIS")),
    ("breadth",        "universe_breadth_pct_sma50", 0.15, False, ("BREADTH_CAUTIOUS", "BREADTH_RISK_OFF", "BREADTH_CRISIS")),
    ("correlation",    "avg_correlation",            0.05, True,  ("CORR_CAUTIOUS", "CORR_RISK_OFF", "CORR_CRISIS")),
]

# Core components — if either missing, confidence gets halved
_CORE_COMPONENTS = {"vix", "term_structure"}


# ── Scoring Helpers ───────────────────────────────────────────────────────────

def _score_component(
    value: float,
    higher_is_worse: bool,
    thresh_cautious: float,
    thresh_risk_off: float,
    thresh_crisis: float,
) -> float:
    """
    Score a single component on 0-100 scale.

    For higher_is_worse=True (e.g. VIX): value below cautious → 0, above crisis → 100.
    For higher_is_worse=False (e.g. breadth): value above cautious → 0, below crisis → 100.
    Linear interpolation between thresholds.
    """
    if higher_is_worse:
        if value <= thresh_cautious:
            return 0.0
        elif value >= thresh_crisis:
            return 100.0
        elif value >= thresh_risk_off:
            # risk_off to crisis → 60-100
            span = thresh_crisis - thresh_risk_off
            if span <= 0:
                return 80.0
            return 60.0 + 40.0 * (value - thresh_risk_off) / span
        else:
            # cautious to risk_off → 0-60
            span = thresh_risk_off - thresh_cautious
            if span <= 0:
                return 30.0
            return 60.0 * (value - thresh_cautious) / span
    else:
        # Lower is worse (breadth, credit proxy)
        if value >= thresh_cautious:
            return 0.0
        elif value <= thresh_crisis:
            return 100.0
        elif value <= thresh_risk_off:
            span = thresh_risk_off - thresh_crisis
            if span <= 0:
                return 80.0
            return 60.0 + 40.0 * (thresh_risk_off - value) / span
        else:
            span = thresh_cautious - thresh_risk_off
            if span <= 0:
                return 30.0
            return 60.0 * (thresh_cautious - value) / span


def _classify_vol_regime(vix: float | None) -> str:
    if vix is None or math.isnan(vix):
        return "UNKNOWN"
    if vix < 15:
        return "LOW_VOL"
    elif vix < 25:
        return "NORMAL_VOL"
    elif vix < 35:
        return "HIGH_VOL"
    else:
        return "EXTREME_VOL"


def _classify_term_structure(ratio: float | None) -> str:
    if ratio is None or math.isnan(ratio):
        return "UNKNOWN"
    if ratio > 1.0:
        return "BACKWARDATION"
    elif ratio > 0.95:
        return "FLAT"
    else:
        return "CONTANGO"


def _classify_breadth(pct: float | None) -> str:
    if pct is None or math.isnan(pct):
        return "UNKNOWN"
    if pct >= 50:
        return "BROAD"
    elif pct >= 30:
        return "NARROW"
    else:
        return "DETERIORATING"


# ── Main Classifier ──────────────────────────────────────────────────────────

def classify_market_regime(ctx: dict) -> MarketRegime:
    """
    Classify market regime from a context dict (typically from get_latest_market_context()).

    Returns MarketRegime with numeric score, regime label, backward-compatible
    stress_level, and per-component audit trail.

    Missing components are excluded from scoring; confidence is reduced accordingly.
    """
    components: dict[str, dict[str, Any]] = {}
    present_weight = 0.0
    weighted_score = 0.0
    missing: list[str] = []
    core_present = True

    for name, key, weight, higher_is_worse, thresh_keys in _COMPONENTS:
        value = ctx.get(key)

        # Check for missing/NaN
        if value is None or (isinstance(value, float) and math.isnan(value)):
            components[name] = {
                "value": None, "subscore": 0, "weight": weight, "present": False,
            }
            missing.append(name)
            if name in _CORE_COMPONENTS:
                core_present = False
            continue

        value = float(value)
        t_cautious = _T[thresh_keys[0]]
        t_risk_off = _T[thresh_keys[1]]
        t_crisis = _T[thresh_keys[2]]

        subscore = _score_component(value, higher_is_worse, t_cautious, t_risk_off, t_crisis)
        components[name] = {
            "value": round(value, 4),
            "subscore": round(subscore, 2),
            "weight": weight,
            "present": True,
        }
        present_weight += weight
        weighted_score += subscore * weight

    # Renormalize by present weight (so missing components don't drag score to 0)
    if present_weight > 0:
        score = weighted_score / present_weight
    else:
        score = 0.0

    # ── Regime from score ─────────────────────────────────────────────────
    if score >= 80:
        regime = "CRISIS"
    elif score >= 60:
        regime = "RISK_OFF"
    elif score >= 40:
        regime = "CAUTIOUS"
    elif score >= 20:
        regime = "NORMAL"
    else:
        regime = "RISK_ON"

    # ── Backward-compatible stress_level ──────────────────────────────────
    _STRESS_MAP = {
        "RISK_ON": "LOW",
        "NORMAL": "NORMAL",
        "CAUTIOUS": "ELEVATED",
        "RISK_OFF": "ELEVATED",
        "CRISIS": "CRISIS",
    }
    stress_level = _STRESS_MAP[regime]

    # ── Component-aware confidence ────────────────────────────────────────
    # Row freshness is handled externally (staleness_bdays); here we focus on
    # component completeness and core presence.
    staleness_bdays = ctx.get("staleness_bdays", 0)
    if staleness_bdays <= 0:
        freshness = 1.0
    elif staleness_bdays == 1:
        freshness = 0.8
    elif staleness_bdays == 2:
        freshness = 0.5
    else:
        freshness = 0.2

    total_components = len(_COMPONENTS)
    pct_present = (total_components - len(missing)) / total_components if total_components > 0 else 0.0
    core_factor = 1.0 if core_present else 0.5

    confidence = round(freshness * pct_present * core_factor, 3)

    # ── Sub-classifications ───────────────────────────────────────────────
    vol_regime = _classify_vol_regime(ctx.get("vix"))
    term_structure = _classify_term_structure(ctx.get("vix_term_ratio"))
    breadth_state = _classify_breadth(ctx.get("universe_breadth_pct_sma50"))

    return MarketRegime(
        score=round(score, 2),
        regime=regime,
        stress_level=stress_level,
        vol_regime=vol_regime,
        term_structure=term_structure,
        breadth_state=breadth_state,
        confidence=confidence,
        components=components,
    )
