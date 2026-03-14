"""
Thesis Review Scorer — replaces ambiguous REVIEW with scored verdict.

When drift detects Data_State='STALE', Signal_State='DEGRADED', or
Regime_State='STRESSED', the drift engine generates a REVIEW action.
Instead of passing this ambiguity to the human, this scorer evaluates
60+ signals already available in df_final and produces a concrete
executable action: HOLD, HOLD_WITH_CAUTION, TRIM, or EXIT.

Pure function: (row) -> ThesisVerdict.  No DB, no side effects.

Passarelli Ch.2: "an exit signal that persists is not noise"
McMillan Ch.4: "position management requires thesis persistence"
"""

import math
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import pandas as pd

from .helpers import check_thesis_degradation
from .thresholds import (
    THESIS_REVIEW_REAFFIRMED_FLOOR,
    THESIS_REVIEW_MONITORING_FLOOR,
    THESIS_REVIEW_WEAKENED_FLOOR,
    THESIS_REVIEW_CATEGORY_MAX,
    THESIS_REVIEW_CATEGORY_MIN,
    THESIS_REVIEW_YOUNG_TRADE_DAYS,
    THESIS_REVIEW_STALE_TRADE_DAYS,
    MFE_SIGNIFICANT,
)


# ---------------------------------------------------------------------------
# Verdict dataclass
# ---------------------------------------------------------------------------

@dataclass
class ThesisVerdict:
    """Scored thesis review result with concrete executable action."""
    verdict: str            # REAFFIRMED | MONITORING | WEAKENED | DEGRADED
    action: str             # HOLD | HOLD_WITH_CAUTION | TRIM | EXIT (executable only)
    score: float            # -100 to +100
    urgency: str            # LOW | MEDIUM | HIGH | CRITICAL
    evidence: List[str]     # Top 3 contributors, sorted by |score|
    category_scores: Dict[str, float] = field(default_factory=dict)
    resets_streak: bool = False


# ---------------------------------------------------------------------------
# Strategy classification helpers
# ---------------------------------------------------------------------------

_MULTI_LEG_STRATEGIES = frozenset({
    'PMCC', 'POOR_MANS_COVERED_CALL', 'CALL_DIAGONAL', 'PUT_DIAGONAL',
    'IRON_CONDOR', 'IRON_BUTTERFLY', 'STRADDLE', 'STRANGLE',
    'VERTICAL_SPREAD', 'CALENDAR_SPREAD',
})

_SPREAD_STRATEGIES = frozenset({
    'VERTICAL_SPREAD', 'IRON_CONDOR', 'IRON_BUTTERFLY',
    'CALENDAR_SPREAD', 'DIAGONAL_SPREAD',
})


def _is_long_vol(strategy: str, qty: float) -> bool:
    """Matches logic in check_thesis_degradation (helpers.py line 282-314)."""
    return (
        'LONG_CALL' in strategy
        or 'LONG_PUT' in strategy
        or ('LEAP' in strategy and qty > 0)
    )


def _can_reduce(strategy: str, quantity: int) -> bool:
    """Whether a position can be partially closed (TRIM)."""
    if quantity < 2:
        return False
    strat_upper = strategy.upper()
    for s in _MULTI_LEG_STRATEGIES:
        if s in strat_upper:
            return False
    for s in _SPREAD_STRATEGIES:
        if s in strat_upper:
            return False
    return True


# ---------------------------------------------------------------------------
# Safe field accessors
# ---------------------------------------------------------------------------

def _sn(row: pd.Series, col: str, default: str = "") -> str:
    """Safe string from row."""
    v = row.get(col)
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return default
    s = str(v).strip()
    return s if s and s not in ('nan', 'None', 'N/A') else default


def _sf(row: pd.Series, col: str, default: float = 0.0) -> float:
    """Safe float from row."""
    v = row.get(col)
    if v is None or (isinstance(v, float) and (pd.isna(v) or math.isnan(v))):
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Category scorers — each returns (score, evidence_label)
# ---------------------------------------------------------------------------

def _score_conviction(row: pd.Series) -> tuple:
    """Category 1: Conviction health."""
    score = 0.0
    label_parts = []

    conviction = _sn(row, 'Conviction_Status').upper()
    if conviction == 'STRENGTHENING':
        score += 15
        label_parts.append("STRENGTHENING (+15)")
    elif conviction == 'STABLE':
        score += 10
        label_parts.append("STABLE (+10)")
    elif conviction == 'WEAKENING':
        score -= 5
        label_parts.append("WEAKENING (-5)")
    elif conviction == 'REVERSING':
        score -= 15
        label_parts.append("REVERSING (-15)")

    streak = _sf(row, 'Delta_Deterioration_Streak', 0)
    if streak >= 3:
        score -= 5
        label_parts.append(f"deterioration streak {int(streak)} (-5)")

    fade_days = _sf(row, 'Conviction_Fade_Days', 0)
    if fade_days >= 5:
        score -= 5
        label_parts.append(f"fade {int(fade_days)}d (-5)")

    label = "Conviction " + ", ".join(label_parts) if label_parts else "Conviction neutral"
    return _clamp(score), label, abs(score)


def _score_structural(row: pd.Series) -> tuple:
    """Category 2: Structural integrity."""
    score = 0.0
    label_parts = []

    thesis_state = _sn(row, 'Thesis_State').upper()
    if thesis_state == 'INTACT':
        score += 15
        label_parts.append("thesis INTACT (+15)")
    elif thesis_state == 'RECOVERING':
        score += 5
        label_parts.append("thesis RECOVERING (+5)")
    elif thesis_state in ('DEGRADED', 'BROKEN'):
        score -= 10
        label_parts.append(f"thesis {thesis_state} (-10)")

    # check_thesis_degradation returns None or {"text": "reason1; reason2; ..."}
    degradation = check_thesis_degradation(row)
    if degradation is None:
        score += 5
        label_parts.append("no structural degradation (+5)")
    else:
        issues = [i.strip() for i in degradation.get("text", "").split(";") if i.strip()]
        n = len(issues)
        if n >= 2:
            score -= 15
            label_parts.append(f"{n} structural issues (-15)")
        elif n == 1:
            score -= 5
            label_parts.append(f"1 structural issue (-5)")

    label = "Structure: " + "; ".join(label_parts) if label_parts else "Structure neutral"
    return _clamp(score), label, abs(score)


def _score_momentum(row: pd.Series) -> tuple:
    """Category 3: Momentum & wave phase."""
    score = 0.0
    label_parts = []

    wave = _sn(row, 'WavePhase_State').upper()
    _wave_scores = {
        'BUILDING': 15, 'FORMING': 5, 'RECOVERING': 5,
        'PEAKING': 0, 'STALLED': -5, 'FADING': -10, 'EXHAUSTED': -15,
    }
    ws = _wave_scores.get(wave, 0)
    if ws != 0:
        score += ws
        label_parts.append(f"wave {wave} ({ws:+d})")

    mom = _sn(row, 'MomentumVelocity_State').upper()
    if mom == 'ACCELERATING':
        score += 5
        label_parts.append("momentum ACCELERATING (+5)")
    elif mom in ('LATE_CYCLE', 'DECELERATING'):
        score -= 5
        label_parts.append(f"momentum {mom} (-5)")

    trend = _sn(row, 'TrendIntegrity_State').upper()
    if trend == 'STRONG_TREND':
        score += 5
        label_parts.append("STRONG_TREND (+5)")
    elif trend in ('TREND_EXHAUSTED', 'NO_TREND'):
        score -= 5
        label_parts.append(f"{trend} (-5)")

    label = "Momentum: " + ", ".join(label_parts) if label_parts else "Momentum neutral"
    return _clamp(score), label, abs(score)


def _score_profit_cushion(row: pd.Series) -> tuple:
    """Category 4: Profit cushion."""
    score = 0.0
    label_parts = []

    pnl = _sf(row, 'PnL_Pct', None)
    if pnl is None:
        pnl = _sf(row, 'Total_GL_Decimal', None)

    if pnl is not None:
        if pnl >= 0.20:
            score += 15
            label_parts.append(f"P&L {pnl:+.0%} (+15)")
        elif pnl >= 0.05:
            score += 10
            label_parts.append(f"P&L {pnl:+.0%} (+10)")
        elif pnl >= 0.0:
            score += 5
            label_parts.append(f"P&L {pnl:+.0%} (+5)")
        elif pnl >= -0.10:
            label_parts.append(f"P&L {pnl:+.0%} (0)")
        elif pnl >= -0.20:
            score -= 10
            label_parts.append(f"P&L {pnl:+.0%} (-10)")
        else:
            score -= 15
            label_parts.append(f"P&L {pnl:+.0%} (-15)")

        # MFE giveback check
        mfe = _sf(row, 'Trajectory_MFE', 0)
        if mfe >= MFE_SIGNIFICANT and pnl < 0.05:
            score -= 5
            label_parts.append(f"MFE giveback {mfe:.0%}→{pnl:+.0%} (-5)")

    # Recovery feasibility
    recovery = _sn(row, 'Recovery_Feasibility').upper()
    if recovery == 'IMPOSSIBLE':
        score -= 10
        label_parts.append("recovery IMPOSSIBLE (-10)")
    elif recovery == 'UNLIKELY':
        score -= 5
        label_parts.append("recovery UNLIKELY (-5)")

    label = "Profit: " + ", ".join(label_parts) if label_parts else "Profit neutral"
    return _clamp(score), label, abs(score)


def _score_trade_age(row: pd.Series) -> tuple:
    """Category 5: Trade age & progress."""
    score = 0.0
    label_parts = []

    days = _sf(row, 'Days_In_Trade', 0)
    pnl = _sf(row, 'PnL_Pct', None) or _sf(row, 'Total_GL_Decimal', 0)
    structural = _sn(row, 'Structural_State').upper()
    has_structural_failure = structural in ('BROKEN', 'CLOSED')
    signal_state = _sn(row, 'Signal_State').upper()
    conviction = _sn(row, 'Conviction_Status').upper()

    if days <= THESIS_REVIEW_YOUNG_TRADE_DAYS:
        if not has_structural_failure:
            score += 10
            label_parts.append(f"young trade {int(days)}d, drift likely noise (+10)")
        else:
            label_parts.append(f"young trade {int(days)}d but structural failure (0)")
    elif days <= 14:
        if pnl is not None and pnl >= 0:
            score += 5
            label_parts.append(f"trade {int(days)}d, P&L non-negative (+5)")
    if days >= THESIS_REVIEW_STALE_TRADE_DAYS:
        if pnl is not None and pnl <= 0 and signal_state == 'DEGRADED':
            score -= 10
            label_parts.append(f"stale trade {int(days)}d, flat/negative + degraded (-10)")
        if pnl is not None and pnl < 0 and conviction == 'REVERSING':
            score -= 5
            label_parts.append(f"stale trade {int(days)}d, negative + REVERSING (-5)")

    label = "Age: " + ", ".join(label_parts) if label_parts else "Age neutral"
    return _clamp(score), label, abs(score)


def _score_macro_risk(row: pd.Series) -> tuple:
    """Category 6: Macro & event risk."""
    score = 0.0
    label_parts = []

    strategy = _sn(row, 'Strategy').upper()
    is_leap = 'LEAP' in strategy

    days_earn = _sf(row, 'days_to_earnings', 999)
    if days_earn <= 5 and not is_leap:
        score -= 10
        label_parts.append(f"earnings in {int(days_earn)}d (-10)")

    days_macro = _sf(row, 'Days_To_Macro', 999)
    if days_macro <= 3:
        score -= 5
        label_parts.append(f"macro event in {int(days_macro)}d (-5)")

    regime = _sn(row, 'Regime_State').upper()
    if regime == 'STRESSED':
        score -= 5
        label_parts.append("regime STRESSED (-5)")
    elif regime == 'STABLE':
        score += 5
        label_parts.append("regime STABLE (+5)")

    portfolio = _sn(row, 'Portfolio_State').upper()
    if portfolio == 'OVER_LIMIT':
        score -= 5
        label_parts.append("portfolio OVER_LIMIT (-5)")

    label = "Macro: " + ", ".join(label_parts) if label_parts else "Macro neutral"
    return _clamp(score), label, abs(score)


def _score_drift_trigger(row: pd.Series) -> tuple:
    """Category 7: Drift trigger classification — why was REVIEW triggered?"""
    score = 0.0
    label_parts = []

    data_state = _sn(row, 'Data_State').upper()
    signal_state = _sn(row, 'Signal_State').upper()
    regime_state = _sn(row, 'Regime_State').upper()

    triggers = []
    if data_state == 'STALE':
        triggers.append('DATA_STALE')
    if signal_state == 'DEGRADED':
        triggers.append('SIGNAL_DEGRADED')
    if regime_state == 'STRESSED':
        triggers.append('REGIME_STRESSED')

    if len(triggers) == 0:
        # Data is fresh — REVIEW from non-drift source or already resolved
        score += 5
        label_parts.append("fresh data (+5)")
    elif len(triggers) >= 2:
        score -= 10
        label_parts.append(f"multiple drift triggers: {', '.join(triggers)} (-10)")
    elif 'DATA_STALE' in triggers and len(triggers) == 1:
        score += 10
        label_parts.append("stale data only, likely noise (+10)")
    elif 'SIGNAL_DEGRADED' in triggers:
        score -= 5
        label_parts.append("signal DEGRADED (-5)")
    elif 'REGIME_STRESSED' in triggers:
        score -= 5
        label_parts.append("regime STRESSED (-5)")

    # Strategy-aware IV adjustment for long vol
    strategy = _sn(row, 'Strategy').upper()
    qty = _sf(row, 'Quantity', 1)
    if _is_long_vol(strategy, qty):
        vol_state = _sn(row, 'VolatilityState_State').upper()
        if vol_state in ('EXPANDING', 'EXTREME') and signal_state == 'DEGRADED':
            score += 10
            label_parts.append("long vol + IV expanding = thesis confirming (+10)")

    label = "Drift: " + ", ".join(label_parts) if label_parts else "Drift neutral"
    return _clamp(score), label, abs(score)


def _clamp(score: float) -> float:
    return max(THESIS_REVIEW_CATEGORY_MIN, min(THESIS_REVIEW_CATEGORY_MAX, score))


# ---------------------------------------------------------------------------
# Main scorer
# ---------------------------------------------------------------------------

def score_thesis_review(row: pd.Series) -> ThesisVerdict:
    """
    Score a REVIEW-action position and produce a concrete executable verdict.

    Returns ThesisVerdict with:
      - verdict: REAFFIRMED | MONITORING | WEAKENED | DEGRADED
      - action: HOLD | HOLD_WITH_CAUTION | TRIM | EXIT (never abstract REDUCE)
      - score: aggregate thesis score (-100 to +100)
      - evidence: top 3 contributors sorted by |score contribution|
    """
    strategy = _sn(row, 'Strategy').upper()
    quantity = abs(int(_sf(row, 'Quantity', 1) or 1))

    # ── Hard overrides — bypass additive scoring ─────────────────────────
    structural = _sn(row, 'Structural_State').upper()
    if structural == 'BROKEN':
        return ThesisVerdict(
            verdict="DEGRADED",
            action="EXIT",
            score=-100.0,
            urgency="CRITICAL",
            evidence=["Structural_State BROKEN — thesis terminated"],
            category_scores={"hard_override": -100},
            resets_streak=False,
        )

    degradation = check_thesis_degradation(row)
    _degradation_count = 0
    if degradation is not None:
        issues = [i.strip() for i in degradation.get("text", "").split(";") if i.strip()]
        _degradation_count = len(issues)

    if _degradation_count >= 3:
        return ThesisVerdict(
            verdict="DEGRADED",
            action="EXIT",
            score=-80.0,
            urgency="MEDIUM",
            evidence=[f"{_degradation_count} structural degradations: {degradation['text']}"],
            category_scores={"hard_override": -80},
            resets_streak=False,
        )

    # ── Additive scoring across 7 categories ─────────────────────────────
    categories = [
        ("conviction", _score_conviction(row)),
        ("structure", _score_structural(row)),
        ("momentum", _score_momentum(row)),
        ("profit", _score_profit_cushion(row)),
        ("age", _score_trade_age(row)),
        ("macro", _score_macro_risk(row)),
        ("drift", _score_drift_trigger(row)),
    ]

    total = 0.0
    cat_scores = {}
    evidence_pool = []  # (abs_score, label)

    for name, (cat_score, label, abs_score) in categories:
        total += cat_score
        cat_scores[name] = cat_score
        evidence_pool.append((abs_score, label))

    total = max(-100.0, min(100.0, total))

    # ── Hard cap: 2 degradation issues → cannot exceed WEAKENED ──────────
    if _degradation_count >= 2 and total >= THESIS_REVIEW_MONITORING_FLOOR:
        total = THESIS_REVIEW_MONITORING_FLOOR - 1  # cap at WEAKENED
        evidence_pool.append((20, f"2 structural issues cap verdict at WEAKENED"))

    # ── Build evidence: top 3 by absolute contribution ───────────────────
    evidence_pool.sort(key=lambda x: x[0], reverse=True)
    evidence = [label for _, label in evidence_pool[:3]]

    # ── Map score to verdict and executable action ───────────────────────
    if total >= THESIS_REVIEW_REAFFIRMED_FLOOR:
        return ThesisVerdict(
            verdict="REAFFIRMED", action="HOLD", score=total,
            urgency="LOW", evidence=evidence,
            category_scores=cat_scores, resets_streak=True,
        )

    if total >= THESIS_REVIEW_MONITORING_FLOOR:
        return ThesisVerdict(
            verdict="MONITORING", action="HOLD_WITH_CAUTION", score=total,
            urgency="MEDIUM", evidence=evidence,
            category_scores=cat_scores, resets_streak=False,
        )

    if total >= THESIS_REVIEW_WEAKENED_FLOOR:
        # WEAKENED → resolve to executable: TRIM if can reduce, else HOLD_WITH_CAUTION
        action = "TRIM" if _can_reduce(strategy, quantity) else "HOLD_WITH_CAUTION"
        return ThesisVerdict(
            verdict="WEAKENED", action=action, score=total,
            urgency="HIGH", evidence=evidence,
            category_scores=cat_scores, resets_streak=False,
        )

    # DEGRADED
    return ThesisVerdict(
        verdict="DEGRADED", action="EXIT", score=total,
        urgency="MEDIUM", evidence=evidence,
        category_scores=cat_scores, resets_streak=False,
    )
