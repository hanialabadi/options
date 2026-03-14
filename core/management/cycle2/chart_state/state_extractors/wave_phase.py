"""
Wave Phase Classifier — determines where a position is in its move lifecycle.

Reads existing Cycle 2 chart state outputs (MomentumVelocity, TrendIntegrity,
PriceStructure, RecoveryQuality) plus chart primitives (SMA distance, RSI,
divergences) and produces a single phase classification.

The phase drives two management decisions:
  1. Scale-up eligibility — only BUILDING phase qualifies
  2. Doctrine urgency — EXHAUSTION/REVERSAL tighten exit leash

Pure function: (row) -> ChartStateResult.  No DB, no side effects.

Phase lifecycle (wave metaphor):
  FORMING    → wave building, structure developing, not confirmed yet
  BUILDING   → confirmed trend, momentum expanding — SCALE-UP WINDOW
  PEAKING    → move extended, momentum plateauing — hold, don't add
  FADING     → momentum declining, divergences appearing — tighten stops
  EXHAUSTED  → move complete, reversal signals — exit
  RECOVERING → bounce after decline — watch for trap vs structural shift
  STALLED    → no directional conviction — no action
"""

import pandas as pd
from ..base import ChartStateResult
from ..state_definitions import MomentumVelocityState


# ---------------------------------------------------------------------------
# Wave Phase enum values (plain strings — matches ChartStateResult pattern)
# ---------------------------------------------------------------------------
FORMING = "FORMING"
BUILDING = "BUILDING"
PEAKING = "PEAKING"
FADING = "FADING"
EXHAUSTED = "EXHAUSTED"
RECOVERING = "RECOVERING"
STALLED = "STALLED"
UNKNOWN = "UNKNOWN"

# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------
SMA_EXTENSION_MODERATE = 0.05   # 5% from SMA20 — getting extended
SMA_EXTENSION_EXTREME = 0.12   # 12% from SMA20 — overextended (McMillan)
PNL_PROFITABLE = 0.0           # Position must be profitable for scale-up
PNL_SCALE_MIN = 0.05           # 5% P&L minimum for scale-up eligibility


def compute_wave_phase(row: pd.Series) -> ChartStateResult:
    """
    Classify where a position is in its move lifecycle.

    Reads from already-computed Cycle 2 columns:
      - MomentumVelocity_State  (ACCELERATING, TRENDING, LATE_CYCLE, etc.)
      - TrendIntegrity_State    (STRONG_TREND, WEAK_TREND, TREND_EXHAUSTED, NO_TREND)
      - PriceStructure_State    (STRUCTURAL_UP, STRUCTURAL_DOWN, RANGE_BOUND, etc.)
      - RecoveryQuality_State   (STRUCTURAL_RECOVERY, DEAD_CAT_BOUNCE, etc.)

    Plus chart primitives:
      - sma_distance_pct        (price extension from SMA20)
      - rsi_14                  (RSI for divergence detection)
      - rsi_slope               (RSI direction)
      - momentum_slope          (momentum direction)
      - Price_Drift_Pct         (how far price has moved since entry)

    Returns ChartStateResult with:
      - state: one of FORMING, BUILDING, PEAKING, FADING, EXHAUSTED,
               RECOVERING, STALLED, UNKNOWN
      - raw_metrics: dict of inputs used
      - resolution_reason: human-readable explanation
      - data_complete: False only if MomentumVelocity is missing
    """
    # ── Read Cycle 2 state columns ─────────────────────────────────────────
    mom_vel = _sn(row, "MomentumVelocity_State")
    trend_int = _sn(row, "TrendIntegrity_State")
    price_struct = _sn(row, "PriceStructure_State")
    recovery = _sn(row, "RecoveryQuality_State")

    # ── Read chart primitives ──────────────────────────────────────────────
    sma_dist = _sf(row, "sma_distance_pct", 0.0)
    rsi = _sf(row, "rsi_14", 50.0)
    rsi_slope = _sf(row, "rsi_slope", 0.0)
    mom_slope = _sf(row, "momentum_slope", 0.0)
    pnl_pct = _sf(row, "PnL_Pct", None)
    drift_pct = _sf(row, "Price_Drift_Pct", 0.0)

    raw = {
        "MomentumVelocity_State": mom_vel,
        "TrendIntegrity_State": trend_int,
        "PriceStructure_State": price_struct,
        "RecoveryQuality_State": recovery,
        "sma_distance_pct": sma_dist,
        "rsi_14": rsi,
        "rsi_slope": rsi_slope,
        "momentum_slope": mom_slope,
        "PnL_Pct": pnl_pct,
        "Price_Drift_Pct": drift_pct,
    }

    # ── Gate: need MomentumVelocity at minimum ─────────────────────────────
    if mom_vel in (None, "", "UNKNOWN", "NOT_APPLICABLE"):
        return ChartStateResult(
            state=UNKNOWN,
            raw_metrics=raw,
            resolution_reason="MISSING_MOMENTUM_VELOCITY",
            data_complete=False,
        )

    # ── Classification tree (evaluated top-down, first match wins) ─────────

    # 1. EXHAUSTED — move complete, momentum reversed or trend broken
    if mom_vel == "REVERSING" and trend_int in ("TREND_EXHAUSTED", "NO_TREND"):
        return _result(EXHAUSTED, raw,
            f"EXHAUSTED: momentum REVERSING + trend {trend_int} — move complete")

    if mom_vel == "REVERSING" and price_struct == "STRUCTURE_BROKEN":
        return _result(EXHAUSTED, raw,
            f"EXHAUSTED: momentum REVERSING + structure BROKEN — wave terminated")

    # 2. RECOVERING — bounce after decline, needs qualification
    if recovery == "STRUCTURAL_RECOVERY":
        return _result(RECOVERING, raw,
            f"RECOVERING: structural recovery confirmed — watch for trend development")

    if recovery == "DEAD_CAT_BOUNCE":
        return _result(FADING, raw,
            f"FADING: dead cat bounce — not structural recovery (McMillan Ch.3)")

    # 3. FADING — momentum declining, divergences present
    if mom_vel == "LATE_CYCLE":
        # LATE_CYCLE already requires 2+ divergence signals in momentum_velocity
        return _result(FADING, raw,
            f"FADING: LATE_CYCLE momentum — divergences present, "
            f"SMA dist={sma_dist:+.1%}, RSI={rsi:.0f}")

    if mom_vel == "DECELERATING" and trend_int in ("WEAK_TREND", "TREND_EXHAUSTED"):
        return _result(FADING, raw,
            f"FADING: decelerating momentum + {trend_int} — wave losing energy")

    # 4. PEAKING — move extended, momentum plateauing
    if mom_vel == "TRENDING" and abs(sma_dist) >= SMA_EXTENSION_EXTREME:
        return _result(PEAKING, raw,
            f"PEAKING: trending but overextended {sma_dist:+.1%} from SMA20 "
            f"(>={SMA_EXTENSION_EXTREME:.0%} threshold)")

    if mom_vel == "TRENDING" and rsi > 70 and rsi_slope <= 0:
        return _result(PEAKING, raw,
            f"PEAKING: trending + RSI={rsi:.0f} overbought and flattening")

    if mom_vel == "TRENDING" and rsi < 30 and rsi_slope >= 0:
        # Put-side peaking (RSI oversold and recovering)
        return _result(PEAKING, raw,
            f"PEAKING: trending + RSI={rsi:.0f} oversold and recovering")

    # 5. BUILDING — confirmed trend, momentum expanding (SCALE-UP WINDOW)
    if mom_vel == "ACCELERATING" and trend_int in ("STRONG_TREND", "WEAK_TREND"):
        if abs(sma_dist) < SMA_EXTENSION_EXTREME:
            return _result(BUILDING, raw,
                f"BUILDING: accelerating momentum + {trend_int}, "
                f"SMA dist={sma_dist:+.1%} — scale-up window")

    if mom_vel == "TRENDING" and trend_int == "STRONG_TREND":
        if abs(sma_dist) < SMA_EXTENSION_MODERATE:
            return _result(BUILDING, raw,
                f"BUILDING: strong trend + trending momentum, "
                f"SMA dist={sma_dist:+.1%} — scale-up eligible")

    # 6. FORMING — trend emerging, not confirmed yet
    if mom_vel == "ACCELERATING" and trend_int in ("NO_TREND", None, ""):
        return _result(FORMING, raw,
            f"FORMING: momentum accelerating but trend not confirmed yet")

    if mom_vel == "TRENDING" and trend_int == "WEAK_TREND":
        return _result(FORMING, raw,
            f"FORMING: trending momentum + weak trend — needs confirmation")

    # 7. STALLED — no directional conviction
    if mom_vel in ("STALLING", "DECELERATING"):
        return _result(STALLED, raw,
            f"STALLED: momentum {mom_vel}, no directional conviction")

    # 8. PEAKING fallback — accelerating but overextended
    if mom_vel == "ACCELERATING" and abs(sma_dist) >= SMA_EXTENSION_EXTREME:
        return _result(PEAKING, raw,
            f"PEAKING: accelerating but overextended {sma_dist:+.1%} — don't chase")

    # 9. FADING fallback — REVERSING without full exhaustion
    if mom_vel == "REVERSING":
        return _result(FADING, raw,
            f"FADING: momentum reversing (ROC crossover) — tighten stops")

    # 10. Default — FORMING if trending, STALLED otherwise
    if mom_vel == "TRENDING":
        return _result(FORMING, raw,
            f"FORMING: trending momentum, watching for confirmation")

    return _result(STALLED, raw,
        f"STALLED: mom_vel={mom_vel}, trend={trend_int} — no clear wave")


def is_scale_up_eligible(wave_phase: str, pnl_pct: float | None) -> bool:
    """
    Check if a position is eligible for scale-up based on wave phase and P&L.

    Scale-up requires:
      1. Wave phase = BUILDING (confirmed trend, momentum expanding)
      2. Position is profitable (P&L > 5%)

    Called by run_all.py MFE-Based Winner Expansion to gate scale-up requests.
    """
    if wave_phase != BUILDING:
        return False
    if pnl_pct is None or pnl_pct < PNL_SCALE_MIN:
        return False
    return True


def is_trim_eligible(
    wave_phase: str, mfe_pct: float | None, pnl_pct: float | None,
) -> tuple[bool, float]:
    """
    Check if a multi-contract position should trim (partial close).

    Returns (should_trim, trim_fraction):
      - PEAKING + MFE ≥ 20% → trim 25%  (McMillan Ch.4: protect extended gains)
      - EXHAUSTED or FADING  → trim 50%  (Passarelli Ch.6: exit fading momentum)
      - All others           → (False, 0.0)

    Trim fraction is the proportion of contracts to close.  Rounding to integer
    contract count and "never trim to zero" guard are handled by the caller
    (gate_income_trim in shared_income_gates.py).
    """
    from core.management.cycle3.doctrine.thresholds import (
        INCOME_TRIM_PEAK_PCT,
        INCOME_TRIM_EXHAUSTION_PCT,
        MFE_SIGNIFICANT,
    )

    if wave_phase == PEAKING:
        if mfe_pct is not None and mfe_pct >= MFE_SIGNIFICANT:
            return True, INCOME_TRIM_PEAK_PCT
    if wave_phase in (EXHAUSTED, FADING):
        return True, INCOME_TRIM_EXHAUSTION_PCT
    return False, 0.0


def compute_pyramid_add_contracts(
    base_quantity: int,
    pyramid_tier: int,
    wave_phase: str,
    pnl_pct: float,
    conviction_status: str,
    momentum_state: str,
) -> int:
    """
    Compute how many contracts to add for a pyramid scale-up.

    Uses the *frozen* entry quantity (Base_Quantity / Entry_Quantity) as base,
    NOT current Quantity — prevents compounding after prior adds.

    McMillan Ch.4 / Passarelli Ch.6 pyramid sizing:
      tier 0 → 60% of base (first add)
      tier 1 → 30% of base (second add)
      tier ≥ 2 → 0 (position is full)
    Floor at 1 contract.

    Gates (return 0 if any fail):
      - wave_phase must be BUILDING
      - pnl ≥ 5%
      - conviction not WEAKENING or REVERSING
      - momentum not REVERSING or DECELERATING
    """
    from core.management.cycle3.doctrine.thresholds import (
        PYRAMID_TIER_0_RATIO,
        PYRAMID_TIER_1_RATIO,
        PYRAMID_PNL_MIN,
    )

    # ── Gates ─────────────────────────────────────────────────────────────
    if wave_phase != BUILDING:
        return 0
    if pnl_pct < PYRAMID_PNL_MIN:
        return 0
    if conviction_status in ("WEAKENING", "REVERSING"):
        return 0
    if momentum_state in ("REVERSING", "DECELERATING"):
        return 0

    # ── Tier sizing ───────────────────────────────────────────────────────
    if pyramid_tier == 0:
        ratio = PYRAMID_TIER_0_RATIO
    elif pyramid_tier == 1:
        ratio = PYRAMID_TIER_1_RATIO
    else:
        return 0  # full position — no more adds

    base = max(1, base_quantity)  # normalize to contract units, floor 1
    add = max(1, round(base * ratio))
    return add


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sn(row: pd.Series, col: str) -> str | None:
    """Safe string from row, None-aware."""
    v = row.get(col)
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    return str(v).strip()


def _sf(row: pd.Series, col: str, default: float | None) -> float | None:
    """Safe float from row, NaN-aware."""
    v = row.get(col)
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _result(phase: str, raw: dict, reason: str) -> ChartStateResult:
    """Build a ChartStateResult for the given wave phase."""
    return ChartStateResult(
        state=phase,
        raw_metrics=raw,
        resolution_reason=reason,
        data_complete=True,
    )
