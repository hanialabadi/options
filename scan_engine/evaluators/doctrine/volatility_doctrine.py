"""
Volatility strategy doctrine — Straddle, Strangle.

Signal direction: BUY_VOL.
Favorable when HV > IV (RV/IV > 1.0) — options cheap relative to realized movement.
"""

from ._rule import DoctrineRule, GraduatedRule

SIGNAL_DIRECTION = "BUY_VOL"

# ── RV / IV Edge (CRITICAL — opposite from income) ───────────
RV_IV_EDGE = DoctrineRule(
    name="rv_iv_vol_edge",
    threshold=1.0,
    comparison="gt",              # RV/IV > 1.0 → HV > IV → options cheap
    deduction=0,
    citation="Natenberg Ch.10: buy vol when IV underprices realized movement",
    note_pass="Vol edge confirmed (HV > IV = options cheap)",
    note_fail="No vol edge (IV >= HV = buying expensive)",
)

RV_IV_STRONG = DoctrineRule(
    name="rv_iv_strong_vol_edge",
    threshold=1.15,
    comparison="gt",
    deduction=0,
    citation="Natenberg Ch.10: strong vol edge (IV significantly underpricing movement)",
    note_pass="Strong vol edge (RV/IV > 1.15 — IV underpricing realized movement)",
)

RV_IV_MARGINAL = DoctrineRule(
    name="rv_iv_marginal_vol",
    threshold=0.85,
    comparison="lt",              # RV/IV < 0.85 → buying slightly rich
    deduction=15,
    citation="Sinclair Ch.3: IV slightly above HV — buying slightly rich",
    note_fail="Marginal vol edge (IV slightly above HV)",
)

RV_IV_WEAK = DoctrineRule(
    name="rv_iv_weak_vol",
    threshold=0.70,
    comparison="lt",              # RV/IV < 0.70 → buying expensive
    deduction=30,
    citation="Sinclair Ch.3: IV well above HV — buying expensive vol",
    note_fail="Weak vol edge (IV well above HV)",
)

RV_IV_SEVERE = DoctrineRule(
    name="rv_iv_buying_expensive",
    threshold=0.70,
    comparison="lte",             # RV/IV <= 0.70 → IV >> HV
    deduction=40,
    citation="Sinclair Ch.3: IV >> HV — buying very expensive vol",
    note_fail="No vol edge (IV >> HV — buying very expensive vol)",
)

# ── Skew hard gate ────────────────────────────────────────────
SKEW_HARD_GATE = DoctrineRule(
    name="skew_violation",
    threshold=1.20,
    comparison="gt",
    hard_reject=True,
    citation="Passarelli Ch.8: puts overpriced, skew > 1.20",
    note_fail="SKEW VIOLATION — puts overpriced",
)

# ── VVIX hard gate ────────────────────────────────────────────
VVIX_HARD_GATE = DoctrineRule(
    name="vvix_extreme",
    threshold=130.0,
    comparison="gt",
    hard_reject=True,
    citation="Sinclair Ch.3: vol-of-vol too elevated, unpredictable",
    note_fail="HIGH VVIX — vol-of-vol too elevated, unpredictable",
)

VVIX_ELEVATED = DoctrineRule(
    name="vvix_elevated",
    threshold=100.0,
    comparison="gt",
    deduction=10,
    citation="Sinclair Ch.3: moderate vol uncertainty",
    note_fail="Elevated VVIX — moderate vol uncertainty",
    note_pass="Normal VVIX — vol predictable",
)

# ── IV percentile ─────────────────────────────────────────────
IV_PERCENTILE_SWEET = DoctrineRule(
    name="iv_percentile_expansion_zone",
    threshold=15.0,
    comparison="between",
    high=60.0,
    deduction=0,
    citation="Sinclair Ch.3: long vol needs expansion potential",
    note_pass="IV in expansion zone (favorable for long vol)",
)

IV_PERCENTILE_TOO_HIGH = DoctrineRule(
    name="iv_percentile_too_high",
    threshold=80.0,
    comparison="gt",
    deduction=25,
    citation="Sinclair Ch.3: buying elevated vol, limited expansion room",
    note_fail="Very high IV — buying expensive vol, limited expansion",
)

IV_PERCENTILE_ELEVATED = DoctrineRule(
    name="iv_percentile_elevated",
    threshold=60.0,
    comparison="gt",
    deduction=10,
    citation="Sinclair Ch.3: moderate expansion risk at elevated IV",
    note_fail="Elevated IV — moderate expansion risk",
)

IV_PERCENTILE_VERY_LOW = DoctrineRule(
    name="iv_percentile_very_low",
    threshold=15.0,
    comparison="lt",
    deduction=10,
    citation="Sinclair Ch.3: cheap but may stay compressed; needs catalyst",
    note_fail="Very low IV — cheap but may stay compressed",
)

# ── Vega requirement ──────────────────────────────────────────
VEGA_FLOOR = DoctrineRule(
    name="vega_floor",
    threshold=0.40,
    comparison="gte",
    deduction=40,
    citation="Passarelli Ch.8: vol strategy requires high vega sensitivity",
    note_pass="Adequate vega sensitivity",
    note_fail="Low vega — weak vol sensitivity",
)

# ── Delta neutral ─────────────────────────────────────────────
DELTA_NEUTRAL = DoctrineRule(
    name="delta_neutral",
    threshold=0.15,
    comparison="lte",
    deduction=20,
    citation="Natenberg Ch.15: straddle/strangle must be delta-neutral",
    note_pass="Delta neutral (|delta| <= 0.15)",
    note_fail="Directional bias — not neutral",
)

# ── Gamma requirements ────────────────────────────────────────
GAMMA_NEGATIVE_REJECT = DoctrineRule(
    name="gamma_negative_on_long_vol",
    threshold=0.0,
    comparison="lte",
    deduction=50,
    citation="Natenberg Ch.11: negative gamma on long vol = structurally wrong",
    note_fail="NEGATIVE Gamma on long vol strategy — structure incorrect",
)

GAMMA_DOLLAR_FLOOR_STRANGLE = DoctrineRule(
    name="dollar_gamma_strangle",
    threshold=2.0,
    comparison="gte",
    deduction=20,
    citation="Natenberg Ch.11: strangle needs >= $2 dollar-gamma",
    note_pass="Adequate dollar-gamma for strangle",
    note_fail="Low dollar-gamma for strangle",
)

GAMMA_DOLLAR_FLOOR_STRADDLE = DoctrineRule(
    name="dollar_gamma_straddle",
    threshold=3.0,
    comparison="gte",
    deduction=20,
    citation="Natenberg Ch.11: straddle needs >= $3 dollar-gamma",
    note_pass="Adequate dollar-gamma for straddle",
    note_fail="Low dollar-gamma for straddle",
)

GAMMA_PER_SHARE_STRANGLE = 0.04
GAMMA_PER_SHARE_STRADDLE = 0.06

# ── Gamma / Theta convexity efficiency ────────────────────────
GAMMA_THETA_RATIO = GraduatedRule(
    name="gamma_theta_convexity_efficiency",
    tiers=(
        (1.5, "gte", 0,  "✅ Excellent convexity (gamma/theta >= 1.5)"),
        (1.0, "gte", 0,  "✅ Good convexity efficiency (gamma/theta >= 1.0)"),
        (0.5, "gte", 15, "⚠️ Marginal convexity (gamma/theta 0.5-1.0)"),
        (None, None,  30, "❌ Poor convexity: theta dominates gamma"),
    ),
    citation="Passarelli Ch.2: dollar-gamma/dollar-theta measures convexity reward vs decay cost",
)

# ── Term structure (IV30/IV60 slope) ──────────────────────────
TERM_STRUCTURE = DoctrineRule(
    name="term_structure_slope",
    comparison="custom",
    custom_fn=lambda iv30=None, iv60=None, **kw: (
        iv30 < iv60 if iv30 is not None and iv60 is not None else None
    ),
    deduction=15,
    citation="Gatheral Ch.3 + Sinclair Ch.5: contango favors long vol",
    note_pass="Contango term structure (front vol cheap — favorable for long vol)",
    note_fail="Inverted term structure (front vol rich — headwind for long vol)",
)

TERM_STRUCTURE_SEVERE = DoctrineRule(
    name="term_structure_steep_inversion",
    comparison="custom",
    custom_fn=lambda iv30=None, iv60=None, **kw: (
        not ((iv30 - iv60) / iv60 > 0.15)
        if iv30 is not None and iv60 is not None and iv60 > 0
        else True
    ),
    deduction=25,
    citation="Gatheral: steep inversion (>15%) = significant front-vol overpricing",
    note_fail="Steep term structure inversion (>15%) — front vol severely overpriced",
)

# ── IV Momentum (direction of vol change) ────────────────────
IV_MOMENTUM_FALLING = DoctrineRule(
    name="iv_momentum_falling",
    comparison="custom",
    custom_fn=lambda iv_30d_5d_roc=None, **kw: (
        iv_30d_5d_roc is None or iv_30d_5d_roc >= -0.05
    ),
    deduction=15,
    citation="Sinclair Ch.5: long vol needs stable-to-rising IV environment",
    note_pass="IV stable or rising (favorable for long vol entry)",
    note_fail="IV falling > 5% in 5 days — headwind for long vol",
)

IV_MOMENTUM_COLLAPSING = DoctrineRule(
    name="iv_momentum_collapsing",
    comparison="custom",
    custom_fn=lambda iv_30d_10d_roc=None, **kw: (
        iv_30d_10d_roc is None or iv_30d_10d_roc >= -0.15
    ),
    deduction=25,
    citation="Sinclair Ch.5: collapsing IV = vol selling pressure, avoid long vol",
    note_fail="IV collapsing (10D ROC < -15%) — severe headwind for long vol",
)

# ── Expected Move Coverage ────────────────────────────────────
EXPECTED_MOVE_COVERAGE = GraduatedRule(
    name="expected_move_to_premium_ratio",
    tiers=(
        (1.5, "gte", 0,  "✅ Expected move well exceeds premium (ratio >= 1.5)"),
        (1.0, "gte", 10, "⚠️ Expected move covers premium but tight (ratio 1.0-1.5)"),
        (0.7, "gte", 25, "⚠️ Expected move barely covers premium (ratio 0.7-1.0)"),
        (None, None,  35, "❌ Premium exceeds expected move — overpaying for vol"),
    ),
    citation="Natenberg Ch.9 + Sinclair Ch.3: expected move framework",
)

# ── Regime gating (Sinclair) ─────────────────────────────────
REGIME_EXPANSION = DoctrineRule(
    name="regime_expansion",
    comparison="custom",
    custom_fn=lambda vol_regime=None, **kw: (
        vol_regime not in ('Expansion', 'High Vol')
        if vol_regime is not None else True
    ),
    deduction=30,
    citation="Sinclair Ch.2-4: don't buy already elevated vol",
    note_pass="Favorable regime for long vol",
    note_fail="Wrong regime — don't buy elevated vol",
)

REGIME_FAVORABLE = DoctrineRule(
    name="regime_favorable",
    comparison="custom",
    custom_fn=lambda vol_regime=None, **kw: (
        vol_regime in ('Compression', 'Low Vol')
        if vol_regime is not None else False
    ),
    deduction=0,
    citation="Sinclair Ch.3: compression/low vol = favorable for long vol",
    note_pass="Favorable regime (Compression/Low Vol)",
)

REGIME_MISSING = DoctrineRule(
    name="regime_missing",
    comparison="custom",
    custom_fn=lambda vol_regime=None, **kw: vol_regime is not None,
    deduction=20,
    citation="Sinclair Ch.2: regime classification required",
    note_fail="Missing vol regime — classification required",
)

# ── Catalyst (Sinclair Ch.3) ─────────────────────────────────
CATALYST_NEAR_TERM = DoctrineRule(
    name="catalyst_near_term",
    comparison="custom",
    custom_fn=lambda catalyst=None, **kw: (
        catalyst is not None and catalyst <= 30
    ),
    deduction=0,
    citation="Sinclair Ch.3: long vol benefits from event justification",
    note_pass="Catalyst present — justified vol purchase",
)

CATALYST_MISSING = DoctrineRule(
    name="catalyst_missing_straddle",
    comparison="custom",
    custom_fn=lambda catalyst=None, **kw: (
        catalyst is not None and catalyst <= 30
    ),
    deduction=20,
    citation="Sinclair Ch.3: straddle/strangle without catalyst = unanchored bet",
    note_fail="No near-term catalyst — long vol benefits from event justification",
)

# ── Vol spike / clustering (Sinclair Ch.4) ────────────────────
VOL_SPIKE_RECENT_DAYS = 5
VOL_SPIKE_HARD_REJECT = True

# ── Skew missing penalty ─────────────────────────────────────
SKEW_MISSING_PENALTY = 15
