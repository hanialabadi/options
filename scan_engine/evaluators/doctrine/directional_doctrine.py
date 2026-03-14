"""
Directional strategy doctrine — Long Call/Put, LEAPs, Debit Spreads.

Signal direction: DELTA_CONVICTION.
"""

from ._rule import DoctrineRule, GraduatedRule

SIGNAL_DIRECTION = "DELTA_CONVICTION"

# ── Delta conviction ──────────────────────────────────────────
DELTA_CONVICTION = DoctrineRule(
    name="delta_conviction",
    threshold=0.45,
    comparison="gte",
    deduction=30,
    citation="Passarelli Ch.4: directional needs delta >= 0.45",
    note_pass="Delta conviction adequate",
    note_fail="Weak delta conviction (< 0.45)",
)

# ── Gamma floor (standard) ───────────────────────────────────
GAMMA_FLOOR = DoctrineRule(
    name="gamma_convexity",
    threshold=0.03,
    comparison="gte",
    deduction=30,
    citation="Passarelli Ch.4: short-dated directionals need gamma >= 0.03",
    note_pass="Gamma adequate for directional",
    note_fail="Low gamma — insufficient convexity",
)

# ── Gamma floor (LEAPs) ──────────────────────────────────────
GAMMA_FLOOR_LEAP = DoctrineRule(
    name="gamma_convexity_leap",
    threshold=0.008,
    comparison="gte",
    deduction=30,
    citation="Passarelli Ch.4: LEAPs have lower gamma by nature; vega dominates",
    note_pass="Gamma adequate for LEAP",
    note_fail="Low gamma for LEAP (< 0.008)",
)

# ── Weak conviction gate (low delta + low gamma) ─────────────
WEAK_CONVICTION = DoctrineRule(
    name="weak_conviction",
    comparison="custom",
    custom_fn=lambda abs_delta=0, gamma=0, is_leap=False, **kw: not (
        abs_delta < 0.30 and gamma < (0.004 if is_leap else 0.02)
    ),
    deduction=20,
    citation="Passarelli Ch.4: low delta + low gamma = coin flip",
    note_fail="Weak conviction (low Delta + low Gamma = coin flip)",
)

# ── Vega floor ────────────────────────────────────────────────
VEGA_FLOOR = DoctrineRule(
    name="vega_floor",
    threshold=0.18,
    comparison="gte",
    deduction=10,
    citation="Natenberg Ch.3: need adjustment potential",
    note_pass="Adequate vega for adjustment potential",
    note_fail="Low vega — limited adjustment potential",
)

# ── Trend alignment ──────────────────────────────────────────
TREND_ALIGNMENT_REQUIRED = True  # Murphy Ch.4-6

BULLISH_TREND = DoctrineRule(
    name="bullish_trend",
    comparison="custom",
    custom_fn=lambda trend=None, **kw: (
        trend is not None and trend in ('Bullish', 'Sustained Bullish')
    ),
    deduction=25,
    citation="Murphy Ch.4-6: trend must align with directional bet",
    note_pass="Trend aligned (Bullish)",
    note_fail="Trend misalignment for bullish strategy",
)

BEARISH_TREND = DoctrineRule(
    name="bearish_trend",
    comparison="custom",
    custom_fn=lambda trend=None, **kw: (
        trend is not None and trend in ('Bearish',)
    ),
    deduction=25,
    citation="Murphy Ch.4-6: trend must align with directional bet",
    note_pass="Trend aligned (Bearish)",
    note_fail="Trend misalignment for bearish strategy",
)

MISSING_TREND = DoctrineRule(
    name="missing_trend",
    comparison="custom",
    custom_fn=lambda trend=None, **kw: trend is not None,
    deduction=15,
    citation="Murphy Ch.4: trend confirmation required",
    note_fail="Missing trend data — confirmation required",
)

# ── Price structure (Murphy) ──────────────────────────────────
PRICE_VS_SMA20_BULLISH = DoctrineRule(
    name="price_vs_sma20_bullish",
    comparison="custom",
    custom_fn=lambda price_vs_sma20=None, **kw: (
        price_vs_sma20 is None or price_vs_sma20 >= 0
    ),
    deduction=20,
    citation="Murphy Ch.4: price below SMA20 = bearish structure",
    note_fail="Price below SMA20 — bearish structure for bullish strategy",
)

PRICE_VS_SMA20_BEARISH = DoctrineRule(
    name="price_vs_sma20_bearish",
    comparison="custom",
    custom_fn=lambda price_vs_sma20=None, **kw: (
        price_vs_sma20 is None or price_vs_sma20 <= 0
    ),
    deduction=20,
    citation="Murphy Ch.4: price above SMA20 = bullish structure",
    note_fail="Price above SMA20 — bullish structure for bearish strategy",
)

# ── Volume confirmation (Murphy Ch.6) ────────────────────────
VOLUME_BULLISH = DoctrineRule(
    name="volume_bullish",
    comparison="custom",
    custom_fn=lambda volume_trend=None, **kw: (
        volume_trend is not None and volume_trend in ('Rising', 'High', 'Increasing')
    ),
    deduction=20,
    citation="Murphy Ch.6: volume must confirm directional move",
    note_pass="Volume confirms uptrend",
    note_fail="Volume not supporting bullish move",
)

VOLUME_BEARISH = DoctrineRule(
    name="volume_bearish",
    comparison="custom",
    custom_fn=lambda volume_trend=None, **kw: (
        volume_trend is not None and volume_trend in ('Rising', 'High', 'Increasing')
    ),
    deduction=15,
    citation="Murphy Ch.6: volume confirms sell-off",
    note_pass="Volume confirms downtrend",
    note_fail="Volume weak for bearish strategy",
)

VOLUME_MISSING = DoctrineRule(
    name="volume_missing",
    comparison="custom",
    custom_fn=lambda volume_trend=None, **kw: volume_trend is not None,
    deduction=10,
    citation="Murphy Ch.6: volume confirmation required for directional",
    note_fail="Volume data missing — confirmation unavailable",
)

# ── Chart pattern (Bulkowski) ─────────────────────────────────
PATTERN_HIGH_CONF = DoctrineRule(
    name="pattern_high_confidence",
    threshold=70,
    comparison="gte",
    deduction=-10,  # Bonus (negative deduction)
    citation="Bulkowski: high-probability pattern (>= 70% success)",
    note_pass="High-confidence chart pattern detected",
)

PATTERN_MODERATE_CONF = DoctrineRule(
    name="pattern_moderate_confidence",
    threshold=60,
    comparison="gte",
    deduction=-5,  # Bonus
    citation="Bulkowski: moderate-probability pattern (>= 60% success)",
    note_pass="Moderate-confidence chart pattern detected",
)

PATTERN_LOW_CONF = DoctrineRule(
    name="pattern_low_confidence",
    threshold=50,
    comparison="lt",
    deduction=10,
    citation="Bulkowski: low-probability pattern (< 50%)",
    note_fail="Weak chart pattern — low success rate",
)

# ── Entry timing (Nison Ch.5-8, short-term only) ─────────────
ENTRY_STRONG = DoctrineRule(
    name="entry_strong",
    comparison="custom",
    custom_fn=lambda entry_timing=None, **kw: entry_timing == 'Strong',
    deduction=-10,  # Bonus
    citation="Nison Ch.5-8: strong reversal confirmation",
    note_pass="Strong entry timing confirmed",
)

ENTRY_MODERATE = DoctrineRule(
    name="entry_moderate",
    comparison="custom",
    custom_fn=lambda entry_timing=None, **kw: entry_timing == 'Moderate',
    deduction=-5,  # Bonus
    citation="Nison Ch.5-8: moderate confirmation",
    note_pass="Moderate entry timing",
)

ENTRY_WEAK = DoctrineRule(
    name="entry_weak",
    comparison="custom",
    custom_fn=lambda entry_timing=None, **kw: entry_timing == 'Weak',
    deduction=5,
    citation="Nison Ch.5-8: weak entry signal",
    note_fail="Weak entry signal — low confidence",
)

ENTRY_MISSING_SHORT_TERM = DoctrineRule(
    name="entry_missing_short_term",
    comparison="custom",
    custom_fn=lambda candlestick_pattern=None, **kw: candlestick_pattern is not None,
    deduction=10,
    citation="Nison: short-term entries need timing validation",
    note_fail="No candlestick confirmation for short-term entry",
)

# ── LEAP fallback penalty ────────────────────────────────────
LEAP_FALLBACK_BASE = 15
LEAP_FALLBACK_HALF_DTE = 20
