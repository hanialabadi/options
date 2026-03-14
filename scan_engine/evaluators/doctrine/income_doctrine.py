"""
Income strategy doctrine — Cash-Secured Put, Covered Call, Buy-Write.

Signal direction: SELL_VOL.
Favorable when IV > HV (RV/IV < 1.0) — selling rich premium.
"""

from ._rule import DoctrineRule, GraduatedRule

SIGNAL_DIRECTION = "SELL_VOL"

# ── RV / IV Edge ──────────────────────────────────────────────
RV_IV_EDGE = DoctrineRule(
    name="rv_iv_premium_edge",
    threshold=1.0,
    comparison="lt",              # RV/IV < 1.0 → IV > HV → selling rich
    deduction=0,
    citation="Sinclair Ch.11: sell vol when IV > RV",
    note_pass="Premium edge confirmed (IV > HV)",
    note_fail="Weak premium edge (HV >= IV)",
)

RV_IV_STRONG = DoctrineRule(
    name="rv_iv_strong_premium",
    threshold=0.80,
    comparison="lt",              # RV/IV < 0.80 → IV significantly > HV
    deduction=0,
    citation="Sinclair Ch.11: optimal sell zone",
    note_pass="Strong premium edge (IV significantly elevated vs HV)",
)

RV_IV_MARGINAL = DoctrineRule(
    name="rv_iv_marginal",
    threshold=1.10,
    comparison="gt",              # RV/IV > 1.10 → weak selling edge
    deduction=25,
    citation="Sinclair: HV exceeds IV by 10%+ — headwind for selling",
    note_fail="Weak premium edge (HV exceeds IV by 10%+)",
)

RV_IV_SEVERE = DoctrineRule(
    name="rv_iv_severe",
    threshold=1.25,
    comparison="gt",              # RV/IV > 1.25 → selling very cheap
    deduction=35,
    citation="Sinclair: HV >> IV — premium too cheap to sell",
    note_fail="No premium edge (HV >> IV — premium is cheap)",
)

# ── POP Gate (Graduated) ─────────────────────────────────────
POP_GATE = GraduatedRule(
    name="pop_gate",
    tiers=(
        (65.0, "gte", 0,  "✅ POP >= 65% (Cohen Ch.28)"),
        (50.0, "gte", 20, "⚠️ POP 50-65% — marginal win rate"),
        (None, None,   0, "❌ POP < 50% — reject"),
    ),
    hard_reject_below=50.0,
    citation="Cohen Ch.28 + Given Ch.7: credit spreads viable at 50-65%",
)

# ── IV Percentile ─────────────────────────────────────────────
IV_PERCENTILE_SWEET = DoctrineRule(
    name="iv_percentile_sweet_spot",
    threshold=40.0,
    comparison="between",
    high=80.0,
    deduction=0,
    citation="Given Ch.4: sell premium when IV elevated",
    note_pass="IV in income sweet spot (40-80th percentile)",
    note_fail="IV outside premium-selling sweet spot",
)

IV_PERCENTILE_LOW = DoctrineRule(
    name="iv_percentile_too_low",
    threshold=25.0,
    comparison="lt",
    deduction=20,
    citation="Cohen Ch.28: low IV = cheap premium, not worth selling",
    note_fail="IV too low — premium insufficient for income strategy",
)

# ── Theta (must be positive for income sellers) ───────────────
THETA_POSITIVE = DoctrineRule(
    name="theta_positive",
    threshold=0.0,
    comparison="gt",
    deduction=30,
    citation="Natenberg Ch.7: income positions require positive theta",
    note_pass="Positive theta (time decay works for seller)",
    note_fail="Non-positive theta — income position not benefiting from decay",
)

# ── Gamma sign (income sellers have negative gamma) ───────────
GAMMA_NEGATIVE = DoctrineRule(
    name="gamma_negative",
    threshold=0.05,
    comparison="lt",              # gamma < 0.05 (should be negative)
    deduction=20,
    citation="Natenberg Ch.7: income sellers have negative gamma",
    note_pass="Correct gamma sign for short premium structure",
    note_fail="Positive gamma on income position — verify contract structure",
)

# ── Short-DTE gamma spike ────────────────────────────────────
SHORT_DTE_GAMMA_SPIKE = DoctrineRule(
    name="short_dte_gamma_spike",
    comparison="custom",
    custom_fn=lambda actual_dte, gamma: (
        actual_dte is not None and gamma is not None
        and actual_dte < 21 and gamma < -0.05
    ),
    deduction=15,
    citation="Passarelli Ch.2: gamma spikes < 21 DTE — gap risk elevated",
    note_fail="Short DTE + high short gamma — gap risk elevated",
)

# ── IV Momentum (income: falling IV = favorable) ─────────────
IV_MOMENTUM_RISING = DoctrineRule(
    name="iv_momentum_rising",
    comparison="custom",
    custom_fn=lambda iv_30d_5d_roc=None, **kw: (
        iv_30d_5d_roc is not None and iv_30d_5d_roc > 0.05
    ),
    deduction=10,
    citation="Sinclair Ch.11: premium sellers benefit from falling/stable IV",
    note_pass="IV stable or falling (favorable for premium sellers)",
    note_fail="IV rising > 5% — headwind for premium sellers (but selling richer)",
)

# ── Vega / Theta ratio (extreme only) ────────────────────────
VEGA_THETA_EXTREME = DoctrineRule(
    name="vega_theta_extreme",
    threshold=3.0,
    comparison="gt",
    deduction=10,
    citation="Natenberg Ch.7: extreme vega/theta = outsized vol exposure for income",
    note_fail="High vega/theta ratio — short position has elevated vol exposure",
)

# ── Market Structure (Murphy) ─────────────────────────────────
CSP_TREND_BULLISH = DoctrineRule(
    name="csp_trend_bullish",
    comparison="custom",
    custom_fn=lambda trend=None, **kw: (
        trend is not None and trend in ('Bullish', 'Sustained Bullish')
    ),
    deduction=20,
    citation="Murphy Ch.4-6: CSP requires bullish structure",
    note_pass="Bullish structure confirmed for CSP",
    note_fail="CSP in non-bullish trend — structural risk",
)

CSP_PRICE_ABOVE_SMA20 = DoctrineRule(
    name="csp_price_above_sma20",
    comparison="custom",
    custom_fn=lambda price_vs_sma20=None, **kw: (
        price_vs_sma20 is not None and price_vs_sma20 >= 0
    ),
    deduction=15,
    citation="Murphy Ch.4: price below SMA20 = weak structure for CSP",
    note_fail="CSP: price below SMA20 — weak structure",
)

CC_NOT_BEARISH = DoctrineRule(
    name="cc_not_bearish",
    comparison="custom",
    custom_fn=lambda trend=None, **kw: (
        trend is None or trend != 'Bearish'
    ),
    deduction=25,
    citation="Murphy Ch.4: covered call in bearish trend = structural risk",
    note_fail="Covered Call in bearish trend — structural risk",
)
