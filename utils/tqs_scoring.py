"""
TQS — Timing Quality Score

Answers: "Is this the right MOMENT to enter the structural trade?"

DQS answers "Is this direction correct?" — TQS is orthogonal to it.
A trade can have DQS=93 (strong structural edge) and TQS=35 (terrible timing —
stock already extended, momentum exhausted, mean reversion likely).

Four components (additive, 0-100 total):

  1. Extension risk     30 pts  — how far price has stretched from SMA20/SMA50.
                                  Overextended = short-term mean reversion risk.
  2. Entry context     25 pts  — EARLY/MODERATE/LATE classification from step5.
                                  LATE_SHORT/LATE_LONG = chasing an exhausted move.
  3. Momentum impulse  25 pts  — RSI level + momentum_tag. Extreme readings
                                  (RSI<20 or >80) signal exhaustion, not conviction.
  4. EMA cross age     20 pts  — Days_Since_Cross. Fresh crosses are higher
                                  probability; stale crosses (110d+) are lower.

Score bands:
  Ideal     >= 75  — well-timed entry, no mean-reversion flags
  Acceptable  50–74 — tradeable but at least one timing caution
  Stretched   25–49 — entry is late or extended; reduce size or wait for pullback
  Chase       <  25 — multiple extension flags; high mean-reversion risk

Design principles:
  - Never modify DQS. These are separate dimensions.
  - Direction-aware: LATE_SHORT is bad for puts, LATE_LONG is bad for calls.
  - Extension penalty is symmetric: -18% from SMA20 is as bad as +18%.
  - RSI extremes flag exhaustion for BOTH directions (contrarian risk at edges).
  - No liquidity or capital inputs — purely price/momentum timing.
"""

import pandas as pd
import numpy as np
from typing import Tuple


# ---------------------------------------------------------------------------
# Component 1 — Extension risk  (30 pts max)
# ---------------------------------------------------------------------------
# Measures how far price has stretched from its mean.
# SMA20 is the primary signal (short-term mean reversion).
# SMA50 adds context for deeper stretches.
#
# For BOTH calls and puts — being far from mean in EITHER direction
# raises mean-reversion risk for the entry, regardless of thesis direction.
# A put entered when stock is already -15% from SMA20 has already captured
# much of the near-term downside; a bounce is the next likely move.

def _score_extension(row: pd.Series) -> Tuple[float, str]:
    try:
        dist_20 = abs(float(row.get('Price_vs_SMA20') or 0))
    except (TypeError, ValueError):
        dist_20 = 0.0
    try:
        dist_50 = abs(float(row.get('Price_vs_SMA50') or 0))
    except (TypeError, ValueError):
        dist_50 = 0.0

    # Primary: SMA20 distance
    if dist_20 <= 2.0:
        pts_20 = 30.0
        note_20 = f"price near SMA20 ({dist_20:.1f}% away) — no extension"
    elif dist_20 <= 5.0:
        pts_20 = 24.0
        note_20 = f"mild extension from SMA20 ({dist_20:.1f}%)"
    elif dist_20 <= 10.0:
        pts_20 = 15.0
        note_20 = f"extended from SMA20 ({dist_20:.1f}%) — mean reversion risk"
    elif dist_20 <= 15.0:
        pts_20 = 7.0
        note_20 = f"significantly extended from SMA20 ({dist_20:.1f}%) — bounce risk"
    else:
        pts_20 = 0.0
        note_20 = f"severely extended from SMA20 ({dist_20:.1f}%) — high mean reversion risk"

    # SMA50 modifier: if deeply extended from 50SMA too, apply additional penalty
    if dist_50 > 20.0:
        pts_20 = max(0.0, pts_20 - 8.0)
        note_20 += f"; also {dist_50:.1f}% from SMA50 (multi-timeframe stretch)"
    elif dist_50 > 12.0:
        pts_20 = max(0.0, pts_20 - 4.0)
        note_20 += f"; {dist_50:.1f}% from SMA50"

    return round(pts_20, 1), note_20


# ---------------------------------------------------------------------------
# Component 2 — Entry context  (25 pts max)
# ---------------------------------------------------------------------------
# entry_timing_context from step5 chart signals:
#   EARLY_LONG / EARLY_SHORT : catching early in the move (best timing)
#   EARLY / MODERATE         : reasonable entry point
#   LATE_LONG                : entering a call late in a bullish move (chasing)
#   LATE_SHORT               : entering a put late in a bearish move (chasing)
#
# Direction-aware: LATE_SHORT is fine for a call (momentum is against you,
# but you're not chasing your own direction). LATE_SHORT is a red flag for a put.

def _score_entry_context(row: pd.Series) -> Tuple[float, str]:
    ctx      = str(row.get('entry_timing_context') or '').upper().strip()
    strategy = str(row.get('Strategy_Name', row.get('Strategy', ''))).lower()
    is_put   = 'put' in strategy

    # EARLY in the direction = best
    if ctx in ('EARLY_SHORT',) and is_put:
        return 25.0, "EARLY_SHORT — entering early in bearish move (25/25)"
    if ctx in ('EARLY_LONG',) and not is_put:
        return 25.0, "EARLY_LONG — entering early in bullish move (25/25)"
    if ctx == 'EARLY':
        return 22.0, "EARLY — early entry, good timing (22/25)"

    # MODERATE = standard, acceptable
    if ctx == 'MODERATE':
        return 16.0, "MODERATE — standard entry point (16/25)"

    # LATE in your own direction = chasing
    if ctx == 'LATE_SHORT' and is_put:
        return 5.0, "LATE_SHORT — entering put after extended bearish move (5/25)"
    if ctx == 'LATE_LONG' and not is_put:
        return 5.0, "LATE_LONG — entering call after extended bullish move (5/25)"

    # LATE in opposite direction = contrarian entry (acceptable for momentum plays)
    if ctx == 'LATE_SHORT' and not is_put:
        return 14.0, "LATE_SHORT — bearish context, calling a put on contrarian (14/25)"
    if ctx == 'LATE_LONG' and is_put:
        return 14.0, "LATE_LONG — bullish context, bearish fade entry (14/25)"

    # Unknown / missing
    return 12.0, f"Entry context unknown ({ctx or 'missing'}) — neutral (12/25)"


# ---------------------------------------------------------------------------
# Component 3 — Momentum impulse  (25 pts max)
# ---------------------------------------------------------------------------
# Two sub-signals:
#   RSI level (15 pts): extremes flag exhaustion, not just direction
#   momentum_tag (10 pts): STRONG_DOWN/UP_DAY on day of entry = extended impulse
#
# Key insight: RSI < 20 or > 80 doesn't mean "great timing" for your direction —
# it means the move is exhausted and a mean reversion is more likely.
# The sweet spot for a put entry is RSI 30-45 (falling, not yet oversold).
# The sweet spot for a call entry is RSI 55-70 (rising, not yet overbought).
# RSI < 20 for a put = the easy money is already made; enter smaller or wait.

def _score_momentum(row: pd.Series) -> Tuple[float, str]:
    strategy = str(row.get('Strategy_Name', row.get('Strategy', ''))).lower()
    is_put   = 'put' in strategy

    pts  = 0.0
    note_parts = []

    # ── RSI (15 pts) ──────────────────────────────────────────────────────────
    try:
        rsi = float(row.get('RSI') or 50)
        if is_put:
            if 28 <= rsi <= 45:
                pts += 15.0; note_parts.append(f"RSI {rsi:.0f} — falling, not yet washed out (15/15)")
            elif 45 < rsi <= 55:
                pts += 10.0; note_parts.append(f"RSI {rsi:.0f} — mid-range, thesis not confirmed (10/15)")
            elif 20 <= rsi < 28:
                pts += 6.0;  note_parts.append(f"RSI {rsi:.0f} — oversold, bounce risk elevated (6/15)")
            elif rsi < 20:
                pts += 2.0;  note_parts.append(f"RSI {rsi:.0f} — extreme oversold, high mean reversion risk (2/15)")
            elif 55 < rsi <= 65:
                pts += 5.0;  note_parts.append(f"RSI {rsi:.0f} — bullish range for put entry (5/15)")
            else:
                pts += 8.0;  note_parts.append(f"RSI {rsi:.0f} — overbought, contrarian put opportunity (8/15)")
        else:  # call
            if 55 <= rsi <= 72:
                pts += 15.0; note_parts.append(f"RSI {rsi:.0f} — rising, not yet extended (15/15)")
            elif 45 <= rsi < 55:
                pts += 10.0; note_parts.append(f"RSI {rsi:.0f} — mid-range, thesis not confirmed (10/15)")
            elif 72 < rsi <= 80:
                pts += 6.0;  note_parts.append(f"RSI {rsi:.0f} — overbought, pullback risk (6/15)")
            elif rsi > 80:
                pts += 2.0;  note_parts.append(f"RSI {rsi:.0f} — extreme overbought, high mean reversion risk (2/15)")
            elif 35 < rsi < 45:
                pts += 5.0;  note_parts.append(f"RSI {rsi:.0f} — bearish range for call (5/15)")
            else:
                pts += 8.0;  note_parts.append(f"RSI {rsi:.0f} — oversold, contrarian call opportunity (8/15)")
    except (TypeError, ValueError):
        pts += 7.0; note_parts.append("RSI missing — neutral")

    # ── Momentum tag (10 pts) ─────────────────────────────────────────────────
    mtag = str(row.get('momentum_tag') or '').upper()
    if mtag == 'NORMAL' or mtag == '':
        pts += 10.0; note_parts.append("momentum normal — no impulse flags")
    elif mtag == 'FLAT_DAY':
        pts += 8.0;  note_parts.append("FLAT_DAY — low impulse, controlled entry")
    elif mtag == 'STRONG_DOWN_DAY' and is_put:
        pts += 3.0;  note_parts.append("STRONG_DOWN_DAY — entering put on capitulation day (3/10)")
    elif mtag == 'STRONG_UP_DAY' and not is_put:
        pts += 3.0;  note_parts.append("STRONG_UP_DAY — entering call on gap/surge day (3/10)")
    elif mtag == 'STRONG_DOWN_DAY' and not is_put:
        pts += 9.0;  note_parts.append("STRONG_DOWN_DAY — bearish impulse, call is contrarian (9/10)")
    elif mtag == 'STRONG_UP_DAY' and is_put:
        pts += 9.0;  note_parts.append("STRONG_UP_DAY — bullish impulse, put is contrarian (9/10)")
    else:
        pts += 5.0;  note_parts.append(f"{mtag} — partial (5/10)")

    return round(pts, 1), "; ".join(note_parts)


# ---------------------------------------------------------------------------
# Component 4 — EMA cross age  (20 pts max)
# ---------------------------------------------------------------------------
# A fresh EMA cross (< 10 days) is the highest-probability entry for a
# trend-following directional trade. As the cross ages, the easy money
# is progressively captured and mean reversion / consolidation becomes
# more likely.
#
# Days_Since_Cross: 0 = no cross (neutral credit); >0 = cross exists
# Has_Crossover: bool — if no crossover at all, neutral score

def _score_cross_age(row: pd.Series) -> Tuple[float, str]:
    try:
        has_cross = bool(row.get('Has_Crossover'))
        days      = float(row.get('Days_Since_Cross') or 0)
    except (TypeError, ValueError):
        return 10.0, "Cross data missing — neutral (10/20)"

    if not has_cross:
        return 10.0, "No EMA crossover — neutral credit (10/20)"

    # ── ADX modifier (Murphy Ch.6) ─────────────────────────────────────────────
    # ADX > 25: trend has directional conviction — slow age decay 50% (halve effective age)
    # ADX < 15: flat/choppy market — accelerate decay 25% (effective age × 1.25)
    # ADX 15–25: no modifier
    adx_note = ""
    effective_days = days
    try:
        adx = float(row.get('ADX') or 0)
        if adx > 25:
            effective_days = days * 0.5
            adx_note = f"; ADX {adx:.0f}>25 (strong trend, decay slowed)"
        elif adx < 15:
            effective_days = days * 1.25
            adx_note = f"; ADX {adx:.0f}<15 (flat market, decay accelerated)"
    except (TypeError, ValueError):
        pass  # ADX unavailable — use raw days

    if effective_days <= 5:
        pts, label = 20.0, "fresh, high probability"
    elif effective_days <= 15:
        pts, label = 17.0, "recent"
    elif effective_days <= 30:
        pts, label = 13.0, "moderate age"
    elif effective_days <= 60:
        pts, label = 8.0,  "aging, thesis partially captured"
    elif effective_days <= 120:
        pts, label = 4.0,  "stale cross, much of move already in price"
    else:
        pts, label = 1.0,  "very stale, mean reversion > trend continuation risk"

    return pts, f"EMA cross {days:.0f}d ago ({effective_days:.0f}d effective) — {label} ({pts:.0f}/20){adx_note}"


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def calculate_tqs_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate Timing Quality Score for directional strategy rows.

    Only scores rows where strategy is Long Call / Long Put / LEAP variant.
    All other rows get TQS_Score = NaN.

    Adds columns:
        TQS_Score     : 0-100 float
        TQS_Band      : 'Ideal' | 'Acceptable' | 'Stretched' | 'Chase'
        TQS_Breakdown : pipe-separated component notes
    """
    _DIRECTIONAL = {'long call', 'long put', 'long call leap', 'long put leap'}

    df['TQS_Score']     = np.nan
    df['TQS_Band']      = 'N/A'
    df['TQS_Breakdown'] = ''

    for idx, row in df.iterrows():
        strat = str(row.get('Strategy_Name', row.get('Strategy', ''))).lower().strip()
        if strat not in _DIRECTIONAL:
            continue

        ext_pts,  ext_note  = _score_extension(row)
        ctx_pts,  ctx_note  = _score_entry_context(row)
        mom_pts,  mom_note  = _score_momentum(row)
        age_pts,  age_note  = _score_cross_age(row)

        total = round(ext_pts + ctx_pts + mom_pts + age_pts, 1)
        total = min(100.0, total)

        if total >= 75:
            band = 'Ideal'
        elif total >= 50:
            band = 'Acceptable'
        elif total >= 25:
            band = 'Stretched'
        else:
            band = 'Chase'

        breakdown = ' | '.join([ext_note, ctx_note, mom_note, age_note])

        df.at[idx, 'TQS_Score']     = total
        df.at[idx, 'TQS_Band']      = band
        df.at[idx, 'TQS_Breakdown'] = breakdown

    return df
