"""
Trading Calendar Context
========================

Provides day-of-week and holiday awareness for entry and management decisions.

No external library dependencies — US market holidays are encoded directly from
the NYSE/CBOE calendar (observed holidays, not raw dates).

Key concepts:
  - Long premium (LONG_PUT, LONG_CALL, LEAPS): bleed theta on non-trading days.
    Friday close → Monday open = 3 days decay, 0 stock moves.
    Passarelli Ch.6: "Long premium positions bleed theta on non-trading days."
    Natenberg Ch.11: "Weekend = 3/252 variance days accruing to short side."

  - Short premium (CSP, CC, BUY_WRITE): collect theta on non-trading days.
    Pre-holiday Friday = prime entry — extra day of premium collected.
    Passarelli Ch.6: "Pre-holiday theta is a known edge for sellers."

  - Pin risk: at ≤3 DTE, gamma → ∞ near strike. Friday expiry within 2% of
    strike = binary binary assignment risk over weekend.
    McMillan Ch.7: "Close short premium before Thursday close if within 2% of strike."
    Natenberg Ch.15: "Pin risk at expiration is the institutional trader's nightmare."

Public API:
    get_calendar_context(dt)       → CalendarContext dataclass
    calendar_risk_flag(strategy_type, dt) → (flag_str, note_str)

Strategy type strings:
    'DIRECTIONAL'  — long call / long put / LEAPS
    'INCOME'       — CSP / covered call / buy-write
    'VOLATILITY'   — straddle / strangle
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

# ── US Market Holidays — imported from shared SSOT ────────────────────────────
from core.shared.calendar.trading_calendar import NYSE_HOLIDAYS as _US_MARKET_HOLIDAYS


def _to_date(dt: datetime | date | None) -> date:
    if dt is None:
        return date.today()
    if isinstance(dt, datetime):
        return dt.date()
    return dt


def is_trading_day(d: datetime | date) -> bool:
    """True if d is a NYSE trading day (Mon-Fri, not a market holiday)."""
    d = _to_date(d)
    return d.weekday() < 5 and d not in _US_MARKET_HOLIDAYS


def next_trading_day(d: datetime | date) -> date:
    """Return the next trading day after d (exclusive)."""
    d = _to_date(d)
    nxt = d + timedelta(days=1)
    while not is_trading_day(nxt):
        nxt += timedelta(days=1)
    return nxt


def days_to_next_trading_day(d: datetime | date) -> int:
    """Calendar days until next trading day (1 on a normal weekday = tomorrow)."""
    d = _to_date(d)
    return (next_trading_day(d) - d).days


def is_pre_long_weekend(d: datetime | date) -> bool:
    """
    True if today is the last trading day before a gap of ≥3 calendar days
    to the next trading day. Covers:
      - Regular 3-day weekends (Friday before Mon holiday)
      - Good Friday (Thursday before a 4-day break)
      - Thanksgiving Wednesday (if market closes early — but we track Thursday here)
    """
    return days_to_next_trading_day(d) >= 3


def trading_days_gap_after(d: datetime | date) -> int:
    """
    Number of calendar days from d (inclusive) to next trading day (exclusive).
    1 = normal day (next trading day is tomorrow)
    3 = regular weekend (Fri → Mon)
    4 = long weekend (Fri before Mon holiday, or Thu Good Friday)
    """
    return days_to_next_trading_day(d)


# ── CalendarContext dataclass ──────────────────────────────────────────────────

@dataclass
class CalendarContext:
    date: date
    is_friday: bool
    is_monday: bool
    is_pre_long_weekend: bool       # next trading day is ≥3 calendar days away
    weekend_gap_days: int           # calendar days until next open (1=normal, 3=weekend, 4=long)
    is_trading_day: bool
    next_open: date                 # next NYSE trading day

    @property
    def theta_bleed_days(self) -> int:
        """Non-trading days between today's close and next open (0 on a normal weekday)."""
        return self.weekend_gap_days - 1

    @property
    def is_expiration_week(self) -> bool:
        """True if this is a Friday (standard monthly/weekly expiration day)."""
        return self.is_friday


def get_calendar_context(dt: Optional[datetime | date] = None) -> CalendarContext:
    """
    Return a CalendarContext for the given datetime (defaults to today).
    """
    d = _to_date(dt) if dt is not None else date.today()
    gap = days_to_next_trading_day(d)
    nxt = next_trading_day(d)
    return CalendarContext(
        date=d,
        is_friday=(d.weekday() == 4),
        is_monday=(d.weekday() == 0),
        is_pre_long_weekend=(gap >= 3),
        weekend_gap_days=gap,
        is_trading_day=is_trading_day(d),
        next_open=nxt,
    )


# ── Calendar risk classification ───────────────────────────────────────────────

# Long-premium strategies that bleed theta on non-trading days
_LONG_PREMIUM = {
    'LONG_CALL', 'LONG_PUT', 'BUY_CALL', 'BUY_PUT',
    'LEAPS_CALL', 'LEAPS_PUT', 'STRADDLE', 'STRANGLE',
    'LONG_STRADDLE', 'LONG_STRANGLE',
}

# Short-premium strategies that COLLECT theta on non-trading days
_SHORT_PREMIUM = {
    'CSP', 'CASH_SECURED_PUT', 'COVERED_CALL', 'BUY_WRITE',
    'CREDIT_SPREAD', 'IRON_CONDOR', 'IRON_BUTTERFLY',
}


def calendar_risk_flag(
    strategy: str,
    dt: Optional[datetime | date] = None,
) -> tuple[str, str]:
    """
    Return (flag, note) describing calendar risk for a given strategy on date dt.

    Flags:
      ''                  — no calendar risk (neutral)
      'ADVANTAGEOUS'      — short premium on pre-weekend/holiday (extra theta collection)
      'ELEVATED_BLEED'    — long premium entering on Friday (3-day theta bleed)
      'HIGH_BLEED'        — long premium entering pre-long-weekend (4-day bleed)
      'PRE_HOLIDAY_EDGE'  — short premium entering pre-long-weekend (extra day premium)

    Args:
        strategy:  strategy name string (e.g. 'LONG_PUT', 'CSP', 'BUY_WRITE')
        dt:        datetime to evaluate (defaults to today)

    Returns:
        (flag_str, note_str) — empty strings when no calendar risk applies
    """
    ctx = get_calendar_context(dt)
    strat = str(strategy or '').upper().strip()

    # Normalize: "Long Call LEAP" → "LONG_CALL_LEAP", "Cash-Secured Put" → "CASH_SECURED_PUT"
    strat_norm = strat.replace(' ', '_').replace('-', '_')
    is_long  = any(s in strat_norm for s in _LONG_PREMIUM)
    is_short = any(s in strat_norm for s in _SHORT_PREMIUM)

    # Nothing to say for mid-week normal days
    if not ctx.is_friday and not ctx.is_pre_long_weekend:
        return '', ''

    bleed_days = ctx.theta_bleed_days  # 2 on regular weekend, 3 on long weekend

    if is_long:
        if ctx.is_pre_long_weekend and bleed_days >= 3:
            flag = 'HIGH_BLEED'
            note = (
                f"Pre-{'holiday ' if bleed_days >= 3 else ''}long weekend entry: "
                f"{bleed_days} non-trading days of theta bleed begin at close today "
                f"before the stock can move again ({ctx.next_open.strftime('%a %b %d')}). "
                f"Long premium pays {bleed_days}× daily theta with zero offsetting delta. "
                f"Passarelli Ch.6: avoid long premium entries pre-holiday unless conviction is extreme."
            )
        elif ctx.is_friday:
            flag = 'ELEVATED_BLEED'
            note = (
                f"Friday entry on long premium: 2 non-trading days (Sat+Sun) of theta "
                f"bleed before the stock opens Monday ({ctx.next_open.strftime('%b %d')}). "
                f"You pay Saturday and Sunday theta before the position can move in your favour. "
                f"Passarelli Ch.6: prefer Monday–Wednesday entries for long premium."
            )
        else:
            flag = ''
            note = ''

    elif is_short:
        if ctx.is_pre_long_weekend and bleed_days >= 3:
            flag = 'PRE_HOLIDAY_EDGE'
            note = (
                f"Pre-holiday short premium entry: {bleed_days} non-trading days of theta "
                f"collection before next open ({ctx.next_open.strftime('%a %b %d')}). "
                f"Sellers collect {bleed_days}× daily theta with stock pinned over holiday. "
                f"Passarelli Ch.6: this is a structural edge for income strategies."
            )
        elif ctx.is_friday:
            flag = 'ADVANTAGEOUS'
            note = (
                f"Friday income entry: collect Sat+Sun theta (2 extra days) before the stock "
                f"can move against you. "
                f"Natenberg Ch.11: 3-day weekend accrues to the short side."
            )
        else:
            flag = ''
            note = ''

    else:
        # Strategy type unknown or VOLATILITY — neutral
        if ctx.is_pre_long_weekend:
            flag = 'ELEVATED_BLEED'
            note = (
                f"Pre-long-weekend entry: {bleed_days} non-trading days of theta bleed "
                f"if long vol. Natenberg Ch.11: weekends accrue to short vega."
            )
        elif ctx.is_friday:
            flag = 'ELEVATED_BLEED'
            note = "Friday vol entry: 2 days of theta bleed before stock opens Monday."
        else:
            flag = ''
            note = ''

    return flag, note


# ── Expiration proximity risk ─────────────────────────────────────────────────

def expiry_proximity_flag(
    dte: float,
    strategy: str,
    ul_last: float = 0.0,
    strike: float = 0.0,
    dt: Optional[datetime | date] = None,
) -> tuple[str, str]:
    """
    Return (flag, note) for positions approaching expiration.

    Flags:
      ''                  — no escalation needed
      'PIN_RISK'          — within 2% of strike at ≤3 DTE (McMillan Ch.7)
      'GAMMA_CRITICAL'    — ≤3 DTE regardless of moneyness (force decision)
      'THETA_ACCELERATING'— ≤7 DTE (theta in final week is non-linear)
      'PRE_HOLIDAY_EXPIRY'— ≤7 DTE AND entering pre-long-weekend (double risk)

    Args:
        dte:       days to expiration
        strategy:  strategy name
        ul_last:   current underlying price (for pin risk calc)
        strike:    option strike (for pin risk calc)
        dt:        current date (defaults to today)
    """
    if dte is None:
        return '', ''
    try:
        dte = float(dte)
    except (TypeError, ValueError):
        return '', ''

    ctx = get_calendar_context(dt)
    strat = str(strategy or '').upper()

    # ≤3 DTE: gamma critical — force decision regardless of strategy
    if dte <= 3:
        # Check for pin risk (within 2% of strike)
        if ul_last > 0 and strike > 0:
            pct_from_strike = abs(ul_last - strike) / strike
            if pct_from_strike <= 0.02:
                return 'PIN_RISK', (
                    f"⚠️ PIN RISK: {dte:.0f} DTE, underlying within "
                    f"{pct_from_strike:.1%} of strike ${strike:.2f}. "
                    f"Close or roll immediately — do not carry into expiration Friday. "
                    f"McMillan Ch.7: close short premium before Thursday if within 2% of strike. "
                    f"Natenberg Ch.15: gamma → ∞ near strike at expiry; binary assignment risk."
                )
        return 'GAMMA_CRITICAL', (
            f"⚠️ GAMMA CRITICAL: {dte:.0f} DTE — theta is non-linear, gamma dominates. "
            f"Force decision now: roll or close. "
            f"Natenberg Ch.15: holding through expiration with no plan = binary outcome."
        )

    # ≤7 DTE AND pre-long-weekend = double escalation
    if dte <= 7 and ctx.is_pre_long_weekend:
        return 'PRE_HOLIDAY_EXPIRY', (
            f"⚠️ {dte:.0f} DTE into a {ctx.theta_bleed_days+1}-day weekend. "
            f"Theta acceleration + extended non-trading gap = elevated risk. "
            f"Roll urgency upgraded: resolve before close today."
        )

    # ≤7 DTE: theta acceleration zone
    if dte <= 7:
        return 'THETA_ACCELERATING', (
            f"{dte:.0f} DTE: entering final-week theta acceleration. "
            f"Hull Ch.18: theta decay is non-linear — the last week costs disproportionately. "
            f"Evaluate roll or close before this decays further."
        )

    return '', ''
