"""
Macro Impact Collector — Compute and store market reactions to macro events.

Runs as part of the daily market context collection flow. After market context
is collected, checks if today is a macro event day. If so, computes the impact
by comparing today's market data vs the prior trading day.

Data sources (all already collected daily):
  - market_context_daily: VIX, regime, term structure, breadth
  - price_history: SPY + universe ticker prices

Never blocks on missing data — partial impact records are still valuable.
"""

import logging
import math
from datetime import date, timedelta
from typing import Optional

from config.macro_calendar import MACRO_EVENTS_2026, MacroEvent
from core.shared.data_layer.duckdb_utils import (
    DbDomain, get_domain_connection,
)
from core.shared.data_layer.macro_event_impact import (
    event_impact_exists,
    write_event_impact,
)

logger = logging.getLogger(__name__)


def _get_events_for_date(d: date) -> list[MacroEvent]:
    """Return all macro events scheduled for date *d*."""
    return [e for e in MACRO_EVENTS_2026 if e.event_date == d]


def _safe_pct(new: Optional[float], old: Optional[float]) -> Optional[float]:
    """Compute percent change, returning None if either value is missing or zero."""
    if new is None or old is None or old == 0:
        return None
    return round((new - old) / abs(old), 6)


def _safe_diff(new: Optional[float], old: Optional[float]) -> Optional[float]:
    """Compute absolute difference, returning None if either is missing."""
    if new is None or old is None:
        return None
    return round(new - old, 6)


def _safe_float(val) -> Optional[float]:
    """Convert to float, returning None for NaN/None."""
    if val is None:
        return None
    try:
        f = float(val)
        return None if math.isnan(f) else f
    except (TypeError, ValueError):
        return None


def _query_market_context_pair(event_date: date) -> tuple[Optional[dict], Optional[dict]]:
    """Query market_context_daily for event day and prior trading day.

    Returns (prior_day, event_day) dicts, either can be None.
    """
    try:
        con = get_domain_connection(DbDomain.MARKET, read_only=True)
        try:
            df = con.execute("""
                SELECT * FROM market_context_daily
                WHERE date <= ?
                ORDER BY date DESC
                LIMIT 2
            """, [event_date]).fetchdf()

            if df.empty:
                return None, None

            rows = [r.to_dict() for _, r in df.iterrows()]

            if len(rows) == 1:
                # Only one row — it's the event day (no prior day data)
                import pandas as pd
                row_date = rows[0].get("date")
                if isinstance(row_date, pd.Timestamp):
                    row_date = row_date.date()
                if row_date == event_date:
                    return None, rows[0]
                else:
                    return rows[0], None

            # Two rows: first is most recent (event day), second is prior
            import pandas as pd
            d0 = rows[0].get("date")
            d1 = rows[1].get("date")
            if isinstance(d0, pd.Timestamp):
                d0 = d0.date()
            if isinstance(d1, pd.Timestamp):
                d1 = d1.date()

            if d0 == event_date:
                return rows[1], rows[0]  # (prior, event)
            else:
                return rows[0], None  # prior exists but event day not collected yet

        finally:
            con.close()
    except Exception as e:
        logger.debug(f"[MacroImpact] market_context query failed: {e}")
        return None, None


def _query_spy_prices(event_date: date) -> tuple[Optional[float], Optional[float]]:
    """Query SPY close on event day and prior trading day.

    Returns (prior_close, event_close).
    """
    try:
        con = get_domain_connection(DbDomain.CHART, read_only=True)
        try:
            df = con.execute("""
                SELECT date, close_price
                FROM price_history
                WHERE ticker = 'SPY' AND date <= ?
                ORDER BY date DESC
                LIMIT 2
            """, [event_date]).fetchdf()

            if len(df) < 2:
                if len(df) == 1:
                    return None, float(df.iloc[0]["close_price"])
                return None, None

            return float(df.iloc[1]["close_price"]), float(df.iloc[0]["close_price"])
        finally:
            con.close()
    except Exception as e:
        logger.debug(f"[MacroImpact] SPY price query failed: {e}")
        return None, None


def _query_universe_moves(event_date: date) -> dict:
    """Compute universe-wide return stats on event day vs prior day.

    Returns dict with avg_move_pct, median_move_pct, pct_advancing, pct_declining.
    """
    result = {
        "universe_avg_move_pct": None,
        "universe_median_move_pct": None,
        "universe_pct_advancing": None,
        "universe_pct_declining": None,
    }
    try:
        con = get_domain_connection(DbDomain.CHART, read_only=True)
        try:
            df = con.execute("""
                WITH today AS (
                    SELECT ticker, close_price AS close_today
                    FROM price_history
                    WHERE date = ?
                ),
                prior AS (
                    SELECT ticker, close_price AS close_prior
                    FROM price_history p
                    WHERE date = (
                        SELECT MAX(date) FROM price_history
                        WHERE date < ? AND ticker = p.ticker
                    )
                ),
                returns AS (
                    SELECT t.ticker,
                           (t.close_today - p.close_prior) / p.close_prior AS ret
                    FROM today t
                    JOIN prior p ON t.ticker = p.ticker
                    WHERE p.close_prior > 0
                )
                SELECT
                    AVG(ABS(ret)) AS avg_abs_ret,
                    MEDIAN(ABS(ret)) AS median_abs_ret,
                    AVG(CASE WHEN ret > 0 THEN 1.0 ELSE 0.0 END) AS pct_adv,
                    AVG(CASE WHEN ret < 0 THEN 1.0 ELSE 0.0 END) AS pct_dec,
                    COUNT(*) AS n
                FROM returns
            """, [event_date, event_date]).fetchdf()

            if not df.empty and df.iloc[0]["n"] > 10:
                result["universe_avg_move_pct"] = _safe_float(df.iloc[0]["avg_abs_ret"])
                result["universe_median_move_pct"] = _safe_float(df.iloc[0]["median_abs_ret"])
                result["universe_pct_advancing"] = _safe_float(df.iloc[0]["pct_adv"])
                result["universe_pct_declining"] = _safe_float(df.iloc[0]["pct_dec"])

        finally:
            con.close()
    except Exception as e:
        logger.debug(f"[MacroImpact] Universe moves query failed: {e}")

    return result


def compute_event_impact(event: MacroEvent) -> Optional[dict]:
    """Compute the market impact of a single macro event.

    Returns a dict ready for write_event_impact(), or None if insufficient data.
    """
    d = event.event_date

    # Get market context for event day and prior day
    prior_ctx, event_ctx = _query_market_context_pair(d)

    # Get SPY prices
    spy_prior, spy_close = _query_spy_prices(d)

    # Get universe moves
    universe = _query_universe_moves(d)

    # We need at least one of event_ctx or spy_close to record anything useful
    if event_ctx is None and spy_close is None:
        logger.info(f"[MacroImpact] No data available for {event.event_type} on {d}")
        return None

    # Build impact record
    vix_prior = _safe_float(prior_ctx.get("vix")) if prior_ctx else None
    vix_close = _safe_float(event_ctx.get("vix")) if event_ctx else None

    impact = {
        "event_date": d,
        "event_type": event.event_type,
        "event_label": event.label,
        "event_impact": event.impact,

        # VIX
        "vix_prior": vix_prior,
        "vix_close": vix_close,
        "vix_change": _safe_diff(vix_close, vix_prior),
        "vix_change_pct": _safe_pct(vix_close, vix_prior),

        # SPY
        "spy_prior_close": spy_prior,
        "spy_close": spy_close,
        "spy_change_pct": _safe_pct(spy_close, spy_prior),

        # Universe
        **universe,

        # Term structure
        "vix_term_spread_prior": _safe_float(prior_ctx.get("vix_term_spread")) if prior_ctx else None,
        "vix_term_spread_after": _safe_float(event_ctx.get("vix_term_spread")) if event_ctx else None,
        "vix_term_ratio_prior": _safe_float(prior_ctx.get("vix_term_ratio")) if prior_ctx else None,
        "vix_term_ratio_after": _safe_float(event_ctx.get("vix_term_ratio")) if event_ctx else None,

        # Regime
        "regime_prior": (prior_ctx.get("market_regime") if prior_ctx else None),
        "regime_after": (event_ctx.get("market_regime") if event_ctx else None),
        "regime_score_prior": _safe_float(prior_ctx.get("regime_score")) if prior_ctx else None,
        "regime_score_after": _safe_float(event_ctx.get("regime_score")) if event_ctx else None,
        "regime_changed": (
            prior_ctx is not None
            and event_ctx is not None
            and prior_ctx.get("market_regime") != event_ctx.get("market_regime")
        ),

        # Breadth
        "breadth_sma50_prior": _safe_float(prior_ctx.get("universe_breadth_pct_sma50")) if prior_ctx else None,
        "breadth_sma50_after": _safe_float(event_ctx.get("universe_breadth_pct_sma50")) if event_ctx else None,
    }

    return impact


def collect_macro_impact(d: date | None = None, force: bool = False) -> dict:
    """Check if date *d* is a macro event day and record impact if so.

    Called after daily market context collection.

    Args:
        d: Date to check (default: today).
        force: If True, overwrite existing impact records.

    Returns:
        {"ok": bool, "events_processed": int, "message": str}
    """
    d = d or date.today()
    events = _get_events_for_date(d)

    if not events:
        return {"ok": True, "events_processed": 0,
                "message": f"No macro events on {d}"}

    processed = 0
    for event in events:
        if not force and event_impact_exists(event.event_date, event.event_type):
            logger.info(
                f"[MacroImpact] {event.event_type} impact already recorded for {d}"
            )
            continue

        impact = compute_event_impact(event)
        if impact is not None:
            write_event_impact(impact)
            processed += 1
            logger.info(
                f"[MacroImpact] Recorded {event.event_type} impact: "
                f"SPY {impact.get('spy_change_pct', 'N/A')}, "
                f"VIX {impact.get('vix_change', 'N/A')}"
            )

    return {
        "ok": True,
        "events_processed": processed,
        "message": f"Processed {processed}/{len(events)} macro events on {d}",
    }


def backfill_macro_impacts(force: bool = False) -> dict:
    """Backfill impact records for all past macro events that have data.

    Scans macro calendar for events before today and computes impact
    for any that don't already have records.

    Returns:
        {"ok": bool, "total_processed": int, "total_events": int}
    """
    today = date.today()
    past_events = [e for e in MACRO_EVENTS_2026 if e.event_date < today]

    total_processed = 0
    for event in past_events:
        if not force and event_impact_exists(event.event_date, event.event_type):
            continue

        impact = compute_event_impact(event)
        if impact is not None:
            write_event_impact(impact)
            total_processed += 1

    msg = f"Backfilled {total_processed}/{len(past_events)} macro events"
    logger.info(f"[MacroImpact] {msg}")
    return {
        "ok": True,
        "total_processed": total_processed,
        "total_events": len(past_events),
        "message": msg,
    }
