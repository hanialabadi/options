#!/usr/bin/env python3
"""
Backfill Macro Event Impact — Historical yfinance data for MC calibration.

Fetches SPY + VIX data around known FOMC/CPI/NFP/GDP dates from 2024-2025
and computes event-day impact (SPY move, VIX change). Writes to
macro_event_impact table in data/market.duckdb.

One-time run: gives MC 80+ empirical data points immediately.

Usage:
    python scripts/cli/backfill_macro_impact.py
    python scripts/cli/backfill_macro_impact.py --force   # overwrite existing
"""

import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(dotenv_path=project_root / '.env', override=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── Historical Macro Events (2024-2025) ──────────────────────────────────────
# Sources: federalreserve.gov, bls.gov, bea.gov
# Only HIGH-impact events (FOMC/CPI/NFP) — these matter most for MC.

HISTORICAL_EVENTS = [
    # ── FOMC 2024 ──
    ("FOMC", date(2024, 1, 31), "FOMC Rate Decision", "HIGH"),
    ("FOMC", date(2024, 3, 20), "FOMC Rate Decision", "HIGH"),
    ("FOMC", date(2024, 5, 1), "FOMC Rate Decision", "HIGH"),
    ("FOMC", date(2024, 6, 12), "FOMC Rate Decision", "HIGH"),
    ("FOMC", date(2024, 7, 31), "FOMC Rate Decision", "HIGH"),
    ("FOMC", date(2024, 9, 18), "FOMC Rate Decision", "HIGH"),
    ("FOMC", date(2024, 11, 7), "FOMC Rate Decision", "HIGH"),
    ("FOMC", date(2024, 12, 18), "FOMC Rate Decision", "HIGH"),

    # ── FOMC 2025 ──
    ("FOMC", date(2025, 1, 29), "FOMC Rate Decision", "HIGH"),
    ("FOMC", date(2025, 3, 19), "FOMC Rate Decision", "HIGH"),
    ("FOMC", date(2025, 5, 7), "FOMC Rate Decision", "HIGH"),
    ("FOMC", date(2025, 6, 18), "FOMC Rate Decision", "HIGH"),
    ("FOMC", date(2025, 7, 30), "FOMC Rate Decision", "HIGH"),
    ("FOMC", date(2025, 9, 17), "FOMC Rate Decision", "HIGH"),
    ("FOMC", date(2025, 10, 29), "FOMC Rate Decision", "HIGH"),
    ("FOMC", date(2025, 12, 17), "FOMC Rate Decision", "HIGH"),

    # ── CPI 2024 ──
    ("CPI", date(2024, 1, 11), "CPI Report", "HIGH"),
    ("CPI", date(2024, 2, 13), "CPI Report", "HIGH"),
    ("CPI", date(2024, 3, 12), "CPI Report", "HIGH"),
    ("CPI", date(2024, 4, 10), "CPI Report", "HIGH"),
    ("CPI", date(2024, 5, 15), "CPI Report", "HIGH"),
    ("CPI", date(2024, 6, 12), "CPI Report", "HIGH"),
    ("CPI", date(2024, 7, 11), "CPI Report", "HIGH"),
    ("CPI", date(2024, 8, 14), "CPI Report", "HIGH"),
    ("CPI", date(2024, 9, 11), "CPI Report", "HIGH"),
    ("CPI", date(2024, 10, 10), "CPI Report", "HIGH"),
    ("CPI", date(2024, 11, 13), "CPI Report", "HIGH"),
    ("CPI", date(2024, 12, 11), "CPI Report", "HIGH"),

    # ── CPI 2025 ──
    ("CPI", date(2025, 1, 15), "CPI Report", "HIGH"),
    ("CPI", date(2025, 2, 12), "CPI Report", "HIGH"),
    ("CPI", date(2025, 3, 12), "CPI Report", "HIGH"),
    ("CPI", date(2025, 4, 10), "CPI Report", "HIGH"),
    ("CPI", date(2025, 5, 13), "CPI Report", "HIGH"),
    ("CPI", date(2025, 6, 11), "CPI Report", "HIGH"),
    ("CPI", date(2025, 7, 15), "CPI Report", "HIGH"),
    ("CPI", date(2025, 8, 12), "CPI Report", "HIGH"),
    ("CPI", date(2025, 9, 10), "CPI Report", "HIGH"),
    ("CPI", date(2025, 10, 14), "CPI Report", "HIGH"),
    ("CPI", date(2025, 11, 12), "CPI Report", "HIGH"),
    ("CPI", date(2025, 12, 10), "CPI Report", "HIGH"),

    # ── NFP 2024 (1st Friday) ──
    ("NFP", date(2024, 1, 5), "Non-Farm Payrolls", "HIGH"),
    ("NFP", date(2024, 2, 2), "Non-Farm Payrolls", "HIGH"),
    ("NFP", date(2024, 3, 8), "Non-Farm Payrolls", "HIGH"),
    ("NFP", date(2024, 4, 5), "Non-Farm Payrolls", "HIGH"),
    ("NFP", date(2024, 5, 3), "Non-Farm Payrolls", "HIGH"),
    ("NFP", date(2024, 6, 7), "Non-Farm Payrolls", "HIGH"),
    ("NFP", date(2024, 7, 5), "Non-Farm Payrolls", "HIGH"),
    ("NFP", date(2024, 8, 2), "Non-Farm Payrolls", "HIGH"),
    ("NFP", date(2024, 9, 6), "Non-Farm Payrolls", "HIGH"),
    ("NFP", date(2024, 10, 4), "Non-Farm Payrolls", "HIGH"),
    ("NFP", date(2024, 11, 1), "Non-Farm Payrolls", "HIGH"),
    ("NFP", date(2024, 12, 6), "Non-Farm Payrolls", "HIGH"),

    # ── NFP 2025 ──
    ("NFP", date(2025, 1, 10), "Non-Farm Payrolls", "HIGH"),
    ("NFP", date(2025, 2, 7), "Non-Farm Payrolls", "HIGH"),
    ("NFP", date(2025, 3, 7), "Non-Farm Payrolls", "HIGH"),
    ("NFP", date(2025, 4, 4), "Non-Farm Payrolls", "HIGH"),
    ("NFP", date(2025, 5, 2), "Non-Farm Payrolls", "HIGH"),
    ("NFP", date(2025, 6, 6), "Non-Farm Payrolls", "HIGH"),
    ("NFP", date(2025, 7, 3), "Non-Farm Payrolls", "HIGH"),
    ("NFP", date(2025, 8, 1), "Non-Farm Payrolls", "HIGH"),
    ("NFP", date(2025, 9, 5), "Non-Farm Payrolls", "HIGH"),
    ("NFP", date(2025, 10, 3), "Non-Farm Payrolls", "HIGH"),
    ("NFP", date(2025, 11, 7), "Non-Farm Payrolls", "HIGH"),
    ("NFP", date(2025, 12, 5), "Non-Farm Payrolls", "HIGH"),
]


def _fetch_spy_vix_around_date(event_date: date) -> dict:
    """Fetch SPY + VIX close prices for event day and prior trading day."""
    import yfinance as yf
    import pandas as pd

    # Fetch 5 days before through event day to ensure we get prior trading day
    start = event_date - timedelta(days=7)
    end = event_date + timedelta(days=1)

    try:
        data = yf.download(
            "SPY ^VIX",
            start=start.isoformat(),
            end=end.isoformat(),
            progress=False,
            threads=True,
        )
        if data.empty:
            return {}

        # Extract closes
        spy_closes = data[("Close", "SPY")].dropna()
        vix_closes = data[("Close", "^VIX")].dropna()

        result = {}
        event_ts = pd.Timestamp(event_date)

        # SPY
        if event_ts in spy_closes.index:
            result["spy_close"] = float(spy_closes[event_ts])
            prior_dates = spy_closes.index[spy_closes.index < event_ts]
            if len(prior_dates) > 0:
                result["spy_prior_close"] = float(spy_closes[prior_dates[-1]])

        # VIX
        if event_ts in vix_closes.index:
            result["vix_close"] = float(vix_closes[event_ts])
            prior_dates = vix_closes.index[vix_closes.index < event_ts]
            if len(prior_dates) > 0:
                result["vix_prior"] = float(vix_closes[prior_dates[-1]])

        return result
    except Exception as e:
        logger.warning(f"yfinance fetch failed for {event_date}: {e}")
        return {}


def _safe_pct(new, old):
    if new is None or old is None or old == 0:
        return None
    return round((new - old) / abs(old), 6)


def _safe_diff(new, old):
    if new is None or old is None:
        return None
    return round(new - old, 6)


def main():
    parser = argparse.ArgumentParser(
        description="Backfill macro event impact from yfinance historical data."
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Overwrite existing impact records",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would be fetched without writing",
    )
    args = parser.parse_args()

    from core.shared.data_layer.macro_event_impact import (
        event_impact_exists, write_event_impact,
    )

    today = date.today()
    # Only process past events
    events = [(t, d, l, i) for t, d, l, i in HISTORICAL_EVENTS if d < today]

    logger.info(f"Processing {len(events)} historical macro events...")

    if args.dry_run:
        for evt_type, evt_date, label, impact in events:
            exists = event_impact_exists(evt_date, evt_type)
            status = "EXISTS" if exists else "WOULD_FETCH"
            logger.info(f"  {evt_date} {evt_type:5s} — {status}")
        return 0

    # Batch fetch: group dates and fetch in bulk to minimize API calls
    processed = 0
    skipped = 0
    failed = 0

    for evt_type, evt_date, label, impact in events:
        if not args.force and event_impact_exists(evt_date, evt_type):
            skipped += 1
            continue

        prices = _fetch_spy_vix_around_date(evt_date)
        if not prices:
            logger.warning(f"  {evt_date} {evt_type} — no data from yfinance")
            failed += 1
            continue

        spy_prior = prices.get("spy_prior_close")
        spy_close = prices.get("spy_close")
        vix_prior = prices.get("vix_prior")
        vix_close = prices.get("vix_close")

        record = {
            "event_date": evt_date,
            "event_type": evt_type,
            "event_label": label,
            "event_impact": impact,
            "vix_prior": vix_prior,
            "vix_close": vix_close,
            "vix_change": _safe_diff(vix_close, vix_prior),
            "vix_change_pct": _safe_pct(vix_close, vix_prior),
            "spy_prior_close": spy_prior,
            "spy_close": spy_close,
            "spy_change_pct": _safe_pct(spy_close, spy_prior),
            # Universe moves not available for historical (no price_history)
            "universe_avg_move_pct": None,
            "universe_median_move_pct": None,
            "universe_pct_advancing": None,
            "universe_pct_declining": None,
            # No regime data for historical
            "regime_prior": None,
            "regime_after": None,
            "regime_score_prior": None,
            "regime_score_after": None,
            "regime_changed": False,
        }

        write_event_impact(record)
        processed += 1

        spy_str = f"SPY {_safe_pct(spy_close, spy_prior):+.2%}" if spy_close and spy_prior else "SPY N/A"
        vix_str = f"VIX {_safe_diff(vix_close, vix_prior):+.1f}" if vix_close and vix_prior else "VIX N/A"
        logger.info(f"  {evt_date} {evt_type:5s} — {spy_str}, {vix_str}")

    logger.info(
        f"\nDone: {processed} written, {skipped} skipped (existing), {failed} failed"
    )

    # Show updated calibration
    from core.shared.data_layer.macro_event_impact import get_mc_macro_calibration
    logger.info("\n=== Updated MC Calibration ===")
    for evt_type in ("FOMC", "CPI", "NFP"):
        cal = get_mc_macro_calibration(evt_type)
        logger.info(
            f"  {evt_type}: source={cal['calibration_source']}, n={cal['n_events']}, "
            f"intensity={cal['jump_intensity_mult']:.2f}×, std={cal['jump_std_mult']:.2f}×, "
            f"avg_spy_abs={cal.get('avg_spy_abs_move_pct', 'N/A')}"
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
