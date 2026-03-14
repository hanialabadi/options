"""
collect_iv_daily.py — Standalone daily IV surface collection

PURPOSE
-------
Collect IV surface data once per trading day and persist to iv_term_history.
Runs independently from the pipeline so subsequent intraday pipeline runs
auto-skip IV collection (Layer 1B).

USAGE
-----
    # Normal daily collection (runs at 15:45 ET via launchd automation)
    python scripts/cli/collect_iv_daily.py

    # Force collection even if already collected today
    python scripts/cli/collect_iv_daily.py --force

    # Dry-run: check if collection needed without running
    python scripts/cli/collect_iv_daily.py --dry-run

    # Test with a single ticker
    python scripts/cli/collect_iv_daily.py --test-ticker AAPL

SCHEDULE
--------
Automated via launchd at 12:45 PT (15:45 ET) Mon–Fri:
    ~/Library/LaunchAgents/com.options.collect_iv_daily.plist

Closing IV is preferred over open: tightest spreads, most representative
term structure. Intraday pipeline runs use yesterday's IV (accurate enough
for rank — typical intraday drift is 1–3 rank points on normal days).

STATUS FLAGS
------------
On completion, writes one of:
    data/iv_collection_status.json  — machine-readable status for dashboard
"""

import sys
import os
import json
import argparse
import logging
import time
from pathlib import Path
from datetime import datetime, date

# Add project root to path
project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Load .env BEFORE any Schwab imports — override=True ensures .env values
# win over launchd EnvironmentVariables (which may have stale or quoted creds).
from dotenv import load_dotenv
load_dotenv(dotenv_path=project_root / '.env', override=True)

from scan_engine.loaders.schwab_api_client import SchwabClient
from scan_engine.iv_collector.rest_collector import IVRestCollector, iv_collected_today
from core.shared.data_contracts.config import TICKER_UNIVERSE_PATH, DATA_DIR
from scan_engine.step0_schwab_snapshot import load_ticker_universe, fetch_all_quotes

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger(__name__)

# Status flag file — dashboard reads this to show IV health badge
IV_STATUS_PATH = DATA_DIR / "iv_collection_status.json"

# ── NYSE Holiday Calendar ─────────────────────────────────────────────────────
# Shared module — single source of truth for all collection scripts.
from core.shared.calendar.trading_calendar import NYSE_HOLIDAYS as _NYSE_HOLIDAYS, is_trading_day


def _write_status(ok: bool, message: str, tickers_ok: int = 0, tickers_total: int = 0,
                  elapsed_s: float = 0.0) -> None:
    """Write machine-readable status file for dashboard consumption."""
    status = {
        "ok": ok,
        "message": message,
        "timestamp": datetime.now().isoformat(),
        "date": datetime.now().strftime("%Y-%m-%d"),
        "tickers_ok": tickers_ok,
        "tickers_total": tickers_total,
        "elapsed_s": round(elapsed_s, 1),
    }
    try:
        IV_STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
        IV_STATUS_PATH.write_text(json.dumps(status, indent=2))
    except Exception as e:
        logger.warning("Could not write IV status file: %s", e)


def read_iv_status() -> dict | None:
    """Read IV collection status. Returns None if file missing."""
    try:
        if IV_STATUS_PATH.exists():
            return json.loads(IV_STATUS_PATH.read_text())
    except Exception:
        pass
    return None


def run_iv_collection(force: bool = False, closing: bool = False, test_ticker: str = None) -> bool:
    """
    Run daily IV surface collection.

    Parameters
    ----------
    force : bool
        If True, collect even if already collected today AND bypass holiday guard.
    closing : bool
        If True, collect even if already collected today (e.g. launchd closing-price run
        at 15:45 ET). Does NOT bypass the holiday guard. Use this for the scheduled
        launchd job so it always captures closing IV regardless of intraday pipeline runs.
    test_ticker : str, optional
        If set, only collect this one ticker (for validation).

    Returns
    -------
    bool
        True if collection ran (or was already done), False on error.
    """
    logger.info("=" * 70)
    logger.info("📡 DAILY IV COLLECTION — %s", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    logger.info("=" * 70)

    # ── Holiday / non-trading-day guard ──────────────────────────────────────
    # launchd fires on all weekdays; skip cleanly on NYSE holidays.
    # --force bypasses this (e.g. half-day early-close manual runs).
    if not force and not is_trading_day():
        today = date.today()
        reason = "weekend" if today.weekday() >= 5 else "NYSE holiday"
        logger.info("[IV_SKIP] Today (%s) is a %s — market closed, skipping IV collection.", today, reason)
        # Do NOT write a failure status; dashboard should show prior session as normal.
        return True

    # Check if already collected today — skip for intraday pipeline runs.
    # The --closing flag bypasses this so the 15:45 ET launchd job always
    # captures closing-price IV even if the pipeline ran IV collection earlier.
    if not force and not closing and iv_collected_today():
        logger.info(
            "[IV_SKIP] IV already collected today — nothing to do. "
            "Use --force or --closing to re-collect."
        )
        # Do NOT overwrite a good status that already has real tickers_ok counts.
        existing = read_iv_status()
        if existing is None or existing.get("date") != datetime.now().strftime("%Y-%m-%d"):
            _write_status(ok=True, message="IV already collected today (skipped re-collection)")
        return True

    # Initialize Schwab client
    client_id = os.getenv("SCHWAB_APP_KEY")
    client_secret = os.getenv("SCHWAB_APP_SECRET")

    if not client_id or not client_secret:
        msg = "SCHWAB_APP_KEY and SCHWAB_APP_SECRET not set in environment"
        logger.error("❌ %s", msg)
        _write_status(ok=False, message=f"Auth error: {msg}")
        return False

    try:
        client = SchwabClient(client_id, client_secret)
        if not client._tokens:
            msg = "No existing tokens — authenticate first via the dashboard"
            logger.error("❌ %s", msg)
            _write_status(ok=False, message=f"Auth error: {msg}")
            return False
        client.ensure_valid_token()
        logger.info("✅ SchwabClient initialized")
    except Exception as e:
        msg = f"Client initialization failed: {e}"
        logger.error("❌ %s", msg)
        _write_status(ok=False, message=msg)
        return False

    # Load tickers
    if test_ticker:
        tickers = [test_ticker]
        logger.info("🧪 TEST MODE: Processing single ticker: %s", test_ticker)
    else:
        tickers = load_ticker_universe(TICKER_UNIVERSE_PATH)
        logger.info("📋 Loaded %d tickers from universe", len(tickers))

    # Fetch spot prices (needed by IVRestCollector) — batched across full universe
    logger.info("💹 Fetching quotes for %d tickers (batched)...", len(tickers))
    try:
        quotes, is_market_open, market_status = fetch_all_quotes(client, tickers)
        logger.info("   Market status: %s", market_status)
    except Exception as e:
        msg = f"Batch quotes failed: {e}"
        logger.error("❌ %s", msg)
        _write_status(ok=False, message=msg, tickers_total=len(tickers))
        return False

    spot_map = {
        t: quotes[t]['last_price']
        for t in tickers
        if quotes.get(t, {}).get('last_price') is not None
    }
    logger.info("   Spot prices: %d/%d tickers", len(spot_map), len(tickers))

    if not spot_map:
        msg = "No valid spot prices — market closed or all quotes failed"
        logger.error("❌ %s", msg)
        _write_status(ok=False, message=msg, tickers_total=len(tickers))
        return False

    # Run collector
    t_start = time.time()
    try:
        collector = IVRestCollector(client, write_to_db=True)
        result = collector.collect(
            tickers=[t for t in tickers if t in spot_map],
            spot_map=spot_map,
            force_run=True,  # market-hours gate handled by launchd schedule
        )
    except Exception as e:
        msg = f"IV collection failed: {e}"
        logger.error("❌ %s", msg)
        _write_status(ok=False, message=msg, tickers_total=len(tickers))
        return False

    elapsed = time.time() - t_start
    n_ok = result.success_count
    n_fail = len(result.failed)
    n_total = len(tickers)

    logger.info("=" * 70)
    logger.info(
        "✅ IV COLLECTION COMPLETE in %.1fs | success=%d  failed=%d  skipped=%d",
        elapsed, n_ok, n_fail, len(result.skipped),
    )
    if result.failed:
        logger.warning("   Failed tickers: %s", result.failed[:20])
    logger.info("=" * 70)
    logger.info(
        "💡 Subsequent pipeline runs today will auto-skip IV collection (~%.0fs saved each run).",
        elapsed,
    )

    # Write status — partial success if >5% failed
    partial = n_fail > 0 and (n_fail / max(n_ok + n_fail, 1)) > 0.05
    if partial:
        msg = f"Partial: {n_ok}/{n_ok+n_fail} tickers OK ({n_fail} failed) in {elapsed:.0f}s"
        _write_status(ok=True, message=msg, tickers_ok=n_ok, tickers_total=n_total, elapsed_s=elapsed)
    else:
        msg = f"{n_ok}/{n_ok+n_fail} tickers collected in {elapsed:.0f}s"
        _write_status(ok=True, message=msg, tickers_ok=n_ok, tickers_total=n_total, elapsed_s=elapsed)

    return True


def main():
    parser = argparse.ArgumentParser(
        description="Standalone daily IV surface collection for options pipeline"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-collection even if IV already collected today (also bypasses holiday guard)",
    )
    parser.add_argument(
        "--closing",
        action="store_true",
        help=(
            "Closing-price collection mode — re-collects even if already collected today "
            "(used by launchd at 15:45 ET to always capture closing IV)"
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Check if collection is needed without actually running",
    )
    parser.add_argument(
        "--test-ticker",
        metavar="SYMBOL",
        default=None,
        help="Collect only this ticker (for validation)",
    )
    args = parser.parse_args()

    if args.dry_run:
        today = date.today()
        trading = is_trading_day(today)
        already = iv_collected_today()
        status = read_iv_status()
        if not trading:
            reason = "weekend" if today.weekday() >= 5 else "NYSE holiday"
            logger.info("[DRY-RUN] Today (%s) is a %s — collection would be skipped.", today, reason)
        elif already:
            logger.info("[DRY-RUN] IV already collected today — pipeline will auto-skip.")
        else:
            logger.info("[DRY-RUN] IV NOT yet collected today — run without --dry-run to collect.")
        if status:
            logger.info("[DRY-RUN] Last status (%s): %s", status.get("date"), status.get("message"))
        sys.exit(0)

    success = run_iv_collection(force=args.force, closing=args.closing, test_ticker=args.test_ticker)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
