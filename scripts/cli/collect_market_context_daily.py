#!/usr/bin/env python3
"""
Daily Market Context Collection — CLI entry point.

Fetches market-wide indicators (VIX, VVIX, term structure, credit spreads,
universe breadth, correlation) and writes to data/market.duckdb.

Schedule at 15:50 ET (5 min after IV collection) via launchd.

Usage:
    python scripts/cli/collect_market_context_daily.py
    python scripts/cli/collect_market_context_daily.py --force    # bypass guards
    python scripts/cli/collect_market_context_daily.py --dry-run  # check without writing
"""

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Load .env before any project imports
from dotenv import load_dotenv
load_dotenv(dotenv_path=project_root / '.env', override=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Collect daily market-wide context indicators."
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force collection (bypass holiday + duplicate guards)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Check readiness without writing to DuckDB",
    )
    args = parser.parse_args()

    from core.shared.calendar.trading_calendar import is_trading_day
    from core.shared.data_layer.market_context import market_context_collected_today

    today = date.today()

    if args.dry_run:
        trading = is_trading_day(today)
        already = market_context_collected_today(today)
        if not trading:
            reason = "weekend" if today.weekday() >= 5 else "NYSE holiday"
            logger.info(f"[DRY-RUN] Today ({today}) is a {reason} — collection would be skipped.")
        elif already:
            logger.info(f"[DRY-RUN] Market context already collected for {today}.")
        else:
            logger.info(f"[DRY-RUN] Ready to collect market context for {today}.")
        return 0

    from core.shared.data_layer.market_context_collector import collect_market_context

    result = collect_market_context(force=args.force)
    if result["ok"]:
        logger.info(result["message"])
        return 0
    else:
        logger.error(result["message"])
        return 1


if __name__ == "__main__":
    sys.exit(main())
