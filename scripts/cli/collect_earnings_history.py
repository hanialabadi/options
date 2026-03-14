"""
collect_earnings_history.py — Batch-collect historical earnings data

PURPOSE
-------
Fetch historical EPS beat/miss data for all stock tickers and persist to
DuckDB (earnings_history table). Optionally compute IV crush analytics.

USAGE
-----
    # Full universe collection
    python scripts/cli/collect_earnings_history.py

    # Single ticker test
    python scripts/cli/collect_earnings_history.py --ticker AAPL

    # Also compute IV crush + expected move analytics
    python scripts/cli/collect_earnings_history.py --compute-crush

    # Force re-collect even if data exists
    python scripts/cli/collect_earnings_history.py --force

SCHEDULE
--------
Weekly via launchd (weekend). Earnings data changes quarterly at most.

DATA SOURCE
-----------
yfinance get_earnings_dates(limit=12) — free, no API key, ~4 quarters.
"""

import sys
import os
import json
import argparse
import logging
import time
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from core.shared.data_contracts.config import (
    TICKER_UNIVERSE_PATH,
    PIPELINE_DB_PATH,
    IV_HISTORY_DB_PATH,
    KNOWN_ETFS,
    DATA_DIR,
)
from core.shared.data_layer.earnings_history import (
    initialize_tables,
    upsert_earnings_batch,
    compute_iv_crush_for_event,
    refresh_all_earnings_stats,
    classify_beat_miss,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("collect_earnings")


def load_stock_tickers() -> list:
    """Load universe and filter out ETFs."""
    import pandas as pd
    df = pd.read_csv(TICKER_UNIVERSE_PATH)
    col = "Ticker" if "Ticker" in df.columns else df.columns[0]
    all_tickers = df[col].dropna().str.strip().tolist()
    stocks = [t for t in all_tickers if t.upper() not in KNOWN_ETFS]
    logger.info(f"Loaded {len(stocks)} stock tickers ({len(all_tickers) - len(stocks)} ETFs excluded)")
    return stocks


def fetch_earnings_yfinance(ticker: str, limit: int = 12) -> list:
    """
    Fetch historical earnings from yfinance.

    Returns list of dicts: {earnings_date, eps_estimate, eps_actual,
                            eps_surprise_pct, fiscal_quarter, beat_miss}
    """
    import yfinance as yf
    import pandas as pd

    try:
        stock = yf.Ticker(ticker)
        df = stock.get_earnings_dates(limit=limit)

        if df is None or df.empty:
            return []

        results = []
        for idx, row in df.iterrows():
            # idx is the earnings datetime (timezone-aware)
            earnings_dt = idx
            if hasattr(earnings_dt, "date"):
                earnings_dt = earnings_dt.date()
            elif hasattr(earnings_dt, "to_pydatetime"):
                earnings_dt = earnings_dt.to_pydatetime().date()

            # Get EPS data — column names vary across yfinance versions
            eps_est = None
            eps_act = None
            surprise = None

            for col in ["EPS Estimate", "epsEstimate", "eps_estimate"]:
                if col in df.columns and pd.notna(row.get(col)):
                    eps_est = float(row[col])
                    break

            for col in ["Reported EPS", "epsActual", "reported_eps"]:
                if col in df.columns and pd.notna(row.get(col)):
                    eps_act = float(row[col])
                    break

            for col in ["Surprise(%)", "surprisePercent", "surprise_pct"]:
                if col in df.columns and pd.notna(row.get(col)):
                    surprise = float(row[col])
                    break

            # Skip future earnings (no actual EPS yet)
            if eps_act is None:
                continue

            # Compute surprise if not provided
            if surprise is None and eps_est is not None and abs(eps_est) > 0.001:
                surprise = ((eps_act - eps_est) / abs(eps_est)) * 100.0

            results.append({
                "ticker": ticker,
                "earnings_date": earnings_dt,
                "fiscal_quarter": None,  # yfinance doesn't provide this reliably
                "eps_estimate": eps_est,
                "eps_actual": eps_act,
                "eps_surprise_pct": surprise,
                "beat_miss": classify_beat_miss(surprise),
                "source": "yfinance",
            })

        return results

    except Exception as e:
        logger.warning(f"Failed to fetch earnings for {ticker}: {e}")
        return []


def main():
    parser = argparse.ArgumentParser(description="Collect historical earnings data")
    parser.add_argument("--ticker", type=str, help="Single ticker to collect")
    parser.add_argument("--compute-crush", action="store_true", help="Also compute IV crush analytics")
    parser.add_argument("--compute-formation", action="store_true", help="Also compute formation detection (Phase 1→2→3)")
    parser.add_argument("--force", action="store_true", help="Re-collect all even if data exists")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be collected")
    args = parser.parse_args()

    import duckdb
    import pandas as pd

    # Determine tickers
    if args.ticker:
        tickers = [args.ticker.upper()]
    else:
        tickers = load_stock_tickers()

    if args.dry_run:
        logger.info(f"DRY RUN: Would collect earnings for {len(tickers)} tickers")
        for t in tickers[:10]:
            logger.info(f"  {t}")
        if len(tickers) > 10:
            logger.info(f"  ... and {len(tickers) - 10} more")
        return

    # Open pipeline DB
    pipeline_con = duckdb.connect(str(PIPELINE_DB_PATH))
    initialize_tables(pipeline_con)

    # Collection stats
    total = len(tickers)
    success = 0
    failed = []
    total_rows = 0
    start_time = time.time()

    for i, ticker in enumerate(tickers, 1):
        if i % 50 == 0 or i == 1:
            logger.info(f"Progress: {i}/{total} ({i/total*100:.0f}%)")

        rows = fetch_earnings_yfinance(ticker)
        if rows:
            df = pd.DataFrame(rows)
            # Ensure earnings_date is proper date type
            df["earnings_date"] = pd.to_datetime(df["earnings_date"]).dt.date
            upserted = upsert_earnings_batch(pipeline_con, df)
            total_rows += upserted
            success += 1
        else:
            failed.append(ticker)

        # Rate limit
        if not args.ticker:  # Skip delay for single-ticker test
            time.sleep(0.5)

    elapsed = time.time() - start_time
    logger.info(
        f"Collection complete: {success}/{total} tickers, "
        f"{total_rows} rows, {elapsed:.0f}s"
    )

    if failed:
        logger.warning(f"Failed ({len(failed)}): {', '.join(failed[:20])}")

    # Compute IV crush analytics if requested
    if args.compute_crush:
        logger.info("Computing IV crush analytics...")
        iv_con = duckdb.connect(str(IV_HISTORY_DB_PATH), read_only=True)

        # Get all earnings events
        events = pipeline_con.execute("""
            SELECT ticker, earnings_date FROM earnings_history
            ORDER BY ticker, earnings_date
        """).fetchall()

        crush_count = 0
        for ticker, edate in events:
            try:
                result = compute_iv_crush_for_event(pipeline_con, iv_con, ticker, edate)
                if result:
                    crush_count += 1
            except Exception as e:
                logger.debug(f"Crush computation failed for {ticker} {edate}: {e}")

        iv_con.close()
        logger.info(f"IV crush computed for {crush_count}/{len(events)} events")

        # Refresh summary stats
        logger.info("Refreshing earnings_stats summary table...")
        stats_count = refresh_all_earnings_stats(pipeline_con)
        logger.info(f"Summary stats refreshed for {stats_count} tickers")

    # Compute formation detection if requested
    if args.compute_formation:
        from core.shared.data_layer.earnings_formation import compute_formation_for_event as _compute_form
        logger.info("Computing earnings formation detection...")

        iv_con_form = duckdb.connect(str(IV_HISTORY_DB_PATH), read_only=True)

        events = pipeline_con.execute("""
            SELECT ticker, earnings_date FROM earnings_history
            ORDER BY ticker, earnings_date
        """).fetchall()

        form_count = 0
        for ticker, edate in events:
            try:
                result = _compute_form(pipeline_con, iv_con_form, ticker, edate)
                if result:
                    form_count += 1
            except Exception as e:
                logger.debug(f"Formation computation failed for {ticker} {edate}: {e}")

        iv_con_form.close()
        logger.info(f"Formation computed for {form_count}/{len(events)} events")

    pipeline_con.close()

    # Write status file
    status = {
        "ok": success > 0,
        "message": f"Collected {total_rows} earnings rows for {success} tickers",
        "timestamp": datetime.now().isoformat(),
        "tickers_ok": success,
        "tickers_total": total,
        "tickers_failed": len(failed),
        "total_rows": total_rows,
        "elapsed_s": round(elapsed, 1),
        "compute_crush": args.compute_crush,
        "compute_formation": args.compute_formation,
    }
    status_path = DATA_DIR / "earnings_collection_status.json"
    with open(status_path, "w") as f:
        json.dump(status, f, indent=2)
    logger.info(f"Status written to {status_path}")


if __name__ == "__main__":
    main()
