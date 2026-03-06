#!/usr/bin/env python3
"""
Bootstrap IV History Database

PURPOSE:
    Initialize the iv_term_history database with sample historical data.
    Use this for testing or initial population before daily collection begins.

USAGE:
    # Bootstrap with sample data
    venv/bin/python scripts/admin/bootstrap_iv_history.py --mode sample

    # Bootstrap from existing snapshots
    venv/bin/python scripts/admin/bootstrap_iv_history.py --mode snapshots

    # Bootstrap from Fidelity (one-time)
    venv/bin/python scripts/admin/bootstrap_iv_history.py --mode fidelity
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import duckdb
import pandas as pd
from datetime import date, datetime, timedelta
import logging
import argparse

from core.shared.data_layer.iv_term_history import (
    get_iv_history_db_path,
    initialize_iv_term_history_table,
    append_daily_iv_data,
    get_history_summary
)
from core.shared.data_contracts.config import PROJECT_ROOT

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def bootstrap_sample_data(con: duckdb.DuckDBPyConnection, days: int = 130):
    """
    Generate sample IV data for testing.

    Args:
        con: DuckDB connection
        days: Number of historical days to generate
    """
    logger.info(f"Generating {days} days of sample IV data...")

    # Sample tickers
    tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'TSLA', 'META', 'SPY', 'QQQ']

    base_date = date.today() - timedelta(days=days)
    total_records = 0

    for ticker in tickers:
        logger.info(f"Generating data for {ticker}...")

        for day in range(days):
            current_date = base_date + timedelta(days=day)

            # Generate realistic IV data (oscillating with trend)
            base_iv = 25.0 + (hash(ticker) % 20)  # Ticker-specific base
            oscillation = 5.0 * (day % 20 / 10.0)  # Short-term cycles
            trend = 0.02 * day  # Slow upward trend

            iv_30d = base_iv + oscillation + trend

            # Generate term structure (contango)
            df_daily = pd.DataFrame([{
                'ticker': ticker,
                'iv_7d': iv_30d - 2,
                'iv_14d': iv_30d - 1,
                'iv_30d': iv_30d,
                'iv_60d': iv_30d + 1,
                'iv_90d': iv_30d + 2,
                'iv_120d': iv_30d + 3,
                'iv_180d': iv_30d + 4,
                'iv_360d': iv_30d + 5,
                'source': 'sample'
            }])

            append_daily_iv_data(con, df_daily, current_date)
            total_records += 1

    logger.info(f"✅ Generated {total_records} sample records")


def bootstrap_from_snapshots(con: duckdb.DuckDBPyConnection):
    """
    Bootstrap from existing IV/HV snapshot files.

    Reads historical snapshots from data/ivhv_timeseries/ directory.
    """
    logger.info("Bootstrapping from existing snapshots...")

    snapshot_dir = PROJECT_ROOT / "data" / "ivhv_timeseries"

    if not snapshot_dir.exists():
        logger.error(f"Snapshot directory not found: {snapshot_dir}")
        return

    # Find all snapshot files
    snapshot_files = sorted(snapshot_dir.glob("ivhv_snapshot_*.csv"))

    if not snapshot_files:
        logger.warning(f"No snapshot files found in {snapshot_dir}")
        return

    logger.info(f"Found {len(snapshot_files)} snapshot files")

    total_records = 0

    for snapshot_file in snapshot_files:
        try:
            # Extract date from filename
            # Expected format: ivhv_snapshot_2026-02-03.csv
            date_str = snapshot_file.stem.replace("ivhv_snapshot_", "")
            snapshot_date = datetime.strptime(date_str, "%Y-%m-%d").date()

            # Read snapshot
            df = pd.read_csv(snapshot_file)

            # Extract IV columns
            iv_cols_map = {
                'IV_7_D_Call': 'iv_7d',
                'IV_14_D_Call': 'iv_14d',
                'IV_30_D_Call': 'iv_30d',
                'IV_60_D_Call': 'iv_60d',
                'IV_90_D_Call': 'iv_90d',
                'IV_120_D_Call': 'iv_120d',
                'IV_180_D_Call': 'iv_180d',
                'IV_360_D_Call': 'iv_360d'
            }

            # Prepare data for insertion
            df_iv = pd.DataFrame()
            df_iv['ticker'] = df['Ticker'] if 'Ticker' in df.columns else df['Symbol']

            for old_col, new_col in iv_cols_map.items():
                if old_col in df.columns:
                    df_iv[new_col] = pd.to_numeric(df[old_col], errors='coerce')

            df_iv['source'] = 'snapshot'

            # Remove rows with no IV data
            df_iv = df_iv.dropna(subset=['iv_30d'])

            if not df_iv.empty:
                append_daily_iv_data(con, df_iv, snapshot_date)
                total_records += len(df_iv)
                logger.info(f"✅ {snapshot_file.name}: {len(df_iv)} records")
            else:
                logger.warning(f"⚠️ {snapshot_file.name}: No IV data found")

        except Exception as e:
            logger.error(f"Failed to process {snapshot_file.name}: {e}")
            continue

    logger.info(f"✅ Bootstrapped {total_records} records from snapshots")


def bootstrap_from_fidelity(con: duckdb.DuckDBPyConnection, tickers: list = None):
    """
    Bootstrap from Fidelity scraper (one-time historical fetch).

    Args:
        con: DuckDB connection
        tickers: List of tickers to fetch (None = default universe)
    """
    logger.warning("⚠️ Fidelity bootstrap not yet implemented")
    logger.info("This would fetch historical IV data from Fidelity screener")
    logger.info("Use --mode sample or --mode snapshots for now")


def main():
    parser = argparse.ArgumentParser(
        description='Bootstrap IV History Database',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        '--mode',
        choices=['sample', 'snapshots', 'fidelity'],
        required=True,
        help='Bootstrap mode: sample (synthetic data), snapshots (from files), fidelity (scraper)'
    )

    parser.add_argument(
        '--days',
        type=int,
        default=130,
        help='Number of days for sample data (default: 130)'
    )

    parser.add_argument(
        '--force',
        action='store_true',
        help='Force overwrite existing database'
    )

    args = parser.parse_args()

    logger.info("="*80)
    logger.info("🔵 IV HISTORY BOOTSTRAP")
    logger.info("="*80)

    # Check if database exists
    db_path = get_iv_history_db_path()

    if db_path.exists() and not args.force:
        logger.warning(f"⚠️ Database already exists: {db_path}")
        response = input("Overwrite? (yes/no): ")
        if response.lower() != 'yes':
            logger.info("Aborted")
            return

    # Initialize database
    con = duckdb.connect(str(db_path))

    try:
        logger.info("Initializing database schema...")
        initialize_iv_term_history_table(con)

        # Bootstrap based on mode
        if args.mode == 'sample':
            bootstrap_sample_data(con, args.days)
        elif args.mode == 'snapshots':
            bootstrap_from_snapshots(con)
        elif args.mode == 'fidelity':
            bootstrap_from_fidelity(con)

        # Show summary
        summary = get_history_summary(con)

        logger.info("="*80)
        logger.info("📊 BOOTSTRAP COMPLETE")
        logger.info("="*80)
        logger.info(f"Total Tickers: {summary['total_tickers']}")
        logger.info(f"Date Range: {summary['earliest_date']} to {summary['latest_date']}")
        logger.info(f"Avg History Depth: {summary['avg_depth']:.1f} days")
        logger.info(f"Median Depth: {summary['median_depth']} days")
        logger.info(f"Tickers with 120+ days: {summary['tickers_120plus']}")
        logger.info(f"Tickers with 252+ days: {summary['tickers_252plus']}")
        logger.info("="*80)

        logger.info(f"\n✅ Database ready: {db_path}")
        logger.info("\nNext steps:")
        logger.info("1. Run scan to verify integration:")
        logger.info("   venv/bin/python scripts/cli/scan_live.py")
        logger.info("2. Schedule daily collection:")
        # logger.info("   venv/bin/python scripts/daily_jobs/collect_iv_history.py") # REMOVED: Daily IV collection job eliminated

    finally:
        con.close()


if __name__ == "__main__":
    main()
