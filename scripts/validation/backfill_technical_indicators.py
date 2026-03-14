"""
Backfill technical_indicators from historical Step5 CSV output files.
===================================================================
Recovers ADX_14, RSI_14, Chart_Regime, IV_Rank_30D, and Trend_Slope
from Step5_Charted_*.csv files in output/ directory.

Signal hub v2 columns (OBV_Slope, Volume_Ratio, Market_Structure) are
not present in CSVs — those will remain NULL for backfilled rows.
Behavioral memory already handles NULLs gracefully.

Usage:
    python scripts/validation/backfill_technical_indicators.py [--dry-run] [--before YYYYMMDD]
"""

from __future__ import annotations

import argparse
import glob
import logging
import os
import re
import sys
from datetime import datetime

import duckdb
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
OUTPUT_DIR = os.path.join(ROOT, 'output')
DB_PATH = os.path.join(ROOT, 'data', 'pipeline.duckdb')

# Map CSV column names to DuckDB schema columns
CSV_TO_DB = {
    'Ticker': 'Ticker',
    'timestamp': '_timestamp',       # used to derive Snapshot_TS
    'ADX': 'ADX_14',
    'RSI': 'RSI_14',
    'Chart_Regime': 'Chart_Regime',
    'IV_Rank_30D': 'IV_Rank_30D',
    'Trend_Slope': 'Trend_Slope',
    'SMA20': 'SMA_20',
    'SMA50': 'SMA_50',
}


def _parse_ts_from_filename(fname: str) -> datetime | None:
    """Extract timestamp from Step5_Charted_YYYYMMDD_HHMMSS.csv filename."""
    m = re.search(r'(\d{8})_(\d{6})\.csv$', fname)
    if not m:
        return None
    return datetime.strptime(m.group(1) + m.group(2), '%Y%m%d%H%M%S')


def _load_csv(path: str) -> pd.DataFrame | None:
    """Load a Step5 CSV and extract relevant columns."""
    try:
        df = pd.read_csv(path, low_memory=False)
    except Exception as e:
        logger.debug(f"Failed to read {path}: {e}")
        return None

    # Check for required columns
    if 'Ticker' not in df.columns or 'ADX' not in df.columns:
        return None

    # Rename columns to match DuckDB schema
    rename = {}
    for csv_col, db_col in CSV_TO_DB.items():
        if csv_col in df.columns:
            rename[csv_col] = db_col
    df = df.rename(columns=rename)

    # Derive Snapshot_TS from filename timestamp or CSV timestamp column
    fname_ts = _parse_ts_from_filename(path)
    if '_timestamp' in df.columns:
        df['Snapshot_TS'] = pd.to_datetime(df['_timestamp'], errors='coerce')
        df.loc[df['Snapshot_TS'].isna(), 'Snapshot_TS'] = fname_ts
        df = df.drop(columns=['_timestamp'])
    elif fname_ts:
        df['Snapshot_TS'] = fname_ts
    else:
        return None

    # Keep only schema columns, fill missing with NULL
    schema_cols = ['Ticker', 'Snapshot_TS', 'ADX_14', 'RSI_14', 'Chart_Regime',
                   'IV_Rank_30D', 'Trend_Slope', 'SMA_20', 'SMA_50']
    for col in schema_cols:
        if col not in df.columns:
            df[col] = None
    df = df[schema_cols].copy()

    # Drop rows without a ticker
    df = df.dropna(subset=['Ticker'])

    # Deduplicate: keep first occurrence per ticker per file
    df = df.drop_duplicates(subset=['Ticker'], keep='first')

    return df


def backfill(dry_run: bool = False, before: str | None = None) -> None:
    """Scan output/ for Step5 CSVs and insert missing rows into technical_indicators."""
    pattern = os.path.join(OUTPUT_DIR, 'Step5_Charted_*.csv')
    files = sorted(glob.glob(pattern))

    if before:
        cutoff = datetime.strptime(before, '%Y%m%d')
        files = [f for f in files if (_parse_ts_from_filename(f) or datetime.max) < cutoff]

    # Only backfill 2026 files
    files_2026 = [f for f in files if '/Step5_Charted_2026' in f]
    logger.info(f"Found {len(files_2026)} Step5 CSVs from 2026 to process")

    if not files_2026:
        logger.info("No files to backfill.")
        return

    # Deduplicate: keep only one file per day (latest scan of the day)
    day_files: dict[str, str] = {}
    for f in files_2026:
        m = re.search(r'(\d{8})_\d{6}\.csv$', f)
        if m:
            day = m.group(1)
            day_files[day] = f  # last one wins (files sorted by name)

    selected = sorted(day_files.values())
    logger.info(f"Selected {len(selected)} files (one per day) for backfill")

    all_rows = []
    for fpath in selected:
        df = _load_csv(fpath)
        if df is not None and not df.empty:
            all_rows.append(df)

    if not all_rows:
        logger.info("No valid rows extracted from CSVs.")
        return

    combined = pd.concat(all_rows, ignore_index=True)
    combined = combined.drop_duplicates(subset=['Ticker', 'Snapshot_TS'], keep='first')

    # Add required columns with defaults
    combined['Signal_Version'] = 1  # mark as backfilled v1
    combined['Computed_TS'] = datetime.now()

    logger.info(f"Prepared {len(combined)} rows for backfill "
                f"({combined['Ticker'].nunique()} tickers, "
                f"{combined['Snapshot_TS'].dt.date.nunique()} dates)")

    if dry_run:
        logger.info("[DRY RUN] Would insert the above rows. Exiting.")
        date_range = combined['Snapshot_TS'].dt.date
        logger.info(f"  Date range: {date_range.min()} to {date_range.max()}")
        return

    # Insert into DuckDB
    con = duckdb.connect(DB_PATH)
    try:
        # Ensure table exists
        from core.shared.data_layer.technical_data_repository import initialize_technical_indicators_table
        initialize_technical_indicators_table(con=con)

        # Use INSERT OR IGNORE to avoid overwriting existing data
        con.execute("CREATE TEMPORARY TABLE _backfill AS SELECT * FROM combined")
        inserted = con.execute(f"""
            INSERT INTO technical_indicators
                (Ticker, Snapshot_TS, ADX_14, RSI_14, Chart_Regime,
                 IV_Rank_30D, Trend_Slope, SMA_20, SMA_50,
                 Signal_Version, Computed_TS)
            SELECT Ticker, Snapshot_TS, ADX_14, RSI_14, Chart_Regime,
                   IV_Rank_30D, Trend_Slope, SMA_20, SMA_50,
                   Signal_Version, Computed_TS
            FROM _backfill b
            WHERE NOT EXISTS (
                SELECT 1 FROM technical_indicators t
                WHERE t.Ticker = b.Ticker AND t.Snapshot_TS = b.Snapshot_TS
            )
        """)
        count = inserted.fetchone()
        row_count = count[0] if count else 0

        # Verify
        verify = con.execute("""
            SELECT MIN(Snapshot_TS)::DATE as earliest,
                   MAX(Snapshot_TS)::DATE as latest,
                   COUNT(DISTINCT Ticker) as tickers,
                   COUNT(*) as total_rows
            FROM technical_indicators
        """).fetchone()
        logger.info(f"Inserted {row_count} new rows.")
        logger.info(f"technical_indicators now: {verify[3]} rows, "
                     f"{verify[2]} tickers, {verify[0]} to {verify[1]}")
    finally:
        con.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Backfill technical_indicators from Step5 CSVs')
    parser.add_argument('--dry-run', action='store_true', help='Preview without inserting')
    parser.add_argument('--before', type=str, help='Only process files before YYYYMMDD')
    args = parser.parse_args()

    # Ensure project root is on path
    sys.path.insert(0, ROOT)

    backfill(dry_run=args.dry_run, before=args.before)
