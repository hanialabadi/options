#!/usr/bin/env python3
"""
Fidelity IV Enrichment CLI

One command to enrich IV data via Fidelity scraper.
Automatically exports tickers needing IV and launches the scraper.

Usage:
    python cli/enrich_iv_fidelity.py

The script will:
1. Find tickers needing IV enrichment from latest pipeline output
2. Export them to a demand file
3. Launch Chrome with Fidelity
4. Wait for you to log in
5. Scrape all tickers automatically
6. Update IV history database
"""

import argparse
import sys
import subprocess
import pandas as pd
from pathlib import Path
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


def find_latest_step12_file() -> Path:
    """Find the most recent Step12 output file."""
    output_dir = PROJECT_ROOT / "output"
    patterns = ["Step12_Acceptance_*.csv", "Step12_Ready_*.csv"]

    all_files = []
    for pattern in patterns:
        all_files.extend(output_dir.glob(pattern))

    if not all_files:
        raise FileNotFoundError("No Step12 output files found in output/")

    all_files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
    return all_files[0]


def get_tickers_needing_iv(df: pd.DataFrame) -> list:
    """Identify tickers that need IV enrichment."""
    tickers = set()

    # Check IV_Maturity_State
    if 'IV_Maturity_State' in df.columns:
        mask = df['IV_Maturity_State'].isin(['MISSING', 'IMMATURE', 'PARTIAL_MATURE'])
        tickers.update(df.loc[mask, 'Ticker'].unique().tolist())

    # Check iv_history_count
    if 'iv_history_count' in df.columns:
        mask = (df['iv_history_count'] < 120) | df['iv_history_count'].isna()
        tickers.update(df.loc[mask, 'Ticker'].unique().tolist())

    return sorted(tickers)


def main():
    parser = argparse.ArgumentParser(
        description="Enrich IV data via Fidelity scraper (automatic)",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        "--input", "-i",
        type=str,
        help="Input CSV file (default: latest Step12 output)"
    )

    parser.add_argument(
        "--tickers", "-t",
        type=str,
        nargs="+",
        help="Specific tickers to scrape (overrides auto-detection)"
    )

    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip tickers already scraped today"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be scraped without launching"
    )

    args = parser.parse_args()

    print()
    print("=" * 60)
    print("  FIDELITY IV ENRICHMENT")
    print("=" * 60)
    print()

    # Get tickers to scrape
    if args.tickers:
        tickers = args.tickers
        print(f"[i] Using provided tickers: {len(tickers)}")
    else:
        # Find input file
        if args.input:
            input_path = Path(args.input)
            if not input_path.exists():
                print(f"[✗] Input file not found: {input_path}")
                sys.exit(1)
        else:
            try:
                input_path = find_latest_step12_file()
                print(f"[i] Using: {input_path.name}")
            except FileNotFoundError as e:
                print(f"[✗] {e}")
                sys.exit(1)

        df = pd.read_csv(input_path)
        print(f"[i] Loaded {len(df)} rows")

        tickers = get_tickers_needing_iv(df)

    if not tickers:
        print()
        print("[✓] No tickers need IV enrichment! All data is MATURE.")
        print()
        sys.exit(0)

    # Show tickers
    print()
    print(f"[i] {len(tickers)} tickers need IV enrichment:")
    print("-" * 40)

    # Display in columns
    cols = 5
    for i in range(0, len(tickers), cols):
        row = tickers[i:i+cols]
        print("    " + "  ".join(f"{t:8}" for t in row))

    print("-" * 40)
    print()

    # Export to demand file
    demand_file = PROJECT_ROOT / "output" / "fidelity_iv_demand.csv"
    demand_file.parent.mkdir(parents=True, exist_ok=True)

    df_demand = pd.DataFrame({'Ticker': tickers})
    df_demand.to_csv(demand_file, index=False)
    print(f"[✓] Exported to: {demand_file}")

    if args.dry_run:
        print()
        print("[DRY RUN] Would run:")
        print(f"  python -m core.shared.scraper.main --file {demand_file}")
        print()
        sys.exit(0)

    # Launch scraper
    print()
    print("=" * 60)
    print("  LAUNCHING FIDELITY SCRAPER")
    print("=" * 60)
    print()
    print("  Chrome will open. Please:")
    print("  1. Log in to Fidelity")
    print("  2. Press ENTER in this terminal when ready")
    print()
    print("=" * 60)
    print()

    # Build command
    cmd = [
        sys.executable, "-m", "core.shared.scraper.main",
        "--file", str(demand_file)
    ]

    if args.resume:
        cmd.append("--resume")

    # Run scraper (this will prompt for login)
    # Use subprocess with inherited stdin for interactive login prompt
    import os
    try:
        result = subprocess.run(
            cmd,
            cwd=PROJECT_ROOT,
            stdin=sys.stdin,
            stdout=sys.stdout,
            stderr=sys.stderr
        )

        if result.returncode == 0:
            print()
            print("=" * 60)
            print("  ENRICHMENT COMPLETE")
            print("=" * 60)
            print()
            print("  IV history has been updated in DuckDB.")
            print("  Run the pipeline again to use the new data.")
            print()
        else:
            print()
            print(f"[✗] Scraper exited with code {result.returncode}")

    except KeyboardInterrupt:
        print()
        print("[!] Cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"[✗] Failed to run scraper: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
