#!/usr/bin/env python3
"""
Fidelity Scraper Trigger CLI

This script:
1. Exports tickers that need IV enrichment to a CSV file
2. Provides the command to run the Fidelity scraper with manual login

Usage:
    # Export tickers from latest pipeline output and show scraper command
    python cli/trigger_fidelity_scraper.py

    # Export from specific file
    python cli/trigger_fidelity_scraper.py --input output/Step12_Acceptance_20260204.csv

    # Immediately launch scraper (will prompt for Fidelity login)
    python cli/trigger_fidelity_scraper.py --run
"""

import argparse
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


def find_latest_step12_file() -> Path:
    """Find the most recent Step12 output file."""
    output_dir = PROJECT_ROOT / "output"

    # Look for Step12_Acceptance or Step12_Ready files
    patterns = ["Step12_Acceptance_*.csv", "Step12_Ready_*.csv"]

    all_files = []
    for pattern in patterns:
        all_files.extend(output_dir.glob(pattern))

    if not all_files:
        raise FileNotFoundError("No Step12 output files found in output/")

    # Sort by modification time, newest first
    all_files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
    return all_files[0]


def get_tickers_needing_iv(df: pd.DataFrame) -> list:
    """
    Identify tickers that need IV enrichment.

    Criteria:
    - IV_Maturity_State is MISSING or IMMATURE
    - iv_history_count < 120
    """
    tickers = []

    # Check IV_Maturity_State
    if 'IV_Maturity_State' in df.columns:
        mask = df['IV_Maturity_State'].isin(['MISSING', 'IMMATURE', 'PARTIAL_MATURE'])
        tickers.extend(df.loc[mask, 'Ticker'].unique().tolist())

    # Check iv_history_count
    if 'iv_history_count' in df.columns:
        mask = df['iv_history_count'] < 120
        tickers.extend(df.loc[mask, 'Ticker'].unique().tolist())

    # Deduplicate
    return sorted(set(tickers))


def export_tickers_for_scraper(tickers: list, output_path: Path) -> None:
    """Export tickers to CSV for scraper consumption."""
    df = pd.DataFrame({'Ticker': tickers})
    df.to_csv(output_path, index=False)
    print(f"[✓] Exported {len(tickers)} tickers to: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Export tickers needing IV and trigger Fidelity scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Export tickers from latest pipeline output
    python cli/trigger_fidelity_scraper.py

    # Export and immediately run scraper
    python cli/trigger_fidelity_scraper.py --run

    # Export from specific file
    python cli/trigger_fidelity_scraper.py --input output/Step12_Acceptance_20260204.csv
        """
    )

    parser.add_argument(
        "--input", "-i",
        type=str,
        help="Input CSV file (default: latest Step12 output)"
    )

    parser.add_argument(
        "--output", "-o",
        type=str,
        default="output/fidelity_iv_demand.csv",
        help="Output CSV for scraper (default: output/fidelity_iv_demand.csv)"
    )

    parser.add_argument(
        "--run",
        action="store_true",
        help="Immediately launch the Fidelity scraper (will prompt for login)"
    )

    parser.add_argument(
        "--no-prompt",
        action="store_true",
        help="Skip login prompt (use persistent Chrome profile)"
    )

    args = parser.parse_args()

    # Find input file
    if args.input:
        input_path = Path(args.input)
        if not input_path.exists():
            print(f"[✗] Input file not found: {input_path}")
            sys.exit(1)
    else:
        try:
            input_path = find_latest_step12_file()
            print(f"[i] Using latest Step12 file: {input_path.name}")
        except FileNotFoundError as e:
            print(f"[✗] {e}")
            sys.exit(1)

    # Load data
    df = pd.read_csv(input_path)
    print(f"[i] Loaded {len(df)} rows from {input_path.name}")

    # Get tickers needing IV
    tickers = get_tickers_needing_iv(df)

    if not tickers:
        print("[✓] No tickers need IV enrichment!")
        sys.exit(0)

    print(f"[i] Found {len(tickers)} tickers needing IV enrichment:")
    for t in tickers[:10]:
        print(f"    - {t}")
    if len(tickers) > 10:
        print(f"    ... and {len(tickers) - 10} more")

    # Export
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    export_tickers_for_scraper(tickers, output_path)

    # Show or run scraper command
    scraper_cmd = f"python -m core.shared.scraper.main --file {output_path}"
    if args.no_prompt:
        scraper_cmd += " --no-prompt"

    print()
    print("=" * 60)
    print("FIDELITY SCRAPER COMMAND")
    print("=" * 60)
    print()
    print(f"  {scraper_cmd}")
    print()
    print("This will:")
    print("  1. Open Chrome with Fidelity login page")
    print("  2. Wait for you to log in manually")
    print("  3. Scrape IV/HV data for each ticker")
    print("  4. Save to data/ivhv_archive/")
    print("  5. Update IV history in DuckDB")
    print()
    print("=" * 60)

    if args.run:
        print()
        print("[!] Launching Fidelity scraper...")
        print()

        import subprocess
        cmd_parts = ["python", "-m", "core.shared.scraper.main", "--file", str(output_path)]
        if args.no_prompt:
            cmd_parts.append("--no-prompt")

        subprocess.run(cmd_parts, cwd=PROJECT_ROOT)
    else:
        print()
        print("Run with --run flag to launch the scraper automatically.")
        print()


if __name__ == "__main__":
    main()
