import sys
import os
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from scan_engine.step0_resolve_snapshot import resolve_snapshot_path
from scan_engine.step2_load_and_enrich_snapshot import load_ivhv_snapshot
from scan_engine.pipeline import run_full_pipeline

# Configure logging
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_cli_diagnostic(explicit_path: str | None = None):
    """Runs Step 0 and Step 2 only for diagnostic purposes."""
    print("\n--- CLI Pipeline Diagnostic Run (Step 0 -> Step 2 Only) ---")

    resolved_snapshot_path = None
    try:
        logger.info("🔍 Step 0: Resolving snapshot path...")
        resolved_snapshot_path = resolve_snapshot_path(
            explicit_path=explicit_path,
            snapshots_dir="data/snapshots"
        )

        file_path_obj = Path(resolved_snapshot_path)
        file_stat = file_path_obj.stat()
        mod_time = datetime.fromtimestamp(file_stat.st_mtime)

        print(f"\nResolved Snapshot Path: {resolved_snapshot_path}")
        print(f"Filename: {file_path_obj.name}")
        print(f"File modification time: {mod_time.strftime('%Y-%m-%d %H:%M:%S')}")

    except FileNotFoundError as e:
        logger.error(f"❌ Step 0 failed: {e}")
        return

    logger.info("📊 Step 2: Loading IV/HV snapshot...")
    df_snapshot = load_ivhv_snapshot(resolved_snapshot_path)
    logger.info("✅ Step 2 complete.")

    print(f"Row count: {len(df_snapshot)}")
    print(f"Columns: {df_snapshot.columns.tolist()}")


def run_full_scan(explicit_path: str | None = None, account_balance: float = 100000.0, max_portfolio_risk: float = 0.20):
    """Runs the complete scan pipeline."""
    print("\n" + "=" * 80)
    print("🚀 FULL PIPELINE EXECUTION")
    print("=" * 80 + "\n")

    try:
        logger.info("🔍 Step 0: Resolving snapshot path...")
        resolved_snapshot_path = resolve_snapshot_path(explicit_path=explicit_path, snapshots_dir="data/snapshots")
        logger.info(f"✅ Resolved: {Path(resolved_snapshot_path).name}")

    except FileNotFoundError as e:
        logger.error(f"❌ Step 0 failed: {e}")
        return

    try:
        results = run_full_pipeline(snapshot_path=resolved_snapshot_path, account_balance=account_balance, max_portfolio_risk=max_portfolio_risk)

        print("\n" + "=" * 80)
        print("✅ PIPELINE COMPLETE - SUMMARY")
        print("=" * 80)

        if 'acceptance_ready' in results:
            ready_count = len(results['acceptance_ready'])
            print(f"📊 READY Candidates: {ready_count}")

        if 'thesis_envelopes' in results:
            print(f"💰 Position Sizing Envelopes: {len(results['thesis_envelopes'])}")

        # Warn if Schwab token expired (market stress fetch failed)
        market_stress = results.get('market_stress', {})
        if market_stress.get('basis') == 'ERROR':
            print()
            print("⚠️  SCHWAB TOKEN EXPIRED — market stress data unavailable.")
            print("   Run: python auth_schwab_minimal.py")
            print("   Then re-run the scan to restore full market context.")

        print("=" * 80)

    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Options Scan Pipeline CLI")

    parser.add_argument('--path', type=str, help="Explicit path to the IV/HV snapshot CSV file")
    parser.add_argument('--full', action='store_true', help="Run the complete pipeline")
    parser.add_argument('--account-balance', type=float, default=100000.0, help="Account balance (default: 100000)")
    parser.add_argument('--max-portfolio-risk', type=float, default=0.20, help="Max portfolio risk (default: 0.20)")

    args = parser.parse_args()

    if args.full:
        run_full_scan(explicit_path=args.path, account_balance=args.account_balance, max_portfolio_risk=args.max_portfolio_risk)
    else:
        run_cli_diagnostic(explicit_path=args.path)
