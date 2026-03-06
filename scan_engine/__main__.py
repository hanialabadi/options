import sys
import argparse
import os
import logging
from pathlib import Path

# Add project root to sys.path to allow imports from core.shared
from core.shared.data_contracts.config import PROJECT_ROOT
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s - %(message)s')

def main():
    parser = argparse.ArgumentParser(description="Scan Engine Pipeline")
    parser.add_argument("--snapshot", type=str, help="Path to IV/HV snapshot CSV")
    parser.add_argument("--balance", type=float, default=100000.0, help="Account balance")
    parser.add_argument("--risk", type=float, default=0.20, help="Max portfolio risk")
    parser.add_argument("--sizing", type=str, default="volatility_scaled", help="Sizing method")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    parser.add_argument("--no-intraday", action="store_true", help="Skip Cycle 3 intraday execution check")

    args = parser.parse_args()

    if args.debug:
        os.environ["PIPELINE_DEBUG"] = "1"
        os.environ["DEBUG_TICKER_MODE"] = "1"
        os.environ["DEBUG_TICKERS"] = os.environ.get("DEBUG_TICKERS", "AMD,PLTR,ORCL")
    else:
        for key in ("PIPELINE_DEBUG", "DEBUG_TICKER_MODE", "DEBUG_TICKERS"):
            os.environ.pop(key, None)

    from scan_engine.pipeline import run_full_scan_pipeline
    from scan_engine.step0_resolve_snapshot import resolve_snapshot_path
    from core.shared.data_contracts.config import PIPELINE_DB_PATH, DEBUG_PIPELINE_DB_PATH

    # Guard: fail fast if another scan process already holds the DuckDB write lock.
    # Use read_only=True to probe — a write-lock conflict on read-only open means
    # another writer is active. This avoids acquiring the write lock ourselves here,
    # which would race with the pipeline's first write operation milliseconds later.
    import duckdb
    db_path = DEBUG_PIPELINE_DB_PATH if args.debug else PIPELINE_DB_PATH
    if db_path.exists():
        try:
            _test_con = duckdb.connect(str(db_path), read_only=True)
            _test_con.close()
        except Exception as e:
            if "Conflicting lock" in str(e):
                print(f"❌ Cannot start scan: {db_path.name} is locked by another process.\n   {e}")
                print("   Wait for the current scan to finish, then retry.")
                return 1

    snapshot_path = resolve_snapshot_path(explicit_path=args.snapshot)
    print(f"🚀 Starting Scan Engine with snapshot: {snapshot_path}")

    pipeline_results = run_full_scan_pipeline(
        snapshot_path=snapshot_path,
        output_dir=None,
        account_balance=args.balance,
        max_portfolio_risk=args.risk,
        sizing_method=args.sizing,
        expiry_intent='ANY',
        skip_intraday=args.no_intraday,
    )

    ready_count = len(pipeline_results.get('acceptance_ready', []))
    print(f"✅ Scan complete. Found {ready_count} READY candidates.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
