#!/usr/bin/env python3
"""
Debug Mode - Production Pipeline with Reduced Universe

PURPOSE:
    Execute the SAME pipeline as production with a restricted ticker universe.
    This is NOT a separate execution path - it's production with SCALE reduction.

ARCHITECTURAL PRINCIPLE:
    Debug = Production ÷ Scale

    Debug MUST:
        ✅ Use same pipeline orchestrator (scan_engine.pipeline.run_full_scan_pipeline)
        ✅ Execute all steps in same order
        ✅ Apply same execution gates
        ✅ Use same data sources (Schwab, DuckDB, Fidelity)
        ✅ Persist to same database
        ✅ Follow same maturity rules

    Debug MUST NOT:
        ❌ Skip steps
        ❌ Bypass gates
        ❌ Use different acceptance logic
        ❌ Change execution semantics
        ❌ Mock data sources

USAGE:
    # Basic debug mode (uses default debug tickers: AAPL, AMZN, NVDA)
    python cli/run_pipeline_debug.py

    # Custom debug tickers
    export DEBUG_TICKERS=TSLA,MSFT,COIN
    python cli/run_pipeline_debug.py

    # Single ticker debug
    python cli/run_pipeline_debug.py --ticker AAPL

    # With custom snapshot
    python cli/run_pipeline_debug.py --snapshot data/snapshots/ivhv_snapshot_live_20260207_120000.csv

OUTPUT:
    - Same CSV exports as production (in output/)
    - Same DuckDB persistence
    - Same audit traces (in audit_trace/)
    - Console summary with READY/BLOCKED/WAIT breakdown

VALIDATION:
    To verify debug parity with production:
    1. Run debug: python cli/run_pipeline_debug.py --ticker AAPL
    2. Run production with same snapshot
    3. Compare: Step12_Acceptance_*.csv should have identical Execution_Status for AAPL
"""

import sys
import os
import argparse
import logging
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scan_engine.pipeline import run_full_scan_pipeline
from core.shared.data_contracts.config import SCAN_OUTPUT_DIR

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """CLI entry point for debug mode"""
    parser = argparse.ArgumentParser(
        description="Debug Mode - Production Pipeline with Reduced Universe",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Default debug tickers (AAPL, AMZN, NVDA)
  python cli/run_pipeline_debug.py

  # Single ticker
  python cli/run_pipeline_debug.py --ticker MSFT

  # Custom ticker list
  export DEBUG_TICKERS=TSLA,COIN,PLTR
  python cli/run_pipeline_debug.py

  # With specific snapshot
  python cli/run_pipeline_debug.py --snapshot data/snapshots/ivhv_snapshot_live_20260207_120000.csv

Purpose:
  Runs the SAME pipeline as production, but with a restricted ticker universe.
  This ensures debug mode maintains exact parity with production behavior.

  Debug = Production ÷ Scale
        """
    )

    parser.add_argument('--ticker', '-t', type=str,
                       help='Single ticker to debug (e.g., AAPL)')
    parser.add_argument('--tickers', type=str,
                       help='Comma-separated list of tickers (e.g., AAPL,MSFT,NVDA)')
    parser.add_argument('--snapshot', '-s', type=str,
                       help='Path to specific snapshot CSV file')
    parser.add_argument('--account-balance', type=float, default=10000.0,
                       help='Account balance for position sizing (default: 10000)')
    parser.add_argument('--output-dir', type=str, default=None,
                       help='Output directory (default: output/)')

    args = parser.parse_args()

    # ============================================================
    # ACTIVATE DEBUG MODE
    # ============================================================
    os.environ["DEBUG_TICKER_MODE"] = "1"

    # Set debug ticker universe
    if args.ticker:
        # Single ticker mode
        debug_tickers = [args.ticker.upper()]
        os.environ["DEBUG_TICKERS"] = args.ticker.upper()
    elif args.tickers:
        # Custom ticker list
        debug_tickers = [t.strip().upper() for t in args.tickers.split(',')]
        os.environ["DEBUG_TICKERS"] = ','.join(debug_tickers)
    else:
        # Use environment variable or default
        env_tickers = os.getenv("DEBUG_TICKERS")
        if env_tickers:
            debug_tickers = [t.strip().upper() for t in env_tickers.split(',')]
        else:
            # Default debug universe
            debug_tickers = ["AAPL", "AMZN", "NVDA"]
            os.environ["DEBUG_TICKERS"] = ','.join(debug_tickers)

    print("\n" + "="*80)
    print("🧪 DEBUG MODE - PRODUCTION PIPELINE WITH REDUCED UNIVERSE")
    print("="*80)
    print(f"Debug Tickers: {', '.join(debug_tickers)}")
    print(f"Account Balance: ${args.account_balance:,.2f}")
    print(f"Snapshot: {args.snapshot if args.snapshot else 'Latest live snapshot'}")
    print(f"Output: {args.output_dir if args.output_dir else 'output/'}")
    print("="*80 + "\n")

    logger.info(f"🧪 DEBUG MODE ACTIVE")
    logger.info(f"   Universe: {debug_tickers}")
    logger.info(f"   This is production pipeline with SCALE reduction only")
    logger.info(f"   All execution gates, maturity checks, and persistence active")

    # ============================================================
    # RUN PRODUCTION PIPELINE
    # ============================================================
    try:
        result = run_full_scan_pipeline(
            snapshot_path=args.snapshot,
            output_dir=args.output_dir or str(SCAN_OUTPUT_DIR),
            account_balance=args.account_balance,
            max_portfolio_risk=0.10,
            sizing_method='volatility_scaled',
            expiry_intent='ANY'
        )

        # ============================================================
        # REPORT RESULTS
        # ============================================================
        print("\n" + "="*80)
        print("📊 DEBUG MODE RESULTS")
        print("="*80)

        # Extract results
        acceptance_all = result.get('acceptance_all', [])
        acceptance_ready = result.get('acceptance_ready', [])

        if hasattr(acceptance_all, '__len__'):
            total = len(acceptance_all)
        else:
            total = 0

        if hasattr(acceptance_ready, '__len__'):
            ready = len(acceptance_ready)
        else:
            ready = 0

        # Breakdown by status
        if total > 0 and hasattr(acceptance_all, 'get'):
            import pandas as pd
            if isinstance(acceptance_all, pd.DataFrame):
                status_counts = acceptance_all.get('Execution_Status', pd.Series()).value_counts().to_dict()
            else:
                status_counts = {}
        else:
            status_counts = {}

        print(f"\n🎯 Execution Summary:")
        print(f"   Total Strategies Evaluated: {total}")
        print(f"   READY (Executable Now): {ready}")

        if status_counts:
            print(f"\n📈 Status Breakdown:")
            for status, count in sorted(status_counts.items()):
                icon = "✅" if status == "READY" else "🟡" if status == "AWAIT_CONFIRMATION" else "⏸️" if status == "CONDITIONAL" else "❌"
                print(f"   {icon} {status}: {count}")

        # Wait list summary
        if 'wait_list_persist_counts' in result:
            wait_counts = result['wait_list_persist_counts']
            print(f"\n🔄 Wait List:")
            print(f"   Saved to wait list: {wait_counts.get('await_confirmation', 0)}")
            print(f"   Rejected: {wait_counts.get('rejected', 0)}")

        # Maturity summary
        if 'maturity_summary' in result:
            mat_summary = result['maturity_summary']
            print(f"\n📅 Maturity Gating:")
            print(f"   Executable Now: {mat_summary.get('executable_now', 0)}")
            print(f"   Gated by Maturity: {mat_summary.get('gated_by_maturity', 0)}")
            print(f"   Blocked: {mat_summary.get('blocked', 0)}")

        # Market stress
        if 'market_stress' in result:
            stress = result['market_stress']
            print(f"\n🌡️  Market Stress: {stress.get('level', 'UNKNOWN')}")
            print(f"   IV History: {stress.get('iv_history_days', 'N/A')}")
            print(f"   Clock State: {stress.get('iv_clock_state', 'UNKNOWN')}")

        # Output files
        print(f"\n💾 Output Files:")
        print(f"   Directory: {args.output_dir or SCAN_OUTPUT_DIR}")
        print(f"   Latest Acceptance: Step12_Acceptance_*.csv")
        print(f"   Latest Ready: Step12_Ready_*.csv")
        print(f"   Audit Traces: audit_trace/*.csv")

        print("\n" + "="*80)
        print("✅ DEBUG MODE COMPLETE")
        print("="*80 + "\n")

        # Exit code based on execution
        if ready > 0:
            logger.info(f"✅ Debug completed successfully with {ready} READY strategies")
            sys.exit(0)
        else:
            logger.info(f"ℹ️  Debug completed with 0 READY strategies (check gating reasons)")
            sys.exit(0)  # Not an error - just diagnostic result

    except Exception as e:
        logger.error(f"❌ Debug mode failed: {e}", exc_info=True)
        print(f"\n❌ ERROR: {e}\n")
        sys.exit(1)


if __name__ == '__main__':
    main()
