#!/usr/bin/env python3
"""
Wait Loop Cleanup Utility

Cleans up stale/stagnating wait entries that can never be satisfied.

Usage:
    python scripts/cleanup_stale_wait_entries.py --inspect    # Show wait list state
    python scripts/cleanup_stale_wait_entries.py --cleanup    # Actually expire stale entries
    python scripts/cleanup_stale_wait_entries.py --inspect-conditions  # Show condition details

The primary issue this fixes: Old wait entries created before the liquidity
threshold fix have conditions with decimal thresholds (0.05) instead of
percentage points (5.0), making them impossible to satisfy.
"""

import argparse
import duckdb
import json
import logging
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database path
DB_PATH = Path(__file__).parent.parent / "data" / "pipeline.duckdb"


def get_connection():
    """Get DuckDB connection"""
    if not DB_PATH.exists():
        logger.error(f"Database not found at {DB_PATH}")
        return None
    return duckdb.connect(str(DB_PATH))


def inspect_wait_list(con):
    """Inspect current wait list state"""
    logger.info("=" * 60)
    logger.info("WAIT LIST INSPECTION")
    logger.info("=" * 60)

    # Check if table exists
    tables = con.execute("SHOW TABLES").fetchall()
    table_names = [t[0] for t in tables]

    if 'wait_list' not in table_names:
        logger.info("wait_list table does not exist")
        return

    # Get counts by status
    status_counts = con.execute("""
        SELECT status, COUNT(*) as count
        FROM wait_list
        GROUP BY status
        ORDER BY count DESC
    """).fetchall()

    logger.info("\nStatus Counts:")
    total = 0
    for status, count in status_counts:
        logger.info(f"  {status}: {count}")
        total += count
    logger.info(f"  TOTAL: {total}")

    # Get active entries with zero progress
    zero_progress = con.execute("""
        SELECT
            ticker,
            strategy_name,
            wait_progress,
            evaluation_count,
            wait_started_at,
            EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - wait_started_at)) / 3600 as hours_waiting
        FROM wait_list
        WHERE status = 'ACTIVE' AND wait_progress = 0
        ORDER BY evaluation_count DESC
        LIMIT 20
    """).fetchall()

    logger.info(f"\nActive entries with ZERO progress (top 20):")
    for ticker, strategy, progress, evals, started, hours in zero_progress:
        logger.info(
            f"  {ticker} {strategy}: {evals} evals, "
            f"{hours:.1f}h waiting, 0% progress"
        )

    # Count stagnating entries
    stagnating = con.execute("""
        SELECT COUNT(*)
        FROM wait_list
        WHERE status = 'ACTIVE'
          AND wait_progress = 0
          AND evaluation_count >= 2
    """).fetchone()[0]

    logger.info(f"\nSTAGNATING entries (0% progress, 2+ evals): {stagnating}")

    return stagnating


def inspect_conditions(con):
    """Inspect the actual conditions stored in wait entries"""
    logger.info("=" * 60)
    logger.info("CONDITION INSPECTION")
    logger.info("=" * 60)

    # Get sample conditions from stagnating entries
    entries = con.execute("""
        SELECT
            wait_id,
            ticker,
            strategy_name,
            wait_conditions,
            evaluation_count
        FROM wait_list
        WHERE status = 'ACTIVE'
          AND wait_progress = 0
          AND evaluation_count >= 2
        LIMIT 10
    """).fetchall()

    problematic_conditions = []

    for wait_id, ticker, strategy, conditions_json, evals in entries:
        conditions = json.loads(conditions_json) if conditions_json else []

        logger.info(f"\n{ticker} {strategy} (wait_id: {wait_id[:8]}...):")
        logger.info(f"  Evaluations: {evals}")

        for cond in conditions:
            cond_type = cond.get('type', 'unknown')
            config = cond.get('config', {})

            logger.info(f"  Condition: {cond_type}")
            logger.info(f"    Config: {json.dumps(config)}")

            # Check for problematic liquidity thresholds
            if cond_type == 'liquidity':
                threshold = config.get('threshold', 0)
                metric = config.get('metric', '')
                operator = config.get('operator', '')

                if metric == 'bid_ask_spread_pct' and operator == 'less_than':
                    # Old thresholds were decimals like 0.05
                    # New thresholds are percentage points like 5.0
                    if threshold < 1.0:
                        logger.warning(
                            f"    PROBLEMATIC: threshold={threshold} "
                            f"(likely old decimal format, should be {threshold * 100}%)"
                        )
                        problematic_conditions.append({
                            'wait_id': wait_id,
                            'ticker': ticker,
                            'threshold': threshold,
                            'expected': threshold * 100
                        })

    if problematic_conditions:
        logger.info(f"\nFound {len(problematic_conditions)} entries with problematic thresholds")
    else:
        logger.info("\nNo obviously problematic thresholds found")
        logger.info("Conditions may be failing for other reasons (market data missing, etc.)")

    return problematic_conditions


def cleanup_stagnating_entries(con, dry_run=True):
    """
    Clean up stagnating wait entries by marking them as EXPIRED.

    Args:
        con: DuckDB connection
        dry_run: If True, just show what would be cleaned up
    """
    logger.info("=" * 60)
    logger.info(f"CLEANUP {'(DRY RUN)' if dry_run else '(EXECUTING)'}")
    logger.info("=" * 60)

    # Find stagnating entries
    stagnating = con.execute("""
        SELECT
            wait_id,
            ticker,
            strategy_name,
            evaluation_count,
            wait_started_at
        FROM wait_list
        WHERE status = 'ACTIVE'
          AND wait_progress = 0
          AND evaluation_count >= 2
    """).fetchall()

    logger.info(f"Found {len(stagnating)} stagnating entries to clean up")

    if not stagnating:
        logger.info("Nothing to clean up!")
        return 0

    # Show sample
    logger.info("\nSample of entries to be expired:")
    for wait_id, ticker, strategy, evals, started in stagnating[:10]:
        logger.info(f"  {ticker} {strategy}: {evals} evals since {started}")

    if len(stagnating) > 10:
        logger.info(f"  ... and {len(stagnating) - 10} more")

    if dry_run:
        logger.info("\nDRY RUN - no changes made. Run with --cleanup to execute.")
        return len(stagnating)

    # Execute cleanup
    now = datetime.now().isoformat()
    expired_count = con.execute("""
        UPDATE wait_list
        SET
            status = 'EXPIRED',
            rejection_reason = 'CLEANUP: Stagnating with 0% progress after multiple evaluations (liquidity threshold format issue)',
            updated_at = ?
        WHERE status = 'ACTIVE'
          AND wait_progress = 0
          AND evaluation_count >= 2
    """, [now]).rowcount

    con.commit()

    logger.info(f"\nExpired {expired_count} stagnating entries")

    return expired_count


def main():
    parser = argparse.ArgumentParser(description="Wait Loop Cleanup Utility")
    parser.add_argument('--inspect', action='store_true', help='Inspect wait list state')
    parser.add_argument('--inspect-conditions', action='store_true', help='Inspect condition details')
    parser.add_argument('--cleanup', action='store_true', help='Actually clean up stale entries')
    parser.add_argument('--db', type=str, help='Override database path')

    args = parser.parse_args()

    global DB_PATH
    if args.db:
        DB_PATH = Path(args.db)

    con = get_connection()
    if con is None:
        return 1

    try:
        if args.inspect:
            inspect_wait_list(con)

        if args.inspect_conditions:
            inspect_conditions(con)

        if args.cleanup:
            cleanup_stagnating_entries(con, dry_run=False)
        elif not args.inspect and not args.inspect_conditions:
            # Default: dry run
            logger.info("No action specified. Running inspection + dry run cleanup.\n")
            inspect_wait_list(con)
            inspect_conditions(con)
            cleanup_stagnating_entries(con, dry_run=True)

    finally:
        con.close()

    return 0


if __name__ == "__main__":
    exit(main())
