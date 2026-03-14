"""
migrate_domain_split.py — Split pipeline.duckdb into domain-specific databases.

Each engine gets its own DB file to eliminate single-writer lock contention:
  scan.duckdb        ← scan_results_latest, scan_results_history, scan_candidates,
                       dqs_multiplier_audit
  management.duckdb  ← management_recommendations, scale_up_requests,
                       executed_actions, premium_ledger
  chart.duckdb       ← chart_state_history, technical_indicators,
                       price_history, price_history_metadata
  wait.duckdb        ← wait_list, wait_list_history

Usage:
  python scripts/admin/migrate_domain_split.py [--dry-run] [--domain scan|management|chart|wait|all]

The script is idempotent — safe to re-run. Tables are copied (not moved) from
pipeline.duckdb so the monolith stays intact as a rollback safety net.
"""

import argparse
import logging
import sys
from pathlib import Path

# Ensure project root is on sys.path
_project_root = Path(__file__).resolve().parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import duckdb
from core.shared.data_contracts.config import (
    PIPELINE_DB_PATH, SCAN_DB_PATH, MANAGEMENT_DB_PATH,
    CHART_DB_PATH, WAIT_DB_PATH,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Domain → table mapping
# ---------------------------------------------------------------------------
DOMAIN_TABLES = {
    "scan": {
        "path": SCAN_DB_PATH,
        "tables": [
            "scan_results_latest",
            "scan_results_history",
            "scan_candidates",
            "dqs_multiplier_audit",
        ],
    },
    "management": {
        "path": MANAGEMENT_DB_PATH,
        "tables": [
            "management_recommendations",
            "scale_up_requests",
            "executed_actions",
            "premium_ledger",
        ],
    },
    "chart": {
        "path": CHART_DB_PATH,
        "tables": [
            "chart_state_history",
            "technical_indicators",
            "price_history",
            "price_history_metadata",
        ],
    },
    "wait": {
        "path": WAIT_DB_PATH,
        "tables": [
            "wait_list",
            "wait_list_history",
        ],
    },
}


def _table_exists(con, table_name: str) -> bool:
    return con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        f"WHERE table_name = '{table_name}' AND table_schema = 'main'"
    ).fetchone()[0] > 0


def _row_count(con, table_name: str) -> int:
    return con.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]


def migrate_domain(domain_name: str, dry_run: bool = False) -> dict:
    """
    Copy tables for a single domain from pipeline.duckdb into domain DB.

    Returns dict of {table: rows_copied} for reporting.
    """
    spec = DOMAIN_TABLES[domain_name]
    domain_path: Path = spec["path"]
    results = {}

    if not PIPELINE_DB_PATH.exists():
        logger.error(f"Source DB not found: {PIPELINE_DB_PATH}")
        return results

    # Open source (read-only)
    src = duckdb.connect(str(PIPELINE_DB_PATH), read_only=True)

    for table in spec["tables"]:
        if not _table_exists(src, table):
            logger.info(f"  [{domain_name}] {table} — not in pipeline.duckdb, skipping")
            results[table] = 0
            continue

        src_rows = _row_count(src, table)

        if dry_run:
            logger.info(f"  [{domain_name}] {table} — {src_rows:,} rows (DRY RUN)")
            results[table] = src_rows
            continue

        # Open/create domain DB (read-write)
        domain_path.parent.mkdir(parents=True, exist_ok=True)
        dst = duckdb.connect(str(domain_path), read_only=False)

        if _table_exists(dst, table):
            dst_rows = _row_count(dst, table)
            if dst_rows >= src_rows:
                logger.info(
                    f"  [{domain_name}] {table} — already has {dst_rows:,} rows "
                    f"(source: {src_rows:,}), skipping"
                )
                results[table] = dst_rows
                dst.close()
                continue
            else:
                # Source has more rows — drop and re-copy
                logger.info(
                    f"  [{domain_name}] {table} — refreshing "
                    f"({dst_rows:,} → {src_rows:,} rows)"
                )
                dst.execute(f'DROP TABLE "{table}"')

        # Copy via ATTACH
        dst.execute(f"ATTACH '{PIPELINE_DB_PATH}' AS src_pipeline (READ_ONLY)")
        dst.execute(
            f'CREATE TABLE "{table}" AS SELECT * FROM src_pipeline."{table}"'
        )
        dst.execute("DETACH src_pipeline")

        copied = _row_count(dst, table)
        logger.info(f"  [{domain_name}] {table} — copied {copied:,} rows")
        results[table] = copied
        dst.close()

    src.close()
    return results


def main():
    parser = argparse.ArgumentParser(description="Split pipeline.duckdb into domain DBs")
    parser.add_argument(
        "--domain", default="all",
        choices=["scan", "management", "chart", "wait", "all"],
        help="Which domain to migrate (default: all)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would be copied without writing",
    )
    args = parser.parse_args()

    domains = list(DOMAIN_TABLES.keys()) if args.domain == "all" else [args.domain]

    logger.info(f"=== Domain DB Migration {'(DRY RUN) ' if args.dry_run else ''}===")
    logger.info(f"Source: {PIPELINE_DB_PATH}")
    logger.info(f"Domains: {', '.join(domains)}")
    logger.info("")

    total_tables = 0
    total_rows = 0

    for domain in domains:
        logger.info(f"[{domain}] → {DOMAIN_TABLES[domain]['path']}")
        results = migrate_domain(domain, dry_run=args.dry_run)
        for table, rows in results.items():
            total_tables += 1
            total_rows += rows
        logger.info("")

    logger.info(f"=== Done: {total_tables} tables, {total_rows:,} total rows ===")


if __name__ == "__main__":
    main()
