import duckdb
import os
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def migrate_timestamps():
    from core.shared.data_contracts.config import PIPELINE_DB_PATH
    db_path = str(PIPELINE_DB_PATH)

    if not os.path.exists(db_path):
        logger.info(f"Database not found at {db_path}. No migration needed.")
        return

    logger.info(f"Starting timestamp migration for {db_path}")
    
    with duckdb.connect(db_path) as con:
        # 1. Audit entry_anchors
        table_exists = con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'entry_anchors'").fetchone()[0] > 0
        if table_exists:
            cols = con.execute("PRAGMA table_info('entry_anchors')").fetchall()
            for col in cols:
                name, type_name = col[1], col[2]
                if name in ['Entry_Timestamp', 'Closed_TS', 'Entry_Snapshot_TS', 'Snapshot_TS'] and type_name == 'DOUBLE':
                    logger.info(f"Migrating entry_anchors.{name} from DOUBLE to TIMESTAMP")
                    # DuckDB doesn't support direct ALTER COLUMN TYPE for all types, so we use a temp column
                    con.execute(f'ALTER TABLE entry_anchors ADD COLUMN "{name}_new" TIMESTAMP')
                    con.execute(f'UPDATE entry_anchors SET "{name}_new" = to_timestamp("{name}")')
                    con.execute(f'ALTER TABLE entry_anchors DROP COLUMN "{name}"')
                    con.execute(f'ALTER TABLE entry_anchors RENAME COLUMN "{name}_new" TO "{name}"')

        # 2. Audit clean_legs_v2
        table_exists = con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'clean_legs_v2'").fetchone()[0] > 0
        if table_exists:
            cols = con.execute("PRAGMA table_info('clean_legs_v2')").fetchall()
            for col in cols:
                name, type_name = col[1], col[2]
                if name in ['Snapshot_TS', 'Expiration', 'As of Date/Time'] and type_name == 'DOUBLE':
                    logger.info(f"Migrating clean_legs_v2.{name} from DOUBLE to TIMESTAMP")
                    con.execute(f'ALTER TABLE clean_legs_v2 ADD COLUMN "{name}_new" TIMESTAMP')
                    con.execute(f'UPDATE clean_legs_v2 SET "{name}_new" = to_timestamp("{name}")')
                    con.execute(f'ALTER TABLE clean_legs_v2 DROP COLUMN "{name}"')
                    con.execute(f'ALTER TABLE clean_legs_v2 RENAME COLUMN "{name}_new" TO "{name}"')

    logger.info("Migration complete.")

if __name__ == "__main__":
    migrate_timestamps()
