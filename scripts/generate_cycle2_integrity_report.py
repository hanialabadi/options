import duckdb
import pandas as pd
import os

DB_PATH = 'data/pipeline.duckdb'

def generate_report():
    if not os.path.exists(DB_PATH):
        print(f"Error: {DB_PATH} not found.")
        return

    con = duckdb.connect(DB_PATH, read_only=True)

    # 1. Active symbols count (latest run_id)
    latest_run_id = con.execute('SELECT MAX(run_id) FROM clean_legs').fetchone()[0]
    active_symbols_count = con.execute('SELECT COUNT(DISTINCT Symbol) FROM clean_legs WHERE run_id = ?', [latest_run_id]).fetchone()[0]
    print(f'Active Symbols (Latest Run: {latest_run_id}): {active_symbols_count}')

    # 2. Oldest / newest snapshot per active symbol (Canonical Anchors Only)
    print('\n=== CANONICAL ANCHOR RANGE PER ACTIVE SYMBOL (Top 10 by depth) ===')
    range_df = con.execute('''
        SELECT 
            Symbol, 
            MIN(Snapshot_TS) as oldest, 
            MAX(Snapshot_TS) as newest, 
            COUNT(*) as snapshots
        FROM canonical_anchors
        WHERE Symbol IN (SELECT Symbol FROM clean_legs WHERE run_id = ?)
        GROUP BY Symbol
        ORDER BY snapshots DESC
        LIMIT 10
    ''', [latest_run_id]).df()
    print(range_df.to_string(index=False))

    # 3. Check for archived transitions
    archive_exists = con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'clean_legs_archive'").fetchone()[0] > 0
    if archive_exists:
        print('\n=== ARCHIVED TRANSITIONS ===')
        transitions = con.execute('SELECT Symbol, MIN(Snapshot_TS) as first_archived, MAX(Snapshot_TS) as last_archived, COUNT(*) as snapshots FROM clean_legs_archive GROUP BY Symbol').df()
        print(transitions.to_string(index=False))
    else:
        print('\nNOTE: clean_legs_archive table does not exist yet. No transitions recorded.')

    con.close()

if __name__ == "__main__":
    generate_report()
