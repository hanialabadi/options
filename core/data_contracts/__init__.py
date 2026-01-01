"""
Data Contracts - Single Source of Truth for All Data Operations

This package provides the ONLY interface for reading/writing:
- active_master.csv (active trades)
- timestamped position snapshots
- archived trades

NO code outside this package should directly read/write CSVs.

Usage:
    from core.data_contracts import load_active_master, save_active_master
    from core.data_contracts import save_snapshot, load_snapshot_timeseries
    from core.data_contracts.config import ACTIVE_MASTER_PATH, SNAPSHOT_DIR
"""

from .master_data import (
    load_active_master,
    save_active_master,
    get_active_trade_ids,
    get_trade_by_id,
    validate_schema
)

from .snapshot_data import (
    save_snapshot,
    load_snapshot,
    load_snapshot_timeseries,
    list_snapshots,
    get_latest_snapshot,
    cleanup_old_snapshots,
    validate_snapshot
)

from .config import (
    ACTIVE_MASTER_PATH,
    SNAPSHOT_DIR,
    ARCHIVE_DIR,
    ensure_data_directories
)

__all__ = [
    # Master data operations
    "load_active_master",
    "save_active_master",
    "get_active_trade_ids",
    "get_trade_by_id",
    "validate_schema",
    
    # Snapshot operations
    "save_snapshot",
    "load_snapshot",
    "load_snapshot_timeseries",
    "list_snapshots",
    "get_latest_snapshot",
    "cleanup_old_snapshots",
    "validate_snapshot",
    
    # Paths
    "ACTIVE_MASTER_PATH",
    "SNAPSHOT_DIR",
    "ARCHIVE_DIR",
    "ensure_data_directories",
]
