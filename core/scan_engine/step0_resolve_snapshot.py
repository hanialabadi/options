"""
Step 0: Snapshot Authority Resolver

This module provides a single, authoritative function to resolve the path to the
IV/HV snapshot CSV file, ensuring consistent logic across CLI and dashboard runs.
"""

import pandas as pd
import logging
import os
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)

def extract_timestamp(f: Path) -> datetime:
    """Extracts timestamp from snapshot filename."""
    stem = f.stem
    if stem.startswith("ivhv_snapshot_live_"):
        ts_str = stem.replace("ivhv_snapshot_live_", "")
    elif stem.startswith("snapshot_"):
        ts_str = stem.replace("snapshot_", "")
    else:
        return datetime.min
    
    try:
        return datetime.strptime(ts_str, "%Y%m%d_%H%M%S")
    except ValueError:
        return datetime.min

def resolve_snapshot_path(
    explicit_path: str | None = None,
    uploaded_path: str | None = None,
    snapshots_dir: str = "data/snapshots"
) -> str:
    """
    Resolves the absolute path to the IV/HV snapshot CSV file based on a defined hierarchy.

    Resolution order:
    1. uploaded_path if provided and exists
    2. explicit_path if provided and exists
    3. Latest snapshot in snapshots_dir by filename timestamp

    If no valid snapshot path is found, a FileNotFoundError is raised.

    Args:
        explicit_path (str | None): An explicitly provided path to the snapshot file.
        uploaded_path (str | None): A path to a temporary uploaded snapshot file (e.g., from a UI).
        snapshots_dir (str): The directory where archived snapshots are stored.

    Returns:
        str: The absolute path to the resolved snapshot file.

    Raises:
        FileNotFoundError: If no valid snapshot file can be resolved.
    """
    resolved_path = None

    # 1. Check uploaded_path
    if uploaded_path and Path(uploaded_path).is_file():
        resolved_path = Path(uploaded_path).resolve()
        logger.info(f"✅ Resolved snapshot path (uploaded): {resolved_path}")
    
    # 2. Check explicit_path
    if resolved_path is None and explicit_path and Path(explicit_path).is_file():
        resolved_path = Path(explicit_path).resolve()
        logger.info(f"✅ Resolved snapshot path (explicit): {resolved_path}")

    # 3. Find latest snapshot in snapshots_dir
    if resolved_path is None:
        archive_dir = Path(snapshots_dir).resolve()
        if archive_dir.is_dir():
            # Support both legacy 'snapshot_*.csv' and new 'ivhv_snapshot_live_*.csv'
            patterns = ["ivhv_snapshot_live_*.csv", "snapshot_*.csv"]
            all_snapshots = []
            for pattern in patterns:
                all_snapshots.extend(list(archive_dir.glob(pattern)))
            
            snapshots = sorted(
                all_snapshots,
                key=extract_timestamp,
                reverse=True
            )
            if snapshots:
                resolved_path = snapshots[0]
                logger.info(f"✅ Resolved snapshot path (latest in archive): {resolved_path}")
            else:
                logger.warning(f"⚠️ No snapshots found in archive directory: {archive_dir}")
        else:
            logger.warning(f"⚠️ Snapshots directory not found: {archive_dir}")

    if resolved_path is None:
        raise FileNotFoundError(
            "❌ No valid IV/HV snapshot file could be resolved. "
            "Please provide an explicit path, upload a file, or ensure snapshots exist in the archive."
        )

    # Log file metadata
    # Deterministic Age: Use filename timestamp instead of filesystem mtime
    ts = extract_timestamp(resolved_path)
    if ts != datetime.min:
        logger.info(f"📊 Snapshot filename: {resolved_path.name}")
        logger.info(f"📊 Snapshot Market Date: {ts.strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        file_stat = resolved_path.stat()
        mod_time = datetime.fromtimestamp(file_stat.st_mtime)
        logger.info(f"📊 Snapshot filename: {resolved_path.name}")
        logger.info(f"📊 File modification time (fallback): {mod_time.strftime('%Y-%m-%d %H:%M:%S')}")

    return str(resolved_path)
