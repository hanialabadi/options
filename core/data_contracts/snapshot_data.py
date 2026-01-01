"""
Snapshot Data Contract - Timestamped Position History

Manages timestamped snapshots used for drift tracking and historical analysis.

File naming convention: positions_YYYY-MM-DD_HH-MM-SS.csv
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
import logging
from typing import Optional, List

from .config import SNAPSHOT_DIR, SNAPSHOT_PATTERN

logger = logging.getLogger(__name__)


# === CANONICAL SNAPSHOT SAVER ===
def save_snapshot(df: pd.DataFrame, timestamp: Optional[datetime] = None) -> Path:
    """
    Save a timestamped position snapshot.
    
    Args:
        df: DataFrame with position data (must include TradeID)
        timestamp: Optional timestamp. If None, uses current time
        
    Returns:
        Path to saved snapshot file
        
    Raises:
        ValueError: If DataFrame is invalid
    """
    if df.empty:
        logger.warning("Attempting to save empty snapshot")
    
    if "TradeID" not in df.columns:
        raise ValueError("DataFrame must contain 'TradeID' column")
    
    # Generate filename
    ts = timestamp or datetime.now()
    filename = ts.strftime(SNAPSHOT_PATTERN)
    file_path = SNAPSHOT_DIR / filename
    
    # Ensure directory exists
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    
    try:
        df.to_csv(file_path, index=False)
        logger.info(f"✅ Saved snapshot: {len(df)} positions to {file_path}")
        return file_path
        
    except Exception as e:
        logger.error(f"❌ Failed to save snapshot: {e}")
        raise


# === CANONICAL SNAPSHOT LOADER ===
def load_snapshot(file_path: Path) -> pd.DataFrame:
    """
    Load a single snapshot file.
    
    Args:
        file_path: Path to snapshot CSV
        
    Returns:
        pd.DataFrame: Snapshot data
    """
    if not file_path.exists():
        raise FileNotFoundError(f"Snapshot not found: {file_path}")
    
    try:
        df = pd.read_csv(file_path)
        logger.info(f"✅ Loaded snapshot: {len(df)} positions from {file_path.name}")
        return df
        
    except Exception as e:
        logger.error(f"❌ Failed to load snapshot {file_path}: {e}")
        raise


# === SNAPSHOT DISCOVERY ===
def list_snapshots(start_date: Optional[datetime] = None, 
                   end_date: Optional[datetime] = None) -> List[Path]:
    """
    List all snapshot files, optionally filtered by date range.
    
    Args:
        start_date: Optional start date filter
        end_date: Optional end date filter
        
    Returns:
        List of snapshot file paths, sorted by timestamp (oldest first)
    """
    if not SNAPSHOT_DIR.exists():
        logger.warning(f"Snapshot directory not found: {SNAPSHOT_DIR}")
        return []
    
    # Find all snapshot files
    snapshots = sorted(SNAPSHOT_DIR.glob("positions_*.csv"))
    
    # Filter by date if specified
    if start_date or end_date:
        filtered = []
        for snapshot in snapshots:
            try:
                ts = parse_snapshot_timestamp(snapshot)
                if start_date and ts < start_date:
                    continue
                if end_date and ts > end_date:
                    continue
                filtered.append(snapshot)
            except ValueError:
                logger.warning(f"Could not parse timestamp from {snapshot.name}")
                continue
        snapshots = filtered
    
    logger.info(f"Found {len(snapshots)} snapshots")
    return snapshots


# === TIMESERIES OPERATIONS ===
def load_snapshot_timeseries(active_trade_ids: Optional[set] = None,
                             start_date: Optional[datetime] = None,
                             end_date: Optional[datetime] = None) -> pd.DataFrame:
    """
    Load all snapshots as a timeseries for drift analysis.
    
    This is used by drift_engine to build historical metrics.
    
    Args:
        active_trade_ids: If provided, filter to these TradeIDs only
        start_date: Optional start date filter
        end_date: Optional end date filter
        
    Returns:
        pd.DataFrame: Combined data with Snapshot_TS column
    """
    snapshots = list_snapshots(start_date, end_date)
    
    if not snapshots:
        logger.warning("No snapshots found for timeseries")
        return pd.DataFrame()
    
    data = []
    for snapshot_path in snapshots:
        try:
            ts = parse_snapshot_timestamp(snapshot_path)
            df = load_snapshot(snapshot_path)
            
            # Filter to active trades if specified
            if active_trade_ids and "TradeID" in df.columns:
                df = df[df["TradeID"].isin(active_trade_ids)]
            
            df["Snapshot_TS"] = ts
            data.append(df)
            
        except Exception as e:
            logger.warning(f"⚠️ Skipping {snapshot_path.name}: {e}")
            continue
    
    if not data:
        return pd.DataFrame()
    
    df_all = pd.concat(data, ignore_index=True)
    logger.info(f"✅ Loaded timeseries: {len(df_all)} rows from {len(snapshots)} snapshots")
    return df_all


def get_latest_snapshot() -> Optional[pd.DataFrame]:
    """
    Get the most recent snapshot.
    
    Returns:
        pd.DataFrame or None if no snapshots exist
    """
    snapshots = list_snapshots()
    if not snapshots:
        return None
    
    latest = snapshots[-1]  # Already sorted
    return load_snapshot(latest)


# === TIMESTAMP UTILITIES ===
def parse_snapshot_timestamp(file_path: Path) -> datetime:
    """
    Extract timestamp from snapshot filename.
    
    Args:
        file_path: Path to snapshot file
        
    Returns:
        datetime object
        
    Raises:
        ValueError: If filename doesn't match expected pattern
    """
    try:
        # Extract timestamp part from "positions_YYYY-MM-DD_HH-MM-SS.csv"
        timestamp_str = file_path.stem.replace("positions_", "")
        return datetime.strptime(timestamp_str, "%Y-%m-%d_%H-%M-%S")
    except Exception as e:
        raise ValueError(f"Could not parse timestamp from {file_path.name}: {e}")


# === CLEANUP UTILITIES ===
def cleanup_old_snapshots(keep_days: int = 30) -> int:
    """
    Delete snapshots older than specified days.
    
    Args:
        keep_days: Number of days to retain
        
    Returns:
        Number of files deleted
    """
    cutoff = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    cutoff = cutoff.replace(day=cutoff.day - keep_days)
    
    snapshots = list_snapshots(end_date=cutoff)
    deleted = 0
    
    for snapshot in snapshots:
        try:
            snapshot.unlink()
            deleted += 1
        except Exception as e:
            logger.warning(f"Could not delete {snapshot.name}: {e}")
    
    logger.info(f"🧹 Deleted {deleted} snapshots older than {keep_days} days")
    return deleted


# === REQUIRED SNAPSHOT COLUMNS ===
REQUIRED_SNAPSHOT_COLUMNS = {
    "TradeID", "PCS", "Delta", "Gamma", "Vega", "Theta", "IVHV_Gap"
}


def validate_snapshot(df: pd.DataFrame) -> tuple[bool, list]:
    """
    Validate snapshot DataFrame schema.
    
    Returns:
        (is_valid, list_of_issues)
    """
    issues = []
    
    missing = REQUIRED_SNAPSHOT_COLUMNS - set(df.columns)
    if missing:
        issues.append(f"Missing required columns: {missing}")
    
    if "TradeID" in df.columns and df["TradeID"].isna().any():
        issues.append(f"Found {df['TradeID'].isna().sum()} rows with missing TradeID")
    
    is_valid = len(issues) == 0
    return is_valid, issues
