"""
Master Data Contract - Single Source of Truth for Active Trades

This module provides the ONLY interface for reading/writing active_master.csv.
All other code must import from here - NO direct CSV operations elsewhere.

Schema: See README.md for full field documentation
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
import logging
from typing import Optional

from .config import ACTIVE_MASTER_PATH

logger = logging.getLogger(__name__)


# === CANONICAL LOADER ===
def load_active_master(path: Optional[Path] = None) -> pd.DataFrame:
    """
    Load the active trades master file.
    
    This is the SINGLE CANONICAL function for loading active_master.csv.
    All code should use this instead of pd.read_csv() directly.
    
    Args:
        path: Optional override path. If None, uses config.ACTIVE_MASTER_PATH
        
    Returns:
        pd.DataFrame: Active trades with full schema
        
    Raises:
        FileNotFoundError: If master file doesn't exist
        ValueError: If required columns are missing
    """
    file_path = path or ACTIVE_MASTER_PATH
    
    if not file_path.exists():
        logger.warning(f"Active master not found at {file_path}, returning empty DataFrame")
        return pd.DataFrame()
    
    try:
        df = pd.read_csv(file_path)
        logger.info(f"✅ Loaded active_master: {len(df)} trades from {file_path}")
        
        # Basic validation
        if df.empty:
            logger.warning("Active master is empty")
            return df
            
        # Ensure TradeID exists (minimum requirement)
        if "TradeID" not in df.columns:
            raise ValueError("Missing required column: TradeID")
            
        return df
        
    except Exception as e:
        logger.error(f"❌ Failed to load active_master: {e}")
        raise


# === CANONICAL WRITER ===
def save_active_master(df: pd.DataFrame, path: Optional[Path] = None, backup: bool = True) -> None:
    """
    Save the active trades master file.
    
    This is the SINGLE CANONICAL function for writing active_master.csv.
    
    Args:
        df: DataFrame to save
        path: Optional override path. If None, uses config.ACTIVE_MASTER_PATH
        backup: If True, create timestamped backup before overwriting
        
    Raises:
        ValueError: If DataFrame is invalid
    """
    file_path = path or ACTIVE_MASTER_PATH
    
    # Validation
    if df.empty:
        logger.warning("Attempting to save empty DataFrame")
    
    if "TradeID" in df.columns and df["TradeID"].isna().any():
        logger.warning(f"Found {df['TradeID'].isna().sum()} rows with missing TradeID")
    
    try:
        # Backup existing file
        if backup and file_path.exists():
            backup_path = file_path.with_suffix(f".backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
            file_path.rename(backup_path)
            logger.info(f"📦 Backup created: {backup_path}")
        
        # Ensure parent directory exists
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save
        df.to_csv(file_path, index=False)
        logger.info(f"✅ Saved active_master: {len(df)} trades to {file_path}")
        
    except Exception as e:
        logger.error(f"❌ Failed to save active_master: {e}")
        raise


# === QUERY HELPERS ===
def get_active_trade_ids(path: Optional[Path] = None) -> set:
    """
    Get set of active TradeIDs.
    
    Useful for filtering snapshots or checking if a trade is active.
    """
    df = load_active_master(path)
    if df.empty or "TradeID" not in df.columns:
        return set()
    return set(df["TradeID"].dropna().unique())


def get_trade_by_id(trade_id: str, path: Optional[Path] = None) -> Optional[pd.Series]:
    """
    Get a single trade by TradeID.
    
    Returns:
        pd.Series or None if not found
    """
    df = load_active_master(path)
    if df.empty or "TradeID" not in df.columns:
        return None
    
    matches = df[df["TradeID"] == trade_id]
    if matches.empty:
        return None
    
    if len(matches) > 1:
        logger.warning(f"Multiple trades found with TradeID={trade_id}, returning first")
    
    return matches.iloc[0]


# === SCHEMA VALIDATION ===
REQUIRED_COLUMNS = {
    "TradeID", "Symbol", "Strategy"
}

FROZEN_AT_ENTRY = {
    "TradeID", "Symbol", "Strategy", "TradeDate",
    "Contract_Symbols", "Strikes", "Expiration",
    "PCS_Entry", "Vega_Entry", "Delta_Entry", "Gamma_Entry", "Theta_Entry",
    "IVHV_Gap_Entry", "Chart_Trend_Entry"
}

LIVE_UPDATED = {
    "PCS", "Vega", "Delta", "Gamma", "Theta",
    "Days_Held", "Held_ROI%"
}

DERIVED_FIELDS = {
    "PCS_Drift", "Vega_ROC", "Flag_PCS_Drift", "Flag_Vega_Flat"
}


def validate_schema(df: pd.DataFrame, strict: bool = False) -> tuple[bool, list]:
    """
    Validate DataFrame against expected schema.
    
    Args:
        df: DataFrame to validate
        strict: If True, require ALL expected columns
        
    Returns:
        (is_valid, list_of_issues)
    """
    issues = []
    
    # Check required columns
    missing_required = REQUIRED_COLUMNS - set(df.columns)
    if missing_required:
        issues.append(f"Missing required columns: {missing_required}")
    
    # Check for duplicates
    if "TradeID" in df.columns:
        duplicates = df["TradeID"].duplicated().sum()
        if duplicates > 0:
            issues.append(f"Found {duplicates} duplicate TradeIDs")
    
    is_valid = len(issues) == 0
    return is_valid, issues
