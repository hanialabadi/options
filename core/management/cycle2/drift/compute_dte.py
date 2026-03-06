"""
Phase 3 Observable: Days to Expiration (DTE)

Calculates explicit DTE field for consistent time decay tracking.

Design:
- DTE = (Expiration - Snapshot_TS).days
- Snapshot_TS must be available (added in Phase 4, but calculated here for determinism)
- Negative DTE = expired position (possible if broker hasn't removed position)

This is an OBSERVATION, not a freeze. Phase 6 will create DTE_Entry.
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)


def compute_dte(df: pd.DataFrame, snapshot_ts: pd.Timestamp = None) -> pd.DataFrame:
    """
    Add DTE (Days to Expiration) column.
    
    Parameters
    ----------
    df : pd.DataFrame
        Must contain 'Expiration' column (datetime)
    snapshot_ts : pd.Timestamp, optional
        Timestamp for DTE calculation. If None, uses pd.Timestamp.now()
        (Phase 4 will pass explicit Snapshot_TS for determinism)
    
    Returns
    -------
    pd.DataFrame
        Input DataFrame with added 'DTE' column (integer days)
    
    Notes
    -----
    - DTE can be negative if position has expired but not yet closed
    - Uses .dt.days for calendar day difference (not trading days)
    - Deterministic when snapshot_ts is provided
    """
    if df.empty:
        logger.warning("Empty DataFrame passed to compute_dte")
        return df
    
    if "Expiration" not in df.columns:
        logger.error("Missing 'Expiration' column for DTE calculation")
        raise ValueError("Cannot compute DTE: 'Expiration' column missing")
    
    # Use provided timestamp or current time
    reference_time = snapshot_ts if snapshot_ts is not None else pd.Timestamp.now()
    
    # Ensure Expiration is datetime
    df["Expiration"] = pd.to_datetime(df["Expiration"], errors='coerce')
    
    # Calculate calendar days to expiration
    df["DTE"] = (df["Expiration"] - reference_time).dt.days
    
    # Handle NaT (invalid expiration dates)
    df["DTE"] = df["DTE"].fillna(-999).astype(int)  # -999 = invalid expiration
    
    logger.info(f"Computed DTE for {len(df)} positions (range: {df['DTE'].min()} to {df['DTE'].max()} days)")
    
    return df
