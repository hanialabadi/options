"""
Phase 7A: Windowed Drift Computation (Facts Only)

Computes deltas for Greeks, IV, and Underlying Price across canonical RAG windows:
- 1D (Tactical)
- 3D (Momentum)
- 10D (Thesis)
- Structural (Inception-to-Date)

This module is READ-ONLY and NON-DIRECTIVE. It produces facts, not decisions.
It handles both option legs and stock positions (delta/price only).
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path
from datetime import datetime, timedelta
from core.data_contracts.config import MANAGEMENT_SAFE_MODE

logger = logging.getLogger(__name__)

# Canonical RAG Windows
WINDOWS = {
    '1D': 1,
    '3D': 3,
    '10D': 10
}

# Metrics to track for options
OPTION_METRICS = ['Delta', 'Gamma', 'Vega', 'Theta', 'IV Mid', 'UL Last']
# Metrics to track for stocks
STOCK_METRICS = ['Delta', 'UL Last']

def compute_windowed_drift(df: pd.DataFrame, db_path: str = None) -> pd.DataFrame:
    """
    Compute drift metrics across multiple time horizons.
    
    Args:
        df: Current enriched positions DataFrame
        db_path: Path to DuckDB truth ledger
        
    Returns:
        DataFrame with windowed drift columns added
    """
    if df.empty:
        return df
        
    df = df.copy()
    
    # 1. Compute Structural Drift (Current vs Entry)
    # This uses frozen entry baselines rehydrated in Phase 3/4
    df = _compute_structural_drift(df)
    
    # 2. Compute Windowed Drift (1D, 3D, 10D)
    # This requires historical snapshots from DuckDB
    df = _compute_historical_window_drift(df, db_path)
    
    # 3. Phase 7B: Drift Smoothing & Persistence (Facts Only)
    # Compute SMA, Acceleration, and Volatility of drift
    df = _compute_drift_smoothing(df, db_path)
    
    return df

def _compute_structural_drift(df: pd.DataFrame) -> pd.DataFrame:
    """Compute drift since trade inception (Current - Entry)."""
    mappings = [
        ('Delta', 'Delta_Entry', 'Delta_Drift_Structural'),
        ('Gamma', 'Gamma_Entry', 'Gamma_Drift_Structural'),
        ('Vega', 'Vega_Entry', 'Vega_Drift_Structural'),
        ('Theta', 'Theta_Entry', 'Theta_Drift_Structural'),
        ('IV Mid', 'IV_Entry', 'IV_Drift_Structural'),
        ('UL Last', 'Underlying_Price_Entry', 'Price_Drift_Structural'),
    ]
    
    for current, entry, output in mappings:
        if current in df.columns and entry in df.columns:
            # Compute drift for all rows where entry data exists
            df[output] = df[current] - df[entry]
            
            # Stock-specific override: Gamma, Vega, Theta, IV drift are NaN for stocks
            if current in ['Gamma', 'Vega', 'Theta', 'IV Mid']:
                df.loc[df['AssetType'] == 'STOCK', output] = np.nan
        else:
            df[output] = np.nan
            
    return df

def _compute_historical_window_drift(df: pd.DataFrame, db_path: str = None) -> pd.DataFrame:
    """Compute drift over 1D, 3D, and 10D windows using historical snapshots."""
    if db_path is None:
        workspace_root = Path(__file__).parent.parent.parent
        db_path = str(workspace_root / "data" / "pipeline.duckdb")
        
    if not Path(db_path).exists():
        if not MANAGEMENT_SAFE_MODE:
            logger.warning(f"DuckDB not found at {db_path}, skipping windowed drift")
        return df
        
    import duckdb
    try:
        with duckdb.connect(db_path) as con:
            # Check if table exists
            table_exists = con.execute("""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = 'clean_legs' AND table_schema = 'main'
            """).fetchone()[0] > 0
            
            if not table_exists:
                return df

            trade_ids = df['TradeID'].unique().tolist()
            placeholders = ', '.join(['?' for _ in trade_ids])
            
            for label, days in WINDOWS.items():
                # Find the snapshot closest to 'days' ago for each TradeID
                # We use a window function to find the record with the smallest time difference
                # day_diff <= 2 allows for weekends/holidays
                query = f"""
                    WITH historical AS (
                        SELECT 
                            TradeID, 
                            Delta, Gamma, Vega, Theta, "IV Mid", "UL Last",
                            Snapshot_TS,
                            abs(date_diff('day', Snapshot_TS, current_timestamp) - {days}) as day_diff
                        FROM clean_legs
                        WHERE TradeID IN ({placeholders})
                        AND Snapshot_TS < current_timestamp - interval '{days-1} day'
                    ),
                    ranked AS (
                        SELECT *,
                        row_number() OVER (PARTITION BY TradeID ORDER BY day_diff ASC, Snapshot_TS DESC) as rank
                        FROM historical
                        WHERE day_diff <= 2
                    )
                    SELECT * FROM ranked WHERE rank = 1
                """
                
                try:
                    df_hist = con.execute(query, trade_ids).df()
                    if not df_hist.empty:
                        # Merge and compute delta
                        df = df.merge(
                            df_hist[['TradeID', 'Delta', 'Gamma', 'Vega', 'Theta', 'IV Mid', 'UL Last']],
                            on='TradeID',
                            how='left',
                            suffixes=('', f'_Hist_{label}')
                        )
                        
                        # Compute deltas (Current - Historical)
                        df[f'Delta_Drift_{label}'] = df['Delta'] - df[f'Delta_Hist_{label}']
                        df[f'Gamma_Drift_{label}'] = df['Gamma'] - df[f'Gamma_Hist_{label}']
                        df[f'Vega_Drift_{label}'] = df['Vega'] - df[f'Vega_Hist_{label}']
                        df[f'Theta_Drift_{label}'] = df['Theta'] - df[f'Theta_Hist_{label}']
                        df[f'IV_Drift_{label}'] = df['IV Mid'] - df[f'IV Mid_Hist_{label}']
                        df[f'Price_Drift_{label}'] = df['UL Last'] - df[f'UL Last_Hist_{label}']
                        
                        # Stock-specific override: Only Delta and Price drift for stocks
                        stock_mask = df['AssetType'] == 'STOCK'
                        for m in ['Gamma', 'Vega', 'Theta', 'IV']:
                            df.loc[stock_mask, f'{m}_Drift_{label}'] = np.nan
                        
                        # Drop temp columns
                        cols_to_drop = [f'{m}_Hist_{label}' for m in ['Delta', 'Gamma', 'Vega', 'Theta', 'IV Mid', 'UL Last']]
                        df = df.drop(columns=cols_to_drop)
                        
                        if not MANAGEMENT_SAFE_MODE:
                            logger.info(f"Computed {label} drift for {df[f'Delta_Drift_{label}'].notna().sum()} positions")
                except Exception as e:
                    if not MANAGEMENT_SAFE_MODE:
                        logger.warning(f"Failed to compute {label} drift: {e}")
                    
    except Exception as e:
        if not MANAGEMENT_SAFE_MODE:
            logger.warning(f"DuckDB connection failed for windowed drift: {e}")
        
    return df

def _compute_drift_smoothing(df: pd.DataFrame, db_path: str = None) -> pd.DataFrame:
    """
    Phase 7B: Compute drift smoothing, acceleration, and stability metrics.
    
    Metrics:
    - Drift_SMA_3: 3-snapshot simple moving average of structural drift
    - Drift_Acceleration: Change in structural drift since last snapshot
    - Drift_Stability: Standard deviation of structural drift over last 5 snapshots
    - Snapshot_Count: Number of historical snapshots available for this trade
    """
    if db_path is None:
        workspace_root = Path(__file__).parent.parent.parent
        db_path = str(workspace_root / "data" / "pipeline.duckdb")
        
    if not Path(db_path).exists():
        return df
        
    import duckdb
    try:
        with duckdb.connect(db_path) as con:
            table_exists = con.execute("""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = 'clean_legs' AND table_schema = 'main'
            """).fetchone()[0] > 0
            
            if not table_exists:
                return df

            trade_ids = df['TradeID'].unique().tolist()
            placeholders = ', '.join(['?' for _ in trade_ids])
            
            # Query last 5 snapshots for each TradeID to compute smoothing
            # We focus on Delta_Drift_Structural as the primary smoothing target
            query = f"""
                WITH history AS (
                    SELECT 
                        TradeID, 
                        Snapshot_TS,
                        Delta - Delta_Entry as delta_drift,
                        row_number() OVER (PARTITION BY TradeID ORDER BY Snapshot_TS DESC) as recency
                    FROM clean_legs
                    WHERE TradeID IN ({placeholders})
                    AND Delta_Entry IS NOT NULL
                )
                SELECT 
                    TradeID,
                    count(*) as snapshot_count,
                    avg(delta_drift) FILTER (WHERE recency <= 3) as delta_drift_sma_3,
                    -- Acceleration: (Current Drift - Previous Drift)
                    max(delta_drift) FILTER (WHERE recency = 1) - max(delta_drift) FILTER (WHERE recency = 2) as delta_drift_accel,
                    stddev(delta_drift) FILTER (WHERE recency <= 5) as delta_drift_stability
                FROM history
                GROUP BY TradeID
            """
            
            try:
                df_smooth = con.execute(query, trade_ids).df()
                if not df_smooth.empty:
                    df = df.merge(
                        df_smooth,
                        on='TradeID',
                        how='left'
                    )
                    
                    # Add sufficiency flags
                    df['Drift_History_Sufficient'] = df['snapshot_count'] >= 3
                    
                    if not MANAGEMENT_SAFE_MODE:
                        logger.info(f"✅ Drift smoothing computed for {len(df_smooth)} trades")
            except Exception as e:
                if not MANAGEMENT_SAFE_MODE:
                    logger.warning(f"Failed to compute drift smoothing: {e}")
                    
    except Exception as e:
        if not MANAGEMENT_SAFE_MODE:
            logger.warning(f"DuckDB connection failed for drift smoothing: {e}")
            
    return df
