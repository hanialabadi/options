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
from core.shared.data_contracts.config import MANAGEMENT_SAFE_MODE
from core.shared.data_layer.duckdb_utils import fetch_historical_legs_data, fetch_drift_history_for_smoothing

logger = logging.getLogger(__name__)

# Canonical RAG Windows
WINDOWS = {
    '1D': 1,
    '3D': 3,
    '10D': 10
}

# Metrics to track for options
OPTION_METRICS = ['Delta', 'Gamma', 'Vega', 'Theta', 'UL Last']
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

    # 4. ROC Persistence Counter
    # Counts consecutive snapshots where Delta_ROC_3D was negative (deterioration).
    # DriftEngine.assess_signal_drift() requires ROC_Persist_3D >= 2 before escalating
    # to DEGRADED — prevents single noisy-day triggers.
    df = _compute_roc_persistence(df, db_path)

    # 5. IV vs HV Gap + IV Percentile
    # IV_vs_HV_Gap: current IV minus HV_20D — positive = premium, negative = crush.
    # IV_Percentile: where today's IV sits within the last N days of IV_Now history
    # for this specific contract (0=historically low, 100=historically high).
    df = _compute_iv_vs_hv_and_percentile(df, db_path)

    return df

def _compute_structural_drift(df: pd.DataFrame) -> pd.DataFrame:
    """Compute drift since trade inception (Current - Entry)."""
    mappings = [
        ('Delta', 'Delta_Entry', 'Delta_Drift_Structural'),
        ('Gamma', 'Gamma_Entry', 'Gamma_Drift_Structural'),
        ('Vega', 'Vega_Entry', 'Vega_Drift_Structural'),
        ('Theta', 'Theta_Entry', 'Theta_Drift_Structural'),
        ('UL Last', 'Underlying_Price_Entry', 'Price_Drift_Structural'),
    ]
    
    for current, entry, output in mappings:
        if current in df.columns and entry in df.columns:
            # Compute drift for all rows where entry data exists
            df[output] = df[current] - df[entry]
            
            # Stock-specific override: Gamma, Vega, Theta drift are NaN for stocks
            if current in ['Gamma', 'Vega', 'Theta']:
                df.loc[df['AssetType'] == 'STOCK', output] = np.nan
        else:
            df[output] = np.nan
    
    # Cycle-2 Authoritative IV Drift (Structural)
    # RAG: Use IV_Now (transient) vs IV_Entry (anchor)
    if 'IV_Now' in df.columns and 'IV_Entry' in df.columns:
        # Normalize IV_Entry to Decimal Vol if it's in percent (Cycle 1 legacy)
        from core.shared.finance_utils import normalize_iv_series
        iv_entry_val = normalize_iv_series(pd.to_numeric(df['IV_Entry'], errors='coerce'))
        df['IV_Drift_Structural'] = df['IV_Now'] - iv_entry_val
        df.loc[df['AssetType'] == 'STOCK', 'IV_Drift_Structural'] = np.nan
    else:
        df['IV_Drift_Structural'] = np.nan
            
    return df

def _compute_historical_window_drift(df: pd.DataFrame, db_path: str = None) -> pd.DataFrame:
    """Compute drift over 1D, 3D, and 10D windows using historical snapshots."""
    trade_ids = df['TradeID'].unique().tolist()
    
    for label, days in WINDOWS.items():
        df_hist = fetch_historical_legs_data(trade_ids, days, db_path)
        
        if df_hist is not None and not df_hist.empty:
            # Merge and compute delta
            df = df.merge(
                df_hist[['LegID', 'Delta', 'Gamma', 'Vega', 'Theta', 'UL Last', 'IV_Now']],
                on='LegID',
                how='left',
                suffixes=('', f'_Hist_{label}')
            )
            
            # Compute deltas (Current - Historical)
            df[f'Delta_Drift_{label}'] = df['Delta'] - df[f'Delta_Hist_{label}']
            df[f'Gamma_Drift_{label}'] = df['Gamma'] - df[f'Gamma_Hist_{label}']
            df[f'Vega_Drift_{label}'] = df['Vega'] - df[f'Vega_Hist_{label}']
            df[f'Theta_Drift_{label}'] = df['Theta'] - df[f'Theta_Hist_{label}']
            df[f'Price_Drift_{label}'] = df['UL Last'] - df[f'UL Last_Hist_{label}']

            # Cycle-2: IV Drift (Windowed)
            if f'IV_Now_Hist_{label}' in df.columns and 'IV_Now' in df.columns:
                df[f'IV_Drift_{label}'] = df['IV_Now'] - df[f'IV_Now_Hist_{label}']
            else:
                df[f'IV_Drift_{label}'] = np.nan

            # Greek ROC (Rate of Change) — slope-based acceleration metric.
            # ROC = drift / |historical_value|, clamped to [-1, 1] to prevent
            # division explosion near zero. Produces a normalized change rate:
            #   0.10 = 10% change in this Greek over the window.
            # ROC columns feed DriftEngine.assess_signal_drift() for slope-based
            # override logic (DEGRADED/VIOLATED) beyond simple magnitude thresholds.
            for greek, hist_col in [
                ('Delta', f'Delta_Hist_{label}'),
                ('Gamma', f'Gamma_Hist_{label}'),
                ('Vega',  f'Vega_Hist_{label}'),
                ('Theta', f'Theta_Hist_{label}'),
            ]:
                roc_col = f'{greek}_ROC_{label}'
                drift_col = f'{greek}_Drift_{label}'
                if hist_col in df.columns and drift_col in df.columns:
                    _hist_abs = df[hist_col].abs().replace(0, np.nan)
                    df[roc_col] = (df[drift_col] / _hist_abs).clip(-1.0, 1.0)
                else:
                    df[roc_col] = np.nan

            # IV ROC
            _iv_roc_col = f'IV_ROC_{label}'
            _iv_hist_col = f'IV_Now_Hist_{label}'
            if _iv_hist_col in df.columns and f'IV_Drift_{label}' in df.columns:
                _iv_hist_abs = df[_iv_hist_col].abs().replace(0, np.nan)
                df[_iv_roc_col] = (df[f'IV_Drift_{label}'] / _iv_hist_abs).clip(-1.0, 1.0)
            else:
                df[_iv_roc_col] = np.nan

            # Stock-specific override: Only Delta and Price drift for stocks
            stock_mask = df['AssetType'] == 'STOCK'
            for m in ['Gamma', 'Vega', 'Theta', 'IV']:
                df.loc[stock_mask, f'{m}_Drift_{label}'] = np.nan
                df.loc[stock_mask, f'{m}_ROC_{label}']  = np.nan
            
            # Drop temp columns
            cols_to_drop = [f'{m}_Hist_{label}' for m in ['Delta', 'Gamma', 'Vega', 'Theta', 'UL Last', 'IV_Now']]
            df = df.drop(columns=[col for col in cols_to_drop if col in df.columns])
            
            if not MANAGEMENT_SAFE_MODE:
                logger.info(f"Computed {label} drift for {df[f'Delta_Drift_{label}'].notna().sum()} positions")
        else:
            if not MANAGEMENT_SAFE_MODE:
                logger.warning(f"No historical data found for {label} drift, skipping.")
                
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
    trade_ids = df['TradeID'].unique().tolist()
    df_smooth = fetch_drift_history_for_smoothing(trade_ids, db_path)
    
    if df_smooth is not None and not df_smooth.empty:
        # fetch_drift_history_for_smoothing now returns one row per TradeID
        df = df.merge(
            df_smooth,
            on='TradeID',
            how='left'
        )

        # Add sufficiency flags
        df['Drift_History_Sufficient'] = df['snapshot_count'].fillna(0) >= 3

        if not MANAGEMENT_SAFE_MODE:
            logger.info(f"✅ Drift smoothing computed for {len(df_smooth)} trades")
    else:
        if not MANAGEMENT_SAFE_MODE:
            logger.warning("No historical data found for drift smoothing, skipping.")

    return df


def _compute_roc_persistence(df: pd.DataFrame, db_path: str = None) -> pd.DataFrame:
    """
    Compute ROC_Persist_3D: number of consecutive snapshots (up to 5) where
    Delta_ROC_3D was negative (deteriorating for long positions).

    This feeds DriftEngine.assess_signal_drift() persistence gate:
      ROC_Persist_3D >= 2  → DEGRADED allowed
      ROC_Persist_3D >= 1  → VIOLATED allowed (half threshold — act faster on severe breaks)

    If historical data is unavailable, defaults to 0 (no persistence confirmed —
    engine will not escalate, which is the safe failure mode).
    """
    # Default: no confirmed persistence
    df['ROC_Persist_3D'] = 0

    try:
        import duckdb
        from pathlib import Path
        from core.shared.data_contracts.config import PIPELINE_DB_PATH
        from core.shared.data_layer.duckdb_utils import _table_exists

        _pipeline_db = str(PIPELINE_DB_PATH)
        if not Path(_pipeline_db).exists():
            return df

        trade_ids = df['TradeID'].unique().tolist()
        if not trade_ids:
            return df

        # Query management_recommendations for daily Delta values.
        # Deduplicate to one row per TradeID per calendar day (latest snapshot wins).
        # Compute day-over-day Delta change; count consecutive declining days
        # from most recent backward — that is the persistence count.
        with duckdb.connect(_pipeline_db, read_only=True) as con:
            if not _table_exists(con, 'management_recommendations'):
                return df

            placeholders = ', '.join(['?' for _ in trade_ids])
            roc_hist = con.execute(f"""
                WITH daily AS (
                    SELECT TradeID, Delta, Snapshot_TS::DATE AS snap_date
                    FROM management_recommendations
                    WHERE TradeID IN ({placeholders})
                      AND Delta IS NOT NULL
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY TradeID, Snapshot_TS::DATE
                        ORDER BY Snapshot_TS DESC
                    ) = 1
                ),
                with_prev AS (
                    SELECT TradeID, snap_date, Delta,
                           LAG(Delta) OVER (PARTITION BY TradeID ORDER BY snap_date) AS prev_delta
                    FROM daily
                )
                SELECT TradeID, snap_date,
                       (Delta - prev_delta) AS delta_day_change
                FROM with_prev
                WHERE prev_delta IS NOT NULL
                ORDER BY TradeID, snap_date DESC
            """, trade_ids).df()

        if roc_hist.empty:
            return df

        # For each TradeID: count consecutive leading days where delta declined
        persist_map: dict = {}
        for tid, grp in roc_hist.groupby('TradeID'):
            count = 0
            for val in grp['delta_day_change']:
                if val < 0:
                    count += 1
                else:
                    break  # streak broken — stop counting
            persist_map[str(tid)] = min(count, 5)  # cap at 5

        df['ROC_Persist_3D'] = df['TradeID'].map(
            lambda t: persist_map.get(str(t), 0)
        ).fillna(0).astype(int)

        if not MANAGEMENT_SAFE_MODE:
            _n_persisting = (df['ROC_Persist_3D'] >= 2).sum()
            logger.info(f"ROC persistence computed: {_n_persisting} positions with ≥2 consecutive declining Delta days")

    except Exception as e:
        logger.warning(f"ROC persistence computation failed (non-fatal): {e}")
        df['ROC_Persist_3D'] = 0

    return df


def _compute_iv_vs_hv_and_percentile(df: pd.DataFrame, db_path: str = None) -> pd.DataFrame:
    """
    Compute two vol-state diagnostics:

    IV_vs_HV_Gap (decimal):
        IV_Now - HV_20D for each option leg.
        Positive = IV trading at premium to realised vol → selling edge.
        Negative = IV crushed below realised → long vol losing edge.
        Stocks: NaN.

    IV_Percentile (0–100):
        Where today's IV_Now sits within the historical distribution of
        IV_Now readings for this TradeID in management_recommendations.
        Formula: percentile_rank = (# days where historical_iv < today_iv) / total_days * 100
        Bounded 0–100. Requires ≥ 3 days of IV history; else NaN.
        Low (< 25) = historically cheap vol.
        High (> 75) = historically expensive vol.

    Both columns default to NaN on failure — non-fatal.
    """
    # --- IV_vs_HV_Gap: pure arithmetic, no DB needed ---
    if 'IV_Now' in df.columns and 'HV_20D' in df.columns:
        iv_now  = pd.to_numeric(df['IV_Now'],  errors='coerce')
        hv_20d  = pd.to_numeric(df['HV_20D'],  errors='coerce')
        df['IV_vs_HV_Gap'] = np.where(
            df.get('AssetType', pd.Series('', index=df.index)) == 'STOCK',
            np.nan,
            iv_now - hv_20d
        )
    else:
        df['IV_vs_HV_Gap'] = np.nan

    # --- IV_Percentile: requires history from management_recommendations ---
    df['IV_Percentile'] = np.nan

    try:
        from pathlib import Path
        from core.shared.data_contracts.config import PIPELINE_DB_PATH
        from core.shared.data_layer.duckdb_utils import _table_exists
        import duckdb

        _pipeline_db = str(PIPELINE_DB_PATH)
        if not Path(_pipeline_db).exists():
            return df

        trade_ids = df.loc[
            df.get('AssetType', pd.Series('', index=df.index)) != 'STOCK',
            'TradeID'
        ].dropna().unique().tolist()

        if not trade_ids:
            return df

        with duckdb.connect(_pipeline_db, read_only=True) as con:
            if not _table_exists(con, 'management_recommendations'):
                return df

            placeholders = ', '.join(['?' for _ in trade_ids])
            hist = con.execute(f"""
                WITH daily AS (
                    -- One deduplicated IV per TradeID per calendar day
                    SELECT TradeID, IV_Now, Snapshot_TS::DATE AS snap_date
                    FROM management_recommendations
                    WHERE TradeID IN ({placeholders})
                      AND IV_Now IS NOT NULL
                      AND AssetType = 'OPTION'
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY TradeID, Snapshot_TS::DATE
                        ORDER BY Snapshot_TS DESC
                    ) = 1
                )
                SELECT
                    TradeID,
                    COUNT(*)       AS n_days,
                    MIN(IV_Now)    AS iv_min,
                    MAX(IV_Now)    AS iv_max,
                    -- Pass all IV values as an array for percentile calc in Python
                    LIST(IV_Now ORDER BY snap_date) AS iv_history
                FROM daily
                GROUP BY TradeID
                HAVING COUNT(*) >= 3
            """, trade_ids).df()

        if hist.empty:
            return df

        # Compute percentile rank for current IV_Now against history
        current_iv_map = df.set_index('TradeID')['IV_Now'].to_dict()

        pct_map: dict = {}
        for _, row in hist.iterrows():
            tid      = row['TradeID']
            iv_hist  = row['iv_history']   # list of floats
            today_iv = current_iv_map.get(tid)
            if today_iv is None or np.isnan(float(today_iv)):
                continue
            today_iv = float(today_iv)
            n_below  = sum(1 for v in iv_hist if float(v) < today_iv)
            pct_map[str(tid)] = round(n_below / len(iv_hist) * 100, 1)

        df['IV_Percentile'] = df['TradeID'].map(
            lambda t: pct_map.get(str(t), np.nan)
        )
        # Stocks always NaN
        if 'AssetType' in df.columns:
            df.loc[df['AssetType'] == 'STOCK', 'IV_Percentile'] = np.nan

        if not MANAGEMENT_SAFE_MODE:
            _n_pct = df['IV_Percentile'].notna().sum()
            logger.info(f"IV_Percentile computed for {_n_pct} positions "
                        f"(IV_vs_HV_Gap: {df['IV_vs_HV_Gap'].notna().sum()} populated)")

    except Exception as e:
        logger.warning(f"IV_vs_HV_Gap / IV_Percentile computation failed (non-fatal): {e}")

    return df
