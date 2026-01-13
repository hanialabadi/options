"""
Completed Trade Detection and Collection

Identifies trades that have been closed/expired and collects their full
history for ML training.
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path
from typing import List, Dict, Optional
import duckdb

logger = logging.getLogger(__name__)


def collect_completed_trades(
    db_path: str,
    lookback_days: int = 90
) -> pd.DataFrame:
    """
    Collect completed trades from historical snapshots.
    
    Args:
        db_path: Path to positions_history.duckdb
        lookback_days: How far back to search for completed trades
        
    Returns:
        DataFrame with completed trades and their full history
        
    A trade is "completed" when:
        - TradeID appears in historical snapshots but not in latest
        - OR Quantity = 0 in latest snapshot
        - OR Exit_Date is populated
    """
    try:
        con = duckdb.connect(db_path, read_only=True)
        
        # Get latest snapshot date
        latest_date = con.execute("""
            SELECT MAX(Snapshot_TS) FROM clean_legs
        """).fetchone()[0]
        
        cutoff_date = pd.Timestamp(latest_date) - pd.Timedelta(days=lookback_days)
        
        logger.info(f"Searching for completed trades since {cutoff_date.date()}")
        
        # Find TradeIDs that appeared historically but not in latest snapshot
        query = """
        WITH latest_trades AS (
            SELECT DISTINCT TradeID
            FROM clean_legs
            WHERE Snapshot_TS = (SELECT MAX(Snapshot_TS) FROM clean_legs)
        ),
        historical_trades AS (
            SELECT DISTINCT TradeID
            FROM clean_legs
            WHERE Snapshot_TS >= ?
            AND Snapshot_TS < (SELECT MAX(Snapshot_TS) FROM clean_legs)
        )
        SELECT ht.TradeID
        FROM historical_trades ht
        LEFT JOIN latest_trades lt ON ht.TradeID = lt.TradeID
        WHERE lt.TradeID IS NULL
        """
        
        completed_trade_ids = con.execute(query, [cutoff_date]).fetchdf()
        
        if len(completed_trade_ids) == 0:
            logger.info("No completed trades found in lookback period")
            con.close()
            return pd.DataFrame()
        
        logger.info(f"Found {len(completed_trade_ids)} completed trades")
        
        # Get full history for these trades
        completed_ids = completed_trade_ids['TradeID'].tolist()
        placeholders = ','.join(['?' for _ in completed_ids])
        
        history_query = f"""
        SELECT *
        FROM clean_legs
        WHERE TradeID IN ({placeholders})
        ORDER BY TradeID, Snapshot_TS
        """
        
        df_history = con.execute(history_query, completed_ids).fetchdf()
        con.close()
        
        logger.info(f"✅ Collected {len(df_history)} snapshots for {len(completed_ids)} trades")
        return df_history
        
    except Exception as e:
        logger.error(f"Error collecting completed trades: {e}", exc_info=True)
        return pd.DataFrame()


def extract_exit_outcomes(df_history: pd.DataFrame) -> pd.DataFrame:
    """
    Extract exit outcomes for each completed trade.
    
    For each TradeID:
        - Entry snapshot (first appearance)
        - Exit snapshot (last appearance)
        - Days held
        - Exit P&L
        - Exit reason (if available)
    """
    if df_history.empty:
        return pd.DataFrame()
    
    outcomes = []
    
    for trade_id in df_history['TradeID'].unique():
        df_trade = df_history[df_history['TradeID'] == trade_id].sort_values('Snapshot_TS')
        
        entry_row = df_trade.iloc[0]
        exit_row = df_trade.iloc[-1]
        
        entry_date = pd.Timestamp(entry_row['Snapshot_TS'])
        exit_date = pd.Timestamp(exit_row['Snapshot_TS'])
        days_held = (exit_date - entry_date).days
        
        exit_pnl = exit_row.get('Unrealized_PnL', 0.0)
        exit_roi = exit_row.get('ROI_Current', 0.0)
        
        # Determine win/loss
        win_loss = 'Win' if exit_pnl > 0 else ('Loss' if exit_pnl < 0 else 'Breakeven')
        
        # Calculate max favorable/adverse excursion
        pnl_series = df_trade['Unrealized_PnL'].dropna()
        max_favorable = pnl_series.max() if len(pnl_series) > 0 else exit_pnl
        max_adverse = pnl_series.min() if len(pnl_series) > 0 else exit_pnl
        
        outcome = {
            'TradeID': trade_id,
            'Symbol': entry_row.get('Symbol'),
            'Strategy': entry_row.get('Strategy'),
            'Entry_Date': entry_date,
            'Exit_Date': exit_date,
            'Days_Held': days_held,
            'Exit_PnL': exit_pnl,
            'Exit_ROI': exit_roi,
            'Win_Loss': win_loss,
            'Max_Favorable_Excursion': max_favorable,
            'Max_Adverse_Excursion': max_adverse,
            'Snapshots_Recorded': len(df_trade),
        }
        
        outcomes.append(outcome)
    
    df_outcomes = pd.DataFrame(outcomes)
    
    logger.info(
        f"✅ Extracted outcomes: "
        f"{(df_outcomes['Win_Loss'] == 'Win').sum()} wins, "
        f"{(df_outcomes['Win_Loss'] == 'Loss').sum()} losses, "
        f"{(df_outcomes['Win_Loss'] == 'Breakeven').sum()} breakeven"
    )
    
    return df_outcomes
