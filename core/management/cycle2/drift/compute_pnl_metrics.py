"""
Phase 3 Enrichment: P&L and Performance Metrics

Computes unrealized P&L, days in trade, ROI, and max profit/loss for positions.
"""

import numpy as np
import pandas as pd
import logging
from datetime import datetime

from core.management.cycle1.identity.constants import (
    STRATEGY_LONG_STRADDLE,
    STRATEGY_LONG_STRANGLE,
    STRATEGY_BUY_PUT,
    STRATEGY_BUY_CALL,
    STRATEGY_COVERED_CALL,
    STRATEGY_CSP,
    ASSET_TYPE_OPTION,
    ASSET_TYPE_STOCK,
    OPTIONS_CONTRACT_MULTIPLIER,
)

logger = logging.getLogger(__name__)


def compute_pnl_metrics(df: pd.DataFrame, snapshot_ts: pd.Timestamp = None) -> pd.DataFrame:
    """
    Compute P&L and performance metrics for all positions.
    """
    if df.empty:
        logger.warning("⚠️ Empty DataFrame in compute_pnl_metrics")
        return df
    
    # Required columns
    required_cols = ['Premium', 'Last', 'Quantity', 'AssetType']
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        logger.error(f"❌ Missing required columns for P&L: {missing}")
        return df
    
    if snapshot_ts is None:
        snapshot_ts = pd.Timestamp.now()
    
    logger.info("Computing P&L and performance metrics...")
    
    # Initialize columns
    df['Unrealized_PnL'] = 0.0
    df['Days_In_Trade'] = 0
    df['ROI_Current'] = 0.0
    df['ROI'] = 0.0  # Alias for audit compatibility
    df['Max_Profit_Potential'] = np.nan
    df['Max_Loss_Potential'] = np.nan
    df['Profit_Target_Pct'] = 0.0
    df['Loss_Distance_Pct'] = 0.0
    
    # 1. Compute Unrealized P&L (per leg)
    # RAG Authority: Hull (Valuation Neutrality)
    # Fidelity Basis Doctrine: 'Basis' is Total Cost (positive for both Long and Short).
    # Unified Formula: PnL = (Current_Price * Qty * Multiplier) - (Basis * sign(Qty))
    # FIX: Prefer Basis_Entry (Frozen) over Basis (Broker/Drifted) to prevent PnL leakage.
    
    option_mask = df['AssetType'] == ASSET_TYPE_OPTION
    stock_mask = df['AssetType'] == ASSET_TYPE_STOCK
    
    if option_mask.any():
        # Prefer Basis_Entry if available and valid
        if 'Basis_Entry' in df.columns and df.loc[option_mask, 'Basis_Entry'].notna().any():
            df.loc[option_mask, 'Unrealized_PnL'] = (
                (df.loc[option_mask, 'Last'] * df.loc[option_mask, 'Quantity'] * OPTIONS_CONTRACT_MULTIPLIER) - 
                (df.loc[option_mask, 'Basis_Entry'] * np.sign(df.loc[option_mask, 'Quantity']))
            )
        elif 'Basis' in df.columns:
            df.loc[option_mask, 'Unrealized_PnL'] = (
                (df.loc[option_mask, 'Last'] * df.loc[option_mask, 'Quantity'] * OPTIONS_CONTRACT_MULTIPLIER) - 
                (df.loc[option_mask, 'Basis'] * np.sign(df.loc[option_mask, 'Quantity']))
            )
        else:
            # Fallback using Premium (assumed to be Entry Price per contract)
            df.loc[option_mask, 'Unrealized_PnL'] = (
                (df.loc[option_mask, 'Last'] - df.loc[option_mask, 'Premium'].abs()) *
                df.loc[option_mask, 'Quantity'] *
                OPTIONS_CONTRACT_MULTIPLIER
            )
    
    if stock_mask.any():
        # FIX: Use Basis_Entry for stocks to prevent the $16k PLTR-style leakage
        if 'Basis_Entry' in df.columns and df.loc[stock_mask, 'Basis_Entry'].notna().any():
            df.loc[stock_mask, 'Unrealized_PnL'] = (
                (df.loc[stock_mask, 'Last'] * df.loc[stock_mask, 'Quantity']) - 
                (df.loc[stock_mask, 'Basis_Entry'] * np.sign(df.loc[stock_mask, 'Quantity']))
            )
        else:
            basis_col = 'Basis' if 'Basis' in df.columns else 'Premium'
            df.loc[stock_mask, 'Unrealized_PnL'] = (
                (df.loc[stock_mask, 'Last'] * df.loc[stock_mask, 'Quantity']) - 
                (df.loc[stock_mask, basis_col] * np.sign(df.loc[stock_mask, 'Quantity']))
            )
    
    # 2. Compute Days in Trade
    # Prefer Entry_Snapshot_TS (from entry_anchors) → First_Seen_Date → fallback 0
    if 'Entry_Snapshot_TS' in df.columns and 'Snapshot_TS' in df.columns:
        try:
            ts_current = pd.to_datetime(df['Snapshot_TS'], errors='coerce')
            ts_entry = pd.to_datetime(df['Entry_Snapshot_TS'], errors='coerce')
            days = (ts_current.dt.normalize() - ts_entry.dt.normalize()).dt.days
            df['Days_In_Trade'] = days.fillna(0).clip(lower=0).astype(int)
        except Exception as e:
            logger.warning(f"⚠️ Could not compute Days_In_Trade from Entry_Snapshot_TS: {e}")
            df['Days_In_Trade'] = 0
    elif 'First_Seen_Date' in df.columns:
        try:
            df['First_Seen_Date_parsed'] = pd.to_datetime(df['First_Seen_Date'], errors='coerce')
            df['Days_In_Trade'] = (snapshot_ts - df['First_Seen_Date_parsed']).dt.days.fillna(0).clip(lower=0).astype(int)
            df = df.drop(columns=['First_Seen_Date_parsed'])
        except Exception as e:
            logger.warning(f"⚠️ Could not compute Days_In_Trade: {e}")
            df['Days_In_Trade'] = 0
    else:
        df['Days_In_Trade'] = 0
    
    # 3. Compute ROI
    if 'Capital_Deployed' in df.columns:
        capital_mask = df['Capital_Deployed'].abs() > 0.01
        df.loc[capital_mask, 'ROI_Current'] = (
            df.loc[capital_mask, 'Unrealized_PnL'] / 
            df.loc[capital_mask, 'Capital_Deployed'].abs() * 100
        )
    
    # 4. Compute Max Profit/Loss
    if 'TradeID' in df.columns and 'Strategy' in df.columns:
        df = _compute_max_profit_loss_by_strategy(df)
    
    # 5. Compute profit target % and loss distance %
    max_profit_mask = df['Max_Profit_Potential'].notna() & (df['Max_Profit_Potential'] > 0)
    df.loc[max_profit_mask, 'Profit_Target_Pct'] = (
        df.loc[max_profit_mask, 'Unrealized_PnL'] / 
        df.loc[max_profit_mask, 'Max_Profit_Potential'] * 100
    ).clip(lower=0)
    
    max_loss_mask = df['Max_Loss_Potential'].notna() & (df['Max_Loss_Potential'] < 0)
    df.loc[max_loss_mask, 'Loss_Distance_Pct'] = (
        df.loc[max_loss_mask, 'Unrealized_PnL'] / 
        df.loc[max_loss_mask, 'Max_Loss_Potential'] * 100
    ).clip(lower=0, upper=100)
    
    return df


def _compute_max_profit_loss_by_strategy(df: pd.DataFrame) -> pd.DataFrame:
    def calculate_trade_max_profit_loss(trade_df):
        strategy = trade_df['Strategy'].iloc[0] if 'Strategy' in trade_df.columns else 'Unknown'
        
        if strategy == STRATEGY_BUY_CALL or strategy == STRATEGY_BUY_PUT:
            premium_paid = (trade_df['Premium'] * trade_df['Quantity'] * OPTIONS_CONTRACT_MULTIPLIER).sum()
            trade_df['Max_Loss_Potential'] = -abs(premium_paid)
            trade_df['Max_Profit_Potential'] = np.inf
            
        elif strategy == STRATEGY_COVERED_CALL:
            call_legs = trade_df[trade_df['AssetType'] == ASSET_TYPE_OPTION]
            stock_legs = trade_df[trade_df['AssetType'] == ASSET_TYPE_STOCK]
            if not call_legs.empty and not stock_legs.empty:
                strike = call_legs['Strike'].iloc[0]
                stock_basis = stock_legs['Basis'].iloc[0] if 'Basis' in stock_legs.columns else stock_legs['Premium'].iloc[0]
                premium_received = abs(call_legs['Premium'].iloc[0] * OPTIONS_CONTRACT_MULTIPLIER)
                trade_df['Max_Profit_Potential'] = (strike - stock_basis) * 100 + premium_received
                trade_df['Max_Loss_Potential'] = -(stock_basis * 100 - premium_received)
                
        elif strategy == STRATEGY_CSP:
            if not trade_df.empty:
                strike = trade_df['Strike'].iloc[0]
                premium = abs(trade_df['Premium'].iloc[0])
                trade_df['Max_Profit_Potential'] = premium * OPTIONS_CONTRACT_MULTIPLIER
                trade_df['Max_Loss_Potential'] = -((strike - premium) * OPTIONS_CONTRACT_MULTIPLIER)
                
        elif strategy in [STRATEGY_LONG_STRADDLE, STRATEGY_LONG_STRANGLE]:
            premium_paid = (trade_df['Premium'] * trade_df['Quantity'].abs() * OPTIONS_CONTRACT_MULTIPLIER).sum()
            trade_df['Max_Loss_Potential'] = -abs(premium_paid)
            trade_df['Max_Profit_Potential'] = np.inf
            
        elif 'Credit' in strategy or 'Iron Condor' in strategy:
            if 'Capital_Deployed' in trade_df.columns:
                capital = trade_df['Capital_Deployed'].abs().sum()
                premium_credit = (trade_df['Premium'] * trade_df['Quantity'] * OPTIONS_CONTRACT_MULTIPLIER).sum()
                trade_df['Max_Profit_Potential'] = abs(premium_credit)
                trade_df['Max_Loss_Potential'] = -(capital - abs(premium_credit))
        
        else:
            if 'Capital_Deployed' in trade_df.columns:
                trade_df['Max_Loss_Potential'] = -trade_df['Capital_Deployed'].abs()
        
        return trade_df
    
    df = df.groupby('TradeID', group_keys=False).apply(calculate_trade_max_profit_loss)
    df['Max_Profit_Potential'] = df['Max_Profit_Potential'].replace([np.inf, -np.inf], np.nan)
    return df


def aggregate_trade_pnl(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate P&L metrics at the trade level.
    """
    if 'TradeID' not in df.columns:
        return df
    
    agg_dict = {
        'Unrealized_PnL': 'sum',
        'Days_In_Trade': 'max',
    }
    if 'Capital_Deployed' in df.columns:
        agg_dict['Capital_Deployed'] = lambda x: x.abs().sum()
        
    trade_agg = df.groupby('TradeID').agg(agg_dict).reset_index()
    
    rename_dict = {
        'Unrealized_PnL': 'Unrealized_PnL_Trade',
        'Days_In_Trade': 'Days_In_Trade_Max',
    }
    if 'Capital_Deployed' in trade_agg.columns:
        rename_dict['Capital_Deployed'] = 'Capital_Deployed_Trade'
        
    trade_agg = trade_agg.rename(columns=rename_dict)
    
    if 'Capital_Deployed_Trade' in trade_agg.columns:
        capital_mask = trade_agg['Capital_Deployed_Trade'] > 0.01
        trade_agg.loc[capital_mask, 'ROI_Trade'] = (
            trade_agg.loc[capital_mask, 'Unrealized_PnL_Trade'] / 
            trade_agg.loc[capital_mask, 'Capital_Deployed_Trade'] * 100
        )
    
    df = df.merge(trade_agg, on='TradeID', how='left')
    return df
