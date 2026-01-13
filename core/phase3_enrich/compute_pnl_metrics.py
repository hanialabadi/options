"""
Phase 3 Enrichment: P&L and Performance Metrics

Computes unrealized P&L, days in trade, ROI, and max profit/loss for positions.

Design Principles:
1. Unrealized P&L = (Current Market Value - Entry Cost) per position
2. Days in Trade = Time since first observation (First_Seen_Date)
3. ROI = Unrealized P&L / Capital at Risk * 100
4. Max Profit/Loss = Strategy-specific risk/reward profiles
5. Trade-level aggregation for multi-leg strategies
"""

import numpy as np
import pandas as pd
import logging
from datetime import datetime

from core.phase3_constants import (
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
    
    Adds columns:
        - Unrealized_PnL: Current profit/loss in dollars
        - Days_In_Trade: Days since first observation
        - ROI_Current: Return on investment percentage
        - Max_Profit_Potential: Maximum possible profit (strategy-specific)
        - Max_Loss_Potential: Maximum possible loss (strategy-specific)
        - Profit_Target_Pct: % of max profit currently captured
        - Loss_Distance_Pct: % distance to max loss
        
    Args:
        df: DataFrame with positions (must have Premium, Last, Quantity, First_Seen_Date, Capital_Deployed)
        snapshot_ts: Current timestamp (defaults to now)
        
    Returns:
        DataFrame with P&L metrics added
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
    df['Theta_Efficiency'] = 0.0
    df['Assignment_Risk'] = 'LOW'  # Will be updated later
    df['Max_Profit_Potential'] = np.nan
    df['Max_Loss_Potential'] = np.nan
    df['Profit_Target_Pct'] = 0.0
    df['Loss_Distance_Pct'] = 0.0
    
    # ========================================================================
    # 1. Compute Unrealized P&L (per leg)
    # ========================================================================
    # For options: (Last - Premium) * Quantity * 100
    # For stocks: (Last - Basis) * Quantity
    
    option_mask = df['AssetType'] == ASSET_TYPE_OPTION
    stock_mask = df['AssetType'] == ASSET_TYPE_STOCK
    
    # Options P&L
    if option_mask.any():
        df.loc[option_mask, 'Unrealized_PnL'] = (
            (df.loc[option_mask, 'Last'] - df.loc[option_mask, 'Premium']) *
            df.loc[option_mask, 'Quantity'] *
            OPTIONS_CONTRACT_MULTIPLIER
        )
    
    # Stock P&L (use Basis if available, else use Premium as entry price)
    if stock_mask.any():
        basis_col = 'Basis' if 'Basis' in df.columns else 'Premium'
        df.loc[stock_mask, 'Unrealized_PnL'] = (
            (df.loc[stock_mask, 'Last'] - df.loc[stock_mask, basis_col]) *
            df.loc[stock_mask, 'Quantity']
        )
    
    # ========================================================================
    # 2. Compute Days in Trade
    # ========================================================================
    if 'First_Seen_Date' in df.columns:
        try:
            df['First_Seen_Date_parsed'] = pd.to_datetime(df['First_Seen_Date'], errors='coerce')
            df['Days_In_Trade'] = (snapshot_ts - df['First_Seen_Date_parsed']).dt.days
            df['Days_In_Trade'] = df['Days_In_Trade'].fillna(0).astype(int)
            df = df.drop(columns=['First_Seen_Date_parsed'])
        except Exception as e:
            logger.warning(f"⚠️ Could not compute Days_In_Trade: {e}")
            df['Days_In_Trade'] = 0
    else:
        logger.warning("⚠️ First_Seen_Date not available, Days_In_Trade set to 0")
        df['Days_In_Trade'] = 0
    
    # ========================================================================
    # 3. Compute ROI (per leg, then aggregate at trade level)
    # ========================================================================
    if 'Capital_Deployed' in df.columns:
        # ROI = (Unrealized P&L / Capital at Risk) * 100
        # Avoid division by zero
        capital_mask = df['Capital_Deployed'].abs() > 0.01
        df.loc[capital_mask, 'ROI_Current'] = (
            df.loc[capital_mask, 'Unrealized_PnL'] / 
            df.loc[capital_mask, 'Capital_Deployed'].abs() * 100
        )
    else:
        logger.warning("⚠️ Capital_Deployed not available, ROI_Current set to 0")
    
    # ========================================================================
    # 4. Compute Max Profit/Loss (strategy-specific)
    # ========================================================================
    if 'TradeID' in df.columns and 'Strategy' in df.columns:
        df = _compute_max_profit_loss_by_strategy(df)
    else:
        logger.warning("⚠️ TradeID or Strategy not available, skipping max profit/loss")
    
    # ========================================================================
    # 5. Compute profit target % and loss distance %
    # ========================================================================
    # Profit target: How much of max profit have we captured?
    max_profit_mask = df['Max_Profit_Potential'].notna() & (df['Max_Profit_Potential'] > 0)
    df.loc[max_profit_mask, 'Profit_Target_Pct'] = (
        df.loc[max_profit_mask, 'Unrealized_PnL'] / 
        df.loc[max_profit_mask, 'Max_Profit_Potential'] * 100
    ).clip(lower=0)  # Can't be negative
    
    # Loss distance: How close are we to max loss?
    max_loss_mask = df['Max_Loss_Potential'].notna() & (df['Max_Loss_Potential'] < 0)
    df.loc[max_loss_mask, 'Loss_Distance_Pct'] = (
        df.loc[max_loss_mask, 'Unrealized_PnL'] / 
        df.loc[max_loss_mask, 'Max_Loss_Potential'] * 100
    ).clip(lower=0, upper=100)  # 0-100%
    
    # Log summary
    total_pnl = df['Unrealized_PnL'].sum()
    avg_days = df['Days_In_Trade'].mean()
    logger.info(f"✅ P&L metrics computed:")
    logger.info(f"   Total Unrealized P&L: ${total_pnl:,.2f}")
    logger.info(f"   Average Days in Trade: {avg_days:.1f}")
    logger.info(f"   Positions with profit: {(df['Unrealized_PnL'] > 0).sum()}")
    logger.info(f"   Positions with loss: {(df['Unrealized_PnL'] < 0).sum()}")
    
    return df


def _compute_max_profit_loss_by_strategy(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute max profit and max loss for each strategy type.
    
    Strategy-specific calculations based on Natenberg/Passarelli:
    - Long Call/Put: Max Loss = Premium paid, Max Profit = Unlimited
    - Short Call/Put: Max Profit = Premium received, Max Loss = Unlimited
    - Covered Call: Max Profit = Strike - Stock Basis + Premium, Max Loss = Stock Basis - Premium
    - CSP: Max Profit = Premium, Max Loss = Strike - Premium
    - Straddle/Strangle (Long): Max Loss = Total Premium, Max Profit = Unlimited
    - Credit Spreads: Max Profit = Credit, Max Loss = Width - Credit
    """
    
    def calculate_trade_max_profit_loss(trade_df):
        """Calculate max profit/loss for a single trade."""
        strategy = trade_df['Strategy'].iloc[0] if 'Strategy' in trade_df.columns else 'Unknown'
        
        # Single-leg strategies
        if strategy == STRATEGY_BUY_CALL or strategy == STRATEGY_BUY_PUT:
            # Long options: Max loss = premium paid
            premium_paid = (trade_df['Premium'] * trade_df['Quantity'] * OPTIONS_CONTRACT_MULTIPLIER).sum()
            trade_df['Max_Loss_Potential'] = -abs(premium_paid)
            trade_df['Max_Profit_Potential'] = np.inf  # Unlimited
            
        elif strategy == STRATEGY_COVERED_CALL:
            # Max profit = (Strike - Stock Basis) * 100 + Premium received
            # Max loss = Stock Basis * 100 - Premium received
            call_legs = trade_df[trade_df['AssetType'] == ASSET_TYPE_OPTION]
            stock_legs = trade_df[trade_df['AssetType'] == ASSET_TYPE_STOCK]
            
            if not call_legs.empty and not stock_legs.empty:
                strike = call_legs['Strike'].iloc[0]
                stock_basis = stock_legs['Basis'].iloc[0] if 'Basis' in stock_legs.columns else stock_legs['Premium'].iloc[0]
                premium_received = abs(call_legs['Premium'].iloc[0] * OPTIONS_CONTRACT_MULTIPLIER)
                
                max_profit = (strike - stock_basis) * 100 + premium_received
                max_loss = -(stock_basis * 100 - premium_received)
                
                trade_df['Max_Profit_Potential'] = max_profit
                trade_df['Max_Loss_Potential'] = max_loss
                
        elif strategy == STRATEGY_CSP:
            # Max profit = premium received
            # Max loss = (Strike - Premium) * 100
            if not trade_df.empty:
                strike = trade_df['Strike'].iloc[0]
                premium = abs(trade_df['Premium'].iloc[0])
                
                max_profit = premium * OPTIONS_CONTRACT_MULTIPLIER
                max_loss = -((strike - premium) * OPTIONS_CONTRACT_MULTIPLIER)
                
                trade_df['Max_Profit_Potential'] = max_profit
                trade_df['Max_Loss_Potential'] = max_loss
                
        elif strategy in [STRATEGY_LONG_STRADDLE, STRATEGY_LONG_STRANGLE]:
            # Long volatility: Max loss = total premium paid
            premium_paid = (trade_df['Premium'] * trade_df['Quantity'].abs() * OPTIONS_CONTRACT_MULTIPLIER).sum()
            trade_df['Max_Loss_Potential'] = -abs(premium_paid)
            trade_df['Max_Profit_Potential'] = np.inf  # Unlimited upside
            
        elif 'Credit' in strategy or 'Iron Condor' in strategy:
            # Credit spreads: Max profit = credit, Max loss = width - credit
            # Simplified: Use Capital_Deployed as max loss proxy
            if 'Capital_Deployed' in trade_df.columns:
                capital = trade_df['Capital_Deployed'].abs().sum()
                premium_credit = (trade_df['Premium'] * trade_df['Quantity'] * OPTIONS_CONTRACT_MULTIPLIER).sum()
                
                trade_df['Max_Profit_Potential'] = abs(premium_credit)
                trade_df['Max_Loss_Potential'] = -(capital - abs(premium_credit))
        
        else:
            # Unknown strategy: Use capital deployed as max loss proxy
            if 'Capital_Deployed' in trade_df.columns:
                trade_df['Max_Loss_Potential'] = -trade_df['Capital_Deployed'].abs()
            trade_df['Max_Profit_Potential'] = np.nan
        
        return trade_df
    
    # Apply per trade
    df = df.groupby('TradeID', group_keys=False).apply(calculate_trade_max_profit_loss)
    
    # Replace inf with NaN for display purposes
    df['Max_Profit_Potential'] = df['Max_Profit_Potential'].replace([np.inf, -np.inf], np.nan)
    
    return df


def aggregate_trade_pnl(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate P&L metrics at the trade level (for multi-leg strategies).
    
    Creates trade-level summary metrics:
        - Unrealized_PnL_Trade: Sum of all legs
        - ROI_Trade: Trade-level return on investment
        - Days_In_Trade_Max: Max days across all legs (most conservative)
        
    Returns:
        DataFrame with trade-level aggregates added
    """
    if 'TradeID' not in df.columns:
        logger.warning("⚠️ TradeID not available, skipping trade-level aggregation")
        return df
    
    # Compute trade-level aggregates
    trade_agg = df.groupby('TradeID').agg({
        'Unrealized_PnL': 'sum',
        'Days_In_Trade': 'max',  # Most conservative (oldest leg)
        'Capital_Deployed': lambda x: x.abs().sum() if 'Capital_Deployed' in df.columns else 0
    }).reset_index()
    
    trade_agg = trade_agg.rename(columns={
        'Unrealized_PnL': 'Unrealized_PnL_Trade',
        'Days_In_Trade': 'Days_In_Trade_Max',
        'Capital_Deployed': 'Capital_Deployed_Trade'
    })
    
    # Compute trade-level ROI
    capital_mask = trade_agg['Capital_Deployed_Trade'] > 0.01
    trade_agg.loc[capital_mask, 'ROI_Trade'] = (
        trade_agg.loc[capital_mask, 'Unrealized_PnL_Trade'] / 
        trade_agg.loc[capital_mask, 'Capital_Deployed_Trade'] * 100
    )
    trade_agg['ROI_Trade'] = trade_agg['ROI_Trade'].fillna(0)
    
    # Merge back to main dataframe
    df = df.merge(trade_agg, on='TradeID', how='left')
    
    logger.info(f"✅ Trade-level P&L aggregated for {len(trade_agg)} trades")
    
    return df
