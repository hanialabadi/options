"""
Phase 3: P&L Attribution

Decomposes realized and unrealized P&L into Greek contributions:
- PnL_From_Delta: Profit/loss from directional price movement
- PnL_From_Theta: Profit/loss from time decay
- PnL_From_Vega: Profit/loss from IV changes
- PnL_From_Gamma: Profit/loss from gamma scalping (second-order delta)

This attribution requires entry Greeks to be frozen (from freeze_entry_data).
Without entry Greeks, attribution cannot be computed.

Formula (simplified first-order approximation):
- Delta P&L = (Current_Price - Entry_Price) * Delta_Entry * 100 * Quantity
- Theta P&L = Theta_Entry * Days_In_Trade
- Vega P&L = (Current_IV - Entry_IV) * Vega_Entry * 100 * Quantity
- Gamma P&L = 0.5 * Gamma_Entry * (Price_Move^2) * 100 * Quantity

Note: These are approximations. Real P&L attribution requires integration
over the path (Greeks change as position evolves). But for short-dated
options (<90 DTE), first-order approximation is reasonable.

Author: System
Date: 2026-01-04
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def rehydrate_entry_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rehydrate frozen entry data from DuckDB for P&L attribution.
    """
    if df.empty:
        return df
        
    trade_ids = df['TradeID'].unique().tolist()
    workspace_root = Path(__file__).parent.parent.parent
    db_path = workspace_root / "data" / "pipeline.duckdb"
    
    if not db_path.exists():
        return df
        
    import duckdb
    try:
        with duckdb.connect(str(db_path)) as con:
            # Check if table exists
            table_exists = con.execute("""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = 'clean_legs' AND table_schema = 'main'
            """).fetchone()[0] > 0
            
            if not table_exists:
                return df

            # Get list of _Entry columns
            db_cols_info = con.execute("PRAGMA table_info('clean_legs')").fetchall()
            entry_cols = [row[1] for row in db_cols_info if row[1].endswith('_Entry') or row[1] == 'Entry_Timestamp']
            
            if not entry_cols:
                return df

            # Query latest entry data
            placeholders = ', '.join(['?' for _ in trade_ids])
            cols_str = ', '.join([f'"{c}"' for c in entry_cols])
            query = f"""
                SELECT TradeID, {cols_str}
                FROM clean_legs
                WHERE TradeID IN ({placeholders})
                AND Entry_Timestamp IS NOT NULL
                QUALIFY row_number() OVER (PARTITION BY TradeID ORDER BY Snapshot_TS DESC) = 1
            """
            df_existing = con.execute(query, trade_ids).df()
            
            if not df_existing.empty:
                # Merge into current df
                # Drop existing entry columns if they are all NaN
                for col in entry_cols:
                    if col in df.columns and df[col].isna().all():
                        df = df.drop(columns=[col])
                
                df = df.merge(df_existing, on='TradeID', how='left')
                logger.info(f"Rehydrated entry data for {len(df_existing)} trades for attribution")
                
    except Exception as e:
        logger.warning(f"Failed to rehydrate entry data: {e}")
        
    return df


def compute_pnl_attribution(df: pd.DataFrame) -> pd.DataFrame:
    """
    Decompose P&L into Greek contributions.
    
    Requires:
    - Entry Greeks (Delta_Entry, Theta_Entry, Vega_Entry, Gamma_Entry)
    - Entry context (Entry_Timestamp or Days_In_Trade, Entry IV)
    - Current Greeks and prices
    
    Adds columns:
    - PnL_From_Delta: Directional P&L
    - PnL_From_Theta: Time decay P&L
    - PnL_From_Vega: IV change P&L
    - PnL_From_Gamma: Convexity P&L
    - PnL_Unexplained: Residual (real P&L - attributed P&L)
    - PnL_Attribution_Quality: % of P&L explained by Greeks
    
    Args:
        df: DataFrame with positions and entry Greeks
        
    Returns:
        DataFrame with P&L attribution columns
        
    Notes:
        - Only computes for positions with entry Greeks frozen
        - Stocks have simplified attribution (pure delta)
        - Multi-leg positions: attribution per leg, then aggregate
    """
    if df.empty:
        return df
    
    df = df.copy()
    
    # Rehydrate entry data if missing
    required_entry_cols = ['Delta_Entry', 'Theta_Entry', 'Vega_Entry', 'Gamma_Entry']
    if any(col not in df.columns or df[col].isna().all() for col in required_entry_cols):
        df = rehydrate_entry_data(df)

    # Initialize attribution columns
    attribution_cols = [
        'PnL_From_Delta',
        'PnL_From_Theta',
        'PnL_From_Vega',
        'PnL_From_Gamma',
        'PnL_Unexplained',
        'PnL_Attribution_Quality'
    ]
    
    for col in attribution_cols:
        if col not in df.columns:
            df[col] = 0.0
    
    # Check if entry Greeks are available
    required_entry_cols = ['Delta_Entry', 'Theta_Entry', 'Vega_Entry', 'Gamma_Entry']
    missing_cols = [col for col in required_entry_cols if col not in df.columns]
    
    if missing_cols:
        # Downgrade to DEBUG to avoid repeated warnings for legacy trades
        logger.debug(f"P&L attribution skipped - missing entry Greeks: {missing_cols}")
        return df
    
    # Only compute for positions with entry Greeks frozen (not NaN)
    has_entry_greeks = df['Delta_Entry'].notna()
    num_with_entry = has_entry_greeks.sum()
    
    if num_with_entry == 0:
        logger.debug("No positions with entry Greeks frozen, skipping P&L attribution")
        return df
    
    logger.debug(f"Entry Greeks present for attribution ({num_with_entry} positions)")
    
    # Compute attribution by asset type
    df = _compute_options_attribution(df, has_entry_greeks)
    df = _compute_stocks_attribution(df, has_entry_greeks)
    
    # Compute quality metric (how much P&L is explained)
    df = _compute_attribution_quality(df, has_entry_greeks)
    
    _log_attribution_summary(df, has_entry_greeks)
    
    return df


def _compute_options_attribution(df: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
    """
    Compute P&L attribution for options positions.
    
    Uses first-order Greek approximations:
    - Delta: Linear price sensitivity
    - Theta: Time decay
    - Vega: Volatility sensitivity
    - Gamma: Convexity (second-order delta effect)
    """
    df = df.copy()
    
    options_mask = mask & (df['AssetType'] == 'OPTION')
    
    for idx in df[options_mask].index:
        # Get entry and current values
        quantity = df.at[idx, 'Quantity']
        delta_entry = df.at[idx, 'Delta_Entry']
        theta_entry = df.at[idx, 'Theta_Entry']
        vega_entry = df.at[idx, 'Vega_Entry']
        gamma_entry = df.at[idx, 'Gamma_Entry']
        
        ul_price_entry = df.at[idx, 'Underlying_Price_Entry']
        ul_price_current = df.at[idx, 'UL Last']
        
        days_in_trade = df.at[idx, 'Days_In_Trade']
        
        # IV (if available)
        iv_entry = df.at[idx, 'IV_Entry'] if 'IV_Entry' in df.columns else np.nan
        iv_current = df.at[idx, 'IV Mid'] if 'IV Mid' in df.columns else np.nan
        
        # Skip if critical data missing
        if pd.isna(delta_entry) or pd.isna(ul_price_entry) or pd.isna(ul_price_current):
            continue
        
        # 1. Delta P&L (directional)
        price_move = ul_price_current - ul_price_entry
        # Delta P&L = price_move * delta * multiplier * quantity
        # For options: 100 shares per contract
        # For short positions (quantity < 0), delta P&L is inverted
        pnl_delta = price_move * delta_entry * 100 * quantity
        df.at[idx, 'PnL_From_Delta'] = pnl_delta
        
        # 2. Theta P&L (time decay)
        if pd.notna(theta_entry) and pd.notna(days_in_trade):
            # Theta is per-day decay
            # For short positions, theta is positive (time helps us)
            pnl_theta = theta_entry * days_in_trade * 100 * quantity
            df.at[idx, 'PnL_From_Theta'] = pnl_theta
        
        # 3. Vega P&L (IV change)
        if pd.notna(vega_entry) and pd.notna(iv_entry) and pd.notna(iv_current):
            iv_change = iv_current - iv_entry
            # Vega is P&L change per 1-point IV move
            # IV is in percentage points (e.g., 0.30 = 30%)
            pnl_vega = iv_change * vega_entry * 100 * quantity
            df.at[idx, 'PnL_From_Vega'] = pnl_vega
        
        # 4. Gamma P&L (convexity)
        if pd.notna(gamma_entry):
            # Gamma P&L = 0.5 * gamma * (price_move^2) * multiplier * quantity
            # This captures the convexity benefit (or cost) from large moves
            pnl_gamma = 0.5 * gamma_entry * (price_move ** 2) * 100 * quantity
            df.at[idx, 'PnL_From_Gamma'] = pnl_gamma
    
    return df


def _compute_stocks_attribution(df: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
    """
    Compute P&L attribution for stock positions (pure delta).
    
    Stocks have:
    - Delta = 1.0 (or -1.0 for short)
    - Gamma, Vega, Theta = 0
    """
    df = df.copy()
    
    stocks_mask = mask & (df['AssetType'] == 'STOCK')
    
    for idx in df[stocks_mask].index:
        quantity = df.at[idx, 'Quantity']
        ul_price_entry = df.at[idx, 'Underlying_Price_Entry']
        ul_price_current = df.at[idx, 'UL Last']
        
        if pd.isna(ul_price_entry) or pd.isna(ul_price_current):
            continue
        
        # Stock P&L is pure directional
        price_move = ul_price_current - ul_price_entry
        pnl_delta = price_move * quantity  # No 100x multiplier for stocks
        
        df.at[idx, 'PnL_From_Delta'] = pnl_delta
        df.at[idx, 'PnL_From_Theta'] = 0.0
        df.at[idx, 'PnL_From_Vega'] = 0.0
        df.at[idx, 'PnL_From_Gamma'] = 0.0
    
    return df


def _compute_attribution_quality(df: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
    """
    Compute how much of the actual P&L is explained by Greek attribution.
    
    Quality = (Sum of attributed P&L) / (Actual Unrealized P&L) * 100
    
    High quality (>90%): Greeks explain most of the P&L
    Medium quality (60-90%): Some unexplained P&L (maybe bid-ask, slippage)
    Low quality (<60%): Significant unexplained P&L (model risk)
    """
    df = df.copy()
    
    for idx in df[mask].index:
        actual_pnl = df.at[idx, 'Unrealized_PnL'] if 'Unrealized_PnL' in df.columns else 0.0
        
        # Sum attributed P&L
        pnl_delta = df.at[idx, 'PnL_From_Delta']
        pnl_theta = df.at[idx, 'PnL_From_Theta']
        pnl_vega = df.at[idx, 'PnL_From_Vega']
        pnl_gamma = df.at[idx, 'PnL_From_Gamma']
        
        attributed_pnl = pnl_delta + pnl_theta + pnl_vega + pnl_gamma
        unexplained_pnl = actual_pnl - attributed_pnl
        
        df.at[idx, 'PnL_Unexplained'] = unexplained_pnl
        
        # Quality percentage
        if abs(actual_pnl) > 0.01:  # Avoid division by zero for tiny P&Ls
            quality = (attributed_pnl / actual_pnl) * 100
            df.at[idx, 'PnL_Attribution_Quality'] = quality
        else:
            df.at[idx, 'PnL_Attribution_Quality'] = 100.0  # No P&L = perfect attribution
    
    return df


def _log_attribution_summary(df: pd.DataFrame, mask: pd.Series) -> None:
    """Log P&L attribution summary statistics."""
    
    positions_with_attribution = df[mask]
    
    if len(positions_with_attribution) == 0:
        return
    
    # Aggregate attribution
    total_delta_pnl = positions_with_attribution['PnL_From_Delta'].sum()
    total_theta_pnl = positions_with_attribution['PnL_From_Theta'].sum()
    total_vega_pnl = positions_with_attribution['PnL_From_Vega'].sum()
    total_gamma_pnl = positions_with_attribution['PnL_From_Gamma'].sum()
    total_attributed = total_delta_pnl + total_theta_pnl + total_vega_pnl + total_gamma_pnl
    
    total_actual_pnl = positions_with_attribution['Unrealized_PnL'].sum() if 'Unrealized_PnL' in df.columns else 0.0
    unexplained = total_actual_pnl - total_attributed
    
    logger.info(f"✅ P&L Attribution Summary ({len(positions_with_attribution)} positions):")
    logger.info(f"   Total Actual P&L: ${total_actual_pnl:,.2f}")
    logger.info(f"   Attributed P&L: ${total_attributed:,.2f}")
    logger.info(f"     From Delta: ${total_delta_pnl:,.2f} ({total_delta_pnl/total_attributed*100 if total_attributed != 0 else 0:.1f}%)")
    logger.info(f"     From Theta: ${total_theta_pnl:,.2f} ({total_theta_pnl/total_attributed*100 if total_attributed != 0 else 0:.1f}%)")
    logger.info(f"     From Vega: ${total_vega_pnl:,.2f} ({total_vega_pnl/total_attributed*100 if total_attributed != 0 else 0:.1f}%)")
    logger.info(f"     From Gamma: ${total_gamma_pnl:,.2f} ({total_gamma_pnl/total_attributed*100 if total_attributed != 0 else 0:.1f}%)")
    logger.info(f"   Unexplained P&L: ${unexplained:,.2f}")
    
    if abs(total_actual_pnl) > 0.01:
        quality = (total_attributed / total_actual_pnl) * 100
        logger.info(f"   Attribution Quality: {quality:.1f}%")


def aggregate_trade_pnl_attribution(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate P&L attribution to trade level (for multi-leg strategies).
    
    Adds columns:
    - PnL_From_Delta_Trade: Total delta P&L for the trade
    - PnL_From_Theta_Trade: Total theta P&L for the trade
    - PnL_From_Vega_Trade: Total vega P&L for the trade
    - PnL_From_Gamma_Trade: Total gamma P&L for the trade
    
    Args:
        df: DataFrame with leg-level P&L attribution
        
    Returns:
        DataFrame with trade-level attribution aggregates
    """
    if df.empty or 'TradeID' not in df.columns:
        return df
    
    df = df.copy()
    
    attribution_cols = ['PnL_From_Delta', 'PnL_From_Theta', 'PnL_From_Vega', 'PnL_From_Gamma']
    trade_attribution_cols = [f"{col}_Trade" for col in attribution_cols]
    
    # Check if attribution columns exist
    missing = [col for col in attribution_cols if col not in df.columns]
    if missing:
        logger.warning(f"Cannot aggregate trade attribution - missing columns: {missing}")
        return df
    
    # Aggregate by TradeID
    for col, trade_col in zip(attribution_cols, trade_attribution_cols):
        df[trade_col] = df.groupby('TradeID')[col].transform('sum')
    
    return df
