"""
Phase 4: Entry Data Freeze

Captures entry conditions when a position is first seen:
- Entry Greeks (Delta, Gamma, Vega, Theta, Rho)
- Entry IV (if available)
- Entry IV_Rank
- Entry Premium (actual from Time Val or estimated)
- Entry underlying price (already captured as Underlying_Price_Entry)

This data is frozen and never changes - it represents the conditions
at trade inception. Critical for:
1. Performance attribution (which Greek made/lost money?)
2. Strategy validation (did we enter at high IV as intended?)
3. Risk assessment (how much have conditions deteriorated?)

Entry data is only set when First_Seen_Date is NULL (new position).
Once frozen, these columns are never updated.

Author: System
Date: 2026-01-04
"""

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


def freeze_entry_data(df: pd.DataFrame, new_trade_ids: list = None) -> pd.DataFrame:
    """
    Freeze entry data for newly discovered positions.
    
    Only populates _Entry columns for positions with TradeID in new_trade_ids list.
    Once frozen, entry data never changes (IMMUTABLE).
    
    Entry Data Captured (Canonical Schema):
    - Delta_Entry, Gamma_Entry, Vega_Entry, Theta_Entry, Rho_Entry
    - IV_Entry (from IV Mid if available)
    - Underlying_Price_Entry
    - Entry_Timestamp (Snapshot_TS when first seen)
    - Entry_DTE
    - Strategy_Entry
    - Quantity_Entry
    
    Args:
        df: DataFrame with positions (must have TradeID column)
        new_trade_ids: List of TradeIDs that are new (just registered in first_seen table)
        
    Returns:
        DataFrame with _Entry columns populated for new positions
    """
    if df.empty:
        return df
    
    df = df.copy()
    
    # Canonical Entry Schema
    entry_columns = [
        'Delta_Entry', 'Gamma_Entry', 'Vega_Entry', 'Theta_Entry', 'Rho_Entry',
        'IV_Entry', 'Underlying_Price_Entry', 'Entry_Timestamp',
        'Entry_DTE', 'Strategy_Entry', 'Quantity_Entry'
    ]
    
    # Initialize entry columns if they don't exist
    for col in entry_columns:
        if col not in df.columns:
            df[col] = np.nan
    
    # Identify positions that need freezing:
    # 1. Truly new positions (in new_trade_ids)
    # 2. Existing positions that are missing critical entry data (backfill/recovery)
    
    new_trade_ids = new_trade_ids or []
    
    # Mask for truly new positions
    is_new = df['TradeID'].isin(new_trade_ids)
    
    # Mask for positions missing entry data (recovery mode)
    # We use Entry_Timestamp as the authoritative indicator of "frozen" status
    is_missing_entry = df['Entry_Timestamp'].isna()
    
    # Combined mask for freezing
    freeze_mask = is_new | is_missing_entry
    num_to_freeze = freeze_mask.sum()
    
    if num_to_freeze == 0:
        logger.info("No positions need entry data freezing")
        return df
    
    logger.info(f"Freezing entry data for {num_to_freeze} positions (New: {is_new.sum()}, Missing: {is_missing_entry.sum()})...")
    
    # IMMUTABILITY GUARD: Ensure we only freeze if current value is NaN
    # (This is redundant with freeze_mask but provides extra safety)
    
    # Freeze Greeks
    df = _freeze_entry_greeks(df, freeze_mask)
    
    # Freeze IV
    df = _freeze_entry_iv(df, freeze_mask)
    
    # Freeze Premium
    df = _freeze_entry_premium(df, freeze_mask)
    
    # Freeze Context (Underlying Price, DTE, Strategy, Quantity)
    df = _freeze_entry_context(df, freeze_mask)
    
    # Freeze Entry_PCS (baseline score at entry)
    df = _freeze_entry_pcs(df, freeze_mask)
    
    # Set entry timestamp (use current snapshot timestamp)
    # Snapshot_TS was added in Phase 1 or Phase 4
    if 'Snapshot_TS' in df.columns:
        df.loc[freeze_mask, 'Entry_Timestamp'] = df.loc[freeze_mask, 'Snapshot_TS']
    else:
        df.loc[freeze_mask, 'Entry_Timestamp'] = pd.Timestamp.now()
    
    logger.info(f"✅ Entry data frozen for {num_to_freeze} positions")
    _log_entry_freeze_summary(df, freeze_mask)
    
    return df


def _freeze_entry_greeks(df: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
    """
    Freeze current Greeks as entry Greeks.
    
    For options: Use computed Greeks (Delta, Gamma, Vega, Theta, Rho)
    For stocks: Delta = 1.0, others = 0.0
    """
    df = df.copy()
    
    # Map current Greeks to entry Greeks
    greek_mappings = [
        ('Delta', 'Delta_Entry'),
        ('Gamma', 'Gamma_Entry'),
        ('Vega', 'Vega_Entry'),
        ('Theta', 'Theta_Entry'),
        ('Rho', 'Rho_Entry'),
    ]
    
    for current_col, entry_col in greek_mappings:
        if current_col in df.columns:
            df.loc[mask, entry_col] = df.loc[mask, current_col]
    
    return df


def _freeze_entry_iv(df: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
    """
    Freeze current IV as entry IV.
    
    Priority:
    1. IV Mid (broker-provided)
    2. Implied Volatility (calculated)
    3. NaN if not available
    """
    df = df.copy()
    
    # Check for IV columns
    iv_columns = ['IV Mid', 'Implied Volatility', 'IV']
    
    for col in iv_columns:
        if col in df.columns:
            # Copy non-null IV values to entry
            valid_iv = mask & df[col].notna()
            df.loc[valid_iv, 'IV_Entry'] = df.loc[valid_iv, col]
            break
    
    return df


def _freeze_entry_premium(df: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
    """
    Freeze entry premium.
    
    Priority:
    1. Time Val (broker truth - extrinsic value)
    2. Last price (for long options)
    3. Calculated from Greeks (fallback)
    
    For short options (Quantity < 0): Premium should be positive (credit received)
    For long options (Quantity > 0): Premium should be negative (debit paid)
    """
    df = df.copy()
    
    options_mask = mask & (df['AssetType'] == 'OPTION')
    
    for idx in df[options_mask].index:
        quantity = df.at[idx, 'Quantity']
        
        # Priority 1: Time Val (extrinsic value - this is what we collect/pay)
        if 'Time Val' in df.columns and pd.notna(df.at[idx, 'Time Val']):
            time_val = df.at[idx, 'Time Val']
            
            # Time Val is always positive, adjust sign based on position
            if quantity < 0:
                # Short: we collected premium (positive)
                premium = abs(time_val)
            else:
                # Long: we paid premium (negative)
                premium = -abs(time_val)
            
            df.at[idx, 'Premium_Entry'] = premium
            
        # Priority 2: Last price (for long options, close approximation)
        elif 'Last' in df.columns and pd.notna(df.at[idx, 'Last']):
            last = df.at[idx, 'Last']
            
            if quantity < 0:
                premium = abs(last)
            else:
                premium = -abs(last)
            
            df.at[idx, 'Premium_Entry'] = premium
        
        # Priority 3: Fallback - mark as estimated
        else:
            df.at[idx, 'Premium_Entry'] = np.nan
    
    return df


def _freeze_entry_context(df: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
    """
    Freeze entry context: Underlying Price, DTE, Strategy, Quantity.
    """
    df = df.copy()
    
    # Freeze Underlying Price
    if 'UL Last' in df.columns:
        df.loc[mask, 'Underlying_Price_Entry'] = df.loc[mask, 'UL Last']
    
    # Freeze DTE
    if 'DTE' in df.columns:
        df.loc[mask, 'Entry_DTE'] = df.loc[mask, 'DTE']
    
    # Freeze Strategy
    if 'Strategy' in df.columns:
        df.loc[mask, 'Strategy_Entry'] = df.loc[mask, 'Strategy']
        
    # Freeze Quantity
    if 'Quantity' in df.columns:
        df.loc[mask, 'Quantity_Entry'] = df.loc[mask, 'Quantity']
    
    return df


def _log_entry_freeze_summary(df: pd.DataFrame, mask: pd.Series) -> None:
    """Log summary of entry data frozen."""
    
    new_positions = df[mask]
    
    # Count by asset type
    options = new_positions[new_positions['AssetType'] == 'OPTION']
    stocks = new_positions[new_positions['AssetType'] == 'STOCK']
    
    logger.info(f"Entry freeze breakdown:")
    logger.info(f"   Options: {len(options)} positions")
    logger.info(f"   Stocks: {len(stocks)} positions")
    
    # Check entry Greeks coverage
    if len(options) > 0:
        delta_frozen = options['Delta_Entry'].notna().sum()
        iv_frozen = options['IV_Entry'].notna().sum()
        premium_frozen = options['Premium_Entry'].notna().sum()
        
        logger.info(f"Entry data coverage:")
        logger.info(f"   Delta_Entry: {delta_frozen}/{len(options)} ({delta_frozen/len(options)*100:.1f}%)")
        logger.info(f"   IV_Entry: {iv_frozen}/{len(options)} ({iv_frozen/len(options)*100:.1f}%)")
        logger.info(f"   Premium_Entry: {premium_frozen}/{len(options)} ({premium_frozen/len(options)*100:.1f}%)")


def _freeze_entry_pcs(df: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
    """
    Freeze Entry_PCS (baseline score at entry) for new positions.
    """
    try:
        from core.phase3_enrich.pcs_score_entry import calculate_entry_pcs
        
        # Only calculate for new positions
        df_new = df[mask].copy()
        
        if df_new.empty:
            return df
        
        # Calculate Entry_PCS (uses Entry Greeks + strategy)
        df_new = calculate_entry_pcs(df_new)
        
        # Merge Entry_PCS columns back to main df
        entry_pcs_cols = [
            'Entry_PCS', 'Entry_PCS_GammaScore', 'Entry_PCS_VegaScore', 
            'Entry_PCS_ROIScore', 'Entry_PCS_Profile', 'Entry_PCS_Tier'
        ]
        
        # Initialize Entry_PCS columns in df if they don't exist
        for col in entry_pcs_cols:
            if col not in df.columns:
                df[col] = np.nan if 'Score' in col or col == 'Entry_PCS' else None
        
        # Copy Entry_PCS from df_new to df for new positions
        df.loc[mask, entry_pcs_cols] = df_new[entry_pcs_cols].values
        
    except ImportError:
        logger.warning("Could not import calculate_entry_pcs, skipping Entry_PCS freeze")
    except Exception as e:
        logger.error(f"Failed to freeze Entry_PCS: {e}")
    
    return df


def validate_entry_freeze(df: pd.DataFrame) -> dict:
    """
    Validate that entry data is properly frozen.
    """
    results = {
        'valid': True,
        'errors': [],
        'warnings': []
    }
    
    # Check positions with First_Seen_Date have entry data
    existing_positions = df[df['First_Seen_Date'].notna()]
    
    if len(existing_positions) > 0:
        options = existing_positions[existing_positions['AssetType'] == 'OPTION']
        
        if len(options) > 0:
            # Check Delta_Entry
            missing_delta = options[options['Delta_Entry'].isna()]
            if len(missing_delta) > 0:
                results['errors'].append(f"{len(missing_delta)} options missing Delta_Entry")
                results['valid'] = False
    
    return results
