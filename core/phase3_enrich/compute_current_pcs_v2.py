"""
Current_PCS v2: Multi-Factor Position Confidence Score

RAG-Compliant Implementation:
- IV_Rank Component: 30% (volatility premium quality)
- Liquidity Component: 25% (OI, Volume, Spread)
- Greeks Component: 20% (Gamma, Vega, Theta efficiency)
- Chart Component: 25% (DEFERRED to Phase 7+)

Score Range: 0-100 (vs Entry_PCS 0-65)
Tier System: S (85+), A (75-84), B (65-74), C (<65)

Design Philosophy:
- Current_PCS tracks LIVE position quality (changes every snapshot)
- Entry_PCS is FROZEN at first_seen (never changes)
- Drift = Current_PCS - Entry_PCS (shows deterioration/improvement)
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, Tuple
from core.data_contracts.config import MANAGEMENT_SAFE_MODE

logger = logging.getLogger(__name__)

# Tier thresholds
TIER_S = 85
TIER_A = 75
TIER_B = 65

# Component weights (totals 75% without Chart)
WEIGHT_IV_RANK = 0.30
WEIGHT_LIQUIDITY = 0.25
WEIGHT_GREEKS = 0.20
# WEIGHT_CHART = 0.25  # Deferred to Phase 7

# Normalization thresholds
IV_RANK_OPTIMAL = 70  # IV_Rank above 70 = full score
OI_THRESHOLD = 1000   # Open Interest benchmark
VOLUME_THRESHOLD = 100  # Daily volume benchmark
SPREAD_MAX_PCT = 0.10  # Spread >10% = zero score


def compute_current_pcs_v2(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute Current_PCS v2 with multi-factor scoring.
    """
    df = df.copy()
    
    if not MANAGEMENT_SAFE_MODE:
        logger.info(f"Computing Current_PCS v2 for {len(df)} positions...")
    
    # Filter to options only (stocks don't have PCS)
    options_mask = df['AssetType'] == 'OPTION'
    n_options = options_mask.sum()
    
    if n_options == 0:
        if not MANAGEMENT_SAFE_MODE:
            logger.warning("No option positions found, skipping Current_PCS v2")
        return _add_empty_pcs_columns(df)
    
    if not MANAGEMENT_SAFE_MODE:
        logger.info(f"Processing {n_options} option positions (excluding {len(df) - n_options} stocks)")
    
    # Compute each component
    iv_scores = _compute_iv_rank_component(df)
    liquidity_scores = _compute_liquidity_component(df)
    greeks_scores = _compute_greeks_component(df)
    
    # Track which components are available (only for options)
    iv_available = (pd.notna(iv_scores) & options_mask)
    liquidity_available = (pd.notna(liquidity_scores) & options_mask)
    greeks_available = (pd.notna(greeks_scores) & options_mask)
    
    # Calculate available weight (max 75% without Chart)
    available_weight = (
        (iv_available.astype(float) * WEIGHT_IV_RANK) +
        (liquidity_available.astype(float) * WEIGHT_LIQUIDITY) +
        (greeks_available.astype(float) * WEIGHT_GREEKS)
    )
    
    # Compute weighted sum (raw sum of all components: 0-75 max)
    current_pcs_raw = (
        iv_scores.fillna(0) +
        liquidity_scores.fillna(0) +
        greeks_scores.fillna(0)
    )
    
    # Only score options with at least one component available
    current_pcs_scaled = pd.Series(np.nan, index=df.index)
    valid_mask = options_mask & (available_weight > 0)
    
    # Scale raw score to 0-100 range
    if MANAGEMENT_SAFE_MODE:
        # Management Safe Mode: Treat missing components as neutral by scaling 
        # only by the weight of available components.
        current_pcs_scaled[valid_mask] = (current_pcs_raw[valid_mask] / available_weight[valid_mask])
    else:
        # Legacy/Scan Mode: Hard-coded 75 point max (penalizes missing data)
        current_pcs_scaled[valid_mask] = (current_pcs_raw[valid_mask] / 75) * 100
    
    # Assign tiers based on 0-100 scale
    tiers = pd.cut(
        current_pcs_scaled,
        bins=[-np.inf, TIER_B, TIER_A, TIER_S, np.inf],
        labels=['C', 'B', 'A', 'S']
    )
    
    # Add columns
    df['Current_PCS_v2'] = current_pcs_scaled
    df['Current_PCS_IV_Score'] = iv_scores
    df['Current_PCS_Liquidity_Score'] = liquidity_scores
    df['Current_PCS_Greeks_Score'] = greeks_scores
    df['Current_PCS_Tier_v2'] = tiers
    df['Current_PCS_Available_Weight'] = available_weight
    
    # Summary statistics
    if not MANAGEMENT_SAFE_MODE:
        valid_scores = df.loc[options_mask, 'Current_PCS_v2'].dropna()
        if len(valid_scores) > 0:
            logger.info(f"✅ Current_PCS v2 computed for {len(valid_scores)} options")
            logger.info(f"   Score range: {valid_scores.min():.1f} - {valid_scores.max():.1f}")
            logger.info(f"   Average: {valid_scores.mean():.1f}")
            
            tier_counts = df.loc[options_mask, 'Current_PCS_Tier_v2'].value_counts()
            logger.info(f"   Tier distribution: {tier_counts.to_dict()}")
        else:
            logger.warning("⚠️  No valid Current_PCS v2 scores computed")
    
    return df


def _compute_iv_rank_component(df: pd.DataFrame) -> pd.Series:
    """
    Compute IV_Rank component (30% weight).
    """
    iv_rank = df.get('IV_Rank', pd.Series(np.nan, index=df.index))
    
    # Normalize to 0-30 range
    scores = np.where(
        iv_rank >= IV_RANK_OPTIMAL,
        30.0,  # Full score above 70
        np.where(
            iv_rank >= 50,
            15 + ((iv_rank - 50) / 20) * 15,  # 50-70 → 15-30
            (iv_rank / 50) * 15  # 0-50 → 0-15
        )
    )
    
    return pd.Series(scores, index=df.index)


def _compute_liquidity_component(df: pd.DataFrame) -> pd.Series:
    """
    Compute Liquidity component (25% weight).
    """
    oi = df.get('Open Interest', pd.Series(np.nan, index=df.index))
    volume = df.get('Volume', pd.Series(np.nan, index=df.index))
    
    # Spread calculation
    bid = df.get('Bid', pd.Series(np.nan, index=df.index))
    ask = df.get('Ask', pd.Series(np.nan, index=df.index))
    mid = (bid + ask) / 2
    spread_pct = np.where(mid > 0, (ask - bid) / mid, np.nan)
    
    # OI score (0-10)
    oi_score = np.where(
        pd.notna(oi),
        np.clip(oi / OI_THRESHOLD, 0, 1) * 10,
        np.nan
    )
    
    # Volume score (0-10)
    volume_score = np.where(
        pd.notna(volume),
        np.clip(volume / VOLUME_THRESHOLD, 0, 1) * 10,
        np.nan
    )
    
    # Spread score (0-5)
    spread_score = np.where(
        pd.notna(spread_pct),
        np.where(
            spread_pct < 0.05,
            5.0,  # Tight spread < 5%
            np.where(
                spread_pct < SPREAD_MAX_PCT,
                5 * (1 - (spread_pct - 0.05) / 0.05),  # 5-10% linear decay
                0.0  # Wide spread >= 10%
            )
        ),
        np.nan
    )
    
    # Combine sub-components
    scores = pd.DataFrame({
        'oi': oi_score,
        'volume': volume_score,
        'spread': spread_score
    })
    
    available_count = scores.notna().sum(axis=1)
    total_score = scores.sum(axis=1)
    
    liquidity_score = np.where(
        available_count > 0,
        (total_score / available_count) * (25 / 25),
        np.nan
    )
    
    return pd.Series(liquidity_score, index=df.index)


def _compute_greeks_component(df: pd.DataFrame) -> pd.Series:
    """
    Compute Greeks component (20% weight).
    """
    gamma = df.get('Gamma', pd.Series(0, index=df.index))
    vega = df.get('Vega', pd.Series(0, index=df.index))
    theta = df.get('Theta', pd.Series(0, index=df.index))
    quantity = df.get('Quantity', pd.Series(1, index=df.index))
    premium = df.get('Premium', pd.Series(np.nan, index=df.index))
    
    gamma_per_contract = np.abs(gamma / quantity.replace(0, 1))
    gamma_score = np.clip(gamma_per_contract * 200, 0, 10)
    
    vega_per_contract = np.abs(vega / quantity.replace(0, 1))
    vega_score = np.clip(vega_per_contract * 10, 0, 5)
    
    theta_efficiency = np.where(
        (pd.notna(theta)) & (pd.notna(premium)) & (np.abs(premium) > 0.01),
        np.abs(theta) / np.abs(premium),
        0
    )
    theta_score = np.clip(theta_efficiency * 250, 0, 5)
    
    greeks_score = gamma_score + vega_score + theta_score
    
    return pd.Series(greeks_score, index=df.index)


def _add_empty_pcs_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add empty PCS columns when no options to process."""
    df['Current_PCS_v2'] = np.nan
    df['Current_PCS_IV_Score'] = np.nan
    df['Current_PCS_Liquidity_Score'] = np.nan
    df['Current_PCS_Greeks_Score'] = np.nan
    df['Current_PCS_Tier_v2'] = None
    df['Current_PCS_Available_Weight'] = np.nan
    return df


def compute_pcs_drift_v2(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute PCS drift using v2 scoring.
    """
    if 'Current_PCS_v2' not in df.columns or 'Entry_PCS' not in df.columns:
        if not MANAGEMENT_SAFE_MODE:
            logger.warning("Cannot compute PCS_Drift_v2: missing Current_PCS_v2 or Entry_PCS")
        return df
    
    df = df.copy()
    entry_pcs_scaled = (df['Entry_PCS'] / 65) * 100
    df['PCS_Drift_v2'] = df['Current_PCS_v2'] - entry_pcs_scaled
    
    df['PCS_Drift_v2_Pct'] = np.where(
        entry_pcs_scaled > 0,
        (df['PCS_Drift_v2'] / entry_pcs_scaled) * 100,
        np.nan
    )
    
    if not MANAGEMENT_SAFE_MODE:
        logger.info(f"✅ PCS_Drift_v2 computed")
    
    return df
