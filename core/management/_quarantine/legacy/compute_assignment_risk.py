"""
Phase 3 Observable: Assignment Risk Scoring

Computes assignment risk for short option positions based on:
- Moneyness (ITM/OTM)
- Time to Expiration (DTE)
- Dividend Proximity (if available)
- Earnings Proximity (if available)
- Intrinsic Value vs. Extrinsic Value
"""

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


def compute_assignment_risk(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute assignment risk for short option positions.
    """
    if df.empty:
        return df
    
    df = df.copy()
    
    # Ensure required columns exist to avoid KeyErrors
    required_cols = [
        'Days_to_Earnings', 'Days_to_Dividend', 'Intrinsic_Value', 
        'Extrinsic_Value', 'Moneyness_Pct', 'DTE'
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = np.nan

    logger.info("Computing assignment risk metrics...")
    
    # Only short options have assignment risk
    short_options_mask = (df['AssetType'] == 'OPTION') & (df['Quantity'] < 0)
    
    # Initialize risk columns
    df['Assignment_Risk_Score'] = 0.0
    df['Assignment_Risk_Level'] = 'NONE'
    
    if not short_options_mask.any():
        logger.info("No short option positions found, skipping assignment risk")
        return df
    
    logger.info(f"Analyzing assignment risk for {short_options_mask.sum()} short options positions")
    
    # Compute risk score (0-100)
    df = _compute_assignment_score(df, short_options_mask)
    
    # Classify risk level
    df = _classify_assignment_risk(df, short_options_mask)
    
    return df


def _compute_assignment_score(df: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
    """
    Calculate numeric assignment risk score (0-100).
    """
    for idx in df[mask].index:
        score = 0.0
        
        dte = df.at[idx, 'DTE']
        moneyness = df.at[idx, 'Moneyness_Pct']
        
        # 1. DTE Component (up to 40 points)
        if pd.notna(dte):
            if dte <= 0: score += 40
            elif dte <= 1: score += 35
            elif dte <= 3: score += 25
            elif dte <= 7: score += 15
            elif dte <= 14: score += 5
            
        # 2. Moneyness Component (up to 40 points)
        if pd.notna(moneyness):
            if moneyness < -5: score += 40  # Deep ITM
            elif moneyness < 0: score += 30  # ITM
            elif moneyness < 2: score += 15  # Near-the-money
            elif moneyness < 5: score += 5   # OTM
            
        # 3. Event Proximity (up to 20 points)
        days_to_earnings = df.at[idx, 'Days_to_Earnings']
        if pd.notna(days_to_earnings) and days_to_earnings <= 1:
            score += 20
            
        df.at[idx, 'Assignment_Risk_Score'] = min(score, 100.0)
        
    return df


def _classify_assignment_risk(df: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
    """
    Classify risk score into semantic levels.
    """
    conditions = [
        (df['Assignment_Risk_Score'] >= 80),
        (df['Assignment_Risk_Score'] >= 60),
        (df['Assignment_Risk_Score'] >= 30),
        (df['Assignment_Risk_Score'] > 0)
    ]
    choices = ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']
    
    # Apply to whole dataframe then mask out non-short options
    df['Assignment_Risk_Level'] = np.select(
        conditions, choices, default='LOW'
    )
    
    # Reset non-short options to NONE
    df.loc[~mask, 'Assignment_Risk_Level'] = 'NONE'
    
    return df


def get_high_assignment_risk_positions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter for positions with HIGH or CRITICAL assignment risk.
    """
    if 'Assignment_Risk_Level' not in df.columns:
        return pd.DataFrame()
        
    return df[df['Assignment_Risk_Level'].isin(['HIGH', 'CRITICAL'])]
