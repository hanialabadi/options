"""
Phase 3 Enrichment: Assignment Risk Scoring

Detects and scores assignment risk for short options positions:
- ITM risk detection (DTE < 7)
- Assignment probability scoring (0-100)
- Pin risk detection (ATM near expiration)
- Early assignment alerts (dividends, deep ITM)

Assignment risk increases with:
1. Moneyness (deeper ITM = higher risk)
2. Time decay (closer to expiration = higher risk)
3. Dividend proximity (ex-div date within DTE)
4. Liquidity (wide spreads increase assignment likelihood)

Author: System
Date: 2026-01-04
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


def compute_assignment_risk(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute assignment risk metrics for options positions.
    
    Assignment Risk Scoring:
    - Short options only (long options can't be assigned)
    - ITM + DTE < 7: High risk
    - Deep ITM (>10%) + DTE < 14: Medium risk
    - ATM (±2%) + DTE < 3: Pin risk
    - Deep ITM (>20%) + dividend: Early assignment risk
    
    Args:
        df: DataFrame with options positions
        
    Returns:
        DataFrame with assignment risk columns added:
        - Is_Short_Position: Boolean, True if Quantity < 0
        - Is_ITM: Boolean, True if in-the-money
        - ITM_Amount: Dollar amount ITM
        - ITM_Pct: Percentage ITM relative to strike
        - Assignment_Risk_Score: 0-100 (0=no risk, 100=very high risk)
        - Assignment_Risk_Level: LOW/MEDIUM/HIGH/CRITICAL
        - Pin_Risk: Boolean, True if ATM near expiration
        - Early_Assignment_Risk: Boolean, True if deep ITM + dividend
        - Days_To_Assignment: Estimated days until likely assignment
    """
    logger.info("Computing assignment risk metrics...")
    
    df = df.copy()
    
    # Initialize assignment risk columns
    df['Is_Short_Position'] = False
    df['Is_ITM'] = False
    df['ITM_Amount'] = 0.0
    df['ITM_Pct'] = 0.0
    df['Assignment_Risk_Score'] = 0.0
    df['Assignment_Risk_Level'] = 'NONE'
    df['Pin_Risk'] = False
    df['Early_Assignment_Risk'] = False
    df['Days_To_Assignment'] = np.nan
    
    # Only compute for options (exclude stocks)
    # Use 'AssetType' column (OPTION vs STOCK)
    options_mask = df['AssetType'] == 'OPTION'
    if not options_mask.any():
        logger.info("No options positions found, skipping assignment risk")
        return df
    
    # Identify short positions (negative quantity)
    df.loc[options_mask, 'Is_Short_Position'] = df.loc[options_mask, 'Quantity'] < 0
    
    short_options_mask = options_mask & df['Is_Short_Position']
    num_short = short_options_mask.sum()
    
    if num_short == 0:
        logger.info("No short options positions, no assignment risk")
        return df
    
    logger.info(f"Analyzing assignment risk for {num_short} short options positions")
    
    # Compute ITM status and amounts
    df = _compute_itm_status(df, short_options_mask)
    
    # Compute assignment risk score (0-100)
    df = _compute_assignment_score(df, short_options_mask)
    
    # Detect pin risk (ATM near expiration)
    df = _detect_pin_risk(df, short_options_mask)
    
    # Detect early assignment risk (deep ITM + dividend)
    df = _detect_early_assignment_risk(df, short_options_mask)
    
    # Estimate days to assignment
    df = _estimate_days_to_assignment(df, short_options_mask)
    
    # Classify risk levels
    df = _classify_risk_levels(df, short_options_mask)
    
    # Log summary
    _log_assignment_risk_summary(df, short_options_mask)
    
    return df


def _compute_itm_status(df: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
    """
    Determine if short options are in-the-money.
    
    ITM Definition:
    - Short Call: Underlying > Strike
    - Short Put: Underlying < Strike
    """
    df = df.copy()
    
    for idx in df[mask].index:
        option_type = df.at[idx, 'OptionType']
        strike = df.at[idx, 'Strike']
        underlying_price = df.at[idx, 'UL Last']  # Underlying last price
        
        if pd.isna(strike) or pd.isna(underlying_price) or pd.isna(option_type):
            continue
        
        # Normalize option type to uppercase for comparison
        option_type_upper = str(option_type).upper()
        
        if option_type_upper == 'CALL':
            # Short call is ITM when underlying > strike
            itm = underlying_price > strike
            itm_amount = max(0, underlying_price - strike) * 100  # Per contract
        elif option_type_upper == 'PUT':
            # Short put is ITM when underlying < strike
            itm = underlying_price < strike
            itm_amount = max(0, strike - underlying_price) * 100  # Per contract
        else:
            itm = False
            itm_amount = 0.0
        
        df.at[idx, 'Is_ITM'] = itm
        df.at[idx, 'ITM_Amount'] = itm_amount if itm else 0.0
        
        # ITM percentage (relative to strike)
        if itm and strike > 0:
            df.at[idx, 'ITM_Pct'] = (itm_amount / 100) / strike * 100
    
    return df


def _compute_assignment_score(df: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
    """
    Compute assignment risk score (0-100).
    """
    df = df.copy()
    
    for idx in df[mask].index:
        dte = df.at[idx, 'DTE']
        itm_pct = df.at[idx, 'ITM_Pct']
        moneyness_pct = df.at[idx, 'Moneyness_Pct']
        
        if pd.isna(dte) or pd.isna(moneyness_pct):
            continue
        
        # Base score from moneyness
        abs_moneyness = abs(moneyness_pct)
        
        if abs_moneyness <= 2:  # ATM
            base_score = 20
        elif itm_pct > 0:  # ITM
            if itm_pct <= 5:
                base_score = 30
            elif itm_pct <= 10:
                base_score = 50
            elif itm_pct <= 20:
                base_score = 70
            else:
                base_score = 85
        else:  # OTM
            base_score = 0
        
        # DTE multiplier
        if dte > 30:
            dte_mult = 0.1
        elif dte >= 15:
            dte_mult = 0.3
        elif dte >= 7:
            dte_mult = 0.6
        elif dte >= 3:
            dte_mult = 1.0
        elif dte >= 1:
            dte_mult = 1.5
        else:  # DTE 0
            dte_mult = 2.0
        
        # Calculate score
        score = base_score * dte_mult
        
        # Dividend adjustment (for calls)
        option_type = df.at[idx, 'OptionType']
        days_to_earnings = df.at[idx, 'Days_to_Earnings']
        
        if not pd.isna(option_type) and str(option_type).upper() == 'CALL' and not pd.isna(days_to_earnings):
            # Approximate dividend risk (earnings often coincide with dividends)
            if 0 <= days_to_earnings <= dte and itm_pct > 10:
                score += 20
        
        # Cap at 100
        score = min(100, score)
        
        df.at[idx, 'Assignment_Risk_Score'] = score
    
    return df


def _detect_pin_risk(df: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
    """
    Detect pin risk: ATM options near expiration.
    """
    df = df.copy()
    
    for idx in df[mask].index:
        dte = df.at[idx, 'DTE']
        moneyness_pct = df.at[idx, 'Moneyness_Pct']
        
        if pd.isna(dte) or pd.isna(moneyness_pct):
            continue
        
        # Pin risk: ATM + near expiration
        is_atm = abs(moneyness_pct) <= 2.0
        near_expiration = dte <= 3
        
        df.at[idx, 'Pin_Risk'] = is_atm and near_expiration
    
    return df


def _detect_early_assignment_risk(df: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
    """
    Detect early assignment risk.
    """
    df = df.copy()
    
    for idx in df[mask].index:
        option_type = df.at[idx, 'OptionType']
        itm_pct = df.at[idx, 'ITM_Pct']
        dte = df.at[idx, 'DTE']
        days_to_earnings = df.at[idx, 'Days_to_Earnings']
        
        if pd.isna(itm_pct) or pd.isna(dte) or pd.isna(option_type):
            continue
        
        early_risk = False
        option_type_upper = str(option_type).upper()
        
        # Deep ITM threshold
        if itm_pct > 20:
            if option_type_upper == 'CALL':
                # Calls: dividend risk
                if not pd.isna(days_to_earnings) and 0 <= days_to_earnings <= dte:
                    early_risk = True
            elif option_type_upper == 'PUT':
                # Puts: deep ITM near expiration (holder may want capital)
                if dte <= 7:
                    early_risk = True
        
        df.at[idx, 'Early_Assignment_Risk'] = early_risk
    
    return df


def _estimate_days_to_assignment(df: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
    """
    Estimate days until likely assignment.
    """
    df = df.copy()
    
    for idx in df[mask].index:
        score = df.at[idx, 'Assignment_Risk_Score']
        dte = df.at[idx, 'DTE']
        early_risk = df.at[idx, 'Early_Assignment_Risk']
        
        if pd.isna(score) or pd.isna(dte):
            continue
        
        if early_risk:
            # Early assignment could happen any day
            days_to = max(0, dte * 0.5)
        elif score >= 50:  # MEDIUM or higher
            # Likely at expiration
            days_to = dte
        elif score >= 20:  # LOW
            # Possible at expiration
            days_to = dte
        else:
            # Unlikely
            days_to = np.nan
        
        df.at[idx, 'Days_To_Assignment'] = days_to
    
    return df


def _classify_risk_levels(df: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
    """
    Classify assignment risk into levels.
    """
    df = df.copy()
    
    def classify_score(score):
        if score == 0:
            return 'NONE'
        elif score < 30:
            return 'LOW'
        elif score < 60:
            return 'MEDIUM'
        elif score < 80:
            return 'HIGH'
        else:
            return 'CRITICAL'
    
    df.loc[mask, 'Assignment_Risk_Level'] = df.loc[mask, 'Assignment_Risk_Score'].apply(classify_score)
    
    return df


def _log_assignment_risk_summary(df: pd.DataFrame, mask: pd.Series) -> None:
    """Log assignment risk summary statistics."""
    
    high_risk = df[mask & (df['Assignment_Risk_Score'] >= 60)]
    critical_risk = df[mask & (df['Assignment_Risk_Score'] >= 80)]
    pin_risk = df[mask & df['Pin_Risk']]
    early_risk = df[mask & df['Early_Assignment_Risk']]
    
    logger.info(f"Assignment risk analysis:")
    logger.info(f"   CRITICAL risk: {len(critical_risk)} positions")
    logger.info(f"   HIGH risk: {len(high_risk)} positions")
    logger.info(f"   Pin risk: {len(pin_risk)} positions")
    logger.info(f"   Early assignment risk: {len(early_risk)} positions")
    
    if len(high_risk) > 0:
        logger.warning("⚠️  High assignment risk detected on positions:")
        for idx in high_risk.head(5).index:
            # Use Underlying_Ticker for logging to align with canonical identity
            ticker = df.at[idx, 'Underlying_Ticker'] if 'Underlying_Ticker' in df.columns else df.at[idx, 'Symbol']
            score = df.at[idx, 'Assignment_Risk_Score']
            dte = df.at[idx, 'DTE']
            itm_pct = df.at[idx, 'ITM_Pct']
            logger.warning(f"      {ticker}: Score {score:.0f}, DTE {dte}, ITM {itm_pct:.1f}%")


def get_high_assignment_risk_positions(df: pd.DataFrame, min_score: float = 60.0) -> pd.DataFrame:
    """
    Get positions with high assignment risk.
    
    Args:
        df: DataFrame with assignment risk columns
        min_score: Minimum assignment risk score (default: 60 = HIGH)
        
    Returns:
        DataFrame with high-risk positions sorted by risk score descending
    """
    high_risk = df[
        (df['Is_Short_Position']) &
        (df['Assignment_Risk_Score'] >= min_score)
    ].copy()
    
    if len(high_risk) == 0:
        return pd.DataFrame()
    
    # Sort by risk score descending
    high_risk = high_risk.sort_values('Assignment_Risk_Score', ascending=False)
    
    return high_risk
