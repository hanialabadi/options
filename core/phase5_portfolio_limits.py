"""
Phase 5: Portfolio Greek Limits and Risk Aggregation

Enforces portfolio-level Greek exposure limits to prevent correlated risk concentration.

Design Principles:
1. Aggregate Greeks across all positions (net exposure)
2. Check against configurable limits per account size
3. Flag positions that would exceed limits
4. Provide visibility into portfolio heat
5. Support multiple personas (conservative, aggressive, balanced)

Persona Requirements:
- Risk Manager: Prevent >5 correlated short-vega positions
- Conservative Income: Limit total short gamma exposure
- Volatility Trader: Track vega P&L separately from delta P&L
"""

import numpy as np
import pandas as pd
import logging
from typing import Dict, Tuple

logger = logging.getLogger(__name__)


# Portfolio Greek Limits (per $100k account)
# These can be adjusted based on account size and risk tolerance

CONSERVATIVE_LIMITS = {
    'max_net_delta': 50.0,          # Max directional exposure (±$50 per $1 underlying move)
    'max_short_vega': -50.0,        # Max -$50 per 1% IV move
    'max_short_gamma': -10.0,       # Max -$10 per $1 underlying move
    'max_long_gamma': 20.0,         # Max +$20 per $1 underlying move
    'max_theta_decay': -100.0,      # Max -$100 per day decay
    'max_positions_per_ticker': 3,  # Max strategies per underlying
    'max_sector_concentration': 30.0,  # Max % of portfolio in one sector
}

BALANCED_LIMITS = {
    'max_net_delta': 100.0,
    'max_short_vega': -100.0,
    'max_short_gamma': -20.0,
    'max_long_gamma': 40.0,
    'max_theta_decay': -200.0,
    'max_positions_per_ticker': 5,
    'max_sector_concentration': 40.0,
}

AGGRESSIVE_LIMITS = {
    'max_net_delta': 200.0,
    'max_short_vega': -200.0,
    'max_short_gamma': -50.0,
    'max_long_gamma': 100.0,
    'max_theta_decay': -500.0,
    'max_positions_per_ticker': 10,
    'max_sector_concentration': 50.0,
}


def compute_portfolio_greeks(df: pd.DataFrame) -> Dict[str, float]:
    """
    Aggregate Greeks across entire portfolio.
    
    Args:
        df: DataFrame with position-level Greeks
        
    Returns:
        Dictionary with portfolio-level Greek totals
    """
    greeks = {
        'net_delta': 0.0,
        'net_gamma': 0.0,
        'net_vega': 0.0,
        'net_theta': 0.0,
        'net_rho': 0.0,
    }
    
    if df.empty:
        return greeks
    
    # Sum trade-level Greeks (not leg-level to avoid double counting)
    if 'Delta_Trade' in df.columns:
        greeks['net_delta'] = df.groupby('TradeID')['Delta_Trade'].first().sum()
    elif 'Delta' in df.columns:
        greeks['net_delta'] = df['Delta'].sum()
    
    if 'Gamma_Trade' in df.columns:
        greeks['net_gamma'] = df.groupby('TradeID')['Gamma_Trade'].first().sum()
    elif 'Gamma' in df.columns:
        greeks['net_gamma'] = df['Gamma'].sum()
    
    if 'Vega_Trade' in df.columns:
        greeks['net_vega'] = df.groupby('TradeID')['Vega_Trade'].first().sum()
    elif 'Vega' in df.columns:
        greeks['net_vega'] = df['Vega'].sum()
    
    if 'Theta_Trade' in df.columns:
        greeks['net_theta'] = df.groupby('TradeID')['Theta_Trade'].first().sum()
    elif 'Theta' in df.columns:
        greeks['net_theta'] = df['Theta'].sum()
    
    if 'Rho' in df.columns:
        greeks['net_rho'] = df['Rho'].sum()
    
    return greeks


def check_portfolio_limits(
    df: pd.DataFrame,
    limits: Dict[str, float] = None,
    account_balance: float = 100000.0
) -> Tuple[pd.DataFrame, Dict[str, any]]:
    """
    Check portfolio against Greek exposure limits.
    
    Args:
        df: DataFrame with positions
        limits: Dictionary of limit thresholds (default: CONSERVATIVE_LIMITS)
        account_balance: Total account value for scaling limits
        
    Returns:
        Tuple of (df with limit flags, diagnostics dictionary)
    """
    if df.empty:
        logger.warning("⚠️ Empty DataFrame in check_portfolio_limits")
        return df, {}
    
    # Use conservative limits by default
    if limits is None:
        limits = CONSERVATIVE_LIMITS.copy()
    
    # Scale limits by account size (limits are per $100k)
    scale_factor = account_balance / 100000.0
    scaled_limits = {k: v * scale_factor for k, v in limits.items()}
    
    # Compute portfolio Greeks
    portfolio_greeks = compute_portfolio_greeks(df)
    
    # Check each limit
    violations = []
    warnings = []
    
    # Delta limit
    if abs(portfolio_greeks['net_delta']) > abs(scaled_limits['max_net_delta']):
        violations.append({
            'type': 'NET_DELTA',
            'current': portfolio_greeks['net_delta'],
            'limit': scaled_limits['max_net_delta'],
            'severity': 'HIGH'
        })
    
    # Vega limit (short vega concentration risk)
    if portfolio_greeks['net_vega'] < scaled_limits['max_short_vega']:
        violations.append({
            'type': 'SHORT_VEGA',
            'current': portfolio_greeks['net_vega'],
            'limit': scaled_limits['max_short_vega'],
            'severity': 'HIGH'
        })
    
    # Gamma limits
    if portfolio_greeks['net_gamma'] < scaled_limits['max_short_gamma']:
        violations.append({
            'type': 'SHORT_GAMMA',
            'current': portfolio_greeks['net_gamma'],
            'limit': scaled_limits['max_short_gamma'],
            'severity': 'MEDIUM'
        })
    
    if portfolio_greeks['net_gamma'] > scaled_limits['max_long_gamma']:
        warnings.append({
            'type': 'LONG_GAMMA',
            'current': portfolio_greeks['net_gamma'],
            'limit': scaled_limits['max_long_gamma'],
            'severity': 'LOW'
        })
    
    # Theta limit (excessive decay)
    if portfolio_greeks['net_theta'] < scaled_limits['max_theta_decay']:
        warnings.append({
            'type': 'THETA_DECAY',
            'current': portfolio_greeks['net_theta'],
            'limit': scaled_limits['max_theta_decay'],
            'severity': 'MEDIUM'
        })
    
    # Position concentration (positions per ticker)
    if 'Underlying' in df.columns and 'TradeID' in df.columns:
        positions_per_ticker = df.groupby('Underlying')['TradeID'].nunique()
        max_positions = positions_per_ticker.max()
        
        if max_positions > scaled_limits['max_positions_per_ticker']:
            violations.append({
                'type': 'TICKER_CONCENTRATION',
                'current': max_positions,
                'limit': scaled_limits['max_positions_per_ticker'],
                'severity': 'MEDIUM',
                'ticker': positions_per_ticker.idxmax()
            })
    
    # Calculate utilization percentages
    utilization = {
        'delta_pct': abs(portfolio_greeks['net_delta'] / scaled_limits['max_net_delta'] * 100) if scaled_limits['max_net_delta'] != 0 else 0,
        'vega_pct': abs(portfolio_greeks['net_vega'] / scaled_limits['max_short_vega'] * 100) if scaled_limits['max_short_vega'] != 0 else 0,
        'gamma_pct': abs(portfolio_greeks['net_gamma'] / scaled_limits['max_short_gamma'] * 100) if scaled_limits['max_short_gamma'] != 0 else 0,
        'theta_pct': abs(portfolio_greeks['net_theta'] / scaled_limits['max_theta_decay'] * 100) if scaled_limits['max_theta_decay'] != 0 else 0,
    }
    
    # Add portfolio-level columns
    df['Portfolio_Net_Delta'] = portfolio_greeks['net_delta']
    df['Portfolio_Net_Vega'] = portfolio_greeks['net_vega']
    df['Portfolio_Net_Gamma'] = portfolio_greeks['net_gamma']
    df['Portfolio_Net_Theta'] = portfolio_greeks['net_theta']
    
    df['Portfolio_Delta_Utilization_Pct'] = utilization['delta_pct']
    df['Portfolio_Vega_Utilization_Pct'] = utilization['vega_pct']
    df['Portfolio_Gamma_Utilization_Pct'] = utilization['gamma_pct']
    df['Portfolio_Theta_Utilization_Pct'] = utilization['theta_pct']
    
    # Flag individual positions contributing to violations
    df['Portfolio_Risk_Flags'] = ''
    
    if violations or warnings:
        # Flag positions with largest Greek contributions
        if 'Delta_Trade' in df.columns:
            top_delta_trades = df.nlargest(3, 'Delta_Trade', keep='all')['TradeID'].unique()
            df.loc[df['TradeID'].isin(top_delta_trades), 'Portfolio_Risk_Flags'] += 'HIGH_DELTA_CONTRIBUTOR;'
        
        if 'Vega_Trade' in df.columns:
            short_vega_mask = df['Vega_Trade'] < 0
            if short_vega_mask.any():
                top_short_vega = df[short_vega_mask].nsmallest(3, 'Vega_Trade', keep='all')['TradeID'].unique()
                df.loc[df['TradeID'].isin(top_short_vega), 'Portfolio_Risk_Flags'] += 'SHORT_VEGA_CONTRIBUTOR;'
    
    # Build diagnostics
    diagnostics = {
        'portfolio_greeks': portfolio_greeks,
        'scaled_limits': scaled_limits,
        'utilization': utilization,
        'violations': violations,
        'warnings': warnings,
        'account_balance': account_balance,
        'scale_factor': scale_factor,
    }
    
    # Log summary
    logger.info("📊 Portfolio Greek Limits Check:")
    logger.info(f"   Net Delta: {portfolio_greeks['net_delta']:.2f} / {scaled_limits['max_net_delta']:.2f} ({utilization['delta_pct']:.1f}%)")
    logger.info(f"   Net Vega: {portfolio_greeks['net_vega']:.2f} / {scaled_limits['max_short_vega']:.2f} ({utilization['vega_pct']:.1f}%)")
    logger.info(f"   Net Gamma: {portfolio_greeks['net_gamma']:.2f} / {scaled_limits['max_short_gamma']:.2f} ({utilization['gamma_pct']:.1f}%)")
    logger.info(f"   Net Theta: {portfolio_greeks['net_theta']:.2f} / {scaled_limits['max_theta_decay']:.2f} ({utilization['theta_pct']:.1f}%)")
    
    if violations:
        logger.warning(f"⚠️ {len(violations)} portfolio limit violation(s) detected:")
        for v in violations:
            logger.warning(f"   {v['type']}: {v['current']:.2f} exceeds {v['limit']:.2f} (severity: {v['severity']})")
    
    if warnings:
        logger.info(f"ℹ️ {len(warnings)} portfolio warning(s):")
        for w in warnings:
            logger.info(f"   {w['type']}: {w['current']:.2f} approaching {w['limit']:.2f}")
    
    if not violations and not warnings:
        logger.info("✅ All portfolio limits within acceptable ranges")
    
    return df, diagnostics


def analyze_correlation_risk(df: pd.DataFrame) -> pd.DataFrame:
    """
    Analyze correlation and concentration risk across positions.
    
    Identifies:
    - Multiple positions on same underlying
    - Correlated underlyings (e.g., AAPL + MSFT + GOOGL = tech concentration)
    - Similar strategy types (e.g., 5 short strangle positions)
    
    Args:
        df: DataFrame with positions
        
    Returns:
        DataFrame with correlation risk columns added
    """
    if df.empty or 'Underlying' not in df.columns or 'TradeID' not in df.columns:
        logger.warning("⚠️ Missing columns for correlation analysis")
        return df
    
    # Count positions per underlying
    positions_per_underlying = df.groupby('Underlying')['TradeID'].nunique()
    df['Positions_On_Underlying'] = df['Underlying'].map(positions_per_underlying)
    
    # Flag high concentration
    df['Underlying_Concentration_Risk'] = 'LOW'
    df.loc[df['Positions_On_Underlying'] >= 3, 'Underlying_Concentration_Risk'] = 'MEDIUM'
    df.loc[df['Positions_On_Underlying'] >= 5, 'Underlying_Concentration_Risk'] = 'HIGH'
    
    # Count similar strategies
    if 'Strategy' in df.columns:
        strategy_counts = df.groupby('Strategy')['TradeID'].nunique()
        df['Strategy_Concentration'] = df['Strategy'].map(strategy_counts)
        
        # Flag correlated strategy risk
        df['Strategy_Correlation_Risk'] = 'LOW'
        df.loc[df['Strategy_Concentration'] >= 5, 'Strategy_Correlation_Risk'] = 'MEDIUM'
        df.loc[df['Strategy_Concentration'] >= 10, 'Strategy_Correlation_Risk'] = 'HIGH'
    
    # Log findings
    high_concentration = df[df['Underlying_Concentration_Risk'] == 'HIGH']['Underlying'].unique()
    if len(high_concentration) > 0:
        logger.warning(f"⚠️ High concentration on underlyings: {', '.join(high_concentration)}")
    
    return df


def get_persona_limits(persona: str = 'conservative') -> Dict[str, float]:
    """
    Get Greek limits for specific trader persona.
    
    Args:
        persona: 'conservative', 'balanced', or 'aggressive'
        
    Returns:
        Dictionary of limits
    """
    persona = persona.lower()
    
    if persona == 'conservative':
        return CONSERVATIVE_LIMITS.copy()
    elif persona == 'balanced':
        return BALANCED_LIMITS.copy()
    elif persona == 'aggressive':
        return AGGRESSIVE_LIMITS.copy()
    else:
        logger.warning(f"Unknown persona '{persona}', using conservative limits")
        return CONSERVATIVE_LIMITS.copy()
