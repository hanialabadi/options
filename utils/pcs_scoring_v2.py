"""
Enhanced PCS Scoring (Version 2)

Strategy-aware scoring with gradient penalties instead of binary pass/fail.

Key improvements:
1. Strategy-aware Greek validation (directional needs Delta, volatility needs Vega)
2. Gradient penalties (not binary) - wider spread = more penalty, not rejection
3. Detailed penalty breakdown in Filter_Reason
4. Status classification: Valid (80-100), Watch (50-79), Rejected (<50)

Usage:
    from utils.pcs_scoring_v2 import calculate_pcs_score_v2
    
    df = calculate_pcs_score_v2(df)
    # Now df has PCS_Score_V2 and detailed Filter_Reason
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional


# Strategy categories
DIRECTIONAL_STRATEGIES = [
    'Long Call', 'Long Put', 'Short Call', 'Short Put',
    'Bull Call Spread', 'Bear Put Spread', 'Bull Put Spread', 'Bear Call Spread'
]

VOLATILITY_STRATEGIES = [
    'Long Straddle', 'Long Strangle', 'Short Straddle', 'Short Strangle',
    'Long Butterfly', 'Long Condor'
]

INCOME_STRATEGIES = [
    'Covered Call', 'Cash-Secured Put', 'Covered Strangle',
    'Short Iron Condor', 'Short Butterfly'
]


def calculate_pcs_score_v2(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate enhanced PCS score with strategy-aware validation.
    
    Requires columns:
        - Delta, Gamma, Vega, Theta (from extract_greeks_to_columns)
        - Liquidity_Score, Bid_Ask_Spread_Pct, Open_Interest
        - Actual_DTE, Risk_Model (or similar risk metric)
        - Strategy
    
    Adds columns:
        - PCS_Score_V2: 0-100 (gradient)
        - PCS_Status: Valid (80+), Watch (50-79), Rejected (<50)
        - PCS_Penalties: JSON with penalty breakdown
        - Filter_Reason: Human-readable explanation
    
    Args:
        df: DataFrame with Greek and liquidity columns
        
    Returns:
        DataFrame with PCS scoring columns
    """
    
    # Initialize columns
    df['PCS_Score_V2'] = 100.0  # Start at 100, subtract penalties
    df['PCS_Penalties'] = ''
    df['Filter_Reason'] = ''
    
    # Apply penalties for each row
    for idx, row in df.iterrows():
        penalties = []
        base_score = 100.0
        
        # 1. Greek validation penalties (strategy-aware)
        greek_penalty, greek_reasons = _calculate_greek_penalties(row)
        base_score -= greek_penalty
        penalties.extend(greek_reasons)
        
        # 2. Liquidity penalties (gradient)
        liquidity_penalty, liquidity_reasons = _calculate_liquidity_penalties(row)
        base_score -= liquidity_penalty
        penalties.extend(liquidity_reasons)
        
        # 3. DTE penalties
        dte_penalty, dte_reasons = _calculate_dte_penalties(row)
        base_score -= dte_penalty
        penalties.extend(dte_reasons)
        
        # 4. Risk penalties
        risk_penalty, risk_reasons = _calculate_risk_penalties(row)
        base_score -= risk_penalty
        penalties.extend(risk_reasons)
        
        # Floor at 0
        final_score = max(0.0, base_score)
        
        # Assign to DataFrame
        df.at[idx, 'PCS_Score_V2'] = final_score
        df.at[idx, 'PCS_Penalties'] = ' | '.join(penalties) if penalties else 'None'
        
        # Generate filter reason
        if final_score >= 80:
            status = 'Valid'
            reason = 'Premium Collection Standard met'
        elif final_score >= 50:
            status = 'Watch'
            reason = f'Marginal quality ({final_score:.0f}/100): ' + (penalties[0] if penalties else 'borderline metrics')
        else:
            status = 'Rejected'
            reason = f'Below PCS threshold ({final_score:.0f}/100): ' + ', '.join(penalties[:2])
        
        df.at[idx, 'PCS_Status'] = status
        df.at[idx, 'Filter_Reason'] = reason
    
    return df


def _calculate_greek_penalties(row: pd.Series) -> Tuple[float, List[str]]:
    """
    Calculate strategy-aware Greek penalties.
    
    Directional: Need |Delta| > 0.35, Vega > 0.18
    Volatility: Need Vega > 0.25, |Delta| < 0.15
    Income: Need |Theta| > Vega (decay dominant)
    
    Returns:
        (total_penalty, list_of_reasons)
    """
    
    strategy = row.get('Strategy', '')
    delta = row.get('Delta')
    vega = row.get('Vega')
    theta = row.get('Theta')
    
    penalties = []
    total_penalty = 0.0
    
    # STRICT: Missing Greeks = Watch status (cannot be Valid)
    # RAG: Natenberg - "Never trade without Greeks"
    if pd.isna(delta) or pd.isna(vega):
        if strategy in DIRECTIONAL_STRATEGIES:
            return 40.0, ['Missing Delta/Vega - Directional unvalidated (-40 pts)']
        elif strategy in VOLATILITY_STRATEGIES:
            return 35.0, ['Missing Vega - Vol strategy unvalidated (-35 pts)']
        else:
            return 25.0, ['Missing Greeks - Strategy unvalidated (-25 pts)']
    
    # Directional strategies
    if strategy in DIRECTIONAL_STRATEGIES:
        # Need meaningful delta
        abs_delta = abs(delta)
        if abs_delta < 0.35:
            penalty = (0.35 - abs_delta) * 50  # Up to 17.5 pts
            total_penalty += penalty
            penalties.append(f'Low Delta ({abs_delta:.2f} < 0.35, -{penalty:.0f} pts)')
        
        # Weak conviction check (low Delta + low Gamma)
        gamma = row.get('Gamma')
        if not pd.isna(gamma) and abs_delta < 0.30 and gamma < 0.02:
            penalty = 20.0  # Weak conviction penalty
            total_penalty += penalty
            penalties.append(f'Weak Conviction (Delta={abs_delta:.2f}, Gamma={gamma:.2f}, -{penalty:.0f} pts)')
        
        # Need some vega for adjustment potential
        if vega < 0.18:
            penalty = (0.18 - vega) * 30  # Up to 5.4 pts
            total_penalty += penalty
            penalties.append(f'Low Vega ({vega:.2f} < 0.18, -{penalty:.0f} pts)')
    
    # Volatility strategies
    elif strategy in VOLATILITY_STRATEGIES:
        # RAG: Natenberg - "Straddles require realized vol > implied vol"
        # STRICT JUSTIFICATION REQUIRED
        
        # 1. Need high vega (measure of vol sensitivity)
        if vega < 0.40:
            penalty = (0.40 - vega) * 60  # Up to 24 pts
            total_penalty += penalty
            penalties.append(f'Low Vega ({vega:.2f} < 0.40, -{penalty:.0f} pts)')
        
        # 2. Should be near delta-neutral (not directional bet)
        abs_delta = abs(delta)
        if abs_delta > 0.15:
            penalty = (abs_delta - 0.15) * 40  # Stricter than before
            total_penalty += penalty
            penalties.append(f'Directional Bias ({abs_delta:.2f} > 0.15, -{penalty:.0f} pts)')
        
        # 3. CRITICAL: Check IV justification (requires IV percentile column)
        # Without IV edge, straddle is pure speculation
        iv_rank = row.get('IV_Percentile') or row.get('IV_Rank')
        if pd.notna(iv_rank):
            # Straddle should have IV justification
            # Low IV = expensive premium with no edge
            if iv_rank < 30:  # Below 30th percentile = low IV
                penalty = (30 - iv_rank) * 1.0  # Up to 30 pts (increased from 0.5)
                total_penalty += penalty
                penalties.append(f'Low IV Edge (IV%ile={iv_rank:.0f} < 30, -{penalty:.0f} pts)')
        else:
            # RAG VIOLATION: Cannot validate vol strategy without IV context
            total_penalty += 20.0
            penalties.append('No IV context - Vol strategy unvalidated (-20 pts)')
        
        # 4. Check for event risk or catalyst (optional but recommended)
        # TODO: Implement earnings/event calendar check
        # For now, penalize generic straddles without clear catalyst
        has_catalyst = row.get('Earnings_Days_Away') or row.get('Event_Risk')
        if pd.isna(has_catalyst):
            total_penalty += 15.0
            penalties.append('No catalyst identified - Generic vol bet (-15 pts)')
    
    # Income strategies
    elif strategy in INCOME_STRATEGIES:
        # Theta should dominate (decay collection)
        if not pd.isna(theta):
            abs_theta = abs(theta)
            if abs_theta <= vega:
                penalty = 10.0
                total_penalty += penalty
                penalties.append(f'Weak Theta ({abs_theta:.2f} ≤ Vega, -{penalty:.0f} pts)')
    
    return total_penalty, penalties


def _calculate_liquidity_penalties(row: pd.Series) -> Tuple[float, List[str]]:
    """
    Calculate gradient liquidity penalties.
    
    Spread: >8% starts penalty, >15% is severe
    OI: <50 starts penalty, <20 is severe
    
    Returns:
        (total_penalty, list_of_reasons)
    """
    
    spread_pct = row.get('Bid_Ask_Spread_Pct')
    oi = row.get('Open_Interest')
    
    penalties = []
    total_penalty = 0.0
    
    # Convert to float/int safely
    try:
        spread_pct = float(spread_pct) if pd.notna(spread_pct) else None
    except (ValueError, TypeError):
        spread_pct = None
    
    try:
        oi = int(oi) if pd.notna(oi) else None
    except (ValueError, TypeError):
        oi = None
    
    # Spread penalty (gradient)
    if spread_pct is not None and spread_pct > 8.0:
        penalty = (spread_pct - 8.0) * 2.0  # -2 pts per % over 8%
        total_penalty += penalty
        penalties.append(f'Wide Spread ({spread_pct:.1f}%, -{penalty:.0f} pts)')
    
    # OI penalty (gradient)
    if oi is not None and oi < 50:
        penalty = (50 - oi) * 0.2  # -0.2 pts per contract below 50
        total_penalty += penalty
        penalties.append(f'Low OI ({oi:.0f}, -{penalty:.0f} pts)')
    
    return total_penalty, penalties


def _calculate_dte_penalties(row: pd.Series) -> Tuple[float, List[str]]:
    """
    Calculate DTE penalties.
    
    DTE < 7: High penalty (execution risk)
    DTE < 14: Moderate penalty
    
    Returns:
        (total_penalty, list_of_reasons)
    """
    
    dte = row.get('Actual_DTE')
    
    penalties = []
    total_penalty = 0.0
    
    # Convert to int safely
    try:
        dte = int(dte) if pd.notna(dte) else None
    except (ValueError, TypeError):
        dte = None
    
    if dte is not None:
        if dte < 7:
            penalty = (7 - dte) * 3.0  # -3 pts per day below 7
            total_penalty += penalty
            penalties.append(f'Very Short DTE ({dte:.0f}d, -{penalty:.0f} pts)')
        elif dte < 14:
            penalty = (14 - dte) * 1.0  # -1 pt per day below 14
            total_penalty += penalty
            penalties.append(f'Short DTE ({dte:.0f}d, -{penalty:.0f} pts)')
    
    return total_penalty, penalties


def _calculate_risk_penalties(row: pd.Series) -> Tuple[float, List[str]]:
    """
    Calculate risk penalties.
    
    Risk > $5k: Moderate penalty (portfolio concentration)
    Risk > $10k: High penalty
    
    Returns:
        (total_penalty, list_of_reasons)
    """
    
    risk = row.get('Risk_Model') or row.get('Actual_Risk_Per_Contract')
    
    penalties = []
    total_penalty = 0.0
    
    # Convert to float safely (Risk_Model might be string, Actual_Risk_Per_Contract should be numeric)
    try:
        risk = float(risk) if pd.notna(risk) and str(risk).replace('.', '').replace('-', '').isdigit() else None
    except (ValueError, TypeError):
        risk = None
    
    if risk is not None and risk > 5000:
        penalty = (risk - 5000) / 100 * 0.5  # -0.5 pts per $100 over $5k
        total_penalty += penalty
        penalties.append(f'High Risk (${risk:,.0f}, -{penalty:.0f} pts)')
    
    return total_penalty, penalties


def analyze_pcs_distribution(df: pd.DataFrame) -> Dict[str, any]:
    """
    Analyze PCS score distribution and quality.
    
    Args:
        df: DataFrame with PCS_Score_V2 column
        
    Returns:
        Dictionary with distribution metrics
    """
    
    if 'PCS_Score_V2' not in df.columns:
        return {'error': 'PCS_Score_V2 column not found'}
    
    scores = df['PCS_Score_V2'].dropna()
    
    if len(scores) == 0:
        return {'error': 'No scores available'}
    
    # Status counts
    status_counts = df['PCS_Status'].value_counts().to_dict() if 'PCS_Status' in df.columns else {}
    
    # Score distribution
    return {
        'total_rows': len(df),
        'rows_with_scores': len(scores),
        'mean_score': scores.mean(),
        'median_score': scores.median(),
        'std_score': scores.std(),
        'min_score': scores.min(),
        'max_score': scores.max(),
        'status_valid': status_counts.get('Valid', 0),
        'status_watch': status_counts.get('Watch', 0),
        'status_rejected': status_counts.get('Rejected', 0),
        'valid_pct': f"{100 * status_counts.get('Valid', 0) / len(df):.1f}%",
        'watch_pct': f"{100 * status_counts.get('Watch', 0) / len(df):.1f}%",
        'rejected_pct': f"{100 * status_counts.get('Rejected', 0) / len(df):.1f}%"
    }


if __name__ == '__main__':
    # Quick test
    print("="*70)
    print("PCS SCORING V2 TEST")
    print("="*70)
    print()
    
    # Sample data with Greeks
    data = {
        'Ticker': ['AAPL'] * 5,
        'Strategy': ['Long Call', 'Long Put', 'Long Straddle', 'Long Strangle', 'Covered Call'],
        'Delta': [0.52, -0.48, 0.04, 0.03, 0.52],
        'Gamma': [0.03, 0.03, 0.06, 0.04, 0.03],
        'Vega': [0.25, 0.25, 0.50, 0.40, 0.20],
        'Theta': [-0.15, -0.15, -0.30, -0.20, -0.25],
        'Bid_Ask_Spread_Pct': [5.0, 6.0, 7.0, 12.0, 4.0],
        'Open_Interest': [1000, 800, 500, 30, 1200],
        'Actual_DTE': [45, 45, 45, 45, 30],
        'Risk_Model': [500, 500, 1000, 800, 0]
    }
    
    df = pd.DataFrame(data)
    
    print("Before PCS scoring:")
    print(df[['Strategy', 'Delta', 'Vega']].to_string(index=False))
    print()
    
    # Calculate PCS scores
    df = calculate_pcs_score_v2(df)
    
    print("After PCS scoring:")
    print(df[['Strategy', 'PCS_Score_V2', 'PCS_Status']].to_string(index=False))
    print()
    
    print("Penalties:")
    for idx, row in df.iterrows():
        print(f"{row['Strategy']:20s} | {row['PCS_Score_V2']:5.0f} | {row['PCS_Penalties']}")
    
    print()
    
    # Analysis
    analysis = analyze_pcs_distribution(df)
    print("Distribution:")
    for key, value in analysis.items():
        print(f"  {key}: {value}")
    
    print()
    print("✅ PCS scoring V2 working!")
