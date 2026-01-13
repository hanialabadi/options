"""
Position Health Monitoring

Computes real-time health scores and generates alerts based on:
- PCS drift and deterioration
- Greek decay (especially Gamma/Vega)
- IV collapse
- P&L performance vs expectations
- Assignment risk
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)


def compute_position_health_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute 0-100 health score for each position.
    
    Factors:
    - PCS maintenance (40%): How well position maintains Entry_PCS
    - Greek stability (30%): Gamma/Vega decay rate
    - P&L trajectory (20%): Unrealized vs expected
    - Risk factors (10%): Assignment, IV collapse, DTE decay
    
    Returns:
        DataFrame with Health_Score column (0-100)
    """
    df = df.copy()
    
    logger.info("Computing position health scores...")
    
    # Component 1: PCS Maintenance (40 points)
    pcs_score = _score_pcs_maintenance(df)
    
    # Component 2: Greek Stability (30 points)
    greek_score = _score_greek_stability(df)
    
    # Component 3: P&L Trajectory (20 points)
    pnl_score = _score_pnl_trajectory(df)
    
    # Component 4: Risk Factors (10 points)
    risk_score = _score_risk_factors(df)
    
    # Combined health score
    df['Health_Score'] = (
        pcs_score * 0.40 +
        greek_score * 0.30 +
        pnl_score * 0.20 +
        risk_score * 0.10
    ).clip(0, 100)
    
    # Health tier
    df['Health_Tier'] = pd.cut(
        df['Health_Score'],
        bins=[-np.inf, 40, 60, 80, np.inf],
        labels=['CRITICAL', 'POOR', 'FAIR', 'GOOD']
    )
    
    logger.info(f"✅ Health scores computed")
    logger.info(f"Health distribution:\n{df['Health_Tier'].value_counts()}")
    
    return df


def _score_pcs_maintenance(df: pd.DataFrame) -> pd.Series:
    """Score how well position maintains Entry_PCS (0-100)."""
    if 'PCS_Drift' not in df.columns:
        return pd.Series(50.0, index=df.index)  # Neutral if no drift data
    
    # Score based on PCS_Drift
    # No drift = 100, -5 = 90, -10 = 70, -15 = 40, -20+ = 0
    score = 100 + (df['PCS_Drift'] * 4)  # Linear decay: -25 drift = 0 score
    return score.clip(0, 100)


def _score_greek_stability(df: pd.DataFrame) -> pd.Series:
    """Score Greek decay rate (0-100)."""
    scores = []
    
    # Gamma decay
    if 'Gamma_Drift_Pct' in df.columns:
        gamma_score = 100 + (df['Gamma_Drift_Pct'])  # -50% = 50 score, -100% = 0
        scores.append(gamma_score.clip(0, 100))
    
    # Vega decay
    if 'Vega_Drift_Pct' in df.columns:
        vega_score = 100 + (df['Vega_Drift_Pct'])
        scores.append(vega_score.clip(0, 100))
    
    if scores:
        return pd.concat(scores, axis=1).mean(axis=1)
    else:
        return pd.Series(50.0, index=df.index)


def _score_pnl_trajectory(df: pd.DataFrame) -> pd.Series:
    """Score P&L performance (0-100)."""
    if 'Unrealized_PnL' not in df.columns or 'Basis' not in df.columns:
        return pd.Series(50.0, index=df.index)
    
    # ROI-based scoring
    roi = (df['Unrealized_PnL'] / df['Basis'].replace(0, np.nan)) * 100
    
    # Score: 0% = 50, 10% = 70, 25% = 90, 50% = 100
    # Negative: -10% = 30, -20% = 0
    score = 50 + (roi * 2)  # Linear: 50% ROI = 150 (capped at 100)
    return score.clip(0, 100)


def _score_risk_factors(df: pd.DataFrame) -> pd.Series:
    """Score risk exposure (0-100, lower risk = higher score)."""
    score = pd.Series(100.0, index=df.index)  # Start at max
    
    # Penalty for assignment risk
    if 'Moneyness' in df.columns and 'DTE' in df.columns:
        itm_short_dte = (df['Moneyness'].isin(['ITM', 'Deep_ITM'])) & (df['DTE'] < 7)
        score = score.where(~itm_short_dte, score - 30)
    
    # Penalty for IV collapse
    if 'IV_Rank_Drift' in df.columns:
        iv_collapsed = df['IV_Rank_Drift'] < -30
        score = score.where(~iv_collapsed, score - 20)
    
    # Penalty for expiry proximity
    if 'DTE' in df.columns:
        imminent_expiry = df['DTE'] <= 3
        score = score.where(~imminent_expiry, score - 25)
    
    return score.clip(0, 100)


def generate_alerts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate alerts for positions requiring attention.
    
    Alert Types:
    - PCS_DETERIORATION: PCS_Drift < -10
    - GAMMA_DECAY: Gamma_Drift_Pct < -75%
    - IV_COLLAPSE: IV_Rank_Drift < -30
    - STOP_LOSS: Unrealized_PnL < -20% of Basis
    - ASSIGNMENT_RISK: ITM + DTE < 7
    - TAKE_PROFIT: ROI >= Target (strategy-specific)
    
    Returns:
        DataFrame with Alert_Type, Alert_Message, Urgency columns
    """
    df = df.copy()
    
    logger.info("Generating position alerts...")
    
    alerts_list = []
    
    for idx, row in df.iterrows():
        position_alerts = _check_position_alerts(row)
        
        if position_alerts:
            # Attach to each alert
            for alert in position_alerts:
                alert_row = row.to_dict()
                alert_row.update(alert)
                alerts_list.append(alert_row)
    
    if alerts_list:
        df_alerts = pd.DataFrame(alerts_list)
        logger.info(f"⚠️  Generated {len(df_alerts)} alerts for {len(set(df_alerts['TradeID']))} positions")
        
        # Summary by type
        alert_summary = df_alerts['Alert_Type'].value_counts()
        for alert_type, count in alert_summary.items():
            logger.info(f"   {alert_type}: {count}")
        
        return df_alerts
    else:
        logger.info("✅ No alerts generated - all positions healthy")
        return pd.DataFrame()


def _check_position_alerts(row: pd.Series) -> List[Dict]:
    """Check all alert conditions for a single position."""
    alerts = []
    
    # 1. PCS Deterioration
    if row.get('PCS_Drift', 0) < -10:
        alerts.append({
            'Alert_Type': 'PCS_DETERIORATION',
            'Alert_Message': f"PCS dropped {row['PCS_Drift']:.1f} points from entry",
            'Urgency': classify_urgency(row, 'PCS_DETERIORATION'),
            'Recommended_Action': 'Review position quality - consider exit or adjustment'
        })
    
    # 2. Gamma Decay
    if row.get('Gamma_Drift_Pct', 0) < -75:
        alerts.append({
            'Alert_Type': 'GAMMA_DECAY',
            'Alert_Message': f"Gamma decayed {row['Gamma_Drift_Pct']:.0f}% from entry",
            'Urgency': 'MEDIUM',
            'Recommended_Action': 'Gamma position exhausted - limited upside remaining'
        })
    
    # 3. IV Collapse
    if row.get('IV_Rank_Drift', 0) < -30:
        alerts.append({
            'Alert_Type': 'IV_COLLAPSE',
            'Alert_Message': f"IV_Rank dropped {row['IV_Rank_Drift']:.0f} points",
            'Urgency': 'HIGH',
            'Recommended_Action': 'Volatility premium collapsed - exit or roll'
        })
    
    # 4. Stop Loss
    basis = row.get('Basis', 0)
    pnl = row.get('Unrealized_PnL', 0)
    if basis > 0:
        roi_pct = (pnl / basis) * 100
        if roi_pct < -20:
            alerts.append({
                'Alert_Type': 'STOP_LOSS',
                'Alert_Message': f"Down {roi_pct:.1f}% - stop loss threshold breached",
                'Urgency': 'HIGH',
                'Recommended_Action': 'CLOSE POSITION - cut losses'
            })
    
    # 5. Assignment Risk
    if row.get('Moneyness', '') in ['ITM', 'Deep_ITM'] and row.get('DTE', 999) < 7:
        alerts.append({
            'Alert_Type': 'ASSIGNMENT_RISK',
            'Alert_Message': f"{row.get('Moneyness')} with {row.get('DTE')} DTE",
            'Urgency': 'HIGH',
            'Recommended_Action': 'Close or roll to avoid assignment'
        })
    
    # 6. Take Profit (strategy-specific)
    profit_alert = _check_profit_target(row)
    if profit_alert:
        alerts.append(profit_alert)
    
    return alerts


def _check_profit_target(row: pd.Series) -> Dict:
    """Check if position hit profit target (strategy-specific)."""
    basis = row.get('Basis', 0)
    pnl = row.get('Unrealized_PnL', 0)
    strategy = row.get('Strategy', '')
    
    if basis <= 0:
        return None
    
    roi_pct = (pnl / basis) * 100
    
    # Strategy-specific targets
    targets = {
        'CSP': 50,
        'Covered_Call': 50,
        'Credit_Spread': 50,
        'Iron_Condor': 50,
        'Buy_Call': 100,
        'Buy_Put': 100,
    }
    
    target = targets.get(strategy, 50)
    
    if roi_pct >= target:
        return {
            'Alert_Type': 'TAKE_PROFIT',
            'Alert_Message': f"Up {roi_pct:.1f}% - target {target}% achieved",
            'Urgency': 'LOW',
            'Recommended_Action': f'Consider closing - {target}% target hit'
        }
    
    return None


def classify_urgency(row: pd.Series, alert_type: str) -> str:
    """
    Classify alert urgency (HIGH/MEDIUM/LOW).
    
    HIGH: Immediate action required (stop loss, assignment)
    MEDIUM: Action needed soon (PCS deterioration, Greek decay)
    LOW: Informational (profit target)
    """
    if alert_type in ['STOP_LOSS', 'ASSIGNMENT_RISK']:
        return 'HIGH'
    
    if alert_type == 'TAKE_PROFIT':
        return 'LOW'
    
    # For PCS/Greek/IV alerts, check severity
    if alert_type == 'PCS_DETERIORATION':
        drift = row.get('PCS_Drift', 0)
        if drift < -15:
            return 'HIGH'
        elif drift < -10:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    if alert_type == 'IV_COLLAPSE':
        iv_drift = row.get('IV_Rank_Drift', 0)
        if iv_drift < -40:
            return 'HIGH'
        else:
            return 'MEDIUM'
    
    return 'MEDIUM'
