"""
Exit Recommendation Engine (Persona-Aware)

Combines structural drift + chart context + risk scoring to generate
actionable exit/hold/adjust recommendations.

Enhanced with persona-specific triggers:
- INCOME: profit_target_50pct, assignment_risk, theta_exhaustion
- NEUTRAL_VOL: iv_collapse, vega_decay, profit_target_50pct
- DIRECTIONAL: profit_target_100pct, chart_breakdown, gamma_decay_75pct

Decision Logic:
- Profit targets: ROI >= target for strategy
- Stop loss: Unrealized P&L below threshold
- Greek deterioration: PCS_Drift < -10 or Gamma decay >75%
- IV collapse: IV_Rank_Drift < -25
- Chart breakdown: Bullish position + Bearish chart regime
- Assignment risk: Short options moving ITM
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List

logger = logging.getLogger(__name__)

# Persona profiles for trigger keywords
PERSONA_TRIGGERS = {
    'INCOME': {
        'profit_target': 0.50,
        'keywords': ['profit_target_50pct', 'assignment_risk', 'theta_exhaustion'],
        'primary_metrics': ['Theta_Efficiency', 'ROI', 'Assignment_Risk'],
    },
    'NEUTRAL_VOL': {
        'profit_target': 0.50,
        'keywords': ['iv_collapse', 'vega_decay', 'profit_target_50pct'],
        'primary_metrics': ['IV_Rank_Drift', 'Vega', 'Gamma'],
    },
    'DIRECTIONAL': {
        'profit_target': 1.00,
        'keywords': ['profit_target_100pct', 'chart_breakdown', 'gamma_decay_75pct'],
        'primary_metrics': ['Delta', 'Gamma', 'Chart_Regime'],
    },
}


def compute_exit_recommendations(
    df: pd.DataFrame,
    profit_targets: Dict[str, float] = None,
    stop_loss_pct: float = -0.20
) -> pd.DataFrame:
    """
    Generate exit recommendations based on multiple factors.
    
    Args:
        df: Positions with Entry, Current, Drift, and Chart data
        profit_targets: Strategy-specific profit targets (e.g., {'CSP': 0.50})
        stop_loss_pct: Universal stop loss threshold (e.g., -0.20 = -20%)
        
    Returns:
        DataFrame with added columns:
            - Recommendation (CLOSE/HOLD/ROLL/ADJUST)
            - Urgency (HIGH/MEDIUM/LOW)
            - Exit_Rationale (why close/hold)
            - Expected_Outcome (profit/loss estimate)
    
    Recommendation Criteria:
        CLOSE triggers:
            - Profit target hit
            - Stop loss triggered
            - PCS_Drift < -15 (severe deterioration)
            - IV_Rank_Drift < -30 + Chart breakdown
            - Assignment risk HIGH + Days_To_Expiration < 7
            
        HOLD criteria:
            - Position performing as expected (PCS_Drift > -5)
            - Chart aligned with thesis
            - No critical risk signals
            
        ROLL triggers:
            - DTE < 7 + Position profitable
            - Can extend duration with favorable pricing
            
        ADJUST triggers:
            - Delta exposure too high
            - One leg underperforming
    """
    df = df.copy()
    
    if profit_targets is None:
        profit_targets = {
            'CSP': 0.50,  # 50% of max profit
            'Covered Call': 0.50,
            'Bull Put Spread': 0.50,
            'Bear Call Spread': 0.50,
            'Buy Call': 1.00,  # 100% gain
            'Buy Put': 1.00,
        }
    
    logger.info(f"Computing exit recommendations for {len(df)} positions")
    
    recommendations = []
    urgencies = []
    rationales = []
    expected_outcomes = []
    
    for idx, row in df.iterrows():
        rec, urgency, rationale, outcome = _evaluate_position(
            row, profit_targets, stop_loss_pct
        )
        recommendations.append(rec)
        urgencies.append(urgency)
        rationales.append(rationale)
        expected_outcomes.append(outcome)
    
    df['Recommendation'] = recommendations
    df['Urgency'] = urgencies
    df['Exit_Rationale'] = rationales
    df['Expected_Outcome'] = expected_outcomes
    
    # Log recommendation summary
    rec_counts = df['Recommendation'].value_counts()
    logger.info(f"Recommendation summary: {rec_counts.to_dict()}")
    
    high_urgency = (df['Urgency'] == 'HIGH').sum()
    if high_urgency > 0:
        logger.warning(f"🚨 {high_urgency} positions require HIGH urgency action")
    
    return df


def _evaluate_position(
    row: pd.Series,
    profit_targets: Dict[str, float],
    stop_loss_pct: float
) -> tuple:
    """Evaluate single position and return (recommendation, urgency, rationale, outcome)."""
    
    rationale_parts = []
    rec = 'HOLD'
    urgency = 'LOW'
    outcome = 'monitor'
    
    strategy = row.get('Strategy', 'Unknown')
    roi_current = row.get('ROI_Current', 0.0)
    unrealized_pnl = row.get('Unrealized_PnL', 0.0)
    pcs_drift = row.get('PCS_Drift', 0.0)
    iv_rank_drift = row.get('IV_Rank_Drift', 0.0)
    gamma_drift_pct = row.get('Gamma_Drift_Pct', 0.0)
    vega_drift_pct = row.get('Vega_Drift_Pct', 0.0)
    dte = row.get('DTE', 999)
    chart_regime = row.get('Chart_Regime', 'Unknown')
    drift_severity = row.get('Drift_Severity', 'LOW')
    
    # Persona-specific logic
    entry_profile = row.get('Entry_PCS_Profile', 'Other')
    persona_config = PERSONA_TRIGGERS.get(entry_profile, None)
    
    # === PERSONA-SPECIFIC TRIGGERS ===
    
    # INCOME persona triggers
    if entry_profile == 'INCOME':
        # Theta exhaustion check
        theta_eff = row.get('Theta_Efficiency', 0.0)
        if pd.notna(theta_eff) and theta_eff < 0.005:  # Less than 0.5% daily decay
            rationale_parts.append('theta_exhaustion: Daily decay below 0.5% of premium')
            urgency = 'MEDIUM' if urgency == 'LOW' else urgency
        
        # Assignment risk check (ITM + near expiration)
        assignment_risk = row.get('Assignment_Risk', 'LOW')
        if assignment_risk in ['HIGH', 'CRITICAL'] and dte < 7:
            rec = 'CLOSE'
            urgency = 'HIGH'
            rationale_parts.append(f'assignment_risk: {assignment_risk} risk with {dte} days remaining')
            outcome = 'assignment_avoidance'
    
    # NEUTRAL_VOL persona triggers
    elif entry_profile == 'NEUTRAL_VOL':
        # IV collapse check
        if pd.notna(iv_rank_drift) and iv_rank_drift < -30:
            rec = 'CLOSE'
            urgency = 'HIGH'
            rationale_parts.append(f'iv_collapse: IV_Rank dropped {iv_rank_drift:.0f} points')
            outcome = 'vol_collapse_exit'
        
        # Vega decay check
        if pd.notna(vega_drift_pct) and vega_drift_pct < -50:
            rationale_parts.append(f'vega_decay: Vega deteriorated {vega_drift_pct:.0f}%')
            urgency = 'MEDIUM' if urgency == 'LOW' else urgency
    
    # DIRECTIONAL persona triggers
    elif entry_profile == 'DIRECTIONAL':
        # Chart breakdown check
        if chart_regime in ['Bearish', 'Breakdown']:
            rec = 'CLOSE'
            urgency = 'HIGH'
            rationale_parts.append(f'chart_breakdown: Regime shifted to {chart_regime}')
            outcome = 'thesis_invalidated'
        
        # Severe gamma decay (75% threshold for directional)
        if pd.notna(gamma_drift_pct) and gamma_drift_pct < -75:
            rec = 'CLOSE'
            urgency = 'HIGH'
            rationale_parts.append(f'gamma_decay_75pct: Gamma collapsed {gamma_drift_pct:.0f}%')
            outcome = 'gamma_exhaustion_exit'
    
    # === UNIVERSAL TRIGGERS (All personas) ===
    
    # 1. Profit target hit (persona-specific targets)
    if persona_config:
        target = persona_config['profit_target']
        target_keyword = f"profit_target_{int(target*100)}pct"
    else:
        target = profit_targets.get(strategy, 0.50)
        target_keyword = f"profit_target_{int(target*100)}pct"
    
    if pd.notna(roi_current) and roi_current >= target:
        rec = 'CLOSE'
        urgency = 'MEDIUM'
        rationale_parts.append(f'{target_keyword}: Target hit ({roi_current:.1%} >= {target:.1%})')
        outcome = 'take_profit'
    
    # 2. Stop loss triggered
    if pd.notna(unrealized_pnl) and pd.notna(row.get('Capital_Deployed')) and row.get('Capital_Deployed', 0) > 0:
        loss_pct = unrealized_pnl / row['Capital_Deployed']
        if loss_pct <= stop_loss_pct:
            rec = 'CLOSE'
            urgency = 'HIGH'
            rationale_parts.append(f'Stop loss triggered ({loss_pct:.1%})')
            outcome = 'cut_loss'
    
    # 3. Severe PCS deterioration
    if pd.notna(pcs_drift) and pcs_drift < -15:
        rec = 'CLOSE'
        urgency = 'HIGH'
        rationale_parts.append(f'Severe quality deterioration (PCS_Drift={pcs_drift:.1f})')
        outcome = 'deterioration_exit'
    
    # 4. IV collapse (if not already handled by persona logic)
    if entry_profile not in ['NEUTRAL_VOL'] and pd.notna(iv_rank_drift) and iv_rank_drift < -30:
        if chart_regime in ['Bearish', 'Breakdown']:
            rec = 'CLOSE'
            urgency = 'HIGH'
            rationale_parts.append(f'IV collapse ({iv_rank_drift:.0f}) + Chart breakdown')
            outcome = 'vol_collapse_exit'
        else:
            rationale_parts.append(f'IV collapsed ({iv_rank_drift:.0f}), watch chart')
            urgency = 'MEDIUM' if urgency == 'LOW' else urgency
    
    # 5. Rapid gamma decay (if not already handled by DIRECTIONAL persona)
    if entry_profile not in ['DIRECTIONAL'] and pd.notna(gamma_drift_pct) and gamma_drift_pct < -75:
        rationale_parts.append(f'Gamma collapsed ({gamma_drift_pct:.0f}%)')
        urgency = 'MEDIUM' if urgency == 'LOW' else urgency
        if rec == 'HOLD':
            rec = 'CLOSE'
            outcome = 'gamma_decay_exit'
    
    # === ROLL TRIGGERS ===
    
    # Approaching expiration + profitable
    if dte < 7 and pd.notna(roi_current) and roi_current > 0:
        if rec == 'HOLD':
            rec = 'ROLL'
            urgency = 'MEDIUM'
            rationale_parts.append(f'DTE={dte}, position profitable, consider roll')
            outcome = 'roll_forward'
    
    # === HOLD CRITERIA ===
    
    if rec == 'HOLD':
        if pd.notna(pcs_drift) and pcs_drift > -5:
            rationale_parts.append('Position performing as expected')
        else:
            rationale_parts.append('Monitoring for deterioration')
    
    # === DRIFT SEVERITY OVERRIDE ===
    
    if drift_severity == 'CRITICAL' and rec == 'HOLD':
        rec = 'CLOSE'
        urgency = 'HIGH'
        rationale_parts.append('Critical drift severity detected')
        outcome = 'risk_exit'
    
    rationale = '; '.join(rationale_parts) if rationale_parts else 'No action needed'
    
    return rec, urgency, rationale, outcome


def prioritize_recommendations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sort positions by urgency and expected impact.
    
    Priority order:
        1. HIGH urgency + Negative outcome (stop losses)
        2. HIGH urgency + Positive outcome (profit targets)
        3. MEDIUM urgency + Large positions
        4. LOW urgency
    """
    df = df.copy()
    
    # Create priority score
    urgency_scores = {'HIGH': 3, 'MEDIUM': 2, 'LOW': 1}
    df['Urgency_Score'] = df['Urgency'].map(urgency_scores).fillna(0)
    
    # Add capital weight
    if 'Capital_Deployed' in df.columns:
        df['Capital_Weight'] = df['Capital_Deployed'] / df['Capital_Deployed'].sum()
    else:
        df['Capital_Weight'] = 1.0 / len(df)
    
    # Sort by urgency (desc) then capital weight (desc)
    df = df.sort_values(['Urgency_Score', 'Capital_Weight'], ascending=[False, False])
    
    logger.info("Recommendations prioritized by urgency and capital weight")
    return df
