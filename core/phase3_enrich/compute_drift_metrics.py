"""
Drift Analysis Module

Computes drift metrics between Entry baseline and Current snapshots.
Enables time-series analysis for performance attribution and deterioration detection.

Architecture:
- Entry baseline comes from Cycle 2 (freeze_entry_data)
- Current values from Cycle 1 (perception loop snapshots)
- Drift = Current - Entry (for each metric)

Output used by:
- Cycle 3: Exit recommendations (drift-based triggers)
- ML Loop: Evolution features for training
"""

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


def compute_drift_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate drift between Entry baseline and Current snapshot values.
    
    Args:
        df: DataFrame with both Entry_* and Current columns
        
    Returns:
        DataFrame with added *_Drift columns
        
    Drift Metrics Computed:
        - Delta_Drift = Delta - Delta_Entry
        - Gamma_Drift = Gamma - Gamma_Entry
        - Vega_Drift = Vega - Vega_Entry
        - Theta_Drift = Theta - Theta_Entry (typically negative)
        - IV_Rank_Drift = IV_Rank - Entry_IV_Rank
        - PCS_Drift = PCS - Entry_PCS
        - Moneyness_Migration = Moneyness_Pct - Entry_Moneyness_Pct
        - DTE_Decay = DTE - Entry_DTE (negative = time passed)
        
    Phase Alignment:
        - Requires: Cycle 2 complete (Entry data frozen)
        - Used by: Cycle 3 (recommendations), ML training
        - Not in: Cycles 1-2 (perception/freeze don't need drift)
    """
    df = df.copy()
    
    logger.info(f"Computing drift metrics for {len(df)} positions")
    
    # Greek drift
    drift_configs = [
        ('Delta', 'Delta_Entry', 'Delta_Drift'),
        ('Gamma', 'Gamma_Entry', 'Gamma_Drift'),
        ('Vega', 'Vega_Entry', 'Vega_Drift'),
        ('Theta', 'Theta_Entry', 'Theta_Drift'),
        ('Rho', 'Rho_Entry', 'Rho_Drift'),
    ]
    
    for current_col, entry_col, drift_col in drift_configs:
        if current_col in df.columns and entry_col in df.columns:
            df[drift_col] = df[current_col] - df[entry_col]
        else:
            df[drift_col] = np.nan
    
    # IV drift
    if 'IV_Rank' in df.columns and 'Entry_IV_Rank' in df.columns:
        df['IV_Rank_Drift'] = df['IV_Rank'] - df['Entry_IV_Rank']
    else:
        df['IV_Rank_Drift'] = np.nan
    
    # PCS drift (quality evolution)
    if 'PCS' in df.columns and 'Entry_PCS' in df.columns:
        df['PCS_Drift'] = df['PCS'] - df['Entry_PCS']
    else:
        df['PCS_Drift'] = np.nan
    
    # Moneyness migration (distance from entry)
    if 'Moneyness_Pct' in df.columns and 'Entry_Moneyness_Pct' in df.columns:
        df['Moneyness_Migration'] = df['Moneyness_Pct'] - df['Entry_Moneyness_Pct']
    else:
        df['Moneyness_Migration'] = np.nan
    
    # DTE decay (time passage)
    if 'DTE' in df.columns and 'Entry_DTE' in df.columns:
        df['DTE_Decay'] = df['DTE'] - df['Entry_DTE']  # Negative value
    else:
        df['DTE_Decay'] = np.nan
    
    # Compute relative drift percentages for Greeks
    for greek in ['Delta', 'Gamma', 'Vega']:
        drift_col = f'{greek}_Drift'
        entry_col = f'{greek}_Entry'
        pct_col = f'{greek}_Drift_Pct'
        
        if drift_col in df.columns and entry_col in df.columns:
            # Avoid division by zero
            mask = (df[entry_col] != 0) & (df[entry_col].notna())
            df.loc[mask, pct_col] = (df.loc[mask, drift_col] / df.loc[mask, entry_col].abs()) * 100
            df.loc[~mask, pct_col] = np.nan
        else:
            df[pct_col] = np.nan
    
    # Log drift summary statistics
    if 'PCS_Drift' in df.columns:
        valid_pcs_drift = df['PCS_Drift'].dropna()
        if len(valid_pcs_drift) > 0:
            logger.info(
                f"PCS_Drift summary: mean={valid_pcs_drift.mean():.1f}, "
                f"deteriorating={( valid_pcs_drift < -5).sum()}, "
                f"improving={(valid_pcs_drift > 5).sum()}"
            )
    
    if 'Gamma_Drift_Pct' in df.columns:
        valid_gamma_drift = df['Gamma_Drift_Pct'].dropna()
        if len(valid_gamma_drift) > 0:
            logger.info(
                f"Gamma_Drift_Pct summary: mean={valid_gamma_drift.mean():.1f}%, "
                f"rapid decay (<-50%)={( valid_gamma_drift < -50).sum()}"
            )
    
    return df


def classify_drift_severity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Classify drift severity for risk management.
    
    Adds columns:
        - Drift_Severity: LOW/MEDIUM/HIGH/CRITICAL
        - Drift_Flags: List of specific deterioration signals
        
    Severity Criteria:
        LOW: Normal time decay, minor drift
        MEDIUM: Moderate drift, watch closely
        HIGH: Significant deterioration, consider action
        CRITICAL: Severe deterioration, immediate attention
    """
    df = df.copy()
    
    df['Drift_Severity'] = 'LOW'
    df['Drift_Flags'] = ''
    
    flags_list = []
    
    for idx, row in df.iterrows():
        flags = []
        severity = 'LOW'
        
        # PCS deterioration check
        if pd.notna(row.get('PCS_Drift')):
            if row['PCS_Drift'] < -15:
                flags.append('PCS_Collapse')
                severity = 'CRITICAL'
            elif row['PCS_Drift'] < -10:
                flags.append('PCS_Severe_Drop')
                severity = 'HIGH'
            elif row['PCS_Drift'] < -5:
                flags.append('PCS_Deteriorating')
                severity = 'MEDIUM' if severity == 'LOW' else severity
        
        # Gamma decay check
        if pd.notna(row.get('Gamma_Drift_Pct')):
            if row['Gamma_Drift_Pct'] < -75:
                flags.append('Gamma_Collapse')
                severity = 'HIGH' if severity in ['LOW', 'MEDIUM'] else severity
            elif row['Gamma_Drift_Pct'] < -50:
                flags.append('Gamma_Rapid_Decay')
                severity = 'MEDIUM' if severity == 'LOW' else severity
        
        # IV collapse check
        if pd.notna(row.get('IV_Rank_Drift')):
            if row['IV_Rank_Drift'] < -30:
                flags.append('IV_Collapse')
                severity = 'HIGH' if severity in ['LOW', 'MEDIUM'] else severity
            elif row['IV_Rank_Drift'] < -20:
                flags.append('IV_Sharp_Drop')
                severity = 'MEDIUM' if severity == 'LOW' else severity
        
        # Moneyness risk check (moving ITM for short positions)
        if pd.notna(row.get('Moneyness_Migration')):
            strategy = row.get('Strategy', '')
            if 'CSP' in strategy or 'Covered Call' in strategy:
                if row['Moneyness_Migration'] < -10:  # Moving 10% toward money
                    flags.append('Assignment_Risk_Increasing')
                    severity = 'HIGH' if severity in ['LOW', 'MEDIUM'] else severity
        
        df.at[idx, 'Drift_Severity'] = severity
        df.at[idx, 'Drift_Flags'] = ', '.join(flags) if flags else 'None'
    
    # Log severity distribution
    severity_counts = df['Drift_Severity'].value_counts()
    logger.info(f"Drift severity distribution: {severity_counts.to_dict()}")
    
    if 'CRITICAL' in severity_counts:
        critical_symbols = df[df['Drift_Severity'] == 'CRITICAL']['Symbol'].tolist()
        logger.warning(f"🚨 CRITICAL drift detected in: {', '.join(critical_symbols)}")
    
    return df


def compute_performance_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute performance metrics vs entry baseline.
    
    Adds columns:
        - ROI_vs_Entry: Current ROI change from entry
        - Theta_Efficiency: Actual P&L / Expected theta decay
        - Max_Favorable_Excursion: Best P&L seen (from history)
        - Max_Adverse_Excursion: Worst P&L seen (from history)
    """
    df = df.copy()
    
    # ROI change
    if 'ROI_Current' in df.columns and 'ROI_Entry' in df.columns:
        df['ROI_vs_Entry'] = df['ROI_Current'] - df['ROI_Entry']
    else:
        df['ROI_vs_Entry'] = np.nan
    
    # Theta efficiency (requires P&L attribution data)
    if all(col in df.columns for col in ['Unrealized_PnL', 'Theta_Entry', 'Days_In_Trade']):
        expected_theta_decay = df['Theta_Entry'] * df['Days_In_Trade']
        # Avoid division by zero
        mask = (expected_theta_decay != 0) & (expected_theta_decay.notna())
        df.loc[mask, 'Theta_Efficiency'] = df.loc[mask, 'Unrealized_PnL'] / expected_theta_decay.abs()
        df.loc[~mask, 'Theta_Efficiency'] = np.nan
    else:
        df['Theta_Efficiency'] = np.nan
    
    logger.info("Performance metrics computed")
    
    return df
