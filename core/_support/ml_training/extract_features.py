"""
ML Feature Engineering

Extracts features from completed trades for model training.

Feature Categories:
1. Entry Features: Structural quality at entry
2. Evolution Features: How position changed over time
3. Context Features: Market conditions at entry/exit
4. Outcome Labels: Win/loss, P&L, exit reason
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List

logger = logging.getLogger(__name__)


def extract_training_features(
    df_history: pd.DataFrame,
    df_outcomes: pd.DataFrame
) -> pd.DataFrame:
    """
    Extract ML training features from trade history.
    
    Args:
        df_history: Full snapshot history for completed trades
        df_outcomes: Exit outcomes from extract_exit_outcomes()
        
    Returns:
        DataFrame with one row per trade, columns:
            - Entry features (Entry_PCS, Entry_Greeks, etc.)
            - Evolution features (Drift metrics, trajectories)
            - Context features (Chart regime, IV_Rank trends)
            - Outcome labels (Win_Loss, Exit_PnL, etc.)
    """
    if df_history.empty or df_outcomes.empty:
        logger.warning("Empty input data for feature extraction")
        return pd.DataFrame()
    
    logger.info(f"Extracting features for {len(df_outcomes)} completed trades")
    
    features_list = []
    
    for _, outcome_row in df_outcomes.iterrows():
        trade_id = outcome_row['TradeID']
        df_trade = df_history[df_history['TradeID'] == trade_id].sort_values('Snapshot_TS')
        
        if len(df_trade) == 0:
            continue
        
        # Extract entry snapshot features
        entry_features = _extract_entry_features(df_trade.iloc[0])
        
        # Extract evolution features
        evolution_features = _extract_evolution_features(df_trade)
        
        # Extract context features
        context_features = _extract_context_features(df_trade)
        
        # Combine with outcome
        trade_features = {
            **entry_features,
            **evolution_features,
            **context_features,
            **outcome_row.to_dict()
        }
        
        features_list.append(trade_features)
    
    df_features = pd.DataFrame(features_list)
    
    logger.info(f"✅ Extracted {len(df_features.columns)} features for {len(df_features)} trades")
    return df_features


def _extract_entry_features(entry_row: pd.Series) -> Dict:
    """Extract features from entry snapshot."""
    features = {}
    
    # Entry PCS and subscores
    entry_cols = [
        'Entry_PCS', 'Entry_PCS_GammaScore', 'Entry_PCS_VegaScore', 'Entry_PCS_ROIScore',
        'Entry_PCS_Profile', 'Entry_PCS_Tier'
    ]
    for col in entry_cols:
        features[col] = entry_row.get(col, np.nan)
    
    # Entry Greeks
    greek_cols = ['Delta_Entry', 'Gamma_Entry', 'Vega_Entry', 'Theta_Entry', 'Rho_Entry']
    for col in greek_cols:
        features[col] = entry_row.get(col, np.nan)
    
    # Entry context
    context_cols = [
        'Entry_IV_Rank', 'Entry_Moneyness_Pct', 'Entry_DTE',
        'Premium_Entry', 'Basis', 'Capital_Deployed'
    ]
    for col in context_cols:
        features[col] = entry_row.get(col, np.nan)
    
    # Strategy and structural
    features['Strategy'] = entry_row.get('Strategy', 'Unknown')
    features['Symbol'] = entry_row.get('Symbol', 'Unknown')
    features['AssetType'] = entry_row.get('AssetType', 'Unknown')
    
    return features


def _extract_evolution_features(df_trade: pd.DataFrame) -> Dict:
    """Extract evolution trajectory features."""
    features = {}
    
    if len(df_trade) < 2:
        # Not enough history for evolution
        return {
            'Days_In_Trade': 0,
            'Avg_PCS_Drift': 0.0,
            'Avg_Gamma_Drift_Pct': 0.0,
            'Avg_IV_Rank_Drift': 0.0,
            'Peak_PnL': 0.0,
            'Trough_PnL': 0.0,
        }
    
    # Time-series metrics
    features['Days_In_Trade'] = (
        pd.Timestamp(df_trade.iloc[-1]['Snapshot_TS']) -
        pd.Timestamp(df_trade.iloc[0]['Snapshot_TS'])
    ).days
    
    # Average drift values
    if 'PCS_Drift' in df_trade.columns:
        features['Avg_PCS_Drift'] = df_trade['PCS_Drift'].mean()
        features['Max_PCS_Drift'] = df_trade['PCS_Drift'].min()  # Most negative
    else:
        features['Avg_PCS_Drift'] = 0.0
        features['Max_PCS_Drift'] = 0.0
    
    if 'Gamma_Drift_Pct' in df_trade.columns:
        features['Avg_Gamma_Drift_Pct'] = df_trade['Gamma_Drift_Pct'].mean()
    else:
        features['Avg_Gamma_Drift_Pct'] = 0.0
    
    if 'IV_Rank_Drift' in df_trade.columns:
        features['Avg_IV_Rank_Drift'] = df_trade['IV_Rank_Drift'].mean()
        features['IV_Rank_Collapsed'] = (df_trade['IV_Rank_Drift'] < -25).any()
    else:
        features['Avg_IV_Rank_Drift'] = 0.0
        features['IV_Rank_Collapsed'] = False
    
    # P&L trajectory
    if 'Unrealized_PnL' in df_trade.columns:
        pnl_series = df_trade['Unrealized_PnL'].dropna()
        if len(pnl_series) > 0:
            features['Peak_PnL'] = pnl_series.max()
            features['Trough_PnL'] = pnl_series.min()
            features['PnL_Volatility'] = pnl_series.std()
        else:
            features['Peak_PnL'] = 0.0
            features['Trough_PnL'] = 0.0
            features['PnL_Volatility'] = 0.0
    
    return features


def _extract_context_features(df_trade: pd.DataFrame) -> Dict:
    """Extract chart and market context features."""
    features = {}
    
    entry_row = df_trade.iloc[0]
    exit_row = df_trade.iloc[-1]
    
    # Chart regime at entry and exit
    features['Entry_Chart_Regime'] = entry_row.get('Chart_Regime', 'Unknown')
    features['Exit_Chart_Regime'] = exit_row.get('Chart_Regime', 'Unknown')
    features['Chart_Regime_Changed'] = (
        features['Entry_Chart_Regime'] != features['Exit_Chart_Regime']
    )
    
    # Signal quality
    features['Entry_Signal_Type'] = entry_row.get('Signal_Type', 'Unknown')
    features['Entry_Days_Since_Cross'] = entry_row.get('Days_Since_Cross', np.nan)
    
    # IV context
    features['Entry_IV_Rank'] = entry_row.get('Entry_IV_Rank', np.nan)
    features['Exit_IV_Rank'] = exit_row.get('IV_Rank', np.nan)
    
    return features


def prepare_ml_dataset(
    df_features: pd.DataFrame,
    target_col: str = 'Win_Loss'
) -> tuple:
    """
    Prepare dataset for ML model training.
    
    Args:
        df_features: Features from extract_training_features()
        target_col: Column to use as target variable
        
    Returns:
        Tuple of (X, y, feature_names)
            - X: Feature matrix (numeric only)
            - y: Target labels
            - feature_names: List of feature column names
    """
    if df_features.empty:
        logger.warning("Empty feature dataset for ML preparation")
        return np.array([]), np.array([]), []
    
    # Separate numeric features from categorical
    numeric_cols = df_features.select_dtypes(include=[np.number]).columns.tolist()
    
    # Remove outcome columns from features
    outcome_cols = ['Win_Loss', 'Exit_PnL', 'Exit_ROI', 'TradeID', 'Entry_Date', 'Exit_Date']
    feature_cols = [col for col in numeric_cols if col not in outcome_cols]
    
    X = df_features[feature_cols].values
    y = df_features[target_col].values
    
    logger.info(f"✅ Prepared ML dataset: {X.shape[0]} samples, {X.shape[1]} features")
    logger.info(f"Target distribution: {pd.Series(y).value_counts().to_dict()}")
    
    return X, y, feature_cols
