"""
Cycle 3: Simulation Engine (The Cockpit)

Calculates projected P&L and Greek migration based on hypothetical price and volatility shifts.
Uses linear approximation (Delta/Vega/Gamma/Theta) for performance.
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class SimulationEngine:
    """
    Authoritative Simulation Engine for 'What-If' analysis.
    """

    @staticmethod
    def project_portfolio(df: pd.DataFrame, price_shift_pct: float, vol_shift_points: float) -> pd.DataFrame:
        """
        Project position states based on shifts.
        
        Args:
            df: Current positions dataframe
            price_shift_pct: Percentage shift in underlying price (e.g., 5.0 for +5%)
            vol_shift_points: Absolute shift in IV points (e.g., 2.0 for +2% IV)
            
        Returns:
            pd.DataFrame: Dataframe with simulated columns
        """
        if df.empty:
            return df
            
        sim_df = df.copy()
        
        # 1. Price Shift (Absolute)
        # Underlying_Price_Sim = UL_Last * (1 + price_shift_pct/100)
        if 'UL Last' in sim_df.columns:
            sim_df['Sim_UL_Price'] = sim_df['UL Last'] * (1 + price_shift_pct / 100.0)
            sim_df['Sim_Price_Delta_Abs'] = sim_df['Sim_UL_Price'] - sim_df['UL Last']
        else:
            sim_df['Sim_Price_Delta_Abs'] = 0.0

        # 2. Vol Shift (Decimal)
        sim_df['Sim_Vol_Delta_Dec'] = vol_shift_points / 100.0

        # 3. P&L Projection (Linear + Second Order)
        # PnL_Sim = Delta * dP + 0.5 * Gamma * dP^2 + Vega * dV + Theta * dT (dT=0 for instant sim)
        
        # Multipliers: Options 100x, Stocks 1x
        multipliers = 1.0
        if 'AssetType' in sim_df.columns:
            multipliers = np.where(sim_df['AssetType'] == 'OPTION', 100.0, 1.0)
            
        # Delta Component
        delta_pnl = sim_df.get('Delta', 0) * sim_df['Sim_Price_Delta_Abs'] * multipliers
        
        # Gamma Component (Second Order)
        gamma_pnl = 0.5 * sim_df.get('Gamma', 0) * (sim_df['Sim_Price_Delta_Abs'] ** 2) * multipliers
        
        # Vega Component
        vega_pnl = sim_df.get('Vega', 0) * sim_df['Sim_Vol_Delta_Dec'] * 100.0 * multipliers # Vega is per 1% vol
        
        sim_df['Sim_PnL_Delta'] = delta_pnl
        sim_df['Sim_PnL_Gamma'] = gamma_pnl
        sim_df['Sim_PnL_Vega'] = vega_pnl
        
        sim_df['Sim_PnL_Total'] = delta_pnl + gamma_pnl + vega_pnl
        
        # 4. Projected ROI
        if 'Capital_Deployed' in sim_df.columns:
            sim_df['Sim_ROI_Pct'] = (sim_df.get('PnL_Total', 0) + sim_df['Sim_PnL_Total']) / sim_df['Capital_Deployed'].replace(0, np.nan)
        
        return sim_df
