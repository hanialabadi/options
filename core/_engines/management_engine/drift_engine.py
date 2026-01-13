import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import logging

from core.data_layer.market_stress_detector import check_market_stress
from core.phase5_portfolio_limits import check_portfolio_limits, get_persona_limits
from core.phase6_freeze.evaluate_leg_status import evaluate_leg_status

logger = logging.getLogger(__name__)

class DriftEngine:
    """
    Authoritative Drift Engine for Options Intelligence System.
    
    Enforces structural, signal, data, regime, and portfolio governance.
    Acts as a non-discretionary filter over the Recommendation Engine.
    """
    
    def __init__(self, persona: str = 'conservative'):
        self.persona = persona
        self.drift_ledger = []

    def assess_data_drift(self, df: pd.DataFrame) -> pd.DataFrame:
        """Category 1: Data Drift (Staleness & Provenance)"""
        now = datetime.now()
        # Assume Snapshot_TS is in the dataframe
        if 'Snapshot_TS' in df.columns:
            df['Snapshot_TS'] = pd.to_datetime(df['Snapshot_TS'])
            df['Data_Age_Min'] = (now - df['Snapshot_TS']).dt.total_seconds() / 60.0
            
            # States: FRESH (<15m), STALE (15-60m), ORPHANED (>60m)
            df['Data_State'] = 'FRESH'
            df.loc[df['Data_Age_Min'] > 15, 'Data_State'] = 'STALE'
            df.loc[df['Data_Age_Min'] > 60, 'Data_State'] = 'ORPHANED'
        else:
            df['Data_State'] = 'ORPHANED'
            df['Data_Age_Min'] = np.inf
            
        return df

    def assess_signal_drift(self, df: pd.DataFrame) -> pd.DataFrame:
        """Category 2: Signal Drift (PCS & Greek Migration)"""
        # States: VALID, DEGRADED, VIOLATED
        df['Signal_State'] = 'VALID'
        
        if 'PCS_Drift' in df.columns:
            df.loc[df['PCS_Drift'] > 15, 'Signal_State'] = 'DEGRADED'
            df.loc[df['PCS_Drift'] > 25, 'Signal_State'] = 'VIOLATED'
            
        if 'Delta_1D_DriftTail' in df.columns:
            # Extreme delta migration (>0.20 in a day)
            df.loc[abs(df['Delta_1D_DriftTail']) > 0.20, 'Signal_State'] = 'DEGRADED'
            
        return df

    def assess_structural_drift(self, df: pd.DataFrame) -> pd.DataFrame:
        """Category 3: Structural Drift (Leg Integrity)"""
        # Uses existing evaluate_leg_status logic
        df = evaluate_leg_status(df)
        
        # Map LegStatus to Structural States: INTACT, BROKEN
        df['Structural_State'] = 'INTACT'
        df.loc[df['LegStatus'] == 'Broken', 'Structural_State'] = 'BROKEN'
        df.loc[df['LegStatus'] == 'Closed', 'Structural_State'] = 'CLOSED'
        
        return df

    def assess_regime_drift(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
        """Category 4: Regime Drift (Market Stress)"""
        stress_level, median_iv = check_market_stress()
        
        # States: STABLE (GREEN), STRESSED (YELLOW), HALTED (RED)
        regime_state_map = {'GREEN': 'STABLE', 'YELLOW': 'STRESSED', 'RED': 'HALTED'}
        regime_state = regime_state_map.get(stress_level, 'STABLE')
        
        df['Regime_State'] = regime_state
        df['Market_Median_IV'] = median_iv
        
        return df, regime_state

    def assess_portfolio_drift(self, df: pd.DataFrame, regime_state: str) -> pd.DataFrame:
        """Category 5: Portfolio Drift (Greek Limits & Concentration)"""
        # Get base limits for persona
        base_limits = get_persona_limits(self.persona)
        
        # Regime-Adaptive Contraction
        multiplier = 1.0
        if regime_state == 'STRESSED':
            multiplier = 0.75
        elif regime_state == 'HALTED':
            multiplier = 0.50
            
        adjusted_limits = {k: v * multiplier if isinstance(v, (int, float)) else v 
                          for k, v in base_limits.items()}
        
        # Run limit check
        df, diagnostics = check_portfolio_limits(df, limits=adjusted_limits)
        
        # States: NOMINAL, CAPPED, OVER_LIMIT
        df['Portfolio_State'] = 'NOMINAL'
        if diagnostics.get('violations'):
            df['Portfolio_State'] = 'OVER_LIMIT'
        elif diagnostics.get('warnings'):
            df['Portfolio_State'] = 'CAPPED'
            
        return df

    def run_drift_analysis(self, df: pd.DataFrame) -> pd.DataFrame:
        """Execute all five categories of drift assessment."""
        logger.info("🛡️ Running Authoritative Drift Analysis...")
        
        df = self.assess_data_drift(df)
        df = self.assess_signal_drift(df)
        df = self.assess_structural_drift(df)
        df, regime_state = self.assess_regime_drift(df)
        df = self.assess_portfolio_drift(df, regime_state)
        
        # Determine Authoritative Drift Action
        df['Drift_Action'] = df.apply(self._determine_action, axis=1)
        
        return df

    def _determine_action(self, row: pd.Series) -> str:
        """Non-discretionary action mapping based on state hierarchy."""
        
        # 1. Structural Failure (Highest Priority)
        if row['Structural_State'] == 'BROKEN':
            return 'FORCE_EXIT'
            
        # 2. Regime Halt
        if row['Regime_State'] == 'HALTED':
            return 'HARD_HALT'
            
        # 3. Data Orphaned
        if row['Data_State'] == 'ORPHANED':
            return 'QUARANTINE'
            
        # 4. Signal Violated
        if row['Signal_State'] == 'VIOLATED':
            return 'EXIT'
            
        # 5. Data Stale or Signal Degraded or Regime Stressed
        if row['Data_State'] == 'STALE' or row['Signal_State'] == 'DEGRADED' or row['Regime_State'] == 'STRESSED':
            return 'REVALIDATE'
            
        # 6. Portfolio Over Limit
        if row['Portfolio_State'] == 'OVER_LIMIT':
            return 'TRIM_ONLY'
            
        return 'NO_ACTION'

    def apply_drift_filter(self, df: pd.DataFrame, rec_col: str = 'Rec_Action') -> pd.DataFrame:
        """
        Authoritative Filter: Risk may only be reduced, never increased.
        
        Intersection logic between Recommendation Engine and Drift Engine.
        """
        if rec_col not in df.columns:
            return df
            
        def filter_logic(row):
            rec = str(row[rec_col]).upper()
            drift = row['Drift_Action']
            
            # Drift Overrides (Authoritative)
            if drift == 'FORCE_EXIT': return 'EXIT'
            if drift == 'HARD_HALT': return 'WAIT'
            if drift == 'QUARANTINE': return 'REVALIDATE'
            if drift == 'EXIT': return 'EXIT'
            
            # Risk Reduction Only
            if drift == 'REVALIDATE':
                if rec in ['HOLD', 'ENTER']: return 'REVALIDATE'
                return rec
                
            if drift == 'TRIM_ONLY':
                if rec in ['HOLD', 'ENTER']: return 'TRIM'
                return rec # EXIT or REVALIDATE are already risk-reducing
                
            return rec

        df[f'{rec_col}_Final'] = df.apply(filter_logic, axis=1)
        return df
