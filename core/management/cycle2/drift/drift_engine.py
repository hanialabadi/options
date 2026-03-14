import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import logging

from core.shared.data_layer.market_stress_detector import check_market_stress
from core.phase5_portfolio_limits import check_portfolio_limits, get_persona_limits
from core.phase6_freeze.evaluate_leg_status import evaluate_leg_status

logger = logging.getLogger(__name__)

class DriftEngine:
    """
    Authoritative Drift Engine for Options Intelligence System.
    
    Enforces structural, signal, data, regime, and portfolio governance.
    Acts as a non-discretionary filter over the Recommendation Engine.
    """
    
    def __init__(self, persona: str = 'conservative', account_balance: float = 100_000.0):
        self.persona = persona
        self.account_balance = account_balance
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
        """Category 2: Signal Drift (PCS, Greek Migration, and Greek ROC acceleration).

        Signal states (ordered by severity):
          VALID     — all metrics within normal range
          DEGRADED  — one or more metrics approaching threshold (monitor)
          VIOLATED  — one or more metrics exceeded threshold (action required)

        Architecture:
          Tier 1 (PCS drift) and Tier 2 (Delta_1D_DriftTail) are blanket rules —
          they apply uniformly regardless of strategy.

          Tier 3 (Greek ROC: Delta, Vega, Gamma, IV) is profile-driven via
          signal_profiles.py. Each strategy family declares how each Greek ROC
          should be interpreted (signed long/short, exempt, far-OTM income guard).
          Adding a new strategy = adding a profile entry, not touching the engine.

        Thresholds: config/indicator_settings.py → SIGNAL_DRIFT_THRESHOLDS
        Profiles:   core/management/cycle2/drift/signal_profiles.py
        """
        from core.management.cycle2.drift.signal_profiles import apply_signal_profiles

        df['Signal_State'] = 'VALID'

        # NOTE: Tier 1 (PCS_Drift) and Tier 2 (Delta_1D_DriftTail) were removed —
        # these columns are never computed by any upstream module. The code was dead
        # (guarded by `if col in df.columns` which never evaluated True). If PCS_Drift
        # or Delta_1D_DriftTail are implemented in the future, they should be added as
        # profile-aware rules in signal_profiles.py, not blanket thresholds here.

        # --- Tier 3: Greek ROC — profile-driven, strategy-aware ---
        # Delegates to signal_profiles.apply_signal_profiles() which loops over
        # strategy family profiles. Each profile declares sign conventions,
        # far-OTM exemptions, DTE gates, and thresholds per Greek ROC.
        df = apply_signal_profiles(df)

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
        stress_level, median_iv, stress_basis = check_market_stress()
        
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
        df, diagnostics = check_portfolio_limits(df, limits=adjusted_limits,
                                                  account_balance=self.account_balance)
        
        # States: NOMINAL, CAPPED, OVER_LIMIT
        df['Portfolio_State'] = 'NOMINAL'
        if diagnostics.get('violations'):
            df['Portfolio_State'] = 'OVER_LIMIT'
        elif diagnostics.get('warnings'):
            df['Portfolio_State'] = 'CAPPED'
            
        return df

    def integrate_sensor_data(self, df: pd.DataFrame, sensor_db: str) -> pd.DataFrame:
        """
        Integrate latest Schwab sensor readings for drift calculation.
        
        RAG: Cycle 2 Measurement.
        Computes drift relative to Cycle 1 anchors.
        """
        import duckdb
        if not os.path.exists(sensor_db):
            logger.warning(f"Sensor DB not found at {sensor_db}. Falling back to Cycle 1 snapshot.")
            df['Attribution_Quality'] = 'PARTIAL'
            return df

        with duckdb.connect(sensor_db) as con:
            # Fetch latest sensor reading per LegID
            # We join with canonical_anchors to ensure we only sense valid identities
            # Note: canonical_anchors is in pipeline.duckdb, so we might need to attach it
            # but for now we assume sensor_readings already has the correct LegID from ingestion
            sensor_df = con.execute("""
                SELECT * FROM sensor_readings
                QUALIFY ROW_NUMBER() OVER (PARTITION BY LegID ORDER BY Sensor_TS DESC) = 1
            """).df()

        if sensor_df.empty:
            df['Attribution_Quality'] = 'PARTIAL'
            return df

        # Merge sensor data into current dataframe
        # We prefix sensor columns to avoid collision with Cycle 1 anchors
        sensor_df = sensor_df.rename(columns={
            'UL_Last': 'Sensor_UL_Last',
            'Opt_Last': 'Sensor_Opt_Last',
            'IV': 'Sensor_IV',
            'Delta': 'Sensor_Delta',
            'Gamma': 'Sensor_Gamma',
            'Vega': 'Sensor_Vega',
            'Theta': 'Sensor_Theta',
            'Rho': 'Sensor_Rho',
            'Sensor_TS': 'Sensor_TS'
        })
        
        df = df.merge(sensor_df.drop(columns=['TradeID', 'Source']), on='LegID', how='left')
        
        # Compute Drift: Schwab_Current - Fidelity_Entry
        # Note: Fidelity_Entry fields are frozen in Cycle 1 (e.g., Underlying_Price_Entry)
        if 'Underlying_Price_Entry' in df.columns and 'Sensor_UL_Last' in df.columns:
            df['UL_Drift'] = df['Sensor_UL_Last'] - df['Underlying_Price_Entry']
            
        # Attribution Quality Enforcement
        now = datetime.now()
        df['Attribution_Quality'] = 'PARTIAL'
        
        def check_quality(row):
            if pd.isna(row.get('Sensor_TS')):
                return 'PARTIAL'
            
            # Staleness check (> 5 minutes)
            age_seconds = (now - row['Sensor_TS']).total_seconds()
            if age_seconds > 300:
                return 'DEGRADED'
                
            return 'FULL'

        df['Attribution_Quality'] = df.apply(check_quality, axis=1)
        
        # Model Divergence: Surface as PnL_Unexplained
        # (Placeholder for complex attribution logic)
        df['PnL_Unexplained'] = 0.0 
        
        return df

    def run_drift_analysis(self, df: pd.DataFrame, sensor_db: str = None) -> pd.DataFrame:
        """Execute all five categories of drift assessment."""
        logger.info("🛡️ Running Authoritative Drift Analysis...")
        
        # Integrate Sensor Data if available
        if sensor_db:
            df = self.integrate_sensor_data(df, sensor_db)
        else:
            df['Attribution_Quality'] = 'PARTIAL'

        df = self.assess_data_drift(df)
        df = self.assess_signal_drift(df)
        df = self.assess_structural_drift(df)
        df, regime_state = self.assess_regime_drift(df)
        df = self.assess_portfolio_drift(df, regime_state)
        
        # Compute High-Level Drift Signals (Migrated from UI)
        df = self.compute_high_level_signals(df)
        
        # Determine Authoritative Drift Action
        df['Drift_Action'] = df.apply(self._determine_action, axis=1)
        
        return df

    def compute_high_level_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute high-level signals for UI consumption.

        NON-DESTRUCTIVE: compute_basic_drift.py runs before this and already
        sets Drift_Direction, Drift_Magnitude, and Lifecycle_Phase from price
        data. Only overwrite if the column is absent — DriftEngine adds
        Dominant_Pressure and Drift_Persistence which basic_drift does NOT set.
        """
        # 1. Drift Direction — only write if compute_basic_drift didn't produce it
        if 'Drift_Direction' not in df.columns or df['Drift_Direction'].isna().all():
            if 'Price_Drift_Pct' in df.columns:
                df['Drift_Direction'] = df['Price_Drift_Pct'].apply(
                    lambda x: 'Up' if x > 0.01 else ('Down' if x < -0.01 else 'Flat')
                )
            else:
                df['Drift_Direction'] = 'N/A'

        # 2. Dominant Pressure — not set by basic_drift; always compute here
        attribution_cols = ['PnL_From_Delta', 'PnL_From_Theta', 'PnL_From_Vega', 'PnL_From_Gamma']
        def get_dominant_pressure(row):
            vals = {col.split('_')[-1]: abs(row[col]) for col in attribution_cols if col in row and pd.notna(row[col])}
            if not vals: return 'N/A'
            return max(vals, key=vals.get)
        df['Dominant_Pressure'] = df.apply(get_dominant_pressure, axis=1)

        # 3. Persistence (Smoothed) — not set by basic_drift; always compute here
        if 'delta_drift_sma_3' in df.columns and 'Delta_Drift_Structural' in df.columns:
            def get_persistence(row):
                if pd.isna(row['delta_drift_sma_3']) or pd.isna(row['Delta_Drift_Structural']): return 'Transient'
                if np.sign(row['delta_drift_sma_3']) == np.sign(row['Delta_Drift_Structural']):
                    return 'Sustained'
                return 'Transient'
            df['Drift_Persistence'] = df.apply(get_persistence, axis=1)
        else:
            df['Drift_Persistence'] = 'Transient'

        # 4. Drift Magnitude — only write if compute_basic_drift didn't produce it
        if 'Drift_Magnitude' not in df.columns or df['Drift_Magnitude'].isna().all():
            if 'PnL_Total' in df.columns and 'Capital_Deployed' in df.columns:
                def get_magnitude(row):
                    if pd.isna(row['PnL_Total']) or pd.isna(row['Capital_Deployed']) or row['Capital_Deployed'] == 0: return 'Low'
                    ratio = abs(row['PnL_Total'] / row['Capital_Deployed'])
                    if ratio > 0.10: return 'High'
                    if ratio > 0.03: return 'Medium'
                    return 'Low'
                df['Drift_Magnitude'] = df.apply(get_magnitude, axis=1)
            else:
                df['Drift_Magnitude'] = 'Low'

        # 5. Lifecycle Phase — only write if compute_basic_drift didn't produce it
        if 'Lifecycle_Phase' not in df.columns or df['Lifecycle_Phase'].isna().all():
            if 'DTE' in df.columns:
                df['Lifecycle_Phase'] = 'ACTIVE'
                df.loc[df['DTE'] > 45, 'Lifecycle_Phase'] = 'ACTIVE'
                df.loc[(df['DTE'] >= 14) & (df['DTE'] <= 45), 'Lifecycle_Phase'] = 'INCOME_WINDOW'
                df.loc[df['DTE'] < 14, 'Lifecycle_Phase'] = 'TERMINAL'
                if 'Days_In_Trade' in df.columns:
                    df.loc[(df['Days_In_Trade'] <= 3) & (df['DTE'] > 45), 'Lifecycle_Phase'] = 'ENTRY'
            else:
                df['Lifecycle_Phase'] = 'ACTIVE'

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
            return 'REVIEW'
            
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

            # Terminal system states are exempt from ALL drift overrides.
            # AWAITING_SETTLEMENT means the option has expired (DTE=0) and we're
            # waiting for broker settlement — no drift signal is meaningful here.
            # Overriding it to EXIT or REVALIDATE would be misleading.
            if rec == 'AWAITING_SETTLEMENT':
                return rec

            # Drift Overrides (Authoritative)
            if drift == 'FORCE_EXIT': return 'EXIT'
            if drift == 'HARD_HALT': return 'WAIT'
            if drift == 'QUARANTINE': return 'REVIEW'
            if drift == 'EXIT':
                # ASSIGN, EXIT, and BUYBACK are all risk-reducing (closing the
                # position). Respect the doctrine's EV-based choice among them
                # rather than forcing EXIT unconditionally.
                # E.g., ASSIGN (+$293 EV) beats EXIT when stock is above net cost
                # and the call expires worthless — overriding to EXIT destroys the
                # better outcome and creates decision instability.
                _RISK_REDUCING = ('EXIT', 'BUYBACK', 'LET_EXPIRE',
                                  'ACCEPT_CALL_AWAY', 'ACCEPT_SHARE_ASSIGNMENT')
                if rec in _RISK_REDUCING:
                    return rec  # already risk-reducing — preserve doctrine choice

                # Recovery doctrine guard: when doctrine has determined the position
                # is in a recovery state (RECOVERY_PREMIUM for BW, RECOVERY_LADDER
                # for CSP/BW/CC), the doctrine already evaluated EXIT as an option
                # and chose recovery based on EV comparison. Drift EXIT here would
                # force realization of the full loss, destroying the recovery path.
                # The position is already below cost basis — that's WHY it's in
                # recovery mode. Premium collection is the rational repair strategy.
                # (Jabbour Ch.4; Passarelli Ch.1: wheel conversion)
                _RECOVERY_ACTIONS = ('ROLL_UP_OUT', 'WRITE_NOW',
                                     'HOLD_STOCK_WAIT', 'PAUSE_WRITING',
                                     'HOLD', 'ROLL')
                _doctrine_state = str(row.get('Doctrine_State', '')).upper()
                if _doctrine_state in ('RECOVERY_PREMIUM', 'RECOVERY_LADDER') and rec in _RECOVERY_ACTIONS:
                    return rec  # recovery doctrine is authoritative — preserve recovery path

                # Income structure guard: for BW/CC with a far-OTM near-expiry
                # short call, doctrine HOLD = "let call expire worthless" which is
                # functionally LET_EXPIRE. This is MORE risk-reducing than active
                # EXIT (no spread to cross, no slippage, full premium captured).
                # The EV comparator already evaluated EXIT vs HOLD and chose HOLD.
                # Overriding to EXIT here destroys the income-optimal outcome.
                # (McMillan Ch.3: near-expiry far-OTM short call is pure income)
                if rec in ('HOLD', 'ROLL'):
                    _strat = str(row.get('Strategy', row.get('Strategy_Name', ''))).upper()
                    _is_income = any(s in _strat for s in ('BUY_WRITE', 'COVERED_CALL', 'CC', 'BW'))
                    _delta = pd.to_numeric(row.get('Short_Call_Delta', row.get('Delta', None)),
                                           errors='coerce')
                    _dte = pd.to_numeric(row.get('DTE', None), errors='coerce')
                    if (_is_income
                            and _delta is not None and not np.isnan(_delta) and _delta < 0.30
                            and _dte is not None and not np.isnan(_dte) and _dte <= 14):
                        return rec  # income-optimal: far-OTM call expiring, doctrine is authoritative

                return 'EXIT'
            
            # Risk Reduction Only
            if drift == 'REVIEW':
                if rec in ['HOLD', 'ENTER']: return 'REVIEW'
                return rec
                
            if drift == 'TRIM_ONLY':
                if rec in ['HOLD', 'ENTER']: return 'TRIM'
                return rec # EXIT or REVIEW are already risk-reducing
                
            return rec

        df[f'{rec_col}_Final'] = df.apply(filter_logic, axis=1)
        return df
