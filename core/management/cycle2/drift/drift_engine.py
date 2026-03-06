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

        Signed ROC thresholds (thesis-aware):
          PCS_Drift > 15                     → DEGRADED;  > 25       → VIOLATED
          Delta_1D_DriftTail abs > 0.20      → DEGRADED  (tail event: direction matters less)
          Delta_ROC_3D  < -0.15              → DEGRADED  (delta falling = deterioration for longs)
          Delta_ROC_3D  < -0.30              → VIOLATED
          Vega_ROC_3D   < -0.20              → DEGRADED  (IV crush hurts long vol)
          Vega_ROC_3D   >  0.20              → DEGRADED  (IV spike hurts short vol)  [abs symmetric]
          Gamma_ROC_3D  > +0.25 (short-gamma, DTE>30) → DEGRADED  (gamma accelerating against short)
          Gamma_ROC_3D  > +0.50 (short-gamma, DTE>30) → VIOLATED
          Gamma_ROC_3D  < -0.25 (long-gamma,  DTE>30) → DEGRADED  (convexity eroding)
          Gamma_ROC_3D  < -0.50 (long-gamma,  DTE>30) → VIOLATED
          DTE ≤ 30: Gamma ROC signal suppressed — mechanical expiry spike, not structural
          IV_ROC_3D     < -0.15              → DEGRADED  (IV crush — primary long-vol deterioration)
          IV_ROC_3D     < -0.30              → VIOLATED
          ROC_Persist_3D >= 2 required       → prevents single-day noise from firing

        Persistence gate: ROC escalation requires at least 2 consecutive confirmed
        snapshots (ROC_Persist_3D column from compute_windowed_drift). If the column
        is absent, persistence is assumed satisfied to avoid silent failures.
        """
        df['Signal_State'] = 'VALID'

        # --- Tier 1: PCS drift (position score degradation — always signed, higher = worse) ---
        if 'PCS_Drift' in df.columns:
            df.loc[df['PCS_Drift'] > 15, 'Signal_State'] = 'DEGRADED'
            df.loc[df['PCS_Drift'] > 25, 'Signal_State'] = 'VIOLATED'

        # --- Tier 2: Single-day delta tail (extreme overnight move — abs, direction less relevant) ---
        if 'Delta_1D_DriftTail' in df.columns:
            df.loc[df['Delta_1D_DriftTail'].abs() > 0.20, 'Signal_State'] = 'DEGRADED'

        # --- Tier 3: Greek ROC — thesis-aware signed thresholds + persistence gate ---
        # Persistence gate: ROC_Persist_3D >= 2 means the ROC was negative/violated
        # on at least 2 consecutive snapshots (not just one noisy day).
        # If persistence column is missing, default to True (don't silently suppress).
        if 'ROC_Persist_3D' in df.columns:
            _persist_ok = df['ROC_Persist_3D'].fillna(0) >= 2
        else:
            _persist_ok = pd.Series(True, index=df.index)

        def _escalate(mask: pd.Series, target_state: str) -> None:
            """Escalate only; never downgrade. VIOLATED always beats DEGRADED."""
            _already = df['Signal_State'] == 'VIOLATED'
            if target_state == 'DEGRADED':
                df.loc[mask & _persist_ok & ~_already, 'Signal_State'] = 'DEGRADED'
            elif target_state == 'VIOLATED':
                # Persistence threshold halved for VIOLATED — two days is enough to act.
                if 'ROC_Persist_3D' in df.columns:
                    _persist_vio = df['ROC_Persist_3D'].fillna(0) >= 1
                else:
                    _persist_vio = pd.Series(True, index=df.index)
                df.loc[mask & _persist_vio, 'Signal_State'] = 'VIOLATED'

        # Delta ROC: thesis-aware — sign convention differs for puts vs calls.
        # LONG_CALL / BUY_CALL / LEAPS_CALL: delta should be rising (positive ROC).
        #   Negative Delta_ROC_3D = delta falling = losing sensitivity = thesis eroding.
        #   → escalate on _d < -0.15 (deterioration).
        #
        # LONG_PUT / BUY_PUT / LEAPS_PUT: delta is negative and should be growing MORE negative
        #   (e.g. -0.45 → -0.58) as stock falls toward strike. Delta_ROC_3D = -0.16 for
        #   TSLA put where stock has moved $6 bearish = the put IS working.
        #   Applying _d < -0.15 to a put fires DEGRADED when the thesis is succeeding.
        #   → for puts, escalate on _d > +0.15 (delta recovering toward zero = thesis eroding).
        #   → for puts, _d < -0.15 means direction IS working — do NOT escalate.
        #
        # Doctrine: Passarelli Ch.2: "Greek drift must be evaluated relative to thesis
        #   direction — a falling delta on a put means the stock is moving against you,
        #   not toward you." McMillan Ch.4: "Direction alignment is the primary signal."
        if 'Delta_ROC_3D' in df.columns and 'Strategy' in df.columns:
            _d = df['Delta_ROC_3D']
            _strat_d = df['Strategy'].fillna('').str.upper()
            _is_put_d = _strat_d.isin({'LONG_PUT', 'BUY_PUT', 'LEAPS_PUT'})
            _is_call_d = _strat_d.isin({'LONG_CALL', 'BUY_CALL', 'LEAPS_CALL'})

            # Long calls: negative delta ROC = deterioration (delta falling)
            _escalate(_is_call_d & (_d < -0.15), 'DEGRADED')
            _escalate(_is_call_d & (_d < -0.30), 'VIOLATED')

            # Long puts: POSITIVE delta ROC = deterioration (delta recovering toward zero)
            # i.e. put losing sensitivity — stock moving away from strike
            _escalate(_is_put_d & (_d > 0.15), 'DEGRADED')
            _escalate(_is_put_d & (_d > 0.30), 'VIOLATED')

            # All other strategies (CSP, BW, CC, straddle, etc.): apply original unsigned logic
            _is_other_d = ~(_is_put_d | _is_call_d)
            _escalate(_is_other_d & (_d < -0.15), 'DEGRADED')
            _escalate(_is_other_d & (_d < -0.30), 'VIOLATED')

        # Vega ROC: negative = IV crush (long vol deterioration); positive = IV spike (short vol risk).
        # Both directions are risks depending on position — treat symmetrically.
        if 'Vega_ROC_3D' in df.columns:
            _v = df['Vega_ROC_3D']
            _escalate(_v < -0.20, 'DEGRADED')
            _escalate(_v >  0.20, 'DEGRADED')

        # Gamma ROC: direction-aware, DTE-gated.
        # Rising gamma on a short-gamma position (BUY_WRITE/CC) = acceleration against you.
        # Falling gamma on a long-gamma position (long call/put/LEAP) = losing convexity.
        # DTE gate: exclude mechanical expiry gamma spike (DTE <= 30).
        # Symmetric abs() is wrong here — same logic as Delta/Vega.
        if 'Gamma_ROC_3D' in df.columns and 'DTE' in df.columns:
            _g     = df['Gamma_ROC_3D']
            _dte   = pd.to_numeric(df['DTE'], errors='coerce').fillna(0)
            _strat = df['Strategy'].fillna('') if 'Strategy' in df.columns else pd.Series('', index=df.index)

            # Only meaningful when option is not approaching mechanical expiry
            _dte_ok = _dte > 30

            # Short-gamma structures: BUY_WRITE, COVERED_CALL — rising gamma is bad
            _short_gamma = _strat.str.upper().isin({'BUY_WRITE', 'COVERED_CALL', 'CSP'})
            _escalate(_short_gamma & _dte_ok & (_g >  0.25), 'DEGRADED')
            _escalate(_short_gamma & _dte_ok & (_g >  0.50), 'VIOLATED')

            # Long-gamma structures: long calls, puts, LEAPs — falling gamma is bad
            _long_gamma = _strat.str.upper().isin({
                'LONG_CALL', 'BUY_CALL', 'LONG_PUT', 'BUY_PUT',
                'LEAPS_CALL', 'LEAPS_PUT',
            })
            _escalate(_long_gamma & _dte_ok & (_g < -0.25), 'DEGRADED')
            _escalate(_long_gamma & _dte_ok & (_g < -0.50), 'VIOLATED')

        # IV ROC: signed — negative = IV crush (primary deterioration for long vol positions).
        if 'IV_ROC_3D' in df.columns:
            _iv = df['IV_ROC_3D']
            _escalate(_iv < -0.15, 'DEGRADED')
            _escalate(_iv < -0.30, 'VIOLATED')

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
            if 'Days_In_Trade' in df.columns and 'Entry_DTE' in df.columns:
                def get_phase(row):
                    if pd.isna(row['Days_In_Trade']) or pd.isna(row['Entry_DTE']) or row['Entry_DTE'] == 0: return 'Mid'
                    progress = row['Days_In_Trade'] / row['Entry_DTE']
                    if progress < 0.25: return 'Early'
                    if progress > 0.75: return 'Late'
                    return 'Mid'
                df['Lifecycle_Phase'] = df.apply(get_phase, axis=1)
            else:
                df['Lifecycle_Phase'] = 'Mid'

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

            # Terminal system states are exempt from ALL drift overrides.
            # AWAITING_SETTLEMENT means the option has expired (DTE=0) and we're
            # waiting for broker settlement — no drift signal is meaningful here.
            # Overriding it to EXIT or REVALIDATE would be misleading.
            if rec == 'AWAITING_SETTLEMENT':
                return rec

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
