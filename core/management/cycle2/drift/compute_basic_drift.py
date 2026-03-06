import pandas as pd
import numpy as np
import logging
from datetime import datetime

from core.management.cycle1.identity.constants import FIDELITY_MARGIN_RATE, FIDELITY_MARGIN_RATE_DAILY

logger = logging.getLogger(__name__)

def compute_drift_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cycle 2: Drift Engine
    
    Computes the migration of state from Cycle 1 anchors to current vital signs.
    
    RAG Authority: Passarelli / Natenberg (Attribution Doctrine)
    """
    if df.empty:
        return df
    
    # --- HARD GUARD: Anchor Presence ---
    # RAG: Auditability. Drift cannot be calculated without frozen anchors.
    if 'Delta_Entry' not in df.columns or df['Delta_Entry'].isna().all():
        logger.error("❌ DRIFT CALCULATION FAILURE: Entry anchors missing from input dataframe.")
        raise RuntimeError("Cycle-2 Halt: Measurement attempted without frozen anchors. Ensure join with entry_anchors table.")

    df = df.copy()
    
    # 1. Temporal Drift
    if 'Entry_Snapshot_TS' in df.columns and 'Snapshot_TS' in df.columns:
        # Ensure datetime types for subtraction
        ts_current = pd.to_datetime(df['Snapshot_TS'], errors='coerce')
        ts_entry = pd.to_datetime(df['Entry_Snapshot_TS'], errors='coerce')
        
        # Days_In_Trade: Integer days elapsed since entry
        # RAG: Use normalized timestamps to ensure date-only subtraction
        df['Days_In_Trade'] = (ts_current.dt.normalize() - ts_entry.dt.normalize()).dt.days.fillna(0).astype(int)
    else:
        df['Days_In_Trade'] = 0

    # DTE (Days To Expiration): Integer days remaining until expiration
    if 'Expiration' in df.columns and 'Snapshot_TS' in df.columns:
        expiry = pd.to_datetime(df['Expiration'], errors='coerce')
        ts_current = pd.to_datetime(df['Snapshot_TS'], errors='coerce')
        
        # DTE = max(0, (Expiration.date() - today.date()).days)
        df['DTE'] = (expiry.dt.normalize() - ts_current.dt.normalize()).dt.days.clip(lower=0)
    else:
        df['DTE'] = np.nan

    # --- HARD GUARDRAIL: Data Integrity Warning ---
    # RAG: Auditability. Flag positions with suspicious lineage.
    if 'Days_In_Trade' in df.columns and 'DTE' in df.columns:
        mask = (df['Days_In_Trade'] == 0) & (df['DTE'] > 30)
        if mask.any():
            bad_legs = df.loc[mask, 'LegID'].tolist()
            logger.warning(f"⚠️ DATA INTEGRITY WARNING: {len(bad_legs)} legs have Days_In_Trade=0 but DTE > 30. Lineage may be incomplete.")
            # Add a flag for UI visibility
            df['Lineage_Warning'] = mask

    # 2. Price Drift (Underlying)
    if 'UL Last' in df.columns and 'Underlying_Price_Entry' in df.columns:
        df['Price_Drift_Abs'] = df['UL Last'] - df['Underlying_Price_Entry']
        df['Price_Drift_Pct'] = df['Price_Drift_Abs'] / df['Underlying_Price_Entry'].replace(0, np.nan)
        
        # RAG: Drift Classification (Cycle 3 Requirement)
        df['Drift_Direction'] = np.where(df['Price_Drift_Abs'] >= 0, 'Up', 'Down')
        df['Drift_Magnitude'] = 'Low'
        df.loc[df['Price_Drift_Pct'].abs() > 0.05, 'Drift_Magnitude'] = 'Medium'
        df.loc[df['Price_Drift_Pct'].abs() > 0.10, 'Drift_Magnitude'] = 'High'
    
    # 2.1 Lifecycle Phase (Cycle 3 Requirement)
    if 'DTE' in df.columns:
        # RAG: Lifecycle Doctrine. 
        # Early: > 45 days
        # Mid: 14-45 days
        # Late: < 14 days
        df['Lifecycle_Phase'] = 'Mid'
        df.loc[df['DTE'] > 45, 'Lifecycle_Phase'] = 'Early'
        df.loc[df['DTE'] < 14, 'Lifecycle_Phase'] = 'Late'
        # Stocks have DTE -999, they are always 'Early' (Perpetual)
        if 'AssetType' in df.columns:
            df.loc[df['AssetType'] == 'STOCK', 'Lifecycle_Phase'] = 'Early'

    # 3. Greek Drift (Sensitivities)
    greek_pairs = [
        ('Delta', 'Delta_Entry', 'Delta_Drift'),
        ('Gamma', 'Gamma_Entry', 'Gamma_Drift'),
        ('Vega', 'Vega_Entry', 'Vega_Drift'),
        ('Theta', 'Theta_Entry', 'Theta_Drift'),
    ]
    
    for current, entry, drift in greek_pairs:
        if current in df.columns and entry in df.columns:
            df[drift] = df[current] - df[entry]

    # 4. IV Drift (Derived Metric)
    # RAG Authority: Passarelli / Natenberg
    # Unit: Decimal Volatility (Canonical Standard)
    if 'IV_Entry' in df.columns:
        # Cycle-2 Authoritative IV Sourcing: IV_Now (transient) -> IV_Entry (anchor) -> NaN
        if 'IV_Now' in df.columns:
            iv_current = df['IV_Now']
        else:
            iv_current = df['IV_Entry'].apply(lambda x: x / 100.0 if pd.notna(x) and x > 2.0 else x)
            
        # Normalize IV_Entry to Decimal Vol if it's in percent (Cycle 1 legacy)
        iv_entry_val = df['IV_Entry'].apply(lambda x: x / 100.0 if pd.notna(x) and x > 2.0 else x)

        # IV Drift (Numeric) - Allowed Cycle-2 Derived Metric
        df['IV_Drift'] = iv_current - iv_entry_val
    else:
        df['IV_Drift'] = 0.0
    
    # 5. Total P&L (Internal Calculation)
    # RAG Authority: Hull (Valuation Neutrality)
    if 'Last' in df.columns and 'Basis' in df.columns and 'Quantity' in df.columns:
        # RAG: Multiplier Doctrine. 
        # Options are 100x, Stocks are 1x.
        # We use a vectorized approach to avoid the 'any()' bug.
        if 'AssetType' in df.columns:
            multipliers = np.where(df['AssetType'] == 'OPTION', 100.0, 1.0)
        else:
            logger.warning("⚠️ AssetType missing during P&L calculation — assuming multiplier 1.0")
            multipliers = 1.0
        
        # Current Value = Last * Quantity * Multiplier
        df['Current_Value'] = df['Last'] * df['Quantity'] * multipliers
        
        # PnL_Total = Current_Value - (Basis * sign(Quantity))
        # Fidelity Basis is always positive; we must sign it to handle Short P&L correctly.
        df['PnL_Total'] = df['Current_Value'] - (df['Basis'] * np.sign(df['Quantity']))

    # 6. HV Enrichment (Cycle 2 Management Requirement)
    # RAG: Smart Enrichment. Uses GovernedHVProvider with DuckDB persistence.
    # Note: This is now primarily handled in run_all.py to avoid redundant API calls.
    if 'HV_20D' not in df.columns:
        try:
            from core.management.cycle2.drift.auto_enrich_hv import auto_enrich_hv_from_schwab
            df = auto_enrich_hv_from_schwab(df)
        except Exception as e:
            logger.error(f"❌ HV Enrichment failed: {e}")
            df['HV_20D'] = np.nan

    # RAG: Chart State Enrichment is now handled at the orchestrator level (run_all.py)
    # to ensure primitives are fetched once and shared across measurement layers.

    # 7. Recovery Feasibility Math (Doctrine Input — Natenberg Ch.5)
    # Answers: "Can this position realistically recover within its remaining DTE?"
    # HV-implied 1-day 1-sigma move in dollars
    if 'HV_20D' in df.columns and 'UL Last' in df.columns:
        df['HV_Daily_Move_1Sigma'] = (
            df['HV_20D'] * df['UL Last'] / np.sqrt(252)
        ).where(df['HV_20D'].notna() & (df['UL Last'] > 0), other=np.nan)
    else:
        df['HV_Daily_Move_1Sigma'] = np.nan

    option_mask = (df['AssetType'] == 'OPTION') if 'AssetType' in df.columns else pd.Series(False, index=df.index)
    df['Recovery_Move_Required'] = np.nan
    df['Recovery_Move_Per_Day']  = np.nan

    if 'Unrealized_PnL' in df.columns and 'Delta' in df.columns and 'DTE' in df.columns:
        delta_abs = df['Delta'].abs().replace(0, np.nan)
        dte_safe  = df['DTE'].replace(0, np.nan)
        # $ of underlying move to recover PnL loss (via delta approximation)
        df.loc[option_mask, 'Recovery_Move_Required'] = (
            (-df.loc[option_mask, 'Unrealized_PnL']) /
            (delta_abs.loc[option_mask] * 100)
        ).clip(lower=0)
        df.loc[option_mask, 'Recovery_Move_Per_Day'] = (
            df.loc[option_mask, 'Recovery_Move_Required'] /
            dte_safe.loc[option_mask]
        )

    # Feasibility classification: required daily move vs HV-implied sigma
    df['Recovery_Feasibility'] = 'N/A'
    if 'Recovery_Move_Per_Day' in df.columns and 'HV_Daily_Move_1Sigma' in df.columns:
        ratio = df['Recovery_Move_Per_Day'] / df['HV_Daily_Move_1Sigma'].replace(0, np.nan)
        df.loc[option_mask & ratio.notna() & (ratio <= 0.5),                    'Recovery_Feasibility'] = 'FEASIBLE'
        df.loc[option_mask & ratio.notna() & (ratio > 0.5) & (ratio <= 1.5),    'Recovery_Feasibility'] = 'UNLIKELY'
        df.loc[option_mask & ratio.notna() & (ratio > 1.5),                     'Recovery_Feasibility'] = 'IMPOSSIBLE'

    # 8. Margin Carry Cost (Fidelity 10.375%/yr on position market value)
    # McMillan Ch.3: "The covered writer must earn at least the cost of carrying the stock."
    # Passarelli Ch.6: daily carry cost is the silent P&L drain — surface it so doctrine can see it.
    #
    # Daily_Margin_Cost: dollar interest Fidelity charges per day on this position's market value.
    #   For stocks/options on margin: market_value * 10.375% / 365
    #   This is what the yield maintenance trigger must beat to be profitable.
    #
    # Margin_Coverage_Days: for SHORT premium positions (BW/CC/CSP):
    #   how many days of option theta income covers the daily margin interest on the STOCK.
    #   > 1.0 = theta covers carry cost.  < 1.0 = losing money to carry even if flat.
    if 'Last' in df.columns and 'Quantity' in df.columns:
        if 'AssetType' in df.columns:
            multipliers = np.where(df['AssetType'] == 'OPTION', 100.0, 1.0)
        else:
            multipliers = 1.0
        market_value_abs = (df['Last'].abs() * df['Quantity'].abs() * multipliers).fillna(0)
        df['Daily_Margin_Cost'] = (market_value_abs * FIDELITY_MARGIN_RATE_DAILY).round(4)
    else:
        df['Daily_Margin_Cost'] = np.nan

    # Margin_Coverage_Days: theta income ÷ daily margin cost (for short premium strategies)
    # Only meaningful for short options — tells you "one day of theta income covers N days of margin"
    short_mask = (
        (df.get('AssetType', pd.Series('', index=df.index)) == 'OPTION') &
        (df.get('Quantity', pd.Series(0, index=df.index)) < 0)
    ) if 'AssetType' in df.columns else pd.Series(False, index=df.index)

    df['Margin_Coverage_Days'] = np.nan
    if 'Theta' in df.columns and 'Daily_Margin_Cost' in df.columns:
        theta_income_per_day = (
            df.loc[short_mask, 'Theta'].abs() * 100 *
            df.loc[short_mask, 'Quantity'].abs()
        )
        margin_cost = df.loc[short_mask, 'Daily_Margin_Cost'].replace(0, np.nan)
        df.loc[short_mask, 'Margin_Coverage_Days'] = (
            theta_income_per_day / margin_cost
        ).round(2)

    logger.info(f"✅ Cycle 2 Drift metrics computed for {len(df)} positions")
    return df
