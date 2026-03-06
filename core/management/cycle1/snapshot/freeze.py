"""
Phase 4: Entry Data Freeze

Captures entry conditions when a position is first seen.
"""

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


def freeze_entry_data(df: pd.DataFrame, new_trade_ids: list = None, new_leg_ids: set = None, ingest_context: str = "unspecified") -> pd.DataFrame:
    """
    Freeze entry data for newly discovered positions.
    
    RAG Authority: Passarelli / Natenberg (Sensitivity Anchoring)
    """
    if df.empty:
        return df
    
    df = df.copy()
    
    # === CYCLE 1 CANONICAL ENTRY SCHEMA (LOCKED) ===
    # These fields are stored in the SEPARATE entry_anchors table.
    # DTE is never persisted; it is derived at runtime.
    
    # RAG: Fix dtype initialization to prevent future warnings and ensure DuckDB schema integrity.
    if 'Entry_Timestamp' not in df.columns:
        df['Entry_Timestamp'] = pd.Series(pd.NaT, index=df.index, dtype='datetime64[ns]')
    if 'Entry_Snapshot_TS' not in df.columns:
        df['Entry_Snapshot_TS'] = pd.Series(pd.NaT, index=df.index, dtype='datetime64[ns]')
    if 'Entry_Structure' not in df.columns:
        df['Entry_Structure'] = pd.Series(dtype='string', index=df.index)
    
    float_cols = [
        'Delta_Entry', 'Gamma_Entry', 'Vega_Entry', 'Theta_Entry', 'Rho_Entry',
        'IV_Entry', 'Underlying_Price_Entry', 'Quantity_Entry', 'Basis_Entry',
        'Premium_Entry',
    ]
    for col in float_cols:
        if col not in df.columns:
            df[col] = pd.Series(dtype='float64', index=df.index)

    # Entry Chart States are STRING values (e.g., "STRUCTURAL_UP", "RANGE_BOUND") —
    # must be dtype='string', not float64.  DuckDB column is VARCHAR.
    string_state_cols = [
        'Entry_Chart_State_PriceStructure', 'Entry_Chart_State_TrendIntegrity',
        'Entry_Chart_State_VolatilityState', 'Entry_Chart_State_CompressionMaturity',
    ]
    for col in string_state_cols:
        if col not in df.columns:
            df[col] = pd.Series(dtype='string', index=df.index)

    if 'IV_Entry_Source' not in df.columns:
        df['IV_Entry_Source'] = pd.Series(dtype='string', index=df.index)
    if 'ingest_context' not in df.columns:
        df['ingest_context'] = pd.Series(dtype='string', index=df.index)

    # Vol/Regime freeze columns (RAG gap analysis — Bennett/Natenberg/Krishnan/Jabbour)
    vol_regime_float_cols = [
        'IV_30D_Entry', 'HV_20D_Entry', 'IV_Percentile_Entry',
        'Expected_Move_10D_Entry', 'Daily_Margin_Cost_Entry',
    ]
    for col in vol_regime_float_cols:
        if col not in df.columns:
            df[col] = pd.Series(dtype='float64', index=df.index)
    if 'Regime_Entry' not in df.columns:
        df['Regime_Entry'] = pd.Series(dtype='string', index=df.index)
    
    new_trade_ids = new_trade_ids or []
    
    # Initialize freeze_mask to an empty Series to prevent UnboundLocalError
    freeze_mask = pd.Series(False, index=df.index)

    # Mask for truly new positions (Keyed on TradeID)
    # RAG: McMillan (Identity). Freezing only occurs on the FIRST observation.
    if 'TradeID' in df.columns:
        freeze_mask = df['TradeID'].isin(new_trade_ids)
    else:
        logger.warning("⚠️ TradeID missing — skipping entry data freezing")
        return df
        
    num_to_freeze = freeze_mask.sum()
    
    if num_to_freeze == 0:
        logger.info("No positions need entry data freezing")
        return df
    
    logger.info(f"Freezing entry data for {num_to_freeze} positions. Context: {ingest_context}")
    
    # Freeze ingest_context
    df.loc[freeze_mask, 'ingest_context'] = ingest_context

    # Freeze Greeks
    df = _freeze_entry_greeks(df, freeze_mask)
    
    # Freeze IV
    df = _freeze_entry_iv(df, freeze_mask)
    
    # Freeze Premium
    df = _freeze_entry_premium(df, freeze_mask)
    
    # Freeze Context
    df = _freeze_entry_context(df, freeze_mask)

    # Freeze Liquidity (OI at entry)
    df = _freeze_entry_liquidity(df, freeze_mask)

    # Freeze Vol/Regime context (RAG gap analysis — Bennett/Natenberg/Krishnan)
    df = _freeze_entry_vol_regime(df, freeze_mask)

    # Canonical volatility entry aliases (Phase 1 migration — coexist with legacy names)
    df['IV_Contract_Entry']       = df.get('IV_Entry')
    df['IV_Underlying_30D_Entry'] = df.get('IV_30D_Entry')
    df['IV_Rank_Entry']           = df.get('IV_Percentile_Entry')

    # Freeze Entry Structure (Deterministic Classification)
    df = _freeze_entry_structure(df, freeze_mask, new_leg_ids)

    # Freeze Entry Chart States
    df = _freeze_entry_chart_states(df, freeze_mask)
    
    # Set entry timestamp
    # RAG: Recovery-Aware Freezing. Only populate if not already recovered from history.
    ts_target = freeze_mask & df['Entry_Timestamp'].isna()
    if ts_target.any():
        if 'Snapshot_TS' in df.columns:
            df.loc[ts_target, 'Entry_Timestamp'] = df.loc[ts_target, 'Snapshot_TS']
        else:
            df.loc[ts_target, 'Entry_Timestamp'] = pd.Timestamp.now()
    
    logger.info(f"✅ Entry data frozen for {num_to_freeze} positions")
    
    return df


def _freeze_entry_greeks(df: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
    df = df.copy()
    greek_mappings = [
        ('Delta', 'Delta_Entry'),
        ('Gamma', 'Gamma_Entry'),
        ('Vega', 'Vega_Entry'),
        ('Theta', 'Theta_Entry'),
        ('Rho', 'Rho_Entry'),
    ]
    for current_col, entry_col in greek_mappings:
        if current_col in df.columns:
            # RAG: Recovery-Aware Freezing. Only populate if not already recovered from history.
            freeze_target = mask & df[entry_col].isna()
            if freeze_target.any():
                df.loc[freeze_target, entry_col] = df.loc[freeze_target, current_col]
    return df


def _normalize_iv(series: pd.Series) -> pd.Series:
    """Normalize IV to decimal form. Brokers often report as percent (22.27 → 0.2227)."""
    return series.apply(lambda x: x / 100.0 if pd.notna(x) and x > 2.0 else x)


def _freeze_entry_iv(df: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
    """
    IV Handling Doctrine: Freeze IV if present in Fidelity.
    Fallback: Use live Schwab IV (IV_Now) if Fidelity IV is missing.
    RAG Authority: Natenberg (Neutrality / No Lookahead Bias)

    CRITICAL: Always normalize to decimal (0.0–2.0 range).
    Fidelity Broker Truth reports IV as percent (e.g. 22.27 = 22.27%).
    """
    df = df.copy()

    # 1. Try original IV column (Broker Truth — may be in percent format)
    if 'IV' in df.columns:
        freeze_target = mask & df['IV_Entry'].isna()
        if freeze_target.any():
            iv_values = _normalize_iv(
                pd.to_numeric(df.loc[freeze_target, 'IV'], errors='coerce')
            ).astype('float64')
            df.loc[freeze_target, 'IV_Entry'] = iv_values
            df.loc[freeze_target, 'IV_Entry_Source'] = 'BROKER_TRUTH'

    # 2. Fallback to IV_Now (Schwab) — already in decimal but normalize defensively
    if 'IV_Now' in df.columns:
        missing_iv_mask = mask & df['IV_Entry'].isna()
        if missing_iv_mask.any():
            iv_values = _normalize_iv(
                pd.to_numeric(df.loc[missing_iv_mask, 'IV_Now'], errors='coerce')
            ).astype('float64')
            df.loc[missing_iv_mask, 'IV_Entry'] = iv_values
            df.loc[missing_iv_mask, 'IV_Entry_Source'] = 'SCHWAB_LIVE'

    return df


def _freeze_entry_premium(df: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
    df = df.copy()
    # RAG: Recovery-Aware Freezing. Only populate if not already recovered from history.
    if 'AssetType' not in df.columns:
        logger.warning("⚠️ AssetType missing — skipping premium freezing")
        return df
        
    options_mask = mask & (df['AssetType'] == 'OPTION') & df['Premium_Entry'].isna()
    for idx in df[options_mask].index:
        quantity = df.at[idx, 'Quantity']
        if 'Time Val' in df.columns and pd.notna(df.at[idx, 'Time Val']):
            time_val = df.at[idx, 'Time Val']
            premium = abs(time_val) if quantity < 0 else -abs(time_val)
            df.at[idx, 'Premium_Entry'] = premium
        elif 'Last' in df.columns and pd.notna(df.at[idx, 'Last']):
            last = df.at[idx, 'Last']
            premium = abs(last) if quantity < 0 else -abs(last)
            df.at[idx, 'Premium_Entry'] = premium
    return df


def _freeze_entry_context(df: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
    """
    Freeze economic and price anchors.
    """
    df = df.copy()

    # Ensure target columns exist
    for col in ('Price_Target_Entry', 'DTE_Entry'):
        if col not in df.columns:
            df[col] = float('nan')

    # RAG: Recovery-Aware Freezing. Only populate if not already recovered from history.
    if 'UL Last' in df.columns:
        target = mask & df['Underlying_Price_Entry'].isna()
        df.loc[target, 'Underlying_Price_Entry'] = df.loc[target, 'UL Last']

    if 'Quantity' in df.columns:
        target = mask & df['Quantity_Entry'].isna()
        df.loc[target, 'Quantity_Entry'] = df.loc[target, 'Quantity']

    if 'Basis' in df.columns:
        target = mask & df['Basis_Entry'].isna()
        df.loc[target, 'Basis_Entry'] = df.loc[target, 'Basis']

    if 'Snapshot_TS' in df.columns:
        target = mask & df['Entry_Snapshot_TS'].isna()
        df.loc[target, 'Entry_Snapshot_TS'] = df.loc[target, 'Snapshot_TS']

    # Freeze DTE at entry (for price target formula and time-to-right comparisons)
    # AUDIT FIX: compute DTE from Expiration - Snapshot_TS instead of trusting
    # Fidelity CSV's pre-computed DTE column, which can be stale (e.g., exported
    # on a weekend with DTE already counting down from Friday's close).
    target = mask & df['DTE_Entry'].isna()
    if target.any() and 'Expiration' in df.columns and 'Snapshot_TS' in df.columns:
        _exp = pd.to_datetime(df.loc[target, 'Expiration'], errors='coerce')
        _ts = pd.to_datetime(df.loc[target, 'Snapshot_TS'], errors='coerce')
        _fresh_dte = (_exp.dt.normalize() - _ts.dt.normalize()).dt.days.clip(lower=0)
        df.loc[target, 'DTE_Entry'] = _fresh_dte
    elif target.any() and 'DTE' in df.columns:
        # Fallback to CSV DTE if Expiration/Snapshot_TS not available
        df.loc[target, 'DTE_Entry'] = df.loc[target, 'DTE']

    # Freeze IV-implied 1-sigma directional price target (OPTION legs only).
    # Natenberg Ch.11: "Once right on both speed and direction, the edge is consumed."
    # For LONG_PUT:  target = UL_Entry × (1 - IV_Entry × √(DTE_Entry/252))  — downside 1-sigma
    # For LONG_CALL: target = UL_Entry × (1 + IV_Entry × √(DTE_Entry/252))  — upside 1-sigma
    # Only computed for directional long option strategies where thesis satisfaction matters.
    _DIRECTIONAL_LONG = {
        'LONG_PUT', 'LONG_CALL', 'BUY_PUT', 'BUY_CALL',
        'LEAPS_PUT', 'LEAPS_CALL',
    }
    # NOTE: Price_Target_Entry is derived from existing anchors (UL_Entry, IV_Entry, DTE_Entry),
    # not from live data. Safe to backfill for ALL positions with missing values — not just new ones.
    # This fixes recovered/existing positions whose anchors were loaded from DuckDB but
    # Price_Target_Entry was never computed because freeze only ran for new_trade_ids.
    opt_mask = (
        df['Price_Target_Entry'].isna()
        & (df.get('AssetType', '') == 'OPTION')
    )
    for idx in df[opt_mask].index:
        try:
            strategy = str(df.at[idx, 'Strategy'] or '').upper()
            if not any(s in strategy for s in _DIRECTIONAL_LONG):
                continue
            ul_entry = float(df.at[idx, 'Underlying_Price_Entry'] or 0)
            iv_entry = float(df.at[idx, 'IV_Entry'] or 0)
            dte_entry = float(df.at[idx, 'DTE_Entry'] or 0)
            cp = str(df.at[idx, 'Call/Put'] or '').upper()
            if ul_entry <= 0 or iv_entry <= 0 or dte_entry <= 0:
                continue
            # Normalize IV: brokers may report as percent (22.3 = 22.3%)
            if iv_entry > 2.0:
                iv_entry = iv_entry / 100.0
            sigma_move = ul_entry * iv_entry * (dte_entry / 252) ** 0.5
            if 'P' in cp:
                df.at[idx, 'Price_Target_Entry'] = round(ul_entry - sigma_move, 2)
            else:
                df.at[idx, 'Price_Target_Entry'] = round(ul_entry + sigma_move, 2)
        except Exception:
            continue

    return df


def _freeze_entry_liquidity(df: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
    """
    Freeze Open Interest at entry for liquidity evolution tracking.

    Murphy (0.704): declining OI on a held contract signals liquidity draining.
    OI_Entry is compared to current Open_Int each management run to detect
    deterioration (crowd leaving the position).
    """
    df = df.copy()
    if 'OI_Entry' not in df.columns:
        df['OI_Entry'] = np.nan
    if 'Open_Int' in df.columns:
        freeze_target = mask & df['OI_Entry'].isna()
        if freeze_target.any():
            df.loc[freeze_target, 'OI_Entry'] = pd.to_numeric(
                df.loc[freeze_target, 'Open_Int'], errors='coerce'
            )
    return df


def _freeze_entry_vol_regime(df: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
    """
    Freeze volatility and regime context at entry.

    RAG Authority:
    - Bennett (0.719): HV at entry + VRP drift baseline
    - Natenberg (0.727): IV percentile context at entry
    - Krishnan (0.727): Regime/VIX context at entry
    - Jabbour (0.706): Expected move baseline for time value tracking
    - McMillan Ch.3: Margin carry cost baseline

    These are computed by Cycle 2 drift/enrichment and available at freeze time.
    Each frozen value enables a corresponding drift metric:
      IV_30D_Entry   → VRP drift = (IV_30D - HV_20D) - (IV_30D_Entry - HV_20D_Entry)
      HV_20D_Entry   → Vol regime shift detection
      IV_Percentile_Entry → IV rank compression/expansion tracking
      Regime_Entry   → Regime flip detection (was LOW_VOL, now HIGH_VOL)
      Expected_Move_10D_Entry → Move expectations drift
      Daily_Margin_Cost_Entry → Carry cost drift
    """
    df = df.copy()
    mappings = [
        ('IV_30D',              'IV_30D_Entry'),
        ('HV_20D',              'HV_20D_Entry'),
        ('IV_Percentile',       'IV_Percentile_Entry'),
        ('Regime_State',        'Regime_Entry'),
        ('Expected_Move_10D',   'Expected_Move_10D_Entry'),
        ('Daily_Margin_Cost',   'Daily_Margin_Cost_Entry'),
    ]
    for current_col, entry_col in mappings:
        if entry_col not in df.columns:
            if entry_col == 'Regime_Entry':
                df[entry_col] = pd.Series(dtype='string', index=df.index)
            else:
                df[entry_col] = np.nan
        if current_col in df.columns:
            freeze_target = mask & df[entry_col].isna()
            if freeze_target.any():
                if entry_col == 'Regime_Entry':
                    df.loc[freeze_target, entry_col] = df.loc[freeze_target, current_col].astype(str)
                else:
                    df.loc[freeze_target, entry_col] = pd.to_numeric(
                        df.loc[freeze_target, current_col], errors='coerce'
                    )
    return df


def _freeze_entry_chart_states(df: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
    """
    Freeze technical regime at entry.
    """
    df = df.copy()
    # RAG: Alignment. Match column names produced by chart_state_engine.py
    state_mappings = [
        ('PriceStructure_State', 'Entry_Chart_State_PriceStructure'),
        ('TrendIntegrity_State', 'Entry_Chart_State_TrendIntegrity'),
        ('VolatilityState_State', 'Entry_Chart_State_VolatilityState'),
        ('CompressionMaturity_State', 'Entry_Chart_State_CompressionMaturity'),
    ]
    for current_col, entry_col in state_mappings:
        if current_col in df.columns:
            # RAG: Recovery-Aware Freezing. Only populate if not already recovered from history.
            freeze_target = mask & df[entry_col].isna()
            if freeze_target.any():
                df.loc[freeze_target, entry_col] = df.loc[freeze_target, current_col]
    return df


def _freeze_entry_structure(df: pd.DataFrame, mask: pd.Series, new_leg_ids: set = None) -> pd.DataFrame:
    """
    Deterministic Strategy Classification at Entry.
    
    RAG Authority: Buy-Write vs Covered Call Doctrine.
    
    Rule: If stock and short call appear as NEW in the same snapshot -> BUY_WRITE.
    """
    df = df.copy()
    new_leg_ids = new_leg_ids or set()

    if 'TradeID' not in df.columns or 'Underlying_Ticker' not in df.columns or 'AssetType' not in df.columns:
        logger.warning("⚠️ Critical columns missing for structure freezing")
        return df

    # Process by TradeID to ensure unified classification for multi-leg entries
    for tid, group in df[mask].groupby('TradeID'):
        if group.empty: continue
        ticker = group['Underlying_Ticker'].iloc[0]
        
        # 1. Identify components in this specific snapshot
        has_new_short_call = any(
            (row['AssetType'] == 'OPTION') and 
            (row['Call/Put'] == 'Call') and 
            (row['Quantity'] < 0) and 
            (row['LegID'] in new_leg_ids)
            for _, row in group.iterrows()
        )
        
        # 2. Check for stock leg (either in this group or already active in the account)
        # To be a BUY_WRITE, the stock must be NEW in this same snapshot.
        has_new_stock = any(
            (row['AssetType'] == 'STOCK') and 
            (row['Quantity'] > 0) and 
            (row['LegID'] in new_leg_ids)
            for _, row in group.iterrows()
        )

        # 3. Determine Structure
        # RAG: Neutrality Mandate. Cycle 1 must not depend on Phase 3 'Strategy' field.
        structure = 'UNKNOWN'
        if has_new_short_call:
            if has_new_stock:
                structure = 'BUY_WRITE'
            else:
                # If call is new but no new stock in this snapshot, 
                # it's either a roll or a Covered Call (if stock existed previously).
                # Note: snapshot.py handles the 'preserved' stock check.
                structure = 'COVERED_CALL'
        elif any((row['AssetType'] == 'OPTION') and (row['Call/Put'] == 'Put') and (row['Quantity'] < 0) for _, row in group.iterrows()):
            structure = 'CSP'
        elif any((row['AssetType'] == 'OPTION') and (row['Call/Put'] == 'Call') and (row['Quantity'] > 0) for _, row in group.iterrows()):
            # LEAPS (DTE > 200 at entry) get distinct classification for doctrine routing.
            _lc_dte = max((float(row.get('DTE', 0) or 0) for _, row in group.iterrows()
                           if row.get('AssetType') == 'OPTION' and row.get('Call/Put') == 'Call'
                           and (row.get('Quantity', 0) or 0) > 0), default=0)
            structure = 'LEAPS_CALL' if _lc_dte > 200 else 'LONG_CALL'
        elif any((row['AssetType'] == 'OPTION') and (row['Call/Put'] == 'Put') and (row['Quantity'] > 0) for _, row in group.iterrows()):
            _lp_dte = max((float(row.get('DTE', 0) or 0) for _, row in group.iterrows()
                           if row.get('AssetType') == 'OPTION' and row.get('Call/Put') == 'Put'
                           and (row.get('Quantity', 0) or 0) > 0), default=0)
            structure = 'LEAPS_PUT' if _lp_dte > 200 else 'LONG_PUT'
        elif any(row['AssetType'] == 'STOCK' for _, row in group.iterrows()):
            structure = 'STOCK'
        else:
            # Fallback to UNKNOWN if no mechanical pattern matches.
            # Phase 3+ will perform full strategy inference later.
            structure = 'UNKNOWN'

        # Apply structure to all legs in this TradeID
        df.loc[df['TradeID'] == tid, 'Entry_Structure'] = structure

    return df


def validate_entry_freeze(df: pd.DataFrame) -> dict:
    results = {'valid': True, 'errors': [], 'warnings': []}
    existing_positions = df[df['First_Seen_Date'].notna()]
    if len(existing_positions) > 0:
        options = existing_positions[existing_positions['AssetType'] == 'OPTION']
        if len(options) > 0:
            missing_delta = options[options['Delta_Entry'].isna()]
            if len(missing_delta) > 0:
                results['errors'].append(f"{len(missing_delta)} options missing Delta_Entry")
                results['valid'] = False
    return results
