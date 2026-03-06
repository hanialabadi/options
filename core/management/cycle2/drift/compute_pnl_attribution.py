"""
Phase 3: P&L Attribution

Decomposes realized and unrealized P&L into Greek contributions.
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)


def rehydrate_entry_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rehydrate frozen entry data from DuckDB for P&L attribution.
    
    RAG Authority: McMillan (Identity). Key on Symbol (OCC).
    """
    if df.empty:
        return df
        
    identity_keys = df['Symbol'].unique().tolist()
    from core.shared.data_contracts.config import PROJECT_ROOT
    db_path = PROJECT_ROOT / "data" / "pipeline.duckdb"
    
    if not db_path.exists():
        return df
        
    import duckdb
    try:
        with duckdb.connect(str(db_path)) as con:
            table_exists = con.execute("""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = 'clean_legs' AND table_schema = 'main'
            """).fetchone()[0] > 0
            
            if not table_exists:
                return df

            db_cols_info = con.execute("PRAGMA table_info('clean_legs')").fetchall()
            entry_cols = [row[1] for row in db_cols_info if row[1].endswith('_Entry') or row[1] == 'Entry_Timestamp' or row[1] == 'First_Seen_Date']
            
            if not entry_cols:
                return df

            placeholders = ', '.join(['?' for _ in identity_keys])
            cols_str = ', '.join([f'"{c}"' for c in entry_cols])
            
            # RAG-Correct Entry: Use Canonical Anchors View
            query = f"""
                SELECT 
                    Symbol, 
                    {cols_str}
                FROM canonical_anchors
                WHERE Symbol IN ({placeholders})
                QUALIFY row_number() OVER (PARTITION BY Symbol ORDER BY Snapshot_TS DESC) = 1
            """
            df_existing = con.execute(query, identity_keys).df()
            
            if not df_existing.empty:
                for col in entry_cols:
                    if col in df.columns:
                        # Only drop if we have better data coming from DB
                        if df[col].isna().all():
                            df = df.drop(columns=[col])
                
                df = df.merge(df_existing, on='Symbol', how='left')
                logger.info(f"Rehydrated entry data for {len(df_existing)} contracts for attribution")
                
    except Exception as e:
        logger.warning(f"Failed to rehydrate entry data: {e}")
        
    return df


def compute_pnl_attribution(df: pd.DataFrame) -> pd.DataFrame:
    """
    Decompose P&L into Greek contributions.
    """
    if df.empty:
        return df
    
    df = df.copy()
    
    # Ensure all potential entry columns exist to avoid KeyErrors
    potential_entry_cols = [
        'Delta_Entry', 'Theta_Entry', 'Vega_Entry', 'Gamma_Entry', 
        'Underlying_Price_Entry', 'IV_Entry', 'IV_Entry_Source', 'Entry_Timestamp'
    ]
    for col in potential_entry_cols:
        if col not in df.columns:
            df[col] = np.nan

    # Rehydrate entry data if missing
    required_entry_cols = ['Delta_Entry', 'Theta_Entry', 'Vega_Entry', 'Gamma_Entry']
    if any(df[col].isna().all() for col in required_entry_cols):
        df = rehydrate_entry_data(df)

    # RAG: Back-fill IV_Entry from history if still missing
    # This ensures positions discovered before IV tracking was active get an anchor.
    if 'IV_Entry' in df.columns:
        missing_iv_entry = df[df['AssetType'] == 'OPTION']['IV_Entry'].isna()
        if missing_iv_entry.any():
            try:
                from core.management.cycle2.providers.iv_history_provider import get_earliest_iv_batch
                symbols_to_fill = df.loc[df['AssetType'] == 'OPTION', 'Symbol'].unique().tolist()
                earliest_iv_map = get_earliest_iv_batch(symbols_to_fill)
                
                # Only fill if IV_Entry is currently NaN
                df.loc[df['IV_Entry'].isna(), 'IV_Entry'] = df['Symbol'].map(earliest_iv_map)
                logger.info(f"Back-filled IV_Entry for {len(earliest_iv_map)} symbols from history")
            except Exception as e:
                logger.warning(f"Failed to back-fill IV_Entry from history: {e}")

    # Cycle 2: Fetch transient IV_Now using Governed Provider (Smart Persistence)
    try:
        from core.management.cycle2.providers.governed_iv_provider import fetch_governed_sensor_readings
        import re
        
        # RAG: IV provider expects option symbols for option IV. Underlying IV is not required.
        # The previous "Hard Assertion" was incorrectly filtering out valid OCC option symbols.
        # Removed redundant OCC option symbol check.
        # GovernedIVProvider is now expected to handle both underlying and option symbols.
        
        symbols_to_fetch = df['Symbol'].unique().tolist() # Pass all symbols, let GovernedIVProvider filter if needed
        
        # RAG: Smart Enrichment. Uses GovernedIVProvider with DuckDB persistence,
        # historical fallbacks, and smart backoffs using DuckDB datastamps.
        # Note: In run_all.py, we pass schwab_live. Here we use default (False) 
        # to favor cache/fallback unless explicitly called with live intent.
        readings = fetch_governed_sensor_readings(symbols_to_fetch, schwab_live=False)
        iv_map = {r['Symbol']: r['IV'] for r in readings if r.get('IV') is not None}
        
        df['IV_Now'] = df['Symbol'].map(iv_map)
        
        # RAG: Compatibility. Map IV_Now to IV_30D for Decision Engine.
        df['IV_30D'] = df['IV_Now']
        
        # Update Greeks if available from fresh readings (not fallbacks)
        for r in readings:
            if not r.get('Is_Fallback') and r.get('Source') == 'schwab':
                mask = df['Symbol'] == r['Symbol']
                for greek in ['Delta', 'Gamma', 'Vega', 'Theta', 'Rho']:
                    if r.get(greek) is not None:
                        df.loc[mask, greek] = r[greek]

        logger.info(f"Enriched {len(symbols_to_fetch)} symbols with Governed IV/Greeks (Smart Persistence)")
    except Exception as e:
        logger.warning(f"Failed to fetch Governed IV: {e}")
        if 'IV_Now' not in df.columns:
            df['IV_Now'] = np.nan

    attribution_cols = [
        'PnL_From_Delta',
        'PnL_From_Theta',
        'PnL_From_Vega',
        'PnL_From_Gamma',
        'PnL_Unexplained'
    ]
    
    for col in attribution_cols:
        if col not in df.columns:
            df[col] = 0.0

    if 'PnL_Attribution_Quality' not in df.columns:
        df['PnL_Attribution_Quality'] = 'High'
    
    has_entry_greeks = df['Delta_Entry'].notna()
    num_with_entry = has_entry_greeks.sum()
    
    if num_with_entry == 0:
        return df
    
    df = _compute_options_attribution(df, has_entry_greeks)
    df = _compute_stocks_attribution(df, has_entry_greeks)
    df = _compute_attribution_quality(df, has_entry_greeks)

    # RAG: Passarelli Signals. Derive Dominant Pressure and Drift Persistence.
    # These are required for the Sensitivity Evidence lens.
    df = _compute_passarelli_signals(df)
    
    # Cast to Categorical for Cycle-3 Contract
    df['PnL_Attribution_Quality'] = pd.Categorical(
        df['PnL_Attribution_Quality'], 
        categories=['Low', 'Medium', 'High'], 
        ordered=True
    )
    
    return df


def _compute_options_attribution(df: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
    df = df.copy()
    options_mask = mask & (df['AssetType'] == 'OPTION')
    
    # Hard Guard: Prevent reference to unknown IV columns (e.g. legacy 'IV Mid')
    # RAG Authority: Cycle-2 Contract (Inputs vs Derived)
    ALLOWED_IV_INPUTS = {'IV_Now', 'IV_Entry', 'IV_Entry_Source', 'IV_30D'}
    ALLOWED_IV_DERIVED = {'IV_Drift', 'IV_Drift_Structural', 'IV_Drift_1D', 'IV_Drift_3D', 'IV_Drift_10D'}
    ALLOWED_IV_ROC = {'IV_ROC_1D', 'IV_ROC_3D', 'IV_ROC_10D'}  # windowed slope metrics
    ALLOWED_IV_VOL_STATE = {'IV_vs_HV_Gap', 'IV_Percentile', 'IV_Percentile_Depth'}  # vol-state diagnostics
    # Phase 1 migration aliases (run_all.py lines 273-275): canonical names coexist with old names
    ALLOWED_IV_ALIASES = {'IV_Contract', 'IV_Underlying_30D', 'IV_Rank'}
    # Entry-frozen IV columns: frozen at position open, recovered from DuckDB anchors
    ALLOWED_IV_ENTRY = {'IV_Contract_Entry', 'IV_Underlying_30D_Entry', 'IV_30D_Entry', 'IV_Rank_Entry', 'IV_Percentile_Entry'}
    ALLOWED_IV_COLS = ALLOWED_IV_INPUTS | ALLOWED_IV_DERIVED | ALLOWED_IV_ROC | ALLOWED_IV_VOL_STATE | ALLOWED_IV_ALIASES | ALLOWED_IV_ENTRY

    found_iv_cols = {
        col for col in df.columns
        if col.startswith('IV_') and col not in ALLOWED_IV_COLS
    }

    if found_iv_cols:
        raise RuntimeError(
            f"Cycle-2 IV contract violated: Unauthorized IV columns found: {found_iv_cols}"
        )

    for idx in df[options_mask].index:
        quantity = df.at[idx, 'Quantity']
        delta_entry = df.at[idx, 'Delta_Entry']
        theta_entry = df.at[idx, 'Theta_Entry']
        vega_entry = df.at[idx, 'Vega_Entry']
        gamma_entry = df.at[idx, 'Gamma_Entry']
        ul_price_entry = df.at[idx, 'Underlying_Price_Entry']
        ul_price_current = df.at[idx, 'UL Last']
        days_in_trade = df.at[idx, 'Days_In_Trade'] if 'Days_In_Trade' in df.columns else 0
        
        # Cycle-2 Authoritative IV Sourcing: IV_Now (transient) -> IV_Entry (anchor) -> NaN
        if 'IV_Now' in df.columns and pd.notna(df.at[idx, 'IV_Now']):
            iv_current = df.at[idx, 'IV_Now']
        elif 'IV_Entry' in df.columns and pd.notna(df.at[idx, 'IV_Entry']):
            iv_current = df.at[idx, 'IV_Entry']
        else:
            iv_current = np.nan
            df.at[idx, 'PnL_Attribution_Quality'] = 'Low'
        
        # RAG: Quality. If any entry Greek is missing, quality is Low.
        if pd.isna(delta_entry) or pd.isna(theta_entry) or pd.isna(vega_entry) or pd.isna(gamma_entry):
            df.at[idx, 'PnL_Attribution_Quality'] = 'Low'
        
        # Normalize IV_Entry to Decimal Vol if it's in percent (Cycle 1 legacy)
        iv_entry = df.at[idx, 'IV_Entry'] if 'IV_Entry' in df.columns else np.nan
        iv_entry_val = iv_entry / 100.0 if pd.notna(iv_entry) and iv_entry > 2.0 else iv_entry

        # RAG: Robustness. Handle cases where UL Last is missing for the option row but available in the stock row.
        if pd.isna(ul_price_current):
            ticker = df.at[idx, 'Underlying_Ticker'] if 'Underlying_Ticker' in df.columns else None
            if ticker:
                stock_rows = df[(df['AssetType'] == 'STOCK') & (df['Symbol'] == ticker)]
                if not stock_rows.empty:
                    ul_price_current = stock_rows['UL Last'].iloc[0]
        
        # Fallback to 'UL_Last' if 'UL Last' is missing (schema variation)
        if pd.isna(ul_price_current) and 'UL_Last' in df.columns:
            ul_price_current = df.at[idx, 'UL_Last']

        if pd.isna(delta_entry) or pd.isna(ul_price_entry) or pd.isna(ul_price_current):
            logger.info(f"Skipping attribution for {df.at[idx, 'Symbol']}: Delta_Entry={delta_entry}, UL_Price_Entry={ul_price_entry}, UL_Price_Current={ul_price_current}")
            continue
        
        price_move = ul_price_current - ul_price_entry
        df.at[idx, 'PnL_From_Delta'] = price_move * delta_entry * 100 * quantity
        
        if pd.notna(theta_entry) and pd.notna(days_in_trade):
            df.at[idx, 'PnL_From_Theta'] = theta_entry * days_in_trade * 100 * quantity
        
        if pd.notna(vega_entry) and pd.notna(iv_entry_val) and pd.notna(iv_current):
            # Vega is price change per 1% (0.01) change in IV
            # iv_change in decimal (e.g. 0.01 for 1% move)
            iv_change = iv_current - iv_entry_val
            # Formula: Vega * (IV_move_in_percent) * 100 * Quantity
            # Since iv_change is decimal, iv_change * 100 is the percent move
            df.at[idx, 'PnL_From_Vega'] = vega_entry * (iv_change * 100.0) * 100.0 * quantity
        else:
            df.at[idx, 'PnL_From_Vega'] = 0.0
        
        if pd.notna(gamma_entry):
            df.at[idx, 'PnL_From_Gamma'] = 0.5 * gamma_entry * (price_move ** 2) * 100 * quantity
        else:
            df.at[idx, 'PnL_From_Gamma'] = 0.0
    return df


def _compute_stocks_attribution(df: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
    df = df.copy()
    stocks_mask = mask & (df['AssetType'] == 'STOCK')
    for idx in df[stocks_mask].index:
        quantity = df.at[idx, 'Quantity']
        ul_price_entry = df.at[idx, 'Underlying_Price_Entry']
        ul_price_current = df.at[idx, 'UL Last']
        if pd.isna(ul_price_entry) or pd.isna(ul_price_current):
            continue
        df.at[idx, 'PnL_From_Delta'] = (ul_price_current - ul_price_entry) * quantity
        df.at[idx, 'PnL_From_Theta'] = 0.0
        df.at[idx, 'PnL_From_Vega'] = 0.0
        df.at[idx, 'PnL_From_Gamma'] = 0.0
    return df


def _compute_attribution_quality(df: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
    """
    Enforce Reconciliation Identity:
    PnL_From_Delta + PnL_From_Theta + PnL_From_Vega + PnL_From_Gamma + PnL_Unexplained = Total_PnL
    """
    df = df.copy()
    for idx in df[mask].index:
        actual_pnl = df.at[idx, 'Unrealized_PnL'] if 'Unrealized_PnL' in df.columns else 0.0
        
        attributed_pnl = (
            df.at[idx, 'PnL_From_Delta'] + 
            df.at[idx, 'PnL_From_Theta'] + 
            df.at[idx, 'PnL_From_Vega'] + 
            df.at[idx, 'PnL_From_Gamma']
        )
        
        df.at[idx, 'PnL_Unexplained'] = actual_pnl - attributed_pnl
        
        # RAG: Quality. If unexplained P&L is more than 20% of total P&L (and total P&L is significant),
        # downgrade quality to Medium.
        if df.at[idx, 'PnL_Attribution_Quality'] == 'High':
            if abs(actual_pnl) > 10.0:
                unexplained_ratio = abs(df.at[idx, 'PnL_Unexplained'] / actual_pnl)
                if unexplained_ratio > 0.50:
                    df.at[idx, 'PnL_Attribution_Quality'] = 'Low'
                elif unexplained_ratio > 0.20:
                    df.at[idx, 'PnL_Attribution_Quality'] = 'Medium'
            
    return df


def _compute_passarelli_signals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Derive Cycle 2 signals for Sensitivity Analysis.
    """
    df = df.copy()
    
    # 1. Dominant Pressure
    attribution_cols = ['PnL_From_Delta', 'PnL_From_Theta', 'PnL_From_Vega', 'PnL_From_Gamma']
    def get_dominant_pressure(row):
        vals = {col.split('_')[-1]: abs(row[col]) for col in attribution_cols if col in row and pd.notna(row[col])}
        if not vals or all(v == 0 for v in vals.values()): return 'N/A'
        return max(vals, key=vals.get)
    
    df['Dominant_Pressure'] = df.apply(get_dominant_pressure, axis=1)

    # 2. Drift Persistence
    # RAG: Persistence. If Delta Drift and Structural Delta Drift have the same sign, it's Sustained.
    if 'Delta_Drift' in df.columns and 'Delta_Drift_Structural' in df.columns:
        def get_persistence(row):
            d = row.get('Delta_Drift', 0)
            s = row.get('Delta_Drift_Structural', 0)
            if pd.isna(d) or pd.isna(s) or d == 0 or s == 0: return 'Transient'
            if np.sign(d) == np.sign(s):
                return 'Sustained'
            return 'Transient'
        df['Drift_Persistence'] = df.apply(get_persistence, axis=1)
    else:
        df['Drift_Persistence'] = 'Transient'
        
    return df


def aggregate_trade_pnl_attribution(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate P&L attribution to trade level.
    """
    if df.empty or 'TradeID' not in df.columns:
        return df
    df = df.copy()
    attribution_cols = ['PnL_From_Delta', 'PnL_From_Theta', 'PnL_From_Vega', 'PnL_From_Gamma', 'PnL_Unexplained']
    for col in attribution_cols:
        if col in df.columns:
            df[f"{col}_Trade"] = df.groupby('TradeID')[col].transform('sum')
    
    # Also aggregate PnL_Total to trade level
    if 'PnL_Total' in df.columns:
        df['PnL_Total_Trade'] = df.groupby('TradeID')['PnL_Total'].transform('sum')

    return df
