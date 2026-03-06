"""
Automatic HV Enrichment with Governance and Fallback
"""

import pandas as pd
import logging
import numpy as np
import re
from core.management.cycle2.providers.governed_hv_provider import fetch_governed_hv_batch

logger = logging.getLogger(__name__)

def auto_enrich_hv_from_schwab(df: pd.DataFrame, schwab_live: bool = False) -> pd.DataFrame:
    """
    Automatically enrich positions with HV_20D data using governed hierarchy.
    
    Requirement:
    - Add columns: HV_20D, HV_20D_Source, HV_20D_Computed_TS, HV_20D_Age_Days
    - One value per underlying symbol
    - Join back to option rows via Underlying_Ticker
    """
    if df.empty:
        return df
    
    if 'Underlying_Ticker' not in df.columns:
        logger.warning("⚠️  No 'Underlying_Ticker' column found, cannot enrich HV_20D.")
        df['HV_20D'] = np.nan
        df['HV_20D_Source'] = 'MISSING'
        return df

    try:
        # RAG: Fix — Filter symbols to underlyings only.
        option_pattern = re.compile(r'\d{6}[CP]\d+')
        tickers = [
            str(s) for s in df['Underlying_Ticker'].unique() 
            if isinstance(s, str) and not option_pattern.search(s)
        ]

        # Runtime log and hard assertion
        logger.error(f"HV ENRICH TICKERS (RUNTIME): {tickers}")
        bad = [t for t in tickers if option_pattern.search(str(t))]
        if bad:
            raise RuntimeError(f"OCC SYMBOLS IN HV ENRICHMENT: {bad}")

        logger.info(f"Fetching governed HV_20D for {len(tickers)} tickers (schwab_live={schwab_live})...")
        
        # Pass schwab_live to batch fetcher
        hv_df = fetch_governed_hv_batch(tickers, schwab_live=schwab_live)
        
        # Merge back to main dataframe
        # RAG: Avoid duplicate columns by dropping existing HV columns before merge
        hv_cols = ['HV_20D', 'HV_20D_Source', 'HV_20D_Computed_TS', 'HV_20D_Age_Days']
        cols_to_drop = [c for c in hv_cols if c in df.columns]
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)
            
        df = df.merge(hv_df, on='Underlying_Ticker', how='left')
        
        # Governance: Never fill with 0.0. Use NaN/MISSING.
        missing_mask = df['HV_20D'].isna()
        if missing_mask.any():
            logger.error(f"❌ Failed to retrieve HV_20D for {missing_mask.sum()} rows.")
            df.loc[missing_mask, 'HV_20D_Source'] = 'MISSING'
            
        return df
        
    except Exception as e:
        logger.error(f"❌ Error enriching HV_20D: {e}")
        if 'HV_20D' not in df.columns:
            df['HV_20D'] = np.nan
            df['HV_20D_Source'] = 'ERROR'
        return df
