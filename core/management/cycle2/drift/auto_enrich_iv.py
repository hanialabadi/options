"""
Automatic IV Enrichment from Historical Data
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def auto_enrich_iv_from_archive(df: pd.DataFrame, as_of_date: pd.Timestamp = None) -> pd.DataFrame:
    """
    Automatically enrich positions with IV data from historical archive.
    """
    if df.empty:
        return df
    
    if 'Underlying_Ticker' in df.columns:
        ticker_col = 'Underlying_Ticker'
    else:
        logger.warning("⚠️  No 'Underlying_Ticker' column found, cannot enrich IV.")
        df['IV_Now'] = 0.0
        df['IV_Source'] = 'missing_ticker_column'
        return df
    
    if as_of_date is None:
        as_of_date = pd.Timestamp.now()
    
    try:
        from core.shared.data_contracts.config import PROJECT_ROOT
        canonical_path = PROJECT_ROOT / "data" / "ivhv_timeseries" / "ivhv_timeseries_canonical.csv"
        
        if not canonical_path.exists():
            logger.warning(f"⚠️  IV canonical time-series not found: {canonical_path}")
            df['IV_Now'] = 0.0
            df['IV_Source'] = 'archive_missing'
            return df
        
        iv_archive = pd.read_csv(canonical_path)
        iv_archive['date'] = pd.to_datetime(iv_archive['date'])
        
        cutoff_date = as_of_date - timedelta(days=7)
        recent_iv = iv_archive[iv_archive['date'] >= cutoff_date]
        
        if recent_iv.empty:
            df['IV_Now'] = 0.0
            df['IV_Source'] = 'stale_data'
            return df
        
        latest_iv = recent_iv.sort_values('date').groupby('ticker').last().reset_index()
        latest_iv = latest_iv[['ticker', 'iv_30d_call', 'date']].rename(columns={
            'ticker': '_ticker_for_merge',
            'iv_30d_call': 'IV_Now',
            'date': 'IV_Snapshot_Date'
        })
        
        df = df.merge(latest_iv, left_on=ticker_col, right_on='_ticker_for_merge', how='left')
        
        if '_ticker_for_merge' in df.columns:
            df = df.drop(columns=['_ticker_for_merge'])
        
        missing_iv_mask = df['IV_Now'].isna()
        df.loc[missing_iv_mask, 'IV_Now'] = 0.0
        df['IV_Source'] = 'archive'
        df.loc[missing_iv_mask, 'IV_Source'] = 'not_in_archive'
        
        return df
        
    except Exception as e:
        logger.error(f"❌ Error enriching IV from archive: {e}")
        df['IV_Now'] = 0.0
        df['IV_Source'] = 'error'
        return df


def get_iv_coverage_report(df: pd.DataFrame) -> dict:
    report = {'total_positions': len(df), 'iv_available': 0, 'iv_coverage_pct': 0.0, 'source_distribution': {}, 'avg_iv': 0.0}
    if df.empty or 'IV_Now' not in df.columns:
        return report
    iv_available_mask = (df['IV_Now'] > 0) & (df['IV_Now'].notna())
    report['iv_available'] = iv_available_mask.sum()
    report['iv_coverage_pct'] = report['iv_available'] / len(df) * 100
    if 'IV_Source' in df.columns:
        report['source_distribution'] = df['IV_Source'].value_counts().to_dict()
    if iv_available_mask.any():
        report['avg_iv'] = df.loc[iv_available_mask, 'IV_Now'].mean()
    return report
