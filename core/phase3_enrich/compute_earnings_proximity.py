"""
Phase 3 Observable: Earnings Proximity

Calculates days until next earnings announcement for risk awareness.
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime
from typing import Optional
from core.data_contracts.config import MANAGEMENT_SAFE_MODE

from core.data_layer.earnings_calendar import (
    get_earnings_date_yfinance,
    get_earnings_date_static,
    load_static_earnings_calendar,
)

logger = logging.getLogger(__name__)


def compute_earnings_proximity(
    df: pd.DataFrame,
    snapshot_ts: Optional[pd.Timestamp] = None
) -> pd.DataFrame:
    """
    Add earnings proximity columns (observation only).
    
    Parameters
    ----------
    df : pd.DataFrame
        Must contain 'Underlying_Ticker' column for canonical equity identity.
    snapshot_ts : pd.Timestamp, optional
        Reference timestamp for days calculation. If None, uses pd.Timestamp.now()
    
    Returns
    -------
    pd.DataFrame
        Input DataFrame with added columns:
        - 'Days_to_Earnings' (int or NaN): Calendar days until next earnings
        - 'Next_Earnings_Date' (datetime or NaT): Date of next earnings
        - 'Earnings_Source' (str): Data provenance
    """
    # === Management Safe Mode: Hard Kill external lookups (FIRST LINE) ===
    if MANAGEMENT_SAFE_MODE:
        logger.info("Earnings proximity skipped (Management Safe Mode)")
        return df.assign(
            Days_to_Earnings=np.nan,
            Next_Earnings_Date=pd.NaT,
            Earnings_Source="unknown"
        )

    if df.empty:
        return df

    # Initialize output columns
    df["Days_to_Earnings"] = np.nan
    df["Next_Earnings_Date"] = pd.NaT
    df["Earnings_Source"] = "unknown"
    
    # Determine ticker column - ALWAYS use Underlying_Ticker for earnings
    # This enforces the canonical symbol identity law.
    if "Underlying_Ticker" in df.columns:
        ticker_col = "Underlying_Ticker"
    else:
        logger.error("Missing 'Underlying_Ticker' column for earnings lookup. Ensure Phase 2 normalization ran.")
        df["Earnings_Source"] = "missing"
        return df
    
    # Use current time if not provided
    if snapshot_ts is None:
        snapshot_ts = pd.Timestamp.now()
    else:
        snapshot_ts = pd.to_datetime(snapshot_ts)
    
    # Load static calendar (fallback)
    static_calendar = load_static_earnings_calendar()
    
    # Process each unique ticker from STOCK rows only
    # Options never trigger equity lookups by design.
    stock_tickers = df[df["AssetType"] == "STOCK"][ticker_col].unique()
    logger.info(f"Computing earnings proximity for {len(stock_tickers)} unique equity tickers")
    
    yfinance_count = 0
    static_count = 0
    unknown_count = 0
    
    for ticker in stock_tickers:
        if pd.isna(ticker):
            continue
        
        earnings_date = None
        source = "unknown"
        
        # Priority 1: Yahoo Finance (primary source)
        try:
            earnings_date = get_earnings_date_yfinance(ticker)
            if earnings_date:
                source = "yfinance"
                yfinance_count += 1
        except Exception as e:
            logger.debug(f"Yahoo Finance lookup failed for {ticker}: {e}")
        
        # Priority 2: Static calendar (fallback)
        if earnings_date is None and static_calendar is not None:
            try:
                earnings_date = get_earnings_date_static(ticker, static_calendar)
                if earnings_date:
                    source = "static"
                    static_count += 1
            except Exception as e:
                logger.debug(f"Static calendar lookup failed for {ticker}: {e}")
        
        # Calculate days to earnings
        if earnings_date:
            earnings_ts = pd.Timestamp(earnings_date)
            days_to = (earnings_ts - snapshot_ts).days
            
            # Update all rows (both stock and option) for this underlying
            mask = df[ticker_col] == ticker
            df.loc[mask, "Days_to_Earnings"] = days_to
            df.loc[mask, "Next_Earnings_Date"] = earnings_ts
            df.loc[mask, "Earnings_Source"] = source
        else:
            unknown_count += 1
    
    # Summary statistics
    logger.info(
        f"Earnings proximity calculation complete: "
        f"{yfinance_count} from Yahoo Finance, "
        f"{static_count} from static calendar, "
        f"{unknown_count} unknown"
    )
    
    return df
