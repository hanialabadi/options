"""
Phase 3 Enrichment: Earnings Event Flagging

Flags positions that are event-driven setups (straddles/strangles near earnings).
"""

import pandas as pd
import numpy as np
import logging
from typing import Optional
from datetime import datetime
from core.data_contracts.config import MANAGEMENT_SAFE_MODE

from core.phase3_constants import (
    STRATEGY_LONG_STRADDLE,
    STRATEGY_LONG_STRANGLE,
    EARNINGS_VEGA_THRESHOLD,
    EARNINGS_PROXIMITY_DAYS_MIN,
    EARNINGS_PROXIMITY_DAYS_MAX,
    ASSET_TYPE_OPTION,
)

logger = logging.getLogger(__name__)

# Column name constants for schema integrity
ASSET_TYPE_COL = "AssetType"
STRATEGY_COL = "Strategy"
VEGA_COL = "Vega"
EARNINGS_DATE_COL = "Earnings_Date"
EARNINGS_DATE_ALT_COL = "Earnings Date"  # Legacy column name
TRADE_ID_COL = "TradeID"
DAYS_TO_EARNINGS_COL = "Days_to_Earnings"
IS_EVENT_SETUP_COL = "Is_Event_Setup"
EVENT_REASON_COL = "Event_Reason"


def tag_earnings_flags(
    df: pd.DataFrame, 
    reference_date: Optional[pd.Timestamp] = None
) -> pd.DataFrame:
    """
    Tag earnings-related event setups at trade level.
    """
    df = df.copy()
    
    # Initialize output columns early (proper NaN semantics)
    df[DAYS_TO_EARNINGS_COL] = np.nan
    df[IS_EVENT_SETUP_COL] = np.nan
    df[EVENT_REASON_COL] = np.nan
    
    # Use provided reference date or default to today
    if reference_date is None:
        reference_date = pd.Timestamp.today().normalize()
    else:
        reference_date = pd.to_datetime(reference_date).normalize()
    
    # Validate required columns
    required_cols = [ASSET_TYPE_COL, STRATEGY_COL, VEGA_COL]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"❌ Missing required columns for earnings flags: {missing_cols}")
    
    # Filter to options only
    options_mask = df[ASSET_TYPE_COL] == ASSET_TYPE_OPTION
    stocks_excluded = (~options_mask).sum()
    
    if not options_mask.any():
        if not MANAGEMENT_SAFE_MODE:
            logger.warning(f"⚠️ No option positions found ({stocks_excluded} non-options excluded)")
        return df
    
    if not MANAGEMENT_SAFE_MODE:
        logger.info(f"Processing earnings flags for {options_mask.sum()} option positions")

    # Normalize column naming
    if EARNINGS_DATE_COL not in df.columns and EARNINGS_DATE_ALT_COL in df.columns:
        df[EARNINGS_DATE_COL] = df[EARNINGS_DATE_ALT_COL]

    if EARNINGS_DATE_COL in df.columns:
        # Convert earnings date column
        df[EARNINGS_DATE_COL] = pd.to_datetime(df[EARNINGS_DATE_COL], errors="coerce")
        
        # Remove timezone from earnings dates
        if df.loc[options_mask, EARNINGS_DATE_COL].dt.tz is not None:
            df.loc[options_mask, EARNINGS_DATE_COL] = df.loc[options_mask, EARNINGS_DATE_COL].dt.tz_localize(None)
        
        # Calculate days to earnings only for options
        df.loc[options_mask, DAYS_TO_EARNINGS_COL] = (
            df.loc[options_mask, EARNINGS_DATE_COL] - reference_date
        ).dt.days
        
        # Validate Vega
        valid_vega = df[VEGA_COL].notna() & (df[VEGA_COL] >= 0)
        
        # Validate Strategy
        valid_strategy_for_setup = df[STRATEGY_COL].isin([STRATEGY_LONG_STRADDLE, STRATEGY_LONG_STRANGLE])
        
        # Validate earnings date
        valid_earnings_date = df[DAYS_TO_EARNINGS_COL].notna() & (df[DAYS_TO_EARNINGS_COL] >= EARNINGS_PROXIMITY_DAYS_MIN)
        
        # Calculate at trade level
        df_options = df[options_mask].copy()
        leg_criteria_options = (
            valid_strategy_for_setup[options_mask] &
            valid_vega[options_mask] &
            (df_options[VEGA_COL] >= EARNINGS_VEGA_THRESHOLD) &
            valid_earnings_date[options_mask] &
            (df_options[DAYS_TO_EARNINGS_COL] <= EARNINGS_PROXIMITY_DAYS_MAX)
        )
        
        df_options['_leg_criteria'] = leg_criteria_options
        trade_all_legs_valid = df_options.groupby(TRADE_ID_COL)['_leg_criteria'].transform('all')
        
        is_event_setup_series = pd.Series(index=df.index, dtype='boolean')
        is_event_setup_series[options_mask] = trade_all_legs_valid.values
        df.loc[options_mask, IS_EVENT_SETUP_COL] = is_event_setup_series[options_mask]

        # Event_Reason
        options_with_event_setup = options_mask & (df[IS_EVENT_SETUP_COL] == True)
        if options_with_event_setup.any():
            df.loc[options_with_event_setup, EVENT_REASON_COL] = (
                df.loc[options_with_event_setup].apply(
                    lambda row: (
                        f"Straddle/Strangle + Vega={row[VEGA_COL]:.2f} "
                        f"(>={EARNINGS_VEGA_THRESHOLD}) + "
                        f"Earnings in {int(row[DAYS_TO_EARNINGS_COL])} days"
                    ),
                    axis=1
                )
            )
        
        if not MANAGEMENT_SAFE_MODE:
            trades_with_events = df[df[IS_EVENT_SETUP_COL] == True][TRADE_ID_COL].nunique()
            if trades_with_events > 0:
                logger.info(f"✅ Earnings flags: {trades_with_events} trades identified as event-driven setups")
            else:
                logger.info("✅ Earnings flags: No event-driven setups found")
    else:
        if not MANAGEMENT_SAFE_MODE:
            logger.warning(f"⚠️ No {EARNINGS_DATE_COL} column found, skipping earnings flags")

    return df
