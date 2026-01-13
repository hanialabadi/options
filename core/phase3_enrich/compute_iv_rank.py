"""
Phase 3 Observable: IV Rank

Calculates IV percentile rank (0-100) for volatility context.
"""

import pandas as pd
import numpy as np
import logging

from core.volatility.compute_iv_rank_252d import compute_iv_rank_batch
from core.data_contracts.config import MANAGEMENT_SAFE_MODE

logger = logging.getLogger(__name__)


def compute_iv_rank(df: pd.DataFrame, lookback_days: int = 252) -> pd.DataFrame:
    """
    Add IV_Rank column (0-100 percentile of current IV vs ticker's history).
    
    Parameters
    ----------
    df : pd.DataFrame
        Must contain 'Underlying_Ticker' column for canonical equity identity.
    """
    if df.empty:
        return df
    
    # Determine ticker column - ALWAYS use Underlying_Ticker for IV history
    # This enforces the canonical symbol identity law.
    if "Underlying_Ticker" in df.columns:
        ticker_col = "Underlying_Ticker"
    else:
        logger.error("Missing 'Underlying_Ticker' column for IV_Rank calculation. Ensure Phase 2 normalization ran.")
        df["IV_Rank"] = np.nan
        df["IV_Rank_Source"] = "error"
        df["IV_Rank_History_Days"] = 0
        return df
    
    if "IV Mid" not in df.columns:
        if not MANAGEMENT_SAFE_MODE:
            logger.warning("Missing 'IV Mid' column for IV_Rank calculation, setting to NaN")
        df["IV_Rank"] = np.nan
        df["IV_Rank_Source"] = "missing_iv"
        df["IV_Rank_History_Days"] = 0
        return df
    
    # Use shared neutral module for calculation
    try:
        # Management Safe Mode: Short-circuit history requirements
        effective_min_history = 0 if MANAGEMENT_SAFE_MODE else 120
        
        df = compute_iv_rank_batch(
            df=df,
            symbol_col=ticker_col,
            iv_col="IV Mid",
            date_col="Snapshot_Date" if "Snapshot_Date" in df.columns else None,
            lookback_days=lookback_days,
            min_history_days=effective_min_history,
            iv_column="iv_30d_call"  # 30-day ATM call IV
        )
        
        # Log summary
        if not MANAGEMENT_SAFE_MODE:
            valid_count = df["IV_Rank"].notna().sum()
            total_count = len(df)
            logger.info(
                f"IV_Rank calculated for {valid_count}/{total_count} positions "
                f"({valid_count/total_count*100:.1f}% coverage)"
            )
        
    except Exception as e:
        logger.error(f"Error calculating IV_Rank: {e}", exc_info=True)
        df["IV_Rank"] = np.nan
        df["IV_Rank_Source"] = "error"
        df["IV_Rank_History_Days"] = 0
    
    return df
