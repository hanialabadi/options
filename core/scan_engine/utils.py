"""
Utility functions for scan pipeline validation and helpers.
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)


def validate_input(df: pd.DataFrame, required_cols: list, step_name: str) -> bool:
    """
    Validate input DataFrame before processing.
    
    Purpose:
        Pre-flight check to ensure required columns exist and DataFrame is not empty.
        Prevents cryptic errors downstream by failing fast with informative messages.
    
    Args:
        df (pd.DataFrame): Input DataFrame to validate
        required_cols (list): List of column names that must exist
        step_name (str): Name of the step (for logging context)
    
    Returns:
        bool: True if validation passes, False if DataFrame is empty
    
    Raises:
        ValueError: If required columns are missing
    
    Example:
        >>> validate_input(df, ['Ticker', 'IVHV_gap_30D'], 'Step 5')
        ✅ Step 5: 150 rows, all required columns present
        True
    """
    missing = [col for col in required_cols if col not in df.columns]
    import warnings
    if missing:
        error_msg = (
            f"❌ {step_name}: Missing required columns {missing}. "
            "This is a critical pipeline error. The schema contract was violated upstream. "
            "Cannot proceed with incomplete data in a production environment."
        )
        logger.error(error_msg)
        raise ValueError(error_msg)
    if df.empty:
        logger.warning(f"⚠️ {step_name}: Input is empty")
        return False
    logger.info(f"✅ {step_name}: {len(df)} rows, all required columns present")
    return True
