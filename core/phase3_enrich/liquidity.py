import pandas as pd
import numpy as np

def enrich_liquidity(
    df: pd.DataFrame,
    oi_threshold: int = 500,
    spread_pct_threshold: float = 0.05,
    min_dollar_volume: float = 5000,
    min_vega_efficiency: float = 0.00001,
    wide_spread_threshold: float = 0.10
) -> pd.DataFrame:
    """
    Enrich options DataFrame with liquidity screening columns:
      - Numeric columns: OI, Spread_Pct, Dollar_Volume, Vega_Efficiency, etc.
      - Tag/flag columns: OI_OK, Spread_OK, DollarVolume_OK, Vega_OK, WideSpread_Flag, Liquidity_OK, etc.
    """
    df = df.copy()
    
    # --- Numeric columns ---
    df['OI'] = df['Open Int']
    mid_price = (df['Ask'] + df['Bid']) / 2
    df['Spread_Pct'] = np.where(mid_price > 0, (df['Ask'] - df['Bid']) / mid_price, np.nan)
    df['Dollar_Volume'] = df['Volume'] * mid_price
    df['Vega_Efficiency'] = df['Vega'] / np.maximum((df['Ask'] - df['Bid']).abs(), 0.01)

    # --- Tag/flag columns (Booleans) ---
    df['OI_OK'] = df['OI'] >= oi_threshold
    df['Spread_OK'] = df['Spread_Pct'] <= spread_pct_threshold
    df['WideSpread_Flag'] = df['Spread_Pct'] > wide_spread_threshold
    df['DollarVolume_OK'] = df['Dollar_Volume'] >= min_dollar_volume
    df['Vega_OK'] = df['Vega_Efficiency'] >= min_vega_efficiency

    # All checks must pass for "Liquidity_OK"
    df['Liquidity_OK'] = (
        df['OI_OK'] &
        df['Spread_OK'] &
        df['DollarVolume_OK'] &
        df['Vega_OK']
    )

    return df
