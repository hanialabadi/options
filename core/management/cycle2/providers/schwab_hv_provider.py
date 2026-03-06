import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from scan_engine.loaders.schwab_api_client import SchwabClient

logger = logging.getLogger(__name__)

class SchwabHVProvider:
    """
    HV Provider for Cycle 2.
    Fetches daily price history from Schwab and computes 20-day Historical Volatility.
    """
    def __init__(self):
        self.client = SchwabClient()

    def compute_hv_20d(self, symbol: str) -> Optional[float]:
        """
        Fetches 20+ days of price history and computes annualized HV_20D.
        Formula:
        log_returns = ln(close_t / close_{t-1})
        HV_20D = std(log_returns_20) * sqrt(252)
        """
        try:
            # We need 21 days of prices to get 20 log returns
            # Requesting 2 months to ensure we have enough daily data even across holidays/weekends
            data = self.client.get_price_history(
                symbol=symbol,
                periodType='month',
                period=2,
                frequencyType='daily',
                frequency=1
            )
            
            candles = data.get('candles', [])
            if len(candles) < 21:
                logger.warning(f"Insufficient price history for {symbol}: found {len(candles)} candles, need 21.")
                return None
            
            # Extract close prices and sort by datetime (though Schwab usually returns them sorted)
            df = pd.DataFrame(candles)
            df = df.sort_values('datetime')
            
            # Take the last 21 candles to ensure we have 20 returns
            df = df.tail(21)
            
            # Compute log returns
            df['log_return'] = np.log(df['close'] / df['close'].shift(1))
            
            # Drop the first NaN
            log_returns = df['log_return'].dropna()
            
            if len(log_returns) < 20:
                logger.warning(f"Insufficient log returns for {symbol}: {len(log_returns)}")
                return None
                
            # Compute annualized HV
            hv_20d = log_returns.std() * np.sqrt(252)
            
            return float(hv_20d)
            
        except Exception as e:
            logger.error(f"Error computing HV_20D for {symbol}: {e}")
            return None

    def fetch_hv_batch(self, symbols: List[str]) -> Dict[str, float]:
        """
        Fetches HV_20D for a list of symbols.
        """
        results = {}
        for symbol in set(symbols):
            hv = self.compute_hv_20d(symbol)
            if hv is not None:
                results[symbol] = hv
        return results

_provider = SchwabHVProvider()

def fetch_hv_20d_batch(symbols: List[str]) -> Dict[str, float]:
    return _provider.fetch_hv_batch(symbols)
