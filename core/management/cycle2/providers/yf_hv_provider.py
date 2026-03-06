import pandas as pd
import numpy as np
import logging
import yfinance as yf
import time
import random
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

class YahooHVProvider:
    """
    HV Provider using Yahoo Finance as a measurement-only fallback.
    """
    _rate_limit_backoff_until = 0

    def _is_rate_limited(self) -> bool:
        return time.time() < self._rate_limit_backoff_until

    def _trigger_rate_limit_backoff(self, duration: int = 300):
        logger.error(f"[YF] Rate limit detected. Backing off for {duration} seconds.")
        YahooHVProvider._rate_limit_backoff_until = time.time() + duration

    def compute_hv_20d(self, symbol: str) -> Optional[float]:
        """
        Fetches 20+ days of price history from Yahoo Finance and computes annualized HV_20D.
        """
        if self._is_rate_limited():
            logger.warning(f"[YF] Skipping {symbol} due to active rate-limit backoff.")
            return None

        try:
            # RAG: Use a custom session with a random user agent to reduce rate limiting
            import requests
            session = requests.Session()
            session.headers.update({
                'User-Agent': f'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(90, 120)}.0.0.0 Safari/537.36'
            })
            
            # Fetch 3 months of daily data to ensure we have 21+ trading days
            ticker = yf.Ticker(symbol, session=session)
            df = ticker.history(period="3mo")
            
            if df.empty:
                # Check if we were rate limited
                logger.warning(f"[YF] No data returned for {symbol}. Possible rate limit.")
                return None

            if len(df) < 21:
                logger.warning(f"[YF] Insufficient price history for {symbol}: found {len(df)} rows, need 21.")
                return None
            
            # Take the last 21 rows to ensure we have 20 returns
            df = df.tail(21)
            
            # Compute log returns
            df['log_return'] = np.log(df['Close'] / df['Close'].shift(1))
            
            # Drop the first NaN
            log_returns = df['log_return'].dropna()
            
            if len(log_returns) < 20:
                logger.warning(f"[YF] Insufficient log returns for {symbol}: {len(log_returns)}")
                return None
                
            # Compute annualized HV
            hv_20d = log_returns.std() * np.sqrt(252)
            
            return float(hv_20d)
            
        except Exception as e:
            if "Rate limited" in str(e) or "429" in str(e):
                self._trigger_rate_limit_backoff()
            else:
                logger.error(f"[YF] Error computing HV_20D for {symbol}: {e}")
            return None

    def fetch_hv_batch(self, symbols: List[str]) -> Dict[str, float]:
        """
        Fetches HV_20D for a list of symbols with aggressive rate-limit protection.
        """
        if self._is_rate_limited():
            logger.warning("[YF] Skipping batch fetch due to active rate-limit backoff.")
            return {}

        results = {}
        unique_symbols = list(set(symbols))
        logger.info(f"[YF] Fetching HV for {len(unique_symbols)} symbols...")
        
        # RAG: Use a custom session with a random user agent
        import requests
        session = requests.Session()
        session.headers.update({
            'User-Agent': f'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(90, 120)}.0.0.0 Safari/537.36'
        })

        # RAG: Use yfinance batch download for efficiency and lower rate-limit risk
        try:
            # Download 3mo history for all symbols at once
            data = yf.download(unique_symbols, period="3mo", group_by='ticker', progress=False, session=session)
            
            if data.empty:
                raise ValueError("YF returned empty batch dataframe")

            for symbol in unique_symbols:
                try:
                    df = data[symbol] if len(unique_symbols) > 1 else data
                    
                    if len(df) < 21:
                        continue
                        
                    # Take last 21 rows
                    df = df.tail(21)
                    log_returns = np.log(df['Close'] / df['Close'].shift(1)).dropna()
                    
                    if len(log_returns) >= 20:
                        hv = log_returns.std() * np.sqrt(252)
                        results[symbol] = float(hv)
                        logger.info(f"[YF] Computed HV for {symbol}: {hv:.4f}")
                except Exception as e:
                    logger.warning(f"[YF] Failed to process batch data for {symbol}: {e}")
                    
        except Exception as e:
            if "Rate limited" in str(e) or "429" in str(e):
                self._trigger_rate_limit_backoff()
                return {}

            logger.error(f"[YF] Batch download failed: {e}. Falling back to sequential with aggressive delays.")
            # Sequential fallback (existing logic)
            for i, symbol in enumerate(unique_symbols):
                # Aggressive delay to try and break the rate limit
                if i > 0: 
                    delay = random.uniform(5.0, 10.0)
                    logger.info(f"[YF] Sleeping {delay:.1f}s before {symbol}...")
                    time.sleep(delay)
                
                hv = self.compute_hv_20d(symbol)
                if hv is not None:
                    results[symbol] = hv
                else:
                    # If we hit a rate limit sequentially, stop trying to avoid further blocking
                    logger.error(f"[YF] Sequential fetch failed for {symbol}. Aborting batch to prevent IP ban.")
                    break
                
        return results

_provider = YahooHVProvider()

def fetch_hv_20d_batch_yf(symbols: List[str]) -> Dict[str, float]:
    return _provider.fetch_hv_batch(symbols)
