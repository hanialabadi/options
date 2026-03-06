import pandas as pd
import numpy as np
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from .schwab_hv_provider import SchwabHVProvider
from .yf_hv_provider import YahooHVProvider
from core.shared.data_contracts.config import PIPELINE_DB_PATH

logger = logging.getLogger(__name__)

class GovernedHVProvider:
    """
    Governed HV Provider with persistence and authority-aware fallback.
    Hierarchy:
    1. Cached Schwab HV (age <= 5 days)
    2. Live Schwab Computation
    3. Yahoo Finance Fallback (tagged as non-authoritative)
    4. Emergency Fallback: Stale Cache (any age)
    """
    def __init__(self, db_path: str = None):
        self.db_path = db_path or str(PIPELINE_DB_PATH)
        self.schwab_provider = SchwabHVProvider()
        self.yf_provider = YahooHVProvider()
        self._init_db()

    def _init_db(self):
        """Initialize the HV cache table in DuckDB."""
        import duckdb
        try:
            with duckdb.connect(self.db_path) as con:
                con.execute("""
                    CREATE TABLE IF NOT EXISTS hv_cache (
                        symbol VARCHAR PRIMARY KEY,
                        hv_20d DOUBLE,
                        source VARCHAR,
                        computed_ts TIMESTAMP,
                        last_failure_ts TIMESTAMP,
                        failure_reason VARCHAR,
                        backoff_until TIMESTAMP
                    )
                """)
                
                # Migration: Add new columns if they don't exist
                cols = con.execute("PRAGMA table_info('hv_cache')").fetchall()
                col_names = [c[1] for c in cols]
                if "last_failure_ts" not in col_names:
                    con.execute("ALTER TABLE hv_cache ADD COLUMN last_failure_ts TIMESTAMP")
                if "failure_reason" not in col_names:
                    con.execute("ALTER TABLE hv_cache ADD COLUMN failure_reason VARCHAR")
                if "backoff_until" not in col_names:
                    con.execute("ALTER TABLE hv_cache ADD COLUMN backoff_until TIMESTAMP")
        except Exception as e:
            logger.error(f"Failed to initialize HV cache DB: {e}")

    def get_cached_hv(self, symbol: str, max_age_days: Optional[int] = 1) -> Optional[Tuple[float, str, datetime]]:
        """
        Retrieve HV from cache. 
        """
        import duckdb
        try:
            with duckdb.connect(self.db_path, read_only=True) as con:
                res = con.execute("""
                    SELECT hv_20d, source, computed_ts, backoff_until 
                    FROM hv_cache 
                    WHERE symbol = ?
                """, [symbol.upper()]).fetchone()
                
                if res:
                    hv, source, ts, backoff = res
                    
                    # Check smart backoff
                    if backoff and backoff > datetime.now():
                        return None # Still in backoff
                    
                    if max_age_days is None:
                        return hv, source, ts
                    
                    if ts:
                        age = datetime.now() - ts
                        if age.days <= max_age_days:
                            return hv, "CACHED", ts
        except Exception as e:
            logger.error(f"Error reading HV cache for {symbol}: {e}")
        return None

    def persist_hv(self, symbol: str, hv: float, source: str, success: bool = True, reason: str = None, backoff_mins: int = 0):
        """Persist computed HV or failure to cache."""
        import duckdb
        now = datetime.now()
        backoff_until = now + timedelta(minutes=backoff_mins) if backoff_mins > 0 else None
        
        try:
            with duckdb.connect(self.db_path) as con:
                if success:
                    con.execute("""
                        INSERT INTO hv_cache (symbol, hv_20d, source, computed_ts, backoff_until)
                        VALUES (?, ?, ?, ?, NULL)
                        ON CONFLICT (symbol) DO UPDATE SET 
                            hv_20d = excluded.hv_20d,
                            source = excluded.source,
                            computed_ts = excluded.computed_ts,
                            backoff_until = NULL
                    """, [symbol.upper(), hv, source, now])
                else:
                    con.execute("""
                        INSERT INTO hv_cache (symbol, last_failure_ts, failure_reason, backoff_until)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT (symbol) DO UPDATE SET 
                            last_failure_ts = excluded.last_failure_ts,
                            failure_reason = excluded.failure_reason,
                            backoff_until = COALESCE(excluded.backoff_until, hv_cache.backoff_until)
                    """, [symbol.upper(), now, reason, backoff_until])
        except Exception as e:
            logger.error(f"Failed to persist HV for {symbol}: {e}")

    def get_hv_20d(self, symbol: str, schwab_live: bool = False) -> Dict[str, any]:
        """
        Retrieves HV_20D using the strict authority hierarchy.
        Returns a dict with: HV_20D, HV_20D_Source, HV_20D_Computed_TS, HV_20D_Age_Days
        """
        # 1. Try Cache (Fresh data age <= 5 days)
        cached = self.get_cached_hv(symbol)
        if cached:
            hv, source, ts = cached
            return self._format_result(hv, source, ts)

        # 2. Try Live Schwab (Priority Authority)
        if schwab_live:
            hv_schwab = None
            try:
                hv_schwab = self.schwab_provider.compute_hv_20d(symbol)
                if hv_schwab is not None:
                    self.persist_hv(symbol, hv_schwab, "SCHWAB")
                    return self._format_result(hv_schwab, "SCHWAB", datetime.now())
            except Exception as e:
                logger.warning(f"Schwab HV failed for {symbol}: {e}")
                self.persist_hv(symbol, 0, "SCHWAB", success=False, reason=str(e)[:50], backoff_mins=30)
        else:
            logger.info(f"[GOVERNANCE] Schwab not live — skipping live HV for {symbol}")

        # 3. Try Yahoo Finance Fallback
        try:
            hv_yf = self.yf_provider.compute_hv_20d(symbol)
            if hv_yf is not None:
                self.persist_hv(symbol, hv_yf, "YF")
                return self._format_result(hv_yf, "YF", datetime.now())
        except Exception as e:
            logger.error(f"Yahoo HV failed for {symbol}: {e}")
            self.persist_hv(symbol, 0, "YF", success=False, reason="RATE_LIMIT" if "429" in str(e) else str(e)[:50], backoff_mins=60)

        # 4. Emergency Fallback: Stale Cache
        stale = self.get_cached_hv(symbol, max_age_days=None)
        if stale:
            hv, source, ts = stale
            logger.warning(f"[GOVERNANCE] Using STALE HV for {symbol} from {ts} (Source: {source})")
            return self._format_result(hv, f"{source}_STALE", ts)

        # 5. Missing
        return self._format_result(np.nan, "MISSING", None)

    def _format_result(self, hv: float, source: str, ts: Optional[datetime]) -> Dict[str, any]:
        age_days = (datetime.now() - ts).days if ts else None
        return {
            "HV_20D": hv,
            "HV_20D_Source": source,
            "HV_20D_Computed_TS": ts,
            "HV_20D_Age_Days": age_days
        }

_governed_provider = GovernedHVProvider()

def fetch_governed_hv_20d(symbol: str, schwab_live: bool = False) -> Dict[str, any]:
    return _governed_provider.get_hv_20d(symbol, schwab_live=schwab_live)

def fetch_governed_hv_batch(symbols: List[str], schwab_live: bool = False) -> pd.DataFrame:
    """
    Batch fetch HV for multiple symbols using the governed hierarchy.
    RAG: Efficiency. Uses batch sub-providers where possible.
    """
    unique_symbols = list(set(symbols))
    results = []
    
    # 1. Check Cache first for all
    remaining_symbols = []
    for symbol in unique_symbols:
        cached = _governed_provider.get_cached_hv(symbol)
        if cached:
            hv, source, ts = cached
            res = _governed_provider._format_result(hv, source, ts)
            res['Underlying_Ticker'] = symbol
            results.append(res)
        else:
            remaining_symbols.append(symbol)
            
    if not remaining_symbols:
        return pd.DataFrame(results)
        
    # 2. Try Live Schwab
    still_remaining = []
    if schwab_live:
        for symbol in remaining_symbols:
            # Check backoff before trying live
            meta = _governed_provider.get_cached_hv(symbol, max_age_days=None)
            # Note: get_cached_hv returns None if in backoff
            
            try:
                hv = _governed_provider.schwab_provider.compute_hv_20d(symbol)
                if hv is not None:
                    _governed_provider.persist_hv(symbol, hv, "SCHWAB")
                    res = _governed_provider._format_result(hv, "SCHWAB", datetime.now())
                    res['Underlying_Ticker'] = symbol
                    results.append(res)
                else:
                    _governed_provider.persist_hv(symbol, 0, "SCHWAB", success=False, reason="EMPTY_RESPONSE", backoff_mins=30)
                    still_remaining.append(symbol)
            except Exception as e:
                _governed_provider.persist_hv(symbol, 0, "SCHWAB", success=False, reason=str(e)[:50], backoff_mins=30)
                still_remaining.append(symbol)
    else:
        logger.info(f"[GOVERNANCE] Schwab not live — skipping live batch HV for {len(remaining_symbols)} symbols")
        still_remaining = remaining_symbols
            
    if not still_remaining:
        return pd.DataFrame(results)
        
    # 3. Try Yahoo Finance Batch (The real fix for rate limits)
    try:
        yf_results = _governed_provider.yf_provider.fetch_hv_batch(still_remaining)
        for symbol in still_remaining:
            hv = yf_results.get(symbol)
            if hv is not None:
                _governed_provider.persist_hv(symbol, hv, "YF")
                res = _governed_provider._format_result(hv, "YF", datetime.now())
            else:
                # RAG: Resilience. Fallback to stale cache if batch failed for this symbol
                stale = _governed_provider.get_cached_hv(symbol, max_age_days=None)
                if stale:
                    h, s, t = stale
                    res = _governed_provider._format_result(h, f"{s}_STALE", t)
                else:
                    res = _governed_provider._format_result(np.nan, "MISSING", None)
            res['Underlying_Ticker'] = symbol
            results.append(res)
    except Exception as e:
        logger.error(f"Governed batch YF failed: {e}")
        for symbol in still_remaining:
            # RAG: Resilience. Fallback to stale cache if entire batch failed
            stale = _governed_provider.get_cached_hv(symbol, max_age_days=None)
            if stale:
                h, s, t = stale
                res = _governed_provider._format_result(h, f"{s}_STALE", t)
            else:
                res = _governed_provider._format_result(np.nan, "ERROR", None)
            res['Underlying_Ticker'] = symbol
            results.append(res)
            
    return pd.DataFrame(results)
