import pandas as pd
import numpy as np
import logging
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from .schwab_iv_provider import SchwabIVProvider
from .iv_history_provider import log_iv_readings, get_latest_iv_batch
import os # Import os to get file path
import inspect # Import inspect to get source code

logger = logging.getLogger(__name__)

logger.debug(f"DEBUG: Loading governed_iv_provider.py from: {os.path.abspath(__file__)}")

class GovernedIVProvider:
    """
    Governed IV & Greeks Provider with smart persistence and hierarchy.
    Hierarchy:
    1. Live Schwab Sensor Readings (Authoritative, includes Greeks)
    2. Historical Fallback: Latest valid IV from DuckDB (IV only, no Greeks)
    3. Missing
    """
    def __init__(self):
        self.schwab_provider = SchwabIVProvider()

    def fetch_enriched_readings(self, symbols: List[str], schwab_live: bool = False) -> List[Dict[str, Any]]:
        """
        Fetch full sensor readings with smart fallbacks and persistence.
        """
        logger.debug(f"DEBUG: Source code of fetch_enriched_readings:\n{inspect.getsource(self.fetch_enriched_readings)}")

        if not symbols:
            return []

        # RAG: Removed redundant OCC option symbol check.
        # SchwabIVProvider._normalize_occ_for_schwab handles conversion.
        # This check was incorrectly preventing valid option symbols from being processed.

        logger.debug(f"[DEBUG_IV] fetch_enriched_readings called with schwab_live={schwab_live} for {len(symbols)} symbols.")

        # 1. Try Live Schwab (Priority Authority)
        # Note: SchwabIVProvider now handles its own internal backoffs/metadata
        readings = []
        if schwab_live:
            try:
                readings = self.schwab_provider.fetch_sensor_readings(symbols)
                logger.debug(f"[DEBUG_IV] Received {len(readings)} readings from Schwab live.")
            except Exception as e:
                logger.error(f"[DEBUG_IV] Schwab live fetch failed: {e}", exc_info=True)
                # Continue to fallback even if live fetch fails
        else:
            logger.info("[GOVERNANCE] Schwab not live — skipping live sensor readings.")
        
        # 2. Persist fresh readings to history
        if readings:
            try:
                log_iv_readings(readings)
                logger.debug(f"[DEBUG_IV] Logged {len(readings)} readings to IV history.")
            except Exception as e:
                logger.warning(f"Failed to log IV history: {e}")

        # 3. Identify missing symbols
        received_symbols = {r['Symbol'] for r in readings}
        missing_symbols = [s for s in symbols if s not in received_symbols]
        logger.debug(f"[DEBUG_IV] Received symbols: {len(received_symbols)}, Missing symbols: {len(missing_symbols)}")

        if not missing_symbols:
            return readings

        # 4. Historical Fallback for missing symbols (IV only)
        logger.info(f"[GOVERNANCE] Attempting historical fallback for {len(missing_symbols)} symbols...")
        logger.debug(f"[DEBUG_IV] Missing symbols for historical fallback: {missing_symbols}")
        historical_ivs = get_latest_iv_batch(missing_symbols)
        logger.debug(f"[DEBUG_IV] Historical IVs found for {len(historical_ivs)} symbols: {historical_ivs.keys()}")
        
        for symbol in missing_symbols:
            if symbol in historical_ivs:
                readings.append({
                    "Symbol": symbol,
                    "IV": historical_ivs[symbol],
                    "Source": "history_fallback",
                    "Sensor_TS": datetime.now(), # Current TS for the snapshot
                    "Is_Fallback": True
                })
            else:
                readings.append({
                    "Symbol": symbol,
                    "IV": np.nan,
                    "Source": "MISSING",
                    "Sensor_TS": datetime.now()
                })
        logger.debug(f"[DEBUG_IV] Final readings count after fallback: {len(readings)}")
        return readings

_governed_iv_provider = GovernedIVProvider()

def fetch_governed_sensor_readings(symbols: List[str], schwab_live: bool = False) -> List[Dict[str, Any]]:
    return _governed_iv_provider.fetch_enriched_readings(symbols, schwab_live=schwab_live)
