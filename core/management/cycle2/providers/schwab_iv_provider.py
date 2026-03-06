import os
import json
import time
import requests
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from core.shared.auth.schwab_tokens import load_tokens

logger = logging.getLogger(__name__)

SCHWAB_API_BASE_URL = "https://api.schwabapi.com"

class SchwabIVProvider:
    """
    Transient IV Provider for Cycle 2 - Passive Consumer Model.
    
    This provider is strictly READ-ONLY regarding authentication.
    It will never attempt to refresh tokens.
    """
    def __init__(self, db_path: str = None):
        self._access_token = None
        self._token_expires_at = 0
        from core.shared.data_contracts.config import PIPELINE_DB_PATH
        self.db_path = db_path or str(PIPELINE_DB_PATH)
        self._init_db()

    def _init_db(self):
        """Initialize the IV metadata table in DuckDB."""
        import duckdb
        try:
            with duckdb.connect(self.db_path) as con:
                con.execute("""
                    CREATE TABLE IF NOT EXISTS iv_metadata (
                        symbol VARCHAR PRIMARY KEY,
                        last_success_ts TIMESTAMP,
                        last_failure_ts TIMESTAMP,
                        failure_reason VARCHAR,
                        backoff_until TIMESTAMP
                    )
                """)
        except Exception as e:
            logger.error(f"Failed to initialize IV metadata DB: {e}")

    def _get_metadata(self, symbol: str):
        import duckdb
        try:
            with duckdb.connect(self.db_path, read_only=True) as con:
                return con.execute("SELECT * FROM iv_metadata WHERE symbol = ?", [symbol.upper()]).fetchone()
        except Exception:
            return None

    def _update_metadata(self, symbol: str, success: bool, reason: str = None, backoff_mins: int = 0):
        import duckdb
        now = datetime.now()
        backoff_until = now + timedelta(minutes=backoff_mins) if backoff_mins > 0 else None
        
        try:
            with duckdb.connect(self.db_path) as con:
                if success:
                    con.execute("""
                        INSERT INTO iv_metadata (symbol, last_success_ts, backoff_until)
                        VALUES (?, ?, NULL)
                        ON CONFLICT (symbol) DO UPDATE SET 
                            last_success_ts = excluded.last_success_ts,
                            backoff_until = NULL
                    """, [symbol.upper(), now])
                else:
                    con.execute("""
                        INSERT INTO iv_metadata (symbol, last_failure_ts, failure_reason, backoff_until)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT (symbol) DO UPDATE SET 
                            last_failure_ts = excluded.last_failure_ts,
                            failure_reason = excluded.failure_reason,
                            backoff_until = COALESCE(excluded.backoff_until, iv_metadata.backoff_until)
                    """, [symbol.upper(), now, reason, backoff_until])
        except Exception as e:
            logger.error(f"Failed to update IV metadata for {symbol}: {e}")

    def get_access_token(self) -> Optional[str]:
        """
        Retrieves a valid access token from disk.
        
        CRITICAL: This method NEVER refreshes tokens. If expired, it returns None.
        """
        tokens, status = load_tokens()
        
        if status != "OK":
            logger.warning(f"[AUTH] Schwab token is {status}. Run `python auth_schwab_minimal.py` manually.")
            return None

        return tokens['access_token']

    def _normalize_occ_for_schwab(self, symbol: str) -> str:
        """
        Converts internal OCC symbol to Schwab's 21-character format.
        Example: AAPL260130C275 -> AAPL  260130C00275000
        """
        import re
        # RAG: Robustness. Handle strikes with decimals (e.g. EOSE...17.5)
        match = re.match(r"^([A-Z]+)(\d{6})([CP])(\d+\.?\d*)$", symbol)
        if not match:
            return symbol
            
        ticker, date, put_call, strike = match.groups()
        
        # Ticker: 6 chars, right-padded
        ticker_part = ticker.ljust(6)
        # Date: 6 chars (YYMMDD)
        # Put/Call: 1 char
        # Strike: 8 chars (5 digits + 3 decimals)
        try:
            strike_val = float(strike)
            strike_part = f"{int(strike_val * 1000):08d}"
        except ValueError:
            return symbol
        
        return f"{ticker_part}{date}{put_call}{strike_part}"

    def fetch_sensor_readings(
        self,
        occ_symbols: List[str],
    ) -> List[Dict]:
        """
        Returns full sensor readings (Price, IV, Greeks) keyed by OCC symbol.
        """
        if not occ_symbols:
            return []

        token = self.get_access_token()
        if not token:
            logger.warning("[FETCH] Schwab fetch skipped: No valid token available.")
            return []

        # Map internal symbols to Schwab format
        schwab_to_internal = {self._normalize_occ_for_schwab(s): s for s in occ_symbols}
        
        # Filter out symbols in backoff
        schwab_symbols = []
        for s in schwab_to_internal.keys():
            meta = self._get_metadata(s)
            if meta and meta[4] and meta[4] > datetime.now(): # backoff_until
                continue
            schwab_symbols.append(s)

        if not schwab_symbols:
            return []

        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json"
        }
        
        readings = []
        for i in range(0, len(schwab_symbols), 100):
            chunk = schwab_symbols[i:i+100]
            params = {
                "symbols": ",".join(chunk),
                "fields": "quote,reference"
            }
            
            try:
                logger.info(f"[FETCH] Requesting full quotes for {len(chunk)} symbols from Schwab...")
                response = requests.get(
                    f"{SCHWAB_API_BASE_URL}/marketdata/v1/quotes",
                    headers=headers,
                    params=params,
                    timeout=15
                )
                
                if response.status_code == 401:
                    logger.error("[FETCH] Schwab API 401 Unauthorized: Token may have been revoked.")
                    return readings # Return partial results

                response.raise_for_status()
                data = response.json()
                
                for s_symbol in chunk:
                    internal_symbol = schwab_to_internal[s_symbol]
                    if s_symbol in data:
                        item = data[s_symbol]
                        quote = item.get('quote', {})
                        
                        iv_raw = quote.get('volatility')
                        # RAG: Authority. Schwab returns IV in percentage points (e.g. 28.10).
                        # Convert to decimal (0.2810) for canonical Cycle-2 standard.
                        iv_decimal = float(iv_raw) / 100.0 if iv_raw is not None else None
                        
                        reading = {
                            "Symbol": internal_symbol,
                            "UL_Last": quote.get('underlyingPrice'),
                            "Opt_Last": quote.get('lastPrice'),
                            "IV": iv_decimal,
                            "Delta": quote.get('delta'),
                            "Gamma": quote.get('gamma'),
                            "Vega": quote.get('vega'),
                            "Theta": quote.get('theta'),
                            "Rho": quote.get('rho'),
                            "Source": "schwab",
                            "Sensor_TS": datetime.now()
                        }
                        readings.append(reading)
                        self._update_metadata(s_symbol, True)
                    else:
                        logger.warning(f"[FETCH] Symbol {s_symbol} not found in Schwab response.")
                        self._update_metadata(s_symbol, False, reason="NOT_FOUND", backoff_mins=60)
            except Exception as e:
                logger.error(f"Error fetching sensor data for chunk: {e}")
                for s_symbol in chunk:
                    self._update_metadata(s_symbol, False, reason=str(e)[:50], backoff_mins=15)

        return readings

_provider = SchwabIVProvider()

def fetch_sensor_readings(occ_symbols: List[str]) -> List[Dict]:
    return _provider.fetch_sensor_readings(occ_symbols)

def fetch_iv_snapshot(occ_symbols: List[str], timestamp: datetime = None) -> Dict[str, float]:
    """
    Compatibility wrapper for Cycle 2 P&L attribution.
    Returns a map of Symbol -> IV (decimal).
    """
    readings = fetch_sensor_readings(occ_symbols)
    return {r['Symbol']: r['IV'] for r in readings if r.get('IV') is not None}
