import os
import json
import time
import requests
from loguru import logger
from dotenv import load_dotenv
from typing import Optional # Import Optional
from core.shared.auth.schwab_tokens import load_tokens, refresh_schwab_tokens

# Load environment variables
load_dotenv()

# --- Configuration Constants ---
SCHWAB_API_BASE_URL = "https://api.schwabapi.com"

class SchwabClient:
    """
    Schwab API Client - Active Consumer Model.
    
    This client handles automatic token refresh using the refresh token.
    Manual authentication is only required if the refresh token is missing or expired.
    """
    def __init__(self, client_id: str = None, client_secret: str = None):
        # Enforce strict env var contract
        if client_id is None:
            client_id = os.getenv("SCHWAB_APP_KEY") or os.getenv("SCHWAB_API_KEY")
        if client_secret is None:
            client_secret = os.getenv("SCHWAB_APP_SECRET")

        if not client_id:
            raise RuntimeError("SCHWAB_APP_KEY not set in environment")

        self.client_id = client_id
        self.client_secret = client_secret
        self._tokens, self._auth_status = load_tokens()
        self._token_validated = False  # Track if token already validated this session

    def ensure_valid_token(self) -> None:
        """
        Pre-flight validation: Ensures token exists and is not expired.
        Attempts silent refresh if the access token is expired.

        CACHED: Only validates once per SchwabClient instance to prevent busy loop.
        """
        # Skip if already validated this session (prevents busy loop in fetch_contracts)
        if self._token_validated:
            return

        # Reload tokens from disk to ensure we have the latest state
        self._tokens, self._auth_status = load_tokens()

        logger.debug(f"[DEBUG_SCHWAB_AUTH] Initial load: status={self._auth_status}, expires_at={self._tokens.get('expires_at')}")

        if self._auth_status == "EXPIRED":
            logger.info("[DEBUG_SCHWAB_AUTH] Access token expired. Attempting silent refresh...")
            new_tokens = refresh_schwab_tokens()
            if new_tokens:
                self._tokens = new_tokens
                self._auth_status = "OK"
                logger.info("✅ SCHWAB_CLIENT: Access token auto-refreshed successfully.")
                logger.debug(f"[DEBUG_SCHWAB_AUTH] Refresh successful: new expires_at={self._tokens.get('expires_at')}")
            else:
                self._auth_status = "REFRESH_FAILED"
                logger.error("[DEBUG_SCHWAB_AUTH] Token refresh failed.")

        if self._auth_status != "OK":
            error_msg = (
                f"❌ AUTH FAILURE: Schwab token is {self._auth_status}.\n"
                "   Fix: Run `python auth_schwab_minimal.py` manually."
            )
            logger.error(f"[DEBUG_SCHWAB_AUTH] Final auth status not OK: {self._auth_status}")
            raise RuntimeError(error_msg)

        # Mark as validated - subsequent calls will skip validation
        self._token_validated = True
        logger.debug("✅ SCHWAB_CLIENT: Token pre-flight check passed")

    def _get_access_token(self) -> str:
        """
        Retrieves the current access token. Raises error if invalid.
        """
        self.ensure_valid_token()
        return self._tokens['access_token']

    def get_quotes(self, symbols: list[str], fields: str = "quote") -> dict:
        """
        Calls the /marketdata/v1/quotes endpoint.
        """
        access_token = self._get_access_token()

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        }
        params = {
            "symbols": ",".join(symbols),
            "fields": fields
        }

        response = requests.get(
            f"{SCHWAB_API_BASE_URL}/marketdata/v1/quotes",
            headers=headers,
            params=params,
            timeout=15
        )

        if response.status_code == 401:
            raise RuntimeError("❌ Schwab API 401 Unauthorized: Token may have been revoked. Run `python auth_schwab_minimal.py`.")

        response.raise_for_status()
        return response.json() # Return the JSON content

    def get_chains(self, symbol: str, strikeCount: int, range: str, strategy: str) -> dict:
        """
        Calls the /marketdata/v1/chains endpoint.
        """
        access_token = self._get_access_token()

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        }
        params = {
            "symbol": symbol,
            "strikeCount": strikeCount,
            "range": range,
            "strategy": strategy,
            "includeQuotes": "TRUE"
        }

        response = requests.get(
            f"{SCHWAB_API_BASE_URL}/marketdata/v1/chains",
            headers=headers,
            params=params,
            timeout=15
        )

        if response.status_code == 401:
            raise RuntimeError("❌ Schwab API 401 Unauthorized: Token may have been revoked. Run `python auth_schwab_minimal.py`.")

        response.raise_for_status()
        return response.json() # Return the JSON content

    def get_price_history(self, symbol: str, periodType: Optional[str] = None, period: Optional[int] = None, 
                          frequencyType: Optional[str] = None, frequency: Optional[int] = None,
                          startDate: Optional[int] = None, endDate: Optional[int] = None) -> dict:
        """
        Calls the /marketdata/v1/pricehistory endpoint.
        
        Can use either periodType/period/frequencyType/frequency OR startDate/endDate.
        startDate/endDate are preferred for deterministic history fetching.
        """
        access_token = self._get_access_token()

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        }
        params = {
            "symbol": symbol,
        }
        
        if startDate is not None and endDate is not None:
            params["startDate"] = startDate
            params["endDate"] = endDate
            params["needExtendedHoursData"] = False # Default to False for clean history
            params["needPreviousClose"] = False # Default to False
        elif periodType and period and frequencyType and frequency:
            params["periodType"] = periodType
            params["period"] = period
            params["frequencyType"] = frequencyType
            params["frequency"] = frequency
        else:
            raise ValueError("Must provide either (periodType, period, frequencyType, frequency) OR (startDate, endDate)")

        response = requests.get(
            f"{SCHWAB_API_BASE_URL}/marketdata/v1/pricehistory",
            headers=headers,
            params=params,
            timeout=15
        )

        if response.status_code == 401:
            raise RuntimeError("❌ Schwab API 401 Unauthorized: Token may have been revoked. Run `python auth_schwab_minimal.py`.")

        response.raise_for_status()
        return response.json()

    def get_streamer_info(self) -> dict:
        """
        Fetch streamer connection details from Schwab Trader API.

        Returns a dict with:
            streamerSocketUrl  str  — wss:// WebSocket endpoint
            schwabClientCustomerId  str  — required for LOGIN request
            schwabClientCorrelId    str  — required for LOGIN request
            schwabClientChannel     str  — 'IO'
            schwabClientFunctionId  str  — 'APIAPP'

        Raises RuntimeError on auth failure or missing fields.

        Endpoint: GET /trader/v1/userPreference
        """
        access_token = self._get_access_token()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        }
        response = requests.get(
            f"https://api.schwabapi.com/trader/v1/userPreference",
            headers=headers,
            timeout=15,
        )
        if response.status_code == 401:
            raise RuntimeError(
                "❌ Schwab Trader API 401: Token may have been revoked. "
                "Run `python auth_schwab_minimal.py`."
            )
        response.raise_for_status()
        data = response.json()

        # Schwab wraps streamer info inside streamerInfo[0]
        streamer_list = data.get("streamerInfo", [])
        if not streamer_list:
            raise RuntimeError(
                f"No streamerInfo in userPreference response. "
                f"Keys present: {list(data.keys())}"
            )
        info = streamer_list[0]

        required = ["streamerSocketUrl", "schwabClientCustomerId",
                    "schwabClientCorrelId", "schwabClientChannel",
                    "schwabClientFunctionId"]
        missing = [k for k in required if not info.get(k)]
        if missing:
            raise RuntimeError(
                f"streamerInfo missing required fields: {missing}. Got: {info}"
            )

        return {k: info[k] for k in required}
