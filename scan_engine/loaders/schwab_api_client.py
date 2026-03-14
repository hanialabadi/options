import os
import json
import subprocess
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


def _send_token_expiry_alert(hours_left: float) -> None:
    """Send a macOS notification when Schwab refresh token is about to expire."""
    try:
        subprocess.run([
            "osascript", "-e",
            f'display notification "Schwab refresh token expires in {hours_left:.0f}h. '
            f'Run: python auth_schwab_minimal.py" '
            f'with title "Options Pipeline" subtitle "⚠️ Auth Expiring"'
        ], timeout=5, capture_output=True)
    except Exception:
        pass  # notification is best-effort

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
        Proactively refreshes when refresh token is within 48h of expiring
        to prevent the 7-day OAuth window from lapsing.

        CACHED: Only validates once per SchwabClient instance to prevent busy loop.
        """
        # Skip if already validated this session (prevents busy loop in fetch_contracts)
        if self._token_validated:
            return

        # Reload tokens from disk to ensure we have the latest state
        self._tokens, self._auth_status = load_tokens()

        logger.debug(f"[DEBUG_SCHWAB_AUTH] Initial load: status={self._auth_status}, expires_at={self._tokens.get('expires_at')}")

        if self._auth_status in ("EXPIRED", "REFRESH_EXPIRED"):
            _reason = "Access token expired" if self._auth_status == "EXPIRED" else "Refresh token reportedly expired"
            logger.info(f"[DEBUG_SCHWAB_AUTH] {_reason}. Attempting silent refresh...")
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

        # Proactive refresh: if refresh token expires within 48h, refresh now
        # to rotate the refresh token and get a fresh 7-day window.
        # This prevents the weekend/holiday gap from killing the session.
        import time as _time
        _refresh_expires = (self._tokens or {}).get("refresh_expires_at", 0)
        _hours_left = max(0, (_refresh_expires - int(_time.time())) / 3600)
        if 0 < _hours_left < 48:
            logger.warning(
                f"⚠️ SCHWAB_AUTH: Refresh token expires in {_hours_left:.0f}h — "
                f"proactively refreshing to extend 7-day window."
            )
            new_tokens = refresh_schwab_tokens()
            if new_tokens:
                self._tokens = new_tokens
                logger.info("✅ SCHWAB_AUTH: Proactive refresh succeeded — new 7-day window.")
            else:
                logger.error(
                    "🔴 SCHWAB_AUTH: Proactive refresh FAILED — refresh token expires "
                    f"in {_hours_left:.0f}h. Run `python auth_schwab_minimal.py` ASAP."
                )
                _send_token_expiry_alert(_hours_left)

        # Mark as validated - subsequent calls will skip validation
        self._token_validated = True
        logger.debug("✅ SCHWAB_CLIENT: Token pre-flight check passed")

    def invalidate_token_cache(self) -> None:
        """
        Reset the _token_validated flag so the next ensure_valid_token()
        re-checks token freshness and refreshes if needed.

        Call this when a 401 is received mid-run (access token expired
        during a long-running operation like IV surface collection).
        """
        self._token_validated = False

    def _get_access_token(self) -> str:
        """
        Retrieves the current access token.

        Proactively refreshes if the access token is within 120s of
        expiry — prevents 401s during long-running chain fetch loops.
        """
        # Proactive expiry check: if token expires within 120s, force re-validation
        if self._token_validated and self._tokens:
            _expires_at = self._tokens.get("expires_at", 0)
            if _expires_at > 0 and (int(time.time()) > (_expires_at - 120)):
                logger.info(
                    "🔄 SCHWAB_CLIENT: Access token expiring within 120s "
                    "(expires_at=%d, now=%d) — forcing refresh.",
                    _expires_at, int(time.time()),
                )
                self._token_validated = False

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
