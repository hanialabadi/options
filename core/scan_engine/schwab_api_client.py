import os
import json
import time
import requests
## No browser import, no OAuth logic
import base64
import tempfile
from urllib.parse import urlparse, parse_qs, urlencode
from loguru import logger
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# --- Configuration Constants (Use placeholders) ---
SCHWAB_API_BASE_URL = "https://api.schwabapi.com"
SCHWAB_TOKEN_URL = f"{SCHWAB_API_BASE_URL}/v1/oauth/token"
DEFAULT_CALLBACK_URL = "http://localhost:8080"

# --- Token Persistence ---
# CENTRALIZED TOKEN PATH - Must match tools/reauth_schwab.py
TOKEN_FILE = os.path.expanduser("~/.schwab/tokens.json")

def save_tokens(tokens: dict, token_file_path: str = TOKEN_FILE):
    """Saves tokens to a JSON file atomically to avoid partial/corrupt writes."""
    token_file_path = os.path.expanduser(token_file_path)
    token_dir = os.path.dirname(token_file_path) or "."
    os.makedirs(token_dir, exist_ok=True)

    fd, temp_path = tempfile.mkstemp(dir=token_dir, prefix=".schwab_tokens_", suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(tokens, f, indent=4)
        os.replace(temp_path, token_file_path)  # atomic on POSIX
        logger.info(f"Tokens saved to {token_file_path}")
    except Exception as e:
        logger.error(f"Failed to save tokens atomically: {e}")
        try:
            if os.path.exists(temp_path):
                os.remove(temp_path)
        except Exception:
            pass
        raise

def load_tokens(token_file_path: str = TOKEN_FILE) -> dict | None:
    """Loads tokens from a JSON file."""
    token_file_path = os.path.expanduser(token_file_path)
    if os.path.exists(token_file_path):
        with open(token_file_path, "r") as f:
            tokens = json.load(f)
        logger.info(f"Tokens loaded from {token_file_path}")
        return tokens
    logger.info(f"No existing token file found at {token_file_path}")
    return None

## No OAuth flow, no code exchange, no browser logic

def _refresh_token_flow(refresh_token: str, client_id: str, client_secret: str, redirect_uri: str = DEFAULT_CALLBACK_URL) -> dict:
    """
    Refreshes an expired access token using the refresh token.
    """
    credentials = f"{client_id}:{client_secret}"
    base64_credentials = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")
    headers = {
        "Authorization": f"Basic {base64_credentials}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "redirect_uri": redirect_uri
    }
    logger.info(f"Attempting to refresh access token (URI: {redirect_uri})...")
    response = requests.post(SCHWAB_TOKEN_URL, headers=headers, data=data, timeout=15)
    
    if not response.ok:
        logger.error(f"Refresh failed: {response.status_code} - {response.text}")
    
    response.raise_for_status()
    tokens = response.json()
    tokens['expires_at'] = time.time() + tokens['expires_in'] - 60  # 1 minute buffer for access token
    # Refresh token is typically not renewed during this flow, so its expiry remains the same
    # or is implicitly handled by the initial 'refresh_token_expires_at'
    logger.info("Successfully refreshed access token.")
    return tokens

class SchwabClient:
    def __init__(self, client_id: str = None, client_secret: str = None, token_file_path: str = TOKEN_FILE, callback_url: str = None):
        # Enforce strict env var contract
        if client_id is None:
            client_id = os.getenv("SCHWAB_APP_KEY")
        if client_secret is None:
            client_secret = os.getenv("SCHWAB_APP_SECRET")
        if callback_url is None:
            callback_url = os.getenv("SCHWAB_CALLBACK_URL", DEFAULT_CALLBACK_URL)
        assert client_id, "SCHWAB_APP_KEY not set"
        assert client_secret, "SCHWAB_APP_SECRET not set"
        self.client_id = client_id
        self.client_secret = client_secret
        self.callback_url = callback_url
        self.token_file_path = token_file_path
        self._tokens = load_tokens(self.token_file_path)
        self._lock = False # Simple lock for token refresh to prevent race conditions

    def ensure_valid_token(self) -> None:
        """
        Pre-flight validation: Ensures token is valid before any fetch begins.
        
        CRITICAL: This method NEVER opens a browser. If tokens are missing or
        refresh token expired, it raises an exception with instructions.
        
        Raises:
            Exception: If token is expired or refresh fails
        """
        if not self._tokens:
            error_msg = (
                "❌ No tokens found\n"
                f"   Token file: {self.token_file_path}\n"
                "\n"
                "🔧 To fix:\n"
                "   python tools/reauth_schwab.py\n"
            )
            logger.error(error_msg)
            raise Exception(error_msg)
        
        # Check refresh token expiry FIRST
        refresh_expires_at = self._tokens.get('refresh_expires_at', 0)
        if time.time() >= refresh_expires_at:
            error_msg = (
                "❌ Refresh token expired\n"
                f"   Token file: {self.token_file_path}\n"
                f"   Expiry: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(refresh_expires_at))}\n"
                "\n"
                "🔧 To fix:\n"
                "   python tools/reauth_schwab.py\n"
            )
            logger.error(error_msg)
            raise Exception(error_msg)
        
        # Check access token expiry (can auto-refresh)
        if time.time() >= self._tokens.get('access_expires_at', 0):
            logger.info("Pre-flight check: Access token expired, refreshing...")
            try:
                new_tokens = self._refresh_token()
                self._tokens.update(new_tokens)
                save_tokens(self._tokens, self.token_file_path)
                logger.info("✅ Pre-flight check passed: Token refreshed")
            except Exception as e:
                logger.error(f"Pre-flight check failed: {e}")
                error_msg = (
                    "❌ Failed to refresh access token\n"
                    f"   Error: {e}\n"
                    "\n"
                    "🔧 To fix:\n"
                    "   python tools/reauth_schwab.py\n"
                )
                logger.error(error_msg)
                raise Exception(error_msg)
        else:
            access_remaining = self._tokens.get('access_expires_at', 0) - time.time()
            refresh_remaining = refresh_expires_at - time.time()
            logger.info(
                f"✅ Pre-flight check passed: Token valid "
                f"(access: {access_remaining/3600:.1f}h, refresh: {refresh_remaining/86400:.1f}d remaining)"
            )

    def _refresh_token(self):
        refresh_token = self._tokens.get('refresh_token')
        if not refresh_token:
            raise Exception("No refresh_token found. Run: python tools/reauth_schwab.py")
        
        # Use the consolidated flow
        new_tokens = _refresh_token_flow(
            refresh_token,
            self.client_id,
            self.client_secret,
            self.callback_url
        )
        
        return {
            "access_token": new_tokens["access_token"],
            "access_expires_at": new_tokens["expires_at"],
        }
    
    def _get_access_token(self) -> str:
        """
        Retrieves a valid access token, refreshing it if necessary.
        
        CRITICAL: This method NEVER opens a browser. If tokens are invalid,
        it raises an exception with instructions to run tools/reauth_schwab.py.
        """
        if self._lock:
            # Wait for another thread/process to finish refreshing
            while self._lock:
                time.sleep(0.1)
            self._tokens = load_tokens(self.token_file_path)  # Reload tokens after refresh
            if self._tokens and self._tokens.get('access_token'):
                return self._tokens['access_token']
            raise Exception("Failed to acquire access token after waiting for refresh.")

        if not self._tokens:
            error_msg = (
                "❌ No tokens found\n"
                f"   Token file: {self.token_file_path}\n"
                "\n"
                "🔧 To fix:\n"
                "   python tools/reauth_schwab.py\n"
            )
            logger.error(error_msg)
            raise Exception(error_msg)

        # Check refresh token expiry
        refresh_expires_at = self._tokens.get('refresh_expires_at', 0)
        if time.time() >= refresh_expires_at:
            error_msg = (
                "❌ Refresh token expired\n"
                f"   Expiry: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(refresh_expires_at))}\n"
                "\n"
                "🔧 To fix:\n"
                "   python tools/reauth_schwab.py\n"
            )
            logger.error(error_msg)
            raise Exception(error_msg)

        # Refresh access token if expired
        if time.time() >= self._tokens.get('expires_at', 0):
            logger.info("Access token expired. Attempting to refresh.")
            self._lock = True
            try:
                new_tokens = _refresh_token_flow(
                    self._tokens['refresh_token'],
                    self.client_id,
                    self.client_secret,
                    self.callback_url
                )
                # Update only access token related fields, keep original refresh token expiry
                self._tokens.update({
                    'access_token': new_tokens['access_token'],
                    'expires_in': new_tokens['expires_in'],
                    'expires_at': new_tokens['expires_at']
                })
                save_tokens(self._tokens, self.token_file_path)
            except Exception as e:
                logger.error(f"Failed to refresh access token: {e}")
                error_msg = (
                    "❌ Failed to refresh access token\n"
                    f"   Error: {e}\n"
                    "\n"
                    "🔧 To fix:\n"
                    "   python tools/reauth_schwab.py\n"
                )
                logger.error(error_msg)
                raise Exception(error_msg)
            finally:
                self._lock = False
        
        if not self._tokens or not self._tokens.get('access_token'):
            raise Exception("Failed to obtain a valid access token.")
        
        return self._tokens['access_token']

    def get_quotes(self, symbols: list[str], fields: str = "quote") -> dict:
        """
        Calls the /marketdata/v1/quotes endpoint.
        Includes automatic token refresh but NEVER opens browser.
        
        If tokens are invalid, raises exception with instructions to run:
            python tools/reauth_schwab.py
        """
        access_token = self._get_access_token()  # Will raise if tokens invalid

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        }
        params = {
            "symbols": ",".join(symbols),
            "fields": fields
        }

        logger.info(f"Fetching quotes for {symbols}...")
        response = requests.get(
            f"{SCHWAB_API_BASE_URL}/marketdata/v1/quotes",
            headers=headers,
            params=params,
            timeout=15
        )

        if response.status_code == 401:
            logger.warning("Received 401 Unauthorized. Attempting token refresh and retry.")
            try:
                # Force refresh token and retry
                self._lock = True # Prevent other threads from trying to refresh
                new_tokens = _refresh_token_flow(
                    self._tokens['refresh_token'],
                    self.client_id,
                    self.client_secret,
                    self.callback_url
                )
                # Only update access-token fields; keep refresh token + refresh expiry
                self._tokens.update({
                    'access_token': new_tokens['access_token'],
                    'expires_in': new_tokens['expires_in'],
                    'expires_at': new_tokens['expires_at']
                })
                save_tokens(self._tokens, self.token_file_path)
                self._lock = False

                access_token = self._tokens['access_token']
                headers["Authorization"] = f"Bearer {access_token}"
                response = requests.get(
                    f"{SCHWAB_API_BASE_URL}/marketdata/v1/quotes",
                    headers=headers,
                    params=params,
                    timeout=15
                )
            except Exception as e:
                logger.error(f"Failed to refresh token and retry after 401: {e}")
                error_msg = (
                    "❌ Token refresh failed after 401 error\n"
                    "\n"
                    "🔧 To fix:\n"
                    "   python tools/reauth_schwab.py\n"
                )
                logger.error(error_msg)
                raise Exception(error_msg)

        response.raise_for_status() # Raise for any other HTTP errors
        logger.info(f"Successfully fetched quotes for {symbols}.")
        return response.json()

    def get_chains(self, symbol: str, strikeCount: int, range: str, strategy: str) -> dict:
        """
        Calls the /marketdata/v1/chains endpoint to get option chain data.
        NEVER opens browser - raises exception if tokens invalid.
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
            "includeQuotes": "TRUE" # Ensure IV is included
        }

        logger.info(f"Fetching option chains for {symbol}...")
        try:
            response = requests.get(
                f"{SCHWAB_API_BASE_URL}/marketdata/v1/chains",
                headers=headers,
                params=params,
                timeout=15
            )
        except requests.exceptions.Timeout:
            logger.warning(f"⏱️ Timeout (15s) fetching chains for {symbol}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error fetching chains for {symbol}: {e}")
            return None

        if response.status_code == 401:
            logger.warning("Received 401 Unauthorized for chains. Attempting token refresh and retry.")
            try:
                self._lock = True
                new_tokens = _refresh_token_flow(
                    self._tokens['refresh_token'],
                    self.client_id,
                    self.client_secret,
                    self.callback_url
                )
                self._tokens.update({
                    'access_token': new_tokens['access_token'],
                    'expires_in': new_tokens['expires_in'],
                    'expires_at': new_tokens['expires_at']
                })
                save_tokens(self._tokens, self.token_file_path)
                self._lock = False

                access_token = self._tokens['access_token']
                headers["Authorization"] = f"Bearer {access_token}"
                try:
                    response = requests.get(
                        f"{SCHWAB_API_BASE_URL}/marketdata/v1/chains",
                        headers=headers,
                        params=params,
                        timeout=15
                    )
                except requests.exceptions.Timeout:
                    logger.warning(f"⏱️ Timeout (15s) on retry fetching chains for {symbol}")
                    return None
                except requests.exceptions.RequestException as e:
                    logger.error(f"Network error on retry fetching chains for {symbol}: {e}")
                    return None
            except Exception as e:
                logger.error(f"Failed to refresh token and retry for chains after 401: {e}")
                error_msg = (
                    "❌ Token refresh failed after 401 error\n"
                    "\n"
                    "🔧 To fix:\n"
                    "   python tools/reauth_schwab.py\n"
                )
                logger.error(error_msg)
                raise Exception(error_msg)

        response.raise_for_status()
        logger.info(f"Successfully fetched option chains for {symbol}.")
        return response.json()

    def get_price_history(self, symbol: str, periodType: str, period: int, frequencyType: str, frequency: int) -> dict:
        """
        Calls the /marketdata/v1/pricehistory endpoint to get historical price data.
        NEVER opens browser - raises exception if tokens invalid.
        """
        access_token = self._get_access_token()

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        }
        params = {
            "symbol": symbol,
            "periodType": periodType,
            "period": period,
            "frequencyType": frequencyType,
            "frequency": frequency
        }

        logger.info(f"Fetching price history for {symbol}...")
        response = requests.get(
            f"{SCHWAB_API_BASE_URL}/marketdata/v1/pricehistory",
            headers=headers,
            params=params,
            timeout=15
        )

        if response.status_code == 401:
            logger.warning("Received 401 Unauthorized for price history. Attempting token refresh and retry.")
            try:
                self._lock = True
                new_tokens = _refresh_token_flow(
                    self._tokens['refresh_token'],
                    self.client_id,
                    self.client_secret,
                    self.callback_url
                )
                self._tokens.update({
                    'access_token': new_tokens['access_token'],
                    'expires_in': new_tokens['expires_in'],
                    'expires_at': new_tokens['expires_at']
                })
                save_tokens(self._tokens, self.token_file_path)
                self._lock = False

                access_token = self._tokens['access_token']
                headers["Authorization"] = f"Bearer {access_token}"
                response = requests.get(
                    f"{SCHWAB_API_BASE_URL}/marketdata/v1/pricehistory",
                    headers=headers,
                    params=params,
                    timeout=15
                )
            except Exception as e:
                logger.error(f"Failed to refresh token and retry for price history after 401: {e}")
                error_msg = (
                    "❌ Token refresh failed after 401 error\n"
                    "\n"
                    "🔧 To fix:\n"
                    "   python tools/reauth_schwab.py\n"
                )
                logger.error(error_msg)
                raise Exception(error_msg)

        response.raise_for_status()
        logger.info(f"Successfully fetched price history for {symbol}.")
        return response.json()

# --- 401 Diagnosis Checklist ---
SCHWAB_401_DIAGNOSIS_CHECKLIST = """
Schwab API 401 Unauthorized Diagnosis Checklist:
1.  **Expired Access Token:** The most common cause. Ensure your client automatically refreshes tokens using the refresh token before the access token expires (typically 30 minutes).
2.  **Invalid Access Token:** The token might be malformed or corrupted. Run: python tools/reauth_schwab.py
3.  **Expired Refresh Token:** Refresh tokens expire after 7 days. Run: python tools/reauth_schwab.py
4.  **Token Clobbering:** Running OAuth twice invalidates old refresh tokens. Only use tools/reauth_schwab.py for re-authentication.
5.  **Incorrect Token Type:** Ensure you are using the `access_token` (Bearer token) and NOT the `id_token` for API calls.
6.  **Missing/Incorrect Authorization Header:** Verify the `Authorization: Bearer <access_token>` header is correctly formatted and included in your API requests.
7.  **Incorrect Client ID/Secret:** Double-check that the `client_id` and `client_secret` used for token exchange are correct and match your Schwab Developer App settings.
8.  **Incorrect Redirect URI:** The `redirect_uri` used in the authorization request and token exchange MUST exactly match the one registered in your Schwab Developer App.
9.  **Market Data Not Enabled:** Confirm that Market Data access is enabled for your application in the Schwab Developer Portal.
10. **Rate Limiting:** Repeated unauthorized requests might temporarily block your client. Check Schwab's rate limit policies.

🔧 To fix most issues:
   python tools/reauth_schwab.py
"""

# Example usage note:
# This client NEVER opens a browser. For initial authentication or when
# refresh tokens expire, manually run: python tools/reauth_schwab.py
