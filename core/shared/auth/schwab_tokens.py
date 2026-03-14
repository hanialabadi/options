from pathlib import Path
import json
import time
import logging
import os
import base64
import requests

logger = logging.getLogger(__name__)

TOKEN_PATH = Path.home() / ".schwab" / "tokens.json"

def save_tokens(tokens):
    """Persist tokens to the canonical location."""
    TOKEN_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    # Ensure we calculate expires_at if not present
    if 'expires_at' not in tokens and 'expires_in' in tokens:
        tokens['expires_at'] = int(time.time()) + tokens['expires_in']
    
    # Schwab refresh tokens typically last 7 days (604800 seconds)
    if 'refresh_expires_at' not in tokens and 'refresh_token_expires_in' in tokens:
        tokens['refresh_expires_at'] = int(time.time()) + tokens['refresh_token_expires_in']
    elif 'refresh_expires_at' not in tokens:
        # Fallback to 7 days if not provided
        tokens['refresh_expires_at'] = int(time.time()) + (7 * 24 * 3600)

    with open(TOKEN_PATH, "w") as f:
        json.dump(tokens, f, indent=4)
    logger.info(f"✅ Tokens saved to {TOKEN_PATH}")

def load_tokens():
    """
    Authoritative token loader for Schwab API.
    
    Returns:
        tuple: (tokens_dict or None, status_string)
        Status: "OK", "MISSING", "EXPIRED"
    """
    if not TOKEN_PATH.exists():
        return None, "MISSING"

    try:
        tokens = json.loads(TOKEN_PATH.read_text())
        
        # Check access token expiry (with 120s safety buffer)
        expires_at = tokens.get("expires_at", 0)
        current_time = int(time.time())
        
        if current_time > (expires_at - 120):
            return tokens, "EXPIRED"

        # Check refresh token expiry
        refresh_expires_at = tokens.get("refresh_expires_at", 0)
        if refresh_expires_at > 0 and current_time > refresh_expires_at:
            return tokens, "REFRESH_EXPIRED"

        return tokens, "OK"
        
    except Exception as e:
        logger.error(f"Failed to parse Schwab tokens: {e}")
        return None, "ERROR"

def _strip_quotes(val: str) -> str:
    """Strip surrounding double/single quotes from credential values.

    launchd plists can accidentally embed literal quotes, e.g.
    ``<string>"abc123"</string>`` passes ``"abc123"`` (with quotes)
    to the process.  This helper normalises to ``abc123``.
    """
    if val and len(val) >= 2:
        if (val[0] == '"' and val[-1] == '"') or (val[0] == "'" and val[-1] == "'"):
            return val[1:-1]
    return val


def refresh_schwab_tokens(*, _retry: int = 1):
    """
    Performs the refresh token flow and returns updated tokens.
    Uses environment variables for credentials.

    Retries once on transient failures (network hiccup, Schwab flakiness).
    Strips stray quotes from credentials (launchd plist defence).
    """
    client_id = _strip_quotes(os.getenv("SCHWAB_APP_KEY") or os.getenv("SCHWAB_API_KEY") or "")
    client_secret = _strip_quotes(os.getenv("SCHWAB_APP_SECRET") or "")

    if not client_id or not client_secret:
        logger.error("Missing SCHWAB_APP_KEY or SCHWAB_APP_SECRET for token refresh.")
        return None

    tokens, status = load_tokens()
    if not tokens or "refresh_token" not in tokens:
        logger.error("No refresh token available in token file.")
        return None

    logger.info("🔄 Refreshing Schwab access token...")

    credentials = f"{client_id}:{client_secret}"
    encoded_creds = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

    headers = {
        "Authorization": f"Basic {encoded_creds}",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    payload = {
        "grant_type": "refresh_token",
        "refresh_token": tokens["refresh_token"]
    }

    try:
        response = requests.post(
            "https://api.schwabapi.com/v1/oauth/token",
            headers=headers,
            data=payload,
            timeout=15
        )

        if not response.ok:
            logger.error(f"❌ Refresh failed: {response.status_code} - {response.text}")
            # Retry once on transient failure (network blip, Schwab rate limit)
            if _retry > 0:
                logger.info(f"🔁 Retrying token refresh in 3s ({_retry} retries left)...")
                time.sleep(3)
                return refresh_schwab_tokens(_retry=_retry - 1)
            return None

        new_tokens = response.json()
        # Schwab rotates refresh tokens. If a new one isn't provided, keep the old one.
        if "refresh_token" not in new_tokens:
            new_tokens["refresh_token"] = tokens["refresh_token"]

        save_tokens(new_tokens)
        logger.info("✅ Token refreshed and persisted.")
        return new_tokens
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error during token refresh: {e}")
        if _retry > 0:
            logger.info(f"🔁 Retrying token refresh in 3s ({_retry} retries left)...")
            time.sleep(3)
            return refresh_schwab_tokens(_retry=_retry - 1)
        return None
    except Exception as e:
        logger.error(f"Unexpected error during token refresh: {e}", exc_info=True)
        return None
