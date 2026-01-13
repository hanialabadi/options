#!/usr/bin/env python3
"""
Schwab Re-Authentication Utility

PURPOSE:
    Centralized, manual-only OAuth flow for Charles Schwab Trader API.
    This is the ONLY file that triggers browser-based authentication.
    
USAGE:
    python tools/reauth_schwab.py
"""

import os
import sys
import json
import time
import secrets
import webbrowser
from urllib.parse import urlparse, parse_qs
import requests
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env if present
load_dotenv()

SCHWAB_CLIENT_ID = os.getenv("SCHWAB_APP_KEY")
SCHWAB_CLIENT_SECRET = os.getenv("SCHWAB_APP_SECRET")
CALLBACK_URL = os.getenv("SCHWAB_CALLBACK_URL", "https://127.0.0.1")
TOKEN_PATH = os.path.expanduser("~/.schwab/tokens.json")
AUTH_URL = "https://api.schwabapi.com/v1/oauth/authorize"
TOKEN_URL = "https://api.schwabapi.com/v1/oauth/token"

if not SCHWAB_CLIENT_ID or not SCHWAB_CLIENT_SECRET:
    print("❌ SCHWAB_APP_KEY and SCHWAB_APP_SECRET must be set in environment.")
    sys.exit(1)

state = secrets.token_urlsafe(32)
auth_params = (
    f"{AUTH_URL}?response_type=code"
    f"&client_id={SCHWAB_CLIENT_ID}"
    f"&redirect_uri={CALLBACK_URL}"
    f"&state={state}"
)

print(f"\n[Schwab OAuth]\n==============================")
print(f"Authorize this app by visiting the following URL in your browser:\n\n  {auth_params}\n")
print(f"(State: {state[:8]}...)")
print("If your browser does not open automatically, copy and paste the above URL into your browser.")
print("\n⚠️  IMPORTANT: After authorizing, your browser will redirect to an unreachable URL.")
print("    Copy the ENTIRE redirect URL from your browser's address bar and paste it here.")
print("\nWaiting for you to paste the redirect URL...")

# Instead of running a server, prompt for manual code entry
try:
    webbrowser.open(auth_params)
except Exception as e:
    print(f"[Warning] Could not open browser automatically: {e}")

redirect_url = input("\nPaste the full redirect URL here: ").strip()
parsed = urlparse(redirect_url)
params = parse_qs(parsed.query)
code = params.get("code", [None])[0]
received_state = params.get("state", [None])[0]
error = params.get("error", [None])[0]

if error:
    print(f"❌ OAuth error: {error}")
    sys.exit(1)
if not code:
    print("❌ No authorization code found in URL.")
    sys.exit(1)
if received_state != state:
    print("❌ CSRF/state mismatch. Aborting.")
    sys.exit(1)

print("✅ Code and state received. Exchanging for tokens...")

data = {
    "grant_type": "authorization_code",
    "code": code,
    "redirect_uri": CALLBACK_URL,
}
auth = (SCHWAB_CLIENT_ID, SCHWAB_CLIENT_SECRET)
resp = requests.post(TOKEN_URL, data=data, auth=auth)
if not resp.ok:
    print(f"❌ Token exchange failed: {resp.text}")
    sys.exit(1)
tokens = resp.json()
now = int(time.time())
tokens_out = {
    "access_token": tokens["access_token"],
    "refresh_token": tokens["refresh_token"],
    "access_expires_at": now + tokens.get("expires_in", 1800),
    "refresh_expires_at": now + 7 * 24 * 3600,
    "token_type": tokens.get("token_type", "Bearer"),
}
os.makedirs(os.path.dirname(TOKEN_PATH), exist_ok=True)
with open(TOKEN_PATH, "w") as f:
    json.dump(tokens_out, f, indent=2)
print(f"✅ Tokens saved to {TOKEN_PATH}")
print(f"  Access expires: {datetime.fromtimestamp(tokens_out['access_expires_at'])}")
print(f"  Refresh expires: {datetime.fromtimestamp(tokens_out['refresh_expires_at'])}")
