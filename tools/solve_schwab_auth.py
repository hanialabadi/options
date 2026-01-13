import os
import json
import time
import requests
import base64
from pathlib import Path
from dotenv import load_dotenv

def solve():
    print("🔍 [Schwab Auth Solver] Starting diagnosis...")
    
    # 1. Check .env
    env_path = Path(".env")
    if not env_path.exists():
        print("❌ Error: .env file not found in project root.")
        return

    load_dotenv()
    key = os.getenv("SCHWAB_APP_KEY")
    secret = os.getenv("SCHWAB_APP_SECRET")
    token_path_env = os.getenv("SCHWAB_TOKEN_PATH")

    if not key or not secret:
        print("❌ Error: SCHWAB_APP_KEY or SCHWAB_APP_SECRET missing in .env")
        return

    print(f"✅ Environment variables loaded (Key: {key[:5]}...)")

    # 2. Check Token Path
    token_path = os.path.expanduser("~/.schwab/tokens.json")
    if token_path_env:
        # Check for shell constructs in .env
        if "$(" in token_path_env or "whoami" in token_path_env:
            print(f"⚠️  Warning: SCHWAB_TOKEN_PATH in .env contains shell constructs: {token_path_env}")
            print("   Python will not expand these. Using default path instead.")
        else:
            token_path = os.path.expanduser(token_path_env)

    print(f"📂 Token Path: {token_path}")
    
    if not os.path.exists(token_path):
        print(f"❌ Error: Token file not found at {token_path}")
        print("🔧 Fix: Run 'python tools/reauth_schwab.py' to generate tokens.")
        return

    # 3. Load and Test Tokens
    try:
        with open(token_path, "r") as f:
            tokens = json.load(f)
    except Exception as e:
        print(f"❌ Error: Failed to read token file: {e}")
        return

    refresh_token = tokens.get("refresh_token")
    if not refresh_token:
        print("❌ Error: No refresh_token found in token file.")
        print("🔧 Fix: Run 'python tools/reauth_schwab.py'")
        return

    # 4. Attempt Refresh
    print("🔄 Attempting to refresh access token...")
    credentials = f"{key}:{secret}"
    base64_credentials = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")
    headers = {
        "Authorization": f"Basic {base64_credentials}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token
    }

    try:
        resp = requests.post("https://api.schwabapi.com/v1/oauth/token", headers=headers, data=data, timeout=15)
        if resp.status_code == 200:
            print("✅ Success: Token refreshed successfully.")
            new_tokens = resp.json()
            now = int(time.time())
            tokens.update({
                "access_token": new_tokens["access_token"],
                "refresh_token": new_tokens.get("refresh_token", refresh_token),
                "access_expires_at": now + new_tokens.get("expires_in", 1800),
            })
            with open(token_path, "w") as f:
                json.dump(tokens, f, indent=2)
            print(f"💾 Updated tokens saved to {token_path}")
        else:
            print(f"❌ Error: Refresh failed (Status {resp.status_code})")
            print(f"   Response: {resp.text}")
            if "invalid_client" in resp.text:
                print("\n💡 Diagnosis: 'invalid_client' usually means:")
                print("   1. Your App Key or Secret in .env is incorrect.")
                print("   2. Your refresh token has been revoked or replaced.")
                print("🔧 Fix: Double-check .env and run 'python tools/reauth_schwab.py'")
    except Exception as e:
        print(f"❌ Error: Request failed: {e}")

if __name__ == "__main__":
    solve()
