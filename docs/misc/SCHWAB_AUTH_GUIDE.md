# Schwab Authentication Guide

## 🎯 Overview

Schwab authentication is now **deterministic, CSRF-proof, and requires zero manual URL pasting**.

- Auth once every ~7 days
- Never opens browser from client code
- Local HTTP server auto-captures OAuth code
- Tokens saved with explicit expiry timestamps

---

## ✅ Quick Start

### 1. Set Environment Variables

```bash
export SCHWAB_APP_KEY="your_app_key"
export SCHWAB_APP_SECRET="your_app_secret"
```

### 2. Run One-Time Authentication

```bash
./venv/bin/python tools/reauth_schwab.py
```

**Expected Output:**
```
[Schwab OAuth]
==============================
Authorize this app by visiting the following URL in your browser:

  https://api.schwabapi.com/v1/oauth/authorize?response_type=code&client_id=...&redirect_uri=http://localhost:8080&state=xTwVtFsJ...

(State: xTwVtFsJ...)
If your browser does not open automatically, copy and paste the above URL into your browser.
Waiting for authentication...
✅ Code and state received. Exchanging for tokens...
✅ Tokens saved to /Users/you/.schwab/tokens.json
  Access expires: 2025-12-31 15:30:00
  Refresh expires: 2026-01-07 14:30:00
```

**What Happens:**
1. Script prints auth URL
2. Browser opens automatically (or you copy/paste URL)
3. You log in to Schwab and authorize
4. Schwab redirects to http://localhost:8080
5. Local server captures code + state
6. Script validates CSRF state
7. Tokens saved to `~/.schwab/tokens.json`

### 3. Run CLI Smoke Test

```bash
./venv/bin/python tests/test_schwab_quote_smoke.py
```

**Expected Output:**
```
=== Schwab Quote Smoke Test ===
AAPL: $195.50 (extended_hours) [Market closed]
MSFT: $425.30 (extended_hours) [Market closed]
NVDA: $525.75 (extended_hours) [Market closed]
✅ All prices valid
```

### 4. Run Step 0 Snapshot

```bash
./venv/bin/python core/scan_engine/step0_schwab_snapshot.py
```

**Expected Output:**
```
[Step 0] Market Status: closed
[Step 0] Price Source: {'extended_hours': 2500, 'regular': 0}
[Step 0] NaN Count: 0 / 2500 (0.0%)
✅ Snapshot validation passed
```

---

## 🔧 Architecture

### OAuth Flow (tools/reauth_schwab.py)

```
1. Generate state = secrets.token_urlsafe(32)
2. Start local HTTP server on localhost:8080
3. Open browser to Schwab OAuth URL
4. Schwab redirects to http://localhost:8080?code=...&state=...
5. Server captures code + state
6. Validate: received_state == generated_state
7. Exchange code for tokens via POST to Schwab
8. Save tokens to ~/.schwab/tokens.json with expiry timestamps
```

**Key Features:**
- ✅ No manual URL paste
- ✅ No CSRF/state mismatch
- ✅ Auto browser open with fallback instructions
- ✅ Deterministic token write

### Client (schwab_api_client.py)

```python
# Token-only behavior
tokens = load_tokens()  # From ~/.schwab/tokens.json

if token_expired(access_token):
    refresh_access_token(refresh_token)
    save_tokens(new_tokens)

# If refresh fails or missing:
raise RuntimeError("Run: python tools/reauth_schwab.py")
```

**Key Features:**
- ❌ No browser logic
- ❌ No OAuth flow
- ✅ Token refresh only
- ✅ Clear error messages

---

## 🛡️ CSRF Protection

**How State is Generated:**
```python
state = secrets.token_urlsafe(32)  # Cryptographically secure
```

**How State is Validated:**
```python
if OAuthHandler.received_state != state:
    print("❌ CSRF/state mismatch. Aborting.")
    sys.exit(1)
```

**Why This Works:**
1. State is generated locally (not by schwab-py)
2. State is passed to Schwab in auth URL
3. Schwab returns state in redirect
4. We validate exact match before token exchange

---

## 📁 Token File Format

`~/.schwab/tokens.json`:
```json
{
  "access_token": "...",
  "refresh_token": "...",
  "access_expires_at": 1735668600,
  "refresh_expires_at": 1736273400,
  "token_type": "Bearer"
}
```

**Expiry Logic:**
- Access token: 30 minutes (refreshed automatically)
- Refresh token: 7 days (requires re-auth)

---

## 🚨 Troubleshooting

### "SCHWAB_APP_KEY and SCHWAB_APP_SECRET must be set"

```bash
export SCHWAB_APP_KEY="..."
export SCHWAB_APP_SECRET="..."
```

### "OAuth failed or timed out"

- Check if browser opened
- Check if you completed Schwab login
- Check if callback URL in Schwab app settings is `http://localhost:8080`

### "CSRF/state mismatch"

This should never happen now. If it does:
1. Check callback URL is exactly `http://localhost:8080` (no HTTPS, no trailing slash)
2. Verify Schwab app redirect URI matches exactly

### "Token exchange failed: 401"

- Verify SCHWAB_APP_KEY and SCHWAB_APP_SECRET are correct
- Check Schwab app is approved (not in sandbox mode)

### "Run: python tools/reauth_schwab.py"

Your refresh token expired (>7 days old). Re-authenticate:
```bash
./venv/bin/python tools/reauth_schwab.py
```

---

## ✅ Success Criteria

After authentication:

- [x] Auth works first try
- [x] No CSRF errors
- [x] Tokens saved every time
- [x] Snapshot runs immediately after
- [x] Re-auth needed weekly, not hourly

---

## 📝 Notes

- **Never commit** `~/.schwab/tokens.json` to git
- **Callback URL** must be `http://localhost:8080` (hardcoded, no override)
- **Browser** opens automatically; fallback instructions printed
- **No schwab-py** easy_client or interactive prompt used
- **State** generated and validated locally (no library dependency)
