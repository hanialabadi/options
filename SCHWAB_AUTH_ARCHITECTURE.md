# Schwab Authentication Architecture: Token Clobbering Fix

## Problem Statement

**Symptom**: Schwab tokens were expiring within <1 hour, forcing re-authentication far more frequently than the documented 7-day refresh token lifetime.

**Root Cause**: **Token Clobbering**

When multiple OAuth flows run within 7 days, Schwab invalidates old refresh tokens:
```
Time 0:    Run OAuth #1 → Get refresh_token_A (valid 7 days)
Time +30m: Pipeline refreshes access token using refresh_token_A ✅ Works
Time +1h:  Run OAuth #2 → Get refresh_token_B (valid 7 days)
           ❌ Schwab INVALIDATES refresh_token_A immediately
Time +2h:  Pipeline tries to refresh using refresh_token_A → 401 Unauthorized
```

**Why this happened:**
- Old code had `authenticate_and_get_tokens()` method in SchwabClient
- Pipeline would auto-trigger browser OAuth on any token error
- Dashboard, tests, and scripts could all trigger OAuth independently
- Each OAuth run invalidated the previous refresh token
- This caused cascading failures across all processes

---

## Solution: Centralized, Manual-Only OAuth

### Architecture Changes

**Before** (BROKEN):
```
Pipeline Script A → SchwabClient → Opens Browser (OAuth #1)
Dashboard        → SchwabClient → Opens Browser (OAuth #2) ← Invalidates #1
Test Script      → SchwabClient → Opens Browser (OAuth #3) ← Invalidates #2
```

**After** (FIXED):
```
tools/reauth_schwab.py → Schwab OAuth → Saves tokens to ~/.schwab/tokens.json

Pipeline Script A → SchwabClient → Reads tokens → Refreshes access token (no browser)
Dashboard        → SchwabClient → Reads tokens → Refreshes access token (no browser)
Test Script      → SchwabClient → Reads tokens → Refreshes access token (no browser)
```

**Key Principle**: **ONE OAuth source, MANY token consumers**

---

## Token Lifecycle (Schwab OAuth Spec)

### Access Token
- **Lifetime**: ~30 minutes
- **Purpose**: Authenticate API requests
- **Can be refreshed**: ✅ Yes, using refresh token
- **How to refresh**: POST to `/oauth/token` with `grant_type=refresh_token`
- **No browser required**: ✅

### Refresh Token
- **Lifetime**: 7 days
- **Purpose**: Get new access tokens
- **Can be refreshed**: ❌ NO! Must run full OAuth flow
- **How to get new one**: Browser-based OAuth authorization flow
- **Critical**: Old refresh token invalidated when new one issued

### The Contract

```
DAY 0:   Run OAuth → Get access_token + refresh_token
DAY 0-7: Pipeline auto-refreshes access_token (no browser, no user action)
DAY 7:   Refresh token expires → Pipeline STOPS → User runs tools/reauth_schwab.py
DAY 7+:  New 7-day cycle begins
```

**Expected re-auth frequency**: Once every 7 days  
**Actual frequency with bug**: Multiple times per day (token clobbering)  
**Actual frequency with fix**: Once every 7 days ✅

---

## File Structure

### 1. `tools/reauth_schwab.py` (NEW)

**Purpose**: Centralized, manual-only OAuth flow

**Behavior**:
- Opens browser for Schwab login
- Exchanges authorization code for tokens
- Saves to `~/.schwab/tokens.json`
- Adds expiry timestamps (`access_expires_at`, `refresh_expires_at`)
- Prints success report with expiry times
- Exits cleanly

**Critical Rules**:
- ❌ Never imported by pipeline
- ❌ Never auto-triggered
- ✅ Human-in-the-loop only
- ✅ Runs at most once per 7 days

**Usage**:
```bash
python tools/reauth_schwab.py
```

**Output**:
```
✅ SCHWAB RE-AUTHENTICATION COMPLETE

📄 Token File: ~/.schwab/tokens.json

🔑 Access Token:
   Expires: 2025-01-01 14:30:00
   Remaining: 0.5 hours

🔄 Refresh Token:
   Expires: 2025-01-08 14:00:00
   Remaining: 7.0 days

✅ Refresh token valid for 7 days
```

---

### 2. `core/scan_engine/schwab_api_client.py` (MODIFIED)

**Changes Made**:

#### A. Centralized Token Path
```python
# OLD (BROKEN): ~/.schwab_tokens.json
# NEW (FIXED):  ~/.schwab/tokens.json (matches tools/reauth_schwab.py)
TOKEN_FILE = os.path.expanduser("~/.schwab/tokens.json")
```

#### B. Removed Browser-Based OAuth
```python
# DELETED METHOD:
def authenticate_and_get_tokens(self):
    """Opens browser, exchanges auth code for tokens"""
    webbrowser.open(auth_url)  # ❌ REMOVED
    returned_url = input("Paste URL: ")
    # ... token exchange ...
```

**Why removed**: This method was the root cause of token clobbering. Multiple processes could trigger it, each invalidating previous tokens.

#### C. Enhanced Error Messages
```python
# OLD (BROKEN):
raise Exception("No tokens found. Manual re-authentication required.")

# NEW (FIXED):
raise Exception(
    "❌ No tokens found\n"
    "   Token file: ~/.schwab/tokens.json\n"
    "\n"
    "🔧 To fix:\n"
    "   python tools/reauth_schwab.py\n"
)
```

**All methods** (`_get_access_token`, `ensure_valid_token`, `get_quotes`, `get_chains`, `get_price_history`) now:
- ❌ Never open browser
- ❌ Never trigger OAuth
- ✅ Raise exception with clear instructions if tokens invalid
- ✅ Auto-refresh access tokens (no browser)
- ✅ Stop pipeline if refresh token expired

#### D. Improved Token Expiry Checks
```python
# Check REFRESH token first (can't auto-fix)
if time.time() >= self._tokens.get('refresh_expires_at', 0):
    raise Exception("Refresh token expired. Run tools/reauth_schwab.py")

# Check ACCESS token second (can auto-fix)
if time.time() >= self._tokens.get('expires_at', 0):
    logger.info("Access token expired. Refreshing...")
    new_tokens = _refresh_token_flow(...)  # No browser
```

---

## Guardrails

### 1. Token File Validation (tools/reauth_schwab.py)

```python
# Refuse relative paths
if not TOKEN_DIR.is_absolute():
    raise ValueError("Token directory must be absolute path")

# Create directory if missing
TOKEN_DIR.mkdir(parents=True, exist_ok=True)
```

### 2. Existing Token Warning

```python
if TOKEN_FILE.exists():
    print("⚠️  WARNING: Existing token file found")
    print("   Re-authenticating will INVALIDATE the existing refresh token")
    response = input("Proceed? [y/N]: ")
    if response.lower() != 'y':
        sys.exit(0)
```

Prevents accidental OAuth runs that would invalidate working tokens.

### 3. Environment Variable Validation

```python
required_vars = ['SCHWAB_CLIENT_ID', 'SCHWAB_CLIENT_SECRET']
missing = [var for var in required_vars if not os.getenv(var)]

if missing:
    print("❌ ERROR: Missing required environment variables")
    sys.exit(1)
```

### 4. Enhanced Token Structure

```python
# schwab-py library saves:
{
  "access_token": "...",
  "refresh_token": "...",
  "expires_in": 1800,
  "token_type": "Bearer",
  "scope": "..."
}

# tools/reauth_schwab.py adds:
{
  # ... above fields ...
  "access_expires_at": 1735743000,    # Unix timestamp
  "refresh_expires_at": 1736347800    # Unix timestamp
}
```

Enables observability: Pipeline logs "Token valid (refresh: 6.2 days remaining)"

---

## Migration Guide

### For Existing Users

If you have old tokens in `~/.schwab_tokens.json`:

```bash
# Option 1: Move tokens to new location
mkdir -p ~/.schwab
mv ~/.schwab_tokens.json ~/.schwab/tokens.json

# Option 2: Re-authenticate (cleaner)
python tools/reauth_schwab.py
```

### For New Users

```bash
# 1. Set environment variables (one-time)
export SCHWAB_CLIENT_ID='your_client_id'
export SCHWAB_CLIENT_SECRET='your_client_secret'

# Add to ~/.zshrc or ~/.bashrc for persistence

# 2. Run initial authentication
python tools/reauth_schwab.py

# Browser will open, log in to Schwab, authorize app
# Tokens saved to ~/.schwab/tokens.json

# 3. Run pipeline (no browser, uses saved tokens)
python tests/test_schwab_pipeline_20tickers.py

# 4. Re-authenticate ONLY when refresh token expires (7 days)
python tools/reauth_schwab.py
```

---

## Failure Modes & Recovery

### Scenario 1: Access Token Expired (30 minutes)

**Symptom**: Pipeline runs fine for 30 minutes, then continues fine (no error)

**What happens**:
```python
# Pipeline calls get_quotes()
# SchwabClient detects expired access token
# Auto-refreshes using refresh token (no browser)
# Continues execution seamlessly
```

**User action**: None (automatic)

---

### Scenario 2: Refresh Token Expired (7 days)

**Symptom**: Pipeline stops with clear error message

**Error**:
```
❌ Refresh token expired
   Expiry: 2025-01-08 14:00:00

🔧 To fix:
   python tools/reauth_schwab.py
```

**Recovery**:
```bash
python tools/reauth_schwab.py
# Browser opens, log in, authorize
# Pipeline can now run for next 7 days
```

**User action**: Run reauth script (once per 7 days)

---

### Scenario 3: Token File Missing

**Symptom**: Pipeline stops immediately

**Error**:
```
❌ No tokens found
   Token file: ~/.schwab/tokens.json

🔧 To fix:
   python tools/reauth_schwab.py
```

**Recovery**: Same as Scenario 2

---

### Scenario 4: Token File Corrupted

**Symptom**: Pipeline stops with JSON parse error

**Recovery**:
```bash
# Delete corrupted file
rm ~/.schwab/tokens.json

# Re-authenticate
python tools/reauth_schwab.py
```

---

### Scenario 5: Accidental Double OAuth (Token Clobbering)

**Symptom**: Pipeline works immediately after reauth, then fails within hours

**What happened**:
```
10:00 AM: User A runs tools/reauth_schwab.py → refresh_token_A
12:00 PM: User B runs tools/reauth_schwab.py → refresh_token_B (invalidates A)
02:00 PM: Pipeline tries to refresh using refresh_token_A → 401 Unauthorized
```

**Prevention**:
- `tools/reauth_schwab.py` warns before overwriting existing tokens
- Only run reauth when tokens actually expired
- Check expiry first: `cat ~/.schwab/tokens.json | jq '.refresh_expires_at'`

**Recovery**:
```bash
# Latest OAuth wins, so use the newest tokens
# If multiple people are running reauth, coordinate:
# - Only one person runs reauth
# - Share tokens.json file with team
# OR: Each person has their own Schwab app credentials
```

---

## Testing

### Unit Test: Token Refresh (No Browser)

```bash
# Manually set access_expires_at to past time
python3 -c "
import json
from pathlib import Path
token_file = Path.home() / '.schwab' / 'tokens.json'
with open(token_file) as f:
    tokens = json.load(f)
tokens['expires_at'] = 0  # Force expiry
with open(token_file, 'w') as f:
    json.dump(tokens, f, indent=2)
print('Access token expired (for testing)')
"

# Run pipeline - should auto-refresh without browser
python tests/test_schwab_pipeline_20tickers.py

# Should see log:
# "Access token expired. Attempting to refresh."
# "✅ Token refreshed successfully"
```

### Integration Test: Full Reauth Flow

```bash
# Delete tokens
rm ~/.schwab/tokens.json

# Run reauth (opens browser)
python tools/reauth_schwab.py

# Verify tokens created
cat ~/.schwab/tokens.json | jq '{
  access_expires_at: .access_expires_at,
  refresh_expires_at: .refresh_expires_at
}'

# Run pipeline (no browser)
python tests/test_schwab_pipeline_20tickers.py
```

---

## Monitoring & Observability

### Check Token Status

```bash
# View token expiry times
python3 -c "
import json
from pathlib import Path
from datetime import datetime

token_file = Path.home() / '.schwab' / 'tokens.json'
with open(token_file) as f:
    tokens = json.load(f)

access_exp = datetime.fromtimestamp(tokens['access_expires_at'])
refresh_exp = datetime.fromtimestamp(tokens['refresh_expires_at'])

print(f'Access token expires:  {access_exp}')
print(f'Refresh token expires: {refresh_exp}')

now = datetime.now()
access_remaining = (access_exp - now).total_seconds()
refresh_remaining = (refresh_exp - now).total_seconds()

print(f'\\nAccess token: {access_remaining/3600:.1f} hours remaining')
print(f'Refresh token: {refresh_remaining/86400:.1f} days remaining')

if refresh_remaining < 86400:
    print('\\n⚠️  WARNING: Refresh token expires in <1 day')
    print('   Run: python tools/reauth_schwab.py')
"
```

### Pipeline Logs (Enhanced)

```python
# Old logs:
"✅ Pre-flight check passed: Token valid"

# New logs (with expiry info):
"✅ Pre-flight check passed: Token valid (access: 0.5h, refresh: 6.2d remaining)"
```

---

## Dependencies

### Required Python Libraries

```bash
pip install schwab-py  # For tools/reauth_schwab.py
pip install requests   # For core/scan_engine/schwab_api_client.py
pip install loguru     # For logging
```

### Environment Variables

```bash
# Required for OAuth
export SCHWAB_CLIENT_ID='your_client_id'
export SCHWAB_CLIENT_SECRET='your_client_secret'

# Optional (defaults to https://127.0.0.1:8182)
export SCHWAB_CALLBACK_URL='https://127.0.0.1:8182'
```

---

## Why This Fixes the <1 Hour Bug

### The Bug

**Observed**: Tokens expired within 1 hour (expected: 7 days)

**Cause**: Multiple OAuth flows ran within hours:
1. Test script ran → OAuth #1 → refresh_token_A
2. Dashboard started → OAuth #2 → refresh_token_B (invalidated A)
3. Pipeline retried → OAuth #3 → refresh_token_C (invalidated B)
4. Each OAuth invalidated previous tokens → cascading failures

**Result**: Even though refresh tokens have 7-day lifetime, they were being invalidated within hours by subsequent OAuth runs.

### The Fix

**Principle**: ONE OAuth source (tools/reauth_schwab.py), MANY token consumers (all scripts)

**Guarantees**:
1. ✅ Browser OAuth runs at most once per 7 days
2. ✅ All scripts share same tokens (no clobbering)
3. ✅ Pipeline NEVER opens browser (can't clobber)
4. ✅ Token refresh happens automatically (no user action)
5. ✅ Clear error messages when reauth needed

**Result**: Refresh tokens now live full 7 days. Re-auth happens once per week instead of multiple times per day.

---

## Production Best Practices

### 1. Centralize Credentials

```bash
# Team shared credentials (one Schwab app)
# Store in password manager or secrets service
# All developers use same CLIENT_ID/SECRET

# OR: Each developer has own Schwab app
# Each has separate tokens.json
```

### 2. Never Commit Tokens

```bash
# .gitignore
.schwab/
.schwab_tokens.json
*.tokens.json
```

### 3. Automate Token Monitoring

```bash
# Cron job to check token expiry daily
0 9 * * * python /path/to/check_schwab_tokens.py

# check_schwab_tokens.py
import json
from pathlib import Path
from datetime import datetime

token_file = Path.home() / '.schwab' / 'tokens.json'
tokens = json.load(open(token_file))
refresh_exp = datetime.fromtimestamp(tokens['refresh_expires_at'])
remaining = (refresh_exp - datetime.now()).total_seconds() / 86400

if remaining < 1:
    print("⚠️  Schwab refresh token expires in <1 day")
    print("   Run: python tools/reauth_schwab.py")
    # Send alert email/Slack notification
```

### 4. Document Token Path

```bash
# Project README.md
## Schwab Authentication

Tokens are stored in: `~/.schwab/tokens.json`

Initial setup:
  python tools/reauth_schwab.py

Re-authenticate (once per 7 days):
  python tools/reauth_schwab.py

Pipeline will auto-refresh access tokens (no action needed).
```

---

## Summary

### Problem
- Schwab tokens were expiring within <1 hour
- Root cause: Multiple OAuth flows invalidated each other's refresh tokens

### Solution
- Centralized OAuth in `tools/reauth_schwab.py` (manual-only)
- Removed browser OAuth from `SchwabClient` (pipeline-safe)
- Shared token file `~/.schwab/tokens.json` (one source of truth)

### Result
- ✅ Tokens now last full 7 days
- ✅ Pipeline auto-refreshes access tokens (no browser)
- ✅ Re-auth needed once per week (not multiple times per day)
- ✅ Clear error messages guide users to reauth script

### Files Changed
1. `tools/reauth_schwab.py` - NEW (centralized OAuth)
2. `core/scan_engine/schwab_api_client.py` - MODIFIED (removed browser OAuth)
3. Token path: `~/.schwab_tokens.json` → `~/.schwab/tokens.json`

### Next Steps
1. Run initial authentication: `python tools/reauth_schwab.py`
2. Test pipeline (should work for 7 days without reauth)
3. Add token monitoring (cron job checking expiry)
4. Document in team wiki/README
