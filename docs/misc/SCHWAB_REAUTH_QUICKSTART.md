# Schwab Re-Authentication: Quick Start

## TL;DR

```bash
# Initial setup (one-time)
export SCHWAB_CLIENT_ID='your_client_id'
export SCHWAB_CLIENT_SECRET='your_client_secret'
python tools/reauth_schwab.py

# Run pipeline (works for 7 days)
python tests/test_schwab_pipeline_20tickers.py

# Re-authenticate when tokens expire (7 days later)
python tools/reauth_schwab.py
```

---

## What Changed?

**Before** (BROKEN):
- Pipeline could open browser unexpectedly
- Tokens expired within hours (token clobbering bug)
- Re-auth needed multiple times per day

**After** (FIXED):
- Pipeline NEVER opens browser
- Tokens last full 7 days
- Re-auth needed once per week

---

## Initial Setup

### 1. Set Environment Variables

```bash
# Required
export SCHWAB_CLIENT_ID='your_app_client_id'
export SCHWAB_CLIENT_SECRET='your_app_client_secret'

# Optional (default: https://127.0.0.1:8182)
export SCHWAB_CALLBACK_URL='https://127.0.0.1:8182'

# Make permanent (add to ~/.zshrc or ~/.bashrc)
echo 'export SCHWAB_CLIENT_ID="your_app_client_id"' >> ~/.zshrc
echo 'export SCHWAB_CLIENT_SECRET="your_app_client_secret"' >> ~/.zshrc
```

**Get credentials from**: [Schwab Developer Portal](https://developer.schwab.com/)

### 2. Run Initial Authentication

```bash
python tools/reauth_schwab.py
```

**What happens**:
1. Opens browser
2. Redirects to Schwab login
3. You log in and authorize
4. Tokens saved to `~/.schwab/tokens.json`
5. Script exits with success message

**Expected output**:
```
✅ SCHWAB RE-AUTHENTICATION COMPLETE

📄 Token File: /Users/you/.schwab/tokens.json

🔑 Access Token:
   Expires: 2025-01-01 14:30:00
   Remaining: 0.5 hours

🔄 Refresh Token:
   Expires: 2025-01-08 14:00:00
   Remaining: 7.0 days

✅ Refresh token valid for 7 days
```

### 3. Test Pipeline (No Browser)

```bash
# Should run without opening browser
python tests/test_schwab_pipeline_20tickers.py

# Or run Step 0 snapshot
python core/scan_engine/step0_schwab_snapshot.py
```

**Expected**: Pipeline runs, no browser opens, tokens auto-refreshed

---

## Daily Usage

### Running the Pipeline

```bash
# Just run - no authentication needed
python tests/test_schwab_pipeline_20tickers.py

# Pipeline auto-refreshes access tokens (no browser)
# You only need to re-auth when refresh token expires (7 days)
```

### When Do I Need to Re-Authenticate?

**Answer**: Once every 7 days (when refresh token expires)

**You'll see this error**:
```
❌ Refresh token expired
   Expiry: 2025-01-08 14:00:00

🔧 To fix:
   python tools/reauth_schwab.py
```

**Then run**:
```bash
python tools/reauth_schwab.py
```

---

## Troubleshooting

### Error: "No tokens found"

**Symptom**:
```
❌ No tokens found
   Token file: ~/.schwab/tokens.json

🔧 To fix:
   python tools/reauth_schwab.py
```

**Solution**:
```bash
python tools/reauth_schwab.py
```

---

### Error: "Missing environment variables"

**Symptom**:
```
❌ ERROR: Missing required environment variables:
   SCHWAB_CLIENT_ID
   SCHWAB_CLIENT_SECRET
```

**Solution**:
```bash
export SCHWAB_CLIENT_ID='your_client_id'
export SCHWAB_CLIENT_SECRET='your_client_secret'
python tools/reauth_schwab.py
```

---

### Error: "Token refresh failed after 401"

**Symptom**:
```
❌ Token refresh failed after 401 error

🔧 To fix:
   python tools/reauth_schwab.py
```

**Cause**: Refresh token was invalidated (ran OAuth twice, or manually edited tokens)

**Solution**:
```bash
python tools/reauth_schwab.py
```

---

### I Accidentally Ran Reauth Twice

**Problem**: Running `tools/reauth_schwab.py` twice within 7 days invalidates old tokens

**Impact**: If multiple scripts are using old tokens, they'll fail with 401 errors

**Solution**: Just keep using the newest tokens - they're valid for 7 days from the last reauth

**Prevention**: The script warns you:
```
⚠️  WARNING: Existing token file found
   Re-authenticating will INVALIDATE the existing refresh token

Proceed with re-authentication? [y/N]:
```

Press **N** to abort if you don't actually need to reauth.

---

## Migration from Old Setup

### If You Have Old Tokens (`~/.schwab_tokens.json`)

**Option 1: Move tokens** (quick):
```bash
mkdir -p ~/.schwab
mv ~/.schwab_tokens.json ~/.schwab/tokens.json
```

**Option 2: Re-authenticate** (cleaner):
```bash
python tools/reauth_schwab.py
```

---

## Advanced Usage

### Check Token Status

```bash
python3 -c "
import json
from pathlib import Path
from datetime import datetime

token_file = Path.home() / '.schwab' / 'tokens.json'
tokens = json.load(open(token_file))

access_exp = datetime.fromtimestamp(tokens['access_expires_at'])
refresh_exp = datetime.fromtimestamp(tokens['refresh_expires_at'])

print(f'Access token expires:  {access_exp}')
print(f'Refresh token expires: {refresh_exp}')

now = datetime.now()
print(f'\\nAccess token: {(access_exp - now).total_seconds()/3600:.1f} hours remaining')
print(f'Refresh token: {(refresh_exp - now).total_seconds()/86400:.1f} days remaining')
"
```

### Force Access Token Refresh (Testing)

```bash
# Manually set access token to expired
python3 -c "
import json
from pathlib import Path
token_file = Path.home() / '.schwab' / 'tokens.json'
tokens = json.load(open(token_file))
tokens['expires_at'] = 0
json.dump(tokens, open(token_file, 'w'), indent=2)
print('Access token expired (for testing)')
"

# Run pipeline - should auto-refresh without browser
python tests/test_schwab_pipeline_20tickers.py
```

---

## Key Takeaways

✅ **DO**:
- Run `python tools/reauth_schwab.py` once every 7 days
- Let pipeline auto-refresh access tokens (it's automatic)
- Check token expiry before long weekend trips

❌ **DON'T**:
- Run reauth script multiple times in same week (causes token clobbering)
- Edit `~/.schwab/tokens.json` manually (will corrupt tokens)
- Commit tokens to git (add to .gitignore)
- Share tokens.json between different Schwab apps (use separate token files)

🎯 **Remember**:
- Access tokens: 30 minutes (auto-refreshed by pipeline)
- Refresh tokens: 7 days (manual reauth via tools/reauth_schwab.py)
- Pipeline NEVER opens browser
- One OAuth source = No token clobbering

---

## Support

For detailed architecture and troubleshooting, see:
- [SCHWAB_AUTH_ARCHITECTURE.md](SCHWAB_AUTH_ARCHITECTURE.md)

For Step 0 NaN price bug fix, see:
- [STEP0_NAN_PRICE_BUG_FIX.md](STEP0_NAN_PRICE_BUG_FIX.md)
