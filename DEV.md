# Developer Documentation

## Schwab API Authentication

The pipeline requires authentication with the Charles Schwab Trader API.

### Required Environment Variables

Ensure the following variables are set in your `.env` file or shell environment:

- `SCHWAB_APP_KEY`: Your Schwab Application Client ID.
- `SCHWAB_APP_SECRET`: Your Schwab Application Client Secret.
- `SCHWAB_CALLBACK_URL`: The redirect URI registered with your Schwab app (default: `https://127.0.0.1`).

### Re-Authentication Flow

If tokens are missing or the refresh token has expired (after 7 days), run the manual re-authentication utility:

```bash
python tools/reauth_schwab.py
```

1. The tool will open your default browser to the Schwab authorization page.
2. Log in and authorize the application.
3. You will be redirected to an unreachable URL (e.g., `https://127.0.0.1/...`).
4. Copy the **entire URL** from the address bar and paste it back into the terminal prompt.

### Token Storage

Tokens are stored locally at:
`~/.schwab/tokens.json`

The `SchwabClient` will automatically handle access token refreshes as long as the refresh token is valid. No runtime browser interaction occurs during the scan pipeline.
