# Charles Schwab API Authentication & Governance Policy

## 1. Policy Overview
Authentication with the Charles Schwab API is a **scarce, high-risk operation**. Repeated or automated authorization and refresh attempts are known to trigger account-level blocks. 

This policy mandates a **Passive-Consumption Model** to ensure operational safety. Authentication is treated as a manual maintenance event, not a runtime feature.

---

## 2. Core Principles

### 2.1 Maintenance vs. Consumption Decoupling
The system is strictly divided into two layers with no cross-contamination of responsibilities:
*   **Maintenance Layer (Active/Manual)**: The only authorized path for communicating with Schwab’s `/oauth/token` endpoint.
*   **Consumption Layer (Passive/Read-Only)**: All application logic (Scans, Sensors, Dashboards) that uses tokens but is forbidden from updating them.

### 2.2 The "Single-Writer" Rule
Only one designated maintenance utility (`auth_schwab_minimal.py`) is permitted to have write access to the `tokens.json` file. All other processes must treat the token file as a static, read-only asset.

### 2.3 Fail-Fast over Self-Healing
The system must never attempt to "fix" an expired or invalid token. If authentication fails, the process must terminate immediately and loudly (Scan Engine) or degrade gracefully (Management Engine).

---

## 3. Operational Rules

### 3.1 Forbidden Behaviors
The following behaviors are strictly prohibited and must be removed from all codebase branches:
*   **No Background Refresh**: No automated process may trigger a token refresh.
*   **No Retry-on-401**: A `401 Unauthorized` response is a hard failure. Automatic retries are forbidden.
*   **No Library-Level Auth**: Data-fetching clients must not contain logic to reach the OAuth servers. `schwab.auth.easy_client` is forbidden in runtime code.
*   **No Hardcoded Credentials**: App keys and secrets must never be stored in source code or shared with the Consumption Layer.

### 3.2 Trading-Window-Only Authorization
OAuth authorization and token refreshes may **only** occur during an explicit "Trading Window" under direct operator supervision (e.g., pre-market preparation). Outside of this window, the system is effectively air-gapped from Schwab's authentication infrastructure.

---

## 4. Technical Governance

### 4.1 Token Validation Safety Buffer
Passive consumers must apply a **120-second safety buffer** when checking token expiry. If a token has less than 2 minutes of remaining life, it must be treated as already expired to prevent mid-process rejections.

### 4.2 The Kill Switch (`SCHWAB_STRICT_MODE`)
The environment variable `SCHWAB_STRICT_MODE=1` must be enabled for all production and background tasks. This flag programmatically disables all code paths capable of initiating network calls to the Schwab Auth servers.

### 4.3 Credential Isolation
The `SCHWAB_APP_SECRET` must only be available in the environment where the manual Maintenance Script is executed. It must be absent from the environment of background scanners and sensors to prevent accidental refresh triggers.

---

## 5. Compliance
Future contributors must ensure that any new module interacting with the Schwab API adheres to the **Passive-Consumption** model. Any PR introducing "seamless" background authentication or automated retry logic will be rejected as a violation of operational safety.
