# Developer Documentation

## 🔐 Schwab API Authentication (Passive-Consumption Model)

The platform follows a strict **Passive-Consumption Model** for authentication to prevent account-level blocks.

### 1. Authoritative Auth Entry Point
The **ONLY** authorized way to generate or refresh tokens is via the manual bootstrap script:

```bash
export SCHWAB_API_KEY='your_key'
export SCHWAB_APP_SECRET='your_secret'
python3 auth_schwab_minimal.py
```

### 2. Runtime Behavior
*   **Read-Only**: All runtime modules (Scan Engine, Drift Engine, UI) are strictly read-only. They consume `~/.schwab/tokens.json` but cannot refresh it.
*   **Fail-Fast**: If a token is expired, the system will not attempt to "self-heal." It will stop and provide instructions for manual re-auth.
*   **Separation**: `schwab.auth` and `easy_client` are forbidden in runtime code.

---

## 🛠️ Safe Development Workflow (Hardened)

To prevent stale code execution and module shadowing during development, follow these rules.

### 1. Deterministic Dev Reset
Always use the canonical reset script when making changes to core logic or data contracts:

```bash
./reset_dev.sh
```

This script performs the following safety operations:
1.  Kills existing Streamlit processes.
2.  Clears all `__pycache__` and `.pyc` files.
3.  Clears Streamlit internal caches.
4.  Restarts the app with `DEV_MODE=1` and `PYTHONDONTWRITEBYTECODE=1`.

### 2. Environment Hardening
*   **Disable Bytecode**: Set `export PYTHONDONTWRITEBYTECODE=1` in your shell to prevent stale `.pyc` files from being written.
*   **Path Priority**: The system automatically injects the current working directory at the front of `sys.path` to prevent shadowing from `site-packages`.
*   **Uninstall Editable Installs**: If you previously ran `pip install -e .`, run `pip uninstall core -y` to prevent import bleed-through.

### 3. Dev-Mode Diagnostics
When running with `DEV_MODE=1`, a diagnostic panel is available in the Streamlit sidebar. Use "Show Module Paths" to verify that the interpreter is loading files from your working directory and not a shadowed path.

---

## 📂 Project Structure & Governance
*   **`auth_schwab_minimal.py`**: Infrastructure-only bootstrap.
*   **`core/shared/auth/schwab_tokens.py`**: Centralized token loader.
*   **`docs/SCHWAB_AUTH_POLICY.md`**: Authoritative governance policy.
