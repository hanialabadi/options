"""
Data Contracts - Centralized Path Configuration

ALL file paths for active trades, snapshots, and archives live here.
NO hardcoded paths anywhere else in the codebase.

Environment-agnostic: Uses relative paths from project root by default,
but can be overridden via environment variables.
"""

import os
from pathlib import Path

# === Project Root ===
PROJECT_ROOT = Path(__file__).parent.parent.parent  # options/

# === Environment Variable Overrides ===
def get_env_path(env_var: str, default: Path) -> Path:
    """Get path from environment variable or use default."""
    env_path = os.getenv(env_var)
    if env_path:
        return Path(env_path)
    return default

# === Data Directory Structure ===
DATA_DIR = get_env_path("OPTIONS_DATA_DIR", PROJECT_ROOT / "data")
LEGACY_DATA_DIR = Path("/Users/haniabadi/Documents/Windows/Optionrec")  # Legacy location

# === Active Trade Data ===
# Primary source of truth for all active positions
ACTIVE_MASTER_PATH = get_env_path(
    "ACTIVE_MASTER_PATH",
    LEGACY_DATA_DIR / "active_master.csv"  # Keep legacy path during migration
)

# === Snapshot Storage ===
# Timestamped position snapshots for drift tracking
SNAPSHOT_DIR = get_env_path(
    "SNAPSHOT_DIR",
    LEGACY_DATA_DIR / "drift"
)

# === Archive Storage ===
# Closed/completed trades
ARCHIVE_DIR = get_env_path(
    "ARCHIVE_DIR",
    LEGACY_DATA_DIR / "archive"
)

# === Logs ===
LOGS_DIR = get_env_path(
    "LOGS_DIR",
    PROJECT_ROOT / "logs"
)

# === Snapshot Naming Convention ===
SNAPSHOT_PATTERN = "positions_%Y-%m-%d_%H-%M-%S.csv"

# === Ensure Directories Exist ===
def ensure_data_directories():
    """Create data directories if they don't exist."""
    for directory in [DATA_DIR, SNAPSHOT_DIR, ARCHIVE_DIR, LOGS_DIR]:
        directory.mkdir(parents=True, exist_ok=True)

# === Validation ===
def validate_paths():
    """Validate that critical paths are accessible."""
    if not ACTIVE_MASTER_PATH.parent.exists():
        raise ValueError(f"Active master directory does not exist: {ACTIVE_MASTER_PATH.parent}")
    if not SNAPSHOT_DIR.exists():
        SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    return True

# Auto-validate on import
try:
    validate_paths()
except Exception as e:
    import warnings
    warnings.warn(f"Path validation warning: {e}", UserWarning)
