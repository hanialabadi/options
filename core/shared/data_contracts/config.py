"""
Data Contracts - Centralized Path Configuration

ALL file paths for active trades, snapshots, and archives live here.
NO hardcoded paths anywhere else in the codebase.

Environment-agnostic: Uses relative paths from project root by default,
but can be overridden via environment variables.
"""

import os
from pathlib import Path

# === Management Safe Mode ===
# When True, disables Scan-only logic, IV maturity gating, and discovery noise.
MANAGEMENT_SAFE_MODE = os.getenv("MANAGEMENT_SAFE_MODE", "True").lower() == "true"

# === Project Root ===
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent  # options/

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

# === Ticker Universe ===
TICKER_UNIVERSE_PATH = PROJECT_ROOT / "core" / "shared" / "scraper" / "tickers.csv"

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

# === Scan Engine Paths ===
SCAN_SNAPSHOT_DIR = PROJECT_ROOT / "data" / "snapshots"
SCAN_OUTPUT_DIR = PROJECT_ROOT / "output"
OUTPUT_DIR = SCAN_OUTPUT_DIR  # Canonical alias for pipeline exports
PRICE_CACHE_DIR = PROJECT_ROOT / "data" / "cache" / "price_history"

# === DuckDB Ledger (Authoritative) ===
# Cycle-1, Cycle-2, and UI must all use this path.
PIPELINE_DB_PATH = PROJECT_ROOT / "data" / "pipeline.duckdb"
# Debug-mode isolated DuckDB (mini-production)
DEBUG_PIPELINE_DB_PATH = PROJECT_ROOT / "data" / "pipeline_debug.duckdb"

# === Domain-Split DuckDB Databases ===
# Each engine writes to its own DB to eliminate single-writer lock contention.
# Cross-domain reads use DuckDB ATTACH (read-only).
SCAN_DB_PATH = get_env_path(
    "SCAN_DB_PATH",
    DATA_DIR / "scan.duckdb"
)
MANAGEMENT_DB_PATH = get_env_path(
    "MANAGEMENT_DB_PATH",
    DATA_DIR / "management.duckdb"
)
CHART_DB_PATH = get_env_path(
    "CHART_DB_PATH",
    DATA_DIR / "chart.duckdb"
)
WAIT_DB_PATH = get_env_path(
    "WAIT_DB_PATH",
    DATA_DIR / "wait.duckdb"
)

# === Known ETFs (excluded from earnings queries) ===
KNOWN_ETFS = frozenset({
    'SPY', 'QQQ', 'IWM', 'DIA', 'TLT', 'GLD', 'SLV', 'GDX',
    'XLE', 'XLF', 'XLI', 'XLK', 'XLU', 'XLP', 'XLV', 'XLY',
    'XLB', 'XLC', 'XRE', 'SMH', 'TECL',
})

# === Specialized DuckDB Ledgers ===
# Historical IV time-series (ML/analysis use case)
IV_HISTORY_DB_PATH = get_env_path(
    "IV_HISTORY_DB_PATH",
    DATA_DIR / "iv_history.duckdb"
)

# Market-wide context (VIX, VVIX, term structure, breadth, credit proxy)
MARKET_DB_PATH = get_env_path(
    "MARKET_DB_PATH",
    DATA_DIR / "market.duckdb"
)

# Post-trade position tracking (Cycle 1/2/3 management engine)
# Contains both clean_legs (OLAP) and clean_legs_v2 (OLTP)
POSITIONS_HISTORY_DB_PATH = get_env_path(
    "POSITIONS_HISTORY_DB_PATH",
    PROJECT_ROOT / "output" / "positions_history.duckdb"
)

# Live Greeks monitoring (telemetry/sensors)
SENSORS_DB_PATH = get_env_path(
    "SENSORS_DB_PATH",
    PROJECT_ROOT / "output" / "sensors.duckdb"
)

# === Snapshot Naming Convention ===
SNAPSHOT_PATTERN = "positions_%Y-%m-%d_%H-%M-%S.csv"

# === Persona Definitions ===
PERSONA_CONSERVATIVE = {
    "name": "conservative",
    "max_loss_pct": -20,
    "max_gain_pct": 30,
    "scaling_factor": 1.0,
    "pcs_threshold": 75,
    "max_net_delta": 0.5,
    "max_short_vega": -500
}

PERSONA_AGGRESSIVE = {
    "name": "aggressive",
    "max_loss_pct": -30,
    "max_gain_pct": 50,
    "scaling_factor": 1.5,
    "pcs_threshold": 65,
    "max_net_delta": 1.0,
    "max_short_vega": -1000
}

PERSONA_REGISTRY = {
    "conservative": PERSONA_CONSERVATIVE,
    "aggressive": PERSONA_AGGRESSIVE,
    "Standard": PERSONA_CONSERVATIVE,
    "McMillan (Action)": PERSONA_CONSERVATIVE,
    "Passarelli (Sensitivity)": PERSONA_CONSERVATIVE,
    "Hull (Economic)": PERSONA_CONSERVATIVE,
    "Natenberg (Volatility)": PERSONA_CONSERVATIVE
}

# === Ensure Directories Exist ===
def ensure_data_directories():
    """Create data directories if they don't exist."""
    for directory in [DATA_DIR, SNAPSHOT_DIR, ARCHIVE_DIR, LOGS_DIR, SCAN_SNAPSHOT_DIR, SCAN_OUTPUT_DIR, PRICE_CACHE_DIR]:
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
