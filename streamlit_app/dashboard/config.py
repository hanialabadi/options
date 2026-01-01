# config.py
# DEPRECATED: Use core.data_contracts instead

from core.data_contracts.config import ACTIVE_MASTER_PATH as MASTER_PATH
from core.data_contracts.config import SNAPSHOT_DIR

# === Legacy paths (to be migrated) ===
TAENV_PYTHON = "/Users/haniabadi/Documents/Github/options/taenv/bin/python"

# === Snapshot Naming ===
SNAPSHOT_PREFIX = "positions_"
SNAPSHOT_EXTENSION = ".csv"
