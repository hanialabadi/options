# Source: File ID `file-JSrRkU9eit1qcRyPiWPgUN`

from datetime import datetime
from pathlib import Path

# === Base Paths ===
REPO_ROOT = Path(__file__).resolve().parents[2]  # Moved one level up from .parents[3] to standardize
TODAY = datetime.today().strftime("%Y-%m-%d")

# === File Locations ===
TICKER_CSV = REPO_ROOT / "inputs" / "tickers.csv"
DEFAULT_TICKER_CSV = TICKER_CSV

ARCHIVE_DIR = REPO_ROOT / "data" / "ivhv_archive"
ARCHIVE_OUT = ARCHIVE_DIR / f"ivhv_snapshot_{TODAY}.csv"

# No legacy file used anymore
LEGACY_OUT = None

# === Chrome Profile ===
PROFILE_DIR = REPO_ROOT / ".chrome_fidauto"

# === Logging Directory ===
LOG_DIR = REPO_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# === Browser Constants ===
WAIT_TIME = 20
PAGE_TIMEOUT = 30
VERSION_MAIN = 138
