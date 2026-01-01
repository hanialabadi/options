
import sys
import os
import logging
import logging.handlers
from pathlib import Path
import time
import argparse
from datetime import datetime

# Add repo root to path to find 'core' module
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.scraper.config import *
from core.scraper.utils import create_browser, scrape_ivhv, save_result, load_tickers
from core.scraper.filters import get_remaining_tickers

# --- Logger Setup ---
# Ensure LOG_DIR exists (from config.py)
LOG_DIR.mkdir(parents=True, exist_ok=True)
SCRAPER_LOG_FILE = LOG_DIR / f"scraper_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# Configure logger for both console and file output
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# File handler (rotating file handler for long runs)
file_handler = logging.handlers.RotatingFileHandler(
    SCRAPER_LOG_FILE,
    maxBytes=10 * 1024 * 1024,  # 10 MB
    backupCount=5
)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Store log file path in an environment variable for Streamlit to pick up
os.environ['SCRAPER_LOG_FILE'] = str(SCRAPER_LOG_FILE)
logger.info(f"Scraper log file: {SCRAPER_LOG_FILE}")

def run_scraper(resume=False, file_override=None, no_prompt=False):
    tickers = load_tickers(file_override)
    remaining = get_remaining_tickers(tickers) if resume else tickers
    logger.info(f"[INFO] {len(remaining)} tickers remaining")

    driver = create_browser()
    if not no_prompt:
        input("[ACTION] Log in to Fidelity, then press Enter...")
    else:
        logger.info("[INFO] Auto-mode: Using persistent Chrome profile (should already be logged in)")
        time.sleep(3)  # Give Chrome time to load profile

    failed = []
    for t in remaining:
        try:
            result = scrape_ivhv(t, driver)
            if result.get("Error") is None:
                save_result(result)
                logger.info(f"[✅] {t}")
            else:
                logger.error(f"[❌] {t} failed: {result['Error']}")
                failed.append(t)
        except Exception as e:
            logger.exception(f"[❌ {t}] Unexpected error:") # Use logger.exception to get traceback
            failed.append(t)
        time.sleep(1.1)

    driver.quit()
    if failed:
        fail_path = LOG_DIR / f"failed_{TODAY}.txt"
        with open(fail_path, "w") as f:
            f.write("\n".join(failed))
        logger.error(f"[⛔ Failures logged to]: {fail_path}")
    else:
        logger.info("[🎉] All tickers scraped successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fidelity IV/HV Scraper")
    parser.add_argument("--resume", action="store_true", help="Resume from failed tickers")
    parser.add_argument("--file", type=str, help="Override ticker CSV file path")
    parser.add_argument("--no-prompt", action="store_true", help="Skip manual login prompt (use persistent profile)")
    args = parser.parse_args()
    
    run_scraper(resume=args.resume, file_override=args.file, no_prompt=args.no_prompt)
