
from core.scraper.config import *
from core.scraper.utils import create_browser, scrape_ivhv, save_result, load_tickers
from core.scraper.filters import get_remaining_tickers
import time
from datetime import datetime

def run_scraper(resume=False, file_override=None):
    tickers = load_tickers(file_override)
    remaining = get_remaining_tickers(tickers) if resume else tickers
    print(f"[INFO] {len(remaining)} tickers remaining")

    driver = create_browser()
    input("[ACTION] Log in to Fidelity, then press Enter...")

    failed = []
    for t in remaining:
        try:
            result = scrape_ivhv(t, driver)
            if result.get("Error") is None:
                save_result(result)
                print(f"[✅] {t}")
            else:
                print(f"[❌] {t} failed: {result['Error']}")
                failed.append(t)
        except Exception as e:
            print(f"[❌ {t}] Unexpected error: {e}")
            failed.append(t)
        time.sleep(1.1)

    driver.quit()
    if failed:
        fail_path = LOG_DIR / f"failed_{TODAY}.txt"
        with open(fail_path, "w") as f:
            f.write("\n".join(failed))
        print(f"[⛔ Failures logged to]: {fail_path}")
    else:
        print("[🎉] All tickers scraped successfully.")
