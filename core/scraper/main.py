
import argparse
import sys
from pathlib import Path
from datetime import datetime

# Add repo root to path to find 'core' module
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.scraper.utils import create_browser, scrape_ivhv, save_result, load_tickers
from core.scraper.filters import get_remaining_tickers

def run_scraper(file_override=None, resume=False, no_prompt=False):
    tickers = load_tickers(file_override)
    if resume:
        tickers = get_remaining_tickers(tickers)

    print(f"[START] Scraping {len(tickers)} tickers...")

    browser = create_browser()
    
    if not no_prompt:
        input("[ACTION] Log in to Fidelity, then press Enter...")
    else:
        print("[INFO] Auto-mode: Using persistent Chrome profile")
        import time
        time.sleep(3)
    
    failed = []

    for i, t in enumerate(tickers, 1):
        print(f"[{i}/{len(tickers)}] Scraping {t}...")
        try:
            result = scrape_ivhv(t, browser)
            if result.get("Error") is None:
                save_result(result)
                print(f"[✅] {t}")
            else:
                print(f"[❌] {t} failed: {result['Error']}")
                failed.append(t)
        except Exception as e:
            print(f"[❌] {t} threw exception: {e}")
            failed.append(t)

    if failed:
        print(f"[⛔ Final Failures]: {failed}")
        fail_log = f"logs/failed_final_{datetime.today().date()}.txt"
        with open(fail_log, "w") as f:
            f.write("\n".join(failed))
        print(f"[📄 Fail log saved to]: {fail_log}")
    else:
        print("[🎉] All tickers scraped successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=str, help="Override CSV file path")
    parser.add_argument("--resume", action="store_true", help="Skip already-scraped tickers for today")
    parser.add_argument("--no-prompt", action="store_true", help="Skip manual login prompt (use persistent profile)")
    args = parser.parse_args()

    run_scraper(file_override=args.file, resume=args.resume, no_prompt=args.no_prompt)
