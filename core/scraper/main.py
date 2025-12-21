
import argparse
from datetime import datetime
from core.scraper.utils import create_browser, scrape_ivhv, save_result, load_tickers
from core.scraper.filters import get_remaining_tickers

def run_scraper(file_override=None, resume=False):
    tickers = load_tickers(file_override)
    if resume:
        tickers = get_remaining_tickers(tickers)

    print(f"[START] Scraping {len(tickers)} tickers...")

    browser = create_browser()
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
    args = parser.parse_args()

    run_scraper(file_override=args.file, resume=args.resume)
