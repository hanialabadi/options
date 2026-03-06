
import argparse
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd

# Add repo root to path to find 'core' module
from core.shared.data_contracts.config import PROJECT_ROOT
REPO_ROOT = PROJECT_ROOT
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.shared.scraper.utils import create_browser, scrape_ivhv, save_result, load_tickers
from core.shared.scraper.filters import get_remaining_tickers
from core.shared.data_layer.ivhv_timeseries_loader import load_and_normalize_archive
from core.shared.data_layer.ivhv_derived_analytics import compute_derived_analytics
from core.shared.data_layer.duckdb_utils import (
    initialize_fidelity_iv_long_term_history_table,
    get_duckdb_write_connection,
    insert_fidelity_iv_long_term_history,
)

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

    # Initialize Fidelity DuckDB table and connection once for this run
    initialize_fidelity_iv_long_term_history_table()
    db_con = get_duckdb_write_connection()

    for i, t in enumerate(tickers, 1):
        print(f"[{i}/{len(tickers)}] Scraping {t}...")
        try:
            result = scrape_ivhv(t, browser)
            if result.get("Error") is None:
                save_result(result)
                # Persist to DuckDB (pipeline.duckdb) for scan engine visibility
                result_for_db = dict(result)
                if "timestamp" not in result_for_db:
                    if "scan_timestamp" in result_for_db:
                        result_for_db["timestamp"] = result_for_db["scan_timestamp"]
                insert_fidelity_iv_long_term_history(pd.DataFrame([result_for_db]), db_con)
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

    try:
        db_con.close()
    except Exception:
        pass

    # OPERATIONAL FIX: Trigger Analytics Pipeline
    # This ensures that scraped data is persisted to the canonical store and metrics are recomputed.
    print("\n" + "="*60)
    print("[PIPELINE] Triggering Data Maturity Pipeline...")
    print("="*60)
    
    try:
        archive_dir = REPO_ROOT / "data" / "ivhv_archive"
        ts_dir = REPO_ROOT / "data" / "ivhv_timeseries"
        canonical_file = ts_dir / "ivhv_timeseries_canonical.csv"
        derived_file = ts_dir / "ivhv_timeseries_derived.csv"
        
        # 1. Normalize Archive -> Canonical
        load_and_normalize_archive(archive_dir, ts_dir)
        
        # 2. Compute Derived Analytics (IV Rank, etc.)
        compute_derived_analytics(canonical_file, derived_file)
        
        print("\n[✅] Data Maturity Pipeline complete. IV history updated.")
    except Exception as e:
        print(f"[❌] Pipeline trigger failed: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=str, help="Override CSV file path")
    parser.add_argument("--resume", action="store_true", help="Skip already-scraped tickers for today")
    parser.add_argument("--no-prompt", action="store_true", help="Skip manual login prompt (use persistent profile)")
    args = parser.parse_args()

    run_scraper(file_override=args.file, resume=args.resume, no_prompt=args.no_prompt)
