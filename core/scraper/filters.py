
import pandas as pd
from core.scraper.config import ARCHIVE_OUT, TODAY

def get_remaining_tickers(all_tickers):
    if not ARCHIVE_OUT.exists():
        return all_tickers

    try:
        archived = pd.read_csv(ARCHIVE_OUT)
    except Exception as e:
        print(f"[WARN] Could not read archive: {e}")
        return all_tickers

    # Filter out known failures if 'Error' column exists
    if "Error" in archived.columns:
        archived = archived[~archived["Error"].fillna("").str.contains("Page Load|No Data", na=False)]

    # Protect against missing timestamp
    if "timestamp" not in archived.columns:
        print("[WARN] Missing 'timestamp' column in archive. Skipping filtering by date.")
        return all_tickers

    archived_today = archived[archived["timestamp"].astype(str).str.startswith(str(TODAY))]
    scraped_today = set(archived_today["Ticker"].dropna().unique())
    remaining = [t for t in all_tickers if t not in scraped_today]

    print(f"[INFO] Skipping {len(scraped_today)} already-scraped tickers for {TODAY}")
    return remaining
