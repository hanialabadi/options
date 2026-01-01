import os
import sys
import math
from core.scan_engine.schwab_api_client import SchwabClient

def main():
    client_id = os.getenv("SCHWAB_APP_KEY")
    client_secret = os.getenv("SCHWAB_APP_SECRET")
    callback_url = os.getenv("SCHWAB_CALLBACK_URL", "https://127.0.0.1")

    assert client_id, "SCHWAB_APP_KEY not set"
    assert client_secret, "SCHWAB_APP_SECRET not set"

    client = SchwabClient(client_id, client_secret)
    tickers = ["AAPL", "MSFT", "NVDA"]
    try:
        quotes = client.get_quotes(tickers)
    except Exception as e:
        print(f"❌ Failed to fetch quotes: {e}")
        sys.exit(2)

    failed = False
    print("Ticker | Price | Source | Market Open")
    print("-------|-------|--------|------------")
    for t in tickers:
        q = quotes.get("quotes", {}).get(t, {})
        price = q.get("lastPrice")
        price_source = q.get("quoteSource")
        is_open = q.get("isMarketOpen")
        print(f"{t:5} | {price} | {price_source} | {is_open}")
        if price is None or (isinstance(price, float) and math.isnan(price)):
            failed = True
    if failed:
        print("❌ At least one price is NaN or missing.")
        sys.exit(1)
    print("✅ All prices fetched and valid.")

if __name__ == "__main__":
    main()
