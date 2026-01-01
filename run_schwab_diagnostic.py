import argparse
import json
import logging
from core.scan_engine.step0_schwab_market_data import fetch_schwab_quotes, fetch_schwab_price_history

# Configure logging for the diagnostic script
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def analyze_quote_response(symbol: str, data: dict):
    """Analyzes and prints details for a single symbol's quote response."""
    print(f"\n--- Analysis for {symbol} ---")

    if not data:
        print(f"No data received for {symbol}.")
        return

    # Per-symbol response structure: assetMainType
    asset_main_type = data.get('assetMainType', 'N/A')
    print(f"assetMainType: {asset_main_type}")

    # Per-symbol response structure: quote keys
    quote_data = data.get('quote', {})
    quote_keys = list(quote_data.keys())
    print(f"Quote keys: {quote_keys}")

    # Volatility presence (example fields, adjust based on actual Schwab response)
    volatility_fields = ["volatility", "impliedVolatility", "historicalVolatility"]
    present_volatility_fields = [k for k in quote_keys if k in volatility_fields]
    print(f"Volatility fields present: {present_volatility_fields if present_volatility_fields else 'None detected'}")

    # Timestamp fields detected (example fields)
    timestamp_fields = ["quoteTimeInLong", "tradeTimeInLong", "expirationDate"]
    present_timestamp_fields = [k for k in quote_keys if k in timestamp_fields]
    print(f"Timestamp fields detected: {present_timestamp_fields if present_timestamp_fields else 'None detected'}")

    # Sample rows (first 1-3 records) - for quotes, this means the quote data itself
    print("\nSample Quote Data (first 1-3 key-value pairs):")
    sample_count = 0
    for k, v in quote_data.items():
        if sample_count >= 3:
            break
        print(f"  {k}: {v}")
        sample_count += 1

def main():
    parser = argparse.ArgumentParser(description="Schwab Market Data Diagnostic Script")
    parser.add_argument("--symbols", type=str, required=True,
                        help="Comma-separated list of symbols (e.g., AAPL,MSFT,SPY)")
    args = parser.parse_args()

    symbols_list = [s.strip().upper() for s in args.symbols.split(',')]
    print(f"Fetching data for symbols: {symbols_list}")

    try:
        # Fetch quotes
        quotes_response = fetch_schwab_quotes(symbols_list)
        print("\n--- Raw Schwab Quotes Response ---")
        print(json.dumps(quotes_response, indent=2))

        for symbol in symbols_list:
            analyze_quote_response(symbol, quotes_response.get(symbol, {}))

        # Price history is a stub, so we'll just call it and print its message
        print("\n--- Schwab Price History (Stub) ---")
        for symbol in symbols_list:
            price_history_stub_response = fetch_schwab_price_history(symbol)
            print(f"Price history for {symbol}: {json.dumps(price_history_stub_response, indent=2)}")

    except ValueError as e:
        logging.error(f"Configuration error: {e}")
    except Exception as e:
        logging.error(f"An error occurred during diagnostic: {e}")

if __name__ == "__main__":
    main()
