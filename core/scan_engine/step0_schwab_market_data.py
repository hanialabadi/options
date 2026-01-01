import os
import requests
import logging
import json
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

SCHWAB_API_BASE_URL = "https://api.schwabapi.com"
QUOTES_ENDPOINT = "/marketdata/v1/quotes"
PRICE_HISTORY_ENDPOINT = "/marketdata/v1/pricehistory" # Stub, not fully implemented yet

def _get_schwab_access_token():
    """Reads SCHWAB_ACCESS_TOKEN from environment variables."""
    token = os.getenv("SCHWAB_ACCESS_TOKEN")
    if not token:
        logging.error("SCHWAB_ACCESS_TOKEN environment variable not set.")
        raise ValueError("SCHWAB_ACCESS_TOKEN not found.")
    return token

def fetch_schwab_quotes(symbols: list) -> dict:
    """
    Fetches raw market data quotes from Schwab API for a list of symbols.

    Args:
        symbols: A list of stock symbols (e.g., ["AAPL", "MSFT"]).

    Returns:
        A dictionary containing the raw JSON response from the Schwab API.
    """
    access_token = _get_schwab_access_token()
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }
    
    # Log headers used (excluding token value)
    logged_headers = {k: v for k, v in headers.items() if k != "Authorization"}
    logging.info(f"Headers used (excluding token): {logged_headers}")

    params = {
        "symbols": ",".join(symbols),
        "fields": "quote" # Requesting quote fields
    }

    api_url = f"{SCHWAB_API_BASE_URL}{QUOTES_ENDPOINT}"
    logging.info(f"Resolved API endpoint: {api_url}")
    logging.info(f"Symbols requested: {symbols}")
    logging.info(f"API endpoint used: {QUOTES_ENDPOINT}")

    try:
        response = requests.get(api_url, headers=headers, params=params)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
        
        logging.info(f"HTTP status: {response.status_code}")
        
        raw_data = response.json()
        
        # Log keys present in response for each symbol
        for symbol in symbols:
            if symbol in raw_data:
                logging.info(f"Keys present in response for {symbol}: {list(raw_data[symbol].keys())}")
            else:
                logging.warning(f"No data found for symbol: {symbol}")

        return raw_data
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err} - Response: {response.text}")
        raise
    except requests.exceptions.ConnectionError as conn_err:
        logging.error(f"Connection error occurred: {conn_err}")
        raise
    except requests.exceptions.Timeout as timeout_err:
        logging.error(f"Timeout error occurred: {timeout_err}")
        raise
    except requests.exceptions.RequestException as req_err:
        logging.error(f"An unexpected error occurred: {req_err}")
        raise

def fetch_schwab_price_history(symbol: str, period_type: str = "day", period: int = 1, frequency_type: str = "minute", frequency: int = 1) -> dict:
    """
    (Stub) Fetches raw price history data from Schwab API for a single symbol.
    This is a placeholder and not fully implemented as per task instructions.

    Args:
        symbol: The stock symbol (e.g., "AAPL").
        period_type: Type of period (e.g., "day", "month", "year", "ytd").
        period: Number of periods to show.
        frequency_type: Type of frequency (e.g., "minute", "daily", "weekly", "monthly").
        frequency: Number of frequencies to show.

    Returns:
        A dictionary containing the raw JSON response from the Schwab API.
    """
    logging.info(f"Price history fetch for {symbol} is a stub and not fully implemented.")
    logging.info(f"API endpoint used (stub): {PRICE_HISTORY_ENDPOINT}")
    return {"message": "Price history functionality is a stub and not implemented."}

if __name__ == "__main__":
    # Example usage (for testing purposes, requires SCHWAB_ACCESS_TOKEN to be set)
    # This block will not run when imported as a module.
    try:
        # Set a dummy token for local testing if not already set
        if "SCHWAB_ACCESS_TOKEN" not in os.environ:
            os.environ["SCHWAB_ACCESS_TOKEN"] = "YOUR_DUMMY_ACCESS_TOKEN" # Replace with a real token for actual testing

        test_symbols = ["AAPL", "MSFT", "SPY"]
        quotes_data = fetch_schwab_quotes(test_symbols)
        print("\n--- Raw Quotes Data ---")
        print(json.dumps(quotes_data, indent=2))

        # Example of converting to DataFrame (if needed, but task asks for raw JSON/flat DataFrame)
        # if quotes_data:
        #     df_list = []
        #     for symbol, data in quotes_data.items():
        #         if 'quote' in data:
        #             quote_df = pd.json_normalize(data['quote'])
        #             quote_df['symbol'] = symbol
        #             df_list.append(quote_df)
        #     if df_list:
        #         flat_df = pd.concat(df_list, ignore_index=True)
        #         print("\n--- Flat DataFrame Sample ---")
        #         print(flat_df.head())

        price_history_stub = fetch_schwab_price_history("AAPL")
        print("\n--- Price History Stub ---")
        print(json.dumps(price_history_stub, indent=2))

    except ValueError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during example usage: {e}")
