import sys
import os
import json
import time
import math
from datetime import datetime, timedelta
from pathlib import Path
from loguru import logger

# Add the project root to the Python path to resolve module imports
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from core.scan_engine.schwab_api_client import SchwabClient

# --- Configuration Constants ---
# Read credentials only from environment variables
CLIENT_ID = os.getenv("SCHWAB_CLIENT_ID")
CLIENT_SECRET = os.getenv("SCHWAB_CLIENT_SECRET")
TICKER = "AAPL"

def get_schwab_client():
    """Initializes and returns a SchwabClient instance."""
    if not CLIENT_ID or not CLIENT_SECRET:
        raise RuntimeError("Missing Schwab API credentials. Set SCHWAB_CLIENT_ID and SCHWAB_CLIENT_SECRET in your environment.")

    client = SchwabClient(CLIENT_ID, CLIENT_SECRET)
    try:
        # Attempt to load existing tokens or authenticate if needed
        if not client._tokens:
            logger.info("No existing tokens found. Starting initial authentication flow.")
            client.authenticate_and_get_tokens()
        else:
            logger.info("Existing tokens found. Checking validity...")
            # The _get_access_token method will handle refreshing if expired
            client._get_access_token() 
            logger.info("Tokens are valid or successfully refreshed.")
    except Exception as e:
        logger.error(f"Failed to initialize SchwabClient or authenticate: {e}")
        raise
    return client

def calculate_historical_volatility(prices: list[float], periods: int) -> float:
    """
    Calculates annualized historical volatility using log returns.
    Assumes daily prices.
    """
    if len(prices) < periods + 1:
        return float('nan') # Not enough data

    log_returns = []
    for i in range(1, len(prices)):
        if prices[i-1] > 0: # Avoid division by zero
            log_returns.append(math.log(prices[i] / prices[i-1]))
        else:
            log_returns.append(0) # Or handle as appropriate for zero/negative prices

    # Take the last 'periods' returns
    relevant_returns = log_returns[-periods:]
    if not relevant_returns:
        return float('nan')

    # Calculate standard deviation of log returns
    # Using population standard deviation for consistency with typical HV calculations
    if len(relevant_returns) > 1:
        mean_return = sum(relevant_returns) / len(relevant_returns)
        variance = sum([(x - mean_return) ** 2 for x in relevant_returns]) / len(relevant_returns)
        std_dev = math.sqrt(variance)
    elif len(relevant_returns) == 1:
        std_dev = 0.0 # Single return has no volatility
    else:
        return float('nan')
    
    # Annualize (assuming 252 trading days in a year)
    annualized_volatility = std_dev * math.sqrt(252)
    return annualized_volatility

def find_atm_iv_for_dte(target_dte: int, spot: float, sorted_exps: list) -> tuple[float | None, float | None, float | None]:
    """
    Helper to find ATM IV for a given DTE bucket.
    Returns (call_iv, put_iv, atm_iv_avg)
    """
    closest_exp_data = None
    min_dte_diff = float('inf')

    # Find the expiration closest to the target DTE
    for dte, exp_data in sorted_exps:
        diff = abs(dte - target_dte)
        if diff < min_dte_diff:
            min_dte_diff = diff
            closest_exp_data = exp_data
    
    if not closest_exp_data:
        return None, None, None

    closest_strike = None
    min_strike_diff = float('inf')
    
    # Find the strike closest to the spot price for this expiration
    for strike_price, contracts in closest_exp_data['strikes'].items():
        diff = abs(strike_price - spot)
        if diff < min_strike_diff:
            min_strike_diff = diff
            closest_strike = strike_price
    
    if closest_strike:
        atm_contracts = closest_exp_data['strikes'][closest_strike]
        # Schwab API returns volatility as a percentage, convert to decimal
        call_iv = (atm_contracts['call']['volatility'] / 100) if atm_contracts['call'] and 'volatility' in atm_contracts['call'] else None
        put_iv = (atm_contracts['put']['volatility'] / 100) if atm_contracts['put'] and 'volatility' in atm_contracts['put'] else None

        if call_iv is not None and put_iv is not None:
            return call_iv, put_iv, (call_iv + put_iv) / 2
    return None, None, None

def main():
    logger.remove() # Remove default logger to control output strictly
    logger.add(sys.stderr, format="{message}", level="INFO") # Add a simple logger for info/error messages

    print(f"{TICKER} Schwab Volatility Validation")
    print("--------------------------------")

    client = get_schwab_client()

    # 1. Spot Price
    spot_price = None
    try:
        logger.info(f"Fetching spot price for {TICKER}...")
        quotes_response = client.get_quotes(symbols=[TICKER])
        if TICKER in quotes_response and 'quote' in quotes_response[TICKER]:
            spot_price = quotes_response[TICKER]['quote']['lastPrice']
            print(f"Spot Price: {spot_price:.2f}")
        else:
            logger.error(f"Could not find spot price for {TICKER} in quotes response.")
            print("Spot Price: N/A")
    except Exception as e:
        logger.error(f"Failed to get spot price: {e}")
        print("Spot Price: N/A")

    if spot_price is None:
        logger.error("Cannot proceed without spot price.")
        return

    # 2. ATM Implied Volatility & 3. IV Term Structure & 4. Call/Put Skew
    print("\nATM IV (≈30–45 DTE):")
    atm_iv_30_45_dte = None
    atm_call_iv = None
    atm_put_iv = None
    skew = None
    iv_term_structure = {}

    try:
        logger.info(f"Fetching option chains for {TICKER}...")
        # Schwab chains API requires strikeCount and range, or specific strike/expiration
        # We'll fetch a broad range and filter. Schwab API doesn't directly support DTE filtering,
        # so we'll get all expirations and filter locally.
        chains_response = client.get_chains(
            symbol=TICKER,
            strikeCount=20, # Fetch a reasonable number of strikes around ATM
            range="ALL", # Get all options (ITM, OTM, ATM) to find closest strike
            strategy="SINGLE", # Single leg options
        )

        expirations = {} # {DTE: {strikes: {strike_price: {call: contract, put: contract}}}}
        
        # Process call options
        if 'callExpDateMap' in chains_response:
            for exp_date_str, strikes_data in chains_response['callExpDateMap'].items():
                exp_date_part, dte_str = exp_date_str.split(':')
                dte = int(dte_str)
                if dte not in expirations:
                    expirations[dte] = {'strikes': {}}
                for strike_price_str, contracts in strikes_data.items():
                    strike_price = float(strike_price_str)
                    if strike_price not in expirations[dte]['strikes']:
                        expirations[dte]['strikes'][strike_price] = {'call': None, 'put': None}
                    if contracts: # Ensure contract exists
                        expirations[dte]['strikes'][strike_price]['call'] = contracts[0] # Assuming one contract per strike

        # Process put options
        if 'putExpDateMap' in chains_response:
            for exp_date_str, strikes_data in chains_response['putExpDateMap'].items():
                exp_date_part, dte_str = exp_date_str.split(':')
                dte = int(dte_str)
                if dte not in expirations:
                    expirations[dte] = {'strikes': {}}
                for strike_price_str, contracts in strikes_data.items():
                    strike_price = float(strike_price_str)
                    if strike_price not in expirations[dte]['strikes']:
                        expirations[dte]['strikes'][strike_price] = {'call': None, 'put': None}
                    if contracts: # Ensure contract exists
                        expirations[dte]['strikes'][strike_price]['put'] = contracts[0] # Assuming one contract per strike
        
        # Sort expirations by DTE
        sorted_expirations = sorted(expirations.items())

        # 2. ATM Implied Volatility (30-45 DTE)
        # Find the expiration closest to 30-45 DTE range (target 30 DTE)
        atm_call_iv, atm_put_iv, atm_iv_30_45_dte = find_atm_iv_for_dte(30, spot_price, sorted_expirations)
        
        if atm_call_iv is not None and atm_put_iv is not None:
            skew = atm_put_iv - atm_call_iv
            print(f"  Call IV: {atm_call_iv:.3f}")
            print(f"  Put IV : {atm_put_iv:.3f}")
            print(f"  ATM IV : {atm_iv_30_45_dte:.3f}")
            print(f"  Skew   : {skew:+.3f}")
        else:
            print("  Call IV: N/A")
            print("  Put IV : N/A")
            print("  ATM IV : N/A")
            print("  Skew   : N/A")

        # 3. IV Term Structure
        dte_buckets = {
            7: range(0, 11),    # <=10
            14: range(11, 21),  # 11-20
            30: range(21, 41),  # 21-40
            60: range(41, 81),  # 41-80
            90: range(81, 121), # 81-120
            180: range(121, 221) # 121-220
        }
        
        for bucket_key, dte_range in dte_buckets.items():
            closest_dte_in_range = None
            min_diff_to_bucket_center = float('inf')
            
            # Find the expiration within the bucket range that is closest to the bucket's center DTE
            bucket_center_dte = (dte_range.start + dte_range.stop -1) // 2 # Approximate center
            
            for dte, _ in sorted_expirations:
                if dte in dte_range:
                    diff = abs(dte - bucket_center_dte)
                    if diff < min_diff_to_bucket_center:
                        min_diff_to_bucket_center = diff
                        closest_dte_in_range = dte
            
            if closest_dte_in_range is not None:
                _, _, iv = find_atm_iv_for_dte(closest_dte_in_range, spot_price, sorted_expirations)
                iv_term_structure[bucket_key] = iv if iv is not None else float('nan')
            else:
                iv_term_structure[bucket_key] = float('nan')

        print("\nIV Term Structure:")
        for dte, iv in iv_term_structure.items():
            print(f"  {dte}D   : {iv:.2f}" if not math.isnan(iv) else f"  {dte}D   : N/A")

    except Exception as e:
        logger.error(f"Failed to get option chain data or calculate IV: {e}")
        print("ATM IV (≈30–45 DTE): N/A")
        print("  Call IV: N/A")
        print("  Put IV : N/A")
        print("  ATM IV : N/A")
        print("  Skew   : N/A")
        print("\nIV Term Structure:")
        for dte in dte_buckets.keys():
            print(f"  {dte}D   : N/A")


    # 5. Historical Volatility (HV)
    print("\nHistorical Volatility:")
    hv_periods = [10, 20, 30, 60]
    hv_values = {}

    try:
        logger.info(f"Fetching price history for {TICKER}...")
        # Schwab API /pricehistory requires a periodType, period, frequencyType, frequency
        # To get enough data for 60D HV (approx 120 trading days), we use periodType="day", period=6.
        # period=6 gives roughly 120 daily candles.
        price_history_response = client.get_price_history(
            symbol=TICKER,
            periodType="day",
            period=6, # Using period=6 to get enough data for 60D HV (approx 120 trading days)
            frequencyType="daily",
            frequency=1
        )

        if price_history_response and 'candles' in price_history_response:
            # Extract closing prices, ensuring they are sorted chronologically for HV calculation
            prices = sorted([candle['close'] for candle in price_history_response['candles']], reverse=False)
            
            for period in hv_periods:
                hv_values[period] = calculate_historical_volatility(prices, period)
        else:
            logger.warning("No price history data found.")
            for period in hv_periods:
                hv_values[period] = float('nan')

        for period, hv in hv_values.items():
            print(f"  HV_{period}D : {hv:.2f}" if not math.isnan(hv) else f"  HV_{period}D : N/A")

    except Exception as e:
        logger.error(f"Failed to get price history or calculate HV: {e}")
        for period in hv_periods:
            print(f"  HV_{period}D : N/A")

    # 6. IV-HV Gap
    print("\nIV–HV Gap (30D):")
    iv_30d = iv_term_structure.get(30)
    hv_30d = hv_values.get(30)

    if iv_30d is not None and not math.isnan(iv_30d) and hv_30d is not None and not math.isnan(hv_30d):
        iv_hv_gap = iv_30d - hv_30d
        print(f"  {iv_hv_gap:+.2f}")
    else:
        print("  N/A")

if __name__ == "__main__":
    main()
