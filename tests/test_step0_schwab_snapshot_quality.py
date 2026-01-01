#!/usr/bin/env python3
"""
CLI Test Script: Step 0 Schwab Snapshot Quality Validation

Validates that the Step 0 snapshot generation produces "not garbage" output:
1. Loads first 25 tickers from test ticker universe
2. Runs Step 0 snapshot generation
3. Prints validation table with key fields (Ticker, last_price, price_source, hv_30, etc.)
4. Prints price source coverage stats
5. Prints 2 raw JSON quote blocks (AAPL, MSFT) to verify key extraction
6. Validates snapshot quality (<30% NaN prices)

Usage:
    python tests/test_step0_schwab_snapshot_quality.py
"""

import sys
import os
import json
from pathlib import Path

# Add core module to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.scan_engine.step0_schwab_snapshot import (
    generate_live_snapshot,
    save_snapshot,
    load_ticker_universe,
    SchwabClient,
    fetch_batch_quotes,
    is_market_open_schwab
)

TICKER_FILE = Path(__file__).parent.parent / "core" / "scraper" / "tickers copy.csv"
TEST_TICKER_COUNT = 25


def print_separator(char="=", length=80):
    """Print a separator line."""
    print(char * length)


def print_section_header(title: str):
    """Print a section header."""
    print("\n")
    print_separator()
    print(f"  {title}")
    print_separator()


def print_validation_table(df):
    """Print a validation table showing key fields for inspection."""
    print("\n📋 VALIDATION TABLE (First 10 rows):")
    print_separator("-")
    
    # Select key columns for validation
    cols = ['Ticker', 'last_price', 'price_source', 'hv_30', 'is_market_open', 'quote_age_sec', 'market_status']
    
    # Print header
    header = f"{'Ticker':<8} {'Price':<10} {'Source':<18} {'HV_30':<8} {'Market':<10} {'Age(s)':<10} {'Status':<10}"
    print(header)
    print_separator("-")
    
    # Print rows (first 10)
    for i in range(min(10, len(df))):
        row = df.iloc[i]
        ticker = row['Ticker']
        price = f"${row['last_price']:.2f}" if row['last_price'] else "NaN"
        source = row['price_source']
        hv_30 = f"{row['hv_30']:.1f}%" if row['hv_30'] and not pd.isna(row['hv_30']) else "NaN"
        market = "OPEN" if row['is_market_open'] else "CLOSED"
        age = f"{row['quote_age_sec']:.0f}s" if row['quote_age_sec'] else "N/A"
        status = row['market_status']
        
        print(f"{ticker:<8} {price:<10} {source:<18} {hv_30:<8} {market:<10} {age:<10} {status:<10}")
    
    print_separator("-")
    print(f"Total: {len(df)} tickers\n")


def print_price_source_coverage(df):
    """Print price source coverage statistics."""
    print("\n📊 PRICE SOURCE COVERAGE:")
    print_separator("-")
    
    for source in ['lastPrice', 'mark', 'closePrice', 'bidAskMid', 'regularMarketLastPrice', 'none']:
        count = (df['price_source'] == source).sum()
        if count > 0:
            pct = count / len(df) * 100
            print(f"  {source:<25} {count:>4} ({pct:>5.1f}%)")
    
    print_separator("-")
    
    # NaN price count
    nan_count = df['last_price'].isna().sum()
    nan_pct = nan_count / len(df) * 100
    
    if nan_count > 0:
        print(f"\n⚠️  NaN prices: {nan_count}/{len(df)} ({nan_pct:.1f}%)")
    else:
        print(f"\n✅ All {len(df)} tickers have valid prices!")


def print_raw_quote_samples(client, tickers):
    """Fetch and print raw JSON quote blocks for 2 sample tickers."""
    print_section_header("RAW JSON QUOTE SAMPLES (Verify Key Names)")
    
    # Check if AAPL and MSFT are in the ticker list
    sample_tickers = []
    if 'AAPL' in tickers:
        sample_tickers.append('AAPL')
    if 'MSFT' in tickers:
        sample_tickers.append('MSFT')
    
    # If neither present, use first 2 tickers
    if len(sample_tickers) < 2:
        sample_tickers = tickers[:2]
    
    # Check market status first
    is_open, market_status = is_market_open_schwab(client)
    print(f"\nMarket Status: {market_status}")
    print(f"Using {'OPEN' if is_open else 'CLOSED'} fallback order\n")
    
    # Fetch quotes
    quotes = fetch_batch_quotes(client, sample_tickers, is_open)
    
    for ticker in sample_tickers:
        quote_data = quotes.get(ticker, {})
        raw_quote = quote_data.get('raw_quote', {})
        
        print(f"\n--- {ticker} ---")
        print(f"Extracted Price: ${quote_data.get('last_price', 'N/A')}")
        print(f"Price Source: {quote_data.get('price_source', 'N/A')}")
        print(f"Raw Quote Block (camelCase keys from Schwab):")
        print(json.dumps(raw_quote, indent=2))
        print()


def main():
    """Main test execution."""
    import pandas as pd
    import logging
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    print_section_header("STEP 0 SCHWAB SNAPSHOT QUALITY TEST")
    
    print(f"\n📂 Test Configuration:")
    print(f"   Ticker file: {TICKER_FILE}")
    print(f"   Test ticker count: {TEST_TICKER_COUNT}")
    
    # Check ticker file exists
    if not TICKER_FILE.exists():
        print(f"\n❌ ERROR: Ticker file not found: {TICKER_FILE}")
        sys.exit(1)
    
    # Load tickers
    print(f"\n📋 Loading tickers...")
    all_tickers = load_ticker_universe(TICKER_FILE)
    test_tickers = all_tickers[:TEST_TICKER_COUNT]
    print(f"✅ Loaded {len(test_tickers)} test tickers: {', '.join(test_tickers[:5])}...")
    
    # Initialize Schwab client
    print(f"\n🔐 Initializing Schwab client...")
    client_id = os.getenv('SCHWAB_CLIENT_ID', 'placeholder')
    client_secret = os.getenv('SCHWAB_CLIENT_SECRET', 'placeholder')
    client = SchwabClient(client_id, client_secret)
    
    if not client._tokens:
        print("\n❌ ERROR: No Schwab tokens found. Please authenticate first.")
        sys.exit(1)
    
    print("✅ Client initialized (using existing tokens)")
    
    # Print raw quote samples BEFORE snapshot generation
    print_raw_quote_samples(client, test_tickers)
    
    # Generate snapshot
    print_section_header("GENERATING SNAPSHOT")
    
    try:
        df = generate_live_snapshot(
            client,
            test_tickers,
            use_cache=True,
            fetch_iv=False  # Skip IV for faster testing
        )
    except ValueError as e:
        # Catch validation error (>30% NaN prices)
        print(f"\n❌ SNAPSHOT VALIDATION FAILED:")
        print(str(e))
        sys.exit(1)
    
    # Print validation table
    print_validation_table(df)
    
    # Print price source coverage
    print_price_source_coverage(df)
    
    # Save snapshot
    print_section_header("SAVING SNAPSHOT")
    output_path = save_snapshot(df)
    print(f"✅ Snapshot saved to: {output_path}")
    print(f"   File size: {output_path.stat().st_size / 1024:.1f} KB")
    
    # Final verdict
    print_section_header("TEST VERDICT")
    
    nan_count = df['last_price'].isna().sum()
    nan_pct = nan_count / len(df) * 100
    
    if nan_pct > 30:
        print(f"❌ FAIL: {nan_pct:.1f}% NaN prices (threshold: 30%)")
        sys.exit(1)
    elif nan_count > 0:
        print(f"⚠️  PASS (with warnings): {nan_pct:.1f}% NaN prices (below threshold)")
    else:
        print(f"✅ PASS: All tickers have valid prices!")
    
    print(f"\n🎉 Step 0 quality test complete!")
    print_separator()


if __name__ == "__main__":
    main()
