#!/usr/bin/env python3
"""
Schwab CLI Pipeline Validation - Data Quality Check

Purpose: Validate Step 0→6 using Schwab as primary data source
Focus: Real data output, not code design

Test Set: 10 liquid tickers (FAANG + mega-cap tech)
"""

import sys
import os
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
from datetime import datetime

# Import pipeline steps
from core.scan_engine.step0_schwab_snapshot import generate_live_snapshot, save_snapshot, SchwabClient
from core.scan_engine.step5_chart_signals import compute_chart_signals
from core.scan_engine.step6_gem_filter import validate_data_quality

# Test ticker set (controlled, liquid)
TEST_TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META",
    "COST", "NFLX", "NOW", "INTU", "KLAC"
]

def print_separator(char="=", length=80):
    print(char * length)

def print_header(title):
    print("\n")
    print_separator()
    print(f"  {title}")
    print_separator()

def validate_step0(df):
    """Validate Step 0 snapshot quality"""
    print_header("STEP 0: SCHWAB SNAPSHOT VALIDATION")
    
    print(f"\n📊 Row Count: {len(df)}")
    print(f"   Expected: {len(TEST_TICKERS)}")
    
    # NaN price check
    nan_count = df['last_price'].isna().sum()
    nan_pct = (nan_count / len(df)) * 100 if len(df) > 0 else 0
    
    print(f"\n💰 Price Quality:")
    print(f"   NaN prices: {nan_count}/{len(df)} ({nan_pct:.1f}%)")
    
    if nan_count == 0:
        print("   ✅ All tickers have valid prices")
    elif nan_pct < 30:
        print(f"   ⚠️  {nan_pct:.1f}% NaN (below 30% threshold)")
    else:
        print(f"   ❌ {nan_pct:.1f}% NaN (EXCEEDS 30% threshold)")
    
    # Price source distribution
    if 'price_source' in df.columns:
        print(f"\n📈 Price Source Distribution:")
        for source in ['lastPrice', 'mark', 'closePrice', 'bidAskMid', 'regularMarketLastPrice', 'none']:
            count = (df['price_source'] == source).sum()
            if count > 0:
                pct = (count / len(df)) * 100
                print(f"   {source:<25} {count:>2} ({pct:>5.1f}%)")
    
    # Market status
    if 'market_status' in df.columns:
        market_status = df['market_status'].iloc[0] if len(df) > 0 else 'UNKNOWN'
        is_open = df['is_market_open'].iloc[0] if len(df) > 0 else False
        print(f"\n🏦 Market Status: {market_status} ({'OPEN' if is_open else 'CLOSED'})")
    
    # Sample rows
    print(f"\n📋 Sample Data (First 10 Rows):")
    print_separator("-")
    
    # Print header
    print(f"{'Ticker':<8} {'Price':<10} {'Source':<18} {'Market':<8} {'Age(s)':<10} {'HV_30':<8}")
    print_separator("-")
    
    # Print rows
    for i in range(min(10, len(df))):
        row = df.iloc[i]
        ticker = row['Ticker']
        price = f"${row['last_price']:.2f}" if pd.notna(row['last_price']) else "NaN"
        source = row.get('price_source', 'N/A')
        market = "OPEN" if row.get('is_market_open', False) else "CLOSED"
        age = f"{row.get('quote_age_sec', 0):.0f}s" if pd.notna(row.get('quote_age_sec')) else "N/A"
        hv_30 = f"{row['hv_30']:.1f}%" if pd.notna(row.get('hv_30')) else "NaN"
        
        print(f"{ticker:<8} {price:<10} {source:<18} {market:<8} {age:<10} {hv_30:<8}")
    
    print_separator("-")
    
    # Data quality verdict
    print("\n🔍 Step 0 Verdict:")
    if nan_count == 0:
        print("   ✅ PASS - All prices valid")
    elif nan_pct < 30:
        print(f"   ⚠️  PASS (with warnings) - {nan_pct:.1f}% NaN")
    else:
        print(f"   ❌ FAIL - {nan_pct:.1f}% NaN exceeds threshold")
        return False
    
    # Check price ranges
    valid_prices = df[df['last_price'].notna()]['last_price']
    if len(valid_prices) > 0:
        min_price = valid_prices.min()
        max_price = valid_prices.max()
        print(f"   Price range: ${min_price:.2f} - ${max_price:.2f}")
        
        if min_price < 10:
            print(f"   ⚠️  Suspiciously low price: ${min_price:.2f}")
        if max_price > 1000:
            print(f"   ⚠️  Suspiciously high price: ${max_price:.2f}")
    
    return True

def validate_step3(df_in, df_out):
    """Validate Step 3 IV rank filtering"""
    print_header("STEP 3: IV RANK FILTER")
    
    print(f"\n📊 Row Counts:")
    print(f"   Input:  {len(df_in)}")
    print(f"   Output: {len(df_out)}")
    print(f"   Dropped: {len(df_in) - len(df_out)}")
    
    if len(df_in) - len(df_out) > 0:
        dropped_tickers = set(df_in['Ticker']) - set(df_out['Ticker'])
        print(f"\n📉 Dropped Tickers: {sorted(dropped_tickers)}")
    
    print("\n🔍 Step 3 Verdict:")
    if len(df_out) > 0:
        print(f"   ✅ PASS - {len(df_out)} tickers passed filter")
    else:
        print(f"   ⚠️  WARNING - No tickers passed filter")
    
    return True

def validate_step5(df_in, df_out):
    """Validate Step 5 chart signals"""
    print_header("STEP 5: CHART SIGNALS")
    
    print(f"\n📊 Row Counts:")
    print(f"   Input:  {len(df_in)}")
    print(f"   Output: {len(df_out)}")
    print(f"   Dropped: {len(df_in) - len(df_out)}")
    
    if len(df_in) - len(df_out) > 0:
        dropped_tickers = set(df_in['Ticker']) - set(df_out['Ticker'])
        print(f"\n📉 Dropped Tickers: {sorted(dropped_tickers)}")
    
    # Check for key columns
    required_cols = ['EMA9', 'SMA20', 'Atr_Pct', 'Trend_Slope', 'Chart_Regime']
    missing_cols = [col for col in required_cols if col not in df_out.columns]
    
    if missing_cols:
        print(f"\n❌ Missing columns: {missing_cols}")
        return False
    
    # Check for NaN cascade
    nan_counts = {}
    for col in required_cols:
        nan_count = df_out[col].isna().sum()
        if nan_count > 0:
            nan_counts[col] = nan_count
    
    if nan_counts:
        print(f"\n⚠️  NaN values detected:")
        for col, count in nan_counts.items():
            pct = (count / len(df_out)) * 100
            print(f"   {col}: {count}/{len(df_out)} ({pct:.1f}%)")
    else:
        print(f"\n✅ No NaN cascade - All indicators computed")
    
    # Sample data
    if len(df_out) > 0:
        print(f"\n📋 Sample Indicators (First 5 Rows):")
        print_separator("-")
        print(f"{'Ticker':<8} {'EMA9':<10} {'SMA20':<10} {'Atr_Pct':<10} {'Slope':<10} {'Regime':<15}")
        print_separator("-")
        
        for i in range(min(5, len(df_out))):
            row = df_out.iloc[i]
            ticker = row['Ticker']
            ema9 = f"{row['EMA9']:.2f}" if pd.notna(row.get('EMA9')) else "NaN"
            sma20 = f"{row['SMA20']:.2f}" if pd.notna(row.get('SMA20')) else "NaN"
            atr = f"{row['Atr_Pct']:.2f}%" if pd.notna(row.get('Atr_Pct')) else "NaN"
            slope = f"{row['Trend_Slope']:.2f}" if pd.notna(row.get('Trend_Slope')) else "NaN"
            regime = row.get('Chart_Regime', 'N/A')
            
            print(f"{ticker:<8} {ema9:<10} {sma20:<10} {atr:<10} {slope:<10} {regime:<15}")
        
        print_separator("-")
    
    print("\n🔍 Step 5 Verdict:")
    if len(df_out) > 0 and not nan_counts:
        print(f"   ✅ PASS - All indicators computed")
    elif len(df_out) > 0:
        print(f"   ⚠️  PASS (with NaN values)")
    else:
        print(f"   ❌ FAIL - No output rows")
        return False
    
    return True

def validate_step6(df_in, df_out):
    """Validate Step 6 data quality"""
    print_header("STEP 6: DATA QUALITY VALIDATION")
    
    print(f"\n📊 Row Counts:")
    print(f"   Input:  {len(df_in)}")
    print(f"   Output: {len(df_out)}")
    print(f"   Rejected: {len(df_in) - len(df_out)}")
    
    if len(df_in) - len(df_out) > 0:
        rejected_tickers = set(df_in['Ticker']) - set(df_out['Ticker'])
        print(f"\n❌ Rejected Tickers: {sorted(rejected_tickers)}")
    else:
        print(f"\n✅ All tickers passed validation")
    
    # Check validation flags
    if 'Universal_Data_Complete' in df_out.columns:
        complete_count = df_out['Universal_Data_Complete'].sum()
        print(f"\n📊 Universal Data Complete: {complete_count}/{len(df_out)}")
    
    if 'Directional_Data_Complete' in df_out.columns:
        complete_count = df_out['Directional_Data_Complete'].sum()
        print(f"📊 Directional Data Complete: {complete_count}/{len(df_out)}")
    
    print("\n🔍 Step 6 Verdict:")
    if len(df_out) == len(df_in):
        print(f"   ✅ PASS - No rejections")
    elif len(df_out) > 0:
        rejection_rate = ((len(df_in) - len(df_out)) / len(df_in)) * 100
        print(f"   ⚠️  PASS - {rejection_rate:.1f}% rejection rate")
    else:
        print(f"   ❌ FAIL - All tickers rejected")
        return False
    
    return True

def main():
    """Main validation workflow"""
    print_separator("=")
    print("  SCHWAB CLI PIPELINE VALIDATION")
    print("  Data Quality Check: Steps 0-6")
    print_separator("=")
    
    print(f"\n📋 Test Configuration:")
    print(f"   Tickers: {len(TEST_TICKERS)}")
    print(f"   List: {', '.join(TEST_TICKERS[:5])}...")
    
    # Initialize Schwab client
    print(f"\n🔐 Initializing Schwab client...")
    client_id = os.getenv('SCHWAB_CLIENT_ID', 'placeholder')
    client_secret = os.getenv('SCHWAB_CLIENT_SECRET', 'placeholder')
    
    try:
        client = SchwabClient(client_id, client_secret)
        
        if not client._tokens:
            print("\n❌ ERROR: No Schwab tokens found")
            print("   Run: python tools/reauth_schwab.py")
            return False
        
        print("✅ Client initialized")
        
        # STEP 0: Generate snapshot
        print_header("RUNNING STEP 0: SCHWAB SNAPSHOT")
        
        df_step0 = generate_live_snapshot(
            client=client,
            tickers=TEST_TICKERS,
            use_cache=True,
            fetch_iv=False  # Skip IV for speed
        )
        
        # Validate Step 0
        if not validate_step0(df_step0):
            print("\n❌ PIPELINE STOPPED: Step 0 validation failed")
            return False
        
        # Save snapshot
        snapshot_path = save_snapshot(df_step0)
        print(f"\n💾 Snapshot saved: {snapshot_path}")
        
        # STEP 3: Skipped (not critical for validation)
        print_header("STEP 3: SKIPPED (Data Quality Focus)")
        print("\n⏭️  Skipping Step 3 for this validation")
        df_step3 = df_step0.copy()
        
        # STEP 5: Chart Signals
        print_header("RUNNING STEP 5: CHART SIGNALS")
        
        df_step5 = compute_chart_signals(df_step3)
        
        if not validate_step5(df_step3, df_step5):
            print("\n❌ PIPELINE STOPPED: Step 5 validation failed")
            return False
        
        # STEP 6: Data Quality
        print_header("RUNNING STEP 6: DATA QUALITY")
        
        df_step6 = validate_data_quality(df_step5)
        
        if not validate_step6(df_step5, df_step6):
            print("\n❌ PIPELINE STOPPED: Step 6 validation failed")
            return False
        
        # Final Summary
        print_header("PIPELINE VALIDATION SUMMARY")
        
        print(f"\n📊 Row Counts at Each Step:")
        print(f"   Step 0 (Snapshot):    {len(df_step0)}")
        print(f"   Step 3 (IV Filter):   {len(df_step3)}")
        print(f"   Step 5 (Charted):     {len(df_step5)}")
        print(f"   Step 6 (Validated):   {len(df_step6)}")
        
        attrition_rate = ((len(df_step0) - len(df_step6)) / len(df_step0)) * 100 if len(df_step0) > 0 else 0
        print(f"\n📉 Attrition Rate: {attrition_rate:.1f}%")
        
        # Final Verdict
        print_header("FINAL VERDICT")
        
        if len(df_step6) > 0:
            nan_pct = (df_step0['last_price'].isna().sum() / len(df_step0)) * 100
            
            print("\n✅ SCHWAB CLI PIPELINE IS USABLE")
            print("\nKey Findings:")
            print(f"   • {len(df_step6)}/{len(TEST_TICKERS)} tickers completed pipeline")
            print(f"   • {100-nan_pct:.1f}% valid prices from Schwab")
            
            if 'price_source' in df_step0.columns:
                source_counts = df_step0['price_source'].value_counts()
                print(f"   • Price sources: {dict(source_counts)}")
            
            if 'market_status' in df_step0.columns:
                market_status = df_step0['market_status'].iloc[0]
                print(f"   • Market status: {market_status}")
            
            print("\n📋 Remaining Issues:")
            if nan_pct > 0:
                print(f"   • {nan_pct:.1f}% NaN prices (investigate specific tickers)")
            if attrition_rate > 20:
                print(f"   • {attrition_rate:.1f}% attrition rate (check filter thresholds)")
            
            if nan_pct == 0 and attrition_rate < 20:
                print("   • None - pipeline is healthy")
            
            return True
        else:
            print("\n❌ SCHWAB CLI PIPELINE IS NOT USABLE")
            print("\nBlocking Issues:")
            print("   • All tickers dropped during pipeline")
            print("   • Check Step 0 data quality")
            print("   • Check filter thresholds")
            return False
        
    except Exception as e:
        print(f"\n❌ PIPELINE ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n❌ Interrupted by user")
        sys.exit(1)
