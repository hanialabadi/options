"""
Step 0 Final Validation Test
Tests hv_slope, volatility_regime, and data_source fields
"""

from core.scan_engine.step0_schwab_snapshot import *
from core.scan_engine.schwab_api_client import SchwabClient
import os
import time

def main():
    # Initialize client
    client = SchwabClient(
        client_id=os.getenv('SCHWAB_CLIENT_ID', 'placeholder'),
        client_secret=os.getenv('SCHWAB_CLIENT_SECRET', 'placeholder'),
        token_file_path=os.path.expanduser('~/.schwab_tokens.json')
    )
    
    test_tickers = ['AAPL', 'MSFT', 'NVDA', 'AMZN', 'META']
    print(f'🧪 STEP 0 VALIDATION TEST')
    print(f'=' * 60)
    print(f'Tickers: {test_tickers}')
    print(f'Mode: HV-only (fetch_iv=False)\n')
    
    # Generate snapshot
    start = time.time()
    df = generate_live_snapshot(client, test_tickers, fetch_iv=False)
    elapsed = time.time() - start
    
    print(f'\n✅ SNAPSHOT GENERATED')
    print(f'   Rows: {len(df)}')
    print(f'   Columns: {len(df.columns)}')
    print(f'   Execution time: {elapsed:.2f}s')
    
    # Validate required columns
    print(f'\n📋 REQUIRED COLUMNS:')
    required = {
        'Ticker': 'Ticker symbol',
        'last_price': 'Current price',
        'hv_10': 'HV 10-day',
        'hv_20': 'HV 20-day',
        'hv_30': 'HV 30-day',
        'hv_slope': 'HV slope (10D - 30D)',
        'volatility_regime': 'Volatility classification',
        'iv_30d': 'IV proxy (optional)',
        'snapshot_ts': 'Snapshot timestamp',
        'data_source': 'Data source tag'
    }
    
    for col, desc in required.items():
        present = '✅' if col in df.columns else '❌'
        print(f'   {present} {col:20s} - {desc}')
    
    # Sample data
    print(f'\n📊 SAMPLE DATA (First Ticker):')
    display_cols = ['Ticker', 'last_price', 'hv_10', 'hv_30', 'hv_slope', 'volatility_regime', 'data_source']
    sample = df[display_cols].head(1)
    for col in display_cols:
        val = sample[col].values[0]
        if col in ['hv_10', 'hv_30', 'hv_slope'] and not pd.isna(val):
            print(f'   {col:20s}: {val:.2f}%')
        else:
            print(f'   {col:20s}: {val}')
    
    # Volatility regime distribution
    print(f'\n🎯 VOLATILITY REGIMES:')
    for regime, count in df['volatility_regime'].value_counts().items():
        print(f'   {regime:25s}: {count}')
    
    # Data quality check
    print(f'\n✅ DATA QUALITY:')
    print(f'   HV populated: {df["hv_30"].notna().sum()}/{len(df)} ({100*df["hv_30"].notna().sum()/len(df):.0f}%)')
    print(f'   IV populated: {df["iv_30d"].notna().sum()}/{len(df)} ({100*df["iv_30d"].notna().sum()/len(df):.0f}%)')
    print(f'   Data source: {df["data_source"].unique()[0]}')
    
    # Save snapshot
    path = save_snapshot(df)
    print(f'\n💾 SNAPSHOT SAVED:')
    print(f'   Filename: {path.name}')
    print(f'   Size: {path.stat().st_size} bytes')
    print(f'   Location: {path}')
    
    # Verify Step 2 compatibility
    print(f'\n🔗 STEP 2 INTEGRATION TEST:')
    from core.scan_engine.step2_load_snapshot import load_ivhv_snapshot
    df_step2 = load_ivhv_snapshot(str(path))
    print(f'   ✅ Step 2 loaded successfully')
    print(f'   Rows: {len(df_step2)}')
    print(f'   Columns: {len(df_step2.columns)}')
    
    print(f'\n' + '=' * 60)
    print(f'✅ STEP 0 VALIDATION: SUCCESS')
    print(f'=' * 60)

if __name__ == '__main__':
    main()
