#!/usr/bin/env python3
"""Quick test of IV surface rehydration"""

import pandas as pd
import sys
sys.path.insert(0, '.')
from datetime import datetime
from core.scan_engine.step2_load_snapshot import load_ivhv_snapshot

print('='*70)
print('TESTING IV SURFACE REHYDRATION')
print('='*70)

# Load snapshot with rehydration
df = load_ivhv_snapshot('data/snapshots/ivhv_snapshot_live_20260102_124337.csv')

print(f'\n✅ Snapshot loaded: {len(df)} tickers, {len(df.columns)} columns')

# Check IV surface columns
iv_cols = sorted([c for c in df.columns if 'IV_' in c and ('Call' in c or 'Put' in c)])
print(f'\nIV columns ({len(iv_cols)}):')
for col in iv_cols[:15]:
    print(f'  - {col}')
if len(iv_cols) > 15:
    print(f'  ... and {len(iv_cols) - 15} more')

# Check metadata
print(f'\nIV Surface Metadata:')
if 'iv_surface_source' in df.columns:
    sources = df['iv_surface_source'].value_counts().to_dict()
    for source, count in sources.items():
        print(f'  {source}: {count} tickers')
        
if 'iv_surface_age_days' in df.columns:
    avg_age = df['iv_surface_age_days'].mean()
    max_age = df['iv_surface_age_days'].max()
    print(f'  Age: avg={avg_age:.1f} days, max={max_age:.0f} days')

# Check AAPL specifically
print(f'\n{"="*70}')
print('AAPL IV VALUES (Before rehydration: only IV_30_D_Call populated)')
print('='*70)
if 'AAPL' in df['Ticker'].values:
    aapl = df[df['Ticker'] == 'AAPL'].iloc[0]
    iv_check_cols = ['IV_7_D_Call', 'IV_14_D_Call', 'IV_21_D_Call', 'IV_30_D_Call',
                     'IV_60_D_Call', 'IV_90_D_Call', 'IV_120_D_Call', 'IV_180_D_Call',
                     'IV_360_D_Call', 'IV_720_D_Call']
    for col in iv_check_cols:
        val = aapl.get(col, 'N/A')
        status = '✅ POPULATED' if pd.notna(val) and val != 'N/A' else '❌ NaN'
        print(f'  {col:20s}: {str(val):>10s}  {status}')
    
    print(f'\nMetadata:')
    print(f'  iv_surface_source: {aapl.get("iv_surface_source", "N/A")}')
    print(f'  iv_surface_date: {aapl.get("iv_surface_date", "N/A")}')
    print(f'  iv_surface_age_days: {aapl.get("iv_surface_age_days", "N/A")}')

print(f'\n{"="*70}')
print('✅ REHYDRATION TEST COMPLETE')
print('='*70)
