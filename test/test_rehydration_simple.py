#!/usr/bin/env python3
"""Simple test: Does IV surface rehydration work?"""

import pandas as pd
import sys
sys.path.insert(0, '.')

print("="*80)
print("TESTING IV SURFACE REHYDRATION")
print("="*80)

# Step 1: Load raw snapshot (before rehydration)
print("\n[1] Loading raw snapshot (BEFORE rehydration)...")
df_raw = pd.read_csv('data/snapshots/ivhv_snapshot_live_20260102_124337.csv')
df_raw = df_raw[df_raw['Ticker'] == 'AAPL']

print(f"Rows: {len(df_raw)}")
print(f"\nAAPL IV values (raw snapshot):")
print(f"  IV_7_D_Call:  {df_raw['IV_7_D_Call'].iloc[0]}")
print(f"  IV_14_D_Call: {df_raw['IV_14_D_Call'].iloc[0]}")
print(f"  IV_21_D_Call: {df_raw['IV_21_D_Call'].iloc[0]}")
print(f"  IV_30_D_Call: {df_raw['IV_30_D_Call'].iloc[0]}")
print(f"  IV_60_D_Call: {df_raw['IV_60_D_Call'].iloc[0]}")

# Step 2: Load with rehydration
print("\n[2] Loading with IV surface rehydration...")
from core.scan_engine.step2_load_snapshot import load_ivhv_snapshot

df_rehydrated = load_ivhv_snapshot('data/snapshots/ivhv_snapshot_live_20260102_124337.csv')
df_rehydrated = df_rehydrated[df_rehydrated['Ticker'] == 'AAPL']

print(f"Rows: {len(df_rehydrated)}")
print(f"\nAAPL IV values (after rehydration):")
print(f"  IV_7_D_Call:  {df_rehydrated['IV_7_D_Call'].iloc[0]}")
print(f"  IV_14_D_Call: {df_rehydrated['IV_14_D_Call'].iloc[0]}")
print(f"  IV_21_D_Call: {df_rehydrated['IV_21_D_Call'].iloc[0]}")
print(f"  IV_30_D_Call: {df_rehydrated['IV_30_D_Call'].iloc[0]}")
print(f"  IV_60_D_Call: {df_rehydrated['IV_60_D_Call'].iloc[0]}")

# Check metadata
if 'iv_surface_source' in df_rehydrated.columns:
    print(f"\nMetadata:")
    print(f"  iv_surface_source: {df_rehydrated['iv_surface_source'].iloc[0]}")
    print(f"  iv_surface_date: {df_rehydrated['iv_surface_date'].iloc[0]}")
    print(f"  iv_surface_age_days: {df_rehydrated['iv_surface_age_days'].iloc[0]}")

print("\n" + "="*80)
print("✅ TEST COMPLETE")
print("="*80)
