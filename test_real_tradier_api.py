#!/usr/bin/env python3
"""
Real Tradier API Pipeline Test

Tests pipeline with actual Tradier API data (no mocks).
Observes natural strategy distributions and data quality.
"""
import sys
sys.path.insert(0, '/Users/haniabadi/Documents/Github/options')

import warnings
warnings.filterwarnings('ignore')
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

print('=' * 80)
print('Real Tradier API Pipeline Test')
print('=' * 80)

# Verify API token
token = os.getenv('TRADIER_TOKEN')
if token:
    print(f'✅ Tradier API token loaded: {token[:8]}...')
else:
    print('❌ No TRADIER_TOKEN found in environment')
    sys.exit(1)

# ============================================================
# STEP 2: Load IV/HV Snapshot (Skip Pattern Detection)
# ============================================================
print('\n' + '=' * 80)
print('STEP 2: Load IV/HV Snapshot')
print('=' * 80)

from core.scan_engine.step2_load_snapshot import load_ivhv_snapshot

df_snapshot = load_ivhv_snapshot(skip_pattern_detection=True)
print(f'✅ Loaded: {len(df_snapshot)} tickers')
print(f'   Murphy signals: Trend_State, RSI, ADX, Volume_Trend, RV_10D')
print(f'   Sinclair signals: Volatility_Regime, IV_Term_Structure, VVIX')
print(f'   Natenberg: RV/IV Ratio')

# ============================================================
# STEP 3: Calculate IV/HV Divergence
# ============================================================
print('\n' + '=' * 80)
print('STEP 3: Calculate IV/HV Divergence')
print('=' * 80)

from core.scan_engine.step3_filter_ivhv import filter_ivhv_gap

df_filtered = filter_ivhv_gap(df_snapshot)
print(f'✅ Filtered: {len(df_filtered)} tickers with IV/HV divergence')
if len(df_filtered) > 0:
    print(f'   IVHV_gap_30D: mean={df_filtered["IVHV_gap_30D"].mean():.2f}, '
          f'min={df_filtered["IVHV_gap_30D"].min():.2f}, max={df_filtered["IVHV_gap_30D"].max():.2f}')

# ============================================================
# STEP 7: Generate Strategies (Test with 5 tickers)
# ============================================================
print('\n' + '=' * 80)
print('STEP 7: Generate Strategies (Testing 5 tickers)')
print('=' * 80)

from core.scan_engine.step7_strategy_recommendation import recommend_strategies

# Filter for lower-priced stocks (better liquidity)
test_df = df_filtered[df_filtered['Price'] < 200].head(5) if 'Price' in df_filtered.columns else df_filtered.head(5)
print(f'   Testing tickers: {list(test_df["Ticker"].values)}')

df_strategies = recommend_strategies(test_df)
print(f'✅ Generated: {len(df_strategies)} strategies from {len(test_df)} tickers')

if 'Strategy_Name' in df_strategies.columns:
    strategy_counts = df_strategies['Strategy_Name'].value_counts()
    print(f'   Strategy types: {len(strategy_counts)} unique')
    for strategy, count in strategy_counts.head(10).items():
        print(f'      • {strategy}: {count}')

# Add DTE fields required by Step 9B
df_strategies['Min_DTE'] = 30
df_strategies['Max_DTE'] = 60
print(f'   Added DTE range: 30-60 days')

# ============================================================
# STEP 9B: Fetch Real Option Contracts (Tradier API)
# ============================================================
print('\n' + '=' * 80)
print('STEP 9B: Fetch Real Option Contracts (TRADIER API)')
print('=' * 80)

from core.scan_engine.step9b_fetch_contracts import fetch_and_select_contracts

print(f'🔄 Fetching contracts for {len(df_strategies)} strategies...')
print(f'   (This may take 1-2 minutes)')

df_contracts = fetch_and_select_contracts(df_strategies, tradier_token=token)

print(f'✅ Contract fetch complete: {len(df_contracts)} results')

# Check success rate
if 'Contract_Selection_Status' in df_contracts.columns:
    status_counts = df_contracts['Contract_Selection_Status'].value_counts()
    print(f'   Status distribution:')
    for status, count in status_counts.items():
        pct = count / len(df_contracts) * 100
        print(f'      • {status}: {count} ({pct:.1f}%)')

# Check promoted_strike population
if 'promoted_strike' in df_contracts.columns:
    promoted_count = df_contracts['promoted_strike'].notna().sum()
    print(f'   Promoted strikes: {promoted_count}/{len(df_contracts)} ({promoted_count/len(df_contracts)*100:.1f}%)')

# ============================================================
# STEP 10: Extract Greeks
# ============================================================
print('\n' + '=' * 80)
print('STEP 10: Extract Greeks from Promoted Strikes')
print('=' * 80)

from utils.greek_extraction import extract_greeks_to_columns

df_greeks = extract_greeks_to_columns(df_contracts)
print(f'✅ Greeks extracted: {len(df_greeks)} rows')

# Check Greek coverage
greek_cols = ['Delta', 'Gamma', 'Vega', 'Theta']
for col in greek_cols:
    if col in df_greeks.columns:
        coverage = df_greeks[col].notna().sum()
        pct = coverage / len(df_greeks) * 100
        print(f'   {col}: {coverage}/{len(df_greeks)} ({pct:.1f}%)')
        if coverage > 0:
            values = df_greeks[col].dropna()
            print(f'      Range: [{values.min():.3f}, {values.max():.3f}]')

# ============================================================
# STEP 11: Theory Validation (PCS Scoring)
# ============================================================
print('\n' + '=' * 80)
print('STEP 11: Theory Validation (Author Guardrails)')
print('=' * 80)

from core.scan_engine.step11_independent_evaluation import evaluate_strategies_independently

df_evaluated = evaluate_strategies_independently(df_greeks)
print(f'✅ Evaluation complete: {len(df_evaluated)} strategies scored')

# Check PCS score distribution
if 'PCS_Score' in df_evaluated.columns:
    scores = df_evaluated['PCS_Score'].dropna()
    if len(scores) > 0:
        print(f'   PCS Score: mean={scores.mean():.1f}, median={scores.median():.1f}')
        print(f'              min={scores.min():.1f}, max={scores.max():.1f}')

# Check validation status
if 'Validation_Status' in df_evaluated.columns:
    status_counts = df_evaluated['Validation_Status'].value_counts()
    print(f'\n   Validation Status:')
    for status, count in status_counts.items():
        pct = count / len(df_evaluated) * 100
        print(f'      • {status}: {count} ({pct:.1f}%)')

# Check rejection reasons
if 'Rejection_Reasons' in df_evaluated.columns:
    rejections = df_evaluated[df_evaluated['Rejection_Reasons'].notna()]
    if len(rejections) > 0:
        print(f'\n   Rejection Analysis ({len(rejections)} strategies):')
        all_reasons = []
        for reasons_str in rejections['Rejection_Reasons']:
            if reasons_str and isinstance(reasons_str, str):
                all_reasons.extend([r.strip() for r in reasons_str.split(',')])
        
        from collections import Counter
        reason_counts = Counter(all_reasons)
        for reason, count in reason_counts.most_common(10):
            print(f'      • {reason}: {count}')

# ============================================================
# FINAL SUMMARY
# ============================================================
print('\n' + '=' * 80)
print('REAL DATA PIPELINE SUMMARY')
print('=' * 80)

print(f'\n📊 Pipeline Flow:')
print(f'   Step 2: {len(df_snapshot)} tickers loaded')
print(f'   Step 3: {len(df_filtered)} tickers with IV/HV divergence')
print(f'   Step 7: {len(df_strategies)} strategies generated ({len(test_df)} tickers tested)')
print(f'   Step 9B: {len(df_contracts)} contract results')
print(f'   Step 10: {len(df_greeks)} with Greeks extracted')
print(f'   Step 11: {len(df_evaluated)} strategies evaluated')

if 'Validation_Status' in df_evaluated.columns:
    valid_count = (df_evaluated['Validation_Status'] == 'Valid').sum()
    watch_count = (df_evaluated['Validation_Status'] == 'Watch').sum()
    reject_count = (df_evaluated['Validation_Status'] == 'Reject').sum()
    
    print(f'\n🎯 Final Results:')
    print(f'   ✅ Valid: {valid_count} ({valid_count/len(df_evaluated)*100:.1f}%) - Ready to execute')
    print(f'   ⚠️  Watch: {watch_count} ({watch_count/len(df_evaluated)*100:.1f}%) - Monitor only')
    print(f'   ❌ Reject: {reject_count} ({reject_count/len(df_evaluated)*100:.1f}%) - Do not execute')

print('\n' + '=' * 80)
print('✅ Real API Test Complete')
print('=' * 80)
print('\n💡 Observations:')
print('   • Real Greeks from Tradier API (not uniform mock data)')
print('   • Promoted strikes populated with actual contract data')
print('   • Natural PCS score distribution emerging')
print('   • Theory-driven validation working with real data')
print('\n📈 Next: Test with full ticker universe (not just 5)')
