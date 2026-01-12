#!/usr/bin/env python3
"""
Full CLI Pipeline Test: 20 Tickers with Schwab as Primary Data Source

Runs Steps 0→2→3→5→6→7→9A→11 and displays real data artifacts.
"""

import sys
import pandas as pd
from pathlib import Path
from datetime import datetime
import os

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.scan_engine.step0_schwab_snapshot import generate_live_snapshot, save_snapshot
from core.scan_engine.schwab_api_client import SchwabClient
from core.scan_engine.step2_load_snapshot import load_ivhv_snapshot
from core.scan_engine.step3_filter_ivhv import filter_ivhv_gap
from core.scan_engine.step5_chart_signals import compute_chart_signals
from core.scan_engine.step6_gem_filter import validate_data_quality
from core.scan_engine.step7_strategy_recommendation import recommend_strategies
from core.scan_engine.step9a_determine_timeframe import determine_timeframe
from core.scan_engine.step11_independent_evaluation import evaluate_strategies_independently

def print_divider(title=""):
    print("\n" + "="*80)
    if title:
        print(f"  {title}")
        print("="*80)
    else:
        print("="*80)

def main():
    print_divider("🧪 FULL CLI PIPELINE TEST: SCHWAB-FIRST (20 TICKERS)")
    
    # Test tickers (first 20 from tickers.csv)
    test_tickers = [
        "BKNG", "AZO", "MELI", "MKL", "FCNCA",
        "FICO", "TDG", "NFLX", "MTD", "TPL",
        "BLK", "GWW", "NOW", "COST", "KLAC",
        "AXON", "INTU", "LLY", "SPOT", "URI"
    ]
    
    print(f"\n📋 Test Universe: {len(test_tickers)} tickers")
    print(f"   {', '.join(test_tickers[:10])}")
    print(f"   {', '.join(test_tickers[10:])}")
    
    # ========================================================================
    # STEP 0: Use Existing Schwab Snapshot
    # ========================================================================
    print_divider("STEP 0: Load Existing Schwab Snapshot")
    
    # Find latest snapshot
    snapshot_dir = Path("data/snapshots")
    snapshots = sorted(snapshot_dir.glob("ivhv_snapshot_live_*.csv"), reverse=True)
    
    if not snapshots:
        print("\n❌ ERROR: No snapshots found in data/snapshots/")
        print("   Please run Step 0 first to generate a snapshot")
        return
    
    snapshot_path = snapshots[0]
    print(f"\n📂 Using existing snapshot: {snapshot_path}")
    
    # Load snapshot
    df_snapshot = pd.read_csv(snapshot_path)
    
    # Filter to test tickers only
    df_snapshot = df_snapshot[df_snapshot['Ticker'].isin(test_tickers)]
    
    print(f"✅ Snapshot loaded: {len(df_snapshot)} tickers (filtered to test set)")
    
    # Load and inspect
    df_snapshot = pd.read_csv(snapshot_path)
    print(f"\n📊 Snapshot Stats:")
    print(f"   Rows: {len(df_snapshot)}")
    print(f"   Columns: {len(df_snapshot.columns)}")
    print(f"   Columns: {df_snapshot.columns.tolist()}")
    
    print(f"\n📋 Sample Rows (Step 0):")
    sample_cols = ['Ticker', 'last_price', 'hv_10', 'hv_30', 'hv_slope', 
                   'volatility_regime', 'iv_30d']
    available_cols = [c for c in sample_cols if c in df_snapshot.columns]
    print(df_snapshot[available_cols].head(5).to_string(index=False))
    
    # Sanity check
    print(f"\n🔍 Data Sanity Check:")
    print(f"   last_price: min={df_snapshot['last_price'].min():.2f}, "
          f"max={df_snapshot['last_price'].max():.2f}, "
          f"nulls={df_snapshot['last_price'].isna().sum()}")
    print(f"   hv_30: min={df_snapshot['hv_30'].min():.2f}, "
          f"max={df_snapshot['hv_30'].max():.2f}, "
          f"nulls={df_snapshot['hv_30'].isna().sum()}")
    print(f"   iv_30d: nulls={df_snapshot['iv_30d'].isna().sum()} "
          f"(expected: {len(df_snapshot)} since skip_iv=True)")
    
    # ========================================================================
    # STEP 2: Load and Enrich Snapshot
    # ========================================================================
    print_divider("STEP 2: Load and Enrich Snapshot")
    
    df_enriched = load_ivhv_snapshot(snapshot_path)
    
    print(f"\n✅ Enrichment complete:")
    print(f"   Rows: {len(df_enriched)}")
    print(f"   New columns from Step 2:")
    step2_cols = [c for c in df_enriched.columns if c not in df_snapshot.columns]
    print(f"   {step2_cols}")
    
    # ========================================================================
    # STEP 3: HV-Based Filter (IV optional)
    # ========================================================================
    print_divider("STEP 3: HV-Based Filter")
    
    initial_count = len(df_enriched)
    df_filtered = filter_ivhv_gap(df_enriched, min_gap=2.0)
    passed_count = len(df_filtered)
    failed_count = initial_count - passed_count
    
    print(f"\n📊 Filter Results:")
    print(f"   Input: {initial_count} tickers")
    print(f"   Passed: {passed_count} tickers")
    print(f"   Failed: {failed_count} tickers")
    
    if passed_count > 0:
        print(f"\n✅ Tickers that PASSED (sample):")
        sample_cols = ['Ticker', 'HV30', 'HV_Trend_30D', 'ModerateVol']
        available_cols = [c for c in sample_cols if c in df_filtered.columns]
        print(df_filtered[available_cols].head(5).to_string(index=False))
    
    if failed_count > 0:
        # Find failed tickers
        failed_tickers = set(df_enriched['Ticker']) - set(df_filtered['Ticker'])
        df_failed = df_enriched[df_enriched['Ticker'].isin(failed_tickers)]
        print(f"\n❌ Tickers that FAILED (sample):")
        sample_cols = ['Ticker', 'HV30', 'HV_Trend_30D']
        available_cols = [c for c in sample_cols if c in df_failed.columns]
        print(df_failed[available_cols].head(5).to_string(index=False))
    
    # Save Step 3 output
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    step3_path = f"output/Step3_Filtered_{timestamp}.csv"
    df_filtered.to_csv(step3_path, index=False)
    print(f"\n💾 Saved: {step3_path}")
    
    # ========================================================================
    # STEP 5: Chart Signals (Schwab price history)
    # ========================================================================
    print_divider("STEP 5: Chart Signals (Schwab Price History)")
    
    df_charted = compute_chart_signals(df_filtered)
    
    print(f"\n✅ Chart signals computed:")
    print(f"   Rows: {len(df_charted)}")
    print(f"   Columns added: {[c for c in df_charted.columns if c not in df_filtered.columns]}")
    
    print(f"\n📋 Sample Indicators:")
    sample_cols = ['Ticker', 'EMA9', 'EMA21', 'SMA20', 'Trend_Slope', 'Chart_Regime']
    available_cols = [c for c in sample_cols if c in df_charted.columns]
    print(df_charted[available_cols].head(5).to_string(index=False))
    
    print(f"\n🔍 Indicator Sanity Check:")
    for col in ['EMA9', 'EMA21', 'SMA20', 'Atr_Pct']:
        if col in df_charted.columns:
            print(f"   {col}: min={df_charted[col].min():.2f}, "
                  f"max={df_charted[col].max():.2f}, "
                  f"nulls={df_charted[col].isna().sum()}")
    
    # Save Step 5 output
    step5_path = f"output/Step5_Charted_{timestamp}.csv"
    df_charted.to_csv(step5_path, index=False)
    print(f"\n💾 Saved: {step5_path}")
    
    # ========================================================================
    # STEP 6: Data Quality Validation
    # ========================================================================
    print_divider("STEP 6: Data Quality Validation")
    
    df_validated = validate_data_quality(df_charted)
    
    print(f"\n✅ Validation complete:")
    print(f"   Input: {len(df_charted)} tickers")
    print(f"   Output: {len(df_validated)} tickers")
    print(f"   Rejected: {len(df_charted) - len(df_validated)} tickers")
    
    # Save Step 6 output
    step6_path = f"output/Step6_Validated_{timestamp}.csv"
    df_validated.to_csv(step6_path, index=False)
    print(f"\n💾 Saved: {step6_path}")
    
    # ========================================================================
    # STEP 7: Strategy Recommendation
    # ========================================================================
    print_divider("STEP 7: Strategy Recommendation")
    
    df_strategies = recommend_strategies(df_validated)
    
    print(f"\n✅ Strategy generation complete:")
    print(f"   Total strategies: {len(df_strategies)}")
    print(f"   Unique tickers: {df_strategies['Ticker'].nunique()}")
    
    # Calculate strategies per ticker
    strategies_per_ticker = df_strategies.groupby('Ticker').size()
    print(f"   Strategies per ticker:")
    print(f"      Min: {strategies_per_ticker.min()}")
    print(f"      Avg: {strategies_per_ticker.mean():.2f}")
    print(f"      Max: {strategies_per_ticker.max()}")
    
    # Strategy distribution
    print(f"\n📊 Strategy Distribution:")
    strategy_counts = df_strategies['Strategy_Name'].value_counts()
    for strategy, count in strategy_counts.items():
        print(f"   {strategy}: {count}")
    
    # Save Step 7 output
    step7_path = f"output/Step7_Recommended_{timestamp}.csv"
    df_strategies.to_csv(step7_path, index=False)
    print(f"\n💾 Saved: {step7_path}")
    
    # ========================================================================
    # STEP 9A: DTE Assignment
    # ========================================================================
    print_divider("STEP 9A: DTE Timeframe Assignment")
    
    df_timeframes = determine_timeframe(df_strategies)
    
    print(f"\n✅ Timeframe assignment complete:")
    print(f"   Rows: {len(df_timeframes)}")
    
    # DTE distribution
    if 'Min_DTE' in df_timeframes.columns and 'Max_DTE' in df_timeframes.columns:
        print(f"\n📊 DTE Ranges:")
        dte_ranges = df_timeframes.apply(
            lambda row: f"{row['Min_DTE']}-{row['Max_DTE']}", axis=1
        ).value_counts()
        for dte_range, count in dte_ranges.head(5).items():
            print(f"   {dte_range} days: {count} strategies")
    
    # Save Step 9A output
    step9a_path = f"output/Step9A_Timeframes_{timestamp}.csv"
    df_timeframes.to_csv(step9a_path, index=False)
    print(f"\n💾 Saved: {step9a_path}")
    
    # ========================================================================
    # STEP 11: Independent Evaluation
    # ========================================================================
    print_divider("STEP 11: Independent Evaluation")
    
    df_evaluated = evaluate_strategies_independently(df_timeframes)
    
    print(f"\n✅ Evaluation complete:")
    print(f"   Rows: {len(df_evaluated)}")
    
    # Validation status distribution
    if 'Validation_Status' in df_evaluated.columns:
        print(f"\n📊 Validation Status:")
        status_counts = df_evaluated['Validation_Status'].value_counts()
        for status, count in status_counts.items():
            print(f"   {status}: {count}")
        
        # Explain rejections
        if 'Reject' in status_counts.index:
            print(f"\n❌ Rejection Analysis:")
            print(f"   Expected: All strategies rejected (no Greeks/contracts yet)")
            print(f"   Reason: Step 9B (contract fetching) not run in this test")
            print(f"   Status: Normal for independent evaluation without contracts")
    
    # Save Step 11 output
    step11_path = f"output/Step11_Evaluated_{timestamp}.csv"
    df_evaluated.to_csv(step11_path, index=False)
    print(f"\n💾 Saved: {step11_path}")
    
    # ========================================================================
    # FINAL ASSESSMENT
    # ========================================================================
    print_divider("FINAL ASSESSMENT: DATA QUALITY & SANITY")
    
    print(f"\n✅ Files Created:")
    print(f"   {snapshot_path}")
    print(f"   {step3_path}")
    print(f"   {step5_path}")
    print(f"   {step6_path}")
    print(f"   {step7_path}")
    print(f"   {step9a_path}")
    print(f"   {step11_path}")
    
    print(f"\n🔍 Data Quality Assessment:")
    
    # Check price sanity
    price_issues = df_snapshot[
        (df_snapshot['last_price'] <= 0) | 
        (df_snapshot['last_price'].isna())
    ]
    if len(price_issues) > 0:
        print(f"   ⚠️ PRICE ISSUES: {len(price_issues)} tickers with bad prices")
        print(f"      Tickers: {price_issues['Ticker'].tolist()}")
    else:
        print(f"   ✅ Prices: All valid (> 0)")
    
    # Check HV sanity
    hv_issues = df_snapshot[
        (df_snapshot['hv_30'] <= 0) | 
        (df_snapshot['hv_30'] > 200) |
        (df_snapshot['hv_30'].isna())
    ]
    if len(hv_issues) > 0:
        print(f"   ⚠️ HV ISSUES: {len(hv_issues)} tickers with questionable HV")
        print(f"      HV range should be 5-100 typically")
        print(f"      Tickers: {hv_issues['Ticker'].tolist()}")
    else:
        print(f"   ✅ HV Values: All realistic (0-200%)")
    
    # Check indicator sanity
    if 'EMA9' in df_charted.columns:
        ema_issues = df_charted[df_charted['EMA9'].isna()]
        if len(ema_issues) > 0:
            print(f"   ⚠️ INDICATOR ISSUES: {len(ema_issues)} tickers with NaN indicators")
            print(f"      Tickers: {ema_issues['Ticker'].tolist()}")
        else:
            print(f"   ✅ Indicators: All computed successfully")
    
    # Check strategy generation
    if len(df_strategies) < len(df_validated):
        print(f"   ⚠️ STRATEGY GENERATION: Some tickers produced no strategies")
        no_strats = set(df_validated['Ticker']) - set(df_strategies['Ticker'])
        print(f"      Tickers with 0 strategies: {list(no_strats)}")
    else:
        avg_strats = len(df_strategies) / len(df_validated)
        if avg_strats < 2:
            print(f"   ⚠️ LOW STRATEGY COUNT: Avg {avg_strats:.1f} per ticker (expected 2-4)")
        else:
            print(f"   ✅ Strategy Generation: {avg_strats:.1f} strategies/ticker (healthy)")
    
    # Check volatility regimes
    if 'volatility_regime' in df_snapshot.columns:
        regime_counts = df_snapshot['volatility_regime'].value_counts()
        print(f"\n📊 Volatility Regime Distribution:")
        for regime, count in regime_counts.items():
            pct = (count / len(df_snapshot)) * 100
            print(f"   {regime}: {count} ({pct:.1f}%)")
    
    print(f"\n🎯 BLUNT ASSESSMENT:")
    
    # Overall sanity
    issues = []
    
    if len(price_issues) > 0:
        issues.append("Bad prices detected")
    
    if len(hv_issues) > 0:
        issues.append("Questionable HV values")
    
    if 'EMA9' in df_charted.columns and len(df_charted[df_charted['EMA9'].isna()]) > 0:
        issues.append("Indicator NaN cascade")
    
    if len(df_strategies) == 0:
        issues.append("Zero strategies generated (CRITICAL)")
    
    if len(issues) > 0:
        print(f"   ❌ RED FLAGS DETECTED:")
        for issue in issues:
            print(f"      - {issue}")
    else:
        print(f"   ✅ DATA LOOKS SANE:")
        print(f"      - Prices realistic")
        print(f"      - Volatility in normal ranges")
        print(f"      - Indicators computed without NaN cascade")
        print(f"      - Strategy generation working")
        print(f"      - Pipeline executed successfully")
    
    print_divider()

if __name__ == "__main__":
    main()
