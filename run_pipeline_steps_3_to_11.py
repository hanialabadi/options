#!/usr/bin/env python3
"""
Pipeline Runner: Steps 3 → 11 (Schwab-First Architecture)

Executes the full options pipeline from IV/HV filtering through independent evaluation.
Uses the latest Schwab-generated snapshot as input.

Steps:
    3  - IV/HV Filter & Volatility Regime Classification
    5  - Chart Signals (Schwab price history)
    6  - Data Quality & Regime Validation
    7  - Strategy Recommendation (multi-strategy per ticker)
    9A - Timeframe Assignment
    11 - Independent Evaluation (pre-Greeks)

Requirements:
    - Fresh Step 0 snapshot in data/snapshots/
    - Schwab auth tokens valid
    - All dependencies installed
"""

import sys
import os
from pathlib import Path
import pandas as pd
import logging
from datetime import datetime
import json

# Add project root to path
project_root = Path(__file__).parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from core.scan_engine.step2_load_snapshot import load_ivhv_snapshot
from core.scan_engine.step3_filter_ivhv import filter_ivhv_gap
from core.scan_engine.step5_chart_signals import compute_chart_signals
from core.scan_engine.step6_gem_filter import validate_data_quality
from core.scan_engine.step7_strategy_recommendation import recommend_strategies
from core.scan_engine.step9a_determine_timeframe import determine_timeframe
from core.scan_engine.step11_independent_evaluation import evaluate_strategies_independently

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def save_step_output(df: pd.DataFrame, step_name: str) -> Path:
    """Save step output to CSV with timestamp."""
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{step_name}_{timestamp}.csv"
    output_path = output_dir / filename
    
    df.to_csv(output_path, index=False)
    logger.info(f"💾 Saved {step_name}: {output_path} ({len(df)} rows)")
    return output_path


def print_step_summary(df: pd.DataFrame, step_name: str, prev_rows: int = None):
    """Print summary statistics for a pipeline step."""
    logger.info(f"\n{'='*70}")
    logger.info(f"📊 {step_name} SUMMARY")
    logger.info(f"{'='*70}")
    logger.info(f"Rows: {len(df)}")
    if prev_rows:
        dropped = prev_rows - len(df)
        pct = (dropped / prev_rows * 100) if prev_rows > 0 else 0
        logger.info(f"Dropped: {dropped} ({pct:.1f}%)")
    
    # Count unique tickers
    if 'Ticker' in df.columns:
        unique_tickers = df['Ticker'].nunique()
        logger.info(f"Unique tickers: {unique_tickers}")
        if unique_tickers > 0:
            strategies_per_ticker = len(df) / unique_tickers
            logger.info(f"Avg strategies/ticker: {strategies_per_ticker:.2f}")
    
    # Check for IV availability
    if 'Has_IV' in df.columns:
        has_iv_count = df['Has_IV'].sum()
        has_iv_pct = (has_iv_count / len(df) * 100) if len(df) > 0 else 0
        logger.info(f"Has IV: {has_iv_count} ({has_iv_pct:.1f}%)")
    
    # Strategy types if available
    if 'Strategy' in df.columns:
        strategy_counts = df['Strategy'].value_counts()
        logger.info(f"\nStrategy distribution:")
        for strategy, count in strategy_counts.items():
            logger.info(f"  {strategy}: {count}")
    
    logger.info(f"{'='*70}\n")


def print_sample_rows(df: pd.DataFrame, n: int = 3):
    """Print sample rows from dataframe."""
    if len(df) == 0:
        logger.warning("⚠️  No rows to display")
        return
    
    sample = df.head(n)
    logger.info("\n📋 Sample rows:")
    for idx, row in sample.iterrows():
        ticker = row.get('Ticker', 'N/A')
        strategy = row.get('Strategy', 'N/A')
        has_iv = row.get('Has_IV', 'N/A')
        logger.info(f"  {ticker} | {strategy} | Has_IV: {has_iv}")


def main():
    """Main pipeline execution."""
    logger.info("\n" + "="*70)
    logger.info("🚀 PIPELINE EXECUTION: Steps 3 → 11")
    logger.info("="*70)
    
    start_time = datetime.now()
    results = {}
    
    try:
        # ===================================================================
        # STEP 2: Load latest snapshot and enrich
        # ===================================================================
        logger.info("\n📂 Loading latest snapshot from Step 0...")
        snapshot_dir = Path("data/snapshots")
        snapshot_files = list(snapshot_dir.glob("ivhv_snapshot_live_*.csv"))
        
        if not snapshot_files:
            raise FileNotFoundError("No Step 0 snapshot found in data/snapshots/")
        
        latest_snapshot = max(snapshot_files, key=lambda p: p.stat().st_mtime)
        logger.info(f"✅ Found snapshot: {latest_snapshot.name}")
        
        # Use Step 2's loader which enriches the data
        logger.info("📊 Step 2: Loading and enriching snapshot...")
        df = load_ivhv_snapshot(str(latest_snapshot))
        logger.info(f"✅ Loaded and enriched {len(df)} rows")
        results['step0_rows'] = len(df)
        
        # ===================================================================
        # STEP 3: IV/HV Filter
        # ===================================================================
        logger.info("\n🔍 STEP 3: IV/HV Filtering...")
        prev_rows = len(df)
        
        df_step3 = filter_ivhv_gap(df, min_gap=2.0)
        results['step3_rows'] = len(df_step3)
        
        print_step_summary(df_step3, "Step 3: IV/HV Filter", prev_rows)
        save_step_output(df_step3, "Step3_Filtered")
        
        if len(df_step3) == 0:
            raise ValueError("❌ Step 3 produced 0 rows. Pipeline halted.")
        
        # ===================================================================
        # STEP 5: Chart Signals
        # ===================================================================
        logger.info("\n📈 STEP 5: Chart Signals...")
        prev_rows = len(df_step3)
        
        df_step5 = compute_chart_signals(df_step3)
        results['step5_rows'] = len(df_step5)
        
        print_step_summary(df_step5, "Step 5: Chart Signals", prev_rows)
        save_step_output(df_step5, "Step5_Charted")
        
        if len(df_step5) == 0:
            raise ValueError("❌ Step 5 produced 0 rows. Pipeline halted.")
        
        # ===================================================================
        # STEP 6: Data Quality & Validation
        # ===================================================================
        logger.info("\n✅ STEP 6: Data Quality Validation...")
        prev_rows = len(df_step5)
        
        df_step6 = validate_data_quality(df_step5)
        results['step6_rows'] = len(df_step6)
        
        print_step_summary(df_step6, "Step 6: Validated", prev_rows)
        save_step_output(df_step6, "Step6_Validated")
        
        if len(df_step6) == 0:
            raise ValueError("❌ Step 6 produced 0 rows. Pipeline halted.")
        
        # ===================================================================
        # STEP 7: Strategy Recommendation
        # ===================================================================
        logger.info("\n🎯 STEP 7: Strategy Recommendation...")
        prev_rows = len(df_step6)
        
        df_step7 = recommend_strategies(df_step6)
        results['step7_rows'] = len(df_step7)
        
        print_step_summary(df_step7, "Step 7: Strategies", prev_rows)
        save_step_output(df_step7, "Step7_Recommended")
        print_sample_rows(df_step7, n=5)
        
        if len(df_step7) == 0:
            raise ValueError("❌ Step 7 produced 0 rows. Pipeline halted.")
        
        # Check strategies per ticker
        if 'Ticker' in df_step7.columns:
            unique_tickers_step7 = df_step7['Ticker'].nunique()
            avg_strategies = len(df_step7) / unique_tickers_step7 if unique_tickers_step7 > 0 else 0
            if avg_strategies < 2.0:
                logger.warning(f"⚠️  Low strategy density: {avg_strategies:.2f} strategies/ticker")
                logger.warning("   Expected: ~2+ strategies per ticker for diversification")
        
        # ===================================================================
        # STEP 9A: Timeframe Assignment
        # ===================================================================
        logger.info("\n⏰ STEP 9A: Timeframe Assignment...")
        prev_rows = len(df_step7)
        
        df_step9a = determine_timeframe(df_step7)
        results['step9a_rows'] = len(df_step9a)
        
        print_step_summary(df_step9a, "Step 9A: Timeframes", prev_rows)
        save_step_output(df_step9a, "Step9A_Timeframes")
        print_sample_rows(df_step9a, n=5)
        
        if len(df_step9a) == 0:
            raise ValueError("❌ Step 9A produced 0 rows. Pipeline halted.")
        
        # ===================================================================
        # STEP 11: Independent Evaluation
        # ===================================================================
        logger.info("\n🔬 STEP 11: Independent Evaluation...")
        prev_rows = len(df_step9a)
        
        df_step11 = evaluate_strategies_independently(df_step9a)
        results['step11_rows'] = len(df_step11)
        
        print_step_summary(df_step11, "Step 11: Evaluated", prev_rows)
        save_step_output(df_step11, "Step11_Evaluated")
        print_sample_rows(df_step11, n=5)
        
        if len(df_step11) == 0:
            raise ValueError("❌ Step 11 produced 0 rows. Pipeline halted.")
        
        # ===================================================================
        # FINAL SUMMARY
        # ===================================================================
        duration = (datetime.now() - start_time).total_seconds()
        
        logger.info("\n" + "="*70)
        logger.info("✅ PIPELINE COMPLETE")
        logger.info("="*70)
        logger.info(f"Duration: {duration:.1f}s")
        logger.info(f"\nRow counts by step:")
        logger.info(f"  Step 0 (Input):    {results.get('step0_rows', 0)}")
        logger.info(f"  Step 3 (Filter):   {results.get('step3_rows', 0)}")
        logger.info(f"  Step 5 (Charts):   {results.get('step5_rows', 0)}")
        logger.info(f"  Step 6 (Validate): {results.get('step6_rows', 0)}")
        logger.info(f"  Step 7 (Strategy): {results.get('step7_rows', 0)}")
        logger.info(f"  Step 9A (Time):    {results.get('step9a_rows', 0)}")
        logger.info(f"  Step 11 (Eval):    {results.get('step11_rows', 0)}")
        
        # Calculate attrition rate
        if results.get('step0_rows', 0) > 0:
            final_pct = (results.get('step11_rows', 0) / results['step0_rows']) * 100
            logger.info(f"\nFinal attrition: {100 - final_pct:.1f}% filtered")
        
        # Check for IV blocking
        if 'Has_IV' in df_step11.columns:
            blocked = (~df_step11['Has_IV']).sum()
            blocked_pct = (blocked / len(df_step11) * 100) if len(df_step11) > 0 else 0
            logger.info(f"\nBlocked by missing IV: {blocked} ({blocked_pct:.1f}%)")
        
        logger.info("="*70)
        
        # Save results summary
        results['duration_seconds'] = duration
        results['timestamp'] = datetime.now().isoformat()
        results_path = Path("output") / f"pipeline_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(results_path, 'w') as f:
            json.dump(results, f, indent=2)
        logger.info(f"\n📊 Results summary saved: {results_path}")
        
        return df_step11
        
    except Exception as e:
        logger.error(f"\n❌ Pipeline failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
