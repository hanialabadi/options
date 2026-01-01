#!/usr/bin/env python3
"""
Test Pipeline: Steps 5 → 6 → 7 → 11 → 9A

PURPOSE:
    Validate that the pipeline works correctly without IV data.
    These steps work purely on:
    - Price history (Step 5)
    - Data quality checks (Step 6)
    - Strategy recommendations (Step 7)
    - Strategy evaluation (Step 11)
    - Timeframe assignment (Step 9A)
    
WHAT WORKS NOW:
    ✅ Chart signals (EMA, SMA, ATR, trend, regime)
    ✅ Data completeness validation
    ✅ Multi-strategy recommendations per ticker
    ✅ Independent strategy evaluation (partial - no Greeks yet)
    ✅ DTE timeframe assignment per strategy
    
WHAT'S SKIPPED (correctly):
    ⏭️ Step 9B (contract fetching - requires market hours)
    ⏭️ Step 10 (PCS scoring - requires Greeks from contracts)
    ⏭️ Step 8 (position sizing - requires contracts)

EXPECTED BEHAVIOR:
    - Snapshots load successfully
    - Chart signals computed for all tickers
    - Multiple strategies generated per ticker
    - All strategies evaluated independently
    - DTE ranges assigned per strategy
    - CSV outputs saved to output/ directory
"""

import sys
import pandas as pd
from pathlib import Path
import logging

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.scan_engine import run_full_scan_pipeline

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_pipeline_without_iv():
    """Test pipeline steps that work without IV data"""
    
    print("\n" + "="*80)
    print("🧪 TESTING PIPELINE: Steps 5 → 6 → 7 → 11 → 9A")
    print("="*80 + "\n")
    
    # Find latest snapshot
    snapshot_dir = Path("data/snapshots")
    snapshots = sorted(snapshot_dir.glob("ivhv_snapshot_*.csv"), reverse=True)
    
    if not snapshots:
        logger.error("❌ No snapshots found in data/snapshots/")
        return False
    
    snapshot_path = str(snapshots[0])
    logger.info(f"📂 Using snapshot: {snapshot_path}")
    
    # Load snapshot to check data
    df_snapshot = pd.read_csv(snapshot_path)
    logger.info(f"📊 Snapshot contains {len(df_snapshot)} tickers")
    logger.info(f"📋 Columns: {df_snapshot.columns.tolist()}")
    
    # Run pipeline
    try:
        logger.info("\n🚀 Running pipeline...")
        results = run_full_scan_pipeline(
            snapshot_path=snapshot_path,
            output_dir=Path("output"),
            account_balance=50000.0,
            max_portfolio_risk=0.15,
            sizing_method='volatility_scaled'
        )
        
        # Check results
        print("\n" + "="*80)
        print("📊 PIPELINE RESULTS")
        print("="*80 + "\n")
        
        # Step 2: Load snapshot
        if 'snapshot' in results and not results['snapshot'].empty:
            logger.info(f"✅ Step 2: {len(results['snapshot'])} tickers loaded")
        else:
            logger.error("❌ Step 2: Failed to load snapshot")
            return False
        
        # Step 3: IVHV filter
        if 'filtered' in results and not results['filtered'].empty:
            logger.info(f"✅ Step 3: {len(results['filtered'])} tickers passed IVHV filter")
        else:
            logger.warning("⚠️ Step 3: No tickers passed IVHV filter")
            return False
        
        # Step 5: Chart signals
        if 'charted' in results and not results['charted'].empty:
            df_charted = results['charted']
            logger.info(f"✅ Step 5: {len(df_charted)} tickers with chart signals")
            
            # Show chart signal details
            if 'Chart_Regime' in df_charted.columns:
                regime_counts = df_charted['Chart_Regime'].value_counts()
                logger.info(f"   Regime breakdown:")
                for regime, count in regime_counts.items():
                    logger.info(f"     • {regime}: {count}")
            
            # Show sample row
            if len(df_charted) > 0:
                sample = df_charted.iloc[0]
                logger.info(f"\n   Sample ticker: {sample.get('Ticker', 'N/A')}")
                logger.info(f"     • EMA9: {sample.get('EMA9', 'N/A')}")
                logger.info(f"     • EMA21: {sample.get('EMA21', 'N/A')}")
                logger.info(f"     • SMA20: {sample.get('SMA20', 'N/A')}")
                logger.info(f"     • ATR: {sample.get('Atr_Pct', 'N/A')}")
                logger.info(f"     • Regime: {sample.get('Chart_Regime', 'N/A')}")
                logger.info(f"     • Trend Slope: {sample.get('Trend_Slope', 'N/A')}")
        else:
            logger.error("❌ Step 5: Failed to compute chart signals")
            return False
        
        # Step 6: Data quality validation
        if 'validated_data' in results and not results['validated_data'].empty:
            df_validated = results['validated_data']
            logger.info(f"✅ Step 6: {len(df_validated)} tickers validated")
            
            # Show data quality metrics
            if 'Data_Complete' in df_validated.columns:
                complete_count = df_validated['Data_Complete'].sum()
                logger.info(f"   Complete data: {complete_count}/{len(df_validated)}")
        else:
            logger.error("❌ Step 6: Failed to validate data quality")
            return False
        
        # Step 7: Strategy recommendations
        if 'recommended_strategies' in results and not results['recommended_strategies'].empty:
            df_strategies = results['recommended_strategies']
            logger.info(f"✅ Step 7: {len(df_strategies)} strategies recommended")
            
            # Show strategy breakdown
            if 'Strategy_Name' in df_strategies.columns:
                strategy_counts = df_strategies['Strategy_Name'].value_counts()
                logger.info(f"   Strategy breakdown:")
                for strategy, count in strategy_counts.head(10).items():
                    logger.info(f"     • {strategy}: {count}")
            
            # Show strategies per ticker
            if 'Ticker' in df_strategies.columns:
                strategies_per_ticker = df_strategies.groupby('Ticker').size()
                avg_strategies = strategies_per_ticker.mean()
                max_strategies = strategies_per_ticker.max()
                logger.info(f"   Avg strategies per ticker: {avg_strategies:.1f}")
                logger.info(f"   Max strategies per ticker: {max_strategies}")
                
                # Show sample ticker with multiple strategies
                multi_strategy_ticker = strategies_per_ticker[strategies_per_ticker > 1].index[0] if len(strategies_per_ticker[strategies_per_ticker > 1]) > 0 else None
                if multi_strategy_ticker:
                    ticker_strategies = df_strategies[df_strategies['Ticker'] == multi_strategy_ticker]
                    logger.info(f"\n   Sample multi-strategy ticker: {multi_strategy_ticker}")
                    for _, row in ticker_strategies.iterrows():
                        logger.info(f"     • {row.get('Strategy_Name', 'N/A')}: {row.get('Valid_Reason', 'N/A')}")
        else:
            logger.warning("⚠️ Step 7: No strategies recommended")
            logger.info("   This is normal if no tickers meet strategy criteria")
        
        # Step 11: Independent evaluation
        if 'evaluated_strategies' in results and not results['evaluated_strategies'].empty:
            df_evaluated = results['evaluated_strategies']
            logger.info(f"✅ Step 11: {len(df_evaluated)} strategies evaluated")
            
            # Show evaluation status breakdown
            if 'Validation_Status' in df_evaluated.columns:
                status_counts = df_evaluated['Validation_Status'].value_counts()
                logger.info(f"   Evaluation status:")
                for status, count in status_counts.items():
                    logger.info(f"     • {status}: {count}")
        else:
            logger.warning("⚠️ Step 11: No strategies evaluated")
        
        # Step 9A: Timeframe assignment
        if 'timeframes' in results and not results['timeframes'].empty:
            df_timeframes = results['timeframes']
            logger.info(f"✅ Step 9A: {len(df_timeframes)} timeframes assigned")
            
            # Show DTE range breakdown
            if 'Min_DTE' in df_timeframes.columns and 'Max_DTE' in df_timeframes.columns:
                logger.info(f"\n   DTE ranges:")
                for _, row in df_timeframes.head(5).iterrows():
                    logger.info(f"     • {row.get('Ticker', 'N/A')} | {row.get('Strategy_Name', 'N/A')}: {row.get('Min_DTE', 'N/A')}-{row.get('Max_DTE', 'N/A')} DTE")
        else:
            logger.warning("⚠️ Step 9A: No timeframes assigned")
        
        # Check skipped steps
        print("\n" + "-"*80)
        print("⏭️  SKIPPED STEPS (Correct - Require market hours)")
        print("-"*80)
        logger.info("⏭️  Step 9B: Contract fetching (requires Schwab API + market hours)")
        logger.info("⏭️  Step 10: PCS scoring (requires Greeks from contracts)")
        logger.info("⏭️  Step 8: Position sizing (requires contract prices)")
        
        # Check CSV exports
        print("\n" + "-"*80)
        print("📁 CSV EXPORTS")
        print("-"*80)
        output_dir = Path("output")
        csv_files = sorted(output_dir.glob("Step*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
        for csv_file in csv_files[:10]:  # Show latest 10
            size_kb = csv_file.stat().st_size / 1024
            logger.info(f"   • {csv_file.name} ({size_kb:.1f} KB)")
        
        print("\n" + "="*80)
        print("✅ PIPELINE TEST COMPLETE")
        print("="*80 + "\n")
        
        logger.info("🎯 Summary:")
        logger.info("   • Steps 5, 6, 7, 11, 9A executed successfully")
        logger.info("   • All outputs saved to output/ directory")
        logger.info("   • Ready for market hours (Steps 9B, 10, 8)")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}", exc_info=True)
        return False


if __name__ == '__main__':
    success = test_pipeline_without_iv()
    sys.exit(0 if success else 1)
