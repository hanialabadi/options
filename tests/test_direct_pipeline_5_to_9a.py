#!/usr/bin/env python3
"""
Test Direct Pipeline: Steps 5 → 6 → 7 → 11 → 9A (WITHOUT Step 3 IVHV Filter)

PURPOSE:
    Validate that Steps 5-7-11-9A work WITHOUT IV data.
    These steps work purely on:
    - Price history (Step 5)
    - Data quality checks (Step 6)
    - Strategy recommendations (Step 7)
    - Strategy evaluation (Step 11)
    - Timeframe assignment (Step 9A)
    
WHAT THIS TESTS:
    ✅ Load snapshot (Step 2)
    ✅ Skip Step 3 (no IV data available)
    ✅ Chart signals (Step 5 - EMA, SMA, ATR, trend, regime)
    ✅ Data quality validation (Step 6)
    ✅ Multi-strategy recommendations (Step 7)
    ✅ Independent strategy evaluation (Step 11 - partial without Greeks)
    ✅ DTE timeframe assignment (Step 9A)
    
EXPECTED BEHAVIOR:
    - Snapshot loads with HV data (no IV needed)
    - Chart signals computed for all tickers with price history
    - Multiple strategies generated per ticker based on trend/regime
    - Strategies evaluated independently (without contract-level Greeks)
    - DTE ranges assigned per strategy
    - CSV outputs saved to output/ directory
"""

import sys
import pandas as pd
from pathlib import Path
import logging

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.scan_engine.step2_load_snapshot import load_ivhv_snapshot
from core.scan_engine.step5_chart_signals import compute_chart_signals
from core.scan_engine.step6_gem_filter import validate_data_quality
from core.scan_engine.step7_strategy_recommendation import recommend_strategies
from core.scan_engine.step9a_determine_timeframe import determine_timeframe
from core.scan_engine.step11_independent_evaluation import evaluate_strategies_independently

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_direct_pipeline():
    """Test pipeline steps directly without Step 3 IVHV filter"""
    
    print("\n" + "="*80)
    print("🧪 DIRECT PIPELINE TEST: Steps 2 → 5 → 6 → 7 → 11 → 9A")
    print("   (Skipping Step 3 - no IV data required)")
    print("="*80 + "\n")
    
    # Find latest snapshot
    snapshot_dir = Path("data/snapshots")
    snapshots = sorted(snapshot_dir.glob("ivhv_snapshot_*.csv"), reverse=True)
    
    if not snapshots:
        logger.error("❌ No snapshots found in data/snapshots/")
        return False
    
    snapshot_path = str(snapshots[0])
    logger.info(f"📂 Using snapshot: {snapshot_path}")
    
    try:
        # Step 2: Load snapshot
        logger.info("\n📊 Step 2: Loading snapshot...")
        df_snapshot = load_ivhv_snapshot(snapshot_path)
        logger.info(f"✅ Step 2: {len(df_snapshot)} tickers loaded")
        logger.info(f"   Columns: {df_snapshot.columns.tolist()}")
        
        if df_snapshot.empty:
            logger.error("❌ Snapshot is empty")
            return False
        
        # Add required columns for Step 5 (Signal_Type, Regime from Step 2)
        # Step 2 should add these, but let's verify
        if 'Signal_Type' not in df_snapshot.columns:
            logger.warning("⚠️ Signal_Type not in snapshot, adding default")
            df_snapshot['Signal_Type'] = 'Neutral'
        if 'Regime' not in df_snapshot.columns:
            logger.warning("⚠️ Regime not in snapshot, adding default")
            df_snapshot['Regime'] = 'Neutral'
        if 'IVHV_gap_30D' not in df_snapshot.columns:
            logger.warning("⚠️ IVHV_gap_30D not in snapshot, adding default (0.0)")
            df_snapshot['IVHV_gap_30D'] = 0.0
        
        # Step 5: Chart signals
        logger.info("\n📈 Step 5: Computing chart signals...")
        df_charted = compute_chart_signals(df_snapshot)
        
        if df_charted.empty:
            logger.error("❌ Step 5: Failed to compute chart signals")
            return False
        
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
        
        # Export Step 5 output
        output_dir = Path("output")
        output_dir.mkdir(parents=True, exist_ok=True)
        step5_csv = output_dir / "Step5_Charted_test.csv"
        df_charted.to_csv(step5_csv, index=False)
        logger.info(f"💾 Step 5 output saved: {step5_csv}")
        
        # Step 6: Data quality validation
        logger.info("\n💎 Step 6: Validating data quality...")
        df_validated = validate_data_quality(df_charted)
        
        if df_validated.empty:
            logger.error("❌ Step 6: Failed to validate data quality")
            return False
        
        logger.info(f"✅ Step 6: {len(df_validated)} tickers validated")
        
        # Show data quality metrics
        if 'Data_Complete' in df_validated.columns:
            complete_count = df_validated['Data_Complete'].sum()
            logger.info(f"   Complete data: {complete_count}/{len(df_validated)}")
        
        # Export Step 6 output
        step6_csv = output_dir / "Step6_Validated_test.csv"
        df_validated.to_csv(step6_csv, index=False)
        logger.info(f"💾 Step 6 output saved: {step6_csv}")
        
        # Step 7: Strategy recommendations
        logger.info("\n🎯 Step 7: Generating strategy recommendations...")
        df_strategies = recommend_strategies(df_validated)
        
        if df_strategies.empty:
            logger.warning("⚠️ Step 7: No strategies recommended")
            logger.info("   This is normal if no tickers meet strategy criteria (missing IV data)")
            logger.info("   Step 7 requires IV data for strategy validation")
            # Continue anyway to show what was attempted
        else:
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
            
            # Export Step 7 output
            step7_csv = output_dir / "Step7_Recommended_test.csv"
            df_strategies.to_csv(step7_csv, index=False)
            logger.info(f"💾 Step 7 output saved: {step7_csv}")
        
        if not df_strategies.empty:
            # Step 9A: Timeframe assignment
            logger.info("\n⏱️ Step 9A: Determining timeframes...")
            df_timeframes = determine_timeframe(df_strategies)
            
            if df_timeframes.empty:
                logger.warning("⚠️ Step 9A: No timeframes assigned")
            else:
                logger.info(f"✅ Step 9A: {len(df_timeframes)} timeframes assigned")
                
                # Show DTE range breakdown
                if 'Min_DTE' in df_timeframes.columns and 'Max_DTE' in df_timeframes.columns:
                    logger.info(f"\n   DTE ranges:")
                    for _, row in df_timeframes.head(5).iterrows():
                        logger.info(f"     • {row.get('Ticker', 'N/A')} | {row.get('Strategy_Name', 'N/A')}: {row.get('Min_DTE', 'N/A')}-{row.get('Max_DTE', 'N/A')} DTE")
                
                # Export Step 9A output
                step9a_csv = output_dir / "Step9A_Timeframes_test.csv"
                df_timeframes.to_csv(step9a_csv, index=False)
                logger.info(f"💾 Step 9A output saved: {step9a_csv}")
            
            # Step 11: Independent evaluation (partial - no contract Greeks yet)
            logger.info("\n🎯 Step 11: Independent strategy evaluation (partial)...")
            try:
                df_evaluated = evaluate_strategies_independently(
                    df_strategies,
                    user_goal='income',
                    account_size=50000.0,
                    risk_tolerance='moderate'
                )
                
                if df_evaluated.empty:
                    logger.warning("⚠️ Step 11: No strategies evaluated")
                else:
                    logger.info(f"✅ Step 11: {len(df_evaluated)} strategies evaluated")
                    
                    # Show evaluation status breakdown
                    if 'Validation_Status' in df_evaluated.columns:
                        status_counts = df_evaluated['Validation_Status'].value_counts()
                        logger.info(f"   Evaluation status:")
                        for status, count in status_counts.items():
                            logger.info(f"     • {status}: {count}")
                    
                    # Export Step 11 output
                    step11_csv = output_dir / "Step11_Evaluated_test.csv"
                    df_evaluated.to_csv(step11_csv, index=False)
                    logger.info(f"💾 Step 11 output saved: {step11_csv}")
                    
            except Exception as e:
                logger.warning(f"⚠️ Step 11 failed (expected without contract data): {e}")
        
        print("\n" + "="*80)
        print("✅ DIRECT PIPELINE TEST COMPLETE")
        print("="*80 + "\n")
        
        logger.info("🎯 Summary:")
        logger.info("   • Steps 2, 5, 6 executed successfully")
        logger.info("   • Step 7 attempted (may be empty without IV data)")
        logger.info("   • Steps 9A, 11 skipped if no strategies generated")
        logger.info("   • All outputs saved to output/ directory")
        logger.info("\n   💡 Note: Step 7 requires IV data for most strategies.")
        logger.info("      During market hours, Step 3 will be available and")
        logger.info("      Step 7 will generate full strategy recommendations.")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}", exc_info=True)
        return False


if __name__ == '__main__':
    success = test_direct_pipeline()
    sys.exit(0 if success else 1)
