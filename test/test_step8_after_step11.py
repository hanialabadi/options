"""
Test Step 8 after Step 11 integration
Quick CLI test to verify the new pipeline flow works end-to-end
"""

import pandas as pd
import logging
from core.scan_engine.step7_strategy_recommendation import recommend_strategies
from core.scan_engine.step9a_determine_timeframe import determine_option_timeframe
from core.scan_engine.step9b_fetch_contracts import fetch_and_select_contracts
from core.scan_engine.step10_pcs_recalibration import recalibrate_and_filter
from core.scan_engine.step11_strategy_pairing import compare_and_rank_strategies
from core.scan_engine.step8_position_sizing import finalize_and_size_positions

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_pipeline():
    """Test full pipeline: 7 → 9A → 9B → 10 → 11 → 8"""
    
    logger.info("=" * 80)
    logger.info("TESTING PIPELINE: Steps 7 → 9A → 9B → 10 → 11 → 8")
    logger.info("=" * 80)
    
    # Step 1: Create test input (simulate Step 6 output)
    logger.info("\n📦 Creating test input data...")
    test_tickers = ['AAPL', 'MSFT', 'GOOGL']  # Just 3 tickers for quick test
    
    test_data = []
    for ticker in test_tickers:
        test_data.append({
            'Ticker': ticker,
            'Current_Price': 150.0,
            'IV_Rank': 60.0,
            'HV_20': 25.0,
            'IVHV_gap_30D': 5.0,  # Required by Step 7
            'Signal_Type': 'High_IV',  # Required by Step 7
            'Regime': 'Neutral',  # Required by Step 7
            'Liquidity_Score': 85.0,
            'Market_Trend': 'Bullish',
            'Earnings_Within_45D': False,
            'Quality_Tier': 1
        })
    
    df_step6 = pd.DataFrame(test_data)
    logger.info(f"   Test data: {len(df_step6)} tickers")
    
    # Step 7: Strategy Recommendation
    logger.info("\n🎯 STEP 7: Strategy Recommendation")
    try:
        df_step7 = recommend_strategies(df_step6)
        logger.info(f"✅ Step 7 complete: {len(df_step7)} strategies")
        logger.info(f"   Strategies: {df_step7['Primary_Strategy'].value_counts().to_dict()}")
    except Exception as e:
        logger.error(f"❌ Step 7 failed: {e}")
        raise
    
    # Step 9A: Timeframe Logic
    logger.info("\n⏰ STEP 9A: Timeframe Logic")
    try:
        df_step9a = determine_option_timeframe(df_step7)
        logger.info(f"✅ Step 9A complete: {len(df_step9a)} strategies")
    except Exception as e:
        logger.error(f"❌ Step 9A failed: {e}")
        raise
    
    # Step 9B: Fetch Contracts (limit to first ticker)
    logger.info("\n📋 STEP 9B: Fetch Contracts (FIRST TICKER ONLY)")
    try:
        first_ticker = df_step9a['Ticker'].iloc[0]
        df_test = df_step9a[df_step9a['Ticker'] == first_ticker].copy()
        logger.info(f"   Testing with {first_ticker}: {len(df_test)} strategies")
        
        df_step9b = fetch_and_select_contracts(df_test)
        logger.info(f"✅ Step 9B complete: {len(df_step9b)} contracts fetched")
    except Exception as e:
        logger.error(f"❌ Step 9B failed: {e}")
        raise
    
    # Step 10: Filter & Validate
    logger.info("\n🔍 STEP 10: Filter & Validate Contracts")
    try:
        df_step10 = recalibrate_and_filter(df_step9b)
        logger.info(f"✅ Step 10 complete: {len(df_step10)} high-quality contracts")
    except Exception as e:
        logger.error(f"❌ Step 10 failed: {e}")
        raise
    
    # Step 11: Compare & Rank Strategies
    logger.info("\n🏆 STEP 11: Compare & Rank Strategies")
    try:
        df_step11 = compare_and_rank_strategies(
            df_step10,
            user_goal='income',
            account_size=100000,
            risk_tolerance='medium'
        )
        logger.info(f"✅ Step 11 complete: {len(df_step11)} ranked strategies")
        
        if 'Strategy_Rank' in df_step11.columns:
            rank_counts = df_step11['Strategy_Rank'].value_counts().sort_index()
            logger.info(f"   Rank distribution: {rank_counts.to_dict()}")
        
        if 'Comparison_Score' in df_step11.columns:
            logger.info(f"   Avg Comparison Score: {df_step11['Comparison_Score'].mean():.2f}")
    except Exception as e:
        logger.error(f"❌ Step 11 failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise
    
    # Step 8: Final Selection & Position Sizing
    logger.info("\n💰 STEP 8: Final Selection & Position Sizing")
    try:
        logger.info(f"   Input: {len(df_step11)} ranked strategies")
        logger.info(f"   Columns available: {list(df_step11.columns)[:15]}...")
        
        df_step8 = finalize_and_size_positions(
            df_step11,
            account_balance=100000,
            max_portfolio_risk=0.20,
            max_trade_risk=0.02,
            min_comparison_score=60.0,
            max_positions=50,
            sizing_method='volatility_scaled',
            risk_per_contract=500.0,
            diversification_limit=3
        )
        
        logger.info(f"✅ Step 8 complete: {len(df_step8)} final trades selected")
        
        if len(df_step8) > 0:
            logger.info("\n📊 FINAL RESULTS:")
            logger.info(f"   Final trades: {len(df_step8)}")
            logger.info(f"   Unique tickers: {df_step8['Ticker'].nunique()}")
            
            if 'Dollar_Allocation' in df_step8.columns:
                total_allocation = df_step8['Dollar_Allocation'].sum()
                logger.info(f"   Total allocation: ${total_allocation:,.0f}")
            
            if 'Num_Contracts' in df_step8.columns:
                total_contracts = df_step8['Num_Contracts'].sum()
                logger.info(f"   Total contracts: {total_contracts}")
            
            logger.info(f"\n   Sample trades:")
            display_cols = ['Ticker', 'Primary_Strategy', 'Comparison_Score', 
                          'Num_Contracts', 'Dollar_Allocation']
            display_cols = [c for c in display_cols if c in df_step8.columns]
            logger.info(f"\n{df_step8[display_cols].head().to_string()}")
        
    except Exception as e:
        logger.error(f"❌ Step 8 failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise
    
    logger.info("\n" + "=" * 80)
    logger.info("✅ PIPELINE TEST COMPLETE!")
    logger.info("=" * 80)
    
    return df_step8


if __name__ == '__main__':
    try:
        result = test_pipeline()
        print(f"\n✅ Test successful! Generated {len(result)} final trades")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
