"""
Full Pipeline Test: Steps 2 → 3 → 4 → 5 → 6 → 7 → 9A → 9B → 10 → 11 → 8
Debug each step and verify meaningful data output
Using 1 ticker (AAPL) for speed
"""

import pandas as pd
import logging
import json
from datetime import datetime

# Set up detailed logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Suppress excessive logging from other modules
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('polygon').setLevel(logging.WARNING)


def print_step_summary(step_name: str, df: pd.DataFrame, key_cols: list = None):
    """Print a summary of the current step's output"""
    print("\n" + "="*80)
    print(f"📊 {step_name}")
    print("="*80)
    print(f"Rows: {len(df)}")
    print(f"Columns: {len(df.columns)}")
    
    if key_cols:
        print(f"\n🔑 Key Columns:")
        for col in key_cols:
            if col in df.columns:
                if df[col].dtype in ['float64', 'float32', 'int64', 'int32']:
                    print(f"  {col}: {df[col].mean():.2f} (avg) | {df[col].min():.2f} - {df[col].max():.2f} (range)")
                elif df[col].dtype == 'object':
                    print(f"  {col}: {df[col].unique()[:5]}")
                else:
                    print(f"  {col}: {df[col].iloc[0] if len(df) > 0 else 'N/A'}")
    
    print(f"\n📋 Sample Row:")
    if len(df) > 0:
        sample_cols = [col for col in df.columns[:10]]
        print(df[sample_cols].head(1).to_string())
    print("="*80 + "\n")


def test_full_pipeline():
    """Run full pipeline with 1 ticker and detailed debugging"""
    
    ticker = 'AAPL'
    logger.info(f"🚀 STARTING FULL PIPELINE TEST WITH {ticker}")
    logger.info(f"Pipeline: 2 → 3 → 4 → 5 → 6 → 7 → 9A → 9B → 10 → 11 → 8")
    
    # ============================================================
    # STEP 1: Create Initial Input (Step 1 output)
    # ============================================================
    print("\n" + "🔷"*40)
    print("STEP 1: Initial Ticker Screening (Simulated)")
    print("🔷"*40)
    
    df_step1 = pd.DataFrame([{
        'Ticker': ticker,
        'Current_Price': 195.50,
        'Volume': 50000000,
        'Market_Cap': 3000000000000,
        'Sector': 'Technology'
    }])
    print(f"✅ Created test data for {ticker}")
    
    # ============================================================
    # STEP 2: Liquidity Filtering
    # ============================================================
    print("\n" + "🔷"*40)
    print("STEP 2: Liquidity Filtering")
    print("🔷"*40)
    
    try:
        from core.scan_engine.step2_liquidity import filter_by_liquidity
        df_step2 = filter_by_liquidity(df_step1)
        print_step_summary("STEP 2 OUTPUT", df_step2, 
                          ['Ticker', 'Volume', 'Liquidity_Score'])
    except Exception as e:
        logger.error(f"❌ Step 2 failed: {e}")
        import traceback
        traceback.print_exc()
        return None
    
    # ============================================================
    # STEP 3: IVHV Context
    # ============================================================
    print("\n" + "🔷"*40)
    print("STEP 3: IV/HV Context Analysis")
    print("🔷"*40)
    
    try:
        from core.scan_engine.step3_ivhv_context import calculate_ivhv_context
        df_step3 = calculate_ivhv_context(df_step2)
        print_step_summary("STEP 3 OUTPUT", df_step3,
                          ['Ticker', 'IV_Rank', 'HV_20', 'IVHV_gap_30D', 'IV_Context'])
    except Exception as e:
        logger.error(f"❌ Step 3 failed: {e}")
        import traceback
        traceback.print_exc()
        return None
    
    # ============================================================
    # STEP 4: Signal Generation
    # ============================================================
    print("\n" + "🔷"*40)
    print("STEP 4: Signal Generation")
    print("🔷"*40)
    
    try:
        from core.scan_engine.step4_signal_generator import generate_signals
        df_step4 = generate_signals(df_step3)
        print_step_summary("STEP 4 OUTPUT", df_step4,
                          ['Ticker', 'Signal_Type', 'Signal_Strength', 'Trade_Bias'])
    except Exception as e:
        logger.error(f"❌ Step 4 failed: {e}")
        import traceback
        traceback.print_exc()
        return None
    
    # ============================================================
    # STEP 5: Market Regime
    # ============================================================
    print("\n" + "🔷"*40)
    print("STEP 5: Market Regime Analysis")
    print("🔷"*40)
    
    try:
        from core.scan_engine.step5_market_regime import classify_market_regime
        df_step5 = classify_market_regime(df_step4)
        print_step_summary("STEP 5 OUTPUT", df_step5,
                          ['Ticker', 'Regime', 'Volatility_Regime', 'Trend_Strength'])
    except Exception as e:
        logger.error(f"❌ Step 5 failed: {e}")
        import traceback
        traceback.print_exc()
        return None
    
    # ============================================================
    # STEP 6: Quality Scoring
    # ============================================================
    print("\n" + "🔷"*40)
    print("STEP 6: Quality Scoring")
    print("🔷"*40)
    
    try:
        from core.scan_engine.step6_scoring import calculate_quality_scores
        df_step6 = calculate_quality_scores(df_step5)
        print_step_summary("STEP 6 OUTPUT", df_step6,
                          ['Ticker', 'Quality_Tier', 'PCS_Final', 'Confidence'])
    except Exception as e:
        logger.error(f"❌ Step 6 failed: {e}")
        import traceback
        traceback.print_exc()
        return None
    
    # ============================================================
    # STEP 7: Strategy Recommendation
    # ============================================================
    print("\n" + "🔷"*40)
    print("STEP 7: Strategy Recommendation (MULTI-STRATEGY)")
    print("🔷"*40)
    
    try:
        from core.scan_engine.step7_strategy_recommendation import recommend_strategies
        df_step7 = recommend_strategies(df_step6)
        print_step_summary("STEP 7 OUTPUT", df_step7,
                          ['Ticker', 'Primary_Strategy', 'Trade_Bias', 'Strategy_Tier'])
        
        if len(df_step7) > 0:
            print("\n📈 Strategies Recommended:")
            for idx, row in df_step7.iterrows():
                print(f"  • {row['Primary_Strategy']} (Tier {row.get('Strategy_Tier', 'N/A')}) - {row.get('Trade_Bias', 'N/A')}")
    except Exception as e:
        logger.error(f"❌ Step 7 failed: {e}")
        import traceback
        traceback.print_exc()
        return None
    
    # ============================================================
    # STEP 9A: Timeframe Determination
    # ============================================================
    print("\n" + "🔷"*40)
    print("STEP 9A: Timeframe Determination")
    print("🔷"*40)
    
    try:
        from core.scan_engine.step9a_determine_timeframe import determine_option_timeframe
        df_step9a = determine_option_timeframe(df_step7)
        print_step_summary("STEP 9A OUTPUT", df_step9a,
                          ['Ticker', 'Primary_Strategy', 'Min_DTE', 'Max_DTE', 'Preferred_DTE'])
        
        if len(df_step9a) > 0:
            print("\n⏰ Timeframes Assigned:")
            for idx, row in df_step9a.iterrows():
                print(f"  • {row['Primary_Strategy']}: {row.get('Min_DTE', 'N/A')}-{row.get('Max_DTE', 'N/A')} DTE (Preferred: {row.get('Preferred_DTE', 'N/A')})")
    except Exception as e:
        logger.error(f"❌ Step 9A failed: {e}")
        import traceback
        traceback.print_exc()
        return None
    
    # ============================================================
    # STEP 9B: Fetch Option Contracts
    # ============================================================
    print("\n" + "🔷"*40)
    print("STEP 9B: Fetch Option Contracts (LIVE API)")
    print("🔷"*40)
    print("⚠️  This may take 30-60 seconds...")
    
    try:
        from core.scan_engine.step9b_fetch_contracts import fetch_and_select_contracts
        df_step9b = fetch_and_select_contracts(df_step9a)
        print_step_summary("STEP 9B OUTPUT", df_step9b,
                          ['Ticker', 'Primary_Strategy', 'Selected_Expiration', 'Actual_DTE',
                           'Total_Debit', 'Total_Credit', 'Liquidity_Score'])
        
        if len(df_step9b) > 0:
            print("\n📋 Contracts Fetched:")
            for idx, row in df_step9b.iterrows():
                expiry = row.get('Selected_Expiration', 'N/A')
                dte = row.get('Actual_DTE', 'N/A')
                debit = row.get('Total_Debit', 0)
                credit = row.get('Total_Credit', 0)
                cost = debit if debit > 0 else credit
                print(f"  • {row['Primary_Strategy']}: {expiry} ({dte} DTE) - ${cost:.2f}")
    except Exception as e:
        logger.error(f"❌ Step 9B failed: {e}")
        import traceback
        traceback.print_exc()
        return None
    
    # ============================================================
    # STEP 10: Filter & Validate Contracts
    # ============================================================
    print("\n" + "🔷"*40)
    print("STEP 10: Filter & Validate Contracts")
    print("🔷"*40)
    
    try:
        from core.scan_engine.step10_pcs_recalibration import recalibrate_and_filter
        df_step10 = recalibrate_and_filter(df_step9b)
        print_step_summary("STEP 10 OUTPUT", df_step10,
                          ['Ticker', 'Primary_Strategy', 'Liquidity_Score', 
                           'Bid_Ask_Spread_Pct', 'Open_Interest'])
        
        if len(df_step10) > 0:
            print("\n✅ High-Quality Contracts:")
            for idx, row in df_step10.iterrows():
                liq = row.get('Liquidity_Score', 0)
                spread = row.get('Bid_Ask_Spread_Pct', 0)
                oi = row.get('Open_Interest', 0)
                print(f"  • {row['Primary_Strategy']}: Liq={liq:.1f}, Spread={spread:.1f}%, OI={oi}")
    except Exception as e:
        logger.error(f"❌ Step 10 failed: {e}")
        import traceback
        traceback.print_exc()
        return None
    
    # ============================================================
    # STEP 11: Compare & Rank Strategies
    # ============================================================
    print("\n" + "🔷"*40)
    print("STEP 11: Compare & Rank Strategies")
    print("🔷"*40)
    
    try:
        from core.scan_engine.step11_strategy_pairing import compare_and_rank_strategies
        df_step11 = compare_and_rank_strategies(
            df_step10,
            user_goal='income',
            account_size=100000,
            risk_tolerance='medium'
        )
        print_step_summary("STEP 11 OUTPUT", df_step11,
                          ['Ticker', 'Primary_Strategy', 'Strategy_Rank', 
                           'Comparison_Score', 'Greeks_Quality_Score'])
        
        if len(df_step11) > 0:
            print("\n🏆 Strategy Rankings:")
            for idx, row in df_step11.iterrows():
                rank = row.get('Strategy_Rank', 'N/A')
                score = row.get('Comparison_Score', 0)
                print(f"  #{rank}: {row['Primary_Strategy']} (Score: {score:.2f})")
    except Exception as e:
        logger.error(f"❌ Step 11 failed: {e}")
        import traceback
        traceback.print_exc()
        return None
    
    # ============================================================
    # STEP 8: Final Selection & Position Sizing
    # ============================================================
    print("\n" + "🔷"*40)
    print("STEP 8: Final Selection & Position Sizing")
    print("🔷"*40)
    
    try:
        from core.scan_engine.step8_position_sizing import finalize_and_size_positions
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
        print_step_summary("STEP 8 OUTPUT (FINAL)", df_step8,
                          ['Ticker', 'Primary_Strategy', 'Num_Contracts', 
                           'Dollar_Allocation', 'Max_Position_Risk'])
        
        if len(df_step8) > 0:
            print("\n💰 FINAL TRADE RECOMMENDATIONS:")
            for idx, row in df_step8.iterrows():
                contracts = row.get('Num_Contracts', 0)
                allocation = row.get('Dollar_Allocation', 0)
                risk = row.get('Max_Position_Risk', 0)
                print(f"\n  🎯 {row['Primary_Strategy']}:")
                print(f"     Contracts: {contracts}")
                print(f"     Allocation: ${allocation:,.0f}")
                print(f"     Max Risk: ${risk:,.0f}")
                if 'Comparison_Score' in row:
                    print(f"     Confidence: {row['Comparison_Score']:.2f}")
    except Exception as e:
        logger.error(f"❌ Step 8 failed: {e}")
        import traceback
        traceback.print_exc()
        return None
    
    # ============================================================
    # FINAL SUMMARY
    # ============================================================
    print("\n" + "="*80)
    print("✅ FULL PIPELINE TEST COMPLETE!")
    print("="*80)
    print(f"\n📊 Pipeline Summary for {ticker}:")
    print(f"  Step 1 (Input):          1 ticker")
    print(f"  Step 2 (Liquidity):      {len(df_step2)} tickers")
    print(f"  Step 3 (IVHV):           {len(df_step3)} tickers")
    print(f"  Step 4 (Signals):        {len(df_step4)} tickers")
    print(f"  Step 5 (Regime):         {len(df_step5)} tickers")
    print(f"  Step 6 (Quality):        {len(df_step6)} tickers")
    print(f"  Step 7 (Strategies):     {len(df_step7)} strategies")
    print(f"  Step 9A (Timeframes):    {len(df_step9a)} strategies")
    print(f"  Step 9B (Contracts):     {len(df_step9b)} strategies")
    print(f"  Step 10 (Filtered):      {len(df_step10)} strategies")
    print(f"  Step 11 (Ranked):        {len(df_step11)} strategies")
    print(f"  Step 8 (FINAL):          {len(df_step8)} trade(s)")
    print("="*80)
    
    return df_step8


if __name__ == '__main__':
    try:
        result = test_full_pipeline()
        if result is not None and len(result) > 0:
            print(f"\n✅ SUCCESS! Generated {len(result)} final trade(s)")
            
            # Save results
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"output/pipeline_test_{timestamp}.csv"
            result.to_csv(output_file, index=False)
            print(f"💾 Results saved to: {output_file}")
        else:
            print("\n❌ Pipeline completed but no trades generated")
    except Exception as e:
        print(f"\n❌ Pipeline test failed: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
