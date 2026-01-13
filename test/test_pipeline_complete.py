"""
Full Pipeline Test Using Existing pipeline.py
Test with 1 ticker and inspect output at each step
"""

import pandas as pd
import logging
from core.scan_engine.pipeline import run_full_scan_pipeline

# Set up detailed logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def inspect_dataframe(df: pd.DataFrame, step_name: str, key_cols: list = None):
    """Print detailed inspection of a DataFrame"""
    print("\n" + "="*80)
    print(f"📊 {step_name}")
    print("="*80)
    
    if df is None or len(df) == 0:
        print("⚠️  Empty or None DataFrame")
        print("="*80)
        return
    
    print(f"Rows: {len(df)}")
    print(f"Columns: {len(df.columns)}")
    
    if key_cols:
        print(f"\n🔑 Key Columns:")
        for col in key_cols:
            if col in df.columns:
                if df[col].dtype in ['float64', 'float32']:
                    vals = df[col].dropna()
                    if len(vals) > 0:
                        print(f"  {col}: {vals.mean():.2f} (avg) | {vals.min():.2f} - {vals.max():.2f}")
                    else:
                        print(f"  {col}: (all null)")
                elif df[col].dtype in ['int64', 'int32']:
                    vals = df[col].dropna()
                    if len(vals) > 0:
                        print(f"  {col}: {int(vals.mean())} (avg) | {int(vals.min())} - {int(vals.max())}")
                    else:
                        print(f"  {col}: (all null)")
                elif df[col].dtype == 'object':
                    uniques = df[col].unique()[:5]
                    print(f"  {col}: {list(uniques)}")
                elif df[col].dtype == 'bool':
                    print(f"  {col}: {df[col].value_counts().to_dict()}")
                else:
                    print(f"  {col}: {df[col].iloc[0] if len(df) > 0 else 'N/A'}")
    
    # Show all column names
    print(f"\n📋 All Columns ({len(df.columns)}):")
    cols_per_line = 4
    for i in range(0, len(df.columns), cols_per_line):
        cols_chunk = df.columns[i:i+cols_per_line]
        print(f"  {', '.join(cols_chunk)}")
    
    # Show sample row
    print(f"\n📝 Sample Data (first row):")
    if len(df) > 0:
        for col in df.columns[:15]:  # Show first 15 columns
            val = df[col].iloc[0]
            if isinstance(val, float):
                print(f"  {col}: {val:.2f}")
            else:
                print(f"  {col}: {val}")
    
    print("="*80)


def test_pipeline_with_one_ticker():
    """
    Run full pipeline and inspect each step's output
    Uses existing data in data/ folder
    """
    
    print("\n" + "🚀"*40)
    print("FULL PIPELINE TEST - USING EXISTING DATA")
    print("🚀"*40)
    print("\nPipeline: 2 → 3 → 5 → 6 → 7 → 9A → 9B → 10 → 11 → 8")
    print("\nUsing most recent snapshot from data/ folder")
    
    # Run full pipeline
    try:
        results = run_full_scan_pipeline(
            include_step7=True,
            include_step8=True,
            include_step9a=True,
            include_step9b=True,
            include_step10=True,
            include_step11=True,
            account_balance=100000,
            max_portfolio_risk=0.20,
            sizing_method='volatility_scaled',
            pcs_min_liquidity=30.0,
            pcs_max_spread=8.0,
            pcs_strict_mode=False
        )
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return None
    
    # ============================================================
    # INSPECT EACH STEP'S OUTPUT
    # ============================================================
    
    # Step 2: Snapshot
    if 'snapshot' in results and results['snapshot'] is not None:
        df = results['snapshot']
        inspect_dataframe(df, "STEP 2: IV/HV Snapshot",
                         ['Ticker', 'Current_Price', 'IV_Rank', 'HV_20', 'Volume'])
        
        # Limit to one ticker for detailed testing
        if len(df) > 0:
            test_ticker = df['Ticker'].iloc[0]
            print(f"\n🎯 Filtering to single ticker for detailed testing: {test_ticker}")
            
            # Filter all results to this one ticker
            for key in results.keys():
                if results[key] is not None and isinstance(results[key], pd.DataFrame):
                    if 'Ticker' in results[key].columns:
                        results[key] = results[key][results[key]['Ticker'] == test_ticker].copy()
    
    # Step 3: IVHV Filtered
    if 'filtered' in results:
        inspect_dataframe(results['filtered'], "STEP 3: IVHV Filtered",
                         ['Ticker', 'IV_Rank', 'HV_20', 'IVHV_gap_30D', 'IV_Context'])
    
    # Step 5: Chart Signals
    if 'charted' in results:
        inspect_dataframe(results['charted'], "STEP 5: Chart Signals",
                         ['Ticker', 'Signal_Type', 'Signal_Strength', 'Trade_Bias', 'Regime'])
    
    # Step 6: Data Quality
    if 'validated_data' in results:
        inspect_dataframe(results['validated_data'], "STEP 6: Data Quality",
                         ['Ticker', 'Quality_Tier', 'PCS_Final', 'Confidence'])
    
    # Step 7: Strategy Recommendations
    if 'recommendations' in results:
        inspect_dataframe(results['recommendations'], "STEP 7: Strategy Recommendations",
                         ['Ticker', 'Primary_Strategy', 'Trade_Bias', 'Strategy_Tier', 'PCS_Final'])
        
        if results['recommendations'] is not None and len(results['recommendations']) > 0:
            print("\n📈 Strategies Recommended:")
            df = results['recommendations']
            for idx, row in df.iterrows():
                tier = row.get('Strategy_Tier', 'N/A')
                bias = row.get('Trade_Bias', 'N/A')
                pcs = row.get('PCS_Final', 0)
                print(f"  • {row['Primary_Strategy']} (Tier {tier}, {bias}, PCS: {pcs:.1f})")
    
    # Step 9A: Timeframes
    if 'timeframed_positions' in results:
        inspect_dataframe(results['timeframed_positions'], "STEP 9A: Timeframes",
                         ['Ticker', 'Primary_Strategy', 'Min_DTE', 'Max_DTE', 'Preferred_DTE'])
        
        if results['timeframed_positions'] is not None and len(results['timeframed_positions']) > 0:
            print("\n⏰ Timeframes Assigned:")
            df = results['timeframed_positions']
            for idx, row in df.iterrows():
                strategy = row['Primary_Strategy']
                min_dte = row.get('Min_DTE', 'N/A')
                max_dte = row.get('Max_DTE', 'N/A')
                pref_dte = row.get('Preferred_DTE', 'N/A')
                print(f"  • {strategy}: {min_dte}-{max_dte} DTE (Preferred: {pref_dte})")
    
    # Step 9B: Contracts
    if 'selected_contracts' in results:
        inspect_dataframe(results['selected_contracts'], "STEP 9B: Option Contracts",
                         ['Ticker', 'Primary_Strategy', 'Selected_Expiration', 'Actual_DTE',
                          'Total_Debit', 'Total_Credit', 'Liquidity_Score', 'Contract_Selection_Status'])
        
        if results['selected_contracts'] is not None and len(results['selected_contracts']) > 0:
            print("\n📋 Contracts Fetched:")
            df = results['selected_contracts']
            for idx, row in df.iterrows():
                strategy = row['Primary_Strategy']
                expiry = row.get('Selected_Expiration', 'N/A')
                dte = row.get('Actual_DTE', 'N/A')
                debit = row.get('Total_Debit', 0)
                credit = row.get('Total_Credit', 0)
                status = row.get('Contract_Selection_Status', 'N/A')
                cost = debit if debit > 0 else credit
                print(f"  • {strategy}: {expiry} ({dte} DTE) - ${cost:.2f} [{status}]")
    
    # Step 10: Filtered Contracts
    if 'filtered_contracts' in results:
        inspect_dataframe(results['filtered_contracts'], "STEP 10: Filtered Contracts",
                         ['Ticker', 'Primary_Strategy', 'Liquidity_Score', 
                          'Bid_Ask_Spread_Pct', 'Open_Interest'])
    
    # Step 11: Ranked Strategies
    if 'ranked_strategies' in results:
        inspect_dataframe(results['ranked_strategies'], "STEP 11: Ranked Strategies",
                         ['Ticker', 'Primary_Strategy', 'Strategy_Rank', 
                          'Comparison_Score', 'Greeks_Quality_Score'])
        
        if results['ranked_strategies'] is not None and len(results['ranked_strategies']) > 0:
            print("\n🏆 Strategy Rankings:")
            df = results['ranked_strategies']
            for idx, row in df.iterrows():
                strategy = row['Primary_Strategy']
                rank = row.get('Strategy_Rank', 'N/A')
                score = row.get('Comparison_Score', 0)
                print(f"  #{rank}: {strategy} (Score: {score:.2f})")
    
    # Step 8: Final Trades
    if 'final_trades' in results:
        inspect_dataframe(results['final_trades'], "STEP 8: Final Trades (FINAL OUTPUT)",
                         ['Ticker', 'Primary_Strategy', 'Num_Contracts', 
                          'Dollar_Allocation', 'Max_Position_Risk', 'Position_Valid'])
        
        if results['final_trades'] is not None and len(results['final_trades']) > 0:
            print("\n💰 FINAL TRADE RECOMMENDATIONS:")
            df = results['final_trades']
            for idx, row in df.iterrows():
                strategy = row['Primary_Strategy']
                contracts = row.get('Num_Contracts', 0)
                allocation = row.get('Dollar_Allocation', 0)
                risk = row.get('Max_Position_Risk', 0)
                score = row.get('Comparison_Score', 0)
                print(f"\n  🎯 {strategy}:")
                print(f"     Contracts: {contracts}")
                print(f"     Allocation: ${allocation:,.0f}")
                print(f"     Max Risk: ${risk:,.0f}")
                print(f"     Confidence: {score:.2f}")
    
    # ============================================================
    # SUMMARY
    # ============================================================
    print("\n" + "="*80)
    print("✅ PIPELINE TEST COMPLETE!")
    print("="*80)
    print(f"\n📊 Row Count Summary:")
    for step_name, key in [
        ('Step 2 (Snapshot)', 'snapshot'),
        ('Step 3 (IVHV)', 'filtered'),
        ('Step 5 (Signals)', 'charted'),
        ('Step 6 (Quality)', 'validated_data'),
        ('Step 7 (Strategies)', 'recommendations'),
        ('Step 9A (Timeframes)', 'timeframed_positions'),
        ('Step 9B (Contracts)', 'selected_contracts'),
        ('Step 10 (Filtered)', 'filtered_contracts'),
        ('Step 11 (Ranked)', 'ranked_strategies'),
        ('Step 8 (FINAL)', 'final_trades'),
    ]:
        if key in results and results[key] is not None:
            count = len(results[key])
            print(f"  {step_name}: {count} row(s)")
        else:
            print(f"  {step_name}: (skipped or empty)")
    print("="*80)
    
    return results


if __name__ == '__main__':
    try:
        results = test_pipeline_with_one_ticker()
        if results and results.get('final_trades') is not None:
            final_count = len(results['final_trades'])
            print(f"\n✅ SUCCESS! Generated {final_count} final trade(s)")
        else:
            print("\n⚠️  Pipeline completed but no final trades generated")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
