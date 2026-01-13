"""
Diagnose Step 11 Data Gaps

Checks what data Step 11 requires vs what's actually present in the pipeline.
Can work with CSV export OR run live pipeline test.
"""

import pandas as pd
import numpy as np
import sys
import os

# Expected fields for each strategy family
DIRECTIONAL_REQUIRED = [
    'Delta', 'Gamma', 'Vega', 'Theta',
    'Trend_State', 'Price_vs_SMA20', 'IV_Percentile'
]

VOLATILITY_REQUIRED = [
    'Delta', 'Vega', 'Theta',
    'Put_Call_Skew',  # CRITICAL - probably missing
    'IV_Percentile',
    'RV_IV_Ratio'  # CRITICAL - probably missing
]

INCOME_REQUIRED = [
    'Theta', 'Vega',
    'IVHV_gap_30D',
    'Probability_of_Profit',  # CRITICAL - probably missing
    'Trend_State', 'Price_vs_SMA20'
]

def check_data_gaps(csv_path: str):
    """Check what data is present vs required."""
    
    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        print(f"❌ File not found: {csv_path}")
        print("Run the dashboard through Step 11 first, then export the data")
        return
    
    print(f"📊 Loaded {len(df)} strategies from {csv_path}")
    print(f"Columns present: {len(df.columns)}")
    print()
    
    # Check each strategy family
    for family_name, required_fields in [
        ('DIRECTIONAL', DIRECTIONAL_REQUIRED),
        ('VOLATILITY', VOLATILITY_REQUIRED),
        ('INCOME', INCOME_REQUIRED)
    ]:
        print(f"{'='*60}")
        print(f"{family_name} STRATEGIES - Required Data Check")
        print(f"{'='*60}")
        
        for field in required_fields:
            if field in df.columns:
                non_null = df[field].notna().sum()
                pct = (non_null / len(df)) * 100
                status = "✅" if pct > 80 else "⚠️" if pct > 20 else "❌"
                print(f"{status} {field:30s} | {non_null:3d}/{len(df)} ({pct:5.1f}%)")
            else:
                print(f"❌ {field:30s} | COLUMN MISSING")
        print()
    
    # Check Validation_Status distribution
    if 'Validation_Status' in df.columns:
        print(f"{'='*60}")
        print("VALIDATION STATUS DISTRIBUTION")
        print(f"{'='*60}")
        status_counts = df['Validation_Status'].value_counts()
        for status, count in status_counts.items():
            pct = (count / len(df)) * 100
            print(f"{status:20s}: {count:3d} ({pct:5.1f}%)")
        print()
    
    # Check Missing_Required_Data (most common issues)
    if 'Missing_Required_Data' in df.columns:
        print(f"{'='*60}")
        print("MOST COMMON MISSING DATA (Top 5)")
        print(f"{'='*60}")
        
        # Parse comma-separated missing data lists
        all_missing = []
        for missing_str in df['Missing_Required_Data'].dropna():
            if missing_str:
                all_missing.extend([x.strip() for x in str(missing_str).split(',')])
        
        if all_missing:
            from collections import Counter
            missing_counts = Counter(all_missing)
            for field, count in missing_counts.most_common(5):
                pct = (count / len(df)) * 100
                print(f"{field:30s}: {count:3d} strategies ({pct:5.1f}%)")
        else:
            print("✅ No missing data reported")
        print()
    
    # Sample rejected strategies
    if 'Validation_Status' in df.columns:
        rejected = df[df['Validation_Status'].isin(['Reject', 'Watch', 'Incomplete_Data'])]
        if len(rejected) > 0:
            print(f"{'='*60}")
            print(f"SAMPLE REJECTED/WATCH STRATEGIES (First 5)")
            print(f"{'='*60}")
            
            cols_to_show = ['Ticker', 'Primary_Strategy', 'Validation_Status', 'Evaluation_Notes']
            cols_present = [c for c in cols_to_show if c in df.columns]
            
            for idx, row in rejected.head(5).iterrows():
                print(f"\n{row.get('Ticker', 'N/A')} | {row.get('Primary_Strategy', 'N/A')}")
                print(f"  Status: {row.get('Validation_Status', 'N/A')}")
                notes = row.get('Evaluation_Notes', '')
                if notes:
                    # Truncate long notes
                    notes_str = str(notes)[:150]
                    print(f"  Reason: {notes_str}...")


if __name__ == '__main__':
    # Option 1: CSV file provided
    if len(sys.argv) > 1:
        csv_path = sys.argv[1]
        if os.path.exists(csv_path):
            check_data_gaps(csv_path)
        else:
            print(f"❌ File not found: {csv_path}")
            sys.exit(1)
    else:
        # Option 2: Run live pipeline test
        print("="*80)
        print("RUNNING LIVE PIPELINE TEST (Mini Audit)")
        print("="*80)
        print("This will run a quick pipeline test to check what data flows to Step 11")
        print()
        
        try:
            # Import pipeline modules
            from core.scan_engine.step2_load_snapshot import load_ivhv_snapshot
            from core.scan_engine.step3_filter_ivhv import filter_ivhv_gap
            from core.scan_engine.step5_chart_signals import compute_chart_signals
            from core.scan_engine.step6_gem_filter import validate_data_quality  # Fixed: correct function name
            from core.scan_engine.step7_strategy_recommendation import recommend_strategies
            from core.scan_engine.step10_pcs_recalibration import recalibrate_and_filter  # Fixed: correct function name
            from core.scan_engine.step11_independent_evaluation import evaluate_strategies_independently
            
            print("✅ All modules imported successfully")
            print()
            
            # Run mini pipeline (first 10 tickers to save time)
            print("🔄 Running Steps 2-7 (limited to 10 tickers for speed)...")
            
            # Step 2: Load snapshot
            df_snapshot = load_ivhv_snapshot()
            df_snapshot = df_snapshot.head(5)  # Limit to 5 tickers for FAST test
            print(f"   Step 2: {len(df_snapshot)} tickers loaded")
            
            # Step 3: IVHV filter
            df_filtered = filter_ivhv_gap(df_snapshot)
            print(f"   Step 3: {len(df_filtered)} tickers after IVHV filter")
            
            # Step 5: Chart signals
            df_signals = compute_chart_signals(df_filtered)
            print(f"   Step 5: {len(df_signals)} tickers with chart signals")
            
            # Step 6: GEM filter
            df_quality = validate_data_quality(df_signals)
            print(f"   Step 6: {len(df_quality)} tickers passed GEM")
            
            # Step 7: Strategy recommendation
            df_strategies = recommend_strategies(df_quality)
            print(f"   Step 7: {len(df_strategies)} strategies recommended")
            print()
            
            # Check what columns are present
            print("📊 Checking Data Availability for Step 11...")
            print()
            
            # Create mock contract data (simplified - no real Tradier call)
            print("⚠️  Simulating Step 9B/10 (mock Greeks for testing)")
            
            # Add mock Greeks (as if Step 10 ran)
            df_strategies['Delta'] = np.random.uniform(0.3, 0.7, len(df_strategies))
            df_strategies['Gamma'] = np.random.uniform(0.02, 0.05, len(df_strategies))
            df_strategies['Vega'] = np.random.uniform(0.15, 0.45, len(df_strategies))
            df_strategies['Theta'] = np.random.uniform(-0.3, -0.1, len(df_strategies))
            df_strategies['Total_Debit'] = np.random.uniform(300, 800, len(df_strategies))
            df_strategies['Contract_Selection_Status'] = 'Success'
            df_strategies['PCS_Final'] = np.random.uniform(60, 90, len(df_strategies))
            
            # Check for CRITICAL missing fields
            critical_missing = []
            
            # Check Skew
            if 'Put_Call_Skew' not in df_strategies.columns:
                critical_missing.append('Put_Call_Skew (CRITICAL for volatility strategies)')
            
            # Check RV/IV ratio
            if 'RV_IV_Ratio' not in df_strategies.columns:
                critical_missing.append('RV_IV_Ratio (CRITICAL for volatility strategies)')
            
            # Check POP
            if 'Probability_of_Profit' not in df_strategies.columns and 'POP' not in df_strategies.columns:
                critical_missing.append('Probability_of_Profit (CRITICAL for income strategies)')
            
            # Check IV_Percentile
            if 'IV_Percentile' not in df_strategies.columns:
                if 'IV_Rank_30D' in df_strategies.columns:
                    print("⚠️  Using IV_Rank_30D as proxy for IV_Percentile (should be 52-week)")
                    df_strategies['IV_Percentile'] = df_strategies['IV_Rank_30D']
                else:
                    critical_missing.append('IV_Percentile (CRITICAL for all strategies)')
            
            print()
            print("="*80)
            print("CRITICAL MISSING DATA")
            print("="*80)
            
            if critical_missing:
                print("❌ The following CRITICAL fields are missing:")
                print()
                for field in critical_missing:
                    print(f"   ❌ {field}")
                print()
                print("These fields must be added or strategies will be marked:")
                print("   - Incomplete_Data (cannot evaluate)")
                print("   - Watch (missing key validation)")
                print("   - Reject (fails hard gates)")
            else:
                print("✅ All critical fields present!")
            
            print()
            print("="*80)
            print("AVAILABLE DATA COLUMNS (Sample)")
            print("="*80)
            
            # Show first few columns and check critical ones
            print(f"Total columns: {len(df_strategies.columns)}")
            print()
            print("Murphy/Sinclair Fields:")
            murphy_sinclair = ['Trend_State', 'Price_vs_SMA20', 'Price_vs_SMA50', 
                               'RSI', 'ADX', 'Volatility_Regime', 'IV_Term_Structure']
            for field in murphy_sinclair:
                if field in df_strategies.columns:
                    non_null = df_strategies[field].notna().sum()
                    pct = (non_null / len(df_strategies)) * 100
                    status = "✅" if pct > 80 else "⚠️"
                    print(f"   {status} {field:25s}: {pct:5.1f}% populated")
                else:
                    print(f"   ❌ {field:25s}: MISSING")
            
            print()
            print("Greek Fields:")
            greeks = ['Delta', 'Gamma', 'Vega', 'Theta']
            for field in greeks:
                if field in df_strategies.columns:
                    non_null = df_strategies[field].notna().sum()
                    pct = (non_null / len(df_strategies)) * 100
                    status = "✅" if pct > 80 else "⚠️"
                    print(f"   {status} {field:25s}: {pct:5.1f}% populated")
                else:
                    print(f"   ❌ {field:25s}: MISSING")
            
            print()
            print("="*80)
            print("NEXT STEPS")
            print("="*80)
            
            if critical_missing:
                print()
                print("🔧 To fix, add these calculations:")
                print()
                if any('Skew' in m for m in critical_missing):
                    print("1. Skew Calculation (Step 9B):")
                    print("   skew = put_iv_atm / call_iv_atm")
                    print("   Location: core/scan_engine/step9b_fetch_contracts.py")
                    print()
                
                if any('RV_IV' in m for m in critical_missing):
                    print("2. RV/IV Ratio (Step 2 or 10):")
                    print("   rv_10d = close.pct_change().rolling(10).std() * sqrt(252) * 100")
                    print("   rv_iv_ratio = rv_10d / iv_30d")
                    print("   Location: core/scan_engine/step2_load_snapshot.py")
                    print()
                
                if any('Probability' in m for m in critical_missing):
                    print("3. Probability of Profit (Step 10):")
                    print("   pop = norm.cdf((ln(S/K) + (r + σ²/2)T) / (σ√T))")
                    print("   Location: core/scan_engine/step10_pcs_recalibration.py")
                    print()
            else:
                print("✅ All critical data present - pipeline should work!")
            
            # Save sample output for inspection
            sample_file = 'diagnostic_sample_output.csv'
            df_strategies.to_csv(sample_file, index=False)
            print()
            print(f"💾 Sample data saved to: {sample_file}")
            print("   You can inspect this to see exact column names and data")
            
        except ImportError as e:
            print(f"❌ Import error: {e}")
            print("   Make sure you're in the options project directory")
            sys.exit(1)
        except Exception as e:
            print(f"❌ Error running pipeline test: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
