#!/usr/bin/env python3
"""
CLI Pipeline Validator: Step 2 → Step 3 (Using Existing Snapshot)
Source of Truth Execution for Dashboard Validation
"""

import sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.scan_engine.step2_load_snapshot import load_ivhv_snapshot
from core.scan_engine.step3_filter_ivhv import filter_ivhv_gap, STEP3_VERSION, STEP3_LOGIC_HASH

def main():
    print("\n" + "="*80)
    print("CLI PIPELINE VALIDATION: Step 2 → Step 3")
    print("Source of Truth for Dashboard Comparison")
    print("="*80)
    
    # Use latest live snapshot
    snapshot_path = "/Users/haniabadi/Documents/Github/options/data/snapshots/ivhv_snapshot_live_20251231_181439.csv"
    
    print(f"\nUsing snapshot: {Path(snapshot_path).name}")
    
    # STEP 2: Load and enrich
    print("\n" + "="*80)
    print("STEP 2: Load and Enrich Snapshot")
    print("="*80)
    
    df_step2 = load_ivhv_snapshot(
        snapshot_path=snapshot_path,
        skip_pattern_detection=True
    )
    
    print(f"\n✅ Step 2 Complete")
    print(f"   Rows: {len(df_step2)}")
    print(f"   Columns: {len(df_step2.columns)}")
    
    # Check data quality
    print(f"\n📊 Data Quality:")
    print(f"   HV_30_D_Cur populated: {df_step2['HV_30_D_Cur'].notna().sum()}/{len(df_step2)}")
    if 'IV_30_D_Call' in df_step2.columns:
        iv_count = df_step2['IV_30_D_Call'].notna().sum()
        print(f"   IV_30_D_Call populated: {iv_count}/{len(df_step2)}")
        if iv_count == 0:
            print(f"   ⚠️  HV-ONLY MODE (all IV values are NaN)")
    
    # Show regime distribution
    if 'volatility_regime' in df_step2.columns:
        print(f"\n📈 Volatility Regimes (from Step 0):")
        regime_counts = df_step2['volatility_regime'].value_counts()
        for regime, count in regime_counts.items():
            print(f"   {regime}: {count}")
    
    # Save Step 2 artifact
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    step2_path = output_dir / "step2_enriched_cli.csv"
    df_step2.to_csv(step2_path, index=False)
    print(f"\n💾 Saved: {step2_path}")
    
    # STEP 3: Filter by IVHV gap
    print("\n" + "="*80)
    print("STEP 3: Filter by IVHV Gap")
    print("="*80)
    print(f"   Threshold: min_gap >= 2.0")
    print(f"   Version: {STEP3_VERSION}")
    print(f"   Logic Hash: {STEP3_LOGIC_HASH}")
    
    try:
        df_step3 = filter_ivhv_gap(
            df=df_step2,
            min_gap=2.0
        )
        
        print(f"\n✅ Step 3 Complete")
        print(f"   Input rows: {len(df_step2)}")
        print(f"   Output rows: {len(df_step3)}")
        
        if len(df_step3) > 0:
            print(f"   Retention: {100*len(df_step3)/len(df_step2):.1f}%")
            
            # Show tickers that passed
            print(f"\n✅ Tickers Passing Filter:")
            identifier = 'Ticker' if 'Ticker' in df_step3.columns else 'Symbol'
            for ticker in sorted(df_step3[identifier].unique()):
                ticker_rows = df_step3[df_step3[identifier] == ticker]
                print(f"   {ticker}: {len(ticker_rows)} rows")
            
            # Save Step 3 artifact
            step3_path = output_dir / "step3_filtered_cli.csv"
            df_step3.to_csv(step3_path, index=False)
            print(f"\n💾 Saved: {step3_path}")
        else:
            print(f"\n⚠️  No tickers passed filter")
            
            # Diagnostic: Why did Step 3 filter fail?
            print(f"\n🔍 Step 3 Filter Diagnostics:")
            
            # Check if IV is present
            if 'IV_30_D_Call' in df_step2.columns:
                iv_missing = df_step2['IV_30_D_Call'].isna().sum()
                print(f"   IV_30_D_Call missing: {iv_missing}/{len(df_step2)}")
                
                if iv_missing == len(df_step2):
                    print(f"\n   ❌ ROOT CAUSE: All IV values are NaN")
                    print(f"   EXPLANATION: Step 3 requires both IV and HV to compute IVHV gap")
                    print(f"   SOLUTION: Either enable IV fetching in Step 0, OR modify Step 3 to support HV-only mode")
            
            # Check HV
            hv_missing = df_step2['HV_30_D_Cur'].isna().sum()
            print(f"   HV_30_D_Cur missing: {hv_missing}/{len(df_step2)}")
            
    except Exception as e:
        print(f"\n❌ Step 3 failed: {e}")
        import traceback
        traceback.print_exc()
        df_step3 = pd.DataFrame()
    
    # Generate validation report
    print("\n" + "="*80)
    print("VALIDATION REPORT")
    print("="*80)
    
    report_lines = []
    report_lines.append("CLI Pipeline Validation Report")
    report_lines.append("="*80)
    report_lines.append("Mode: HV-only (IV disabled)")
    report_lines.append(f"Snapshot: {Path(snapshot_path).name}")
    report_lines.append("")
    
    report_lines.append("STEP 2 RESULTS:")
    report_lines.append(f"  Rows: {len(df_step2)}")
    report_lines.append(f"  HV populated: {df_step2['HV_30_D_Cur'].notna().sum()}/{len(df_step2)}")
    if 'IV_30_D_Call' in df_step2.columns:
        report_lines.append(f"  IV populated: {df_step2['IV_30_D_Call'].notna().sum()}/{len(df_step2)}")
    report_lines.append("")
    
    report_lines.append("STEP 3 RESULTS:")
    report_lines.append(f"  Rows: {len(df_step3)}")
    if len(df_step3) == 0:
        report_lines.append("  ⚠️  No tickers passed filter")
        if 'IV_30_D_Call' in df_step2.columns and df_step2['IV_30_D_Call'].isna().all():
            report_lines.append("  ROOT CAUSE: Step 3 requires IV but all IV values are NaN")
            report_lines.append("  IMPACT: Step 3 cannot compute IVHV gap without IV")
    else:
        identifier = 'Ticker' if 'Ticker' in df_step3.columns else 'Symbol'
        report_lines.append(f"  Tickers: {sorted(df_step3[identifier].unique())}")
    report_lines.append("")
    
    report_lines.append("="*80)
    report_lines.append("DASHBOARD VALIDATION CHECKLIST")
    report_lines.append("="*80)
    report_lines.append("1. Dashboard loads SAME snapshot file")
    report_lines.append(f"   Expected: {Path(snapshot_path).name}")
    report_lines.append("")
    report_lines.append("2. Dashboard Step 2 row count matches CLI")
    report_lines.append(f"   Expected: {len(df_step2)} rows")
    report_lines.append("")
    report_lines.append("3. Dashboard handles NaN IV gracefully")
    report_lines.append("   Expected: No crashes, no synthetic IV injection")
    report_lines.append("")
    report_lines.append("4. Dashboard shows HV-only mode warning")
    report_lines.append("   Expected: Clear UI indication that IV is unavailable")
    report_lines.append("")
    report_lines.append("5. Dashboard Step 3 row count matches CLI")
    report_lines.append(f"   Expected: {len(df_step3)} rows (likely 0 due to missing IV)")
    report_lines.append("")
    report_lines.append("6. No pipeline logic changes in dashboard")
    report_lines.append("   Expected: Same filtering, same thresholds, same regime tags")
    report_lines.append("")
    
    # Save report
    report_path = output_dir / "cli_validation_report.txt"
    with open(report_path, 'w') as f:
        f.write('\n'.join(report_lines))
    
    print('\n'.join(report_lines))
    print(f"\n💾 Report saved: {report_path}")
    
    print("\n" + "="*80)
    print("✅ CLI VALIDATION COMPLETE")
    print("="*80)
    print("\nNext Steps:")
    print("1. Open dashboard at http://localhost:8501")
    print("2. Enable '🔴 LIVE MODE' checkbox")
    print("3. Click '▶️ Load Step 2 Data'")
    print("4. Compare counts with CLI report above")
    print("5. Document any dashboard-specific issues")

if __name__ == '__main__':
    main()
