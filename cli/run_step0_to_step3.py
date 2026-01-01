#!/usr/bin/env python3
"""
CLI Pipeline Validator: Step 0 → Step 2 → Step 3
Source of Truth Execution for Dashboard Validation

PURPOSE:
    Execute Step 0, Step 2, and Step 3 in sequence with HV-only mode.
    Save all intermediate artifacts for dashboard comparison.
    Schwab API as sole data source (no scraping, yfinance, or Tradier).

USAGE:
    python cli/run_step0_to_step3.py --fetch-iv false

OUTPUT:
    - data/snapshots/ivhv_snapshot_live_YYYYMMDD_HHMMSS.csv (Step 0)
    - output/step2_enriched.csv (Step 2)
    - output/step3_filtered.csv (Step 3)
    - output/cli_validation_report.txt (Summary)
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.scan_engine.step0_schwab_snapshot import main as step0_main
from core.scan_engine.step2_load_snapshot import load_ivhv_snapshot, load_latest_live_snapshot
from core.scan_engine.step3_filter_ivhv import filter_ivhv_gap, STEP3_VERSION, STEP3_LOGIC_HASH

def run_step0(fetch_iv: bool = False) -> str:
    """Execute Step 0: Generate live snapshot"""
    print("\n" + "="*80)
    print("STEP 0: Generate Live Snapshot (Schwab API)")
    print("="*80)
    
    # Run Step 0 with full ticker universe
    df_step0 = step0_main(
        test_mode=False,  # Full ticker universe
        use_cache=True,
        fetch_iv=fetch_iv
    )
    
    # Get the snapshot path that was just created
    snapshot_path = load_latest_live_snapshot()
    
    # Load and inspect
    df = pd.read_csv(snapshot_path)
    print(f"\n✅ Step 0 Complete")
    print(f"   Output: {Path(snapshot_path).name}")
    print(f"   Rows: {len(df)}")
    print(f"   Columns: {list(df.columns)}")
    print(f"   Tickers: {sorted(df['Ticker'].unique())}")
    
    # Data quality
    print(f"\n📊 Data Quality:")
    print(f"   HV populated: {df['HV_30_D_Cur'].notna().sum()}/{len(df)}")
    if 'IV_30_D_Call' in df.columns:
        iv_count = df['IV_30_D_Call'].notna().sum()
        print(f"   IV populated: {iv_count}/{len(df)}")
        if iv_count == 0:
            print(f"   ⚠️  HV-ONLY MODE (IV disabled)")
    
    # Volatility regimes
    if 'volatility_regime' in df.columns:
        print(f"\n📈 Volatility Regimes:")
        regime_counts = df['volatility_regime'].value_counts()
        for regime, count in regime_counts.items():
            print(f"   {regime}: {count}")
    
    return snapshot_path

def run_step2(snapshot_path: str) -> pd.DataFrame:
    """Execute Step 2: Load and enrich snapshot"""
    print("\n" + "="*80)
    print("STEP 2: Load and Enrich Snapshot")
    print("="*80)
    
    df = load_ivhv_snapshot(
        snapshot_path=snapshot_path,
        skip_pattern_detection=True
    )
    
    print(f"\n✅ Step 2 Complete")
    print(f"   Rows: {len(df)}")
    print(f"   Columns: {len(df.columns)}")
    
    # Check enrichment
    enrichment_cols = ['SnapshotDate', 'chart_regime', 'anchor_48h', 'murphy_regime']
    present = [col for col in enrichment_cols if col in df.columns]
    print(f"\n🔧 Enrichment Columns Present: {len(present)}/{len(enrichment_cols)}")
    for col in present:
        print(f"   ✅ {col}")
    
    # Save artifact
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    step2_path = output_dir / "step2_enriched.csv"
    df.to_csv(step2_path, index=False)
    print(f"\n💾 Saved: {step2_path}")
    
    return df

def run_step3(df_step2: pd.DataFrame, min_gap: float = 2.0) -> pd.DataFrame:
    """Execute Step 3: Filter by IVHV gap"""
    print("\n" + "="*80)
    print("STEP 3: Filter by IVHV Gap")
    print("="*80)
    print(f"   Threshold: min_gap >= {min_gap}")
    print(f"   Version: {STEP3_VERSION}")
    print(f"   Logic Hash: {STEP3_LOGIC_HASH}")
    
    df_filtered = filter_ivhv_gap(
        df=df_step2,
        min_gap=min_gap
    )
    
    print(f"\n✅ Step 3 Complete")
    print(f"   Input rows: {len(df_step2)}")
    print(f"   Output rows: {len(df_filtered)}")
    print(f"   Retention: {100*len(df_filtered)/len(df_step2):.1f}%")
    
    if len(df_filtered) > 0:
        # Show tickers that passed
        print(f"\n✅ Tickers Passing Filter:")
        identifier = 'Ticker' if 'Ticker' in df_filtered.columns else 'Symbol'
        for ticker in sorted(df_filtered[identifier].unique()):
            ticker_rows = df_filtered[df_filtered[identifier] == ticker]
            print(f"   {ticker}: {len(ticker_rows)} rows")
    else:
        print(f"\n⚠️  No tickers passed filter")
        # Check why
        if 'IV_30_D_Call' in df_step2.columns:
            iv_missing = df_step2['IV_30_D_Call'].isna().sum()
            if iv_missing == len(df_step2):
                print(f"   Reason: All IV values are NaN (HV-only mode)")
                print(f"   Solution: IVHV gap cannot be computed without IV")
    
    # Save artifact
    output_dir = Path("output")
    step3_path = output_dir / "step3_filtered.csv"
    df_filtered.to_csv(step3_path, index=False)
    print(f"\n💾 Saved: {step3_path}")
    
    return df_filtered

def generate_validation_report(
    snapshot_path: str,
    df_step2: pd.DataFrame,
    df_step3: pd.DataFrame,
    fetch_iv: bool
):
    """Generate validation report for dashboard comparison"""
    print("\n" + "="*80)
    print("VALIDATION REPORT")
    print("="*80)
    
    report_lines = []
    report_lines.append("CLI Pipeline Validation Report")
    report_lines.append("=" * 80)
    report_lines.append(f"Timestamp: {datetime.now().isoformat()}")
    report_lines.append(f"Mode: {'HV+IV' if fetch_iv else 'HV-only'}")
    report_lines.append("")
    
    report_lines.append("STEP 0: Generate Live Snapshot")
    report_lines.append("-" * 80)
    df_step0 = pd.read_csv(snapshot_path)
    report_lines.append(f"Output: {Path(snapshot_path).name}")
    report_lines.append(f"Rows: {len(df_step0)}")
    report_lines.append(f"Tickers: {sorted(df_step0['Ticker'].unique())}")
    report_lines.append(f"HV populated: {df_step0['HV_30_D_Cur'].notna().sum()}/{len(df_step0)}")
    if 'IV_30_D_Call' in df_step0.columns:
        report_lines.append(f"IV populated: {df_step0['IV_30_D_Call'].notna().sum()}/{len(df_step0)}")
    if 'volatility_regime' in df_step0.columns:
        report_lines.append("\nVolatility Regimes:")
        for regime, count in df_step0['volatility_regime'].value_counts().items():
            report_lines.append(f"  {regime}: {count}")
    report_lines.append("")
    
    report_lines.append("STEP 2: Load and Enrich Snapshot")
    report_lines.append("-" * 80)
    report_lines.append(f"Rows: {len(df_step2)}")
    report_lines.append(f"Columns: {len(df_step2.columns)}")
    report_lines.append("")
    
    report_lines.append("STEP 3: Filter by IVHV Gap")
    report_lines.append("-" * 80)
    report_lines.append(f"Input rows: {len(df_step2)}")
    report_lines.append(f"Output rows: {len(df_step3)}")
    report_lines.append(f"Retention: {100*len(df_step3)/len(df_step2):.1f}%")
    
    if len(df_step3) == 0:
        report_lines.append("\n⚠️ WARNING: No tickers passed Step 3 filter")
        if 'IV_30_D_Call' in df_step2.columns:
            iv_missing = df_step2['IV_30_D_Call'].isna().sum()
            if iv_missing == len(df_step2):
                report_lines.append("Reason: All IV values are NaN (HV-only mode)")
                report_lines.append("IVHV gap cannot be computed without IV")
    else:
        identifier = 'Ticker' if 'Ticker' in df_step3.columns else 'Symbol'
        report_lines.append(f"\nTickers passing filter: {sorted(df_step3[identifier].unique())}")
    
    report_lines.append("")
    report_lines.append("=" * 80)
    report_lines.append("DASHBOARD VALIDATION CHECKLIST")
    report_lines.append("=" * 80)
    report_lines.append("□ Dashboard loads same snapshot (check filename)")
    report_lines.append("□ Dashboard Step 2 row count matches CLI")
    report_lines.append("□ Dashboard Step 3 row count matches CLI")
    report_lines.append("□ Dashboard regime distribution matches CLI")
    report_lines.append("□ Dashboard tolerates NaN IV values")
    report_lines.append("□ No synthetic IV values injected")
    report_lines.append("□ No reordering of pipeline steps")
    report_lines.append("")
    
    # Save report
    output_dir = Path("output")
    report_path = output_dir / "cli_validation_report.txt"
    with open(report_path, 'w') as f:
        f.write('\n'.join(report_lines))
    
    print(f"\n💾 Validation report saved: {report_path}")
    
    # Print to console
    print("\n" + '\n'.join(report_lines))

def main():
    parser = argparse.ArgumentParser(
        description="Run Step 0 → Step 2 → Step 3 pipeline validation",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '--fetch-iv',
        type=str,
        choices=['true', 'false'],
        default='false',
        help='Enable IV fetching from Schwab API (default: false for HV-only mode)'
    )
    parser.add_argument(
        '--min-gap',
        type=float,
        default=2.0,
        help='Minimum IVHV gap threshold for Step 3 (default: 2.0)'
    )
    
    args = parser.parse_args()
    fetch_iv = args.fetch_iv.lower() == 'true'
    
    print("\n🔍 CLI PIPELINE VALIDATOR")
    print("Source of Truth Execution for Dashboard Validation")
    print(f"\nMode: {'HV+IV' if fetch_iv else 'HV-only (IV disabled)'}")
    print(f"Data Source: Schwab API (no scraping/yfinance/Tradier)")
    
    try:
        # Step 0: Generate live snapshot
        snapshot_path = run_step0(fetch_iv=fetch_iv)
        
        # Step 2: Load and enrich
        df_step2 = run_step2(snapshot_path)
        
        # Step 3: Filter by IVHV gap
        df_step3 = run_step3(df_step2, min_gap=args.min_gap)
        
        # Generate validation report
        generate_validation_report(snapshot_path, df_step2, df_step3, fetch_iv)
        
        print("\n✅ CLI VALIDATION COMPLETE")
        print("\nNext Steps:")
        print("1. Open dashboard in browser")
        print("2. Enable live mode checkbox")
        print("3. Load the same snapshot")
        print("4. Compare counts and distributions")
        print("5. Document any mismatches")
        
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()
