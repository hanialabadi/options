#!/usr/bin/env python3
"""
Regime Classifier Validation Study

Purpose:
    Run scan pipeline across multiple historical snapshots and validate that
    observed READY_NOW counts and final trades fall within expected ranges
    per market regime.

Constraints:
    - ❌ NO pipeline logic changes
    - ❌ NO threshold changes
    - ❌ NO strategy logic touched
    - ✅ Diagnostics only
    - ✅ CLI-based
    - ✅ Read-only analysis

Usage:
    python validate_regime_classifier.py
    
    # Or with custom snapshots
    python validate_regime_classifier.py --snapshots snapshot1.csv snapshot2.csv

Output:
    validation_results.csv with columns:
        - date
        - regime
        - confidence
        - expected_min
        - expected_max
        - ready_now
        - final_trades
        - within_range
        - notes
"""

import sys
import os
from pathlib import Path
import pandas as pd
from datetime import datetime
import argparse

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from core.scan_engine.pipeline import run_full_scan_pipeline
from core.scan_engine.market_regime_classifier import classify_market_regime


def extract_date_from_snapshot(snapshot_path: str) -> str:
    """
    Extract date from snapshot filename.
    
    Expected formats:
        - ivhv_snapshot_live_20260102_124337.csv → 2026-01-02
        - ivhv_snapshot_2025-10-14.csv → 2025-10-14
    
    Args:
        snapshot_path: Path to snapshot file
    
    Returns:
        str: Date in YYYY-MM-DD format
    """
    filename = Path(snapshot_path).stem
    
    # Try format: ivhv_snapshot_live_20260102_HHMMSS
    if 'live_' in filename:
        parts = filename.split('_')
        if len(parts) >= 3:
            date_str = parts[3]  # 20260102
            if len(date_str) == 8:
                return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    
    # Try format: ivhv_snapshot_2025-10-14
    if filename.count('-') >= 2:
        parts = filename.split('_')
        if len(parts) >= 2:
            date_part = parts[-1]  # 2025-10-14
            if date_part.count('-') == 2:
                return date_part
    
    # Fallback: use file modification time
    mtime = os.path.getmtime(snapshot_path)
    return datetime.fromtimestamp(mtime).strftime('%Y-%m-%d')


def validate_single_snapshot(snapshot_path: str) -> dict:
    """
    Run pipeline on single snapshot and extract validation metrics.
    
    Args:
        snapshot_path: Path to IV/HV snapshot CSV
    
    Returns:
        dict: {
            'date': str,
            'regime': str,
            'confidence': str,
            'expected_min': int,
            'expected_max': int,
            'ready_now': int,
            'final_trades': int,
            'within_range': bool,
            'notes': str,
            'status': 'SUCCESS' | 'ERROR',
            'error_message': str (if status=ERROR)
        }
    """
    date = extract_date_from_snapshot(snapshot_path)
    
    print(f"\n{'='*80}")
    print(f"Validating: {Path(snapshot_path).name}")
    print(f"Date: {date}")
    print(f"{'='*80}")
    
    try:
        # Run full pipeline
        print("Running pipeline...")
        results = run_full_scan_pipeline(
            snapshot_path=snapshot_path,
            output_dir=None,  # Don't save intermediate outputs
            account_balance=100000.0,
            max_portfolio_risk=0.20
        )
        
        # Extract regime classification
        if 'charted' not in results or 'filtered' not in results:
            return {
                'date': date,
                'status': 'ERROR',
                'error_message': 'Missing charted or filtered results',
                'regime': 'UNKNOWN',
                'confidence': 'N/A',
                'expected_min': 0,
                'expected_max': 0,
                'ready_now': 0,
                'final_trades': 0,
                'within_range': False,
                'notes': 'Pipeline did not produce required outputs'
            }
        
        df_step5 = results['charted']
        df_step3 = results['filtered']
        
        if df_step5.empty or df_step3.empty:
            return {
                'date': date,
                'status': 'ERROR',
                'error_message': 'Empty pipeline outputs',
                'regime': 'INSUFFICIENT_DATA',
                'confidence': 'N/A',
                'expected_min': 0,
                'expected_max': 0,
                'ready_now': 0,
                'final_trades': 0,
                'within_range': False,
                'notes': 'Pipeline produced empty results (no tickers passed filters)'
            }
        
        # Classify regime
        print("Classifying market regime...")
        regime_info = classify_market_regime(df_step5, df_step3)
        
        # Extract metrics
        ready_now = len(results.get('acceptance_ready', pd.DataFrame()))
        final_trades = len(results.get('final_trades', pd.DataFrame()))
        
        expected_min, expected_max = regime_info['expected_ready_range']
        within_range = expected_min <= ready_now <= expected_max
        
        # Generate notes
        if within_range:
            notes = f"✅ Within expected range. {regime_info['explanation']}"
        elif ready_now < expected_min:
            notes = (
                f"⚠️ Below expected range. Actual: {ready_now}, Expected: {expected_min}-{expected_max}. "
                f"Possible: (1) Strategy rules stricter than regime suggests, "
                f"(2) Step 12 filtered aggressively, (3) Unusual market conditions. "
                f"{regime_info['explanation']}"
            )
        else:
            notes = (
                f"⚠️ Above expected range. Actual: {ready_now}, Expected: {expected_min}-{expected_max}. "
                f"Possible: (1) Strategy rules more lenient than regime suggests, "
                f"(2) Strong opportunities despite regime, (3) Regime misclassification. "
                f"{regime_info['explanation']}"
            )
        
        # Print summary
        print(f"\nRegime: {regime_info['regime']}")
        print(f"Confidence: {regime_info['confidence']}")
        print(f"Expected READY_NOW: {expected_min}-{expected_max}")
        print(f"Actual READY_NOW: {ready_now}")
        print(f"Final Trades: {final_trades}")
        print(f"Within Range: {'✅ YES' if within_range else '⚠️ NO'}")
        
        return {
            'date': date,
            'status': 'SUCCESS',
            'regime': regime_info['regime'],
            'confidence': regime_info['confidence'],
            'expected_min': expected_min,
            'expected_max': expected_max,
            'ready_now': ready_now,
            'final_trades': final_trades,
            'within_range': within_range,
            'notes': notes
        }
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        
        return {
            'date': date,
            'status': 'ERROR',
            'error_message': str(e),
            'regime': 'ERROR',
            'confidence': 'N/A',
            'expected_min': 0,
            'expected_max': 0,
            'ready_now': 0,
            'final_trades': 0,
            'within_range': False,
            'notes': f'Pipeline error: {str(e)}'
        }


def run_validation_study(snapshot_paths: list, output_path: str = 'validation_results.csv'):
    """
    Run validation study across multiple snapshots.
    
    Args:
        snapshot_paths: List of snapshot file paths
        output_path: Path to save validation results CSV
    
    Returns:
        pd.DataFrame: Validation results
    """
    print("="*80)
    print("REGIME CLASSIFIER VALIDATION STUDY")
    print("="*80)
    print(f"\nSnapshots to validate: {len(snapshot_paths)}")
    print(f"Output: {output_path}")
    print()
    
    results = []
    for i, snapshot_path in enumerate(snapshot_paths, 1):
        print(f"\n[{i}/{len(snapshot_paths)}] Processing: {snapshot_path}")
        
        result = validate_single_snapshot(snapshot_path)
        results.append(result)
    
    # Create DataFrame
    df = pd.DataFrame(results)
    
    # Calculate summary statistics
    print("\n" + "="*80)
    print("VALIDATION SUMMARY")
    print("="*80)
    
    successful = df[df['status'] == 'SUCCESS']
    if len(successful) > 0:
        within_range_count = successful['within_range'].sum()
        total_count = len(successful)
        percentage = (within_range_count / total_count) * 100
        
        print(f"\nTotal Snapshots: {len(df)}")
        print(f"Successful: {len(successful)}")
        print(f"Errors: {len(df) - len(successful)}")
        print(f"\nWithin Expected Range: {within_range_count}/{total_count} ({percentage:.1f}%)")
        
        if percentage >= 80:
            print("\n✅ VALIDATION PASSED (≥80% within expected range)")
            print("   - Regime classifier validated")
            print("   - Strategy thresholds appropriate for regimes")
            print("   - System correctly selective (not broken)")
        elif percentage >= 60:
            print("\n⚠️ VALIDATION MARGINAL (60-80% within expected range)")
            print("   - Regime classifier mostly accurate")
            print("   - Some regime boundaries may need refinement")
            print("   - Review outlier cases for patterns")
        else:
            print("\n❌ VALIDATION FAILED (<60% within expected range)")
            print("   - Regime classifier needs refinement OR")
            print("   - Expected ranges too narrow OR")
            print("   - Missing directional context (Phase 1 candidate)")
        
        # Regime distribution
        print("\nRegime Distribution:")
        regime_counts = successful['regime'].value_counts()
        for regime, count in regime_counts.items():
            print(f"  {regime}: {count}")
        
        # Confidence distribution
        print("\nConfidence Distribution:")
        conf_counts = successful['confidence'].value_counts()
        for conf, count in conf_counts.items():
            print(f"  {conf}: {count}")
        
    else:
        print("\n❌ NO SUCCESSFUL VALIDATIONS")
        print("All snapshots encountered errors. Check pipeline configuration.")
    
    # Save results
    df.to_csv(output_path, index=False)
    print(f"\n✅ Results saved to: {output_path}")
    
    return df


def find_available_snapshots(snapshot_dir: str = 'data/snapshots', limit: int = 10) -> list:
    """
    Find available snapshot files in directory.
    
    Args:
        snapshot_dir: Directory containing snapshots
        limit: Maximum number of snapshots to return
    
    Returns:
        list: Paths to snapshot files (most recent first)
    """
    snapshot_path = Path(snapshot_dir)
    
    if not snapshot_path.exists():
        print(f"❌ Snapshot directory not found: {snapshot_dir}")
        return []
    
    # Find all snapshot files
    snapshots = sorted(
        snapshot_path.glob("ivhv_snapshot*.csv"),
        key=lambda f: f.stat().st_mtime,
        reverse=True  # Most recent first
    )
    
    if not snapshots:
        print(f"❌ No snapshot files found in {snapshot_dir}")
        return []
    
    # Take most recent N snapshots (diverse dates)
    selected = []
    dates_seen = set()
    
    for snap in snapshots:
        date = extract_date_from_snapshot(str(snap))
        if date not in dates_seen:
            selected.append(str(snap))
            dates_seen.add(date)
            if len(selected) >= limit:
                break
    
    return selected


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Validate regime classifier across historical snapshots')
    parser.add_argument('--snapshots', nargs='+', help='Paths to snapshot files')
    parser.add_argument('--snapshot-dir', default='data/snapshots', help='Directory containing snapshots')
    parser.add_argument('--limit', type=int, default=10, help='Maximum number of snapshots to validate')
    parser.add_argument('--output', default='validation_results.csv', help='Output CSV path')
    
    args = parser.parse_args()
    
    # Get snapshot paths
    if args.snapshots:
        snapshot_paths = args.snapshots
    else:
        print(f"Finding snapshots in {args.snapshot_dir}...")
        snapshot_paths = find_available_snapshots(args.snapshot_dir, args.limit)
        
        if not snapshot_paths:
            print("❌ No snapshots found. Please specify --snapshots or check --snapshot-dir")
            sys.exit(1)
    
    # Validate paths exist
    valid_paths = []
    for path in snapshot_paths:
        if Path(path).exists():
            valid_paths.append(path)
        else:
            print(f"⚠️ Skipping missing file: {path}")
    
    if not valid_paths:
        print("❌ No valid snapshot files found")
        sys.exit(1)
    
    print(f"\nSelected snapshots ({len(valid_paths)}):")
    for path in valid_paths:
        date = extract_date_from_snapshot(path)
        print(f"  {date}: {Path(path).name}")
    
    # Run validation study
    results_df = run_validation_study(valid_paths, args.output)
    
    print("\n" + "="*80)
    print("VALIDATION STUDY COMPLETE")
    print("="*80)
