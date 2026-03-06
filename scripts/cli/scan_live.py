#!/usr/bin/env python3
"""
Live Market Scan CLI - Real-time strategy discovery
Market hours only. No enrichment, no lifecycle, no freezing.

Usage:
    venv/bin/python scan_live.py [snapshot_path]
    venv/bin/python scan_live.py --audit --tickers AAPL,MSFT --snapshot path.csv
"""

import sys
import os
import argparse
import numpy as np
from pathlib import Path

# Add project root to path
# This must be done BEFORE any imports from the project itself
project_root = Path(__file__).parent.parent.parent # Assuming scripts/cli is 3 levels deep from project root
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Now import project modules
import pandas as pd
from datetime import datetime
from core.shared.data_contracts.config import PROJECT_ROOT, SCAN_OUTPUT_DIR # Re-import PROJECT_ROOT and SCAN_OUTPUT_DIR
from scan_engine.pipeline import run_full_scan_pipeline
from scan_engine.step0_resolve_snapshot import resolve_snapshot_path
from scan_engine.market_regime_classifier import classify_market_regime

# ============================================================
# ARGUMENT PARSING
# ============================================================
parser = argparse.ArgumentParser(
    description='Live Market Scan CLI',
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog="""
Examples:
  # Standard scan (auto-detect snapshot)
  venv/bin/python scan_live.py
  
  # Scan with specific snapshot
  venv/bin/python scan_live.py data/snapshots/ivhv_snapshot_20250102.csv
  
  # Forensic audit mode (fixed tickers, materialize every step)
  venv/bin/python scan_live.py --audit --tickers AAPL,MSFT,NVDA --snapshot data/snapshots/ivhv_snapshot_20250102.csv
"""
)

parser.add_argument(
    'snapshot',
    nargs='?',
    help='Path to snapshot CSV (optional, auto-detects if omitted)'
)

parser.add_argument(
    '--audit',
    action='store_true',
    help='Enable forensic audit mode: saves CSV at every step + per-ticker trace tables'
)

parser.add_argument(
    '--tickers',
    type=str,
    help='Comma-separated ticker list (required with --audit)'
)

parser.add_argument(
    '--snapshot',
    dest='snapshot_flag',
    type=str,
    help='Path to snapshot (alternative to positional argument)'
)

args = parser.parse_args()

# ============================================================
# VALIDATE AUDIT MODE
# ============================================================
if args.audit:
    if not args.tickers:
        print("❌ ERROR: --audit requires --tickers")
        print("   Example: --audit --tickers AAPL,MSFT,NVDA,TSLA")
        sys.exit(1)

print("="*80)
if args.audit:
    print("🔍 FORENSIC AUDIT MODE")
    print(f"   Fixed universe: {args.tickers}")
    print(f"   Output: audit_steps/*.csv + audit_trace/*.csv")
else:
    print("🔴 LIVE MARKET SCAN")
print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*80)

# Step 0: Resolve snapshot path
print("\n📂 Resolving snapshot path...")
snapshot_path = args.snapshot or args.snapshot_flag

if snapshot_path:
    if not os.path.exists(snapshot_path):
        print(f"❌ Snapshot not found: {snapshot_path}")
        sys.exit(1)
    print(f"✅ Using specified snapshot: {snapshot_path}")
else:
    try:
        snapshot_path = resolve_snapshot_path()
        print(f"✅ Using snapshot: {snapshot_path}")
    except Exception as e:
        print(f"❌ Failed to resolve snapshot: {e}")
        print("Please ensure you have a recent IV/HV snapshot in data/ivhv_snapshots/")
        sys.exit(1)

# ============================================================
# INITIALIZE AUDIT MODE (if enabled)
# ============================================================
audit_mode = None
if args.audit:
    from core._support.audit.pipeline_audit_mode import create_audit_mode
    
    ticker_list = [t.strip().upper() for t in args.tickers.split(',')]
    audit_mode = create_audit_mode(ticker_list, snapshot_path)
    
    print(f"\n🔍 Audit Mode Initialized")
    print(f"   Tickers: {', '.join(ticker_list)}")
    print(f"   Snapshot: {snapshot_path}")
    print(f"   Output: audit_steps/ + audit_trace/")
    print("")

# Run full scan pipeline
print("\n🔍 Running scan pipeline...")
print("-"*80)

try:
    from core.shared.data_contracts.config import SCAN_OUTPUT_DIR
    results = run_full_scan_pipeline(
        snapshot_path=snapshot_path,
        output_dir=SCAN_OUTPUT_DIR,
        account_balance=100000.0,
        max_portfolio_risk=0.20,
        audit_mode=audit_mode  # Pass audit mode to pipeline
    )
    
    # ============================================================
    # MARKET REGIME CLASSIFICATION (Diagnostic)
    # ============================================================
    if 'charted' in results and 'filtered' in results:
        if not results['charted'].empty and not results['filtered'].empty:
            print("\n" + "="*80)
            print("📊 MARKET REGIME ANALYSIS")
            print("="*80)
            
            regime_info = classify_market_regime(results['charted'], results['filtered'])
            
            print(f"\nRegime Type: {regime_info['regime']}")
            print(f"Confidence: {regime_info['confidence']}")
            print(f"Expected READY_NOW Range: {regime_info['expected_ready_range'][0]}-{regime_info['expected_ready_range'][1]}")
            print(f"\nExplanation: {regime_info['explanation']}")
            
            # P1 Guardrail: Market Stress Mode Banner
            from core.shared.data_layer.market_stress_detector import check_market_stress, get_market_stress_summary
            stress_level, median_iv, stress_basis = check_market_stress()
            
            if stress_level != 'GREEN':
                print("\n" + "-"*80)
                print(get_market_stress_summary(stress_level, median_iv, stress_basis))
                print("-"*80)
                
                if stress_level == 'RED':
                    print("\n🛑 ALL TRADES WILL BE HALTED IN STEP 12")
                    print("   No execution allowed until market conditions normalize")
            
            # Compare to actual
            actual_ready = len(results.get('acceptance_ready', pd.DataFrame()))
            min_exp, max_exp = regime_info['expected_ready_range']
            
            print(f"\nActual READY_NOW: {actual_ready}")
            
            # Phase 3: IV Availability Diagnostics
            if 'acceptance_ready' in results and not results['acceptance_ready'].empty:
                df_ready = results['acceptance_ready']
                if 'iv_rank_available' in df_ready.columns:
                    iv_unavailable = (~df_ready['iv_rank_available']).sum()
                    if iv_unavailable > 0:
                        avg_history = df_ready[~df_ready['iv_rank_available']]['iv_history_days'].mean() if 'iv_history_days' in df_ready.columns else 0
                        print(f"\n📊 IV Availability Status:")
                        print(f"   ⚠️ {iv_unavailable}/{len(df_ready)} strategies lack sufficient IV history")
                        print(f"   📅 Average history: {avg_history:.1f} days (need 120+)")
            
            if min_exp <= actual_ready <= max_exp:

                print(f"✅ WITHIN EXPECTED RANGE ({min_exp}-{max_exp})")
                print("   Output matches market regime expectations")
            elif actual_ready < min_exp:
                print(f"⚠️ BELOW EXPECTED RANGE ({min_exp}-{max_exp})")
                print("   Possible reasons:")
                print("   1. Strategy rules more strict than regime suggests")
                print("   2. Step 12 filtered aggressively (LOW confidence contracts)")
                print("   3. Unusual market conditions within regime")
            else:
                print(f"⚠️ ABOVE EXPECTED RANGE ({min_exp}-{max_exp})")
                print("   Possible reasons:")
                print("   1. Strategy rules more lenient than regime suggests")
                print("   2. Strong opportunities despite regime classification")
                print("   3. Regime misclassification (review signal distributions)")
            
            print("="*80)
    
    # Extract final results
    if 'thesis_envelopes' in results and results['thesis_envelopes'] is not None:
        df_final = results['thesis_envelopes']
        
        if df_final.empty:
            print("\n❌ NO CANDIDATES FOUND")
            print("Market conditions may not meet GEM criteria.")
        else:
            print(f"\n✅ {len(df_final)} CANDIDATES FOUND")
            print("="*80)
            
            def require_field(row, field_name, is_float=False):
                val = row.get(field_name)
                if pd.isna(val) or val == '' or val == 'N/A':
                    return "MISSING"
                if is_float and isinstance(val, (int, float, np.number)):
                    return f"{float(val):.2f}"
                return val

            # Display format
            for idx, row in df_final.iterrows():
                print(f"\n{row['Ticker']} | {require_field(row, 'Strategy_Name')}")
                
                print(f"  PCS: {require_field(row, 'PCS_Score_V2')}")
                print(f"  Delta: {require_field(row, 'Delta', is_float=True)}")
                print(f"  Vega: {require_field(row, 'Vega', is_float=True)}")
                print(f"  DTE: {require_field(row, 'Actual_DTE')}")
                print(f"  Strike: {require_field(row, 'Selected_Strike')}")
                print(f"  IV Rank: {require_field(row, 'IV_Rank_30D', is_float=True)}")
                print(f"  Chart: {require_field(row, 'Chart_Regime')}")
                
                alloc = row.get('Capital_Allocation')
                if pd.notna(alloc) and isinstance(alloc, (int, float, np.number)):
                    print(f"  Allocation: ${float(alloc):,.2f}")
                else:
                    print(f"  Allocation: N/A")
            
            print("\n" + "="*80)
            total_alloc = df_final['Capital_Allocation'].sum() if 'Capital_Allocation' in df_final.columns else 0
            print(f"Total Capital Allocated: ${total_alloc:,.2f}")
            
            # Phase 3: IV Availability Summary
            if 'iv_rank_available' in df_final.columns:
                iv_available_count = df_final['iv_rank_available'].sum()
                iv_unavailable_count = (~df_final['iv_rank_available']).sum()
                
                if iv_unavailable_count > 0:
                    print("\n" + "="*80)
                    print("📊 IV AVAILABILITY SUMMARY")
                    print("="*80)
                    print(f"✅ IV Rank available: {iv_available_count}/{len(df_final)} strategies")
                    print(f"⚠️  IV Rank unavailable: {iv_unavailable_count}/{len(df_final)} strategies")
                    
                    if 'iv_history_days' in df_final.columns:
                        avg_history = df_final[~df_final['iv_rank_available']]['iv_history_days'].mean()
                        max_history = df_final[~df_final['iv_rank_available']]['iv_history_days'].max()
                        print(f"📅 History: avg={avg_history:.1f} days, max={max_history} days (need 120+)")
            
            # P1 Guardrail: Market Stress Mode Summary
            if 'acceptance_status' in df_final.columns:
                halted_count = (df_final['acceptance_status'] == 'HALTED_MARKET_STRESS').sum()
                
                if halted_count > 0:
                    print("\n" + "="*80)
                    print("🛑 MARKET STRESS MODE ALERT")
                    print("="*80)
                    print(f"🛑 {halted_count}/{len(df_final)} strategies HALTED due to market stress")
                    
                    # Show halt reason from first halted strategy
                    halted_strategies = df_final[df_final['acceptance_status'] == 'HALTED_MARKET_STRESS']
                    if not halted_strategies.empty and 'acceptance_reason' in halted_strategies.columns:
                        halt_reason = halted_strategies['acceptance_reason'].iloc[0]
                        print(f"📢 Reason: {halt_reason}")
                    
                    print("\n⚠️  ALL TRADES BLOCKED - Market volatility exceeds safe threshold")
                    print("   No partial execution, no sizing adjustment - HARD HALT active")
                    print("   System will resume when market conditions normalize")
            
            
    else:
        print("\n⚠️  Pipeline completed but no final trades returned")
        print("Check intermediate steps for filtering reasons")
    
    # Show rejection reasons from intermediate steps
    if 'evaluated_strategies' in results and results['evaluated_strategies'] is not None:
        df_evaluated = results['evaluated_strategies']
        
        rejected = df_evaluated[df_evaluated.get('Selected', True) == False] if 'Selected' in df_evaluated.columns else pd.DataFrame()
        
        if not rejected.empty:
            print("\n" + "="*80)
            print("REJECTED CANDIDATES")
            print("="*80)
            
            for idx, row in rejected.head(10).iterrows():
                print(f"\n{row.get('Ticker', 'Unknown')} | {row.get('Strategy', 'Unknown')}")
                print(f"  Reason: {row.get('Rejection_Reason', 'Not specified')}")
                if 'PCS' in row and pd.notna(row['PCS']):
                    print(f"  PCS: {row['PCS']:.1f}")
                if 'Delta' in row and pd.notna(row['Delta']):
                    print(f"  Delta: {row['Delta']:.2f}")
                if 'Vega' in row and pd.notna(row['Vega']):
                    print(f"  Vega: {row['Vega']:.2f}")

except Exception as e:
    print(f"\n❌ SCAN FAILED: {e}")
    import traceback
    traceback.print_exc()
    
    # Finalize audit mode even on error
    if audit_mode:
        audit_mode.generate_trace_tables()
        audit_mode.generate_summary()
    
    sys.exit(1)

# ============================================================
# FINALIZE AUDIT MODE (if enabled)
# ============================================================
if audit_mode:
    print("\n" + "="*80)
    print("📊 FINALIZING AUDIT MODE")
    print("="*80)
    
    audit_mode.generate_trace_tables()
    audit_mode.generate_summary()
    
    print("\n✅ Audit artifacts generated:")
    print(f"   📁 audit_steps/*.csv - Step-by-step DataFrames")
    print(f"   📁 audit_trace/*.csv - Per-ticker progression")
    print(f"   📄 AUDIT_NAVIGATION.md - Manual inspection guide")
    print("")

print("\n" + "="*80)
print("✅ SCAN COMPLETE")
print("="*80)
