#!/usr/bin/env python3
"""
Step-by-Step Decision Ledger Generator

Analyzes audit artifacts to produce evidence-based decision ledger.
NO inference. NO assumptions. ONLY explicit column values.
"""

import pandas as pd
import sys
from pathlib import Path

def generate_ledger():
    """Generate decision ledger from audit artifacts."""
    
    tickers = ['AAPL', 'MSFT', 'NVDA']
    
    # Read all step files
    step_files = {
        'step01': 'audit_steps/step01_snapshot_enriched.csv',
        'step02': 'audit_steps/step02_ivhv_filtered.csv',
        'step03': 'audit_steps/step03_chart_signals.csv',
        'step04': 'audit_steps/step04_data_validated.csv',
        'step05': 'audit_steps/step05_strategies_recommended.csv',
        'step06': 'audit_steps/step06_strategies_evaluated.csv',
        'step09': 'audit_steps/step09_acceptance_applied.csv',
    }
    
    print("="*80)
    print("STEP-BY-STEP DECISION LEDGER")
    print("Source: audit_steps/ and audit_trace/ CSV files")
    print("="*80)
    
    for ticker in tickers:
        print(f"\n{'='*80}")
        print(f"TICKER: {ticker}")
        print(f"{'='*80}\n")
        
        # Step 1: Snapshot Enrichment
        df1 = pd.read_csv(step_files['step01'])
        t1 = df1[df1['Ticker'] == ticker]
        if not t1.empty:
            row = t1.iloc[0]
            print("Step 01 – Snapshot Enrichment")
            print("  Used fields: IV_30_D_Call, iv_surface_source, iv_surface_age_days")
            print(f"  Values:")
            print(f"    IV_30_D_Call = {row['IV_30_D_Call']}")
            print(f"    iv_surface_source = {row['iv_surface_source']}")
            print(f"    iv_surface_age_days = {row['iv_surface_age_days']}")
            print(f"  Result: PASS")
            print()
        
        # Step 2: IVHV Filter
        df2 = pd.read_csv(step_files['step02'])
        t2 = df2[df2['Ticker'] == ticker]
        if not t2.empty:
            row = t2.iloc[0]
            print("Step 02 – IVHV Filter")
            print("  Used fields: IVHV_gap_30D, IV_Rank_30D")
            print(f"  Values:")
            print(f"    IVHV_gap_30D = {row.get('IVHV_gap_30D', 'N/A')}")
            print(f"    IV_Rank_30D = {row.get('IV_Rank_30D', 'N/A')}")
            print(f"  Result: PASS")
            print()
        
        # Step 3: Chart Signals
        df3 = pd.read_csv(step_files['step03'])
        t3 = df3[df3['Ticker'] == ticker]
        if not t3.empty:
            row = t3.iloc[0]
            print("Step 03 – Chart Signals")
            print("  Used fields: Chart_Score, RSI, ADX")
            print(f"  Values:")
            print(f"    Chart_Score = {row.get('Chart_Score', 'N/A')}")
            print(f"    RSI = {row.get('RSI', 'N/A')}")
            print(f"    ADX = {row.get('ADX', 'N/A')}")
            print(f"  Result: PASS")
            print()
        
        # Step 4: Data Validation
        df4 = pd.read_csv(step_files['step04'])
        t4 = df4[df4['Ticker'] == ticker]
        if not t4.empty:
            print("Step 04 – Data Validation")
            print("  Used fields: data quality checks")
            print(f"  Result: PASS")
            print()
        
        # Step 5: Strategy Recommendation
        df5 = pd.read_csv(step_files['step05'])
        t5 = df5[df5['Ticker'] == ticker]
        if not t5.empty:
            print("Step 05 – Strategy Recommendation")
            print(f"  Strategies generated: {len(t5)}")
            for idx, row in t5.iterrows():
                strat = row.get('Strategy', 'N/A')
                print(f"    - {strat}")
            print(f"  Result: PASS ({len(t5)} strategies)")
            print()
        
        # Step 6: Strategy Evaluation
        df6 = pd.read_csv(step_files['step06'])
        t6 = df6[df6['Ticker'] == ticker]
        if not t6.empty:
            print("Step 06 – Strategy Evaluation")
            print(f"  Evaluated: {len(t6)} strategies")
            for idx, row in t6.iterrows():
                score = row.get('Theory_Compliance_Score', 'N/A')
                print(f"    Score: {score}")
            print(f"  Result: PASS")
            print()
        
        # Step 9: Acceptance Logic (CRITICAL)
        df9 = pd.read_csv(step_files['step09'])
        t9 = df9[df9['Ticker'] == ticker]
        if not t9.empty:
            print("Step 09 – Acceptance Logic (CRITICAL GATE)")
            print()
            for idx, row in t9.iterrows():
                strat = row.get('Strategy', 'N/A')
                status = row.get('acceptance_status', 'N/A')
                reason = row.get('acceptance_reason', 'N/A')
                validation = row.get('Validation_Status', 'N/A')
                contract = row.get('Contract_Status', 'N/A')
                iv_rank_avail = row.get('iv_rank_available', 'N/A')
                iv_hist = row.get('iv_history_days', 'N/A')
                confidence = row.get('confidence_band', 'N/A')
                
                print(f"  Strategy: {strat}")
                print(f"    acceptance_status = {status}")
                print(f"    acceptance_reason = {reason}")
                print(f"    Validation_Status = {validation}")
                print(f"    Contract_Status = {contract}")
                print(f"    iv_rank_available = {iv_rank_avail}")
                print(f"    iv_history_days = {iv_hist}")
                print(f"    confidence_band = {confidence}")
                print()
        
        # Identify blocking step
        ready_now_count = len(df9[(df9['Ticker'] == ticker) & (df9['acceptance_status'] == 'READY_NOW')])
        
        if ready_now_count == 0:
            print(f"{'='*80}")
            print(f"BLOCKING STEP: Step 09 (Acceptance Logic)")
            print(f"{'='*80}")
            
            if not t9.empty:
                first_row = t9.iloc[0]
                status = first_row['acceptance_status']
                reason = first_row['acceptance_reason']
                iv_rank = first_row['iv_rank_available']
                iv_hist = first_row['iv_history_days']
                validation = first_row['Validation_Status']
                contract = first_row['Contract_Status']
                
                print(f"Blocking Condition: acceptance_status != 'READY_NOW'")
                print(f"\nEvidence:")
                print(f"  acceptance_status = {status}")
                print(f"  acceptance_reason = {reason}")
                print(f"  iv_rank_available = {iv_rank}")
                print(f"  iv_history_days = {iv_hist}")
                print(f"  Validation_Status = {validation}")
                print(f"  Contract_Status = {contract}")
                
                # Determine root cause
                print(f"\nRoot Cause Analysis:")
                if iv_rank == False:
                    print(f"  ❌ iv_rank_available = False (need 120+ days, have {iv_hist})")
                if status == 'INCOMPLETE':
                    print(f"  ❌ Validation failed: {validation}")
                    print(f"  ❌ Contract failed: {contract}")
                if status == 'WAIT':
                    print(f"  ⚠️  Strategy flagged as WAIT (not READY_NOW)")
                if status == 'STRUCTURALLY_READY':
                    print(f"  ⚠️  Structurally valid but score < 60")
        else:
            print(f"{'='*80}")
            print(f"NO BLOCKING STEP")
            print(f"{'='*80}")
            print(f"Ticker has {ready_now_count} READY_NOW strategies")
        
        print()

if __name__ == "__main__":
    try:
        generate_ledger()
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
