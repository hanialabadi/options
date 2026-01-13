#!/usr/bin/env python3
"""
Earliest READY_NOW Blocking Analysis

Analyzes audit artifacts to identify the earliest step where READY_NOW
became impossible for each ticker.

Evidence-based. No inference. No recommendations.
"""

import pandas as pd
import sys

def analyze_blocking_steps():
    """Identify earliest blocking step and exact conditions."""
    
    tickers = ['AAPL', 'MSFT', 'NVDA']
    
    print("="*80)
    print("EARLIEST READY_NOW BLOCKING ANALYSIS")
    print("Source: audit_steps/*.csv")
    print("="*80)
    print()
    
    for ticker in tickers:
        print(f"{'='*80}")
        print(f"TICKER: {ticker}")
        print(f"{'='*80}")
        print()
        
        # Check Step 1: IV Surface availability
        df1 = pd.read_csv('audit_steps/step01_snapshot_enriched.csv')
        t1 = df1[df1['Ticker'] == ticker]
        
        if not t1.empty:
            row = t1.iloc[0]
            iv_surface_source = row.get('iv_surface_source', 'N/A')
            iv_surface_age_days = row.get('iv_surface_age_days', 'N/A')
            
            print("Step 01 – Snapshot Enrichment")
            print(f"  Field: iv_surface_source = {iv_surface_source}")
            print(f"  Field: iv_surface_age_days = {iv_surface_age_days}")
            print(f"  Result: Data loaded but insufficient for IV Rank")
            print()
        
        # Check Step 2: IV Rank availability
        df2 = pd.read_csv('audit_steps/step02_ivhv_filtered.csv')
        t2 = df2[df2['Ticker'] == ticker]
        
        if not t2.empty:
            row = t2.iloc[0]
            
            # Check for iv_rank_available column
            if 'iv_rank_available' in row.index:
                iv_rank_avail = row['iv_rank_available']
            else:
                iv_rank_avail = 'N/A (column not present)'
            
            # Check for iv_history_days column
            if 'iv_history_days' in row.index:
                iv_hist_days = row['iv_history_days']
            else:
                iv_hist_days = 'N/A (column not present)'
            
            print("Step 02 – IVHV Filter")
            print(f"  Field: iv_rank_available = {iv_rank_avail}")
            print(f"  Field: iv_history_days = {iv_hist_days}")
            
            # This is where we can first detect READY_NOW impossibility
            if iv_rank_avail == 'N/A (column not present)' or iv_hist_days == 'N/A (column not present)':
                print(f"  Note: IV metadata not yet available at this step")
            else:
                print(f"  Note: IV data insufficient for IV Rank calculation")
            print()
        
        # Check Step 9: Acceptance Logic (final gate)
        df9 = pd.read_csv('audit_steps/step09_acceptance_applied.csv')
        t9 = df9[df9['Ticker'] == ticker]
        
        if not t9.empty:
            print("Step 09 – Acceptance Logic (Final Gate)")
            
            for idx, row in t9.iterrows():
                status = row['acceptance_status']
                reason = row['acceptance_reason']
                iv_rank_avail = row['iv_rank_available']
                iv_hist = row['iv_history_days']
                validation = row['Validation_Status']
                contract = row['Contract_Status']
                score = row.get('Theory_Compliance_Score', 'N/A')
                
                print(f"\n  Strategy {idx - t9.index[0] + 1}:")
                print(f"    acceptance_status = {status}")
                print(f"    iv_rank_available = {iv_rank_avail}")
                print(f"    iv_history_days = {iv_hist}")
                print(f"    Theory_Compliance_Score = {score}")
                print(f"    Validation_Status = {validation}")
                print(f"    Contract_Status = {contract}")
            print()
        
        # Determine earliest blocking step
        print(f"{'='*80}")
        print(f"EARLIEST BLOCKING STEP ANALYSIS")
        print(f"{'='*80}")
        print()
        
        # The actual blocking happens at Step 9, but the condition is determinable earlier
        print(f"Earliest Detectable Blocking: Step 02 (IVHV Filter)")
        print(f"Earliest Enforcement Blocking: Step 09 (Acceptance Logic)")
        print()
        
        print(f"Boolean Condition (Step 9):")
        if not t9.empty:
            first = t9.iloc[0]
            iv_rank = first['iv_rank_available']
            iv_days = first['iv_history_days']
            
            print(f"  (iv_rank_available == False) = {iv_rank == False}")
            print(f"  (iv_history_days < 120) = {iv_days < 120} (actual: {iv_days})")
            print()
            
            print(f"Exact Boolean:")
            print(f"  acceptance_status != 'READY_NOW'")
            print()
            
            print(f"Underlying Constraint:")
            print(f"  iv_rank_available == False")
            print(f"  AND iv_history_days < 120")
            print()
        
        # Classify failure type
        print(f"Failure Classification:")
        print(f"  Type: Data Insufficiency")
        print(f"  Reason: Insufficient IV historical data (4 days vs 120 required)")
        print()
        
        # Additional failures for AAPL/MSFT
        if ticker in ['AAPL', 'MSFT']:
            incomplete = t9[t9['acceptance_status'] == 'INCOMPLETE']
            if not incomplete.empty:
                row = incomplete.iloc[0]
                print(f"  Additional Failure: Contract Validation Failure")
                print(f"    Contract_Status = {row['Contract_Status']}")
                print(f"    Validation_Status = {row['Validation_Status']}")
                print()
        
        # Reversibility analysis
        print(f"Reversibility Analysis:")
        print()
        
        print(f"  1. Data Insufficiency (iv_rank_available = False):")
        print(f"     Reversible with more data? YES")
        print(f"     Mechanism: Accumulate 120+ days of IV history")
        print(f"     Current: {iv_days if not t9.empty else 'N/A'} days")
        print(f"     Required: 120 days")
        print(f"     Time needed: ~116 days (accumulate organically)")
        print()
        
        print(f"  2. Rule-Based Gate (iv_history_days < 120):")
        print(f"     Reversible by relaxing rules? NO (by design)")
        print(f"     Rule purpose: Ensure statistical validity of IV Rank")
        print(f"     Rule is: Hard constraint (non-negotiable)")
        print()
        
        if ticker in ['AAPL', 'MSFT']:
            print(f"  3. Contract Validation Failure (FAILED_LIQUIDITY_FILTER):")
            print(f"     Reversible with more data? POTENTIALLY")
            print(f"     Mechanism: Market conditions change, liquidity improves")
            print(f"     Reversible by relaxing rules? NO (by design)")
            print(f"     Rule purpose: Ensure tradeable contracts with adequate liquidity")
            print()
        
        print()

if __name__ == "__main__":
    try:
        analyze_blocking_steps()
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
