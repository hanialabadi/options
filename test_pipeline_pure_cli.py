"""
Pure CLI Pipeline Test - No Streamlit Dependencies
Runs Steps 2→8 with 10 tickers showing complete output
"""

import pandas as pd
import sys
from pathlib import Path
import logging

# Setup logging to show INFO messages
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add parent to path
parent_dir = Path(__file__).parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

print("="*100)
print("COMPLETE PIPELINE TEST: STEPS 2 → 8 (10 TICKERS)")
print("="*100)
print()

# ============================================================================
# STEP 2: LOAD SNAPSHOT
# ============================================================================
print("📂 STEP 2: LOADING SNAPSHOT")
print("-"*100)

try:
    from core.scraper.ivhv_bootstrap import get_today_snapshot_path
    import os
    
    # Find latest snapshot
    today_snapshot = get_today_snapshot_path()
    
    if not os.path.exists(today_snapshot):
        # Fallback to archived snapshots
        archive_dir = Path("data/ivhv_archive")
        if archive_dir.exists():
            snapshots = sorted(archive_dir.glob("ivhv_snapshot_*.csv"), reverse=True)
            if snapshots:
                today_snapshot = str(snapshots[0])
            else:
                print("❌ No snapshots found in data/ivhv_archive/")
                sys.exit(1)
        else:
            print("❌ No snapshot data available")
            sys.exit(1)
    
    df_snapshot = pd.read_csv(today_snapshot)
    
    print(f"✅ Loaded snapshot: {len(df_snapshot)} rows")
    print(f"   File: {Path(today_snapshot).name}")
    print(f"   Unique tickers: {df_snapshot['Ticker'].nunique()}")
    print()
    
except Exception as e:
    print(f"❌ Step 2 failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ============================================================================
# STEPS 3-6: SCAN ENGINE (IVHV/Chart/Regime/GEM)
# ============================================================================
print("🔍 STEPS 3-6: SCAN ENGINE")
print("-"*100)

try:
    from core.scan_engine import (
        filter_ivhv_gap,
        compute_chart_signals,
        classify_regime,
        validate_data_quality
    )
    
    # Step 3: IVHV filter
    df_ivhv = filter_ivhv_gap(df_snapshot, min_gap=2.0)
    print(f"✅ Step 3: {len(df_ivhv)} candidates after IVHV filter")
    
    # Step 5: Chart signals
    df_charts = compute_chart_signals(df_ivhv)
    df_regime = classify_regime(df_charts)
    print(f"✅ Step 5: Chart signals computed")
    
    # Step 6: GEM validation
    df_gem = validate_data_quality(df_regime)
    print(f"✅ Step 6: {len(df_gem)} candidates validated")
    
    if 'GEM_Score' in df_gem.columns:
        print(f"   GEM Score range: {df_gem['GEM_Score'].min():.2f} - {df_gem['GEM_Score'].max():.2f}")
    print()
    
    # Show top 5 candidates
    print("Top 5 Candidates:")
    score_col = 'GEM_Score' if 'GEM_Score' in df_gem.columns else 'IV_Rank_30D'
    top5 = df_gem.nlargest(5, score_col)[['Ticker', score_col, 'Close']]
    for idx, row in top5.iterrows():
        print(f"   {row['Ticker']:6s} | Score={row[score_col]:.2f} | ${row['Close']:.2f}")
    print()
    
except Exception as e:
    print(f"❌ Scan steps failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ============================================================================
# LIMIT TO 10 TICKERS FOR SPEED
# ============================================================================
print("⚡ SPEED OPTIMIZATION: LIMITING TO 10 TICKERS")
print("-"*100)

# Get top 10 by GEM score
top_10_tickers = df_gem.nlargest(10, 'GEM_Score')['Ticker'].tolist()
df_limited = df_gem[df_gem['Ticker'].isin(top_10_tickers)].copy()

print(f"Selected tickers: {', '.join(top_10_tickers)}")
print(f"Rows: {len(df_limited)}")
print()

# ============================================================================
# STEP 7: STRATEGY RECOMMENDATIONS
# ============================================================================
print("🎯 STEP 7: STRATEGY RECOMMENDATIONS")
print("-"*100)

try:
    from core.scan_engine import generate_multi_strategy_suggestions
    
    df_strategies = generate_multi_strategy_suggestions(
        df_limited,
        tier_filter='tier1_only',
        exploration_mode=False
    )
    
    print(f"✅ Generated {len(df_strategies)} strategy recommendations")
    print(f"   Unique tickers: {df_strategies['Ticker'].nunique()}")
    print(f"   Avg strategies per ticker: {len(df_strategies) / df_strategies['Ticker'].nunique():.2f}")
    print()
    
    # Show strategy breakdown
    strategy_counts = df_strategies['Recommended_Strategy'].value_counts()
    print("Strategy Distribution:")
    for strategy, count in strategy_counts.head(7).items():
        print(f"   {strategy:25s}: {count:3d}")
    print()
    
except Exception as e:
    print(f"❌ Step 7 failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ============================================================================
# STEP 9A: DTE TIMEFRAMES
# ============================================================================
print("📅 STEP 9A: DTE TIMEFRAMES")
print("-"*100)

try:
    from core.scan_engine import determine_option_timeframe
    
    df_dte = determine_option_timeframe(df_strategies)
    
    print(f"✅ Assigned DTE windows for {len(df_dte)} strategy pairs")
    
    # Show timeframe distribution
    timeframe_counts = df_dte['DTE_Timeframe'].value_counts()
    print("\nTimeframe Distribution:")
    for timeframe, count in timeframe_counts.items():
        print(f"   {timeframe:15s}: {count:3d}")
    
    # Show DTE range
    print(f"\nDTE Range:")
    print(f"   Min Target: {df_dte['DTE_Min'].min():.0f} days")
    print(f"   Max Target: {df_dte['DTE_Max'].max():.0f} days")
    print(f"   Avg Target: {df_dte['DTE_Target'].mean():.0f} days")
    print()
    
except Exception as e:
    print(f"❌ Step 9A failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ============================================================================
# STEP 9B: EXPLORATION (FETCH CONTRACTS)
# ============================================================================
print("🔎 STEP 9B: EXPLORATION - FETCH CONTRACTS")
print("-"*100)

try:
    from core.scan_engine import fetch_and_select_contracts
    
    df_contracts = fetch_and_select_contracts(
        df_dte,
        num_contracts=1,
        dollar_allocation=1000.0
    )
    
    print(f"✅ Explored {len(df_contracts)} strategy opportunities")
    print()
    
    # Show exploration status breakdown
    if 'Contract_Selection_Status' in df_contracts.columns:
        status_counts = df_contracts['Contract_Selection_Status'].value_counts()
        print("Exploration Status:")
        for status, count in status_counts.items():
            print(f"   {status:25s}: {count:3d}")
        print()
        
        # Count successful explorations
        success_count = (df_contracts['Contract_Selection_Status'] == 'Success').sum()
        print(f"✅ Successful explorations: {success_count}/{len(df_contracts)} ({100*success_count/len(df_contracts):.1f}%)")
        print()
    
    # Show LEAP detection
    if 'Is_LEAP' in df_contracts.columns:
        leap_count = df_contracts['Is_LEAP'].sum()
        print(f"📅 LEAP strategies detected: {leap_count}/{len(df_contracts)}")
        if leap_count > 0:
            leap_tickers = df_contracts[df_contracts['Is_LEAP']][['Ticker', 'Recommended_Strategy', 'DTE_Actual']].head(3)
            print("   Example LEAPs:")
            for idx, row in leap_tickers.iterrows():
                print(f"      {row['Ticker']:6s} | {row['Recommended_Strategy']:20s} | DTE={row['DTE_Actual']:.0f}")
        print()
    
    # Show liquidity distribution
    if 'Liquidity_Class' in df_contracts.columns:
        liq_counts = df_contracts['Liquidity_Class'].value_counts()
        print("Liquidity Distribution:")
        for liq_class, count in liq_counts.items():
            print(f"   {liq_class:15s}: {count:3d}")
        print()
    
    # Show successful contracts with details
    successful = df_contracts[df_contracts['Contract_Selection_Status'] == 'Success']
    if len(successful) > 0:
        print(f"\n✅ SUCCESSFUL CONTRACTS ({len(successful)} found):")
        print("-"*100)
        for idx, row in successful.head(5).iterrows():
            print(f"   {row['Ticker']:6s} | {row['Recommended_Strategy']:20s} | "
                  f"Strike={row.get('Strike', 'N/A'):>8s} | "
                  f"Exp={row.get('Expiration_Date', 'N/A')} | "
                  f"DTE={row.get('DTE_Actual', 0):.0f} | "
                  f"{row.get('Liquidity_Class', 'Unknown'):12s}")
        print()
    
except Exception as e:
    print(f"❌ Step 9B failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ============================================================================
# STEP 10: PCS RECALIBRATION
# ============================================================================
print("🔧 STEP 10: PCS RECALIBRATION")
print("-"*100)

try:
    from core.scan_engine import recalibrate_and_filter
    
    df_calibrated = recalibrate_and_filter(df_contracts)
    
    print(f"✅ Calibrated {len(df_calibrated)} contracts")
    print()
    
except Exception as e:
    print(f"❌ Step 10 failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ============================================================================
# STEP 11: STRATEGY RANKING
# ============================================================================
print("🏆 STEP 11: STRATEGY RANKING")
print("-"*100)

try:
    from core.scan_engine import compare_and_rank_strategies
    
    # User profile for ranking
    user_profile = {
        'account_size': 10000,
        'risk_tolerance': 'medium',
        'goal': 'income'
    }
    
    df_ranked = compare_and_rank_strategies(df_calibrated, user_profile)
    
    print(f"✅ Ranked {len(df_ranked)} strategies")
    
    # Show ranking distribution
    if 'Strategy_Rank' in df_ranked.columns:
        rank_counts = df_ranked.groupby('Strategy_Rank').size().head(10)
        print("\nRanking Distribution:")
        for rank, count in rank_counts.items():
            if rank == 999:
                print(f"   Rank {rank} (Failed): {count:3d}")
            else:
                print(f"   Rank {rank:3d}: {count:3d}")
        print()
        
        # Show top-ranked strategies
        top_ranked = df_ranked[df_ranked['Strategy_Rank'] <= 3].sort_values('Strategy_Rank')
        if len(top_ranked) > 0:
            print(f"Top-Ranked Strategies ({len(top_ranked)} found):")
            for idx, row in top_ranked.head(5).iterrows():
                print(f"   Rank {row['Strategy_Rank']:3.0f} | "
                      f"{row['Ticker']:6s} | "
                      f"{row['Recommended_Strategy']:20s} | "
                      f"Score={row.get('Comparison_Score', 0):.2f}")
        print()
    
except Exception as e:
    print(f"❌ Step 11 failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ============================================================================
# STEP 8: FINAL SELECTION WITH AUDIT
# ============================================================================
print("✅ STEP 8: FINAL SELECTION & AUDIT")
print("-"*100)

try:
    from core.scan_engine import finalize_and_size_positions
    
    df_final = finalize_and_size_positions(
        df_ranked,
        account_size=10000,
        max_positions=50,
        min_score=60.0,
        max_risk_per_position=0.20
    )
    
    print(f"✅ Final selections: {len(df_final)}")
    print()
    
    if len(df_final) == 0:
        print("⚠️  No strategies passed final filters")
        print()
        
        # Show why strategies failed
        failed = df_ranked[df_ranked['Strategy_Rank'] > 3]
        if len(failed) > 0:
            print("Common rejection reasons:")
            if 'Contract_Selection_Status' in failed.columns:
                status_counts = failed['Contract_Selection_Status'].value_counts()
                for status, count in status_counts.head(5).items():
                    print(f"   {status}: {count}")
            print()
    else:
        print("="*100)
        print("FINAL TRADE RECOMMENDATIONS WITH AUDIT")
        print("="*100)
        print()
        
        for idx, row in df_final.iterrows():
            print(f"{'='*100}")
            print(f"TRADE #{idx+1}: {row['Ticker']} - {row['Recommended_Strategy']}")
            print(f"{'='*100}")
            print()
            
            # Contract details
            print("CONTRACT DETAILS:")
            print(f"   Strike:     {row.get('Strike', 'N/A')}")
            print(f"   Expiration: {row.get('Expiration_Date', 'N/A')}")
            print(f"   DTE:        {row.get('DTE_Actual', 0):.0f} days")
            print(f"   Premium:    ${row.get('Projected_Premium', 0):.2f}")
            print()
            
            # Allocation
            print("ALLOCATION:")
            print(f"   Contracts:       {row.get('Num_Contracts', 0):.0f}")
            print(f"   Dollar Amount:   ${row.get('Dollar_Allocation', 0):.2f}")
            print(f"   % of Account:    {100 * row.get('Dollar_Allocation', 0) / 10000:.1f}%")
            print()
            
            # Quality metrics
            print("QUALITY METRICS:")
            print(f"   Comparison Score: {row.get('Comparison_Score', 0):.2f}")
            print(f"   Strategy Rank:    {row.get('Strategy_Rank', 999):.0f}")
            print(f"   Liquidity:        {row.get('Liquidity_Class', 'Unknown')}")
            if 'Is_LEAP' in row:
                print(f"   LEAP:             {'Yes' if row['Is_LEAP'] else 'No'}")
            print()
            
            # 5 WHY Audit
            if 'Selection_Audit' in row and pd.notna(row['Selection_Audit']):
                print("AUDITABLE DECISION (5 WHY):")
                print("-"*100)
                audit = row['Selection_Audit']
                
                # Parse audit JSON if it's a string
                if isinstance(audit, str):
                    import json
                    try:
                        audit = json.loads(audit)
                    except:
                        pass
                
                if isinstance(audit, dict):
                    sections = [
                        ('WHY_Strategy', 'WHY THIS STRATEGY?'),
                        ('WHY_Contract', 'WHY THIS CONTRACT?'),
                        ('WHY_Liquidity', 'WHY THIS LIQUIDITY?'),
                        ('WHY_Capital', 'WHY THIS CAPITAL ALLOCATION?'),
                        ('WHY_Competitive', 'WHY BETTER THAN ALTERNATIVES?')
                    ]
                    
                    for key, title in sections:
                        if key in audit:
                            print(f"\n{title}")
                            print(f"{audit[key]}")
                
                print()
            
            print()
    
except Exception as e:
    print(f"❌ Step 8 failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("="*100)
print("PIPELINE TEST COMPLETE")
print("="*100)
print()
print("Next steps:")
print("1. Check dashboard at http://localhost:8501")
print("2. Review audit logs in output/")
print("3. Examine chain audit: output/step9b_chain_audit_*.csv")
print()
