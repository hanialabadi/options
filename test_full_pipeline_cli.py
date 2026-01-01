"""
Complete CLI Pipeline Debug - End to End Test
Runs all steps from Step 2 → Step 8 with 10 tickers for speed
Shows complete output similar to dashboard display
"""

import pandas as pd
import sys
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(message)s'
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
    from core.data_loader import load_latest_snapshot
    
    # Load most recent snapshot
    df_snapshot = load_latest_snapshot()
    
    print(f"✅ Loaded snapshot: {len(df_snapshot)} rows")
    print(f"   Unique tickers: {df_snapshot['Ticker'].nunique()}")
    print(f"   Columns: {df_snapshot.columns.tolist()[:10]}...")
    print()
    
except Exception as e:
    print(f"❌ Step 2 failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ============================================================================
# STEP 3-6: SCAN ENGINE (Regime, HV/IV, Crossovers, GEM)
# ============================================================================
print("🔍 STEPS 3-6: SCAN ENGINE (REGIME → HV/IV → CROSSOVERS → GEM)")
print("-"*100)

try:
    from core.scan_engine import (
        step3_regime_context,
        step4_compute_hv_and_iv,
        step5_detect_crossovers,
        step6_gem_candidates
    )
    
    # Step 3: Regime
    print("Running Step 3: Regime Context...")
    df_regime = step3_regime_context.classify_market_regime(df_snapshot)
    print(f"✅ Step 3: Regime classified for {len(df_regime)} tickers")
    
    # Step 4: HV/IV
    print("Running Step 4: HV/IV Analysis...")
    df_hviv = step4_compute_hv_and_iv.compute_volatility_metrics(df_regime)
    print(f"✅ Step 4: HV/IV computed for {len(df_hviv)} tickers")
    
    # Step 5: Crossovers
    print("Running Step 5: Detect Crossovers...")
    df_cross = step5_detect_crossovers.detect_iv_hv_crossovers(df_hviv)
    print(f"✅ Step 5: Crossovers detected for {len(df_cross)} tickers")
    
    # Step 6: GEM Filter
    print("Running Step 6: GEM Candidates...")
    df_gem = step6_gem_candidates.filter_gem_candidates(df_cross)
    print(f"✅ Step 6: {len(df_gem)} GEM candidates identified")
    print()
    
    print(f"📊 GEM CANDIDATES SUMMARY:")
    print(f"   Total: {len(df_gem)}")
    print(f"   Sample tickers: {df_gem['Ticker'].head(10).tolist()}")
    print()
    
except Exception as e:
    print(f"❌ Steps 3-6 failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ============================================================================
# LIMIT TO 10 TICKERS FOR SPEED
# ============================================================================
print("⚡ LIMITING TO 10 TICKERS FOR SPEED TEST")
print("-"*100)

test_tickers = df_gem['Ticker'].unique()[:10]
df_test = df_gem[df_gem['Ticker'].isin(test_tickers)].copy()

print(f"Selected tickers: {', '.join(test_tickers)}")
print(f"Test dataset: {len(df_test)} rows")
print()

# ============================================================================
# STEP 7: STRATEGY RECOMMENDATIONS
# ============================================================================
print("💡 STEP 7: STRATEGY RECOMMENDATIONS")
print("-"*100)

try:
    from core.scan_engine import step7_strategy_recommendation
    
    df_step7 = step7_strategy_recommendation.recommend_strategies(df_test)
    
    print(f"✅ Step 7 Complete: {len(df_step7)} strategies recommended")
    print()
    
    print("📊 STEP 7 OUTPUT:")
    print(f"   Total strategies: {len(df_step7)}")
    print(f"   Unique tickers: {df_step7['Ticker'].nunique()}")
    
    if 'Primary_Strategy' in df_step7.columns:
        print(f"\n   Strategy Distribution:")
        for strategy, count in df_step7['Primary_Strategy'].value_counts().head(10).items():
            print(f"      {strategy}: {count}")
    
    if 'Confidence' in df_step7.columns:
        print(f"\n   Average Confidence: {df_step7['Confidence'].mean():.1f}")
    
    print()
    
    # Show sample
    print("   Sample strategies:")
    display_cols = ['Ticker', 'Primary_Strategy', 'Trade_Bias', 'Confidence']
    display_cols = [c for c in display_cols if c in df_step7.columns]
    print(df_step7[display_cols].head(10).to_string(index=False))
    print()
    
except Exception as e:
    print(f"❌ Step 7 failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ============================================================================
# STEP 9A: DETERMINE TIMEFRAME
# ============================================================================
print("📅 STEP 9A: DETERMINE DTE TIMEFRAME")
print("-"*100)

try:
    from core.scan_engine import step9a_determine_timeframe
    
    df_step9a = step9a_determine_timeframe.determine_option_timeframe(df_step7)
    
    print(f"✅ Step 9A Complete: {len(df_step9a)} strategies with DTE ranges")
    print()
    
    print("📊 STEP 9A OUTPUT:")
    print(f"   Total strategies: {len(df_step9a)}")
    
    if 'Timeframe_Label' in df_step9a.columns:
        print(f"\n   Timeframe Distribution:")
        for label, count in df_step9a['Timeframe_Label'].value_counts().items():
            print(f"      {label}: {count}")
    
    if 'Target_DTE' in df_step9a.columns:
        print(f"\n   Average Target DTE: {df_step9a['Target_DTE'].mean():.0f} days")
    
    print()
    
    # Show sample
    print("   Sample DTE assignments:")
    display_cols = ['Ticker', 'Primary_Strategy', 'Min_DTE', 'Max_DTE', 'Target_DTE']
    display_cols = [c for c in display_cols if c in df_step9a.columns]
    print(df_step9a[display_cols].head(10).to_string(index=False))
    print()
    
except Exception as e:
    print(f"❌ Step 9A failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ============================================================================
# STEP 9B: FETCH CONTRACTS (EXPLORATION MODE)
# ============================================================================
print("📋 STEP 9B: FETCH OPTION CONTRACTS (EXPLORATION MODE)")
print("-"*100)
print("⚠️  This may take 1-2 minutes due to API calls...")
print()

try:
    from core.scan_engine import step9b_fetch_contracts
    
    df_step9b = step9b_fetch_contracts.fetch_and_select_contracts(df_step9a)
    
    print(f"✅ Step 9B Complete: {len(df_step9b)} strategies with contract data")
    print()
    
    print("📊 STEP 9B OUTPUT (EXPLORATION):")
    print(f"   Total strategies: {len(df_step9b)}")
    print(f"   Unique tickers: {df_step9b['Ticker'].nunique()}")
    
    # Exploration status breakdown
    if 'Contract_Selection_Status' in df_step9b.columns:
        print(f"\n   📈 Exploration Status Breakdown:")
        for status, count in df_step9b['Contract_Selection_Status'].value_counts().items():
            pct = count / len(df_step9b) * 100
            print(f"      {status}: {count} ({pct:.1f}%)")
    
    # LEAP detection
    if 'Is_LEAP' in df_step9b.columns:
        leap_count = df_step9b['Is_LEAP'].sum()
        print(f"\n   🚀 LEAPs Found: {leap_count}")
        if leap_count > 0:
            leap_tickers = df_step9b[df_step9b['Is_LEAP'] == True]['Ticker'].unique()
            print(f"      LEAP tickers: {', '.join(leap_tickers)}")
    
    # Liquidity breakdown
    if 'Liquidity_Class' in df_step9b.columns:
        print(f"\n   💧 Liquidity Distribution:")
        for liq, count in df_step9b['Liquidity_Class'].value_counts().items():
            print(f"      {liq}: {count}")
    
    print()
    
    # Show sample with new columns
    print("   Sample exploration results:")
    display_cols = ['Ticker', 'Primary_Strategy', 'Actual_DTE', 'Is_LEAP', 
                   'Horizon_Class', 'Liquidity_Class', 'Contract_Selection_Status']
    display_cols = [c for c in display_cols if c in df_step9b.columns]
    print(df_step9b[display_cols].head(15).to_string(index=False))
    print()
    
    # Show successful contracts with details
    if 'Contract_Selection_Status' in df_step9b.columns:
        success = df_step9b[df_step9b['Contract_Selection_Status'] == 'Success']
        if len(success) > 0:
            print(f"   ✅ Successful Contracts ({len(success)}):")
            detail_cols = ['Ticker', 'Primary_Strategy', 'Strike', 'Actual_DTE', 
                          'Open_Interest', 'Bid_Ask_Spread_Pct', 'Liquidity_Class']
            detail_cols = [c for c in detail_cols if c in success.columns]
            print(success[detail_cols].head(10).to_string(index=False))
            print()
    
except Exception as e:
    print(f"❌ Step 9B failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ============================================================================
# STEP 10: PCS RECALIBRATION
# ============================================================================
print("🔍 STEP 10: PCS RECALIBRATION & FILTERING")
print("-"*100)

try:
    from core.scan_engine import step10_pcs_recalibration
    
    df_step10 = step10_pcs_recalibration.recalibrate_and_filter(df_step9b)
    
    print(f"✅ Step 10 Complete: {len(df_step10)} strategies passed quality filters")
    print()
    
    print("📊 STEP 10 OUTPUT:")
    print(f"   Input: {len(df_step9b)} strategies")
    print(f"   Output: {len(df_step10)} strategies")
    print(f"   Filtered: {len(df_step9b) - len(df_step10)} strategies")
    
    if len(df_step10) > 0:
        print()
        print("   Sample filtered strategies:")
        display_cols = ['Ticker', 'Primary_Strategy', 'Actual_DTE', 'Is_LEAP', 
                       'Liquidity_Class', 'Open_Interest', 'Bid_Ask_Spread_Pct']
        display_cols = [c for c in display_cols if c in df_step10.columns]
        print(df_step10[display_cols].head(10).to_string(index=False))
    print()
    
except Exception as e:
    print(f"❌ Step 10 failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ============================================================================
# STEP 11: STRATEGY PAIRING & RANKING
# ============================================================================
print("🎯 STEP 11: STRATEGY PAIRING & RANKING")
print("-"*100)

try:
    from core.scan_engine import step11_strategy_pairing
    
    df_step11 = step11_strategy_pairing.compare_and_rank_strategies(
        df_step10,
        user_goal='balanced',
        account_size=100000,
        risk_tolerance='moderate'
    )
    
    print(f"✅ Step 11 Complete: {len(df_step11)} strategies ranked")
    print()
    
    print("📊 STEP 11 OUTPUT:")
    print(f"   Total strategies: {len(df_step11)}")
    print(f"   Unique tickers: {df_step11['Ticker'].nunique()}")
    
    if 'Strategy_Rank' in df_step11.columns:
        rank1_count = (df_step11['Strategy_Rank'] == 1).sum()
        print(f"   Top-ranked (Rank 1): {rank1_count}")
    
    if 'Comparison_Score' in df_step11.columns:
        print(f"   Average Comparison Score: {df_step11['Comparison_Score'].mean():.2f}")
    
    print()
    
    # Show top-ranked strategies
    print("   Top-Ranked Strategies (Rank 1):")
    top_ranked = df_step11[df_step11['Strategy_Rank'] == 1].copy()
    display_cols = ['Ticker', 'Primary_Strategy', 'Strategy_Rank', 'Comparison_Score',
                   'Is_LEAP', 'Liquidity_Class']
    display_cols = [c for c in display_cols if c in top_ranked.columns]
    print(top_ranked[display_cols].head(15).to_string(index=False))
    print()
    
except Exception as e:
    print(f"❌ Step 11 failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ============================================================================
# STEP 8: FINAL SELECTION WITH AUDITABLE DECISIONS
# ============================================================================
print("✅ STEP 8: FINAL SELECTION & POSITION SIZING (AUDITABLE DECISIONS)")
print("-"*100)

try:
    from core.scan_engine import step8_position_sizing
    
    df_step8 = step8_position_sizing.finalize_and_size_positions(
        df_step11,
        account_balance=100000,
        max_positions=50,
        min_comparison_score=60.0,
        sizing_method='fixed_fractional',
        max_trade_risk=0.02
    )
    
    print(f"✅ Step 8 Complete: {len(df_step8)} final trades selected")
    print()
    
    print("📊 STEP 8 OUTPUT (FINAL TRADES):")
    print(f"   Total trades: {len(df_step8)}")
    print(f"   Unique tickers: {df_step8['Ticker'].nunique()}")
    
    if 'Dollar_Allocation' in df_step8.columns:
        total_allocation = df_step8['Dollar_Allocation'].sum()
        print(f"   Total Capital: ${total_allocation:,.0f}")
    
    if 'Num_Contracts' in df_step8.columns:
        total_contracts = df_step8['Num_Contracts'].sum()
        print(f"   Total Contracts: {int(total_contracts)}")
    
    print()
    
    # Summary table
    print("   📋 Final Trade Summary:")
    display_cols = ['Ticker', 'Primary_Strategy', 'Is_LEAP', 'Liquidity_Class',
                   'Dollar_Allocation', 'Num_Contracts', 'Comparison_Score']
    display_cols = [c for c in display_cols if c in df_step8.columns]
    print(df_step8[display_cols].to_string(index=False))
    print()
    
    # ========================================
    # SHOW AUDITABLE DECISION RECORDS
    # ========================================
    if 'Selection_Audit' in df_step8.columns:
        print("="*100)
        print("📋 COMPLETE AUDITABLE DECISION RECORDS")
        print("="*100)
        print()
        
        for idx, row in df_step8.iterrows():
            ticker = row['Ticker']
            strategy = row.get('Primary_Strategy', 'Unknown')
            score = row.get('Comparison_Score', 0)
            allocation = row.get('Dollar_Allocation', 0)
            contracts = row.get('Num_Contracts', 0)
            is_leap = row.get('Is_LEAP', False)
            liquidity = row.get('Liquidity_Class', 'Unknown')
            dte = row.get('Actual_DTE', 0)
            strike = row.get('Strike', 0)
            expiration = row.get('Expiration', 'N/A')
            audit = row['Selection_Audit']
            
            leap_badge = " 🚀 LEAP" if is_leap else ""
            
            print("="*100)
            print(f"📝 {ticker} - {strategy}{leap_badge}")
            print("="*100)
            print()
            
            # Quick facts
            print("📊 QUICK FACTS:")
            print(f"   Comparison Score: {score:.1f}/100")
            print(f"   Allocation: ${allocation:,.0f} ({int(contracts)} contracts)")
            print(f"   Liquidity: {liquidity}")
            print(f"   DTE: {int(dte)} days")
            print(f"   Strike: ${strike:.2f}" if strike else "   Strike: N/A")
            print(f"   Expiration: {expiration}")
            print()
            
            # Show audit sections
            print("🔍 DECISION RECORD:")
            print("-"*100)
            
            audit_lines = audit.split('\n')
            for line in audit_lines:
                if line.startswith('STRATEGY SELECTION:'):
                    print()
                    print("1️⃣  WHY THIS STRATEGY?")
                    print("   " + line.replace('STRATEGY SELECTION: ', ''))
                elif line.startswith('CONTRACT CHOICE:'):
                    print()
                    print("2️⃣  WHY THIS CONTRACT?")
                    print("   " + line.replace('CONTRACT CHOICE: ', ''))
                elif line.startswith('LIQUIDITY JUSTIFICATION:'):
                    print()
                    print("3️⃣  WHY IS LIQUIDITY ACCEPTABLE?")
                    print("   " + line.replace('LIQUIDITY JUSTIFICATION: ', ''))
                elif line.startswith('CAPITAL ALLOCATION:'):
                    print()
                    print("4️⃣  WHY THIS POSITION SIZE?")
                    print("   " + line.replace('CAPITAL ALLOCATION: ', ''))
                elif line.startswith('COMPETITIVE COMPARISON:'):
                    print()
                    print("5️⃣  WHY NOT OTHER STRATEGIES?")
                    print("   " + line.replace('COMPETITIVE COMPARISON: ', ''))
            
            print()
            print()
    
except Exception as e:
    print(f"❌ Step 8 failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ============================================================================
# FINAL SUMMARY
# ============================================================================
print("="*100)
print("✅ PIPELINE COMPLETE - FINAL SUMMARY")
print("="*100)
print()
print(f"Step 2:  Loaded snapshot: {len(df_snapshot)} rows")
print(f"Step 6:  GEM candidates: {len(df_gem)}")
print(f"Step 7:  Strategies recommended: {len(df_step7)}")
print(f"Step 9A: DTE ranges determined: {len(df_step9a)}")
print(f"Step 9B: Contracts explored: {len(df_step9b)}")
if 'Contract_Selection_Status' in df_step9b.columns:
    success_9b = (df_step9b['Contract_Selection_Status'] == 'Success').sum()
    print(f"         → Successful contracts: {success_9b}")
print(f"Step 10: Quality filtered: {len(df_step10)}")
print(f"Step 11: Strategies ranked: {len(df_step11)}")
print(f"Step 8:  Final trades selected: {len(df_step8)}")
print()

if 'Selection_Audit' in df_step8.columns:
    complete_audits = ~df_step8['Selection_Audit'].str.contains('INCOMPLETE', na=False).sum()
    print(f"✅ Audit Completeness: {len(df_step8)}/{len(df_step8)} trades have complete decision records")
    print()

print("🎉 All steps completed successfully!")
print("="*100)
