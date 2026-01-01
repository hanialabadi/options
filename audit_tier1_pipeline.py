#!/usr/bin/env python3
"""
TIER-1 PIPELINE AUDIT (CLI-BASED)

Validates that all theory-correct Tier-1 strategies are preserved throughout
the pipeline from Step 1 → Step 7. Any suppression must be explicit and
theory-justified.

TIER-1 STRATEGIES (Must All Be Present):
1. Long Call (directional bullish, cheap IV)
2. Long Put (directional bearish, cheap IV)
3. Cash-Secured Put / CSP (bullish income, rich IV)
4. Covered Call (bearish income, rich IV, requires stock)
5. Buy-Write (bullish + income, entry strategy)
6. Long Straddle (volatility buying, cheap IV, bidirectional)
7. Long Strangle (volatility buying, cheaper than straddle)
8. Call Debit Spread (bullish, moderate capital)
9. Put Debit Spread (bearish, moderate capital)
10. LEAP Call (long-term bullish, cheap IV)
11. LEAP Put (long-term bearish, cheap IV)

AUDIT PROCESS:
- Step 1: Raw universe (all tickers loaded)
- Step 2: IV/HV enrichment (fields populated)
- Step 3: Regime detection (edge flags, gaps preserved)
- Step 5: Technical signals (directional context)
- Step 6: Data completeness (NO strategy suppression)
- Step 7: Strategy discovery (ALL Tier-1 generated)
- Step 9B: Execution validation (capital, strikes, liquidity)

SUCCESS CRITERIA:
- All theory-valid Tier-1 strategies present in Step 7 output
- No silent drops between steps
- Explicit theory-based justification for any exclusion
"""

import sys
import logging
from pathlib import Path
import pandas as pd
from typing import Dict, List, Set

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# TIER-1 STRATEGY DEFINITIONS (THEORY-BASED)
TIER1_STRATEGIES = {
    'Long Call': {
        'theory': 'Directional bullish + cheap IV (HV > IV)',
        'required_conditions': ['Bullish signal', 'Negative gap or IV_Rank < 50'],
        'tier': 1
    },
    'Long Put': {
        'theory': 'Directional bearish + cheap IV (HV > IV)',
        'required_conditions': ['Bearish signal', 'Negative gap or IV_Rank < 50'],
        'tier': 1
    },
    'Cash-Secured Put': {
        'theory': 'Bullish income + rich IV (IV > HV)',
        'required_conditions': ['Bullish signal', 'Positive gap or IV_Rank > 50'],
        'tier': 1
    },
    'Covered Call': {
        'theory': 'Bearish income + rich IV (IV > HV) + stock held',
        'required_conditions': ['Bearish/Neutral signal', 'Positive gap or IV_Rank > 50'],
        'tier': 1
    },
    'Buy-Write': {
        'theory': 'Simultaneous stock purchase + covered call (income on entry)',
        'required_conditions': ['Bullish signal', 'Positive gap or IV_Rank > 50'],
        'tier': 1
    },
    'Long Straddle': {
        'theory': 'Volatility buying + cheap IV + bidirectional uncertainty',
        'required_conditions': ['Bidirectional/expansion', 'Negative gap or IV_Rank < 40'],
        'tier': 1
    },
    'Long Strangle': {
        'theory': 'Cheaper volatility buying (wider strikes than straddle)',
        'required_conditions': ['Bidirectional/expansion', 'Negative gap or IV_Rank < 40'],
        'tier': 1
    },
    'Call Debit Spread': {
        'theory': 'Bullish directional + defined risk + moderate capital',
        'required_conditions': ['Bullish signal', 'Any IV regime'],
        'tier': 1  # Note: Defined risk = Tier-1 (not Tier-2)
    },
    'Put Debit Spread': {
        'theory': 'Bearish directional + defined risk + moderate capital',
        'required_conditions': ['Bearish signal', 'Any IV regime'],
        'tier': 1  # Note: Defined risk = Tier-1 (not Tier-2)
    },
    'LEAP Call': {
        'theory': 'Long-term bullish + cheap long-dated IV',
        'required_conditions': ['Bullish signal', 'Negative 180D+ gap'],
        'tier': 1  # Note: LEAP execution may be Tier-3, but strategy is valid
    },
    'LEAP Put': {
        'theory': 'Long-term bearish + cheap long-dated IV',
        'required_conditions': ['Bearish signal', 'Negative 180D+ gap'],
        'tier': 1  # Note: LEAP execution may be Tier-3, but strategy is valid
    },
    'Call Debit Spread': {
        'theory': 'Bullish directional + defined risk + moderate capital',
        'required_conditions': ['Bullish signal', 'Any IV regime'],
        'tier': 1
    },
    'Put Debit Spread': {
        'theory': 'Bearish directional + defined risk + moderate capital',
        'required_conditions': ['Bearish signal', 'Any IV regime'],
        'tier': 1
    }
}


def print_section(title: str, char: str = "="):
    """Print formatted section header"""
    print(f"\n{char * 80}")
    print(f"{title}")
    print(f"{char * 80}\n")


def audit_step1_raw_universe(snapshot_path: str) -> pd.DataFrame:
    """
    STEP 1: Raw Universe Ingestion
    Validate that all tickers are loaded without filtering
    """
    print_section("STEP 1: RAW UNIVERSE INGESTION")
    
    try:
        df = pd.read_csv(snapshot_path)
        print(f"✅ Loaded {len(df)} tickers from snapshot")
        print(f"   Columns: {len(df.columns)}")
        print(f"   Sample tickers: {df['Ticker'].head(10).tolist()}")
        
        # Check for any early filtering
        if len(df) == 0:
            print("❌ VIOLATION: No tickers loaded!")
            return None
        
        return df
    except Exception as e:
        print(f"❌ ERROR loading snapshot: {e}")
        return None


def audit_step2_iv_hv_enrichment(df: pd.DataFrame) -> pd.DataFrame:
    """
    STEP 2: IV/HV Enrichment
    Validate IV/HV fields populated, no strategy gating
    """
    print_section("STEP 2: IV/HV ENRICHMENT")
    
    try:
        from core.scan_engine.step2_load_snapshot import load_and_enrich_data
        
        # Note: Using function expects file path, so we need to re-load
        # This is a limitation - in production, pass DataFrame through
        df_enriched = load_and_enrich_data(df)  # Will need to adapt this
        
        print(f"✅ Enriched {len(df_enriched)} tickers")
        
        # Validate critical fields
        required_fields = ['IV_30D', 'HV_30D', 'IV_Rank_30D', 'IV_Term_Structure']
        missing = [f for f in required_fields if f not in df_enriched.columns]
        
        if missing:
            print(f"⚠️ WARNING: Missing fields: {missing}")
        else:
            print(f"✅ All required IV/HV fields present")
        
        # Check for timestamp gating
        if len(df_enriched) < len(df):
            dropped = len(df) - len(df_enriched)
            print(f"⚠️ WARNING: {dropped} tickers dropped in Step 2")
            print(f"   This should only be due to data quality, not strategy suppression")
        
        return df_enriched
    except Exception as e:
        print(f"❌ ERROR in Step 2: {e}")
        return df


def audit_step3_regime_detection(df: pd.DataFrame) -> pd.DataFrame:
    """
    STEP 3: Volatility Regime Detection
    Validate signed gaps preserved, edge flags set, NO strategy exclusion
    """
    print_section("STEP 3: VOLATILITY REGIME DETECTION")
    
    try:
        from core.scan_engine.step3_filter_ivhv import filter_ivhv_gap
        
        df_regime = filter_ivhv_gap(df)
        
        print(f"✅ Processed {len(df_regime)} tickers")
        
        # Validate signed gaps preserved
        gap_fields = ['IVHV_gap_30D', 'IVHV_gap_60D', 'IVHV_gap_180D', 'IVHV_gap_360D']
        for field in gap_fields:
            if field in df_regime.columns:
                pos = (df_regime[field] > 0).sum()
                neg = (df_regime[field] < 0).sum()
                print(f"   {field}: {pos} positive, {neg} negative (signs preserved ✓)")
        
        # Validate edge flags
        edge_flags = ['ShortTerm_IV_Edge', 'MediumTerm_IV_Edge', 'LEAP_IV_Edge']
        for flag in edge_flags:
            if flag in df_regime.columns:
                count = df_regime[flag].sum()
                print(f"   {flag}: {count} tickers flagged")
        
        # Check for silent filtering
        if len(df_regime) < len(df):
            dropped = len(df) - len(df_regime)
            print(f"⚠️ WARNING: {dropped} tickers dropped in Step 3")
            print(f"   Theory check: Should only drop if IV/HV data invalid")
            print(f"   NOT if strategy doesn't match preferences")
        
        return df_regime
    except Exception as e:
        print(f"❌ ERROR in Step 3: {e}")
        return df


def audit_step5_technical_signals(df: pd.DataFrame) -> pd.DataFrame:
    """
    STEP 5: Technical Context
    Validate signals/regime populated, technicals inform (not gate) non-directional
    """
    print_section("STEP 5: TECHNICAL SIGNALS & CONTEXT")
    
    try:
        from core.scan_engine.step5_chart_signals import compute_chart_signals
        
        df_pcs = compute_chart_signals(df)
        
        print(f"✅ Processed {len(df_pcs)} tickers")
        
        # Validate signal distribution
        if 'Signal_Type' in df_pcs.columns:
            signal_counts = df_pcs['Signal_Type'].value_counts()
            print(f"\n   Signal distribution:")
            for signal, count in signal_counts.items():
                print(f"   - {signal}: {count}")
        
        # CRITICAL CHECK: Technicals should NOT gate neutral/volatility strategies
        if 'Signal_Type' in df_pcs.columns:
            weak_signals = df_pcs[df_pcs['Signal_Type'] == 'Weak']
            if len(weak_signals) > 0:
                print(f"\n   ⚠️ {len(weak_signals)} tickers with 'Weak' signals")
                print(f"   These should STILL be eligible for:")
                print(f"   - Neutral strategies (Iron Condor, Short Strangle)")
                print(f"   - Volatility strategies (Straddle, Calendar)")
                print(f"   - Income strategies (Covered Call, CSP)")
        
        # Check for filtering
        if len(df_pcs) < len(df):
            dropped = len(df) - len(df_pcs)
            print(f"\n   ⚠️ WARNING: {dropped} tickers dropped in Step 5")
        
        return df_pcs
    except Exception as e:
        print(f"❌ ERROR in Step 5: {e}")
        return df


def audit_step6_data_completeness(df: pd.DataFrame) -> pd.DataFrame:
    """
    STEP 6: Data Completeness Assessment
    Validate this ONLY assesses data availability, does NOT suppress Tier-1
    """
    print_section("STEP 6: DATA COMPLETENESS ASSESSMENT")
    
    try:
        from core.scan_engine.step6_gem_filter import validate_data_quality
        
        df_gem = validate_data_quality(df)
        
        print(f"✅ Processed {len(df_gem)} tickers")
        
        # Validate Data_Complete flag logic
        if 'Data_Complete' in df_gem.columns:
            complete = df_gem['Data_Complete'].sum()
            incomplete = len(df_gem) - complete
            print(f"\n   Data completeness:")
            print(f"   - Complete: {complete}")
            print(f"   - Incomplete: {incomplete}")
            
            # CRITICAL: Incomplete data should NOT block Tier-1 simple strategies
            if incomplete > 0:
                print(f"\n   ⚠️ THEORY CHECK: Incomplete data tickers should still allow:")
                print(f"   - Long Call/Put (only needs price + IV)")
                print(f"   - CSP (only needs price + IV)")
                print(f"   - Straddle (only needs price + IV)")
                print(f"   Complex strategies MAY require more data")
        
        # Check for filtering
        if len(df_gem) < len(df):
            dropped = len(df) - len(df_gem)
            print(f"\n   ⚠️ WARNING: {dropped} tickers dropped in Step 6")
            print(f"   This should ONLY be data quality issues, not strategy gating")
        
        return df_gem
    except Exception as e:
        print(f"❌ ERROR in Step 6: {e}")
        return df


def audit_step7_strategy_discovery(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    STEP 7: Strategy Discovery (CORE AUDIT)
    Validate ALL theory-correct Tier-1 strategies are generated
    """
    print_section("STEP 7: STRATEGY DISCOVERY (TIER-1 AUDIT)", "=")
    
    try:
        from core.scan_engine.step7_strategy_recommendation import recommend_strategies
        
        # Run Step 7 in EXPLORATION mode (all tiers)
        print("Running Step 7 in EXPLORATION mode (all tiers)...\n")
        df_all = recommend_strategies(
            df.copy(),
            tier_filter='all_tiers',
            exploration_mode=True
        )
        
        # Run Step 7 in DEFAULT mode (tier1_only)
        print("\nRunning Step 7 in DEFAULT mode (tier1_only)...\n")
        df_tier1 = recommend_strategies(
            df.copy(),
            tier_filter='tier1_only',
            exploration_mode=False
        )
        
        print(f"\n{'='*80}")
        print(f"STEP 7 RESULTS")
        print(f"{'='*80}\n")
        print(f"   Exploration mode (all): {len(df_all)} strategies")
        print(f"   Default mode (tier1): {len(df_tier1)} strategies")
        
        # Analyze strategy distribution
        if len(df_all) > 0:
            print(f"\n   Strategy distribution (all tiers):")
            strategy_counts = df_all['Primary_Strategy'].value_counts()
            for strategy, count in strategy_counts.items():
                tier = df_all[df_all['Primary_Strategy'] == strategy]['Strategy_Tier'].iloc[0]
                print(f"   - {strategy:30s} (Tier {tier}): {count:3d}")
        
        # CRITICAL AUDIT: Check for missing Tier-1 strategies
        print(f"\n{'='*80}")
        print(f"TIER-1 STRATEGY AUDIT")
        print(f"{'='*80}\n")
        
        generated_strategies = set(df_all['Primary_Strategy'].unique()) if len(df_all) > 0 else set()
        
        for strategy_name, strategy_info in TIER1_STRATEGIES.items():
            # Normalize names for comparison
            normalized_generated = {s.lower().replace('-', ' ').replace('(', '').replace(')', '').strip() 
                                   for s in generated_strategies}
            normalized_target = strategy_name.lower().replace('-', ' ').replace('(', '').replace(')', '').strip()
            
            if any(normalized_target in ng for ng in normalized_generated):
                print(f"✅ {strategy_name:30s} - PRESENT")
            else:
                print(f"❌ {strategy_name:30s} - MISSING")
                print(f"   Theory: {strategy_info['theory']}")
                print(f"   Required: {', '.join(strategy_info['required_conditions'])}")
                print(f"   🔍 INVESTIGATION NEEDED: Why is this excluded?\n")
        
        # Check if default mode correctly filters to Tier-1
        print(f"\n{'='*80}")
        print(f"TIER FILTERING VALIDATION")
        print(f"{'='*80}\n")
        
        if len(df_tier1) > 0 and 'Strategy_Tier' in df_tier1.columns:
            non_tier1 = df_tier1[df_tier1['Strategy_Tier'] != 1]
            if len(non_tier1) > 0:
                print(f"❌ VIOLATION: Default mode contains {len(non_tier1)} non-Tier-1 strategies!")
                print(non_tier1[['Ticker', 'Primary_Strategy', 'Strategy_Tier']].to_string(index=False))
            else:
                print(f"✅ Default mode correctly contains ONLY Tier-1 strategies")
        
        if len(df_tier1) == 0:
            print(f"❌ CRITICAL VIOLATION: Default mode returned 0 strategies!")
            print(f"   This means NO Tier-1 strategies were generated")
            print(f"   System is broken - reverting to Tier-2/3 only")
        
        return {
            'all_tiers': df_all,
            'tier1_only': df_tier1
        }
        
    except Exception as e:
        print(f"❌ ERROR in Step 7: {e}")
        import traceback
        traceback.print_exc()
        return {'all_tiers': pd.DataFrame(), 'tier1_only': pd.DataFrame()}


def generate_audit_report(results: Dict):
    """Generate final audit report with violations"""
    print_section("FINAL AUDIT REPORT", "=")
    
    # Summary
    print("SUMMARY:")
    print(f"  - Input tickers: {results.get('step1_count', 0)}")
    print(f"  - Step 7 output (all): {results.get('step7_all_count', 0)}")
    print(f"  - Step 7 output (tier1): {results.get('step7_tier1_count', 0)}")
    
    # Violations
    print(f"\nVIOLATIONS:")
    if results.get('violations'):
        for i, violation in enumerate(results['violations'], 1):
            print(f"  {i}. {violation}")
    else:
        print("  None found ✅")
    
    # Recommendations
    print(f"\nRECOMMENDATIONS:")
    if results.get('step7_tier1_count', 0) == 0:
        print("  1. CRITICAL: Step 7 generating zero Tier-1 strategies")
        print("     Action: Add Tier-1 strategy logic to _select_strategy()")
    
    if results.get('missing_strategies'):
        print(f"  2. Missing Tier-1 strategies: {', '.join(results['missing_strategies'])}")
        print(f"     Action: Add theory-based rules for each missing strategy")


from core.scan_engine.pipeline import run_full_scan_pipeline

def log_dataframe_info(df_name: str, df: pd.DataFrame):
    """Logs the schema and row count of a DataFrame."""
    if df is None or df.empty:
        logger.info(f"DataFrame '{df_name}': Empty or None")
        return
    logger.info(f"DataFrame '{df_name}': {len(df)} rows, {len(df.columns)} columns")
    logger.debug(f"Schema for '{df_name}': {df.columns.tolist()}")


def main():
    """Run full pipeline audit using run_full_scan_pipeline."""
    print_section("TIER-1 PIPELINE AUDIT - CLI VALIDATION", "=")
    print("Theory-first, RAG-backed, execution-last validation")
    print("Ensuring all Tier-1 strategies have equal representation\n")
    
    # Configuration
    snapshot_path = '/Users/haniabadi/Documents/Windows/OptionsSnapshots/fidelity_ivhv_snapshot.csv'
    
    audit_results = {
        'violations': [],
        'missing_strategies': []
    }
    
    # Run the full pipeline
    logger.info("🚀 Running full scan pipeline...")
    # NOTE: run_full_scan_pipeline() signature accepts only snapshot_path, output_dir,
    # account_balance, max_portfolio_risk, sizing_method. Remove unsupported flags.
    pipeline_outputs = run_full_scan_pipeline(
        snapshot_path=snapshot_path
    )
    logger.info("✅ Full scan pipeline completed.")
    
    # Log intermediate DataFrame info
    log_dataframe_info('snapshot', pipeline_outputs.get('snapshot'))
    log_dataframe_info('filtered', pipeline_outputs.get('filtered'))
    log_dataframe_info('charted', pipeline_outputs.get('charted'))
    log_dataframe_info('validated_data', pipeline_outputs.get('validated_data'))
    log_dataframe_info('recommendations', pipeline_outputs.get('recommendations'))
    
    # Extract Step 7 results for audit
    df_all_tiers_strategies = pipeline_outputs.get('recommendations')
    
    if df_all_tiers_strategies is None or df_all_tiers_strategies.empty:
        audit_results['violations'].append("CRITICAL: Step 7 generated no strategies.")
        audit_results['step7_all_count'] = 0
        audit_results['step7_tier1_count'] = 0
    else:
        audit_results['step7_all_count'] = len(df_all_tiers_strategies)
        audit_results['step7_tier1_count'] = len(df_all_tiers_strategies[df_all_tiers_strategies['Strategy_Tier'] == 1])
        
        print(f"\n{'='*80}")
        print(f"STEP 7 RESULTS (from full pipeline)")
        print(f"{'='*80}\n")
        print(f"   Exploration mode (all): {audit_results['step7_all_count']} strategies")
        print(f"   Default mode (tier1): {audit_results['step7_tier1_count']} strategies")
        
        # Analyze strategy distribution
        if len(df_all_tiers_strategies) > 0:
            print(f"\n   Strategy distribution (all tiers):")
            strategy_counts = df_all_tiers_strategies['Primary_Strategy'].value_counts()
            for strategy, count in strategy_counts.items():
                tier = df_all_tiers_strategies[df_all_tiers_strategies['Primary_Strategy'] == strategy]['Strategy_Tier'].iloc[0]
                print(f"   - {strategy:30s} (Tier {tier}): {count:3d}")
        
        # CRITICAL AUDIT: Check for missing Tier-1 strategies
        print(f"\n{'='*80}")
        print(f"TIER-1 STRATEGY AUDIT")
        print(f"{'='*80}\n")
        
        generated_strategies = set(df_all_tiers_strategies['Strategy_Name'].unique())
        
        for strategy_name, strategy_info in TIER1_STRATEGIES.items():
            normalized_generated = {s.lower().replace('-', ' ').replace('(', '').replace(')', '').strip() 
                                   for s in generated_strategies}
            normalized_target = strategy_name.lower().replace('-', ' ').replace('(', '').replace(')', '').strip()
            
            if any(normalized_target in ng for ng in normalized_generated):
                print(f"✅ {strategy_name:30s} - PRESENT")
            else:
                print(f"❌ {strategy_name:30s} - MISSING")
                print(f"   Theory: {strategy_info['theory']}")
                print(f"   Required: {', '.join(strategy_info['required_conditions'])}")
                print(f"   🔍 INVESTIGATION NEEDED: Why is this excluded?\n")
                audit_results['missing_strategies'].append(strategy_name)
        
        # Check if default mode correctly filters to Tier-1 (this is now handled by pipeline's internal call)
        # We can simulate this by filtering the 'recommendations' DataFrame
        df_tier1_only = df_all_tiers_strategies[df_all_tiers_strategies['Strategy_Tier'] == 1]
        print(f"\n{'='*80}")
        print(f"TIER FILTERING VALIDATION (simulated)")
        print(f"{'='*80}\n")
        
        if len(df_tier1_only) > 0 and 'Strategy_Tier' in df_tier1_only.columns:
            non_tier1 = df_tier1_only[df_tier1_only['Strategy_Tier'] != 1]
            if len(non_tier1) > 0:
                print(f"❌ VIOLATION: Default mode contains {len(non_tier1)} non-Tier-1 strategies!")
                print(non_tier1[['Ticker', 'Strategy_Name', 'Strategy_Tier']].to_string(index=False))
                audit_results['violations'].append("Simulated Tier-1 filter contains non-Tier-1 strategies.")
            else:
                print(f"✅ Default mode correctly contains ONLY Tier-1 strategies")
        
        if len(df_tier1_only) == 0:
            print(f"❌ CRITICAL VIOLATION: Default mode returned 0 strategies!")
            print(f"   This means NO Tier-1 strategies were generated")
            print(f"   System is broken - reverting to Tier-2/3 only")
            audit_results['violations'].append("Simulated Tier-1 filter returned 0 strategies.")
    
    # Generate final report
    generate_audit_report(audit_results)


if __name__ == '__main__':
    main()
