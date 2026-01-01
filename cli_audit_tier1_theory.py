#!/usr/bin/env python3
"""
TIER-1 STRATEGY THEORY AUDIT (CLI-ONLY)

Purpose: Validate that all theory-correct Tier-1 strategies are generated
         in Step 7 with equal opportunity, bounded only by canonical options theory.

Theory Reference: Natenberg, Passarelli, Cohen, Hull
- Long Call/Put: Directional bias + cheap IV (HV > IV)
- CSP/Covered Call: Directional bias + rich IV (IV > HV)  
- Buy-Write: Stock purchase + immediate covered call
- Long Straddle/Strangle: Bidirectional uncertainty + cheap IV

Architectural Rules:
- Step 7 = Strategy Discovery (prescriptive, not executable)
- Tier-1 = Atomic primitives (single or two-leg max)
- Strike selection, POP, capital sizing = Step 9B (execution layer)
- No ticker filtering in Step 7 (all tickers in → all tickers out with strategies)

Audit Scope:
1. Pipeline integrity (Step 1→2→3→5→6→7)
2. Strategy coverage (all Tier-1 strategies present when conditions met)
3. Multi-strategy validation (single ticker → multiple valid strategies)
4. Theory compliance (no arbitrary suppression)
5. Safety enforcement (default = Tier-1 only, exploration = all tiers)
"""

import sys
import pandas as pd
from pathlib import Path
from typing import Dict, List, Tuple
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s'
)
logger = logging.getLogger(__name__)

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

# TIER-1 STRATEGY DEFINITIONS (THEORY-BASED)
TIER1_STRATEGIES = {
    'Long Call': {
        'conditions': 'Bullish signal + Cheap IV (HV > IV or IV_Rank < 50)',
        'theory': 'Natenberg Ch.3 - Directional with vega positive bias',
        'when_valid': lambda row: (
            row.get('Signal_Type', '') in ['Bullish', 'Sustained Bullish'] and
            (row.get('IVHV_gap_30D', 0) < 0 or row.get('IV_Rank_30D', 50) < 50)
        )
    },
    'Long Put': {
        'conditions': 'Bearish signal + Cheap IV (HV > IV or IV_Rank < 50)',
        'theory': 'Natenberg Ch.3 - Directional with vega positive bias',
        'when_valid': lambda row: (
            row.get('Signal_Type', '') in ['Bearish'] and
            (row.get('IVHV_gap_30D', 0) < 0 or row.get('IV_Rank_30D', 50) < 50)
        )
    },
    'Cash-Secured Put': {
        'conditions': 'Bullish signal + Rich IV (IV > HV or IV_Rank > 50)',
        'theory': 'Passarelli - Income via premium collection on bullish bias',
        'when_valid': lambda row: (
            row.get('Signal_Type', '') in ['Bullish', 'Sustained Bullish'] and
            (row.get('IVHV_gap_30D', 0) > 0 or row.get('IV_Rank_30D', 50) > 50)
        )
    },
    'Covered Call': {
        'conditions': 'Bearish/Neutral + Rich IV (IV > HV or IV_Rank > 50) + stock owned',
        'theory': 'Cohen - Income on existing position with bearish hedge',
        'when_valid': lambda row: (
            row.get('Signal_Type', '') in ['Bearish', 'Neutral', 'Base'] and
            (row.get('IVHV_gap_30D', 0) > 0 or row.get('IV_Rank_30D', 50) > 50)
        )
    },
    'Buy-Write': {
        'conditions': 'Bullish + Rich IV + simultaneous stock purchase',
        'theory': 'Passarelli - Entry strategy combining acquisition + income',
        'when_valid': lambda row: (
            row.get('Signal_Type', '') in ['Bullish', 'Sustained Bullish'] and
            (row.get('IVHV_gap_30D', 0) > 0 or row.get('IV_Rank_30D', 50) > 50)
        )
    },
    'Long Straddle': {
        'conditions': 'Bidirectional uncertainty + Cheap IV + expansion expected',
        'theory': 'Natenberg Ch.9 - Volatility buying on expected increase',
        'when_valid': lambda row: (
            (row.get('Signal_Type', '') in ['Bidirectional', 'Base', 'Neutral'] or
             row.get('Expansion_Setup', False)) and
            (row.get('IVHV_gap_180D', 0) < 0 or row.get('IV_Rank_30D', 50) < 40)
        )
    },
    'Long Strangle': {
        'conditions': 'Bidirectional uncertainty + Cheap IV (wider strikes than straddle)',
        'theory': 'Natenberg Ch.9 - Lower cost volatility buying',
        'when_valid': lambda row: (
            (row.get('Signal_Type', '') in ['Bidirectional', 'Base', 'Neutral'] or
             row.get('Expansion_Setup', False)) and
            (row.get('IVHV_gap_180D', 0) < 0 or row.get('IV_Rank_30D', 50) < 40)
        )
    }
}


def print_header(title: str, level: int = 1):
    """Print formatted section header"""
    if level == 1:
        char = "="
        width = 100
    elif level == 2:
        char = "-"
        width = 80
    else:
        char = "·"
        width = 60
    
    print(f"\n{char * width}")
    print(f"{title}")
    print(f"{char * width}\n")


def validate_dataframe(df: pd.DataFrame, step_name: str, expected_cols: List[str]) -> bool:
    """Validate DataFrame has required columns"""
    if df is None or len(df) == 0:
        logger.error(f"❌ {step_name}: DataFrame is empty or None")
        return False
    
    missing = [col for col in expected_cols if col not in df.columns]
    if missing:
        logger.warning(f"⚠️  {step_name}: Missing columns: {missing}")
        return False
    
    return True


def audit_step1_load(snapshot_path: str) -> pd.DataFrame:
    """Step 1: Load raw snapshot data"""
    print_header("STEP 1: RAW DATA LOAD", 2)
    
    try:
        df = pd.read_csv(snapshot_path)
        logger.info(f"✅ Loaded {len(df)} tickers from snapshot")
        logger.info(f"   Columns: {len(df.columns)}")
        logger.info(f"   Sample: {df['Ticker'].head(5).tolist()}")
        
        # Validate essential columns (raw snapshot structure)
        essential = ['Ticker']
        missing = [c for c in essential if c not in df.columns]
        if missing:
            logger.error(f"❌ Missing essential columns: {missing}")
            return None
        
        logger.info(f"   ℹ️  Raw snapshot will be enriched in Step 2")
        return df
    except Exception as e:
        logger.error(f"❌ Failed to load snapshot: {e}")
        return None


def audit_step2_enrich(snapshot_path: str) -> pd.DataFrame:
    """Step 2: IV/HV enrichment"""
    print_header("STEP 2: IV/HV ENRICHMENT", 2)
    
    try:
        from core.scan_engine.step2_load_snapshot import load_ivhv_snapshot
        
        df = load_ivhv_snapshot(snapshot_path)
        logger.info(f"✅ Enriched {len(df)} tickers")
        
        # Validate enrichment
        enriched_cols = ['IV_Rank_30D', 'IV_Term_Structure', 'IV_Trend_7D', 'HV_Trend_30D']
        present = [c for c in enriched_cols if c in df.columns]
        logger.info(f"   Enriched fields: {present}")
        
        return df
    except Exception as e:
        logger.error(f"❌ Step 2 failed: {e}")
        import traceback
        traceback.print_exc()
        return None


def audit_step3_regime(df: pd.DataFrame) -> pd.DataFrame:
    """Step 3: Volatility regime detection"""
    print_header("STEP 3: VOLATILITY REGIME DETECTION", 2)
    
    try:
        from core.scan_engine.step3_ivhv_filter import filter_ivhv_edge
        
        df_regime = filter_ivhv_edge(df)
        logger.info(f"✅ Regime detection: {len(df)} → {len(df_regime)} tickers")
        
        # Validate gaps preserved
        if 'IVHV_gap_30D' in df_regime.columns:
            pos = (df_regime['IVHV_gap_30D'] > 0).sum()
            neg = (df_regime['IVHV_gap_30D'] < 0).sum()
            logger.info(f"   Gap distribution: +{pos} / -{neg} (signs preserved)")
        
        # Validate edge flags
        edge_flags = ['ShortTerm_IV_Edge', 'MediumTerm_IV_Edge', 'LEAP_IV_Edge']
        for flag in edge_flags:
            if flag in df_regime.columns:
                count = df_regime[flag].sum()
                logger.info(f"   {flag}: {count} tickers")
        
        # Check for silent filtering
        if len(df_regime) < len(df):
            logger.warning(f"   ⚠️  Dropped {len(df) - len(df_regime)} tickers")
        
        return df_regime
    except Exception as e:
        logger.error(f"❌ Step 3 failed: {e}")
        import traceback
        traceback.print_exc()
        return df


def audit_step5_signals(df: pd.DataFrame) -> pd.DataFrame:
    """Step 5: Technical signal detection"""
    print_header("STEP 5: TECHNICAL SIGNALS", 2)
    
    try:
        from core.scan_engine.step5_pcs_score import assign_pcs_scores
        
        df_signals = assign_pcs_scores(df)
        logger.info(f"✅ Signal assignment: {len(df)} → {len(df_signals)} tickers")
        
        # Validate signal distribution
        if 'Signal_Type' in df_signals.columns:
            signals = df_signals['Signal_Type'].value_counts()
            logger.info(f"   Signal distribution:")
            for sig, count in signals.items():
                logger.info(f"      {sig:20s}: {count:3d}")
        
        # Theory check: Weak signals should NOT block neutral/volatility strategies
        if 'Signal_Type' in df_signals.columns:
            weak_count = (df_signals['Signal_Type'] == 'Weak').sum()
            if weak_count > 0:
                logger.info(f"\n   📌 {weak_count} 'Weak' signal tickers")
                logger.info(f"      Theory: Should still allow neutral/volatility strategies")
        
        return df_signals
    except Exception as e:
        logger.error(f"❌ Step 5 failed: {e}")
        import traceback
        traceback.print_exc()
        return df


def audit_step6_gem(df: pd.DataFrame) -> pd.DataFrame:
    """Step 6: Data completeness scoring"""
    print_header("STEP 6: DATA COMPLETENESS (GEM)", 2)
    
    try:
        from core.scan_engine.step6_gem import generate_gem_score
        
        df_gem = generate_gem_score(df)
        logger.info(f"✅ GEM scoring: {len(df)} → {len(df_gem)} tickers")
        
        # Validate completeness flags
        if 'Data_Complete' in df_gem.columns:
            complete = df_gem['Data_Complete'].sum()
            incomplete = len(df_gem) - complete
            logger.info(f"   Complete data: {complete} / {len(df_gem)}")
            
            if incomplete > 0:
                logger.info(f"\n   📌 {incomplete} tickers with incomplete data")
                logger.info(f"      Theory: Simple Tier-1 strategies should still be valid")
                logger.info(f"      (Long Call/Put/Straddle need only: price + IV)")
        
        return df_gem
    except Exception as e:
        logger.error(f"❌ Step 6 failed: {e}")
        import traceback
        traceback.print_exc()
        return df


def audit_step7_strategies(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Step 7: Strategy discovery (CORE AUDIT)"""
    print_header("STEP 7: STRATEGY DISCOVERY (TIER-1 AUDIT)", 1)
    
    try:
        from core.scan_engine.step7_strategy_recommendation import recommend_strategies
        
        # Test 1: Exploration mode (all tiers)
        logger.info("🔍 Test 1: EXPLORATION MODE (all tiers)\n")
        df_all = recommend_strategies(
            df.copy(),
            tier_filter='all_tiers',
            exploration_mode=True
        )
        
        # Test 2: Default mode (tier1_only)
        logger.info("\n🔍 Test 2: DEFAULT MODE (tier1_only)\n")
        df_tier1 = recommend_strategies(
            df.copy(),
            tier_filter='tier1_only',
            exploration_mode=False
        )
        
        return {
            'all_tiers': df_all,
            'tier1_only': df_tier1,
            'input': df
        }
    except Exception as e:
        logger.error(f"❌ Step 7 failed: {e}")
        import traceback
        traceback.print_exc()
        return {
            'all_tiers': pd.DataFrame(),
            'tier1_only': pd.DataFrame(),
            'input': df
        }


def analyze_strategy_coverage(results: Dict[str, pd.DataFrame]) -> Dict:
    """Analyze Tier-1 strategy coverage"""
    print_header("TIER-1 STRATEGY COVERAGE ANALYSIS", 1)
    
    df_all = results['all_tiers']
    df_tier1 = results['tier1_only']
    df_input = results['input']
    
    analysis = {
        'total_input_tickers': len(df_input),
        'total_strategies_all': len(df_all),
        'total_strategies_tier1': len(df_tier1),
        'strategy_counts': {},
        'missing_strategies': [],
        'violations': []
    }
    
    # 1. Overall counts
    logger.info("📊 OVERALL RESULTS:")
    logger.info(f"   Input tickers: {len(df_input)}")
    logger.info(f"   Strategies generated (all tiers): {len(df_all)}")
    logger.info(f"   Strategies generated (tier1 only): {len(df_tier1)}")
    logger.info(f"   Average strategies per ticker: {len(df_all) / max(len(df_input), 1):.2f}")
    
    # 2. Strategy distribution
    if len(df_all) > 0:
        logger.info("\n📋 STRATEGY DISTRIBUTION (All Tiers):")
        strategy_dist = df_all['Primary_Strategy'].value_counts()
        for strategy, count in strategy_dist.items():
            # Get tier for this strategy
            tier = df_all[df_all['Primary_Strategy'] == strategy]['Strategy_Tier'].iloc[0] if 'Strategy_Tier' in df_all.columns else '?'
            logger.info(f"   {strategy:30s} [Tier {tier}]: {count:3d}")
            analysis['strategy_counts'][strategy] = {
                'count': count,
                'tier': tier
            }
    
    # 3. Tier-1 validation
    logger.info("\n🔒 TIER-1 VALIDATION:")
    if len(df_tier1) > 0 and 'Strategy_Tier' in df_tier1.columns:
        non_tier1 = df_tier1[df_tier1['Strategy_Tier'] != 1]
        if len(non_tier1) > 0:
            logger.error(f"❌ VIOLATION: {len(non_tier1)} non-Tier-1 strategies in default mode!")
            analysis['violations'].append(f"Non-Tier-1 in default mode: {len(non_tier1)}")
        else:
            logger.info(f"✅ All {len(df_tier1)} strategies are correctly Tier-1")
    
    if len(df_tier1) == 0:
        logger.error("❌ CRITICAL: Default mode returned ZERO strategies!")
        analysis['violations'].append("Zero Tier-1 strategies generated")
    
    # 4. Check for missing Tier-1 strategies
    logger.info("\n🔍 TIER-1 STRATEGY PRESENCE CHECK:")
    generated_strategies = set(df_all['Primary_Strategy'].unique()) if len(df_all) > 0 else set()
    
    for strategy_name, strategy_info in TIER1_STRATEGIES.items():
        # Normalize names for comparison
        normalized_generated = {
            s.lower().replace('-', ' ').replace('(', '').replace(')', '').strip()
            for s in generated_strategies
        }
        normalized_target = strategy_name.lower().replace('-', ' ').replace('(', '').replace(')', '').strip()
        
        # Check if strategy exists
        found = any(normalized_target in ng or ng in normalized_target for ng in normalized_generated)
        
        if found:
            logger.info(f"✅ {strategy_name:30s} - PRESENT")
        else:
            logger.warning(f"❌ {strategy_name:30s} - MISSING")
            logger.info(f"   Conditions: {strategy_info['conditions']}")
            logger.info(f"   Theory: {strategy_info['theory']}")
            
            # Count how many tickers meet conditions
            eligible_count = df_input.apply(strategy_info['when_valid'], axis=1).sum()
            logger.info(f"   Eligible tickers: {eligible_count} / {len(df_input)}")
            
            if eligible_count > 0:
                logger.warning(f"   ⚠️  THEORY VIOLATION: {eligible_count} tickers meet conditions but strategy not generated!")
                analysis['missing_strategies'].append({
                    'name': strategy_name,
                    'eligible_count': eligible_count
                })
            else:
                logger.info(f"   ℹ️  No eligible tickers (not a violation)")
    
    return analysis


def analyze_multi_strategy_diversity(results: Dict[str, pd.DataFrame]):
    """Analyze whether tickers get multiple strategy recommendations"""
    print_header("MULTI-STRATEGY DIVERSITY ANALYSIS", 1)
    
    df_all = results['all_tiers']
    df_input = results['input']
    
    if len(df_all) == 0:
        logger.warning("⚠️  No strategies to analyze")
        return
    
    logger.info("📌 Theory: A single ticker can support multiple strategies simultaneously:")
    logger.info("   - Directional (Long Call/Put based on signal)")
    logger.info("   - Income (CSP/Covered Call based on IV regime)")
    logger.info("   - Volatility (Straddle/Strangle based on uncertainty)")
    logger.info("\n   No mutual exclusion unless theory requires it.\n")
    
    # Count strategies per ticker
    if 'Ticker' in df_all.columns:
        strategies_per_ticker = df_all.groupby('Ticker').size()
        
        logger.info(f"📊 STRATEGIES PER TICKER:")
        logger.info(f"   Single strategy only: {(strategies_per_ticker == 1).sum()}")
        logger.info(f"   Multiple strategies: {(strategies_per_ticker > 1).sum()}")
        logger.info(f"   Max strategies for one ticker: {strategies_per_ticker.max()}")
        logger.info(f"   Average: {strategies_per_ticker.mean():.2f}")
        
        # Show examples of multi-strategy tickers
        multi_strategy_tickers = strategies_per_ticker[strategies_per_ticker > 1].head(5)
        if len(multi_strategy_tickers) > 0:
            logger.info(f"\n   Example tickers with multiple strategies:")
            for ticker, count in multi_strategy_tickers.items():
                strategies = df_all[df_all['Ticker'] == ticker]['Primary_Strategy'].tolist()
                logger.info(f"      {ticker}: {count} strategies - {strategies}")
    
    # Diversity score
    unique_strategies = df_all['Primary_Strategy'].nunique()
    total_strategies = len(df_all)
    diversity_score = unique_strategies / max(total_strategies, 1) * 100
    
    logger.info(f"\n📈 DIVERSITY SCORE: {diversity_score:.1f}%")
    logger.info(f"   ({unique_strategies} unique strategies / {total_strategies} total)")


def analyze_signal_iv_coverage(results: Dict[str, pd.DataFrame]):
    """Analyze strategy coverage across signal types and IV regimes"""
    print_header("SIGNAL × IV REGIME COVERAGE", 1)
    
    df_all = results['all_tiers']
    df_input = results['input']
    
    if len(df_all) == 0 or 'Signal_Type' not in df_all.columns:
        logger.warning("⚠️  Cannot analyze signal/IV coverage")
        return
    
    logger.info("📊 STRATEGY GENERATION BY SIGNAL TYPE:")
    
    # Get signal distribution in input
    input_signals = df_input['Signal_Type'].value_counts()
    
    for signal in input_signals.index:
        input_count = input_signals[signal]
        output_count = len(df_all[df_all['Signal_Type'] == signal])
        
        logger.info(f"\n   {signal}:")
        logger.info(f"      Input tickers: {input_count}")
        logger.info(f"      Strategies generated: {output_count}")
        logger.info(f"      Average per ticker: {output_count / max(input_count, 1):.2f}")
        
        # Show strategy distribution for this signal
        if output_count > 0:
            signal_strategies = df_all[df_all['Signal_Type'] == signal]['Primary_Strategy'].value_counts()
            for strategy, count in signal_strategies.head(3).items():
                logger.info(f"         - {strategy}: {count}")


def generate_final_report(analysis: Dict):
    """Generate final audit report with recommendations"""
    print_header("FINAL AUDIT REPORT & RECOMMENDATIONS", 1)
    
    logger.info("📋 SUMMARY:")
    logger.info(f"   Input tickers: {analysis['total_input_tickers']}")
    logger.info(f"   Total strategies (all): {analysis['total_strategies_all']}")
    logger.info(f"   Total strategies (tier1): {analysis['total_strategies_tier1']}")
    
    # Violations
    logger.info(f"\n🚨 VIOLATIONS:")
    if analysis['violations']:
        for i, violation in enumerate(analysis['violations'], 1):
            logger.error(f"   {i}. {violation}")
    else:
        logger.info("   ✅ None found")
    
    # Missing strategies
    logger.info(f"\n⚠️  MISSING TIER-1 STRATEGIES:")
    if analysis['missing_strategies']:
        for missing in analysis['missing_strategies']:
            logger.warning(f"   - {missing['name']}: {missing['eligible_count']} eligible tickers")
    else:
        logger.info("   ✅ All expected Tier-1 strategies present")
    
    # Recommendations
    logger.info(f"\n💡 RECOMMENDATIONS:")
    
    if analysis['total_strategies_tier1'] == 0:
        logger.warning("   1. CRITICAL: Step 7 generating ZERO Tier-1 strategies")
        logger.warning("      → Verify Tier-1 logic in _select_strategy()")
        logger.warning("      → Check if all strategies defaulting to Tier-2/3")
    
    if analysis['missing_strategies']:
        logger.warning(f"   2. MISSING STRATEGIES: {len(analysis['missing_strategies'])} Tier-1 strategies not generated")
        logger.warning("      → Add theory-based rules for:")
        for missing in analysis['missing_strategies']:
            logger.warning(f"         • {missing['name']}")
    
    if analysis['total_strategies_all'] == analysis['total_input_tickers']:
        logger.info("   3. ⚠️  Single-strategy-per-ticker pattern detected")
        logger.info("      → Theory allows multiple strategies per ticker")
        logger.info("      → Consider enabling parallel strategy generation")
    
    if not analysis['violations'] and not analysis['missing_strategies']:
        logger.info("   ✅ System is theory-compliant and generating all Tier-1 strategies")


def main():
    """Run complete Tier-1 strategy audit"""
    print_header("TIER-1 STRATEGY THEORY AUDIT", 1)
    logger.info("CLI-based pipeline validation with theory-first approach")
    logger.info("No UI dependency • RAG-backed validation • Execution-last design\n")
    
    # Configuration
    snapshot_path = '/Users/haniabadi/Documents/Windows/OptionsSnapshots/fidelity_ivhv_snapshot.csv'
    
    # Run full pipeline to Step 6
    logger.info("📦 Running Steps 1-6 via scan_pipeline...")
    try:
        from core.scan_engine.pipeline import run_full_scan_pipeline
        
        # Run full pipeline (includes all steps)
        pipeline_results = run_full_scan_pipeline(snapshot_path=snapshot_path)
        
        if not pipeline_results or 'step6_gem' not in pipeline_results:
            logger.error("❌ Pipeline failed - no Step 6 output")
            logger.info(f"   Available keys: {list(pipeline_results.keys()) if pipeline_results else 'None'}")
            return 1
        
        df = pipeline_results['step6_gem']
        logger.info(f"✅ Pipeline Steps 1-6 complete: {len(df)} tickers ready for Step 7\n")
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    # Step 7: Strategy Discovery (CRITICAL)
    results = audit_step7_strategies(df)
    
    # Analysis
    analysis = analyze_strategy_coverage(results)
    analyze_multi_strategy_diversity(results)
    analyze_signal_iv_coverage(results)
    generate_final_report(analysis)
    
    # Exit code
    if analysis['violations'] or analysis['missing_strategies']:
        logger.warning("\n⚠️  AUDIT FAILED: Violations or missing strategies detected")
        return 1
    else:
        logger.info("\n✅ AUDIT PASSED: All Tier-1 strategies present and theory-compliant")
        return 0


if __name__ == '__main__':
    sys.exit(main())
