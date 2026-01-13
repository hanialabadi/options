#!/usr/bin/env python3
"""
Persona-Based System Audit CLI

Audits the complete trading system through the lens of each RAG persona:
- INCOME: Theta decay focus, covered calls, CSPs, ROI targets
- NEUTRAL_VOL: Volatility trading, straddles, iron condors, IV_Rank
- DIRECTIONAL: Directional plays, buy calls/puts, delta exposure

Usage:
    python audit_persona_compliance.py --persona INCOME
    python audit_persona_compliance.py --persona NEUTRAL_VOL
    python audit_persona_compliance.py --persona DIRECTIONAL
    python audit_persona_compliance.py --all
"""

import pandas as pd
import numpy as np
import argparse
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple
import logging

# Add project root
sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


# ============================================================================
# PERSONA DEFINITIONS (from RAG)
# ============================================================================

PERSONA_STRATEGIES = {
    'INCOME': {
        'primary': ['CSP', 'Covered_Call', 'Credit_Spread', 'Iron_Condor'],
        'focus': 'Theta decay, premium collection, ROI optimization',
        'key_metrics': ['Theta', 'Theta_Efficiency', 'ROI', 'Premium', 'Days_In_Trade', 'Assignment_Risk'],
        'pcs_weights': {'Gamma': 0.20, 'Vega': 0.20, 'ROI': 0.60},
        'targets': {'roi': 0.50, 'dte_range': (30, 60), 'theta_efficiency': 0.01},
        'exit_triggers': ['profit_target_50pct', 'assignment_risk', 'theta_exhaustion'],
    },
    'NEUTRAL_VOL': {
        'primary': ['Straddle', 'Strangle', 'Iron_Condor', 'Iron_Butterfly'],
        'focus': 'Volatility premium capture, IV_Rank exploitation, vega management',
        'key_metrics': ['IV_Rank', 'IV_Rank_Drift', 'Vega', 'Gamma', 'Theta', 'Moneyness'],
        'pcs_weights': {'Gamma': 0.40, 'Vega': 0.60, 'ROI': 0.00},
        'targets': {'iv_rank_entry': 70, 'iv_rank_exit': 30, 'vega_neutral': True},
        'exit_triggers': ['iv_collapse', 'vega_decay', 'profit_target_50pct'],
    },
    'DIRECTIONAL': {
        'primary': ['Buy_Call', 'Buy_Put', 'Debit_Spread', 'Vertical_Spread'],
        'focus': 'Directional conviction, delta exposure, gamma acceleration',
        'key_metrics': ['Delta', 'Gamma', 'Moneyness_Pct', 'Unrealized_PnL', 'DTE', 'Chart_Regime'],
        'pcs_weights': {'Gamma': 0.50, 'Vega': 0.30, 'ROI': 0.20},
        'targets': {'roi': 1.00, 'dte_range': (30, 90), 'delta_target': 0.50},
        'exit_triggers': ['profit_target_100pct', 'chart_breakdown', 'gamma_decay_75pct'],
    },
}


# ============================================================================
# AUDIT FUNCTIONS
# ============================================================================

def audit_data_completeness(df: pd.DataFrame, persona: str) -> Dict:
    """Audit if all required metrics for persona are available."""
    results = {
        'persona': persona,
        'total_positions': len(df),
        'missing_metrics': [],
        'incomplete_metrics': [],
        'coverage': {},
        'score': 0.0,
    }
    
    required_metrics = PERSONA_STRATEGIES[persona]['key_metrics']
    
    for metric in required_metrics:
        if metric not in df.columns:
            results['missing_metrics'].append(metric)
            results['coverage'][metric] = 0.0
        else:
            coverage = df[metric].notna().sum() / len(df) if len(df) > 0 else 0
            results['coverage'][metric] = coverage
            if coverage < 1.0:
                results['incomplete_metrics'].append((metric, coverage))
    
    # Overall score
    total_required = len(required_metrics)
    total_coverage = sum(results['coverage'].values())
    results['score'] = (total_coverage / total_required * 100) if total_required > 0 else 0
    
    return results


def audit_pcs_weights(df: pd.DataFrame, persona: str) -> Dict:
    """Audit if Entry_PCS weights match persona expectations."""
    results = {
        'persona': persona,
        'expected_weights': PERSONA_STRATEGIES[persona]['pcs_weights'],
        'actual_profile': None,
        'misaligned_positions': [],
        'score': 0.0,
    }
    
    if 'Entry_PCS_Profile' not in df.columns:
        results['score'] = 0.0
        results['issue'] = 'Entry_PCS_Profile column missing'
        return results
    
    # Map persona to profile
    persona_to_profile = {
        'INCOME': 'INCOME',
        'NEUTRAL_VOL': 'NEUTRAL_VOL',
        'DIRECTIONAL': 'DIRECTIONAL',
    }
    
    expected_profile = persona_to_profile[persona]
    profile_match = (df['Entry_PCS_Profile'] == expected_profile).sum()
    total = len(df)
    
    results['actual_profile'] = df['Entry_PCS_Profile'].value_counts().to_dict()
    results['score'] = (profile_match / total * 100) if total > 0 else 0
    results['aligned_positions'] = profile_match
    results['total_positions'] = total
    
    return results


def audit_strategy_alignment(df: pd.DataFrame, persona: str) -> Dict:
    """Audit if positions match persona's primary strategies."""
    results = {
        'persona': persona,
        'primary_strategies': PERSONA_STRATEGIES[persona]['primary'],
        'actual_strategies': {},
        'aligned_count': 0,
        'misaligned_count': 0,
        'score': 0.0,
    }
    
    if 'Strategy' not in df.columns:
        results['score'] = 0.0
        results['issue'] = 'Strategy column missing'
        return results
    
    primary_strategies = PERSONA_STRATEGIES[persona]['primary']
    results['actual_strategies'] = df['Strategy'].value_counts().to_dict()
    
    aligned = df['Strategy'].isin(primary_strategies).sum()
    total = len(df)
    
    results['aligned_count'] = aligned
    results['misaligned_count'] = total - aligned
    results['score'] = (aligned / total * 100) if total > 0 else 0
    
    # List misaligned strategies
    misaligned = df[~df['Strategy'].isin(primary_strategies)]['Strategy'].value_counts()
    results['misaligned_strategies'] = misaligned.to_dict()
    
    return results


def audit_exit_triggers(df: pd.DataFrame, persona: str) -> Dict:
    """Audit if exit recommendations align with persona's triggers."""
    results = {
        'persona': persona,
        'expected_triggers': PERSONA_STRATEGIES[persona]['exit_triggers'],
        'recommendations': {},
        'missing_triggers': [],
        'score': 0.0,
    }
    
    if 'Recommendation' not in df.columns:
        results['score'] = 0.0
        results['issue'] = 'Recommendation column missing (Phase 7 not run?)'
        return results
    
    if 'Exit_Rationale' not in df.columns:
        results['score'] = 50.0
        results['issue'] = 'Exit_Rationale column missing'
        return results
    
    results['recommendations'] = df['Recommendation'].value_counts().to_dict()
    
    # Check if expected triggers are present in rationale
    expected_triggers = PERSONA_STRATEGIES[persona]['exit_triggers']
    trigger_keywords = {
        'profit_target_50pct': ['profit', '50%', 'target'],
        'profit_target_100pct': ['profit', '100%', 'target'],
        'assignment_risk': ['assignment', 'ITM', 'pin risk'],
        'theta_exhaustion': ['theta', 'decay', 'exhaustion'],
        'iv_collapse': ['IV', 'collapse', 'IV_Rank'],
        'vega_decay': ['vega', 'decay'],
        'chart_breakdown': ['chart', 'breakdown', 'bearish'],
        'gamma_decay_75pct': ['gamma', 'decay', '75%'],
    }
    
    for trigger in expected_triggers:
        keywords = trigger_keywords.get(trigger, [])
        found = False
        for keyword in keywords:
            if df['Exit_Rationale'].str.contains(keyword, case=False, na=False).any():
                found = True
                break
        if not found:
            results['missing_triggers'].append(trigger)
    
    implemented = len(expected_triggers) - len(results['missing_triggers'])
    results['score'] = (implemented / len(expected_triggers) * 100) if expected_triggers else 100
    
    return results


def audit_target_metrics(df: pd.DataFrame, persona: str) -> Dict:
    """Audit if positions meet persona's target criteria."""
    results = {
        'persona': persona,
        'targets': PERSONA_STRATEGIES[persona]['targets'],
        'violations': [],
        'warnings': [],
        'score': 0.0,
    }
    
    targets = PERSONA_STRATEGIES[persona]['targets']
    
    # Income persona checks
    if persona == 'INCOME':
        if 'Theta_Efficiency' in df.columns:
            low_theta = df[df['Theta_Efficiency'] < targets['theta_efficiency']]
            if len(low_theta) > 0:
                results['violations'].append(f"{len(low_theta)} positions with Theta_Efficiency < {targets['theta_efficiency']}")
        
        if 'DTE' in df.columns:
            dte_min, dte_max = targets['dte_range']
            out_of_range = df[(df['DTE'] < dte_min) | (df['DTE'] > dte_max)]
            if len(out_of_range) > 0:
                results['warnings'].append(f"{len(out_of_range)} positions outside optimal DTE range ({dte_min}-{dte_max})")
    
    # NEUTRAL_VOL checks
    elif persona == 'NEUTRAL_VOL':
        if 'IV_Rank' in df.columns:
            low_iv = df[df['IV_Rank'] < targets['iv_rank_entry']]
            if len(low_iv) > 0:
                results['warnings'].append(f"{len(low_iv)} positions entered below IV_Rank {targets['iv_rank_entry']}")
        
        if 'IV_Rank_Drift' in df.columns:
            collapsed = df[df['IV_Rank_Drift'] < -30]
            if len(collapsed) > 0:
                results['violations'].append(f"{len(collapsed)} positions with IV collapse (drift < -30)")
    
    # DIRECTIONAL checks
    elif persona == 'DIRECTIONAL':
        if 'Delta' in df.columns:
            weak_delta = df[np.abs(df['Delta']) < 0.30]
            if len(weak_delta) > 0:
                results['warnings'].append(f"{len(weak_delta)} positions with weak Delta (<0.30)")
        
        if 'Gamma_Drift_Pct' in df.columns:
            decayed = df[df['Gamma_Drift_Pct'] < -75]
            if len(decayed) > 0:
                results['violations'].append(f"{len(decayed)} positions with severe Gamma decay (<-75%)")
    
    # Score based on violations
    total_checks = 2  # Each persona has ~2 key checks
    violations_count = len(results['violations'])
    results['score'] = max(0, (total_checks - violations_count) / total_checks * 100)
    
    return results


def audit_current_pcs_v2(df: pd.DataFrame, persona: str) -> Dict:
    """Audit Current_PCS v2 implementation for persona."""
    results = {
        'persona': persona,
        'pcs_v2_available': False,
        'component_scores': {},
        'tier_distribution': {},
        'issues': [],
        'score': 0.0,
    }
    
    if 'Current_PCS_v2' not in df.columns:
        results['issues'].append('Current_PCS_v2 not implemented')
        return results
    
    results['pcs_v2_available'] = True
    
    # Check component availability
    components = ['Current_PCS_IV_Score', 'Current_PCS_Liquidity_Score', 'Current_PCS_Greeks_Score']
    for comp in components:
        if comp in df.columns:
            coverage = df[comp].notna().sum() / len(df) if len(df) > 0 else 0
            results['component_scores'][comp] = coverage
        else:
            results['issues'].append(f'{comp} missing')
    
    # Tier distribution
    if 'Current_PCS_Tier_v2' in df.columns:
        results['tier_distribution'] = df['Current_PCS_Tier_v2'].value_counts().to_dict()
    
    # Persona-specific PCS checks
    if persona == 'INCOME':
        # Income needs good liquidity and Greeks
        if results['component_scores'].get('Current_PCS_Liquidity_Score', 0) < 0.8:
            results['issues'].append('Low liquidity coverage for INCOME persona')
    elif persona == 'NEUTRAL_VOL':
        # NEUTRAL_VOL needs IV_Rank
        if results['component_scores'].get('Current_PCS_IV_Score', 0) < 0.5:
            results['issues'].append('CRITICAL: IV_Rank component missing - essential for NEUTRAL_VOL')
    elif persona == 'DIRECTIONAL':
        # DIRECTIONAL needs Greeks (Delta/Gamma)
        if results['component_scores'].get('Current_PCS_Greeks_Score', 0) < 0.8:
            results['issues'].append('Low Greeks coverage for DIRECTIONAL persona')
    
    # Overall score
    component_coverage = sum(results['component_scores'].values()) / len(components) if components else 0
    results['score'] = component_coverage * 100
    
    return results


# ============================================================================
# REPORT GENERATION
# ============================================================================

def generate_persona_report(df: pd.DataFrame, persona: str) -> Dict:
    """Generate comprehensive audit report for a persona."""
    logger.info(f"\n{'='*80}")
    logger.info(f"🔍 PERSONA AUDIT: {persona}")
    logger.info(f"{'='*80}")
    logger.info(f"\n📋 Focus: {PERSONA_STRATEGIES[persona]['focus']}")
    logger.info(f"🎯 Primary Strategies: {', '.join(PERSONA_STRATEGIES[persona]['primary'])}")
    
    report = {
        'persona': persona,
        'timestamp': datetime.now().isoformat(),
        'total_positions': len(df),
        'audits': {},
        'overall_score': 0.0,
    }
    
    # Run all audits
    audits = [
        ('Data Completeness', audit_data_completeness),
        ('PCS Weights', audit_pcs_weights),
        ('Strategy Alignment', audit_strategy_alignment),
        ('Exit Triggers', audit_exit_triggers),
        ('Target Metrics', audit_target_metrics),
        ('Current_PCS v2', audit_current_pcs_v2),
    ]
    
    scores = []
    for audit_name, audit_func in audits:
        logger.info(f"\n{'─'*80}")
        logger.info(f"📊 {audit_name}")
        logger.info(f"{'─'*80}")
        
        result = audit_func(df, persona)
        report['audits'][audit_name] = result
        scores.append(result['score'])
        
        # Print results
        _print_audit_results(audit_name, result)
    
    # Overall score
    report['overall_score'] = np.mean(scores)
    
    logger.info(f"\n{'='*80}")
    logger.info(f"📈 OVERALL SCORE: {report['overall_score']:.1f}/100")
    logger.info(f"{'='*80}")
    
    return report


def _print_audit_results(audit_name: str, result: Dict):
    """Print audit results in readable format."""
    score = result.get('score', 0)
    
    # Color coding
    if score >= 80:
        status = '✅'
    elif score >= 60:
        status = '⚠️'
    else:
        status = '❌'
    
    logger.info(f"{status} Score: {score:.1f}/100")
    
    # Specific audit details
    if audit_name == 'Data Completeness':
        if result['missing_metrics']:
            logger.info(f"   Missing: {', '.join(result['missing_metrics'])}")
        if result['incomplete_metrics']:
            logger.info(f"   Incomplete:")
            for metric, coverage in result['incomplete_metrics']:
                logger.info(f"      • {metric}: {coverage*100:.1f}%")
    
    elif audit_name == 'PCS Weights':
        if 'issue' not in result:
            logger.info(f"   Aligned: {result.get('aligned_positions', 0)}/{result.get('total_positions', 0)}")
            if result.get('actual_profile'):
                logger.info(f"   Profiles: {result['actual_profile']}")
    
    elif audit_name == 'Strategy Alignment':
        if 'issue' not in result:
            logger.info(f"   Aligned: {result['aligned_count']}/{result['aligned_count'] + result['misaligned_count']}")
            if result.get('misaligned_strategies'):
                logger.info(f"   Misaligned strategies:")
                for strat, count in result['misaligned_strategies'].items():
                    logger.info(f"      • {strat}: {count}")
    
    elif audit_name == 'Exit Triggers':
        if result.get('missing_triggers'):
            logger.info(f"   Missing triggers: {', '.join(result['missing_triggers'])}")
        if 'issue' in result:
            logger.info(f"   ⚠️  {result['issue']}")
    
    elif audit_name == 'Target Metrics':
        if result.get('violations'):
            logger.info(f"   ❌ Violations:")
            for v in result['violations']:
                logger.info(f"      • {v}")
        if result.get('warnings'):
            logger.info(f"   ⚠️  Warnings:")
            for w in result['warnings']:
                logger.info(f"      • {w}")
    
    elif audit_name == 'Current_PCS v2':
        if result.get('issues'):
            logger.info(f"   Issues:")
            for issue in result['issues']:
                logger.info(f"      • {issue}")
        if result.get('component_scores'):
            logger.info(f"   Component Coverage:")
            for comp, coverage in result['component_scores'].items():
                comp_name = comp.replace('Current_PCS_', '').replace('_Score', '')
                logger.info(f"      • {comp_name}: {coverage*100:.1f}%")


def generate_recommendations(reports: Dict[str, Dict]) -> List[str]:
    """Generate actionable recommendations based on audit results."""
    recommendations = []
    
    # Aggregate issues across personas
    all_issues = {}
    for persona, report in reports.items():
        for audit_name, audit_result in report['audits'].items():
            if audit_result['score'] < 80:
                issue_key = f"{audit_name}"
                if issue_key not in all_issues:
                    all_issues[issue_key] = []
                all_issues[issue_key].append((persona, audit_result))
    
    # Generate recommendations
    if any('Data Completeness' in k for k in all_issues):
        recommendations.append("🔴 CRITICAL: Populate missing metrics, especially IV_Rank historical data for NEUTRAL_VOL persona")
    
    if any('Current_PCS v2' in k for k in all_issues):
        recommendations.append("🟡 IMPORTANT: Complete Current_PCS v2 implementation - IV_Rank component at 0% coverage")
    
    if any('Exit Triggers' in k for k in all_issues):
        recommendations.append("🟡 ENHANCE: Implement persona-specific exit triggers in Phase 7 recommendations")
    
    if any('Strategy Alignment' in k for k in all_issues):
        recommendations.append("🟢 OPTIMIZE: Review Unknown strategies and improve Phase 2 strategy detection")
    
    return recommendations


# ============================================================================
# MAIN CLI
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Audit trading system through persona-based lens',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Audit for INCOME persona
    python audit_persona_compliance.py --persona INCOME
    
    # Audit all personas
    python audit_persona_compliance.py --all
    
    # Audit specific file
    python audit_persona_compliance.py --persona NEUTRAL_VOL --input output/positions_with_recommendations_latest.csv
        """
    )
    
    parser.add_argument('--persona', choices=['INCOME', 'NEUTRAL_VOL', 'DIRECTIONAL'],
                        help='Persona to audit')
    parser.add_argument('--all', action='store_true',
                        help='Audit all personas')
    parser.add_argument('--input', type=str,
                        help='Input CSV file (default: latest from output/)')
    parser.add_argument('--options-only', action='store_true',
                        help='Analyze options only (exclude stocks)')
    
    args = parser.parse_args()
    
    if not args.persona and not args.all:
        parser.error('Must specify --persona or --all')
    
    # Load data
    if args.input:
        input_file = args.input
    else:
        import glob
        files = sorted(glob.glob('output/positions_with_recommendations_*.csv'))
        if not files:
            logger.error("❌ No recommendation files found. Run phase 1-7 pipeline first.")
            sys.exit(1)
        input_file = files[-1]
    
    logger.info(f"\n📁 Loading: {input_file}")
    df = pd.read_csv(input_file)
    
    if args.options_only:
        df = df[df['AssetType'] == 'OPTION']
        logger.info(f"   Filtered to {len(df)} options positions")
    else:
        logger.info(f"   Loaded {len(df)} positions")
    
    # Run audits
    if args.all:
        personas = ['INCOME', 'NEUTRAL_VOL', 'DIRECTIONAL']
    else:
        personas = [args.persona]
    
    reports = {}
    for persona in personas:
        reports[persona] = generate_persona_report(df, persona)
    
    # Summary
    logger.info(f"\n\n{'='*80}")
    logger.info(f"📊 SUMMARY: ALL PERSONAS")
    logger.info(f"{'='*80}\n")
    
    for persona, report in reports.items():
        logger.info(f"   {persona:15s}: {report['overall_score']:.1f}/100")
    
    # Recommendations
    logger.info(f"\n{'='*80}")
    logger.info(f"💡 RECOMMENDATIONS")
    logger.info(f"{'='*80}\n")
    
    recommendations = generate_recommendations(reports)
    for i, rec in enumerate(recommendations, 1):
        logger.info(f"{i}. {rec}")
    
    logger.info(f"\n{'='*80}")
    logger.info(f"✅ Audit complete!")
    logger.info(f"{'='*80}\n")


if __name__ == '__main__':
    main()
