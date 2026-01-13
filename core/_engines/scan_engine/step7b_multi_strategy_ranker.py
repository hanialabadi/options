"""
Step 7B: Multi-Strategy Ranker with Personalization

⚠️  CANONICAL RULES: See STEP7_CANONICAL_RULES.md for authoritative system contract.
    User_Fit_Score = contextual alignment (NOT PCS, NOT GEM indicator).
    Capital = indicative allocation (real sizing in Step 9B).

Purpose:
    Rank and annotate strategies emitted by Step 7 with personalization factors.
    This layer ONLY operates on existing strategies; it does NOT generate new ones.
    
    Timeframe Categories (DETERMINISTIC, NOT RANDOM):
    - Short (30-45 DTE): Premium selling, income, high-probability plays
    - Medium (60-120 DTE): Directional spreads, calendars, momentum plays
    - Long-LEAP (180-365 DTE): Stock replacement, PMCC base, structural trends
    - Ultra-LEAP (450-900 DTE): Multi-year directional, deep value plays
    
    CRITICAL: LEAPs are NOT "long versions" of short trades:
    - Different evaluation: Delta 0.60-0.80 (not ATM), structural trends (not crossover freshness)
    - Different use cases: Capital efficiency, PMCC base legs, long-term thesis
    - Different pricing: Low IV preferred (not high IV like short-term)
    
    Personalization: "For THIS ticker, THIS strategy is best for YOU, NOW"
    - Account size constraints
    - Risk tolerance alignment
    - Goal-specific recommendations (income vs growth vs hedging)

Example Output for AAPL:
    For YOU (Conservative, $10K account, Income goal):
    → #1: Put Credit Spread (30-45 DTE) - Why: Matches your income goal, fits account size
    
    For YOU (Aggressive, $50K account, Growth goal):
    → #1: LEAP Call (270 DTE) - Why: Structural uptrend, capital-efficient growth play
"""

import pandas as pd
import numpy as np
import logging
from typing import List, Dict
from core.strategy_tiers import get_strategy_tier, is_execution_ready

logger = logging.getLogger(__name__)

# DETERMINISTIC DTE WINDOWS (not random expirations)
DTE_WINDOWS = {
    'Short': {'min': 30, 'max': 45, 'preferred': 37},
    'Medium': {'min': 60, 'max': 120, 'preferred': 90},
    'Long-LEAP': {'min': 180, 'max': 365, 'preferred': 270},
    'Ultra-LEAP': {'min': 450, 'max': 900, 'preferred': 540}
}


def generate_multi_strategy_suggestions(
    df: pd.DataFrame,
    max_strategies_per_ticker: int = 6,
    account_size: float = 10000.0,
    risk_tolerance: str = 'Moderate',  # 'Conservative', 'Moderate', 'Aggressive'
    primary_goal: str = 'Income',  # 'Income', 'Growth', 'Hedging', 'Balanced'
    tier_filter: str = 'tier1_only',
    exploration_mode: bool = False
) -> pd.DataFrame:
    """
    Rank and annotate strategies emitted by Step 7 with personalization factors.
    
    🚨 SAFETY GATE: By default, outputs ONLY Tier-1 (executable) strategies.
    
    Args:
        df: DataFrame from Step 7 with market context and pre-assigned strategies.
            Required columns: ['Ticker', 'Signal_Type', 'Regime', 'IVHV_gap_30D', 
                               'IVHV_gap_60D', 'IVHV_gap_180D', 'Strategy_Name', 
                               'Strategy_Tier', 'Risk_Profile', 'Timeframe_Category',
                               'DTE_Min', 'DTE_Max', 'DTE_Preferred', 'Confidence',
                               'Success_Probability', 'Trade_Bias', 'Entry_Priority']
        max_strategies_per_ticker: Maximum strategies to suggest per ticker
        account_size: User's account size ($) - affects capital allocation
        risk_tolerance: 'Conservative', 'Moderate', 'Aggressive'
        primary_goal: 'Income', 'Growth', 'Hedging', 'Balanced'
        tier_filter: 'tier1_only' (default, executable) | 'include_tier2' | 'all_tiers'
        exploration_mode: If True, include all tiers with NON_EXECUTABLE flag (educational)
    
    Returns:
        DataFrame with one row per strategy option (exploded from tickers)
        Columns:
            - Ticker
            - Strategy_Option_Rank (1-6, 1=best FOR THIS USER)
            - Strategy_Name (e.g., "Put Credit Spread")
            - Timeframe_Category ("Short", "Medium", "Long-LEAP", "Ultra-LEAP")
            - DTE_Min, DTE_Max, DTE_Preferred
            - Risk_Reward_Ratio (e.g., 1:3)
            - Success_Probability (0-1)
            - Capital_Requirement_Est ($)
            - Percent_Of_Account (%)
            - Risk_Profile ("Conservative", "Moderate", "Aggressive")
            - Fits_User_Profile (True/False) - matches user's risk tolerance
            - Goal_Alignment_Score (0-100) - how well it fits user's goal
            - User_Fit_Score (0-100, higher = better fit FOR THIS USER)
              # NOTE: User_Fit_Score is NOT PCS, NOT an edge metric, NOT used for execution decisions.
              # It is a heuristic score for how well a strategy aligns with the user's profile and market context.
            - Personal_Recommendation (why this is good for YOU)
            - Strategy_Tier (1=executable, 2=blocked, 3=not ready)
            - EXECUTABLE (bool: True=can execute, False=educational only)
    """
    logger.info(f"🎯 Ranking personalized strategies for {len(df)} strategies from Step 7")
    logger.info(f"   User Profile: ${account_size:,.0f} account | {risk_tolerance} risk | {primary_goal} goal")
    logger.info(f"   Tier Filter: {tier_filter} | Exploration Mode: {exploration_mode}")
    
    # Strict schema validation: Fail fast if authoritative columns are missing
    required_cols = [
        'Ticker', 'Signal_Type', 'Regime', 'IVHV_gap_30D', 'IVHV_gap_60D', 
        'IVHV_gap_180D', 'Strategy_Name', 'Strategy_Tier', 'Risk_Profile', 
        'Timeframe_Category', 'DTE_Min', 'DTE_Max', 'DTE_Preferred', 'Confidence',
        'Success_Probability', 'Trade_Bias', 'Entry_Priority'
    ]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        error_msg = (
            f"❌ Step 7B: Missing required authoritative columns from Step 7: {missing}. "
            "This is a critical pipeline error. The schema contract was violated upstream. "
            "Step 7B will not proceed with incomplete data in a production environment."
        )
        logger.error(error_msg)
        raise ValueError(error_msg)

    # Ensure df is not empty after validation
    if df.empty:
        logger.warning("⚠️ Input DataFrame to Step 7B is empty. No strategies to rank.")
        return pd.DataFrame()

    df_suggestions = df.copy()

    # User profile factors
    max_position_size = account_size * 0.10  # Max 10% per position
    risk_multiplier = {'Conservative': 0.5, 'Moderate': 1.0, 'Aggressive': 1.5}.get(risk_tolerance, 1.0)
    
    # Goal alignment scoring
    def score_goal_alignment(strategy_type: str, timeframe: str, primary_goal: str) -> int:
        """Score 0-100 how well strategy fits user's goal."""
        if primary_goal == 'Income':
            if strategy_type in ['Put Credit Spread', 'Call Credit Spread', 'Iron Condor', 'Covered Call (if holding stock)', 'Wheel Strategy (Cash-Secured Puts)'] and timeframe == 'Short':
                return 95
            elif strategy_type in ['Poor Man\'s Covered Call (LEAP base)'] and timeframe == 'Long-LEAP':
                return 90
            elif timeframe in ['Medium', 'Long-LEAP']:
                return 40  # Income traders prefer short-term
        
        elif primary_goal == 'Growth':
            if strategy_type in ['LEAP Call Debit Spread', 'LEAP Call (Buy to Open)'] and timeframe == 'Long-LEAP':
                return 95
            elif strategy_type in ['Poor Man\'s Covered Call (LEAP base)'] and timeframe == 'Long-LEAP':
                return 90
            elif strategy_type in ['Call Debit Spread', 'Put Debit Spread'] and timeframe == 'Medium':
                return 75
            elif timeframe == 'Short':
                return 50  # Growth traders can use short-term, but not ideal
        
        elif primary_goal == 'Hedging':
            if strategy_type in ['LEAP Put Debit Spread', 'LEAP Put (Buy to Open)'] and timeframe in ['Medium', 'Long-LEAP']:
                return 95
            elif strategy_type in ['Put Debit Spread', 'Long Put'] and timeframe == 'Short':
                return 70
        
        elif primary_goal == 'Balanced':
            return 70  # All strategies have merit for balanced approach
        
        return 50  # Default moderate fit

    # Apply personalization and calculate scores for each strategy
    processed_strategies = []
    for idx, row in df_suggestions.iterrows():
        strategy = row.to_dict() # Start with the existing strategy from Step 7

        # Calculate Fits_User_Profile
        strategy['Fits_User_Profile'] = strategy['Risk_Profile'] in [risk_tolerance, 'Moderate'] if risk_tolerance == 'Conservative' else True

        # Calculate Goal_Alignment_Score
        strategy['Goal_Alignment_Score'] = score_goal_alignment(
            strategy['Strategy_Name'], 
            strategy['Timeframe_Category'], 
            primary_goal
        )

        # Calculate User_Fit_Score (formerly Suitability_Score)
        # This is a heuristic score based on existing context and user profile.
        # It is NOT PCS, NOT an edge metric, NOT used for execution decisions.
        base_score = strategy['Confidence'] # Use Confidence from Step 7 as a base
        goal_bonus = (strategy['Goal_Alignment_Score'] - 50) // 5 # Adjust based on goal alignment
        risk_penalty = 0
        if risk_tolerance == 'Conservative' and strategy['Risk_Profile'] == 'Aggressive':
            risk_penalty = -20
        elif risk_tolerance == 'Aggressive' and strategy['Risk_Profile'] == 'Conservative':
            risk_penalty = 10 # Aggressive users might prefer conservative for diversification

        strategy['User_Fit_Score'] = min(100, max(0, base_score + goal_bonus + risk_penalty))
        
        # Generate Personal_Recommendation
        strategy['Personal_Recommendation'] = f"This {strategy['Strategy_Name']} ({strategy['Timeframe_Category']}) strategy aligns with your {primary_goal.lower()} goal and {risk_tolerance.lower()} risk profile. Confidence from Step 7: {strategy['Confidence']:.0f}."

        # Estimate Capital Requirement (simplified for this layer, actual sizing in Step 8)
        # This is a placeholder, actual capital will be determined in Step 8
        if 'Capital_Requirement_Est' not in strategy or pd.isna(strategy['Capital_Requirement_Est']):
            strategy['Capital_Requirement_Est'] = 1000 # Default estimate
        
        strategy['Percent_Of_Account'] = (strategy['Capital_Requirement_Est'] / account_size) * 100 if account_size > 0 else 0

        processed_strategies.append(strategy)

    df_suggestions = pd.DataFrame(processed_strategies)
    
    # Enrich strategies with tier metadata (Tier 1 = executable, Tier 2+ = strategy-only)
    # This is already done in Step 7, but re-apply to ensure consistency if df was sliced
    df_suggestions = _enrich_with_tier_metadata(df_suggestions.to_dict('records'))
    df_suggestions = pd.DataFrame(df_suggestions) # Convert back to DataFrame

    # Rank strategies by personalized User_Fit_Score
    df_suggestions = df_suggestions.sort_values(by='User_Fit_Score', ascending=False).reset_index(drop=True)
    df_suggestions['Strategy_Option_Rank'] = df_suggestions.groupby('Ticker').cumcount() + 1
    
    # 🚨 TIER-1 ENFORCEMENT (SAFETY GATE)
    if not df_suggestions.empty and 'Strategy_Tier' in df_suggestions.columns:
        # Apply tier filtering based on mode
        if tier_filter == 'tier1_only' and not exploration_mode:
            # DEFAULT SAFETY MODE: Output only Tier-1 strategies
            tier1_count = (df_suggestions['Strategy_Tier'] == 1).sum()
            total_count = len(df_suggestions)
            df_suggestions = df_suggestions[df_suggestions['Strategy_Tier'] == 1].copy()
            logger.info(f"🔒 TIER-1 FILTER: {tier1_count}/{total_count} strategies are Tier-1 (executable)")
            logger.info(f"   Non-Tier-1 strategies excluded for safety (use exploration_mode=True to see all)")
        elif tier_filter == 'include_tier2':
            # Include Tier 1 and Tier 2
            df_suggestions = df_suggestions[df_suggestions['Strategy_Tier'].isin([1, 2])].copy()
            logger.info(f"📋 TIER-1+2 FILTER: Including Tier-1 and Tier-2 strategies")
        elif tier_filter == 'all_tiers' or exploration_mode:
            # Educational mode: include all tiers
            logger.info(f"📚 EXPLORATION MODE: Including all strategy tiers (educational only)")
        
        # Tag exploration data as NON-EXECUTABLE
        if exploration_mode or tier_filter != 'tier1_only':
            df_suggestions['EXECUTABLE'] = (df_suggestions['Strategy_Tier'] == 1)
            non_exec_count = (~df_suggestions['EXECUTABLE']).sum()
            logger.warning(f"⚠️ {non_exec_count} strategies tagged NON_EXECUTABLE (Tier 2/3)")
        else:
            # All remaining strategies are Tier-1, thus executable
            df_suggestions['EXECUTABLE'] = True
    
    logger.info(f"✅ Generated {len(df_suggestions)} personalized strategy options")
    if len(df) > 0:
        logger.info(f"   Average: {len(df_suggestions)/df['Ticker'].nunique():.1f} options per ticker")
    
    # Log tier distribution
    if not df_suggestions.empty:
        tier1_count = (df_suggestions.get('Execution_Ready', False) == True).sum()
        tier2_plus_count = (df_suggestions.get('Execution_Ready', False) == False).sum()
        logger.info(f"   Tier 1 (executable now): {tier1_count}")
        logger.info(f"   Tier 2+ (strategy-only): {tier2_plus_count}")
    
    # Log goal alignment distribution
    if not df_suggestions.empty:
        high_alignment = (df_suggestions['Goal_Alignment_Score'] >= 80).sum()
        logger.info(f"   High goal alignment (≥80): {high_alignment}/{len(df_suggestions)}")
    
    return df_suggestions


def _enrich_with_tier_metadata(strategies: List[Dict]) -> List[Dict]:
    """
    Add execution tier metadata to each strategy.
    
    Adds columns:
        - Strategy_Tier (int): 1 = executable, 2+ = strategy-only
        - Execution_Ready (bool): Can we scan option chains for this?
        - Broker_Approval_Level (str): 'Tier 1', 'Spreads', etc.
        - Execution_Blocker (str): Why not executable (if Tier 2+)
    
    Args:
        strategies (List[Dict]): List of strategy dictionaries
    
    Returns:
        List[Dict]: Same strategies with tier metadata added
    """
    for strat in strategies:
        strategy_name = strat['Strategy_Name'] # No .get() fallback, must exist
        tier_meta = get_strategy_tier(strategy_name)
        
        # Add tier metadata
        strat['Strategy_Tier'] = tier_meta['tier'] # No .get() fallback
        strat['Execution_Ready'] = tier_meta['execution_ready'] # No .get() fallback
        strat['Broker_Approval_Level'] = tier_meta['broker_approval'] # No .get() fallback
        strat['Execution_Blocker'] = tier_meta['blocker'] # No .get() fallback
        
        # Log if strategy not found in tier map
        if tier_meta['tier'] == 2 and 'not in tier map' in tier_meta['blocker']:
            logger.warning(f"Strategy '{strategy_name}' not in tier map - defaulting to Tier 2 (not executable)")
    
    return strategies


def rank_strategies_by_criteria(
    df_suggestions: pd.DataFrame,
    sort_by: str = 'User_Fit_Score' # Renamed from Suitability_Score
) -> pd.DataFrame:
    """
    Re-rank strategies by different criteria.
    
    Args:
        df_suggestions: Output from generate_multi_strategy_suggestions()
        sort_by: One of:
            - 'User_Fit_Score' (default, best overall fit)
            - 'Success_Probability' (highest win rate)
            - 'Risk_Reward_Ratio' (best reward potential)
            - 'Capital_Requirement_Est' (cheapest first)
    
    Returns:
        Re-ranked DataFrame
    """
    if sort_by == 'Risk_Reward_Ratio':
        # Parse "1:3" → 3.0 for sorting
        df_suggestions['RR_Numeric'] = df_suggestions['Risk_Reward_Ratio'].apply(
            lambda x: float(x.split(':')[1]) if ':' in str(x) else 1.0
        )
        df_sorted = df_suggestions.sort_values(['Ticker', 'RR_Numeric'], ascending=[True, False])
        df_sorted = df_sorted.drop(columns=['RR_Numeric'])
    elif sort_by == 'Capital_Requirement_Est':
        df_sorted = df_suggestions.sort_values(['Ticker', sort_by], ascending=[True, True])
    else:
        df_sorted = df_suggestions.sort_values(['Ticker', sort_by], ascending=[True, False])
    
    # Recalculate ranks within each ticker
    df_sorted['Strategy_Option_Rank'] = df_sorted.groupby('Ticker').cumcount() + 1
    
    return df_sorted
