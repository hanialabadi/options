"""
LEGACY FILE: Step 11: Strategy Comparison & Ranking (DEPRECATED)

⚠️  THIS FILE IS DEPRECATED AND SHOULD NOT BE USED.
    The authoritative strategy evaluation is now performed by `step11_independent_evaluation.py`.
    This file is preserved for historical context only.

PURPOSE:
    Compare ALL strategies per ticker with real contract data and rank them.
    Provide comparison metrics to enable informed decision-making in Step 8.
    
    CRITICAL ARCHITECTURAL PRINCIPLE:
    - Process EACH (Ticker, Strategy) independently (preserves multi-strategy ledger)
    - Compare strategies PER TICKER (apples-to-apples comparison)
    - Rank strategies with comparison metrics (NOT final selection)
    - NO final decision here (that's Step 8's job after repositioning)
    - Row count IN == Row count OUT (all strategies preserved)

DESIGN PRINCIPLES:
    - Strategy-aware comparison (each strategy evaluated on its own merits)
    - Apples-to-apples metrics (same ticker, different strategies)
    - Real contract data (Greeks, premiums, spreads from Step 9B)
    - Goal-aware scoring (align with user's trading objectives)
    - Preserve all strategies (let Step 8 make final decision)

COMPARISON METRICS:
    1. Expected Return: Risk-adjusted profit potential
    2. Greeks Quality: Delta exposure, Vega sensitivity, Gamma risk
    3. Cost Efficiency: Premium cost vs max profit potential
    4. Liquidity Quality: Bid-ask spread, open interest
    5. Strategy Fit: Alignment with IV conditions and trade bias
    6. Rank Per Ticker: 1 = best, 2 = second-best, etc.

INPUTS (from Step 9B/10):
    - Ticker, Strategy_Name, Strategy_Type, Trade_Bias
    - Selected_Strikes, Contract_Symbols (from Step 9B)
    - Actual_DTE, Min_DTE, Max_DTE, Target_DTE
    - Delta, Vega, Gamma, Theta (Greeks from Step 10)
    - Total_Debit, Total_Credit, Bid_Ask_Spread_Pct
    - Open_Interest, Liquidity_Score
    - Contract_Selection_Status (Success/Failed)

OUTPUTS:
    - Strategy_Rank: Rank within ticker (1 = best, 2 = second, etc.)
    - Expected_Return_Pct: Estimated return percentage
    - Risk_Adjusted_Score: Return / risk ratio
    - Greeks_Quality_Score: Quality of Greeks profile (0-100)
    - Cost_Efficiency_Score: Premium efficiency (0-100)
    - Liquidity_Quality_Score: Market depth quality (0-100)
    - Comparison_Score: Overall comparison metric (0-100)
    - Comparison_Notes: Why this strategy ranked where it did

MULTI-STRATEGY EXAMPLE:
    Input: AAPL with 3 strategies (all with contracts)
      AAPL | Long Call     | Contract: 150C @ $5.00 | Delta: 0.50
      AAPL | Long Straddle | Contract: 150C+P @ $12.00 | Vega: 0.80
      AAPL | Buy-Write     | Contract: 155C @ $2.50 | Delta: -0.30
    
    Output: AAPL with 3 ranked strategies
      AAPL | Long Call     | Rank: 1 | Expected Return: 15% | Score: 85
      AAPL | Buy-Write     | Rank: 2 | Expected Return: 12% | Score: 78
      AAPL | Long Straddle | Rank: 3 | Expected Return: 10% | Score: 72
    
    (Step 8 will then select 0-1 strategy for execution)
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, Tuple, List
import json
from datetime import datetime

logger = logging.getLogger(__name__)


def compare_and_rank_strategies(
    df: pd.DataFrame,
    user_goal: str = 'income',
    account_size: float = 10000.0,
    risk_tolerance: str = 'moderate'
) -> pd.DataFrame:
    # AGENT SAFETY: This function is DEPRECATED.
    # It performs legacy strategy ranking and comparison, which violates the new architecture.
    # All strategy evaluation is now handled by `step11_independent_evaluation.py`.
    raise RuntimeError(
        "❌ DEPRECATED FUNCTION CALL: `step11_strategy_pairing.compare_and_rank_strategies()` "
        "is no longer valid. Refer to LEGACY.md for details. "
        "Use `step11_independent_evaluation.evaluate_strategies_independently()` instead."
    )
    """
    Compare all strategies per ticker and rank them with comparison metrics.
    
    This function evaluates each (Ticker, Strategy) with real contract data,
    calculates comparison metrics, ranks strategies within each ticker,
    and preserves ALL strategies for Step 8's final decision.
    
    CRITICAL: NO final selection here - all strategies preserved.
    Step 8 (after repositioning) will make the final 0-1 decision per ticker.
    
    Args:
        df (pd.DataFrame): Step 9B/10 output with contracts and Greeks
        user_goal (str): 'income', 'growth', or 'capital_preservation'. Default 'income'.
        account_size (float): Account size for risk calculations. Default 10000.
        risk_tolerance (str): 'conservative', 'moderate', or 'aggressive'. Default 'moderate'.
    
    Returns:
        pd.DataFrame: All strategies with comparison metrics and ranks
        
    Row Preservation:
        Input rows == Output rows (no strategies dropped)
        
    Example:
        >>> df_contracts = fetch_and_select_contracts(df_step9a)
        >>> df_ranked = compare_and_rank_strategies(
        ...     df_contracts,
        ...     user_goal='income',
        ...     account_size=10000
        ... )
        >>> # Result: All 266 strategies preserved with comparison metrics
        >>> # AAPL: Long Call (Rank 1), Buy-Write (Rank 2), Straddle (Rank 3)
    """
    
    if df.empty:
        logger.warning("⚠️ Empty DataFrame passed to Step 11")
        return df
    
    # ========================================
    # CRITICAL: Row Count Preservation (Strategy-Aware Architecture)
    # ========================================
    input_row_count = len(df)
    logger.info(f"🎯 Step 11: Comparing {input_row_count} strategies (strategy-aware comparison)")
    logger.info(f"   User Goal: {user_goal} | Account: ${account_size:,.0f} | Risk: {risk_tolerance}")
    
    # Filter for strategies with successful contract selection
    # Note: Failed strategies are preserved but marked (for transparency)
    if 'Contract_Selection_Status' in df.columns:
        df_with_contracts = df[df['Contract_Selection_Status'] == 'Success'].copy()
        df_without_contracts = df[df['Contract_Selection_Status'] != 'Success'].copy()
    else:
        # If column doesn't exist, assume all have contracts
        df_with_contracts = df.copy()
        df_without_contracts = pd.DataFrame()
    
    logger.info(f"   📊 {len(df_with_contracts)} strategies with contracts")
    logger.info(f"   ⚠️  {len(df_without_contracts)} strategies without contracts (will rank lower)")
    
    # Calculate comparison metrics for all strategies
    df_with_metrics = _calculate_comparison_metrics(
        df_with_contracts,
        user_goal=user_goal,
        account_size=account_size,
        risk_tolerance=risk_tolerance
    )
    
    # Rank strategies per ticker
    df_ranked = _rank_strategies_per_ticker(df_with_metrics)
    
    # Add failed strategies back with low ranks
    if not df_without_contracts.empty:
        df_without_contracts = _mark_failed_strategies(df_without_contracts)
        df_ranked = pd.concat([df_ranked, df_without_contracts], ignore_index=True)
    
    # ========================================
    # ROW COUNT ASSERTION (No strategies dropped)
    # ========================================
    output_row_count = len(df_ranked)
    assert output_row_count == input_row_count, (
        f"❌ Row count mismatch: {output_row_count} != {input_row_count}. "
        f"Step 11 must preserve all strategies (no silent filtering)."
    )
    logger.info(f"✅ Row count preserved: {output_row_count} strategies processed")
    
    # Audit multi-strategy architecture
    _audit_multi_strategy_rankings(df_ranked)
    
    logger.info(f"🎯 Step 11 Complete: {len(df_ranked)} strategies ranked")
    _log_ranking_summary(df_ranked)
    
    return df_ranked


def _calculate_comparison_metrics(df: pd.DataFrame, user_goal: str, account_size: float, risk_tolerance: str) -> pd.DataFrame:
    """
    Calculate comparison metrics for all strategies.
    
    Metrics include:
    - Expected Return Score: PCS_Final + Greeks quality
    - Cost Efficiency: Return per dollar invested
    - Risk-Adjusted Return: Incorporates Delta/Gamma risk
    - Liquidity Quality: Bid-ask spread + open interest
    - Goal Alignment: Matches user's stated goal (income/growth/volatility)
    
    Args:
        df: Strategies with contracts
        user_goal: User's trading goal (income, growth, volatility)
        account_size: Account capital in dollars
        risk_tolerance: low, medium, high
    
    Returns:
        DataFrame with comparison metrics added
    """
    
    df_metrics = df.copy()
    
    # 1. Expected Return Score (baseline from PCS_Final)
    df_metrics['Expected_Return_Score'] = df_metrics.get('PCS_Final', 50)
    
    # 2. Greeks Quality Score (0-100)
    # High Vega = good for volatility strategies
    # Low Delta = good for neutral strategies
    # High Gamma = sensitive to price moves
    if 'Vega' in df_metrics.columns:
        vega_norm = df_metrics['Vega'].clip(0, 2) / 2 * 100  # Normalize 0-2 to 0-100
    else:
        vega_norm = 0
    
    if 'Delta' in df_metrics.columns:
        delta_risk = (1 - df_metrics['Delta'].abs().clip(0, 1)) * 100  # Low Delta = lower risk
    else:
        delta_risk = 50  # Neutral default
    
    df_metrics['Greeks_Quality_Score'] = (vega_norm * 0.6 + delta_risk * 0.4)
    
    # 3. Cost Efficiency (return per $1000 invested)
    if 'Total_Debit' in df_metrics.columns:
        total_debit = df_metrics['Total_Debit'].clip(10, None)
    else:
        total_debit = 100
    
    df_metrics['Cost_Efficiency_Score'] = (
        df_metrics['Expected_Return_Score'] / (total_debit / 1000)
    ).clip(0, 100)
    
    # 4. Liquidity Quality (0-100, lower bid-ask = better)
    if 'Bid_Ask_Spread_Pct' in df_metrics.columns:
        spread_penalty = (1 - df_metrics['Bid_Ask_Spread_Pct'].clip(0, 10) / 10) * 100
    else:
        spread_penalty = 50  # Neutral default
    
    if 'Open_Interest' in df_metrics.columns:
        oi_quality = (df_metrics['Open_Interest'].clip(0, 1000) / 1000) * 100
    else:
        oi_quality = 0
    
    df_metrics['Liquidity_Quality_Score'] = (spread_penalty * 0.7 + oi_quality * 0.3)
    
    # 5. Goal Alignment Score
    df_metrics['Goal_Alignment_Score'] = _calculate_goal_alignment(
        df_metrics, user_goal
    )
    
    # 6. Risk-Adjusted Return (incorporates risk tolerance)
    risk_multiplier = {'low': 0.5, 'medium': 1.0, 'high': 1.5}.get(risk_tolerance, 1.0)
    df_metrics['Risk_Adjusted_Return'] = (
        df_metrics['Expected_Return_Score'] * risk_multiplier
    )
    
    # 7. Composite Comparison Score (weighted average)
    # Fill any NaN component scores with neutral values (50) before combining
    df_metrics['Expected_Return_Score'] = df_metrics['Expected_Return_Score'].fillna(50)
    df_metrics['Greeks_Quality_Score'] = df_metrics['Greeks_Quality_Score'].fillna(50)
    df_metrics['Cost_Efficiency_Score'] = df_metrics['Cost_Efficiency_Score'].fillna(50)
    df_metrics['Liquidity_Quality_Score'] = df_metrics['Liquidity_Quality_Score'].fillna(50)
    df_metrics['Goal_Alignment_Score'] = df_metrics['Goal_Alignment_Score'].fillna(50)
    df_metrics['Risk_Adjusted_Return'] = df_metrics['Risk_Adjusted_Return'].fillna(50)
    
    df_metrics['Comparison_Score'] = (
        df_metrics['Expected_Return_Score'] * 0.30 +
        df_metrics['Greeks_Quality_Score'] * 0.20 +
        df_metrics['Cost_Efficiency_Score'] * 0.20 +
        df_metrics['Liquidity_Quality_Score'] * 0.15 +
        df_metrics['Goal_Alignment_Score'] * 0.10 +
        df_metrics['Risk_Adjusted_Return'] * 0.05
    ).round(2)
    
    logger.info(f"   📊 Calculated comparison metrics for {len(df_metrics)} strategies")
    logger.info(f"      Avg Comparison Score: {df_metrics['Comparison_Score'].mean():.2f}")
    logger.info(f"      Score Range: {df_metrics['Comparison_Score'].min():.2f} - {df_metrics['Comparison_Score'].max():.2f}")
    
    return df_metrics


def _calculate_goal_alignment(df: pd.DataFrame, user_goal: str) -> pd.Series:
    """
    Calculate how well each strategy aligns with user's goal.
    
    Args:
        df: Strategies with contracts
        user_goal: income, growth, volatility, balanced
    
    Returns:
        Series of alignment scores (0-100)
    """
    
    alignment = pd.Series(50, index=df.index)  # Baseline neutral
    
    if user_goal == 'income':
        # Prefer short premium strategies (credit spreads, covered calls)
        # Prefer lower DTE (faster theta decay)
        if 'Trade_Type' in df.columns:
            is_credit = df['Trade_Type'].str.contains('Credit|Covered', case=False, na=False)
            alignment[is_credit] = 80
        
        if 'Actual_DTE' in df.columns:
            low_dte = df['Actual_DTE'] < 35
            alignment[low_dte] += 10
        
    elif user_goal == 'growth':
        # Prefer directional strategies (long calls/puts)
        # RAG: Passarelli - "Directional trades require Delta + Gamma alignment"
        
        # Check for directional conviction (need multiple confirmations)
        if 'Trade_Bias' in df.columns:
            is_directional = df['Trade_Bias'] == 'Directional'
            alignment[is_directional] = 60  # Base score (not automatic win)
        
        # Bonus for strong Delta (directional exposure)
        if 'Delta' in df.columns:
            strong_delta = df['Delta'].abs() > 0.45
            alignment[strong_delta] += 20
        
        # Bonus for positive Gamma (convexity in direction)
        if 'Gamma' in df.columns:
            positive_gamma = df['Gamma'] > 0.03
            alignment[positive_gamma] += 15
        
        # Penalize weak conviction (low Delta + low Gamma)
        if 'Delta' in df.columns and 'Gamma' in df.columns:
            weak_conviction = (df['Delta'].abs() < 0.30) & (df['Gamma'] < 0.02)
            alignment[weak_conviction] -= 25
        
    elif user_goal == 'volatility':
        # Prefer straddles/strangles
        # RAG: Cohen - "Buying vol without an edge is expensive"
        # STRICT: Must show IV justification
        
        if 'Primary_Strategy' in df.columns:
            is_vol_strategy = df['Primary_Strategy'].str.contains('Straddle|Strangle', case=False, na=False)
            alignment[is_vol_strategy] = 50  # Baseline (NOT 85 - must earn it)
        
        # Bonus for high Vega (vol sensitivity) - ONLY for vol strategies
        if 'Vega' in df.columns and 'Primary_Strategy' in df.columns:
            is_vol_strategy = df['Primary_Strategy'].str.contains('Straddle|Strangle', case=False, na=False)
            high_vega = (df['Vega'] > 0.40) & is_vol_strategy
            alignment[high_vega] += 20
        
        # CRITICAL: Bonus for IV justification (expansion potential)
        if 'IV_Percentile' in df.columns:
            # IV in 30-60 range = potential expansion
            iv_edge = (df['IV_Percentile'] >= 30) & (df['IV_Percentile'] <= 60)
            alignment[iv_edge] += 25
            
            # High IV = expensive premium (penalize)
            high_iv = df['IV_Percentile'] > 70
            alignment[high_iv] -= 15
            
            # Low IV = no edge (penalize harder)
            low_iv = df['IV_Percentile'] < 25
            alignment[low_iv] -= 30
            
            # RAG VIOLATION: Vol strategies with missing IV values
            if 'Primary_Strategy' in df.columns:
                is_vol_strategy = df['Primary_Strategy'].str.contains('Straddle|Strangle', case=False, na=False)
                missing_iv = df['IV_Percentile'].isna()
                has_vol_but_no_iv = is_vol_strategy & missing_iv
                alignment[has_vol_but_no_iv] = 30  # Force to low score
        else:
            # RAG VIOLATION: Cannot select vol strategy without IV context
            if 'Primary_Strategy' in df.columns:
                is_vol_strategy_no_iv = df['Primary_Strategy'].str.contains('Straddle|Strangle', case=False, na=False)
                alignment[is_vol_strategy_no_iv] = 30  # Force to low score
        
        # Penalty for directional bias (should be delta-neutral)
        if 'Delta' in df.columns:
            directional_bias = df['Delta'].abs() > 0.20
            alignment[directional_bias] -= 15
        
        # Penalty for directional strategies when goal is volatility
        if 'Trade_Bias' in df.columns:
            is_directional = df['Trade_Bias'] == 'Directional'
            alignment[is_directional] -= 20  # Not aligned with vol goal
        
    elif user_goal == 'balanced':
        # Slight preference for medium risk strategies
        # No strong bias
        alignment = pd.Series(60, index=df.index)
    
    return alignment.clip(0, 100)


def _rank_strategies_per_ticker(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rank strategies per ticker (1 = best, 2 = second-best, etc.).
    
    Args:
        df: Strategies with comparison metrics
    
    Returns:
        DataFrame with Strategy_Rank column added
    """
    
    df_ranked = df.copy()
    
    # Fill NaN Comparison_Score values with 0 before ranking
    # (NaN scores indicate incomplete data - rank them lowest)
    df_ranked['Comparison_Score'] = df_ranked['Comparison_Score'].fillna(0)
    
    # Group by ticker and rank by comparison score (descending)
    df_ranked['Strategy_Rank'] = (
        df_ranked.groupby('Ticker')['Comparison_Score']
        .rank(method='dense', ascending=False)
        .astype(int)
    )
    
    # Log multi-strategy tickers
    multi_strategy_tickers = df_ranked.groupby('Ticker').size()
    multi_strategy_tickers = multi_strategy_tickers[multi_strategy_tickers > 1]
    
    logger.info(f"   🎯 Ranked {len(df_ranked)} strategies")
    logger.info(f"      {len(multi_strategy_tickers)} tickers have multiple strategies")
    
    # Show top-ranked strategies per ticker
    top_ranked = df_ranked[df_ranked['Strategy_Rank'] == 1]
    logger.info(f"      Top-ranked strategies: {top_ranked['Primary_Strategy'].value_counts().to_dict()}")
    
    return df_ranked


def _mark_failed_strategies(df: pd.DataFrame) -> pd.DataFrame:
    """
    Mark strategies without contracts with comparison metrics.
    
    Args:
        df: Strategies without contracts
    
    Returns:
        DataFrame with comparison columns added (low scores)
    """
    
    df_marked = df.copy()
    
    # Add comparison columns with low scores
    df_marked['Expected_Return_Score'] = 0
    df_marked['Greeks_Quality_Score'] = 0
    df_marked['Cost_Efficiency_Score'] = 0
    df_marked['Liquidity_Quality_Score'] = 0
    df_marked['Goal_Alignment_Score'] = 0
    df_marked['Risk_Adjusted_Return'] = 0
    df_marked['Comparison_Score'] = 0
    df_marked['Strategy_Rank'] = 999  # Low rank (out of comparison)
    
    logger.info(f"   ⚠️  Marked {len(df_marked)} failed strategies (rank 999)")
    
    return df_marked


def _audit_multi_strategy_rankings(df: pd.DataFrame) -> None:
    """
    Audit multi-strategy architecture (validate strategy preservation).
    
    Args:
        df: Ranked strategies
    """
    
    # Count strategies per ticker
    strategies_per_ticker = df.groupby('Ticker').size()
    multi_strategy_tickers = strategies_per_ticker[strategies_per_ticker > 1]
    
    logger.info(f"   📊 Multi-Strategy Architecture Audit:")
    logger.info(f"      Total Tickers: {len(strategies_per_ticker)}")
    logger.info(f"      Multi-Strategy Tickers: {len(multi_strategy_tickers)} ({len(multi_strategy_tickers)/len(strategies_per_ticker)*100:.1f}%)")
    logger.info(f"      Avg Strategies/Ticker: {strategies_per_ticker.mean():.2f}")
    logger.info(f"      Max Strategies/Ticker: {strategies_per_ticker.max()}")
    
    # Show example multi-strategy ticker
    if not multi_strategy_tickers.empty:
        example_ticker = multi_strategy_tickers.index[0]
        example_strategies = df[df['Ticker'] == example_ticker][['Primary_Strategy', 'Strategy_Rank', 'Comparison_Score']]
        logger.info(f"      Example: {example_ticker}")
        for _, row in example_strategies.iterrows():
            logger.info(f"         Rank {row['Strategy_Rank']}: {row['Primary_Strategy']} (Score: {row['Comparison_Score']:.2f})")


def _log_ranking_summary(df: pd.DataFrame) -> None:
    """
    Log summary of strategy rankings.
    
    Args:
        df: Ranked strategies
    """
    
    logger.info(f"   📊 Ranking Summary:")
    
    # Count by rank
    rank_counts = df['Strategy_Rank'].value_counts().sort_index()
    logger.info(f"      Rank 1 (Top): {rank_counts.get(1, 0)} strategies")
    logger.info(f"      Rank 2: {rank_counts.get(2, 0)} strategies")
    logger.info(f"      Rank 3+: {rank_counts[rank_counts.index > 2].sum()} strategies")
    logger.info(f"      Rank 999 (Failed): {rank_counts.get(999, 0)} strategies")
    
    # Comparison score distribution
    with_scores = df[df['Comparison_Score'] > 0]
    if not with_scores.empty:
        logger.info(f"      Avg Comparison Score: {with_scores['Comparison_Score'].mean():.2f}")
        logger.info(f"      Top Score: {with_scores['Comparison_Score'].max():.2f} ({with_scores.loc[with_scores['Comparison_Score'].idxmax(), 'Ticker']} - {with_scores.loc[with_scores['Comparison_Score'].idxmax(), 'Primary_Strategy']})")


# ============================================================
# DEPRECATED FUNCTIONS (Kept for backward compatibility)
# ============================================================

def _pair_straddles(df: pd.DataFrame) -> pd.DataFrame:
    """DEPRECATED: Step 11 now compares strategies, not pairs them."""
    logger.warning("⚠️ _pair_straddles() is deprecated (Step 11 now does comparison, not pairing)")
    return pd.DataFrame()


def _pair_strangles(df: pd.DataFrame) -> pd.DataFrame:
    """DEPRECATED: Step 11 now compares strategies, not pairs them."""
    logger.warning("⚠️ _pair_strangles() is deprecated (Step 11 now does comparison, not pairing)")
    return pd.DataFrame()


def _select_best_per_ticker(df: pd.DataFrame) -> pd.DataFrame:
    """DEPRECATED: Step 11 now ranks strategies, not selects them."""
    logger.warning("⚠️ _select_best_per_ticker() is deprecated (Step 11 now ranks, Step 8 will select)")
    return pd.DataFrame()


def _calculate_capital_allocation(df: pd.DataFrame, capital_limit: float, max_contracts: int) -> pd.DataFrame:
    """DEPRECATED: Capital allocation moved to Step 8 (after final selection)."""
    logger.warning("⚠️ _calculate_capital_allocation() is deprecated (moved to Step 8)")
    return df


def _log_strategy_summary(df: pd.DataFrame) -> None:
    """DEPRECATED: Replaced by _log_ranking_summary()."""
    logger.warning("⚠️ _log_strategy_summary() is deprecated (use _log_ranking_summary())")
    pass


def _pair_strangles(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create strangle pairs (OTM call + OTM put at same expiration).
    
    DEPRECATED: Step 11 now compares strategies, not pairs them.
    This function is kept for backward compatibility but returns empty DataFrame.
    """
    logger.warning("⚠️ _pair_strangles() called but is deprecated")
    return pd.DataFrame()


def _select_best_per_ticker(df: pd.DataFrame) -> pd.DataFrame:
    """
    Select highest PCS_Final strategy per ticker.
    
    DEPRECATED: Step 11 now ranks strategies, Step 8 will select.
    This function is kept for backward compatibility but returns empty DataFrame.
    """
    logger.warning("⚠️ _select_best_per_ticker() called but is deprecated")
    return pd.DataFrame()


def _calculate_capital_allocation(df: pd.DataFrame, capital_limit: float, max_contracts_per_leg: int = 20) -> pd.DataFrame:
    """
    Calculate RECOMMENDED capital allocation based on PCS_Final tiers.
    
    DEPRECATED: Capital allocation moved to Step 8 (after final selection).
    This function is kept for backward compatibility but returns input unchanged.
    """
    logger.warning("⚠️ _calculate_capital_allocation() called but is deprecated (moved to Step 8)")
    return df


def _log_strategy_summary(df: pd.DataFrame):
    """
    Log summary of selected strategies.
    
    DEPRECATED: Replaced by _log_ranking_summary().
    """
    logger.warning("⚠️ _log_strategy_summary() called but is deprecated (use _log_ranking_summary())")
    pass


# ============================================================
# BACKWARD COMPATIBILITY WRAPPER
# ============================================================

def pair_and_select_strategies(
    df: pd.DataFrame,
    enable_straddles: bool = True,
    enable_strangles: bool = True,
    capital_limit: float = 10000.0,
    max_contracts_per_leg: int = 20
) -> pd.DataFrame:
    """
    DEPRECATED: Legacy function name for backward compatibility.
    
    Use compare_and_rank_strategies() instead.
    
    This function now calls compare_and_rank_strategies() and returns
    the top-ranked strategy per ticker (for compatibility with old code).
    """
    
    logger.warning("⚠️ pair_and_select_strategies() is DEPRECATED")
    logger.warning("   Use compare_and_rank_strategies() instead")
    logger.warning("   This compatibility wrapper will select top-ranked strategy per ticker")
    
    # Call new comparison function with default parameters
    df_ranked = compare_and_rank_strategies(
        df,
        user_goal='income',  # Default for old behavior
        account_size=capital_limit,
        risk_tolerance='medium'
    )
    
    # For backward compatibility: select top-ranked strategy per ticker
    # (Old code expects one per ticker, not all ranked)
    df_best = df_ranked[df_ranked['Strategy_Rank'] == 1].copy()
    
    logger.info(f"   Legacy mode: Selected {len(df_best)} top-ranked strategies")
    
    return df_best
