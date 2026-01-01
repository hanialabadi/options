"""
LEGACY FUNCTIONS for Step 8: Portfolio Management

This file contains functions that are DEPRECATED and are kept ONLY for backward compatibility.
They assume an old architectural model (e.g., cross-strategy ranking, single-strategy selection)
that is no longer authoritative.

DO NOT USE THESE FUNCTIONS FOR NEW DEVELOPMENT.
Refer to LEGACY.md for more details.

The authoritative Step 8 functions are in core/scan_engine/step8_position_sizing.py.
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Tuple, Optional

logger = logging.getLogger(__name__)

# ============================================================
# DEPRECATED: Legacy Position Sizing and Audit Functions
# (These assume pre-Step 11 architecture with Comparison_Score/Strategy_Rank)
# ============================================================

def _select_top_ranked_per_ticker(df: pd.DataFrame) -> pd.DataFrame:
    """
    DEPRECATED: This function uses Strategy_Rank which violates strategy isolation.
    
    Legacy function for backward compatibility only.
    New code should use _filter_by_validation_status() instead.
    """
    
    logger.warning("⚠️ _select_top_ranked_per_ticker() is DEPRECATED")
    logger.warning("   Uses Strategy_Rank (removed in Step 11 refactor)")
    logger.warning("   Use _filter_by_validation_status() instead")
    
    # Check if Strategy_Rank exists
    if 'Strategy_Rank' not in df.columns:
        logger.error("❌ Strategy_Rank column not found - Step 11 refactored to strategy isolation")
        logger.error("   Call allocate_portfolio_capital() instead of finalize_and_size_positions()")
        return pd.DataFrame()  # Return empty
    
    # Filter to rank 1 only
    df_top = df[df['Strategy_Rank'] == 1].copy()
    
    # Sanity check: should be one per ticker
    duplicates = df_top['Ticker'].duplicated().sum()
    if duplicates > 0:
        logger.warning(f"⚠️ Found {duplicates} tickers with multiple rank-1 strategies (taking first)")
        df_top = df_top.drop_duplicates(subset='Ticker', keep='first')
    
    return df_top


def _apply_final_filters(
    df: pd.DataFrame,
    min_comparison_score: float,
    account_balance: float
) -> pd.DataFrame:
    """
    DEPRECATED: This function uses Comparison_Score which violates strategy isolation.
    
    Legacy function for backward compatibility only.
    New code should use _filter_by_validation_status() instead.
    """
    
    logger.warning("⚠️ _apply_final_filters() is DEPRECATED")
    logger.warning("   Uses Comparison_Score (removed in Step 11 refactor)")
    logger.warning("   Use _filter_by_validation_status() instead")
    
    if 'Comparison_Score' not in df.columns:
        logger.error("❌ Comparison_Score column not found - Step 11 refactored to strategy isolation")
        return pd.DataFrame()
    
    df_filtered = df.copy()
    initial_count = len(df_filtered)
    
    # Filter 1: Minimum comparison score
    df_filtered = df_filtered[df_filtered.get('Comparison_Score', 0) >= min_comparison_score]
    logger.info(f"      Filter: Comparison Score ≥ {min_comparison_score}: {len(df_filtered)}/{initial_count}")
    
    # Filter 2: Contract selection success
    if 'Contract_Selection_Status' in df_filtered.columns:
        df_filtered = df_filtered[df_filtered['Contract_Selection_Status'] == 'Success']
        logger.info(f"      Filter: Contract Selection Success: {len(df_filtered)}/{initial_count}")
    
    # Filter 3: Affordable (Total_Debit ≤ 10% of account)
    if 'Total_Debit' in df_filtered.columns:
        max_debit = account_balance * 0.10  # Max 10% per trade
        df_filtered = df_filtered[df_filtered['Total_Debit'] <= max_debit]
        logger.info(f"      Filter: Affordable (≤${max_debit:,.0f}): {len(df_filtered)}/{initial_count}")
    
    # Filter 4: Execution ready (if available)
    if 'Execution_Ready' in df_filtered.columns:
        df_filtered = df_filtered[df_filtered['Execution_Ready'] == True]
        logger.info(f"      Filter: Execution Ready: {len(df_filtered)}/{initial_count}")
    
    return df_filtered


def _apply_portfolio_constraints(
    df: pd.DataFrame,
    max_positions: int,
    diversification_limit: int
) -> pd.DataFrame:
    """
    DEPRECATED: This function uses Comparison_Score sorting which violates strategy isolation.
    
    Legacy function for backward compatibility only.
    New code should use _apply_portfolio_risk_limits() instead.
    """
    
    logger.warning("⚠️ _apply_portfolio_constraints() is DEPRECATED")
    logger.warning("   Uses Comparison_Score sorting (cross-strategy comparison)")
    logger.warning("   Use _apply_portfolio_risk_limits() instead")
    
    if 'Comparison_Score' not in df.columns:
        logger.error("❌ Comparison_Score column not found - Step 11 refactored to strategy isolation")
        return pd.DataFrame()
    
    df_constrained = df.copy()
    
    # Sort by comparison score (best first)
    df_constrained = df_constrained.sort_values('Comparison_Score', ascending=False)
    
    # Constraint 1: Max positions
    if len(df_constrained) > max_positions:
        logger.info(f"      Constraint: Limiting to top {max_positions} positions")
        df_constrained = df_constrained.head(max_positions)
    
    # Constraint 2: Diversification limit
    if 'Primary_Strategy' in df_constrained.columns:
        # Count strategies per type
        strategy_counts = df_constrained['Primary_Strategy'].value_counts()
        
        # Identify over-concentrated strategies
        over_limit = strategy_counts[strategy_counts > diversification_limit]
        
        if not over_limit.empty:
            logger.info(f"      Constraint: Applying diversification limit ({diversification_limit} per strategy)")
            
            # For each over-concentrated strategy, keep top N by score
            filtered_rows = []
            for strategy in df_constrained['Primary_Strategy'].unique():
                strategy_rows = df_constrained[df_constrained['Primary_Strategy'] == strategy]
                
                if len(strategy_rows) > diversification_limit:
                    # Keep top N by comparison score
                    strategy_rows = strategy_rows.head(diversification_limit)
                
                filtered_rows.append(strategy_rows)
            
            df_constrained = pd.concat(filtered_rows, ignore_index=True)
            df_constrained = df_constrained.sort_values('Comparison_Score', ascending=False)
    
    return df_constrained


def _calculate_position_sizing_new(
    df: pd.DataFrame,
    account_balance: float,
    max_portfolio_risk: float,
    max_trade_risk: float,
    sizing_method: str,
    risk_per_contract: float
) -> pd.DataFrame:
    """
    Legacy wrapper - redirects to _allocate_capital_by_score().
    
    Kept for backward compatibility with old Step 8 callers.
    """
    
    logger.info(f"   🔧 Mapping Step 11 columns to Step 8 format...")
    logger.info(f"      Input columns: {list(df.columns)[:10]}...")  # Show first 10
    
    # Map Step 11 columns to Step 8 expected format
    # Step 11 has: Comparison_Score, Strategy_Rank, Greeks_Quality_Score, etc.
    # Step 8 expects: Confidence, Success_Probability, Risk_Level, Strategy_Type
    
    # Map comparison score to confidence (0-100)
    if 'Comparison_Score' in df.columns:
        df['Confidence'] = df['Comparison_Score'].clip(0, 100)
        logger.info(f"      ✓ Mapped Comparison_Score → Confidence (avg: {df['Confidence'].mean():.1f})")
    else:
        df['Confidence'] = 70  # Default
        logger.info(f"      ⚠️ Comparison_Score missing - using default Confidence=70")
    
    # Map from existing columns or use defaults
    if 'Success_Probability' not in df.columns:
        df['Success_Probability'] = 0.55  # Default 55% win rate
        logger.info(f"      ⚠️ Success_Probability missing - using default=0.55")
    
    if 'Risk_Level' not in df.columns:
        df['Risk_Level'] = 'Medium'
        logger.info(f"      ⚠️ Risk_Level missing - using default='Medium'")
    
    if 'Strategy_Type' not in df.columns:
        # Infer from Trade_Bias or use default
        if 'Trade_Bias' in df.columns:
            df['Strategy_Type'] = df['Trade_Bias']
            logger.info(f"      ✓ Mapped Trade_Bias → Strategy_Type")
        else:
            df['Strategy_Type'] = 'Directional'
            logger.info(f"      ⚠️ Trade_Bias missing - using default Strategy_Type='Directional'")
    
    # Ensure Primary_Strategy exists (critical for legacy function)
    if 'Primary_Strategy' not in df.columns:
        logger.error("❌ Primary_Strategy column missing - cannot proceed with position sizing")
        return df
    
    # Call existing position sizing function
    df_sized = calculate_position_sizing_legacy( # Renamed to avoid conflict
        df,
        account_balance=account_balance,
        max_portfolio_risk=max_portfolio_risk,
        max_trade_risk=max_trade_risk,
        sizing_method=sizing_method,
        risk_per_contract=risk_per_contract
    )
    
    return df_sized


def _generate_selection_audit(
    df: pd.DataFrame,
    df_all_strategies: pd.DataFrame,
    account_balance: float
) -> pd.DataFrame:
    """
    Generate auditable decision record for each selected trade.
    
    CRITICAL: No trade is valid unless the system can explain:
    1. WHY this strategy was selected
    2. WHY this expiration and strike were chosen
    3. WHY liquidity is acceptable (with context)
    4. WHY the capital allocation and sizing were approved
    5. WHY other strategies for the same ticker were not chosen
    
    If ANY explanation is missing → Position_Valid = False
    
    Args:
        df: Selected trades with position sizing
        df_all_strategies: ALL strategies from Step 11 (for competitive comparison)
        account_balance: Account balance for context
    
    Returns:
        DataFrame with Selection_Audit column containing WHY explanations
    """
    
    df_audited = df.copy()
    audit_records = []
    
    logger.info(f"")
    logger.info(f"📋 GENERATING AUDITABLE DECISION RECORDS...")
    logger.info(f"   Auditing {len(df)} selected trades")
    
    for idx, row in df_audited.iterrows():
        ticker = row['Ticker']
        strategy = row.get('Primary_Strategy', 'Unknown')
        
        # Initialize audit components
        audit_parts = []
        missing_parts = []
        
        # ==========================================
        # 1. WHY THIS STRATEGY WAS SELECTED
        # ==========================================
        strategy_reason = _explain_strategy_selection(row)
        if strategy_reason:
            audit_parts.append(f"STRATEGY SELECTION: {strategy_reason}")
        else:
            missing_parts.append("Strategy Selection")
        
        # ==========================================
        # 2. WHY THIS EXPIRATION/STRIKE WAS CHOSEN
        # ==========================================
        contract_reason = _explain_contract_selection(row)
        if contract_reason:
            audit_parts.append(f"CONTRACT CHOICE: {contract_reason}")
        else:
            missing_parts.append("Contract Choice")
        
        # ==========================================
        # 3. WHY LIQUIDITY IS ACCEPTABLE
        # ==========================================
        liquidity_reason = _explain_liquidity_acceptance(row)
        if liquidity_reason:
            audit_parts.append(f"LIQUIDITY JUSTIFICATION: {liquidity_reason}")
        else:
            missing_parts.append("Liquidity Justification")
        
        # ==========================================
        # 4. WHY CAPITAL ALLOCATION WAS APPROVED
        # ==========================================
        capital_reason = _explain_capital_approval(row, account_balance)
        if capital_reason:
            audit_parts.append(f"CAPITAL ALLOCATION: {capital_reason}")
        else:
            missing_parts.append("Capital Allocation")
        
        # ==========================================
        # 5. WHY OTHER STRATEGIES WERE REJECTED
        # ==========================================
        competitive_reason = _explain_competitive_rejection(
            row, ticker, df_all_strategies
        )
        if competitive_reason:
            audit_parts.append(f"COMPETITIVE COMPARISON: {competitive_reason}")
        else:
            missing_parts.append("Competitive Comparison")
        
        # ==========================================
        # ASSEMBLE FINAL AUDIT RECORD
        # ==========================================
        if missing_parts:
            # INCOMPLETE AUDIT - MARK AS INVALID
            audit_record = (
                f"⚠️ INCOMPLETE AUDIT - Missing: {', '.join(missing_parts)}\n" +
                "\n".join(audit_parts)
            )
            logger.warning(f"   ⚠️ {ticker} ({strategy}): Incomplete audit - missing {missing_parts}")
        else:
            # COMPLETE AUDIT - VALID TRADE
            audit_record = "\n".join(audit_parts)
        
        audit_records.append(audit_record)
    
    # Add audit column
    df_audited['Selection_Audit'] = audit_records
    
    # Count complete vs incomplete
    complete = sum(1 for r in audit_records if 'INCOMPLETE' not in r)
    incomplete = len(audit_records) - complete
    
    logger.info(f"   ✅ Complete audits: {complete}/{len(audit_records)}")
    if incomplete > 0:
        logger.warning(f"   ⚠️ Incomplete audits: {incomplete} (will be marked invalid)")
    
    return df_audited


def _explain_strategy_selection(row: pd.Series) -> str:
    """
    Explain WHY this strategy was selected.
    
    Returns:
        Human-readable explanation or empty string if insufficient data
    """
    
    strategy = row.get('Primary_Strategy', None)
    if not strategy:
        return ""
    
    # Get selection criteria
    score = row.get('Comparison_Score', None)
    rank = row.get('Strategy_Rank', None)
    trade_bias = row.get('Trade_Bias', 'Unknown')
    
    parts = []
    
    # Core reason
    parts.append(f"{strategy} selected for {trade_bias} exposure")
    
    # Ranking justification
    if rank == 1:
        parts.append("ranked #1 among all strategies for this ticker")
    
    # Score justification
    if score is not None:
        if score >= 80:
            parts.append(f"excellent comparison score ({score:.1f}/100)")
        elif score >= 70:
            parts.append(f"strong comparison score ({score:.1f}/100)")
        elif score >= 60:
            parts.append(f"acceptable comparison score ({score:.1f}/100)")
        else:
            parts.append(f"comparison score ({score:.1f}/100)")
    
    # Greeks alignment (if available)
    greeks_quality = row.get('Greeks_Quality_Score', None)
    if greeks_quality is not None and greeks_quality >= 70:
        parts.append(f"favorable Greeks profile ({greeks_quality:.0f}/100)")
    
    # Signal strength (if available)
    confidence = row.get('Confidence', None)
    if confidence is not None and confidence >= 70:
        parts.append(f"high setup confidence ({confidence:.0f}%)")
    
    return "; ".join(parts) if parts else ""


def _explain_contract_selection(row: pd.Series) -> str:
    """
    Explain WHY this expiration and strike were chosen.
    
    Returns:
        Human-readable explanation or empty string if insufficient data
    """
    
    expiration = row.get('Expiration', row.get('Selected_Expiration', None))
    actual_dte = row.get('Actual_DTE', None)
    target_dte = row.get('Target_DTE', None)
    strike = row.get('Strike', None)
    
    if not expiration and actual_dte is None:
        return ""
    
    parts = []
    
    # Expiration choice
    if actual_dte is not None:
        parts.append(f"{actual_dte:.0f} DTE expiration")
        
        # Compare to target
        if target_dte is not None:
            diff = abs(actual_dte - target_dte)
            if diff <= 5:
                parts.append(f"matches target DTE ({target_dte:.0f})")
            else:
                parts.append(f"closest available to target ({target_dte:.0f} DTE)")
        
        # Horizon classification
        horizon = row.get('Horizon_Class', None)
        if horizon == 'LEAP':
            parts.append("LEAP horizon for long-term positioning")
        elif horizon == 'Medium':
            parts.append("medium-term horizon for strategy deployment")
        elif horizon == 'Short':
            parts.append("short-term horizon for tactical entry")
    elif expiration:
        parts.append(f"expiration {expiration}")
    
    # Strike selection
    if strike is not None:
        underlying_price = row.get('Underlying_Price', None)
        if underlying_price is not None:
            moneyness = (strike / underlying_price - 1) * 100
            if abs(moneyness) < 2:
                parts.append(f"ATM strike (${strike:.2f})")
            elif moneyness > 5:
                parts.append(f"OTM strike (${strike:.2f}, +{moneyness:.1f}%)")
            elif moneyness < -5:
                parts.append(f"ITM strike (${strike:.2f}, {moneyness:.1f}%)")
            else:
                parts.append(f"near-money strike (${strike:.2f})")
        else:
            parts.append(f"strike ${strike:.2f}")
    
    return "; ".join(parts) if parts else ""


def _explain_liquidity_acceptance(row: pd.Series) -> str:
    """
    Explain WHY liquidity is acceptable (with context from Step 9B).
    
    This is CRITICAL - volume must NEVER be a hard gate.
    Open Interest is primary, volume is secondary/contextual.
    
    Returns:
        Human-readable explanation or empty string if insufficient data
    """
    
    liquidity_class = row.get('Liquidity_Class', None)
    liquidity_context = row.get('Liquidity_Context', None)
    oi = row.get('Open_Interest', None)
    spread = row.get('Bid_Ask_Spread_Pct', None)
    horizon = row.get('Horizon_Class', 'Short')
    
    if not liquidity_class:
        # Fallback to numeric metrics
        if oi is None and spread is None:
            return ""
    
    parts = []
    
    # Primary liquidity assessment
    if liquidity_class:
        if liquidity_class == 'Excellent':
            parts.append("excellent liquidity - tight spreads, deep OI")
        elif liquidity_class == 'Good':
            parts.append("good liquidity - acceptable spreads, sufficient OI")
        elif liquidity_class == 'Acceptable':
            parts.append("acceptable liquidity for intended holding period")
        elif liquidity_class == 'Thin':
            parts.append("thin liquidity - requires context-aware execution")
        else:
            parts.append(f"liquidity class: {liquidity_class}")
    
    # Contextual justification from Step 9B
    if liquidity_context:
        parts.append(f"context: {liquidity_context}")
    
    # Numeric support
    if oi is not None:
        if oi >= 500:
            parts.append(f"deep OI ({oi:,} contracts)")
        elif oi >= 100:
            parts.append(f"adequate OI ({oi:,} contracts)")
        elif oi >= 50:
            parts.append(f"modest OI ({oi} contracts)")
        else:
            parts.append(f"limited OI ({oi} contracts)")
    
    if spread is not None:
        if spread <= 5:
            parts.append(f"tight spread ({spread:.1f}%)")
        elif spread <= 10:
            parts.append(f"moderate spread ({spread:.1f}%)")
        elif spread <= 20:
            parts.append(f"wide spread ({spread:.1f}%)")
        else:
            parts.append(f"very wide spread ({spread:.1f}%)")
    
    # Horizon adjustment
    if horizon == 'LEAP':
        parts.append("LEAP horizon - lower liquidity acceptable")
    
    return "; ".join(parts) if parts else ""


def _explain_capital_approval(row: pd.Series, account_balance: float) -> str:
    """
    Explain WHY the capital allocation and sizing were approved.
    
    Returns:
        Human-readable explanation or empty string if insufficient data
    """
    
    dollar_allocation = row.get('Dollar_Allocation', None)
    num_contracts = row.get('Num_Contracts', None)
    risk = row.get('Max_Position_Risk', None)
    
    if dollar_allocation is None:
        return ""
    
    parts = []
    
    # Allocation size
    allocation_pct = (dollar_allocation / account_balance) * 100
    if allocation_pct <= 2:
        parts.append(f"conservative allocation (${dollar_allocation:,.0f}, {allocation_pct:.1f}% of account)")
    elif allocation_pct <= 5:
        parts.append(f"moderate allocation (${dollar_allocation:,.0f}, {allocation_pct:.1f}% of account)")
    elif allocation_pct <= 10:
        parts.append(f"substantial allocation (${dollar_allocation:,.0f}, {allocation_pct:.1f}% of account)")
    else:
        parts.append(f"large allocation (${dollar_allocation:,.0f}, {allocation_pct:.1f}% of account)")
    
    # Contract quantity
    if num_contracts is not None:
        if num_contracts == 1:
            parts.append("single contract for risk control")
        elif num_contracts <= 3:
            parts.append(f"{num_contracts} contracts for moderate exposure")
        else:
            parts.append(f"{num_contracts} contracts for scaled position")
    
    # Risk assessment
    if risk is not None:
        risk_pct = (risk / account_balance) * 100
        if risk_pct <= 1:
            parts.append(f"low risk ({risk_pct:.1f}% of account)")
        elif risk_pct <= 2:
            parts.append(f"acceptable risk ({risk_pct:.1f}% of account)")
        else:
            parts.append(f"elevated risk ({risk_pct:.1f}% of account)")
    
    return "; ".join(parts) if parts else ""


def _explain_competitive_rejection(
    row: pd.Series,
    ticker: str,
    df_all_strategies: pd.DataFrame
) -> str:
    """
    Explain WHY other strategies for the same ticker were NOT chosen.
    
    This provides transparency into the competitive selection process.
    
    Returns:
        Human-readable explanation or empty string if insufficient data
    """
    
    # Find all strategies for this ticker
    ticker_strategies = df_all_strategies[
        df_all_strategies['Ticker'] == ticker
    ].copy()
    
    if len(ticker_strategies) <= 1:
        return "only viable strategy for this ticker"
    
    # Get selected strategy details
    selected_strategy = row.get('Primary_Strategy', 'Unknown')
    selected_rank = row.get('Strategy_Rank', None)
    selected_score = row.get('Comparison_Score', None)
    
    # Find competing strategies
    competitors = ticker_strategies[
        ticker_strategies['Primary_Strategy'] != selected_strategy
    ]
    
    if competitors.empty:
        return f"{len(ticker_strategies)} instances of same strategy compared"
    
    parts = []
    
    # Count total alternatives
    total_alternatives = len(ticker_strategies) - 1
    unique_alternatives = competitors['Primary_Strategy'].nunique()
    parts.append(f"selected over {total_alternatives} alternatives ({unique_alternatives} unique strategies)")
    
    # Explain why this one won
    if selected_rank == 1 and selected_score is not None:
        # Find next-best score
        other_scores = competitors.get('Comparison_Score', pd.Series())
        if not other_scores.empty:
            next_best = other_scores.max()
            margin = selected_score - next_best
            if margin >= 10:
                parts.append(f"clear winner (score advantage: {margin:.1f} points)")
            elif margin >= 5:
                parts.append(f"moderate advantage (score: {selected_score:.1f} vs {next_best:.1f})")
            else:
                parts.append(f"narrow advantage (score: {selected_score:.1f} vs {next_best:.1f})")
    
    # Mention top rejected strategies
    top_rejected = competitors.nlargest(2, 'Comparison_Score', keep='first')
    if not top_rejected.empty:
        rejected_names = top_rejected['Primary_Strategy'].tolist()
        parts.append(f"rejected alternatives: {', '.join(rejected_names)}")
    
    return "; ".join(parts) if parts else ""


def _log_audit_summary(df: pd.DataFrame) -> None:
    """
    Log summary of audit record completeness.
    
    Args:
        df: Final trades with Selection_Audit column
    """
    
    if 'Selection_Audit' not in df.columns:
        logger.warning("⚠️ No Selection_Audit column found - skipping audit summary")
        return
    
    logger.info(f"")
    logger.info(f"📋 AUDIT RECORD SUMMARY:")
    
    # Count complete audits
    complete = df['Selection_Audit'].str.contains('INCOMPLETE', na=False).sum() == 0
    incomplete = df['Selection_Audit'].str.contains('INCOMPLETE', na=False).sum()
    
    if incomplete == 0:
        logger.info(f"   ✅ All {len(df)} trades have complete audit records")
    else:
        logger.warning(f"   ⚠️ {incomplete}/{len(df)} trades have incomplete audits (marked invalid)")
    
    # Sample audit record
    if len(df) > 0:
        sample_ticker = df.iloc[0]['Ticker']
        sample_audit = df.iloc[0]['Selection_Audit']
        logger.info(f"")
        logger.info(f"   Sample audit ({sample_ticker}):")
        for line in sample_audit.split('\n')[:3]:  # Show first 3 lines
            logger.info(f"      {line}")
        audit_lines = sample_audit.split('\n')
        if len(audit_lines) > 3:
            logger.info(f"      ... ({len(audit_lines) - 3} more lines)")


def _log_final_selection_summary(df: pd.DataFrame, account_balance: float, input_count: int) -> None:
    """
    Log summary of final selection and position sizing.
    
    Args:
        df: Final selected trades
        account_balance: Account balance
        input_count: Initial strategy count
    """
    
    if df.empty:
        logger.warning("⚠️ No final trades selected")
        return
    
    # Safely get values with defaults
    total_allocation = df['Dollar_Allocation'].sum() if 'Dollar_Allocation' in df.columns else 0
    total_risk = df['Max_Position_Risk'].sum() if 'Max_Position_Risk' in df.columns else 0
    total_contracts = df['Num_Contracts'].sum() if 'Num_Contracts' in df.columns else 0
    
    logger.info(f"")
    logger.info(f"📊 FINAL SELECTION SUMMARY:")
    logger.info(f"   Input strategies: {input_count}")
    logger.info(f"   Final trades: {len(df)} ({len(df)/input_count*100:.1f}% selection rate)")
    logger.info(f"   Unique tickers: {df['Ticker'].nunique()}")
    logger.info(f"")
    
    if total_allocation > 0:
        logger.info(f"💰 CAPITAL ALLOCATION:")
        logger.info(f"   Total allocation: ${total_allocation:,.0f} ({total_allocation/account_balance*100:.1f}% of account)")
        logger.info(f"   Total risk: ${total_risk:,.0f} ({total_risk/account_balance*100:.1f}% of account)")
        logger.info(f"   Total contracts: {total_contracts}")
        logger.info(f"   Avg allocation/trade: ${total_allocation/len(df):,.0f}")
        logger.info(f"")
    
    logger.info(f"📈 STRATEGY DISTRIBUTION:")
    strategy_counts = df['Primary_Strategy'].value_counts()
    for strategy, count in strategy_counts.head(10).items():
        pct = count / len(df) * 100
        logger.info(f"   {strategy}: {count} ({pct:.1f}%)")
    
    # Comparison score distribution
    if 'Comparison_Score' in df.columns:
        avg_score = df['Comparison_Score'].mean()
        min_score = df['Comparison_Score'].min()
        max_score = df['Comparison_Score'].max()
        logger.info(f"")
        logger.info(f"🎯 COMPARISON SCORES:")
        logger.info(f"   Average: {avg_score:.2f}")
        logger.info(f"   Range: {min_score:.2f} - {max_score:.2f}")

# ============================================================
# LEGACY FUNCTIONS (Deprecated - Moved to _legacy_step8_functions.py)
# ============================================================
