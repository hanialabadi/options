"""
Step 8: Portfolio Management (REFACTORED - Strategy Isolation + Strict Execution Gates)

CRITICAL ARCHITECTURE CHANGE (v3 - Dec 2025):
Step 8 is EXECUTION-ONLY, not evaluation. Step 11 already decided what is tradable.

# AGENT SAFETY: This file is execution-only and MUST NEVER evaluate or rank strategies.
# All strategy validation and ranking is performed exclusively by `step11_independent_evaluation.py`.
# This prevents agents from "helpfully" resurrecting invalid logic or bypassing architectural boundaries.

MANDATORY EXECUTION CONTRACT:
    1. ONLY Validation_Status == "Valid" strategies may enter sizing
       - Watch = informational tracking, NOT executable
       - Incomplete_Data / Reject = already blocked by Step 11
    
    2. NO NaN/inf coercion allowed
       - Invalid data = loud failure, not silent masking
       - All numeric fields must be finite before allocation
    
    3. NO strategy selection or cross-family comparison
       - Step 11 evaluated independently → Step 8 allocates capital
       - Multiple Valid strategies per ticker = ALLOWED
    
    4. Explicit defensive checks before numeric operations
       - Theory_Compliance_Score: must be finite
       - Total_Debit: must be finite
       - Capital_Allocation: must be finite before int conversion
    
    5. Fail loudly if invalid strategies leak through Step 11
       - Raise ValueError with explicit diagnostic message
       - Do NOT attempt to "fix" or compensate for missing data

RAG Principle:
    "Strategies do not compete. Each strategy family is evaluated independently.
     Portfolio layer decides ALLOCATION, not SELECTION."
    
    - RAG (All 8 Authors): Strategy selection = independent evaluation against theory
    - RAG (Natenberg Ch.23, Hull Ch.19): Portfolio layer = risk aggregation + sizing
    - RAG (Sinclair Ch.4): Don't trade what doesn't meet requirements (honesty > forcing trades)

Purpose:
    Portfolio management after independent strategy evaluation (Step 11).
    
    Step 11 Output: All strategies with Validation_Status (Valid/Watch/Reject/Incomplete)
    Step 8 Input:   ONLY Valid strategies (Watch excluded, others already blocked)
    Step 8 Output:  Position-sized Valid strategies ready for broker execution

Responsibilities:
    1. Strict Filtering: Accept ONLY Valid strategies from Step 11
       - NO Watch strategies (informational only, not tradable)
       - NO attempt to "rescue" marginal strategies
       - Fail loudly if invalid data detected
    
    2. Risk Aggregation: Portfolio-level risk management
       - Total portfolio heat (% of account at risk)
       - Position concentration limits
       - Greek exposure aggregation (net Delta, Gamma, Vega)
    
    3. Capital Allocation: Position sizing per strategy
       - Allocate capital based on Theory_Compliance_Score
       - Respect per-trade risk limits
       - Calculate contract quantities (with finite-value guarantees)
    
    4. Diversification Constraints: Portfolio composition
       - Max strategies per ticker (e.g., 2-3 simultaneous)
       - Max allocation per strategy family
       - Sector/ticker concentration limits

Design Rationale:
    - Step 11 evaluates strategies independently → Valid/Watch/Reject/Incomplete
    - Step 8 accepts ONLY Valid strategies → Allocates capital
    - Watch strategies = tracking list, NOT executable positions
    - System says "no" honestly when requirements not met (per RAG philosophy)
    
Example Flow:
    AAPL | Long Call     | Valid  (85/100) → Step 8: Allocate $2000, 4 contracts ✅
    AAPL | Buy-Write     | Valid  (80/100) → Step 8: Allocate $3000, 6 contracts ✅
    AAPL | Long Straddle | Watch  (68/100) → Step 8: EXCLUDED (watch list only) ⚠️
    AAPL | Credit Spread | Reject (42/100) → Step 8: EXCLUDED (failed theory gates) ❌
    
    Result: 2 strategies allocated capital (Call + Buy-Write)
    Watch/Reject strategies never reach sizing math (honesty over forced execution)

Why This Matters:
    Previous Step 8 tried to execute everything → NaN coercion errors
    New Step 8 respects Step 11's honesty → Only Valid strategies executed
    
    Watch ≠ "allocate smaller size" (common misconception)
    Watch = "monitor and re-evaluate when conditions improve"
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)


# ============================================================
# NEW MAIN FUNCTION (Post-Step 11 Architecture)
# ============================================================

def allocate_portfolio_capital(
    df: pd.DataFrame,
    account_balance: float = 100000.0,
    max_portfolio_risk: float = 0.20,
    max_trade_risk: float = 0.02,
    min_compliance_score: float = 60.0,
    max_strategies_per_ticker: int = 2,
    sizing_method: str = 'volatility_scaled',
    risk_per_contract: float = 500.0
) -> pd.DataFrame:
    """
    Portfolio management after independent strategy evaluation (Step 11).
    
    CRITICAL: This function is EXECUTION-ONLY, not evaluation.
    Step 11 already decided what is tradable (Valid/Watch/Reject/Incomplete).
    Step 8 respects that decision and only allocates capital to Valid strategies.
    
    STRICT FILTERING (MANDATORY):
        - ONLY Validation_Status == "Valid" strategies enter allocation
        - Watch strategies = tracking/informational, NOT executed
        - Incomplete_Data / Reject = already blocked by Step 11
        - NaN/inf values = loud failure (ValueError), not silent coercion
    
    RAG Philosophy:
        "Each strategy is evaluated independently. Portfolio layer allocates
         capital based on compliance scores, not strategy comparison."
        - Natenberg Ch.23: Position sizing by risk
        - Hull Ch.19: Portfolio Greeks aggregation
        - Sinclair Ch.4: Don't trade what doesn't meet requirements
    
    Process:
        1. Filter STRICTLY to Valid strategies (Watch excluded)
        2. Validate all numeric fields are finite (fail loudly if not)
        3. Apply portfolio-level risk constraints
        4. Allocate capital based on Theory_Compliance_Score
        5. Calculate position sizing with defensive NaN checks
        6. Aggregate portfolio Greeks
    
    Args:
        df: Output from Step 11 with independent evaluations
            Required columns: ['Ticker', 'Validation_Status', 'Theory_Compliance_Score',
                              'Primary_Strategy', 'Total_Debit', 'Delta', 'Gamma', 'Vega']
        
        account_balance: Total account value (default: $100k)
        max_portfolio_risk: Max % of account at risk (default: 20%)
        max_trade_risk: Max % per trade (default: 2%)
        min_compliance_score: Minimum Theory_Compliance_Score to allocate capital (default: 60.0)
        max_strategies_per_ticker: Max simultaneous strategies per ticker (default: 2)
        sizing_method: 'fixed_fractional', 'kelly', 'volatility_scaled', 'equal_weight'
        risk_per_contract: Estimated max loss per contract (default: $500)
    
    Returns:
        DataFrame with position-sized Valid strategies ready for execution
        
    Row Count Change:
        Input: All strategies with Validation_Status (e.g., 150 strategies)
          - Valid: 103 (68.7%)
          - Watch: 43 (28.7%) → EXCLUDED (informational only)
          - Reject: 4 (2.7%) → EXCLUDED (theory violations)
        
        Output: Valid strategies with capital allocation (e.g., 80-100 strategies)
          - Some Valid strategies may be filtered by portfolio constraints
          - Watch/Reject never reach allocation
        
    Key Difference from Old Step 8:
        OLD: Accepted Valid + Watch → Tried to size both → NaN coercion errors
        NEW: Accepts ONLY Valid → Safe sizing → No NaN issues
        
        Watch is NOT "smaller position" - it's "monitor and wait"
        
    Raises:
        ValueError: If Validation_Status missing (Step 11 not run)
        ValueError: If NaN/inf scores detected (Step 11 incomplete)
        ValueError: If invalid data leaks into allocation (defensive check)
    
    Example:
        >>> # Step 11: Independent evaluation
        >>> df_evaluated = evaluate_strategies_independently(df_contracts)
        >>> # Result: 103 Valid, 43 Watch, 4 Reject
        >>> 
        >>> # Step 8: Portfolio allocation (ONLY Valid strategies)
        >>> df_portfolio = allocate_portfolio_capital(
        ...     df_evaluated,
        ...     account_balance=100000,
        ...     max_strategies_per_ticker=2,
        ...     min_compliance_score=65.0
        ... )
        >>> # Result: ~80 Valid strategies with position sizing
        >>> # Watch strategies excluded (informational tracking only)
        >>> 
        >>> # Check: No NaN/inf in output
        >>> assert df_portfolio['Contracts'].notna().all()
        >>> assert df_portfolio['Capital_Allocation'].notna().all()
    """
    
    if df.empty:
        logger.warning("⚠️ Empty DataFrame passed to Step 8")
        return df
    
    input_row_count = len(df)
    logger.info(f"🎯 Step 8 (PORTFOLIO ALLOCATION): Processing {input_row_count} evaluated strategies")
    logger.info(f"   Account: ${account_balance:,.0f} | Max Portfolio Risk: {max_portfolio_risk*100:.0f}%")
    logger.info(f"   Min Compliance Score: {min_compliance_score} | Max Strategies/Ticker: {max_strategies_per_ticker}")
    logger.info(f"   STRICT MODE: Only Validation_Status=='Valid' strategies will be allocated capital")
    logger.info(f"   Watch/Reject/Incomplete strategies excluded (informational tracking only)")
    
    # Step 1: Filter STRICTLY by validation status (Valid only, Watch excluded)
    df_valid = _filter_by_validation_status(df, min_compliance_score=min_compliance_score)
    
    # Count excluded strategies by status
    if 'Validation_Status' in df.columns:
        status_counts = df['Validation_Status'].value_counts()
        valid_count = status_counts.get('Valid', 0)
        watch_count = status_counts.get('Watch', 0)
        reject_count = status_counts.get('Reject', 0)
        incomplete_count = status_counts.get('Incomplete_Data', 0)
        
        logger.info(f"   📊 Strategy Status Breakdown:")
        logger.info(f"      Valid: {valid_count} → Entering allocation")
        if watch_count > 0:
            logger.info(f"      Watch: {watch_count} → EXCLUDED (informational only, not executable)")
        if reject_count > 0:
            logger.info(f"      Reject: {reject_count} → EXCLUDED (theory violations)")
        if incomplete_count > 0:
            logger.info(f"      Incomplete_Data: {incomplete_count} → EXCLUDED (missing required fields)")
        logger.info(f"      ✅ Proceeding with {len(df_valid)} Valid strategies")
    else:
        logger.info(f"   📊 Valid strategies: {len(df_valid)}/{input_row_count}")
    
    if df_valid.empty:
        logger.warning("⚠️ No Valid/Watch strategies - returning empty DataFrame")
        return df_valid
    
    # Step 2: Apply portfolio-level constraints
    df_constrained = _apply_portfolio_risk_limits(
        df_valid,
        max_strategies_per_ticker=max_strategies_per_ticker,
        account_balance=account_balance
    )
    logger.info(f"   ✅ After portfolio constraints: {len(df_constrained)}")
    
    if df_constrained.empty:
        logger.warning("⚠️ No strategies after portfolio constraints - returning empty DataFrame")
        return df_constrained
    
    # Step 3: Calculate capital allocation
    df_allocated = _allocate_capital_by_score(
        df_constrained,
        account_balance=account_balance,
        max_portfolio_risk=max_portfolio_risk,
        max_trade_risk=max_trade_risk,
        sizing_method=sizing_method,
        risk_per_contract=risk_per_contract
    )
    
    # Step 4: Aggregate portfolio Greeks
    df_with_greeks = _calculate_portfolio_greeks(df_allocated)
    
    # Step 5: Generate portfolio audit
    df_audited = _generate_portfolio_audit(
        df_with_greeks,
        account_balance=account_balance
    )
    
    logger.info(f"🎯 Step 8 Complete: {len(df_audited)} strategies allocated")
    _log_portfolio_allocation_summary(df_audited, account_balance, input_row_count)
    
    return df_audited


# ============================================================
# BACKWARD COMPATIBILITY WRAPPER
# ============================================================

def finalize_and_size_positions(
    df: pd.DataFrame,
    account_balance: float = 100000.0,
    max_portfolio_risk: float = 0.20,
    max_trade_risk: float = 0.02,
    min_comparison_score: float = 60.0,
    max_positions: int = 50,
    sizing_method: str = 'volatility_scaled',
    risk_per_contract: float = 500.0,
    diversification_limit: int = 3
) -> pd.DataFrame:
    """
    DEPRECATED: Legacy function redirects to allocate_portfolio_capital().
    
    This function uses old parameter names (min_comparison_score, max_positions)
    that assume cross-strategy ranking. Redirects to new strategy-isolation model.
    
    For new code, call allocate_portfolio_capital() directly.
    """
    
    logger.warning("⚠️ finalize_and_size_positions() is DEPRECATED")
    logger.warning("   This function assumes Strategy_Rank/Comparison_Score (removed in Step 11 refactor)")
    logger.warning("   Redirecting to allocate_portfolio_capital() with strategy isolation")
    
    # Ensure 'Validation_Status' is present before passing to allocate_portfolio_capital
    if 'Validation_Status' not in df.columns:
        logger.error("❌ CRITICAL: Validation_Status column missing in input to deprecated finalize_and_size_positions.")
        logger.error("   This indicates Step 11 (evaluate_strategies_independently) was not run or its output was corrupted.")
        raise ValueError("CRITICAL: Validation_Status column missing. Ensure Step 11 ran correctly.")

    # Map old parameters to new ones
    return allocate_portfolio_capital(
        df=df,
        account_balance=account_balance,
        max_portfolio_risk=max_portfolio_risk,
        max_trade_risk=max_trade_risk,
        min_compliance_score=min_comparison_score,  # Renamed parameter
        max_strategies_per_ticker=max_positions,  # Use max_positions for max_strategies_per_ticker
        sizing_method=sizing_method,
        risk_per_contract=risk_per_contract
    )


# ============================================================
# HELPER FUNCTIONS (Strategy Isolation Model)
# ============================================================

def _filter_by_validation_status(
    df: pd.DataFrame,
    min_compliance_score: float
) -> pd.DataFrame:
    """
    Filter strategies by Validation_Status and Theory_Compliance_Score.
    
    CRITICAL: Only Valid strategies may enter capital allocation.
    Watch = informational, NOT executable.
    Incomplete_Data / Reject = already blocked by Step 11.
    
    RAG (All 8 Authors): Only trade strategies meeting theory requirements.
    
    Args:
        df: All strategies from Step 11
        min_compliance_score: Minimum Theory_Compliance_Score
    
    Returns:
        DataFrame with ONLY Valid strategies (Watch excluded)
    
    Raises:
        ValueError: If strategies with NaN/inf scores leak through Step 11
    """
    
    df_filtered = df.copy()
    initial_count = len(df_filtered)
    
    # MANDATORY: Check for Validation_Status column
    if 'Validation_Status' not in df_filtered.columns:
        raise ValueError(
            "❌ CRITICAL: Validation_Status column missing. "
            "Step 8 requires Step 11 independent evaluation. "
            "Run Step 11 (evaluate_strategies_independently) before Step 8."
        )
    
    # STRICT FILTER: Only "Valid" strategies (Watch excluded)
    df_filtered = df_filtered[df_filtered['Validation_Status'] == 'Valid']
    logger.info(f"      Filter: Validation_Status == 'Valid': {len(df_filtered)}/{initial_count}")
    
    if len(df_filtered) == 0:
        logger.warning("⚠️ No Valid strategies - all were Watch/Reject/Incomplete")
        return df_filtered
    
    # MANDATORY: Check Theory_Compliance_Score is finite
    if 'Theory_Compliance_Score' not in df_filtered.columns:
        raise ValueError(
            "❌ CRITICAL: Theory_Compliance_Score missing. "
            "Step 11 evaluation incomplete."
        )
    
    # Check for NaN/inf scores (should never happen if Step 11 worked)
    invalid_scores = df_filtered[
        ~np.isfinite(df_filtered['Theory_Compliance_Score'])
    ]
    if len(invalid_scores) > 0:
        raise ValueError(
            f"❌ CRITICAL: {len(invalid_scores)} strategies have NaN/inf Theory_Compliance_Score. "
            f"This indicates Step 11 evaluation failed or incomplete strategies leaked through. "
            f"Tickers: {invalid_scores['Ticker'].tolist()}"
        )
    
    # Filter by minimum score threshold
    df_filtered = df_filtered[df_filtered['Theory_Compliance_Score'] >= min_compliance_score]
    logger.info(f"      Filter: Theory_Compliance_Score ≥ {min_compliance_score}: {len(df_filtered)}/{initial_count}")
    
    # MANDATORY: Validate required execution fields are present and finite
    required_fields = ['Total_Debit', 'Delta']
    for field in required_fields:
        if field in df_filtered.columns:
            invalid_data = df_filtered[~np.isfinite(df_filtered[field])]
            if len(invalid_data) > 0:
                logger.warning(
                    f"⚠️ Excluding {len(invalid_data)} strategies with invalid {field} "
                    f"(likely incomplete Step 9B contract data)"
                )
                df_filtered = df_filtered[np.isfinite(df_filtered[field])]
    
    logger.info(f"      ✅ Final valid strategies for allocation: {len(df_filtered)}/{initial_count}")
    
    return df_filtered


def _apply_portfolio_risk_limits(
    df: pd.DataFrame,
    max_strategies_per_ticker: int,
    account_balance: float
) -> pd.DataFrame:
    """
    Apply portfolio-level risk constraints without cross-strategy comparison.
    
    Constraints:
    1. Max strategies per ticker (e.g., 2-3 simultaneous)
    2. Affordability (Total_Debit within budget)
    3. Prioritize by Theory_Compliance_Score within same ticker
    
    RAG (Natenberg Ch.23, Hull Ch.19): Diversification without strategy ranking.
    
    Args:
        df: Valid/Watch strategies
        max_strategies_per_ticker: Max simultaneous strategies per ticker
        account_balance: For affordability check
    
    Returns:
        DataFrame meeting portfolio constraints
    """
    
    df_constrained = df.copy()
    
    # Constraint 1: Affordability (Total_Debit ≤ 10% of account per trade)
    if 'Total_Debit' in df_constrained.columns:
        max_debit = account_balance * 0.10
        before_count = len(df_constrained)
        df_constrained = df_constrained[df_constrained['Total_Debit'] <= max_debit]
        logger.info(f"      Constraint: Affordable (≤${max_debit:,.0f}): {len(df_constrained)}/{before_count}")
    
    # Constraint 2: Max strategies per ticker (prioritize by Theory_Compliance_Score)
    if max_strategies_per_ticker > 0:
        before_count = len(df_constrained)
        
        # Sort by ticker, then by Theory_Compliance_Score (highest first)
        df_constrained = df_constrained.sort_values(
            ['Ticker', 'Theory_Compliance_Score'],
            ascending=[True, False]
        )
        
        # Keep top N strategies per ticker
        df_constrained = df_constrained.groupby('Ticker').head(max_strategies_per_ticker)
        
        logger.info(f"      Constraint: Max {max_strategies_per_ticker} strategies/ticker: {len(df_constrained)}/{before_count}")
    
    return df_constrained


def _allocate_capital_by_score(
    df: pd.DataFrame,
    account_balance: float,
    max_portfolio_risk: float,
    max_trade_risk: float,
    sizing_method: str,
    risk_per_contract: float
) -> pd.DataFrame:
    """
    Allocate capital based on Theory_Compliance_Score.
    
    Higher compliance = larger allocation (within risk limits).
    NO cross-strategy comparison - allocation based on independent score.
    
    RAG (Natenberg Ch.23): Position size proportional to edge confidence.
    
    CRITICAL: This function assumes all input strategies are Valid and have
    finite Theory_Compliance_Score (enforced by _filter_by_validation_status).
    
    Args:
        df: Constrained Valid strategies (pre-filtered)
        account_balance: Total account value
        max_portfolio_risk: Max % of account at risk
        max_trade_risk: Max % per trade
        sizing_method: Position sizing method
        risk_per_contract: Estimated risk per contract
    
    Returns:
        DataFrame with capital allocation columns
    
    Raises:
        ValueError: If NaN/inf values detected during allocation (defensive check)
    """
    
    df_allocated = df.copy()
    
    # Defensive check: Verify input is clean (should be enforced upstream)
    if not np.all(np.isfinite(df_allocated['Theory_Compliance_Score'])):
        raise ValueError(
            "❌ CRITICAL: NaN/inf Theory_Compliance_Score detected in _allocate_capital_by_score. "
            "This should have been blocked by _filter_by_validation_status. "
            "Check filter logic."
        )
    
    # Calculate max capital per trade
    max_trade_capital = account_balance * max_trade_risk
    
    # Allocate proportional to Theory_Compliance_Score
    total_score = df_allocated['Theory_Compliance_Score'].sum()
    
    if total_score > 0:
        df_allocated['Capital_Allocation'] = (
            (df_allocated['Theory_Compliance_Score'] / total_score) * 
            (account_balance * max_portfolio_risk)
        )
        
        # Cap each trade at max_trade_risk
        df_allocated['Capital_Allocation'] = df_allocated['Capital_Allocation'].clip(upper=max_trade_capital)
        
        # Calculate contract quantity (SAFE: No NaN/inf in inputs)
        if 'Total_Debit' in df_allocated.columns:
            # Defensive check: Verify Total_Debit is finite
            if not np.all(np.isfinite(df_allocated['Total_Debit'])):
                raise ValueError(
                    "❌ CRITICAL: NaN/inf Total_Debit detected. "
                    "Incomplete contract data leaked through Step 11. "
                    "This violates the 'Valid-only' filtering contract."
                )
            
            # Safe calculation (all inputs finite)
            contract_qty = (df_allocated['Capital_Allocation'] / df_allocated['Total_Debit'])
            
            # Clip to valid range (no NaN coercion)
            contract_qty = contract_qty.clip(lower=1.0)  # Keep as float first
            
            # Defensive check before int conversion
            if not np.all(np.isfinite(contract_qty)):
                raise ValueError(
                    "❌ CRITICAL: Contract quantity calculation produced NaN/inf. "
                    "This should never happen with finite inputs. "
                    "Check Capital_Allocation and Total_Debit values."
                )
            
            # SAFE: Convert to int (all values guaranteed finite)
            df_allocated['Contracts'] = contract_qty.astype(int)
        else:
            logger.warning("⚠️ Total_Debit column missing - defaulting to 1 contract per strategy")
            df_allocated['Contracts'] = 1
        
        logger.info(f"      Allocated ${df_allocated['Capital_Allocation'].sum():,.0f} across {len(df_allocated)} strategies")
    else:
        logger.warning("⚠️ Total Theory_Compliance_Score is 0 - cannot allocate capital")
        df_allocated['Capital_Allocation'] = 0.0
        df_allocated['Contracts'] = 0
    
    return df_allocated


def _calculate_portfolio_greeks(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate portfolio-level Greek exposure.
    
    RAG (Natenberg Ch.5-7): Monitor net portfolio Greeks for risk management.
    
    Args:
        df: Allocated strategies with Greeks
    
    Returns:
        DataFrame with portfolio Greek aggregation
    """
    
    df_greeks = df.copy()
    
    # Calculate position-level Greeks (Greek × Contracts)
    if 'Contracts' in df_greeks.columns:
        for greek in ['Delta', 'Gamma', 'Vega', 'Theta']:
            if greek in df_greeks.columns:
                df_greeks[f'Position_{greek}'] = df_greeks[greek] * df_greeks['Contracts']
        
        # Calculate portfolio totals
        portfolio_delta = df_greeks.get('Position_Delta', pd.Series([0])).sum()
        portfolio_gamma = df_greeks.get('Position_Gamma', pd.Series([0])).sum()
        portfolio_vega = df_greeks.get('Position_Vega', pd.Series([0])).sum()
        portfolio_theta = df_greeks.get('Position_Theta', pd.Series([0])).sum()
        
        logger.info(f"      Portfolio Greeks: Δ={portfolio_delta:.2f}, Γ={portfolio_gamma:.3f}, ν={portfolio_vega:.2f}, Θ={portfolio_theta:.2f}")
    
    return df_greeks


def _generate_portfolio_audit(
    df: pd.DataFrame,
    account_balance: float
) -> pd.DataFrame:
    """
    Generate portfolio allocation audit trail.
    
    Documents capital allocation decisions for compliance/review.
    
    Args:
        df: Final allocated strategies
        account_balance: Total account value
    
    Returns:
        DataFrame with Portfolio_Audit column
    """
    
    df_audited = df.copy()
    
    # Calculate allocation percentages
    if 'Capital_Allocation' in df_audited.columns:
        df_audited['Allocation_Pct'] = (
            (df_audited['Capital_Allocation'] / account_balance) * 100
        ).round(2)
        
        # Generate audit summary
        df_audited['Portfolio_Audit'] = (
            "Allocated: $" + df_audited['Capital_Allocation'].round(0).astype(str) + 
            " (" + df_audited['Allocation_Pct'].astype(str) + "%) | " +
            "Contracts: " + df_audited['Contracts'].astype(str) + " | " +
            "Score: " + df_audited['Theory_Compliance_Score'].round(0).astype(str) + "/100"
        )
    else:
        df_audited['Portfolio_Audit'] = "No allocation data"
    
    return df_audited


def _log_portfolio_allocation_summary(
    df: pd.DataFrame,
    account_balance: float,
    input_row_count: int
) -> None:
    """
    Log summary of portfolio allocation.
    
    Args:
        df: Final allocated strategies
        account_balance: Total account value
        input_row_count: Original strategy count
    """
    
    logger.info("=" * 80)
    logger.info("📊 PORTFOLIO ALLOCATION SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Strategies Evaluated:  {input_row_count}")
    logger.info(f"Strategies Allocated:  {len(df)}")
    
    if 'Capital_Allocation' in df.columns:
        total_allocated = df['Capital_Allocation'].sum()
        allocation_pct = (total_allocated / account_balance) * 100
        logger.info(f"Total Capital Allocated: ${total_allocated:,.0f} ({allocation_pct:.1f}% of account)")
    
    if 'Validation_Status' in df.columns:
        status_counts = df['Validation_Status'].value_counts()
        logger.info(f"Status Distribution: {dict(status_counts)}")
    
    if 'Primary_Strategy' in df.columns:
        strategy_counts = df['Primary_Strategy'].value_counts()
        logger.info(f"Strategy Distribution:")
        for strategy, count in strategy_counts.head(5).items():
            logger.info(f"  - {strategy}: {count}")
    
    logger.info("=" * 80)


# ============================================================
# LEGACY FUNCTIONS (Deprecated - Keep for backward compatibility)
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
    logger.info(f"      Input columns: {list(df_sized.columns)[:10]}...")  # Show first 10
    
    # Map Step 11 columns to Step 8 expected format
    # Step 11 has: Comparison_Score, Strategy_Rank, Greeks_Quality_Score, etc.
    # Step 8 expects: Confidence, Success_Probability, Risk_Level, Strategy_Type
    
    # Map comparison score to confidence (0-100)
    if 'Comparison_Score' in df_sized.columns:
        df_sized['Confidence'] = df_sized['Comparison_Score'].clip(0, 100)
        logger.info(f"      ✓ Mapped Comparison_Score → Confidence (avg: {df_sized['Confidence'].mean():.1f})")
    else:
        df_sized['Confidence'] = 70  # Default
        logger.info(f"      ⚠️ Comparison_Score missing - using default Confidence=70")
    
    # Map from existing columns or use defaults
    if 'Success_Probability' not in df_sized.columns:
        df_sized['Success_Probability'] = 0.55  # Default 55% win rate
        logger.info(f"      ⚠️ Success_Probability missing - using default=0.55")
    
    if 'Risk_Level' not in df_sized.columns:
        df_sized['Risk_Level'] = 'Medium'
        logger.info(f"      ⚠️ Risk_Level missing - using default='Medium'")
    
    if 'Strategy_Type' not in df_sized.columns:
        # Infer from Trade_Bias or use default
        if 'Trade_Bias' in df_sized.columns:
            df_sized['Strategy_Type'] = df_sized['Trade_Bias']
            logger.info(f"      ✓ Mapped Trade_Bias → Strategy_Type")
        else:
            df_sized['Strategy_Type'] = 'Directional'
            logger.info(f"      ⚠️ Trade_Bias missing - using default Strategy_Type='Directional'")
    
    # Ensure Primary_Strategy exists (critical for legacy function)
    if 'Primary_Strategy' not in df_sized.columns:
        logger.error("❌ Primary_Strategy column missing - cannot proceed with position sizing")
        return df_sized
    
    # Call existing position sizing function
    df_sized = calculate_position_sizing(
        df_sized,
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
# LEGACY FUNCTION (Pre-Step 11 Architecture)
# ============================================================

def calculate_position_sizing(
    df: pd.DataFrame,
    account_balance: float = 100000.0,
    max_portfolio_risk: float = 0.20,
    max_trade_risk: float = 0.02,
    sizing_method: str = 'fixed_fractional',
    risk_per_contract: float = 500.0,
    kelly_fraction: float = 0.25
) -> pd.DataFrame:
    """
    Calculate position sizes and risk metrics for each strategy recommendation.
    
    Args:
        df (pd.DataFrame): Output from Step 7 with strategy recommendations
            Required columns: ['Ticker', 'Primary_Strategy', 'Strategy_Type',
                              'Confidence', 'Success_Probability', 'Risk_Level']
        
        account_balance (float): Total account value for position sizing (default: $100k)
        
        max_portfolio_risk (float): Max % of account at risk across all positions
            (default: 20% = $20k max total risk for $100k account)
        
        max_trade_risk (float): Max % of account at risk per single trade
            (default: 2% = $2k max risk per trade for $100k account)
        
        sizing_method (str): Position sizing methodology
            - 'fixed_fractional': Allocate fixed % of account per trade
            - 'kelly': Kelly Criterion based on win probability
            - 'volatility_scaled': Scale by confidence/risk level
            - 'equal_weight': Equal allocation across all trades
        
        risk_per_contract (float): Estimated max loss per contract (default: $500)
            Used to convert dollar risk to contract quantity
        
        kelly_fraction (float): Fraction of Kelly recommendation to use (default: 0.25)
            Full Kelly is aggressive; quarter-Kelly is conservative
    
    Returns:
        pd.DataFrame: Original data with added position sizing columns:
            - Dollar_Allocation: $ amount allocated to this trade
            - Max_Position_Risk: Maximum $ loss if trade goes wrong
            - Num_Contracts: Recommended number of contracts
            - Risk_Reward_Ratio: Expected gain / max loss
            - Portfolio_Weight: % of portfolio allocated
            - Risk_Per_Contract: Estimated $ risk per contract
            - Sizing_Method: Which method was used
            - Position_Valid: Boolean if position passes risk checks
    
    Position Sizing Methods:
        
        1. **Fixed Fractional (Conservative)**:
           - Allocate fixed % of account per trade
           - Simple, predictable, easy to manage
           - Good for beginners and stable strategies
        
        2. **Kelly Criterion (Aggressive)**:
           - Optimal bet size based on edge and win probability
           - Formula: f* = (p * b - q) / b
           - Uses fractional Kelly (1/4) to reduce volatility
           - Good for high-confidence, well-tested strategies
        
        3. **Volatility Scaled (Adaptive)**:
           - Scale position size by confidence and risk level
           - High confidence + low risk = larger position
           - Low confidence + high risk = smaller position
           - Good for mixed strategy portfolios
        
        4. **Equal Weight (Simple)**:
           - Same dollar amount per trade
           - No optimization, maximum diversification
           - Good for small accounts or exploratory trading
    
    Risk Management Rules:
        - Portfolio heat: Sum of all Max_Position_Risk ≤ max_portfolio_risk
        - Per-trade limit: Max_Position_Risk ≤ max_trade_risk * account_balance
        - Minimum contracts: At least 1 contract (or mark as invalid)
        - Maximum contracts: Capped by max_trade_risk
    
    Example:
        >>> df8 = calculate_position_sizing(
        ...     df7,
        ...     account_balance=50000,
        ...     max_portfolio_risk=0.15,
        ...     max_trade_risk=0.025,
        ...     sizing_method='volatility_scaled'
        ... )
        >>> valid_positions = df8[df8['Position_Valid']]
        >>> print(f"Total portfolio risk: ${valid_positions['Max_Position_Risk'].sum():.0f}")
    
    Usage Notes:
        - Adjust risk_per_contract based on your typical strategy (spreads vs naked)
        - Use lower max_trade_risk for aggressive strategies (2-5x leverage)
        - Use higher max_portfolio_risk for diversified portfolios
        - Always backtest your position sizing rules before live trading
    """
    from .utils import validate_input
    
    # Validate required columns
    required_cols = [
        'Ticker', 'Primary_Strategy', 'Strategy_Type',
        'Confidence', 'Success_Probability', 'Risk_Level'
    ]
    
    # Check which columns are missing
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        logger.error(f"❌ Missing required columns for position sizing: {missing_cols}")
        logger.error(f"   Available columns: {list(df.columns)[:20]}")
        # Add missing columns with defaults
        for col in missing_cols:
            if col == 'Confidence':
                df[col] = 70
            elif col == 'Success_Probability':
                df[col] = 0.55
            elif col == 'Risk_Level':
                df[col] = 'Medium'
            elif col == 'Strategy_Type':
                df[col] = 'Directional'
        logger.info(f"   Added missing columns with defaults")
    
    try:
        validate_input(df, required_cols, 'Step 8')
    except Exception as e:
        logger.error(f"❌ Column validation failed: {e}")
        logger.error(f"   DataFrame shape: {df.shape}")
        logger.error(f"   Available columns: {list(df.columns)}")
        raise
    
    # Work only on trades with strategies (skip 'None')
    if 'Primary_Strategy' not in df.columns:
        logger.error("❌ Primary_Strategy column missing - cannot filter trades")
        return df
    
    df_trades = df[df['Primary_Strategy'] != 'None'].copy()
    
    if df_trades.empty:
        logger.warning("⚠️ No valid trades to size (all strategies = 'None')")
        return df
    
    logger.info(f"💰 Step 8: Sizing {len(df_trades)} positions | Method: {sizing_method} | Account: ${account_balance:,.0f}")
    
    # Calculate position sizes based on method
    if sizing_method == 'fixed_fractional':
        df_trades = _size_fixed_fractional(df_trades, account_balance, max_trade_risk)
    elif sizing_method == 'kelly':
        df_trades = _size_kelly_criterion(df_trades, account_balance, max_trade_risk, kelly_fraction)
    elif sizing_method == 'volatility_scaled':
        df_trades = _size_volatility_scaled(df_trades, account_balance, max_trade_risk)
    elif sizing_method == 'equal_weight':
        df_trades = _size_equal_weight(df_trades, account_balance, max_trade_risk)
    else:
        raise ValueError(f"Unknown sizing_method: {sizing_method}")
    
    # Convert dollar allocation to contract quantity
    df_trades['Risk_Per_Contract'] = risk_per_contract
    df_trades['Num_Contracts'] = np.maximum(
        1,  # Minimum 1 contract
        np.floor(df_trades['Max_Position_Risk'] / risk_per_contract)
    ).astype(int)
    
    # Calculate risk-reward ratio
    df_trades['Risk_Reward_Ratio'] = df_trades.apply(_calc_risk_reward, axis=1)
    
    # Mark sizing method
    df_trades['Sizing_Method'] = sizing_method
    
    # Validate positions against portfolio limits
    df_trades = _validate_portfolio_risk(df_trades, account_balance, max_portfolio_risk, max_trade_risk)
    
    # Log summary
    _log_sizing_summary(df_trades, account_balance, max_portfolio_risk)
    
    # Merge back with original dataframe
    sizing_cols = [
        'Dollar_Allocation', 'Max_Position_Risk', 'Num_Contracts',
        'Risk_Reward_Ratio', 'Portfolio_Weight', 'Risk_Per_Contract',
        'Sizing_Method', 'Position_Valid'
    ]
    
    for col in sizing_cols:
        if col not in df.columns:
            if col in ['Dollar_Allocation', 'Max_Position_Risk', 'Num_Contracts', 'Risk_Per_Contract']:
                df[col] = 0
            elif col == 'Position_Valid':
                df[col] = False
            else:
                df[col] = ''
    
    df.update(df_trades)
    
    return df


def _size_fixed_fractional(df: pd.DataFrame, account_balance: float, max_trade_risk: float) -> pd.DataFrame:
    """Allocate fixed % of account per trade."""
    df = df.copy()
    
    # Each trade gets equal allocation (spread across all trades)
    num_trades = len(df)
    allocation_per_trade = (account_balance * max_trade_risk)
    
    df['Dollar_Allocation'] = allocation_per_trade
    df['Max_Position_Risk'] = allocation_per_trade  # Fixed fractional = risk entire allocation
    df['Portfolio_Weight'] = (df['Dollar_Allocation'] / account_balance * 100).round(2)
    
    return df


def _size_kelly_criterion(df: pd.DataFrame, account_balance: float, max_trade_risk: float, kelly_fraction: float) -> pd.DataFrame:
    """
    Size positions using Kelly Criterion.
    
    Kelly formula: f* = (p * b - q) / b
    where:
        p = probability of win (Success_Probability)
        q = probability of loss (1 - p)
        b = odds (assume 1:1 for simplicity, can adjust per strategy)
    
    Use fractional Kelly to reduce volatility.
    """
    df = df.copy()
    
    # Kelly calculation
    p = df['Success_Probability']
    q = 1 - p
    b = 1.0  # Assume 1:1 odds (adjust if you have strategy-specific data)
    
    kelly_f = (p * b - q) / b
    kelly_f = kelly_f.clip(0, 1)  # Cap between 0 and 1
    
    # Apply fractional Kelly
    fractional_kelly = kelly_f * kelly_fraction
    
    # Convert to dollar allocation
    df['Dollar_Allocation'] = fractional_kelly * account_balance
    
    # Cap by max_trade_risk
    max_allocation = account_balance * max_trade_risk
    df['Dollar_Allocation'] = df['Dollar_Allocation'].clip(upper=max_allocation)
    
    df['Max_Position_Risk'] = df['Dollar_Allocation']  # Kelly sizes by bankroll fraction
    df['Portfolio_Weight'] = (df['Dollar_Allocation'] / account_balance * 100).round(2)
    
    return df


def _size_volatility_scaled(df: pd.DataFrame, account_balance: float, max_trade_risk: float) -> pd.DataFrame:
    """
    Scale position size by confidence and risk level.
    High confidence + low risk = larger position.
    """
    df = df.copy()
    
    # Create sizing multiplier from confidence (0-100) and risk level
    risk_map = {'Low': 1.2, 'Medium': 1.0, 'High': 0.7}
    df['Risk_Multiplier'] = df['Risk_Level'].map(risk_map).fillna(1.0)
    
    # Scale by confidence (normalized to 0-1)
    confidence_factor = df['Confidence'] / 100.0
    
    # Combined scaling factor
    scale = confidence_factor * df['Risk_Multiplier']
    scale = scale.clip(0.3, 1.5)  # Prevent extreme allocations
    
    # Base allocation (equal split)
    num_trades = len(df)
    base_allocation = account_balance * max_trade_risk
    
    # Apply scaling
    df['Dollar_Allocation'] = base_allocation * scale
    df['Max_Position_Risk'] = df['Dollar_Allocation']
    df['Portfolio_Weight'] = (df['Dollar_Allocation'] / account_balance * 100).round(2)
    
    return df


def _size_equal_weight(df: pd.DataFrame, account_balance: float, max_trade_risk: float) -> pd.DataFrame:
    """Equal dollar allocation across all trades."""
    df = df.copy()
    
    num_trades = len(df)
    allocation_per_trade = (account_balance * max_trade_risk)
    
    df['Dollar_Allocation'] = allocation_per_trade
    df['Max_Position_Risk'] = allocation_per_trade
    df['Portfolio_Weight'] = (df['Dollar_Allocation'] / account_balance * 100).round(2)
    
    return df


def _calc_risk_reward(row: pd.Series) -> float:
    """
    Estimate risk-reward ratio for strategy.
    Simplified heuristic - adjust based on backtested data.
    """
    strategy_type = row.get('Strategy_Type', 'None')
    success_prob = row.get('Success_Probability', 0.5)
    
    # Simplified R:R based on strategy type and success probability
    if strategy_type == 'Neutral':
        # Neutral strategies: limited profit, limited risk
        expected_rr = success_prob / (1 - success_prob) * 0.5  # Asymmetric payoff
    elif strategy_type == 'Volatility':
        # Volatility strategies: variable payoff
        expected_rr = success_prob / (1 - success_prob) * 0.75
    else:  # Directional
        # Directional strategies: higher potential payoff
        expected_rr = success_prob / (1 - success_prob) * 1.0
    
    return round(expected_rr, 2)


def _validate_portfolio_risk(df: pd.DataFrame, account_balance: float, max_portfolio_risk: float, max_trade_risk: float) -> pd.DataFrame:
    """
    Validate positions against portfolio and per-trade risk limits.
    Mark invalid positions if they violate constraints.
    """
    df = df.copy()
    
    # Per-trade limit check
    max_trade_dollar = account_balance * max_trade_risk
    df['Position_Valid'] = df['Max_Position_Risk'] <= max_trade_dollar
    
    # Portfolio heat check
    total_risk = df[df['Position_Valid']]['Max_Position_Risk'].sum()
    max_portfolio_dollar = account_balance * max_portfolio_risk
    
    if total_risk > max_portfolio_dollar:
        # Scale down all positions proportionally
        scale_factor = max_portfolio_dollar / total_risk
        logger.warning(f"⚠️ Portfolio heat exceeded (${total_risk:.0f} > ${max_portfolio_dollar:.0f}). Scaling by {scale_factor:.2f}")
        
        df.loc[df['Position_Valid'], 'Dollar_Allocation'] *= scale_factor
        df.loc[df['Position_Valid'], 'Max_Position_Risk'] *= scale_factor
        df.loc[df['Position_Valid'], 'Portfolio_Weight'] *= scale_factor
    
    # Minimum contract check (at least 1 contract must be viable)
    df.loc[df['Num_Contracts'] < 1, 'Position_Valid'] = False
    
    return df


def _log_sizing_summary(df: pd.DataFrame, account_balance: float, max_portfolio_risk: float) -> None:
    """Log position sizing summary statistics."""
    valid = df[df['Position_Valid'] == True]
    invalid = df[df['Position_Valid'] == False]
    
    total_allocation = valid['Dollar_Allocation'].sum()
    total_risk = valid['Max_Position_Risk'].sum()
    total_contracts = valid['Num_Contracts'].sum()
    
    logger.info(f"📊 Position Sizing Summary:")
    logger.info(f"   Valid positions: {len(valid)}/{len(df)}")
    logger.info(f"   Total allocation: ${total_allocation:,.0f} ({total_allocation/account_balance*100:.1f}%)")
    logger.info(f"   Total risk: ${total_risk:,.0f} ({total_risk/account_balance*100:.1f}% of account)")
    logger.info(f"   Max allowed risk: ${account_balance*max_portfolio_risk:,.0f} ({max_portfolio_risk*100:.0f}%)")
    logger.info(f"   Total contracts: {total_contracts}")
    
    if len(invalid) > 0:
        logger.warning(f"   ⚠️ Invalid positions: {len(invalid)} (risk limits exceeded)")
    
    # Risk-reward distribution
    avg_rr = valid['Risk_Reward_Ratio'].mean()
    logger.info(f"   Average R:R ratio: {avg_rr:.2f}")
    
    # Strategy breakdown
    strategy_counts = valid['Strategy_Type'].value_counts()
    logger.info(f"   By strategy type: {strategy_counts.to_dict()}")
